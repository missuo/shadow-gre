package tunnel

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// WriteBufferSize is the size of write buffer before flushing
	WriteBufferSize = 4096
	// WriteFlushInterval is the max time to hold data before flushing
	WriteFlushInterval = 5 * time.Millisecond
	// ReadChannelSize is the size of read channel buffer
	ReadChannelSize = 512
)

// Connection represents a virtual connection over the tunnel
type Connection struct {
	ID      uint32
	manager *Manager

	// Read buffering
	readCh  chan []byte
	readBuf bytes.Buffer
	readMu  sync.Mutex

	// Write buffering
	writeBuf   bytes.Buffer
	writeMu    sync.Mutex
	flushTimer *time.Timer

	closed  atomic.Bool
	closeCh chan struct{}
	seq     atomic.Uint32
}

// NewConnection creates a new virtual connection
func NewConnection(id uint32, manager *Manager) *Connection {
	c := &Connection{
		ID:      id,
		manager: manager,
		readCh:  make(chan []byte, ReadChannelSize),
		closeCh: make(chan struct{}),
	}
	return c
}

// Read reads data from the connection
func (c *Connection) Read(p []byte) (n int, err error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	// First, try to read from existing buffer
	if c.readBuf.Len() > 0 {
		return c.readBuf.Read(p)
	}

	if c.closed.Load() {
		return 0, io.EOF
	}

	// Wait for first data block
	select {
	case data := <-c.readCh:
		if data == nil {
			return 0, io.EOF
		}
		c.readBuf.Write(data)
	case <-c.closeCh:
		return 0, io.EOF
	}

	// Try to read more data without blocking (drain the channel)
	for {
		select {
		case data := <-c.readCh:
			if data == nil {
				break
			}
			c.readBuf.Write(data)
			// Don't accumulate too much
			if c.readBuf.Len() >= len(p) {
				goto done
			}
		default:
			goto done
		}
	}

done:
	return c.readBuf.Read(p)
}

// Write writes data to the connection with buffering
func (c *Connection) Write(p []byte) (n int, err error) {
	if c.closed.Load() {
		return 0, io.ErrClosedPipe
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	n, _ = c.writeBuf.Write(p)

	// Flush if buffer is large enough
	if c.writeBuf.Len() >= WriteBufferSize {
		c.flushLocked()
		return n, nil
	}

	// Set timer to flush after interval
	if c.flushTimer == nil {
		c.flushTimer = time.AfterFunc(WriteFlushInterval, func() {
			c.writeMu.Lock()
			defer c.writeMu.Unlock()
			c.flushLocked()
		})
	}

	return n, nil
}

// flushLocked sends buffered data (must hold writeMu)
func (c *Connection) flushLocked() {
	if c.flushTimer != nil {
		c.flushTimer.Stop()
		c.flushTimer = nil
	}

	if c.writeBuf.Len() == 0 {
		return
	}

	data := c.writeBuf.Bytes()
	c.writeBuf.Reset()

	seq := c.seq.Add(1)
	frame := NewDataFrame(c.ID, seq, data)
	c.manager.sendFrame(frame)
}

// Flush forces a flush of the write buffer
func (c *Connection) Flush() error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.flushLocked()
	return nil
}

// Close closes the connection
func (c *Connection) Close() error {
	if c.closed.Swap(true) {
		return nil
	}

	// Flush remaining data
	c.Flush()

	close(c.closeCh)
	frame := NewFinFrame(c.ID)
	c.manager.sendFrame(frame)
	c.manager.removeConnection(c.ID)
	return nil
}

// deliver delivers data to the connection's read buffer
func (c *Connection) deliver(data []byte) {
	if c.closed.Load() {
		return
	}

	// Make a copy
	buf := make([]byte, len(data))
	copy(buf, data)

	select {
	case c.readCh <- buf:
	case <-c.closeCh:
	default:
		// Buffer full, try harder with short timeout
		select {
		case c.readCh <- buf:
		case <-c.closeCh:
		case <-time.After(10 * time.Millisecond):
			// Drop if still can't deliver
		}
	}
}

// Manager manages virtual connections over the tunnel
type Manager struct {
	sendFunc    func([]byte) error
	connections sync.Map // map[uint32]*Connection
	connIDSeq   atomic.Uint32
	acceptCh    chan *Connection
	onConnect   func(conn *Connection)
	mu          sync.RWMutex
}

// NewManager creates a new connection manager
func NewManager(sendFunc func([]byte) error) *Manager {
	return &Manager{
		sendFunc: sendFunc,
		acceptCh: make(chan *Connection, 64),
	}
}

// SetOnConnect sets the callback for new incoming connections (server mode)
func (m *Manager) SetOnConnect(fn func(conn *Connection)) {
	m.onConnect = fn
}

// Dial creates a new outgoing connection
func (m *Manager) Dial() (*Connection, error) {
	connID := m.connIDSeq.Add(1)
	conn := NewConnection(connID, m)
	m.connections.Store(connID, conn)

	// Send SYN
	synFrame := NewSynFrame(connID)
	if err := m.sendFrame(synFrame); err != nil {
		m.connections.Delete(connID)
		return nil, err
	}

	return conn, nil
}

// Accept accepts a new incoming connection (blocking)
func (m *Manager) Accept() (*Connection, error) {
	conn := <-m.acceptCh
	if conn == nil {
		return nil, io.EOF
	}
	return conn, nil
}

// HandleFrame processes an incoming frame
func (m *Manager) HandleFrame(data []byte) error {
	frame, err := UnmarshalFrame(data)
	if err != nil {
		return err
	}

	switch frame.Type {
	case FrameSyn:
		conn := NewConnection(frame.ConnID, m)
		m.connections.Store(frame.ConnID, conn)

		synAckFrame := NewSynAckFrame(frame.ConnID)
		if err := m.sendFrame(synAckFrame); err != nil {
			return err
		}

		if m.onConnect != nil {
			go m.onConnect(conn)
		} else {
			select {
			case m.acceptCh <- conn:
			default:
			}
		}

	case FrameSynAck:
		// Connection established

	case FrameData:
		if connI, ok := m.connections.Load(frame.ConnID); ok {
			conn := connI.(*Connection)
			conn.deliver(frame.Payload)
		}

	case FrameFin:
		if connI, ok := m.connections.Load(frame.ConnID); ok {
			conn := connI.(*Connection)
			conn.closed.Store(true)
			close(conn.closeCh)
			m.connections.Delete(frame.ConnID)

			finAckFrame := NewFinAckFrame(frame.ConnID)
			m.sendFrame(finAckFrame)
		}

	case FrameFinAck:
		m.connections.Delete(frame.ConnID)

	case FramePing:
		pongFrame := NewPongFrame()
		m.sendFrame(pongFrame)

	case FramePong:
		// Keepalive received
	}

	return nil
}

// sendFrame marshals and sends a frame
func (m *Manager) sendFrame(frame *Frame) error {
	data := frame.Marshal()
	return m.sendFunc(data)
}

// removeConnection removes a connection from the manager
func (m *Manager) removeConnection(id uint32) {
	m.connections.Delete(id)
}

// Close closes the manager and all connections
func (m *Manager) Close() error {
	m.connections.Range(func(key, value interface{}) bool {
		conn := value.(*Connection)
		conn.Close()
		return true
	})
	close(m.acceptCh)
	return nil
}
