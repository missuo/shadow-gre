package tunnel

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// MaxPayloadSize is the maximum payload per GRE packet (MTU - IP header - GRE header - frame header)
	// MTU(1500) - IP(20) - GRE(12) - Frame(11) = 1457, use 1400 for safety
	MaxPayloadSize = 1400
	// ReadChannelSize is the size of read channel buffer (increased for better throughput)
	ReadChannelSize = 16384
	// DialTimeout is the timeout for connection establishment
	DialTimeout = 5 * time.Second
)

var (
	ErrDialTimeout = errors.New("dial timeout: no SYN-ACK received")
)

// Connection represents a virtual connection over the tunnel
type Connection struct {
	ID      uint32
	manager *Manager

	// Read buffering
	readCh  chan []byte
	readBuf bytes.Buffer
	readMu  sync.Mutex

	// Connection state
	established chan struct{}
	closed      atomic.Bool
	closeCh     chan struct{}
	seq         atomic.Uint32

	// Diagnostics
	packetsDropped atomic.Uint64
}

// NewConnection creates a new virtual connection
func NewConnection(id uint32, manager *Manager) *Connection {
	c := &Connection{
		ID:          id,
		manager:     manager,
		readCh:      make(chan []byte, ReadChannelSize),
		closeCh:     make(chan struct{}),
		established: make(chan struct{}),
	}
	return c
}

// markEstablished marks the connection as established
func (c *Connection) markEstablished() {
	select {
	case <-c.established:
		// Already established
	default:
		close(c.established)
	}
}

// waitEstablished waits for connection to be established
func (c *Connection) waitEstablished(timeout time.Duration) error {
	select {
	case <-c.established:
		return nil
	case <-time.After(timeout):
		return ErrDialTimeout
	case <-c.closeCh:
		return io.EOF
	}
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
drainLoop:
	for {
		select {
		case data := <-c.readCh:
			if data == nil {
				break drainLoop
			}
			c.readBuf.Write(data)
			if c.readBuf.Len() >= len(p) {
				break drainLoop
			}
		default:
			break drainLoop
		}
	}
	return c.readBuf.Read(p)
}

// Write writes data to the connection
func (c *Connection) Write(p []byte) (n int, err error) {
	if c.closed.Load() {
		return 0, io.ErrClosedPipe
	}

	// For small writes, send immediately without buffering (better latency)
	if len(p) <= MaxPayloadSize {
		seq := c.seq.Add(1)
		frame := NewDataFrame(c.ID, seq, p)
		err = c.manager.sendFrame(frame)
		if err != nil {
			return 0, err
		}
		return len(p), nil
	}

	// For large writes, split into chunks
	written := 0
	for written < len(p) {
		end := written + MaxPayloadSize
		if end > len(p) {
			end = len(p)
		}
		chunk := p[written:end]

		seq := c.seq.Add(1)
		frame := NewDataFrame(c.ID, seq, chunk)
		if err = c.manager.sendFrame(frame); err != nil {
			return written, err
		}
		written = end
	}
	return written, nil
}

// Close closes the connection
func (c *Connection) Close() error {
	if c.closed.Swap(true) {
		return nil
	}

	// Safe close of closeCh
	select {
	case <-c.closeCh:
		// Already closed
	default:
		close(c.closeCh)
	}

	frame := NewFinFrame(c.ID)
	c.manager.sendFrame(frame)
	c.manager.removeConnection(c.ID)
	return nil
}

// deliver delivers data to the connection's read buffer (non-blocking)
func (c *Connection) deliver(data []byte) {
	if c.closed.Load() {
		return
	}

	buf := make([]byte, len(data))
	copy(buf, data)

	// Non-blocking send - if buffer is full, drop the packet
	// This is acceptable because GRE is unreliable and upper layer (TCP) will retransmit
	select {
	case c.readCh <- buf:
	case <-c.closeCh:
	default:
		// Buffer full, drop packet
		c.packetsDropped.Add(1)
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

	// Diagnostics
	framesSent       atomic.Uint64
	framesReceived   atomic.Uint64
	bytesPayloadSent atomic.Uint64
	bytesPayloadRecv atomic.Uint64
	packetsDropped   atomic.Uint64
}

// NewManager creates a new connection manager
func NewManager(sendFunc func([]byte) error) *Manager {
	return &Manager{
		sendFunc: sendFunc,
		acceptCh: make(chan *Connection, 64),
	}
}

// SetOnConnect sets the callback for new incoming connections
func (m *Manager) SetOnConnect(fn func(conn *Connection)) {
	m.onConnect = fn
}

// Dial creates a new outgoing connection with SYN-ACK handshake
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

	// Wait for SYN-ACK
	if err := conn.waitEstablished(DialTimeout); err != nil {
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

	m.framesReceived.Add(1)
	if frame.Type == FrameData {
		m.bytesPayloadRecv.Add(uint64(len(frame.Payload)))
	}

	switch frame.Type {
	case FrameSyn:
		conn := NewConnection(frame.ConnID, m)
		conn.markEstablished() // Server side is immediately established
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
		// Mark connection as established
		if connI, ok := m.connections.Load(frame.ConnID); ok {
			conn := connI.(*Connection)
			conn.markEstablished()
		}

	case FrameData:
		if connI, ok := m.connections.Load(frame.ConnID); ok {
			conn := connI.(*Connection)
			conn.deliver(frame.Payload)
			if conn.packetsDropped.Load() > 0 {
				m.packetsDropped.Store(conn.packetsDropped.Load())
			}
		}

	case FrameFin:
		if connI, ok := m.connections.Load(frame.ConnID); ok {
			conn := connI.(*Connection)
			// Use atomic to prevent double close
			if conn.closed.Swap(true) {
				return nil // Already closed
			}
			select {
			case <-conn.closeCh:
				// Already closed
			default:
				close(conn.closeCh)
			}
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
	m.framesSent.Add(1)
	if frame.Type == FrameData {
		m.bytesPayloadSent.Add(uint64(len(frame.Payload)))
	}
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

// Stats returns diagnostic statistics
func (m *Manager) Stats() (framesSent, framesRecv, bytesSent, bytesRecv, dropped uint64) {
	return m.framesSent.Load(), m.framesReceived.Load(), m.bytesPayloadSent.Load(), m.bytesPayloadRecv.Load(), m.packetsDropped.Load()
}
