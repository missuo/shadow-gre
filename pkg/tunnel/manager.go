package tunnel

import (
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// Connection represents a virtual connection over the tunnel
type Connection struct {
	ID       uint32
	manager  *Manager
	readCh   chan []byte
	closed   atomic.Bool
	closeCh  chan struct{}
	seq      atomic.Uint32
	mu       sync.Mutex
}

// NewConnection creates a new virtual connection
func NewConnection(id uint32, manager *Manager) *Connection {
	return &Connection{
		ID:      id,
		manager: manager,
		readCh:  make(chan []byte, 256),
		closeCh: make(chan struct{}),
	}
}

// Read reads data from the connection
func (c *Connection) Read(p []byte) (n int, err error) {
	if c.closed.Load() {
		return 0, io.EOF
	}

	select {
	case data := <-c.readCh:
		if data == nil {
			return 0, io.EOF
		}
		n = copy(p, data)
		return n, nil
	case <-c.closeCh:
		return 0, io.EOF
	}
}

// Write writes data to the connection
func (c *Connection) Write(p []byte) (n int, err error) {
	if c.closed.Load() {
		return 0, io.ErrClosedPipe
	}

	seq := c.seq.Add(1)
	frame := NewDataFrame(c.ID, seq, p)
	return len(p), c.manager.sendFrame(frame)
}

// Close closes the connection
func (c *Connection) Close() error {
	if c.closed.Swap(true) {
		return nil
	}

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

	select {
	case c.readCh <- data:
	case <-c.closeCh:
	default:
		// Buffer full, drop packet
	}
}

// Manager manages virtual connections over the tunnel
type Manager struct {
	sendFunc    func([]byte) error
	connections sync.Map // map[uint32]*Connection
	connIDSeq   atomic.Uint32
	acceptCh    chan *Connection
	onConnect   func(conn *Connection) // Server-side callback for new connections
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

	// Wait for SYN-ACK with timeout
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()

	// For simplicity, we consider the connection established after sending SYN
	// In a production system, you'd wait for SYN-ACK
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
		// New incoming connection
		conn := NewConnection(frame.ConnID, m)
		m.connections.Store(frame.ConnID, conn)

		// Send SYN-ACK
		synAckFrame := NewSynAckFrame(frame.ConnID)
		if err := m.sendFrame(synAckFrame); err != nil {
			return err
		}

		// Notify accept channel or callback
		if m.onConnect != nil {
			go m.onConnect(conn)
		} else {
			select {
			case m.acceptCh <- conn:
			default:
				// Accept queue full
			}
		}

	case FrameSynAck:
		// Connection established acknowledgment
		// Connection is already stored, nothing more to do

	case FrameData:
		// Data frame
		if connI, ok := m.connections.Load(frame.ConnID); ok {
			conn := connI.(*Connection)
			conn.deliver(frame.Payload)
		}

	case FrameFin:
		// Connection close request
		if connI, ok := m.connections.Load(frame.ConnID); ok {
			conn := connI.(*Connection)
			conn.closed.Store(true)
			close(conn.closeCh)
			m.connections.Delete(frame.ConnID)

			// Send FIN-ACK
			finAckFrame := NewFinAckFrame(frame.ConnID)
			m.sendFrame(finAckFrame)
		}

	case FrameFinAck:
		// Connection close acknowledgment
		m.connections.Delete(frame.ConnID)

	case FramePing:
		// Respond with pong
		pongFrame := NewPongFrame()
		m.sendFrame(pongFrame)

	case FramePong:
		// Keepalive response received
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
