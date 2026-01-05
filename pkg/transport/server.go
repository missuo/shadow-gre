package transport

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/missuo/shadow-gre/pkg/gre"
)

// ServerTransport handles raw socket communication for GRE server mode
// It can accept connections from any client IP
type ServerTransport struct {
	localIP   net.IP
	conn      *net.IPConn
	key       uint32
	seq       atomic.Uint32
	clients   sync.Map // map[string]*clientState
	onReceive func(clientIP net.IP, payload []byte)
	closed    atomic.Bool
	closeCh   chan struct{}
	wg        sync.WaitGroup
}

type clientState struct {
	ip  net.IP
	seq atomic.Uint32
}

// NewServerTransport creates a new raw socket transport for server mode
func NewServerTransport(localIP net.IP, key uint32) (*ServerTransport, error) {
	conn, err := net.ListenIP("ip4:gre", &net.IPAddr{IP: localIP})
	if err != nil {
		return nil, fmt.Errorf("failed to create raw socket: %w (hint: run with sudo/root)", err)
	}

	return &ServerTransport{
		localIP: localIP,
		conn:    conn,
		key:     key,
		closeCh: make(chan struct{}),
	}, nil
}

// SetReceiveHandler sets the handler for received packets
func (t *ServerTransport) SetReceiveHandler(handler func(clientIP net.IP, payload []byte)) {
	t.onReceive = handler
}

// Start starts the receive loop
func (t *ServerTransport) Start() {
	t.wg.Add(1)
	go t.receiveLoop()
}

// Send sends data to a specific client
func (t *ServerTransport) Send(clientIP net.IP, payload []byte) error {
	if t.closed.Load() {
		return fmt.Errorf("transport closed")
	}

	// Get or create client state
	clientKey := clientIP.String()
	stateI, _ := t.clients.LoadOrStore(clientKey, &clientState{ip: clientIP})
	state := stateI.(*clientState)

	seq := state.seq.Add(1)
	packet := gre.NewPacket(t.key, seq, payload)
	greData := packet.Marshal()

	_, err := t.conn.WriteToIP(greData, &net.IPAddr{IP: clientIP})
	return err
}

// receiveLoop reads incoming GRE packets
func (t *ServerTransport) receiveLoop() {
	defer t.wg.Done()

	buf := make([]byte, 65535)
	for {
		if t.closed.Load() {
			return
		}

		n, addr, err := t.conn.ReadFromIP(buf)
		if err != nil {
			if t.closed.Load() {
				return
			}
			continue
		}

		// Parse GRE packet
		packet, err := gre.UnmarshalPacket(buf[:n])
		if err != nil {
			continue
		}

		// Verify key
		if packet.Header.Key != t.key {
			continue
		}

		// Register client
		clientKey := addr.IP.String()
		t.clients.LoadOrStore(clientKey, &clientState{ip: addr.IP})

		// Process payload synchronously (handler is lightweight)
		if t.onReceive != nil && len(packet.Payload) > 0 {
			payload := make([]byte, len(packet.Payload))
			copy(payload, packet.Payload)
			t.onReceive(addr.IP, payload)
		}
	}
}

// Close closes the transport
func (t *ServerTransport) Close() error {
	if t.closed.Swap(true) {
		return nil
	}

	close(t.closeCh)
	if t.conn != nil {
		t.conn.Close()
	}
	t.wg.Wait()
	return nil
}
