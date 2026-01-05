// Package transport implements raw socket transport for GRE packets.
package transport

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/missuo/shadow-gre/pkg/gre"
)

// RawTransport handles raw socket communication for GRE packets
type RawTransport struct {
	localIP   net.IP
	remoteIP  net.IP
	conn      *net.IPConn
	key       uint32
	seq       atomic.Uint32
	onReceive func([]byte)
	closed    atomic.Bool
	closeCh   chan struct{}
	wg        sync.WaitGroup
}

// NewRawTransport creates a new raw socket transport
func NewRawTransport(localIP, remoteIP net.IP, key uint32) (*RawTransport, error) {
	// Create raw socket for GRE protocol
	conn, err := net.ListenIP("ip4:gre", &net.IPAddr{IP: localIP})
	if err != nil {
		return nil, fmt.Errorf("failed to create raw socket: %w (hint: run with sudo/root)", err)
	}

	return &RawTransport{
		localIP:  localIP,
		remoteIP: remoteIP,
		conn:     conn,
		key:      key,
		closeCh:  make(chan struct{}),
	}, nil
}

// SetReceiveHandler sets the handler for received packets
func (t *RawTransport) SetReceiveHandler(handler func([]byte)) {
	t.onReceive = handler
}

// Start starts the receive loop
func (t *RawTransport) Start() {
	t.wg.Add(1)
	go t.receiveLoop()
}

// Send sends data through the GRE tunnel
func (t *RawTransport) Send(payload []byte) error {
	if t.closed.Load() {
		return fmt.Errorf("transport closed")
	}

	seq := t.seq.Add(1)
	packet := gre.NewPacket(t.key, seq, payload)
	greData := packet.Marshal()

	// Send to remote IP
	_, err := t.conn.WriteToIP(greData, &net.IPAddr{IP: t.remoteIP})
	return err
}

// receiveLoop reads incoming GRE packets
func (t *RawTransport) receiveLoop() {
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

		// Filter packets from our peer
		if t.remoteIP != nil && !addr.IP.Equal(t.remoteIP) {
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

		// Deliver payload to handler
		if t.onReceive != nil && len(packet.Payload) > 0 {
			// Make a copy of the payload
			payload := make([]byte, len(packet.Payload))
			copy(payload, packet.Payload)
			t.onReceive(payload)
		}
	}
}

// LocalIP returns the local IP address
func (t *RawTransport) LocalIP() net.IP {
	return t.localIP
}

// RemoteIP returns the remote IP address
func (t *RawTransport) RemoteIP() net.IP {
	return t.remoteIP
}

// Close closes the transport
func (t *RawTransport) Close() error {
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
