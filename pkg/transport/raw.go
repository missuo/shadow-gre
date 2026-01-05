// Package transport implements raw socket transport for GRE packets.
package transport

import (
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"github.com/missuo/shadow-gre/pkg/gre"
)

// RawTransport handles raw socket communication for GRE packets
type RawTransport struct {
	localIP     net.IP
	remoteIP    net.IP
	conn        *net.IPConn
	key         uint32
	seq         atomic.Uint32
	onReceive   func([]byte)
	closed      atomic.Bool
	closeCh     chan struct{}
	wg          sync.WaitGroup
	recvCount   atomic.Uint64
	sendCount   atomic.Uint64
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

	packet := gre.NewPacket(t.key, payload)
	greData := packet.Marshal()

	// Send to remote IP
	_, err := t.conn.WriteToIP(greData, &net.IPAddr{IP: t.remoteIP})
	if err == nil {
		t.sendCount.Add(1)
	}
	return err
}

// SendZeroCopy sends data through the GRE tunnel with zero-copy
// buf: the buffer containing the payload
// headerOffset: where to write the GRE header (usually 0 or greHeaderSize)
// payloadSize: size of the payload after the header
func (t *RawTransport) SendZeroCopy(buf []byte, headerOffset, payloadSize int) error {
	if t.closed.Load() {
		return fmt.Errorf("transport closed")
	}

	// Write GRE header directly at the beginning of the buffer
	// GRE header format (8 bytes with Key):
	// 0-1: Flags (0x2000 = Key flag)
	// 2-3: Protocol Type (0x6558)
	// 4-7: Key
	buf[0] = 0x20 // Flags high byte (FlagKey = 0x2000)
	buf[1] = 0x00 // Flags low byte
	buf[2] = 0x65 // Protocol Type high byte
	buf[3] = 0x58 // Protocol Type low byte
	buf[4] = byte(t.key >> 24)
	buf[5] = byte(t.key >> 16)
	buf[6] = byte(t.key >> 8)
	buf[7] = byte(t.key)

	totalSize := headerOffset + payloadSize

	// Send to remote IP
	_, err := t.conn.WriteToIP(buf[:totalSize], &net.IPAddr{IP: t.remoteIP})
	if err == nil {
		t.sendCount.Add(1)
	}
	return err
}

// receiveLoop reads incoming GRE packets
func (t *RawTransport) receiveLoop() {
	defer t.wg.Done()

	buf := make([]byte, 65535)
	var totalRecv, filtered, keyMismatch, delivered uint64

	for {
		if t.closed.Load() {
			log.Printf("Transport stats: total_recv=%d, filtered=%d, key_mismatch=%d, delivered=%d, sent=%d",
				totalRecv, filtered, keyMismatch, delivered, t.sendCount.Load())
			return
		}

		n, addr, err := t.conn.ReadFromIP(buf)
		if err != nil {
			if t.closed.Load() {
				return
			}
			continue
		}

		totalRecv++

		// Filter packets from our peer
		if t.remoteIP != nil && !addr.IP.Equal(t.remoteIP) {
			filtered++
			continue
		}

		// Parse GRE packet
		packet, err := gre.UnmarshalPacket(buf[:n])
		if err != nil {
			continue
		}

		// Verify key
		if packet.Header.Key != t.key {
			keyMismatch++
			continue
		}

		// Process payload synchronously (handler is lightweight)
		if t.onReceive != nil && len(packet.Payload) > 0 {
			payload := make([]byte, len(packet.Payload))
			copy(payload, packet.Payload)
			t.onReceive(payload)
			delivered++
			t.recvCount.Add(1)
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
