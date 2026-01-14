package tunnel

import (
	"log"
	"net"
	"sync"
	"sync/atomic"

	"github.com/missuo/shadow-gre/pkg/gre"
	"github.com/missuo/shadow-gre/pkg/transport"
	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
)

// GRELinkEndpoint implements stack.LinkEndpoint for GRE transport
// It acts as a "virtual network card" that sends/receives packets through GRE
// The IP packets from gVisor are wrapped in ReliablePacket for transmission
type GRELinkEndpoint struct {
	// Network dispatcher from netstack
	dispatcher stack.NetworkDispatcher

	// Link address (MAC address equivalent)
	linkAddr tcpip.LinkAddress

	// MTU for this link (must account for GRE + IP overhead)
	mtu uint32

	// Underlying GRE transport (either RawTransport or ServerTransport)
	rawTransport    *transport.RawTransport
	serverTransport *transport.ServerTransport
	isServer        bool
	clientIP        net.IP // For server mode: which client to send to

	// Capabilities
	capabilities stack.LinkEndpointCapabilities

	// Attached flag
	attached bool

	// Sequence numbers per stream for reliable layer
	streamSeqNums sync.Map // map[uint32]*atomic.Uint32
}

// NewGRELinkEndpoint creates a link endpoint for client mode
func NewGRELinkEndpoint(transport *transport.RawTransport) *GRELinkEndpoint {
	e := &GRELinkEndpoint{
		linkAddr:     generateLinkAddr(true),
		mtu:          1400, // Conservative MTU: 1500 - GRE(8) - outer IP(20) - reliable header - safety margin
		rawTransport: transport,
		isServer:     false,
		capabilities: stack.CapabilityRXChecksumOffload | stack.CapabilityTXChecksumOffload,
	}
	return e
}

// NewGRELinkEndpointServer creates a link endpoint for server mode
func NewGRELinkEndpointServer(transport *transport.ServerTransport, clientIP net.IP) *GRELinkEndpoint {
	e := &GRELinkEndpoint{
		linkAddr:        generateLinkAddr(false),
		mtu:             1400,
		serverTransport: transport,
		clientIP:        clientIP,
		isServer:        true,
		capabilities:    stack.CapabilityRXChecksumOffload | stack.CapabilityTXChecksumOffload,
	}
	return e
}

// MTU implements stack.LinkEndpoint
func (e *GRELinkEndpoint) MTU() uint32 {
	return e.mtu
}

// SetMTU sets the MTU for this endpoint
func (e *GRELinkEndpoint) SetMTU(mtu uint32) {
	e.mtu = mtu
}

// Capabilities implements stack.LinkEndpoint
func (e *GRELinkEndpoint) Capabilities() stack.LinkEndpointCapabilities {
	return e.capabilities
}

// MaxHeaderLength implements stack.LinkEndpoint
func (e *GRELinkEndpoint) MaxHeaderLength() uint16 {
	return 0 // We don't add any link-layer header
}

// LinkAddress implements stack.LinkEndpoint
func (e *GRELinkEndpoint) LinkAddress() tcpip.LinkAddress {
	return e.linkAddr
}

// SetLinkAddress implements stack.LinkEndpoint
func (e *GRELinkEndpoint) SetLinkAddress(addr tcpip.LinkAddress) {
	e.linkAddr = addr
}

// SetOnCloseAction implements stack.LinkEndpoint
func (e *GRELinkEndpoint) SetOnCloseAction(func()) {
	// No-op: we don't need close actions for GRE
}

// Attach implements stack.LinkEndpoint
func (e *GRELinkEndpoint) Attach(dispatcher stack.NetworkDispatcher) {
	e.dispatcher = dispatcher
	e.attached = true
}

// IsAttached implements stack.LinkEndpoint
func (e *GRELinkEndpoint) IsAttached() bool {
	return e.attached
}

// Wait implements stack.LinkEndpoint
func (e *GRELinkEndpoint) Wait() {
	// No background goroutines to wait for
}

// ARPHardwareType implements stack.LinkEndpoint
func (e *GRELinkEndpoint) ARPHardwareType() header.ARPHardwareType {
	return header.ARPHardwareNone
}

// AddHeader implements stack.LinkEndpoint
func (e *GRELinkEndpoint) AddHeader(pkt *stack.PacketBuffer) {
	// No link-layer header needed
}

// ParseHeader implements stack.LinkEndpoint
func (e *GRELinkEndpoint) ParseHeader(pkt *stack.PacketBuffer) bool {
	// No link-layer header to parse
	return true
}

// WritePackets implements stack.LinkEndpoint
// This is called by gVisor when it wants to send IP packets
func (e *GRELinkEndpoint) WritePackets(pkts stack.PacketBufferList) (int, tcpip.Error) {
	n := 0
	for _, pkt := range pkts.AsSlice() {
		if err := e.writePacket(pkt); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// writePacket sends a single packet through GRE wrapped in ReliablePacket format
func (e *GRELinkEndpoint) writePacket(pkt *stack.PacketBuffer) tcpip.Error {
	// Extract the IP packet from gVisor
	payload := pkt.ToBuffer()
	ipPacket := payload.Flatten()

	// Parse IP header to extract source IP (which encodes the stream ID)
	if len(ipPacket) < header.IPv4MinimumSize {
		return &tcpip.ErrInvalidEndpointState{}
	}

	// Extract source IP address (contains stream ID in last 2 bytes)
	srcIP := header.IPv4(ipPacket).SourceAddress()
	streamID := uint32(srcIP.As4()[2])<<8 | uint32(srcIP.As4()[3])

	// Get or create sequence number counter for this stream
	seqNumI, _ := e.streamSeqNums.LoadOrStore(streamID, &atomic.Uint32{})
	seqNum := seqNumI.(*atomic.Uint32)
	seq := seqNum.Add(1)

	// Wrap in ReliablePacket format
	// The TCP layer handles reliability, so we just wrap for stream multiplexing
	reliablePkt := &ReliablePacket{
		StreamID: streamID,
		Flags:    FlagData, // Just data, no ACK needed since TCP handles reliability
		Seq:      seq,
		Data:     ipPacket,
	}

	reliableData := reliablePkt.Marshal()

	// Send through GRE transport
	var err error
	if e.isServer {
		err = e.serverTransport.Send(e.clientIP, reliableData)
	} else {
		err = e.rawTransport.Send(reliableData)
	}

	if err != nil {
		log.Printf("Failed to send GRE packet for stream %d: %v", streamID, err)
		return &tcpip.ErrAborted{}
	}

	return nil
}

// DeliverReliablePacket is called when receiving a ReliablePacket
// It unwraps the IP packet and injects it into gVisor's network stack
func (e *GRELinkEndpoint) DeliverReliablePacket(reliablePkt *ReliablePacket) {
	if !e.attached || e.dispatcher == nil {
		return
	}

	// Extract IP packet from reliable packet
	ipPacket := reliablePkt.Data
	if len(ipPacket) < 1 {
		return
	}

	// Parse IP version
	var protocol tcpip.NetworkProtocolNumber
	ipVersion := header.IPVersion(ipPacket)

	switch ipVersion {
	case header.IPv4Version:
		protocol = header.IPv4ProtocolNumber
	case header.IPv6Version:
		protocol = header.IPv6ProtocolNumber
	default:
		log.Printf("Unknown IP version: %d (streamID: %d, seq: %d)", ipVersion, reliablePkt.StreamID, reliablePkt.Seq)
		return
	}

	// Create packet buffer from received data
	buf := buffer.MakeWithData(reliablePkt.Data)
	pkt := stack.NewPacketBuffer(stack.PacketBufferOptions{
		Payload: buf,
	})
	defer pkt.DecRef()

	// Deliver to network layer
	e.dispatcher.DeliverNetworkPacket(protocol, pkt)
}

// SetupReceiveHandler sets up the GRE receive handler to deliver packets to gVisor
func (e *GRELinkEndpoint) SetupReceiveHandler() {
	if e.isServer {
		// Server mode: receive handler will be set per-client by TCPStackManager
		// The server needs to handle packets from multiple clients
		// This is handled in the server's packet receive loop
	} else {
		// Client mode: set receive handler on RawTransport
		e.rawTransport.SetReceiveHandler(func(payload []byte) {
			// Unwrap GRE packet
			grePacket, err := gre.UnmarshalPacket(payload)
			if err != nil {
				log.Printf("Failed to unmarshal GRE packet: %v", err)
				return
			}

			// Unwrap ReliablePacket
			reliablePkt, err := UnmarshalReliable(grePacket.Payload)
			if err != nil {
				log.Printf("Failed to unmarshal ReliablePacket: %v", err)
				return
			}

			// Deliver the IP packet to gVisor
			e.DeliverReliablePacket(reliablePkt)
		})
	}
}

// generateLinkAddr generates a pseudo MAC address
func generateLinkAddr(isClient bool) tcpip.LinkAddress {
	// Use a locally administered address (bit 1 of first byte set)
	if isClient {
		return tcpip.LinkAddress([]byte{0x02, 0x00, 0x00, 0x00, 0x00, 0x01})
	}
	return tcpip.LinkAddress([]byte{0x02, 0x00, 0x00, 0x00, 0x00, 0x02})
}

// Close closes the link endpoint
func (e *GRELinkEndpoint) Close() {
	e.attached = false
	e.dispatcher = nil
}

// WriteRawPacket implements stack.LinkEndpoint (optional method)
func (e *GRELinkEndpoint) WriteRawPacket(pkt *stack.PacketBuffer) tcpip.Error {
	return e.writePacket(pkt)
}
