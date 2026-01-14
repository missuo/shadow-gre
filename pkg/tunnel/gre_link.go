package tunnel

import (
	"log"
	"net"

	"github.com/missuo/shadow-gre/pkg/gre"
	"github.com/missuo/shadow-gre/pkg/transport"
	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
)

// GRELinkEndpoint implements stack.LinkEndpoint for GRE transport
// It acts as a "virtual network card" that sends/receives packets through GRE
type GRELinkEndpoint struct {
	// Network dispatcher from netstack
	dispatcher stack.NetworkDispatcher

	// Link address (MAC address equivalent)
	linkAddr tcpip.LinkAddress

	// MTU for this link (must account for GRE + IP overhead)
	mtu uint32

	// Underlying GRE transport (either RawTransport or ServerTransport)
	rawTransport   *transport.RawTransport
	serverTransport *transport.ServerTransport
	isServer       bool
	clientIP       net.IP // For server mode: which client to send to

	// Capabilities
	capabilities stack.LinkEndpointCapabilities

	// Attached flag
	attached bool
}

// NewGRELinkEndpoint creates a link endpoint for client mode
func NewGRELinkEndpoint(transport *transport.RawTransport) *GRELinkEndpoint {
	return &GRELinkEndpoint{
		linkAddr:     generateLinkAddr(true),
		mtu:          1400, // Conservative MTU: 1500 - GRE(8) - outer IP(20) - safety margin
		rawTransport: transport,
		isServer:     false,
		capabilities: stack.CapabilityRXChecksumOffload | stack.CapabilityTXChecksumOffload,
	}
}

// NewGRELinkEndpointServer creates a link endpoint for server mode
func NewGRELinkEndpointServer(transport *transport.ServerTransport, clientIP net.IP) *GRELinkEndpoint {
	return &GRELinkEndpoint{
		linkAddr:        generateLinkAddr(false),
		mtu:             1400,
		serverTransport: transport,
		clientIP:        clientIP,
		isServer:        true,
		capabilities:    stack.CapabilityRXChecksumOffload | stack.CapabilityTXChecksumOffload,
	}
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

// writePacket sends a single packet through GRE
func (e *GRELinkEndpoint) writePacket(pkt *stack.PacketBuffer) tcpip.Error {
	// Extract the IP packet from gVisor
	payload := pkt.ToBuffer()
	ipPacket := payload.Flatten()

	// Send through GRE transport
	var err error
	if e.isServer {
		err = e.serverTransport.Send(e.clientIP, ipPacket)
	} else {
		err = e.rawTransport.Send(ipPacket)
	}

	if err != nil {
		log.Printf("Failed to send GRE packet: %v", err)
		return &tcpip.ErrAborted{}
	}

	return nil
}

// DeliverNetworkPacket is called when receiving a packet from GRE
// This injects the packet into gVisor's network stack
func (e *GRELinkEndpoint) DeliverNetworkPacket(payload []byte) {
	if !e.attached || e.dispatcher == nil {
		return
	}

	// Parse IP version
	if len(payload) < 1 {
		return
	}

	var protocol tcpip.NetworkProtocolNumber
	ipVersion := header.IPVersion(payload)

	switch ipVersion {
	case header.IPv4Version:
		protocol = header.IPv4ProtocolNumber
	case header.IPv6Version:
		protocol = header.IPv6ProtocolNumber
	default:
		log.Printf("Unknown IP version: %d", ipVersion)
		return
	}

	// Create packet buffer from received data
	buf := buffer.MakeWithData(payload)
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
		// Server mode: receive handler is set per-client in ServerTransport
		// We'll handle this in TCPStackManager
	} else {
		// Client mode: set receive handler on RawTransport
		e.rawTransport.SetReceiveHandler(func(payload []byte) {
			// Unwrap GRE packet
			packet, err := gre.UnmarshalPacket(payload)
			if err != nil {
				return
			}
			// Deliver the inner IP packet to gVisor
			e.DeliverNetworkPacket(packet.Payload)
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
