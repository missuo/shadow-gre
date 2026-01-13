package tunnel

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/missuo/shadow-gre/pkg/transport"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/waiter"
)

const (
	// NIC ID for the GRE link
	nicID = 1

	// Default TCP buffer sizes
	defaultTCPSendBuffer = 512 * 1024  // 512KB
	defaultTCPRecvBuffer = 512 * 1024  // 512KB
	maxTCPSendBuffer     = 4 * 1024 * 1024 // 4MB
	maxTCPRecvBuffer     = 4 * 1024 * 1024 // 4MB
)

// TCPStackManager manages a gVisor TCP/IP stack and multiple TCP streams
type TCPStackManager struct {
	stack       *stack.Stack
	linkEP      *GRELinkEndpoint
	ipAllocator *IPAllocator
	isClient    bool

	// Stream management
	streams   map[uint32]*TCPStream
	streamsMu sync.RWMutex

	// Listener for incoming connections (server mode)
	listener *tcpip.Endpoint

	// Shutdown
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// TCPStream represents a single TCP connection over the tunnel
type TCPStream struct {
	streamID   uint32
	endpoint   tcpip.Endpoint
	wq         waiter.Queue
	localAddr  tcpip.FullAddress
	remoteAddr tcpip.FullAddress

	// For bridging with application
	appConn net.Conn

	// Shutdown
	closed bool
	mu     sync.Mutex
}

// NewTCPStackManager creates a new TCP stack manager for client mode
func NewTCPStackManager(rawTransport *transport.RawTransport, isClient bool) (*TCPStackManager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create gVisor network stack
	s := stack.New(stack.Options{
		NetworkProtocols: []stack.NetworkProtocolFactory{
			ipv4.NewProtocol,
		},
		TransportProtocols: []stack.TransportProtocolFactory{
			tcp.NewProtocol,
		},
	})

	// Create link endpoint
	linkEP := NewGRELinkEndpoint(rawTransport)

	// Create NIC with the link endpoint
	if err := s.CreateNIC(nicID, linkEP); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create NIC: %v", err)
	}

	// Configure TCP stack options
	if err := configureTCPStack(s); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to configure TCP stack: %v", err)
	}

	// Add default route
	subnet, _ := tcpip.NewSubnet(tcpip.AddrFromSlice([]byte{0, 0, 0, 0}), tcpip.MaskFromBytes([]byte{0, 0, 0, 0}))
	s.SetRouteTable([]tcpip.Route{
		{
			Destination: subnet,
			NIC:         nicID,
		},
	})

	// Set up receive handler
	linkEP.SetupReceiveHandler()

	mgr := &TCPStackManager{
		stack:       s,
		linkEP:      linkEP,
		ipAllocator: NewIPAllocator(isClient),
		isClient:    isClient,
		streams:     make(map[uint32]*TCPStream),
		ctx:         ctx,
		cancel:      cancel,
	}

	return mgr, nil
}

// NewTCPStackManagerServer creates a new TCP stack manager for server mode
func NewTCPStackManagerServer(serverTransport *transport.ServerTransport, clientIP net.IP) (*TCPStackManager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create gVisor network stack
	s := stack.New(stack.Options{
		NetworkProtocols: []stack.NetworkProtocolFactory{
			ipv4.NewProtocol,
		},
		TransportProtocols: []stack.TransportProtocolFactory{
			tcp.NewProtocol,
		},
	})

	// Create link endpoint for server
	linkEP := NewGRELinkEndpointServer(serverTransport, clientIP)

	// Create NIC with the link endpoint
	if err := s.CreateNIC(nicID, linkEP); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create NIC: %v", err)
	}

	// Configure TCP stack options
	if err := configureTCPStack(s); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to configure TCP stack: %v", err)
	}

	// Add default route
	subnet, _ := tcpip.NewSubnet(tcpip.AddrFromSlice([]byte{0, 0, 0, 0}), tcpip.MaskFromBytes([]byte{0, 0, 0, 0}))
	s.SetRouteTable([]tcpip.Route{
		{
			Destination: subnet,
			NIC:         nicID,
		},
	})

	mgr := &TCPStackManager{
		stack:       s,
		linkEP:      linkEP,
		ipAllocator: NewIPAllocator(false), // server uses 10.254.x.x
		isClient:    false,
		streams:     make(map[uint32]*TCPStream),
		ctx:         ctx,
		cancel:      cancel,
	}

	return mgr, nil
}

// configureTCPStack configures TCP stack parameters
func configureTCPStack(s *stack.Stack) error {
	// Enable SACK
	sackOpt := tcpip.TCPSACKEnabled(true)
	if err := s.SetTransportProtocolOption(tcp.ProtocolNumber, &sackOpt); err != nil {
		return fmt.Errorf("failed to enable SACK: %v", err)
	}

	// Set congestion control to BBR (or cubic as fallback)
	ccOpt := tcpip.CongestionControlOption("bbr")
	if err := s.SetTransportProtocolOption(tcp.ProtocolNumber, &ccOpt); err != nil {
		// Try cubic if BBR is not available
		ccOpt = tcpip.CongestionControlOption("cubic")
		if err := s.SetTransportProtocolOption(tcp.ProtocolNumber, &ccOpt); err != nil {
			log.Printf("Warning: failed to set congestion control: %v", err)
		}
	}

	// Configure send buffer sizes
	sendBufOpt := tcpip.TCPSendBufferSizeRangeOption{
		Min:     4096,
		Default: defaultTCPSendBuffer,
		Max:     maxTCPSendBuffer,
	}
	if err := s.SetTransportProtocolOption(tcp.ProtocolNumber, &sendBufOpt); err != nil {
		return fmt.Errorf("failed to set send buffer: %v", err)
	}

	// Configure receive buffer sizes
	recvBufOpt := tcpip.TCPReceiveBufferSizeRangeOption{
		Min:     4096,
		Default: defaultTCPRecvBuffer,
		Max:     maxTCPRecvBuffer,
	}
	if err := s.SetTransportProtocolOption(tcp.ProtocolNumber, &recvBufOpt); err != nil {
		return fmt.Errorf("failed to set recv buffer: %v", err)
	}

	// Enable moderate receive buffer auto-tuning
	moderateRecvBuf := tcpip.TCPModerateReceiveBufferOption(true)
	if err := s.SetTransportProtocolOption(tcp.ProtocolNumber, &moderateRecvBuf); err != nil {
		log.Printf("Warning: failed to enable moderate receive buffer: %v", err)
	}

	return nil
}

// CreateStream creates a new TCP stream for client mode
func (m *TCPStackManager) CreateStream(streamID uint32, appConn net.Conn) (*TCPStream, error) {
	m.streamsMu.Lock()
	defer m.streamsMu.Unlock()

	// Check if stream already exists
	if _, exists := m.streams[streamID]; exists {
		return nil, fmt.Errorf("stream %d already exists", streamID)
	}

	// Allocate virtual IPs
	localIP := m.ipAllocator.AllocateIP(streamID)
	remoteIP := m.ipAllocator.AllocateIP(streamID + 0x10000) // Use different range for remote

	// Add IP address to NIC
	protocolAddr := tcpip.ProtocolAddress{
		Protocol:          ipv4.ProtocolNumber,
		AddressWithPrefix: tcpip.AddressWithPrefix{Address: localIP, PrefixLen: 16},
	}
	if err := m.stack.AddProtocolAddress(nicID, protocolAddr, stack.AddressProperties{}); err != nil {
		return nil, fmt.Errorf("failed to add IP address: %v", err)
	}

	// Create TCP endpoint
	var wq waiter.Queue
	ep, err := m.stack.NewEndpoint(tcp.ProtocolNumber, ipv4.ProtocolNumber, &wq)
	if err != nil {
		return nil, fmt.Errorf("failed to create endpoint: %v", err)
	}

	// Bind to local address
	localAddr := tcpip.FullAddress{
		NIC:  nicID,
		Addr: localIP,
		Port: uint16(10000 + streamID%10000), // Use streamID to generate port
	}
	if err := ep.Bind(localAddr); err != nil {
		ep.Close()
		return nil, fmt.Errorf("failed to bind: %v", err)
	}

	// Connect to remote address
	remoteAddr := tcpip.FullAddress{
		NIC:  nicID,
		Addr: remoteIP,
		Port: uint16(20000 + streamID%10000),
	}

	// Wait for connection to complete
	waitEntry, notifyCh := waiter.NewChannelEntry(waiter.WritableEvents)
	wq.EventRegister(&waitEntry)
	defer wq.EventUnregister(&waitEntry)

	err = ep.Connect(remoteAddr)
	if _, ok := err.(*tcpip.ErrConnectStarted); ok {
		// Connection is in progress, wait for it
		select {
		case <-notifyCh:
			err = ep.LastError()
		case <-time.After(10 * time.Second):
			ep.Close()
			return nil, fmt.Errorf("connection timeout")
		case <-m.ctx.Done():
			ep.Close()
			return nil, fmt.Errorf("manager closed")
		}
	}

	if err != nil {
		ep.Close()
		return nil, fmt.Errorf("failed to connect: %v", err)
	}

	stream := &TCPStream{
		streamID:   streamID,
		endpoint:   ep,
		wq:         wq,
		localAddr:  localAddr,
		remoteAddr: remoteAddr,
		appConn:    appConn,
	}

	m.streams[streamID] = stream

	// Start bridging data
	m.wg.Add(1)
	go m.bridgeStream(stream)

	return stream, nil
}

// bridgeStream bridges data between app connection and gVisor endpoint
func (m *TCPStackManager) bridgeStream(stream *TCPStream) {
	defer m.wg.Done()
	defer m.CloseStream(stream.streamID)

	var wg sync.WaitGroup
	wg.Add(2)

	// App -> gVisor
	go func() {
		defer wg.Done()
		buf := make([]byte, 32*1024)
		for {
			n, err := stream.appConn.Read(buf)
			if err != nil {
				return
			}

			// Write to gVisor endpoint
			r := stream.endpoint.Write(tcpip.SlicePayload(buf[:n]), tcpip.WriteOptions{})
			if r.Err != nil {
				log.Printf("Failed to write to gVisor endpoint: %v", r.Err)
				return
			}
		}
	}()

	// gVisor -> App
	go func() {
		defer wg.Done()
		waitEntry, notifyCh := waiter.NewChannelEntry(waiter.ReadableEvents)
		stream.wq.EventRegister(&waitEntry)
		defer stream.wq.EventUnregister(&waitEntry)

		for {
			res := stream.endpoint.Read(io.Discard, tcpip.ReadOptions{})
			if res.Err != nil {
				if _, ok := res.Err.(*tcpip.ErrWouldBlock); ok {
					// Wait for data
					select {
					case <-notifyCh:
						continue
					case <-m.ctx.Done():
						return
					}
				}
				return
			}

			// Write to app connection
			if res.Count > 0 {
				data := res.Buffer.Flatten()
				if _, err := stream.appConn.Write(data); err != nil {
					return
				}
			}
		}
	}()

	wg.Wait()
}

// CloseStream closes a TCP stream
func (m *TCPStackManager) CloseStream(streamID uint32) error {
	m.streamsMu.Lock()
	stream, exists := m.streams[streamID]
	if !exists {
		m.streamsMu.Unlock()
		return fmt.Errorf("stream %d not found", streamID)
	}
	delete(m.streams, streamID)
	m.streamsMu.Unlock()

	stream.mu.Lock()
	defer stream.mu.Unlock()

	if stream.closed {
		return nil
	}
	stream.closed = true

	if stream.endpoint != nil {
		stream.endpoint.Close()
	}
	if stream.appConn != nil {
		stream.appConn.Close()
	}

	m.ipAllocator.ReleaseIP(streamID)
	m.ipAllocator.ReleaseIP(streamID + 0x10000)

	return nil
}

// Close closes the TCP stack manager
func (m *TCPStackManager) Close() error {
	m.cancel()

	// Close all streams
	m.streamsMu.Lock()
	streamIDs := make([]uint32, 0, len(m.streams))
	for id := range m.streams {
		streamIDs = append(streamIDs, id)
	}
	m.streamsMu.Unlock()

	for _, id := range streamIDs {
		m.CloseStream(id)
	}

	m.wg.Wait()

	// Close link endpoint
	m.linkEP.Close()

	// Close stack
	m.stack.Close()

	return nil
}

// GetStack returns the underlying gVisor stack (for advanced usage)
func (m *TCPStackManager) GetStack() *stack.Stack {
	return m.stack
}

// StreamCount returns the number of active streams
func (m *TCPStackManager) StreamCount() int {
	m.streamsMu.RLock()
	defer m.streamsMu.RUnlock()
	return len(m.streams)
}

// Listen starts listening for incoming TCP connections (server mode)
func (m *TCPStackManager) Listen(port uint16) error {
	// Allocate an IP for the server
	serverIP := m.ipAllocator.AllocateIP(0) // Use streamID 0 for server

	// Add IP address to NIC
	protocolAddr := tcpip.ProtocolAddress{
		Protocol:          ipv4.ProtocolNumber,
		AddressWithPrefix: tcpip.AddressWithPrefix{Address: serverIP, PrefixLen: 16},
	}
	if err := m.stack.AddProtocolAddress(nicID, protocolAddr, stack.AddressProperties{}); err != nil {
		return fmt.Errorf("failed to add server IP address: %v", err)
	}

	// Create listening TCP endpoint
	var wq waiter.Queue
	ep, err := m.stack.NewEndpoint(tcp.ProtocolNumber, ipv4.ProtocolNumber, &wq)
	if err != nil {
		return fmt.Errorf("failed to create listening endpoint: %v", err)
	}

	// Bind to the address
	listenAddr := tcpip.FullAddress{
		NIC:  nicID,
		Addr: serverIP,
		Port: port,
	}
	if err := ep.Bind(listenAddr); err != nil {
		ep.Close()
		return fmt.Errorf("failed to bind: %v", err)
	}

	// Start listening
	if err := ep.Listen(128); err != nil { // backlog of 128
		ep.Close()
		return fmt.Errorf("failed to listen: %v", err)
	}

	m.listener = &ep
	log.Printf("gVisor TCP stack listening on %s:%d", serverIP, port)

	// Start accept loop
	m.wg.Add(1)
	go m.acceptLoop(&ep, &wq)

	return nil
}

// acceptLoop accepts incoming connections
func (m *TCPStackManager) acceptLoop(listener *tcpip.Endpoint, wq *waiter.Queue) {
	defer m.wg.Done()

	waitEntry, notifyCh := waiter.NewChannelEntry(waiter.ReadableEvents)
	wq.EventRegister(&waitEntry)
	defer wq.EventUnregister(&waitEntry)

	for {
		ep, wq, err := (*listener).Accept(nil)
		if err != nil {
			if _, ok := err.(*tcpip.ErrWouldBlock); ok {
				// Wait for connection
				select {
				case <-notifyCh:
					continue
				case <-m.ctx.Done():
					return
				}
			}
			if _, ok := err.(*tcpip.ErrAborted); !ok {
				if _, ok := err.(*tcpip.ErrConnectionAborted); !ok {
					log.Printf("Accept error: %v", err)
				}
			}
			if m.ctx.Err() != nil {
				return
			}
			continue
		}

		// Got a connection, handle it
		m.wg.Add(1)
		go m.handleAcceptedConnection(ep, wq)
	}
}

// handleAcceptedConnection handles an accepted connection from gVisor
func (m *TCPStackManager) handleAcceptedConnection(ep tcpip.Endpoint, wq *waiter.Queue) {
	defer m.wg.Done()
	defer ep.Close()

	// Get the remote address to determine streamID
	remoteAddr, err := ep.GetRemoteAddress()
	if err != nil {
		log.Printf("Failed to get remote address: %v", err)
		return
	}

	// Extract streamID from remote IP
	streamID, err := m.ipAllocator.IPToStreamID(remoteAddr.Addr)
	if err != nil {
		log.Printf("Failed to extract streamID from IP: %v", err)
		return
	}

	log.Printf("Accepted gVisor connection for stream %d", streamID)

	// This connection needs to be forwarded to backend
	// The actual forwarding logic will be in the server code
	// For now, we just store the endpoint
	// TODO: Add callback mechanism for server to handle accepted connections
}
