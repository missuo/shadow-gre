package shadowgre

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/missuo/shadow-gre/pkg/transport"
	"github.com/missuo/shadow-gre/pkg/tunnel"
	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/waiter"
)

// ServerGVisor represents a shadow-gre server using gVisor TCP stack
type ServerGVisor struct {
	localIP     net.IP
	backendAddr string
	key         uint32
	transport   *transport.ServerTransport
	closed      atomic.Bool
	wg          sync.WaitGroup

	// Per-client TCP stack managers
	clients   sync.Map // map[string]*clientTCPState
}

// clientTCPState manages the gVisor TCP stack for a single client
type clientTCPState struct {
	clientIP    net.IP
	tcpStackMgr *tunnel.TCPStackManager
	server      *ServerGVisor
}

// NewServerGVisor creates a new shadow-gre server with gVisor
func NewServerGVisor(localIP net.IP, backendAddr string, key uint32) *ServerGVisor {
	return &ServerGVisor{
		localIP:     localIP,
		backendAddr: backendAddr,
		key:         key,
	}
}

// Start starts the server
func (s *ServerGVisor) Start() error {
	// Create transport
	trans, err := transport.NewServerTransport(s.localIP, s.key)
	if err != nil {
		return fmt.Errorf("failed to create transport: %w", err)
	}
	s.transport = trans

	// Set receive handler to process incoming GRE packets
	s.transport.SetReceiveHandler(s.handleGREPacket)

	// Start transport
	s.transport.Start()

	log.Printf("Server listening on %s (GRE+gVisor TCP with SACK+BBR), forwarding to %s",
		s.localIP, s.backendAddr)

	return nil
}

// handleGREPacket handles incoming GRE packets from clients
// Note: payload is already unwrapped from GRE by ServerTransport
func (s *ServerGVisor) handleGREPacket(clientIP net.IP, payload []byte) {
	if s.closed.Load() {
		return
	}

	// Unwrap ReliablePacket (payload is already GRE-unwrapped)
	reliablePkt, err := tunnel.UnmarshalReliable(payload)
	if err != nil {
		log.Printf("Failed to unmarshal ReliablePacket from %s: %v", clientIP, err)
		return
	}

	// Debug: log packet info
	if len(reliablePkt.Data) > 0 {
		log.Printf("GRE packet from %s: streamID=%d, flags=0x%02x, seq=%d, dataLen=%d",
			clientIP, reliablePkt.StreamID, reliablePkt.Flags, reliablePkt.Seq, len(reliablePkt.Data))
	}

	// Get or create client TCP state
	clientState := s.getOrCreateClient(clientIP)
	if clientState == nil {
		return
	}

	// Deliver the IP packet to gVisor TCP stack
	clientState.tcpStackMgr.DeliverPacket(reliablePkt)
}

// getOrCreateClient gets or creates TCP stack for a client
func (s *ServerGVisor) getOrCreateClient(clientIP net.IP) *clientTCPState {
	clientKey := clientIP.String()
	csI, loaded := s.clients.Load(clientKey)
	if loaded {
		return csI.(*clientTCPState)
	}

	// Create TCP stack manager for this client
	tcpStackMgr, err := tunnel.NewTCPStackManagerServer(s.transport, clientIP)
	if err != nil {
		log.Printf("Failed to create TCP stack for client %s: %v", clientIP, err)
		return nil
	}

	cs := &clientTCPState{
		clientIP:    clientIP,
		tcpStackMgr: tcpStackMgr,
		server:      s,
	}

	// Set up server-side stream handler
	// When a new TCP connection is established from the client,
	// we need to accept it and forward to backend
	s.wg.Add(1)
	go s.acceptIncomingStreams(cs)

	actual, loaded := s.clients.LoadOrStore(clientKey, cs)
	if loaded {
		// Another goroutine created it
		tcpStackMgr.Close()
		return actual.(*clientTCPState)
	}

	log.Printf("Created gVisor TCP stack for client %s", clientIP)
	return cs
}

// acceptIncomingStreams sets up the accept callback and starts listening
func (s *ServerGVisor) acceptIncomingStreams(cs *clientTCPState) {
	defer s.wg.Done()

	// Set up accept callback
	cs.tcpStackMgr.SetAcceptCallback(func(streamID uint32, ep tcpip.Endpoint, wq *waiter.Queue) {
		s.wg.Add(1)
		go s.handleAcceptedStream(streamID, ep, wq, cs)
	})

	// Start listening on a fixed port (e.g., 80)
	// The actual port doesn't matter since we're using virtual IPs
	if err := cs.tcpStackMgr.Listen(80); err != nil {
		log.Printf("Failed to start listening for client %s: %v", cs.clientIP, err)
	}
}

// handleAcceptedStream handles an accepted TCP connection
// It connects to the backend and bridges the connection
func (s *ServerGVisor) handleAcceptedStream(streamID uint32, ep tcpip.Endpoint, wq *waiter.Queue, cs *clientTCPState) {
	defer s.wg.Done()
	defer ep.Close()

	log.Printf("Stream %d from %s: connecting to backend %s", streamID, cs.clientIP, s.backendAddr)

	// Connect to backend
	backendConn, err := net.DialTimeout("tcp", s.backendAddr, 10*time.Second)
	if err != nil {
		log.Printf("Stream %d: failed to connect to backend: %v", streamID, err)
		return
	}
	defer backendConn.Close()

	log.Printf("Stream %d: connected to backend", streamID)

	// Bridge gVisor endpoint with backend connection
	s.bridgeConnections(streamID, ep, wq, backendConn)

	log.Printf("Stream %d: closed", streamID)
}

// bridgeConnections bridges data between gVisor TCP endpoint and backend connection
func (s *ServerGVisor) bridgeConnections(streamID uint32, ep tcpip.Endpoint, wq *waiter.Queue, backendConn net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)

	// gVisor -> Backend
	go func() {
		defer wg.Done()
		buf := make([]byte, 32*1024)

		waitEntry, notifyCh := waiter.NewChannelEntry(waiter.ReadableEvents)
		wq.EventRegister(&waitEntry)
		defer wq.EventUnregister(&waitEntry)

		for {
			view := buffer.NewViewWithData(buf)
			res, err := ep.Read(view, tcpip.ReadOptions{})
			if err != nil {
				if _, ok := err.(*tcpip.ErrWouldBlock); ok {
					<-notifyCh
					continue
				}
				if _, ok := err.(*tcpip.ErrClosedForReceive); !ok {
					log.Printf("Stream %d: gVisor read error: %v", streamID, err)
				}
				break
			}

			if res.Count == 0 {
				continue
			}

			data := buf[:res.Count]
			log.Printf("Stream %d: read %d bytes from gVisor, forwarding to backend", streamID, res.Count)
			if _, err := backendConn.Write(data); err != nil {
				log.Printf("Stream %d: backend write error: %v", streamID, err)
				break
			}
		}
		backendConn.(*net.TCPConn).CloseWrite()
	}()

	// Backend -> gVisor
	go func() {
		defer wg.Done()
		buf := make([]byte, 32*1024)

		for {
			n, err := backendConn.Read(buf)
			if err != nil {
				if err != io.EOF {
					log.Printf("Stream %d: backend read error: %v", streamID, err)
				}
				break
			}

			reader := bytes.NewReader(buf[:n])
			_, writeErr := ep.Write(reader, tcpip.WriteOptions{})
			if writeErr != nil {
				log.Printf("Stream %d: gVisor write error: %v", streamID, writeErr)
				break
			}
		}
		ep.Shutdown(tcpip.ShutdownWrite)
	}()

	wg.Wait()
}

// Close closes the server
func (s *ServerGVisor) Close() error {
	if s.closed.Swap(true) {
		return nil
	}

	log.Printf("Closing server...")

	// Close all client TCP stacks
	s.clients.Range(func(key, value interface{}) bool {
		cs := value.(*clientTCPState)
		if cs.tcpStackMgr != nil {
			cs.tcpStackMgr.Close()
		}
		return true
	})

	if s.transport != nil {
		s.transport.Close()
	}

	s.wg.Wait()
	log.Printf("Server closed")
	return nil
}

// Wait waits for the server to finish
func (s *ServerGVisor) Wait() {
	s.wg.Wait()
}
