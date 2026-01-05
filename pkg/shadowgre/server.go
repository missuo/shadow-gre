package shadowgre

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/missuo/shadow-gre/pkg/transport"
	"github.com/missuo/shadow-gre/pkg/tunnel"
)

// Server represents a shadow-gre server
type Server struct {
	localIP     net.IP
	backendAddr string
	key         uint32
	transport   *transport.ServerTransport
	managers    sync.Map // map[string]*clientManager
	closed      atomic.Bool
	wg          sync.WaitGroup
	stopStats   chan struct{}
}

// clientManager manages connections from a single client IP
type clientManager struct {
	clientIP    net.IP
	manager     *tunnel.Manager
	server      *Server
}

// NewServer creates a new shadow-gre server
func NewServer(localIP net.IP, backendAddr string, key uint32) *Server {
	return &Server{
		localIP:     localIP,
		backendAddr: backendAddr,
		key:         key,
		stopStats:   make(chan struct{}),
	}
}

// Start starts the server
func (s *Server) Start() error {
	// Create transport
	trans, err := transport.NewServerTransport(s.localIP, s.key)
	if err != nil {
		return fmt.Errorf("failed to create transport: %w", err)
	}
	s.transport = trans

	// Set receive handler
	s.transport.SetReceiveHandler(func(clientIP net.IP, data []byte) {
		s.handlePacket(clientIP, data)
	})

	// Start transport
	s.transport.Start()

	log.Printf("Server listening on %s (GRE protocol), forwarding to %s", s.localIP, s.backendAddr)

	// Start stats logging
	go s.statsLoop()

	return nil
}

// handlePacket handles a packet from a client
func (s *Server) handlePacket(clientIP net.IP, data []byte) {
	clientKey := clientIP.String()

	// Get or create client manager
	managerI, loaded := s.managers.LoadOrStore(clientKey, s.newClientManager(clientIP))
	cm := managerI.(*clientManager)

	if !loaded {
		log.Printf("New client connected: %s", clientIP)
	}

	// Handle frame
	if err := cm.manager.HandleFrame(data); err != nil {
		log.Printf("Failed to handle frame from %s: %v", clientIP, err)
	}
}

// newClientManager creates a new client manager
func (s *Server) newClientManager(clientIP net.IP) *clientManager {
	cm := &clientManager{
		clientIP: clientIP,
		server:   s,
	}

	// Create tunnel manager with send function
	cm.manager = tunnel.NewManager(func(data []byte) error {
		return s.transport.Send(clientIP, data)
	})

	// Set callback for new connections
	cm.manager.SetOnConnect(func(conn *tunnel.Connection) {
		s.wg.Add(1)
		go s.handleConnection(conn)
	})

	return cm
}

// handleConnection handles a tunnel connection by forwarding to backend
func (s *Server) handleConnection(tunnelConn *tunnel.Connection) {
	defer s.wg.Done()
	defer tunnelConn.Close()

	// Connect to backend
	backendConn, err := net.Dial("tcp", s.backendAddr)
	if err != nil {
		log.Printf("Failed to connect to backend %s: %v", s.backendAddr, err)
		return
	}
	defer backendConn.Close()

	log.Printf("Tunnel connection %d forwarding to %s", tunnelConn.ID, s.backendAddr)

	// Bidirectional copy with larger buffer
	var wg sync.WaitGroup
	wg.Add(2)

	var bytesFromBackend, bytesToBackend int64

	// Backend -> Tunnel
	go func() {
		defer wg.Done()
		buf := make([]byte, copyBufferSize)
		n, _ := io.CopyBuffer(tunnelConn, backendConn, buf)
		bytesFromBackend = n
		tunnelConn.Close()
	}()

	// Tunnel -> Backend
	go func() {
		defer wg.Done()
		buf := make([]byte, copyBufferSize)
		n, _ := io.CopyBuffer(backendConn, tunnelConn, buf)
		bytesToBackend = n
		backendConn.Close()
	}()

	wg.Wait()
	log.Printf("Tunnel connection %d closed (sent: %d bytes, recv: %d bytes)", tunnelConn.ID, bytesToBackend, bytesFromBackend)
}

// statsLoop periodically logs statistics
func (s *Server) statsLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.managers.Range(func(key, value interface{}) bool {
				cm := value.(*clientManager)
				clientIP := key.(string)
				fSent, fRecv, bSent, bRecv, dropped := cm.manager.Stats()
				if fSent > 0 || fRecv > 0 {
					log.Printf("[STATS] Client %s: frames_sent=%d frames_recv=%d payload_sent=%d payload_recv=%d dropped=%d",
						clientIP, fSent, fRecv, bSent, bRecv, dropped)
				}
				return true
			})
		case <-s.stopStats:
			return
		}
	}
}

// Close closes the server
func (s *Server) Close() error {
	if s.closed.Swap(true) {
		return nil
	}

	close(s.stopStats)

	// Close all client managers
	s.managers.Range(func(key, value interface{}) bool {
		cm := value.(*clientManager)
		cm.manager.Close()
		return true
	})

	if s.transport != nil {
		s.transport.Close()
	}
	s.wg.Wait()
	return nil
}

// Wait waits for the server to finish
func (s *Server) Wait() {
	s.wg.Wait()
}
