// Package shadowgre implements the main client and server logic
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

const copyBufferSize = 64 * 1024 // 64KB buffer for io.Copy

// Client represents a shadow-gre client
type Client struct {
	listenAddr string
	localIP    net.IP
	serverIP   net.IP
	key        uint32
	transport  *transport.RawTransport
	manager    *tunnel.Manager
	listener   net.Listener
	closed     atomic.Bool
	wg         sync.WaitGroup
	stopStats  chan struct{}
}

// NewClient creates a new shadow-gre client
func NewClient(listenAddr string, localIP, serverIP net.IP, key uint32) *Client {
	return &Client{
		listenAddr: listenAddr,
		localIP:    localIP,
		serverIP:   serverIP,
		key:        key,
		stopStats:  make(chan struct{}),
	}
}

// Start starts the client
func (c *Client) Start() error {
	// Create transport
	trans, err := transport.NewRawTransport(c.localIP, c.serverIP, c.key)
	if err != nil {
		return fmt.Errorf("failed to create transport: %w", err)
	}
	c.transport = trans

	// Create tunnel manager
	c.manager = tunnel.NewManager(func(data []byte) error {
		return c.transport.Send(data)
	})

	// Set receive handler
	c.transport.SetReceiveHandler(func(data []byte) {
		if err := c.manager.HandleFrame(data); err != nil {
			log.Printf("Failed to handle frame: %v", err)
		}
	})

	// Start transport
	c.transport.Start()

	// Listen for TCP connections
	listener, err := net.Listen("tcp", c.listenAddr)
	if err != nil {
		c.transport.Close()
		return fmt.Errorf("failed to listen on %s: %w", c.listenAddr, err)
	}
	c.listener = listener

	log.Printf("Client listening on %s, forwarding via GRE to %s", c.listenAddr, c.serverIP)

	c.wg.Add(1)
	go c.acceptLoop()

	// Start stats logging
	go c.statsLoop()

	return nil
}

// acceptLoop accepts incoming TCP connections
func (c *Client) acceptLoop() {
	defer c.wg.Done()

	for {
		if c.closed.Load() {
			return
		}

		conn, err := c.listener.Accept()
		if err != nil {
			if c.closed.Load() {
				return
			}
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		c.wg.Add(1)
		go c.handleConnection(conn)
	}
}

// handleConnection handles a single TCP connection
func (c *Client) handleConnection(tcpConn net.Conn) {
	defer c.wg.Done()
	defer tcpConn.Close()

	// Create tunnel connection
	tunnelConn, err := c.manager.Dial()
	if err != nil {
		log.Printf("Failed to create tunnel connection: %v", err)
		return
	}
	defer tunnelConn.Close()

	log.Printf("New connection from %s, tunnel ID: %d", tcpConn.RemoteAddr(), tunnelConn.ID)

	// Bidirectional copy with larger buffer
	var wg sync.WaitGroup
	wg.Add(2)

	var bytesToTunnel, bytesFromTunnel int64

	// TCP -> Tunnel
	go func() {
		defer wg.Done()
		buf := make([]byte, copyBufferSize)
		n, _ := io.CopyBuffer(tunnelConn, tcpConn, buf)
		bytesToTunnel = n
		tunnelConn.Close()
	}()

	// Tunnel -> TCP
	go func() {
		defer wg.Done()
		buf := make([]byte, copyBufferSize)
		n, _ := io.CopyBuffer(tcpConn, tunnelConn, buf)
		bytesFromTunnel = n
		tcpConn.Close()
	}()

	wg.Wait()
	log.Printf("Connection closed, tunnel ID: %d (to_tunnel: %d bytes, from_tunnel: %d bytes)", tunnelConn.ID, bytesToTunnel, bytesFromTunnel)
}

// statsLoop periodically logs statistics
func (c *Client) statsLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fSent, fRecv, bSent, bRecv, dropped := c.manager.Stats()
			if fSent > 0 || fRecv > 0 {
				log.Printf("[STATS] frames_sent=%d frames_recv=%d payload_sent=%d payload_recv=%d dropped=%d",
					fSent, fRecv, bSent, bRecv, dropped)
			}
		case <-c.stopStats:
			return
		}
	}
}

// Close closes the client
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil
	}

	close(c.stopStats)

	if c.listener != nil {
		c.listener.Close()
	}
	if c.manager != nil {
		c.manager.Close()
	}
	if c.transport != nil {
		c.transport.Close()
	}
	c.wg.Wait()
	return nil
}

// Wait waits for the client to finish
func (c *Client) Wait() {
	c.wg.Wait()
}
