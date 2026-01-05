// Package shadowgre implements the main client and server logic
package shadowgre

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"github.com/missuo/shadow-gre/pkg/transport"
	"github.com/missuo/shadow-gre/pkg/tunnel"
)

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
}

// NewClient creates a new shadow-gre client
func NewClient(listenAddr string, localIP, serverIP net.IP, key uint32) *Client {
	return &Client{
		listenAddr: listenAddr,
		localIP:    localIP,
		serverIP:   serverIP,
		key:        key,
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

	// Bidirectional copy
	var wg sync.WaitGroup
	wg.Add(2)

	// TCP -> Tunnel
	go func() {
		defer wg.Done()
		io.Copy(tunnelConn, tcpConn)
		tunnelConn.Close()
	}()

	// Tunnel -> TCP
	go func() {
		defer wg.Done()
		io.Copy(tcpConn, tunnelConn)
		tcpConn.Close()
	}()

	wg.Wait()
	log.Printf("Connection closed, tunnel ID: %d", tunnelConn.ID)
}

// Close closes the client
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil
	}

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
