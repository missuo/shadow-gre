// Package shadowgre implements the main client and server logic
package shadowgre

import (
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"github.com/missuo/shadow-gre/pkg/transport"
	"github.com/missuo/shadow-gre/pkg/tunnel"
)

// Client represents a shadow-gre client using gVisor TCP stack
type Client struct {
	listenAddr string
	localIP    net.IP
	serverIP   net.IP
	key        uint32
	transport  *transport.RawTransport
	listener   net.Listener
	closed     atomic.Bool
	wg         sync.WaitGroup

	// Stream management with gVisor TCP stack
	nextStreamID atomic.Uint32
	tcpStackMgr  *tunnel.TCPStackManager
}

// NewClient creates a new shadow-gre client
func NewClient(listenAddr string, localIP, serverIP net.IP, key uint32) *Client {
	c := &Client{
		listenAddr: listenAddr,
		localIP:    localIP,
		serverIP:   serverIP,
		key:        key,
	}

	// Start from stream ID 1
	c.nextStreamID.Store(1)

	return c
}

// Start starts the client
func (c *Client) Start() error {
	// Create transport
	trans, err := transport.NewRawTransport(c.localIP, c.serverIP, c.key)
	if err != nil {
		return fmt.Errorf("failed to create transport: %w", err)
	}
	c.transport = trans

	// Create TCP stack manager with gVisor
	tcpStackMgr, err := tunnel.NewTCPStackManager(trans, true)
	if err != nil {
		c.transport.Close()
		return fmt.Errorf("failed to create TCP stack manager: %w", err)
	}
	c.tcpStackMgr = tcpStackMgr

	// Start transport
	c.transport.Start()

	// Listen for TCP connections
	listener, err := net.Listen("tcp", c.listenAddr)
	if err != nil {
		c.tcpStackMgr.Close()
		c.transport.Close()
		return fmt.Errorf("failed to listen on %s: %w", c.listenAddr, err)
	}
	c.listener = listener

	log.Printf("Client listening on %s, forwarding via GRE to %s (gVisor TCP stack with SACK+BBR)",
		c.listenAddr, c.serverIP)

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
func (c *Client) handleConnection(conn net.Conn) {
	defer c.wg.Done()

	// Allocate stream ID
	streamID := c.nextStreamID.Add(1)

	log.Printf("New connection from %s, stream ID: %d", conn.RemoteAddr(), streamID)

	// Create TCP stream through gVisor stack
	_, err := c.tcpStackMgr.CreateStream(streamID, conn)
	if err != nil {
		log.Printf("Failed to create stream %d: %v", streamID, err)
		conn.Close()
		return
	}

	log.Printf("Connection closed, stream ID: %d", streamID)

	// Stream will be automatically cleaned up by TCPStackManager
	// when the bridging goroutines finish
}

// Close closes the client
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil
	}

	log.Printf("Closing client...")

	if c.listener != nil {
		c.listener.Close()
	}

	// Close TCP stack manager (this will close all streams)
	if c.tcpStackMgr != nil {
		c.tcpStackMgr.Close()
	}

	if c.transport != nil {
		c.transport.Close()
	}

	c.wg.Wait()
	log.Printf("Client closed")
	return nil
}

// Wait waits for the client to finish
func (c *Client) Wait() {
	c.wg.Wait()
}
