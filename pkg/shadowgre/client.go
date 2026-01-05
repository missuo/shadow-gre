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

const (
	copyBufferSize = 64 * 1024 // 64KB buffer for io.Copy
	// maxReadSize must fit in MTU: 1500 - IP(20) - GRE(12) - StreamHeader(5) = 1463
	// Use 1400 for safety margin
	maxReadSize = 1400
	greBufferSize = maxReadSize + tunnel.StreamHeaderSize
)

// streamConn represents an active TCP connection
type streamConn struct {
	id       uint32
	conn     net.Conn
	writeMu  sync.Mutex
	closed   atomic.Bool
	bytesIn  atomic.Int64
	bytesOut atomic.Int64
	closedCh chan struct{} // Closed when stream should be cleaned up
}

// Client represents a shadow-gre client
type Client struct {
	listenAddr string
	localIP    net.IP
	serverIP   net.IP
	key        uint32
	transport  *transport.RawTransport
	listener   net.Listener
	closed     atomic.Bool
	wg         sync.WaitGroup

	// Stream management
	nextStreamID atomic.Uint32
	streams      sync.Map // map[uint32]*streamConn

	// Buffer pool for zero-copy
	bufferPool sync.Pool
}

// NewClient creates a new shadow-gre client
func NewClient(listenAddr string, localIP, serverIP net.IP, key uint32) *Client {
	c := &Client{
		listenAddr: listenAddr,
		localIP:    localIP,
		serverIP:   serverIP,
		key:        key,
	}

	// Initialize buffer pool
	c.bufferPool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, greBufferSize)
			return &buf
		},
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

	// Set receive handler to process incoming GRE packets
	c.transport.SetReceiveHandler(c.handleGREPacket)

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

	// Allocate stream ID
	streamID := c.nextStreamID.Add(1)

	// Create stream connection
	sc := &streamConn{
		id:       streamID,
		conn:     tcpConn,
		closedCh: make(chan struct{}),
	}
	c.streams.Store(streamID, sc)
	defer c.streams.Delete(streamID)

	log.Printf("New connection from %s, stream ID: %d", tcpConn.RemoteAddr(), streamID)

	// TCP → GRE (read from TCP, send to GRE)
	bufPtr := c.bufferPool.Get().(*[]byte)
	buf := *bufPtr
	defer c.bufferPool.Put(bufPtr)

	for {
		// Read from TCP (limit to maxReadSize to respect MTU)
		n, err := tcpConn.Read(buf[tunnel.StreamHeaderSize : tunnel.StreamHeaderSize+maxReadSize])
		if err != nil {
			if err != io.EOF {
				log.Printf("Stream %d read error: %v", streamID, err)
			}
			break
		}

		sc.bytesOut.Add(int64(n))

		// Build stream packet directly in buffer (zero-copy)
		pkt := tunnel.StreamPacket{
			StreamID: streamID,
			Flags:    tunnel.StreamData,
			Data:     buf[tunnel.StreamHeaderSize : tunnel.StreamHeaderSize+n],
		}
		size := pkt.MarshalTo(buf)

		// Send via GRE
		if err := c.transport.Send(buf[:size]); err != nil {
			log.Printf("Stream %d send error: %v", streamID, err)
			break
		}
	}

	// Send close packet to notify server (half-close)
	closePkt := tunnel.NewClosePacket(streamID)
	if closeData := closePkt.Marshal(); len(closeData) > 0 {
		c.transport.Send(closeData)
	}

	// Wait for server to close the stream or timeout
	select {
	case <-sc.closedCh:
		// Server closed the stream
	case <-time.After(30 * time.Second):
		// Timeout waiting for server close
		log.Printf("Stream %d timeout waiting for server close", streamID)
	}

	log.Printf("Connection closed, stream ID: %d (sent: %d bytes, recv: %d bytes)",
		streamID, sc.bytesOut.Load(), sc.bytesIn.Load())
}

// handleGREPacket processes incoming GRE packets
func (c *Client) handleGREPacket(data []byte) {
	// Parse stream packet
	pkt, err := tunnel.UnmarshalStream(data)
	if err != nil {
		return
	}

	// Look up stream
	scI, ok := c.streams.Load(pkt.StreamID)
	if !ok {
		// Stream not found, ignore
		return
	}
	sc := scI.(*streamConn)

	// Handle based on flags
	switch pkt.Flags {
	case tunnel.StreamData:
		// Write data to TCP connection
		if len(pkt.Data) > 0 {
			sc.writeMu.Lock()
			n, err := sc.conn.Write(pkt.Data)
			sc.writeMu.Unlock()

			if err != nil {
				sc.conn.Close()
				return
			}
			sc.bytesIn.Add(int64(n))
		}

	case tunnel.StreamClose:
		// Server closed the stream, notify handleConnection
		select {
		case <-sc.closedCh:
			// Already closed
		default:
			close(sc.closedCh)
		}
		// Close TCP connection
		sc.conn.Close()
	}
}

// Close closes the client
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil
	}

	if c.listener != nil {
		c.listener.Close()
	}

	// Close all streams
	c.streams.Range(func(key, value interface{}) bool {
		sc := value.(*streamConn)
		sc.conn.Close()
		return true
	})

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
