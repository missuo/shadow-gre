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
	// maxReadSize must fit in MTU: 1500 - IP(20) - GRE(8) - ReliableHeader(13+SACK) = ~1450
	// Use 1300 for safety margin with reliable headers and SACK blocks
	maxReadSize = 1300
	// greBufferSize needs to fit: GRE(8) + ReliableHeader(~45) + Data(1300) = ~1353
	// Round up for alignment
	greBufferSize = 1400
)

// streamConn represents an active TCP connection
type streamConn struct {
	id           uint32
	conn         net.Conn
	writeCh      chan []byte   // Async write channel (from GRE to TCP)
	closeCh      chan struct{} // Signal to stop accepting new data
	writerDone   chan struct{} // Signal that writer has finished
	serverClosed atomic.Bool   // Server sent StreamClose
	closed       atomic.Bool
	bytesIn      atomic.Int64
	bytesOut     atomic.Int64
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

	// Reliable transport layer
	reliable *tunnel.ReliableManager

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

	// Create reliable manager
	c.reliable = tunnel.NewReliableManager(
		func(data []byte) error {
			return c.transport.Send(data)
		},
		c.handleReliableData,
		c.handleReliableClose,
	)

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

	log.Printf("Client listening on %s, forwarding via GRE to %s (reliable mode)", c.listenAddr, c.serverIP)

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

	// Allocate stream ID
	streamID := c.nextStreamID.Add(1)

	// Create stream connection
	sc := &streamConn{
		id:         streamID,
		conn:       tcpConn,
		writeCh:    make(chan []byte, 4096), // Larger buffer for throughput
		closeCh:    make(chan struct{}),
		writerDone: make(chan struct{}),
	}
	c.streams.Store(streamID, sc)

	log.Printf("New connection from %s, stream ID: %d", tcpConn.RemoteAddr(), streamID)

	// Start writer goroutine (GRE → TCP)
	go sc.writeLoop()

	// TCP → GRE (read from TCP, send to GRE via reliable layer)
	buf := make([]byte, maxReadSize)

	for {
		// Set read deadline to allow checking for server close
		tcpConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

		n, err := tcpConn.Read(buf)
		if err != nil {
			// Check if server closed the stream
			if sc.serverClosed.Load() {
				break
			}
			// Check for timeout (not a real error, just checking for server close)
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			if err != io.EOF {
				log.Printf("Stream %d read error: %v", streamID, err)
			}
			break
		}

		sc.bytesOut.Add(int64(n))

		// Send via reliable layer
		// Make a copy since reliable layer stores for retransmission
		data := make([]byte, n)
		copy(data, buf[:n])

		if err := c.reliable.Send(streamID, data); err != nil {
			log.Printf("Stream %d reliable send error: %v", streamID, err)
			break
		}
	}

	// Send close packet to notify server (half-close)
	if !sc.serverClosed.Load() {
		if err := c.reliable.SendClose(streamID); err != nil {
			log.Printf("Stream %d send close error: %v", streamID, err)
		}
	}

	// Wait for writer to finish (server close or timeout)
	select {
	case <-sc.writerDone:
		// Writer finished, all data written
	case <-time.After(30 * time.Second):
		// Timeout waiting for server close
		log.Printf("Stream %d timeout waiting for server close", streamID)
		sc.signalClose()
		<-sc.writerDone
	}

	// Now safe to close TCP connection
	tcpConn.Close()

	log.Printf("Connection closed, stream ID: %d (sent: %d bytes, recv: %d bytes)",
		streamID, sc.bytesOut.Load(), sc.bytesIn.Load())

	// Keep the stream in the map for a while to handle late-arriving packets
	// This prevents "stream not found" errors for packets that arrive after we close
	go func() {
		time.Sleep(5 * time.Second)
		c.streams.Delete(streamID)
		c.reliable.RemoveStream(streamID)
	}()
}

// writeLoop writes data from writeCh to TCP connection
func (sc *streamConn) writeLoop() {
	defer close(sc.writerDone)

	for {
		select {
		case data, ok := <-sc.writeCh:
			if !ok {
				return
			}
			// Set write deadline to prevent blocking forever
			sc.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
			n, err := sc.conn.Write(data)
			if err != nil {
				// Write error (e.g., broken pipe), drain channel and exit
				log.Printf("Stream %d: TCP write error after %d bytes: %v", sc.id, n, err)
				sc.closed.Store(true)
				sc.drainWriteCh()
				return
			}
			sc.bytesIn.Add(int64(n))
		case <-sc.closeCh:
			// Close signal received
			// Wait for any in-flight GRE packets to be queued
			// Network reordering can cause StreamClose to arrive before data
			time.Sleep(100 * time.Millisecond)
			// Now block new data and drain
			sc.closed.Store(true)
			sc.drainWriteCh()
			return
		}
	}
}

// drainWriteCh writes all remaining data in writeCh
func (sc *streamConn) drainWriteCh() {
	for {
		select {
		case data, ok := <-sc.writeCh:
			if !ok {
				return
			}
			// Set write deadline for drain as well
			sc.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if _, err := sc.conn.Write(data); err != nil {
				return
			}
			sc.bytesIn.Add(int64(len(data)))
		default:
			return
		}
	}
}

// signalClose signals the writer to drain and exit
// Does NOT set closed immediately - allows in-flight data to be queued
func (sc *streamConn) signalClose() {
	select {
	case <-sc.closeCh:
	default:
		close(sc.closeCh)
	}
}

// close fully closes the stream connection (for error cases)
func (sc *streamConn) close() {
	sc.closed.Store(true)
	sc.signalClose()
	sc.conn.Close()
}

// handleGREPacket processes incoming GRE packets (reliable layer)
func (c *Client) handleGREPacket(data []byte) {
	// Pass to reliable manager for processing
	if err := c.reliable.Receive(data); err != nil {
		// Ignore parse errors silently
		return
	}
}

// handleReliableData handles data delivered by reliable layer
func (c *Client) handleReliableData(streamID uint32, data []byte) {
	// Look up stream
	scI, ok := c.streams.Load(streamID)
	if !ok {
		// Stream not found, ignore (normal for late-arriving packets after close)
		return
	}
	sc := scI.(*streamConn)

	// Check if stream is already closed
	if sc.closed.Load() {
		return
	}

	// Queue data to write channel
	select {
	case sc.writeCh <- data:
		// Queued successfully
	case <-time.After(5 * time.Second):
		// Timeout - TCP write is too slow
		log.Printf("Stream %d client write timeout", streamID)
		sc.close()
	}
}

// handleReliableClose handles close notification from reliable layer
func (c *Client) handleReliableClose(streamID uint32) {
	// Look up stream
	scI, ok := c.streams.Load(streamID)
	if !ok {
		return
	}
	sc := scI.(*streamConn)

	// Server closed the stream
	sc.serverClosed.Store(true)
	// Signal writer to drain - it will wait for in-flight packets before setting closed
	sc.signalClose()
}

// Close closes the client
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil
	}

	if c.listener != nil {
		c.listener.Close()
	}

	// Close reliable manager
	if c.reliable != nil {
		c.reliable.Close()
	}

	// Close all streams
	c.streams.Range(func(key, value interface{}) bool {
		sc := value.(*streamConn)
		sc.close()
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
