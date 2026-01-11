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
	// maxReadSize must fit in MTU: 1500 - IP(20) - GRE(8) - StreamHeader(5) = 1467
	// Use 1400 for safety margin
	maxReadSize = 1400
	// GRE header size with Key flag: 4(base) + 4(key) = 8
	greHeaderSize = 8
	// greBufferSize needs to fit: GRE(8) + StreamHeader(5) + Data(1400) = 1413
	// Round up for alignment
	greBufferSize = 1420
)

// streamConn represents an active TCP connection
type streamConn struct {
	id            uint32
	conn          net.Conn
	writeCh       chan []byte   // Async write channel
	closeCh       chan struct{} // Signal to stop accepting new data
	writerDone    chan struct{} // Signal that writer has finished
	serverClosed  atomic.Bool   // Server sent StreamClose
	closed        atomic.Bool
	bytesIn       atomic.Int64
	bytesOut      atomic.Int64
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
	defer c.streams.Delete(streamID)

	log.Printf("New connection from %s, stream ID: %d", tcpConn.RemoteAddr(), streamID)

	// Start writer goroutine (GRE → TCP)
	go sc.writeLoop()

	// TCP → GRE (read from TCP, send to GRE)
	bufPtr := c.bufferPool.Get().(*[]byte)
	buf := *bufPtr
	defer c.bufferPool.Put(bufPtr)

	for {
		// Set read deadline to allow checking for server close
		tcpConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

		// Reserve space for GRE header at the beginning, then Stream header
		// Layout: [GRE header (8)][Stream header (5)][TCP data (up to 1400)]
		dataOffset := greHeaderSize + tunnel.StreamHeaderSize
		n, err := tcpConn.Read(buf[dataOffset : dataOffset+maxReadSize])
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

		// Build stream packet directly in buffer starting after GRE header (zero-copy)
		pkt := tunnel.StreamPacket{
			StreamID: streamID,
			Flags:    tunnel.StreamData,
			Data:     buf[dataOffset : dataOffset+n],
		}
		streamSize := pkt.MarshalTo(buf[greHeaderSize:])

		// Send via GRE (will add GRE header to the reserved space)
		if err := c.transport.SendZeroCopy(buf, greHeaderSize, streamSize); err != nil {
			log.Printf("Stream %d send error: %v", streamID, err)
			break
		}
	}

	// Send close packet to notify server (half-close)
	if !sc.serverClosed.Load() {
		closePkt := tunnel.NewClosePacket(streamID)
		if closeData := closePkt.Marshal(); len(closeData) > 0 {
			c.transport.Send(closeData)
		}
	}

	// If server already closed, signal writer to drain
	// Give a small delay to allow any in-flight GRE packets to arrive
	if sc.serverClosed.Load() {
		time.Sleep(50 * time.Millisecond)
		sc.signalClose()
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
			if _, err := sc.conn.Write(data); err != nil {
				// Write error, drain channel and exit
				log.Printf("Stream %d: TCP write error: %v", sc.id, err)
				sc.drainWriteCh()
				return
			}
			sc.bytesIn.Add(int64(len(data)))
		case <-sc.closeCh:
			// Close signal received, drain remaining data then exit
			log.Printf("Stream %d: writeLoop received close signal, draining", sc.id)
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

// signalClose signals the writer to stop accepting new data and drain remaining
func (sc *streamConn) signalClose() {
	sc.closed.Store(true)
	select {
	case <-sc.closeCh:
	default:
		close(sc.closeCh)
	}
}

// close fully closes the stream connection (for error cases)
func (sc *streamConn) close() {
	sc.signalClose()
	sc.conn.Close()
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
		// Queue data to write channel
		if len(pkt.Data) == 0 {
			return
		}
		// Check if stream is already closed
		if sc.closed.Load() {
			log.Printf("Stream %d: received data but stream is closed, dropping %d bytes", pkt.StreamID, len(pkt.Data))
			return
		}
		// pkt.Data is already copied by UnmarshalStream
		// Use simple select with timeout - don't check closeCh here
		// to avoid Go's random selection causing data loss
		select {
		case sc.writeCh <- pkt.Data:
			// Queued successfully
		case <-time.After(5 * time.Second):
			// Timeout - TCP write is too slow
			log.Printf("Stream %d client write timeout", pkt.StreamID)
			sc.close()
		}

	case tunnel.StreamClose:
		// Server closed the stream
		// Only set serverClosed - let handleConnection handle the close sequence
		// This allows any remaining data packets to be queued before signalClose
		log.Printf("Stream %d: received StreamClose from server", pkt.StreamID)
		sc.serverClosed.Store(true)
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
