package shadowgre

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"
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

// clientState manages streams for a single client
type clientState struct {
	clientIP net.IP
	streams  sync.Map // map[uint32]*serverStream
	server   *Server
	reliable *tunnel.ReliableManager
}

// streamState represents the connection state
type streamState int32

const (
	streamStateConnecting streamState = iota // Dial in progress
	streamStateConnected                     // Backend connected
	streamStateClosed                        // Stream closed
)

// serverStream represents a backend connection for a stream
type serverStream struct {
	id              uint32
	clientIP        net.IP
	backendConn     net.Conn
	writeCh         chan []byte   // Buffered write channel
	writeCloseCh    chan struct{} // Signal to stop writer (client closed)
	closeCh         chan struct{} // Signal stream fully closed
	state           atomic.Int32  // streamState
	clientCloseRecv atomic.Bool   // Received StreamClose from client
	bytesIn         atomic.Int64
	bytesOut        atomic.Int64
	server          *Server
	cs              *clientState
	pendingData     [][]byte   // Data received before backend connected
	pendingMu       sync.Mutex // Protects pendingData
}

// Server represents a shadow-gre server
type Server struct {
	localIP     net.IP
	backendAddr string
	key         uint32
	transport   *transport.ServerTransport
	clients     sync.Map // map[string]*clientState
	closed      atomic.Bool

	wg         sync.WaitGroup
	bufferPool sync.Pool
}

// NewServer creates a new shadow-gre server
func NewServer(localIP net.IP, backendAddr string, key uint32) *Server {
	s := &Server{
		localIP:     localIP,
		backendAddr: backendAddr,
		key:         key,
	}

	// Initialize buffer pool
	s.bufferPool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, greBufferSize)
			return &buf
		},
	}

	return s
}

// Start starts the server
func (s *Server) Start() error {
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

	log.Printf("Server listening on %s (GRE protocol, reliable mode), forwarding to %s", s.localIP, s.backendAddr)

	return nil
}

// getOrCreateClient gets or creates client state for a client IP
func (s *Server) getOrCreateClient(clientIP net.IP) *clientState {
	clientKey := clientIP.String()
	csI, loaded := s.clients.Load(clientKey)
	if loaded {
		return csI.(*clientState)
	}

	// Create new client state with reliable manager
	cs := &clientState{
		clientIP: clientIP,
		server:   s,
	}

	// Create reliable manager for this client
	cs.reliable = tunnel.NewReliableManager(
		func(data []byte) error {
			return s.transport.Send(clientIP, data)
		},
		func(streamID uint32, data []byte) {
			s.handleReliableData(cs, streamID, data)
		},
		func(streamID uint32) {
			s.handleReliableClose(cs, streamID)
		},
	)

	actual, loaded := s.clients.LoadOrStore(clientKey, cs)
	if loaded {
		// Another goroutine created it, close ours
		cs.reliable.Close()
		return actual.(*clientState)
	}

	return cs
}

// handleGREPacket processes incoming GRE packets
func (s *Server) handleGREPacket(clientIP net.IP, data []byte) {
	// Get or create client state
	cs := s.getOrCreateClient(clientIP)

	// Pass to reliable manager for processing
	if err := cs.reliable.Receive(data); err != nil {
		// Ignore parse errors silently
		return
	}
}

// handleReliableData handles data delivered by reliable layer
func (s *Server) handleReliableData(cs *clientState, streamID uint32, data []byte) {
	// Get or create stream
	ssI, loaded := cs.streams.Load(streamID)
	if !loaded {
		// Create new stream (async dial)
		ss := s.createStream(streamID, cs.clientIP, cs)
		actual, alreadyExists := cs.streams.LoadOrStore(streamID, ss)
		if alreadyExists {
			// Another goroutine created it first, use that one
			ss = actual.(*serverStream)
		}
		ssI = ss
	}

	ss := ssI.(*serverStream)
	state := streamState(ss.state.Load())

	// Check if stream is closed
	if state == streamStateClosed {
		return
	}

	if len(data) == 0 {
		return
	}

	// If still connecting, queue to pending
	if state == streamStateConnecting {
		ss.pendingMu.Lock()
		// Double-check state after acquiring lock
		if streamState(ss.state.Load()) == streamStateConnecting {
			// Make a copy of data
			dataCopy := make([]byte, len(data))
			copy(dataCopy, data)
			ss.pendingData = append(ss.pendingData, dataCopy)
			ss.pendingMu.Unlock()
			return
		}
		ss.pendingMu.Unlock()
		// State changed to connected, fall through to send via channel
	}

	// Check if client has already closed write side
	select {
	case <-ss.writeCloseCh:
		// Client closed, ignore new data
		return
	default:
	}

	// Stream is connected, send via channel
	// Make a copy of data
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	select {
	case ss.writeCh <- dataCopy:
		// Queued successfully
	case <-time.After(5 * time.Second):
		// Timeout - backend is too slow
		log.Printf("Stream %d write timeout (backend too slow), closing stream", streamID)
		ss.close()
	}
}

// handleReliableClose handles close notification from reliable layer
func (s *Server) handleReliableClose(cs *clientState, streamID uint32) {
	ssI, ok := cs.streams.Load(streamID)
	if !ok {
		return
	}
	ss := ssI.(*serverStream)
	ss.clientCloseRecv.Store(true)
	// Only close write channel, let readFromBackend continue
	// This allows backend to finish sending response
	ss.closeWrite()
}

// createStream creates a new stream and starts async backend connection
func (s *Server) createStream(streamID uint32, clientIP net.IP, cs *clientState) *serverStream {
	ss := &serverStream{
		id:           streamID,
		clientIP:     clientIP,
		writeCh:      make(chan []byte, 4096), // Larger buffer for better throughput
		writeCloseCh: make(chan struct{}),
		closeCh:      make(chan struct{}),
		server:       s,
		cs:           cs,
	}
	ss.state.Store(int32(streamStateConnecting))

	// Start async connection
	s.wg.Add(1)
	go ss.connectAndRun()

	return ss
}

// connectAndRun connects to backend and starts read/write goroutines
func (ss *serverStream) connectAndRun() {
	defer ss.server.wg.Done()

	// Connect to backend with timeout
	dialer := net.Dialer{Timeout: 10 * time.Second}
	backendConn, err := dialer.Dial("tcp", ss.server.backendAddr)
	if err != nil {
		log.Printf("Stream %d failed to connect to backend: %v", ss.id, err)
		ss.state.Store(int32(streamStateClosed))
		ss.cs.streams.Delete(ss.id)
		// Send close to client via reliable layer
		ss.cs.reliable.SendClose(ss.id)
		return
	}

	ss.backendConn = backendConn
	log.Printf("Stream %d from %s forwarding to %s", ss.id, ss.clientIP, ss.server.backendAddr)

	// Flush pending data
	ss.pendingMu.Lock()
	pendingData := ss.pendingData
	ss.pendingData = nil
	ss.state.Store(int32(streamStateConnected))
	ss.pendingMu.Unlock()

	// Write pending data
	for _, data := range pendingData {
		if _, err := ss.backendConn.Write(data); err != nil {
			log.Printf("Stream %d backend write error (pending): %v", ss.id, err)
			ss.close()
			return
		}
		ss.bytesOut.Add(int64(len(data)))
	}

	// Start read/write goroutines
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		ss.writeToBackend()
	}()
	go func() {
		defer wg.Done()
		ss.readFromBackend()
	}()
	wg.Wait()

	// Cleanup
	ss.cs.streams.Delete(ss.id)
	ss.cs.reliable.RemoveStream(ss.id)
	log.Printf("Stream %d from %s closed (sent: %d bytes, recv: %d bytes)",
		ss.id, ss.clientIP, ss.bytesIn.Load(), ss.bytesOut.Load())
}

// writeToBackend writes data from GRE to backend (async, preserves order)
func (ss *serverStream) writeToBackend() {
	for {
		select {
		case data, ok := <-ss.writeCh:
			if !ok {
				return
			}
			// Set write deadline to prevent blocking forever
			ss.backendConn.SetWriteDeadline(time.Now().Add(30 * time.Second))
			if _, err := ss.backendConn.Write(data); err != nil {
				log.Printf("Stream %d backend write error: %v", ss.id, err)
				ss.close()
				return
			}
			ss.bytesOut.Add(int64(len(data)))
		case <-ss.writeCloseCh:
			// Client closed, drain remaining data and shutdown write side
			ss.drainAndShutdownWrite()
			return
		case <-ss.closeCh:
			// Full close, drain and exit
			ss.drainWriteCh()
			return
		}
	}
}

// drainWriteCh drains remaining data from writeCh
func (ss *serverStream) drainWriteCh() {
	for {
		select {
		case data, ok := <-ss.writeCh:
			if !ok {
				return
			}
			// Set write deadline for drain as well
			ss.backendConn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			ss.backendConn.Write(data)
			ss.bytesOut.Add(int64(len(data)))
		default:
			return
		}
	}
}

// drainAndShutdownWrite drains writeCh and shuts down write side of backend
func (ss *serverStream) drainAndShutdownWrite() {
	// Drain remaining data
	ss.drainWriteCh()
	// Shutdown write side of backend connection to signal EOF
	if tcpConn, ok := ss.backendConn.(*net.TCPConn); ok {
		tcpConn.CloseWrite()
	}
}

// readFromBackend reads data from backend and sends to client via reliable GRE
func (ss *serverStream) readFromBackend() {
	buf := make([]byte, maxReadSize)
	idleTimeout := 30 * time.Second // Timeout after client closes

	for {
		// If client has closed, set read deadline to avoid hanging forever
		if ss.clientCloseRecv.Load() {
			ss.backendConn.SetReadDeadline(time.Now().Add(idleTimeout))
		}

		n, err := ss.backendConn.Read(buf)
		if err != nil {
			// Don't log EOF, timeout, or closed connection errors (normal closure)
			if err != io.EOF && !isClosedConnError(err) && !isTimeoutError(err) {
				log.Printf("Stream %d backend read error: %v", ss.id, err)
			}
			break
		}

		// Reset deadline on successful read
		ss.backendConn.SetReadDeadline(time.Time{})

		ss.bytesIn.Add(int64(n))

		// Send via reliable layer
		// Make a copy since reliable layer stores for retransmission
		data := make([]byte, n)
		copy(data, buf[:n])

		if err := ss.cs.reliable.Send(ss.id, data); err != nil {
			log.Printf("Stream %d reliable send error: %v", ss.id, err)
			break
		}
	}

	// Send close packet to client via reliable layer
	ss.cs.reliable.SendClose(ss.id)

	// Close the stream
	ss.close()
}

// isClosedConnError checks if error is due to closed connection
func isClosedConnError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "use of closed network connection")
}

// isTimeoutError checks if error is a timeout
func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}
	return false
}

// closeWrite signals writer to stop (client sent StreamClose)
func (ss *serverStream) closeWrite() {
	select {
	case <-ss.writeCloseCh:
		// Already closed
	default:
		close(ss.writeCloseCh)
	}
}

// close fully closes the stream
func (ss *serverStream) close() {
	if !ss.state.CompareAndSwap(int32(streamStateConnecting), int32(streamStateClosed)) &&
		!ss.state.CompareAndSwap(int32(streamStateConnected), int32(streamStateClosed)) {
		return // Already closed
	}

	// Signal write close first
	ss.closeWrite()

	// Signal full close
	select {
	case <-ss.closeCh:
	default:
		close(ss.closeCh)
	}

	// Close backend connection if exists
	if ss.backendConn != nil {
		ss.backendConn.Close()
	}
}

// Close closes the server
func (s *Server) Close() error {
	if s.closed.Swap(true) {
		return nil
	}

	// Close all client streams and reliable managers
	s.clients.Range(func(key, value interface{}) bool {
		cs := value.(*clientState)
		// Close reliable manager
		if cs.reliable != nil {
			cs.reliable.Close()
		}
		// Close all streams
		cs.streams.Range(func(k, v interface{}) bool {
			if ss := v.(*serverStream); ss != nil {
				ss.close()
			}
			return true
		})
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
