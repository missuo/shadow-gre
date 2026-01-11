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

// clientState manages streams for a single client
type clientState struct {
	clientIP net.IP
	streams  sync.Map // map[uint32]*serverStream
	server   *Server
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
	closeCh         chan struct{} // Signal to stop writer
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

	log.Printf("Server listening on %s (GRE protocol), forwarding to %s", s.localIP, s.backendAddr)

	return nil
}

// handleGREPacket processes incoming GRE packets
func (s *Server) handleGREPacket(clientIP net.IP, data []byte) {
	// Parse stream packet
	pkt, err := tunnel.UnmarshalStreamNoCopy(data)
	if err != nil {
		log.Printf("Failed to unmarshal stream packet from %s: %v (data length: %d)", clientIP, err, len(data))
		return
	}

	// Get or create client state
	clientKey := clientIP.String()
	csI, _ := s.clients.LoadOrStore(clientKey, &clientState{
		clientIP: clientIP,
		server:   s,
	})
	cs := csI.(*clientState)

	// Handle based on flags
	switch pkt.Flags {
	case tunnel.StreamData:
		s.handleStreamData(cs, clientIP, pkt)

	case tunnel.StreamClose:
		s.handleStreamClose(cs, pkt.StreamID)
	}
}

// handleStreamData handles incoming data packets
func (s *Server) handleStreamData(cs *clientState, clientIP net.IP, pkt *tunnel.StreamPacket) {
	// Copy data immediately (pkt.Data references the receive buffer which will be reused)
	var dataCopy []byte
	if len(pkt.Data) > 0 {
		dataCopy = make([]byte, len(pkt.Data))
		copy(dataCopy, pkt.Data)
	}

	// Get or create stream
	ssI, loaded := cs.streams.Load(pkt.StreamID)
	if !loaded {
		// Create new stream (async dial)
		ss := s.createStream(pkt.StreamID, clientIP, cs)
		actual, alreadyExists := cs.streams.LoadOrStore(pkt.StreamID, ss)
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

	if len(dataCopy) == 0 {
		return
	}

	// If still connecting, queue to pending
	if state == streamStateConnecting {
		ss.pendingMu.Lock()
		// Double-check state after acquiring lock
		if streamState(ss.state.Load()) == streamStateConnecting {
			ss.pendingData = append(ss.pendingData, dataCopy)
			ss.pendingMu.Unlock()
			return
		}
		ss.pendingMu.Unlock()
		// State changed to connected, fall through to send via channel
	}

	// Stream is connected, send via channel
	// Use select with timeout to avoid blocking receiveLoop forever
	select {
	case ss.writeCh <- dataCopy:
		// Queued successfully
	case <-ss.closeCh:
		// Stream is closing
	case <-time.After(5 * time.Second):
		// Timeout - backend is too slow
		log.Printf("Stream %d write timeout (backend too slow), closing stream", pkt.StreamID)
		ss.close()
	}
}

// handleStreamClose handles stream close packets
func (s *Server) handleStreamClose(cs *clientState, streamID uint32) {
	ssI, ok := cs.streams.Load(streamID)
	if !ok {
		return
	}
	ss := ssI.(*serverStream)
	ss.clientCloseRecv.Store(true)
	ss.close()
}

// createStream creates a new stream and starts async backend connection
func (s *Server) createStream(streamID uint32, clientIP net.IP, cs *clientState) *serverStream {
	ss := &serverStream{
		id:       streamID,
		clientIP: clientIP,
		writeCh:  make(chan []byte, 4096), // Larger buffer for better throughput
		closeCh:  make(chan struct{}),
		server:   s,
		cs:       cs,
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
		// Send close to client
		closePkt := tunnel.NewClosePacket(ss.id)
		if closeData := closePkt.Marshal(); len(closeData) > 0 {
			ss.server.transport.Send(ss.clientIP, closeData)
		}
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
			if _, err := ss.backendConn.Write(data); err != nil {
				log.Printf("Stream %d backend write error: %v", ss.id, err)
				ss.close()
				return
			}
			ss.bytesOut.Add(int64(len(data)))
		case <-ss.closeCh:
			// Drain remaining data before exit
			for {
				select {
				case data, ok := <-ss.writeCh:
					if !ok {
						return
					}
					ss.backendConn.Write(data)
				default:
					return
				}
			}
		}
	}
}

// readFromBackend reads data from backend and sends to client via GRE
func (ss *serverStream) readFromBackend() {
	bufPtr := ss.server.bufferPool.Get().(*[]byte)
	buf := *bufPtr
	defer ss.server.bufferPool.Put(bufPtr)

	for {
		// Reserve space for GRE header at the beginning, then Stream header
		// Layout: [GRE header (8)][Stream header (5)][Backend data (up to 1400)]
		dataOffset := greHeaderSize + tunnel.StreamHeaderSize
		n, err := ss.backendConn.Read(buf[dataOffset : dataOffset+maxReadSize])
		if err != nil {
			if err != io.EOF {
				log.Printf("Stream %d backend read error: %v", ss.id, err)
			}
			break
		}

		ss.bytesIn.Add(int64(n))

		// Build stream packet directly in buffer starting after GRE header (zero-copy)
		pkt := tunnel.StreamPacket{
			StreamID: ss.id,
			Flags:    tunnel.StreamData,
			Data:     buf[dataOffset : dataOffset+n],
		}
		streamSize := pkt.MarshalTo(buf[greHeaderSize:])

		// Send via GRE (will add GRE header to the reserved space)
		if err := ss.server.transport.SendZeroCopy(ss.clientIP, buf, greHeaderSize, streamSize); err != nil {
			log.Printf("Stream %d send error: %v", ss.id, err)
			break
		}
	}

	// Send close packet to client
	closePkt := tunnel.NewClosePacket(ss.id)
	if closeData := closePkt.Marshal(); len(closeData) > 0 {
		ss.server.transport.Send(ss.clientIP, closeData)
	}

	// Close the stream
	ss.close()
}

// close closes the stream
func (ss *serverStream) close() {
	if !ss.state.CompareAndSwap(int32(streamStateConnecting), int32(streamStateClosed)) &&
		!ss.state.CompareAndSwap(int32(streamStateConnected), int32(streamStateClosed)) {
		return // Already closed
	}

	// Signal close
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

	// Close all client streams
	s.clients.Range(func(key, value interface{}) bool {
		cs := value.(*clientState)
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
