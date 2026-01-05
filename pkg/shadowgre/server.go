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

// serverStream represents a backend connection for a stream
type serverStream struct {
	id               uint32
	clientIP         net.IP
	backendConn      net.Conn
	writeCh          chan []byte // Sequenced write channel
	closed           atomic.Bool
	backendClosed    atomic.Bool // Backend read side closed
	clientCloseRecv  atomic.Bool // Received StreamClose from client
	bytesIn          atomic.Int64
	bytesOut         atomic.Int64
	server           *Server
	wg               sync.WaitGroup
}

// Server represents a shadow-gre server
type Server struct {
	localIP     net.IP
	backendAddr string
	key         uint32
	transport   *transport.ServerTransport
	clients     sync.Map // map[string]*clientState
	closed      atomic.Bool
	wg          sync.WaitGroup

	// Buffer pool for zero-copy
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
	// Parse stream packet (no copy - just parse header and reference data)
	pkt, err := tunnel.UnmarshalStreamNoCopy(data)
	if err != nil {
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
		// Get or create stream
		ssI, ok := cs.streams.Load(pkt.StreamID)
		if !ok {
			// Create new backend connection
			ss, err := s.createStream(pkt.StreamID, clientIP, cs)
			if err != nil {
				log.Printf("Failed to create stream %d for %s: %v", pkt.StreamID, clientIP, err)
				return
			}
			// Try to store, if another goroutine already created it, use that one
			actual, loaded := cs.streams.LoadOrStore(pkt.StreamID, ss)
			if loaded {
				// Another goroutine created the stream, close ours and use theirs
				ss.close()
				ssI = actual
			} else {
				ssI = ss
			}
		}

		ss := ssI.(*serverStream)
		if ss == nil {
			return
		}

		// Queue data for async write to backend (copy once here, not twice!)
		if len(pkt.Data) > 0 {
			// Don't accept data if backend already closed
			if ss.closed.Load() {
				return
			}

			// Copy data only once (pkt.Data references the receive buffer)
			dataCopy := make([]byte, len(pkt.Data))
			copy(dataCopy, pkt.Data)

			// Try to queue without blocking
			select {
			case ss.writeCh <- dataCopy:
				// Queued successfully
			default:
				// Channel full - backend is too slow, must close stream to avoid blocking receiveLoop
				log.Printf("Stream %d write channel full (backend too slow), closing stream", pkt.StreamID)
				go ss.close()  // Close async to avoid blocking
				cs.streams.Delete(pkt.StreamID)
			}
		}

	case tunnel.StreamClose:
		// Client closed, mark and cleanup if backend also closed
		if ssI, ok := cs.streams.Load(pkt.StreamID); ok {
			if ss := ssI.(*serverStream); ss != nil {
				ss.clientCloseRecv.Store(true)
				ss.close()  // Close backend connection

				// Delete stream if backend also closed, otherwise let readFromBackend handle it
				if ss.backendClosed.Load() {
					cs.streams.Delete(pkt.StreamID)
				}
			}
		}
	}
}

// createStream creates a new backend connection for a stream
func (s *Server) createStream(streamID uint32, clientIP net.IP, cs *clientState) (*serverStream, error) {
	// Connect to backend
	backendConn, err := net.Dial("tcp", s.backendAddr)
	if err != nil {
		return nil, err
	}

	ss := &serverStream{
		id:          streamID,
		clientIP:    clientIP,
		backendConn: backendConn,
		writeCh:     make(chan []byte, 1024), // Larger buffer to avoid blocking
		server:      s,
	}

	log.Printf("Stream %d from %s forwarding to %s", streamID, clientIP, s.backendAddr)

	// Start goroutines
	s.wg.Add(2)
	go ss.writeToBackend(cs)     // Write to backend (GRE → Backend)
	go ss.readFromBackend(cs)    // Read from backend (Backend → GRE)

	return ss, nil
}

// writeToBackend writes data from GRE to backend (async, preserves order)
func (ss *serverStream) writeToBackend(cs *clientState) {
	defer ss.server.wg.Done()

	for data := range ss.writeCh {
		n, err := ss.backendConn.Write(data)
		if err != nil {
			log.Printf("Stream %d backend write error: %v", ss.id, err)
			ss.close()
			return
		}
		ss.bytesOut.Add(int64(n))
	}
}

// readFromBackend reads data from backend and sends to client via GRE
func (ss *serverStream) readFromBackend(cs *clientState) {
	defer ss.server.wg.Done()

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

	// Mark backend as closed
	ss.backendClosed.Store(true)

	// Send close packet to client
	closePkt := tunnel.NewClosePacket(ss.id)
	if closeData := closePkt.Marshal(); len(closeData) > 0 {
		ss.server.transport.Send(ss.clientIP, closeData)
	}

	// Only delete stream if client also closed, otherwise wait for client close
	if ss.clientCloseRecv.Load() {
		cs.streams.Delete(ss.id)
		log.Printf("Stream %d from %s closed (sent: %d bytes, recv: %d bytes)",
			ss.id, ss.clientIP, ss.bytesIn.Load(), ss.bytesOut.Load())
	} else {
		// Wait for client close with timeout
		go func() {
			time.Sleep(10 * time.Second)
			if !ss.clientCloseRecv.Load() {
				log.Printf("Stream %d timeout waiting for client close, cleaning up", ss.id)
				cs.streams.Delete(ss.id)
			}
		}()
	}
}

// close closes the stream
func (ss *serverStream) close() {
	if !ss.closed.Swap(true) {
		close(ss.writeCh)         // Close write channel
		ss.backendConn.Close()    // Close backend connection
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
