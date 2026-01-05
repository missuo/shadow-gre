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

// clientState manages streams for a single client
type clientState struct {
	clientIP net.IP
	streams  sync.Map // map[uint32]*serverStream
	server   *Server
}

// serverStream represents a backend connection for a stream
type serverStream struct {
	id          uint32
	clientIP    net.IP
	backendConn net.Conn
	writeMu     sync.Mutex
	closed      atomic.Bool
	bytesIn     atomic.Int64
	bytesOut    atomic.Int64
	server      *Server
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
	// Parse stream packet
	pkt, err := tunnel.UnmarshalStream(data)
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

		// Write data to backend
		if len(pkt.Data) > 0 {
			ss.writeMu.Lock()
			n, err := ss.backendConn.Write(pkt.Data)
			ss.writeMu.Unlock()

			if err != nil {
				ss.close()
				return
			}
			ss.bytesOut.Add(int64(n))
		}

	case tunnel.StreamClose:
		// Close stream
		if ssI, ok := cs.streams.Load(pkt.StreamID); ok {
			if ss := ssI.(*serverStream); ss != nil {
				ss.close()
			}
		}
		cs.streams.Delete(pkt.StreamID)
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
		server:      s,
	}

	log.Printf("Stream %d from %s forwarding to %s", streamID, clientIP, s.backendAddr)

	// Start reading from backend
	s.wg.Add(1)
	go ss.readFromBackend(cs)

	return ss, nil
}

// readFromBackend reads data from backend and sends to client via GRE
func (ss *serverStream) readFromBackend(cs *clientState) {
	defer ss.server.wg.Done()
	defer ss.close()

	bufPtr := ss.server.bufferPool.Get().(*[]byte)
	buf := *bufPtr
	defer ss.server.bufferPool.Put(bufPtr)

	for {
		// Read from backend
		n, err := ss.backendConn.Read(buf[tunnel.StreamHeaderSize:])
		if err != nil {
			if err != io.EOF {
				log.Printf("Stream %d backend read error: %v", ss.id, err)
			}
			break
		}

		ss.bytesIn.Add(int64(n))

		// Build stream packet directly in buffer (zero-copy)
		pkt := tunnel.StreamPacket{
			StreamID: ss.id,
			Flags:    tunnel.StreamData,
			Data:     buf[tunnel.StreamHeaderSize : tunnel.StreamHeaderSize+n],
		}
		size := pkt.MarshalTo(buf)

		// Send via GRE
		if err := ss.server.transport.Send(ss.clientIP, buf[:size]); err != nil {
			log.Printf("Stream %d send error: %v", ss.id, err)
			break
		}
	}

	// Send close packet
	closePkt := tunnel.NewClosePacket(ss.id)
	if closeData := closePkt.Marshal(); len(closeData) > 0 {
		ss.server.transport.Send(ss.clientIP, closeData)
	}

	// Remove from client streams
	cs.streams.Delete(ss.id)

	log.Printf("Stream %d from %s closed (sent: %d bytes, recv: %d bytes)",
		ss.id, ss.clientIP, ss.bytesIn.Load(), ss.bytesOut.Load())
}

// close closes the stream
func (ss *serverStream) close() {
	if !ss.closed.Swap(true) {
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
