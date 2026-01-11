// Package tunnel implements lightweight stream multiplexing over GRE.
package tunnel

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// Reliable packet flags (bitmask)
	FlagData  uint8 = 0x01 // Has data payload
	FlagAck   uint8 = 0x02 // Has ACK field
	FlagClose uint8 = 0x04 // Close stream
	FlagSyn   uint8 = 0x08 // Synchronize (first packet)
	FlagSack  uint8 = 0x10 // Has SACK blocks

	// ReliableHeaderSize: StreamID(4) + Flags(1) + Seq(4) = 9 bytes
	// With ACK: +4 bytes = 13 bytes
	// With SACK: +1 (count) + N*8 (blocks) bytes
	ReliableHeaderSize    = 9
	ReliableHeaderWithAck = 13
	MaxSackBlocks         = 4 // Maximum SACK blocks per packet

	// Protocol parameters
	DefaultWindowSize   = 256            // Maximum unacked packets
	DefaultRTOMs        = 150            // Initial retransmission timeout in ms
	MinRTOMs            = 30             // Minimum RTO
	MaxRTOMs            = 2000           // Maximum RTO
	MaxRetries          = 20             // Max retransmission attempts
	AckDelayMs          = 5              // Delay before sending pure ACK
	AckEveryN           = 4              // Send ACK every N packets (like TCP delayed ACK)
	MaxOutOfOrderBuffer = 512            // Max out-of-order packets to buffer
	CleanupIntervalMs   = 5000           // Stream cleanup interval
	FastRetransmitCount = 3              // Duplicate ACKs before fast retransmit

	// RTT estimation parameters (RFC 6298)
	RTTAlpha = 0.125 // SRTT smoothing factor
	RTTBeta  = 0.25  // RTTVAR smoothing factor
)

// SackBlock represents a range of received out-of-order packets
type SackBlock struct {
	Left  uint32 // First sequence in block
	Right uint32 // Last sequence in block (inclusive)
}

// ReliablePacket represents a reliable stream packet
type ReliablePacket struct {
	StreamID   uint32
	Flags      uint8
	Seq        uint32      // Sequence number
	Ack        uint32      // Acknowledgment number (if FlagAck set)
	SackBlocks []SackBlock // SACK blocks (if FlagSack set)
	Data       []byte
}

// HeaderSize returns the header size based on flags
func (p *ReliablePacket) HeaderSize() int {
	size := ReliableHeaderSize
	if p.Flags&FlagAck != 0 {
		size = ReliableHeaderWithAck
	}
	if p.Flags&FlagSack != 0 {
		size += 1 + len(p.SackBlocks)*8
	}
	return size
}

// Marshal serializes a reliable packet to bytes
func (p *ReliablePacket) Marshal() []byte {
	headerSize := p.HeaderSize()
	buf := make([]byte, headerSize+len(p.Data))
	p.MarshalTo(buf)
	return buf
}

// MarshalTo serializes into an existing buffer
func (p *ReliablePacket) MarshalTo(buf []byte) int {
	binary.BigEndian.PutUint32(buf[0:4], p.StreamID)
	buf[4] = p.Flags
	binary.BigEndian.PutUint32(buf[5:9], p.Seq)

	offset := 9
	if p.Flags&FlagAck != 0 {
		binary.BigEndian.PutUint32(buf[9:13], p.Ack)
		offset = 13
	}

	if p.Flags&FlagSack != 0 {
		buf[offset] = uint8(len(p.SackBlocks))
		offset++
		for _, block := range p.SackBlocks {
			binary.BigEndian.PutUint32(buf[offset:offset+4], block.Left)
			binary.BigEndian.PutUint32(buf[offset+4:offset+8], block.Right)
			offset += 8
		}
	}

	if len(p.Data) > 0 {
		copy(buf[offset:], p.Data)
	}
	return offset + len(p.Data)
}

// UnmarshalReliable deserializes a reliable packet from bytes
func UnmarshalReliable(data []byte) (*ReliablePacket, error) {
	if len(data) < ReliableHeaderSize {
		return nil, ErrStreamTooShort
	}

	p := &ReliablePacket{
		StreamID: binary.BigEndian.Uint32(data[0:4]),
		Flags:    data[4],
		Seq:      binary.BigEndian.Uint32(data[5:9]),
	}

	offset := 9
	if p.Flags&FlagAck != 0 {
		if len(data) < ReliableHeaderWithAck {
			return nil, ErrStreamTooShort
		}
		p.Ack = binary.BigEndian.Uint32(data[9:13])
		offset = 13
	}

	if p.Flags&FlagSack != 0 {
		if len(data) < offset+1 {
			return nil, ErrStreamTooShort
		}
		count := int(data[offset])
		offset++
		if count > MaxSackBlocks {
			count = MaxSackBlocks
		}
		if len(data) < offset+count*8 {
			return nil, ErrStreamTooShort
		}
		p.SackBlocks = make([]SackBlock, count)
		for i := 0; i < count; i++ {
			p.SackBlocks[i].Left = binary.BigEndian.Uint32(data[offset : offset+4])
			p.SackBlocks[i].Right = binary.BigEndian.Uint32(data[offset+4 : offset+8])
			offset += 8
		}
	}

	if len(data) > offset {
		p.Data = make([]byte, len(data)-offset)
		copy(p.Data, data[offset:])
	}

	return p, nil
}

// UnmarshalReliableNoCopy deserializes without copying data
func UnmarshalReliableNoCopy(data []byte) (*ReliablePacket, error) {
	if len(data) < ReliableHeaderSize {
		return nil, ErrStreamTooShort
	}

	p := &ReliablePacket{
		StreamID: binary.BigEndian.Uint32(data[0:4]),
		Flags:    data[4],
		Seq:      binary.BigEndian.Uint32(data[5:9]),
	}

	offset := 9
	if p.Flags&FlagAck != 0 {
		if len(data) < ReliableHeaderWithAck {
			return nil, ErrStreamTooShort
		}
		p.Ack = binary.BigEndian.Uint32(data[9:13])
		offset = 13
	}

	if p.Flags&FlagSack != 0 {
		if len(data) < offset+1 {
			return nil, ErrStreamTooShort
		}
		count := int(data[offset])
		offset++
		if count > MaxSackBlocks {
			count = MaxSackBlocks
		}
		if len(data) < offset+count*8 {
			return nil, ErrStreamTooShort
		}
		p.SackBlocks = make([]SackBlock, count)
		for i := 0; i < count; i++ {
			p.SackBlocks[i].Left = binary.BigEndian.Uint32(data[offset : offset+4])
			p.SackBlocks[i].Right = binary.BigEndian.Uint32(data[offset+4 : offset+8])
			offset += 8
		}
	}

	if len(data) > offset {
		p.Data = data[offset:]
	}

	return p, nil
}

// unackedPacket represents a packet waiting for ACK
type unackedPacket struct {
	pkt       *ReliablePacket
	data      []byte // Marshaled data for retransmission
	sentTime  time.Time
	retries   int
	sacked    bool // Marked by SACK, but not yet fully acked
}

// RTTEstimator estimates round-trip time using RFC 6298 algorithm
type RTTEstimator struct {
	srtt    float64 // Smoothed RTT
	rttvar  float64 // RTT variance
	rto     float64 // Retransmission timeout
	hasRTT  bool    // Whether we have an RTT sample
	mu      sync.Mutex
}

// NewRTTEstimator creates a new RTT estimator
func NewRTTEstimator() *RTTEstimator {
	return &RTTEstimator{
		rto: float64(DefaultRTOMs),
	}
}

// Update updates RTT estimate with a new sample (in milliseconds)
func (r *RTTEstimator) Update(rttMs float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.hasRTT {
		// First measurement
		r.srtt = rttMs
		r.rttvar = rttMs / 2
		r.hasRTT = true
	} else {
		// Subsequent measurements
		r.rttvar = (1-RTTBeta)*r.rttvar + RTTBeta*abs(r.srtt-rttMs)
		r.srtt = (1-RTTAlpha)*r.srtt + RTTAlpha*rttMs
	}

	// Calculate RTO: SRTT + 4*RTTVAR
	r.rto = r.srtt + 4*r.rttvar
	if r.rto < MinRTOMs {
		r.rto = MinRTOMs
	}
	if r.rto > MaxRTOMs {
		r.rto = MaxRTOMs
	}
}

// RTO returns the current retransmission timeout in milliseconds
func (r *RTTEstimator) RTO() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return int(r.rto)
}

// Backoff doubles the RTO (for retransmission)
func (r *RTTEstimator) Backoff() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rto *= 2
	if r.rto > MaxRTOMs {
		r.rto = MaxRTOMs
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// ReliableStream handles reliable delivery for a single stream
type ReliableStream struct {
	streamID uint32

	// Sender state
	sendSeq    atomic.Uint32          // Next sequence to send
	sendBase   atomic.Uint32          // Oldest unacked sequence
	sendWindow int                    // Window size
	unacked    map[uint32]*unackedPacket
	unackedMu  sync.Mutex
	dupAckCount int                   // Duplicate ACK counter for fast retransmit

	// Receiver state
	recvNext     atomic.Uint32        // Next expected sequence
	outOfOrder   map[uint32][]byte    // Out-of-order packets
	outOfOrderMu sync.Mutex

	// Callbacks
	sendFunc    func([]byte) error    // Function to send packets
	deliverFunc func([]byte)          // Function to deliver data to app

	// Control
	closed  atomic.Bool
	closeCh chan struct{}

	// RTT estimation
	rttEstimator *RTTEstimator

	// ACK coalescing
	pendingAck   atomic.Bool
	ackTimer     *time.Timer
	ackMu        sync.Mutex
	recvCounter  atomic.Uint32 // Counter for ACK every N packets

	// Stats
	retransmits atomic.Uint64
	packetsIn   atomic.Uint64
	packetsOut  atomic.Uint64
}

// NewReliableStream creates a new reliable stream
func NewReliableStream(streamID uint32, sendFunc func([]byte) error, deliverFunc func([]byte)) *ReliableStream {
	rs := &ReliableStream{
		streamID:     streamID,
		sendWindow:   DefaultWindowSize,
		unacked:      make(map[uint32]*unackedPacket),
		outOfOrder:   make(map[uint32][]byte),
		sendFunc:     sendFunc,
		deliverFunc:  deliverFunc,
		closeCh:      make(chan struct{}),
		rttEstimator: NewRTTEstimator(),
	}

	// Start retransmission timer
	go rs.retransmitLoop()

	return rs
}

// Send sends data reliably
func (rs *ReliableStream) Send(data []byte) error {
	if rs.closed.Load() {
		return ErrStreamClosed
	}

	// Get sequence number
	seq := rs.sendSeq.Add(1) - 1

	// Check window
	for seq-rs.sendBase.Load() >= uint32(rs.sendWindow) {
		// Window full, wait a bit
		select {
		case <-rs.closeCh:
			return ErrStreamClosed
		case <-time.After(10 * time.Millisecond):
		}
		if rs.closed.Load() {
			return ErrStreamClosed
		}
	}

	// Create packet
	pkt := &ReliablePacket{
		StreamID: rs.streamID,
		Flags:    FlagData,
		Seq:      seq,
		Data:     data,
	}

	// Piggyback ACK if we have pending acks
	if rs.pendingAck.Load() {
		pkt.Flags |= FlagAck
		pkt.Ack = rs.recvNext.Load()
		// Add SACK blocks if we have out-of-order packets
		sackBlocks := rs.buildSackBlocks()
		if len(sackBlocks) > 0 {
			pkt.Flags |= FlagSack
			pkt.SackBlocks = sackBlocks
		}
		rs.pendingAck.Store(false)
		rs.cancelAckTimer()
	}

	// Marshal
	pktData := pkt.Marshal()

	// Store for retransmission
	rs.unackedMu.Lock()
	rs.unacked[seq] = &unackedPacket{
		pkt:      pkt,
		data:     pktData,
		sentTime: time.Now(),
	}
	rs.unackedMu.Unlock()

	rs.packetsOut.Add(1)

	// Send
	return rs.sendFunc(pktData)
}

// SendClose sends a close packet
func (rs *ReliableStream) SendClose() error {
	if rs.closed.Load() {
		return nil
	}

	seq := rs.sendSeq.Add(1) - 1

	pkt := &ReliablePacket{
		StreamID: rs.streamID,
		Flags:    FlagClose | FlagAck,
		Seq:      seq,
		Ack:      rs.recvNext.Load(),
	}

	pktData := pkt.Marshal()

	// Store for retransmission
	rs.unackedMu.Lock()
	rs.unacked[seq] = &unackedPacket{
		pkt:      pkt,
		data:     pktData,
		sentTime: time.Now(),
	}
	rs.unackedMu.Unlock()

	rs.packetsOut.Add(1)

	return rs.sendFunc(pktData)
}

// Receive processes an incoming packet
func (rs *ReliableStream) Receive(pkt *ReliablePacket) (isClose bool) {
	rs.packetsIn.Add(1)

	// Process ACK
	if pkt.Flags&FlagAck != 0 {
		rs.processAck(pkt.Ack, pkt.SackBlocks)
	}

	// Process data
	if pkt.Flags&FlagData != 0 && len(pkt.Data) > 0 {
		rs.processData(pkt.Seq, pkt.Data)
	}

	// Check close
	if pkt.Flags&FlagClose != 0 {
		// Deliver any remaining out-of-order data
		rs.deliverOutOfOrder()
		return true
	}

	return false
}

// processAck handles acknowledgment with SACK support
func (rs *ReliableStream) processAck(ack uint32, sackBlocks []SackBlock) {
	rs.unackedMu.Lock()
	defer rs.unackedMu.Unlock()

	oldBase := rs.sendBase.Load()

	// Check for duplicate ACK (same ACK number, no new data acked)
	// ACK=N means "I expect seq=N", so duplicate ACK is when ack == oldBase
	if ack == oldBase && len(sackBlocks) == 0 {
		rs.dupAckCount++
		if rs.dupAckCount >= FastRetransmitCount {
			// Fast retransmit: retransmit the first unacked packet
			if up, ok := rs.unacked[oldBase]; ok && !up.sacked {
				rs.retransmits.Add(1)
				up.retries++
				up.sentTime = time.Now()
				rs.sendFunc(up.data)
			}
			rs.dupAckCount = 0
		}
		return
	}

	// New ACK, reset duplicate counter
	rs.dupAckCount = 0

	// Remove all packets before ack (cumulative ACK)
	// ACK=N means "I expect seq=N next", i.e., "I received 0 to N-1"
	for seq := range rs.unacked {
		if seqLess(seq, ack) {
			// Calculate RTT for packets that weren't retransmitted
			up := rs.unacked[seq]
			if up.retries == 0 {
				rtt := float64(time.Since(up.sentTime).Milliseconds())
				rs.rttEstimator.Update(rtt)
			}
			delete(rs.unacked, seq)
		}
	}

	// Process SACK blocks
	for _, block := range sackBlocks {
		for seq := block.Left; seqLessOrEqual(seq, block.Right); seq++ {
			if up, ok := rs.unacked[seq]; ok {
				up.sacked = true
			}
		}
	}

	// Update send base
	if seqGreater(ack, rs.sendBase.Load()) {
		rs.sendBase.Store(ack)
	}
}

// buildSackBlocks builds SACK blocks from out-of-order buffer
func (rs *ReliableStream) buildSackBlocks() []SackBlock {
	rs.outOfOrderMu.Lock()
	defer rs.outOfOrderMu.Unlock()

	if len(rs.outOfOrder) == 0 {
		return nil
	}

	// Collect and sort sequences
	seqs := make([]uint32, 0, len(rs.outOfOrder))
	for seq := range rs.outOfOrder {
		seqs = append(seqs, seq)
	}

	// Sort using insertion sort (small list)
	for i := 1; i < len(seqs); i++ {
		for j := i; j > 0 && seqLess(seqs[j], seqs[j-1]); j-- {
			seqs[j], seqs[j-1] = seqs[j-1], seqs[j]
		}
	}

	// Build contiguous blocks
	var blocks []SackBlock
	if len(seqs) > 0 {
		block := SackBlock{Left: seqs[0], Right: seqs[0]}
		for i := 1; i < len(seqs); i++ {
			if seqs[i] == block.Right+1 {
				block.Right = seqs[i]
			} else {
				blocks = append(blocks, block)
				if len(blocks) >= MaxSackBlocks {
					break
				}
				block = SackBlock{Left: seqs[i], Right: seqs[i]}
			}
		}
		if len(blocks) < MaxSackBlocks {
			blocks = append(blocks, block)
		}
	}

	return blocks
}

// processData handles incoming data
func (rs *ReliableStream) processData(seq uint32, data []byte) {
	expected := rs.recvNext.Load()

	if seq == expected {
		// In-order delivery
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)
		rs.deliverFunc(dataCopy)
		rs.recvNext.Add(1)

		// Check for buffered out-of-order packets
		rs.deliverOutOfOrder()

		// Schedule ACK
		rs.scheduleAck()

	} else if seqGreater(seq, expected) {
		// Out-of-order, buffer it
		rs.outOfOrderMu.Lock()
		if len(rs.outOfOrder) < MaxOutOfOrderBuffer {
			if _, exists := rs.outOfOrder[seq]; !exists {
				dataCopy := make([]byte, len(data))
				copy(dataCopy, data)
				rs.outOfOrder[seq] = dataCopy
			}
		}
		rs.outOfOrderMu.Unlock()

		// Send duplicate ACK with SACK immediately
		rs.sendAckWithSack()
	} else {
		// seq < expected: duplicate, send ACK anyway
		// This is important for recovery when original ACK was lost
		rs.sendAckNow()
	}
}

// deliverOutOfOrder delivers buffered out-of-order packets
func (rs *ReliableStream) deliverOutOfOrder() {
	rs.outOfOrderMu.Lock()
	defer rs.outOfOrderMu.Unlock()

	for {
		expected := rs.recvNext.Load()
		data, ok := rs.outOfOrder[expected]
		if !ok {
			break
		}
		delete(rs.outOfOrder, expected)
		rs.deliverFunc(data)
		rs.recvNext.Add(1)
	}
}

// scheduleAck schedules a delayed ACK or sends immediately every N packets
func (rs *ReliableStream) scheduleAck() {
	// Increment receive counter and check if we should ACK immediately
	count := rs.recvCounter.Add(1)
	if count%AckEveryN == 0 {
		// Send ACK immediately every N packets
		// sendAckNow will handle pendingAck and timer
		rs.sendAckNow()
		return
	}

	// Otherwise schedule delayed ACK
	if rs.pendingAck.Swap(true) {
		return // Already scheduled
	}

	rs.ackMu.Lock()
	defer rs.ackMu.Unlock()

	if rs.ackTimer != nil {
		rs.ackTimer.Stop()
	}

	rs.ackTimer = time.AfterFunc(time.Duration(AckDelayMs)*time.Millisecond, func() {
		if rs.pendingAck.Swap(false) {
			rs.sendAckNow()
		}
	})
}

// cancelAckTimer cancels pending ACK timer
func (rs *ReliableStream) cancelAckTimer() {
	rs.ackMu.Lock()
	defer rs.ackMu.Unlock()

	if rs.ackTimer != nil {
		rs.ackTimer.Stop()
		rs.ackTimer = nil
	}
}

// sendAckNow sends ACK immediately
func (rs *ReliableStream) sendAckNow() {
	rs.pendingAck.Store(false)
	rs.cancelAckTimer()

	pkt := &ReliablePacket{
		StreamID: rs.streamID,
		Flags:    FlagAck,
		Seq:      0, // Not used for pure ACK
		Ack:      rs.recvNext.Load(),
	}

	rs.sendFunc(pkt.Marshal())
}

// sendAckWithSack sends ACK with SACK blocks immediately
func (rs *ReliableStream) sendAckWithSack() {
	rs.pendingAck.Store(false)
	rs.cancelAckTimer()

	pkt := &ReliablePacket{
		StreamID: rs.streamID,
		Flags:    FlagAck,
		Seq:      0,
		Ack:      rs.recvNext.Load(),
	}

	sackBlocks := rs.buildSackBlocks()
	if len(sackBlocks) > 0 {
		pkt.Flags |= FlagSack
		pkt.SackBlocks = sackBlocks
	}

	rs.sendFunc(pkt.Marshal())
}

// retransmitLoop handles retransmissions
func (rs *ReliableStream) retransmitLoop() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rs.closeCh:
			return
		case <-ticker.C:
			rs.checkRetransmit()
		}
	}
}

// checkRetransmit checks and retransmits timed out packets
func (rs *ReliableStream) checkRetransmit() {
	now := time.Now()
	rto := time.Duration(rs.rttEstimator.RTO()) * time.Millisecond

	// Collect packets to retransmit while holding lock
	var toRetransmit [][]byte

	rs.unackedMu.Lock()
	for seq, up := range rs.unacked {
		// Skip SACK'd packets (they'll be cleaned up by cumulative ACK)
		if up.sacked {
			continue
		}

		if now.Sub(up.sentTime) > rto {
			if up.retries >= MaxRetries {
				// Too many retries, give up
				delete(rs.unacked, seq)
				continue
			}

			// Mark for retransmit
			rs.retransmits.Add(1)
			up.retries++
			up.sentTime = now
			toRetransmit = append(toRetransmit, up.data)
		}
	}

	// Backoff RTO if we have retransmissions
	if len(toRetransmit) > 0 {
		rs.rttEstimator.Backoff()
	}
	rs.unackedMu.Unlock()

	// Send retransmissions without holding lock
	for _, data := range toRetransmit {
		rs.sendFunc(data)
	}
}

// Close closes the reliable stream
func (rs *ReliableStream) Close() {
	if rs.closed.Swap(true) {
		return
	}
	close(rs.closeCh)
	rs.cancelAckTimer()
}

// GetStats returns stream statistics
func (rs *ReliableStream) GetStats() (unacked, outOfOrder int, retransmits uint64) {
	rs.unackedMu.Lock()
	unacked = len(rs.unacked)
	rs.unackedMu.Unlock()

	rs.outOfOrderMu.Lock()
	outOfOrder = len(rs.outOfOrder)
	rs.outOfOrderMu.Unlock()

	retransmits = rs.retransmits.Load()

	return
}

// Sequence number comparison (handles wraparound)
func seqLess(a, b uint32) bool {
	return int32(a-b) < 0
}

func seqLessOrEqual(a, b uint32) bool {
	return int32(a-b) <= 0
}

func seqGreater(a, b uint32) bool {
	return int32(a-b) > 0
}

// ReliableManager manages multiple reliable streams
type ReliableManager struct {
	streams   sync.Map // map[uint32]*ReliableStream
	sendFunc  func([]byte) error
	onData    func(streamID uint32, data []byte)
	onClose   func(streamID uint32)
	closed    atomic.Bool
}

// NewReliableManager creates a new reliable manager
func NewReliableManager(sendFunc func([]byte) error, onData func(streamID uint32, data []byte), onClose func(streamID uint32)) *ReliableManager {
	return &ReliableManager{
		sendFunc: sendFunc,
		onData:   onData,
		onClose:  onClose,
	}
}

// GetOrCreateStream gets or creates a reliable stream
func (rm *ReliableManager) GetOrCreateStream(streamID uint32) *ReliableStream {
	if rs, ok := rm.streams.Load(streamID); ok {
		return rs.(*ReliableStream)
	}

	// Create new stream
	rs := NewReliableStream(streamID, rm.sendFunc, func(data []byte) {
		rm.onData(streamID, data)
	})

	actual, loaded := rm.streams.LoadOrStore(streamID, rs)
	if loaded {
		// Another goroutine created it, close ours
		rs.Close()
		return actual.(*ReliableStream)
	}

	return rs
}

// GetStream gets an existing stream (returns nil if not found)
func (rm *ReliableManager) GetStream(streamID uint32) *ReliableStream {
	if rs, ok := rm.streams.Load(streamID); ok {
		return rs.(*ReliableStream)
	}
	return nil
}

// RemoveStream removes a stream
func (rm *ReliableManager) RemoveStream(streamID uint32) {
	if rs, ok := rm.streams.LoadAndDelete(streamID); ok {
		rs.(*ReliableStream).Close()
	}
}

// Receive processes an incoming reliable packet
func (rm *ReliableManager) Receive(data []byte) error {
	pkt, err := UnmarshalReliable(data)
	if err != nil {
		return err
	}

	rs := rm.GetOrCreateStream(pkt.StreamID)
	isClose := rs.Receive(pkt)

	if isClose {
		rm.onClose(pkt.StreamID)
		rm.RemoveStream(pkt.StreamID)
	}

	return nil
}

// Send sends data on a stream
func (rm *ReliableManager) Send(streamID uint32, data []byte) error {
	rs := rm.GetOrCreateStream(streamID)
	return rs.Send(data)
}

// SendClose sends a close packet on a stream
func (rm *ReliableManager) SendClose(streamID uint32) error {
	rs := rm.GetStream(streamID)
	if rs == nil {
		return nil
	}
	return rs.SendClose()
}

// Close closes all streams
func (rm *ReliableManager) Close() {
	if rm.closed.Swap(true) {
		return
	}

	rm.streams.Range(func(key, value interface{}) bool {
		value.(*ReliableStream).Close()
		return true
	})
}

// ErrStreamClosed indicates the stream is closed
var ErrStreamClosed = errStreamClosed{}

type errStreamClosed struct{}

func (errStreamClosed) Error() string { return "stream closed" }
