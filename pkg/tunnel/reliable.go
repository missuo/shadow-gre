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

	// Congestion-control window (in packets)
	InitialCwnd     = 32    // Initial cwnd (slow start)
	MinCwnd         = 8     // Floor
	MaxCwnd         = 16384 // Hard cap
	DefaultSsthresh = 1024  // Slow-start threshold seed

	// Backwards compat (kept for tests / external readers)
	DefaultWindowSize = MaxCwnd

	DefaultRTOMs        = 1000  // Initial RTO before any RTT sample (RFC 6298)
	MinRTOMs            = 100   // Minimum RTO (avoid spurious retransmits)
	MaxRTOMs            = 4000  // Maximum RTO
	MaxRetries          = 20    // Max retransmission attempts
	AckDelayMs          = 5     // Delay before sending pure ACK
	AckEveryN           = 4     // Send ACK every N packets (like TCP delayed ACK)
	MaxOutOfOrderBuffer = 16384 // Max out-of-order packets to buffer
	CleanupIntervalMs   = 5000  // Stream cleanup interval
	FastRetransmitCount = 3     // Duplicate ACKs before fast retransmit

	// RTT estimation parameters (RFC 6298)
	RTTAlpha = 0.125 // SRTT smoothing factor
	RTTBeta  = 0.25  // RTTVAR smoothing factor

	// Pacing. We pace sends at cwnd/SRTT × gain. Disabled by default because
	// the OS sleep granularity (~100µs+) makes fine-grained pacing harmful at
	// any reasonable cwnd, and the cwnd window itself already rate-limits sends.
	// Set MinPacingIntervalNs lower to re-enable.
	PacingGain          = 1.0
	MinPacingIntervalNs = 1 << 62 // effectively disabled
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
	pkt      *ReliablePacket
	data     []byte // Marshaled data for retransmission
	sentTime time.Time
	retries  int
	sacked   bool // Marked by SACK, but not yet fully acked
}

// RTTEstimator estimates round-trip time using RFC 6298 algorithm
type RTTEstimator struct {
	srtt   float64 // Smoothed RTT
	rttvar float64 // RTT variance
	rto    float64 // Retransmission timeout
	hasRTT bool    // Whether we have an RTT sample
	mu     sync.Mutex
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

// SRTT returns the smoothed RTT in milliseconds (0 if no sample yet)
func (r *RTTEstimator) SRTT() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.hasRTT {
		return 0
	}
	return r.srtt
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
	sendSeq      atomic.Uint32 // Next sequence to send
	sendBase     atomic.Uint32 // Oldest unacked sequence
	unacked      map[uint32]*unackedPacket
	unackedMu    sync.Mutex
	dupAckCount  int       // Duplicate ACK counter for fast retransmit
	lastSackTime time.Time // Last SACK-based retransmit time (for rate limiting)

	// Congestion control (AIMD with slow start)
	cwnd             atomic.Int32 // current congestion window (packets)
	ssthresh         atomic.Int32 // slow-start threshold (packets)
	caAccum          atomic.Int32 // fractional accumulator for CA: cwnd += 1/cwnd per ACK
	lastLossReduceNs atomic.Int64 // unix-nanos of last cwnd reduction (for per-RTT rate-limit)

	// Window-available signaller. Buffered with capacity 1: ACK handler signals,
	// Send waits on it instead of polling.
	windowAvail chan struct{}

	// Pacing: earliest unix-nanos at which next Send may release a packet
	nextSendTimeNs atomic.Int64

	// Receiver state
	recvNext     atomic.Uint32     // Next expected sequence
	outOfOrder   map[uint32][]byte // Out-of-order packets
	outOfOrderMu sync.Mutex

	// Callbacks
	sendFunc    func([]byte) error // Function to send packets
	deliverFunc func([]byte)       // Function to deliver data to app

	// Control
	closed  atomic.Bool
	closeCh chan struct{}

	// RTT estimation
	rttEstimator *RTTEstimator

	// ACK coalescing
	pendingAck  atomic.Bool
	ackTimer    *time.Timer
	ackMu       sync.Mutex
	recvCounter atomic.Uint32 // Counter for ACK every N packets

	// Stats
	retransmits atomic.Uint64
	packetsIn   atomic.Uint64
	packetsOut  atomic.Uint64
}

// sendWindow returns current cwnd as int (compat with tests).
func (rs *ReliableStream) sendWindow() int {
	return int(rs.cwnd.Load())
}

// NewReliableStream creates a new reliable stream
func NewReliableStream(streamID uint32, sendFunc func([]byte) error, deliverFunc func([]byte)) *ReliableStream {
	rs := &ReliableStream{
		streamID:     streamID,
		unacked:      make(map[uint32]*unackedPacket),
		outOfOrder:   make(map[uint32][]byte),
		sendFunc:     sendFunc,
		deliverFunc:  deliverFunc,
		closeCh:      make(chan struct{}),
		rttEstimator: NewRTTEstimator(),
		windowAvail:  make(chan struct{}, 1),
	}
	rs.cwnd.Store(InitialCwnd)
	rs.ssthresh.Store(DefaultSsthresh)

	// Start retransmission timer
	go rs.retransmitLoop()

	return rs
}

// signalWindowAvail wakes any Send waiter (non-blocking).
func (rs *ReliableStream) signalWindowAvail() {
	select {
	case rs.windowAvail <- struct{}{}:
	default:
	}
}

// waitForWindow blocks until cwnd has space, the stream closes, or 100ms safety timeout.
func (rs *ReliableStream) waitForWindow(seq uint32) bool {
	for {
		if rs.closed.Load() {
			return false
		}
		cwnd := rs.cwnd.Load()
		// Signed diff handles the rare case where sendBase has already
		// advanced past seq (multi-writer racing with an in-flight ACK).
		inFlight := int32(seq - rs.sendBase.Load())
		if inFlight < cwnd {
			return true
		}
		// Drain any stale signal first, then wait for new one
		select {
		case <-rs.windowAvail:
			continue
		default:
		}
		select {
		case <-rs.windowAvail:
		case <-rs.closeCh:
			return false
		case <-time.After(100 * time.Millisecond):
			// Safety net: retransmitLoop may need to free packets via timeout
		}
	}
}

// pace blocks (if needed) so that consecutive sends respect cwnd/RTT pacing.
// Pacing rate = cwnd / SRTT * gain. Skipped when interval too small or SRTT unknown.
func (rs *ReliableStream) pace() {
	srttMs := rs.rttEstimator.SRTT()
	if srttMs <= 0 {
		return
	}
	cwnd := rs.cwnd.Load()
	if cwnd <= 0 {
		return
	}
	intervalNs := int64(srttMs * 1e6 / float64(cwnd) / PacingGain)
	if intervalNs < MinPacingIntervalNs {
		return
	}
	now := time.Now().UnixNano()
	for {
		next := rs.nextSendTimeNs.Load()
		var newNext int64
		if next <= now {
			newNext = now + intervalNs
		} else {
			newNext = next + intervalNs
		}
		if rs.nextSendTimeNs.CompareAndSwap(next, newNext) {
			if next > now {
				sleep := next - now
				if sleep > int64(50*time.Millisecond) {
					sleep = int64(50 * time.Millisecond)
				}
				time.Sleep(time.Duration(sleep))
			}
			return
		}
	}
}

// onAckAdvance updates cwnd when sendBase moves forward by pktsAcked packets.
// Implements slow-start (until ssthresh) then AIMD congestion-avoidance.
func (rs *ReliableStream) onAckAdvance(pktsAcked int32) {
	if pktsAcked <= 0 {
		return
	}
	cwnd := rs.cwnd.Load()
	ssthresh := rs.ssthresh.Load()
	if cwnd < ssthresh {
		// Slow start: cwnd += pktsAcked
		cwnd += pktsAcked
		if cwnd > MaxCwnd {
			cwnd = MaxCwnd
		}
		rs.cwnd.Store(cwnd)
	} else {
		// Congestion avoidance: add pktsAcked / cwnd, using fractional accumulator
		accum := rs.caAccum.Add(pktsAcked)
		if accum >= cwnd {
			inc := accum / cwnd
			rs.caAccum.Add(-inc * cwnd)
			cwnd += inc
			if cwnd > MaxCwnd {
				cwnd = MaxCwnd
			}
			rs.cwnd.Store(cwnd)
		}
	}
}

// onLossFast reduces cwnd on fast retransmit (loss inferred from SACK / dup-ACKs).
// Rate-limited so a single loss episode only reduces cwnd once.
func (rs *ReliableStream) onLossFast() {
	now := time.Now().UnixNano()
	last := rs.lastLossReduceNs.Load()
	srttMs := rs.rttEstimator.SRTT()
	if srttMs <= 0 {
		srttMs = float64(rs.rttEstimator.RTO())
	}
	if int64(now-last) < int64(srttMs*1e6) {
		return // already reduced within the last RTT
	}
	if !rs.lastLossReduceNs.CompareAndSwap(last, now) {
		return
	}
	cwnd := rs.cwnd.Load()
	newCwnd := cwnd / 2
	if newCwnd < MinCwnd {
		newCwnd = MinCwnd
	}
	rs.ssthresh.Store(newCwnd)
	rs.cwnd.Store(newCwnd)
	rs.caAccum.Store(0)
}

// onLossRTO reduces cwnd on RTO timeout. Halves cwnd (less catastrophic than
// dropping to MinCwnd), also rate-limited per RTT.
func (rs *ReliableStream) onLossRTO() {
	now := time.Now().UnixNano()
	last := rs.lastLossReduceNs.Load()
	srttMs := rs.rttEstimator.SRTT()
	if srttMs <= 0 {
		srttMs = float64(rs.rttEstimator.RTO())
	}
	if int64(now-last) < int64(srttMs*1e6) {
		return
	}
	if !rs.lastLossReduceNs.CompareAndSwap(last, now) {
		return
	}
	cwnd := rs.cwnd.Load()
	newSsthresh := cwnd / 2
	if newSsthresh < MinCwnd {
		newSsthresh = MinCwnd
	}
	rs.ssthresh.Store(newSsthresh)
	rs.cwnd.Store(newSsthresh)
	rs.caAccum.Store(0)
}

// Send sends data reliably
func (rs *ReliableStream) Send(data []byte) error {
	if rs.closed.Load() {
		return ErrStreamClosed
	}

	// Get sequence number
	seq := rs.sendSeq.Add(1) - 1

	// Wait for window space (signal-based)
	if !rs.waitForWindow(seq) {
		return ErrStreamClosed
	}

	// Pace
	rs.pace()

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

	// Marshal once; the buffer is held for retransmission.
	buf := pkt.Marshal()

	// Store for retransmission
	rs.unackedMu.Lock()
	rs.unacked[seq] = &unackedPacket{
		pkt:      pkt,
		data:     buf,
		sentTime: time.Now(),
	}
	rs.unackedMu.Unlock()

	rs.packetsOut.Add(1)

	// Send
	return rs.sendFunc(buf)
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

// markSackedAndCollectGaps marks unacked packets that fall in SACK blocks and
// returns a list of un-SACKed packets that should be considered lost
// (i.e. packets older than the highest SACKed seq).
//
// Iterates the seq range [ack, maxRight] looking up the unacked map — O(span)
// rather than O(total-unacked).
//
// Must be called with unackedMu held.
func (rs *ReliableStream) markSackedAndCollectGaps(ack uint32, sackBlocks []SackBlock) []*unackedPacket {
	if len(sackBlocks) == 0 {
		return nil
	}
	// Find max sacked right edge
	maxRight := sackBlocks[0].Right
	for _, b := range sackBlocks[1:] {
		if seqGreater(b.Right, maxRight) {
			maxRight = b.Right
		}
	}
	// Bound the scan defensively.
	span := int32(maxRight - ack + 1)
	if span <= 0 || span > MaxCwnd*2 {
		span = MaxCwnd * 2
	}
	var lost []*unackedPacket
	for i := int32(0); i < span; i++ {
		seq := ack + uint32(i)
		up, ok := rs.unacked[seq]
		if !ok || up.sacked {
			continue
		}
		// In any SACK block?
		inSack := false
		for _, b := range sackBlocks {
			if !seqLess(seq, b.Left) && seqLessOrEqual(seq, b.Right) {
				inSack = true
				break
			}
		}
		if inSack {
			up.sacked = true
			continue
		}
		// Older than highest SACKed seq → loss candidate.
		if seqLess(seq, maxRight) {
			lost = append(lost, up)
		}
	}
	return lost
}

// processAck handles acknowledgment with SACK support
func (rs *ReliableStream) processAck(ack uint32, sackBlocks []SackBlock) {
	rs.unackedMu.Lock()

	oldBase := rs.sendBase.Load()
	now := time.Now()

	// Duplicate-ACK path: ack == oldBase (no new cumulative progress)
	if ack == oldBase {
		rs.dupAckCount++

		// Mark SACKed packets and collect loss candidates.
		lost := rs.markSackedAndCollectGaps(ack, sackBlocks)

		shouldRetransmit := false
		if len(sackBlocks) > 0 {
			// SACK signals a gap. Rate-limit to half-RTT to avoid duplicate work.
			minInterval := time.Duration(rs.rttEstimator.RTO()/2) * time.Millisecond
			if minInterval < 10*time.Millisecond {
				minInterval = 10 * time.Millisecond
			}
			if now.Sub(rs.lastSackTime) >= minInterval {
				shouldRetransmit = true
				rs.lastSackTime = now
			}
		} else if rs.dupAckCount >= FastRetransmitCount {
			shouldRetransmit = true
			rs.dupAckCount = 0
		}

		if shouldRetransmit {
			var toRetransmit [][]byte
			if len(sackBlocks) > 0 {
				rs.onLossFast() // reduce cwnd (only once per loss event)
				for _, up := range lost {
					rs.retransmits.Add(1)
					up.retries++
					up.sentTime = now
					toRetransmit = append(toRetransmit, up.data)
				}
			} else {
				// Classic fast retransmit: just the first unacked packet
				if up, ok := rs.unacked[oldBase]; ok && !up.sacked {
					rs.onLossFast()
					rs.retransmits.Add(1)
					up.retries++
					up.sentTime = now
					toRetransmit = append(toRetransmit, up.data)
				}
			}
			rs.unackedMu.Unlock()
			for _, data := range toRetransmit {
				rs.sendFunc(data)
			}
			return
		}

		rs.unackedMu.Unlock()
		return
	}

	// New cumulative ACK
	rs.dupAckCount = 0

	// Count and remove packets covered by cumulative ACK. Iterate the seq
	// range (oldBase..ack-1) instead of the whole map — O(newly-acked) not
	// O(total-unacked). The map is sparse only when packets are SACKed without
	// cumulative coverage; lookups handle that case.
	pktsAcked := int32(0)
	// Bound the loop defensively so a malicious peer can't make us spin 2^32 times.
	span := int32(ack - oldBase)
	if span < 0 || span > MaxCwnd*2 {
		span = MaxCwnd * 2
	}
	for i := int32(0); i < span; i++ {
		seq := oldBase + uint32(i)
		up, ok := rs.unacked[seq]
		if !ok {
			continue
		}
		if up.retries == 0 {
			rtt := float64(time.Since(up.sentTime).Milliseconds())
			rs.rttEstimator.Update(rtt)
		}
		delete(rs.unacked, seq)
		pktsAcked++
	}

	// Mark SACKed + identify gap losses (rate-limited).
	var toRetransmit [][]byte
	if len(sackBlocks) > 0 {
		lost := rs.markSackedAndCollectGaps(ack, sackBlocks)
		minInterval := time.Duration(rs.rttEstimator.RTO()/2) * time.Millisecond
		if minInterval < 10*time.Millisecond {
			minInterval = 10 * time.Millisecond
		}
		if now.Sub(rs.lastSackTime) >= minInterval && len(lost) > 0 {
			rs.lastSackTime = now
			rs.onLossFast()
			for _, up := range lost {
				rs.retransmits.Add(1)
				up.retries++
				up.sentTime = now
				toRetransmit = append(toRetransmit, up.data)
			}
		}
	}

	// Advance send base, grow cwnd, wake any Send waiter
	if seqGreater(ack, rs.sendBase.Load()) {
		rs.sendBase.Store(ack)
	}
	if pktsAcked > 0 {
		rs.onAckAdvance(pktsAcked)
	}

	rs.unackedMu.Unlock()

	if pktsAcked > 0 {
		rs.signalWindowAvail()
	}

	for _, data := range toRetransmit {
		rs.sendFunc(data)
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

	// Insertion sort with wrap-aware comparison
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
		// Note: when buffer is full we still send SACK below, so the sender
		// knows the current state and won't stall waiting for these gaps.
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

	// On RTO retransmissions, drop cwnd hard and back off RTO.
	if len(toRetransmit) > 0 {
		rs.onLossRTO()
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
	// Wake any Send waiter
	rs.signalWindowAvail()
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
	streams  sync.Map // map[uint32]*ReliableStream
	sendFunc func([]byte) error
	onData   func(streamID uint32, data []byte)
	onClose  func(streamID uint32)
	closed   atomic.Bool
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
