package tunnel

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestReliablePacketMarshalUnmarshal(t *testing.T) {
	tests := []struct {
		name string
		pkt  ReliablePacket
	}{
		{
			name: "data only",
			pkt: ReliablePacket{
				StreamID: 1,
				Flags:    FlagData,
				Seq:      100,
				Data:     []byte("hello world"),
			},
		},
		{
			name: "data with ack",
			pkt: ReliablePacket{
				StreamID: 2,
				Flags:    FlagData | FlagAck,
				Seq:      50,
				Ack:      49,
				Data:     []byte("test data"),
			},
		},
		{
			name: "ack only",
			pkt: ReliablePacket{
				StreamID: 3,
				Flags:    FlagAck,
				Seq:      0,
				Ack:      100,
			},
		},
		{
			name: "ack with sack",
			pkt: ReliablePacket{
				StreamID: 4,
				Flags:    FlagAck | FlagSack,
				Seq:      0,
				Ack:      50,
				SackBlocks: []SackBlock{
					{Left: 55, Right: 60},
					{Left: 70, Right: 75},
				},
			},
		},
		{
			name: "close with ack",
			pkt: ReliablePacket{
				StreamID: 5,
				Flags:    FlagClose | FlagAck,
				Seq:      200,
				Ack:      199,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := tt.pkt.Marshal()
			parsed, err := UnmarshalReliable(data)
			if err != nil {
				t.Fatalf("UnmarshalReliable failed: %v", err)
			}

			if parsed.StreamID != tt.pkt.StreamID {
				t.Errorf("StreamID mismatch: got %d, want %d", parsed.StreamID, tt.pkt.StreamID)
			}
			if parsed.Flags != tt.pkt.Flags {
				t.Errorf("Flags mismatch: got %d, want %d", parsed.Flags, tt.pkt.Flags)
			}
			if parsed.Seq != tt.pkt.Seq {
				t.Errorf("Seq mismatch: got %d, want %d", parsed.Seq, tt.pkt.Seq)
			}
			if parsed.Ack != tt.pkt.Ack {
				t.Errorf("Ack mismatch: got %d, want %d", parsed.Ack, tt.pkt.Ack)
			}
			if !bytes.Equal(parsed.Data, tt.pkt.Data) {
				t.Errorf("Data mismatch: got %v, want %v", parsed.Data, tt.pkt.Data)
			}
			if len(parsed.SackBlocks) != len(tt.pkt.SackBlocks) {
				t.Errorf("SackBlocks length mismatch: got %d, want %d", len(parsed.SackBlocks), len(tt.pkt.SackBlocks))
			}
			for i, block := range tt.pkt.SackBlocks {
				if i < len(parsed.SackBlocks) {
					if parsed.SackBlocks[i].Left != block.Left || parsed.SackBlocks[i].Right != block.Right {
						t.Errorf("SackBlock[%d] mismatch: got %v, want %v", i, parsed.SackBlocks[i], block)
					}
				}
			}
		})
	}
}

func TestSequenceComparison(t *testing.T) {
	tests := []struct {
		a, b       uint32
		less       bool
		lessOrEq   bool
		greater    bool
	}{
		{0, 1, true, true, false},
		{1, 0, false, false, true},
		{0, 0, false, true, false},
		{0xFFFFFFFF, 0, true, true, false},  // wraparound: FFFFFFFF is "before" 0
		{0, 0xFFFFFFFF, false, false, true}, // wraparound: 0 is "after" FFFFFFFF
		{0x7FFFFFFF, 0, false, false, true}, // large positive difference
		{0, 0x7FFFFFFF, true, true, false},  // large negative difference
		{100, 200, true, true, false},
	}

	for _, tt := range tests {
		if seqLess(tt.a, tt.b) != tt.less {
			t.Errorf("seqLess(%d, %d) = %v, want %v", tt.a, tt.b, seqLess(tt.a, tt.b), tt.less)
		}
		if seqLessOrEqual(tt.a, tt.b) != tt.lessOrEq {
			t.Errorf("seqLessOrEqual(%d, %d) = %v, want %v", tt.a, tt.b, seqLessOrEqual(tt.a, tt.b), tt.lessOrEq)
		}
		if seqGreater(tt.a, tt.b) != tt.greater {
			t.Errorf("seqGreater(%d, %d) = %v, want %v", tt.a, tt.b, seqGreater(tt.a, tt.b), tt.greater)
		}
	}
}

func TestRTTEstimator(t *testing.T) {
	r := NewRTTEstimator()

	// Initial RTO should be default
	if r.RTO() != DefaultRTOMs {
		t.Errorf("Initial RTO = %d, want %d", r.RTO(), DefaultRTOMs)
	}

	// Update with some RTT samples
	r.Update(50)
	if r.RTO() < 50 {
		t.Errorf("RTO after first sample should be >= 50, got %d", r.RTO())
	}

	// Update with more samples
	for i := 0; i < 10; i++ {
		r.Update(100)
	}

	// RTO should be around 100 + variance
	rto := r.RTO()
	if rto < 100 || rto > 300 {
		t.Errorf("RTO after samples should be in range [100, 300], got %d", rto)
	}

	// Test backoff
	oldRTO := r.RTO()
	r.Backoff()
	if r.RTO() != oldRTO*2 && r.RTO() != MaxRTOMs {
		t.Errorf("RTO after backoff should be doubled, got %d, want %d or %d", r.RTO(), oldRTO*2, MaxRTOMs)
	}
}

func TestReliableStreamBasic(t *testing.T) {
	var sendBuf bytes.Buffer
	var sendMu sync.Mutex
	var delivered [][]byte
	var deliverMu sync.Mutex

	sendFunc := func(data []byte) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		sendBuf.Write(data)
		return nil
	}

	deliverFunc := func(data []byte) {
		deliverMu.Lock()
		defer deliverMu.Unlock()
		cp := make([]byte, len(data))
		copy(cp, data)
		delivered = append(delivered, cp)
	}

	rs := NewReliableStream(1, sendFunc, deliverFunc)
	defer rs.Close()

	// Send some data
	err := rs.Send([]byte("hello"))
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	err = rs.Send([]byte("world"))
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Verify packets were sent
	sendMu.Lock()
	if sendBuf.Len() == 0 {
		t.Error("No data was sent")
	}
	sendMu.Unlock()

	// Simulate receiving data
	pkt := &ReliablePacket{
		StreamID: 1,
		Flags:    FlagData,
		Seq:      0,
		Data:     []byte("response"),
	}
	rs.Receive(pkt)

	// Wait for delivery
	time.Sleep(50 * time.Millisecond)

	deliverMu.Lock()
	if len(delivered) != 1 {
		t.Errorf("Expected 1 delivered packet, got %d", len(delivered))
	}
	if len(delivered) > 0 && !bytes.Equal(delivered[0], []byte("response")) {
		t.Errorf("Delivered data mismatch: got %v", delivered[0])
	}
	deliverMu.Unlock()
}

func TestReliableStreamOutOfOrder(t *testing.T) {
	var delivered [][]byte
	var deliverMu sync.Mutex

	sendFunc := func(data []byte) error {
		return nil
	}

	deliverFunc := func(data []byte) {
		deliverMu.Lock()
		defer deliverMu.Unlock()
		cp := make([]byte, len(data))
		copy(cp, data)
		delivered = append(delivered, cp)
	}

	rs := NewReliableStream(1, sendFunc, deliverFunc)
	defer rs.Close()

	// Receive packets out of order: 2, 0, 1
	pkt2 := &ReliablePacket{StreamID: 1, Flags: FlagData, Seq: 2, Data: []byte("pkt2")}
	rs.Receive(pkt2)

	// Only out-of-order buffered, not delivered
	deliverMu.Lock()
	if len(delivered) != 0 {
		t.Errorf("Expected 0 delivered packets after out-of-order, got %d", len(delivered))
	}
	deliverMu.Unlock()

	// Receive packet 0 (expected)
	pkt0 := &ReliablePacket{StreamID: 1, Flags: FlagData, Seq: 0, Data: []byte("pkt0")}
	rs.Receive(pkt0)

	// Wait for ACK timer
	time.Sleep(50 * time.Millisecond)

	deliverMu.Lock()
	if len(delivered) != 1 {
		t.Errorf("Expected 1 delivered packet after pkt0, got %d", len(delivered))
	}
	deliverMu.Unlock()

	// Receive packet 1
	pkt1 := &ReliablePacket{StreamID: 1, Flags: FlagData, Seq: 1, Data: []byte("pkt1")}
	rs.Receive(pkt1)

	// Wait for delivery
	time.Sleep(50 * time.Millisecond)

	deliverMu.Lock()
	// Now all 3 should be delivered in order
	if len(delivered) != 3 {
		t.Errorf("Expected 3 delivered packets, got %d", len(delivered))
	}
	if len(delivered) >= 3 {
		if !bytes.Equal(delivered[0], []byte("pkt0")) {
			t.Errorf("First packet mismatch: got %s", delivered[0])
		}
		if !bytes.Equal(delivered[1], []byte("pkt1")) {
			t.Errorf("Second packet mismatch: got %s", delivered[1])
		}
		if !bytes.Equal(delivered[2], []byte("pkt2")) {
			t.Errorf("Third packet mismatch: got %s", delivered[2])
		}
	}
	deliverMu.Unlock()
}

func TestReliableManagerBasic(t *testing.T) {
	var sentPackets [][]byte
	var sentMu sync.Mutex
	var dataReceived []struct {
		streamID uint32
		data     []byte
	}
	var dataMu sync.Mutex
	var closedStreams []uint32
	var closeMu sync.Mutex

	sendFunc := func(data []byte) error {
		sentMu.Lock()
		defer sentMu.Unlock()
		cp := make([]byte, len(data))
		copy(cp, data)
		sentPackets = append(sentPackets, cp)
		return nil
	}

	onData := func(streamID uint32, data []byte) {
		dataMu.Lock()
		defer dataMu.Unlock()
		cp := make([]byte, len(data))
		copy(cp, data)
		dataReceived = append(dataReceived, struct {
			streamID uint32
			data     []byte
		}{streamID, cp})
	}

	onClose := func(streamID uint32) {
		closeMu.Lock()
		defer closeMu.Unlock()
		closedStreams = append(closedStreams, streamID)
	}

	rm := NewReliableManager(sendFunc, onData, onClose)
	defer rm.Close()

	// Send data on stream 1
	err := rm.Send(1, []byte("stream1-data"))
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Send data on stream 2
	err = rm.Send(2, []byte("stream2-data"))
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Verify packets were sent
	sentMu.Lock()
	if len(sentPackets) != 2 {
		t.Errorf("Expected 2 sent packets, got %d", len(sentPackets))
	}
	sentMu.Unlock()

	// Simulate receiving data on stream 1
	inPkt := &ReliablePacket{
		StreamID: 1,
		Flags:    FlagData,
		Seq:      0,
		Data:     []byte("response1"),
	}
	rm.Receive(inPkt.Marshal())

	// Wait for delivery
	time.Sleep(50 * time.Millisecond)

	dataMu.Lock()
	if len(dataReceived) != 1 {
		t.Errorf("Expected 1 data received, got %d", len(dataReceived))
	}
	if len(dataReceived) > 0 {
		if dataReceived[0].streamID != 1 {
			t.Errorf("Expected stream 1, got %d", dataReceived[0].streamID)
		}
		if !bytes.Equal(dataReceived[0].data, []byte("response1")) {
			t.Errorf("Data mismatch: got %s", dataReceived[0].data)
		}
	}
	dataMu.Unlock()

	// Send close on stream 1
	closePkt := &ReliablePacket{
		StreamID: 1,
		Flags:    FlagClose | FlagAck,
		Seq:      1,
		Ack:      0,
	}
	rm.Receive(closePkt.Marshal())

	closeMu.Lock()
	if len(closedStreams) != 1 || closedStreams[0] != 1 {
		t.Errorf("Expected stream 1 closed, got %v", closedStreams)
	}
	closeMu.Unlock()
}

func TestReliableStreamRetransmission(t *testing.T) {
	var sendCount atomic.Int32

	sendFunc := func(data []byte) error {
		sendCount.Add(1)
		return nil
	}

	deliverFunc := func(data []byte) {}

	rs := NewReliableStream(1, sendFunc, deliverFunc)
	defer rs.Close()

	// Lower RTO for faster test
	rs.rttEstimator.mu.Lock()
	rs.rttEstimator.rto = 100 // 100ms
	rs.rttEstimator.mu.Unlock()

	// Send data (will be stored for retransmission)
	err := rs.Send([]byte("test"))
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	initialCount := sendCount.Load()

	// Wait for retransmission
	time.Sleep(300 * time.Millisecond)

	// Should have retransmitted at least once
	if sendCount.Load() <= initialCount {
		t.Error("Expected retransmission, but none occurred")
	}

	// Simulate ACK
	ackPkt := &ReliablePacket{
		StreamID: 1,
		Flags:    FlagAck,
		Ack:      0, // ACK seq 0
	}
	rs.Receive(ackPkt)

	// Wait a bit
	time.Sleep(200 * time.Millisecond)

	// Should stop retransmitting after ACK
	countAfterAck := sendCount.Load()
	time.Sleep(200 * time.Millisecond)

	// No more retransmissions
	if sendCount.Load() > countAfterAck+1 { // Allow for some timing variance
		t.Error("Should stop retransmitting after ACK")
	}
}

func TestBuildSackBlocks(t *testing.T) {
	sendFunc := func(data []byte) error { return nil }
	deliverFunc := func(data []byte) {}

	rs := NewReliableStream(1, sendFunc, deliverFunc)
	defer rs.Close()

	// Manually add out-of-order packets
	rs.outOfOrder[5] = []byte("5")
	rs.outOfOrder[6] = []byte("6")
	rs.outOfOrder[7] = []byte("7")
	rs.outOfOrder[10] = []byte("10")
	rs.outOfOrder[11] = []byte("11")
	rs.outOfOrder[15] = []byte("15")

	blocks := rs.buildSackBlocks()

	if len(blocks) != 3 {
		t.Fatalf("Expected 3 SACK blocks, got %d", len(blocks))
	}

	// Should be: [5,7], [10,11], [15,15]
	expected := []SackBlock{
		{Left: 5, Right: 7},
		{Left: 10, Right: 11},
		{Left: 15, Right: 15},
	}

	for i, block := range blocks {
		if block.Left != expected[i].Left || block.Right != expected[i].Right {
			t.Errorf("Block %d: got [%d,%d], want [%d,%d]",
				i, block.Left, block.Right, expected[i].Left, expected[i].Right)
		}
	}
}
