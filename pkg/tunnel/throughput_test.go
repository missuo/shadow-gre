package tunnel

import (
	"crypto/rand"
	mrand "math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// wire is a duplex simulated link between two ReliableManagers. Each direction
// is handled by a single goroutine that delivers in FIFO order after a
// per-packet propagation delay (with optional jitter and random loss). This
// matches how a real network link serializes packets on the wire.
type wire struct {
	delay   time.Duration
	jitter  time.Duration
	lossPct int

	closed atomic.Bool
	wg     sync.WaitGroup

	aToB chan []byte
	bToA chan []byte
}

func newWire(delay, jitter time.Duration, lossPct int, queueDepth int) *wire {
	return &wire{
		delay:   delay,
		jitter:  jitter,
		lossPct: lossPct,
		aToB:    make(chan []byte, queueDepth),
		bToA:    make(chan []byte, queueDepth),
	}
}

func (w *wire) start(deliverToB, deliverToA func([]byte)) {
	w.wg.Add(2)
	go w.run(w.aToB, deliverToB)
	go w.run(w.bToA, deliverToA)
}

// run delivers packets from the input channel. Each packet has its own
// "exit time" = enqueue_time + delay; the dequeuer waits until each exit
// time and then delivers. This correctly models propagation: many packets
// can be in flight in parallel, but they're delivered serially as their
// deadlines pass.
func (w *wire) run(in <-chan []byte, deliver func([]byte)) {
	defer w.wg.Done()

	type item struct {
		deadline time.Time
		data     []byte
	}
	queue := make(chan item, cap(in))

	go func() {
		defer close(queue)
		rng := mrand.New(mrand.NewSource(time.Now().UnixNano()))
		for data := range in {
			if w.closed.Load() {
				return
			}
			if w.lossPct > 0 && rng.Intn(100) < w.lossPct {
				continue
			}
			d := w.delay
			if w.jitter > 0 {
				d += time.Duration(rng.Int63n(int64(w.jitter)))
			}
			select {
			case queue <- item{deadline: time.Now().Add(d), data: data}:
			default:
				// drop on queue overflow
			}
		}
	}()

	for it := range queue {
		if w.closed.Load() {
			return
		}
		wait := time.Until(it.deadline)
		if wait > 0 {
			time.Sleep(wait)
		}
		if w.closed.Load() {
			return
		}
		deliver(it.data)
	}
}

func (w *wire) sendAToB(data []byte) error {
	if w.closed.Load() {
		return nil
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	select {
	case w.aToB <- cp:
	default:
		// Drop on overflow (simulates link buffer overflow)
	}
	return nil
}

func (w *wire) sendBToA(data []byte) error {
	if w.closed.Load() {
		return nil
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	select {
	case w.bToA <- cp:
	default:
	}
	return nil
}

func (w *wire) close() {
	if w.closed.Swap(true) {
		return
	}
	close(w.aToB)
	close(w.bToA)
	w.wg.Wait()
}

// throughputScenario runs a single-stream throughput test through the wire.
// Returns megabytes per second achieved.
func throughputScenario(t testing.TB, totalBytes int, mss int, delay, jitter time.Duration, lossPct int) (float64, uint64) {
	w := newWire(delay, jitter, lossPct, 65536)
	defer w.close()

	var received atomic.Int64
	done := make(chan struct{})

	managerB := NewReliableManager(
		w.sendBToA,
		func(streamID uint32, data []byte) {
			n := received.Add(int64(len(data)))
			if n >= int64(totalBytes) {
				select {
				case <-done:
				default:
					close(done)
				}
			}
		},
		func(streamID uint32) {},
	)

	managerA := NewReliableManager(
		w.sendAToB,
		func(streamID uint32, data []byte) {},
		func(streamID uint32) {},
	)

	w.start(
		func(data []byte) { managerB.Receive(data) },
		func(data []byte) { managerA.Receive(data) },
	)

	chunk := make([]byte, mss)
	rand.Read(chunk)

	start := time.Now()
	go func() {
		written := 0
		for written < totalBytes {
			n := mss
			if written+n > totalBytes {
				n = totalBytes - written
			}
			if err := managerA.Send(1, chunk[:n]); err != nil {
				return
			}
			written += n
		}
	}()

	timeout := time.NewTimer(60 * time.Second)
	defer timeout.Stop()

	select {
	case <-done:
	case <-timeout.C:
		t.Logf("timeout: received %d / %d bytes", received.Load(), totalBytes)
	}
	elapsed := time.Since(start)

	// Collect stats before close
	var retrans uint64
	if rs := managerA.GetStream(1); rs != nil {
		_, _, retrans = rs.GetStats()
	}

	managerA.Close()
	managerB.Close()

	mbPerSec := float64(received.Load()) / elapsed.Seconds() / (1024 * 1024)
	return mbPerSec, retrans
}

func TestThroughputLoopback(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	cases := []struct {
		name    string
		bytes   int
		delay   time.Duration
		jitter  time.Duration
		lossPct int
	}{
		{"loopback-0ms", 16 * 1024 * 1024, 0, 0, 0},
		{"rtt-20ms-noloss", 16 * 1024 * 1024, 10 * time.Millisecond, 0, 0},
		{"rtt-100ms-noloss", 8 * 1024 * 1024, 50 * time.Millisecond, 0, 0},
		{"rtt-200ms-noloss", 8 * 1024 * 1024, 100 * time.Millisecond, 0, 0},
		{"rtt-100ms-1pct-loss", 4 * 1024 * 1024, 50 * time.Millisecond, 5 * time.Millisecond, 1},
		{"rtt-200ms-2pct-loss", 4 * 1024 * 1024, 100 * time.Millisecond, 10 * time.Millisecond, 2},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			mbps, retx := throughputScenario(t, c.bytes, 1400, c.delay, c.jitter, c.lossPct)
			t.Logf("%-25s throughput = %7.2f MB/s (%7.2f Mbps), retransmits=%d",
				c.name, mbps, mbps*8, retx)
		})
	}
}
