package tunnel

import (
	"fmt"
	"net"
	"sync"

	"gvisor.dev/gvisor/pkg/tcpip"
)

// IPAllocator allocates virtual IP addresses for TCP streams
// Uses private address space (10.0.0.0/8) to avoid conflicts
type IPAllocator struct {
	// Base network for allocation (e.g., 10.255.0.0/16 for client)
	baseIP net.IP
	prefix byte // Second octet: 255 for client, 254 for server

	// StreamID to IP mapping
	allocations map[uint32]tcpip.Address
	mu          sync.RWMutex
}

// NewIPAllocator creates a new IP allocator
// isClient: true for client (10.255.x.x), false for server (10.254.x.x)
func NewIPAllocator(isClient bool) *IPAllocator {
	prefix := byte(254) // server
	if isClient {
		prefix = 255 // client
	}

	return &IPAllocator{
		baseIP:      net.IPv4(10, prefix, 0, 0),
		prefix:      prefix,
		allocations: make(map[uint32]tcpip.Address),
	}
}

// AllocateIP allocates or returns existing IP for a stream
func (a *IPAllocator) AllocateIP(streamID uint32) tcpip.Address {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Check if already allocated
	if addr, exists := a.allocations[streamID]; exists {
		return addr
	}

	// StreamID to IP: 10.prefix.high_byte.low_byte
	// This allows up to 65536 streams per client/server
	highByte := byte(streamID >> 8)
	lowByte := byte(streamID & 0xFF)

	ip := tcpip.AddrFrom4([4]byte{10, a.prefix, highByte, lowByte})
	a.allocations[streamID] = ip

	return ip
}

// ReleaseIP releases an IP address
func (a *IPAllocator) ReleaseIP(streamID uint32) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.allocations, streamID)
}

// GetIP returns the IP for a streamID without allocating
func (a *IPAllocator) GetIP(streamID uint32) (tcpip.Address, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	addr, exists := a.allocations[streamID]
	return addr, exists
}

// IPToStreamID converts an IP address back to streamID
func (a *IPAllocator) IPToStreamID(addr tcpip.Address) (uint32, error) {
	if addr.Len() != 4 {
		return 0, fmt.Errorf("not an IPv4 address")
	}

	ip := addr.As4()
	if ip[0] != 10 || ip[1] != a.prefix {
		return 0, fmt.Errorf("IP not in allocation range")
	}

	streamID := uint32(ip[2])<<8 | uint32(ip[3])
	return streamID, nil
}

// Count returns the number of allocated IPs
func (a *IPAllocator) Count() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.allocations)
}
