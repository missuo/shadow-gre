// Package gre implements GRE (Generic Routing Encapsulation) protocol handling.
// RFC 2784 - Generic Routing Encapsulation (GRE)
// RFC 2890 - Key and Sequence Number Extensions to GRE
package gre

import (
	"encoding/binary"
	"errors"
	"net"
)

const (
	// ProtocolNumber is the IP protocol number for GRE
	ProtocolNumber = 47

	// MinHeaderSize is the minimum GRE header size (4 bytes)
	MinHeaderSize = 4

	// EtherTypeTransparentEthernet for our custom payload
	EtherTypeTransparentEthernet uint16 = 0x6558

	// Flag bits
	FlagChecksum uint16 = 0x8000
	FlagKey      uint16 = 0x2000
	FlagSequence uint16 = 0x1000
)

var (
	ErrPacketTooShort  = errors.New("packet too short")
	ErrInvalidChecksum = errors.New("invalid checksum")
	ErrInvalidVersion  = errors.New("invalid GRE version")
)

// Header represents a GRE header
type Header struct {
	Flags        uint16
	ProtocolType uint16
	Checksum     uint16 // Optional, present if FlagChecksum is set
	Reserved1    uint16 // Optional, present if FlagChecksum is set
	Key          uint32 // Optional, present if FlagKey is set
	Sequence     uint32 // Optional, present if FlagSequence is set
}

// HeaderSize returns the size of the GRE header based on flags
func (h *Header) HeaderSize() int {
	size := MinHeaderSize
	if h.Flags&FlagChecksum != 0 {
		size += 4
	}
	if h.Flags&FlagKey != 0 {
		size += 4
	}
	if h.Flags&FlagSequence != 0 {
		size += 4
	}
	return size
}

// Marshal serializes the GRE header to bytes
func (h *Header) Marshal() []byte {
	buf := make([]byte, h.HeaderSize())
	binary.BigEndian.PutUint16(buf[0:2], h.Flags)
	binary.BigEndian.PutUint16(buf[2:4], h.ProtocolType)

	offset := 4
	if h.Flags&FlagChecksum != 0 {
		binary.BigEndian.PutUint16(buf[offset:offset+2], h.Checksum)
		binary.BigEndian.PutUint16(buf[offset+2:offset+4], h.Reserved1)
		offset += 4
	}
	if h.Flags&FlagKey != 0 {
		binary.BigEndian.PutUint32(buf[offset:offset+4], h.Key)
		offset += 4
	}
	if h.Flags&FlagSequence != 0 {
		binary.BigEndian.PutUint32(buf[offset:offset+4], h.Sequence)
	}
	return buf
}

// Unmarshal deserializes a GRE header from bytes
func Unmarshal(data []byte) (*Header, int, error) {
	if len(data) < MinHeaderSize {
		return nil, 0, ErrPacketTooShort
	}

	h := &Header{
		Flags:        binary.BigEndian.Uint16(data[0:2]),
		ProtocolType: binary.BigEndian.Uint16(data[2:4]),
	}

	// Check version (must be 0)
	if h.Flags&0x0007 != 0 {
		return nil, 0, ErrInvalidVersion
	}

	offset := 4
	if h.Flags&FlagChecksum != 0 {
		if len(data) < offset+4 {
			return nil, 0, ErrPacketTooShort
		}
		h.Checksum = binary.BigEndian.Uint16(data[offset : offset+2])
		h.Reserved1 = binary.BigEndian.Uint16(data[offset+2 : offset+4])
		offset += 4
	}
	if h.Flags&FlagKey != 0 {
		if len(data) < offset+4 {
			return nil, 0, ErrPacketTooShort
		}
		h.Key = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
	}
	if h.Flags&FlagSequence != 0 {
		if len(data) < offset+4 {
			return nil, 0, ErrPacketTooShort
		}
		h.Sequence = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
	}

	return h, offset, nil
}

// Packet represents a complete GRE packet with header and payload
type Packet struct {
	Header  *Header
	Payload []byte
}

// NewPacket creates a new GRE packet with the given key and payload
func NewPacket(key uint32, seq uint32, payload []byte) *Packet {
	return &Packet{
		Header: &Header{
			Flags:        FlagKey | FlagSequence,
			ProtocolType: EtherTypeTransparentEthernet,
			Key:          key,
			Sequence:     seq,
		},
		Payload: payload,
	}
}

// Marshal serializes the complete GRE packet
func (p *Packet) Marshal() []byte {
	header := p.Header.Marshal()
	result := make([]byte, len(header)+len(p.Payload))
	copy(result, header)
	copy(result[len(header):], p.Payload)
	return result
}

// UnmarshalPacket deserializes a complete GRE packet from bytes
func UnmarshalPacket(data []byte) (*Packet, error) {
	header, offset, err := Unmarshal(data)
	if err != nil {
		return nil, err
	}

	return &Packet{
		Header:  header,
		Payload: data[offset:],
	}, nil
}

// IPHeader represents a minimal IPv4 header for raw socket operations
type IPHeader struct {
	Version    uint8
	IHL        uint8
	TOS        uint8
	TotalLen   uint16
	ID         uint16
	FragOff    uint16
	TTL        uint8
	Protocol   uint8
	Checksum   uint16
	SrcIP      net.IP
	DstIP      net.IP
}

// MarshalIPHeader creates an IPv4 header for GRE packets
func MarshalIPHeader(srcIP, dstIP net.IP, payloadLen int) []byte {
	header := make([]byte, 20)

	// Version (4) and IHL (5 = 20 bytes)
	header[0] = 0x45
	// TOS
	header[1] = 0
	// Total length
	totalLen := uint16(20 + payloadLen)
	binary.BigEndian.PutUint16(header[2:4], totalLen)
	// ID
	binary.BigEndian.PutUint16(header[4:6], 0)
	// Flags and Fragment Offset (Don't Fragment)
	binary.BigEndian.PutUint16(header[6:8], 0x4000)
	// TTL
	header[8] = 64
	// Protocol (GRE = 47)
	header[9] = ProtocolNumber
	// Checksum (will be calculated by kernel or set to 0)
	binary.BigEndian.PutUint16(header[10:12], 0)
	// Source IP
	copy(header[12:16], srcIP.To4())
	// Destination IP
	copy(header[16:20], dstIP.To4())

	return header
}

// ParseIPHeader parses an IPv4 header from raw bytes
func ParseIPHeader(data []byte) (*IPHeader, int, error) {
	if len(data) < 20 {
		return nil, 0, ErrPacketTooShort
	}

	ihl := int(data[0]&0x0f) * 4
	if len(data) < ihl {
		return nil, 0, ErrPacketTooShort
	}

	h := &IPHeader{
		Version:    data[0] >> 4,
		IHL:        uint8(ihl),
		TOS:        data[1],
		TotalLen:   binary.BigEndian.Uint16(data[2:4]),
		ID:         binary.BigEndian.Uint16(data[4:6]),
		FragOff:    binary.BigEndian.Uint16(data[6:8]),
		TTL:        data[8],
		Protocol:   data[9],
		Checksum:   binary.BigEndian.Uint16(data[10:12]),
		SrcIP:      net.IP(data[12:16]),
		DstIP:      net.IP(data[16:20]),
	}

	return h, ihl, nil
}
