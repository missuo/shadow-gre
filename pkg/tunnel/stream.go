// Package tunnel implements lightweight stream multiplexing over GRE.
package tunnel

import (
	"encoding/binary"
	"errors"
)

const (
	// Stream flags
	StreamData  uint8 = 0x00 // Data packet
	StreamClose uint8 = 0x01 // Close stream

	// StreamHeaderSize is the header size (StreamID + Flags)
	StreamHeaderSize = 5
)

var (
	ErrStreamTooShort = errors.New("stream packet too short")
)

// StreamPacket represents a lightweight stream packet
type StreamPacket struct {
	StreamID uint32 // Stream identifier (replaces ConnID)
	Flags    uint8  // Packet flags
	Data     []byte // Packet data
}

// Marshal serializes a stream packet to bytes (zero-copy when possible)
func (p *StreamPacket) Marshal() []byte {
	buf := make([]byte, StreamHeaderSize+len(p.Data))
	binary.BigEndian.PutUint32(buf[0:4], p.StreamID)
	buf[4] = p.Flags
	copy(buf[StreamHeaderSize:], p.Data)
	return buf
}

// MarshalTo serializes into an existing buffer (true zero-copy)
func (p *StreamPacket) MarshalTo(buf []byte) int {
	binary.BigEndian.PutUint32(buf[0:4], p.StreamID)
	buf[4] = p.Flags
	n := copy(buf[StreamHeaderSize:], p.Data)
	return StreamHeaderSize + n
}

// UnmarshalStream deserializes a stream packet from bytes
func UnmarshalStream(data []byte) (*StreamPacket, error) {
	if len(data) < StreamHeaderSize {
		return nil, ErrStreamTooShort
	}

	p := &StreamPacket{
		StreamID: binary.BigEndian.Uint32(data[0:4]),
		Flags:    data[4],
	}

	if len(data) > StreamHeaderSize {
		p.Data = data[StreamHeaderSize:]
	}

	return p, nil
}

// NewDataPacket creates a data packet
func NewDataPacket(streamID uint32, data []byte) *StreamPacket {
	return &StreamPacket{
		StreamID: streamID,
		Flags:    StreamData,
		Data:     data,
	}
}

// NewClosePacket creates a close packet
func NewClosePacket(streamID uint32) *StreamPacket {
	return &StreamPacket{
		StreamID: streamID,
		Flags:    StreamClose,
	}
}
