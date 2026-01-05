// Package tunnel implements connection multiplexing over GRE.
// It provides a simple protocol to multiplex multiple TCP connections
// over a single GRE tunnel.
package tunnel

import (
	"encoding/binary"
	"errors"
)

const (
	// Frame types
	FrameData    uint8 = 0x00 // Data frame
	FrameSyn     uint8 = 0x01 // Connection open
	FrameSynAck  uint8 = 0x02 // Connection open acknowledgment
	FrameFin     uint8 = 0x03 // Connection close
	FrameFinAck  uint8 = 0x04 // Connection close acknowledgment
	FramePing    uint8 = 0x05 // Keepalive ping
	FramePong    uint8 = 0x06 // Keepalive pong

	// MinFrameSize is the minimum frame header size
	// Type (1) + ConnID (4) + Seq (4) + Length (2) = 11 bytes
	MinFrameSize = 11

	// MaxPayloadSize is the maximum payload size
	MaxPayloadSize = 65000
)

var (
	ErrFrameTooShort    = errors.New("frame too short")
	ErrPayloadTooLarge  = errors.New("payload too large")
	ErrInvalidFrameType = errors.New("invalid frame type")
)

// Frame represents a tunnel protocol frame
type Frame struct {
	Type     uint8  // Frame type
	ConnID   uint32 // Connection identifier
	Sequence uint32 // Sequence number for ordering
	Payload  []byte // Frame payload
}

// Marshal serializes a frame to bytes
func (f *Frame) Marshal() []byte {
	payloadLen := len(f.Payload)
	buf := make([]byte, MinFrameSize+payloadLen)

	buf[0] = f.Type
	binary.BigEndian.PutUint32(buf[1:5], f.ConnID)
	binary.BigEndian.PutUint32(buf[5:9], f.Sequence)
	binary.BigEndian.PutUint16(buf[9:11], uint16(payloadLen))
	copy(buf[MinFrameSize:], f.Payload)

	return buf
}

// UnmarshalFrame deserializes a frame from bytes
func UnmarshalFrame(data []byte) (*Frame, error) {
	if len(data) < MinFrameSize {
		return nil, ErrFrameTooShort
	}

	f := &Frame{
		Type:     data[0],
		ConnID:   binary.BigEndian.Uint32(data[1:5]),
		Sequence: binary.BigEndian.Uint32(data[5:9]),
	}

	payloadLen := binary.BigEndian.Uint16(data[9:11])
	if len(data) < MinFrameSize+int(payloadLen) {
		return nil, ErrFrameTooShort
	}

	if payloadLen > 0 {
		f.Payload = make([]byte, payloadLen)
		copy(f.Payload, data[MinFrameSize:MinFrameSize+int(payloadLen)])
	}

	return f, nil
}

// NewDataFrame creates a new data frame
func NewDataFrame(connID uint32, seq uint32, payload []byte) *Frame {
	return &Frame{
		Type:     FrameData,
		ConnID:   connID,
		Sequence: seq,
		Payload:  payload,
	}
}

// NewSynFrame creates a new SYN frame for connection establishment
func NewSynFrame(connID uint32) *Frame {
	return &Frame{
		Type:   FrameSyn,
		ConnID: connID,
	}
}

// NewSynAckFrame creates a new SYN-ACK frame
func NewSynAckFrame(connID uint32) *Frame {
	return &Frame{
		Type:   FrameSynAck,
		ConnID: connID,
	}
}

// NewFinFrame creates a new FIN frame for connection termination
func NewFinFrame(connID uint32) *Frame {
	return &Frame{
		Type:   FrameFin,
		ConnID: connID,
	}
}

// NewFinAckFrame creates a new FIN-ACK frame
func NewFinAckFrame(connID uint32) *Frame {
	return &Frame{
		Type:   FrameFinAck,
		ConnID: connID,
	}
}

// NewPingFrame creates a new PING frame for keepalive
func NewPingFrame() *Frame {
	return &Frame{
		Type: FramePing,
	}
}

// NewPongFrame creates a new PONG frame
func NewPongFrame() *Frame {
	return &Frame{
		Type: FramePong,
	}
}
