package core

import (
	"encoding/binary"
	"io"
)

// FrameType is the type of frame.
type FrameType byte

const (
	// CurrentVersion is the current wire format version.
	CurrentVersion byte = 1

	FrameData    FrameType = iota
	FrameControl
)

// ControlOp is the lifecycle operation encoded in a control frame flag byte.
type ControlOp byte

const (
	ControlOpen ControlOp = iota + 1
	ControlClose
	ControlReset
)

// Frame is the basic unit of data transmission.
// Binary format:
//
//	Version: 1 byte
//	StreamID: 4 bytes (uint32, big-endian)
//	Type: 1 byte
//	Flags: 1 byte
//	PayloadLen: 4 bytes (uint32, big-endian)
//	Payload: PayloadLen bytes
type Frame struct {
	Version  byte
	StreamID uint32
	Type     FrameType
	Flags    byte
	Payload  []byte
}

// Encode encodes the frame into bytes.
func (f *Frame) Encode() []byte {
	payloadLen := uint32(len(f.Payload))
	version := f.Version
	if version == 0 {
		version = CurrentVersion
	}

	buf := make([]byte, 11+payloadLen)
	buf[0] = version
	binary.BigEndian.PutUint32(buf[1:5], f.StreamID)
	buf[5] = byte(f.Type)
	buf[6] = f.Flags
	binary.BigEndian.PutUint32(buf[7:11], payloadLen)
	copy(buf[11:], f.Payload)

	return buf
}

// Decode decodes bytes from reader into the frame.
func (f *Frame) Decode(r io.Reader) error {
	header := make([]byte, 11)
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}

	f.Version = header[0]
	f.StreamID = binary.BigEndian.Uint32(header[1:5])
	f.Type = FrameType(header[5])
	f.Flags = header[6]
	payloadLen := binary.BigEndian.Uint32(header[7:11])

	if payloadLen > 0 {
		f.Payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(r, f.Payload); err != nil {
			return err
		}
	} else {
		f.Payload = nil
	}

	return nil
}

// Control returns the control operation of a control frame.
func (f *Frame) Control() ControlOp {
	return ControlOp(f.Flags)
}

// NewControlFrame creates a control frame for a stream lifecycle operation.
func NewControlFrame(streamID uint32, op ControlOp) *Frame {
	return &Frame{
		Version:  CurrentVersion,
		StreamID: streamID,
		Type:     FrameControl,
		Flags:    byte(op),
	}
}
