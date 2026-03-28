package core

import (
	"encoding/binary"
	"io"
)

// FrameType is the type of frame
type FrameType byte

const (
	FrameData    FrameType = iota // data frame
	FrameControl                  // control frame (e.g. window update, PING)
)

// Frame is the basic unit of data transmission
// Binary format:
//   StreamID: 4 bytes (uint32, big-endian)
//   Type: 1 byte
//   Flags: 1 byte
//   PayloadLen: 4 bytes (uint32, big-endian)
//   Payload: PayloadLen bytes
type Frame struct {
	StreamID uint32
	Type     FrameType
	Flags    byte
	Payload  []byte
}

// Encode encodes the frame into bytes
func (f *Frame) Encode() []byte {
	payloadLen := uint32(len(f.Payload))
	buf := make([]byte, 10+payloadLen) // 4+1+1+4+payloadLen
	
	binary.BigEndian.PutUint32(buf[0:4], f.StreamID)
	buf[4] = byte(f.Type)
	buf[5] = f.Flags
	binary.BigEndian.PutUint32(buf[6:10], payloadLen)
	copy(buf[10:], f.Payload)
	
	return buf
}

// Decode decodes bytes from reader into the frame
func (f *Frame) Decode(r io.Reader) error {
	header := make([]byte, 10)
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}
	
	f.StreamID = binary.BigEndian.Uint32(header[0:4])
	f.Type = FrameType(header[4])
	f.Flags = header[5]
	payloadLen := binary.BigEndian.Uint32(header[6:10])
	
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
