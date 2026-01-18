package core

import (
	"encoding/binary"
	"io"
)

type Frame struct {
	Header Header
	Data   []byte
}

type Header struct {
	Version  uint8
	Flags    uint8
	StreamID uint32
	Length   uint32
}

const (
	FrameVersion = 1
	FrameFlags   = 0
)

func (frame *Frame) Encode() []byte {
	// Encode frame: 1 byte Version + 1 byte Flags + 4 bytes StreamID + 4 bytes Length + data
	headerSize := 10 // 1 + 1 + 4 + 4
	buf := make([]byte, headerSize+len(frame.Data))

	offset := 0
	buf[offset] = frame.Header.Version
	offset++
	buf[offset] = frame.Header.Flags
	offset++
	binary.BigEndian.PutUint32(buf[offset:offset+4], frame.Header.StreamID)
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:offset+4], frame.Header.Length)
	offset += 4
	copy(buf[offset:], frame.Data)

	return buf
}

func (frame *Frame) Decode(r io.Reader) error {
	// Read fixed 10-byte header: 1 byte Version + 1 byte Flags + 4 bytes StreamID + 4 bytes Length
	header := make([]byte, 10)
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}

	offset := 0
	frame.Header.Version = header[offset]
	offset++
	frame.Header.Flags = header[offset]
	offset++
	frame.Header.StreamID = binary.BigEndian.Uint32(header[offset : offset+4])
	offset += 4
	frame.Header.Length = binary.BigEndian.Uint32(header[offset : offset+4])

	// Read actual data
	data := make([]byte, frame.Header.Length)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}
	frame.Data = data
	return nil
}
