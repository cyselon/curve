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
	// 编码帧：1字节Version + 1字节Flags + 4字节StreamID + 4字节Length + 数据
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
	// 读取固定10字节头部：1字节Version + 1字节Flags + 4字节StreamID + 4字节Length
	header := make([]byte, 10)
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}

	offset := 0
	frame.Header.Version = header[offset]
	frame.Header.Flags = header[offset]
	offset++
	frame.Header.StreamID = binary.BigEndian.Uint32(header[offset : offset+4])
	offset += 4
	frame.Header.Length = binary.BigEndian.Uint32(header[offset : offset+4])

	// 读取实际数据
	data := make([]byte, frame.Header.Length)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}
	frame.Data = data
	return nil
}
