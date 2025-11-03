package core

import (
	"encoding/binary"
	"io"
)

func EncodePacket(packet *Packet) []byte {
	// 编码数据包：4字节流ID + 4字节数据长度 + 数据
	buf := make([]byte, 8+len(packet.Data))
	binary.BigEndian.PutUint32(buf[:4], packet.StreamID)
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(packet.Data)))
	copy(buf[8:], packet.Data)
	return buf
}

func DecodePacket(r io.Reader) (*Packet, error) {
	// 读取固定8字节头部：4字节流ID + 4字节数据长度
	header := make([]byte, 8)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	streamID := binary.BigEndian.Uint32(header[:4])
	dataLen := binary.BigEndian.Uint32(header[4:8])

	// 读取实际数据
	data := make([]byte, dataLen)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	return &Packet{
		StreamID: streamID,
		Data:     data,
	}, nil
}
