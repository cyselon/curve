package core

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
)

// Multiplexer 多路复用器
type Multiplexer struct {
	conn       net.Conn
	streams    map[uint32]chan []byte // 流 ID 到数据通道的映射
	nextStream uint32                 // 下一个流的 ID
	mu         sync.Mutex
}

// NewMultiplexer 创建一个新的多路复用器
func NewMultiplexer(conn net.Conn) *Multiplexer {
	return &Multiplexer{
		conn:       conn,
		streams:    make(map[uint32]chan []byte),
		nextStream: 1,
	}
}

// SendPacket 发送数据包
func (m *Multiplexer) SendPacket(packet *Packet) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 编码数据包：4字节流ID + 4字节数据长度 + 数据
	buf := make([]byte, 8+len(packet.Data))
	binary.BigEndian.PutUint32(buf[:4], packet.StreamID)
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(packet.Data)))
	copy(buf[8:], packet.Data)

	// 发送数据
	_, err := m.conn.Write(buf)
	return err
}

// ReceivePacket 接收数据包
func (m *Multiplexer) ReceivePacket() (*Packet, error) {
	// 读取固定8字节头部：4字节流ID + 4字节数据长度
	header := make([]byte, 8)
	if _, err := io.ReadFull(m.conn, header); err != nil {
		return nil, err
	}

	streamID := binary.BigEndian.Uint32(header[:4])
	dataLen := binary.BigEndian.Uint32(header[4:8])

	// 读取实际数据
	data := make([]byte, dataLen)
	if _, err := io.ReadFull(m.conn, data); err != nil {
		return nil, err
	}

	return &Packet{
		StreamID: streamID,
		Data:     data,
	}, nil
}

// CreateStream 创建一个新的流
func (m *Multiplexer) CreateStream() (uint32, chan []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	streamID := m.nextStream
	m.nextStream++

	ch := make(chan []byte, 10) // 带缓冲的通道
	m.streams[streamID] = ch

	return streamID, ch
}

// CloseStream 关闭一个流
func (m *Multiplexer) CloseStream(streamID uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ch, ok := m.streams[streamID]; ok {
		close(ch)
		delete(m.streams, streamID)
	}
}
