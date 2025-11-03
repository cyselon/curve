package core

import (
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

// SendFrame 发送帧
func (m *Multiplexer) SendFrame(frame *Frame) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 使用 Encode 编码帧
	buf := frame.Encode()

	// 发送数据
	_, err := m.conn.Write(buf)
	return err
}

// ReceiveFrame 接收帧
func (m *Multiplexer) ReceiveFrame() (*Frame, error) {
	frame := &Frame{}
	err := frame.Decode(m.conn)
	if err != nil {
		return nil, err
	}
	return frame, nil
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
