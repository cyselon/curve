package core

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// setupTestConnection 创建一对连接的 client 和 server Connection
func setupTestConnection(t *testing.T) (clientConn, serverConn *Connection) {
	t.Helper()

	clientNet, serverNet := net.Pipe()

	clientConn = NewConnection(clientNet, true)
	serverConn = NewConnection(serverNet, false)

	clientConn.Start()
	serverConn.Start()

	return clientConn, serverConn
}

func TestNewConnection_ClientStreamIDs(t *testing.T) {
	clientNet, serverNet := net.Pipe()
	defer clientNet.Close()
	defer serverNet.Close()

	client := NewConnection(clientNet, true)
	client.Start()

	// 客户端创建流应该是奇数 (1, 3, 5...)
	stream1, err := client.CreateStream()
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}
	if stream1.ID()%2 == 0 {
		t.Errorf("client stream ID should be odd, got %d", stream1.ID())
	}

	stream2, err := client.CreateStream()
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}
	if stream2.ID()%2 == 0 {
		t.Errorf("client stream ID should be odd, got %d", stream2.ID())
	}

	client.Close()
}

func TestNewConnection_ServerStreamIDs(t *testing.T) {
	clientNet, serverNet := net.Pipe()
	defer clientNet.Close()
	defer serverNet.Close()

	server := NewConnection(serverNet, false)
	server.Start()

	// 服务端创建流应该是偶数 (2, 4, 6...)
	stream1, err := server.CreateStream()
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}
	if stream1.ID()%2 != 0 {
		t.Errorf("server stream ID should be even, got %d", stream1.ID())
	}

	stream2, err := server.CreateStream()
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}
	if stream2.ID()%2 != 0 {
		t.Errorf("server stream ID should be even, got %d", stream2.ID())
	}

	server.Close()
}

func TestConnection_WriteFrame(t *testing.T) {
	clientConn, serverConn := setupTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	clientStream, err := clientConn.CreateStream()
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	payload := []byte("hello")
	frame := &Frame{
		StreamID: clientStream.ID(),
		Type:     FrameData,
		Flags:    0,
		Payload:  payload,
	}

	if err := clientConn.WriteFrame(frame); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	// 验证 WriteFrame 成功，数据会通过 writeLoop 发送到对端
	// 接收端验证在 TestConnection_ClientToServerDataTransfer 中
}

func TestConnection_ClientToServerDataTransfer(t *testing.T) {
	clientNet, serverNet := net.Pipe()

	clientConn := NewConnection(clientNet, true)
	serverConn := NewConnection(serverNet, false)
	clientConn.Start()
	serverConn.Start()

	defer clientConn.Close()
	defer serverConn.Close()

	// 客户端创建流并写入数据
	clientStream, err := clientConn.CreateStream()
	if err != nil {
		t.Fatalf("client CreateStream failed: %v", err)
	}

	data := []byte("ping")
	_, err = clientStream.Write(data)
	if err != nil {
		t.Fatalf("client Write failed: %v", err)
	}

	// 服务端获取对应流（奇数流，由 readLoop 自动创建）
	streamID := clientStream.ID()
	var serverStream *Stream
	for i := 0; i < 100; i++ {
		if s, ok := serverConn.GetStream(streamID); ok {
			serverStream = s
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if serverStream == nil {
		t.Fatal("server should have received stream from client")
	}

	buf := make([]byte, 10)
	n, err := serverStream.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("server Read failed: %v", err)
	}
	if n != len(data) || string(buf[:n]) != string(data) {
		t.Errorf("expected %q, got %q", data, buf[:n])
	}

	clientStream.Close()
}

func TestConnection_GetStream(t *testing.T) {
	clientConn, serverConn := setupTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	stream, err := clientConn.CreateStream()
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	streamID := stream.ID()
	if s, ok := clientConn.GetStream(streamID); !ok || s != stream {
		t.Error("GetStream should return the created stream")
	}

	if _, ok := clientConn.GetStream(999); ok {
		t.Error("GetStream should not find non-existent stream")
	}

	// 服务端不应有客户端创建的流（除非收到数据）
	if _, ok := serverConn.GetStream(streamID); ok {
		t.Error("server should not have client stream before receiving data")
	}
}

func TestConnection_CloseStream(t *testing.T) {
	clientConn, serverConn := setupTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	stream, err := clientConn.CreateStream()
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	streamID := stream.ID()
	stream.Close()

	if _, ok := clientConn.GetStream(streamID); ok {
		t.Error("stream should be removed after Close")
	}
}

func TestConnection_WriteFrameAfterClose(t *testing.T) {
	clientNet, serverNet := net.Pipe()
	defer serverNet.Close()

	// 启动 goroutine 从 serverNet 读取，使 client 的 write 能完成
	go io.Copy(io.Discard, serverNet)

	clientConn := NewConnection(clientNet, true)
	clientConn.Start()

	// 填充 writeCh（容量 100），使后续 WriteFrame 无法立即写入
	for i := 0; i < 100; i++ {
		_ = clientConn.WriteFrame(&Frame{StreamID: 1, Type: FrameData, Payload: []byte("x")})
	}

	clientConn.Close()

	frame := &Frame{StreamID: 1, Type: FrameData, Payload: []byte("test")}
	err := clientConn.WriteFrame(frame)
	if err != ErrConnectionClosed {
		t.Errorf("expected ErrConnectionClosed, got %v", err)
	}
}

func TestConnection_MaxConcurrentStreams(t *testing.T) {
	clientNet, serverNet := net.Pipe()
	defer clientNet.Close()
	defer serverNet.Close()

	client := NewConnection(clientNet, true)
	client.SetMaxConcurrentStreams(2)
	client.Start()
	defer client.Close()

	_, err := client.CreateStream()
	if err != nil {
		t.Fatalf("first CreateStream failed: %v", err)
	}

	_, err = client.CreateStream()
	if err != nil {
		t.Fatalf("second CreateStream failed: %v", err)
	}

	_, err = client.CreateStream()
	if err != ErrMaxStreamsReached {
		t.Errorf("expected ErrMaxStreamsReached, got %v", err)
	}
}

func TestConnection_ConcurrentCreateStream(t *testing.T) {
	clientConn, serverConn := setupTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	var wg sync.WaitGroup
	streams := make([]*Stream, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			stream, err := clientConn.CreateStream()
			if err != nil {
				t.Errorf("CreateStream %d failed: %v", idx, err)
				return
			}
			streams[idx] = stream
		}(i)
	}

	wg.Wait()

	// 检查所有流 ID 唯一且为奇数
	ids := make(map[uint32]bool)
	for i, s := range streams {
		if s == nil {
			t.Errorf("stream %d is nil", i)
			continue
		}
		if s.ID()%2 == 0 {
			t.Errorf("stream %d has even ID %d", i, s.ID())
		}
		if ids[s.ID()] {
			t.Errorf("duplicate stream ID %d", s.ID())
		}
		ids[s.ID()] = true
	}
}

func TestConnection_CloseIsIdempotent(t *testing.T) {
	clientConn, serverConn := setupTestConnection(t)

	err1 := clientConn.Close()
	err2 := clientConn.Close()

	if err1 != nil {
		t.Errorf("first Close failed: %v", err1)
	}
	if err2 != nil {
		t.Errorf("second Close failed: %v", err2)
	}

	// 确保不会 panic
	serverConn.Close()
	serverConn.Close()
}
