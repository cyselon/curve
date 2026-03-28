package core

import (
	"bytes"
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

func waitForStream(t *testing.T, conn *Connection, streamID uint32) *Stream {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if stream, ok := conn.GetStream(streamID); ok {
			return stream
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for stream %d", streamID)
	return nil
}

func TestFrame_EncodeDecodeIncludesVersion(t *testing.T) {
	original := &Frame{
		Version:  CurrentVersion,
		StreamID: 7,
		Type:     FrameData,
		Flags:    3,
		Payload:  []byte("hello"),
	}

	encoded := original.Encode()
	if got := encoded[0]; got != CurrentVersion {
		t.Fatalf("expected first header byte to be version %d, got %d", CurrentVersion, got)
	}

	var decoded Frame
	if err := decoded.Decode(bytes.NewReader(encoded)); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Version != CurrentVersion {
		t.Fatalf("expected version %d, got %d", CurrentVersion, decoded.Version)
	}
	if decoded.StreamID != original.StreamID {
		t.Fatalf("expected StreamID %d, got %d", original.StreamID, decoded.StreamID)
	}
	if decoded.Type != original.Type {
		t.Fatalf("expected Type %d, got %d", original.Type, decoded.Type)
	}
	if decoded.Flags != original.Flags {
		t.Fatalf("expected Flags %d, got %d", original.Flags, decoded.Flags)
	}
	if !bytes.Equal(decoded.Payload, original.Payload) {
		t.Fatalf("expected payload %q, got %q", original.Payload, decoded.Payload)
	}
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

func TestConnection_OpenStreamAlias(t *testing.T) {
	clientNet, serverNet := net.Pipe()
	defer clientNet.Close()
	defer serverNet.Close()

	client := NewConnection(clientNet, true)
	client.Start()
	defer client.Close()

	stream, err := client.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	if stream.StreamID() != stream.ID() {
		t.Fatalf("expected StreamID alias to match ID, got %d and %d", stream.StreamID(), stream.ID())
	}
	if stream.StreamID()%2 == 0 {
		t.Fatalf("client OpenStream should allocate odd stream IDs, got %d", stream.StreamID())
	}
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

func TestConnection_ConcurrentMultiStreamTransmission(t *testing.T) {
	clientConn, serverConn := setupTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	payloads := make(map[uint32][]byte)
	streams := make([]*Stream, 5)

	for i := range streams {
		stream, err := clientConn.CreateStream()
		if err != nil {
			t.Fatalf("CreateStream %d failed: %v", i, err)
		}
		streams[i] = stream
		payloads[stream.ID()] = bytes.Repeat([]byte{byte('a' + i)}, 1024+(i*37))
	}

	var wg sync.WaitGroup
	for _, stream := range streams {
		stream := stream
		payload := payloads[stream.ID()]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := stream.Write(payload); err != nil {
				t.Errorf("Write failed for stream %d: %v", stream.ID(), err)
			}
		}()
	}
	wg.Wait()

	for _, stream := range streams {
		remoteStream := waitForStream(t, serverConn, stream.ID())
		got := make([]byte, len(payloads[stream.ID()]))
		if _, err := io.ReadFull(remoteStream, got); err != nil {
			t.Fatalf("ReadFull failed for stream %d: %v", stream.ID(), err)
		}
		if !bytes.Equal(got, payloads[stream.ID()]) {
			t.Fatalf("payload mismatch for stream %d", stream.ID())
		}
	}
}

func TestConnection_LargePayloadSplitAndReassembly(t *testing.T) {
	clientConn, serverConn := setupTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	clientStream, err := clientConn.CreateStream()
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	payload := bytes.Repeat([]byte("curve"), (DefaultFrameSize*3/5)+257)

	if _, err := clientStream.Write(payload); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	serverStream := waitForStream(t, serverConn, clientStream.ID())
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(serverStream, got); err != nil {
		t.Fatalf("ReadFull failed: %v", err)
	}

	if !bytes.Equal(got, payload) {
		t.Fatal("reassembled payload does not match original payload")
	}
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

func TestConnection_CloseUnblocksStreamRead(t *testing.T) {
	clientConn, serverConn := setupTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	clientStream, err := clientConn.CreateStream()
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	if _, err := clientStream.Write([]byte("init")); err != nil {
		t.Fatalf("initial Write failed: %v", err)
	}

	serverStream := waitForStream(t, serverConn, clientStream.ID())
	buf := make([]byte, 4)
	if _, err := io.ReadFull(serverStream, buf); err != nil {
		t.Fatalf("initial ReadFull failed: %v", err)
	}

	readDone := make(chan error, 1)
	go func() {
		blockingBuf := make([]byte, 1)
		_, err := serverStream.Read(blockingBuf)
		readDone <- err
	}()

	time.Sleep(50 * time.Millisecond)
	if err := clientConn.Close(); err != nil {
		t.Fatalf("client Close failed: %v", err)
	}

	select {
	case err := <-readDone:
		if err != io.EOF {
			t.Fatalf("expected io.EOF after close, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Read did not unblock after connection close")
	}
}
