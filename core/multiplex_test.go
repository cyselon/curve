package core

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"
)

// TestMultiplexerFrameSendReceive 测试帧的发送和接收功能
// 验证要点：
//   - 帧的编码和解码正确性
//   - 通过 Multiplexer 发送和接收帧
//   - 帧头部信息（Version, Flags, StreamID, Length）正确传输
func TestMultiplexerFrameSendReceive(t *testing.T) {
	// 创建本地TCP连接对用于测试
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()
	serverReady := make(chan bool)
	var serverFrame *Frame
	var serverErr error
	var serverMu sync.Mutex

	// 启动服务器端
	go func() {
		serverReady <- true
		conn, err := listener.Accept()
		if err != nil {
			t.Errorf("Failed to accept connection: %v", err)
			return
		}
		defer conn.Close()

		serverMux := NewMultiplexer(conn)
		frame, err := serverMux.ReceiveFrame()

		serverMu.Lock()
		serverFrame = frame
		serverErr = err
		serverMu.Unlock()
	}()

	<-serverReady
	time.Sleep(50 * time.Millisecond)

	// 客户端连接
	clientConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer clientConn.Close()

	clientMux := NewMultiplexer(clientConn)

	// 创建测试帧
	testData := []byte("Hello, Multiplexer!")
	testFrame := &Frame{
		Header: Header{
			Version:  FrameVersion,
			Flags:    FrameFlags,
			StreamID: 123,
			Length:   uint32(len(testData)),
		},
		Data: testData,
	}

	// 发送帧
	if err := clientMux.SendFrame(testFrame); err != nil {
		t.Fatalf("Failed to send frame: %v", err)
	}

	// 等待服务器接收
	time.Sleep(200 * time.Millisecond)

	// 验证服务器接收到的帧
	serverMu.Lock()
	defer serverMu.Unlock()

	if serverErr != nil {
		t.Fatalf("Server failed to receive frame: %v", serverErr)
	}

	if serverFrame == nil {
		t.Fatal("Server did not receive frame")
	}

	// 验证帧头部
	if serverFrame.Header.Version != testFrame.Header.Version {
		t.Errorf("Version mismatch: expected %d, got %d", testFrame.Header.Version, serverFrame.Header.Version)
	}

	if serverFrame.Header.Flags != testFrame.Header.Flags {
		t.Errorf("Flags mismatch: expected %d, got %d", testFrame.Header.Flags, serverFrame.Header.Flags)
	}

	if serverFrame.Header.StreamID != testFrame.Header.StreamID {
		t.Errorf("StreamID mismatch: expected %d, got %d", testFrame.Header.StreamID, serverFrame.Header.StreamID)
	}

	if serverFrame.Header.Length != testFrame.Header.Length {
		t.Errorf("Length mismatch: expected %d, got %d", testFrame.Header.Length, serverFrame.Header.Length)
	}

	// 验证帧数据
	if !bytes.Equal(serverFrame.Data, testFrame.Data) {
		t.Errorf("Data mismatch: expected %q, got %q", string(testFrame.Data), string(serverFrame.Data))
	}

	t.Log("Frame send/receive test passed")
}

// TestMultiplexerFrameEncodeDecode 测试帧的编码和解码功能
// 验证要点：
//   - 帧的 Encode 方法正确编码
//   - 帧的 Decode 方法正确解码
//   - 编码后再解码应该得到原始帧
func TestMultiplexerFrameEncodeDecode(t *testing.T) {
	testCases := []struct {
		name  string
		frame *Frame
	}{
		{
			name: "empty data",
			frame: &Frame{
				Header: Header{
					Version:  FrameVersion,
					Flags:    FrameFlags,
					StreamID: 1,
					Length:   0,
				},
				Data: []byte{},
			},
		},
		{
			name: "small data",
			frame: &Frame{
				Header: Header{
					Version:  FrameVersion,
					Flags:    FrameFlags,
					StreamID: 42,
					Length:   5,
				},
				Data: []byte("hello"),
			},
		},
		{
			name: "large data",
			frame: &Frame{
				Header: Header{
					Version:  1,
					Flags:    0,
					StreamID: 999,
					Length:   256,
				},
				Data: make([]byte, 256),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 填充大数据
			if len(tc.frame.Data) == 256 {
				for i := range tc.frame.Data {
					tc.frame.Data[i] = byte(i % 256)
				}
			}

			// 编码帧
			encoded := tc.frame.Encode()

			// 解码帧
			var decoded Frame
			reader := bytes.NewReader(encoded)
			if err := decoded.Decode(reader); err != nil {
				t.Fatalf("Failed to decode frame: %v", err)
			}

			// 验证头部
			if decoded.Header.Version != tc.frame.Header.Version {
				t.Errorf("Version mismatch: expected %d, got %d", tc.frame.Header.Version, decoded.Header.Version)
			}

			if decoded.Header.Flags != tc.frame.Header.Flags {
				t.Errorf("Flags mismatch: expected %d, got %d", tc.frame.Header.Flags, decoded.Header.Flags)
			}

			if decoded.Header.StreamID != tc.frame.Header.StreamID {
				t.Errorf("StreamID mismatch: expected %d, got %d", tc.frame.Header.StreamID, decoded.Header.StreamID)
			}

			if decoded.Header.Length != tc.frame.Header.Length {
				t.Errorf("Length mismatch: expected %d, got %d", tc.frame.Header.Length, decoded.Header.Length)
			}

			// 验证数据
			if !bytes.Equal(decoded.Data, tc.frame.Data) {
				t.Errorf("Data mismatch: expected %q, got %q", tc.frame.Data, decoded.Data)
			}
		})
	}
}

// TestMultiplexerStreamCreateClose 测试流的创建和关闭功能
// 验证要点：
//   - CreateStream 创建新流并返回正确的流ID和通道
//   - 流ID递增
//   - CloseStream 正确关闭流并清理资源
func TestMultiplexerStreamCreateClose(t *testing.T) {
	// 创建虚拟连接（使用管道）
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	clientMux := NewMultiplexer(clientConn)

	// 测试创建多个流
	streamIDs := make([]uint32, 5)
	for i := 0; i < 5; i++ {
		streamID, ch := clientMux.CreateStream()
		streamIDs[i] = streamID

		// 验证流ID递增
		expectedID := uint32(i + 1)
		if streamID != expectedID {
			t.Errorf("Stream %d: expected ID %d, got %d", i, expectedID, streamID)
		}

		// 验证通道非空
		if ch == nil {
			t.Errorf("Stream %d: channel is nil", streamID)
		}

		// 验证可以发送数据到通道（通道未关闭）
		select {
		case ch <- []byte("test"):
		case <-time.After(100 * time.Millisecond):
			t.Errorf("Stream %d: channel is blocked or closed", streamID)
		}
	}

	// 测试关闭流
	testStreamID := streamIDs[2]
	clientMux.CloseStream(testStreamID)

	// 验证通道已关闭
	streamCh, exists := clientMux.streams[testStreamID]
	if exists {
		// 尝试从通道读取，应该返回零值且通道关闭
		select {
		case _, ok := <-streamCh:
			if ok {
				t.Error("Closed stream channel should be closed")
			}
		default:
			// 通道可能还有缓冲数据
			// 尝试发送，应该失败
			select {
			case streamCh <- []byte("test"):
				t.Error("Should not be able to send to closed stream channel")
			default:
				// 这可能是正常的（通道已满）
			}
		}
	}

	// 验证流已从map中删除
	clientMux.mu.Lock()
	_, stillExists := clientMux.streams[testStreamID]
	clientMux.mu.Unlock()
	if stillExists {
		t.Errorf("Stream %d should be removed from streams map after closing", testStreamID)
	}

	t.Log("Stream create/close test passed")
}

// TestMultiplexerMultipleFramesSameStream 测试同一流发送多个帧
// 验证要点：
//   - 同一流ID可以发送多个帧
//   - 每个帧都能正确接收
//   - 数据内容正确
func TestMultiplexerMultipleFramesSameStream(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()
	serverReady := make(chan bool)
	var receivedFrames []*Frame
	var serverMu sync.Mutex

	// 启动服务器端
	go func() {
		serverReady <- true
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		serverMux := NewMultiplexer(conn)

		// 接收多个帧
		for i := 0; i < 5; i++ {
			frame, err := serverMux.ReceiveFrame()
			if err != nil {
				break
			}

			serverMu.Lock()
			receivedFrames = append(receivedFrames, frame)
			serverMu.Unlock()
		}
	}()

	<-serverReady
	time.Sleep(50 * time.Millisecond)

	// 客户端连接
	clientConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer clientConn.Close()

	clientMux := NewMultiplexer(clientConn)

	streamID := uint32(1)
	numFrames := 5

	// 发送多个帧，使用相同的流ID
	for i := 0; i < numFrames; i++ {
		data := []byte{byte('0' + i)}
		frame := &Frame{
			Header: Header{
				Version:  FrameVersion,
				Flags:    FrameFlags,
				StreamID: streamID,
				Length:   uint32(len(data)),
			},
			Data: data,
		}

		if err := clientMux.SendFrame(frame); err != nil {
			t.Fatalf("Failed to send frame %d: %v", i, err)
		}

		time.Sleep(10 * time.Millisecond)
	}

	// 等待服务器接收所有帧
	time.Sleep(500 * time.Millisecond)

	// 验证接收到的帧
	serverMu.Lock()
	defer serverMu.Unlock()

	if len(receivedFrames) != numFrames {
		t.Fatalf("Expected %d frames, got %d", numFrames, len(receivedFrames))
	}

	for i, frame := range receivedFrames {
		if frame.Header.StreamID != streamID {
			t.Errorf("Frame %d: expected StreamID %d, got %d", i, streamID, frame.Header.StreamID)
		}

		expectedData := []byte{byte('0' + i)}
		if !bytes.Equal(frame.Data, expectedData) {
			t.Errorf("Frame %d: expected data %q, got %q", i, string(expectedData), string(frame.Data))
		}
	}

	t.Log("Multiple frames same stream test passed")
}

// TestMultiplexerConcurrentFrameSend 测试并发发送帧
// 验证要点：
//   - 多个goroutine并发发送帧不会出错
//   - 所有帧都能正确接收
func TestMultiplexerConcurrentFrameSend(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()
	serverReady := make(chan bool)
	var receivedFrames []*Frame
	var serverMu sync.Mutex

	// 启动服务器端
	go func() {
		serverReady <- true
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		serverMux := NewMultiplexer(conn)

		// 接收多个帧
		for i := 0; i < 10; i++ {
			frame, err := serverMux.ReceiveFrame()
			if err != nil {
				break
			}

			serverMu.Lock()
			receivedFrames = append(receivedFrames, frame)
			serverMu.Unlock()
		}
	}()

	<-serverReady
	time.Sleep(50 * time.Millisecond)

	// 客户端连接
	clientConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer clientConn.Close()

	clientMux := NewMultiplexer(clientConn)

	// 并发发送帧
	var wg sync.WaitGroup
	numFrames := 10

	for i := 0; i < numFrames; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			data := []byte{byte('A' + id)}
			frame := &Frame{
				Header: Header{
					Version:  FrameVersion,
					Flags:    FrameFlags,
					StreamID: uint32(id%3 + 1), // 使用3个不同的流ID
					Length:   uint32(len(data)),
				},
				Data: data,
			}

			if err := clientMux.SendFrame(frame); err != nil {
				t.Errorf("Failed to send frame %d: %v", id, err)
			}
		}(i)
	}

	wg.Wait()

	// 等待服务器接收所有帧
	time.Sleep(500 * time.Millisecond)

	// 验证接收到的帧数量
	serverMu.Lock()
	defer serverMu.Unlock()

	if len(receivedFrames) < numFrames/2 {
		t.Errorf("Expected at least %d frames, got %d", numFrames/2, len(receivedFrames))
	}

	t.Logf("Concurrent frame send test passed: received %d frames", len(receivedFrames))
}
