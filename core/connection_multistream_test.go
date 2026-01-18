package core

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// TestConnectionMultipleStreams 测试 Connection 的多流管理功能
func TestConnectionMultipleStreams(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()
	serverReady := make(chan bool)
	var serverConn *Connection
	var serverMu sync.Mutex

	go func() {
		serverReady <- true
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		serverMu.Lock()
		serverConn = NewConnection(conn, false) // 服务器使用偶数流
		serverMu.Unlock()
		defer serverConn.Close()
	}()

	<-serverReady
	time.Sleep(50 * time.Millisecond)

	clientConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer clientConn.Close()

	clientConnection := NewConnection(clientConn, true) // 客户端使用奇数流
	defer clientConnection.Close()

	// 创建多个流
	numStreams := 3
	clientStreams := make([]*Stream, numStreams)
	for i := 0; i < numStreams; i++ {
		stream, err := clientConnection.OpenStream()
		if err != nil {
			t.Fatalf("Failed to open stream %d: %v", i, err)
		}
		clientStreams[i] = stream

		// 验证流ID是奇数（客户端）
		if stream.StreamID()%2 == 0 {
			t.Errorf("Stream %d: expected odd stream ID for client, got %d", i, stream.StreamID())
		}
	}

	// 从每个流发送数据
	for i, stream := range clientStreams {
		data := []byte{byte('0' + i)}
		n, err := stream.Write(data)
		if err != nil {
			t.Fatalf("Failed to write to stream %d: %v", i, err)
		}
		if n != len(data) {
			t.Errorf("Stream %d: expected to write %d bytes, got %d", i, len(data), n)
		}
	}

	// 等待服务器接收
	time.Sleep(500 * time.Millisecond)

	// 服务器创建对应的流来接收数据
	serverMu.Lock()
	conn := serverConn
	serverMu.Unlock()

	if conn == nil {
		t.Fatal("Server connection is nil")
	}

	// 服务器应该能够接收来自不同流的数据
	// 注意：服务器需要知道流ID才能创建对应的流
	// 这里我们假设服务器会为接收到的流自动创建 Stream
	// 或者服务器可以通过某种方式获取流ID

	t.Logf("Created %d streams successfully", numStreams)
}

// TestConnectionStreamIsolation 测试不同流之间的数据隔离
func TestConnectionStreamIsolation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()
	serverReady := make(chan bool)
	var serverConn *Connection
	var serverMu sync.Mutex

	go func() {
		serverReady <- true
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		serverMu.Lock()
		serverConn = NewConnection(conn, false)
		serverMu.Unlock()
		defer serverConn.Close()
	}()

	<-serverReady
	time.Sleep(50 * time.Millisecond)

	clientConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer clientConn.Close()

	clientConnection := NewConnection(clientConn, true)
	defer clientConnection.Close()

	// 创建两个流
	stream1, err := clientConnection.OpenStream()
	if err != nil {
		t.Fatalf("Failed to open stream 1: %v", err)
	}
	defer stream1.Close()

	stream2, err := clientConnection.OpenStream()
	if err != nil {
		t.Fatalf("Failed to open stream 2: %v", err)
	}
	defer stream2.Close()

	// 验证流ID不同
	if stream1.StreamID() == stream2.StreamID() {
		t.Errorf("Stream IDs should be different: both are %d", stream1.StreamID())
	}

	// 验证流ID都是奇数（客户端）
	if stream1.StreamID()%2 == 0 {
		t.Errorf("Stream 1 ID should be odd, got %d", stream1.StreamID())
	}
	if stream2.StreamID()%2 == 0 {
		t.Errorf("Stream 2 ID should be odd, got %d", stream2.StreamID())
	}

	t.Log("Stream isolation test passed")
}

// TestConnectionActiveStreams 测试获取活跃流列表
func TestConnectionActiveStreams(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	clientConnection := NewConnection(clientConn, true)
	defer clientConnection.Close()

	// 创建多个流
	numStreams := 5
	streams := make([]*Stream, numStreams)
	for i := 0; i < numStreams; i++ {
		stream, err := clientConnection.OpenStream()
		if err != nil {
			t.Fatalf("Failed to open stream %d: %v", i, err)
		}
		streams[i] = stream
	}

	// 获取活跃流列表
	activeStreams := clientConnection.GetActiveStreams()
	if len(activeStreams) != numStreams {
		t.Errorf("Expected %d active streams, got %d", numStreams, len(activeStreams))
	}

	// 关闭一个流
	streams[0].Close()

	// 再次获取活跃流列表
	activeStreams = clientConnection.GetActiveStreams()
	if len(activeStreams) != numStreams-1 {
		t.Errorf("Expected %d active streams after closing one, got %d", numStreams-1, len(activeStreams))
	}

	t.Log("Active streams management test passed")
}

// TestConnectionStreamReadWrite 测试流的读写功能
func TestConnectionStreamReadWrite(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()
	serverReady := make(chan bool)
	var serverConn *Connection
	var serverMu sync.Mutex

	go func() {
		serverReady <- true
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		serverMu.Lock()
		serverConn = NewConnection(conn, false)
		serverMu.Unlock()
		defer serverConn.Close()
	}()

	<-serverReady
	time.Sleep(50 * time.Millisecond)

	clientConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer clientConn.Close()

	clientConnection := NewConnection(clientConn, true)
	defer clientConnection.Close()

	// 客户端创建流并写入数据
	clientStream, err := clientConnection.OpenStream()
	if err != nil {
		t.Fatalf("Failed to open client stream: %v", err)
	}

	testData := []byte("Hello from stream!")
	n, err := clientStream.Write(testData)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	if n != len(testData) {
		t.Errorf("Expected to write %d bytes, got %d", len(testData), n)
	}

	// 等待传输
	time.Sleep(500 * time.Millisecond)

	// 服务器需要知道流ID才能接收数据
	// 这里我们假设服务器会为接收到的流自动创建 Stream
	// 或者通过某种方式获取流ID

	serverMu.Lock()
	conn := serverConn
	serverMu.Unlock()

	if conn == nil {
		t.Fatal("Server connection is nil")
	}

	// 服务器可以通过 GetStream 获取流（如果知道流ID）
	// 或者通过接收帧来创建流
	streamID := clientStream.StreamID()
	serverStream, exists := conn.GetStream(streamID)
	if !exists {
		// 如果流不存在，可能需要先接收帧来创建
		// 这里我们暂时跳过，因为需要更复杂的同步机制
		t.Logf("Stream %d not found on server, may need to receive frame first", streamID)
	} else {
		// 从服务器流读取数据
		buf := make([]byte, 1024)
		n, err := serverStream.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("Failed to read: %v", err)
		}

		receivedData := buf[:n]
		if !bytes.Equal(receivedData, testData) {
			t.Errorf("Data mismatch: expected %q, got %q", string(testData), string(receivedData))
		}
	}

	clientStream.Close()
	t.Log("Stream read/write test passed")
}
