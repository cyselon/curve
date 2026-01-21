package core

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// TestConnectionReadWrite 测试 Connection 的 Read/Write 接口
// 验证要点：
//   - Connection 实现 io.Reader 和 io.Writer
//   - Write 将数据拆分成帧发送
//   - Read 收集帧并重组数据
func TestConnectionReadWrite(t *testing.T) {
	// 创建测试服务器
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()
	serverReady := make(chan bool)
	var serverConn *Connection

	// 启动服务器
	go func() {
		serverReady <- true
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		serverConn = NewConnection(conn, false) // 服务器使用偶数流
		defer serverConn.Close()
	}()

	<-serverReady
	time.Sleep(50 * time.Millisecond)

	// 创建客户端连接
	clientConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer clientConn.Close()

	clientConnection := NewConnection(clientConn, true) // 客户端使用奇数流
	defer clientConnection.Close()

	// 测试数据
	testData := []byte("Hello, Connection!")

	// 客户端写入数据
	n, err := clientConnection.Write(testData)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	if n != len(testData) {
		t.Errorf("Expected to write %d bytes, got %d", len(testData), n)
	}

	// 等待服务器接收
	time.Sleep(200 * time.Millisecond)

	// 服务器读取数据
	if serverConn == nil {
		t.Fatal("Server connection is nil")
	}

	buf := make([]byte, 1024)
	n, err = serverConn.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read: %v", err)
	}

	receivedData := buf[:n]
	if !bytes.Equal(receivedData, testData) {
		t.Errorf("Data mismatch: expected %q, got %q", string(testData), string(receivedData))
	}

	t.Log("Read/Write test passed")
}

// TestConnectionLargeData 测试 Connection 处理大数据
// 验证要点：
//   - 大数据被正确拆分成多个帧
//   - 多个帧被正确重组
func TestConnectionLargeData(t *testing.T) {
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

	// 创建大于 MaxFrameDataSize 的数据
	largeDataSize := MaxFrameDataSize*2 + 1000
	largeData := make([]byte, largeDataSize)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	// 写入大数据
	n, err := clientConnection.Write(largeData)
	if err != nil {
		t.Fatalf("Failed to write large data: %v", err)
	}
	if n != len(largeData) {
		t.Errorf("Expected to write %d bytes, got %d", len(largeData), n)
	}

	// 等待传输完成
	time.Sleep(500 * time.Millisecond)

	// 服务器读取数据
	serverMu.Lock()
	conn := serverConn
	serverMu.Unlock()

	if conn == nil {
		t.Fatal("Server connection is nil")
	}

	// 多次读取直到获取所有数据
	var allData []byte
	buf := make([]byte, 10240) // 10KB 缓冲区

	for len(allData) < largeDataSize {
		n, err := conn.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Failed to read: %v", err)
		}
		if n == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		allData = append(allData, buf[:n]...)

		// 如果已经收到所有数据，退出
		if len(allData) >= largeDataSize {
			break
		}
	}

	// 验证数据
	if len(allData) != len(largeData) {
		t.Errorf("Expected to receive %d bytes, got %d", len(largeData), len(allData))
	}

	if !bytes.Equal(allData, largeData) {
		// 找到第一个不匹配的位置
		for i := 0; i < len(allData) && i < len(largeData); i++ {
			if allData[i] != largeData[i] {
				t.Errorf("Data mismatch at byte %d: expected %d, got %d", i, largeData[i], allData[i])
				break
			}
		}
		t.Fatal("Large data mismatch")
	}

	t.Logf("Large data test passed: %d bytes", len(allData))
}

// TestConnectionNetConnMethods 测试 Connection 实现的 net.Conn 接口方法
func TestConnectionNetConnMethods(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()
	serverReady := make(chan bool)
	var serverConn *Connection

	go func() {
		serverReady <- true
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		serverConn = NewConnection(conn, false) // 服务器使用偶数流
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

	// 测试 LocalAddr
	localAddr := clientConnection.LocalAddr()
	if localAddr == nil {
		t.Error("LocalAddr is nil")
	}

	// 测试 RemoteAddr
	remoteAddr := clientConnection.RemoteAddr()
	if remoteAddr == nil {
		t.Error("RemoteAddr is nil")
	}

	// 测试 SetDeadline
	err = clientConnection.SetDeadline(time.Now().Add(1 * time.Second))
	if err != nil {
		t.Errorf("SetDeadline failed: %v", err)
	}

	// 测试 SetReadDeadline
	err = clientConnection.SetReadDeadline(time.Now().Add(1 * time.Second))
	if err != nil {
		t.Errorf("SetReadDeadline failed: %v", err)
	}

	// 测试 SetWriteDeadline
	err = clientConnection.SetWriteDeadline(time.Now().Add(1 * time.Second))
	if err != nil {
		t.Errorf("SetWriteDeadline failed: %v", err)
	}

	// 测试 GetFramer
	mux := clientConnection.GetFramer()
	if mux == nil {
		t.Error("GetFramer returned nil")
	}

	// 测试 OpenStream
	stream, err := clientConnection.OpenStream()
	if err != nil {
		t.Errorf("OpenStream failed: %v", err)
	} else {
		streamID := stream.StreamID()
		if streamID == 0 {
			t.Error("StreamID returned 0")
		}
		stream.Close()
	}

	t.Log("Net.Conn methods test passed")
}

// TestConnectionClose 测试 Connection 关闭
func TestConnectionClose(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()
	serverReady := make(chan bool)

	go func() {
		serverReady <- true
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		serverConn := NewConnection(conn, false) // 服务器使用偶数流
		defer serverConn.Close()
	}()

	<-serverReady
	time.Sleep(50 * time.Millisecond)

	clientConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}

	clientConnection := NewConnection(clientConn, true) // 客户端使用奇数流

	// 测试正常关闭
	err = clientConnection.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// 测试关闭后写入
	_, err = clientConnection.Write([]byte("test"))
	if err != io.ErrClosedPipe {
		t.Errorf("Expected ErrClosedPipe after close, got: %v", err)
	}

	// 测试多次关闭
	err = clientConnection.Close()
	if err != nil {
		t.Errorf("Second close failed: %v", err)
	}

	t.Log("Close test passed")
}

// TestConnectionRoundTrip 测试 Connection 的双向通信
func TestConnectionRoundTrip(t *testing.T) {
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

	// 客户端 -> 服务器
	clientData := []byte("Hello from client!")
	n, err := clientConnection.Write(clientData)
	if err != nil {
		t.Fatalf("Client write failed: %v", err)
	}
	if n != len(clientData) {
		t.Errorf("Client write: expected %d, got %d", len(clientData), n)
	}

	time.Sleep(200 * time.Millisecond)

	serverMu.Lock()
	conn := serverConn
	serverMu.Unlock()

	if conn == nil {
		t.Fatal("Server connection is nil")
	}

	// 服务器读取
	buf := make([]byte, 1024)
	n, err = conn.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Server read failed: %v", err)
	}

	serverReceived := buf[:n]
	if !bytes.Equal(serverReceived, clientData) {
		t.Errorf("Server received: expected %q, got %q", string(clientData), string(serverReceived))
	}

	// 服务器 -> 客户端
	serverData := []byte("Hello from server!")
	n, err = conn.Write(serverData)
	if err != nil {
		t.Fatalf("Server write failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// 客户端读取
	buf = make([]byte, 1024)
	n, err = clientConnection.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Client read failed: %v", err)
	}

	clientReceived := buf[:n]
	if !bytes.Equal(clientReceived, serverData) {
		t.Errorf("Client received: expected %q, got %q", string(serverData), string(clientReceived))
	}

	t.Log("Round trip test passed")
}
