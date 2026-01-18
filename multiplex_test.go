package main

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"curve/core"
)

// 本文件包含TCP多路复用功能测试
//
// 测试覆盖以下场景：
// 1. TestMultiplexConcurrentStreams - 测试多个流并发发送数据，验证多路复用核心功能
// 2. TestMultiplexStreamOrdering - 测试多个流的数据包是否保持顺序
// 3. TestMultiplexLargeData - 测试大数据的多路复用传输
//
// 运行所有测试：
//   go test -v multiplex_test.go
//
// 运行单个测试：
//   go test -v -run TestMultiplexConcurrentStreams multiplex_test.go

// TestMultiplexConcurrentStreams 测试多个流并发发送数据
// 验证核心多路复用功能：使用5个流，每个流发送6条消息，总共30个数据包
// 测试要点：
//   - 多个流可以同时通过一个TCP连接发送数据
//   - 服务器端能正确接收并区分来自不同流的数据
//   - 数据不丢失（每个流应该收到6条消息）
//   - 数据内容正确（验证每条消息的内容是否符合预期）
func TestMultiplexConcurrentStreams(t *testing.T) {
	// 测试参数 - 在函数开始定义，供服务器和客户端使用
	numStreams := 5
	messagesPerStream := 6
	totalPackets := numStreams * messagesPerStream

	// 设置测试端口
	addr := "127.0.0.1:18888"

	// 启动服务器
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer listener.Close()

	// 使用 channel 跟踪服务器启动
	serverReady := make(chan bool)

	// 启动服务器处理连接
	serverErrors := make(chan error, 100)
	go func() {
		serverReady <- true
		conn, err := listener.Accept()
		if err != nil {
			serverErrors <- fmt.Errorf("failed to accept connection: %v", err)
			return
		}
		defer conn.Close()

		// 创建多路复用器处理服务器端
		serverMux := core.NewMultiplexer(conn, false) // 服务器使用偶数流

		// 创建 map 来存储每个流接收的数据
		receivedData := make(map[uint32][]string)
		receivedMu := sync.Mutex{}

		// 在后台启动接收数据包的 goroutine
		done := make(chan bool)
		go func() {
			for i := 0; i < totalPackets; i++ { // 预期接收所有数据包
				frame, err := serverMux.ReceiveFrame()
				if err != nil {
					// 忽略错误，可能是因为客户端关闭连接
					break
				}

				receivedMu.Lock()
				receivedData[frame.Header.StreamID] = append(receivedData[frame.Header.StreamID], string(frame.Data))
				receivedMu.Unlock()

				// 回显数据
				if err := serverMux.SendFrame(frame); err != nil {
					// 忽略错误
				}
			}
			done <- true
		}()

		<-done

		// 验证接收到的数据
		receivedMu.Lock()
		defer receivedMu.Unlock()

		// 验证：应该有5个流收到数据
		if len(receivedData) != numStreams {
			serverErrors <- fmt.Errorf("expected %d streams, got %d", numStreams, len(receivedData))
		}

		// 验证每个流的数据
		for streamID := uint32(1); streamID <= uint32(numStreams); streamID++ {
			data, exists := receivedData[streamID]
			if !exists {
				serverErrors <- fmt.Errorf("stream %d did not receive any data", streamID)
				continue
			}

			// 验证消息数量
			if len(data) != messagesPerStream {
				serverErrors <- fmt.Errorf("stream %d: expected %d messages, got %d", streamID, messagesPerStream, len(data))
				continue
			}

			// 验证每条消息的内容（并发发送时顺序可能不同，所以检查集合而非顺序）
			expectedMessages := make(map[string]bool)
			for msgNum := 0; msgNum < messagesPerStream; msgNum++ {
				expectedMsg := fmt.Sprintf("Stream%d-Message%d", streamID, msgNum)
				expectedMessages[expectedMsg] = true
			}

			receivedMessages := make(map[string]bool)
			for _, msg := range data {
				receivedMessages[msg] = true
				if !expectedMessages[msg] {
					serverErrors <- fmt.Errorf("stream %d: unexpected message %q", streamID, msg)
				}
			}

			// 验证所有期望的消息都收到了
			for expectedMsg := range expectedMessages {
				if !receivedMessages[expectedMsg] {
					serverErrors <- fmt.Errorf("stream %d: missing expected message %q", streamID, expectedMsg)
				}
			}

			t.Logf("Stream %d received %d messages correctly", streamID, len(data))
		}
	}()

	// 等待服务器启动
	<-serverReady
	time.Sleep(100 * time.Millisecond)

	// 创建客户端连接
	clientConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer clientConn.Close()

	// 创建客户端多路复用器
	clientMux := core.NewMultiplexer(clientConn, true) // 客户端使用奇数流

	// 使用 WaitGroup 等待所有 goroutine 完成
	var wg sync.WaitGroup
	errors := make(chan error, numStreams*messagesPerStream)

	// 为每个流创建并发送数据
	for streamNum := 1; streamNum <= numStreams; streamNum++ {
		streamNum := streamNum // 捕获循环变量
		wg.Add(1)
		go func() {
			defer wg.Done()

			// 为这个流发送多个消息
			for msgNum := 0; msgNum < messagesPerStream; msgNum++ {
				wg.Add(1)
				go func(streamID int, msgID int) {
					defer wg.Done()

					// 构造测试数据
					data := []byte(fmt.Sprintf("Stream%d-Message%d", streamID, msgID))

					// 创建并发送帧
					frame := &core.Frame{
						Header: core.Header{
							Version:  core.FrameVersion,
							Flags:    core.FrameFlags,
							StreamID: uint32(streamID),
							Length:   uint32(len(data)),
						},
						Data: data,
					}

					if err := clientMux.SendFrame(frame); err != nil {
						errors <- fmt.Errorf("Failed to send packet for stream %d: %v", streamID, err)
						return
					}

					t.Logf("Sent: StreamID=%d, Data=%s", streamID, string(data))
				}(streamNum, msgNum)

				// 添加小延迟以避免数据包粘包
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}

	// 等待所有发送完成
	wg.Wait()

	// 关闭错误 channel
	close(errors)

	// 检查是否有错误
	for err := range errors {
		t.Errorf("Error: %v", err)
	}

	// 给一点时间让服务器处理完所有消息
	time.Sleep(1 * time.Second)

	// 检查服务器错误
	// 使用 select 避免在空 channel 上阻塞
	for {
		select {
		case err := <-serverErrors:
			if err != nil {
				t.Errorf("Server error: %v", err)
			}
		default:
			goto done
		}
	}
done:
	t.Log("Concurrent stream test completed successfully")
}

// TestMultiplexStreamOrdering 测试多个流的数据包是否保持顺序
// 验证要点：
//   - 两个流交替发送数据时，每个流的数据能正确区分
//   - 每个流的数据内容符合预期
func TestMultiplexStreamOrdering(t *testing.T) {
	numMessages := 10
	numStreams := 2
	totalPackets := numMessages * numStreams

	addr := "127.0.0.1:18889"

	// 启动服务器
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer listener.Close()

	serverReady := make(chan bool)
	serverReceivedData := make(map[uint32][]string)
	serverMu := sync.Mutex{}
	serverDone := make(chan bool)

	go func() {
		serverReady <- true
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- false
			return
		}
		defer conn.Close()

		serverMux := core.NewMultiplexer(conn, false) // 服务器使用偶数流

		// 接收所有数据包并记录
		for i := 0; i < totalPackets; i++ {
			frame, err := serverMux.ReceiveFrame()
			if err != nil {
				break
			}

			serverMu.Lock()
			serverReceivedData[frame.Header.StreamID] = append(serverReceivedData[frame.Header.StreamID], string(frame.Data))
			serverMu.Unlock()

			// 回显
			serverMux.SendFrame(frame)
		}
		serverDone <- true
	}()

	<-serverReady
	time.Sleep(100 * time.Millisecond)

	// 客户端连接
	clientConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer clientConn.Close()

	clientMux := core.NewMultiplexer(clientConn, true) // 客户端使用奇数流

	// 从两个流交替发送数据
	for i := 0; i < numMessages; i++ {
		// 流1
		data1 := []byte(fmt.Sprintf("Stream1-Msg%d", i))
		frame1 := &core.Frame{
			Header: core.Header{
				Version:  core.FrameVersion,
				Flags:    core.FrameFlags,
				StreamID: 1,
				Length:   uint32(len(data1)),
			},
			Data: data1,
		}
		if err := clientMux.SendFrame(frame1); err != nil {
			t.Errorf("Failed to send frame for stream 1: %v", err)
		}

		// 流2
		data2 := []byte(fmt.Sprintf("Stream2-Msg%d", i))
		frame2 := &core.Frame{
			Header: core.Header{
				Version:  core.FrameVersion,
				Flags:    core.FrameFlags,
				StreamID: 2,
				Length:   uint32(len(data2)),
			},
			Data: data2,
		}
		if err := clientMux.SendFrame(frame2); err != nil {
			t.Errorf("Failed to send frame for stream 2: %v", err)
		}
	}

	// 等待服务器处理完所有数据包
	<-serverDone
	time.Sleep(200 * time.Millisecond)

	// 验证服务器接收到的数据
	serverMu.Lock()
	defer serverMu.Unlock()

	// 验证应该有两个流
	if len(serverReceivedData) != numStreams {
		t.Errorf("Expected %d streams, got %d", numStreams, len(serverReceivedData))
	}

	// 验证每个流的数据
	for streamID := uint32(1); streamID <= uint32(numStreams); streamID++ {
		data, exists := serverReceivedData[streamID]
		if !exists {
			t.Errorf("Stream %d did not receive any data", streamID)
			continue
		}

		// 验证消息数量
		if len(data) != numMessages {
			t.Errorf("Stream %d: expected %d messages, got %d", streamID, numMessages, len(data))
			continue
		}

		// 验证每条消息的内容
		for msgNum := 0; msgNum < numMessages; msgNum++ {
			expectedMsg := fmt.Sprintf("Stream%d-Msg%d", streamID, msgNum)
			if msgNum < len(data) && data[msgNum] != expectedMsg {
				t.Errorf("Stream %d message %d: expected %q, got %q", streamID, msgNum, expectedMsg, data[msgNum])
			}
		}

		t.Logf("Stream %d received %d messages correctly", streamID, len(data))
	}

	t.Log("Stream ordering test completed successfully")
}

// TestMultiplexLargeData 测试大数据的多路复用
// 验证要点：
//   - 大数据包（512字节）能够正确传输
//   - 多个流的大数据包能够正确区分
//   - 数据内容完整无误
func TestMultiplexLargeData(t *testing.T) {
	numStreams := 5
	largeDataSize := 512

	addr := "127.0.0.1:18890"

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer listener.Close()

	serverReady := make(chan bool)
	serverReceivedData := make(map[uint32][]byte)
	serverMu := sync.Mutex{}
	serverDone := make(chan bool)

	go func() {
		serverReady <- true
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- false
			return
		}
		defer conn.Close()

		serverMux := core.NewMultiplexer(conn, false) // 服务器使用偶数流

		for i := 0; i < numStreams; i++ {
			frame, err := serverMux.ReceiveFrame()
			if err != nil {
				break
			}

			serverMu.Lock()
			serverReceivedData[frame.Header.StreamID] = frame.Data
			serverMu.Unlock()

			// 回显数据
			serverMux.SendFrame(frame)
		}
		serverDone <- true
	}()

	<-serverReady
	time.Sleep(100 * time.Millisecond)

	clientConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer clientConn.Close()

	clientMux := core.NewMultiplexer(clientConn, true) // 客户端使用奇数流

	// 准备大数据
	largeData := make([]byte, largeDataSize)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	// 记录每个流发送的数据
	clientSentData := make(map[uint32][]byte)

	// 从不同流发送大数据包
	for streamID := 1; streamID <= numStreams; streamID++ {
		prefix := []byte(fmt.Sprintf("Stream%d:", streamID))
		fullData := append(prefix, largeData...)

		frame := &core.Frame{
			Header: core.Header{
				Version:  core.FrameVersion,
				Flags:    core.FrameFlags,
				StreamID: uint32(streamID),
				Length:   uint32(len(fullData)),
			},
			Data: fullData,
		}

		clientSentData[uint32(streamID)] = fullData

		if err := clientMux.SendFrame(frame); err != nil {
			t.Errorf("Failed to send large data for stream %d: %v", streamID, err)
		}
	}

	// 等待服务器处理完所有数据包
	<-serverDone
	time.Sleep(200 * time.Millisecond)

	// 验证服务器接收到的数据
	serverMu.Lock()
	defer serverMu.Unlock()

	// 验证应该有多少个流收到数据
	if len(serverReceivedData) != numStreams {
		t.Errorf("Expected %d streams, got %d", numStreams, len(serverReceivedData))
	}

	// 验证每个流的数据
	for streamID := uint32(1); streamID <= uint32(numStreams); streamID++ {
		receivedData, exists := serverReceivedData[streamID]
		if !exists {
			t.Errorf("Stream %d did not receive any data", streamID)
			continue
		}

		expectedData, clientExists := clientSentData[streamID]
		if !clientExists {
			t.Errorf("Stream %d: no client data to compare", streamID)
			continue
		}

		// 验证数据长度
		if len(receivedData) != len(expectedData) {
			t.Errorf("Stream %d: expected data length %d, got %d", streamID, len(expectedData), len(receivedData))
			continue
		}

		// 验证数据内容
		for i := 0; i < len(expectedData); i++ {
			if receivedData[i] != expectedData[i] {
				t.Errorf("Stream %d: data mismatch at byte %d: expected %d, got %d", streamID, i, expectedData[i], receivedData[i])
				break
			}
		}

		// 验证前缀
		expectedPrefix := fmt.Sprintf("Stream%d:", streamID)
		if !hasPrefix(receivedData, expectedPrefix) {
			t.Errorf("Stream %d: data does not have expected prefix %q", streamID, expectedPrefix)
			continue
		}

		t.Logf("Stream %d: received %d bytes correctly", streamID, len(receivedData))
	}

	t.Log("Large data test completed successfully")
}

// hasPrefix 辅助函数检查字节切片是否以字符串为前缀
func hasPrefix(data []byte, prefix string) bool {
	prefixBytes := []byte(prefix)
	if len(data) < len(prefixBytes) {
		return false
	}
	for i := 0; i < len(prefixBytes); i++ {
		if data[i] != prefixBytes[i] {
			return false
		}
	}
	return true
}
