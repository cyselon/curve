package core

import (
	"fmt"
	"net"
)

type Handler interface {
	HandlePacket(packet *Packet, mux *Multiplexer)
	Multiplexer(conn net.Conn) *Multiplexer
}

type Server struct {
	addr    string
	handler Handler
}

func NewServer(addr string, handler Handler) *Server {
	return &Server{
		addr:    addr,
		handler: handler,
	}
}

// StartServer 启动一个 TCP 服务器
func (s *Server) Start() {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		fmt.Printf("Failed to start server on %s: %v\n", s.addr, err)
		return
	}
	defer listener.Close()

	fmt.Printf("Server listening on %s\n", s.addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Failed to accept connection: %v\n", err)
			continue
		}

		// 为每个连接创建一个 goroutine 处理
		go s.handleConnection(conn)
	}
}

// handleConnection 处理客户端连接
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	mux := s.handler.Multiplexer(conn)
	fmt.Printf("Client connected from %s\n", conn.RemoteAddr())

	// 简单回显所有接收到的数据包
	for {
		packet, err := mux.ReceivePacket()
		if err != nil {
			fmt.Printf("Error receiving packet: %v\n", err)
			return
		}

		fmt.Printf("Received: StreamID=%d, Data=%s\n", packet.StreamID, string(packet.Data))

		s.handler.HandlePacket(packet, mux)
	}
}
