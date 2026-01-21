package core

import (
	"fmt"
	"net"
)

type Handler interface {
	ServeConn(conn Connection)
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

// Start starts a TCP server
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

		// Handle each connection in a separate goroutine
		go s.serveConn(conn)
	}
}

// serveConn handles client connections
func (s *Server) serveConn(conn net.Conn) {
	defer conn.Close()

	mux := s.handler.Framer(conn)
	fmt.Printf("Client connected from %s\n", conn.RemoteAddr())

	// Simple echo of all received frames
	for {
		frame, err := mux.ReceiveFrame()
		if err != nil {
			fmt.Printf("Error receiving frame: %v\n", err)
			return
		}

		fmt.Printf("Received: StreamID=%d, Data=%s\n", frame.Header.StreamID, string(frame.Data))

		s.handler.HandlePacket(frame, mux)
	}
}
