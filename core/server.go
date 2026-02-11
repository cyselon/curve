package core

import (
	"log/slog"
	"net"
)

// Handler is the interface for handling connections
type Handler interface {
	ServeConn(conn *Connection)
}

// Server represents a server that can listen and accept connections
// It is the entry point for server-side operations
type Server struct {
	mgr      *SessionManager
	listener net.Listener
	handler  Handler
}

// NewServer creates a new server
func NewServer() *Server {
	return &Server{
		mgr: NewSessionManager(),
	}
}

// SetHandler sets the handler for incoming connections
func (s *Server) SetHandler(handler Handler) {
	s.handler = handler
}

// Listen starts listening on the given address
func (s *Server) Listen(network, address string) error {
	listener, err := net.Listen(network, address)
	if err != nil {
		return err
	}
	s.listener = listener
	return nil
}

// Serve starts accepting connections and handling them
func (s *Server) Serve() error {
	if s.listener == nil {
		return net.ErrClosed
	}

	for {
		netConn, err := s.listener.Accept()
		if err != nil {
			return err
		}
		slog.Debug("Accepted connection", "remote address", netConn.RemoteAddr())
		// Create connection (server side, uses even streams)
		conn := NewConnection(netConn, false)
		conn.Start()

		// Create session
		session := &Session{
			conn: conn,
		}

		// Add session to manager using remote address as key
		key := netConn.RemoteAddr().String()
		slog.Debug("Added session", "remote address", key)
		s.mgr.AddSession(key, session)

		// Handle connection
		if s.handler != nil {
			slog.Info("Serving connection", "remote address", netConn.RemoteAddr())
			go s.handler.ServeConn(conn)
		}
	}
}

// Close closes the server and all sessions
func (s *Server) Close() error {
	if s.listener != nil {
		s.listener.Close()
	}
	s.mgr.CloseAll()
	return nil
}
