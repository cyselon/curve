package core

import (
	"net"
)

// Handler is a handler for the server
type Handler interface {
	ServeConn(conn Connection)
}

// Server represents a server that can accept client connections
type Server struct {
	mgr      *SessionManager
	listener net.Listener
	handler  Handler
}
