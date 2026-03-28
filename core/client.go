package core

import (
	"net"
)

// Client represents a client that can connect to a server
// It is the entry point for client-side operations
type Client struct {
	// session manager
	mgr *SessionManager
}

// NewClient creates a new client
func NewClient() *Client {
	return &Client{
		mgr: NewSessionManager(),
	}
}

// Dial connects to a server at the given address and returns a Session
func (c *Client) Dial(network, address string) (*Session, error) {
	netConn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}

	// Create connection (client side, uses odd streams)
	conn := NewConnection(netConn, true)
	conn.Start()

	// Create session
	session := &Session{
		conn: conn,
	}

	// Add session to manager using remote address as key
	key := netConn.RemoteAddr().String()
	c.mgr.AddSession(key, session)

	return session, nil
}

// Close closes all sessions managed by this client
func (c *Client) Close() error {
	c.mgr.CloseAll()
	return nil
}
