package core

import "sync"

type Session struct {
	conn *Connection
	// session level configuration, such as timeout, heartbeat detection
}

type SessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*Session // key can be remote address or specific ID
}
