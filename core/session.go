package core

import "sync"

// Session represents a session with a server
// It is a virtual connection to the server
type Session struct {
	id   string
	conn *Connection
	// session level configuration, such as timeout, heartbeat detection
}

// SessionManager represents a manager for sessions
// It is responsible for how a session connects to server
type SessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*Session // key can be remote address or specific ID
}

// NewSession creates a session from an active connection.
func NewSession(id string, conn *Connection) *Session {
	return &Session{
		id:   id,
		conn: conn,
	}
}

// NewSessionManager creates a new session manager
func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: make(map[string]*Session),
	}
}

// AddSession adds a session to the manager
func (sm *SessionManager) AddSession(key string, session *Session) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	session.id = key
	sm.sessions[key] = session
}

// GetSession gets a session by key
func (sm *SessionManager) GetSession(key string) (*Session, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	session, exists := sm.sessions[key]
	return session, exists
}

// RemoveSession removes a session from the manager
func (sm *SessionManager) RemoveSession(key string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.sessions, key)
}

// CloseAll closes all sessions
func (sm *SessionManager) CloseAll() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for _, session := range sm.sessions {
		session.Close()
	}
	sm.sessions = make(map[string]*Session)
}

// Close closes the session and its underlying connection
func (s *Session) Close() error {
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}

// CreateStream creates a new stream on this session
func (s *Session) CreateStream() (*Stream, error) {
	if s.conn == nil {
		return nil, ErrConnectionClosed
	}
	return s.conn.CreateStream()
}

// OpenStream creates a new stream on this session.
func (s *Session) OpenStream() (*Stream, error) {
	if s.conn == nil {
		return nil, ErrConnectionClosed
	}
	return s.conn.OpenStream()
}

// ResetStream aborts a stream on this session.
func (s *Session) ResetStream(streamID uint32) error {
	if s.conn == nil {
		return ErrConnectionClosed
	}
	return s.conn.resetStreamLocally(streamID)
}

// AcceptStream waits for the next remotely opened stream on this session.
func (s *Session) AcceptStream() (*Stream, error) {
	if s.conn == nil {
		return nil, ErrConnectionClosed
	}

	select {
	case <-s.conn.done:
		return nil, ErrConnectionClosed
	case stream, ok := <-s.conn.IncomingStreams():
		if !ok {
			return nil, ErrConnectionClosed
		}
		return stream, nil
	}
}

// IncomingStreams returns the stream-accept channel for this session.
func (s *Session) IncomingStreams() <-chan *Stream {
	if s.conn == nil {
		return nil
	}
	return s.conn.IncomingStreams()
}

// GetConnection returns the underlying connection
func (s *Session) GetConnection() *Connection {
	return s.conn
}

func (s *Session) ID() string {
	return s.id
}
