package core

import (
	"errors"
	"net"
	"sync"
)

const (
	// DefaultMaxConcurrentStreams is the default maximum number of concurrent streams
	DefaultMaxConcurrentStreams = 100
	// ControlStreamID is reserved for control stream
	ControlStreamID = 0
)

var (
	// ErrMaxStreamsReached indicates the maximum number of streams has been reached
	ErrMaxStreamsReached = errors.New("maximum concurrent streams reached")
	// ErrInvalidStreamID indicates an invalid stream ID (stream 0 is reserved)
	ErrInvalidStreamID = errors.New("invalid stream ID: stream 0 is reserved for control")
	// ErrStreamDirectionMismatch indicates stream direction mismatch
	ErrStreamDirectionMismatch = errors.New("stream direction mismatch")
)

// Framer is a Framer for managing streams
type Framer struct {
	conn                 net.Conn
	streams              map[uint32]chan []byte // map of stream ID to data channel
	nextStream           uint32                 // next stream ID
	maxConcurrentStreams uint32                 // maximum number of concurrent streams
	isClient             bool                   // whether this is a client (clients use odd streams, servers use even streams)
	mu                   sync.Mutex
}

// NewFramer creates a new framer
// isClient: true for client (uses odd streams), false for server (uses even streams)
func NewFramer(conn net.Conn, isClient bool) *Framer {
	var initialStream uint32
	if isClient {
		initialStream = 1 // client starts from 1 (odd)
	} else {
		initialStream = 2 // server starts from 2 (even)
	}

	return &Framer{
		conn:                 conn,
		streams:              make(map[uint32]chan []byte),
		nextStream:           initialStream,
		maxConcurrentStreams: DefaultMaxConcurrentStreams,
		isClient:             isClient,
	}
}

// SetMaxConcurrentStreams sets the maximum number of concurrent streams
func (m *Framer) SetMaxConcurrentStreams(max uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.maxConcurrentStreams = max
}

// GetMaxConcurrentStreams returns the maximum number of concurrent streams
func (m *Framer) GetMaxConcurrentStreams() uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.maxConcurrentStreams
}

// SendFrame sends a frame
func (m *Framer) SendFrame(frame *Frame) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Encode frame using Encode
	buf := frame.Encode()

	// Send data
	_, err := m.conn.Write(buf)
	return err
}

// ReceiveFrame receives a frame
func (m *Framer) ReceiveFrame() (*Frame, error) {
	frame := &Frame{}
	err := frame.Decode(m.conn)
	if err != nil {
		return nil, err
	}
	return frame, nil
}

// CreateStream creates a new stream
// Clients create odd streams (1, 3, 5...), servers create even streams (2, 4, 6...)
// Returns stream ID and data channel, or error if maximum stream limit is reached
func (m *Framer) CreateStream() (uint32, chan []byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if maximum stream limit is reached
	if uint32(len(m.streams)) >= m.maxConcurrentStreams {
		return 0, nil, ErrMaxStreamsReached
	}

	// Find next valid stream ID
	var streamID uint32
	for {
		streamID = m.nextStream

		// Ensure stream ID is not 0 (0 is reserved for control stream)
		if streamID == ControlStreamID {
			streamID++
		}

		// Ensure stream ID matches direction requirements
		if m.isClient {
			// Client must use odd streams
			if streamID%2 == 0 {
				streamID++
			}
		} else {
			// Server must use even streams
			if streamID%2 != 0 {
				streamID++
			}
		}

		// Check if stream ID is already in use
		if _, exists := m.streams[streamID]; !exists {
			break
		}

		// Stream ID is already in use, try next
		m.nextStream = streamID + 2 // Skip by 2 (to maintain odd/even)
	}

	// Update next stream ID (skip by 2 to maintain odd/even)
	m.nextStream = streamID + 2

	ch := make(chan []byte, 10) // buffered channel
	m.streams[streamID] = ch

	return streamID, ch, nil
}

// ValidateStreamID validates if a stream ID is valid
// Checks if stream ID is 0 (reserved) and if it matches direction requirements
func (m *Framer) ValidateStreamID(streamID uint32) error {
	if streamID == ControlStreamID {
		return ErrInvalidStreamID
	}

	// Check stream direction
	if m.isClient {
		// Client-created streams should be odd
		if streamID%2 == 0 {
			return ErrStreamDirectionMismatch
		}
	} else {
		// Server-created streams should be even
		if streamID%2 != 0 {
			return ErrStreamDirectionMismatch
		}
	}

	return nil
}

// CloseStream closes a stream
func (m *Framer) CloseStream(streamID uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ch, ok := m.streams[streamID]; ok {
		close(ch)
		delete(m.streams, streamID)
	}
}
