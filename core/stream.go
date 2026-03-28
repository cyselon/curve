package core

import (
	"io"
	"sync"
)

const (
	// DefaultFrameSize is the default maximum payload size per frame
	DefaultFrameSize = 16384 // 16KB
)

// Stream represents an active stream with independent Reader/Writer interfaces
// It implements io.ReadWriteCloser
type Stream struct {
	id      uint32
	conn    *Connection   // parent connection
	readBuf chan []byte   // data channel from connection
	closeCh chan struct{} // close channel
	once    sync.Once
	mu      sync.Mutex
	buffer  []byte // internal buffer for partial reads
}

// Read reads data from the stream
func (s *Stream) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	if s.isClosed() {
		return 0, io.EOF
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// First, try to read from internal buffer
	if len(s.buffer) > 0 {
		n = copy(p, s.buffer)
		s.buffer = s.buffer[n:]
		if len(p) > n {
			// Still have space, try to read more from channel
			// But we can't block here, so return what we have
			return n, nil
		}
		return n, nil
	}

	// No buffer, read from channel
	select {
	case <-s.closeCh:
		return 0, io.EOF
	case data, ok := <-s.readBuf:
		if !ok {
			return 0, io.EOF
		}
		n = copy(p, data)
		// Store remaining data in buffer
		if n < len(data) {
			s.buffer = data[n:]
		}
		return n, nil
	}
}

// Write writes data to the stream
// Data is split into frames and sent via the connection
func (s *Stream) Write(p []byte) (n int, err error) {
	if s.isClosed() {
		return 0, io.ErrClosedPipe
	}

	totalWritten := 0
	offset := 0

	for offset < len(p) {
		// Determine how much to send in this frame
		chunkSize := len(p) - offset
		if chunkSize > DefaultFrameSize {
			chunkSize = DefaultFrameSize
		}

		// Create frame
		frame := &Frame{
			Version:  CurrentVersion,
			StreamID: s.id,
			Type:     FrameData,
			Flags:    0,
			Payload:  p[offset : offset+chunkSize],
		}

		// Send frame
		if err := s.conn.WriteFrame(frame); err != nil {
			return totalWritten, err
		}

		offset += chunkSize
		totalWritten += chunkSize
	}

	return totalWritten, nil
}

// Close closes the stream
func (s *Stream) Close() error {
	s.once.Do(func() {
		close(s.closeCh)
		s.conn.CloseStream(s.id)
	})
	return nil
}

// ID returns the stream ID
func (s *Stream) ID() uint32 {
	return s.id
}

func (s *Stream) isClosed() bool {
	select {
	case <-s.closeCh:
		return true
	default:
		return false
	}
}
