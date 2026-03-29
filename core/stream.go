package core

import (
	"errors"
	"io"
	"sync"
)

const (
	// DefaultFrameSize is the default maximum payload size per frame.
	DefaultFrameSize = 16384
)

var (
	// ErrStreamReset indicates the remote peer reset the stream.
	ErrStreamReset = errors.New("stream reset")
)

// Stream represents an active stream with independent Reader/Writer interfaces.
// It implements io.ReadWriteCloser.
type Stream struct {
	id      uint32
	conn    *Connection
	readBuf chan []byte
	closeCh chan struct{}
	once    sync.Once
	mu      sync.Mutex
	buffer  []byte

	localClosed  bool
	remoteClosed bool
	reset        bool
	connClosed   bool
}

// Read reads data from the stream.
func (s *Stream) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	for {
		s.mu.Lock()
		if len(s.buffer) > 0 {
			n = copy(p, s.buffer)
			s.buffer = s.buffer[n:]
			s.mu.Unlock()
			return n, nil
		}

		if err := s.readTerminalErrorLocked(); err != nil {
			s.mu.Unlock()
			return 0, err
		}
		s.mu.Unlock()

		select {
		case <-s.closeCh:
		case data, ok := <-s.readBuf:
			if !ok {
				return 0, io.EOF
			}

			s.mu.Lock()
			n = copy(p, data)
			if n < len(data) {
				s.buffer = data[n:]
			}
			s.mu.Unlock()
			return n, nil
		}
	}
}

// Write writes data to the stream.
// Data is split into frames and sent via the connection.
func (s *Stream) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	s.mu.Lock()
	err = s.writeTerminalErrorLocked()
	s.mu.Unlock()
	if err != nil {
		return 0, err
	}

	totalWritten := 0
	offset := 0

	for offset < len(p) {
		chunkSize := len(p) - offset
		if chunkSize > DefaultFrameSize {
			chunkSize = DefaultFrameSize
		}

		frame := &Frame{
			Version:  CurrentVersion,
			StreamID: s.id,
			Type:     FrameData,
			Payload:  p[offset : offset+chunkSize],
		}

		if err := s.conn.WriteFrame(frame); err != nil {
			return totalWritten, err
		}

		offset += chunkSize
		totalWritten += chunkSize
	}

	return totalWritten, nil
}

// Close closes the stream.
func (s *Stream) Close() error {
	return s.conn.closeStreamLocally(s.id)
}

// Reset aborts the stream and notifies the remote peer.
func (s *Stream) Reset() error {
	return s.conn.resetStreamLocally(s.id)
}

// ID returns the stream ID.
func (s *Stream) ID() uint32 {
	return s.id
}

// StreamID returns the logical stream ID.
func (s *Stream) StreamID() uint32 {
	return s.id
}

func (s *Stream) signalClose() {
	s.once.Do(func() {
		close(s.closeCh)
	})
}

func (s *Stream) forceConnectionClose() {
	s.mu.Lock()
	s.connClosed = true
	s.mu.Unlock()
	s.signalClose()
}

func (s *Stream) readTerminalErrorLocked() error {
	switch {
	case s.reset:
		return ErrStreamReset
	case s.connClosed:
		return ErrConnectionClosed
	case s.localClosed || s.remoteClosed:
		return io.EOF
	default:
		return nil
	}
}

func (s *Stream) writeTerminalErrorLocked() error {
	switch {
	case s.reset:
		return ErrStreamReset
	case s.connClosed:
		return ErrConnectionClosed
	case s.localClosed || s.remoteClosed:
		return io.ErrClosedPipe
	default:
		return nil
	}
}
