package core

import (
	"io"
	"net"
	"sync"
	"time"
)

const (
	// MaxFrameDataSize is the maximum frame data size (excluding header)
	// If data exceeds this size, it will be split into multiple frames
	MaxFrameDataSize = 64 * 1024 // 64KB
)

// Stream represents an active stream with independent Reader/Writer interfaces
type Stream struct {
	streamID uint32      // stream ID
	conn     *Connection // parent connection
	readBuf  []byte      // read buffer
	readMu   sync.Mutex  // read mutex
	readCond *sync.Cond  // read condition variable for waiting data
	closed   bool        // whether stream is closed
	closeMu  sync.Mutex  // close mutex
}

// newStream creates a new stream
func newStream(streamID uint32, conn *Connection) *Stream {
	s := &Stream{
		streamID: streamID,
		conn:     conn,
		readBuf:  make([]byte, 0),
		closed:   false,
	}
	s.readCond = sync.NewCond(&s.readMu)
	return s
}

// Read reads data from the stream
func (s *Stream) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return 0, io.EOF
	}
	s.closeMu.Unlock()

	// Read from buffer
	s.readMu.Lock()
	defer s.readMu.Unlock()

	// Wait for data if buffer is empty
	for len(s.readBuf) == 0 {
		s.closeMu.Lock()
		if s.closed {
			s.closeMu.Unlock()
			return 0, io.EOF
		}
		s.closeMu.Unlock()

		// Wait for data to arrive (via condition variable)
		s.readCond.Wait()
	}

	// Copy data from buffer
	n = copy(p, s.readBuf)
	s.readBuf = s.readBuf[n:]
	return n, nil
}

// Write writes data to the stream
func (s *Stream) Write(p []byte) (n int, err error) {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return 0, io.ErrClosedPipe
	}
	s.closeMu.Unlock()

	return s.conn.writeToStream(s.streamID, p)
}

// Close closes the stream
func (s *Stream) Close() error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil
	}
	s.closed = true
	s.closeMu.Unlock()

	// Wake up waiting readers
	s.readMu.Lock()
	s.readCond.Broadcast()
	s.readMu.Unlock()

	return s.conn.CloseStream(s.streamID)
}

// StreamID returns the stream ID
func (s *Stream) StreamID() uint32 {
	return s.streamID
}

// writeFrame writes frame data to the stream's buffer (called internally by Connection)
func (s *Stream) writeFrame(data []byte) {
	s.readMu.Lock()
	defer s.readMu.Unlock()
	s.readBuf = append(s.readBuf, data...)
	// Notify waiting readers
	s.readCond.Signal()
}

// Connection wraps a TCP connection and implements Reader/Writer interfaces via Multiplexer
// Connection splits data into frames for transmission and collects frames to reconstruct data
// Connection manages multiple active streams
type Connection struct {
	conn        net.Conn           // underlying TCP connection
	mux         *Multiplexer       // multiplexer
	streams     map[uint32]*Stream // map of active streams
	streamsMu   sync.RWMutex       // streams map mutex
	closed      bool               // whether connection is closed
	closeMu     sync.Mutex         // close state mutex
	receiveDone chan struct{}      // receive loop completion signal
	isClient    bool               // whether this is a client
}

// NewConnection creates a new Connection
// isClient: true for client (uses odd streams), false for server (uses even streams)
func NewConnection(conn net.Conn, isClient bool) *Connection {
	mux := NewMultiplexer(conn, isClient)

	c := &Connection{
		conn:        conn,
		mux:         mux,
		streams:     make(map[uint32]*Stream),
		closed:      false,
		receiveDone: make(chan struct{}),
		isClient:    isClient,
	}

	// Start background frame receiving loop
	go c.receiveFrames()

	return c
}

// receiveFrames receives frames in background and distributes them to corresponding streams
func (c *Connection) receiveFrames() {
	defer close(c.receiveDone)

	for {
		// Check if connection is closed
		c.closeMu.Lock()
		closed := c.closed
		c.closeMu.Unlock()

		if closed {
			return
		}

		// Set read deadline to avoid permanent blocking
		c.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

		// Receive frame
		frame, err := c.mux.ReceiveFrame()
		if err != nil {
			// Check if it's a timeout error
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// Timeout, continue loop to check close status
				continue
			}
			// Other errors (e.g., connection closed), exit loop
			return
		}

		// Clear read deadline
		c.conn.SetReadDeadline(time.Time{})

		// Distribute frame to corresponding stream
		c.streamsMu.RLock()
		stream, exists := c.streams[frame.Header.StreamID]
		c.streamsMu.RUnlock()

		if exists {
			stream.writeFrame(frame.Data)
		}
		// If stream doesn't exist, ignore the frame (may be from a closed stream)
	}
}

// OpenStream creates a new stream and returns it
func (c *Connection) OpenStream() (*Stream, error) {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return nil, io.ErrClosedPipe
	}
	c.closeMu.Unlock()

	// Create new stream
	streamID, _, err := c.mux.CreateStream()
	if err != nil {
		return nil, err
	}

	newStream := newStream(streamID, c)

	// Register stream
	c.streamsMu.Lock()
	c.streams[streamID] = newStream
	c.streamsMu.Unlock()

	return newStream, nil
}

// CloseStream closes the specified stream
func (c *Connection) CloseStream(streamID uint32) error {
	c.streamsMu.Lock()
	stream, exists := c.streams[streamID]
	if exists {
		delete(c.streams, streamID)
	}
	c.streamsMu.Unlock()

	if exists {
		stream.closeMu.Lock()
		stream.closed = true
		stream.closeMu.Unlock()
		c.mux.CloseStream(streamID)
	}

	return nil
}

// writeToStream writes data to the specified stream (called by Stream.Write)
func (c *Connection) writeToStream(streamID uint32, p []byte) (n int, err error) {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return 0, io.ErrClosedPipe
	}
	c.closeMu.Unlock()

	totalWritten := 0

	// If data is too large, split it into multiple frames
	for len(p) > 0 {
		chunkSize := MaxFrameDataSize
		if len(p) < MaxFrameDataSize {
			chunkSize = len(p)
		}

		chunk := p[:chunkSize]
		p = p[chunkSize:]

		frame := &Frame{
			Header: Header{
				Version:  FrameVersion,
				Flags:    FrameFlags,
				StreamID: streamID,
				Length:   uint32(len(chunk)),
			},
			Data: chunk,
		}

		if err := c.mux.SendFrame(frame); err != nil {
			return totalWritten, err
		}

		totalWritten += chunkSize
	}

	return totalWritten, nil
}

// Read implements io.Reader interface (uses default stream, backward compatible)
// Note: It's recommended to use OpenStream() to create independent streams
func (c *Connection) Read(p []byte) (n int, err error) {
	// Get or create default stream
	stream, err := c.getOrCreateDefaultStream()
	if err != nil {
		return 0, err
	}
	return stream.Read(p)
}

// Write implements io.Writer interface (uses default stream, backward compatible)
// Note: It's recommended to use OpenStream() to create independent streams
func (c *Connection) Write(p []byte) (n int, err error) {
	stream, err := c.getOrCreateDefaultStream()
	if err != nil {
		return 0, err
	}
	return stream.Write(p)
}

// getOrCreateDefaultStream gets or creates the default stream
func (c *Connection) getOrCreateDefaultStream() (*Stream, error) {
	var defaultStreamID uint32
	if c.isClient {
		defaultStreamID = 1
	} else {
		defaultStreamID = 2
	}

	c.streamsMu.RLock()
	stream, exists := c.streams[defaultStreamID]
	c.streamsMu.RUnlock()

	if exists {
		return stream, nil
	}

	// Create default stream
	c.streamsMu.Lock()
	// Double-check
	if stream, exists := c.streams[defaultStreamID]; exists {
		c.streamsMu.Unlock()
		return stream, nil
	}

	// Create new stream
	streamID, _, err := c.mux.CreateStream()
	if err != nil {
		c.streamsMu.Unlock()
		return nil, err
	}

	stream = newStream(streamID, c)
	c.streams[streamID] = stream
	c.streamsMu.Unlock()

	return stream, nil
}

// Close closes the connection and all active streams
func (c *Connection) Close() error {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return nil
	}
	c.closed = true
	c.closeMu.Unlock()

	// Close all streams
	c.streamsMu.Lock()
	for streamID, stream := range c.streams {
		stream.closeMu.Lock()
		stream.closed = true
		stream.closeMu.Unlock()
		c.mux.CloseStream(streamID)
	}
	c.streams = make(map[uint32]*Stream)
	c.streamsMu.Unlock()

	// Close underlying connection (this will interrupt ReceiveFrame blocking)
	c.conn.Close()

	// Wait for receive loop to finish (with timeout to avoid permanent blocking)
	select {
	case <-c.receiveDone:
	case <-time.After(1 * time.Second):
		// Timeout, force exit
	}

	return nil
}

// LocalAddr returns the local network address
func (c *Connection) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr returns the remote network address
func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines
func (c *Connection) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

// SetReadDeadline sets the read deadline
func (c *Connection) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline
func (c *Connection) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// GetMultiplexer returns the internal Multiplexer
func (c *Connection) GetMultiplexer() *Multiplexer {
	return c.mux
}

// GetActiveStreams returns all active stream IDs
func (c *Connection) GetActiveStreams() []uint32 {
	c.streamsMu.RLock()
	defer c.streamsMu.RUnlock()

	streamIDs := make([]uint32, 0, len(c.streams))
	for streamID := range c.streams {
		streamIDs = append(streamIDs, streamID)
	}
	return streamIDs
}

// GetStream returns the stream by stream ID
func (c *Connection) GetStream(streamID uint32) (*Stream, bool) {
	c.streamsMu.RLock()
	defer c.streamsMu.RUnlock()
	stream, exists := c.streams[streamID]
	return stream, exists
}
