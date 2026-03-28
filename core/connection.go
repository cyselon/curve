package core

import (
	"errors"
	"io"
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
	// ErrConnectionClosed indicates the connection is closed
	ErrConnectionClosed = errors.New("connection closed")
)

// Connection represents a connection to a server
// It manages the physical net.Conn, handles stream lifecycle, read/write loops,
// and dispatches frames to the correct Stream
type Connection struct {
	netConn net.Conn
	framer  *Framer

	// stream management
	mu                  sync.RWMutex
	streams             map[uint32]*Stream
	nextStreamID        uint32
	maxConcurrentStreams uint32
	isClient             bool // true for client (uses odd streams), false for server (uses even streams)

	// signal and channel
	writeCh          chan *Frame // all streams share this write channel
	done             chan struct{}
	once             sync.Once
	incomingStreamCh chan *Stream // receives streams when auto-created from incoming data
}

// NewConnection creates a new connection
// isClient: true for client (uses odd streams), false for server (uses even streams)
func NewConnection(netConn net.Conn, isClient bool) *Connection {
	var initialStream uint32
	if isClient {
		initialStream = 1 // client starts from 1 (odd)
	} else {
		initialStream = 2 // server starts from 2 (even)
	}

	return &Connection{
		netConn:              netConn,
		framer:               NewFramer(),
		streams:               make(map[uint32]*Stream),
		nextStreamID:          initialStream,
		maxConcurrentStreams: DefaultMaxConcurrentStreams,
		isClient:             isClient,
		writeCh:               make(chan *Frame, 100),
		done:                  make(chan struct{}),
		incomingStreamCh:     make(chan *Stream, 32),
	}
}

// SetMaxConcurrentStreams sets the maximum number of concurrent streams
func (c *Connection) SetMaxConcurrentStreams(max uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxConcurrentStreams = max
}

// Start starts the read and write loops
func (c *Connection) Start() {
	go c.readLoop()
	go c.writeLoop()
}

// readLoop continuously reads frames from the connection and dispatches them to streams
func (c *Connection) readLoop() {
	defer c.Close()

	for {
		select {
		case <-c.done:
			return
		default:
		}

		frame, err := c.framer.DecodeFrame(c.netConn)
		if err != nil {
			if err == io.EOF {
				return
			}
			// Handle other errors, maybe log them
			return
		}

		// Dispatch frame to the correct stream
		c.mu.RLock()
		stream, exists := c.streams[frame.StreamID]
		c.mu.RUnlock()

		if exists {
			select {
			case stream.readBuf <- frame.Payload:
			case <-c.done:
				return
			}
		} else if frame.StreamID == ControlStreamID {
			// Handle control frames
			// TODO: implement control frame handling
		} else {
			// Auto-create stream for incoming frames from the other side
			// Client creates odd streams, server receives them
			// Server creates even streams, client receives them
			isIncomingStream := false
			if c.isClient {
				// Client receives even streams from server
				isIncomingStream = frame.StreamID%2 == 0
			} else {
				// Server receives odd streams from client
				isIncomingStream = frame.StreamID%2 != 0
			}

			if isIncomingStream {
				// Create stream for receiving data
				isNewStream := false
				c.mu.Lock()
				// Check again after acquiring lock
				if _, stillExists := c.streams[frame.StreamID]; !stillExists {
					// Check stream limit
					if uint32(len(c.streams)) < c.maxConcurrentStreams {
						stream = &Stream{
							id:      frame.StreamID,
							conn:    c,
							readBuf: make(chan []byte, 10),
							closeCh: make(chan struct{}),
						}
						c.streams[frame.StreamID] = stream
						isNewStream = true
					}
				} else {
					stream = c.streams[frame.StreamID]
				}
				c.mu.Unlock()

				if stream != nil {
					// Notify handler of new stream (non-blocking)
					if isNewStream && c.incomingStreamCh != nil {
						select {
						case c.incomingStreamCh <- stream:
						case <-c.done:
							return
						default:
							// channel full, handler will poll GetStream if needed
						}
					}
					select {
					case stream.readBuf <- frame.Payload:
					case <-c.done:
						return
					}
				}
			}
		}
	}
}

// writeLoop continuously writes frames from the write channel to the connection
func (c *Connection) writeLoop() {
	defer c.Close()

	for {
		select {
		case <-c.done:
			return
		case frame := <-c.writeCh:
			if frame == nil {
				return
			}
			buf := c.framer.EncodeFrame(frame)
			if _, err := c.netConn.Write(buf); err != nil {
				return
			}
		}
	}
}

// CreateStream creates a new stream
// Returns the stream or error if maximum stream limit is reached
func (c *Connection) CreateStream() (*Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if maximum stream limit is reached
	if uint32(len(c.streams)) >= c.maxConcurrentStreams {
		return nil, ErrMaxStreamsReached
	}

	// Find next valid stream ID
	var streamID uint32
	for {
		streamID = c.nextStreamID

		// Ensure stream ID is not 0 (0 is reserved for control stream)
		if streamID == ControlStreamID {
			streamID++
		}

		// Ensure stream ID matches direction requirements
		if c.isClient {
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
		if _, exists := c.streams[streamID]; !exists {
			break
		}

		// Stream ID is already in use, try next
		c.nextStreamID = streamID + 2 // Skip by 2 (to maintain odd/even)
	}

	// Update next stream ID (skip by 2 to maintain odd/even)
	c.nextStreamID = streamID + 2

	// Create stream
	stream := &Stream{
		id:      streamID,
		conn:    c,
		readBuf: make(chan []byte, 10),
		closeCh: make(chan struct{}),
	}

	c.streams[streamID] = stream
	return stream, nil
}

// GetStream gets a stream by ID
func (c *Connection) GetStream(streamID uint32) (*Stream, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	stream, exists := c.streams[streamID]
	return stream, exists
}

// CloseStream closes a stream
func (c *Connection) CloseStream(streamID uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if stream, ok := c.streams[streamID]; ok {
		close(stream.closeCh)
		close(stream.readBuf)
		delete(c.streams, streamID)
	}
}

// WriteFrame writes a frame to the connection
func (c *Connection) WriteFrame(frame *Frame) error {
	select {
	case <-c.done:
		return ErrConnectionClosed
	case c.writeCh <- frame:
		return nil
	}
}

// IncomingStreams returns a channel that receives streams when they are auto-created
// from incoming data (e.g. server receives odd streams from client).
// Handler should read from this to process client-initiated streams.
func (c *Connection) IncomingStreams() <-chan *Stream {
	return c.incomingStreamCh
}

// Close closes the connection and all streams
func (c *Connection) Close() error {
	c.once.Do(func() {
		close(c.done)
		c.netConn.Close()

		// Close incoming stream channel so handler's range loop exits
		if c.incomingStreamCh != nil {
			close(c.incomingStreamCh)
		}

		// Close all streams
		c.mu.Lock()
		for streamID, stream := range c.streams {
			close(stream.closeCh)
			close(stream.readBuf)
			delete(c.streams, streamID)
		}
		c.mu.Unlock()
	})
	return nil
}
