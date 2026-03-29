package core

import (
	"errors"
	"io"
	"net"
	"sync"
)

const (
	// DefaultMaxConcurrentStreams is the default maximum number of concurrent streams.
	DefaultMaxConcurrentStreams = 100
	// ControlStreamID is reserved for control stream.
	ControlStreamID = 0
)

var (
	// ErrMaxStreamsReached indicates the maximum number of streams has been reached.
	ErrMaxStreamsReached = errors.New("maximum concurrent streams reached")
	// ErrInvalidStreamID indicates an invalid stream ID.
	ErrInvalidStreamID = errors.New("invalid stream ID: stream 0 is reserved for control")
	// ErrStreamDirectionMismatch indicates stream direction mismatch.
	ErrStreamDirectionMismatch = errors.New("stream direction mismatch")
	// ErrConnectionClosed indicates the connection is closed.
	ErrConnectionClosed = errors.New("connection closed")
)

// Connection represents a connection to a server.
type Connection struct {
	netConn net.Conn
	framer  *Framer

	mu                   sync.RWMutex
	streams              map[uint32]*Stream
	nextStreamID         uint32
	maxConcurrentStreams uint32
	isClient             bool

	writeCh          chan *Frame
	done             chan struct{}
	once             sync.Once
	incomingStreamCh chan *Stream
}

// NewConnection creates a new connection.
func NewConnection(netConn net.Conn, isClient bool) *Connection {
	var initialStream uint32
	if isClient {
		initialStream = 1
	} else {
		initialStream = 2
	}

	return &Connection{
		netConn:              netConn,
		framer:               NewFramer(),
		streams:              make(map[uint32]*Stream),
		nextStreamID:         initialStream,
		maxConcurrentStreams: DefaultMaxConcurrentStreams,
		isClient:             isClient,
		writeCh:              make(chan *Frame, 100),
		done:                 make(chan struct{}),
		incomingStreamCh:     make(chan *Stream, 32),
	}
}

// SetMaxConcurrentStreams sets the maximum number of concurrent streams.
func (c *Connection) SetMaxConcurrentStreams(max uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxConcurrentStreams = max
}

// Start starts the read and write loops.
func (c *Connection) Start() {
	go c.readLoop()
	go c.writeLoop()
}

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
			return
		}

		if frame.Type == FrameControl {
			if !c.handleControlFrame(frame) {
				return
			}
			continue
		}

		if !c.handleDataFrame(frame) {
			return
		}
	}
}

func (c *Connection) writeLoop() {
	defer c.Close()

	for {
		select {
		case <-c.done:
			return
		case frame, ok := <-c.writeCh:
			if !ok || frame == nil {
				return
			}
			buf := c.framer.EncodeFrame(frame)
			if _, err := c.netConn.Write(buf); err != nil {
				return
			}
		}
	}
}

// CreateStream creates a new stream.
func (c *Connection) CreateStream() (*Stream, error) {
	c.mu.Lock()

	if uint32(len(c.streams)) >= c.maxConcurrentStreams {
		c.mu.Unlock()
		return nil, ErrMaxStreamsReached
	}

	var streamID uint32
	for {
		streamID = c.nextStreamID
		if streamID == ControlStreamID {
			streamID++
		}
		if c.isClient && streamID%2 == 0 {
			streamID++
		}
		if !c.isClient && streamID%2 != 0 {
			streamID++
		}
		if _, exists := c.streams[streamID]; !exists {
			break
		}
		c.nextStreamID = streamID + 2
	}

	c.nextStreamID = streamID + 2

	stream := newStream(c, streamID)
	c.streams[streamID] = stream
	c.mu.Unlock()

	if err := c.WriteFrame(NewControlFrame(streamID, ControlOpen)); err != nil {
		c.mu.Lock()
		delete(c.streams, streamID)
		c.mu.Unlock()
		stream.forceConnectionClose()
		return nil, err
	}

	return stream, nil
}

// OpenStream creates a new local stream.
func (c *Connection) OpenStream() (*Stream, error) {
	return c.CreateStream()
}

// GetStream gets a stream by ID.
func (c *Connection) GetStream(streamID uint32) (*Stream, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	stream, exists := c.streams[streamID]
	return stream, exists
}

// CloseStream removes a stream from the active map.
func (c *Connection) CloseStream(streamID uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.streams, streamID)
}

// WriteFrame writes a frame to the connection.
func (c *Connection) WriteFrame(frame *Frame) error {
	select {
	case <-c.done:
		return ErrConnectionClosed
	case c.writeCh <- frame:
		return nil
	}
}

// IncomingStreams returns a channel that receives remotely opened streams.
func (c *Connection) IncomingStreams() <-chan *Stream {
	return c.incomingStreamCh
}

// Close closes the connection and all streams.
func (c *Connection) Close() error {
	c.once.Do(func() {
		close(c.done)
		close(c.writeCh)
		_ = c.netConn.Close()

		if c.incomingStreamCh != nil {
			close(c.incomingStreamCh)
		}

		c.mu.Lock()
		for streamID, stream := range c.streams {
			stream.forceConnectionClose()
			delete(c.streams, streamID)
		}
		c.mu.Unlock()
	})
	return nil
}

func (c *Connection) deliverToStream(stream *Stream, payload []byte) bool {
	select {
	case stream.readBuf <- payload:
		return true
	case <-stream.closeCh:
		return true
	case <-c.done:
		return false
	}
}

func (c *Connection) handleDataFrame(frame *Frame) bool {
	c.mu.RLock()
	stream, exists := c.streams[frame.StreamID]
	c.mu.RUnlock()
	if !exists {
		_ = c.WriteFrame(NewControlFrame(frame.StreamID, ControlReset))
		return true
	}

	stream.mu.Lock()
	closed := stream.localClosed || stream.remoteClosed || stream.reset || stream.connClosed
	stream.mu.Unlock()
	if closed {
		_ = c.WriteFrame(NewControlFrame(frame.StreamID, ControlReset))
		return true
	}

	return c.deliverToStream(stream, frame.Payload)
}

func (c *Connection) handleControlFrame(frame *Frame) bool {
	switch frame.Control() {
	case ControlOpen:
		return c.handleOpen(frame.StreamID)
	case ControlClose:
		return c.handleRemoteClose(frame.StreamID)
	case ControlReset:
		return c.handleRemoteReset(frame.StreamID)
	default:
		return true
	}
}

func (c *Connection) handleOpen(streamID uint32) bool {
	if streamID == ControlStreamID || !c.isRemoteInitiatedStream(streamID) {
		_ = c.WriteFrame(NewControlFrame(streamID, ControlReset))
		return true
	}

	c.mu.Lock()
	if _, exists := c.streams[streamID]; exists {
		c.mu.Unlock()
		_ = c.WriteFrame(NewControlFrame(streamID, ControlReset))
		return true
	}
	if uint32(len(c.streams)) >= c.maxConcurrentStreams {
		c.mu.Unlock()
		_ = c.WriteFrame(NewControlFrame(streamID, ControlReset))
		return true
	}

	stream := newStream(c, streamID)
	c.streams[streamID] = stream
	c.mu.Unlock()

	if c.incomingStreamCh != nil {
		select {
		case c.incomingStreamCh <- stream:
		case <-c.done:
			return false
		default:
		}
	}

	return true
}

func (c *Connection) handleRemoteClose(streamID uint32) bool {
	c.mu.RLock()
	stream, exists := c.streams[streamID]
	c.mu.RUnlock()
	if !exists {
		_ = c.WriteFrame(NewControlFrame(streamID, ControlReset))
		return true
	}

	stream.mu.Lock()
	alreadyTerminal := stream.remoteClosed || stream.localClosed || stream.reset || stream.connClosed
	if !alreadyTerminal {
		stream.remoteClosed = true
	}
	stream.mu.Unlock()
	if alreadyTerminal {
		_ = c.WriteFrame(NewControlFrame(streamID, ControlReset))
		return true
	}

	stream.signalClose()
	c.CloseStream(streamID)
	return true
}

func (c *Connection) handleRemoteReset(streamID uint32) bool {
	c.mu.RLock()
	stream, exists := c.streams[streamID]
	c.mu.RUnlock()
	if !exists {
		return true
	}

	stream.mu.Lock()
	stream.reset = true
	stream.mu.Unlock()
	stream.signalClose()
	c.CloseStream(streamID)
	return true
}

func (c *Connection) closeStreamLocally(streamID uint32) error {
	c.mu.RLock()
	stream, exists := c.streams[streamID]
	c.mu.RUnlock()
	if !exists {
		return nil
	}

	stream.mu.Lock()
	if stream.localClosed || stream.remoteClosed || stream.reset || stream.connClosed {
		stream.mu.Unlock()
		return nil
	}
	stream.localClosed = true
	stream.mu.Unlock()

	if err := c.WriteFrame(NewControlFrame(streamID, ControlClose)); err != nil {
		return err
	}

	stream.signalClose()
	c.CloseStream(streamID)
	return nil
}

func (c *Connection) resetStreamLocally(streamID uint32) error {
	c.mu.RLock()
	stream, exists := c.streams[streamID]
	c.mu.RUnlock()
	if !exists {
		return nil
	}

	stream.mu.Lock()
	if stream.reset || stream.connClosed {
		stream.mu.Unlock()
		return nil
	}
	stream.reset = true
	stream.mu.Unlock()

	if err := c.WriteFrame(NewControlFrame(streamID, ControlReset)); err != nil {
		return err
	}

	stream.signalClose()
	c.CloseStream(streamID)
	return nil
}

func (c *Connection) isRemoteInitiatedStream(streamID uint32) bool {
	if streamID == ControlStreamID {
		return false
	}
	if c.isClient {
		return streamID%2 == 0
	}
	return streamID%2 != 0
}

func newStream(conn *Connection, streamID uint32) *Stream {
	return &Stream{
		id:      streamID,
		conn:    conn,
		readBuf: make(chan []byte, 10),
		closeCh: make(chan struct{}),
	}
}
