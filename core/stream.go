package core

// Stream represents an active stream with independent Reader/Writer interfaces
type Stream struct {
	id      uint32
	conn    *Connection   // parent connection
	readBuf chan []byte   // data channel from connection
	closeCh chan struct{} // close channel
}

// Read reads data from the stream
func (s *Stream) Read(p []byte) (n int, err error) {
	return s.readBuf.Read(p)
}

// Write writes data to the stream
func (s *Stream) Write(p []byte) (n int, err error) {
	return s.conn.Write(p)
}
