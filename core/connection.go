package core

import (
	"net"
	"sync"
)

// Connection represents a connection to a server
type Connection struct {
	netConn net.Conn
	framer  *Framer

	// stream management
	mu           sync.Mutex
	streams      map[uint32]*Stream
	nextStreamID uint32

	// signal and channel
	writeCh chan *Frame // all streams share this write channel
	done    chan struct{}
}
