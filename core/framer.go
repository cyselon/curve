package core

import (
	"io"
)

// Framer is a stateless module that converts between raw bytes and Frame objects.
// It handles the binary protocol specification.
type Framer struct {
	// Protocol configuration can be added here, such as MaxFrameSize
}

// NewFramer creates a new stateless framer
func NewFramer() *Framer {
	return &Framer{}
}

// EncodeFrame encodes a frame into bytes
func (f *Framer) EncodeFrame(frame *Frame) []byte {
	return frame.Encode()
}

// DecodeFrame decodes bytes from reader into a frame
func (f *Framer) DecodeFrame(r io.Reader) (*Frame, error) {
	frame := &Frame{}
	err := frame.Decode(r)
	if err != nil {
		return nil, err
	}
	return frame, nil
}
