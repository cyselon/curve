package app

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"curve/core"
)

func setupAppTestConnection(t *testing.T) (clientConn, serverConn *core.Connection) {
	t.Helper()

	clientNet, serverNet := net.Pipe()
	clientConn = core.NewConnection(clientNet, true)
	serverConn = core.NewConnection(serverNet, false)
	clientConn.Start()
	serverConn.Start()
	return clientConn, serverConn
}

func waitForRemoteStream(t *testing.T, conn *core.Connection, streamID uint32) *core.Stream {
	t.Helper()

	for i := 0; i < 200; i++ {
		if stream, ok := conn.GetStream(streamID); ok {
			return stream
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for stream %d", streamID)
	return nil
}

func TestMplHandlerPingRoundTripWithLengthPrefixedJSON(t *testing.T) {
	clientConn, serverConn := setupAppTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	handler := NewMplHandler()
	go handler.ServeConn(serverConn)

	stream, err := clientConn.CreateStream()
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}
	defer stream.Close()

	cmd := &Command{
		Action: "ping",
		Params: map[string]any{
			"payload": string(make([]byte, core.DefaultFrameSize*2)),
		},
	}

	if err := writeJSONMessage(stream, cmd); err != nil {
		t.Fatalf("writeJSONMessage failed: %v", err)
	}

	var response map[string]any
	if err := readJSONMessage(stream, &response); err != nil {
		t.Fatalf("readJSONMessage failed: %v", err)
	}

	if got := response["message"]; got != "pong" {
		t.Fatalf("expected pong response, got %#v", got)
	}
}

func TestReadJSONMessageReturnsEOFOnTruncatedPayload(t *testing.T) {
	var cmd Command
	err := readJSONMessage(bytes.NewReader([]byte{0, 0, 0, 1}), &cmd)
	if err == nil {
		t.Fatal("expected error reading truncated message")
	}
	if err != io.EOF && err != io.ErrUnexpectedEOF {
		t.Fatalf("expected EOF-style error, got %v", err)
	}
}
