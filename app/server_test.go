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
	go handler.ServeSession(core.NewSession("test-server", serverConn))

	stream, err := clientConn.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
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

	var response Response
	if err := readJSONMessage(stream, &response); err != nil {
		t.Fatalf("readJSONMessage failed: %v", err)
	}

	if !response.OK {
		t.Fatalf("expected OK response, got error %q", response.Error)
	}
	if got := response.Data["message"]; got != "pong" {
		t.Fatalf("expected pong response, got %#v", got)
	}
}

func TestMplHandlerSupportsMultipleMessagesOnOneStream(t *testing.T) {
	clientConn, serverConn := setupAppTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	handler := NewMplHandler()
	go handler.ServeSession(core.NewSession("test-server", serverConn))

	stream, err := clientConn.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	defer stream.Close()

	commands := []*Command{
		{
			Action: "ping",
			Params: map[string]any{
				"seq": 1,
			},
		},
		{
			Action: "ping",
			Params: map[string]any{
				"seq":     2,
				"payload": string(make([]byte, core.DefaultFrameSize+128)),
			},
		},
	}

	for _, cmd := range commands {
		if err := writeJSONMessage(stream, cmd); err != nil {
			t.Fatalf("writeJSONMessage failed: %v", err)
		}
	}

	for i := range commands {
		var response Response
		if err := readJSONMessage(stream, &response); err != nil {
			t.Fatalf("readJSONMessage %d failed: %v", i, err)
		}
		if !response.OK {
			t.Fatalf("expected OK response for message %d, got error %q", i, response.Error)
		}
		if got := response.Data["message"]; got != "pong" {
			t.Fatalf("expected pong response for message %d, got %#v", i, got)
		}
	}
}

func TestClientRoundTripReturnsStructuredResponse(t *testing.T) {
	clientConn, serverConn := setupAppTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	handler := NewMplHandler()
	go handler.ServeSession(core.NewSession("test-server", serverConn))

	client := &Client{
		Client:  core.NewClient(),
		session: core.NewSession("test-client", clientConn),
	}

	resp, err := client.RoundTrip(&Command{Action: "ping"})
	if err != nil {
		t.Fatalf("RoundTrip failed: %v", err)
	}
	if !resp.OK {
		t.Fatalf("expected success response, got error %q", resp.Error)
	}
	if got := resp.Data["message"]; got != "pong" {
		t.Fatalf("expected pong response, got %#v", got)
	}
}

func TestClientRoundTripReturnsCommandError(t *testing.T) {
	clientConn, serverConn := setupAppTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	handler := NewMplHandler()
	go handler.ServeSession(core.NewSession("test-server", serverConn))

	client := &Client{
		Client:  core.NewClient(),
		session: core.NewSession("test-client", clientConn),
	}

	resp, err := client.RoundTrip(&Command{Action: "unknown"})
	if err == nil {
		t.Fatal("expected command error")
	}
	if resp == nil || resp.OK {
		t.Fatalf("expected structured error response, got %#v", resp)
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
