package app

import (
	"fmt"
	"io"
	"log/slog"

	"curve/core"
)

// Command represents a command
type Command struct {
	Action string                 `json:"action"` // command type
	Params map[string]interface{} `json:"params"` // command parameters
}

// Response represents an application response message.
type Response struct {
	OK    bool           `json:"ok"`
	Data  map[string]any `json:"data,omitempty"`
	Error string         `json:"error,omitempty"`
}

// Client represents an application layer client
type Client struct {
	*core.Client
	session *core.Session
}

// NewClient 创建一个新的客户端并连接到服务器
func NewClient(server string) (*Client, error) {
	client := core.NewClient()
	session, err := client.Dial("tcp", server)
	if err != nil {
		return nil, fmt.Errorf("failed to dial server: %w", err)
	}
	slog.Info("Connected to server:", "server", server, "session", session.ID())
	return &Client{
		Client:  client,
		session: session,
	}, nil
}

// SendCommand sends a command
func (c *Client) SendCommand(cmd *Command) error {
	_, err := c.RoundTrip(cmd)
	return err
}

// RoundTrip sends a command and waits for a single response on the same stream.
func (c *Client) RoundTrip(cmd *Command) (*Response, error) {
	slog.Info("Sending command:", "command", cmd)
	stream, err := c.session.OpenStream()
	if err != nil {
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}
	defer stream.Close()

	if err := writeJSONMessage(stream, cmd); err != nil {
		return nil, fmt.Errorf("failed to send command: %w", err)
	}

	var resp Response
	if err := readJSONMessage(stream, &resp); err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	if !resp.OK {
		return &resp, fmt.Errorf("command failed: %s", resp.Error)
	}

	return &resp, nil
}

// MplHandler represents a multiplexer handler
type MplHandler struct {
}

// NewMplHandler creates a new multiplexer handler
func NewMplHandler() *MplHandler {
	return &MplHandler{}
}

// ServeSession implements core.Handler interface, handling streams symmetrically
// through the session-level incoming stream API.
func (s *MplHandler) ServeSession(session *core.Session) {
	for stream := range session.IncomingStreams() {
		// Handle each client-initiated stream in a goroutine
		go s.handleStream(stream)
	}
}

// handleStream reads data from a stream and processes commands
func (s *MplHandler) handleStream(stream *core.Stream) {
	defer stream.Close()

	for {
		var cmd Command
		err := readJSONMessage(stream, &cmd)
		if err != nil {
			if err == io.EOF {
				slog.Info("EOF reading from stream:", "stream", stream.ID())
				break
			}
			slog.Error("Error reading from stream:", "error", err)
			break
		}

		s.handleCommand(&cmd, stream)
	}
}

// handleCommand handles a decoded command message.
func (s *MplHandler) handleCommand(cmd *Command, stream *core.Stream) {
	slog.Info("Received command:", "command", cmd.Action, "stream", stream.ID())
	// execute command logic
	switch cmd.Action {
	case "ping":
		slog.Info("Received ping command")
		s.sendSuccess(stream, map[string]any{"message": "pong"})
	case "upload":
		slog.Info("Received upload command")
		s.sendSuccess(stream, map[string]any{"status": "accepted"})
	default:
		slog.Info("Unknown command:", "command", cmd.Action)
		s.sendError(stream, fmt.Sprintf("unknown command: %s", cmd.Action))
	}
}

func (s *MplHandler) sendSuccess(stream *core.Stream, data map[string]any) {
	if err := writeJSONMessage(stream, &Response{
		OK:   true,
		Data: data,
	}); err != nil {
		slog.Error("Error sending response:", "error", err)
	}
}

func (s *MplHandler) sendError(stream *core.Stream, message string) {
	if err := writeJSONMessage(stream, &Response{
		OK:    false,
		Error: message,
	}); err != nil {
		slog.Error("Error sending error response:", "error", err)
	}
}
