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
	slog.Info("Sending command:", "command", cmd)
	// create stream
	stream, err := c.session.CreateStream()
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}
	defer stream.Close()

	if err := writeJSONMessage(stream, cmd); err != nil {
		return fmt.Errorf("failed to send command: %w", err)
	}
	return nil
}

// MplHandler represents a multiplexer handler
type MplHandler struct {
}

// NewMplHandler creates a new multiplexer handler
func NewMplHandler() *MplHandler {
	return &MplHandler{}
}

// ServeConn implements core.Handler interface, handling connection
// Reads from IncomingStreams() - streams auto-created when client sends data (odd stream IDs)
func (s *MplHandler) ServeConn(conn *core.Connection) {
	for stream := range conn.IncomingStreams() {
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
		response := map[string]any{"message": "pong"}
		s.sendResponse(stream, response)
	case "upload":
		slog.Info("Received upload command")
		// handle upload logic
	default:
		slog.Info("Unknown command:", "command", cmd.Action)
	}
}

// sendResponse sends a response
func (s *MplHandler) sendResponse(stream *core.Stream, data map[string]interface{}) {
	if err := writeJSONMessage(stream, data); err != nil {
		slog.Error("Error sending response:", "error", err)
	}
}
