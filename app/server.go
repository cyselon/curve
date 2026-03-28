package app

import (
	"encoding/json"
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

	// encode command to JSON
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	// write data to stream (will be split into frames automatically)
	_, err = stream.Write(data)
	return err
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

	buf := make([]byte, 4096)
	for {
		n, err := stream.Read(buf)
		if err != nil {
			if err == io.EOF {
				slog.Info("EOF reading from stream:", "stream", stream.ID())
				break
			}
			slog.Error("Error reading from stream:", "error", err)
			break
		}

		// handle received data
		s.handleData(buf[:n], stream)
	}
}

// handleData handles received data
func (s *MplHandler) handleData(data []byte, stream *core.Stream) {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		slog.Error("Error decoding command:", "error", err, "stream", stream.ID(), "size", len(data), "data", string(data))
		return
	}
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
	response, err := json.Marshal(data)
	if err != nil {
		slog.Error("Error encoding response:", "error", err)
		return
	}

	// use stream's Write method to send response
	_, err = stream.Write(response)
	if err != nil {
		slog.Error("Error sending response:", "error", err)
	}
}
