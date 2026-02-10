package app

import (
	"encoding/json"
	"fmt"
	"io"

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
	return &Client{
		Client:  client,
		session: session,
	}, nil
}

// SendCommand sends a command
func (c *Client) SendCommand(cmd *Command) error {
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
// server will receive data from client created odd stream
// Connection's readLoop will automatically create the receiving stream
// here we create a server side stream for demonstration, in actual application, should handle client created stream
func (s *MplHandler) ServeConn(conn *core.Connection) {
	// create a server side stream for handling request
	// note: in actual application, should handle client created odd stream
	// here we create a even stream for demonstration
	stream, err := conn.CreateStream()
	if err != nil {
		fmt.Printf("Error creating stream: %v\n", err)
		return
	}
	defer stream.Close()

	// read data and handle
	buf := make([]byte, 4096)
	for {
		n, err := stream.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error reading from stream: %v\n", err)
			break
		}

		// handle received data
		s.handleData(buf[:n], stream, conn)
	}
}

// handleData handles received data
func (s *MplHandler) handleData(data []byte, stream *core.Stream, conn *core.Connection) {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		fmt.Printf("Error decoding command: %v, stream: %d, size: %d, data: %s\n", err, stream.ID(), len(data), string(data))
		return
	}

	// execute command logic
	switch cmd.Action {
	case "ping":
		fmt.Println("Received ping command")
		response := map[string]any{"message": "pong"}
		s.sendResponse(stream, response)
	case "upload":
		fmt.Println("Received upload command")
		// handle upload logic
	default:
		fmt.Println("Unknown command:", cmd.Action)
	}
}

// sendResponse sends a response
func (s *MplHandler) sendResponse(stream *core.Stream, data map[string]interface{}) {
	response, err := json.Marshal(data)
	if err != nil {
		fmt.Println("Error encoding response:", err)
		return
	}

	// use stream's Write method to send response
	_, err = stream.Write(response)
	if err != nil {
		fmt.Println("Error sending response:", err)
	}
}
