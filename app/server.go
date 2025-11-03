package app

import (
	"encoding/json"
	"fmt"
	"net"

	"curve/core"
)

// Command 表示一个命令
type Command struct {
	Action string                 `json:"action"` // 命令类型
	Params map[string]interface{} `json:"params"` // 命令参数
}

// Client 应用层客户端
type Client struct {
	core.Client
}

// NewClient 创建一个新的客户端
func NewClient(server string) *Client {
	client, err := core.NewClient(server)
	if err != nil {
		fmt.Println("Error creating client:", err)
		return nil
	}
	return &Client{Client: *client}
}

// SendCommand 发送命令
func (c *Client) SendCommand(stream uint32, cmd *Command) error {

	// 编码命令为 JSON
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	// 发送数据包
	if err := c.Client.SendPacket(stream, data); err != nil {
		return err
	}

	return nil
}

// MplHandler 多路复用器处理器
type MplHandler struct {
}

// NewMplHandler 创建一个新的多路复用器处理器
func NewMplHandler() *MplHandler {
	return &MplHandler{}
}

func (s *MplHandler) Multiplexer(conn net.Conn) *core.Multiplexer {
	return core.NewMultiplexer(conn)
}

// handlePacket 处理数据包
func (s *MplHandler) HandlePacket(packet *core.Packet, mux *core.Multiplexer) {
	var cmd Command
	if err := json.Unmarshal(packet.Data, &cmd); err != nil {
		fmt.Printf("Error decoding command: %v, stream: %d, size: %d, data: %s\n", err, packet.StreamID, len(packet.Data), string(packet.Data))
		return
	}

	// 根据命令执行逻辑
	switch cmd.Action {
	case "ping":
		fmt.Println("Received ping command")
		response := map[string]any{"message": "pong"}
		s.sendResponse(mux, packet.StreamID, response)
	case "upload":
		fmt.Println("Received upload command")
		// 处理上传逻辑
	default:
		fmt.Println("Unknown command:", cmd.Action)
	}
}

// sendResponse 发送响应
func (s *MplHandler) sendResponse(mux *core.Multiplexer, streamID uint32, data map[string]interface{}) {
	response, err := json.Marshal(data)
	if err != nil {
		fmt.Println("Error encoding response:", err)
		return
	}

	packet := &core.Packet{
		StreamID: streamID,
		Data:     response,
	}
	if err := mux.SendPacket(packet); err != nil {
		fmt.Println("Error sending response:", err)
	}
}
