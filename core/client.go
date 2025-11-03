package core

import (
	"net"
)

type Client struct {
	conn net.Conn
}

func NewClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{conn: conn}, nil
}

func (c *Client) SendPacket(streamID uint32, data []byte) error {
	packet := &Packet{
		StreamID: streamID,
		Data:     data,
	}

	encoded := EncodePacket(packet)
	_, err := c.conn.Write(encoded)
	return err
}

func (c *Client) Close() {
	c.conn.Close()
}
