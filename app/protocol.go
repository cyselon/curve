package app

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

const maxMessageSize = 4 << 20 // 4 MiB

func writeJSONMessage(w io.Writer, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	if len(payload) > maxMessageSize {
		return fmt.Errorf("message too large: %d bytes", len(payload))
	}

	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))

	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("write message header: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("write message payload: %w", err)
	}
	return nil
}

func readJSONMessage(r io.Reader, v any) error {
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}

	size := binary.BigEndian.Uint32(header)
	if size > maxMessageSize {
		return fmt.Errorf("message too large: %d bytes", size)
	}

	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return err
	}

	if err := json.Unmarshal(payload, v); err != nil {
		return fmt.Errorf("unmarshal message: %w", err)
	}
	return nil
}
