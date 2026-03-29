package core

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func setupTestConnection(t *testing.T) (clientConn, serverConn *Connection) {
	t.Helper()

	clientNet, serverNet := net.Pipe()
	clientConn = NewConnection(clientNet, true)
	serverConn = NewConnection(serverNet, false)
	clientConn.Start()
	serverConn.Start()

	return clientConn, serverConn
}

func waitForStream(t *testing.T, conn *Connection, streamID uint32) *Stream {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if stream, ok := conn.GetStream(streamID); ok {
			return stream
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for stream %d", streamID)
	return nil
}

func TestFrame_EncodeDecodeIncludesVersion(t *testing.T) {
	original := &Frame{
		Version:  CurrentVersion,
		StreamID: 7,
		Type:     FrameData,
		Flags:    3,
		Payload:  []byte("hello"),
	}

	encoded := original.Encode()
	if got := encoded[0]; got != CurrentVersion {
		t.Fatalf("expected version %d, got %d", CurrentVersion, got)
	}

	var decoded Frame
	if err := decoded.Decode(bytes.NewReader(encoded)); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Version != CurrentVersion {
		t.Fatalf("expected version %d, got %d", CurrentVersion, decoded.Version)
	}
	if decoded.StreamID != original.StreamID {
		t.Fatalf("expected StreamID %d, got %d", original.StreamID, decoded.StreamID)
	}
	if decoded.Type != original.Type {
		t.Fatalf("expected Type %d, got %d", original.Type, decoded.Type)
	}
	if decoded.Flags != original.Flags {
		t.Fatalf("expected Flags %d, got %d", original.Flags, decoded.Flags)
	}
	if !bytes.Equal(decoded.Payload, original.Payload) {
		t.Fatalf("expected payload %q, got %q", original.Payload, decoded.Payload)
	}
}

func TestNewConnection_ClientStreamIDs(t *testing.T) {
	clientNet, serverNet := net.Pipe()
	defer clientNet.Close()
	defer serverNet.Close()

	client := NewConnection(clientNet, true)
	client.Start()
	defer client.Close()

	stream1, err := client.CreateStream()
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}
	if stream1.ID()%2 == 0 {
		t.Fatalf("client stream ID should be odd, got %d", stream1.ID())
	}

	stream2, err := client.CreateStream()
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}
	if stream2.ID()%2 == 0 {
		t.Fatalf("client stream ID should be odd, got %d", stream2.ID())
	}
}

func TestConnection_OpenStreamAlias(t *testing.T) {
	clientNet, serverNet := net.Pipe()
	defer clientNet.Close()
	defer serverNet.Close()

	client := NewConnection(clientNet, true)
	client.Start()
	defer client.Close()

	stream, err := client.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	if stream.StreamID() != stream.ID() {
		t.Fatalf("expected StreamID alias to match ID, got %d and %d", stream.StreamID(), stream.ID())
	}
	if stream.StreamID()%2 == 0 {
		t.Fatalf("client OpenStream should allocate odd stream IDs, got %d", stream.StreamID())
	}
}

func TestNewConnection_ServerStreamIDs(t *testing.T) {
	clientNet, serverNet := net.Pipe()
	defer clientNet.Close()
	defer serverNet.Close()

	server := NewConnection(serverNet, false)
	server.Start()
	defer server.Close()

	stream1, err := server.CreateStream()
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}
	if stream1.ID()%2 != 0 {
		t.Fatalf("server stream ID should be even, got %d", stream1.ID())
	}

	stream2, err := server.CreateStream()
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}
	if stream2.ID()%2 != 0 {
		t.Fatalf("server stream ID should be even, got %d", stream2.ID())
	}
}

func TestConnection_OpenDataCloseLifecycle(t *testing.T) {
	clientConn, serverConn := setupTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	clientStream, err := clientConn.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}

	serverStream := waitForStream(t, serverConn, clientStream.ID())

	payload := []byte("ping")
	if _, err := clientStream.Write(payload); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	buf := make([]byte, len(payload))
	if _, err := io.ReadFull(serverStream, buf); err != nil {
		t.Fatalf("ReadFull failed: %v", err)
	}
	if !bytes.Equal(buf, payload) {
		t.Fatalf("expected %q, got %q", payload, buf)
	}

	if err := clientStream.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := serverConn.GetStream(clientStream.ID()); !ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	readBuf := make([]byte, 1)
	n, err := serverStream.Read(readBuf)
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF after remote close, got n=%d err=%v", n, err)
	}
}

func TestConnection_RemoteResetReturnsDistinctError(t *testing.T) {
	clientConn, serverConn := setupTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	clientStream, err := clientConn.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}

	serverStream := waitForStream(t, serverConn, clientStream.ID())
	if err := serverConn.WriteFrame(NewControlFrame(serverStream.ID(), ControlReset)); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}
	n, err := clientStream.Read(make([]byte, 1))
	if n != 0 || !errors.Is(err, ErrStreamReset) {
		t.Fatalf("expected reset error, got n=%d err=%v", n, err)
	}
	if _, err := clientStream.Write([]byte("x")); !errors.Is(err, ErrStreamReset) {
		t.Fatalf("expected reset error on write, got %v", err)
	}
}

func TestConnection_LocalResetPropagatesToRemote(t *testing.T) {
	clientConn, serverConn := setupTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	clientStream, err := clientConn.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}

	serverStream := waitForStream(t, serverConn, clientStream.ID())
	if err := clientStream.Reset(); err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n, err := serverStream.Read(make([]byte, 1))
		if n == 0 && errors.Is(err, ErrStreamReset) {
			if _, err := serverStream.Write([]byte("x")); !errors.Is(err, ErrStreamReset) {
				t.Fatalf("expected reset error on remote write, got %v", err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("expected local reset to propagate to remote")
}

func TestConnection_LargePayloadSplitAndReassembly(t *testing.T) {
	clientConn, serverConn := setupTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	clientStream, err := clientConn.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}

	payload := bytes.Repeat([]byte("curve"), (DefaultFrameSize*3/5)+257)
	if _, err := clientStream.Write(payload); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	serverStream := waitForStream(t, serverConn, clientStream.ID())
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(serverStream, got); err != nil {
		t.Fatalf("ReadFull failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("reassembled payload does not match original payload")
	}
}

func TestConnection_UnknownDataFrameTriggersReset(t *testing.T) {
	clientConn, serverConn := setupTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	if err := clientConn.WriteFrame(&Frame{
		Version:  CurrentVersion,
		StreamID: 99,
		Type:     FrameData,
		Payload:  []byte("orphan"),
	}); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := clientConn.GetStream(99); ok {
			t.Fatal("client should not create a stream from reset")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestConnection_DuplicateOpenTriggersReset(t *testing.T) {
	clientConn, serverConn := setupTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	clientStream, err := clientConn.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}

	_ = waitForStream(t, serverConn, clientStream.ID())
	if err := clientConn.WriteFrame(NewControlFrame(clientStream.ID(), ControlOpen)); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := clientStream.Write([]byte("x")); errors.Is(err, ErrStreamReset) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("expected duplicate OPEN to trigger reset")
}

func TestConnection_DuplicateCloseTriggersReset(t *testing.T) {
	clientConn, serverConn := setupTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	clientStream, err := clientConn.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}

	_ = waitForStream(t, serverConn, clientStream.ID())
	if err := clientStream.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := clientConn.WriteFrame(NewControlFrame(clientStream.ID(), ControlClose)); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := clientStream.Write([]byte("x")); errors.Is(err, ErrStreamReset) || errors.Is(err, io.ErrClosedPipe) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("expected duplicate CLOSE to reach terminal state")
}

func TestConnection_DuplicateResetIsIgnoredByPeerState(t *testing.T) {
	clientConn, serverConn := setupTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	clientStream, err := clientConn.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}

	serverStream := waitForStream(t, serverConn, clientStream.ID())
	if err := serverConn.WriteFrame(NewControlFrame(serverStream.ID(), ControlReset)); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}
	if err := serverConn.WriteFrame(NewControlFrame(serverStream.ID(), ControlReset)); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := clientStream.Write([]byte("x")); errors.Is(err, ErrStreamReset) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("expected duplicate RESET to leave stream reset")
}

func TestConnection_InvalidDirectionOpenIsRejected(t *testing.T) {
	clientConn, serverConn := setupTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	if err := clientConn.WriteFrame(NewControlFrame(2, ControlOpen)); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	if _, ok := serverConn.GetStream(2); ok {
		t.Fatal("server should reject client OPEN for even stream ID")
	}
}

func TestSessionAcceptStreamReturnsRemoteOpenedStream(t *testing.T) {
	clientConn, serverConn := setupTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	session := NewSession("server-test", serverConn)
	gotCh := make(chan *Stream, 1)
	errCh := make(chan error, 1)
	go func() {
		stream, err := session.AcceptStream()
		if err != nil {
			errCh <- err
			return
		}
		gotCh <- stream
	}()

	clientStream, err := clientConn.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}

	select {
	case err := <-errCh:
		t.Fatalf("AcceptStream failed: %v", err)
	case stream := <-gotCh:
		if stream.ID() != clientStream.ID() {
			t.Fatalf("expected stream %d, got %d", clientStream.ID(), stream.ID())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for AcceptStream")
	}
}

func TestConnection_ConcurrentOpenCloseReset(t *testing.T) {
	clientConn, serverConn := setupTestConnection(t)
	defer clientConn.Close()
	defer serverConn.Close()

	const streamCount = 8
	streams := make([]*Stream, streamCount)
	for i := range streams {
		stream, err := clientConn.OpenStream()
		if err != nil {
			t.Fatalf("OpenStream %d failed: %v", i, err)
		}
		streams[i] = stream
		_ = waitForStream(t, serverConn, stream.ID())
	}

	var wg sync.WaitGroup
	for i, stream := range streams {
		wg.Add(1)
		go func(i int, stream *Stream) {
			defer wg.Done()
			if i%2 == 0 {
				if err := serverConn.WriteFrame(NewControlFrame(stream.ID(), ControlReset)); err != nil {
					t.Errorf("reset stream %d: %v", stream.ID(), err)
				}
				return
			}
			if err := stream.Close(); err != nil {
				t.Errorf("close stream %d: %v", stream.ID(), err)
			}
		}(i, stream)
	}
	wg.Wait()

	deadline := time.Now().Add(2 * time.Second)
	for _, stream := range streams {
		for time.Now().Before(deadline) {
			if _, ok := clientConn.GetStream(stream.ID()); !ok {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	for i, stream := range streams {
		if i%2 == 0 {
			if _, err := stream.Write([]byte("x")); !errors.Is(err, ErrStreamReset) {
				t.Fatalf("expected reset error for stream %d, got %v", stream.ID(), err)
			}
			continue
		}
		n, err := stream.Read(make([]byte, 1))
		if n != 0 || !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF for closed stream %d, got n=%d err=%v", stream.ID(), n, err)
		}
	}
}

func TestConnection_CloseUnblocksStreamReads(t *testing.T) {
	clientConn, serverConn := setupTestConnection(t)
	defer clientConn.Close()

	clientStream, err := clientConn.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}

	serverStream := waitForStream(t, serverConn, clientStream.ID())
	done := make(chan error, 1)
	go func() {
		_, err := serverStream.Read(make([]byte, 1))
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)
	if err := clientConn.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, ErrConnectionClosed) {
			t.Fatalf("expected connection closed error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for blocked read to return")
	}
}

func TestConnection_RemoteDisconnectUnblocksWrites(t *testing.T) {
	clientConn, serverConn := setupTestConnection(t)
	defer clientConn.Close()

	clientStream, err := clientConn.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}

	_ = waitForStream(t, serverConn, clientStream.ID())
	if err := serverConn.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := clientStream.Write([]byte("x")); errors.Is(err, ErrConnectionClosed) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("expected remote disconnect to unblock writes with connection closed")
}
