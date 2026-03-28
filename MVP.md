# MVP Specification

## Goal

Build a minimal framing and multiplexing prototype on top of a single TCP connection.

At this stage, the objective is limited to:

- split byte streams into frames
- carry multiple logical streams over one `net.Conn`
- reassemble frame payloads into per-stream byte streams on the receiving side

This is not yet a complete general-purpose multiplexing protocol. It is a minimal transport prototype used to validate the core data path.

## Scope

The MVP supports multiple logical streams over one TCP connection, where each stream behaves like an independent byte stream.

The implementation focus is:

- frame encoding and decoding
- stream-based frame dispatch
- large payload splitting and reassembly
- safe shutdown of connections and streams

## Minimal Requirements

1. A single physical connection

All logical streams must share one underlying `net.Conn`.

2. Fixed frame header

Every frame must include at least:

- `Version`
- `StreamID`
- `Length`

3. Data frames only

`Frame.Data` carries raw bytes only.

The MVP does not introduce separate control frame semantics yet.

An implementation may keep placeholder enum values or reserved constants for future control frames, but the active wire path for this stage must remain data-only.

4. Payload splitting on write

When the payload size exceeds `MaxFrameDataSize`, the sender must split it into multiple frames and send them in order.

5. Per-stream dispatch on read

The receiver must read frames sequentially from the connection and dispatch payloads by `StreamID`.

Data from different streams must not be mixed in one stream buffer.

6. Independent stream abstraction

Each logical stream must expose at least:

- `Read`
- `Write`
- `Close`
- `StreamID`

7. Local stream creation

The local side must be able to create a new stream with `OpenStream()`.

Compatibility aliases such as `CreateStream()` are acceptable as long as the primary API converges on the MVP naming.

Client and server must use distinct stream ID spaces to avoid collisions.

8. Implicit remote stream creation

If the receiver gets a data frame for an unknown `StreamID`, it should automatically create the corresponding stream.

The MVP uses the first data frame as the implicit open signal.

9. Safe connection shutdown

When the connection is closed, all active streams must be unblocked and exit cleanly.

`Read()` must not block forever after connection shutdown.

10. Required tests

The MVP test suite must cover at least:

- single-stream read and write
- concurrent multi-stream transmission
- large payload split and reassembly
- connection close behavior

## Explicit Non-Goals

The following are intentionally out of scope for this stage:

- `OPEN` / `CLOSE` / `RESET` / `PING` control frames
- stream lifecycle protocol
- flow control and window updates
- stream priorities and scheduling
- half-close semantics
- protocol-level error codes
- retransmission or recovery behavior

## Architecture Constraint

The transport stack should converge on one primary path:

`Connection/Stream -> Framer -> Frame -> net.Conn`

Application code should not treat `Framer` as a parallel public transport API.

## Summary

This MVP should be treated as:

"a TCP framing and multiplexing prototype that proves splitting, dispatching, and reassembly of byte streams across multiple logical streams."
