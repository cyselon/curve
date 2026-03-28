# Roadmap

## Near Term

Focus on making the MVP data path correct and stable.

- unify the transport path around `Connection/Stream -> Framer -> Frame -> net.Conn`
- remove or stop extending parallel abstractions that bypass the main connection model
- make multi-stream byte transport work end to end
- ensure unknown remote `StreamID` values can be mapped into usable streams
- make connection shutdown and stream shutdown behave predictably
- keep tests centered on single-stream, multi-stream, large payload, and close behavior

## Mid Term

Add the minimum protocol semantics needed to move beyond a prototype.

- define stream lifecycle semantics
- introduce basic control signaling such as open, close, reset, or ping
- make client and server stream handling symmetrical
- improve error handling at stream level and connection level
- clarify application-facing handler interfaces
- strengthen tests around invalid input, abnormal disconnects, and concurrency behavior

## Phase 2 Breakdown

The second phase should turn the current byte-transport MVP into a protocol with explicit stream semantics.

Work should proceed in this order:

1. Define lifecycle semantics first

- specify when a stream is considered opened
- specify normal close versus abnormal reset
- define local close and remote close behavior for `Read()` and `Write()`
- explicitly decide whether half-close is unsupported or deferred

Expected output:

- a short protocol note or state machine for `idle -> open -> closed/reset`
- clear rules for stream creation, closure, and error propagation

2. Introduce the minimum control frames

- add `OPEN`
- add `CLOSE`
- add `RESET`
- keep `PING` optional until lifecycle behavior is stable

Expected output:

- control frames become real protocol elements instead of placeholders
- stream creation no longer depends only on the first data frame

3. Rework `Stream` and `Connection` shutdown semantics

- local `Close()` should emit `CLOSE`
- remote `CLOSE` should cause `Read()` to return EOF
- remote `RESET` should cause stream operations to return a distinct reset error
- connection teardown should move all active streams into a predictable terminal state

Expected output:

- distinct stream-level errors such as closed versus reset
- predictable behavior for local close, remote close, reset, and connection teardown

4. Make client and server behavior symmetrical

- both sides should be able to open streams
- both sides should be able to receive remote `OPEN`
- both sides should handle `CLOSE` and `RESET` with the same semantics
- application-facing APIs should converge on one shared model for incoming and outgoing streams

Expected output:

- less special-case behavior between client and server
- a cleaner high-level connection API

5. Expand protocol-behavior tests

- cover `OPEN -> DATA -> CLOSE`
- cover duplicate or invalid lifecycle events
- cover `RESET` behavior
- cover abnormal disconnect behavior
- cover concurrent open/close/reset interactions

Expected output:

- tests focused on protocol semantics rather than only byte transport

## Phase 2 Work Packages

Suggested implementation slices:

- `core/frame.go` and `core/framer.go`
  define control frame encoding and decoding
- `core/connection.go`
  dispatch control frames and enforce lifecycle transitions
- `core/stream.go`
  implement stream state, close/reset behavior, and read/write semantics
- `core/session.go`, `core/client.go`, and `core/server.go`
  align the public API around symmetric stream handling
- `core/*test.go`
  add lifecycle, reset, abnormal disconnect, and concurrency tests
- `app/*`
  adapt the demo layer only after the protocol semantics are stable

## Phase 2 Priority

- `P0`: lifecycle definition plus `OPEN/CLOSE/RESET`
- `P1`: stream and connection state model cleanup
- `P1`: symmetric client/server handling
- `P2`: broader abnormal-path and concurrency coverage
- `P2`: app-layer adaptation to the finalized protocol behavior

## Long Term

Turn the prototype into a reusable general-purpose multiplexing layer.

- add flow control and backpressure
- improve fairness across active streams
- define clearer protocol error and recovery behavior
- improve resource management under sustained load
- harden the transport for long-lived connections and high concurrency
- shape the public API so application code depends on stable high-level abstractions instead of transport internals

## Summary

The roadmap is intentionally staged:

1. prove the byte transport model
2. add protocol semantics
3. harden it into a reusable transport layer
