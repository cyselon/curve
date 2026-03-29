# Phase 2 Protocol Note

## Stream Lifecycle

Each stream follows a minimal terminal-state model:

`idle -> open -> closed`

`idle -> open -> reset`

Rules:

- A locally created stream becomes `open` when `OpenStream()` allocates it and emits an `OPEN` control frame.
- A remotely created stream becomes `open` when an `OPEN` control frame is received.
- Data frames are valid only for streams already in the `open` state.
- Half-close is deferred. A normal `CLOSE` moves the stream directly to the terminal `closed` state for both reading and writing.
- A `RESET` moves the stream directly to the terminal `reset` state for both reading and writing.

## Read And Write Semantics

- After remote `CLOSE`, `Read()` returns `io.EOF` once any buffered data is consumed.
- After remote `RESET`, stream operations return a distinct reset error.
- After local `Close()`, the stream stops accepting local reads and writes and emits `CLOSE`.
- After local `Reset()`, the stream stops accepting local reads and writes and emits `RESET`.
- After connection teardown, blocked stream operations are unblocked and return the connection-closed error.

## Invalid Lifecycle Events

- `OPEN` for an already known stream is invalid.
- `DATA`, `CLOSE`, or `RESET` for an unknown stream is invalid.
- Invalid lifecycle events are answered with `RESET` for that stream when possible.
