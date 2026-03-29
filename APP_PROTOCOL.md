# App Protocol Note

The application layer uses length-prefixed JSON messages over a single logical stream.

## Request And Response Model

- A stream may carry multiple requests in sequence.
- Each request receives at most one response.
- Responses are ordered and must match request order on the same stream.
- The handler keeps the stream open for additional requests until either side closes or resets it.

## Response Shape

- Successful responses set `ok=true` and may include `data`.
- Failed responses set `ok=false` and include `error`.

This keeps the app layer independent from transport control-frame details while still making request failures explicit to callers.
