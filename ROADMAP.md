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
