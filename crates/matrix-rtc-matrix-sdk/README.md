# matrix-rtc-matrix-sdk

The matrix-rust-sdk implementation of `matrix-rtc-core`'s `MatrixBackend`. The
core and the call layer (`matrix-rtc-call`, which also holds the feeder and the
pre-2026 Element Call dialects) drive any backend; this crate is the one over a
`matrix_sdk::Client`. It is deliberately **transport-free** — nothing in it
knows what a LiveKit SFU is.

## Module map

| Module | What it does |
| --- | --- |
| **`sdk`** | `SdkBackend` implements `MatrixBackend` over a `matrix_sdk::Client`: Client-Server requests for the sends, and for the reads a task per room subscription that delivers the complete current sets on subscribe and again on every sticky-store or room-state wake, plus the to-device handlers, `/relations`, the OpenID token and `GET /rtc/transports`. |

## Testing

```sh
cargo test -p matrix-rtc-matrix-sdk
```
