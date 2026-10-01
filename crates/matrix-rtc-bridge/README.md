# matrix-rtc-bridge

The matrix-rust-sdk implementation of `matrix-rtc-core`'s `MatrixBackend`. The
core and the call layer (`matrix-rtc-call`, which also holds the feeder and the
pre-2026 Element Call dialects) drive any backend; this crate is the one over a
`matrix_sdk::Client`. It is deliberately **transport-free** — nothing in it
knows what a LiveKit SFU is.

## Module map

| Module | What it does |
| --- | --- |
| **`sdk`** *(feature `matrix-sdk`)* | `SdkBackend` implements `MatrixBackend` over a `matrix_sdk::Client`: Client-Server requests for the sends, and for the reads a task per room subscription that delivers the complete current sets on subscribe and again on every sticky-store or room-state wake, plus the to-device handlers, `/relations`, the OpenID token and `GET /rtc/transports`. |

## Features

| Feature | Effect |
| --- | --- |
| *(default)* | Nothing: the crate is empty without the SDK. |
| `matrix-sdk` | `sdk`. Depends on upstream matrix-rust-sdk (rev in the workspace manifest) with its `unstable-msc4354` feature, for the MSC4354 sticky carrier. |

## Testing

```sh
cargo test -p matrix-rtc-bridge --features matrix-sdk
```
