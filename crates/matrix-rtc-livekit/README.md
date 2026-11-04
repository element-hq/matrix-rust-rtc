# matrix-rtc-livekit

The [MSC4195](https://github.com/matrix-org/matrix-spec-proposals/pull/4195)
LiveKit transport for MatrixRTC — the "LiveKit SDK" layer that turns the
membership and key outputs of `matrix-rtc-core` into a live SFU media session
with per-participant frame E2EE.

This crate is **native-only** (the LiveKit client pulls in `libwebrtc`); it
never targets wasm. Building it requires a C++ toolchain.

It implements `matrix-rtc-transport`'s contract and knows no call. To join a
call with it — the `LiveKitCall` facade, the examples, the load generator and
the end-to-end test — see [`matrix-rtc-call-sdk`](../matrix-rtc-call-sdk/README.md).

## Module map

| Module | What it does |
| --- | --- |
| **`token`** | The legacy JWT service's token exchange: Matrix OpenID token → LiveKit SFU JWT via `POST /sfu/get`, for transports naming a `livekit_service_url`. A transport naming only an SFU `url` gets its JWT from the homeserver's MSC4195 `get_token` instead, through the core's `MatrixBackend::livekit_token`. |
| **`identity`** | The MSC4195 hash derivations (`livekit_alias`, participant identity) used to map keys onto LiveKit participants; `identity_mapper` (crate root) gives each member the identity its published transport's token flow mints. |
| **`session`** | Connects to the SFU and exposes the LiveKit room + event stream. |
| **`keys`** | Bridges `matrix-rtc-core` media keys into the LiveKit `KeyProvider` (`MediaKeyBridge`), keyed by participant identity — this is what makes frame E2EE per-participant. |
| **`media`** | `record_track` / `write_wav` (shipped: the recording-bot path), plus test-gated tone generation and frequency detection. |
| **`transport_impl`** | `LiveKitMediaTransport`, `matrix-rtc-transport`'s `MediaTransport`/`OwnFocusTransport` over the LiveKit client, and its `LiveKitTransportConnection`. |

## End-to-end encryption

Frame E2EE **is wired**: the core generates a per-participant media key,
distributes it as an Olm-encrypted `m.rtc.encryption_key` to-device message,
and `MediaKeyBridge` imports received keys into the LiveKit `KeyProvider`
(MSC4195 per-participant HKDF mode, GCM frames). Use `connect_e2ee` — or `LiveKitCall::join` (`matrix-rtc-call-sdk`), which
does — rather than plain `connect`. The end-to-end test asserts a tone survives
an encrypt→SFU→decrypt round trip.

## Features

| Feature | Effect |
| --- | --- |
| `testing` | Test-only parts of `media` (tone generator, Goertzel detector), used by `matrix-rtc-call-sdk`'s examples and e2e test. |
