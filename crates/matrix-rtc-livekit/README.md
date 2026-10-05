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
| **`token`** | MSC4195 token exchange: Matrix OpenID token → LiveKit SFU JWT via the authorisation service's `POST /get_token`. The OpenID token comes through the core's `MatrixBackend::openid_token`, so this layer is not hard-wired to a particular Matrix SDK. |
| **`identity`** | The MSC4195 hash derivations (`livekit_alias`, pseudonymous participant identity) used to map keys onto LiveKit participants; `identity_mapper` (crate root) picks the one a given compat generation's authorisation service issues. |
| **`session`** | Connects to the SFU and exposes the LiveKit room + event stream. |
| **`keys`** | Bridges `matrix-rtc-core` media keys into the LiveKit `KeyProvider` (`MediaKeyBridge`), keyed by pseudonymous identity — this is what makes frame E2EE per-participant. |
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
