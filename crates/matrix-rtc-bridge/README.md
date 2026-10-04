# matrix-rtc-bridge

The Matrix side of the MatrixRTC stack: how `matrix-rtc-core`'s protocol
behaviour reaches a homeserver, and how it interoperates with clients that speak
an older wire format.

This crate is deliberately **transport-free** — nothing in it knows what a
LiveKit SFU is. That is the point: a second transport (P2P/WebTransport, which
`matrix-rtc-media` already designs for) reuses it unchanged.

```
matrix-rtc-core        what the protocol says
      ▲
matrix-rtc-bridge      how it reaches a homeserver     ← this crate
      ▲
matrix-rtc-livekit     how bytes flow (MSC4195 SFU)
```

One direction only. The bridge never depends on a transport; a transport depends
on the bridge.

## Module map

| Module | What it does |
| --- | --- |
| **`compat`** | Interop with MatrixRTC implementations that predate the 2026 MSC4143 rewrite (today: Element Call on the JS SDK), in two generations. `StickyEvents` is the 2025 format — MSC4354 stickies with pre-2026 field names; reading it is always on, writing it is opt-in. `StateEvents` is the format before MSC4354, with membership as `org.matrix.msc3401.call.member` **room state**; opt-in in both directions, and visible to nobody but that generation. Pure JSON in, pure JSON out — no Matrix SDK, no async runtime. Scaffolding, to be deleted once Element Call catches up. |
| **`feeder`** | What turns a host's `MatrixBackend` into a fed `CallSessionManager`. `RoomFeeder::attach(backend, manager, room_id, options)` subscribes to what the room's compatibility mode needs, applies encryption, slot state and joined members before the first membership, translates the member events (client-reported decryption facts → `EventOrigin`; the pre-2026 funnels), feeds timeline events, redactions and `/relations`, and reports `seeded` once the current state is in. `SessionFeeder` does the same for the to-device key messages. The core spawns nothing; the caller runs each feeder's future where it likes. |
| **`compat::dialect_backend`** | `DialectBackend<B>`: the one `MatrixBackend` wrapper applying a room's outbound dialect (member-event routing, legacy key type, pre-sticky leave as a delayed *state* event) before delegating. Registered at join, from the mode given at attach. |
| **`transports`** | `choose(rtc_transports, override)`: the library's transport pick — the join's override, else the first LiveKit entry the homeserver advertises. |
| **`sdk`** *(feature `matrix-sdk`)* | `SdkBackend` implements `MatrixBackend` over a `matrix_sdk::Client`: Client-Server requests for the sends, and for the reads a task per room subscription that delivers the complete current sets on subscribe and again on every sticky-store or room-state wake, plus the to-device handlers, `/relations`, the OpenID token and `GET /rtc/transports`. |

## Features

| Feature | Effect |
| --- | --- |
| *(default)* | `feeder`, `compat`, `transports`. Depends only on `matrix-rtc-core`, `matrix-rtc-call`, serde/serde_json, thiserror, async-trait, tokio's sync primitives and log — no Matrix SDK, no git dependencies; compiles for wasm32. |
| `matrix-sdk` *(off by default)* | `sdk`. Depends on upstream matrix-rust-sdk (rev in the workspace manifest) with its `unstable-msc4354` feature, for the MSC4354 sticky carrier. |

## Testing

`compat` is the largest and most-tested part of this crate and needs nothing but
`serde_json`, which is why the SDK is optional at all:

```sh
cargo test -p matrix-rtc-bridge                         # compat + feeder, no git deps, no libwebrtc
cargo test -p matrix-rtc-bridge --features matrix-sdk   # adds sdk.rs
```

The feeder tests run over the core's `testing::MockBackend`: a test delivers
sets into the sinks and reads what reached the manager.

That first line is the reason this crate exists. All of these tests used to live
inside `matrix-rtc-livekit`, where running them meant building `libwebrtc` and so
needing a C++ toolchain — a native media dependency gating tests that only ever
compare JSON.

## What this crate deliberately does not own

The boundary is load-bearing, so the exclusions are explicit:

- **`MemberClaims`** stays in `matrix-rtc-livekit`. Those are the `member` claims
  of the MSC4195 `/get_token` request body, which no homeserver ever sees.
- **The participant-identity derivation** (`matrix_rtc_livekit::identity_mapper`)
  and **the token endpoint** (`TokenEndpoint`). `compat` decides *which
  generation*; what that means for an identity or an endpoint is MSC4195 — a
  LiveKit document — so it lives with the transport. Each is one `match` on
  `ElementCallCompat`.
- **Media, frame encryption, and SFU connections.** `matrix-rtc-media` defines
  the transport-agnostic media model; a transport crate implements it.

`compat` keeps its own notes on why compatibility is confined to JSON funnels at
the edge — read the module docs before touching it.

## Who uses the feeder

Every entry point does the same three things before joining — wrap the backend
in `DialectBackend`, start a `SessionFeeder`, attach rooms with `RoomFeeder` —
and differs only in where the backend comes from and where the feeder's future
runs: `matrix_rtc_livekit::Call::join` (`SdkBackend`, `spawn_local`),
`matrix_rtc_ffi::RtcSessionManagerHandle` (the host's foreign trait, the FFI
runtime) and `matrix_rtc_wasm::WasmRtcSessionManager` (the page's
`MatrixBackendHost`, `spawn_local`).
