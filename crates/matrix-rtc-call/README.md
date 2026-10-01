# matrix-rtc-call

The call application over `matrix-rtc-core`: Element Call's reactions and raised
hand, MSC4075 ringing, and the host-facing call manager — plus how a host's
`MatrixBackend` reaches it, and how it interoperates with clients that speak an
older wire format. None of it needs a Matrix SDK or a transport: the SDK
backend is `matrix-rtc-matrix-sdk`, LiveKit is `matrix-rtc-livekit`.

```
matrix-rtc-core        what the protocol says
      ▲
matrix-rtc-call        the call, its feeder and dialects   ← this crate
      ▲
matrix-rtc-matrix-sdk  the matrix-rust-sdk backend
matrix-rtc-livekit     how bytes flow (MSC4195 SFU)
```

## Module map

| Module | What it does |
| --- | --- |
| **`reactions`** | Element Call's emoji reactions and raised hand: the wire format, the send cooldown, and the per-session state the call keeps. |
| **`notification`** | MSC4075 ringing: who notifies, and the notification content. |
| **`compat`** | Interop with MatrixRTC implementations that predate the 2026 MSC4143 rewrite (today: Element Call on the JS SDK), in two generations. `StickyEvents` is the 2025 format — MSC4354 stickies with pre-2026 field names; reading it is always on, writing it is opt-in. `StateEvents` is the format before MSC4354, with membership as `org.matrix.msc3401.call.member` **room state**; opt-in in both directions, and visible to nobody but that generation. Pure JSON in, pure JSON out — no Matrix SDK, no async runtime. Scaffolding, to be deleted once Element Call catches up. |
| **`feeder`** | What turns a host's `MatrixBackend` into a fed `CallSessionManager`. `RoomFeeder::attach(backend, manager, room_id, options)` subscribes to what the room's compatibility mode needs, applies encryption, slot state and joined members before the first membership, translates the member events (client-reported decryption facts → `EventOrigin`; the pre-2026 funnels), feeds timeline events, redactions and `/relations`, and reports `seeded` once the current state is in. `ToDeviceFeeder` does the same for the to-device key messages. The core spawns nothing; the caller runs each feeder's future where it likes. |
| **`compat::dialect_backend`** | `DialectBackend<B>`: the one `MatrixBackend` wrapper applying a room's outbound dialect (member-event routing, legacy key type, pre-sticky leave as a delayed *state* event) before delegating. Registered at join, from the mode given at attach. |
| **`transports`** | `choose(rtc_transports, override)`: the library's transport pick — the join's override, else the first LiveKit entry the homeserver advertises. |

## Testing

Everything here tests against the core's `testing::MockBackend` — no git
dependencies, no libwebrtc, compiles for wasm32:

```sh
cargo test -p matrix-rtc-call
```

The feeder tests deliver sets into the sinks and read what reached the manager.

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
in `DialectBackend`, start a `ToDeviceFeeder`, attach rooms with `RoomFeeder` —
and differs only in where the backend comes from and where the feeder's future
runs: `matrix_rtc_livekit::Call::join` (`SdkBackend`, `spawn_local`),
`matrix_rtc_ffi::RtcSessionManagerHandle` (the host's foreign trait, the FFI
runtime) and `matrix_rtc_wasm::WasmRtcSessionManager` (the page's
`MatrixBackendHost`, `spawn_local`).
