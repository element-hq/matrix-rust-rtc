# matrix-rtc-call

The call application over `matrix-rtc-core`: Element Call's reactions and raised
hand, MSC4075 ringing, and the host-facing objects (client → room → session) —
plus how a host's `MatrixBackend` reaches them, and how it interoperates with clients that speak an
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

## The objects: client → room → session

`RtcClient::new(backend)` is one per backend and does no I/O. `client.room(room_id,
RoomOptions)` opens a room: it subscribes to what the room's compatibility mode
needs, starts the to-device subscription with the first open room (it stops with
the last), and returns the room's feed futures for the caller to run. A room has
one live object at a time; opening it again while one is alive is refused.

On an `RtcRoom`, `join(JoinOptions)` returns an `RtcSession` — our participation in
one slot — and `join_call(CallJoinOptions)` an `RtcCall`, which is an `RtcSession`
plus reactions, the raised hand and the ring. Room-scoped reads (`slot_state`,
`member_count`, `observe`) and `open_slot`/`close_slot` need no join.

Ending things: `session.leave()` leaves; `room.close()` leaves every slot joined
through it, then unsubscribes. Dropping a session or a room sends no leave — the
membership expires through its delayed leave, and joining the slot again leaves
the orphaned participation first. A session is over once left, dropped or its
room closed, and its calls then fail with `RtcError::SessionOver`.

The library does not detect incoming calls: MSC4075 is send-only here, and a host
learns of a ring through its own SDK or push path.

## Module map

| Module | What it does |
| --- | --- |
| **`client`** | `RtcClient`, `RtcRoom`, `RtcSession`, `RtcCall`: the objects above, and the join orchestration (transport choice, `member.id`, the room's dialect). |
| **`room_state`** | `CallRoomState`, the call layer over one core `BaseRtcRoom`: reactions per joined slot, the ring at join. |
| **`reactions`** | Element Call's emoji reactions and raised hand: the wire format, the send cooldown, and the per-session state the call keeps. |
| **`notification`** | MSC4075 ringing: who notifies, and the notification content. |
| **`compat`** | Interop with MatrixRTC implementations that predate the 2026 MSC4143 rewrite (today: Element Call on the JS SDK), in two generations. `StickyEvents` is the 2025 format — MSC4354 stickies with pre-2026 field names; reading it is always on, writing it is opt-in. `StateEvents` is the format before MSC4354, with membership as `org.matrix.msc3401.call.member` **room state**; opt-in in both directions, and visible to nobody but that generation. Pure JSON in, pure JSON out — no Matrix SDK, no async runtime. Scaffolding, to be deleted once Element Call catches up. |
| **`feeder`** | What feeds an open room from a host's `MatrixBackend`. `RoomFeeder::attach(backend, room, mode)` subscribes to what the room's compatibility mode needs, applies encryption, slot state and joined members before the first membership, translates the member events (client-reported decryption facts → `EventOrigin`; the pre-2026 funnels), feeds timeline events, redactions and `/relations`, and reports `seeded` once the current state is in. `ToDeviceFeeder` routes the to-device key messages to the open room they are for, through the `RoomRegistry`, and drops those for a room that is not open. The core spawns nothing; the caller runs each feeder's future where it likes. |
| **`compat::dialect_backend`** | `DialectBackend<B>`: the one `MatrixBackend` wrapper applying a room's outbound dialect (member-event routing, legacy key type, pre-sticky leave as a delayed *state* event) before delegating. Registered at join, from the mode the room was opened in. |
| **`transports`** | `choose(rtc_transports, override)`: the library's transport pick — the join's override, else the first LiveKit entry the homeserver advertises. |

## Testing

Everything here tests against the core's `testing::MockBackend` — no git
dependencies, no libwebrtc, compiles for wasm32:

```sh
cargo test -p matrix-rtc-call
```

The feeder and client tests deliver sets into the sinks and read what reached the room.

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

## Who uses the client

Every entry point opens rooms through `RtcClient` and differs only in where the
backend comes from and where the feed futures run:
`matrix_rtc_livekit::LiveKitCall::join` (`SdkMatrixBackend`, `spawn_local`),
`matrix_rtc_ffi::RtcClient` (the host's foreign trait, the FFI runtime) and
`matrix_rtc_wasm::WasmRtcClient` (the page's `MatrixBackendHost`, `spawn_local`).
