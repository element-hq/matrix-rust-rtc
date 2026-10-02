# matrix-rtc-call

The call application over `matrix-rtc-core`: Element Call's reactions and raised
hand, MSC4075 ringing, and the host-facing objects (client → room → session) —
plus how a host's `MatrixBackend` reaches them, and how it interoperates with clients that speak an
older wire format. None of it needs a Matrix SDK or a transport: the SDK
backend is `matrix-rtc-matrix-sdk`, LiveKit is `matrix-rtc-livekit`.

```
matrix-rtc-core        what the protocol says
      ▲
matrix-rtc-call        the call                            ← this crate
      ▲
matrix-rtc-matrix-sdk  the matrix-rust-sdk backend
matrix-rtc-livekit     how bytes flow (MSC4195 SFU)
```

## The objects: client → room → session

`RtcClient::new(backend)` is one per backend and does no I/O. `client.room(room_id,
RoomOptions)` opens a room through the core's `BaseRtcClient`: it subscribes to
what the room's membership format needs, starts the to-device subscription with
the first open room (it stops with the last), and runs the room's feeds on the
core's executor while the room lives. A room has one live object at a time;
opening it again while one is alive is refused.

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
| **`client`** | `RtcClient`, `RtcRoom`, `RtcSession`, `RtcCall`: the objects above, and the join orchestration (transport choice, R8, the orphan leave; the `member.id` and dialect come from the core's `prepare_join`). |
| **`room_state`** | `CallRoomState`, the call layer over one core `BaseRtcRoom`: reactions per joined slot, the ring at join. |
| **`reactions`** | Element Call's emoji reactions and raised hand: the wire format, the send cooldown, and the per-session state the call keeps. |
| **`notification`** | MSC4075 ringing: who notifies, and the notification content. |
| **`transports`** | `choose(rtc_transports, override)`: the library's transport pick — the join's override, else the first LiveKit entry the homeserver advertises. |

## Testing

Everything here tests against the core's `testing::MockBackend` — no git
dependencies, no libwebrtc, compiles for wasm32:

```sh
cargo test -p matrix-rtc-call
```

The client tests and the feeder tests (`feeder_tests`, the call layer's timeline events) deliver sets into the sinks and read what reached the room.

## What this crate deliberately does not own

The boundary is load-bearing, so the exclusions are explicit:

- **`MemberClaims`** stays in `matrix-rtc-livekit`. Those are the `member` claims
  of the MSC4195 `/get_token` request body, which no homeserver ever sees.
- **The pre-2026 membership formats** (`matrix_rtc_core::compat`): they carry
  any application, so they are the core's; a room is opened in one
  (`RoomOptions::format`).
- **The participant-identity derivation** (`matrix_rtc_livekit::identity_mapper`)
  and **the token endpoint** (`TokenEndpoint`). The format decides *which
  generation*; what that means for an identity or an endpoint is MSC4195 — a
  LiveKit document — so it lives with the transport. Each is one `match` on
  `MembershipFormat`.
- **Media, frame encryption, and SFU connections.** `matrix-rtc-media` defines
  the transport-agnostic media model; a transport crate implements it.

The core's `compat` keeps its own notes on why compatibility is confined to JSON
funnels at the edge — read the module docs before touching it.

## Who uses the client

Every entry point opens rooms through `RtcClient` and differs only in where the
backend comes from and which runtime is current when it does:
`matrix_rtc_livekit::LiveKitCall::join` (`SdkMatrixBackend`, the caller's tokio runtime),
`matrix_rtc_ffi::RtcClient` (the host's foreign trait, the FFI runtime) and
`matrix_rtc_wasm::WasmRtcClient` (the page's `MatrixBackendHost`, the JS event loop).
The library spawns the feeds itself; the core runs each joined slot's upkeep.
