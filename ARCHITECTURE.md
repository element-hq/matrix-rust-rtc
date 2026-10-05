# Architecture

This document explains the initial architecture of the Matrix RTC Rust workspace.

## Why this structure

The goal is to keep protocol logic in one Rust core crate and make all platform adaptation explicit at the edges.

- `matrix-rtc-core` owns MSC4143, for any application, how a host's
  `MatrixBackend` feeds it (the feeder), and the pre-2026 membership formats a
  room can be read and written in (`compat`).
- `matrix-rtc-call` owns the call application on top of it.
- `matrix-rtc-matrix-sdk` owns the matrix-rust-sdk implementation of that backend.
- `matrix-rtc-wasm` owns JavaScript-facing conversion and wasm export details.
- `matrix-rtc-ffi` owns native binding-facing conversion and UniFFI boundary types.
- `matrix-rtc-transport` owns how media flows for any application on the
  core: the transport contract, frames, constraints, the media key handler,
  and the pure MSC4195 control plane (`livekit`: identity derivations, token
  shapes, dialect choices) shared by the native transport and the web binding.
- `matrix-rtc-call-sdk` owns what a host uses for a call's media: the media
  model over that contract (the roster, tiles, the unified event stream),
  attaching media to a call, and behind features the native LiveKit wiring and
  the `LiveKitCall` facade.

Three axes, kept separate on purpose: the core answers *what the protocol says*,
`matrix-rtc-call` and a backend (`matrix-rtc-matrix-sdk`'s, or the host's) *how it
reaches a homeserver*, and `matrix-rtc-transport` + an implementation of it
*how bytes flow*. Only the top-level facade
(`matrix_rtc_call_sdk::LiveKitCall`) knows all three.

Arrows point at what a crate depends on:

```
 matrix-rtc-wasm                       matrix-rtc-ffi
      │                                  │  ╎ feature "media"
      ▼                                  │  ╎
 matrix-rtc-call-sdk ◀╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌│╌╌┘ (with "livekit")
   │  roster, tiles, attach_media        │
   │   ╎ "livekit"     ╎ "matrix-sdk"    │
   │   ▼               ▼                 │
   │  matrix-rtc-     matrix-rtc-        │
   │  livekit         matrix-sdk         │
   │   │ native SFU     │                │
   ▼   ▼                │                │
 matrix-rtc-transport   │                │
   contract, keys,      │                │
   MSC4195 shapes       │                │
   ╎                    ▼                ▼
┌────────────────────────────────────────────────────────────────────┐
│     matrix-rtc-call   (the call, the dialects)                     │
├────────────────────────────────────────────────────────────────────┤
│                          matrix-rtc-core                           │
└────────────────────────────────────────────────────────────────────┘
   (every crate above also depends on matrix-rtc-core directly, and
    matrix-rtc-transport on it alone — ╎ passes through to the core;
    dashed edges are features: the ffi takes the media crates only
    under "media", and the call SDK with "livekit", never "matrix-sdk")
```

Three things that shape reveals. **`matrix-rtc-matrix-sdk` and the media plane
are siblings, not layers** — the SDK backend and the media plane both sit on the
call layer, neither knows the other exists, and only `matrix-rtc-call-sdk`'s
`matrix-sdk` feature brings them together. **`matrix-rtc-transport` and
`matrix-rtc-livekit` know no call**: it sits on the core
alone, so another application on MSC4143 can carry media with it. And **the
media plane splits by what owns the bytes**: `matrix-rtc-call-sdk` (without
features) and
`matrix-rtc-transport` compile for wasm32 and are shared by both bindings,
while `matrix-rtc-livekit` (libwebrtc, reqwest) stays native-only — browsers keep
using livekit-js for the media itself, driven through a JS delegate.
`matrix-rtc-ffi`'s default build stays slim: the transport and media crates enter
only under its `media` feature, which is what keeps that mobile artifact free of
`libwebrtc`. For legibility the diagram omits the direct edges from
`matrix-rtc-wasm` and `matrix-rtc-ffi` to `matrix-rtc-transport`, and from
`matrix-rtc-ffi` to `matrix-rtc-livekit`.

This keeps the core reusable and testable while avoiding platform-specific dependencies in core.

### Applications on the core

MSC4143 is application-agnostic; a call (`m.call`) is one application, and a
shared board would be a sibling of `matrix-rtc-call` on the same core:

```
   bindings (ffi, wasm) · matrix-rtc-call-sdk's LiveKitCall facade
                 │
                 ▼
          matrix-rtc-call-sdk  engine, pool, tiles, attach_media
                 │                 └──▶ matrix-rtc-transport  contract, keys, MSC4195
                 ▼
          matrix-rtc-call    RtcClient → RtcRoom → RtcSession/RtcCall; reactions, MSC4075
                 │
                 ▼
          matrix-rtc-core    MSC4143: membership, slots, encryption; host vocabulary
```

The dependency diagram at the top omits `matrix-rtc-call`. The media → call edge is temporary,
until the call-side roster and tiles move above the call crate.

What lets an application sit on the core without the core knowing it:

- **`MembershipListener`**: told synchronously of every change to a slot's
  joined memberships in a `BaseRtcRoom`, in order and under the lock that
  applied them.
- **`host/`**: `send_room_event`, `redact_event` and `RawTimelineEvent` exist for
  applications; the core uses none of them.
- **`ApplicationIntake`**: how the feeder feeds an application timeline events
  and relations without knowing which application it is.
- **`IngestDialect`**: which generation of MatrixRTC the feeder reads a room
  in; the core implements it for each `MembershipFormat`.

## Who drives the call

The DAG above never mentions a Matrix SDK. That is not because there is only one
place it could go — it is because there are **two**, and neither is on a default
dependency path. `matrix-sdk` enters the workspace only through
`matrix-rtc-matrix-sdk`, which nothing depends on but `matrix-rtc-call-sdk`'s
`matrix-sdk` feature, off by default.

`MatrixBackend` (defined in `matrix-rtc-core`) is the seam that makes both
topologies work. It is the library's one view of a Matrix client, in both
directions: the sends (sticky, state, delayed and plain room events, redactions,
Olm-encrypted to-device messages) and the reads (a per-room subscription that
delivers the room's *complete current* sticky set, the state events of the
requested types, its joined members and its encryption flag — first on
subscribe, again on every change — plus to-device messages, `/relations`, the
OpenID token and `GET /rtc/transports`). The host's app (through the FFI or
wasm trait) and `matrix_rtc_matrix_sdk::SdkMatrixBackend` (a real `matrix_sdk::Client`)
are two implementations, and the core cannot tell them apart.

The core's **feeder** (`matrix_rtc_core::feeder`: `RoomFeeder`,
`ToDeviceFeeder`) is the read half's one consumer. It subscribes through the
backend, orders what arrives (encryption and slots and members before the
first membership, so nobody is briefly joined to a closed slot), derives
`EventOrigin`/`KeyOrigin` from the decryption facts the client reported, reads
membership and keys through the room's `IngestDialect` (the pre-2026 funnels
of the `MembershipFormat` the room was opened in), and feeds that room. `BaseRtcClient` opens rooms through it and runs the feeds on the
core's executor. One copy, for every host.

### Host-driven — production mobile and web

The Matrix client lives **outside** this workspace, *above* the bindings. It is a
consumer, not a dependency:

```
┌────────────────────────────────────────────────────────┐
│ host app and its own Matrix client                     │
│ matrix-rust-sdk (mobile) / matrix-js-sdk (web)         │
└────────────────────────────────────────────────────────┘
        │ sends                  ▲ subscriptions
        │ MatrixBackend (trait)  │ RoomSink / ToDeviceSink
        ▼                        │
┌────────────────────────────────────────────────────────┐
│ matrix-rtc-ffi   /   matrix-rtc-wasm                   │
│ no matrix-sdk anywhere in the graph                    │
└────────────────────────────────────────────────────────┘
                              │
                              ▼
   matrix-rtc-call ──▶ matrix-rtc-core feeder  (+ call-sdk / livekit under "media")
```

The bindings carry no Matrix SDK at all — not even transitively, and not even
with the FFI's `media` feature on. `cargo tree -p matrix-rtc-ffi --features media`
contains zero `matrix-sdk` entries, because the FFI depends on
`matrix-rtc-call-sdk` with `livekit` but *without* `matrix-sdk`, deliberately,
and `matrix-rtc-livekit` has no Matrix SDK to offer.

### Rust-driven — tests, examples, recording bots

Here the Rust process owns a `matrix_sdk::Client` and the SDK is a **dependency
below**. This is the topology of the e2e call test, `join_and_record`,
`load_test`, and the `connect` example:

```
┌────────────────────────────────────────────────────────┐
│ tests/, examples/, recording bot                       │
│ owns a matrix_sdk::Client directly                     │
└────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────┐
│ matrix_rtc_call_sdk::LiveKitCall                       │
│ matrix_rtc_matrix_sdk::SdkMatrixBackend ──▶ matrix_sdk │
│ both behind feature "matrix-sdk", off by default       │
└────────────────────────────────────────────────────────┘
                              │
                              ▼
   matrix-rtc-call ──▶ matrix-rtc-core feeder
```

### What the two topologies share

`LiveKitCall` still exists only in the Rust-driven topology — it owns a
`matrix_sdk::Client`, so it is gated on `matrix-sdk`. But the wiring under it
is no longer its own: `LiveKitCall::join`, the FFI's `RtcClient` and the wasm
`WasmRtcClient` all open rooms through `matrix_rtc_call::RtcClient` — which
opens each room through the core's `BaseRtcClient`, which wraps the backend in
`DialectBackend`, runs the `ToDeviceFeeder` while any room is open and attaches
each room through `RoomFeeder` in its `MembershipFormat` — and then join. A host of the core
alone opens rooms through `BaseRtcClient` directly. What differs
between them is only where the backend comes from and which runtime is current:
the library spawns the feeds, and the core each joined slot's upkeep, on
`matrix_rtc_core::executor` (the current tokio runtime natively, `spawn_local`
on wasm).

## High-level data flow

This is the host-driven topology above, in detail.

1. The host opens a room (`RtcClient::room`), naming its membership format.
2. The core's feeder asks the backend to `subscribe_room` for what that format needs; the host's client
   delivers the current sets into the `RoomSink` and keeps delivering them as they change.
3. The feeder applies encryption, slot state and joined members first, then translates the
   sticky (or, pre-sticky, state) member events — content verbatim plus the client's decryption
   facts — into `RawStickyEvent`s and hands the whole set to the room's `BaseRtcRoom`.
4. The room groups events by `slot_id` and forwards each batch once to that slot's `SlotSession`.
5. The host joins a slot on the room (`join` / `join_call`) and gets back an `RtcSession` /
   `RtcCall`: our participation, with its keys and leave. The core's join starts the slot's upkeep
   task, which keeps it alive (MSC4140 restart, sticky refresh) and performs each key rotation at
   its deadline until the leave; the session object aborts it when dropped without leaving.

Membership is always applied as a complete set: a member whose event is absent from the set has left.

## Crate boundaries

## `crates/matrix-rtc-core`

- MSC4143 only: membership, slots and their join conditions, per-member
  encryption, our own membership's lifecycle.
- `BaseRtcRoom` is one room: its slots, members, encryption, and a `SlotSession`
  per observed or joined slot. Events and keys for another room are dropped.
- `ApplicationInfo` carries the whole `application` object both ways; the core
  reads only `type`.
- `MatrixBackend`, the host's contract (`host/backend.rs`), with the `EventIn`
  / `ToDeviceMessageIn` carriers the read half delivers; `testing::MockBackend`
  under the `testing` feature.
- `feeder`: `RoomFeeder::attach` subscribes a room through the backend and runs
  the routing described under "Who drives the call", reading it through an
  `IngestDialect`; `ToDeviceFeeder` routes to-device keys to the open room they
  are for (`RoomRegistry`) and drops those for a room that is not open.
- `BaseRtcClient` opens rooms (one live handle per room; a second is refused)
  and runs their feeds; `BaseRtcRoomHandle::seeded` resolves once the room's
  current state has been applied.

- A join names its slot, application and transport; who joins is the
  backend's account. Choosing the transport is the application's.
- `BaseRtcClient` wraps the backend in `compat::dialect_backend::DialectBackend`,
  the one `MatrixBackend` wrapper that applies the outbound half of a room's
  format (member-event routing, legacy key type, pre-sticky leave) before
  delegating; nothing else rewrites outbound JSON. A room is opened in a
  `MembershipFormat` (`RoomOptions`), and `BaseRtcRoomHandle::prepare_join`
  gives a join that format's `member.id` and registers its outbound dialect.
- `compat`: the MatrixRTC membership formats that predate the 2026 MSC4143
  rewrite, which matrix-js-sdk — and so Element Call, the sole other
  implementation available to test against — still speaks. They carry any
  application. Pure JSON translation: its unit tests need no homeserver.
  Scaffolding with a delete-by date, selected per room by `MembershipFormat`:
  - **`Sticky2025`**, the 2025 format: already MSC4354 sticky-based, differing
    only in the fields inside the member content. *Reading* it is permissive and
    always on, `Current` included (it only fills in modern fields that are
    absent, so spec-shaped events pass through untouched); *writing* it is
    opt-in, being the half that changes what other clients see.
  - **`RoomState`**, the format before MSC4354: membership as
    `org.matrix.msc3401.call.member` **room state**, a plain `{user}:{device}` SFU
    participant identity, and the pre-MSC4195 `/sfu/get` token endpoint. Opt-in in
    both directions, and not additive — such a session is visible to that
    generation and to nobody else. `BaseRtcRoom` still sees only MSC4143: the
    state events are translated into synthetic sticky memberships by the feeder,
    and the slot condition is left unenforced because that generation has no
    slot concept.
  - Two things refuse to be JSON and so live outside `compat` as one `match`
    each, in `matrix_rtc_transport::livekit` because both are MSC4195 rather than
    Matrix concerns: the token endpoint and the identity derivation. The backend
    knows no format: it delivers whatever state types the feeder asks for.

## `crates/matrix-rtc-call`

- The host-facing objects: `RtcClient` (one per backend, no I/O) opens an
  `RtcRoom` (one live object per room; a second is refused), whose `join`
  returns an `RtcSession` and `join_call` an `RtcCall` (a session plus
  reactions, the raised hand and the ring). `close`/`leave` are the clean
  paths; dropping sends no leave, and the delayed leave expires the
  membership.
- Reactions and the raised hand (Element Call, unspecced) and MSC4075
  notifications, in `CallRoomState` over the room's `BaseRtcRoom`: `join` rings
  the room if we started, `leave` lowers our hand first, and the hand follows
  our membership event onto each refresh (the core's event-id watch).
- The library does not detect incoming calls: MSC4075 is send-only here, and a
  host learns of a ring through its own SDK or push path.
- Depends on the core alone; its timers are the core executor's; compiles for wasm32.
- `RtcClient` opens each room through the core's `BaseRtcClient`, with
  `CallRoomState` as the room, in the host's `MembershipFormat`; its join
  renders through `BaseRtcRoomHandle::prepare_join`. `RtcRoom::seeded`
  resolves once the room's current state has been applied, which is what the
  bindings' `room` awaits.
- `transports::choose`: the library's transport choice (first LiveKit entry of
  the backend's `rtc_transports()`, unless the join names one), asked before
  the room's lock is taken and handed to the core's join.

## `crates/matrix-rtc-matrix-sdk`

- The matrix-rust-sdk backend, and deliberately transport-free — nothing in it
  knows what a LiveKit SFU is, so a second transport reuses it unchanged.
- `sdk`: `SdkMatrixBackend` implements
  `MatrixBackend` over a `matrix_sdk::Client` — Client-Server requests for the
  sends, and for the reads one task per room subscription that re-emits the
  complete sets on sticky-store and room-state wakes. It reads MSC4354 sticky
  events (the SDK's `unstable-msc4354`) and the state types the feeder asks
  for: from the SDK store for a type sliding sync keeps there (the pre-sticky
  `m.call.member`, which it recognises through ruma), and from one `/state`
  fetch for the rest. Depends on the core alone.

## `crates/matrix-rtc-call-sdk`

- The call's media model over `matrix-rtc-transport`: `Participant` roster
  keyed by `member_id`, `CallEvent` (the unified membership + media event
  stream), tiles, and the `CallEngine` that reconciles core membership
  snapshots with transport `ConnectionEvent`s: reverse identity mapping
  (pseudonymous identity → membership), buffering of media that arrives
  before its membership, roster/event emission. It stores per-stream
  `MediaConstraints` (debounced and re-applied whenever a stream (re)appears)
  and routes publications to the own focus; `CallEngine::unpublish` retracts
  one — a mute keeps the publication up, which is right for a camera but not
  for a stopped screen share.
- The engine owns the **multi-focus connection pool** (MSC4195 multi-SFU):
  members are grouped by their published transports' connection key
  (LiveKit: the `livekit_service_url`); the engine connects to every peer
  focus via `MediaTransport::connect` (exponential backoff on failure),
  closes connections whose last member left after an idle grace, and
  reconnects a dead peer-focus connection after tearing down its streams.
  Only the *own* focus — established synchronously by the caller so join can
  fail fast, then handed over via `adopt_own_connection` — ends the call
  when it dies.
- `attach_media` attaches media to a joined `RtcCall`: the one copy of the
  order-sensitive wiring (identity mapper before key handler, key listeners
  before the replay of held keys, replay before the own-focus connect, our
  sender's key index after it) shared by the FFI and wasm media sessions and
  `LiveKitCall`. Transports opt in with `OwnFocusTransport`, which hands the
  caller a typed own-focus connection.
- Behind `livekit`: `attach_livekit` builds the MSC4195 key provider, bridge
  and `matrix-rtc-livekit` transport for a joined call (token endpoint and
  identity from the call's `MembershipFormat`) and runs `attach_media` over
  them — what the FFI's media session is.
- Behind `matrix-sdk`: `LiveKitCall::join`/`LiveKitCall::leave`, a facade that
  composes `matrix-rtc-matrix-sdk`'s `SdkMatrixBackend` and `matrix-rtc-call`'s
  client with `attach_livekit`: membership, key exchange, the library's
  transport choice, the E2EE SFU connection, and a `CallEngine` in one handle
  (the crate README's quick start; also what the examples and the e2e test
  drive). `LiveKitCall::subscribe_call_events`/`LiveKitCall::participants` are
  the transport-agnostic surface; the raw `LiveKitCall::events`/
  `LiveKitCall::session` accessors remain during the transition.
- Without features it depends only on `matrix-rtc-core`, `matrix-rtc-transport`,
  `matrix-rtc-call` + tokio/futures — no LiveKit, no
  libwebrtc, fully unit-testable (`FakeTransport`). Compiles for wasm32:
  the transport traits are `Send + Sync` off wasm (via `MaybeSend`) and
  unconstrained on it, and tasks/timers go through `matrix_rtc_core::executor`
  (tokio natively; `spawn_local` + setTimeout-backed sleeps in the browser,
  where the engine's actor runs on the JS microtask queue).
- Design/feasibility notes and the phased plan:
  `agent-workspace/media-abstraction/PLAN.md`.

## `crates/matrix-rtc-transport`

- How media flows, for any application on the core — no call vocabulary, no
  IO, no libwebrtc; compiles for wasm32.
- `connection`: the `MediaTransport`/`TransportConnection`/`RemoteTrackHandle`
  traits a transport implements (LiveKit today; P2P/WebTransport designed
  for), `OwnFocusTransport` for a typed own-focus connection, and the
  `ConnectionEvent`s it reports.
- Owned frames (`AudioFrame` PCM, `VideoFrame` I420), the publish surface
  (`PublishOptions` → `LocalTrackHandle`; the application pushes captured
  frames in, the transport owns encoding/simulcast), `ReceiveStats`, and
  per-stream `MediaConstraints` (visibility, rendered size, quality cap →
  subscribe-side simulcast control).
- `keys`: `FrameKeyRing` is the seam a transport's key ring implements
  (LiveKit native's `KeyProvider`, livekit-js's `ExternalE2EEKeyProvider`),
  and `MediaKeyHandler` owns the recording, ring-size guard, rejected-key
  rule, local sender's index switch, and the MSC4143 `delayBeforeUse` wait.
- `livekit`: the pure half of the MSC4195 control plane, shared by the native
  transport and the web binding — `identity` (the hash derivations), `token`
  (`/get_token` and legacy `/sfu/get` request builders and the response
  decoder), `TokenEndpoint`, and `identity_mapper` (the per-generation identity
  derivation, deliberately a Rust closure because `RtcIdentityMapper` is
  `Send + Sync`). `matrix-rtc-livekit` re-exports it under its original paths
  and adds the IO (reqwest, the LiveKit client).

## `crates/matrix-rtc-wasm`

- Exposes `WasmRtcClient` → `WasmRtcRoom` → `WasmRtcCall` to JavaScript, the
  client constructed over the page's `MatrixBackendHost` (`backend.rs`:
  `JsBackend` adapts the JS object to the core trait;
  `WasmRoomSink`/`WasmToDeviceSink` are the sinks the host pushes into).
  `client.room` runs the room's feed on `spawn_local`; `room.close` leaves and
  unsubscribes. The room has `openSlot`/`closeSlot` and `joinCall`. A joined
  call keeps itself alive and rotates its keys; the page ticks nothing.
- `media/`: `call.connectMedia` attaches media to the joined call through
  `matrix_rtc_call_sdk::attach_media` — the shared `CallEngine` (roster +
  multi-focus pool) over `JsMediaTransport`, a JS
  delegate driving livekit-js. Rust owns the protocol (token requests via
  `matrix_rtc_transport::livekit`, identities, pool policy, key bookkeeping via the shared
  `MediaKeyHandler` backed by the delegate's `setKey`); JS owns the IO
  (OpenID token, `fetch`, `Room.connect`) and all media. Room events come
  back through the typed `WasmConnectionEventSink`; roster entries carry
  `rtc_identity` for joining to livekit-js participants. Roster/event/
  switch-complete delivery is push (delegate callbacks from spawned
  pumps) — deliberately not async session methods, which would park a
  wasm-bindgen object borrow across an await.

## `web`

- Browser-first JavaScript packaging around `crates/matrix-rtc-wasm`.
- Uses `wasm-pack` to generate browser and Node.js runtime bundles into ignored `pkg/` subdirectories.
- Keeps generated JavaScript/WASM artifacts out of git while providing a small JS test surface.
- `src/matrix-js-sdk-host.mjs` (export `./matrix-js-sdk-host`): `MatrixHost`,
  the `MatrixBackendHost` over matrix-js-sdk — sends, the per-room
  subscription (complete sets on subscribe and on every change, decrypted,
  with megolm sender attribution) and to-device keys with their Olm metadata.
- `src/matrix-rtc-call.mjs` (export `./call`): the `MatrixRtcCall` wrapper —
  implements the media delegate over `livekit-client` (optional peer
  dependency, injected) and joins roster entries to
  live livekit-js participants by `rtc_identity`.

## `crates/matrix-rtc-ffi`

- Exposes UniFFI objects and records for Swift/Kotlin consumers.
- Keeps FFI DTOs local to the crate and converts them into core DTOs.
- Preserves session subscription semantics through a polling subscription object.
- The host implements the `MatrixBackend` foreign trait (`backend.rs`) and
  constructs `RtcClient` over it; `room(room_id, FfiRoomOptions)` subscribes
  through it and resolves to an `RtcRoom` once the room's current state is
  applied, and `RtcRoom::shutdown` leaves and unsubscribes (not `close`, which
  Kotlin's `AutoCloseable` owns). `join_call` returns an `RtcCall` whose 10 s
  upkeep (keep-alive, key rotations) runs until it leaves or is dropped. Both hop onto the
  crate's own runtime (`runtime.rs`) so what they spawn lands there. Inbound events reach
  the library only through the `RoomSink`/`ToDeviceSink` objects the
  subscriptions hand the host — there are no feed methods on the objects.
- Behind the **`media` cargo feature** (default off — pulls the LiveKit
  client and libwebrtc, ~8–15 MB per ABI): `src/media/` exposes the
  transport-agnostic media model to mobile. The host joins the slot through
  the room as usual, then `connect_media_session(call, config)` attaches media
  through `matrix_rtc_call_sdk::attach_livekit` (E2EE key bridge into the core,
  the `CallEngine` with its multi-focus pool, the own-focus SFU connection). `MediaSession` surfaces `next_event()` (async
  pull → Kotlin `Flow` / Swift `AsyncStream`), the participant roster,
  `set_constraints`, frame streams (audio frames by value; video frames as
  objects with safe copies *and* zero-copy plane pointers), and local
  publications the host pushes captured PCM/I420 into. OpenID tokens and
  outbound keys go through the same `MatrixBackend`. All media work
  runs on a dedicated multithreaded tokio runtime — the core's `?Send`
  futures never touch it. Android gets a `JNI_OnLoad` that initialises
  libwebrtc.

## `crates/matrix-rtc-livekit`

- Implements the MSC4195 LiveKit transport: the "LiveKit SDK" layer that turns
  `matrix-rtc-core`'s membership/key outputs into a live SFU media session.
- Owns the authorisation-service `/get_token` exchange (`token`; and the
  pre-MSC4195 `/sfu/get` one, for legacy interop) and the MSC4195
  hash derivations (`identity`), drives a LiveKit `Room` (`session`), and bridges
  core media keys into LiveKit per-participant frame encryption (`keys`,
  `MediaKeyBridge` → `KeyProvider`, HKDF mode, GCM frames).
- Obtains the Matrix OpenID token via the core's `MatrixBackend::openid_token`,
  so the crate is not hard-wired to a particular Matrix SDK. `MemberClaims`
  stays here: those are the `/get_token` request body's claims, which no
  homeserver ever sees.
- Implements `matrix-rtc-transport`'s traits in `transport_impl`
  (`LiveKitMediaTransport`): connection key = `livekit_service_url`, remote
  identity = MSC4195 pseudonymous identity, `RoomEvent` → `ConnectionEvent`
  translation, and `NativeAudioStream` → owned PCM frame streams behind
  `RemoteTrackHandle`.
- Knows no call: no `matrix-rtc-call`, no call SDK, no Matrix SDK. The token
  endpoint and identity derivation a membership format implies are
  `matrix_rtc_transport::livekit`'s; the caller picks them.
- Native-only by nature (the LiveKit client pulls in `libwebrtc`); never targets wasm.

## Spec alignment

- `MSC4143` (MatrixRTC): membership events represented by `m.rtc.member`.
- `MSC4354` (Sticky events): membership updates are received as sticky events.
- `MSC4075` (Notifications & ringing): `m.rtc.notification`, send side only.

The core uses the stable ids (`m.rtc.member`, `m.rtc.slot`) internally, but the
deployed ecosystem still matches on the unstable `org.matrix.msc4143.*` ones, so
bindings translate on the way out: the `matrix-sdk` host via ruma's alias table
(`matrix-rtc-matrix-sdk`'s `sdk::wire_event_type`), the FFI and wasm bindings — which hand the
type to an SDK that puts the string on the wire verbatim — via
`matrix_rtc_core::wire_event_type`. Inbound, both spellings are accepted.

Current implementation only establishes event intake and membership state wiring; protocol completeness is intentionally deferred.

### MSC4143 catch-up status

Tracked against the rewritten proposal
([MSC4143](https://github.com/matrix-org/matrix-spec-proposals/pull/4143)). The
`m.rtc.member` wire format now matches it:

- `member.membership` (`join` / `leave`) is the explicit join signal; the old
  inference from content shape is gone.
- `leave_reason {code, reason}` replaces `disconnect_reason {class, reason,
  description}`. Note the inversion: `code` is the machine-readable half and
  `reason` the human-readable one.
- `transports {published, can_subscribe}` replaces the flat `rtc_transports`
  array.
- `member.claimed_user_id`, `member.claimed_device_id`, `versions`,
  `m.relates_to` and `created_ts` are gone. The sending device now comes from the
  event's decryption metadata and rides on `RawStickyEvent::origin`, which the
  Matrix bridge fills from the sticky event's `EncryptionInfo`.
- `member.id` is generated fresh per join (`generate_member_id`), as the spec
  requires; it is no longer derived from the user and device IDs.

Inbound `m.rtc.encryption_key` messages are checked before use. A key is only
stored and signalled once it has been matched against the sender's member event:

- The host reports how the message arrived via `KeyOrigin`, built from Olm
  decryption metadata. Cleartext messages are discarded, since nothing in the
  payload can be trusted to identify a sender.
- The to-device sender and its device must equal the sender and sending device
  of the `m.rtc.member` event the message names, or the key is discarded. A key
  naming another room is discarded too.
- If the member event names no device to check against — cleartext, or encrypted
  but not attributable to one — the match cannot be performed, so the key is
  discarded rather than accepted on the user match alone. An encrypted member
  event should always resolve to a device (Olm messages carry the sender's device
  keys), so that half is a backstop rather than an expected path. The exception
  is `EventOrigin::Unknown`, where the host reported nothing at all and the rule
  is skipped like every other unreported fact.
- Keys from devices that are not cross-signed are discarded unless
  `EncryptionConfig::require_cross_signed_sender` is turned off (MSC4153).
- A key that arrives before its member event is buffered *with its origin* and
  checked when the membership shows up — verification is deferred, never
  skipped. Rejected keys never reach the outdated-key filter, so a bogus key
  cannot take the `(member, index)` slot and suppress the genuine one.

The outgoing key message declares `format: 0` as the spec requires.

### Notifications and ringing (MSC4075)

Membership says who is *in* a session, never who should be *summoned* to one, so
a mobile client had nothing to raise an incoming call from. `matrix-rtc-call`'s
`notification.rs` builds the `m.rtc.notification` that fills the gap;
`RtcRoom::join_call` sends it when the host set `CallJoinOptions::notify`.
Three decisions are worth knowing:

- **The relation is what forced a breaking host change.** MSC4075 requires an
  `m.reference` to the sender's own `m.rtc.member` event, and nothing in this
  workspace had ever seen an event id — `send_sticky_event` returned `()` and
  `RawStickyEvent` carried no id either (it does now, for reactions; see
  below). Both `send_sticky_event` and
  `send_state_event` now return `String`, filled from the send response at every
  implementation site, and `OwnMembershipMachine::join` hands the membership's id
  back to its caller. Not `Option<String>`: every Matrix send responds with an
  event id, so an implementation that cannot produce one is broken, and failing
  loudly beats a call that joins fine and quietly never rings. `send_state_event`
  is included because the pre-MSC4354 Element Call dialect routes the membership
  through it. Recovering the id from our own membership echoing back through sync
  (what matrix-js-sdk does) was rejected for putting a full sync round trip in
  front of the ring.
- **Only the starter notifies.** The MSC leaves the question open, but every
  joiner sending one rings the room once per participant, so the send is
  suppressed unless the roster holds somebody *else*. "Else" is load-bearing:
  the host feeds the room's whole sticky map, so our own membership is in it as
  soon as the homeserver echoes it back, and a session outlives `leave()`
  keeping the previous call's membership as a candidate. Counting either
  concludes somebody else started the call and rings nobody. The check therefore
  excludes memberships from our own user whose sending device is ours *or
  unreported* — deliberately wider than the roster's own
  `SupersededOwnParticipation` rule, which needs a known device and so leaves
  such a candidate in. Being wrong that way costs one extra ring in an
  unencrypted room; being wrong the other way is a call that silently never
  rings.
- **The content states the call fields twice.** The MSC nests them under
  `application`; Element Call and ruma's `RtcNotificationEventContent` read them
  at the top level, and ruma *requires* them there, so a purely nested event
  fails to deserialize in the very SDK the mobile client uses. Both are written.
  A pre-2026 `format` additionally strips `application` and `m.text` for the
  byte-exact legacy shape.

Receiving is not implemented: the MSC's ring conditions, lifetime expiry against
`origin_server_ts`, `m.call.ring.ack` acknowledgements and the sender-side
"still ringing" indication are all absent, and on mobile the first signal is a
push notification that never passes through this workspace anyway.

### Reactions and the raised hand (Element Call, unspecced)

Element Call's reactions are ordinary room events that *relate to the reacting
member's own membership event*: an emoji reaction is an `io.element.call.reaction`
with an `m.reference` and `emoji` / `name` fields, a raised hand is an
`m.reaction` annotation with key `🖐️`, lowered by redacting it. Nothing in the
content is trusted beyond that relation — the receiver checks that the reaction's
sender is the membership's sender, which the homeserver authenticated. The
protocol lives in `matrix-rtc-call`'s `reactions.rs` and is driven per room by
`CallRoomState`; the decisions worth knowing:

- **Only the protocol is in the SDK; sound and display are the host's.** The
  media crate has no playout path, and capture and render are platform-side by
  design, so a received reaction carries a sound *hint* (`ReactionSound`,
  resolved from Element Call's catalogue by `name`; unknown names map to the
  generic sound) and the host plays its bundled asset. Element Call's "play
  reaction sounds" toggle is therefore a host setting; what the SDK owns is the
  gating — an enable flag, the three-second per-member active window Element Call
  applies on receipt, and a send cooldown that refuses what peers would drop.
- **The membership event id moves, and the hand follows it.** A sticky refresh
  re-sends the membership and the new event replaces the old in the sticky map;
  matrix-js-sdk's `CallMembership.eventId` follows it, and Element Call drops a
  raised hand whose membership event has moved on, re-querying the new event's
  relations. So `OwnMembershipMachine` now tracks the latest event id, and
  each keep-alive tick re-annotates our hand onto the new event (redacting the old
  annotation) whenever it has moved — every 30 minutes at the default lifetime.
  Peers may see the hand drop for one round trip in between; that is the
  protocol's, not ours. As a *receiver* we are more lenient: a hand stays up for
  as long as the member is in the call, and a reaction is validated against every
  membership event id seen for that member, not only the latest.
- **Event ids had to reach the core.** `RawStickyEvent` and `JoinedMembership`
  carry the membership event id (optional in the DTO, so a host that cannot
  supply one still compiles — but its members cannot then be reacted for). The
  roster republishes when only ids moved; nothing downstream churns on it, since
  the media engine and key distribution diff by `member_id` and `membership_ts`.
- **Hands raised before we joined come from `/relations`.** The timeline we see
  live starts at our join; the annotation lives in the relations of the member's
  membership event. The call layer lists membership events whose relations it
  has not seen (`pending_relation_lookups`), the host answers each with
  `rel_type=m.annotation`, `event_type=m.reaction` (`on_relations_received`), and
  only hands are taken from the answer — an hour-old applause is not replayed.
  The feeder does this for every host after each membership apply, one
  `MatrixBackend::relations` request per new membership event id.
- **Inbound needed a new intake.** The core only ever saw sticky, slot and
  to-device traffic. `CallRoomState::on_timeline_events` and
  `on_event_redacted` take the room's events — a reaction names no slot — and
  hand them to every joined slot of the room, each keeping what relates to its
  own members. They arrive
  through the room subscription's `on_timeline_events`/`on_redaction`, which the
  feeder subscribes to for the application's `timeline_event_types`
  (`SdkMatrixBackend` from a room event handler; the web host from `RoomEvent.Timeline`,
  `MatrixEventEvent.Decrypted` and `RoomEvent.Redaction`).
- **Outbound needed two sends.** `MatrixBackend::send_room_event` (a plain
  message-like send, encrypted by the client SDK in an encrypted room) and
  `redact_event`.

The media layer merges the result onto the roster: `Participant.hand_raised_at_ms`
plus `CallEvent::HandRaised` / `HandLowered` / `Reaction`, so a UI can order
tiles by who asked first without touching the core.

### Slots and the join conditions

`m.rtc.slot` is modelled in `slot.rs` and resolved to `SlotState::Open`/`Closed`
per MSC4143: open requires `status = "open"` plus an application whose `type`
agrees with the state key, and anything else — a closed status, a missing
application, empty content, a status from a future revision — is closed.

A session keeps its member events as *candidates* and projects the joined set
from them, so the conditions are re-evaluated whenever their inputs move rather
than only at ingestion. Closing a slot therefore leaves everyone in it, and
reopening restores whoever is still sticky. The projection also drives key
distribution, so a member who drops out of the joined set stops receiving keys.

The two room-state conditions are only enforced once a host supplies the state:

- `BaseRtcRoom::on_slots_received` takes the room's complete slot state.
  Calling it is what switches the room from "unknown" (condition unevaluable, so
  unenforced) to enforcing; a slot absent from that call is closed, not unknown.
- `BaseRtcRoom::on_members_received` supplies the room's joined users.

This is deliberate: enforcing an unevaluable condition would silently empty every
session for hosts that do not yet feed room state. The feeder feeds both.

`open_slot` / `close_slot` send the state event through the command sender's new
`send_state_event`.

### Encryption negotiation

Whether RTC data is encrypted is prescribed by the slot, not chosen locally.
`RawSlotEvent::resolve` takes the room's encryption state alongside the event,
because MSC4143 ties the two together in both directions:

- **Encrypted room.** A slot MUST carry an `encryption` object, so one without it
  resolves closed. A mechanism this client cannot implement also closes the slot,
  since encryption is required there and taking part without it would break the
  same requirement. `m.per_member` (and its unstable id) is the only one
  implemented.
- **Unencrypted room.** RTC encryption MUST NOT be used, so a declared mechanism
  is dropped rather than honoured. The slot stays open; `OpenSlot::mechanism` is
  `None` while `OpenSlot::encryption` still reports what was declared, so callers
  can see the mismatch.
- **Unknown.** Neither rule applies and the declared mechanism is taken at face
  value, matching how the other room-state conditions stay unenforced until a
  host opts in via `on_room_encryption_received`.

`RtcSession::negotiated_encryption` turns that into the key-management decision
at join time, overriding `EncryptionConfig::manage_media_keys`; the local flag
only applies where there is no slot state to negotiate from. A slot whose
mechanism changes mid-session is not renegotiated — the dangerous direction, the
slot closing, is already covered because that leaves every member.

Separately, a member event that arrived in the clear does not count as joined in
an encrypted room. That and the sending device are one value,
`RawStickyEvent::origin` (`EventOrigin`), because both come from the same
decryption metadata — a cleartext event cannot carry a sending device, and the
type makes that unrepresentable. `EventOrigin::Unknown` is distinct from
`Cleartext`: it means the host did not report, so the rule is skipped rather
than failed. A host never builds an `EventOrigin`: it reports what its client
can honestly say — `EventEncryption::{Cleartext, Encrypted { sender_device_id,
sender_cross_signed }}` on each `EventIn` — and the feeder derives the origin
(and, for keys, `KeyOrigin`) from it.

### Transports and who chooses them

The core has no HTTP of its own; `GET /_matrix/client/v1/rtc/transports` is
the backend's `rtc_transports()`, returned raw. The **library** then chooses
(`matrix_rtc_call::transports::choose`): the first LiveKit entry the
homeserver advertises, unless the join names a transport — kept as an override
for tests, debugging and pinning a specific focus. A homeserver without the
endpoint reports `[]`, and a join with no override then fails with a clear
message rather than guessing.

What the core does model is the *intent*, via `TransportIntent`:

- `Publish(transport)` — publish on this transport, and advertise its type as
  `can_subscribe`.
- `ReceiveOnly { can_subscribe }` — publish nothing. MSC4143 puts no REQUIRED
  marker on `transports`, so a member that only receives — a recorder, an
  observer — is a valid participant rather than a broken one. Stating
  `can_subscribe` still matters, since that is what tells other members which
  transport to publish on so this one can hear them.

Still outstanding:

1. **Mid-session renegotiation** — a slot that changes its encryption mechanism
   while a session is live keeps the mechanism negotiated at join.
2. **Slot state comes from a server fetch, not the store** (`SdkMatrixBackend`) —
   sliding sync only delivers state types listed in `required_state`, and the
   SDK's room-list defaults do not include the MSC4143 slot type, so the local
   store reports every room as slotless (which the core reads as "slot closed,
   everyone left"). The backend therefore fetches `GET /rooms/{id}/state` on
   each wake and skips the set when the fetch fails. The real fix is adding the
   slot type to the SDK's sliding sync `required_state`, then reading the state
   store.
3. **A unified `CallEvent` stream on the `LiveKitCall` facade** — landed as
   `matrix_rtc_call_sdk::CallEvent` via `LiveKitCall::subscribe_call_events` (peer
   joined/left, stream started/stopped, key imported, connection health,
   ended-with-reason). Remaining: migrate the e2e test and examples off the
   raw `LiveKitCall::events`/`LiveKitCall::session` accessors and delete them, and surface
   slot-close as `CallEvent::Ended`.

## Logging

Every crate emits through the [`log`] facade — as do `livekit` and `libwebrtc`, so one
`log::Log` implementation captures the SFU and WebRTC stacks too. Nothing is visible
until a binding installs that implementation; hosts must do this first or the SDK is
silent:

| Binding | Entry point | Destination |
|---|---|---|
| `matrix-rtc-ffi` | `setup_logging(RtcLogConfig, Option<Arc<dyn RtcLogSink>>)` | logcat on Android (tag `matrix-rtc`), stderr elsewhere, and/or a host `RtcLogSink` |
| `matrix-rtc-wasm` | `initLogging(level, filter)` | the JS console |

Both take the same `RUST_LOG` filter syntax, e.g.
`"matrix_rtc_core::session=trace,livekit=info,webrtc_sys=warn"`. Hosts can push their own
lines into the stream with `log_event` / `logEvent` so app and SDK logs interleave in one
timeline, and dump current state with `debug_snapshot()` / `debugSnapshot()`.

**Conventions.**

- **Targets are module paths** (the `log` default — no explicit `target:`). The filterable
  roots are `matrix_rtc_core`, `matrix_rtc_call_sdk`, `matrix_rtc_transport`, `matrix_rtc_livekit`, `matrix_rtc_ffi`,
  plus third-party `livekit` and `webrtc_sys`.
- **Session-scoped lines are prefixed `[{room_id}/{slot_id}]`.** `SlotSession` carries a
  pre-formatted `log_tag` for this, built from the room and slot it was created for.
- **Levels.** `error` = broken invariant. `warn` = recoverable or protocol-deviant.
  `info` = lifecycle milestones only (a whole call should produce a few dozen). `debug` =
  one line per decision: sticky event ingested, membership diff, slot resolved, key
  received, command sent. `trace` = hot paths and payloads — keep-alive ticks, per-frame
  work, event content JSON.
- **Never logged:** key material, LiveKit JWTs, OpenID tokens. `token.rs` logs a JWT's
  length, never the token. Event content JSON is `trace`-only because to-device messages
  carry keys.

**The membership-projection logs are the load-bearing ones.** A member silently vanishing
from the roster is the hardest failure to diagnose from the outside, so
`RtcSession::join_condition` returns a `JoinCondition` reason rather than a `bool`, and
`refresh` logs both the joined/left diff and every excluded candidate with its reason
(`SlotClosed`, `UnencryptedInEncryptedRoom`, `SenderNotInRoom`). `debug_snapshot()`
reports the same per-candidate verdicts as JSON.

## Non-goals in this first skeleton

- No dependency on `ruma` in core.
- No persistence/storage layer.
- No to-device processing.
- No transport integration (`MSC4195`) yet.
- No production-ready ABI/error model yet.

## Next increments

1. Add a richer membership schema validation layer aligned with MSC field requirements.
2. Introduce explicit machine outputs (commands/events) to communicate with host clients.
3. Add persistence abstraction for sessions and sticky membership maps.
4. Add transport discovery and focus modeling (`MSC4195`).
5. Model `transports.published` / `can_subscribe` in sticky membership DTOs and membership projections (`MSC4143` / `MSC4195`).
