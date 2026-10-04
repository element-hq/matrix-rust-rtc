# matrix-rtc-core

The [MSC4143](https://github.com/matrix-org/matrix-spec-proposals/pull/4143) MatrixRTC core, for
any application: a call, a shared board, a game. It holds no call features; those live in
`matrix-rtc-call`.

It does four things:

- **Membership.** It turns the room's `m.rtc.member` sticky events into, per `(room, slot)`, the
  memberships joined to the slot, applying the MSC4143 join conditions (an open `m.rtc.slot`, the
  sender in the room, encryption in an encrypted room).
- **Our own membership.** It joins and leaves a slot, refreshes the sticky entry, and arms a
  delayed leave as a dead man's switch.
- **Encryption.** It creates, distributes, rotates and checks per-member media keys.
- **Slots.** It resolves `m.rtc.slot` state, and opens and closes slots.

The core does its I/O through the host-implemented `MatrixBackend`: it sends through it, and
its `BaseRtcClient` opens a room by subscribing through it. The `feeder` then applies the room's
state and membership, slot and room state, and decrypted key messages. Its `executor` module
(tokio natively, `spawn_local` and `setTimeout` on wasm) is what the library's background work
runs on: a room's feeds while its handle lives, and from a join until the leave the slot's
upkeep (keep-alive, sticky refresh, key rotations at their deadline). A host using the core alone
feeds and ticks nothing. Off a tokio runtime (natively) nothing is spawned, and the host calls
`keep_alive` itself.

## Quick start: join a slot and follow its memberships

`BaseRtcClient::room` opens a room: it subscribes through the backend to what the room needs and
applies the room's current state in the right order (encryption and slots and members before the
first membership), in the room's membership format:

```rust,ignore
use std::sync::Arc;

use matrix_rtc_core::{
    BaseRtcClient, JoinSessionParams, LeaveSessionParams, LiveKitTransport, MatrixBackend,
    RoomOptions, RtcTransport,
};

const ROOM: &str = "!room:example.org";
const SLOT: &str = "org.example.board#planning";

async fn run(
    // The host's Matrix client, behind the one trait the library knows.
    backend: Arc<impl MatrixBackend + 'static>,
) -> Result<(), Box<dyn std::error::Error>> {
    // One per backend; creating it does no I/O.
    let client = BaseRtcClient::new(backend);

    // Natively, from within a tokio runtime: the room spawns its feeds onto it.
    let room = client.room(ROOM, RoomOptions::default()).await?;
    // Resolves once the room's current state has been applied.
    room.seeded().await;

    // The slot's joined memberships, without joining it.
    let mut members = room.observe(SLOT).await;
    tokio::spawn(async move {
        while members.changed().await.is_ok() {
            println!("{} joined", members.borrow().len());
        }
    });

    // Who joins is the backend's account. The transport is the application's
    // choice (`matrix-rtc-call` takes the homeserver's first LiveKit one).
    // Without `.slot`, the application's room-wide slot,
    // `org.example.board#room`; other setters override one default each
    // (`.keep_alive_interval_ms(..)`, `.member_id(..)`, …). The joined slot
    // keeps itself alive until it leaves.
    let join = JoinSessionParams::application("org.example.board")
        .slot("planning")
        .transport(RtcTransport::LiveKit(LiveKitTransport {
            livekit_service_url: "https://sfu.example.org".to_owned(),
        }));
    room.join(join).await?;

    room.leave(SLOT, LeaveSessionParams::new()).await?;
    Ok(())
}
```

Dropping the room handle ends its subscriptions without leaving: a membership left behind expires
through its delayed leave.

### Older membership formats

`RoomOptions::format` picks the MatrixRTC membership format the room is read and written in
(`compat::MembershipFormat`):

- `Current` (the default) is MSC4143 + MSC4354, and also reads the 2025 sticky shapes.
- `Sticky2025` writes our membership with the 2025 fields alongside the spec ones.
- `RoomState` reads and writes membership as pre-sticky `org.matrix.msc3401.call.member` room
  state, joining with the `{user}:{device}` member id that generation expects.

These are matrix-js-sdk's (and so Element Call's) pre-2026 formats, for any application.
`compat` is scaffolding, to be deleted once Element Call catches up.

### An application over the core

An application opens its own room state with `BaseRtcClient::open_with(room_id, state,
options)`: the state is any `ApplicationIntake`, which also receives the timeline events,
redactions and `/relations` it asks for. It renders a join in the room's format with
`BaseRtcRoomHandle::prepare_join` before joining. `matrix-rtc-call`'s `RtcClient` is this, with
the call's room state; `matrix_rtc_livekit::LiveKitCall::join` and the FFI and wasm `RtcClient`
objects are built on it.

The per-room core type is `BaseRtcRoom`. What the feeder calls on it —
`set_current_sticky_state(Vec<RawStickyEvent>)` with the room's **complete** current membership,
`on_slots_received`, `on_members_received`, `on_encryption_received`, `receive_encryption_key` —
is public so the core can be driven by hand in a unit test over `testing::MockBackend`, which
records every send and lets a test deliver sets into the sinks. Production code never calls them.

For a call, use `RtcRoom::join_call`, which adds reactions, the raised hand and the MSC4075 ring.
