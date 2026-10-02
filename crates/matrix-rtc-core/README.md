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

The core does no I/O. It sends through the host-implemented `MatrixBackend`, and it is fed —
by the feeder in `matrix-rtc-call`, which subscribes through that same backend — the room's
state and membership as `RawStickyEvent`s, slot and room state, and decrypted key messages. It
spawns no tasks and arms no timers: the host calls `heartbeat` periodically while joined.

## Quick start: join a slot and follow its memberships

The core is **fed**, not polled: it spawns nothing and never reads from the backend itself. A host
reaches it through `matrix-rtc-call`'s `RtcClient`, which opens a room: it subscribes through the
backend to what the room needs and applies the room's current state in the right order
(encryption and slots and members before the first membership). Joining a slot on the room
returns our participation in it:

```rust,ignore
use std::sync::Arc;

use matrix_rtc_call::{JoinOptions, RoomOptions, RtcClient};
use matrix_rtc_core::{LeaveSessionParams, MatrixBackend};

const ROOM: &str = "!room:example.org";
const SLOT: &str = "org.example.board#ROOM";

async fn run(
    // The host's Matrix client, behind the one trait the library knows.
    backend: Arc<impl MatrixBackend + 'static>,
) -> Result<(), Box<dyn std::error::Error>> {
    // One per backend; creating it does no I/O.
    let client = RtcClient::new(backend);

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

    // The transport is the homeserver's first advertised LiveKit one.
    let session = room.join(JoinOptions::new(SLOT, "org.example.board")).await?;

    // Periodically, while joined.
    session.heartbeat().await;

    session.leave(LeaveSessionParams::new()).await?;
    room.close().await;
    Ok(())
}
```

`matrix_rtc_livekit::LiveKitCall::join` and the FFI and wasm `RtcClient` objects are all this
sequence with a different backend and a different place to run the feed futures. Dropping the room
without `close` ends its subscriptions without leaving: a membership left behind expires through
its delayed leave.

The per-room core type is `BaseRtcRoom`. What the feeder calls on it —
`set_current_sticky_state(Vec<RawStickyEvent>)` with the room's **complete** current membership,
`on_slots_received`, `on_members_received`, `on_encryption_received`, `receive_encryption_key` —
is public so the core can be driven by hand in a unit test over `testing::MockBackend`, which
records every send and lets a test deliver sets into the sinks. Production code never calls them.

For a call, use `RtcRoom::join_call`, which adds reactions, the raised hand and the MSC4075 ring.
