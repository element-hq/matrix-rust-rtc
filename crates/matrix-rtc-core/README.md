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

The core is **fed**, not polled: it spawns nothing and never reads from the backend itself. The
feeder in `matrix-rtc-call` does that — it subscribes through the backend to what the room needs,
applies the room's current state in the right order (encryption and slots and members before the
first membership), translates the member events and keeps the manager current. So the entry point
is the feeder, over the core manager:

```rust,ignore
use std::sync::Arc;
use tokio::sync::Mutex;

use matrix_rtc_call::{AttachOptions, RoomFeeder, RoomModes, ToDeviceFeeder};
use matrix_rtc_call::compat::DialectBackend;
use matrix_rtc_core::{
    JoinSessionParams, JoinedMembership, LeaveSessionParams, LiveKitTransport, MatrixBackend,
    RtcSessionManager, RtcTransport,
};

const ROOM: &str = "!room:example.org";
const SLOT: &str = "org.example.board#ROOM";

async fn run(
    // The host's Matrix client, behind the one trait the library knows.
    backend: Arc<impl MatrixBackend + 'static>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Every send renders in the room's dialect (a no-op in the spec-current mode).
    let backend = Arc::new(DialectBackend::new(backend));
    let manager = Arc::new(Mutex::new(RtcSessionManager::with_backend(backend.clone())));

    // Told of every change to a session's joined memberships.
    manager.lock().await.add_membership_listener(Arc::new(
        |room_id: &str, slot_id: &str, members: &[JoinedMembership]| {
            println!("[{room_id}/{slot_id}] {} joined", members.len());
        },
    ));

    // Media keys for every attached room, then the room itself. Each feeder
    // returns the future that applies what arrives; run it where you like
    // (the futures are `!Send`: a `LocalSet`, or `spawn_local` on wasm).
    let modes = RoomModes::default();
    let (_keys, keys_run) = ToDeviceFeeder::start(backend.clone(), manager.clone(), modes.clone()).await?;
    tokio::task::spawn_local(keys_run.run());
    let (attachment, room_run) = RoomFeeder::attach(
        backend.clone(), manager.clone(), modes, ROOM.to_owned(), AttachOptions::default(),
    ).await?;
    tokio::task::spawn_local(room_run.run());
    // Resolves once the room's current state has been applied.
    attachment.seeded().await;

    manager.lock().await
        .join(JoinSessionParams::new(
            backend.own_user_id(),
            backend.own_device_id(),
            ROOM.to_owned(),
            SLOT.to_owned(),
            "org.example.board",
            RtcTransport::LiveKit(LiveKitTransport {
                livekit_service_url: "https://sfu.example.org".to_owned(),
            }),
        ))
        .await?;

    // Periodically, while joined.
    manager.lock().await.heartbeat(ROOM, SLOT).await;

    manager.lock().await
        .leave(ROOM.to_owned(), SLOT.to_owned(), LeaveSessionParams::new())
        .await?;
    attachment.detach();
    Ok(())
}
```

`matrix_rtc_livekit::Call::join`, the FFI handle and the wasm manager are all this sequence with
a different backend and a different place to run the feeder futures.

The same memberships are also on a watch: `subscribe_membership_snapshots(room_id, slot_id)`.

What the feeder calls on the manager — `set_current_sticky_state(room, Vec<RawStickyEvent>)`
with the room's **complete** current membership, `on_room_slots_received`, `on_room_members_received`,
`on_room_encryption_received`, `receive_encryption_key` — is public so the core can be driven by
hand in a unit test over `testing::MockBackend`, which records every send and lets a test deliver
sets into the sinks. Production code never calls them.

For a call, use `matrix-rtc-call`'s `CallSessionManager`, which wraps this manager.
