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

The core does no I/O. The host feeds it events (`RawStickyEvent`, slot and room state, decrypted
key messages), and it sends through the host-implemented `MatrixBackend`. It spawns no tasks and
arms no timers: the host calls `heartbeat` periodically while joined.

## Quick start: join a slot and follow its memberships

```rust,no_run
use std::sync::Arc;

use matrix_rtc_core::{
    JoinSessionParams, JoinedMembership, LeaveSessionParams, LiveKitTransport, MatrixBackend,
    RawStickyEvent, RtcSessionManager, RtcTransport,
};

const ROOM: &str = "!room:example.org";
const SLOT: &str = "org.example.board#ROOM";

async fn run(
    // The host's Matrix client, behind the one trait the library knows.
    backend: Arc<impl MatrixBackend + 'static>,
    // The room's current membership, already translated. In practice the
    // feeder in `matrix-rtc-bridge` subscribes through the backend and
    // produces these; a host does not build them by hand.
    sticky_events: Vec<RawStickyEvent>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut manager = RtcSessionManager::with_backend(backend);

    // Told of every change to a session's joined memberships.
    manager.add_membership_listener(Arc::new(
        |room_id: &str, slot_id: &str, members: &[JoinedMembership]| {
            println!("[{room_id}/{slot_id}] {} joined", members.len());
        },
    ));

    // Hand over the complete current state, again each time it changes.
    manager.set_current_sticky_state(ROOM, sticky_events).await?;

    manager
        .join(JoinSessionParams::new(
            "@alice:example.org".to_owned(),
            "ALICEDEVICE".to_owned(),
            ROOM.to_owned(),
            SLOT.to_owned(),
            "org.example.board",
            RtcTransport::LiveKit(LiveKitTransport {
                livekit_service_url: "https://sfu.example.org".to_owned(),
            }),
        ))
        .await?;

    // Periodically, while joined.
    manager.heartbeat(ROOM, SLOT).await;

    manager
        .leave(ROOM.to_owned(), SLOT.to_owned(), LeaveSessionParams::new())
        .await?;
    Ok(())
}
```

The same memberships are also on a watch: `subscribe_membership_snapshots(room_id, slot_id)`.

The core spawns nothing and never reads from the backend itself: the read half
of `MatrixBackend` (room and to-device subscriptions, `/relations`, the OpenID
token, `GET /rtc/transports`) is driven by `matrix_rtc_bridge::feeder`, which
is what the bindings and the `Call` facade use. `testing::MockBackend` records
every send and lets a test deliver sets into the sinks.

For a call, use `matrix-rtc-call`'s `CallSessionManager`, which wraps this manager.
