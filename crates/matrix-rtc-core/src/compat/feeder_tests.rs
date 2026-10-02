// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The core's feeder reading a room through each [`MembershipFormat`]: the
//! subjects each asks for, the pre-sticky funnel and legacy media keys.

use std::sync::Arc;
use std::time::Duration;

use crate::feeder::{RoomAttachment, RoomFeeder, parse_key_message};
use crate::testing::MockBackend;
use crate::{EventEncryption, EventIn, RoomSink, SLOT_EVENT_TYPE, ToDeviceMessageIn};
use serde_json::json;
use tokio::sync::Mutex;

use super::{DialectBackend, LEGACY_KEY_EVENT_TYPE, MembershipFormat, STATE_MEMBER_EVENT_TYPE};
use crate::BaseRtcRoom;

const ROOM: &str = "!room:example.org";
const SLOT: &str = "m.call#ROOM";
const ME: &str = "@mock:example.org";
const BOB: &str = "@bob:example.org";

type Backend = DialectBackend<MockBackend>;
type Manager = Arc<Mutex<BaseRtcRoom<Backend>>>;

struct Harness {
    mock: Arc<MockBackend>,
    manager: Manager,
    attachment: RoomAttachment,
}

impl Harness {
    async fn attach(mode: MembershipFormat) -> Self {
        let mock = Arc::new(MockBackend::new());
        let backend = Arc::new(DialectBackend::new(mock.clone()));
        let manager = Arc::new(Mutex::new(BaseRtcRoom::with_backend(ROOM, backend.clone())));
        let (attachment, run) = RoomFeeder::attach(backend, manager.clone(), Arc::new(mode))
            .await
            .expect("attach");
        tokio::spawn(run.run());
        Self {
            mock,
            manager,
            attachment,
        }
    }

    fn sink(&self) -> Arc<dyn RoomSink> {
        self.mock
            .room_subscription(ROOM)
            .expect("a live subscription")
            .sink
            .clone()
    }

    /// Feeds the three gating subjects.
    fn seed_room_state(&self) {
        let sink = self.sink();
        sink.on_encryption(false);
        sink.on_state_events(SLOT_EVENT_TYPE.to_owned(), vec![open_slot()]);
        sink.on_joined_members(vec![ME.to_owned(), BOB.to_owned()]);
    }

    async fn member_count(&self) -> Option<usize> {
        self.manager.lock().await.member_count(SLOT)
    }

    async fn wait_until<F, Fut>(&self, mut condition: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !condition().await {
            assert!(
                tokio::time::Instant::now() < deadline,
                "condition not met in time"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

fn open_slot() -> EventIn {
    EventIn {
        event_id: "$slot".to_owned(),
        sender: ME.to_owned(),
        event_type: SLOT_EVENT_TYPE.to_owned(),
        state_key: Some(SLOT.to_owned()),
        origin_server_ts: 1,
        content: json!({ "status": "open", "application": { "type": "m.call" } }),
        encryption: EventEncryption::Cleartext,
    }
}

fn member_event(sender: &str, member_id: &str, event_id: &str) -> EventIn {
    EventIn {
        event_id: event_id.to_owned(),
        sender: sender.to_owned(),
        event_type: "m.rtc.member".to_owned(),
        state_key: None,
        origin_server_ts: 2,
        content: json!({
            "slot_id": SLOT,
            "msc4354_sticky_key": member_id,
            "member": { "id": member_id, "membership": "join" },
            "application": { "type": "m.call" },
        }),
        encryption: EventEncryption::Cleartext,
    }
}

#[tokio::test]
async fn attach_subscribes_to_what_the_mode_needs() {
    let harness = Harness::attach(MembershipFormat::Current).await;
    let subjects = &harness.mock.room_subscription(ROOM).unwrap().subjects;
    assert_eq!(
        subjects.state_event_types,
        [SLOT_EVENT_TYPE, "org.matrix.msc4143.rtc.slot"]
    );
    assert!(subjects.timeline_event_types.is_empty());

    let legacy = Harness::attach(MembershipFormat::RoomState).await;
    let subjects = &legacy.mock.room_subscription(ROOM).unwrap().subjects;
    assert_eq!(subjects.state_event_types, vec![STATE_MEMBER_EVENT_TYPE]);
}

#[tokio::test]
async fn pre_sticky_state_membership_is_funnelled() {
    let harness = Harness::attach(MembershipFormat::RoomState).await;
    let sink = harness.sink();
    sink.on_encryption(false);
    sink.on_joined_members(vec![ME.to_owned(), BOB.to_owned()]);
    let now = crate::compat::room_state::now_ms();
    sink.on_state_events(
        STATE_MEMBER_EVENT_TYPE.to_owned(),
        vec![EventIn {
            event_id: "$legacy".to_owned(),
            sender: BOB.to_owned(),
            event_type: STATE_MEMBER_EVENT_TYPE.to_owned(),
            state_key: Some(BOB.to_owned()),
            origin_server_ts: now,
            content: json!({
                "application": "m.call",
                "call_id": "",
                "scope": "m.room",
                "device_id": "BOBDEVICE",
                "membershipID": format!("{BOB}:BOBDEVICE"),
                "expires": 3_600_000,
                "created_ts": now,
                "focus_active": { "type": "livekit", "focus_selection": "multi_sfu" },
                "foci_preferred": [{
                    "type": "livekit",
                    "livekit_alias": ROOM,
                    "livekit_service_url": "https://sfu.example.org/livekit/jwt"
                }]
            }),
            encryption: EventEncryption::Cleartext,
        }],
    );
    harness.attachment.seeded().await;
    assert_eq!(harness.member_count().await, Some(1));
}

#[tokio::test]
async fn a_2025_sticky_member_is_read_in_the_current_format() {
    let harness = Harness::attach(MembershipFormat::Current).await;
    harness.seed_room_state();
    harness.sink().on_sticky_events(vec![EventIn {
        content: json!({
            "slot_id": SLOT,
            "msc4354_sticky_key": "bob-1",
            "application": { "type": "m.call" },
            "member": { "id": "bob-1", "user_id": BOB, "device_id": "BOBDEVICE" },
            "rtc_transports": [{ "type": "livekit", "livekit_service_url": "https://sfu" }],
        }),
        ..member_event(BOB, "bob-1", "$m1")
    }]);
    harness.attachment.seeded().await;
    harness
        .wait_until(|| async { harness.member_count().await == Some(1) })
        .await;
}

#[test]
fn a_legacy_key_is_bound_in_the_mode_its_room_was_opened_in() {
    let message = ToDeviceMessageIn {
        sender: BOB.to_owned(),
        event_type: LEGACY_KEY_EVENT_TYPE.to_owned(),
        content: json!({
            "room_id": ROOM,
            "member": { "id": "ec-session-uuid", "claimed_device_id": "BOBDEVICE" },
            "keys": { "index": 0, "key": "a2V5" },
            "session": { "call_id": "", "application": "m.call", "scope": "m.room" },
        }),
        encryption: EventEncryption::encrypted(Some("BOBDEVICE".to_owned()), None),
    };
    let key_in = |mode: MembershipFormat| {
        parse_key_message(|_| Some(Arc::new(mode)), message.clone()).expect("a key")
    };
    assert_eq!(
        key_in(MembershipFormat::Current).member_id,
        "ec-session-uuid"
    );
    assert_eq!(
        key_in(MembershipFormat::RoomState).member_id,
        format!("{BOB}:BOBDEVICE")
    );
}
