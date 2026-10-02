// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The core's feeder reading a room through [`ElementCallCompat`]: the
//! subjects each mode asks for, the pre-sticky funnel, legacy media keys and
//! the call layer's timeline events.

use std::sync::Arc;
use std::time::Duration;

use matrix_rtc_core::feeder::{RoomAttachment, RoomFeeder, parse_key_message};
use matrix_rtc_core::testing::MockBackend;
use matrix_rtc_core::{EventEncryption, EventIn, RoomSink, SLOT_EVENT_TYPE, ToDeviceMessageIn};
use serde_json::json;
use tokio::sync::Mutex;

use super::{DialectBackend, ElementCallCompat, LEGACY_KEY_EVENT_TYPE, STATE_MEMBER_EVENT_TYPE};
use crate::room_state::CallRoomState;

const ROOM: &str = "!room:example.org";
const SLOT: &str = "m.call#ROOM";
const ME: &str = "@mock:example.org";
const BOB: &str = "@bob:example.org";

type Backend = DialectBackend<MockBackend>;
type Manager = Arc<Mutex<CallRoomState<Backend>>>;

struct Harness {
    mock: Arc<MockBackend>,
    manager: Manager,
    attachment: RoomAttachment,
}

impl Harness {
    async fn attach(mode: ElementCallCompat) -> Self {
        let mock = Arc::new(MockBackend::new());
        let backend = Arc::new(DialectBackend::new(mock.clone()));
        let manager = Arc::new(Mutex::new(CallRoomState::with_backend(
            ROOM,
            backend.clone(),
        )));
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
    let harness = Harness::attach(ElementCallCompat::Off).await;
    let subjects = &harness.mock.room_subscription(ROOM).unwrap().subjects;
    assert_eq!(
        subjects.state_event_types,
        [SLOT_EVENT_TYPE, "org.matrix.msc4143.rtc.slot"]
    );
    assert_eq!(
        subjects.timeline_event_types,
        vec!["io.element.call.reaction", "m.reaction"]
    );

    let legacy = Harness::attach(ElementCallCompat::StateEvents).await;
    let subjects = &legacy.mock.room_subscription(ROOM).unwrap().subjects;
    assert_eq!(subjects.state_event_types, vec![STATE_MEMBER_EVENT_TYPE]);
}

#[tokio::test]
async fn pre_sticky_state_membership_is_funnelled() {
    let harness = Harness::attach(ElementCallCompat::StateEvents).await;
    let sink = harness.sink();
    sink.on_encryption(false);
    sink.on_joined_members(vec![ME.to_owned(), BOB.to_owned()]);
    let now = crate::compat::element_call_state::now_ms();
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
async fn a_raised_hand_reaches_the_call_layer_and_a_redaction_lowers_it() {
    let harness = Harness::attach(ElementCallCompat::Off).await;
    harness.seed_room_state();
    harness
        .sink()
        .on_sticky_events(vec![member_event(BOB, "bob-1", "$m1")]);
    harness.attachment.seeded().await;

    // Hands raised before we joined come from the relations of the
    // membership event: one lookup per new event id.
    harness
        .wait_until(|| async { !harness.mock.relations_requests.lock().unwrap().is_empty() })
        .await;
    let requests = harness.mock.relations_requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].1, "$m1");
    assert_eq!(requests[0].2, "m.annotation");
    assert_eq!(requests[0].3, "m.reaction");

    harness.sink().on_timeline_events(vec![EventIn {
        event_id: "$hand".to_owned(),
        sender: BOB.to_owned(),
        event_type: "m.reaction".to_owned(),
        state_key: None,
        origin_server_ts: 3,
        content: json!({
            "m.relates_to": { "rel_type": "m.annotation", "event_id": "$m1", "key": "🖐️" }
        }),
        encryption: EventEncryption::Cleartext,
    }]);
    harness
        .wait_until(|| async {
            harness
                .manager
                .lock()
                .await
                .raised_hands(SLOT)
                .is_some_and(|hands| hands.len() == 1)
        })
        .await;

    harness.sink().on_redaction("$hand".to_owned());
    harness
        .wait_until(|| async {
            harness
                .manager
                .lock()
                .await
                .raised_hands(SLOT)
                .is_some_and(|hands| hands.is_empty())
        })
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
    let key_in = |mode: ElementCallCompat| {
        parse_key_message(|_| Some(Arc::new(mode)), message.clone()).expect("a key")
    };
    assert_eq!(key_in(ElementCallCompat::Off).member_id, "ec-session-uuid");
    assert_eq!(
        key_in(ElementCallCompat::StateEvents).member_id,
        format!("{BOB}:BOBDEVICE")
    );
}
