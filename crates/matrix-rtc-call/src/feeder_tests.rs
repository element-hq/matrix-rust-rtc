// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The core's feeder feeding the call layer: its timeline events, redactions
//! and relations.

use std::sync::Arc;
use std::time::Duration;

use matrix_rtc_core::compat::{DialectBackend, MembershipFormat};
use matrix_rtc_core::feeder::{RoomAttachment, RoomFeeder};
use matrix_rtc_core::testing::MockBackend;
use matrix_rtc_core::{EventEncryption, EventIn, RoomSink, SLOT_EVENT_TYPE};
use serde_json::json;
use tokio::sync::Mutex;

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
    async fn attach(mode: MembershipFormat) -> Self {
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
async fn a_raised_hand_reaches_the_call_layer_and_a_redaction_lowers_it() {
    let harness = Harness::attach(MembershipFormat::Current).await;
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
