// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use serde_json::json;
use tokio::sync::Mutex;

use super::*;
use crate::compat::MembershipFormat;
use crate::testing::MockBackend;
use crate::{BaseRtcRoom, RawStickyEvent};

const ROOM: &str = "!room:example.org";
const SLOT: &str = "m.call#ROOM";
const ME: &str = "@mock:example.org";
const BOB: &str = "@bob:example.org";

type Manager = Arc<Mutex<BaseRtcRoom<MockBackend>>>;

struct Harness {
    mock: Arc<MockBackend>,
    manager: Manager,
    attachment: RoomAttachment,
}

impl Harness {
    async fn attach() -> Self {
        Self::attach_in(Arc::new(MembershipFormat::Current)).await
    }

    async fn attach_in(dialect: Arc<dyn IngestDialect>) -> Self {
        let mock = Arc::new(MockBackend::new());
        let manager = Arc::new(Mutex::new(BaseRtcRoom::with_backend(ROOM, mock.clone())));
        let (attachment, run) = RoomFeeder::attach(mock.clone(), manager.clone(), dialect)
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

const STATE_MEMBER: &str = "org.example.call.member";
const EXTRA_KEY: &str = "org.example.call.key";

/// Reads membership only from `STATE_MEMBER` state, one member per event, and
/// keys of the `EXTRA_KEY` type as well.
struct StateDialect;

impl IngestDialect for StateDialect {
    fn membership_state_event_types(&self) -> Vec<String> {
        vec![STATE_MEMBER.to_owned()]
    }

    fn reads_slots(&self) -> bool {
        false
    }

    fn current_membership(
        &self,
        room_id: &str,
        _sticky: Vec<EventIn>,
        state: Vec<EventIn>,
    ) -> Vec<RawStickyEvent> {
        let sticky = state
            .into_iter()
            .map(|event| EventIn {
                event_type: "m.rtc.member".to_owned(),
                ..event
            })
            .collect();
        MembershipFormat::Current.current_membership(room_id, sticky, Vec::new())
    }

    fn parse_key(&self, message: &ToDeviceMessageIn) -> Option<ReceivedEncryptionKey> {
        Some(ReceivedEncryptionKey {
            origin: key_origin(&message.encryption, &message.sender),
            room_id: message.content["room_id"].as_str()?.to_owned(),
            member_id: "from-the-dialect".to_owned(),
            key_b64: "a2V5".to_owned(),
            key_index: 0,
        })
    }
}

#[tokio::test]
async fn attach_subscribes_to_what_the_dialect_needs() {
    let harness = Harness::attach().await;
    let subjects = &harness.mock.room_subscription(ROOM).unwrap().subjects;
    assert_eq!(subjects.state_event_types, SLOT_EVENT_TYPES);
    assert!(subjects.timeline_event_types.is_empty());

    let state = Harness::attach_in(Arc::new(StateDialect)).await;
    let subjects = &state.mock.room_subscription(ROOM).unwrap().subjects;
    assert_eq!(subjects.state_event_types, vec![STATE_MEMBER]);
}

#[tokio::test]
async fn a_dialect_without_slots_seeds_from_its_membership_state() {
    let harness = Harness::attach_in(Arc::new(StateDialect)).await;
    let sink = harness.sink();
    sink.on_encryption(false);
    sink.on_joined_members(vec![ME.to_owned(), BOB.to_owned()]);
    sink.on_state_events(
        STATE_MEMBER.to_owned(),
        vec![EventIn {
            event_type: STATE_MEMBER.to_owned(),
            state_key: Some(BOB.to_owned()),
            ..member_event(BOB, "bob-1", "$m1")
        }],
    );
    harness.attachment.seeded().await;
    assert!(harness.attachment.is_seeded());
}

#[test]
fn a_key_of_an_extra_type_is_read_by_its_rooms_dialect() {
    let message = ToDeviceMessageIn {
        sender: BOB.to_owned(),
        event_type: EXTRA_KEY.to_owned(),
        content: json!({ "room_id": ROOM }),
        encryption: EventEncryption::Cleartext,
    };
    let dialect: Arc<dyn IngestDialect> = Arc::new(StateDialect);
    let key = parse_key_message(|_| Some(dialect.clone()), message.clone()).expect("a key");
    assert_eq!(key.member_id, "from-the-dialect");

    // A room that is not open has no dialect to read it.
    assert!(parse_key_message(|_| None, message).is_none());
}

#[tokio::test]
async fn a_membership_set_waits_for_the_room_state_and_then_seeds() {
    let harness = Harness::attach().await;
    let sink = harness.sink();

    sink.on_sticky_events(vec![member_event(BOB, "bob-1", "$m1")]);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!harness.attachment.is_seeded());
    assert_eq!(harness.member_count().await, None);

    harness.seed_room_state();
    harness.attachment.seeded().await;
    assert_eq!(harness.member_count().await, Some(1));
}

#[tokio::test]
async fn a_members_set_without_us_is_ignored() {
    let harness = Harness::attach().await;
    let sink = harness.sink();
    sink.on_encryption(false);
    sink.on_state_events(SLOT_EVENT_TYPE.to_owned(), vec![open_slot()]);
    sink.on_joined_members(vec![BOB.to_owned()]);
    sink.on_sticky_events(vec![member_event(BOB, "bob-1", "$m1")]);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!harness.attachment.is_seeded());

    sink.on_joined_members(vec![ME.to_owned(), BOB.to_owned()]);
    harness.attachment.seeded().await;
    assert_eq!(harness.member_count().await, Some(1));
}

#[tokio::test]
async fn an_empty_sticky_set_clears_the_session() {
    let harness = Harness::attach().await;
    harness.seed_room_state();
    harness
        .sink()
        .on_sticky_events(vec![member_event(BOB, "bob-1", "$m1")]);
    harness.attachment.seeded().await;
    harness
        .wait_until(|| async { harness.member_count().await == Some(1) })
        .await;

    harness.sink().on_sticky_events(Vec::new());
    harness
        .wait_until(|| async { harness.member_count().await == Some(0) })
        .await;
}

#[tokio::test]
async fn a_closed_slot_projects_the_member_out() {
    let harness = Harness::attach().await;
    let sink = harness.sink();
    sink.on_encryption(false);
    sink.on_state_events(SLOT_EVENT_TYPE.to_owned(), Vec::new());
    sink.on_joined_members(vec![ME.to_owned(), BOB.to_owned()]);
    sink.on_sticky_events(vec![member_event(BOB, "bob-1", "$m1")]);
    harness.attachment.seeded().await;
    assert_eq!(harness.member_count().await, Some(0));
}

const UNSTABLE_SLOT_EVENT_TYPE: &str = "org.matrix.msc4143.rtc.slot";

fn slot(event_type: &str, status: &str) -> EventIn {
    EventIn {
        event_type: event_type.to_owned(),
        content: json!({ "status": status, "application": { "type": "m.call" } }),
        ..open_slot()
    }
}

/// Seeds encryption, members and a sticky member, then the given slot sets in
/// order, and returns the projected member count once they are applied.
async fn member_count_after_slots(sets: Vec<(&str, Vec<EventIn>)>) -> Option<usize> {
    let harness = Harness::attach().await;
    let sink = harness.sink();
    sink.on_encryption(false);
    sink.on_joined_members(vec![ME.to_owned(), BOB.to_owned()]);
    sink.on_sticky_events(vec![member_event(BOB, "bob-1", "$m1")]);
    for (event_type, events) in sets {
        sink.on_state_events(event_type.to_owned(), events);
    }
    harness.attachment.seeded().await;
    // Seeded on the first slot set; let the rest apply.
    tokio::time::sleep(Duration::from_millis(20)).await;
    harness.member_count().await
}

#[tokio::test]
async fn an_empty_unstable_slot_set_does_not_close_the_stable_slot() {
    let count = member_count_after_slots(vec![
        (SLOT_EVENT_TYPE, vec![slot(SLOT_EVENT_TYPE, "open")]),
        (UNSTABLE_SLOT_EVENT_TYPE, Vec::new()),
    ])
    .await;
    assert_eq!(count, Some(1));
}

#[tokio::test]
async fn an_unstable_slot_counts_when_the_room_has_no_stable_one() {
    let count = member_count_after_slots(vec![
        (SLOT_EVENT_TYPE, Vec::new()),
        (
            UNSTABLE_SLOT_EVENT_TYPE,
            vec![slot(UNSTABLE_SLOT_EVENT_TYPE, "open")],
        ),
    ])
    .await;
    assert_eq!(count, Some(1));
}

#[tokio::test]
async fn the_stable_slot_wins_over_the_unstable_one_in_either_order() {
    for sets in [
        vec![
            (SLOT_EVENT_TYPE, vec![slot(SLOT_EVENT_TYPE, "closed")]),
            (
                UNSTABLE_SLOT_EVENT_TYPE,
                vec![slot(UNSTABLE_SLOT_EVENT_TYPE, "open")],
            ),
        ],
        vec![
            (
                UNSTABLE_SLOT_EVENT_TYPE,
                vec![slot(UNSTABLE_SLOT_EVENT_TYPE, "open")],
            ),
            (SLOT_EVENT_TYPE, vec![slot(SLOT_EVENT_TYPE, "closed")]),
        ],
    ] {
        assert_eq!(member_count_after_slots(sets).await, Some(0));
    }
}

#[tokio::test]
async fn nothing_delivered_after_the_attachment_drops_is_applied() {
    let harness = Harness::attach().await;
    harness.seed_room_state();
    let sink = harness.sink();
    sink.on_sticky_events(vec![member_event(BOB, "bob-1", "$m1")]);
    harness.attachment.seeded().await;
    harness
        .wait_until(|| async { harness.member_count().await == Some(1) })
        .await;

    let subscription = harness.mock.room_subscription(ROOM).unwrap();
    let Harness {
        manager,
        attachment,
        ..
    } = harness;
    drop(attachment);
    assert!(
        subscription
            .cancelled
            .load(std::sync::atomic::Ordering::SeqCst)
    );
    sink.on_sticky_events(Vec::new());
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(manager.lock().await.member_count(SLOT), Some(1));
}

#[tokio::test]
async fn the_to_device_feeder_subscribes_to_the_spec_and_extra_key_types() {
    let mock = Arc::new(MockBackend::new());
    let registry: RoomRegistry<BaseRtcRoom<MockBackend>> = RoomRegistry::default();
    let (feeder, run) = ToDeviceFeeder::start(mock.clone(), registry, vec![EXTRA_KEY.to_owned()])
        .await
        .expect("start");
    tokio::spawn(run.run());

    let subscription = mock.to_device_subscription().expect("a live subscription");
    assert_eq!(
        subscription.event_types,
        [KEY_EVENT_TYPES.as_slice(), &[EXTRA_KEY]].concat()
    );

    // A message the loop cannot use neither kills it nor reaches the core.
    subscription.sink.on_to_device_message(ToDeviceMessageIn {
        sender: BOB.to_owned(),
        event_type: KEY_MESSAGE_TYPE.to_owned(),
        content: json!({ "garbage": true }),
        encryption: EventEncryption::Cleartext,
    });
    tokio::time::sleep(Duration::from_millis(10)).await;

    feeder.stop();
    assert!(
        subscription
            .cancelled
            .load(std::sync::atomic::Ordering::SeqCst)
    );
}

#[test]
fn key_origin_treats_an_unreported_cross_signing_as_not_cross_signed() {
    let origin = key_origin(&EventEncryption::encrypted(Some("D".to_owned()), None), BOB);
    assert!(matches!(
        origin,
        KeyOrigin::Encrypted {
            sender_is_cross_signed: false,
            ..
        }
    ));
    assert!(matches!(
        key_origin(&EventEncryption::Cleartext, BOB),
        KeyOrigin::Cleartext
    ));
}

#[tokio::test]
async fn dropping_an_attachment_ends_its_subscription() {
    let Harness {
        mock, attachment, ..
    } = Harness::attach().await;
    let subscription = mock.room_subscription(ROOM).unwrap();
    assert!(!subscription.cancelled.load(Ordering::SeqCst));

    drop(attachment);
    assert!(subscription.cancelled.load(Ordering::SeqCst));
}

#[tokio::test]
async fn dropping_the_to_device_feeder_ends_its_subscription() {
    let mock = Arc::new(MockBackend::new());
    let registry: RoomRegistry<BaseRtcRoom<MockBackend>> = RoomRegistry::default();
    let (feeder, _run) = ToDeviceFeeder::start(mock.clone(), registry, Vec::new())
        .await
        .expect("start");
    let subscription = mock.to_device_subscription().unwrap();

    drop(feeder);
    assert!(subscription.cancelled.load(Ordering::SeqCst));
}
