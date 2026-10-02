// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use matrix_rtc_core::testing::MockBackend;
use matrix_rtc_core::{
    DiscardedKey, EncryptionKeySignalHandler, EventEncryption, EventIn, KEY_MESSAGE_TYPE,
    KeyMaterialSignal, LeaveSessionParams, SLOT_EVENT_TYPE, ToDeviceMessageIn,
};
use serde_json::json;

use super::*;

const ROOM: &str = "!room:example.org";
const OTHER_ROOM: &str = "!other:example.org";
const SLOT: &str = "m.call#ROOM";
const BOB: &str = "@bob:example.org";

fn mock() -> Arc<MockBackend> {
    let mock = Arc::new(MockBackend::new());
    *mock.transports.lock().unwrap() =
        Ok(json!([{ "type": "livekit", "livekit_service_url": "https://sfu.example.org" }]));
    mock
}

/// Opens `room_id` with its feeds running, and feeds it an open slot with Bob
/// in it, the way a homeserver would.
async fn open(
    client: &RtcClient<MockBackend>,
    mock: &MockBackend,
    room_id: &str,
) -> RtcRoom<MockBackend> {
    let room = client
        .room(room_id, RoomOptions::default())
        .await
        .expect("the room opens");

    let sink = mock
        .room_subscription(room_id)
        .expect("subscribed")
        .sink
        .clone();
    sink.on_encryption(false);
    sink.on_state_events(
        SLOT_EVENT_TYPE.to_owned(),
        vec![EventIn {
            event_id: "$slot".to_owned(),
            sender: BOB.to_owned(),
            event_type: SLOT_EVENT_TYPE.to_owned(),
            state_key: Some(SLOT.to_owned()),
            origin_server_ts: 1,
            content: json!({ "status": "open", "application": { "type": "m.call" } }),
            encryption: EventEncryption::Cleartext,
        }],
    );
    sink.on_joined_members(vec![mock.user_id.clone(), BOB.to_owned()]);
    sink.on_sticky_events(vec![EventIn {
        event_id: "$bob".to_owned(),
        sender: BOB.to_owned(),
        event_type: "m.rtc.member".to_owned(),
        state_key: None,
        origin_server_ts: 2,
        content: json!({
            "slot_id": SLOT,
            "msc4354_sticky_key": "bob-1",
            "member": { "id": "bob-1", "membership": "join" },
            "application": { "type": "m.call" },
        }),
        encryption: EventEncryption::encrypted(Some("BOBDEV".to_owned()), Some(true)),
    }]);
    room.seeded().await;
    room
}

/// Every inbound key a session heard about, accepted or refused, by the
/// identity it was signalled under.
#[derive(Default)]
struct KeyRecorder(std::sync::Mutex<Vec<String>>);

#[async_trait::async_trait]
impl EncryptionKeySignalHandler for KeyRecorder {
    async fn on_new_key_material(&self, signal: KeyMaterialSignal) {
        self.0.lock().unwrap().push(signal.rtc_backend_identity);
    }

    async fn on_key_discarded(&self, discarded: DiscardedKey) {
        self.0.lock().unwrap().push(discarded.member_id);
    }
}

impl KeyRecorder {
    fn heard_bob(&self) -> bool {
        self.0
            .lock()
            .unwrap()
            .iter()
            .any(|identity| identity == "bob-1")
    }
}

fn bob_key(room_id: &str) -> ToDeviceMessageIn {
    ToDeviceMessageIn {
        sender: BOB.to_owned(),
        event_type: KEY_MESSAGE_TYPE.to_owned(),
        content: json!({
            "room_id": room_id,
            "member_id": "bob-1",
            "media_key": { "index": 0, "key": "AAECAwQFBgcICQoLDA0ODw==" },
        }),
        encryption: EventEncryption::encrypted(Some("BOBDEV".to_owned()), Some(true)),
    }
}

async fn settle() {
    tokio::time::sleep(Duration::from_millis(30)).await;
}

#[tokio::test]
async fn a_client_does_nothing_until_a_room_is_opened() {
    let mock = mock();
    let _client = RtcClient::new(mock.clone());
    assert!(mock.room_subscriptions.lock().unwrap().is_empty());
    assert!(mock.to_device_subscriptions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn one_to_device_subscription_serves_every_open_room() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let _room = open(&client, &mock, ROOM).await;
    let _other = open(&client, &mock, OTHER_ROOM).await;
    assert_eq!(mock.to_device_subscriptions.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn the_to_device_subscription_stops_with_the_last_room_and_restarts_with_the_next() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let other = open(&client, &mock, OTHER_ROOM).await;
    let first = mock.to_device_subscription().unwrap();

    drop(room);
    assert!(
        !first.cancelled.load(Ordering::SeqCst),
        "a room is still open"
    );
    other.close().await;
    assert!(
        first.cancelled.load(Ordering::SeqCst),
        "no room left to route to"
    );

    let _again = open(&client, &mock, ROOM).await;
    let subscriptions = mock.to_device_subscriptions.lock().unwrap();
    assert_eq!(subscriptions.len(), 2);
    assert!(!subscriptions[1].cancelled.load(Ordering::SeqCst));
}

#[tokio::test]
async fn a_second_room_object_for_an_open_room_is_refused() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;

    assert!(matches!(
        client.room(ROOM, RoomOptions::default()).await,
        Err(RtcError::RoomAlreadyOpen(_))
    ));

    drop(room);
    let _reopened = open(&client, &mock, ROOM).await;
}

#[tokio::test]
async fn a_slot_is_observed_without_joining() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    assert_eq!(room.member_count(SLOT).await, 1);
    assert_eq!(room.member_count("m.call#EMPTY").await, 0);
}

#[tokio::test]
async fn a_left_session_is_over_and_a_rejoin_yields_a_new_one() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;

    let call = room
        .join_call(CallJoinOptions::new(SLOT))
        .await
        .expect("join");
    assert!(call.is_live());
    call.leave(LeaveSessionParams::new()).await.expect("leave");

    assert!(!call.is_live());
    assert!(matches!(
        call.raise_hand().await,
        Err(RtcError::SessionOver)
    ));
    assert!(matches!(
        call.leave(LeaveSessionParams::new()).await,
        Err(RtcError::SessionOver)
    ));

    let again = room
        .join_call(CallJoinOptions::new(SLOT))
        .await
        .expect("rejoin");
    assert!(again.is_live());
    assert_ne!(
        again.member_id(),
        call.member_id(),
        "MSC4143: a new id per join"
    );
}

#[tokio::test]
async fn joining_a_slot_held_by_a_live_session_is_refused() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;

    let call = room
        .join_call(CallJoinOptions::new(SLOT))
        .await
        .expect("join");
    assert!(matches!(
        room.join_call(CallJoinOptions::new(SLOT)).await,
        Err(RtcError::Join(JoinError::AlreadyJoined(_)))
    ));

    // A dropped session no longer holds the slot: the next join leaves the
    // orphaned participation first instead of waiting out its delayed leave.
    let cancelled_before = mock.cancelled_events.lock().unwrap().len();
    drop(call);
    let again = room
        .join_call(CallJoinOptions::new(SLOT))
        .await
        .expect("join");
    assert!(again.is_live());
    assert!(mock.cancelled_events.lock().unwrap().len() > cancelled_before);
}

#[tokio::test]
async fn a_slot_id_of_another_application_is_refused() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    assert!(matches!(
        room.join(JoinOptions::new(SLOT, "org.example.whiteboard"))
            .await,
        Err(RtcError::Command(_))
    ));
}

#[tokio::test]
async fn closing_a_room_leaves_and_ends_its_sessions() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let call = room
        .join_call(CallJoinOptions::new(SLOT))
        .await
        .expect("join");
    let subscription = mock.room_subscription(ROOM).unwrap();
    let cancelled_before = mock.cancelled_events.lock().unwrap().len();

    room.close().await;

    assert!(!call.is_live());
    assert!(
        mock.cancelled_events.lock().unwrap().len() > cancelled_before,
        "the leave cancels the delayed leave"
    );
    assert!(subscription.cancelled.load(Ordering::SeqCst));
}

#[tokio::test]
async fn dropping_a_room_ends_its_subscription_without_leaving() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let call = room
        .join_call(CallJoinOptions::new(SLOT))
        .await
        .expect("join");
    let subscription = mock.room_subscription(ROOM).unwrap();
    let sticky_before = mock.sticky_events.lock().unwrap().len();
    let cancelled_before = mock.cancelled_events.lock().unwrap().len();

    drop(room);
    settle().await;

    assert!(subscription.cancelled.load(Ordering::SeqCst));
    assert_eq!(mock.sticky_events.lock().unwrap().len(), sticky_before);
    assert_eq!(
        mock.cancelled_events.lock().unwrap().len(),
        cancelled_before
    );
    drop(call);
}

#[tokio::test]
async fn a_media_key_reaches_only_the_room_it_names() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let other = open(&client, &mock, OTHER_ROOM).await;
    let call = room
        .join_call(CallJoinOptions::new(SLOT))
        .await
        .expect("join");
    let other_call = other
        .join_call(CallJoinOptions::new(SLOT))
        .await
        .expect("join");
    let here = Arc::new(KeyRecorder::default());
    let there = Arc::new(KeyRecorder::default());
    assert!(call.set_encryption_signal_handler(here.clone()).await);
    assert!(
        other_call
            .set_encryption_signal_handler(there.clone())
            .await
    );

    let to_device = mock.to_device_subscription().unwrap();
    to_device.sink.on_to_device_message(bob_key(ROOM));
    settle().await;

    assert!(here.heard_bob());
    assert!(
        !there.heard_bob(),
        "a key for one room never reaches another"
    );
}

#[tokio::test]
async fn a_media_key_for_a_room_that_is_no_longer_open_is_dropped() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let _other = open(&client, &mock, OTHER_ROOM).await;
    let call = room
        .join_call(CallJoinOptions::new(SLOT))
        .await
        .expect("join");
    let recorder = Arc::new(KeyRecorder::default());
    assert!(call.set_encryption_signal_handler(recorder.clone()).await);

    // The session still holds the room's state, but the room is no longer
    // open, so nothing routes to it.
    drop(room);
    mock.to_device_subscription()
        .unwrap()
        .sink
        .on_to_device_message(bob_key(ROOM));
    settle().await;

    assert!(!recorder.heard_bob());
}

#[tokio::test]
async fn an_opening_that_fails_leaves_nothing_behind() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    *mock.room_subscription_error.lock().unwrap() = Some(
        matrix_rtc_core::BackendError::not_implemented("subscribe_room"),
    );

    assert!(matches!(
        client.room(ROOM, RoomOptions::default()).await,
        Err(RtcError::Backend(_))
    ));
    let to_device = mock.to_device_subscriptions.lock().unwrap()[0].clone();
    assert!(
        to_device.cancelled.load(Ordering::SeqCst),
        "no room is open, so nothing routes keys"
    );

    // The room is free again.
    *mock.room_subscription_error.lock().unwrap() = None;
    let _room = open(&client, &mock, ROOM).await;
}

const DEFAULT_KEEP_ALIVE_INTERVAL: Duration =
    Duration::from_millis(matrix_rtc_core::DEFAULT_KEEP_ALIVE_INTERVAL_MS);

fn restarts(mock: &MockBackend) -> usize {
    mock.restarted_events.lock().unwrap().len()
}

#[tokio::test(start_paused = true)]
async fn a_joined_session_keeps_itself_alive() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let _call = room
        .join_call(CallJoinOptions::new(SLOT))
        .await
        .expect("join");
    assert_eq!(
        restarts(&mock),
        0,
        "the join itself armed the delayed leave"
    );

    tokio::time::sleep(DEFAULT_KEEP_ALIVE_INTERVAL * 3 + Duration::from_millis(1)).await;
    assert_eq!(
        restarts(&mock),
        3,
        "one restart per interval, nobody ticking"
    );
}

#[tokio::test(start_paused = true)]
async fn the_keep_alive_interval_is_capped_at_half_the_timeout() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let mut options = CallJoinOptions::new(SLOT);
    options.join.keep_alive_timeout_ms = Some(4_000);
    options.join.keep_alive_interval_ms = Some(60_000);
    let _call = room.join_call(options).await.expect("join");

    tokio::time::sleep(Duration::from_millis(4_001)).await;
    assert_eq!(restarts(&mock), 2);
}

#[tokio::test(start_paused = true)]
async fn leaving_stops_the_keep_alive() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let call = room
        .join_call(CallJoinOptions::new(SLOT))
        .await
        .expect("join");
    tokio::time::sleep(DEFAULT_KEEP_ALIVE_INTERVAL + Duration::from_millis(1)).await;
    assert_eq!(restarts(&mock), 1);

    call.leave(LeaveSessionParams::new()).await.expect("leave");
    tokio::time::sleep(DEFAULT_KEEP_ALIVE_INTERVAL * 3).await;
    assert_eq!(restarts(&mock), 1, "nothing restarted after the leave");
}

#[tokio::test(start_paused = true)]
async fn dropping_a_session_stops_its_keep_alive_without_leaving() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let call = room
        .join_call(CallJoinOptions::new(SLOT))
        .await
        .expect("join");
    let cancelled_before = mock.cancelled_events.lock().unwrap().len();

    drop(call);
    tokio::time::sleep(DEFAULT_KEEP_ALIVE_INTERVAL * 3).await;
    assert_eq!(restarts(&mock), 0);
    assert_eq!(
        mock.cancelled_events.lock().unwrap().len(),
        cancelled_before,
        "no leave: the delayed leave ends the membership"
    );
}

/// Real time: the rotation deadline is wall-clock, which paused tokio time
/// does not move.
#[tokio::test]
async fn an_expired_key_is_rotated_at_its_deadline_with_nobody_ticking() {
    let key_sends = |mock: &MockBackend| {
        mock.to_device_messages
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, _, event_type, _)| event_type == KEY_MESSAGE_TYPE)
            .count()
    };
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let sink = mock.room_subscription(ROOM).unwrap().sink.clone();
    sink.on_encryption(true);
    sink.on_state_events(
        SLOT_EVENT_TYPE.to_owned(),
        vec![EventIn {
            event_id: "$slot".to_owned(),
            sender: BOB.to_owned(),
            event_type: SLOT_EVENT_TYPE.to_owned(),
            state_key: Some(SLOT.to_owned()),
            origin_server_ts: 1,
            content: json!({
                "status": "open",
                "application": { "type": "m.call" },
                "encryption": { "type": "m.per_member" },
            }),
            encryption: EventEncryption::Cleartext,
        }],
    );
    settle().await;
    let mut options = CallJoinOptions::new(SLOT);
    options.join.encryption_config = Some(EncryptionConfig {
        delay_before_use_ms: 0,
        key_rotation_grace_period_ms: 50,
        max_key_lifetime_ms: 100,
        require_cross_signed_sender: false,
        ..EncryptionConfig::default()
    });
    let _call = room.join_call(options).await.expect("join");
    settle().await;
    let first = key_sends(&mock);
    assert!(first > 0, "the first key went to Bob");

    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(
        key_sends(&mock) > first,
        "the key was replaced on its own deadline, long before a keep-alive tick"
    );
}

#[tokio::test]
async fn dropping_a_room_aborts_its_feed() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let sink = mock.room_subscription(ROOM).unwrap().sink.clone();
    let state = Arc::downgrade(&room.state);

    drop(room);
    settle().await;
    sink.on_joined_members(vec![BOB.to_owned()]);
    settle().await;
    assert!(
        state.upgrade().is_none(),
        "the feed task no longer holds the room state"
    );
}

#[tokio::test(start_paused = true)]
async fn a_raised_hand_follows_our_membership_across_a_refresh_by_itself() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let mut options = CallJoinOptions::new(SLOT);
    // A zero lifetime makes every keep-alive tick refresh the membership.
    options.join.sticky_duration_ms = Some(0);
    let call = room.join_call(options).await.expect("join");
    call.raise_hand().await.expect("raise");
    let annotations = |mock: &MockBackend| mock.room_events.lock().unwrap().len();
    assert_eq!(annotations(&mock), 1);

    tokio::time::sleep(DEFAULT_KEEP_ALIVE_INTERVAL + Duration::from_millis(1)).await;
    settle().await;
    assert_eq!(
        annotations(&mock),
        2,
        "re-raised on the refreshed membership"
    );
    assert_eq!(
        mock.redactions.lock().unwrap().len(),
        1,
        "the old one redacted"
    );
}
