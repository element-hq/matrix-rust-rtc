// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use matrix_rtc_core::testing::{MockBackend, MockMethod, MockOutcome};
use matrix_rtc_core::{
    DiscardedKey, EncryptionKeySignalHandler, EventEncryption, EventIn, KEY_MESSAGE_TYPE,
    KeyMaterialSignal, LeaveCode, LeaveSessionParams, SLOT_EVENT_TYPE, ToDeviceMessageIn,
};
use serde_json::json;

use super::*;
use crate::{CallEnd, RingPhase};

const ROOM: &str = "!room:example.org";
const OTHER_ROOM: &str = "!other:example.org";
const SLOT: &str = "m.call#room";
const BOB: &str = "@bob:example.org";

fn offline() -> MockOutcome {
    MockOutcome::Fail(BackendError::new("offline"))
}

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

    let call = room.join_call(CallJoinOptions::new()).await.expect("join");
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
        .join_call(CallJoinOptions::new())
        .await
        .expect("rejoin");
    assert!(again.is_live());
    assert_ne!(
        again.member_id(),
        call.member_id(),
        "MSC4143: a new id per join"
    );
}

/// The slot closing under a call ends it without the host asking: the core
/// leaves, the session is over, and why is still readable afterwards.
#[tokio::test]
async fn a_closed_slot_ends_the_session_and_says_why() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let call = room.join_call(CallJoinOptions::new()).await.expect("join");
    assert_eq!(*call.subscribe_ended().borrow(), None);

    let sink = mock
        .room_subscription(ROOM)
        .expect("subscribed")
        .sink
        .clone();
    sink.on_state_events(
        SLOT_EVENT_TYPE.to_owned(),
        vec![EventIn {
            event_id: "$closed".to_owned(),
            sender: BOB.to_owned(),
            event_type: SLOT_EVENT_TYPE.to_owned(),
            state_key: Some(SLOT.to_owned()),
            origin_server_ts: 3,
            content: json!({ "status": "closed" }),
            encryption: EventEncryption::Cleartext,
        }],
    );
    settle().await;

    assert!(!call.is_live());
    let reason = call.subscribe_ended().borrow().clone();
    assert_eq!(
        reason.map(|reason| reason.code),
        Some(LeaveCode::SlotClosed)
    );
    assert!(matches!(
        call.leave(LeaveSessionParams::new()).await,
        Err(RtcError::SessionOver)
    ));
}

/// A leave that fails to send still ends the session, so media stops
/// publishing; the delayed leave removes the membership.
#[tokio::test]
async fn a_closed_slot_ends_the_session_even_when_the_leave_fails() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let call = room.join_call(CallJoinOptions::new()).await.expect("join");
    let ended = call.subscribe_ended();
    *mock.sticky_event_error.lock().unwrap() = Some("offline".to_owned());

    let sink = mock
        .room_subscription(ROOM)
        .expect("subscribed")
        .sink
        .clone();
    sink.on_state_events(
        SLOT_EVENT_TYPE.to_owned(),
        vec![EventIn {
            event_id: "$closed".to_owned(),
            sender: BOB.to_owned(),
            event_type: SLOT_EVENT_TYPE.to_owned(),
            state_key: Some(SLOT.to_owned()),
            origin_server_ts: 3,
            content: json!({ "status": "closed" }),
            encryption: EventEncryption::Cleartext,
        }],
    );
    settle().await;

    assert!(!call.is_live());
    assert_eq!(
        ended.borrow().clone().map(|reason| reason.code),
        Some(LeaveCode::SlotClosed)
    );
}

/// R8: a leave that fails to send still ends the session, so media stops
/// publishing; `leave()` never reports the send, and fails only when the call
/// was already over.
#[tokio::test]
async fn leave_fails_only_when_the_call_is_already_over() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let call = room.join_call(CallJoinOptions::new()).await.expect("join");
    let ended = call.subscribe_ended();
    *mock.sticky_event_error.lock().unwrap() = Some("offline".to_owned());

    assert!(call.leave(LeaveSessionParams::new()).await.is_ok());

    assert!(!call.is_live());
    assert_eq!(
        ended.borrow().clone().map(|reason| reason.code),
        Some(LeaveCode::Leave)
    );
    assert!(matches!(
        call.leave(LeaveSessionParams::new()).await,
        Err(RtcError::SessionOver)
    ));
}

/// R8: hanging up returns at once, whatever the send does, and the leave is
/// attempted exactly once.
#[tokio::test(start_paused = true)]
async fn leave_returns_at_once_and_sends_once() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let call = room.join_call(CallJoinOptions::new()).await.expect("join");
    mock.script(MockMethod::StickyEvent, [MockOutcome::Hang]);
    let sent_before = mock.calls_to(MockMethod::StickyEvent).len();
    let started = tokio::time::Instant::now();

    call.leave(LeaveSessionParams::new()).await.expect("leave");

    assert_eq!(
        started.elapsed(),
        Duration::ZERO,
        "nothing awaited the send"
    );
    tokio::time::sleep(Duration::from_secs(120)).await;
    assert_eq!(
        mock.calls_to(MockMethod::StickyEvent).len(),
        sent_before + 1,
        "one attempt, never retried"
    );
}

/// R8: shutting the room down right after a leave neither waits for the
/// leave's send nor sends a second one.
#[tokio::test(start_paused = true)]
async fn closing_the_room_after_a_leave_neither_waits_nor_recalls_it() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let call = room.join_call(CallJoinOptions::new()).await.expect("join");
    mock.script(MockMethod::StickyEvent, [MockOutcome::Hang]);
    let sent_before = mock.calls_to(MockMethod::StickyEvent).len();
    let started = tokio::time::Instant::now();

    call.leave(LeaveSessionParams::new()).await.expect("leave");
    room.close().await;

    assert_eq!(started.elapsed(), Duration::ZERO);
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(
        mock.calls_to(MockMethod::StickyEvent).len(),
        sent_before + 1,
        "the one leave, not a second from the close"
    );
}

/// R6: the join retries the transports read and the publish inside its
/// budget.
#[tokio::test(start_paused = true)]
async fn a_join_retries_transient_failures_inside_its_budget() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    mock.script(MockMethod::RtcTransports, [offline()]);
    mock.script(MockMethod::StickyEvent, [offline()]);

    room.join_call(CallJoinOptions::new()).await.expect("join");

    assert_eq!(mock.calls_to(MockMethod::RtcTransports).len(), 2);
    assert_eq!(mock.calls_to(MockMethod::StickyEvent).len(), 2);
}

/// R6, R27: a refused publish fails the join at once, naming the code; one
/// that never answers fails it at the budget.
#[tokio::test(start_paused = true)]
async fn join_failures_say_whether_they_timed_out_or_were_refused() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;

    mock.script(
        MockMethod::StickyEvent,
        [MockOutcome::Fail(BackendError::with_matrix_error(
            Some("M_FORBIDDEN".to_owned()),
            Some(403),
            "no",
        ))],
    );
    let started = tokio::time::Instant::now();
    assert!(matches!(
        room.join_call(CallJoinOptions::new()).await,
        Err(RtcError::Join(JoinError::Refused { errcode: Some(ref code), .. })) if code == "M_FORBIDDEN"
    ));
    assert_eq!(started.elapsed(), Duration::ZERO);

    mock.script(MockMethod::StickyEvent, [MockOutcome::Hang]);
    let started = tokio::time::Instant::now();
    assert!(matches!(
        room.join_call(CallJoinOptions::new()).await,
        Err(RtcError::Join(JoinError::TimedOut))
    ));
    assert_eq!(started.elapsed(), JOIN_BUDGET);
}

/// R6: neither the first key rollout nor the notification fails a join, and
/// the notification, retried, never holds it past its budget.
#[tokio::test(start_paused = true)]
async fn a_failed_key_rollout_or_notification_never_fails_the_join() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    // Bob leaves, so the join starts the session and notifies the room.
    let sink = mock
        .room_subscription(ROOM)
        .expect("subscribed")
        .sink
        .clone();
    sink.on_sticky_events(Vec::new());
    tokio::time::sleep(Duration::from_millis(1)).await;
    mock.script(MockMethod::ToDevice, std::iter::repeat_n(offline(), 100));
    mock.script(
        MockMethod::StickyEvent,
        std::iter::once(MockOutcome::Ok).chain(std::iter::repeat_n(offline(), 100)),
    );
    let started = tokio::time::Instant::now();

    let call = room
        .join_call(CallJoinOptions::new().kind(CallKind::Group {
            notify: Some(crate::GroupNotify {
                intent: None,
                lifetime_ms: None,
                mentions: Default::default(),
            }),
        }))
        .await;

    assert!(call.is_ok(), "{:?}", call.err());
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the join waits for neither: it is over once the membership has landed"
    );
    tokio::time::sleep(JOIN_BUDGET).await;
    assert!(
        mock.calls_to(MockMethod::StickyEvent).len() > 2,
        "the notification was retried after the join returned"
    );
}

#[tokio::test]
async fn joining_a_slot_held_by_a_live_session_is_refused() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;

    let call = room.join_call(CallJoinOptions::new()).await.expect("join");
    assert!(matches!(
        room.join_call(CallJoinOptions::new()).await,
        Err(RtcError::Join(JoinError::AlreadyJoined(_)))
    ));

    // A dropped session no longer holds the slot: the next join leaves the
    // orphaned participation first instead of waiting out its delayed leave.
    let cancelled_before = mock.cancelled_events.lock().unwrap().len();
    drop(call);
    let again = room.join_call(CallJoinOptions::new()).await.expect("join");
    assert!(again.is_live());
    assert!(mock.cancelled_events.lock().unwrap().len() > cancelled_before);
}

#[tokio::test]
async fn a_slot_id_of_another_application_is_refused() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    assert!(matches!(
        room.join(JoinOptions {
            slot_id: SLOT.to_owned(),
            ..JoinOptions::application("org.example.whiteboard")
        })
        .await,
        Err(RtcError::Command(_))
    ));
}

#[tokio::test]
async fn closing_a_room_leaves_and_ends_its_sessions() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let call = room.join_call(CallJoinOptions::new()).await.expect("join");
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
    let call = room.join_call(CallJoinOptions::new()).await.expect("join");
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
    let call = room.join_call(CallJoinOptions::new()).await.expect("join");
    let other_call = other.join_call(CallJoinOptions::new()).await.expect("join");
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
    let call = room.join_call(CallJoinOptions::new()).await.expect("join");
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
    let _call = room.join_call(CallJoinOptions::new()).await.expect("join");
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
    let mut options = CallJoinOptions::new();
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
    let call = room.join_call(CallJoinOptions::new()).await.expect("join");
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
    let call = room.join_call(CallJoinOptions::new()).await.expect("join");
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
    let mut options = CallJoinOptions::new();
    options.join.encryption_config = Some(EncryptionConfig {
        delay_before_use_ms: 0,
        key_rotation_grace_period_ms: 50,
        max_key_lifetime_ms: 100,
        require_confirmed_sender: false,
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
    let mut options = CallJoinOptions::new();
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

#[test]
fn a_call_join_names_only_its_slot() {
    assert_eq!(CallJoinOptions::new().join.slot_id, "m.call#room");
    assert_eq!(
        CallJoinOptions::new().slot("standup").join.slot_id,
        "m.call#standup"
    );
    assert_eq!(
        JoinOptions::application("org.example.board")
            .slot("planning")
            .slot_id,
        "org.example.board#planning"
    );
}

#[tokio::test]
async fn a_call_knows_the_transport_its_join_resolved() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;

    let call = room.join_call(CallJoinOptions::new()).await.expect("join");
    assert!(matches!(
        call.transport(),
        TransportIntent::Publish(matrix_rtc_core::RtcTransport::LiveKit(livekit))
            if livekit.livekit_service_url == "https://sfu.example.org"
    ));
    call.leave(LeaveSessionParams::new()).await.expect("leave");

    let call = room
        .join_call(CallJoinOptions::new().transport(JoinTransport::ReceiveOnly))
        .await
        .expect("rejoin");
    assert!(matches!(
        call.transport(),
        TransportIntent::ReceiveOnly { .. }
    ));
}

#[tokio::test]
async fn a_host_leave_publishes_why_the_session_ended() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let call = room.join_call(CallJoinOptions::new()).await.expect("join");
    let ended = call.subscribe_ended();

    call.leave(LeaveSessionParams::new()).await.expect("leave");

    assert_eq!(
        ended.borrow().clone().map(|reason| reason.code),
        Some(LeaveCode::Leave)
    );
}

#[tokio::test]
async fn closing_the_room_publishes_why_its_sessions_ended() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let call = room.join_call(CallJoinOptions::new()).await.expect("join");
    let ended = call.subscribe_ended();

    room.close().await;

    assert!(!call.is_live());
    assert_eq!(
        ended.borrow().clone().map(|reason| reason.code),
        Some(LeaveCode::Leave)
    );
}

// ---- DM calls ----

/// The call's status once it satisfies `done`, or a panic after two seconds.
async fn status_once(
    call: &RtcCall<MockBackend>,
    done: impl Fn(&CallStatus) -> bool,
) -> CallStatus {
    let mut states = call.subscribe_status();
    tokio::time::timeout(Duration::from_secs(2), states.wait_for(|state| done(state)))
        .await
        .expect("the state arrives in time")
        .expect("the call is alive")
        .clone()
}

/// The lifecycle ends the call itself: nobody has to leave for it.
#[tokio::test]
async fn a_dm_call_leaves_by_itself_when_the_other_person_leaves() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let call = room
        .join_call(CallJoinOptions::new().kind(CallKind::Direct { ring: None }))
        .await
        .expect("the join succeeds");
    // Bob is here, and our media is not: nobody to wait for.
    assert_eq!(call.status(), CallStatus::Connecting);
    call.media_status_reporter().connected();
    assert!(matches!(call.status(), CallStatus::Connected { .. }));

    let sink = mock.room_subscription(ROOM).unwrap().sink.clone();
    sink.on_sticky_events(Vec::new());

    let status = status_once(&call, |status| matches!(status, CallStatus::Ended { .. })).await;
    assert_eq!(
        status,
        CallStatus::Ended {
            reason: CallEnd::OthersLeft
        }
    );
    let mut ended = call.subscribe_ended();
    let reason = tokio::time::timeout(Duration::from_secs(2), ended.wait_for(Option::is_some))
        .await
        .expect("the call ends in time")
        .unwrap()
        .clone()
        .unwrap();
    assert_eq!(reason.code, LeaveCode::Leave);
    assert_eq!(reason.reason.as_deref(), Some("others_left"));
    assert!(!call.is_live());
}

/// matrix-js-sdk sends a decline as a sticky event, which the feeder hands the
/// call before narrowing the set to memberships.
#[tokio::test]
async fn a_sticky_decline_reaches_the_call_through_the_feeder() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let sink = mock.room_subscription(ROOM).unwrap().sink.clone();
    // Bob is in the room, not in the call: there is somebody to ring.
    sink.on_sticky_events(Vec::new());
    settle().await;

    let call = room
        .join_call(CallJoinOptions::new().kind(CallKind::Direct {
            ring: Some(crate::RingConfig::default()),
        }))
        .await
        .expect("the join succeeds");
    status_once(&call, |status| {
        matches!(
            status,
            CallStatus::Calling {
                callee,
                ring: RingPhase::Ringing { .. },
                ..
            } if callee == BOB
        )
    })
    .await;
    let ring = mock
        .sticky_events
        .lock()
        .unwrap()
        .iter()
        .position(|(_, event_type, _, _)| event_type.ends_with("rtc.notification"))
        .map(|index| format!("$sticky-{}", index + 1))
        .expect("the ring went out");

    sink.on_sticky_events(vec![EventIn {
        event_id: "$decline".to_owned(),
        sender: BOB.to_owned(),
        event_type: "org.matrix.msc4310.rtc.decline".to_owned(),
        state_key: None,
        origin_server_ts: 3,
        content: json!({
            "m.relates_to": { "rel_type": "m.reference", "event_id": ring },
            "msc4354_sticky_key": ring,
        }),
        encryption: EventEncryption::Cleartext,
    }]);

    let status = status_once(&call, |status| matches!(status, CallStatus::Ended { .. })).await;
    assert_eq!(
        status,
        CallStatus::Ended {
            reason: CallEnd::Declined
        }
    );
}

/// A group call waits for nobody: it connects with our media, and ends when
/// we leave.
#[tokio::test]
async fn a_group_call_is_connected_by_its_media_and_ended_by_the_leave() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let call = room.join_call(CallJoinOptions::new()).await.expect("join");
    assert_eq!(call.status(), CallStatus::Connecting);

    call.media_status_reporter().connected();
    assert!(matches!(call.status(), CallStatus::Connected { .. }));

    call.leave(LeaveSessionParams::new()).await.expect("leave");
    assert_eq!(
        call.status(),
        CallStatus::Ended {
            reason: CallEnd::Left
        }
    );
}

/// 034 R4, R5: reactions and our hand through failing sends, on paused time.
mod reactions_and_hands {
    use super::*;

    fn forbidden() -> MockOutcome {
        MockOutcome::Fail(BackendError::with_matrix_error(
            Some("M_FORBIDDEN".to_owned()),
            Some(403),
            "not allowed",
        ))
    }

    async fn joined() -> (Arc<MockBackend>, RtcRoom<MockBackend>, RtcCall<MockBackend>) {
        let mock = mock();
        let client = RtcClient::new(mock.clone());
        let room = open(&client, &mock, ROOM).await;
        let call = room.join_call(CallJoinOptions::new()).await.expect("join");
        (mock, room, call)
    }

    /// Room-event attempts from `from` on, as offsets from it.
    fn room_event_attempts(mock: &MockBackend, from: Duration) -> Vec<Duration> {
        mock.calls_to(MockMethod::RoomEvent)
            .into_iter()
            .filter(|at| *at >= from)
            .map(|at| at - from)
            .collect()
    }

    fn since(mock: &MockBackend) -> Duration {
        mock.created_at.elapsed()
    }

    async fn our_hand(call: &RtcCall<MockBackend>, mock: &MockBackend) -> Option<RaisedHand> {
        call.raised_hands()
            .await
            .into_iter()
            .find(|hand| hand.sender == mock.user_id)
    }

    #[tokio::test(start_paused = true)]
    async fn a_reaction_is_retried_for_five_seconds_then_dropped() {
        let (mock, _room, call) = joined().await;
        mock.script(MockMethod::RoomEvent, std::iter::repeat_n(offline(), 10));
        let from = since(&mock);

        let sent = call.send_reaction("👏", "clapping").await;

        assert!(matches!(
            sent,
            Err(RtcError::Reaction(ReactionError::NotSent))
        ));
        assert_eq!(since(&mock) - from, Duration::from_secs(5));
        assert_eq!(
            room_event_attempts(&mock, from),
            [0, 1, 3].map(Duration::from_secs),
            "nothing after"
        );
        // No cooldown was recorded: the next one goes out at once.
        mock.scripted.lock().unwrap().clear();
        call.send_reaction("👏", "clapping")
            .await
            .expect("no cooldown");
    }

    #[tokio::test(start_paused = true)]
    async fn a_reaction_whose_attempt_timed_out_is_never_retried() {
        let (mock, _room, call) = joined().await;
        mock.script(MockMethod::RoomEvent, [MockOutcome::Hang]);
        let from = since(&mock);

        let sent = call.send_reaction("👏", "clapping").await;

        assert!(matches!(
            sent,
            Err(RtcError::Reaction(ReactionError::NotSent))
        ));
        assert_eq!(since(&mock) - from, Duration::from_secs(5));
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(room_event_attempts(&mock, from).len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_raised_hand_shows_locally_from_the_first_attempt_and_the_call_suspends_until_it_lands()
     {
        let (mock, _room, call) = joined().await;
        mock.script(MockMethod::RoomEvent, [offline(), offline()]);
        let from = since(&mock);

        let (raised, ()) = tokio::join!(call.raise_hand(), async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            assert!(our_hand(&call, &mock).await.is_some(), "shown at once");
        });

        raised.expect("raised");
        assert_eq!(
            since(&mock) - from,
            Duration::from_secs(3),
            "the third send"
        );
        let hand = our_hand(&call, &mock).await.expect("up");
        let annotations = mock.room_events.lock().unwrap().len();
        assert_eq!(hand.reaction_event_id, format!("$room-{annotations}"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_suspended_raise_outlives_an_outage() {
        let (mock, _room, call) = joined().await;
        mock.script(MockMethod::RoomEvent, std::iter::repeat_n(offline(), 1_000));

        let (raised, ()) = tokio::join!(call.raise_hand(), async {
            for _ in 0..9 {
                tokio::time::sleep(Duration::from_secs(60)).await;
                assert!(our_hand(&call, &mock).await.is_some(), "up throughout");
            }
            mock.scripted.lock().unwrap().clear();
        });

        raised.expect("raised at the first success");
        assert!(
            !our_hand(&call, &mock)
                .await
                .unwrap()
                .reaction_event_id
                .is_empty()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_raise_and_a_lower_that_both_failed_publish_nothing() {
        let (mock, _room, call) = joined().await;
        mock.script(MockMethod::RoomEvent, std::iter::repeat_n(offline(), 1_000));
        mock.script(MockMethod::Redaction, std::iter::repeat_n(offline(), 1_000));
        let landed = mock.room_events.lock().unwrap().len();

        let (raised, lowered) = tokio::join!(call.raise_hand(), async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let started = tokio::time::Instant::now();
            let lowered = call.lower_hand().await;
            assert_eq!(started.elapsed(), Duration::ZERO, "nothing to send");
            lowered
        });

        raised.expect("the reversal answers the raise quietly");
        lowered.expect("lowered");
        assert!(our_hand(&call, &mock).await.is_none());
        tokio::time::sleep(Duration::from_secs(120)).await;
        assert_eq!(mock.room_events.lock().unwrap().len(), landed);
        assert!(mock.redactions.lock().unwrap().is_empty());
        assert!(mock.calls_to(MockMethod::Redaction).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_permanent_refusal_fails_the_call_and_reverts_the_hand() {
        let (mock, _room, call) = joined().await;
        mock.script(MockMethod::RoomEvent, [forbidden()]);
        let from = since(&mock);

        let raised = call.raise_hand().await;

        assert!(matches!(
            raised,
            Err(RtcError::Reaction(ReactionError::Command(_)))
        ));
        assert!(our_hand(&call, &mock).await.is_none(), "back down");
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(room_event_attempts(&mock, from).len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_lower_waits_for_a_pending_raise_then_redacts() {
        let (mock, _room, call) = joined().await;
        mock.script(
            MockMethod::RoomEvent,
            [MockOutcome::After(
                Duration::from_secs(2),
                Box::new(MockOutcome::Ok),
            )],
        );
        let from = since(&mock);

        let (raised, lowered) = tokio::join!(call.raise_hand(), async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            call.lower_hand().await
        });

        raised.expect("raise");
        lowered.expect("lower");
        let annotation = format!("$room-{}", mock.room_events.lock().unwrap().len());
        let redactions = mock.redactions.lock().unwrap().clone();
        assert_eq!(redactions.len(), 1);
        assert_eq!(redactions[0].1, annotation, "the raise's own event");
        let redacted_at = mock.calls_to(MockMethod::Redaction)[0] - from;
        assert!(redacted_at >= Duration::from_secs(2));
        assert!(our_hand(&call, &mock).await.is_none());
    }

    /// The end of the call answers a suspended hand call: a leave, or the
    /// slot closing.
    #[tokio::test(start_paused = true)]
    async fn a_suspended_hand_call_resolves_ok_when_the_call_ends() {
        for close_slot in [false, true] {
            let (mock, _room, call) = joined().await;
            mock.script(MockMethod::RoomEvent, std::iter::repeat_n(offline(), 1_000));
            let from = since(&mock);

            let (raised, ()) = tokio::join!(call.raise_hand(), async {
                tokio::time::sleep(Duration::from_secs(5)).await;
                if close_slot {
                    mock.room_subscription(ROOM)
                        .expect("subscribed")
                        .sink
                        .on_state_events(
                            SLOT_EVENT_TYPE.to_owned(),
                            vec![EventIn {
                                event_id: "$closed".to_owned(),
                                sender: BOB.to_owned(),
                                event_type: SLOT_EVENT_TYPE.to_owned(),
                                state_key: Some(SLOT.to_owned()),
                                origin_server_ts: 3,
                                content: json!({ "status": "closed" }),
                                encryption: EventEncryption::Cleartext,
                            }],
                        );
                } else {
                    call.leave(LeaveSessionParams::new()).await.expect("leave");
                }
            });

            raised.expect("answered by the end");
            assert_eq!(since(&mock) - from, Duration::from_secs(5));
            let attempts = room_event_attempts(&mock, from).len();
            tokio::time::sleep(Duration::from_secs(120)).await;
            assert_eq!(
                room_event_attempts(&mock, from).len(),
                attempts,
                "no annotation after the end"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_a_suspended_hand_call_keeps_the_desired_state() {
        let (mock, _room, call) = joined().await;
        mock.script(MockMethod::RoomEvent, std::iter::repeat_n(offline(), 3));
        let from = since(&mock);

        let dropped = tokio::time::timeout(Duration::from_secs(1), call.raise_hand()).await;
        assert!(dropped.is_err(), "still suspended at 1 s");
        assert!(our_hand(&call, &mock).await.is_some());

        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(
            room_event_attempts(&mock, from),
            [0, 1, 3, 7].map(Duration::from_secs)
        );
        assert!(
            !our_hand(&call, &mock)
                .await
                .unwrap()
                .reaction_event_id
                .is_empty()
        );
    }
}
