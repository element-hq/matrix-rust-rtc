// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

use std::sync::atomic::Ordering;
use std::time::Duration;

use serde_json::json;

use super::*;
use crate::compat::{OutboundDialect, STATE_MEMBER_EVENT_TYPE};
use crate::testing::MockBackend;
use crate::{
    EventEncryption, EventIn, KEY_MESSAGE_TYPE, LiveKitTransport, RtcTransport, SLOT_EVENT_TYPE,
    ToDeviceMessageIn,
};

const ROOM: &str = "!room:example.org";
const OTHER_ROOM: &str = "!other:example.org";
const SLOT: &str = "m.call#room";
const BOB: &str = "@bob:example.org";

/// What a homeserver delivers for a room: an open slot with Bob in it.
fn deliver_room(mock: &MockBackend, room_id: &str) {
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
}

async fn open(
    client: &BaseRtcClient<MockBackend>,
    mock: &MockBackend,
    room_id: &str,
) -> BaseRtcRoomHandle<MockBackend> {
    let room = client
        .room(room_id, RoomOptions::default())
        .await
        .expect("the room opens");
    deliver_room(mock, room_id);
    room.seeded().await;
    room
}

fn mock() -> Arc<MockBackend> {
    Arc::new(MockBackend::new())
}

/// Who joins is the backend's account; the transport is the host's choice.
fn join_params() -> JoinSessionParams {
    JoinSessionParams::application("m.call").transport(RtcTransport::LiveKit(LiveKitTransport {
        livekit_service_url: "https://sfu.example.org".to_owned(),
    }))
}

#[tokio::test(start_paused = true)]
async fn a_room_feeds_itself_and_a_join_keeps_itself_alive() {
    let mock = mock();
    let client = BaseRtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    assert_eq!(room.observe(SLOT).await.borrow().len(), 1);

    room.join(join_params()).await.expect("join");
    assert_eq!(room.state().lock().await.joined_slots(), vec![SLOT]);
    tokio::time::sleep(
        Duration::from_millis(crate::DEFAULT_KEEP_ALIVE_INTERVAL_MS * 2) + Duration::from_millis(1),
    )
    .await;
    assert_eq!(mock.restarted_events.lock().unwrap().len(), 2);

    room.leave(SLOT, LeaveSessionParams::new())
        .await
        .expect("leave");
}

#[tokio::test]
async fn a_second_handle_for_an_open_room_is_refused() {
    let mock = mock();
    let client = BaseRtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;

    assert!(matches!(
        client.room(ROOM, RoomOptions::default()).await,
        Err(OpenError::RoomAlreadyOpen(_))
    ));

    drop(room);
    let _reopened = open(&client, &mock, ROOM).await;
}

#[tokio::test]
async fn an_opening_that_fails_leaves_nothing_behind() {
    let mock = mock();
    let client = BaseRtcClient::new(mock.clone());
    *mock.room_subscription_error.lock().unwrap() =
        Some(BackendError::not_implemented("subscribe_room"));

    assert!(matches!(
        client.room(ROOM, RoomOptions::default()).await,
        Err(OpenError::Backend(_))
    ));
    let to_device = mock.to_device_subscriptions.lock().unwrap()[0].clone();
    assert!(to_device.cancelled.load(Ordering::SeqCst));

    *mock.room_subscription_error.lock().unwrap() = None;
    let _room = open(&client, &mock, ROOM).await;
}

#[tokio::test]
async fn the_to_device_subscription_stops_with_the_last_room() {
    let mock = mock();
    let client = BaseRtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;
    let other = open(&client, &mock, OTHER_ROOM).await;
    assert_eq!(mock.to_device_subscriptions.lock().unwrap().len(), 1);
    let to_device = mock.to_device_subscription().unwrap();

    drop(room);
    assert!(!to_device.cancelled.load(Ordering::SeqCst));
    drop(other);
    assert!(to_device.cancelled.load(Ordering::SeqCst));
}

#[tokio::test]
async fn a_media_key_for_a_room_that_is_not_open_is_dropped() {
    let mock = mock();
    let client = BaseRtcClient::new(mock.clone());
    let _room = open(&client, &mock, ROOM).await;

    let key = |room_id: &str| ToDeviceMessageIn {
        sender: BOB.to_owned(),
        event_type: KEY_MESSAGE_TYPE.to_owned(),
        content: json!({
            "room_id": room_id,
            "member_id": "bob-1",
            "media_key": { "index": 0, "key": "AAECAwQFBgcICQoLDA0ODw==" },
        }),
        encryption: EventEncryption::encrypted(Some("BOBDEV".to_owned()), Some(true)),
    };
    let sink = mock.to_device_subscription().unwrap().sink.clone();
    sink.on_to_device_message(key(OTHER_ROOM));
    sink.on_to_device_message(key(ROOM));
    tokio::time::sleep(Duration::from_millis(30)).await;
    // The routing loop survived the key it could not deliver.
    assert!(
        !mock
            .to_device_subscription()
            .unwrap()
            .cancelled
            .load(Ordering::SeqCst)
    );
}

/// Opens `ROOM` in `format` and delivers what a homeserver would for it: Bob
/// in the call, as a pre-sticky state membership in `RoomState`.
async fn open_in(
    client: &BaseRtcClient<MockBackend>,
    mock: &MockBackend,
    format: MembershipFormat,
) -> BaseRtcRoomHandle<MockBackend> {
    let room = client
        .room(ROOM, RoomOptions { format })
        .await
        .expect("the room opens");
    if format != MembershipFormat::RoomState {
        deliver_room(mock, ROOM);
    } else {
        let sink = mock
            .room_subscription(ROOM)
            .expect("subscribed")
            .sink
            .clone();
        sink.on_encryption(false);
        sink.on_joined_members(vec![mock.user_id.clone(), BOB.to_owned()]);
        let now = crate::compat::room_state::now_ms();
        sink.on_state_events(
            STATE_MEMBER_EVENT_TYPE.to_owned(),
            vec![EventIn {
                event_id: "$bob".to_owned(),
                sender: BOB.to_owned(),
                event_type: STATE_MEMBER_EVENT_TYPE.to_owned(),
                state_key: Some(format!("_{BOB}_BOBDEV_m.call")),
                origin_server_ts: now,
                content: json!({
                    "application": "m.call",
                    "call_id": "",
                    "scope": "m.room",
                    "device_id": "BOBDEV",
                    "expires": 3_600_000,
                    "created_ts": now,
                    "focus_active": { "type": "livekit", "focus_selection": "multi_sfu" },
                    "foci_preferred": [{
                        "type": "livekit",
                        "livekit_alias": ROOM,
                        "livekit_service_url": "https://sfu.example.org"
                    }]
                }),
                encryption: EventEncryption::Cleartext,
            }],
        );
    }
    room.seeded().await;
    room
}

#[tokio::test]
async fn a_room_state_room_reads_its_membership_from_state_and_joins_as_state() {
    let mock = mock();
    let client = BaseRtcClient::new(mock.clone());
    let room = open_in(&client, &mock, MembershipFormat::RoomState).await;
    let subjects = mock.room_subscription(ROOM).unwrap().subjects.clone();
    assert_eq!(subjects.state_event_types, vec![STATE_MEMBER_EVENT_TYPE]);
    assert_eq!(room.observe(SLOT).await.borrow().len(), 1);

    let member_id = room.join(join_params()).await.expect("join");
    assert_eq!(
        room.state().lock().await.own_member_id(SLOT),
        Some(format!("{}:{}", mock.user_id, mock.device_id)),
        "this format keys a membership on {{user}}:{{device}}; got {member_id}"
    );
    let state = mock.state_events.lock().unwrap();
    assert!(
        state
            .iter()
            .any(|(room_id, event_type, _, _)| room_id == ROOM
                && event_type == STATE_MEMBER_EVENT_TYPE),
        "the membership went out as room state"
    );
    assert!(mock.sticky_events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_sticky_2025_join_carries_the_legacy_fields() {
    let mock = mock();
    let client = BaseRtcClient::new(mock.clone());
    let room = open_in(&client, &mock, MembershipFormat::Sticky2025).await;
    room.join(join_params()).await.expect("join");

    let sticky = mock.sticky_events.lock().unwrap();
    let (_, _, content, _) = sticky.last().expect("a membership");
    assert_eq!(content["member"]["user_id"], mock.user_id.as_str());
    assert_eq!(content["member"]["device_id"], mock.device_id.as_str());
}

#[tokio::test]
async fn dropping_the_room_forgets_its_dialect() {
    let mock = mock();
    let client = BaseRtcClient::new(mock.clone());
    let room = open_in(&client, &mock, MembershipFormat::Sticky2025).await;
    room.join(join_params()).await.expect("join");
    assert!(matches!(
        client.backend().dialect(ROOM),
        OutboundDialect::Sticky(_)
    ));

    drop(room);
    assert!(matches!(
        client.backend().dialect(ROOM),
        OutboundDialect::None
    ));
}

#[tokio::test]
async fn a_join_without_a_transport_is_refused_and_publishes_nothing() {
    let mock = mock();
    *mock.transports.lock().unwrap() =
        Ok(json!([{ "type": "livekit", "livekit_service_url": "https://advertised.example.org" }]));
    let client = BaseRtcClient::new(mock.clone());
    let room = open(&client, &mock, ROOM).await;

    let refused = room.join(JoinSessionParams::application("m.call")).await;
    assert!(matches!(
        refused,
        Err(JoinError::MissingParameter("transport is required"))
    ));
    assert!(mock.sticky_events.lock().unwrap().is_empty());
    assert_eq!(
        mock.transports_requests.load(Ordering::SeqCst),
        0,
        "choosing a transport is the application's, not the core's"
    );
}
