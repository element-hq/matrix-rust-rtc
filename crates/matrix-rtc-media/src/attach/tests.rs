// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use matrix_rtc_call::{CallJoinOptions, JoinTransport, RtcClient, RtcRoom};
use matrix_rtc_core::testing::MockBackend;
use matrix_rtc_core::{
    EventEncryption, EventIn, JoinedMembership, KEY_MESSAGE_TYPE, LeaveSessionParams, RoomOptions,
    RtcTransport, SLOT_EVENT_TYPE, ToDeviceMessageIn,
};
use serde_json::json;
use tokio::sync::mpsc;

use super::*;
use crate::keys::FrameKeyRing;
use crate::transport::{ConnectionEvent, TransportConnection};

const ROOM: &str = "!room:example.org";
const SLOT: &str = "m.call#room";
const BOB: &str = "@bob:example.org";
const FOCUS: &str = "https://sfu.example.org";

/// Not the default derivation, so a key imported under the raw `member_id`
/// fallback (the mapper installed too late) shows.
fn mapper() -> RtcIdentityMapper {
    Arc::new(|user: &str, device: &str, member: &str| format!("{user}|{device}|{member}"))
}

fn bob_identity() -> String {
    mapper()(BOB, "BOBDEV", "bob-1")
}

/// Every key the ring was handed, by identity.
#[derive(Default)]
struct RecordingRing(Mutex<Vec<String>>);

#[async_trait]
impl FrameKeyRing for RecordingRing {
    fn ring_size(&self) -> u16 {
        16
    }

    async fn set_key(&self, identity: &str, _index: u8, _key: Vec<u8>) -> bool {
        self.0.lock().unwrap().push(identity.to_owned());
        true
    }
}

#[derive(Clone, Default)]
struct FakeConnection {
    local_key_indexes: Arc<Mutex<Vec<u8>>>,
    /// Held so the engine sees the connection alive.
    _events: Option<mpsc::UnboundedSender<ConnectionEvent>>,
}

#[async_trait]
impl TransportConnection for FakeConnection {
    fn connection_key(&self) -> &str {
        FOCUS
    }

    fn set_local_key_index(&self, key_index: u8) {
        self.local_key_indexes.lock().unwrap().push(key_index);
    }

    async fn close(&self) -> Result<(), TransportError> {
        Ok(())
    }
}

#[derive(Default)]
struct FakeTransport {
    own_connects: Mutex<Vec<String>>,
    refuse: bool,
}

#[async_trait]
impl MediaTransport for FakeTransport {
    fn transport_type(&self) -> &'static str {
        "livekit"
    }

    fn connection_key(&self, transport: &RtcTransport) -> Option<String> {
        match transport {
            RtcTransport::LiveKit(livekit) => Some(livekit.livekit_service_url.clone()),
            RtcTransport::Unsupported(_) => None,
        }
    }

    fn remote_identity(&self, member: &JoinedMembership) -> Option<String> {
        let device = member.origin.sender_device_id()?;
        Some(mapper()(&member.sender, device, &member.member_id))
    }

    async fn connect(
        &self,
        connection_key: &str,
        ctx: &ConnectionContext,
    ) -> Result<
        (
            Box<dyn TransportConnection>,
            mpsc::UnboundedReceiver<ConnectionEvent>,
        ),
        TransportError,
    > {
        let (connection, events) = self.connect_own(connection_key, ctx).await?;
        Ok((Box::new(connection), events))
    }
}

#[async_trait]
impl OwnFocusTransport for FakeTransport {
    type Connection = FakeConnection;

    async fn connect_own(
        &self,
        connection_key: &str,
        _ctx: &ConnectionContext,
    ) -> Result<(FakeConnection, mpsc::UnboundedReceiver<ConnectionEvent>), TransportError> {
        self.own_connects
            .lock()
            .unwrap()
            .push(connection_key.to_owned());
        if self.refuse {
            return Err(TransportError::Connect("refused".into()));
        }
        let (tx, rx) = mpsc::unbounded_channel();
        Ok((
            FakeConnection {
                _events: Some(tx),
                ..FakeConnection::default()
            },
            rx,
        ))
    }
}

/// An encrypted room with an open per-member-encrypted slot and Bob in it, the way a homeserver would feed it.
async fn open(client: &RtcClient<MockBackend>, mock: &MockBackend) -> RtcRoom<MockBackend> {
    let room = client
        .room(ROOM, RoomOptions::default())
        .await
        .expect("the room opens");
    let sink = mock
        .room_subscription(ROOM)
        .expect("subscribed")
        .sink
        .clone();
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
            "transports": { "published": [{ "type": "livekit", "livekit_service_url": FOCUS }] },
        }),
        encryption: EventEncryption::encrypted(Some("BOBDEV".to_owned()), Some(true)),
    }]);
    room.seeded().await;
    room
}

fn bob_key() -> ToDeviceMessageIn {
    ToDeviceMessageIn {
        sender: BOB.to_owned(),
        event_type: KEY_MESSAGE_TYPE.to_owned(),
        content: json!({
            "room_id": ROOM,
            "member_id": "bob-1",
            "media_key": { "index": 0, "key": "AAECAwQFBgcICQoLDA0ODw==" },
        }),
        encryption: EventEncryption::encrypted(Some("BOBDEV".to_owned()), Some(true)),
    }
}

fn mock() -> Arc<MockBackend> {
    let mock = Arc::new(MockBackend::new());
    *mock.transports.lock().unwrap() =
        Ok(json!([{ "type": "livekit", "livekit_service_url": FOCUS }]));
    mock
}

fn options(mock: &MockBackend) -> AttachOptions {
    AttachOptions {
        own_user_id: mock.user_id.clone(),
        own_device_id: mock.device_id.clone(),
        identity_mapper: mapper(),
        stability: StabilityConfig::default(),
    }
}

async fn settle() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn a_key_held_before_attaching_is_imported_under_the_mapped_identity_and_reported() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock).await;
    let call = room.join_call(CallJoinOptions::new()).await.expect("join");

    // Bob's key arrives between the join and the attach, with nobody listening.
    mock.to_device_subscription()
        .unwrap()
        .sink
        .on_to_device_message(bob_key());
    settle().await;

    let ring = Arc::new(RecordingRing::default());
    let handler = Arc::new(MediaKeyHandler::with_ring(ring.clone()));
    let transport = Arc::new(FakeTransport::default());
    let mut attached = attach_media(&call, transport.clone(), handler, options(&mock))
        .await
        .expect("attaches");
    settle().await;

    assert!(
        ring.0.lock().unwrap().contains(&bob_identity()),
        "the replay derives through the mapper, installed first: {:?}",
        ring.0.lock().unwrap(),
    );
    let mut imported = false;
    while let Ok(event) = attached.events.try_recv() {
        imported |=
            matches!(event, CallEvent::KeyImported { member_id, .. } if member_id == "bob-1");
    }
    assert!(imported, "the replayed key reaches the event stream");

    assert_eq!(*transport.own_connects.lock().unwrap(), [FOCUS]);
    assert!(attached.own_connection.is_some());
    assert_eq!(
        attached.own_identity,
        mapper()(&mock.user_id, &mock.device_id, call.member_id())
    );
}

#[tokio::test]
async fn our_sender_adopts_the_key_index_we_are_already_on() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock).await;
    let call = room.join_call(CallJoinOptions::new()).await.expect("join");
    let handler = Arc::new(MediaKeyHandler::new());

    let attached = attach_media(
        &call,
        Arc::new(FakeTransport::default()),
        handler.clone(),
        options(&mock),
    )
    .await
    .expect("attaches");

    let connection = attached.own_connection.expect("publishes on the focus");
    // The join distributed our first key; the replay recorded it under our
    // mapped identity before the connect.
    let own = handler
        .key_for(&attached.own_identity)
        .expect("our own key was replayed");
    assert_eq!(
        *connection.local_key_indexes.lock().unwrap(),
        [own.key_index]
    );
}

#[tokio::test]
async fn a_receive_only_call_connects_no_own_focus() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock).await;
    let call = room
        .join_call(CallJoinOptions::new().transport(JoinTransport::ReceiveOnly))
        .await
        .expect("join");
    let transport = Arc::new(FakeTransport::default());

    let attached = attach_media(
        &call,
        transport.clone(),
        Arc::new(MediaKeyHandler::new()),
        options(&mock),
    )
    .await
    .expect("attaches");

    assert!(attached.own_connection.is_none());
    assert!(transport.own_connects.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_call_that_is_over_does_not_attach() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock).await;
    let call = room.join_call(CallJoinOptions::new()).await.expect("join");
    call.leave(LeaveSessionParams::new()).await.expect("leave");

    let result = attach_media(
        &call,
        Arc::new(FakeTransport::default()),
        Arc::new(MediaKeyHandler::new()),
        options(&mock),
    )
    .await;
    assert!(matches!(result, Err(AttachError::NotJoined(_))));
}

#[tokio::test]
async fn a_refusing_own_focus_fails_the_attach() {
    let mock = mock();
    let client = RtcClient::new(mock.clone());
    let room = open(&client, &mock).await;
    let call = room.join_call(CallJoinOptions::new()).await.expect("join");
    let transport = Arc::new(FakeTransport {
        refuse: true,
        ..FakeTransport::default()
    });

    let result = attach_media(
        &call,
        transport,
        Arc::new(MediaKeyHandler::new()),
        options(&mock),
    )
    .await;
    assert!(matches!(result, Err(AttachError::Transport(_))));
}
