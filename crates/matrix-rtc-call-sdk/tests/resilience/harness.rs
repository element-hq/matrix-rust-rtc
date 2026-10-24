// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! A call the way a host makes one — `RtcClient` → `RtcRoom` → `join_call` →
//! `attach_media` — over the core's `MockBackend` (the homeserver) and a
//! `ScriptedTransport` (the SFUs), so a scenario can take either away.

use std::sync::Arc;
use std::time::Duration;

use matrix_rtc_call::{CallJoinOptions, RtcCall, RtcClient, RtcRoom};
use matrix_rtc_call_sdk::{AttachOptions, CallEvent, MediaAttachment, attach_media};
use matrix_rtc_core::compat::MembershipFormat;
use matrix_rtc_core::testing::MockBackend;
use matrix_rtc_core::{
    BackendError, EventEncryption, EventIn, Outbox, RoomOptions, SLOT_EVENT_TYPE, SignallingHealth,
};
use matrix_rtc_transport::livekit::identity_mapper;
use matrix_rtc_transport::testing::{ScriptedConnection, ScriptedTransport};
use matrix_rtc_transport::{MediaKeyHandler, PublishOptions, VideoSourceConfig};
use serde_json::{Value, json};
use tokio::sync::broadcast;

pub const ROOM: &str = "!room:example.org";
pub const SLOT: &str = "m.call#room";
/// Our own SFU.
pub const SFU_A: &str = "https://sfu-a.example.org";
/// Another member's SFU.
pub const SFU_B: &str = "https://sfu-b.example.org";

/// Another member of the call.
#[derive(Clone, Copy)]
pub struct Peer {
    pub user: &'static str,
    pub device: &'static str,
    pub member_id: &'static str,
    pub focus: &'static str,
}

pub const BOB: Peer = Peer {
    user: "@bob:example.org",
    device: "BOBDEV",
    member_id: "bob-1",
    focus: SFU_A,
};
pub const CAROL: Peer = Peer {
    user: "@carol:example.org",
    device: "CAROLDEV",
    member_id: "carol-1",
    focus: SFU_A,
};
pub const DAVE: Peer = Peer {
    user: "@dave:example.org",
    device: "DAVEDEV",
    member_id: "dave-1",
    focus: SFU_A,
};

/// One entry of a sticky set as the homeserver serves it.
pub enum Entry {
    Joined(Peer),
    /// A leave carrying `reason` (`delayed_leave` for one the homeserver
    /// fired on a member's behalf).
    Left(Peer, &'static str),
}

pub struct Call {
    pub mock: Arc<MockBackend>,
    pub client: RtcClient<MockBackend>,
    /// Kept open for the call's lifetime.
    pub _room: RtcRoom<MockBackend>,
    pub call: RtcCall<MockBackend>,
    pub media: MediaAttachment<ScriptedConnection>,
    pub transport: ScriptedTransport,
    pub events: broadcast::Receiver<CallEvent>,
}

/// Joins a call with `peers` present, our media published on [`SFU_A`]
/// unless `own_sfu_down`, and our own entry delivered back to us, as a
/// homeserver echoes it.
pub async fn start(peers: &[Peer], own_sfu_down: bool, sfu_fallback: bool) -> Call {
    let mock = Arc::new(MockBackend::new());
    *mock.transports.lock().unwrap() =
        Ok(json!([{ "type": "livekit", "livekit_service_url": SFU_A }]));
    // Every deadline of the join is read from this clock: paused time moves it.
    let origin = tokio::time::Instant::now();
    let client = RtcClient::with_outbox(
        mock.clone(),
        Outbox::with_clock(Arc::new(move || {
            1_700_000_000_000 + origin.elapsed().as_millis() as u64
        })),
    );
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
            sender: BOB.user.to_owned(),
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
    let mut users = vec![mock.user_id.clone()];
    users.extend([BOB, CAROL, DAVE].iter().map(|peer| peer.user.to_owned()));
    sink.on_joined_members(users);
    sink.on_sticky_events(
        peers
            .iter()
            .map(|peer| peer_event(&Entry::Joined(*peer), 2))
            .collect(),
    );
    room.seeded().await;

    let call = room.join_call(CallJoinOptions::new()).await.expect("join");
    let transport = ScriptedTransport::new();
    if own_sfu_down {
        transport.close(SFU_A);
    }
    let media = attach_media(
        &call,
        Arc::new(transport.clone()),
        Arc::new(MediaKeyHandler::new()),
        AttachOptions {
            own_user_id: mock.user_id.clone(),
            own_device_id: mock.device_id.clone(),
            identity_mapper: identity_mapper(MembershipFormat::Current),
            format: MembershipFormat::Current,
            stability: Default::default(),
            own_reconnect_budget: matrix_rtc_transport::pool::OWN_RECONNECT_BUDGET,
            sfu_fallback,
        },
    )
    .await
    .expect("attaches");
    let events = media.engine.subscribe_events();
    let mut call = Call {
        mock,
        client,
        _room: room,
        call,
        media,
        transport,
        events,
    };
    let entries: Vec<Entry> = peers.iter().map(|peer| Entry::Joined(*peer)).collect();
    call.deliver(&entries, true).await;
    settle().await;
    call
}

impl Call {
    /// Delivers `entries` as the room's complete sticky set, with our own
    /// current membership when `with_us`.
    pub async fn deliver(&mut self, entries: &[Entry], with_us: bool) {
        let mut events: Vec<EventIn> = entries.iter().map(|entry| peer_event(entry, 3)).collect();
        if with_us {
            events.push(self.own_event().await);
        }
        self.mock
            .room_subscription(ROOM)
            .expect("subscribed")
            .sink
            .on_sticky_events(events);
        settle().await;
    }

    /// Our membership as the homeserver holds it: the last content we sent,
    /// under the event id we were answered.
    async fn own_event(&self) -> EventIn {
        let (_, _, content, _) = self.mock.last_sticky_event().expect("we published");
        EventIn {
            event_id: self
                .call
                .membership_event_id()
                .await
                .expect("we have a membership"),
            sender: self.mock.user_id.clone(),
            event_type: "m.rtc.member".to_owned(),
            state_key: None,
            origin_server_ts: 3,
            content,
            encryption: EventEncryption::encrypted(Some(self.mock.device_id.clone()), Some(true)),
        }
    }

    /// Publishes a microphone and a camera.
    pub async fn publish(&self) {
        self.media
            .engine
            .publish(PublishOptions::microphone())
            .await
            .expect("microphone");
        self.media
            .engine
            .publish(PublishOptions::camera(VideoSourceConfig {
                width: 640,
                height: 480,
            }))
            .await
            .expect("camera");
    }

    /// Every send fails transiently, as a homeserver that is away.
    pub fn homeserver_away(&self) {
        self.mock
            .set_outage(Some(matrix_rtc_core::testing::MockOutcome::Fail(
                BackendError::new("connection refused"),
            )));
    }

    pub fn homeserver_back(&self) {
        self.mock.set_outage(None);
    }

    pub fn health(&self) -> SignallingHealth {
        self.client.signalling_health()
    }

    /// The engine's events since the last call.
    pub fn drain_events(&mut self) -> Vec<CallEvent> {
        let mut events = Vec::new();
        while let Ok(event) = self.events.try_recv() {
            events.push(event);
        }
        events
    }

    /// Whether every local publication is muted at the transport.
    pub fn publications_muted(&self) -> bool {
        let tracks = self.transport.local_tracks();
        !tracks.is_empty() && tracks.iter().all(|track| track.is_muted())
    }

    /// Our sticky sends so far.
    pub fn sticky_sends(&self) -> Vec<Value> {
        self.mock
            .sticky_events
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, content, _)| content.clone())
            .collect()
    }
}

fn peer_event(entry: &Entry, ts: u64) -> EventIn {
    let (peer, content) = match entry {
        Entry::Joined(peer) => (
            peer,
            json!({
                "slot_id": SLOT,
                "msc4354_sticky_key": peer.member_id,
                "member": { "id": peer.member_id, "membership": "join" },
                "application": { "type": "m.call" },
                "transports": {
                    "published": [{ "type": "livekit", "livekit_service_url": peer.focus }],
                    "can_subscribe": ["livekit"],
                },
            }),
        ),
        Entry::Left(peer, reason) => (
            peer,
            json!({
                "slot_id": SLOT,
                "msc4354_sticky_key": peer.member_id,
                "member": { "id": peer.member_id, "membership": "leave" },
                "leave_reason": { "code": reason },
            }),
        ),
    };
    EventIn {
        event_id: format!("${}-{ts}", peer.member_id),
        sender: peer.user.to_owned(),
        event_type: "m.rtc.member".to_owned(),
        state_key: None,
        origin_server_ts: ts,
        content,
        encryption: EventEncryption::encrypted(Some(peer.device.to_owned()), Some(true)),
    }
}

/// Lets spawned tasks run.
pub async fn settle() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

/// Advances call time by `secs`, a second at a time.
pub async fn run_for(secs: u64) {
    for _ in 0..secs {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
