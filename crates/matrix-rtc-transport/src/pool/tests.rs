// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The pool against a fake transport, driven the way an owner drives it: a
//! snapshot or a call in, the inbox drained, the returned events checked.
//! The call SDK's engine tests cover the same behaviour through the roster.

use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU32, Ordering};

use matrix_rtc_core::{EventOrigin, LiveKitTransport, RtcTransport};
use tokio::sync::mpsc::UnboundedSender;

use super::*;
use crate::{OwnMemberClaims, ResolvedConstraints};

const OWN_FOCUS: &str = "https://sfu.example.org";
const PEER_FOCUS: &str = "https://sfu-b.example.org";

#[derive(Default)]
struct TransportState {
    connects: StdMutex<Vec<String>>,
    closes: StdMutex<Vec<String>>,
    senders: StdMutex<HashMap<String, UnboundedSender<ConnectionEvent>>>,
    /// Connect attempts that fail before one succeeds.
    fail_attempts: AtomicU32,
    applied: StdMutex<Vec<(String, MediaStreamKind, ResolvedConstraints)>>,
}

struct FakeTransport(Arc<TransportState>);

#[async_trait::async_trait]
impl MediaTransport for FakeTransport {
    fn transport_type(&self) -> &'static str {
        "fake"
    }

    fn connection_key(&self, transport: &RtcTransport) -> Option<String> {
        match transport {
            RtcTransport::LiveKit(livekit) => Some(livekit.livekit_service_url.clone()),
            RtcTransport::Unsupported(_) => None,
        }
    }

    fn remote_identity(&self, member: &JoinedMembership) -> Option<String> {
        Some(format!("id-{}", member.member_id))
    }

    async fn connect(&self, connection_key: &str, _ctx: &ConnectionContext) -> ConnectOutcome {
        self.0
            .connects
            .lock()
            .unwrap()
            .push(connection_key.to_owned());
        if self.0.fail_attempts.load(Ordering::SeqCst) > 0 {
            self.0.fail_attempts.fetch_sub(1, Ordering::SeqCst);
            return Err(TransportError::Connect("fake failure".into()));
        }
        Ok(fake_connection(&self.0, connection_key))
    }
}

struct FakeConnection {
    key: String,
    state: Arc<TransportState>,
}

#[async_trait::async_trait]
impl TransportConnection for FakeConnection {
    fn connection_key(&self) -> &str {
        &self.key
    }

    async fn publish(
        &self,
        _options: crate::PublishOptions,
    ) -> Result<Arc<dyn crate::LocalTrackHandle>, TransportError> {
        Err(TransportError::Unsupported("not in these tests".into()))
    }

    async fn apply_constraints(
        &self,
        identity: &str,
        kind: MediaStreamKind,
        resolved: ResolvedConstraints,
    ) -> Result<(), TransportError> {
        self.state
            .applied
            .lock()
            .unwrap()
            .push((identity.to_owned(), kind, resolved));
        Ok(())
    }

    async fn close(&self) -> Result<(), TransportError> {
        self.state.closes.lock().unwrap().push(self.key.clone());
        Ok(())
    }
}

fn fake_connection(
    state: &Arc<TransportState>,
    key: &str,
) -> (
    Box<dyn TransportConnection>,
    mpsc::UnboundedReceiver<ConnectionEvent>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    state.senders.lock().unwrap().insert(key.to_owned(), tx);
    (
        Box::new(FakeConnection {
            key: key.to_owned(),
            state: state.clone(),
        }),
        rx,
    )
}

struct FakeTrack(MediaStreamKind);

#[async_trait::async_trait]
impl RemoteTrackHandle for FakeTrack {
    fn kind(&self) -> MediaStreamKind {
        self.0
    }
}

fn member_on(member_id: &str, focus: &str) -> JoinedMembership {
    JoinedMembership {
        room_id: "!room:example.org".to_owned(),
        slot_id: "m.call#room".to_owned(),
        sender: format!("@{member_id}:example.org"),
        origin: EventOrigin::encrypted(Some("DEVICE".to_owned())),
        sticky_key: member_id.to_owned(),
        member_id: member_id.to_owned(),
        membership_event_id: None,
        membership_ts: None,
        origin_server_ts: None,
        application: "m.call".into(),
        transports: vec![RtcTransport::LiveKit(LiveKitTransport {
            livekit_service_url: focus.to_owned(),
        })],
        can_subscribe: vec!["livekit".to_owned()],
    }
}

struct Fixture {
    pool: MediaPool,
    inbox: mpsc::UnboundedReceiver<PoolMessage>,
    state: Arc<TransportState>,
}

impl Fixture {
    fn new() -> Self {
        let state = Arc::new(TransportState::default());
        let (pool, inbox) = MediaPool::new(PoolConfig {
            transports: vec![Arc::new(FakeTransport(state.clone()))],
            ctx: ConnectionContext {
                room_id: "!room:example.org".to_owned(),
                slot_id: "m.call#room".to_owned(),
                member: OwnMemberClaims {
                    member_id: "own".to_owned(),
                    user_id: "@own:example.org".to_owned(),
                    device_id: "DEVICE".to_owned(),
                },
            },
            own_connection_key: Some(OWN_FOCUS.to_owned()),
        });
        Self { pool, inbox, state }
    }

    /// Adopt an own-focus connection; returns its event sender.
    fn adopt(&mut self) -> UnboundedSender<ConnectionEvent> {
        let (connection, events) = fake_connection(&self.state, OWN_FOCUS);
        self.pool.adopt_own_connection(connection, events);
        self.sender(OWN_FOCUS)
    }

    fn sender(&self, key: &str) -> UnboundedSender<ConnectionEvent> {
        self.state.senders.lock().unwrap()[key].clone()
    }

    /// Let spawned connects, forwarders and timers run, and feed everything
    /// they sent back into the pool, until nothing more arrives.
    async fn drain(&mut self) -> Vec<PoolEvent> {
        let mut events = Vec::new();
        loop {
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            let mut any = false;
            while let Ok(message) = self.inbox.try_recv() {
                any = true;
                events.extend(self.pool.handle(message));
            }
            if !any {
                return events;
            }
        }
    }

    fn connects(&self, key: &str) -> usize {
        let connects = self.state.connects.lock().unwrap();
        connects.iter().filter(|k| *k == key).count()
    }
}

fn track_added(identity: &str, kind: MediaStreamKind) -> ConnectionEvent {
    ConnectionEvent::TrackAdded {
        identity: identity.to_owned(),
        kind,
        track: Arc::new(FakeTrack(kind)),
    }
}

#[tokio::test]
async fn media_and_keys_that_beat_the_membership_are_released_when_it_lands() {
    let mut fx = Fixture::new();
    let own = fx.adopt();

    own.send(track_added("id-bob", MediaStreamKind::Microphone))
        .unwrap();
    assert!(fx.drain().await.is_empty());
    assert!(fx.pool.key_imported("id-bob".into(), 0).is_empty());
    assert!(fx.pool.key_imported("id-bob".into(), 1).is_empty());

    let events = fx.pool.apply_snapshot(&[member_on("bob", OWN_FOCUS)]);
    assert_eq!(
        events,
        vec![
            PoolEvent::StreamAdded {
                member_id: "bob".into(),
                kind: MediaStreamKind::Microphone,
            },
            PoolEvent::KeyImported {
                member_id: "bob".into(),
                identity: "id-bob".into(),
                key_index: 0,
            },
            PoolEvent::KeyImported {
                member_id: "bob".into(),
                identity: "id-bob".into(),
                key_index: 1,
            },
        ]
    );
    assert!(
        fx.pool
            .remote_tracks()
            .get("bob", MediaStreamKind::Microphone)
            .is_some()
    );
}

#[tokio::test]
async fn a_departure_forgets_its_buffers_and_tracks() {
    let mut fx = Fixture::new();
    let own = fx.adopt();
    fx.pool.apply_snapshot(&[member_on("bob", OWN_FOCUS)]);
    own.send(track_added("id-bob", MediaStreamKind::Camera))
        .unwrap();
    fx.drain().await;

    // Gone, and a key that arrives for the identity afterwards is parked —
    // then dropped with nothing to release it into.
    fx.pool.apply_snapshot(&[]);
    assert!(
        fx.pool
            .remote_tracks()
            .get("bob", MediaStreamKind::Camera)
            .is_none()
    );
    assert!(fx.pool.key_imported("id-bob".into(), 3).is_empty());
}

#[tokio::test]
async fn transport_events_are_reported_per_member() {
    let mut fx = Fixture::new();
    let own = fx.adopt();
    fx.pool.apply_snapshot(&[member_on("bob", OWN_FOCUS)]);

    own.send(ConnectionEvent::RemoteJoined {
        identity: "id-stranger".into(),
    })
    .unwrap();
    own.send(ConnectionEvent::TrackMuted {
        identity: "id-bob".into(),
        kind: MediaStreamKind::Microphone,
    })
    .unwrap();
    own.send(ConnectionEvent::ActiveSpeakers {
        speakers: vec![
            crate::SpeakingParticipant {
                identity: "id-bob".into(),
                level: 0.5,
            },
            crate::SpeakingParticipant {
                identity: "id-stranger".into(),
                level: 0.9,
            },
        ],
    })
    .unwrap();
    own.send(ConnectionEvent::EncryptionStateChanged {
        identity: "id-bob".into(),
        state: FrameEncryptionState::MissingKey,
    })
    .unwrap();

    assert_eq!(
        fx.drain().await,
        vec![
            PoolEvent::UnknownParticipant {
                identity: "id-stranger".into(),
            },
            PoolEvent::StreamMuted {
                member_id: "bob".into(),
                kind: MediaStreamKind::Microphone,
                muted: true,
            },
            PoolEvent::ActiveSpeakers {
                speakers: vec![ActiveSpeaker {
                    member_id: "bob".into(),
                    level: 0.5,
                }],
            },
            PoolEvent::EncryptionStateChanged {
                member_id: "bob".into(),
                state: FrameEncryptionState::MissingKey,
            },
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_peer_focus_is_connected_and_reconnected_when_it_drops() {
    let mut fx = Fixture::new();
    fx.adopt();
    fx.pool
        .apply_snapshot(&[member_on("own", OWN_FOCUS), member_on("carol", PEER_FOCUS)]);
    fx.drain().await;
    // The own focus is adopted, never connected by the pool.
    assert_eq!(fx.connects(OWN_FOCUS), 0);
    assert_eq!(fx.connects(PEER_FOCUS), 1);

    let peer = fx.sender(PEER_FOCUS);
    peer.send(track_added("id-carol", MediaStreamKind::Camera))
        .unwrap();
    fx.drain().await;
    drop(peer);
    fx.state.senders.lock().unwrap().remove(PEER_FOCUS);

    // The stream ending is the connection dropping.
    assert_eq!(
        fx.drain().await,
        vec![
            PoolEvent::StreamRemoved {
                member_id: "carol".into(),
                kind: MediaStreamKind::Camera,
            },
            PoolEvent::Degraded(true),
        ]
    );
    tokio::time::sleep(BACKOFF_BASE * 2).await;
    assert_eq!(fx.drain().await, vec![PoolEvent::Degraded(false)]);
    assert_eq!(fx.connects(PEER_FOCUS), 2);
}

#[tokio::test(start_paused = true)]
async fn a_failed_connect_backs_off_and_retries() {
    let mut fx = Fixture::new();
    fx.state.fail_attempts.store(2, Ordering::SeqCst);
    fx.pool.apply_snapshot(&[member_on("carol", PEER_FOCUS)]);
    assert_eq!(fx.drain().await, vec![PoolEvent::Degraded(true)]);

    tokio::time::sleep(backoff_delay(1)).await;
    fx.drain().await;
    assert_eq!(fx.connects(PEER_FOCUS), 2);

    tokio::time::sleep(backoff_delay(2)).await;
    assert_eq!(fx.drain().await, vec![PoolEvent::Degraded(false)]);
    assert_eq!(fx.connects(PEER_FOCUS), 3);
}

#[tokio::test(start_paused = true)]
async fn an_idle_peer_focus_closes_after_the_grace_unless_a_member_returns() {
    let mut fx = Fixture::new();
    fx.pool.apply_snapshot(&[member_on("carol", PEER_FOCUS)]);
    fx.drain().await;

    // A flap: gone and back inside the grace keeps the connection.
    fx.pool.apply_snapshot(&[]);
    tokio::time::sleep(IDLE_GRACE / 2).await;
    fx.pool.apply_snapshot(&[member_on("carol", PEER_FOCUS)]);
    tokio::time::sleep(IDLE_GRACE).await;
    fx.drain().await;
    assert!(fx.state.closes.lock().unwrap().is_empty());

    fx.pool.apply_snapshot(&[]);
    tokio::time::sleep(IDLE_GRACE * 2).await;
    fx.drain().await;
    assert_eq!(*fx.state.closes.lock().unwrap(), vec![PEER_FOCUS]);
    assert_eq!(fx.connects(PEER_FOCUS), 1);
}

#[tokio::test]
async fn losing_the_own_focus_is_reported_and_close_spares_it() {
    let mut fx = Fixture::new();
    let own = fx.adopt();
    assert!(fx.pool.own_connection().is_some());

    own.send(ConnectionEvent::Closed {
        message: "kicked".into(),
    })
    .unwrap();
    assert_eq!(
        fx.drain().await,
        vec![PoolEvent::OwnConnectionLost {
            message: "kicked".into(),
        }]
    );
    assert!(fx.pool.own_connection().is_none());

    fx.pool.close();
    fx.drain().await;
    assert!(fx.state.closes.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn constraints_are_debounced_and_follow_the_stream() {
    let mut fx = Fixture::new();
    let own = fx.adopt();
    fx.pool.apply_snapshot(&[member_on("bob", OWN_FOCUS)]);

    let hidden = MediaConstraints {
        visible: false,
        ..MediaConstraints::default()
    };
    fx.pool.set_constraints(
        "bob".into(),
        MediaStreamKind::Camera,
        MediaConstraints::default(),
    );
    fx.pool
        .set_constraints("bob".into(), MediaStreamKind::Camera, hidden);
    tokio::time::sleep(CONSTRAINTS_DEBOUNCE * 2).await;
    fx.drain().await;
    let expected = (
        "id-bob".to_owned(),
        MediaStreamKind::Camera,
        hidden.resolve(MediaStreamKind::Camera),
    );
    // Only the latest of the burst reaches the connection.
    assert_eq!(*fx.state.applied.lock().unwrap(), vec![expected.clone()]);

    // A fresh subscription starts from server defaults: pushed again, at once.
    own.send(track_added("id-bob", MediaStreamKind::Camera))
        .unwrap();
    fx.drain().await;
    assert_eq!(
        *fx.state.applied.lock().unwrap(),
        vec![expected.clone(), expected]
    );
}
