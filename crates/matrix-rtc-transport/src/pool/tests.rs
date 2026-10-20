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
    /// Outcomes of the own connection's `reconnect` calls, in order; `Ok`
    /// once they run out.
    reconnect_outcomes: StdMutex<std::collections::VecDeque<Reconnect>>,
    /// `(when, fresh_token)` of every `reconnect` call.
    reconnects: StdMutex<Vec<(tokio::time::Instant, bool)>>,
    /// Every connection key the own handle took over.
    takeovers: StdMutex<Vec<String>>,
}

#[derive(Clone, Copy, Debug)]
enum Reconnect {
    Ok,
    Fail,
    TokenRejected,
    Refused,
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

    async fn reconnect(
        &self,
        fresh_token: bool,
    ) -> Result<mpsc::UnboundedReceiver<ConnectionEvent>, TransportError> {
        self.state
            .reconnects
            .lock()
            .unwrap()
            .push((tokio::time::Instant::now(), fresh_token));
        let outcome = self
            .state
            .reconnect_outcomes
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Reconnect::Ok);
        match outcome {
            Reconnect::Ok => {
                let (tx, rx) = mpsc::unbounded_channel();
                self.state
                    .senders
                    .lock()
                    .unwrap()
                    .insert(self.key.clone(), tx);
                Ok(rx)
            }
            Reconnect::Fail => Err(TransportError::Connect("unreachable".into())),
            Reconnect::TokenRejected => Err(TransportError::TokenRejected("401".into())),
            Reconnect::Refused => Err(TransportError::Refused("403".into())),
        }
    }

    fn hand_over(&self) -> Option<Box<dyn std::any::Any>> {
        Some(Box::new(self.key.clone()))
    }

    fn take_over(&self, handover: Box<dyn std::any::Any>) -> Result<(), TransportError> {
        let key = handover.downcast::<String>().expect("a fake's handover");
        self.state.takeovers.lock().unwrap().push(*key);
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
        Self::with_fallback(false)
    }

    fn with_fallback(sfu_fallback: bool) -> Self {
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
            own_reconnect_budget: OWN_RECONNECT_BUDGET,
            sfu_fallback,
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
            reconnecting(PEER_FOCUS),
        ]
    );
    assert_eq!(
        fx.pool.member_connection("carol"),
        Some(ConnectionState::Reconnecting)
    );
    assert_eq!(
        fx.pool.member_connection("own"),
        Some(ConnectionState::Connected)
    );
    tokio::time::sleep(BACKOFF_BASE * 2).await;
    assert_eq!(
        fx.drain().await,
        vec![PoolEvent::Degraded(false), connected(PEER_FOCUS)]
    );
    assert_eq!(
        fx.pool.member_connection("carol"),
        Some(ConnectionState::Connected)
    );
    assert_eq!(fx.connects(PEER_FOCUS), 2);
}

#[tokio::test(start_paused = true)]
async fn a_failed_connect_backs_off_and_retries() {
    let mut fx = Fixture::new();
    fx.state.fail_attempts.store(2, Ordering::SeqCst);
    fx.pool.apply_snapshot(&[member_on("carol", PEER_FOCUS)]);
    assert_eq!(
        fx.drain().await,
        vec![PoolEvent::Degraded(true), reconnecting(PEER_FOCUS)]
    );

    tokio::time::sleep(backoff_delay(1)).await;
    fx.drain().await;
    assert_eq!(fx.connects(PEER_FOCUS), 2);

    tokio::time::sleep(backoff_delay(2)).await;
    assert_eq!(
        fx.drain().await,
        vec![PoolEvent::Degraded(false), connected(PEER_FOCUS)]
    );
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

/// 034 R24: the transport giving up is not the end: the own focus is
/// reconnected behind the same handle, at once and then on backoff, reusing
/// the token; `close` still spares it.
#[tokio::test(start_paused = true)]
async fn the_own_focus_is_reconnected_after_the_transport_gives_up() {
    let mut fx = Fixture::new();
    let own = fx.adopt();
    let start = tokio::time::Instant::now();
    own.send(ConnectionEvent::Reconnecting).unwrap();
    fx.drain().await;
    tokio::time::sleep(Duration::from_secs(50)).await;
    fx.state
        .reconnect_outcomes
        .lock()
        .unwrap()
        .push_back(Reconnect::Fail);
    own.send(ConnectionEvent::Closed {
        message: "ten attempts spent".into(),
    })
    .unwrap();
    let mut events = fx.drain().await;
    for _ in 0..2 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        events.extend(fx.drain().await);
    }

    let attempts: Vec<(u64, bool)> = fx
        .state
        .reconnects
        .lock()
        .unwrap()
        .iter()
        .map(|(at, fresh)| ((*at - start).as_secs(), *fresh))
        .collect();
    assert_eq!(attempts, [(50, false), (51, false)], "token reused");
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, PoolEvent::OwnConnectionLost { .. }))
    );
    assert_eq!(fx.pool.own_connection_state(), ConnectionState::Connected);
    assert!(fx.pool.own_connection().is_some());

    fx.pool.close();
    fx.drain().await;
    assert!(fx.state.closes.lock().unwrap().is_empty());
}

fn lost(events: &[PoolEvent]) -> Option<OwnConnectionLoss> {
    events.iter().find_map(|event| match event {
        PoolEvent::OwnConnectionLost { reason, .. } => Some(*reason),
        _ => None,
    })
}

/// 034 R24: a token the SFU rejects is fetched again, once.
#[tokio::test(start_paused = true)]
async fn a_rejected_token_is_fetched_again_once() {
    let mut fx = Fixture::new();
    let own = fx.adopt();
    fx.state
        .reconnect_outcomes
        .lock()
        .unwrap()
        .push_back(Reconnect::TokenRejected);
    own.send(ConnectionEvent::Closed {
        message: "gone".into(),
    })
    .unwrap();
    fx.drain().await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    fx.drain().await;
    let fresh: Vec<bool> = fx
        .state
        .reconnects
        .lock()
        .unwrap()
        .iter()
        .map(|(_, fresh)| *fresh)
        .collect();
    assert_eq!(fresh, [false, true]);
    assert_eq!(fx.pool.own_connection_state(), ConnectionState::Connected);
}

/// 034 R24: rejected again after the refetch, or any stated refusal, ends
/// the budget at once.
#[tokio::test(start_paused = true)]
async fn a_token_rejected_again_after_one_refetch_ends_the_budget_at_once() {
    for outcomes in [
        vec![Reconnect::TokenRejected, Reconnect::TokenRejected],
        vec![Reconnect::Refused],
    ] {
        let mut fx = Fixture::new();
        let own = fx.adopt();
        let start = tokio::time::Instant::now();
        fx.state.reconnect_outcomes.lock().unwrap().extend(outcomes);
        own.send(ConnectionEvent::Closed {
            message: "gone".into(),
        })
        .unwrap();
        let mut events = fx.drain().await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        events.extend(fx.drain().await);
        assert_eq!(lost(&events), Some(OwnConnectionLoss::Refused));
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "at once, not at 60 s"
        );
        let attempts = fx.state.reconnects.lock().unwrap().len();
        tokio::time::sleep(Duration::from_secs(120)).await;
        fx.drain().await;
        assert_eq!(
            fx.state.reconnects.lock().unwrap().len(),
            attempts,
            "never retried"
        );
        assert_eq!(fx.pool.own_connection_state(), ConnectionState::Unreachable);
    }
}

/// 034 R24: the budget runs from the first sign of the loss, and ends the
/// own focus at 60 s, not before.
#[tokio::test(start_paused = true)]
async fn the_reconnect_budget_ends_the_own_focus_after_sixty_seconds() {
    let mut fx = Fixture::new();
    let own = fx.adopt();
    fx.state
        .reconnect_outcomes
        .lock()
        .unwrap()
        .extend(std::iter::repeat_n(Reconnect::Fail, 100));
    own.send(ConnectionEvent::Reconnecting).unwrap();
    fx.drain().await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    own.send(ConnectionEvent::Closed {
        message: "gone".into(),
    })
    .unwrap();
    fx.drain().await;
    tokio::time::sleep(Duration::from_millis(49_900)).await;
    assert_eq!(lost(&fx.drain().await), None, "not before 60 s");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(lost(&fx.drain().await), Some(OwnConnectionLoss::Budget));
}

/// 034 R24, C10: the first connect runs under the budget from the request;
/// never established, it is `NeverConnected`; established, it is up.
#[tokio::test(start_paused = true)]
async fn the_first_connect_runs_under_the_budget() {
    let mut fx = Fixture::new();
    fx.state
        .reconnect_outcomes
        .lock()
        .unwrap()
        .extend(std::iter::repeat_n(Reconnect::Fail, 100));
    let (connection, _) = fake_connection(&fx.state, OWN_FOCUS);
    let events = fx.pool.connect_own(connection);
    assert!(events.contains(&PoolEvent::Degraded(true)));
    assert_eq!(
        fx.pool.own_connection_state(),
        ConnectionState::Reconnecting
    );
    assert!(
        fx.pool.own_handle().is_some(),
        "publishable before it is up"
    );
    tokio::time::sleep(Duration::from_secs(61)).await;
    assert_eq!(
        lost(&fx.drain().await),
        Some(OwnConnectionLoss::NeverConnected)
    );

    let mut fx = Fixture::new();
    let (connection, _) = fake_connection(&fx.state, OWN_FOCUS);
    fx.pool.connect_own(connection);
    let events = fx.drain().await;
    assert!(events.contains(&connected(OWN_FOCUS)));
    assert_eq!(fx.pool.own_connection_state(), ConnectionState::Connected);
    assert!(fx.pool.own_connection().is_some());
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

fn reconnecting(key: &str) -> PoolEvent {
    PoolEvent::ConnectionState {
        connection_key: key.into(),
        state: ConnectionState::Reconnecting,
    }
}

fn connected(key: &str) -> PoolEvent {
    PoolEvent::ConnectionState {
        connection_key: key.into(),
        state: ConnectionState::Connected,
    }
}

/// 034 R30: a member's focus is retried on capped backoff for as long as
/// they publish there, and only their departure stops it.
#[tokio::test(start_paused = true)]
async fn a_peer_focus_is_retried_for_as_long_as_a_member_publishes_there() {
    let mut fx = Fixture::new();
    fx.state.fail_attempts.store(u32::MAX, Ordering::SeqCst);
    fx.pool.apply_snapshot(&[member_on("carol", PEER_FOCUS)]);
    for _ in 0..100 {
        tokio::time::sleep(BACKOFF_MAX).await;
        fx.drain().await;
    }
    let attempts = fx.connects(PEER_FOCUS);
    assert!(attempts >= 95, "still trying at 50 min: {attempts}");
    assert_eq!(
        fx.pool.member_connection("carol"),
        Some(ConnectionState::Reconnecting)
    );

    fx.pool.apply_snapshot(&[]);
    tokio::time::sleep(BACKOFF_MAX * 4).await;
    fx.drain().await;
    assert!(
        fx.connects(PEER_FOCUS) <= attempts + 1,
        "stopped with the member"
    );
}

/// 034 R16, R28: unmappable identities are listed until they map or leave;
/// a member with no supported transport is unreachable; our own media is
/// reconnecting until the own focus is adopted.
#[tokio::test(start_paused = true)]
async fn unmappable_identities_and_unreachable_members_are_reported() {
    let mut fx = Fixture::new();
    assert_eq!(
        fx.pool.own_connection_state(),
        ConnectionState::Reconnecting
    );
    let own = fx.adopt();
    assert_eq!(fx.pool.own_connection_state(), ConnectionState::Connected);
    fx.pool.apply_snapshot(&[member_on("own", OWN_FOCUS)]);

    own.send(ConnectionEvent::RemoteJoined {
        identity: "id-bob".into(),
    })
    .unwrap();
    own.send(ConnectionEvent::RemoteJoined {
        identity: "id-stranger".into(),
    })
    .unwrap();
    fx.drain().await;
    assert_eq!(fx.pool.unmappable(), ["id-bob", "id-stranger"]);

    fx.pool
        .apply_snapshot(&[member_on("own", OWN_FOCUS), member_on("bob", OWN_FOCUS)]);
    assert_eq!(fx.pool.unmappable(), ["id-stranger"], "bob mapped");
    own.send(ConnectionEvent::RemoteLeft {
        identity: "id-stranger".into(),
    })
    .unwrap();
    fx.drain().await;
    assert!(fx.pool.unmappable().is_empty(), "the stranger left");

    own.send(ConnectionEvent::Reconnecting).unwrap();
    fx.drain().await;
    assert_eq!(
        fx.pool.own_connection_state(),
        ConnectionState::Reconnecting
    );
    assert_eq!(
        fx.pool.member_connection("bob"),
        Some(ConnectionState::Reconnecting)
    );
}

mod fallback {
    //! 034 R25: our own focus down past the grace, with signalling working
    //! and a peer connection up, moves our media onto the busiest peer focus.

    use super::*;

    const THIRD_FOCUS: &str = "https://sfu-c.example.org";

    /// Own focus adopted; bob and carol on the peer focus, dave on a third.
    async fn call(sfu_fallback: bool) -> (Fixture, UnboundedSender<ConnectionEvent>) {
        let mut fx = Fixture::with_fallback(sfu_fallback);
        let own = fx.adopt();
        fx.pool.apply_snapshot(&[
            member_on("own", OWN_FOCUS),
            member_on("bob", PEER_FOCUS),
            member_on("carol", PEER_FOCUS),
            member_on("dave", THIRD_FOCUS),
        ]);
        fx.drain().await;
        (fx, own)
    }

    /// The transport gives our own focus up, and every reconnect fails.
    fn lose_own(fx: &Fixture, own: &UnboundedSender<ConnectionEvent>) {
        fx.state
            .reconnect_outcomes
            .lock()
            .unwrap()
            .extend([Reconnect::Fail; 40]);
        own.send(ConnectionEvent::Closed {
            message: "gone".into(),
        })
        .unwrap();
    }

    /// Runs `secs` seconds a second at a time; every event with the second
    /// it came at, counted from `start`.
    async fn run(
        fx: &mut Fixture,
        start: tokio::time::Instant,
        secs: u64,
    ) -> Vec<(u64, PoolEvent)> {
        let mut events = Vec::new();
        for _ in 0..secs {
            let at = (tokio::time::Instant::now() - start).as_secs();
            events.extend(fx.drain().await.into_iter().map(|event| (at, event)));
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        events
    }

    fn moved(events: &[(u64, PoolEvent)]) -> Option<(u64, String)> {
        events.iter().find_map(|(at, event)| match event {
            PoolEvent::OwnFocusMoved {
                connection_key,
                transport,
            } => {
                assert_eq!(
                    *transport,
                    RtcTransport::LiveKit(LiveKitTransport {
                        livekit_service_url: connection_key.clone(),
                    })
                );
                Some((*at, connection_key.clone()))
            }
            _ => None,
        })
    }

    fn lost_at(events: &[(u64, PoolEvent)]) -> Option<(u64, OwnConnectionLoss)> {
        events.iter().find_map(|(at, event)| match event {
            PoolEvent::OwnConnectionLost { reason, .. } => Some((*at, *reason)),
            _ => None,
        })
    }

    #[tokio::test(start_paused = true)]
    async fn the_busiest_peer_connection_is_taken_over_at_ten_seconds() {
        let (mut fx, own) = call(true).await;
        let start = tokio::time::Instant::now();
        lose_own(&fx, &own);
        let events = run(&mut fx, start, 70).await;

        assert_eq!(moved(&events), Some((10, PEER_FOCUS.to_owned())));
        assert_eq!(*fx.state.takeovers.lock().unwrap(), [PEER_FOCUS]);
        assert_eq!(lost_at(&events), None);
        assert_eq!(fx.pool.own_connection_state(), ConnectionState::Connected);
        let after: Vec<u64> = fx
            .state
            .reconnects
            .lock()
            .unwrap()
            .iter()
            .map(|(at, _)| (*at - start).as_secs())
            .filter(|at| *at >= 10)
            .collect();
        assert!(after.is_empty(), "the own attempt dropped: {after:?}");
        // The old focus has nobody else on it: nothing to reconnect.
        assert_eq!(fx.connects(OWN_FOCUS), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn without_the_host_flag_the_budget_runs_out() {
        let (mut fx, own) = call(false).await;
        let start = tokio::time::Instant::now();
        lose_own(&fx, &own);
        let events = run(&mut fx, start, 70).await;
        assert_eq!(moved(&events), None);
        assert_eq!(lost_at(&events), Some((60, OwnConnectionLoss::Budget)));
    }

    #[tokio::test(start_paused = true)]
    async fn no_fallback_while_every_connection_is_down() {
        let (mut fx, own) = call(true).await;
        let start = tokio::time::Instant::now();
        fx.sender(PEER_FOCUS)
            .send(ConnectionEvent::Reconnecting)
            .unwrap();
        fx.sender(THIRD_FOCUS)
            .send(ConnectionEvent::Reconnecting)
            .unwrap();
        lose_own(&fx, &own);
        let events = run(&mut fx, start, 70).await;
        assert_eq!(moved(&events), None);
        assert!(fx.state.takeovers.lock().unwrap().is_empty());
        assert_eq!(lost_at(&events), Some((60, OwnConnectionLoss::Budget)));
    }

    #[tokio::test(start_paused = true)]
    async fn no_fallback_in_a_single_sfu_call() {
        let mut fx = Fixture::with_fallback(true);
        let own = fx.adopt();
        fx.pool
            .apply_snapshot(&[member_on("own", OWN_FOCUS), member_on("bob", OWN_FOCUS)]);
        fx.drain().await;
        let start = tokio::time::Instant::now();
        lose_own(&fx, &own);
        let events = run(&mut fx, start, 70).await;
        assert_eq!(moved(&events), None);
        assert_eq!(lost_at(&events), Some((60, OwnConnectionLoss::Budget)));
    }

    /// The busiest focus is down while another peer's is up: the move waits
    /// for it, and happens as it comes back.
    #[tokio::test(start_paused = true)]
    async fn a_peer_connection_coming_back_mid_outage_enables_the_fallback() {
        let (mut fx, own) = call(true).await;
        let start = tokio::time::Instant::now();
        let peer = fx.sender(PEER_FOCUS);
        peer.send(ConnectionEvent::Reconnecting).unwrap();
        lose_own(&fx, &own);
        let mut events = run(&mut fx, start, 25).await;
        assert_eq!(moved(&events), None);
        peer.send(ConnectionEvent::Reconnected).unwrap();
        events.extend(run(&mut fx, start, 5).await);
        assert_eq!(moved(&events), Some((25, PEER_FOCUS.to_owned())));
    }

    #[tokio::test(start_paused = true)]
    async fn the_fallback_needs_signalling() {
        let (mut fx, own) = call(true).await;
        let start = tokio::time::Instant::now();
        fx.pool.set_signalling_reachable(false);
        lose_own(&fx, &own);
        let mut events = run(&mut fx, start, 20).await;
        assert_eq!(moved(&events), None);
        events.extend(
            fx.pool
                .set_signalling_reachable(true)
                .into_iter()
                .map(|event| (20, event)),
        );
        assert_eq!(moved(&events), Some((20, PEER_FOCUS.to_owned())));
    }

    /// Our own focus back first: the pending fallback is dropped.
    #[tokio::test(start_paused = true)]
    async fn our_own_focus_back_first_wins() {
        let (mut fx, own) = call(true).await;
        let start = tokio::time::Instant::now();
        let peer = fx.sender(PEER_FOCUS);
        peer.send(ConnectionEvent::Reconnecting).unwrap();
        fx.state
            .reconnect_outcomes
            .lock()
            .unwrap()
            .extend([Reconnect::Fail; 4]);
        own.send(ConnectionEvent::Closed {
            message: "gone".into(),
        })
        .unwrap();
        // Attempts at 0, 1, 3, 7 fail; the one at 15 lands.
        let mut events = run(&mut fx, start, 18).await;
        peer.send(ConnectionEvent::Reconnected).unwrap();
        events.extend(run(&mut fx, start, 5).await);
        assert_eq!(moved(&events), None);
        assert!(fx.state.takeovers.lock().unwrap().is_empty());
        assert_eq!(fx.pool.own_connection_state(), ConnectionState::Connected);
    }

    /// Whoever else publishes on the old focus is subscribed to as a peer's.
    #[tokio::test(start_paused = true)]
    async fn the_old_focus_is_connected_as_a_peer_when_others_remain_there() {
        let (mut fx, own) = call(true).await;
        fx.pool.apply_snapshot(&[
            member_on("own", OWN_FOCUS),
            member_on("erin", OWN_FOCUS),
            member_on("bob", PEER_FOCUS),
            member_on("carol", PEER_FOCUS),
        ]);
        let start = tokio::time::Instant::now();
        lose_own(&fx, &own);
        let events = run(&mut fx, start, 12).await;
        assert_eq!(moved(&events), Some((10, PEER_FOCUS.to_owned())));
        assert_eq!(fx.connects(OWN_FOCUS), 1);
        assert_eq!(
            fx.pool.member_connection("erin"),
            Some(ConnectionState::Connected)
        );
    }
    #[derive(Clone, Debug)]
    enum Step {
        OwnClosed,
        PeerReconnecting,
        PeerReconnected,
        Signalling(bool),
        Wait(u64),
    }

    fn step() -> impl proptest::strategy::Strategy<Value = Step> {
        use proptest::prelude::*;
        prop_oneof![
            Just(Step::OwnClosed),
            Just(Step::PeerReconnecting),
            Just(Step::PeerReconnected),
            any::<bool>().prop_map(Step::Signalling),
            (1u64..20).prop_map(Step::Wait),
        ]
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(64))]

        /// Whatever the order of losses, reconnect outcomes, peer outages and
        /// signalling: our media moves only with the host flag, only while
        /// signalling is reachable, first to the busiest focus, never back to
        /// our own, and again (or given up) only once the focus it moved to
        /// was lost in turn.
        #[test]
        fn a_fallback_is_taken_only_on_its_terms(
            sfu_fallback: bool,
            fails in proptest::collection::vec(proptest::bool::ANY, 0..40),
            steps in proptest::collection::vec(step(), 1..30),
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .unwrap();
            runtime.block_on(async {
                let (mut fx, own) = call(sfu_fallback).await;
                fx.state.reconnect_outcomes.lock().unwrap().extend(
                    fails
                        .iter()
                        .map(|fail| if *fail { Reconnect::Fail } else { Reconnect::Ok }),
                );
                let peer = fx.sender(PEER_FOCUS);
                let mut reachable = true;
                let mut moves = 0;
                let mut new_focus_lost = false;
                for step in steps {
                    new_focus_lost |= moves > 0 && matches!(step, Step::PeerReconnecting);
                    let events = match step {
                        Step::OwnClosed => {
                            let _ = own.send(ConnectionEvent::Closed {
                                message: "gone".into(),
                            });
                            fx.drain().await
                        }
                        Step::PeerReconnecting => {
                            let _ = peer.send(ConnectionEvent::Reconnecting);
                            fx.drain().await
                        }
                        Step::PeerReconnected => {
                            let _ = peer.send(ConnectionEvent::Reconnected);
                            fx.drain().await
                        }
                        Step::Signalling(now) => {
                            reachable = now;
                            let mut events = fx.pool.set_signalling_reachable(now);
                            events.extend(fx.drain().await);
                            events
                        }
                        Step::Wait(secs) => {
                            let mut events = Vec::new();
                            for _ in 0..secs {
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                events.extend(fx.drain().await);
                            }
                            events
                        }
                    };
                    for event in &events {
                        if let PoolEvent::OwnFocusMoved { connection_key, .. } = event {
                            assert!(sfu_fallback, "moved without the host flag");
                            assert!(reachable, "moved while signalling was down");
                            assert_ne!(connection_key, OWN_FOCUS, "never back");
                            if moves == 0 {
                                assert_eq!(connection_key, PEER_FOCUS, "the busiest focus");
                            } else {
                                assert!(new_focus_lost, "moved again with no new loss");
                            }
                            moves += 1;
                        }
                        if matches!(event, PoolEvent::OwnConnectionLost { .. }) {
                            assert!(moves == 0 || new_focus_lost, "given up after moving away");
                        }
                    }
                }
            });
        }
    }
}

/// 034 R25: a member moving focus is added on the new connection before the
/// old one removes them; that late removal leaves the new track alone.
#[tokio::test(start_paused = true)]
async fn a_late_removal_from_the_old_connection_keeps_the_moved_track() {
    let mut fx = Fixture::new();
    let own = fx.adopt();
    fx.pool
        .apply_snapshot(&[member_on("own", OWN_FOCUS), member_on("bob", OWN_FOCUS)]);
    own.send(track_added("id-bob", MediaStreamKind::Camera))
        .unwrap();
    fx.drain().await;

    fx.pool
        .apply_snapshot(&[member_on("own", OWN_FOCUS), member_on("bob", PEER_FOCUS)]);
    fx.drain().await;
    fx.sender(PEER_FOCUS)
        .send(track_added("id-bob", MediaStreamKind::Camera))
        .unwrap();
    own.send(ConnectionEvent::TrackRemoved {
        identity: "id-bob".into(),
        kind: MediaStreamKind::Camera,
    })
    .unwrap();
    let events = fx.drain().await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, PoolEvent::StreamRemoved { .. })),
        "{events:?}"
    );
    assert!(
        fx.pool
            .remote_tracks()
            .get("bob", MediaStreamKind::Camera)
            .is_some()
    );
}
