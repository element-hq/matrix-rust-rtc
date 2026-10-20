// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The connection pool: a session's media over N transport connections, keyed
//! by `member_id`.
//!
//! # Connections (MSC4195 multi-SFU)
//!
//! Every member publishes media on the focus they announced in their
//! membership's `transports`; subscribing to them means connecting to *their*
//! focus. The pool groups members by [`MediaTransport::connection_key`] and
//! keeps exactly one connection per key:
//!
//! - a key appearing in the snapshot opens a connection via
//!   [`MediaTransport::connect`], retried with exponential backoff while
//!   members still need it;
//! - a key whose last member left is closed after a short idle grace (so
//!   membership flaps don't churn connections);
//! - a peer-focus connection dying tears down its members' streams and
//!   reconnects — only the *own* focus (the one we publish on, adopted via
//!   [`MediaPool::adopt_own_connection`]) ending is reported as lost, once
//!   its reconnect budget is spent;
//! - with [`PoolConfig::sfu_fallback`], our own focus down for
//!   [`FALLBACK_GRACE`] while a peer's connection is up moves our media onto
//!   the busiest peer focus instead (034 R25).
//!
//! # Identity mapping
//!
//! Transports know participants by their own identities (LiveKit: the MSC4195
//! pseudonymous identity), and the pool reverse-maps those to `member_id`s
//! using [`MediaTransport::remote_identity`]. Transport participants that map
//! to no membership never surface as members (and their media could not be
//! decrypted anyway — keys are distributed per membership). Media and keys
//! that arrive *before* their membership (SFU connects are often faster than
//! sticky event propagation) are buffered and flushed when the membership
//! lands.
//!
//! # Driving it
//!
//! The pool is a state machine, not a task: its owner (an application's actor,
//! the call SDK's engine for calls) feeds it membership snapshots and the
//! [`PoolMessage`]s from the inbox [`MediaPool::new`] returns, and applies the
//! [`PoolEvent`]s each call hands back. Owning the ordering is what lets that
//! actor keep its own state (a roster) consistent with the media, which a pool
//! running beside it could not. Connects and timers run on
//! [`matrix_rtc_core::executor`] and report back through the inbox.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use matrix_rtc_core::executor as rt;
use matrix_rtc_core::{JoinedMembership, RtcTransport};
use tokio::sync::mpsc;

use crate::{
    ConnectionContext, ConnectionEvent, ConnectionState, FrameEncryptionState, MediaConstraints,
    MediaStreamKind, MediaTransport, RemoteTrackHandle, TransportConnection, TransportError,
};

/// How long a connection with no remaining members is kept before closing,
/// so a membership flap (sticky expiry glitch, quick rejoin) doesn't tear a
/// connection down just to rebuild it.
pub const IDLE_GRACE: Duration = Duration::from_secs(10);

/// First-retry delay after a failed connect; doubles per attempt.
const BACKOFF_BASE: Duration = Duration::from_secs(1);
/// Retry delay ceiling.
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// How long our own focus may stay unreachable, from the loss or the first
/// connect request, before the call ends (034 R24).
pub const OWN_RECONNECT_BUDGET: Duration = Duration::from_secs(60);

/// Our own focus's reconnect backoff ceiling: lower than a peer focus's, so
/// an SFU back late in the budget is tried before it runs out (034 R24).
const OWN_BACKOFF_MAX: Duration = Duration::from_secs(10);

/// How long the attempt made at the budget's end may take before our own
/// focus is given up regardless (034 R24).
const OWN_FINAL_ATTEMPT_GRACE: Duration = Duration::from_secs(10);

/// How long our own focus is down before our media may move to a peer's
/// focus, when the host allows it (034 R25). A first value: the core logs
/// each loss's time to recovery so real outages can tune it.
pub const FALLBACK_GRACE: Duration = Duration::from_secs(10);

/// Constraint changes are coalesced for this long before being applied —
/// scroll-driven visibility churn is the hot path, and only the final state
/// matters to the transport.
pub const CONSTRAINTS_DEBOUNCE: Duration = Duration::from_millis(150);

fn backoff_delay(attempt: u32) -> Duration {
    BACKOFF_BASE
        .saturating_mul(2u32.saturating_pow(attempt.saturating_sub(1)))
        .min(BACKOFF_MAX)
}

/// Subscribed remote tracks, keyed by member and stream kind. Shared between
/// the pool (writes) and [`RemoteTracks`] (reads).
type TrackMap = Arc<Mutex<HashMap<(String, MediaStreamKind), PooledTrack>>>;

/// A subscribed track and the connection it came over. A member moving
/// focus (034 R25) is added on the new connection before the old one
/// removes them, and that late removal must not take the new track.
#[derive(Clone)]
struct PooledTrack {
    connection_key: String,
    track: Arc<dyn RemoteTrackHandle>,
}

/// Tracks whose transport identity has no membership yet, buffered per
/// identity until the membership lands.
type PendingTracks = HashMap<String, Vec<(MediaStreamKind, PooledTrack)>>;

/// What [`MediaTransport::connect`] resolves to, as carried in the inbox.
type ConnectOutcome = Result<
    (
        Box<dyn TransportConnection>,
        mpsc::UnboundedReceiver<ConnectionEvent>,
    ),
    TransportError,
>;

/// Static configuration of a [`MediaPool`].
pub struct PoolConfig {
    /// Transport backends, in descending order of preference.
    pub transports: Vec<Arc<dyn MediaTransport>>,
    /// Session-scoped context passed to [`MediaTransport::connect`].
    pub ctx: ConnectionContext,
    /// Connection key of the focus we publish on. The pool never opens this
    /// one itself — the owner establishes it (so a join can fail fast) and
    /// hands it over via [`MediaPool::adopt_own_connection`].
    pub own_connection_key: Option<String>,
    /// [`OWN_RECONNECT_BUDGET`] unless a host configured another.
    pub own_reconnect_budget: Duration,
    /// Whether our media may move to a peer's focus while ours is down
    /// (034 R25). Off unless the host turns it on: the peer's SFU must accept
    /// a token for an account of another homeserver.
    pub sfu_fallback: bool,
}

/// Why our own focus was given up (034 R24, contract C10).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnConnectionLoss {
    /// Lost, and not back within the budget.
    Budget,
    /// The SFU or the token service refused us.
    Refused,
    /// Never established within the budget from the connect request.
    NeverConnected,
}

/// What the pool reports, keyed by `member_id`.
#[derive(Clone, Debug, PartialEq)]
pub enum PoolEvent {
    /// A member's stream is subscribed; its handle is in [`RemoteTracks`].
    StreamAdded {
        member_id: String,
        kind: MediaStreamKind,
    },
    /// A member's stream went away (unpublished, or its connection died).
    StreamRemoved {
        member_id: String,
        kind: MediaStreamKind,
    },
    /// The sender muted or unmuted one of their streams.
    StreamMuted {
        member_id: String,
        kind: MediaStreamKind,
        muted: bool,
    },
    /// Who is speaking now, as the transport reports it. Only members are
    /// listed: a level nothing maps to a membership is not actionable.
    ActiveSpeakers { speakers: Vec<ActiveSpeaker> },
    /// The frame cryptor's verdict on a member's media changed.
    EncryptionStateChanged {
        member_id: String,
        state: FrameEncryptionState,
    },
    /// A media key for a member was imported (reported through
    /// [`MediaPool::key_imported`], released once the membership is known),
    /// with the transport identity it was installed under.
    KeyImported {
        member_id: String,
        identity: String,
        key_index: u8,
    },
    /// A transport participant joined that maps to no membership.
    UnknownParticipant { identity: String },
    /// A connection's state moved (034 R28): its members', or our own
    /// media's for the own focus. Read a member's through
    /// [`MediaPool::member_connection`].
    ConnectionState {
        connection_key: String,
        state: ConnectionState,
    },
    /// Whether any connection is impaired (reconnecting or failing to
    /// connect). Reported on transitions only.
    Degraded(bool),
    /// The own-focus connection is gone for good; nothing we publish reaches
    /// anyone. Only after the reconnect budget, or a refusal (034 R24).
    OwnConnectionLost {
        reason: OwnConnectionLoss,
        message: String,
    },
    /// Our media moved to a peer's focus while ours was down (034 R25): our
    /// membership must name `transport` from now on, under the same member
    /// id. Followed by that connection's [`PoolEvent::ConnectionState`].
    OwnFocusMoved {
        connection_key: String,
        transport: RtcTransport,
    },
}

/// One speaking member and how loud they are.
#[derive(Clone, Debug, PartialEq)]
pub struct ActiveSpeaker {
    pub member_id: String,
    /// `0.0` (silent) to `1.0` (loudest).
    pub level: f32,
}

/// A connect result, connection event or timer for the pool, from the inbox
/// [`MediaPool::new`] returns. Opaque: hand it to [`MediaPool::handle`].
pub struct PoolMessage(Message);

enum Message {
    /// An event from a pooled connection. `generation` guards against events
    /// of a replaced connection being applied to its successor.
    Connection {
        connection_key: String,
        generation: u64,
        event: ConnectionEvent,
    },
    /// A pooled connection's event stream ended.
    ConnectionEnded {
        connection_key: String,
        generation: u64,
    },
    /// A [`MediaTransport::connect`] attempt resolved.
    ConnectFinished {
        connection_key: String,
        attempt: u32,
        result: ConnectOutcome,
    },
    /// A backoff timer elapsed; try connecting again.
    RetryConnect {
        connection_key: String,
        attempt: u32,
    },
    /// An idle grace timer elapsed; close the connection if still unneeded.
    CloseIfIdle {
        connection_key: String,
        idle_generation: u64,
    },
    /// A constraints debounce timer elapsed; apply if still current.
    ApplyConstraints {
        member_id: String,
        kind: MediaStreamKind,
        generation: u64,
    },
    /// An own-focus (re)connect attempt resolved.
    OwnConnectFinished {
        attempt: u32,
        result: Result<mpsc::UnboundedReceiver<ConnectionEvent>, TransportError>,
    },
    /// An own-focus backoff timer elapsed.
    OwnRetry { attempt: u32 },
    /// The reconnect budget of the loss numbered `epoch` ran out.
    OwnBudget { epoch: u64 },
    /// The attempt made at the end of the loss numbered `epoch`'s budget did
    /// not answer in time.
    OwnFinalAttemptExpired { epoch: u64 },
    /// The loss numbered `epoch` has lasted [`FALLBACK_GRACE`].
    OwnFallback { epoch: u64 },
}

/// Our own focus while it is being recovered (034 R24): the handle every
/// connect goes through, and how long it has been down.
struct OwnRecovery {
    connection: Arc<dyn TransportConnection>,
    /// Since when our media has not been carried; `None` while it is.
    lost_since: Option<rt::Instant>,
    /// Bumped on every loss and recovery, so a budget timer of an earlier
    /// loss is stale.
    epoch: u64,
    /// A fresh token was asked for during this loss: a second rejection is
    /// a refusal.
    token_refetched: bool,
    /// Ask for a fresh token on the next attempt.
    refetch_next: bool,
    /// The connection has been up at least once.
    ever_up: bool,
    /// The budget ran out: the attempt in flight, or the one made then, is
    /// the last.
    budget_spent: bool,
}

/// Read access to the subscribed remote tracks, from outside the owner's
/// actor. Cheap to clone.
#[derive(Clone)]
pub struct RemoteTracks(TrackMap);

impl RemoteTracks {
    /// The handle for a member's subscribed stream; `None` while no such
    /// stream is up (see [`PoolEvent::StreamAdded`]).
    pub fn get(
        &self,
        member_id: &str,
        kind: MediaStreamKind,
    ) -> Option<Arc<dyn RemoteTrackHandle>> {
        self.0
            .lock()
            .expect("track map mutex poisoned")
            .get(&(member_id.to_owned(), kind))
            .map(|pooled| pooled.track.clone())
    }

    /// [`RemoteTracks::get`] for many streams under one lock, in order.
    pub fn get_many(
        &self,
        streams: &[(String, MediaStreamKind)],
    ) -> Vec<Option<Arc<dyn RemoteTrackHandle>>> {
        let map = self.0.lock().expect("track map mutex poisoned");
        streams
            .iter()
            .map(|key| map.get(key).map(|pooled| pooled.track.clone()))
            .collect()
    }
}

/// A pooled connection to one focus.
struct ManagedConnection {
    /// Backend that opened (and re-opens) this connection. `None` for the
    /// adopted own-focus connection, which the pool never re-opens.
    backend: Option<Arc<dyn MediaTransport>>,
    /// Members whose media lives on this focus, per the latest snapshot.
    members: HashSet<String>,
    /// Whether this is the focus we publish on.
    is_own: bool,
    state: ConnState,
    /// Bumped every time a live connection is installed; events carrying an
    /// older generation belong to a replaced connection and are dropped.
    generation: u64,
    /// Bumped whenever the member set changes; invalidates pending idle-close
    /// timers.
    idle_generation: u64,
}

enum ConnState {
    Connecting {
        attempt: u32,
    },
    Up {
        // Arc, not Box: publish/apply-constraints calls run in spawned tasks
        // that need shared ownership while the entry stays in the pool.
        connection: Arc<dyn TransportConnection>,
    },
    Backoff {
        attempt: u32,
    },
}

/// One session's media connections and the identity mapping over them.
pub struct MediaPool {
    transports: Vec<Arc<dyn MediaTransport>>,
    ctx: ConnectionContext,
    own_connection_key: Option<String>,
    /// Handed to connection forwarders and timers so everything funnels into
    /// the owner's inbox.
    messages_tx: mpsc::UnboundedSender<PoolMessage>,
    tracks: TrackMap,
    /// The latest membership snapshot, kept for pool reconciliation.
    members_snapshot: Vec<JoinedMembership>,
    /// Every `member_id` of the latest snapshot, mapped or not.
    known_members: HashSet<String>,
    /// Transport identity → `member_id`.
    identity_map: HashMap<String, String>,
    /// `member_id` → transport identity (the reverse of `identity_map`),
    /// for pushing constraints at a member's connection.
    member_identities: HashMap<String, String>,
    /// Latest constraints per stream, with a generation counter that
    /// invalidates superseded debounce timers.
    constraints: HashMap<(String, MediaStreamKind), (MediaConstraints, u64)>,
    /// Media that arrived before its membership, flushed when it lands.
    pending_tracks: PendingTracks,
    /// Imported key indices awaiting their membership, in arrival order.
    ///
    /// A `Vec`, not a single index: at join a member can be handed more than one
    /// index before their sticky membership lands (their first key plus a
    /// rotation, say), and keeping only the latest silently dropped the others —
    /// so the host saw one key import where the core had imported several, and
    /// could not tell which index the transport actually held.
    pending_keys: HashMap<String, Vec<u8>>,
    pool: HashMap<String, ManagedConnection>,
    connection_generation: u64,
    /// Connections currently impaired (reconnecting or failing to connect);
    /// media is reported degraded while this is non-empty.
    degraded_keys: HashSet<String>,
    /// Transport identities present on a connection that map to no
    /// membership, with the connection they are on (034 R16).
    unmapped: HashMap<String, String>,
    /// The own-focus connection was given up.
    own_lost: bool,
    /// Our own focus's handle and recovery, once adopted or requested.
    own: Option<OwnRecovery>,
    /// [`Self::close`] was called.
    closed: bool,
    own_reconnect_budget: Duration,
    /// [`PoolConfig::sfu_fallback`]; turned off for good when the transport
    /// cannot move a handle.
    sfu_fallback: bool,
    /// What the owner last said about signalling: a fallback must be able
    /// to republish our membership (034 R25).
    signalling_reachable: bool,
    /// The peer focus our media moves to once its connection is up, while
    /// our own is still down past the grace.
    fallback_target: Option<String>,
    /// Events produced by the call in progress, handed back when it returns.
    out: Vec<PoolEvent>,
}

impl MediaPool {
    /// A pool with no members and no connections, and the inbox its owner
    /// must drain into [`MediaPool::handle`].
    pub fn new(config: PoolConfig) -> (MediaPool, mpsc::UnboundedReceiver<PoolMessage>) {
        let (messages_tx, messages_rx) = mpsc::unbounded_channel();
        // On wasm32 the map is `!Send` (track handles hold JS values), but it
        // is still shared — pool and readers — so `Arc` stays, uncontended.
        #[cfg_attr(target_arch = "wasm32", expect(clippy::arc_with_non_send_sync))]
        let tracks: TrackMap = Arc::new(Mutex::new(HashMap::new()));
        let pool = MediaPool {
            transports: config.transports,
            ctx: config.ctx,
            own_connection_key: config.own_connection_key,
            messages_tx,
            tracks,
            members_snapshot: Vec::new(),
            known_members: HashSet::new(),
            identity_map: HashMap::new(),
            member_identities: HashMap::new(),
            constraints: HashMap::new(),
            pending_tracks: HashMap::new(),
            pending_keys: HashMap::new(),
            pool: HashMap::new(),
            connection_generation: 0,
            degraded_keys: HashSet::new(),
            unmapped: HashMap::new(),
            own_lost: false,
            own: None,
            closed: false,
            own_reconnect_budget: config.own_reconnect_budget,
            sfu_fallback: config.sfu_fallback,
            signalling_reachable: true,
            fallback_target: None,
            out: Vec::new(),
        };
        (pool, messages_rx)
    }

    /// Read access to the subscribed tracks, for use outside the owner.
    pub fn remote_tracks(&self) -> RemoteTracks {
        RemoteTracks(self.tracks.clone())
    }

    /// Whether any backend can serve one of the member's advertised
    /// transports — i.e. whether their media can reach us at all.
    pub fn is_reachable(&self, member: &JoinedMembership) -> bool {
        select_transport(&self.transports, member).is_some()
    }

    /// The state of the connection carrying `member_id`'s media, per the
    /// latest snapshot (034 R28); `None` for someone not in it.
    pub fn member_connection(&self, member_id: &str) -> Option<ConnectionState> {
        let member = self
            .members_snapshot
            .iter()
            .find(|member| member.member_id == member_id)?;
        Some(match select_transport(&self.transports, member) {
            None => ConnectionState::Unreachable,
            Some((_, key)) if self.degraded_keys.contains(&key) => ConnectionState::Reconnecting,
            Some(_) => ConnectionState::Connected,
        })
    }

    /// The state of our own media's connection (034 R23): `Connected` with no
    /// own focus to publish on, `Reconnecting` until it is adopted and while
    /// it recovers, `Unreachable` once it is lost.
    pub fn own_connection_state(&self) -> ConnectionState {
        let Some(key) = &self.own_connection_key else {
            return ConnectionState::Connected;
        };
        if self.own_lost {
            ConnectionState::Unreachable
        } else if !self.pool.contains_key(key) || self.degraded_keys.contains(key) {
            ConnectionState::Reconnecting
        } else {
            ConnectionState::Connected
        }
    }

    /// Transport identities present that map to no membership, sorted
    /// (034 R16): a participant whose membership we have not seen.
    pub fn unmappable(&self) -> Vec<String> {
        self.unmapped
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// The own-focus connection, while it is up.
    pub fn own_connection(&self) -> Option<Arc<dyn TransportConnection>> {
        self.pool
            .values()
            .find(|entry| entry.is_own)
            .and_then(|entry| match &entry.state {
                ConnState::Up { connection } => Some(connection.clone()),
                _ => None,
            })
    }

    /// Apply the latest membership snapshot: map the members that joined
    /// (releasing their buffered media and keys), forget the ones that left,
    /// and open or idle connections to match.
    pub fn apply_snapshot(&mut self, snapshot: &[JoinedMembership]) -> Vec<PoolEvent> {
        let current: HashSet<String> = snapshot
            .iter()
            .map(|member| member.member_id.clone())
            .collect();
        // Departures first, so a member_id rejoining in the same snapshot
        // (new membership, same key space) starts clean.
        let departed: Vec<String> = self.known_members.difference(&current).cloned().collect();
        for member_id in departed {
            self.forget_member(&member_id);
        }
        for member in snapshot {
            if self.known_members.insert(member.member_id.clone()) {
                self.map_member(member);
            }
        }
        self.members_snapshot = snapshot.to_vec();
        self.reconcile_pool();
        std::mem::take(&mut self.out)
    }

    /// Take ownership of the owner-established own-focus connection: its
    /// events are consumed here from now on. Closing it stays with the owner,
    /// so a clean leave can report the close result.
    pub fn adopt_own_connection(
        &mut self,
        connection: Box<dyn TransportConnection>,
        events: mpsc::UnboundedReceiver<ConnectionEvent>,
    ) {
        let key = connection.connection_key().to_owned();
        self.connection_generation += 1;
        let generation = self.connection_generation;
        let members = self.members_on_key(&key);
        let connection: Arc<dyn TransportConnection> = Arc::from(connection);
        self.own = Some(OwnRecovery {
            connection: connection.clone(),
            lost_since: None,
            epoch: 0,
            token_refetched: false,
            refetch_next: false,
            ever_up: true,
            budget_spent: false,
        });
        self.pool.insert(
            key.clone(),
            ManagedConnection {
                backend: None,
                members,
                is_own: true,
                state: ConnState::Up { connection },
                generation,
                idle_generation: 0,
            },
        );
        self.spawn_forwarder(key, generation, events);
    }

    /// Take the own-focus handle before it is connected, and connect it
    /// (034 R24): the first connect runs under the reconnect budget from now,
    /// as a connection lost at the moment of the request, so a join never
    /// waits for the SFU.
    pub fn connect_own(&mut self, connection: Box<dyn TransportConnection>) -> Vec<PoolEvent> {
        let key = connection.connection_key().to_owned();
        let members = self.members_on_key(&key);
        self.own = Some(OwnRecovery {
            connection: Arc::from(connection),
            lost_since: None,
            epoch: 0,
            token_refetched: false,
            refetch_next: false,
            ever_up: false,
            budget_spent: false,
        });
        self.pool.insert(
            key,
            ManagedConnection {
                backend: None,
                members,
                is_own: true,
                state: ConnState::Connecting { attempt: 0 },
                generation: 0,
                idle_generation: 0,
            },
        );
        self.start_own_loss();
        self.spawn_own_attempt(0);
        std::mem::take(&mut self.out)
    }

    /// The own-focus handle, connected or being reconnected: what our
    /// publications go through. `None` before it is adopted and once given up.
    pub fn own_handle(&self) -> Option<Arc<dyn TransportConnection>> {
        self.own.as_ref().map(|own| own.connection.clone())
    }

    /// A media decryption key for a transport `identity` was imported.
    /// Reported against its member at once, or once their membership lands
    /// (to-device keys regularly beat sticky memberships).
    pub fn key_imported(&mut self, identity: String, key_index: u8) -> Vec<PoolEvent> {
        match self.identity_map.get(&identity) {
            Some(member_id) => vec![PoolEvent::KeyImported {
                member_id: member_id.clone(),
                identity,
                key_index,
            }],
            None => {
                let pending = self.pending_keys.entry(identity).or_default();
                if !pending.contains(&key_index) {
                    pending.push(key_index);
                }
                Vec::new()
            }
        }
    }

    /// Set the subscription constraints for one stream of one member.
    ///
    /// Applied after a short debounce (rapid changes coalesce, e.g. while
    /// scrolling a participant grid) and re-applied whenever the stream
    /// (re)appears or its connection resumes. They die with the membership.
    pub fn set_constraints(
        &mut self,
        member_id: String,
        kind: MediaStreamKind,
        constraints: MediaConstraints,
    ) {
        let entry = self
            .constraints
            .entry((member_id.clone(), kind))
            .or_insert((constraints, 0));
        entry.0 = constraints;
        entry.1 += 1;
        let generation = entry.1;
        let messages = self.messages_tx.clone();
        rt::spawn(async move {
            rt::sleep(CONSTRAINTS_DEBOUNCE).await;
            let _ = messages.send(PoolMessage(Message::ApplyConstraints {
                member_id,
                kind,
                generation,
            }));
        });
    }

    /// Whether signalling reaches the homeserver: a fallback is only taken
    /// while it does, since it must republish our membership (034 R25).
    pub fn set_signalling_reachable(&mut self, reachable: bool) -> Vec<PoolEvent> {
        self.signalling_reachable = reachable;
        if reachable {
            self.try_fallback();
        }
        std::mem::take(&mut self.out)
    }

    /// Close every peer-focus connection and forget them all. The adopted
    /// own-focus connection is never closed here (its owner closes it; at
    /// this point it is usually already gone).
    pub fn close(&mut self) {
        // Our own focus's recovery stops: its budget no longer runs, and a
        // connect still in flight is closed when it lands.
        self.closed = true;
        if let Some(own) = self.own.as_mut() {
            own.epoch += 1;
            own.lost_since = None;
        }
        for (_, entry) in self.pool.drain() {
            if let ConnState::Up { connection } = entry.state
                && !entry.is_own
            {
                rt::spawn(async move {
                    if let Err(error) = connection.close().await {
                        log::debug!("closing connection on shutdown failed: {error}");
                    }
                });
            }
        }
    }

    /// Process one message from the inbox.
    pub fn handle(&mut self, PoolMessage(message): PoolMessage) -> Vec<PoolEvent> {
        match message {
            Message::Connection {
                connection_key,
                generation,
                event,
            } => {
                let current = self.pool.get(&connection_key).map(|entry| entry.generation);
                if current == Some(generation) {
                    self.handle_connection_event(&connection_key, event);
                } else {
                    log::trace!("dropping event of replaced connection {connection_key}");
                }
            }
            Message::ConnectionEnded {
                connection_key,
                generation,
            } => {
                self.connection_down(&connection_key, generation, "connection event stream ended");
            }
            Message::ConnectFinished {
                connection_key,
                attempt,
                result,
            } => self.connect_finished(&connection_key, attempt, result),
            Message::RetryConnect {
                connection_key,
                attempt,
            } => self.retry_connect(&connection_key, attempt),
            Message::CloseIfIdle {
                connection_key,
                idle_generation,
            } => self.close_if_idle(&connection_key, idle_generation),
            Message::ApplyConstraints {
                member_id,
                kind,
                generation,
            } => {
                // Only the newest timer applies; older ones were superseded.
                if self
                    .constraints
                    .get(&(member_id.clone(), kind))
                    .is_some_and(|(_, current)| *current == generation)
                {
                    self.apply_constraints_now(&member_id, kind);
                }
            }
            Message::OwnConnectFinished { attempt, result } => {
                self.own_connect_finished(attempt, result);
            }
            Message::OwnRetry { attempt } => self.own_retry(attempt),
            Message::OwnBudget { epoch } => self.own_budget(epoch),
            Message::OwnFinalAttemptExpired { epoch } => {
                if self.own.as_ref().is_some_and(|own| {
                    own.epoch == epoch && own.budget_spent && own.lost_since.is_some()
                }) {
                    self.give_up_own_budget();
                }
            }
            Message::OwnFallback { epoch } => {
                if self.own.as_ref().is_some_and(|own| own.epoch == epoch) {
                    self.try_fallback();
                }
            }
        }
        std::mem::take(&mut self.out)
    }

    // ---- members ------------------------------------------------------

    fn map_member(&mut self, member: &JoinedMembership) {
        let identity = self
            .transports
            .iter()
            .find_map(|backend| backend.remote_identity(member));
        let Some(identity) = identity else {
            // Their media arrives under an identity nothing maps back, so it
            // will be buffered forever rather than surfacing as their stream.
            log::warn!(
                "member {} ({}) has no transport identity; their media cannot be attributed",
                member.member_id,
                member.sender,
            );
            return;
        };
        log::debug!(
            "member {} ({}) maps to transport identity {identity}",
            member.member_id,
            member.sender,
        );
        self.unmapped.remove(&identity);
        self.identity_map
            .insert(identity.clone(), member.member_id.clone());
        self.member_identities
            .insert(member.member_id.clone(), identity.clone());

        // Media and keys that arrived before this membership.
        if let Some(pending) = self.pending_tracks.remove(&identity) {
            log::debug!(
                "flushing {} buffered track(s) onto member {}",
                pending.len(),
                member.member_id,
            );
            for (kind, pooled) in pending {
                self.add_track(&member.member_id, kind, pooled);
            }
        }
        for key_index in self.pending_keys.remove(&identity).unwrap_or_default() {
            self.out.push(PoolEvent::KeyImported {
                member_id: member.member_id.clone(),
                identity: identity.clone(),
                key_index,
            });
        }
    }

    fn forget_member(&mut self, member_id: &str) {
        self.known_members.remove(member_id);
        // Read the identity out before dropping the mappings, so the
        // identity-keyed buffers can be cleared too: a stale index could
        // otherwise resurface against a later member that reuses the identity,
        // and in a long-lived process the maps would only ever grow.
        if let Some(identity) = self.member_identities.remove(member_id) {
            self.pending_keys.remove(&identity);
            self.pending_tracks.remove(&identity);
        }
        self.identity_map.retain(|_, mapped| mapped != member_id);
        // A rejoining member gets a fresh member_id, so their constraints
        // die with the membership.
        self.constraints
            .retain(|(member, _), _| member != member_id);
        self.tracks
            .lock()
            .expect("track map mutex poisoned")
            .retain(|(track_member, _), _| track_member != member_id);
    }

    // ---- streams ------------------------------------------------------

    fn add_track(&mut self, member_id: &str, kind: MediaStreamKind, pooled: PooledTrack) {
        let replaced = self
            .tracks
            .lock()
            .expect("track map mutex poisoned")
            .insert((member_id.to_owned(), kind), pooled)
            .is_some();
        if replaced {
            // Same stream re-announced (e.g. events replayed on attach); the
            // handle is refreshed, but it is not a new stream.
            return;
        }
        self.out.push(PoolEvent::StreamAdded {
            member_id: member_id.to_owned(),
            kind,
        });
        // A fresh subscription starts with server-default settings; push the
        // stored constraints at it immediately (no debounce — nothing to
        // coalesce with).
        if self.constraints.contains_key(&(member_id.to_owned(), kind)) {
            self.apply_constraints_now(member_id, kind);
        }
    }

    /// Removes a member's stream, if it came over `connection_key`: a removal
    /// the old connection reports after the member moved is not theirs.
    fn remove_track(&mut self, member_id: &str, kind: MediaStreamKind, connection_key: &str) {
        let removed = {
            let mut tracks = self.tracks.lock().expect("track map mutex poisoned");
            let key = (member_id.to_owned(), kind);
            let ours = tracks
                .get(&key)
                .is_some_and(|pooled| pooled.connection_key == connection_key);
            ours && tracks.remove(&key).is_some()
        };
        if removed {
            self.out.push(PoolEvent::StreamRemoved {
                member_id: member_id.to_owned(),
                kind,
            });
        }
    }

    /// Push the resolved constraints for one stream to the connection its
    /// member lives on. No-op while the member, its identity, or its
    /// connection is missing — a new track and a reconnect re-apply.
    fn apply_constraints_now(&self, member_id: &str, kind: MediaStreamKind) {
        let Some((constraints, _)) = self.constraints.get(&(member_id.to_owned(), kind)) else {
            return;
        };
        let resolved = constraints.resolve(kind);
        let Some(identity) = self.member_identities.get(member_id).cloned() else {
            return;
        };
        let connection = self
            .pool
            .values()
            .find(|entry| entry.members.contains(member_id))
            .and_then(|entry| match &entry.state {
                ConnState::Up { connection } => Some(connection.clone()),
                _ => None,
            });
        let Some(connection) = connection else {
            return;
        };
        rt::spawn(async move {
            if let Err(error) = connection
                .apply_constraints(&identity, kind, resolved)
                .await
            {
                log::warn!("applying constraints for {identity} ({kind:?}) failed: {error}");
            }
        });
    }

    /// Re-apply every stored constraint for members living on `key` (used
    /// after a transport-level reconnect: subscription settings are
    /// server-side state of the connection).
    fn reapply_connection_constraints(&self, key: &str) {
        let Some(entry) = self.pool.get(key) else {
            return;
        };
        for (member_id, kind) in self.constraints.keys() {
            if entry.members.contains(member_id) {
                self.apply_constraints_now(member_id, *kind);
            }
        }
    }

    // ---- connections --------------------------------------------------

    /// Sync the pool with the latest snapshot: update per-connection member
    /// sets, schedule idle closes, open connections for new focus groups.
    fn reconcile_pool(&mut self) {
        let mut desired: HashMap<String, (Arc<dyn MediaTransport>, HashSet<String>)> =
            HashMap::new();
        for member in &self.members_snapshot {
            // We never subscribe to ourselves; our own media's focus is the
            // own entry's, whatever our membership names meanwhile.
            if member.member_id == self.ctx.member.member_id {
                continue;
            }
            if let Some((backend, key)) = select_transport(&self.transports, member) {
                desired
                    .entry(key)
                    .or_insert_with(|| (backend, HashSet::new()))
                    .1
                    .insert(member.member_id.clone());
            }
        }

        log::debug!(
            "focus grouping: {}",
            desired
                .iter()
                .map(|(key, (_, members))| format!("{key} -> {} member(s)", members.len()))
                .collect::<Vec<_>>()
                .join(", "),
        );

        for (key, entry) in &mut self.pool {
            entry.members = desired.remove(key).map(|(_, m)| m).unwrap_or_default();
            // Any member-set change invalidates pending idle timers; an empty
            // set (re)arms one.
            entry.idle_generation += 1;
            if entry.members.is_empty() && !entry.is_own {
                log::debug!("peer focus {key} has no members left; arming the idle timer");
                let idle_generation = entry.idle_generation;
                let messages = self.messages_tx.clone();
                let key = key.clone();
                rt::spawn(async move {
                    rt::sleep(IDLE_GRACE).await;
                    let _ = messages.send(PoolMessage(Message::CloseIfIdle {
                        connection_key: key,
                        idle_generation,
                    }));
                });
            }
        }

        for (key, (backend, members)) in desired {
            // The own focus is established by the owner and adopted; never
            // race it with a pool-initiated connect.
            if self.own_connection_key.as_deref() == Some(key.as_str()) {
                continue;
            }
            log::info!(
                "connecting to peer focus {key} for {} member(s)",
                members.len(),
            );
            self.pool.insert(
                key.clone(),
                ManagedConnection {
                    backend: Some(backend),
                    members,
                    is_own: false,
                    state: ConnState::Connecting { attempt: 0 },
                    generation: 0,
                    idle_generation: 0,
                },
            );
            self.start_connect(&key, 0);
        }
    }

    /// Spawn a connect attempt for an existing pool entry.
    fn start_connect(&mut self, key: &str, attempt: u32) {
        let Some(entry) = self.pool.get_mut(key) else {
            return;
        };
        let Some(backend) = entry.backend.clone() else {
            return;
        };
        entry.state = ConnState::Connecting { attempt };

        let ctx = self.ctx.clone();
        let messages = self.messages_tx.clone();
        let key = key.to_owned();
        rt::spawn(async move {
            let result = backend.connect(&key, &ctx).await;
            let _ = messages.send(PoolMessage(Message::ConnectFinished {
                connection_key: key,
                attempt,
                result,
            }));
        });
    }

    fn retry_connect(&mut self, key: &str, attempt: u32) {
        let Some(entry) = self.pool.get(key) else {
            return;
        };
        if !matches!(entry.state, ConnState::Backoff { attempt: a } if a == attempt) {
            return;
        }
        if entry.members.is_empty() && !entry.is_own {
            self.pool.remove(key);
            self.clear_degraded(key);
            return;
        }
        self.start_connect(key, attempt);
    }

    fn close_if_idle(&mut self, key: &str, idle_generation: u64) {
        let Some(entry) = self.pool.get(key) else {
            return;
        };
        if entry.idle_generation != idle_generation || !entry.members.is_empty() || entry.is_own {
            return;
        }
        log::debug!("closing idle connection {key}");
        if let Some(entry) = self.pool.remove(key)
            && let ConnState::Up { connection } = entry.state
        {
            rt::spawn(async move {
                if let Err(error) = connection.close().await {
                    log::debug!("closing idle connection failed: {error}");
                }
            });
        }
        self.clear_degraded(key);
    }

    fn connect_finished(&mut self, key: &str, attempt: u32, result: ConnectOutcome) {
        let close_stray = |result: ConnectOutcome| {
            if let Ok((connection, _events)) = result {
                rt::spawn(async move {
                    let _ = connection.close().await;
                });
            }
        };

        let (still_needed, matches_attempt) = match self.pool.get(key) {
            // The group emptied and was removed while connecting.
            None => (false, false),
            Some(entry) => (
                !entry.members.is_empty() || entry.is_own,
                matches!(entry.state, ConnState::Connecting { attempt: a } if a == attempt),
            ),
        };
        if !matches_attempt {
            close_stray(result);
            return;
        }
        if !still_needed {
            self.pool.remove(key);
            self.clear_degraded(key);
            close_stray(result);
            return;
        }

        match result {
            Ok((connection, events)) => {
                self.connection_generation += 1;
                let generation = self.connection_generation;
                let entry = self.pool.get_mut(key).expect("entry checked above");
                entry.generation = generation;
                entry.state = ConnState::Up {
                    connection: Arc::from(connection),
                };
                log::info!("media connection up: {key}");
                self.spawn_forwarder(key.to_owned(), generation, events);
                self.clear_degraded(key);
                self.peer_up(key);
            }
            Err(error) => {
                log::warn!("connecting to {key} failed (attempt {attempt}): {error}");
                self.mark_degraded(key);
                let next = attempt + 1;
                let entry = self.pool.get_mut(key).expect("entry checked above");
                entry.state = ConnState::Backoff { attempt: next };
                self.schedule_retry(key, next, backoff_delay(next));
            }
        }
    }

    fn schedule_retry(&self, key: &str, attempt: u32, delay: Duration) {
        let messages = self.messages_tx.clone();
        let key = key.to_owned();
        rt::spawn(async move {
            rt::sleep(delay).await;
            let _ = messages.send(PoolMessage(Message::RetryConnect {
                connection_key: key,
                attempt,
            }));
        });
    }

    /// A live connection is gone (transport `Closed` event or its event
    /// stream ending). Own focus ⇒ reported lost; peer focus ⇒ tear down its
    /// members' streams and reconnect while still needed.
    fn connection_down(&mut self, key: &str, generation: u64, message: &str) {
        let Some(entry) = self.pool.get(key) else {
            return;
        };
        if entry.generation != generation || !matches!(entry.state, ConnState::Up { .. }) {
            return;
        }

        self.unmapped.retain(|_, on| on != key);
        let is_own = entry.is_own;
        if is_own && self.own.is_none() {
            self.give_up_own(OwnConnectionLoss::Budget, message.to_owned());
            return;
        }
        if is_own {
            log::warn!("own-focus connection {key} is gone ({message}); reconnecting");
        } else {
            log::warn!("peer-focus connection {key} is gone ({message}); reconnecting");
        }
        let members_left = entry.members.is_empty();
        let mut lost: Vec<(String, MediaStreamKind)> = self
            .tracks
            .lock()
            .expect("track map mutex poisoned")
            .iter()
            .filter(|(_, pooled)| pooled.connection_key == key)
            .map(|(stream, _)| stream.clone())
            .collect();
        // The map is a `HashMap`; the events derived from it must come out
        // in a stable order.
        lost.sort();
        for (member_id, kind) in lost {
            self.remove_track(&member_id, kind, key);
        }
        self.mark_degraded(key);

        if is_own {
            // The transport gave up; we have not. At once, then on backoff,
            // within the budget that started at the loss (034 R24).
            self.start_own_loss();
            self.spawn_own_attempt(0);
            return;
        }
        if members_left {
            self.pool.remove(key);
            self.clear_degraded(key);
            return;
        }
        let entry = self.pool.get_mut(key).expect("entry checked above");
        entry.state = ConnState::Backoff { attempt: 0 };
        self.schedule_retry(key, 0, backoff_delay(0));
    }

    fn spawn_forwarder(
        &self,
        connection_key: String,
        generation: u64,
        mut events: mpsc::UnboundedReceiver<ConnectionEvent>,
    ) {
        let forward = self.messages_tx.clone();
        rt::spawn(async move {
            while let Some(event) = events.recv().await {
                if forward
                    .send(PoolMessage(Message::Connection {
                        connection_key: connection_key.clone(),
                        generation,
                        event,
                    }))
                    .is_err()
                {
                    return;
                }
            }
            let _ = forward.send(PoolMessage(Message::ConnectionEnded {
                connection_key,
                generation,
            }));
        });
    }

    /// The members whose media lives on `key`, per the latest snapshot.
    fn members_on_key(&self, key: &str) -> HashSet<String> {
        self.members_snapshot
            .iter()
            .filter(|member| {
                select_transport(&self.transports, member)
                    .is_some_and(|(_, member_key)| member_key == key)
            })
            .map(|member| member.member_id.clone())
            .collect()
    }

    fn handle_connection_event(&mut self, connection_key: &str, event: ConnectionEvent) {
        match event {
            ConnectionEvent::RemoteJoined { identity } => {
                if !self.identity_map.contains_key(&identity) {
                    // Either their membership is still propagating (buffered
                    // media will flush when it lands) or the participant does
                    // not belong to this session. Diagnostics only.
                    log::debug!(
                        "remote participant {identity} on {connection_key} has no known membership"
                    );
                    self.unmapped
                        .insert(identity.clone(), connection_key.to_owned());
                    self.out.push(PoolEvent::UnknownParticipant { identity });
                }
            }
            ConnectionEvent::RemoteLeft { identity } => {
                // Membership is the truth; a transport-level leave on its own
                // changes nothing for a member (tracks get their own events),
                // only for a participant we could not map.
                self.unmapped.remove(&identity);
            }
            ConnectionEvent::TrackAdded {
                identity,
                kind,
                track,
            } => {
                let pooled = PooledTrack {
                    connection_key: connection_key.to_owned(),
                    track,
                };
                match self.identity_map.get(&identity).cloned() {
                    Some(member_id) => self.add_track(&member_id, kind, pooled),
                    None => {
                        log::debug!(
                            "buffering {kind:?} track of unknown identity {identity} on {connection_key}"
                        );
                        self.pending_tracks
                            .entry(identity)
                            .or_default()
                            .push((kind, pooled));
                    }
                }
            }
            ConnectionEvent::TrackRemoved { identity, kind } => {
                match self.identity_map.get(&identity).cloned() {
                    Some(member_id) => self.remove_track(&member_id, kind, connection_key),
                    None => {
                        if let Some(pending) = self.pending_tracks.get_mut(&identity) {
                            pending.retain(|(pending_kind, _)| *pending_kind != kind);
                        }
                    }
                }
            }
            ConnectionEvent::TrackMuted { identity, kind } => self.muted(&identity, kind, true),
            ConnectionEvent::TrackUnmuted { identity, kind } => self.muted(&identity, kind, false),
            ConnectionEvent::ActiveSpeakers { speakers } => {
                let speakers = speakers
                    .into_iter()
                    .filter_map(|speaker| {
                        self.identity_map
                            .get(&speaker.identity)
                            .map(|member_id| ActiveSpeaker {
                                member_id: member_id.clone(),
                                level: speaker.level,
                            })
                    })
                    .collect();
                self.out.push(PoolEvent::ActiveSpeakers { speakers });
            }
            ConnectionEvent::EncryptionStateChanged { identity, state } => {
                match self.identity_map.get(&identity).cloned() {
                    Some(member_id) => self
                        .out
                        .push(PoolEvent::EncryptionStateChanged { member_id, state }),
                    None => {
                        // No membership to attribute it to; `UnknownParticipant`
                        // already covers that case on its own.
                        log::debug!(
                            "encryption state {state:?} for unmapped identity {identity} on {connection_key}"
                        );
                    }
                }
            }
            ConnectionEvent::Reconnecting => {
                self.mark_degraded(connection_key);
                if self.own_connection_key.as_deref() == Some(connection_key) {
                    self.start_own_loss();
                }
            }
            ConnectionEvent::Reconnected => {
                if self.own_connection_key.as_deref() == Some(connection_key) {
                    self.end_own_loss();
                }
                self.clear_degraded(connection_key);
                self.peer_up(connection_key);
                // Subscription settings are server-side connection state; a
                // resumed connection may have lost them.
                self.reapply_connection_constraints(connection_key);
            }
            ConnectionEvent::Closed { message } => {
                let generation = self
                    .pool
                    .get(connection_key)
                    .map(|entry| entry.generation)
                    .unwrap_or_default();
                self.connection_down(connection_key, generation, &message);
            }
        }
    }

    // ---- our own focus (034 R24) ---------------------------------------

    /// Our media stopped being carried: the budget starts, unless a loss is
    /// already running.
    fn start_own_loss(&mut self) {
        let Some(key) = self.own_connection_key.clone() else {
            return;
        };
        let budget = self.own_reconnect_budget;
        let Some(own) = self.own.as_mut() else {
            return;
        };
        if own.lost_since.is_some() {
            return;
        }
        own.lost_since = Some(rt::Instant::now());
        own.epoch += 1;
        let epoch = own.epoch;
        let messages = self.messages_tx.clone();
        rt::spawn(async move {
            rt::sleep(budget).await;
            let _ = messages.send(PoolMessage(Message::OwnBudget { epoch }));
        });
        if self.sfu_fallback {
            let messages = self.messages_tx.clone();
            rt::spawn(async move {
                rt::sleep(FALLBACK_GRACE).await;
                let _ = messages.send(PoolMessage(Message::OwnFallback { epoch }));
            });
        }
        self.mark_degraded(&key);
    }

    /// Our media is carried again. Logged with how long it took: the figure
    /// a fallback grace is tuned from (034 R25).
    fn end_own_loss(&mut self) {
        let Some(own) = self.own.as_mut() else {
            return;
        };
        self.fallback_target = None;
        if let Some(since) = own.lost_since.take() {
            own.epoch += 1;
            own.token_refetched = false;
            own.refetch_next = false;
            own.budget_spent = false;
            log::info!(
                "own focus {} back after {} ms",
                if own.ever_up {
                    "connection"
                } else {
                    "first connect"
                },
                since.elapsed().as_millis(),
            );
        }
    }

    fn spawn_own_attempt(&mut self, attempt: u32) {
        let Some(key) = self.own_connection_key.clone() else {
            return;
        };
        let Some(own) = self.own.as_mut() else {
            return;
        };
        let fresh_token = std::mem::take(&mut own.refetch_next);
        let connection = own.connection.clone();
        if let Some(entry) = self.pool.get_mut(&key) {
            entry.state = ConnState::Connecting { attempt };
        }
        log::info!(
            "connecting our own focus {key} (attempt {attempt}{})",
            if fresh_token { ", fresh token" } else { "" },
        );
        let messages = self.messages_tx.clone();
        rt::spawn(async move {
            let result = connection.reconnect(fresh_token).await;
            let _ = messages.send(PoolMessage(Message::OwnConnectFinished { attempt, result }));
        });
    }

    fn own_connect_finished(
        &mut self,
        attempt: u32,
        result: Result<mpsc::UnboundedReceiver<ConnectionEvent>, TransportError>,
    ) {
        let Some(key) = self.own_connection_key.clone() else {
            return;
        };
        let current = self.pool.get(&key).is_some_and(
            |entry| matches!(entry.state, ConnState::Connecting { attempt: a } if a == attempt),
        );
        let Some(own) = self.own.as_mut() else {
            return;
        };
        if !current {
            if self.closed && result.is_ok() {
                let connection = own.connection.clone();
                rt::spawn(async move {
                    let _ = connection.close().await;
                });
            }
            return;
        }
        match result {
            Ok(events) => {
                own.ever_up = true;
                let connection = own.connection.clone();
                self.connection_generation += 1;
                let generation = self.connection_generation;
                let entry = self.pool.get_mut(&key).expect("entry checked above");
                entry.generation = generation;
                entry.state = ConnState::Up { connection };
                self.spawn_forwarder(key.clone(), generation, events);
                self.end_own_loss();
                self.clear_degraded(&key);
                self.reapply_connection_constraints(&key);
            }
            Err(error) if own.budget_spent => {
                log::warn!("the last attempt at our own focus failed: {error}");
                if let TransportError::TokenRejected(message) | TransportError::Refused(message) =
                    error
                {
                    self.give_up_own(OwnConnectionLoss::Refused, message);
                } else {
                    self.give_up_own_budget();
                }
            }
            Err(TransportError::TokenRejected(message)) if !own.token_refetched => {
                log::warn!("our own focus rejected the token ({message}); fetching a fresh one");
                own.token_refetched = true;
                own.refetch_next = true;
                self.own_backoff(attempt + 1);
            }
            Err(TransportError::TokenRejected(message) | TransportError::Refused(message)) => {
                log::warn!("our own focus refused us: {message}");
                self.give_up_own(OwnConnectionLoss::Refused, message);
            }
            Err(error) => {
                log::warn!("connecting our own focus failed (attempt {attempt}): {error}");
                self.own_backoff(attempt + 1);
            }
        }
    }

    fn own_backoff(&mut self, attempt: u32) {
        let Some(key) = self.own_connection_key.clone() else {
            return;
        };
        if let Some(entry) = self.pool.get_mut(&key) {
            entry.state = ConnState::Backoff { attempt };
        }
        // Never past the budget's end, where the last attempt is made.
        let left = self
            .own
            .as_ref()
            .and_then(|own| own.lost_since)
            .map(|since| self.own_reconnect_budget.saturating_sub(since.elapsed()));
        let delay = backoff_delay(attempt).min(OWN_BACKOFF_MAX);
        let delay = left.map_or(delay, |left| delay.min(left));
        let messages = self.messages_tx.clone();
        rt::spawn(async move {
            rt::sleep(delay).await;
            let _ = messages.send(PoolMessage(Message::OwnRetry { attempt }));
        });
    }

    fn own_retry(&mut self, attempt: u32) {
        // Each step re-checks the fallback: a peer connection coming back
        // mid-outage enables it (034 R25).
        self.try_fallback();
        let due = self.own_connection_key.as_ref().is_some_and(|key| {
            self.pool.get(key).is_some_and(
                |entry| matches!(entry.state, ConnState::Backoff { attempt: a } if a == attempt),
            )
        });
        if due {
            self.spawn_own_attempt(attempt);
        }
    }

    /// The budget ran out: one last attempt, at once unless one is in flight,
    /// decides; it is given [`OWN_FINAL_ATTEMPT_GRACE`] to answer (034 R24).
    fn own_budget(&mut self, epoch: u64) {
        let Some(own) = self.own.as_mut() else {
            return;
        };
        if own.epoch != epoch || own.lost_since.is_none() {
            return;
        }
        own.budget_spent = true;
        let backoff = self.own_connection_key.as_ref().and_then(|key| {
            self.pool.get(key).and_then(|entry| match entry.state {
                ConnState::Backoff { attempt } => Some(attempt),
                _ => None,
            })
        });
        if let Some(attempt) = backoff {
            log::info!("our own focus's budget ran out; one last attempt");
            self.spawn_own_attempt(attempt);
        }
        let messages = self.messages_tx.clone();
        rt::spawn(async move {
            rt::sleep(OWN_FINAL_ATTEMPT_GRACE).await;
            let _ = messages.send(PoolMessage(Message::OwnFinalAttemptExpired { epoch }));
        });
    }

    /// Our own focus not back within the budget.
    fn give_up_own_budget(&mut self) {
        let Some(own) = &self.own else {
            return;
        };
        let reason = if own.ever_up {
            OwnConnectionLoss::Budget
        } else {
            OwnConnectionLoss::NeverConnected
        };
        let message = format!(
            "no connection to our own focus for {} s",
            self.own_reconnect_budget.as_secs()
        );
        self.give_up_own(reason, message);
    }

    /// Our own focus is given up: the call ends (034 R24, R26).
    fn give_up_own(&mut self, reason: OwnConnectionLoss, message: String) {
        let Some(key) = self.own_connection_key.clone() else {
            return;
        };
        log::warn!("giving up our own focus {key} ({reason:?}): {message}");
        self.fallback_target = None;
        self.unmapped.retain(|_, on| *on != key);
        self.pool.remove(&key);
        self.own = None;
        self.own_lost = true;
        self.clear_degraded(&key);
        self.out.push(PoolEvent::ConnectionState {
            connection_key: key,
            state: ConnectionState::Unreachable,
        });
        self.out
            .push(PoolEvent::OwnConnectionLost { reason, message });
    }

    // ---- falling back to a peer's focus (034 R25) ------------------------

    /// Moves our media to the busiest peer focus if our own has been down for
    /// the grace, signalling works (the move republishes our membership) and
    /// a peer connection is up, the evidence that the network does. Its
    /// connection is taken over at once if up, else as soon as it is.
    fn try_fallback(&mut self) {
        if !self.sfu_fallback || !self.signalling_reachable || self.closed {
            return;
        }
        let Some(own_key) = self.own_connection_key.clone() else {
            return;
        };
        let Some(since) = self.own.as_ref().and_then(|own| own.lost_since) else {
            return;
        };
        if since.elapsed() < FALLBACK_GRACE {
            return;
        }
        let up = |key: &String, entry: &ManagedConnection| {
            matches!(entry.state, ConnState::Up { .. }) && !self.degraded_keys.contains(key)
        };
        if !self
            .pool
            .iter()
            .any(|(key, entry)| key != &own_key && up(key, entry))
        {
            log::debug!("own focus {own_key} down, and no peer connection is up: no fallback");
            return;
        }
        // Ties go to the smallest key, so the choice does not depend on the
        // map's order.
        let Some(target) = self
            .pool
            .iter()
            .filter(|(key, entry)| **key != own_key && !entry.members.is_empty())
            .max_by(|(a_key, a), (b_key, b)| {
                a.members
                    .len()
                    .cmp(&b.members.len())
                    .then_with(|| b_key.cmp(a_key))
            })
            .map(|(key, _)| key.clone())
        else {
            return;
        };
        let target_up = self
            .pool
            .get(&target)
            .is_some_and(|entry| up(&target, entry));
        if target_up {
            self.fall_back(&own_key, &target);
        } else if self.fallback_target.as_ref() != Some(&target) {
            log::info!("own focus {own_key} down: falling back to {target} once it is up");
            self.fallback_target = Some(target);
        }
    }

    /// A peer connection is up: the one a fallback waits for, or evidence
    /// that enables one.
    fn peer_up(&mut self, key: &str) {
        if self.own_connection_key.as_deref() != Some(key) {
            self.try_fallback();
        }
    }

    /// Our own handle takes over the up connection to `target`, which becomes
    /// our own focus; the attempt on `own_key` is dropped, and that focus is
    /// connected as a peer's if anyone else publishes there.
    fn fall_back(&mut self, own_key: &str, target: &str) {
        let Some(own) = &self.own else {
            return;
        };
        let Some(transport) = self.transport_for_key(target) else {
            return;
        };
        let handover = match self.pool.get(target).map(|entry| &entry.state) {
            Some(ConnState::Up { connection }) => connection.hand_over(),
            _ => return,
        };
        let taken = match handover {
            Some(handover) => own.connection.take_over(handover),
            None => Err(TransportError::Unsupported(
                "the peer connection cannot be handed over".into(),
            )),
        };
        if let Err(error) = taken {
            log::warn!("cannot fall back to {target}, keeping to our own focus: {error}");
            self.sfu_fallback = false;
            self.fallback_target = None;
            return;
        }
        let connection = own.connection.clone();
        log::info!("our media goes out through {target} now, our own focus {own_key} being down");

        self.pool.remove(own_key);
        self.unmapped.retain(|_, on| on != own_key);
        let entry = self.pool.get_mut(target).expect("checked up above");
        entry.is_own = true;
        entry.backend = None;
        entry.state = ConnState::Up { connection };
        self.own_connection_key = Some(target.to_owned());
        self.end_own_loss();
        self.out.push(PoolEvent::OwnFocusMoved {
            connection_key: target.to_owned(),
            transport,
        });
        self.out.push(PoolEvent::ConnectionState {
            connection_key: target.to_owned(),
            state: ConnectionState::Connected,
        });
        // Whoever else publishes on the old focus is subscribed to as a
        // peer's, still reconnecting until that connection is up.
        self.reconcile_pool();
        if !self.pool.contains_key(own_key) {
            self.clear_degraded(own_key);
        }
    }

    /// The transport a member publishes on that `key` serves.
    fn transport_for_key(&self, key: &str) -> Option<RtcTransport> {
        self.members_snapshot
            .iter()
            .flat_map(|member| &member.transports)
            .find(|transport| {
                self.transports
                    .iter()
                    .any(|backend| backend.connection_key(transport).as_deref() == Some(key))
            })
            .cloned()
    }

    fn muted(&mut self, identity: &str, kind: MediaStreamKind, muted: bool) {
        if let Some(member_id) = self.identity_map.get(identity) {
            self.out.push(PoolEvent::StreamMuted {
                member_id: member_id.clone(),
                kind,
                muted,
            });
        }
    }

    fn mark_degraded(&mut self, key: &str) {
        let was_clear = self.degraded_keys.is_empty();
        if self.degraded_keys.insert(key.to_owned()) {
            if was_clear {
                self.out.push(PoolEvent::Degraded(true));
            }
            self.out.push(PoolEvent::ConnectionState {
                connection_key: key.to_owned(),
                state: ConnectionState::Reconnecting,
            });
        }
    }

    fn clear_degraded(&mut self, key: &str) {
        if self.degraded_keys.remove(key) {
            if self.degraded_keys.is_empty() {
                self.out.push(PoolEvent::Degraded(false));
            }
            self.out.push(PoolEvent::ConnectionState {
                connection_key: key.to_owned(),
                state: ConnectionState::Connected,
            });
        }
    }
}

/// First backend (in preference order) that can serve any of the member's
/// published transports, with the resulting connection key.
fn select_transport(
    transports: &[Arc<dyn MediaTransport>],
    member: &JoinedMembership,
) -> Option<(Arc<dyn MediaTransport>, String)> {
    for backend in transports {
        for transport in &member.transports {
            if let Some(key) = backend.connection_key(transport) {
                return Some((backend.clone(), key));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests;
