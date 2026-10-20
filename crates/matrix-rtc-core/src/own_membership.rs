// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Own membership management for RTC sessions.
//!
//! This module provides the `OwnMembershipMachine` that manages the lifecycle of the
//! current user's own membership in an RTC session, including the dead man's switch
//! keep-alive mechanism.
//!
//! The dead man's switch strategy works as follows:
//! 1. **Schedule delayed leave FIRST** - This is the safety net. If the client dies at any
//!    point, the delayed leave will fire and clean up our membership.
//! 2. **Send join membership** - Send the sticky join event to announce our presence.
//! 3. **Heartbeat** - Periodically restart the delayed leave to extend the timeout.
//!
//! This ensures that if the client crashes or loses connection, the delayed leave will
//! automatically clean up after the timeout period, preventing ghost memberships.
//!
//! # Two clocks, one keep-alive
//!
//! Our membership expires in two independent ways, and
//! [`OwnMembershipMachine::keep_alive`] has to tend both:
//!
//! - The **delayed leave** (`keep_alive_timeout_ms`, seconds) is the dead man's
//!   switch above. Its timer is pushed back out on every tick via MSC4140's
//!   `restart` action — never cancel-and-recreate, which leaves a window with
//!   nothing armed and can leak a delay that then marks us departed mid-call.
//! - The **sticky-map entry** (`sticky_duration_ms`, an hour by default) is how
//!   long the homeserver keeps the membership at all. Re-sent once a third of
//!   it has passed.
//!
//! Tending only the first produces a membership that vanishes mid-call with a
//! perfectly healthy keep-alive; tending only the second leaves a ghost
//! membership behind when the client dies. The slot session's upkeep
//! (`upkeep.rs`) ticks it on an interval while joined.
//!
//! # Through an outage
//!
//! Every send after the join is retried through the client's
//! [`Outbox`](crate::outbox) (034 R1). While they fail, the machine predicts
//! from its own deadlines when the others stop seeing us
//! ([`OwnMembershipMachine::subscribe_lapsed`], R17); when signalling returns
//! it probes the delayed leave, and re-arms and republishes only what was lost
//! ([`OwnMembershipMachine::recover`], R19).
//!
//! # When the homeserver has no delayed events
//!
//! MSC4140 is optional and plenty of homeservers refuse it — matrix.org answers
//! `403 M_FORBIDDEN "Sending delayed events has been disallowed"`. Only the first
//! of the two clocks is lost there, so the call is perfectly joinable: the
//! membership still expires on its own, just on the slower schedule. The machine
//! therefore degrades rather than failing the join, and publishes on
//! [`crate::join::DEFAULT_DEGRADED_LIFETIME_MS`] so a crashed client is forgotten
//! in minutes rather than in an hour. See [`DelayedLeaveSupport`].
//!
//! That choice is made **before the first membership goes out** and never
//! revisited, because a sticky entry cannot be shortened afterwards: MSC4354
//! keeps whichever event expires last, so a refresh carrying a shorter duration
//! loses to the entry already in the map. It is only expressible at all because
//! the delayed leave is armed one step earlier than the membership.

use rand::Rng;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;
// `std::time::SystemTime::now()` panics on wasm32-unknown-unknown; web-time's
// is the same API over `Date.now()`.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use web_time::{SystemTime, UNIX_EPOCH};

use crate::encryption::RtcClock;
use crate::error::{CommandError, JoinError};
use crate::executor;
use crate::host::backend::MatrixBackend;
use crate::host::event::RawStickyEventContent;
use crate::outbox::{Outbox, PendingSend, SendFailure, SendKind, SendPolicy, SlotKey};
use crate::session::{ApplicationInfo, LeaveCode, LeaveReason};
use crate::transport::{MemberTransports, RtcTransport};

/// Default keep-alive timeout in milliseconds (30 seconds).
pub const DEFAULT_KEEP_ALIVE_TIMEOUT_MS: u64 = 30_000;

/// How long to wait before asking a homeserver that just refused a delayed event
/// whether it has changed its mind (5 minutes).
///
/// Only used when the refusal was *unclassified* — a plain send failure that may
/// well have been a network blip. A homeserver that said so in as many words
/// ([`CommandError::is_delayed_events_unsupported`]) is never probed again for
/// the life of the session.
const DELAYED_LEAVE_PROBE_INTERVAL_MS: u64 = 5 * 60 * 1000;

/// The sticky entry is refreshed once `1 / STICKY_REFRESH_FRACTION` of its
/// lifetime has passed (034 R18): a third, so an outage of two thirds of the
/// lifetime, less one backoff step, never lapses it.
const STICKY_REFRESH_FRACTION: u64 = 3;

/// The longest a recovery waits before its first send (034 R22), so the
/// members of one outage do not all hit the returning homeserver at once.
const RECOVERY_JITTER_MAX_MS: u64 = 3_000;

/// Wall-clock milliseconds since the Unix epoch.
///
/// The sticky refresh is decided by comparing timestamps on each
/// [`OwnMembershipMachine::keep_alive`] tick, so it does not depend on how
/// punctually the session's upkeep wakes.
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Converts an RtcTransport to a JSON value for event content.
pub fn transport_to_json(transport: &RtcTransport) -> Value {
    match transport {
        RtcTransport::LiveKit(livekit) => {
            json!({
                "type": "livekit",
                "livekit_service_url": livekit.livekit_service_url
            })
        }
        RtcTransport::Unsupported(unsupported) => {
            let mut obj = json!({
                "type": unsupported.transport_type
            });
            for (key, value) in &unsupported.extra_fields {
                obj[key] = value.clone();
            }
            obj
        }
    }
}

/// State of the own membership machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnMembershipState {
    /// Not joined, no keep-alive active.
    NotJoined,
    /// Join is in progress (join event sent, waiting for confirmation).
    Joining,
    /// Successfully joined with active keep-alive.
    Joined,
    /// Leave is in progress.
    Leaving,
    /// Successfully left, keep-alive canceled.
    Left,
}

/// Information about the active keep-alive delayed event.
#[derive(Debug, Clone)]
pub struct KeepAliveInfo {
    /// The delay id of the delayed cleanup event.
    pub delayed_event_id: String,
    /// The timeout in milliseconds before the event fires, measured from the
    /// last successful restart.
    pub timeout_ms: u64,
    /// Unix ms of the last successful arm or restart.
    ///
    /// Used to decide whether a delay whose restarts keep failing must by now
    /// have fired — the only way to re-arm without risking a leak.
    pub last_restart_ms: u64,
}

/// What this homeserver has told us about MSC4140 delayed events.
///
/// Learned by trying, never by probing a capability endpoint: there is no
/// reliable one, and the arm we need to make anyway is the most honest test
/// there is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelayedLeaveSupport {
    /// Nothing tried yet. Treated as supported, because assuming the worst would
    /// shorten the membership lifetime of every join before it had a reason to.
    Unknown,
    /// A delayed leave was armed successfully at least once.
    Supported,
    /// The last arm failed. `permanent` distinguishes a homeserver that said so
    /// in as many words from one that merely failed; `last_probe_ms` paces the
    /// retries in the latter case.
    Unsupported {
        /// Unix ms of the last arm attempt.
        last_probe_ms: u64,
        /// Whether the failure was classified as "this will never work".
        permanent: bool,
    },
}

impl DelayedLeaveSupport {
    /// Whether a delayed leave is believed to be available, which is what the
    /// membership lifetime turns on.
    fn is_available(&self) -> bool {
        !matches!(self, Self::Unsupported { .. })
    }
}

/// The three clocks a membership runs on.
///
/// One struct rather than three positional `u64`s because they are all
/// milliseconds and all plausible at each other's positions — swapping the
/// sticky lifetime and the delayed-leave timeout compiles, and produces a call
/// that quietly drops out every thirty seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MembershipTimings {
    /// How long the delayed leave waits before firing, restarted on every beat.
    pub keep_alive_timeout_ms: u64,
    /// How long the homeserver keeps our sticky-map entry.
    pub sticky_duration_ms: u64,
    /// The shorter lifetime to use instead once delayed events turn out to be
    /// unavailable.
    pub degraded_lifetime_ms: u64,
}

impl Default for MembershipTimings {
    fn default() -> Self {
        Self {
            keep_alive_timeout_ms: DEFAULT_KEEP_ALIVE_TIMEOUT_MS,
            sticky_duration_ms: crate::join::DEFAULT_STICKY_DURATION_MS,
            degraded_lifetime_ms: crate::join::DEFAULT_DEGRADED_LIFETIME_MS,
        }
    }
}

/// The OwnMembershipMachine manages the lifecycle of our own membership in an RTC session.
///
/// It implements the dead man's switch strategy:
/// 1. Schedule delayed leave membership event (safety net)
/// 2. Send join membership sticky event
/// 3. Heartbeat (every n seconds) -> restarts the delayed leave
///
/// The delayed leave is scheduled FIRST because it's safer - if the client dies at any
/// point, worst case we're cleaning up our membership.
///
/// Every send after the join goes through the client's [`Outbox`] (034 R1): a
/// transient failure is retried with capped backoff rather than waiting for the
/// next beat, sends of one kind never overtake one another, and a leave ends
/// every pending one. While those sends fail the machine predicts when the
/// other members stop seeing us ([`Self::subscribe_lapsed`], R17), and when
/// signalling returns it probes and republishes only what was lost
/// ([`Self::recover`], R19).
pub struct OwnMembershipMachine<T: MatrixBackend> {
    /// Reference to the backend for sending events.
    backend: Arc<T>,
    /// Room ID for the session.
    room_id: String,
    /// Slot ID for the session.
    slot_id: String,
    /// Our `member.id`, which doubles as the sticky key (MSC4143).
    sticky_key: String,
    /// `content.application` of our join (MSC4143), e.g. `{"type": "m.call"}`.
    application: ApplicationInfo,
    /// Current state of the membership machine. Also the gate every retried
    /// send registers through, so a leave and a send cannot interleave.
    state: Arc<Mutex<OwnMembershipState>>,
    /// Information about the active delayed event, if any.
    keep_alive_info: Arc<Mutex<Option<KeepAliveInfo>>>,
    /// The keep-alive timeout in milliseconds.
    keep_alive_timeout_ms: u64,
    /// How long the homeserver keeps our sticky-map entry.
    sticky_duration_ms: u64,
    /// The shorter lifetime to use instead once delayed events turn out to be
    /// unavailable.
    degraded_lifetime_ms: u64,
    /// What we know about this homeserver's delayed-event support.
    delayed_support: Arc<Mutex<DelayedLeaveSupport>>,
    /// The lifetime every membership event of this join is published with,
    /// chosen once by [`Self::join`] and never moved.
    ///
    /// Fixed rather than tracking `delayed_support`, because in the sticky
    /// dialect a lifetime cannot be shortened after the fact. MSC4354 resolves
    /// two events with one `sticky_key` by *last to expire*, so a refresh
    /// carrying a shorter duration is simply ignored in favour of the longer
    /// entry already in the map — "there is no mechanism for sticky events to
    /// expire earlier than their timeout value" — and the MSC asks clients to
    /// reuse the same duration for a key for exactly that reason. So the choice
    /// has to be made before the first publish, which is possible because the
    /// delayed leave is armed one step earlier.
    published_lifetime_ms: AtomicU64,
    /// The join content and when we last sent it, so the heartbeat can re-send
    /// it before the sticky entry expires. `None` until we join.
    last_sticky: Arc<Mutex<Option<SentSticky>>>,
    /// The event id of the member event currently representing us in the
    /// sticky map: the join's, then each refresh's. `None` until we join and
    /// again once we leave. A watch, so an application can follow it
    /// ([`Self::subscribe_membership_event_id`]).
    latest_event_id: watch::Sender<Option<String>>,
    /// Where every send after the join is retried; the client's, so one
    /// homeserver's health covers every room.
    outbox: Outbox,
    /// `room/slot/member`: this join's slots in the outbox, all cancelled by
    /// its leave.
    scope: String,
    /// Whether the other members no longer see us, predicted from our own
    /// deadlines (R17); see [`Self::lapse_at_ms`].
    lapsed: watch::Sender<bool>,
    /// Set by a recovery whose delayed leave survived, until the next complete
    /// membership set shows whether our membership did too (R19).
    awaiting_catch_up: AtomicBool,
    /// Bumped to ask the session's upkeep for a republish, which it runs
    /// alongside its other sends.
    republish_requested: watch::Sender<u64>,
    /// A refresh is retrying; the beat does not start another.
    refreshing: AtomicBool,
    /// A delayed leave is being armed; the beat does not start another.
    arming: AtomicBool,
    /// How long a recovery waits before its first send (R22).
    recovery_jitter: Arc<dyn Fn() -> Duration + Send + Sync>,
    /// Where "now" comes from (see [`Self::set_clock`]).
    clock: RtcClock,
}

/// The membership event we last put in the sticky map, and when.
#[derive(Debug, Clone)]
struct SentSticky {
    /// The join content, re-sent verbatim to refresh the entry.
    content: Value,
    /// Unix ms at which it was accepted by the server.
    sent_at_ms: u64,
}

/// What a complete membership set held under our sticky key, for the
/// catch-up comparison after a recovery (R19).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnEntry {
    /// Nothing: our membership is gone.
    Absent,
    /// Our membership, with its event id when the host reported one.
    Present(Option<String>),
}

/// Clears a flag when dropped, so an abandoned send never leaves it set.
struct Busy<'a>(&'a AtomicBool);

impl Drop for Busy<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

impl<'a> Busy<'a> {
    /// Takes the flag, or `None` if it is already taken.
    fn take(flag: &'a AtomicBool) -> Option<Self> {
        (!flag.swap(true, Ordering::SeqCst)).then_some(Self(flag))
    }
}

impl<T: MatrixBackend + 'static> OwnMembershipMachine<T> {
    /// Creates a new own membership machine.
    ///
    /// # Arguments
    ///
    /// * `backend` - The backend for sending events
    /// * `room_id` - The room ID for the session
    /// * `slot_id` - The slot ID for the session
    /// * `sticky_key` - Our `member.id`, which doubles as the sticky key
    /// * `application` - `content.application`: the type (e.g. "m.call") plus any
    ///   application-defined properties
    /// * `timings` - The three lifetimes the membership runs on
    ///
    /// The machine retries through an outbox of its own until
    /// [`Self::set_outbox`] hands it the client's.
    pub fn new(
        backend: Arc<T>,
        room_id: String,
        slot_id: String,
        sticky_key: String,
        application: impl Into<ApplicationInfo>,
        timings: MembershipTimings,
    ) -> Self {
        let MembershipTimings {
            keep_alive_timeout_ms,
            sticky_duration_ms,
            degraded_lifetime_ms,
        } = timings;
        let scope = format!("{room_id}/{slot_id}/{sticky_key}");
        Self {
            backend,
            room_id,
            slot_id,
            sticky_key,
            application: application.into(),
            state: Arc::new(Mutex::new(OwnMembershipState::NotJoined)),
            keep_alive_info: Arc::new(Mutex::new(None)),
            keep_alive_timeout_ms,
            sticky_duration_ms,
            degraded_lifetime_ms,
            delayed_support: Arc::new(Mutex::new(DelayedLeaveSupport::Unknown)),
            published_lifetime_ms: AtomicU64::new(sticky_duration_ms),
            last_sticky: Arc::new(Mutex::new(None)),
            latest_event_id: watch::Sender::new(None),
            outbox: Outbox::new(),
            scope,
            lapsed: watch::Sender::new(false),
            awaiting_catch_up: AtomicBool::new(false),
            republish_requested: watch::Sender::new(0),
            refreshing: AtomicBool::new(false),
            arming: AtomicBool::new(false),
            recovery_jitter: Arc::new(|| {
                Duration::from_millis(rand::thread_rng().gen_range(0..=RECOVERY_JITTER_MAX_MS))
            }),
            clock: Arc::new(now_ms),
        }
    }

    /// Replaces the source of "now", in epoch milliseconds (see [`RtcClock`]).
    ///
    /// Every deadline the machine keeps — the delayed leave's, the sticky
    /// refresh's, the probe interval — is a comparison against this, so a test
    /// drives a clock it advances itself instead of waiting in real time.
    pub fn set_clock(&mut self, clock: RtcClock) {
        self.clock = clock;
    }

    /// Retries through `outbox`, the client's, so its health covers this
    /// membership's sends, and keeps time by its clock: one "now" per client.
    pub fn set_outbox(&mut self, outbox: Outbox) {
        self.clock = outbox.clock();
        self.outbox = outbox;
    }

    /// Replaces the random wait before a recovery (R22), for tests.
    pub fn set_recovery_jitter(&mut self, jitter: impl Fn() -> Duration + Send + Sync + 'static) {
        self.recovery_jitter = Arc::new(jitter);
    }

    pub(crate) fn now_ms(&self) -> u64 {
        (self.clock)()
    }

    /// The outbox this machine sends through.
    pub fn outbox(&self) -> &Outbox {
        &self.outbox
    }

    /// The outbox scope of this participation's sends, `room/slot/member`:
    /// the leave cancels everything under it.
    pub(crate) fn scope(&self) -> &str {
        &self.scope
    }

    /// Creates a new own membership machine with the default keep-alive timeout.
    pub fn with_default_timeout(
        backend: Arc<T>,
        room_id: String,
        slot_id: String,
        sticky_key: String,
        application: impl Into<ApplicationInfo>,
    ) -> Self {
        Self::new(
            backend,
            room_id,
            slot_id,
            sticky_key,
            application,
            MembershipTimings::default(),
        )
    }

    /// Gets the current state of the membership machine.
    pub fn state(&self) -> OwnMembershipState {
        self.state.lock().unwrap().clone()
    }

    /// Gets the sticky key (membership ID) for our membership.
    pub fn sticky_key(&self) -> &str {
        &self.sticky_key
    }

    /// Gets the room ID.
    pub fn room_id(&self) -> &str {
        &self.room_id
    }

    /// Gets the slot ID.
    pub fn slot_id(&self) -> &str {
        &self.slot_id
    }

    /// What this homeserver has told us about MSC4140 delayed events.
    pub fn delayed_leave_support(&self) -> DelayedLeaveSupport {
        *self.delayed_support.lock().unwrap()
    }

    /// Whether a dead man's switch is protecting this membership.
    ///
    /// `false` means the homeserver refused to arm one and the membership is
    /// being kept alive by its lifetime alone — still a working call, with a
    /// slower cleanup if this client dies.
    pub fn delayed_leave_supported(&self) -> bool {
        self.delayed_leave_support().is_available()
    }

    /// How long every membership event of this join lives.
    ///
    /// The sticky duration, or the degraded one if the homeserver had already
    /// refused a delayed leave by the time we published. Settled once, at join;
    /// see [`Self::published_lifetime_ms`] for why it cannot move afterwards.
    pub fn membership_lifetime_ms(&self) -> u64 {
        self.published_lifetime_ms.load(Ordering::Relaxed)
    }

    /// The event id of the member event currently representing us in the
    /// sticky map, or `None` while not joined.
    ///
    /// Moves on every sticky refresh; read it when needed rather than caching
    /// it.
    pub fn membership_event_id(&self) -> Option<String> {
        self.latest_event_id.borrow().clone()
    }

    /// Follows [`Self::membership_event_id`]; wakes on every move.
    pub fn subscribe_membership_event_id(&self) -> watch::Receiver<Option<String>> {
        self.latest_event_id.subscribe()
    }

    /// Gets the delayed event ID, if one is active.
    pub fn delayed_event_id(&self) -> Option<String> {
        self.keep_alive_info
            .lock()
            .unwrap()
            .as_ref()
            .map(|info| info.delayed_event_id.clone())
    }

    /// When the other members stop seeing us if nothing more lands, in epoch
    /// ms: the earlier of the delayed leave firing and the sticky entry
    /// expiring, each moved forward by its own successful send. `None` while
    /// not joined.
    pub fn lapse_at_ms(&self) -> Option<u64> {
        if self.state() != OwnMembershipState::Joined {
            return None;
        }
        let delayed = self
            .keep_alive_info
            .lock()
            .unwrap()
            .as_ref()
            .map(|info| info.last_restart_ms.saturating_add(info.timeout_ms));
        let sticky = self.last_sticky.lock().unwrap().as_ref().map(|sticky| {
            sticky
                .sent_at_ms
                .saturating_add(self.membership_lifetime_ms())
        });
        match (delayed, sticky) {
            (Some(delayed), Some(sticky)) => Some(delayed.min(sticky)),
            (delayed, sticky) => delayed.or(sticky),
        }
    }

    /// Whether our membership has lapsed for the other members (R17).
    pub fn is_lapsed(&self) -> bool {
        *self.lapsed.borrow()
    }

    /// Follows [`Self::is_lapsed`], current value first.
    pub fn subscribe_lapsed(&self) -> watch::Receiver<bool> {
        self.lapsed.subscribe()
    }

    /// Re-reads [`Self::is_lapsed`] against the clock and the deadlines.
    pub(crate) fn update_lapsed(&self) {
        let lapsed = self
            .lapse_at_ms()
            .is_some_and(|lapse_at| self.now_ms() >= lapse_at);
        self.lapsed.send_if_modified(|current| {
            if *current == lapsed {
                return false;
            }
            if lapsed {
                log::warn!(
                    "[{}] our membership has lapsed for the other members",
                    self.room_id
                );
            } else {
                log::info!("[{}] our membership is live again", self.room_id);
            }
            *current = lapsed;
            true
        });
    }

    /// Asks for a republish from whoever runs [`Self::republish`]: the
    /// session's upkeep.
    pub(crate) fn subscribe_republish_requests(&self) -> watch::Receiver<u64> {
        self.republish_requested.subscribe()
    }

    fn request_republish(&self) {
        self.republish_requested
            .send_modify(|n| *n = n.wrapping_add(1));
    }

    fn key(&self, kind: SendKind) -> SlotKey {
        SlotKey::new(self.scope.clone(), kind)
    }

    /// Takes `kind`'s slot if still joined. Under the state lock, so a leave
    /// either sees the send and cancels it, or the send sees the leave.
    fn begin_if_joined(&self, kind: SendKind) -> Option<PendingSend> {
        let state = self.state.lock().unwrap();
        (*state == OwnMembershipState::Joined).then(|| self.outbox.begin(self.key(kind)))
    }

    /// One attempt at putting `content` in the sticky map.
    fn publish_once(
        &self,
        content: Value,
    ) -> impl std::future::Future<Output = Result<String, CommandError>> + '_ {
        self.backend.send_sticky_event(
            self.room_id.clone(),
            "m.rtc.member".to_string(),
            content,
            self.membership_lifetime_ms(),
        )
    }

    /// Records a membership send that landed: the event now representing us,
    /// and a sticky clock restarted if `content` is still what we track.
    fn record_published(&self, content: &Value, event_id: String) {
        self.latest_event_id.send_replace(Some(event_id));
        {
            let mut guard = self.last_sticky.lock().unwrap();
            if let Some(current) = guard.as_mut()
                && current.content == *content
            {
                current.sent_at_ms = self.now_ms();
            }
        }
        self.update_lapsed();
    }

    /// Joins the session by implementing the dead man's switch strategy.
    ///
    /// 1. Schedules a delayed leave FIRST (the safety net).
    /// 2. Sends the join membership event.
    /// 3. The upkeep restarts the delayed leave from then on.
    ///
    /// Both sends go through the outbox and the whole join runs under one
    /// budget (034 R6): `deadline`, or [`DEFAULT_JOIN_BUDGET_MS`] from now.
    ///
    /// A homeserver that refuses step 1 does not fail the join. MSC4140 is
    /// optional, and losing it costs only the *speed* of the cleanup — the
    /// membership still expires on its own. Step 1 is one attempt, given at most
    /// half the budget so step 2 keeps time; when it fails the machine degrades
    /// (see [`DelayedLeaveSupport`]) and the membership goes out with the shorter
    /// [`Self::membership_lifetime_ms`] instead. Step 2 is retried on transient
    /// failures until the deadline: a refusal fails the join at once
    /// ([`JoinError::Refused`]), the deadline fails it with
    /// [`JoinError::TimedOut`], and a join that fails after arming its delayed
    /// leave cancels it with one detached attempt.
    ///
    /// # Returns
    ///
    /// The event id of the membership event if the join was sent successfully;
    /// an error if it was not. The caller needs the id to relate an MSC4075
    /// notification to the membership that justifies it.
    ///
    /// [`DEFAULT_JOIN_BUDGET_MS`]: crate::join::DEFAULT_JOIN_BUDGET_MS
    pub async fn join(
        &self,
        transports: MemberTransports,
        deadline: Option<executor::Instant>,
    ) -> Result<String, JoinError> {
        let room_id = self.room_id.clone();
        let keep_alive_timeout_ms = self.keep_alive_timeout_ms;
        let deadline = deadline.unwrap_or_else(|| {
            executor::Instant::now() + Duration::from_millis(crate::join::DEFAULT_JOIN_BUDGET_MS)
        });
        let remaining = || deadline.saturating_duration_since(executor::Instant::now());

        *self.state.lock().unwrap() = OwnMembershipState::Joining;

        // Step 1: the delayed leave FIRST (safety net). If the client dies at
        // any point from here, the worst case is cleaning up.
        let delayed_content = self.build_delayed_leave_content(&self.slot_id, &self.sticky_key);
        log::info!(
            "[{}] Scheduling delayed leave event (dead man's switch safety net)",
            room_id
        );
        let armed = self
            .outbox
            .send(
                self.key(SendKind::DelayedLeave),
                SendPolicy::single_attempt().deadline(remaining() / 2),
                || {
                    self.backend.send_delayed_event(
                        room_id.clone(),
                        "m.rtc.member".to_string(),
                        None,
                        delayed_content.clone(),
                        keep_alive_timeout_ms,
                    )
                },
            )
            .await;
        match armed {
            // Store the delayed event ID for later cancellation
            Ok(delayed_event_id) => {
                *self.delayed_support.lock().unwrap() = DelayedLeaveSupport::Supported;
                *self.keep_alive_info.lock().unwrap() = Some(KeepAliveInfo {
                    delayed_event_id,
                    timeout_ms: keep_alive_timeout_ms,
                    last_restart_ms: self.now_ms(),
                });
            }
            // Not fatal: the dead man's switch is a cleanup optimisation, not a
            // precondition for being in the call. Join without it, and let the
            // membership's own lifetime do the cleanup instead — shortened, so a
            // client that dies is forgotten in minutes rather than in an hour.
            //
            // Here, before the first publish, is the only place that shortening
            // can happen; see `published_lifetime_ms`.
            Err(failure) => {
                let error = failure.into_command_error();
                self.mark_delayed_leave_unsupported(&error);
                self.published_lifetime_ms
                    .store(self.degraded_lifetime_ms, Ordering::Relaxed);
                log::warn!(
                    "[{room_id}] no dead man's switch: the delayed leave could not be scheduled \
                     ({error}). The homeserver may not support MSC4140; joining anyway, with the \
                     membership kept alive by its lifetime alone ({}ms instead of {}ms).",
                    self.degraded_lifetime_ms,
                    self.sticky_duration_ms,
                );
            }
        }

        // Step 2: the join membership event.
        let join_content = self.build_join_content(
            &self.slot_id,
            &self.sticky_key,
            self.application.clone(),
            transports,
        );
        log::info!(
            "[{}] Sending join membership event (step 2 of dead man's switch)",
            room_id
        );
        let published = self
            .outbox
            .send(
                self.key(SendKind::Membership),
                SendPolicy::new().deadline(remaining()),
                || self.publish_once(join_content.clone()),
            )
            .await;
        let event_id = match published {
            Ok(event_id) => event_id,
            Err(failure) => {
                // Back to NotJoined, not left at `Joining`: we never made it
                // into the call, and a state that says otherwise would have
                // the beat arming delayed leaves for a membership that does
                // not exist.
                *self.state.lock().unwrap() = OwnMembershipState::NotJoined;
                log::warn!(
                    "[{room_id}] join aborted at step 2: the join membership event was not \
                     sent ({failure})",
                );
                self.cancel_armed_delayed_leave();
                return Err(JoinError::from_send_failure(failure));
            }
        };

        // Remember it so the heartbeat can refresh the sticky entry before the
        // server expires it. Recorded only on success: a failed send left
        // nothing in the map, so there is nothing to refresh.
        *self.last_sticky.lock().unwrap() = Some(SentSticky {
            content: join_content,
            sent_at_ms: self.now_ms(),
        });
        self.latest_event_id.send_replace(Some(event_id.clone()));
        *self.state.lock().unwrap() = OwnMembershipState::Joined;

        log::info!(
            "[{}] Successfully joined with dead man's switch armed",
            room_id
        );

        Ok(event_id)
    }

    /// A join that failed after arming its delayed leave cancels it (R6), with
    /// one attempt that the failed join does not wait for. Where nothing can
    /// spawn it, the delay fires its leave for a membership that never went
    /// out, which nobody sees.
    fn cancel_armed_delayed_leave(&self) {
        let Some(info) = self.keep_alive_info.lock().unwrap().take() else {
            return;
        };
        if !executor::can_spawn() {
            return;
        }
        let backend = self.backend.clone();
        let outbox = self.outbox.clone();
        let key = self.key(SendKind::DelayedLeave);
        let room_id = self.room_id.clone();
        executor::spawn(async move {
            let cancelled = outbox
                .send(key, SendPolicy::single_attempt(), || {
                    backend.cancel_delayed_event(room_id.clone(), info.delayed_event_id.clone())
                })
                .await;
            if let Err(failure) = cancelled {
                log::debug!(
                    "[{room_id}] the failed join's delayed leave was not cancelled ({failure}); \
                     it fires for a membership that never went out",
                );
            }
        });
    }

    /// Records that an attempt to arm a delayed leave failed.
    ///
    /// A homeserver that named the reason ([`CommandError::DelayedEventsNotSupported`])
    /// is taken at its word and never asked again; anything else may have been a
    /// blip, so the heartbeat re-probes on
    /// [`DELAYED_LEAVE_PROBE_INTERVAL_MS`].
    fn mark_delayed_leave_unsupported(&self, error: &CommandError) {
        *self.delayed_support.lock().unwrap() = DelayedLeaveSupport::Unsupported {
            last_probe_ms: self.now_ms(),
            permanent: error.is_delayed_events_unsupported(),
        };
    }

    /// Builds MSC4143-compliant content for a delayed leave event (dead man's switch).
    ///
    /// The homeserver sends this on our behalf when we stop heartbeating, which is
    /// exactly what MSC4143's `delayed_leave` code describes.
    fn build_delayed_leave_content(&self, slot_id: &str, sticky_key: &str) -> Value {
        let leave_reason = LeaveReason::with_reason(
            LeaveCode::DelayedLeave,
            "Dead man's switch: client failed to heartbeat",
        );
        let content = RawStickyEventContent::for_leave(
            slot_id.to_string(),
            sticky_key.to_string(),
            Some(leave_reason),
        );
        serde_json::to_value(content).expect("m.rtc.member content is always serializable")
    }

    /// Builds MSC4143-compliant content for a join membership event.
    ///
    fn build_join_content(
        &self,
        slot_id: &str,
        sticky_key: &str,
        application: ApplicationInfo,
        transports: MemberTransports,
    ) -> Value {
        let content = RawStickyEventContent::for_join(
            slot_id.to_string(),
            sticky_key.to_string(),
            application,
            transports,
        );
        serde_json::to_value(content).expect("m.rtc.member content is always serializable")
    }

    /// Leaves the session: our leave membership, then the delayed leave
    /// cancelled.
    ///
    /// Every pending send of this join is cancelled first, in the same
    /// critical section that stops new ones registering, so nothing this join
    /// retries can land after the leave. Each of the two sends is one attempt
    /// (R8); the leave's waits for a refresh already at the host, so it cannot
    /// be overtaken by it.
    pub async fn leave(&self, leave_reason: Option<LeaveReason>) -> Result<(), CommandError> {
        let room_id = self.room_id.clone();

        // A voluntary leave with no stated cause is MSC4143's plain `leave` code.
        let leave_reason = Some(leave_reason.unwrap_or_else(|| LeaveReason::new(LeaveCode::Leave)));
        let leave_content = serde_json::to_value(RawStickyEventContent::for_leave(
            self.slot_id.clone(),
            self.sticky_key.clone(),
            leave_reason,
        ))
        .expect("m.rtc.member content is always serializable");

        self.begin_leave();

        log::info!("[{}] Sending leave membership event", room_id);

        self.outbox
            .send(
                self.key(SendKind::Membership),
                SendPolicy::single_attempt(),
                || self.publish_once(leave_content.clone()),
            )
            .await
            .map_err(SendFailure::into_command_error)?;

        // We are gone from the call; stop refreshing the sticky entry.
        {
            let mut guard = self.last_sticky.lock().unwrap();
            *guard = None;
        }
        self.latest_event_id.send_replace(None);

        // Cancel the delayed leave event if one exists
        if let Some(event_id) = self.delayed_event_id() {
            log::debug!("[{}] Canceling delayed leave event: {}", room_id, event_id);
            // Deliberately not propagated. The leave event above already went
            // through, so we *have* left; the cancellation is only tidying up a
            // safety net that is now redundant. The common failure is a 404
            // because the delay already fired — which is the outcome we wanted
            // anyway. Failing the whole leave here would leave the machine
            // stuck in `Leaving` after a successful leave.
            match self
                .outbox
                .send(
                    self.key(SendKind::DelayedLeave),
                    SendPolicy::single_attempt(),
                    || {
                        self.backend
                            .cancel_delayed_event(room_id.clone(), event_id.clone())
                    },
                )
                .await
            {
                Ok(()) => log::debug!("[{}] Delayed leave event canceled", room_id),
                Err(error) => log::debug!(
                    "[{room_id}] Delayed leave {event_id} could not be canceled ({error}); \
                     it has most likely already fired, which leaves us departed either way.",
                ),
            }

            // Clear the stored event ID regardless: either it is canceled, or
            // it fired and no longer exists.
            {
                let mut info_guard = self.keep_alive_info.lock().unwrap();
                *info_guard = None;
            }
        }

        // Transition to Left state
        {
            let mut state_guard = self.state.lock().unwrap();
            *state_guard = OwnMembershipState::Left;
        }

        log::info!("[{}] Successfully left session", room_id);

        Ok(())
    }

    /// Stops this join sending: `Leaving` from now, and every pending send of
    /// the join cancelled in the same critical section, so none registers
    /// after. Idempotent; [`Self::leave`] does it first.
    pub fn begin_leave(&self) {
        {
            let mut state_guard = self.state.lock().unwrap();
            if matches!(
                *state_guard,
                OwnMembershipState::Leaving | OwnMembershipState::Left
            ) {
                return;
            }
            *state_guard = OwnMembershipState::Leaving;
            self.outbox.cancel_scope(&self.scope);
        }
        self.update_lapsed();
    }

    /// One keep-alive beat, run to completion: the sticky refresh if due, then
    /// the delayed leave's restart, each retried until it lands, is superseded
    /// or the join leaves.
    ///
    /// For a host that ticks the session itself. The session's upkeep runs the
    /// same two sends concurrently instead ([`Self::refresh_if_due`],
    /// [`Self::restart_delayed_leave`]), so a refresh retrying through an
    /// outage never holds back the restart. A no-op unless joined.
    pub async fn keep_alive(&self) {
        if self.state() != OwnMembershipState::Joined {
            return;
        }
        log::trace!("[{}] restarting keep-alive", self.room_id);
        self.refresh_if_due().await;
        self.restart_delayed_leave().await;
        self.update_lapsed();
    }

    /// Pushes the delayed leave's timer back out, or arms one if none is.
    ///
    /// Uses MSC4140's `restart` action rather than cancel-then-reschedule. That
    /// matters for three reasons: it is one request instead of two; there is
    /// never a moment with no delayed leave armed; and it cannot leak a delay.
    /// A leaked delay is not benign — nobody restarts it, so it fires, and
    /// because the sticky map resolves conflicts by *last to expire* (MSC4354),
    /// its leave out-expires our live membership and marks us departed while we
    /// are still in the call.
    ///
    /// Retried until it lands; each beat's restart supersedes the previous
    /// one's. A restart answered `M_NOT_FOUND` means the delay is gone — it
    /// fired, so the homeserver sent our leave — and the machine re-arms and
    /// republishes (R19). A host that reports no error code gets the same from
    /// the clock: once a whole delay period has passed since the last restart
    /// that landed, the delay cannot still be pending.
    pub async fn restart_delayed_leave(&self) {
        self.restart(true).await;
    }

    /// Whether a whole delay period has passed since the last restart that
    /// landed, so the delayed leave cannot still be pending.
    pub(crate) fn delayed_leave_overdue(&self) -> bool {
        self.keep_alive_info
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|info| {
                self.now_ms() > info.last_restart_ms.saturating_add(info.timeout_ms)
            })
    }

    async fn restart(&self, clock_fallback: bool) -> bool {
        let Some(info) = self.keep_alive_info.lock().unwrap().clone() else {
            self.arm_if_due().await;
            return false;
        };
        let room_id = &self.room_id;
        let deadline_ms = info.last_restart_ms.saturating_add(info.timeout_ms);
        if clock_fallback && self.now_ms() > deadline_ms {
            log::warn!(
                "[{room_id}] Delayed leave {} has not been restarted for longer than its {}ms \
                 delay, so it has already fired; arming a replacement and republishing.",
                info.delayed_event_id,
                info.timeout_ms,
            );
            self.rearm_and_republish().await;
            return false;
        }
        let Some(pending) = self.begin_if_joined(SendKind::DelayedLeave) else {
            return false;
        };
        let tolerance = Duration::from_millis(deadline_ms.saturating_sub(self.now_ms()));
        match pending
            .run(SendPolicy::new().tolerance(tolerance), || {
                self.backend
                    .restart_delayed_event(room_id.clone(), info.delayed_event_id.clone())
            })
            .await
        {
            Ok(()) => {
                {
                    let mut guard = self.keep_alive_info.lock().unwrap();
                    if let Some(current) = guard.as_mut()
                        && current.delayed_event_id == info.delayed_event_id
                    {
                        current.last_restart_ms = self.now_ms();
                    }
                }
                self.update_lapsed();
                true
            }
            Err(SendFailure::Refused(error)) if is_not_found(&error) => {
                log::warn!(
                    "[{room_id}] Delayed leave {} is gone ({error}): it fired; arming a \
                     replacement and republishing.",
                    info.delayed_event_id,
                );
                self.rearm_and_republish().await;
                false
            }
            Err(SendFailure::Superseded | SendFailure::Cancelled) => false,
            Err(failure) => {
                // Kept armed: the clock decides when it must have fired.
                log::warn!(
                    "[{room_id}] Failed to restart delayed leave {}: {failure}",
                    info.delayed_event_id,
                );
                false
            }
        }
    }

    /// Arms a delayed leave where none is, at the probe pace: a homeserver that
    /// refused in as many words is never asked again, one that merely failed is
    /// asked every [`DELAYED_LEAVE_PROBE_INTERVAL_MS`].
    async fn arm_if_due(&self) {
        if !self.delayed_leave_probe_due() {
            return;
        }
        let Some(_arming) = Busy::take(&self.arming) else {
            return;
        };
        let was_degraded = !self.delayed_leave_supported();
        match self.arm(SendPolicy::single_attempt()).await {
            // The membership lifetime stays where the join left it. It is the
            // safe direction to be wrong in — a shorter lifetime than we now
            // need costs signalling, where a longer one would cost a ghost —
            // and MSC4354 would ignore a change of duration mid-key anyway.
            Ok(()) if was_degraded => log::info!(
                "[{}] the homeserver accepts delayed events after all; the dead man's switch \
                 is armed again",
                self.room_id,
            ),
            Ok(()) => {}
            Err(SendFailure::Superseded | SendFailure::Cancelled) => {}
            Err(failure) => {
                let error = failure.into_command_error();
                self.mark_delayed_leave_unsupported(&error);
                log::warn!("[{}] Failed to arm a delayed leave: {error}", self.room_id);
            }
        }
    }

    /// Our delayed leave is gone: arm a new one, retried until it lands, then
    /// put the same join content back (R19), so the member id is the one it
    /// always was.
    async fn rearm_and_republish(&self) {
        let Some(_arming) = Busy::take(&self.arming) else {
            return;
        };
        *self.keep_alive_info.lock().unwrap() = None;
        match self.arm(SendPolicy::new()).await {
            Ok(()) => {}
            Err(SendFailure::Superseded | SendFailure::Cancelled) => return,
            Err(failure) => {
                // Refused: without a dead man's switch, the lifetime cleans up.
                let error = failure.into_command_error();
                self.mark_delayed_leave_unsupported(&error);
                log::warn!(
                    "[{}] Failed to arm a replacement delayed leave: {error}",
                    self.room_id
                );
            }
        }
        self.republish().await;
    }

    /// Re-sends our join content, retried until it lands, is superseded or the
    /// join leaves. Same content, so the same member id.
    pub async fn republish(&self) {
        let Some(sticky) = self.last_sticky.lock().unwrap().clone() else {
            return;
        };
        let Some(pending) = self.begin_if_joined(SendKind::Membership) else {
            return;
        };
        log::info!("[{}] republishing our membership", self.room_id);
        match pending
            .run(SendPolicy::new(), || {
                self.publish_once(sticky.content.clone())
            })
            .await
        {
            Ok(event_id) => self.record_published(&sticky.content, event_id),
            Err(SendFailure::Superseded | SendFailure::Cancelled) => {}
            Err(failure) => log::warn!(
                "[{}] Failed to republish our membership: {failure}",
                self.room_id
            ),
        }
    }

    /// Re-sends our membership event once a third of its lifetime has passed,
    /// keeping us in the map for another full lifetime (R18).
    ///
    /// A third rather than at the brink so an outage of up to two thirds of the
    /// lifetime, less one backoff step, never lapses us: the refresh is retried
    /// on capped backoff from the moment it falls due until it lands, the join
    /// leaves, or the entry lapses. Content is re-sent verbatim, so peers see
    /// an update identical to what they already hold. One refresh at a time.
    pub async fn refresh_if_due(&self) {
        let Some(sticky) = self.last_sticky.lock().unwrap().clone() else {
            // Not joined (or the join's sticky send failed): nothing to refresh.
            return;
        };

        // The lifetime the join settled on, which is both when the refresh falls
        // due and what it re-publishes — a refresh that stated a different
        // duration is what MSC4354 asks clients not to do.
        let lifetime_ms = self.membership_lifetime_ms();
        let elapsed = self.now_ms().saturating_sub(sticky.sent_at_ms);
        if elapsed < lifetime_ms / STICKY_REFRESH_FRACTION {
            return;
        }
        let Some(_refreshing) = Busy::take(&self.refreshing) else {
            return;
        };
        let Some(pending) = self.begin_if_joined(SendKind::Membership) else {
            return;
        };

        let room_id = &self.room_id;
        log::debug!(
            "[{room_id}] Refreshing sticky membership ({elapsed}ms of {lifetime_ms}ms elapsed)"
        );
        let tolerance = Duration::from_millis(lifetime_ms.saturating_sub(elapsed));
        match pending
            .run(SendPolicy::new().tolerance(tolerance), || {
                self.publish_once(sticky.content.clone())
            })
            .await
        {
            // The refresh replaces our entry in the sticky map, so from here on
            // *this* is the event a peer's reaction must relate to.
            Ok(event_id) => self.record_published(&sticky.content, event_id),
            Err(SendFailure::Superseded | SendFailure::Cancelled) => {}
            Err(failure) => {
                log::warn!("[{room_id}] Failed to refresh sticky membership: {failure}");
            }
        }
    }

    /// Signalling is back: after a random wait (R22), probe the delayed leave
    /// with a restart; one that is gone is re-armed and our membership
    /// republished. One that survived leaves our membership to the next
    /// complete membership set ([`Self::on_catch_up`]), which republishes only
    /// if it lost us (R19). Never republishes blindly.
    pub async fn recover(&self) {
        let jitter = (self.recovery_jitter)();
        log::info!(
            "[{}] signalling is back; recovering in {}ms",
            self.room_id,
            jitter.as_millis()
        );
        executor::sleep(jitter).await;
        if self.state() != OwnMembershipState::Joined {
            return;
        }
        let survived = if self.keep_alive_info.lock().unwrap().is_some() {
            self.restart(false).await
        } else {
            // No dead man's switch: the lifetime path; the set decides.
            true
        };
        if survived {
            self.awaiting_catch_up.store(true, Ordering::SeqCst);
        }
    }

    /// The first complete membership set after a recovery held `own` under our
    /// sticky key: republish if our membership is absent or carries another
    /// event id than the one we last published (R19). Any other set does
    /// nothing.
    pub fn on_catch_up(&self, own: OwnEntry) {
        if self.state() != OwnMembershipState::Joined
            || !self.awaiting_catch_up.swap(false, Ordering::SeqCst)
        {
            return;
        }
        let room_id = &self.room_id;
        match own {
            OwnEntry::Absent => {
                log::info!("[{room_id}] catch-up: our membership is gone; republishing");
                self.request_republish();
            }
            OwnEntry::Present(Some(event_id))
                if self.membership_event_id().as_deref() != Some(event_id.as_str()) =>
            {
                log::info!(
                    "[{room_id}] catch-up: our membership is {event_id}, not the one we last \
                     published; republishing"
                );
                self.request_republish();
            }
            OwnEntry::Present(_) => {
                log::info!("[{room_id}] catch-up: own membership present, no republish");
            }
        }
    }

    /// Whether it is worth asking the homeserver for a delayed leave again.
    ///
    /// Always, unless it has already refused one: a homeserver that refused in
    /// as many words is never asked again, and one that merely failed is asked
    /// at [`DELAYED_LEAVE_PROBE_INTERVAL_MS`] rather than on every beat, so a
    /// permanently-off endpoint is not hammered every ten seconds for the
    /// length of a call.
    fn delayed_leave_probe_due(&self) -> bool {
        match self.delayed_leave_support() {
            DelayedLeaveSupport::Unknown | DelayedLeaveSupport::Supported => true,
            DelayedLeaveSupport::Unsupported {
                permanent: true, ..
            } => false,
            DelayedLeaveSupport::Unsupported {
                last_probe_ms,
                permanent: false,
            } => self.now_ms().saturating_sub(last_probe_ms) >= DELAYED_LEAVE_PROBE_INTERVAL_MS,
        }
    }

    /// Arms a delayed leave through the outbox under `policy`, recording it.
    async fn arm(&self, policy: SendPolicy) -> Result<(), SendFailure> {
        let Some(pending) = self.begin_if_joined(SendKind::DelayedLeave) else {
            return Err(SendFailure::Cancelled);
        };
        let keep_alive_timeout_ms = self.keep_alive_timeout_ms;
        log::trace!(
            "[{}] Scheduling new delayed leave (timeout: {}ms)",
            self.room_id,
            keep_alive_timeout_ms
        );
        let delayed_content = self.build_delayed_leave_content(&self.slot_id, &self.sticky_key);
        let delayed_event_id = pending
            .run(policy, || {
                self.backend.send_delayed_event(
                    self.room_id.clone(),
                    "m.rtc.member".to_string(),
                    None,
                    delayed_content.clone(),
                    keep_alive_timeout_ms,
                )
            })
            .await?;

        *self.delayed_support.lock().unwrap() = DelayedLeaveSupport::Supported;
        *self.keep_alive_info.lock().unwrap() = Some(KeepAliveInfo {
            delayed_event_id,
            timeout_ms: keep_alive_timeout_ms,
            last_restart_ms: self.now_ms(),
        });
        self.update_lapsed();
        log::trace!("[{}] Delayed leave scheduled successfully", self.room_id);
        Ok(())
    }
}

/// Whether the homeserver said the thing does not exist: for a restart, a
/// delayed leave that fired or was forgotten.
fn is_not_found(error: &CommandError) -> bool {
    error
        .backend_error()
        .and_then(|error| error.errcode.as_deref())
        == Some("M_NOT_FOUND")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use crate::host::backend::{MockBackend, NoopBackend, ToDeviceDelivery, ToDeviceRecipient};
    use crate::transport::RawRtcTransport;

    const APPLICATION_TYPE: &str = "m.call";

    #[test]
    fn test_machine_starts_not_joined() {
        let machine = OwnMembershipMachine::with_default_timeout(
            Arc::new(NoopBackend),
            "!room:example.org".to_string(),
            "m.call#room".to_string(),
            "alice-device-a".to_string(),
            APPLICATION_TYPE.to_string(),
        );

        assert_eq!(machine.state(), OwnMembershipState::NotJoined);
        assert!(machine.delayed_event_id().is_none());
    }

    #[test]
    fn test_machine_room_id() {
        let machine = OwnMembershipMachine::with_default_timeout(
            Arc::new(NoopBackend),
            "!room:example.org".to_string(),
            "m.call#room".to_string(),
            "alice-device-a".to_string(),
            APPLICATION_TYPE.to_string(),
        );

        assert_eq!(machine.room_id(), "!room:example.org");
    }

    #[test]
    fn test_machine_slot_id() {
        let machine = OwnMembershipMachine::with_default_timeout(
            Arc::new(NoopBackend),
            "!room:example.org".to_string(),
            "m.call#room".to_string(),
            "alice-device-a".to_string(),
            APPLICATION_TYPE.to_string(),
        );

        assert_eq!(machine.slot_id(), "m.call#room");
    }

    #[test]
    fn test_machine_sticky_key() {
        let machine = OwnMembershipMachine::with_default_timeout(
            Arc::new(NoopBackend),
            "!room:example.org".to_string(),
            "m.call#room".to_string(),
            "alice-device-a".to_string(),
            APPLICATION_TYPE.to_string(),
        );

        assert_eq!(machine.sticky_key(), "alice-device-a");
    }

    #[tokio::test]
    async fn test_machine_join_schedules_delayed_leave() {
        let mock_sender = Arc::new(MockBackend::new());
        let machine = OwnMembershipMachine::with_default_timeout(
            mock_sender.clone(),
            "!room:example.org".to_string(),
            "m.call#room".to_string(),
            "alice-device-a".to_string(),
            APPLICATION_TYPE.to_string(),
        );

        machine
            .join(MemberTransports::default(), None)
            .await
            .expect("join should succeed");

        // Check that delayed events were scheduled
        let delayed_events = mock_sender.delayed_events.lock().unwrap();
        assert_eq!(delayed_events.len(), 1);

        // The first delayed event should be the leave (dead man's switch)
        let (room_id, event_type, _state_key, content, _delay) = &delayed_events[0];
        assert_eq!(room_id, "!room:example.org");
        assert_eq!(event_type, "m.rtc.member");

        // The homeserver fires this on our behalf when heartbeats stop, so it must
        // be distinguishable from a user-initiated leave.
        let leave_reason = content
            .get("leave_reason")
            .expect("leave_reason should be present");
        assert_eq!(
            leave_reason.get("code").and_then(|v| v.as_str()),
            Some("delayed_leave")
        );
        assert_eq!(
            content
                .pointer("/member/membership")
                .and_then(|v| v.as_str()),
            Some("leave")
        );
    }

    #[tokio::test]
    async fn test_machine_join_sends_join_event() {
        let mock_sender = Arc::new(MockBackend::new());
        let machine = OwnMembershipMachine::with_default_timeout(
            mock_sender.clone(),
            "!room:example.org".to_string(),
            "m.call#room".to_string(),
            "alice-device-a".to_string(),
            APPLICATION_TYPE.to_string(),
        );

        machine
            .join(MemberTransports::default(), None)
            .await
            .expect("join should succeed");

        // Check that sticky events were sent
        let sticky_events = mock_sender.sticky_events.lock().unwrap();
        assert_eq!(sticky_events.len(), 1);

        // The sticky event should be the join
        let (room_id, event_type, content, _) = &sticky_events[0];
        assert_eq!(room_id, "!room:example.org");
        assert_eq!(event_type, "m.rtc.member");
        assert_eq!(
            content.get("slot_id").and_then(|v| v.as_str()),
            Some("m.call#room")
        );
    }

    #[tokio::test]
    async fn test_machine_join_with_transport() {
        let mock_sender = Arc::new(MockBackend::new());
        let machine = OwnMembershipMachine::with_default_timeout(
            mock_sender.clone(),
            "!room:example.org".to_string(),
            "m.call#room".to_string(),
            "alice-device-a".to_string(),
            APPLICATION_TYPE.to_string(),
        );

        let transport = MemberTransports::publishing(RawRtcTransport {
            transport_type: "livekit".to_owned(),
            extra_fields: [(
                "livekit_service_url".to_owned(),
                serde_json::Value::String("https://example.com".to_owned()),
            )]
            .into_iter()
            .collect(),
        });

        machine
            .join(transport, None)
            .await
            .expect("join should succeed");

        // Check that the join event includes the transport
        let sticky_events = mock_sender.sticky_events.lock().unwrap();
        assert_eq!(sticky_events.len(), 1);

        // Publishing a transport also declares we can subscribe to its type, so
        // peers can pick one every member can receive.
        let (_, _, content, _) = &sticky_events[0];
        assert_eq!(
            content
                .pointer("/transports/published/0/type")
                .and_then(|v| v.as_str()),
            Some("livekit")
        );
        assert_eq!(
            content
                .pointer("/transports/can_subscribe/0")
                .and_then(|v| v.as_str()),
            Some("livekit")
        );
    }

    #[tokio::test]
    async fn test_machine_leave_sends_leave_event() {
        let mock_sender = Arc::new(MockBackend::new());
        let machine = OwnMembershipMachine::with_default_timeout(
            mock_sender.clone(),
            "!room:example.org".to_string(),
            "m.call#room".to_string(),
            "alice-device-a".to_string(),
            APPLICATION_TYPE.to_string(),
        );

        machine
            .leave(Some(LeaveReason::with_reason(
                LeaveCode::Leave,
                "user hung up",
            )))
            .await
            .expect("leave should succeed");

        // Check that leave event was sent
        let sticky_events = mock_sender.sticky_events.lock().unwrap();
        assert_eq!(sticky_events.len(), 1);

        let (room_id, event_type, content, _) = &sticky_events[0];
        assert_eq!(room_id, "!room:example.org");
        assert_eq!(event_type, "m.rtc.member");

        let leave_reason = content
            .get("leave_reason")
            .expect("leave_reason should be present");
        assert_eq!(
            leave_reason.get("code").and_then(|v| v.as_str()),
            Some("leave")
        );
        assert_eq!(
            leave_reason.get("reason").and_then(|v| v.as_str()),
            Some("user hung up")
        );
        assert!(leave_reason.get("class").is_none());
    }

    #[tokio::test]
    async fn test_machine_heartbeat_restarts_delayed_leave() {
        let mock_sender = Arc::new(MockBackend::new());
        let machine = OwnMembershipMachine::with_default_timeout(
            mock_sender.clone(),
            "!room:example.org".to_string(),
            "m.call#room".to_string(),
            "alice-device-a".to_string(),
            APPLICATION_TYPE.to_string(),
        );

        // Join to start the initial delayed leave
        machine
            .join(MemberTransports::default(), None)
            .await
            .expect("join should succeed");

        let delay_id = machine
            .delayed_event_id()
            .expect("join arms a delayed leave");

        machine.keep_alive().await;

        // The beat restarts the existing delay in place: one request, the same
        // delay id, and no second delay scheduled. Scheduling a replacement
        // instead would leak the original, which then fires and (last-to-expire
        // wins) marks us departed while we are still here.
        assert_eq!(
            *mock_sender.restarted_events.lock().unwrap(),
            vec![("!room:example.org".to_string(), delay_id.clone())],
        );
        assert_eq!(
            mock_sender.delayed_events.lock().unwrap().len(),
            1,
            "the heartbeat must not schedule a second delayed leave"
        );
        assert!(
            mock_sender.cancelled_events.lock().unwrap().is_empty(),
            "the heartbeat must not cancel anything"
        );
        assert_eq!(machine.delayed_event_id(), Some(delay_id));
    }

    /// A restart that fails while the delay is still armed must not be
    /// "recovered" by arming a second one — that is exactly the leak that gets
    /// us marked as departed mid-call. It is retried instead (034 R1).
    #[tokio::test(start_paused = true)]
    async fn a_failed_restart_does_not_immediately_arm_a_replacement() {
        let sender = Arc::new(CancelFailsSender::default());
        let machine = OwnMembershipMachine::with_default_timeout(
            sender.clone(),
            "!room:example.org".to_string(),
            "m.call#room".to_string(),
            "alice-device-a".to_string(),
            APPLICATION_TYPE.to_string(),
        );
        machine
            .join(MemberTransports::default(), None)
            .await
            .expect("join should succeed");
        let delay_id = machine.delayed_event_id().expect("armed by join");

        // The mock fails every restart, with no code. The delay was armed
        // moments ago (the machine's clock does not move under paused time),
        // so it cannot have fired yet: keep it and retry the restart.
        let still_retrying =
            tokio::time::timeout(Duration::from_secs(10), machine.keep_alive()).await;

        assert!(still_retrying.is_err(), "a transient failure is retried");
        assert!(*sender.restarts.lock().unwrap() > 1, "retried on backoff");
        assert_eq!(*sender.scheduled.lock().unwrap(), 1, "no replacement armed");
        assert_eq!(machine.delayed_event_id(), Some(delay_id));
    }

    /// Once a full delay period has passed with no successful restart, the old
    /// delay must have fired, so arming a replacement is leak-free — and
    /// necessary, or we are left with no dead man's switch at all.
    #[tokio::test]
    async fn a_delay_that_must_have_fired_is_replaced() {
        let sender = Arc::new(CancelFailsSender::default());
        // A zero-length delay has always "already fired".
        let mut machine = OwnMembershipMachine::new(
            sender.clone(),
            "!room:example.org".to_string(),
            "m.call#room".to_string(),
            "alice-device-a".to_string(),
            APPLICATION_TYPE.to_string(),
            MembershipTimings {
                keep_alive_timeout_ms: 0,
                ..MembershipTimings::default()
            },
        );
        let now = Arc::new(AtomicU64::new(1_000_000));
        machine.set_clock({
            let now = now.clone();
            Arc::new(move || now.load(Ordering::SeqCst))
        });
        machine
            .join(MemberTransports::default(), None)
            .await
            .expect("join should succeed");

        // Step past the (zero) delay, so the check has unambiguously elapsed
        // rather than sitting exactly on the boundary.
        now.fetch_add(5, Ordering::SeqCst);
        machine.keep_alive().await;

        assert_eq!(
            *sender.scheduled.lock().unwrap(),
            2,
            "the fired delay should have been replaced"
        );
        assert!(machine.delayed_event_id().is_some());
        assert_eq!(
            sender.sticky_events.lock().unwrap().len(),
            2,
            "the fired delay sent our leave, so the join content is republished"
        );
    }

    /// 034 tests drive the machine and its outbox from one paused clock, so
    /// the outbox's backoff and the machine's deadlines move together.
    mod resilience {
        use super::*;
        use crate::host::backend::{BackendError, MockMethod, MockOutcome};
        use crate::outbox::Outbox;

        const T0_MS: u64 = 1_000_000_000;

        fn secs(at: &[u64]) -> Vec<Duration> {
            at.iter().copied().map(Duration::from_secs).collect()
        }

        fn fail() -> MockOutcome {
            MockOutcome::Fail(BackendError::new("offline"))
        }

        fn fail_with(errcode: &str, status: u16) -> MockOutcome {
            MockOutcome::Fail(BackendError::with_matrix_error(
                Some(errcode.to_owned()),
                Some(status),
                errcode,
            ))
        }

        /// A machine on `mock` whose clock is paused tokio time, recovering
        /// without the random wait.
        fn machine(
            mock: &Arc<MockBackend>,
            timings: MembershipTimings,
        ) -> OwnMembershipMachine<MockBackend> {
            let origin = tokio::time::Instant::now();
            let mut machine = OwnMembershipMachine::new(
                mock.clone(),
                "!room:example.org".to_string(),
                "m.call#room".to_string(),
                "alice-device-a".to_string(),
                APPLICATION_TYPE.to_string(),
                timings,
            );
            machine.set_outbox(Outbox::with_clock(Arc::new(move || {
                T0_MS + origin.elapsed().as_millis() as u64
            })));
            machine.set_recovery_jitter(|| Duration::ZERO);
            machine
        }

        async fn at(seconds: f64, origin: tokio::time::Instant) {
            tokio::time::sleep_until(origin + Duration::from_secs_f64(seconds)).await;
        }

        /// A join on a homeserver that refuses delayed events, so the
        /// membership lives on the degraded lifetime alone.
        async fn degraded_join(mock: &Arc<MockBackend>) -> OwnMembershipMachine<MockBackend> {
            mock.script(MockMethod::DelayedEvent, [fail_with("M_UNRECOGNIZED", 404)]);
            let machine = machine(mock, MembershipTimings::default());
            machine
                .join(MemberTransports::default(), None)
                .await
                .unwrap();
            assert_eq!(machine.membership_lifetime_ms(), 300_000);
            machine
        }

        /// R18: due at a third of the lifetime, not before, re-sending the
        /// same content with the same lifetime.
        #[tokio::test(start_paused = true)]
        async fn the_sticky_refresh_falls_due_at_a_third_of_the_lifetime() {
            let origin = tokio::time::Instant::now();
            let mock = Arc::new(MockBackend::new());
            let machine = degraded_join(&mock).await;

            at(99.0, origin).await;
            machine.keep_alive().await;
            assert_eq!(
                mock.sticky_events.lock().unwrap().len(),
                1,
                "not due at 99 s"
            );

            at(100.0, origin).await;
            machine.keep_alive().await;
            let sticky = mock.sticky_events.lock().unwrap();
            assert_eq!(sticky.len(), 2, "due at 100 s");
            assert_eq!(sticky[0].2, sticky[1].2);
            assert_eq!(sticky[1].3, 300_000);
        }

        /// R18: a refresh failing from the moment it falls due is retried on
        /// backoff; one that lands before the lifetime runs out never lapses
        /// the membership.
        #[tokio::test(start_paused = true)]
        async fn an_outage_shorter_than_two_thirds_never_lapses() {
            let origin = tokio::time::Instant::now();
            let mock = Arc::new(MockBackend::new());
            let machine = degraded_join(&mock).await;
            let lapsed = machine.subscribe_lapsed();
            mock.script(MockMethod::StickyEvent, std::iter::repeat_n(fail(), 10));

            at(100.0, origin).await;
            machine.keep_alive().await;

            assert_eq!(
                mock.calls_to(MockMethod::StickyEvent),
                secs(&[0, 100, 101, 103, 107, 115, 131, 161, 191, 221, 251, 281]),
            );
            assert!(!machine.is_lapsed());
            assert!(!lapsed.has_changed().unwrap(), "never lapsed on the way");
            assert_eq!(machine.lapse_at_ms(), Some(T0_MS + 281_000 + 300_000));
        }

        /// R17: lapsed at the earlier of the two deadlines — the delayed
        /// leave's 30 s, or the sticky lifetime on the degraded path — and
        /// live again once a send lands.
        #[tokio::test(start_paused = true)]
        async fn the_membership_lapses_at_the_earlier_of_its_two_deadlines() {
            let origin = tokio::time::Instant::now();

            // The delayed-leave path: every restart fails from the start.
            let mock = Arc::new(MockBackend::new());
            let machine = machine(&mock, MembershipTimings::default());
            machine
                .join(MemberTransports::default(), None)
                .await
                .unwrap();
            mock.script(
                MockMethod::RestartDelayedEvent,
                std::iter::repeat_n(fail(), 100),
            );
            let observed = async {
                at(29.0, origin).await;
                machine.update_lapsed();
                let before = machine.is_lapsed();
                at(31.0, origin).await;
                machine.update_lapsed();
                (before, machine.is_lapsed())
            };
            let (_, observed) = tokio::join!(
                tokio::time::timeout(Duration::from_secs(40), async {
                    at(10.0, origin).await;
                    machine.keep_alive().await;
                }),
                observed,
            );
            assert_eq!(
                observed,
                (false, true),
                "lapsed at 30 s, the delayed leave's"
            );

            // The degraded path: the refresh fails from 100 s until 311 s.
            let origin = tokio::time::Instant::now();
            let mock = Arc::new(MockBackend::new());
            let machine = degraded_join(&mock).await;
            mock.script(MockMethod::StickyEvent, std::iter::repeat_n(fail(), 11));
            let observed = async {
                at(299.0, origin).await;
                machine.update_lapsed();
                let before = machine.is_lapsed();
                at(301.0, origin).await;
                machine.update_lapsed();
                (before, machine.is_lapsed())
            };
            let ((), observed) = tokio::join!(
                async {
                    at(100.0, origin).await;
                    machine.keep_alive().await;
                },
                observed,
            );
            assert_eq!(observed, (false, true), "lapsed at 300 s, the lifetime's");
            assert!(
                !machine.is_lapsed(),
                "live again once the refresh landed at 311 s"
            );
        }

        /// R19: a restart answered `M_NOT_FOUND` means the delay fired: re-arm,
        /// then put the same join content back; the set that follows with the
        /// new event id causes nothing more.
        #[tokio::test(start_paused = true)]
        async fn recovery_restarts_then_rearms_and_republishes_when_the_delay_is_gone() {
            let mock = Arc::new(MockBackend::new());
            let machine = machine(&mock, MembershipTimings::default());
            machine
                .join(MemberTransports::default(), None)
                .await
                .unwrap();
            mock.script(
                MockMethod::RestartDelayedEvent,
                [fail_with("M_NOT_FOUND", 404)],
            );
            let mut republish = machine.subscribe_republish_requests();

            machine.recover().await;

            let order: Vec<_> = mock
                .calls()
                .into_iter()
                .skip(2)
                .map(|call| call.method)
                .collect();
            assert_eq!(
                order,
                [
                    MockMethod::RestartDelayedEvent,
                    MockMethod::DelayedEvent,
                    MockMethod::StickyEvent,
                ],
            );
            let sticky = mock.sticky_events.lock().unwrap().clone();
            assert_eq!(
                sticky[0].2, sticky[1].2,
                "same content, so the same member id"
            );
            assert_eq!(
                machine.delayed_event_id().as_deref(),
                Some("delayed-!room:example.org-m.rtc.member")
            );

            machine.on_catch_up(OwnEntry::Present(machine.membership_event_id()));
            assert!(!republish.has_changed().unwrap(), "no second publish");
            let _ = republish.borrow_and_update();
        }

        /// R19: a delay that survived leaves the decision to the next complete
        /// set: our current event id means nothing to do; absent, or another
        /// id of ours, means republish. Only the first set after a recovery
        /// counts.
        #[tokio::test(start_paused = true)]
        async fn the_catch_up_republishes_only_what_the_outage_lost() {
            let mock = Arc::new(MockBackend::new());
            let machine = machine(&mock, MembershipTimings::default());
            let joined = machine
                .join(MemberTransports::default(), None)
                .await
                .unwrap();
            let mut republish = machine.subscribe_republish_requests();

            machine.on_catch_up(OwnEntry::Absent);
            assert!(
                !republish.has_changed().unwrap(),
                "no recovery, no catch-up"
            );

            machine.recover().await;
            machine.on_catch_up(OwnEntry::Present(Some(joined.clone())));
            assert!(!republish.has_changed().unwrap(), "intact: nothing");

            machine.recover().await;
            machine.on_catch_up(OwnEntry::Absent);
            assert!(republish.has_changed().unwrap(), "gone: republish");
            republish.mark_unchanged();
            machine.on_catch_up(OwnEntry::Absent);
            assert!(
                !republish.has_changed().unwrap(),
                "only the first set after a recovery"
            );

            machine.recover().await;
            machine.on_catch_up(OwnEntry::Present(Some("$older".to_owned())));
            assert!(
                republish.has_changed().unwrap(),
                "a stale event id: republish"
            );
            assert_eq!(
                mock.calls_to(MockMethod::StickyEvent).len(),
                1,
                "the machine only asks; the upkeep sends"
            );
        }

        /// R22: a recovery's first send waits the random delay.
        #[tokio::test(start_paused = true)]
        async fn recovery_waits_its_random_delay() {
            let mock = Arc::new(MockBackend::new());
            let mut machine = machine(&mock, MembershipTimings::default());
            machine.set_recovery_jitter(|| Duration::from_millis(2_400));
            machine
                .join(MemberTransports::default(), None)
                .await
                .unwrap();

            machine.recover().await;

            assert_eq!(
                mock.calls_to(MockMethod::RestartDelayedEvent),
                [Duration::from_millis(2_400)],
            );
        }

        /// R6: inside its budget a join retries a transient failure of its
        /// publish.
        #[tokio::test(start_paused = true)]
        async fn a_join_retries_transient_failures_inside_its_budget() {
            let mock = Arc::new(MockBackend::new());
            let machine = machine(&mock, MembershipTimings::default());
            mock.script(MockMethod::StickyEvent, [fail(), fail()]);

            machine
                .join(MemberTransports::default(), None)
                .await
                .unwrap();

            assert_eq!(mock.calls_to(MockMethod::StickyEvent), secs(&[0, 1, 3]));
            assert_eq!(machine.state(), OwnMembershipState::Joined);
        }

        /// R6, R27: a refusal fails the join at once, naming the code.
        #[tokio::test(start_paused = true)]
        async fn a_join_fails_at_once_on_a_refusal() {
            let mock = Arc::new(MockBackend::new());
            let machine = machine(&mock, MembershipTimings::default());
            mock.script(MockMethod::StickyEvent, [fail_with("M_FORBIDDEN", 403)]);

            let failed = machine
                .join(MemberTransports::default(), None)
                .await
                .unwrap_err();

            assert!(matches!(
                failed,
                JoinError::Refused { errcode: Some(ref code), .. } if code == "M_FORBIDDEN"
            ));
            assert_eq!(mock.calls_to(MockMethod::StickyEvent), [Duration::ZERO]);
        }

        /// R6: a publish that never answers fails the join at the budget, and
        /// the delayed leave it armed is cancelled with one attempt.
        #[tokio::test(start_paused = true)]
        async fn a_join_that_hangs_times_out_and_cancels_its_delayed_leave() {
            let mock = Arc::new(MockBackend::new());
            let machine = machine(&mock, MembershipTimings::default());
            mock.script(MockMethod::StickyEvent, [MockOutcome::Hang]);
            let started = tokio::time::Instant::now();

            let failed = machine
                .join(MemberTransports::default(), None)
                .await
                .unwrap_err();

            assert!(matches!(failed, JoinError::TimedOut));
            assert_eq!(started.elapsed(), Duration::from_secs(30));
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert_eq!(mock.calls_to(MockMethod::CancelDelayedEvent).len(), 1);
            assert_eq!(machine.state(), OwnMembershipState::NotJoined);
        }

        /// R6: the budget is the caller's: a deadline already half spent
        /// leaves the join the other half.
        #[tokio::test(start_paused = true)]
        async fn the_join_budget_is_counted_from_the_callers_deadline() {
            let mock = Arc::new(MockBackend::new());
            let machine = machine(&mock, MembershipTimings::default());
            mock.script(MockMethod::StickyEvent, [MockOutcome::Hang]);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

            let failed = machine
                .join(MemberTransports::default(), Some(deadline))
                .await
                .unwrap_err();

            assert!(matches!(failed, JoinError::TimedOut));
            assert_eq!(tokio::time::Instant::now(), deadline);
        }

        /// R6: an arm that does not answer degrades the join at half the
        /// budget, and the publish still has time to land.
        #[tokio::test(start_paused = true)]
        async fn an_arm_that_hangs_degrades_and_the_join_still_lands() {
            let mock = Arc::new(MockBackend::new());
            let machine = machine(&mock, MembershipTimings::default());
            mock.script(MockMethod::DelayedEvent, [MockOutcome::Hang]);

            machine
                .join(MemberTransports::default(), None)
                .await
                .unwrap();

            assert_eq!(mock.calls_to(MockMethod::StickyEvent), secs(&[15]));
            assert!(!machine.delayed_leave_supported());
            assert_eq!(machine.membership_lifetime_ms(), 300_000);
        }

        /// R1: a leave ends every send this join is retrying, and nothing of
        /// the join is sent after the leave.
        #[tokio::test(start_paused = true)]
        async fn a_leave_ends_every_retrying_send() {
            let origin = tokio::time::Instant::now();
            let mock = Arc::new(MockBackend::new());
            let machine = machine(&mock, MembershipTimings::default());
            machine
                .join(MemberTransports::default(), None)
                .await
                .unwrap();
            mock.script(
                MockMethod::RestartDelayedEvent,
                std::iter::repeat_n(fail(), 100),
            );

            let ((), left) = tokio::join!(machine.keep_alive(), async {
                at(5.0, origin).await;
                machine.leave(None).await
            });

            left.unwrap();
            let restarts = mock.calls_to(MockMethod::RestartDelayedEvent);
            assert!(restarts.iter().all(|at| *at < Duration::from_secs(5)));
            tokio::time::sleep(Duration::from_secs(120)).await;
            assert_eq!(mock.calls_to(MockMethod::RestartDelayedEvent), restarts);
        }

        /// R1: the leave waits for a refresh already at the host, so the refresh
        /// cannot land after it and put us back in the call.
        #[tokio::test(start_paused = true)]
        async fn a_leave_is_never_overtaken_by_a_refresh_in_flight() {
            let origin = tokio::time::Instant::now();
            let mock = Arc::new(MockBackend::new());
            let machine = machine(
                &mock,
                MembershipTimings {
                    sticky_duration_ms: 3_000,
                    ..MembershipTimings::default()
                },
            );
            machine
                .join(MemberTransports::default(), None)
                .await
                .unwrap();
            mock.script(
                MockMethod::StickyEvent,
                [MockOutcome::After(
                    Duration::from_secs(5),
                    Box::new(MockOutcome::Ok),
                )],
            );

            let ((), left) = tokio::join!(
                async {
                    at(1.0, origin).await;
                    machine.refresh_if_due().await;
                },
                async {
                    at(2.0, origin).await;
                    machine.leave(None).await
                },
            );

            left.unwrap();
            assert_eq!(mock.calls_to(MockMethod::StickyEvent), secs(&[0, 1, 6]));
            let last = mock.last_sticky_event().unwrap().2;
            assert_eq!(
                last["member"]["membership"], "leave",
                "the leave is the last word: {last}"
            );
        }
    }

    fn test_machine(mock_sender: Arc<MockBackend>) -> OwnMembershipMachine<MockBackend> {
        OwnMembershipMachine::with_default_timeout(
            mock_sender,
            "!room:example.org".to_string(),
            "m.call#room".to_string(),
            "alice-device-a".to_string(),
            APPLICATION_TYPE.to_string(),
        )
    }

    /// A machine whose sticky entry lives `sticky_duration_ms`, so a test can
    /// make the refresh due (or not) without waiting on a clock.
    fn test_machine_with_sticky_duration(
        mock_sender: Arc<MockBackend>,
        sticky_duration_ms: u64,
    ) -> OwnMembershipMachine<MockBackend> {
        OwnMembershipMachine::new(
            mock_sender,
            "!room:example.org".to_string(),
            "m.call#room".to_string(),
            "alice-device-a".to_string(),
            APPLICATION_TYPE.to_string(),
            MembershipTimings {
                sticky_duration_ms,
                ..MembershipTimings::default()
            },
        )
    }

    /// Sends everything successfully except `cancel_delayed_event`, which fails
    /// the way a homeserver fails it once the delay has already fired.
    #[derive(Default)]
    struct CancelFailsSender {
        sticky_events: std::sync::Mutex<Vec<Value>>,
        /// How many delayed events have been scheduled, to catch leaks.
        scheduled: std::sync::Mutex<u32>,
        /// How many restarts were attempted.
        restarts: std::sync::Mutex<u32>,
        fail_sticky: bool,
    }

    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    impl MatrixBackend for CancelFailsSender {
        fn own_user_id(&self) -> String {
            "@alice:example.org".to_owned()
        }

        fn own_device_id(&self) -> String {
            "DEVICE".to_owned()
        }

        async fn send_sticky_event(
            &self,
            _room_id: String,
            _event_type: String,
            content: Value,
            _duration_ms: u64,
        ) -> Result<String, CommandError> {
            if self.fail_sticky {
                return Err(CommandError::from_message("sticky send rejected"));
            }
            self.sticky_events.lock().unwrap().push(content);
            Ok("$sticky".to_string())
        }

        async fn send_delayed_event(
            &self,
            _room_id: String,
            _event_type: String,
            _state_key: Option<String>,
            _content: Value,
            _delay_ms: u64,
        ) -> Result<String, CommandError> {
            let mut scheduled = self.scheduled.lock().unwrap();
            *scheduled += 1;
            Ok(format!("delay-{scheduled}"))
        }

        async fn send_room_event(
            &self,
            _room_id: String,
            _event_type: String,
            _content: Value,
        ) -> Result<String, CommandError> {
            Ok("$room".to_string())
        }

        async fn redact_event(
            &self,
            _room_id: String,
            _event_id: String,
            _reason: Option<String>,
        ) -> Result<(), CommandError> {
            Ok(())
        }

        async fn restart_delayed_event(
            &self,
            _room_id: String,
            _event_id: String,
        ) -> Result<(), CommandError> {
            *self.restarts.lock().unwrap() += 1;
            Err(CommandError::from_message(
                "M_NOT_FOUND: Unknown delay_id (it already fired)",
            ))
        }

        async fn cancel_delayed_event(
            &self,
            _room_id: String,
            _event_id: String,
        ) -> Result<(), CommandError> {
            Err(CommandError::from_message(
                "M_NOT_FOUND: Unknown delay_id (it already fired)",
            ))
        }

        async fn send_to_device_message(
            &self,
            recipients: Vec<ToDeviceRecipient>,
            _message_type: String,
            _content: Value,
        ) -> Result<Vec<ToDeviceDelivery>, CommandError> {
            Ok(recipients.into_iter().map(ToDeviceDelivery::sent).collect())
        }

        async fn send_state_event(
            &self,
            _room_id: String,
            _event_type: String,
            _state_key: String,
            _content: Value,
        ) -> Result<String, CommandError> {
            Ok("$state".to_string())
        }
    }

    /// A homeserver with MSC4140 switched off: every attempt to arm a delayed
    /// leave is refused, everything else works.
    ///
    /// `permanent` is the difference between the two refusals a real homeserver
    /// gives — one that named the reason, and one that merely failed.
    #[derive(Default)]
    struct NoDelayedEventsSender {
        /// `(content, duration_ms)` for every membership put in the sticky map.
        sticky_events: std::sync::Mutex<Vec<(Value, u64)>>,
        /// Attempts to arm a delayed leave, all of which failed.
        refused: std::sync::Mutex<u32>,
        restarts: std::sync::Mutex<u32>,
        cancels: std::sync::Mutex<u32>,
        permanent: bool,
        /// Flipped in a test to model a homeserver that starts accepting them.
        accepts_now: std::sync::atomic::AtomicBool,
    }

    impl NoDelayedEventsSender {
        fn permanent() -> Self {
            Self {
                permanent: true,
                ..Self::default()
            }
        }

        /// The `duration_ms` the membership was last published with — the whole
        /// point of degrading, so every test here asserts on it.
        fn last_lifetime_ms(&self) -> u64 {
            self.sticky_events.lock().unwrap().last().expect("sent").1
        }
    }

    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    impl MatrixBackend for NoDelayedEventsSender {
        fn own_user_id(&self) -> String {
            "@alice:example.org".to_owned()
        }

        fn own_device_id(&self) -> String {
            "DEVICE".to_owned()
        }

        async fn send_sticky_event(
            &self,
            _room_id: String,
            _event_type: String,
            content: Value,
            duration_ms: u64,
        ) -> Result<String, CommandError> {
            self.sticky_events
                .lock()
                .unwrap()
                .push((content, duration_ms));
            Ok("$sticky".to_string())
        }

        async fn send_delayed_event(
            &self,
            _room_id: String,
            _event_type: String,
            _state_key: Option<String>,
            _content: Value,
            _delay_ms: u64,
        ) -> Result<String, CommandError> {
            if self.accepts_now.load(Ordering::Relaxed) {
                return Ok("delay-1".to_string());
            }
            *self.refused.lock().unwrap() += 1;
            Err(if self.permanent {
                CommandError::DelayedEventsNotSupported(
                    "M_FORBIDDEN: Sending delayed events has been disallowed".to_string(),
                )
            } else {
                CommandError::from_message("502 Bad Gateway")
            })
        }

        async fn restart_delayed_event(
            &self,
            _room_id: String,
            _event_id: String,
        ) -> Result<(), CommandError> {
            *self.restarts.lock().unwrap() += 1;
            Ok(())
        }

        async fn send_room_event(
            &self,
            _room_id: String,
            _event_type: String,
            _content: Value,
        ) -> Result<String, CommandError> {
            Ok("$room".to_string())
        }

        async fn redact_event(
            &self,
            _room_id: String,
            _event_id: String,
            _reason: Option<String>,
        ) -> Result<(), CommandError> {
            Ok(())
        }

        async fn cancel_delayed_event(
            &self,
            _room_id: String,
            _event_id: String,
        ) -> Result<(), CommandError> {
            *self.cancels.lock().unwrap() += 1;
            Ok(())
        }

        async fn send_to_device_message(
            &self,
            recipients: Vec<ToDeviceRecipient>,
            _message_type: String,
            _content: Value,
        ) -> Result<Vec<ToDeviceDelivery>, CommandError> {
            Ok(recipients.into_iter().map(ToDeviceDelivery::sent).collect())
        }

        async fn send_state_event(
            &self,
            _room_id: String,
            _event_type: String,
            _state_key: String,
            _content: Value,
        ) -> Result<String, CommandError> {
            Ok("$state".to_string())
        }
    }

    /// A machine against a homeserver with no delayed events. The degraded
    /// lifetime is zero so a single heartbeat makes the refresh due, the same
    /// trick [`heartbeat_refreshes_the_sticky_membership_once_due`]
    /// uses — and zero is also unmistakably not `sticky_duration_ms`.
    fn degraded_machine(
        sender: Arc<NoDelayedEventsSender>,
    ) -> OwnMembershipMachine<NoDelayedEventsSender> {
        OwnMembershipMachine::new(
            sender,
            "!room:example.org".to_string(),
            "m.call#room".to_string(),
            "alice-device-a".to_string(),
            APPLICATION_TYPE.to_string(),
            MembershipTimings {
                degraded_lifetime_ms: 0,
                ..MembershipTimings::default()
            },
        )
    }

    /// The regression this whole degradation path exists for: matrix.org refuses
    /// delayed events, and until now that failed the join outright — no call at
    /// all on a homeserver where the call would have worked fine.
    #[tokio::test]
    async fn a_homeserver_without_delayed_events_can_still_be_joined() {
        let sender = Arc::new(NoDelayedEventsSender::permanent());
        let machine = degraded_machine(sender.clone());

        machine
            .join(MemberTransports::default(), None)
            .await
            .expect("a refused delayed leave must not fail the join");

        assert_eq!(machine.state(), OwnMembershipState::Joined);
        assert!(machine.delayed_event_id().is_none(), "nothing armed");
        assert!(!machine.delayed_leave_supported());
        assert_eq!(
            sender.sticky_events.lock().unwrap().len(),
            1,
            "the membership itself must still go out",
        );
    }

    /// Losing the dead man's switch is survivable only because the membership
    /// expires on its own — so it has to expire *soon*, not in an hour.
    #[tokio::test]
    async fn a_degraded_membership_is_refreshed_on_the_short_lifetime() {
        let sender = Arc::new(NoDelayedEventsSender::permanent());
        let machine = degraded_machine(sender.clone());
        machine
            .join(MemberTransports::default(), None)
            .await
            .expect("join should succeed");

        machine.keep_alive().await;

        assert_eq!(
            sender.sticky_events.lock().unwrap().len(),
            2,
            "the beat should have refreshed the membership",
        );
        assert_eq!(machine.membership_lifetime_ms(), 0);
        assert_eq!(sender.last_lifetime_ms(), 0, "on the degraded lifetime");
    }

    /// The shortening has to reach the *first* membership, not just its
    /// refreshes. MSC4354 keeps whichever event expires last, so an hour-long
    /// join entry would out-live every short refresh that followed it and the
    /// degradation would buy nothing at all.
    #[tokio::test]
    async fn the_first_membership_of_a_degraded_join_is_already_short() {
        let sender = Arc::new(NoDelayedEventsSender::permanent());
        let machine = degraded_machine(sender.clone());

        machine
            .join(MemberTransports::default(), None)
            .await
            .expect("join should succeed");

        assert_eq!(
            sender.last_lifetime_ms(),
            0,
            "the join itself must already carry the degraded lifetime",
        );
    }

    /// The other half of the same rule: every event for one `sticky_key` states
    /// the same duration, which MSC4354 asks for and which a mid-call change of
    /// lifetime would break.
    #[tokio::test]
    async fn every_membership_of_a_join_states_one_lifetime() {
        let sender = Arc::new(NoDelayedEventsSender::permanent());
        let machine = degraded_machine(sender.clone());
        machine
            .join(MemberTransports::default(), None)
            .await
            .expect("join should succeed");

        machine.keep_alive().await;
        machine.keep_alive().await;
        machine.leave(None).await.expect("leave should succeed");

        let durations: Vec<u64> = sender
            .sticky_events
            .lock()
            .unwrap()
            .iter()
            .map(|(_, duration)| *duration)
            .collect();
        assert!(durations.len() >= 3, "join, refreshes, and the leave");
        assert!(
            durations.iter().all(|duration| *duration == 0),
            "the lifetime settled at join must not move: {durations:?}",
        );
    }

    /// There is no delay id to restart, and asking the homeserver to restart
    /// nothing is a request per beat that can only fail.
    #[tokio::test]
    async fn a_degraded_heartbeat_restarts_nothing() {
        let sender = Arc::new(NoDelayedEventsSender::permanent());
        let machine = degraded_machine(sender.clone());
        machine
            .join(MemberTransports::default(), None)
            .await
            .expect("join should succeed");

        machine.keep_alive().await;

        assert_eq!(*sender.restarts.lock().unwrap(), 0);
    }

    /// A homeserver that said "never" is taken at its word: re-asking every ten
    /// seconds for the length of a call is a 403 per beat and buys nothing.
    #[tokio::test]
    async fn a_stated_refusal_is_never_asked_again() {
        let sender = Arc::new(NoDelayedEventsSender::permanent());
        let machine = degraded_machine(sender.clone());
        machine
            .join(MemberTransports::default(), None)
            .await
            .expect("join should succeed");

        machine.keep_alive().await;
        machine.keep_alive().await;

        assert_eq!(
            *sender.refused.lock().unwrap(),
            1,
            "only the arm at join; the beats must not re-ask",
        );
    }

    /// A refusal that named no reason may have been a blip, so it is retried,
    /// and a homeserver that starts accepting them gets its dead man's switch
    /// back. The membership lifetime does not follow it back up: it was settled
    /// at join and MSC4354 would ignore a change of duration mid-key anyway.
    #[tokio::test]
    async fn an_unexplained_refusal_is_retried_and_can_recover() {
        let sender = Arc::new(NoDelayedEventsSender::default());
        let machine = degraded_machine(sender.clone());
        machine
            .join(MemberTransports::default(), None)
            .await
            .expect("join should succeed");
        assert!(!machine.delayed_leave_supported());

        // Backdate the probe so the retry is due without waiting out the
        // interval, then let the homeserver start accepting them.
        *machine.delayed_support.lock().unwrap() = DelayedLeaveSupport::Unsupported {
            last_probe_ms: 0,
            permanent: false,
        };
        sender.accepts_now.store(true, Ordering::Relaxed);

        machine.keep_alive().await;

        assert!(machine.delayed_leave_supported());
        assert!(machine.delayed_event_id().is_some(), "armed on the retry");
        assert_eq!(
            machine.membership_lifetime_ms(),
            0,
            "the lifetime the join settled on stands for the whole join",
        );
    }

    /// Leaving a call we joined without a dead man's switch has nothing to
    /// cancel, and must not invent a cancellation to fail on.
    #[tokio::test]
    async fn a_degraded_leave_cancels_nothing() {
        let sender = Arc::new(NoDelayedEventsSender::permanent());
        let machine = degraded_machine(sender.clone());
        machine
            .join(MemberTransports::default(), None)
            .await
            .expect("join should succeed");

        machine.leave(None).await.expect("leave should succeed");

        assert_eq!(machine.state(), OwnMembershipState::Left);
        assert_eq!(*sender.cancels.lock().unwrap(), 0);
    }

    /// A join that never published a membership is not a join. Leaving the
    /// state at `Joining` would have the heartbeat arming delayed leaves for a
    /// membership nobody can see.
    #[tokio::test(start_paused = true)]
    async fn a_failed_join_returns_to_not_joined() {
        let sender = Arc::new(CancelFailsSender {
            fail_sticky: true,
            ..CancelFailsSender::default()
        });
        let machine = OwnMembershipMachine::with_default_timeout(
            sender,
            "!room:example.org".to_string(),
            "m.call#room".to_string(),
            "alice-device-a".to_string(),
            APPLICATION_TYPE.to_string(),
        );

        let failed = machine
            .join(MemberTransports::default(), None)
            .await
            .expect_err("the membership send fails");

        assert!(
            matches!(failed, JoinError::TimedOut),
            "a transient failure is retried until the budget runs out: {failed:?}"
        );
        assert_eq!(machine.state(), OwnMembershipState::NotJoined);
    }

    /// The sticky entry expires independently of the delayed leave, so a call
    /// running past its lifetime must re-announce the membership.
    #[tokio::test]
    async fn heartbeat_refreshes_the_sticky_membership_once_due() {
        let mock_sender = Arc::new(MockBackend::new());
        // A zero lifetime is always due.
        let machine = test_machine_with_sticky_duration(mock_sender.clone(), 0);
        machine
            .join(MemberTransports::default(), None)
            .await
            .expect("join should succeed");

        machine.keep_alive().await;

        let sticky = mock_sender.sticky_events.lock().unwrap();
        assert_eq!(sticky.len(), 2, "the heartbeat should re-send the join");
        // Re-sent verbatim: peers must not see a different membership.
        assert_eq!(sticky[0].2, sticky[1].2);
        // And with the lifetime the machine was configured with.
        assert_eq!(sticky[1].3, 0);
    }

    #[tokio::test]
    async fn heartbeat_leaves_a_fresh_sticky_membership_alone() {
        let mock_sender = Arc::new(MockBackend::new());
        let machine = test_machine_with_sticky_duration(mock_sender.clone(), 60 * 60 * 1000);
        machine
            .join(MemberTransports::default(), None)
            .await
            .expect("join should succeed");

        machine.keep_alive().await;

        assert_eq!(
            mock_sender.sticky_events.lock().unwrap().len(),
            1,
            "an hour-long entry needs no refresh seconds after joining"
        );
    }

    #[tokio::test]
    async fn heartbeat_refreshes_nothing_before_a_join() {
        let mock_sender = Arc::new(MockBackend::new());
        let machine = test_machine_with_sticky_duration(mock_sender.clone(), 0);

        machine.keep_alive().await;

        assert!(
            mock_sender.sticky_events.lock().unwrap().is_empty(),
            "there is no membership to refresh yet"
        );
    }

    #[tokio::test]
    async fn heartbeat_stops_refreshing_the_sticky_after_leaving() {
        let mock_sender = Arc::new(MockBackend::new());
        let machine = test_machine_with_sticky_duration(mock_sender.clone(), 0);
        machine
            .join(MemberTransports::default(), None)
            .await
            .expect("join should succeed");
        machine.leave(None).await.expect("leave should succeed");

        let after_leave = mock_sender.sticky_events.lock().unwrap().len();
        machine.keep_alive().await;

        assert_eq!(
            mock_sender.sticky_events.lock().unwrap().len(),
            after_leave,
            "refreshing after a leave would resurrect the membership"
        );
    }

    /// The delayed leave firing is the outcome a leave wants anyway; failing to
    /// cancel it must not turn a successful leave into an error.
    #[tokio::test]
    async fn leave_succeeds_when_the_delayed_event_already_fired() {
        let sender = Arc::new(CancelFailsSender::default());
        let machine = OwnMembershipMachine::with_default_timeout(
            sender.clone(),
            "!room:example.org".to_string(),
            "m.call#room".to_string(),
            "alice-device-a".to_string(),
            APPLICATION_TYPE.to_string(),
        );
        machine
            .join(MemberTransports::default(), None)
            .await
            .expect("join should succeed");

        machine
            .leave(None)
            .await
            .expect("a 404 on cancel must not fail the leave");

        assert_eq!(machine.state(), OwnMembershipState::Left);
        assert_eq!(
            machine.delayed_event_id(),
            None,
            "the stale delay id must be cleared either way"
        );
        assert_eq!(
            sender.sticky_events.lock().unwrap().len(),
            2,
            "join then leave"
        );
    }

    /// The tolerance above must not extend to the leave event itself.
    #[tokio::test]
    async fn leave_still_fails_when_the_leave_event_cannot_be_sent() {
        let sender = Arc::new(CancelFailsSender {
            fail_sticky: true,
            ..Default::default()
        });
        let machine = OwnMembershipMachine::with_default_timeout(
            sender,
            "!room:example.org".to_string(),
            "m.call#room".to_string(),
            "alice-device-a".to_string(),
            APPLICATION_TYPE.to_string(),
        );

        machine
            .leave(None)
            .await
            .expect_err("a rejected leave event is a real failure");
        assert_ne!(machine.state(), OwnMembershipState::Left);
    }

    // Regression: every write path must emit the MSC4354 unstable id `msc4354_sticky_key`,
    // never the bare `sticky_key`. The join, delayed-leave and leave content are all built
    // from the single shared `RawStickyEventContent`, so the rename is applied everywhere.

    #[tokio::test]
    async fn test_join_and_delayed_leave_use_unstable_sticky_key() {
        let mock_sender = Arc::new(MockBackend::new());
        test_machine(mock_sender.clone())
            .join(MemberTransports::default(), None)
            .await
            .expect("join should succeed");

        let (_, _, join_content, _) = &mock_sender.sticky_events.lock().unwrap()[0];
        assert_eq!(
            join_content
                .get("msc4354_sticky_key")
                .and_then(|v| v.as_str()),
            Some("alice-device-a")
        );
        assert!(join_content.get("sticky_key").is_none());
        // `leave_reason` is skipped rather than serialized as null on a join.
        assert!(join_content.get("leave_reason").is_none());
        assert_eq!(
            join_content
                .pointer("/member/membership")
                .and_then(|v| v.as_str()),
            Some("join")
        );

        let (_, _, _, delayed_content, _) = &mock_sender.delayed_events.lock().unwrap()[0];
        assert_eq!(
            delayed_content
                .get("msc4354_sticky_key")
                .and_then(|v| v.as_str()),
            Some("alice-device-a")
        );
        assert!(delayed_content.get("sticky_key").is_none());
    }

    #[tokio::test]
    async fn test_leave_uses_unstable_sticky_key_and_round_trips() {
        use crate::host::event::RawStickyEventContent;

        let mock_sender = Arc::new(MockBackend::new());
        test_machine(mock_sender.clone())
            .leave(Some(LeaveReason::with_reason(
                LeaveCode::Leave,
                "user hung up",
            )))
            .await
            .expect("leave should succeed");

        let (_, _, leave_content, _) = &mock_sender.sticky_events.lock().unwrap()[0];
        assert_eq!(
            leave_content
                .get("msc4354_sticky_key")
                .and_then(|v| v.as_str()),
            Some("alice-device-a")
        );
        assert!(leave_content.get("sticky_key").is_none());
        // A leave carries only slot_id / sticky_key / member / leave_reason; the
        // join-only fields must be skipped, not emitted empty.
        assert!(leave_content.get("application").is_none());
        assert!(leave_content.get("transports").is_none());
        assert_eq!(
            leave_content
                .pointer("/member/membership")
                .and_then(|v| v.as_str()),
            Some("leave")
        );
        assert_eq!(
            leave_content.pointer("/member/id").and_then(|v| v.as_str()),
            Some("alice-device-a")
        );

        // The emitted content must deserialize back through the shared struct with the
        // sticky_key intact — this is the exact regression the refactor guards against.
        let parsed: RawStickyEventContent =
            serde_json::from_value(leave_content.clone()).expect("leave content must round-trip");
        assert_eq!(parsed.sticky_key, "alice-device-a");
    }
}
