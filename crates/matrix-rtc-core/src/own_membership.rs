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
//! 3. **Keep-alive** - Restart the delayed leave before it fires, retrying on a backoff.
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
//!   switch above. Its timer is pushed back out, 30 % into it, via MSC4140's
//!   `restart` action — never cancel-and-recreate, which leaves a window with
//!   nothing armed and can leak a delay that then marks us departed mid-call.
//! - The **sticky-map entry** (`sticky_duration_ms`, an hour by default) is how
//!   long the homeserver keeps the membership at all. Re-sent once it is
//!   halfway to expiry.
//!
//! Tending only the first produces a membership that vanishes mid-call with a
//! perfectly healthy keep-alive; tending only the second leaves a ghost
//! membership behind when the client dies. The slot session's upkeep
//! (`upkeep.rs`) wakes it when [`OwnMembershipMachine::next_due_at_ms`] says
//! something is due.
//!
//! # Knowing that we left, without being told
//!
//! The delay's lifecycle is followed locally: it fires a full delay after the
//! last restart the homeserver confirmed. Once that moment passes without a
//! confirmed restart — the homeserver is unreachable, say — our leave has gone
//! out, or will the moment the homeserver is back. The membership is then
//! [`OwnMembershipState::Lost`] and the session ends; so it is too when a
//! restart is answered with `M_NOT_FOUND`. Nothing is sent at that point: the
//! homeserver is most likely down, and a leave retried into a later rejoin
//! would end the rejoin. The armed delay does the leaving; a later join
//! retires it first ([`OwnMembershipMachine::supersede_delayed_leave`]).
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

use serde_json::{Value, json};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{Mutex as AsyncMutex, watch};
// `std::time::SystemTime::now()` panics on wasm32-unknown-unknown; web-time's
// is the same API over `Date.now()`.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use std::time::{SystemTime, UNIX_EPOCH};
// The keep-alive's own clock: monotonic, and natively tokio's, so it follows
// paused time in tests as the upkeep's sleeps do.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use web_time::{SystemTime, UNIX_EPOCH};
// Gated like the tokio `time` feature in Cargo.toml: by architecture.
#[cfg(not(target_arch = "wasm32"))]
use tokio::time::Instant;
#[cfg(target_arch = "wasm32")]
use web_time::Instant;

use crate::delayed_leave::{Backoff, LOCAL_RESTART_PERCENT};
use crate::error::CommandError;
use crate::host::backend::MatrixBackend;
use crate::host::event::RawStickyEventContent;
use crate::session::{ApplicationInfo, LeaveCode, LeaveReason};
use crate::transport::{MemberTransports, RtcTransport};

/// Default keep-alive timeout in milliseconds (30 seconds).
pub const DEFAULT_KEEP_ALIVE_TIMEOUT_MS: u64 = 30_000;

/// The shortest the upkeep sleeps between keep-alive wake-ups.
const MIN_WAKE_MS: u64 = 50;

/// The shortest wait before retrying a failed restart near the deadline.
const MIN_RETRY_MS: u64 = 500;

/// The keep-alive policy this one replaced — a restart every
/// [`legacy_policy::INTERVAL_MS`], failures not retried before the next — for
/// the chaos suite to compare against. Off unless a test turns it on.
/// Process-global: for a separate process such as `chaos_peer`; in-process
/// tests must not flip it, or they change every other test's machines.
pub mod legacy_policy {
    /// The old fixed interval.
    pub const INTERVAL_MS: u64 = 10_000;

    #[cfg(feature = "testing")]
    static ENABLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

    /// Turns the old policy on or off for the whole process.
    #[cfg(feature = "testing")]
    pub fn set_enabled(on: bool) {
        ENABLED.store(on, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn enabled() -> bool {
        #[cfg(feature = "testing")]
        return ENABLED.load(std::sync::atomic::Ordering::Relaxed);
        #[cfg(not(feature = "testing"))]
        false
    }
}

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
    /// Our membership timed out while we were still in the call: no restart
    /// was confirmed within the delay, the homeserver no longer has it, or the
    /// room dropped our membership.
    /// Its leave is out, or goes out once the homeserver is reachable. Final:
    /// the session ends, and only a new join brings us back.
    Lost,
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
    /// in as many words from one that merely failed; the latter is retried on
    /// the backoff.
    Unsupported {
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
    /// How long the delayed leave waits before firing; restarted 30 % into it.
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
/// 3. Keep-alive -> restarts the delayed leave 30 % into its delay
///
/// The delayed leave is scheduled FIRST because it's safer - if the client dies at any
/// point, worst case we're cleaning up our membership.
///
/// This machine is responsible for:
/// - Managing our membership state (joined/left)
/// - Sending join/leave events via the backend
/// - Managing the keep-alive delayed event lifecycle
/// - Storing and retrieving the delayed event ID from callbacks
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
    /// Current state of the membership machine.
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
    /// Held by a keep-alive tick and by `leave` for their whole run. The
    /// session's upkeep ticks from its own task, so without this a refresh in
    /// flight could re-send the join content after the leave content and put
    /// us back in the call.
    sending: AsyncMutex<()>,
    /// A delayed leave of an earlier join of this membership, to retire before
    /// joining; see [`Self::supersede_delayed_leave`].
    superseded: Mutex<Option<String>>,
    /// Retries of the delayed leave (restart, or arm when none is armed).
    retry: Mutex<Backoff>,
    /// Retries of the sticky refresh.
    sticky_retry: Mutex<Backoff>,
    /// When the last restart was attempted, successful or not; only
    /// [`legacy_policy`] paces by it.
    last_attempt_ms: AtomicU64,
    /// Whether the loss was announced, so it is announced once.
    loss_reported: std::sync::atomic::AtomicBool,
    /// The clock's anchor: `epoch` read as wall-clock `epoch_wall_ms`. The
    /// keep-alive's deadlines are measured from it on the monotonic clock, so
    /// a wall-clock jump cannot move them.
    epoch: Instant,
    epoch_wall_ms: u64,
    /// Added to the clock, so a test can move time without sleeping.
    #[cfg(test)]
    clock_offset_ms: AtomicU64,
}

/// The membership event we last put in the sticky map, and when.
#[derive(Debug, Clone)]
struct SentSticky {
    /// The join content, re-sent verbatim to refresh the entry.
    content: Value,
    /// Unix ms at which it was accepted by the server.
    sent_at_ms: u64,
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
            sending: AsyncMutex::new(()),
            superseded: Mutex::new(None),
            retry: Mutex::new(Backoff::default()),
            sticky_retry: Mutex::new(Backoff::default()),
            epoch: Instant::now(),
            epoch_wall_ms: now_ms(),
            last_attempt_ms: AtomicU64::new(now_ms()),
            loss_reported: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            clock_offset_ms: AtomicU64::new(0),
        }
    }

    /// The clock everything here is decided on: Unix ms, advanced
    /// monotonically from the machine's creation.
    fn now(&self) -> u64 {
        let now = self.epoch_wall_ms + self.epoch.elapsed().as_millis() as u64;
        #[cfg(test)]
        return now + self.clock_offset_ms.load(Ordering::Relaxed);
        #[cfg(not(test))]
        now
    }

    /// Moves this machine's clock forward.
    #[cfg(test)]
    pub(crate) fn advance_clock_ms(&self, ms: u64) {
        self.clock_offset_ms.fetch_add(ms, Ordering::Relaxed);
    }

    /// Retire `delay_id` before the next [`Self::join`]: the delayed leave of an
    /// earlier join of this membership that ended [`OwnMembershipState::Lost`].
    ///
    /// Left alone, it may fire *after* the new join — a homeserver coming back
    /// sends its overdue delays as soon as it can — and end the membership we
    /// just made. The join cancels it first; `M_NOT_FOUND` means it fired
    /// already, which is as good.
    pub fn supersede_delayed_leave(&self, delay_id: String) {
        *self.superseded.lock().unwrap() = Some(delay_id);
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

    /// Gets the delayed event ID, if one is active.
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

    pub fn delayed_event_id(&self) -> Option<String> {
        self.keep_alive_info
            .lock()
            .unwrap()
            .as_ref()
            .map(|info| info.delayed_event_id.clone())
    }

    /// Joins the session by implementing the dead man's switch strategy.
    ///
    /// This method:
    /// 1. Schedules a delayed leave event FIRST (safety net) - **awaited for completion**
    /// 2. Sends the join membership event - **awaited for completion**
    /// 3. The delayed leave will be restarted by heartbeat calls
    ///
    /// The async design ensures that we verify the delayed leave is successfully scheduled
    /// before sending the join event, providing a proper safety net for the dead man's switch.
    ///
    /// # Arguments
    ///
    /// * `transports` - The `transports` object to publish for this member
    ///
    /// A homeserver that refuses step 1 does not fail the join. MSC4140 is
    /// optional, and losing it costs only the *speed* of the cleanup — the
    /// membership still expires on its own. Step 1 therefore degrades (see
    /// [`DelayedLeaveSupport`]) and the membership goes out with the shorter
    /// [`Self::membership_lifetime_ms`] instead. Step 2 failing is different:
    /// that means we are not in the call, and it still fails the join.
    ///
    /// # Returns
    ///
    /// The event id of the membership event if the join was sent successfully;
    /// an error if it was not. The caller needs the id to relate an MSC4075
    /// notification to the membership that justifies it.
    pub async fn join(&self, transports: MemberTransports) -> Result<String, CommandError> {
        let room_id = self.room_id.clone();
        let slot_id = self.slot_id.clone();
        let sticky_key = self.sticky_key.clone();
        let application = self.application.clone();
        let keep_alive_timeout_ms = self.keep_alive_timeout_ms;

        // Update state to Joining first
        {
            let mut state_guard = self.state.lock().unwrap();
            *state_guard = OwnMembershipState::Joining;
        }

        // Step 0: retire the delayed leave of an earlier join of this
        // membership, or it may end this one when it fires. Sent now, so the
        // old membership ends with its leave; cancelled where the host cannot
        // send one.
        let superseded = self.superseded.lock().unwrap().take();
        if let Some(old) = superseded {
            let retired = match self
                .backend
                .send_delayed_event_now(room_id.clone(), old.clone())
                .await
            {
                Err(error) if error.is_not_implemented() => {
                    self.backend
                        .cancel_delayed_event(room_id.clone(), old.clone())
                        .await
                }
                sent => sent,
            };
            match retired {
                Ok(()) => log::info!("[{room_id}] retired the earlier delayed leave {old}"),
                Err(error) if error.is_delayed_event_gone() => {
                    log::info!("[{room_id}] the earlier delayed leave {old} has already fired")
                }
                Err(error) => {
                    // Not joining: the old leave could still land after us. The
                    // caller retries with the delay it still holds.
                    *self.state.lock().unwrap() = OwnMembershipState::NotJoined;
                    log::warn!(
                        "[{room_id}] join deferred: the earlier delayed leave could not be \
                         retired yet ({error})"
                    );
                    return Err(error);
                }
            }
        }

        // Step 1: Schedule delayed leave event FIRST (safety net)
        // If client dies at any point, worst case we're cleaning up.
        // **We await this to ensure the delayed event is scheduled before proceeding.**
        let delayed_content = self.build_delayed_leave_content(&slot_id, &sticky_key);

        log::info!(
            "[{}] Scheduling delayed leave event (dead man's switch safety net)",
            room_id
        );

        // Schedule the delayed leave (Step 1 of dead man's switch)
        // This returns the event_id on success. The homeserver's timer starts
        // at the request at the earliest, so the local deadline counts from
        // before it, never later than the server's.
        let armed_at = self.now();
        match self
            .backend
            .send_delayed_event(
                room_id.clone(),
                "m.rtc.member".to_string(),
                None,
                delayed_content,
                keep_alive_timeout_ms,
            )
            .await
        {
            // Store the delayed event ID for later cancellation
            Ok(delayed_event_id) => {
                *self.delayed_support.lock().unwrap() = DelayedLeaveSupport::Supported;
                self.retry.lock().unwrap().reset();
                let mut info_guard = self.keep_alive_info.lock().unwrap();
                *info_guard = Some(KeepAliveInfo {
                    delayed_event_id,
                    timeout_ms: keep_alive_timeout_ms,
                    last_restart_ms: armed_at,
                });
                self.last_attempt_ms.store(armed_at, Ordering::Relaxed);
            }
            // Not fatal: the dead man's switch is a cleanup optimisation, not a
            // precondition for being in the call. Join without it, and let the
            // membership's own lifetime do the cleanup instead — shortened, so a
            // client that dies is forgotten in minutes rather than in an hour.
            //
            // Here, before the first publish, is the only place that shortening
            // can happen; see `published_lifetime_ms`.
            Err(error) => {
                self.mark_delayed_leave_unsupported(&error);
                self.retry.lock().unwrap().failed(self.now());
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

        // Step 2: Send join membership event
        let join_content = self.build_join_content(&slot_id, &sticky_key, application, transports);

        log::info!(
            "[{}] Sending join membership event (step 2 of dead man's switch)",
            room_id
        );

        // Send the join event (Step 2 of dead man's switch)
        let event_id = self
            .backend
            .send_sticky_event(
                room_id.clone(),
                "m.rtc.member".to_string(),
                join_content.clone(),
                self.membership_lifetime_ms(),
            )
            .await
            .inspect_err(|error| {
                // Back to NotJoined, not left at `Joining`: we never made it
                // into the call, and a state that says otherwise would have
                // `heartbeat` arming delayed leaves for a membership that does
                // not exist.
                *self.state.lock().unwrap() = OwnMembershipState::NotJoined;
                log::warn!(
                    "[{room_id}] join aborted at step 2: the join membership event was not \
                     sent ({error}). The delayed leave, if one is armed, will clean up.",
                )
            })?;

        // Remember it so the heartbeat can refresh the sticky entry before the
        // server expires it. Recorded only on success: a failed send left
        // nothing in the map, so there is nothing to refresh.
        {
            let mut guard = self.last_sticky.lock().unwrap();
            *guard = Some(SentSticky {
                content: join_content,
                sent_at_ms: self.now(),
            });
        }
        self.latest_event_id.send_replace(Some(event_id.clone()));

        // Both steps completed successfully, transition to Joined state
        {
            let mut state_guard = self.state.lock().unwrap();
            *state_guard = OwnMembershipState::Joined;
        }

        log::info!(
            "[{}] Successfully joined with dead man's switch armed",
            room_id
        );

        Ok(event_id)
    }

    /// Records that an attempt to arm a delayed leave failed.
    ///
    /// A homeserver that named the reason ([`CommandError::DelayedEventsNotSupported`])
    /// is taken at its word and never asked again; anything else may have been a
    /// blip, so it is asked again on the backoff.
    fn mark_delayed_leave_unsupported(&self, error: &CommandError) {
        *self.delayed_support.lock().unwrap() = DelayedLeaveSupport::Unsupported {
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

    /// Leaves the session by sending a leave event and canceling the keep-alive.
    ///
    /// This method:
    /// 1. Sends a leave membership event
    /// 2. Cancels the active delayed leave event (if any)
    ///
    /// Both operations are awaited to ensure proper cleanup.
    pub async fn leave(&self, leave_reason: Option<LeaveReason>) -> Result<(), CommandError> {
        let _sending = self.sending.lock().await;
        let room_id = self.room_id.clone();
        let slot_id = self.slot_id.clone();
        let sticky_key = self.sticky_key.clone();

        // A lost membership is already out of the call, and the homeserver may
        // be unreachable: tidy up locally and send nothing. A leave retried into
        // a later rejoin would end it.
        if self.state() == OwnMembershipState::Lost {
            *self.last_sticky.lock().unwrap() = None;
            self.latest_event_id.send_replace(None);
            *self.state.lock().unwrap() = OwnMembershipState::Left;
            log::info!("[{room_id}] left a lost membership locally; nothing sent");
            return Ok(());
        }

        // A voluntary leave with no stated cause is MSC4143's plain `leave` code.
        // `MembershipLost` never goes on the wire; a host echoing it leaves
        // plainly, with its explanation.
        let leave_reason = Some(match leave_reason {
            None => LeaveReason::new(LeaveCode::Leave),
            Some(reason) if reason.code == LeaveCode::MembershipLost => LeaveReason {
                code: LeaveCode::Leave,
                reason: reason.reason,
                delay_id: None,
            },
            Some(reason) => reason,
        });
        let leave_content = serde_json::to_value(RawStickyEventContent::for_leave(
            slot_id.clone(),
            sticky_key.clone(),
            leave_reason,
        ))
        .expect("m.rtc.member content is always serializable");

        // Update state to Leaving
        {
            let mut state_guard = self.state.lock().unwrap();
            *state_guard = OwnMembershipState::Leaving;
        }

        log::info!("[{}] Sending leave membership event", room_id);

        // Send leave event
        self.backend
            .send_sticky_event(
                room_id.clone(),
                "m.rtc.member".to_string(),
                leave_content,
                self.membership_lifetime_ms(),
            )
            .await?;

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
                .backend
                .cancel_delayed_event(room_id.clone(), event_id.clone())
                .await
            {
                Ok(()) => log::debug!("[{}] Delayed leave event canceled", room_id),
                Err(error) => log::debug!(
                    "[{room_id}] Delayed leave {event_id} could not be canceled ({error:?}); \
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

    /// Tends the keep-alive: restarts the delayed leave when due, re-sends the
    /// membership if its sticky entry is nearing expiry, and notices when the
    /// membership has timed out ([`OwnMembershipState::Lost`]).
    ///
    /// Woken by the session's upkeep when [`Self::next_due_at_ms`] says so; a
    /// wake-up with nothing due does nothing, so waking early is harmless.
    ///
    /// The restart falls due [`LOCAL_RESTART_PERCENT`] into the delay after the
    /// last confirmed one. A failed restart is retried on an exponential backoff
    /// with jitter, brought forward to the delay's deadline. The delay itself is
    /// never replaced while we are joined: a restart can fail while it sits
    /// there perfectly armed, and a second one would leak — nobody restarts it,
    /// so it fires and marks us departed mid-call.
    ///
    /// Instead its lifecycle is followed: past the deadline with no confirmed
    /// restart, or answered with `M_NOT_FOUND`, our leave is out and the
    /// membership is [`OwnMembershipState::Lost`]. Nothing is sent then; see
    /// the module documentation.
    ///
    /// Uses MSC4140's `restart` action rather than cancel-then-reschedule: one
    /// request, never a moment with nothing armed, and no leaked delay.
    ///
    /// Fire-and-forget (no `Result`). A no-op unless joined, so a wake-up that
    /// waited out a leave sends nothing.
    pub async fn keep_alive(&self) {
        let _sending = self.sending.lock().await;
        if self.state() != OwnMembershipState::Joined {
            return;
        }
        log::trace!("[{}] keep-alive", self.room_id);

        // Two independent clocks expire our membership: the delayed leave
        // first, then the sticky-map entry. In that order, and the refresh
        // bounded by whatever falls due next, so a slow homeserver answering
        // the refresh can never keep the restart from being tried.
        self.tend_delayed_leave().await;
        if self.state() == OwnMembershipState::Joined {
            let until = self.next_due_at_ms();
            let _ = self.within(until, self.refresh_sticky_if_due()).await;
        }
    }

    /// The delayed-leave half of [`Self::keep_alive`]: restart when due, retry
    /// on the backoff, arm when none is armed, notice the deadline passing.
    async fn tend_delayed_leave(&self) {
        let room_id = self.room_id.clone();
        let now = self.now();
        let Some(info) = self.keep_alive_info.lock().unwrap().clone() else {
            // Nothing armed: the homeserver refused one. Arming one is the safe
            // move — without it a crash leaves a ghost behind.
            if self.delayed_leave_probe_due(now) {
                let was_degraded = !self.delayed_leave_supported();
                match self.schedule_delayed_leave().await {
                    // The membership lifetime stays where the join left it. It
                    // is the safe direction to be wrong in — a shorter lifetime
                    // than we now need costs signalling, where a longer one
                    // would cost a ghost — and MSC4354 would ignore a change of
                    // duration mid-key anyway.
                    Ok(()) if was_degraded => log::info!(
                        "[{room_id}] the homeserver accepts delayed events after all; the dead \
                         man's switch is armed again",
                    ),
                    Ok(()) => {}
                    Err(error) => {
                        self.mark_delayed_leave_unsupported(&error);
                        self.retry.lock().unwrap().failed(now);
                        log::warn!("[{room_id}] Failed to arm a delayed leave: {error:?}");
                    }
                }
            }
            return;
        };

        let deadline = info.last_restart_ms.saturating_add(info.timeout_ms);
        if now >= deadline {
            self.lose(&format!(
                "no restart of delayed leave {} was confirmed within its {}ms delay",
                info.delayed_event_id, info.timeout_ms,
            ));
            return;
        }
        if !self.restart_due(&info, now) {
            return;
        }

        self.last_attempt_ms.store(now, Ordering::Relaxed);
        let restart = self.within_deadline(
            self.backend
                .restart_delayed_event(room_id.clone(), info.delayed_event_id.clone()),
        );
        let Some(restarted) = restart.await else {
            self.lose(&format!(
                "the restart of delayed leave {} was not answered before its deadline",
                info.delayed_event_id,
            ));
            return;
        };
        match restarted {
            Ok(()) => {
                self.retry.lock().unwrap().reset();
                let mut guard = self.keep_alive_info.lock().unwrap();
                if let Some(current) = guard.as_mut()
                    && current.delayed_event_id == info.delayed_event_id
                {
                    current.last_restart_ms = now;
                }
            }
            Err(error) if error.is_delayed_event_gone() => {
                self.lose(&format!(
                    "the homeserver no longer has delayed leave {}: it fired",
                    info.delayed_event_id,
                ));
            }
            Err(error) => {
                let mut retry = self.retry.lock().unwrap();
                if legacy_policy::enabled() {
                    // The policy before this one: no retry before the next beat.
                    retry.reset();
                } else {
                    // From the failure, not the attempt: a request that took
                    // its whole timeout must still be followed by a jittered
                    // wait, or the retries go out back to back.
                    let failed_at = self.now();
                    retry.failed(failed_at);
                    // Denser, not sparser, as the deadline nears: never wait
                    // more than half the time left. A homeserver back with
                    // seconds to spare must find a retry in those seconds.
                    let left = deadline.saturating_sub(failed_at);
                    retry.clamp_to(failed_at, failed_at + (left / 2).max(MIN_RETRY_MS));
                }
                log::warn!(
                    "[{room_id}] Failed to restart delayed leave {}: {error:?}. Retrying at {:?} \
                     (deadline {deadline}).",
                    info.delayed_event_id,
                    retry.retry_due_at_ms(),
                );
            }
        }
    }

    /// Runs `request` until it completes or the armed delay's deadline passes,
    /// whichever comes first; `None` if the deadline won. Unbounded while
    /// nothing is armed, and off a runtime.
    ///
    /// The request is dropped at the deadline, which cancels it. A homeserver
    /// cut off by a partition does not refuse requests, it never answers them,
    /// and the host's own timeouts and retries can outlast the whole delay.
    pub(crate) async fn within_deadline<F: std::future::Future>(
        &self,
        request: F,
    ) -> Option<F::Output> {
        let deadline = self
            .keep_alive_info
            .lock()
            .unwrap()
            .as_ref()
            .map(|info| info.last_restart_ms.saturating_add(info.timeout_ms));
        self.within(deadline, request).await
    }

    /// Runs `request` until it completes or `until_ms` passes; `None` if time
    /// ran out. Unbounded without `until_ms`, and off a runtime (a host ticking
    /// us itself has no timer to race against; its own cadence bounds the
    /// wait there).
    async fn within<F: std::future::Future>(
        &self,
        until_ms: Option<u64>,
        request: F,
    ) -> Option<F::Output> {
        let Some(until_ms) = until_ms.filter(|_| crate::executor::can_spawn()) else {
            return Some(request.await);
        };
        let left = std::time::Duration::from_millis(until_ms.saturating_sub(self.now()));
        tokio::select! {
            output = request => Some(output),
            _ = crate::executor::sleep(left) => None,
        }
    }

    /// Whether the delayed leave `info` describes is due a restart at `now`: a
    /// pending retry once it is due, otherwise [`LOCAL_RESTART_PERCENT`] into
    /// the delay after the last confirmed restart.
    fn restart_due(&self, info: &KeepAliveInfo, now: u64) -> bool {
        if legacy_policy::enabled() {
            return now >= self.legacy_restart_at();
        }
        match self.retry.lock().unwrap().retry_due_at_ms() {
            Some(due) => now >= due,
            None => now >= Self::restart_at(info),
        }
    }

    /// Under [`legacy_policy`]: one attempt per beat, successful or not.
    fn legacy_restart_at(&self) -> u64 {
        self.last_attempt_ms.load(Ordering::Relaxed) + legacy_policy::INTERVAL_MS
    }

    fn restart_at(info: &KeepAliveInfo) -> u64 {
        info.last_restart_ms
            .saturating_add(info.timeout_ms * LOCAL_RESTART_PERCENT / 100)
    }

    /// Whether a loss is to be announced now: true once per machine, the
    /// first time it is asked after the membership was lost.
    pub(crate) fn take_loss_report(&self) -> bool {
        self.state() == OwnMembershipState::Lost && !self.loss_reported.swap(true, Ordering::SeqCst)
    }

    /// The membership is gone; see [`OwnMembershipState::Lost`].
    pub(crate) fn lose(&self, why: &str) {
        *self.state.lock().unwrap() = OwnMembershipState::Lost;
        log::warn!(
            "[{}] our membership is lost: {why}. Nothing more is sent for this join.",
            self.room_id,
        );
    }

    /// When the next [`Self::keep_alive`] has something to do for the delayed
    /// leave — a restart, a retry, the delay's deadline, an arm — or `None`
    /// while not joined.
    ///
    /// The sticky refresh is not in it: its lifetime is an hour, and it is
    /// checked on every wake-up, which the upkeep's interval caps. A refresh
    /// that drove the wake-ups would turn a tiny lifetime into a busy loop.
    pub fn next_due_at_ms(&self) -> Option<u64> {
        if self.state() != OwnMembershipState::Joined {
            return None;
        }
        let now = self.now();
        let mut due: Vec<u64> = Vec::new();
        match self.keep_alive_info.lock().unwrap().as_ref() {
            Some(info) if legacy_policy::enabled() => {
                due.push(self.legacy_restart_at());
                due.push(info.last_restart_ms.saturating_add(info.timeout_ms));
            }
            Some(info) => {
                let retry = self.retry.lock().unwrap().retry_due_at_ms();
                due.push(retry.unwrap_or_else(|| Self::restart_at(info)));
                due.push(info.last_restart_ms.saturating_add(info.timeout_ms));
            }
            None if self.delayed_leave_probe_due(now) => due.push(now),
            None => {
                if let Some(retry) = self.retry.lock().unwrap().retry_due_at_ms()
                    && !matches!(
                        self.delayed_leave_support(),
                        DelayedLeaveSupport::Unsupported {
                            permanent: true,
                            ..
                        }
                    )
                {
                    due.push(retry);
                }
            }
        }
        due.into_iter().min()
    }

    /// How long until [`Self::next_due_at_ms`], at most `cap`; `cap` while
    /// nothing is due. Never less than [`MIN_WAKE_MS`], so nothing that keeps
    /// being due can turn the upkeep into a busy loop.
    pub fn next_due_in(&self, cap: std::time::Duration) -> std::time::Duration {
        let floor = std::time::Duration::from_millis(MIN_WAKE_MS);
        let wait = match self.next_due_at_ms() {
            Some(due) => std::time::Duration::from_millis(due.saturating_sub(self.now())),
            None => cap,
        };
        wait.min(cap).max(floor)
    }

    /// Whether it is worth asking the homeserver for a delayed leave again:
    /// never once it refused in as many words, otherwise when the backoff
    /// allows.
    fn delayed_leave_probe_due(&self, now: u64) -> bool {
        match self.delayed_leave_support() {
            DelayedLeaveSupport::Unsupported {
                permanent: true, ..
            } => false,
            _ => self.retry.lock().unwrap().may_attempt(now),
        }
    }

    /// Re-sends our membership event once the sticky entry is halfway to
    /// expiring, keeping us in the map for another full duration.
    ///
    /// Halfway rather than at the brink so a single failed refresh (or a
    /// missed heartbeat) is survivable: there is a whole half-duration of
    /// further attempts before the entry actually lapses. Content is re-sent
    /// verbatim, so peers see an update identical to what they already hold.
    ///
    /// Fire-and-forget like the rest of the heartbeat — the next tick retries.
    async fn refresh_sticky_if_due(&self) {
        let Some(sticky) = self.last_sticky.lock().unwrap().clone() else {
            // Not joined (or the join's sticky send failed): nothing to refresh.
            return;
        };

        // The lifetime the join settled on, which is both when the refresh falls
        // due and what it re-publishes — a refresh that stated a different
        // duration is what MSC4354 asks clients not to do.
        let lifetime_ms = self.membership_lifetime_ms();
        let now = self.now();
        let elapsed = now.saturating_sub(sticky.sent_at_ms);
        if elapsed < lifetime_ms / 2 || !self.sticky_retry.lock().unwrap().may_attempt(now) {
            return;
        }

        let room_id = self.room_id.clone();
        log::debug!(
            "[{room_id}] Refreshing sticky membership ({elapsed}ms of {lifetime_ms}ms elapsed)"
        );

        match self
            .backend
            .send_sticky_event(
                room_id.clone(),
                "m.rtc.member".to_string(),
                sticky.content.clone(),
                lifetime_ms,
            )
            .await
        {
            // The refresh replaces our entry in the sticky map, so from here on
            // *this* is the event a peer's reaction must relate to.
            Ok(event_id) => {
                self.sticky_retry.lock().unwrap().reset();
                self.latest_event_id.send_replace(Some(event_id));
                let mut guard = self.last_sticky.lock().unwrap();
                // Only advance the clock if we are still tracking the same
                // content: a concurrent join/leave may have replaced it while
                // this send was in flight, and stamping our timestamp onto
                // theirs would delay their refresh.
                if let Some(current) = guard.as_mut()
                    && current.content == sticky.content
                {
                    current.sent_at_ms = self.now();
                }
            }
            Err(error) => {
                self.sticky_retry.lock().unwrap().failed(now);
                log::warn!(
                    "[{room_id}] Failed to refresh sticky membership: {error:?}. Retrying on \
                     the backoff.",
                );
            }
        }
    }

    /// Schedules a delayed leave event to clean up our membership if we disconnect.
    ///
    /// This is used internally by join() and keep_alive().
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the delayed event was scheduled successfully.
    /// Returns an error if scheduling failed.
    async fn schedule_delayed_leave(&self) -> Result<(), CommandError> {
        let room_id = self.room_id.clone();
        let slot_id = self.slot_id.clone();
        let sticky_key = self.sticky_key.clone();
        let keep_alive_timeout_ms = self.keep_alive_timeout_ms;

        log::trace!(
            "[{}] Scheduling new delayed leave (timeout: {}ms)",
            room_id,
            keep_alive_timeout_ms
        );

        // Use MSC4143-compliant content
        let delayed_content = self.build_delayed_leave_content(&slot_id, &sticky_key);

        // Schedule the delayed event and await its completion
        let armed_at = self.now();
        let delayed_event_id = self
            .backend
            .send_delayed_event(
                room_id.clone(),
                "m.rtc.member".to_string(),
                None,
                delayed_content,
                keep_alive_timeout_ms,
            )
            .await?;

        // Store the event ID
        {
            *self.delayed_support.lock().unwrap() = DelayedLeaveSupport::Supported;
            self.retry.lock().unwrap().reset();
            let mut info_guard = self.keep_alive_info.lock().unwrap();
            *info_guard = Some(KeepAliveInfo {
                delayed_event_id,
                timeout_ms: keep_alive_timeout_ms,
                last_restart_ms: armed_at,
            });
            self.last_attempt_ms.store(armed_at, Ordering::Relaxed);
        }

        log::trace!("[{}] Delayed leave scheduled successfully", room_id);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::host::backend::{
        MockBackend, MockDelayFailure, NoopBackend, ToDeviceDelivery, ToDeviceRecipient,
    };
    use crate::transport::RawRtcTransport;

    const APPLICATION_TYPE: &str = "m.call";

    /// When a default local switch's restart falls due after the last one.
    const RESTART_POINT_MS: u64 = DEFAULT_KEEP_ALIVE_TIMEOUT_MS * LOCAL_RESTART_PERCENT / 100;

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
            .join(MemberTransports::default())
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
            .join(MemberTransports::default())
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

        machine.join(transport).await.expect("join should succeed");

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
            .join(MemberTransports::default())
            .await
            .expect("join should succeed");

        let delay_id = machine
            .delayed_event_id()
            .expect("join arms a delayed leave");

        machine.advance_clock_ms(RESTART_POINT_MS);
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
    /// us marked as departed mid-call.
    #[tokio::test]
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
            .join(MemberTransports::default())
            .await
            .expect("join should succeed");
        let delay_id = machine.delayed_event_id().expect("armed by join");

        // The mock fails every restart. The delay is well inside its period,
        // so it cannot have fired yet: keep it and retry on the backoff.
        machine.advance_clock_ms(RESTART_POINT_MS);
        machine.keep_alive().await;

        assert_eq!(*sender.scheduled.lock().unwrap(), 1, "no replacement armed");
        assert_eq!(machine.delayed_event_id(), Some(delay_id));
        assert_eq!(machine.state(), OwnMembershipState::Joined);
    }

    /// The restart of a local switch falls due 30 % into its delay, not on
    /// every wake-up.
    #[tokio::test]
    async fn a_local_restart_falls_due_at_thirty_percent_of_the_delay() {
        let mock = Arc::new(MockBackend::new());
        let machine = test_machine(mock.clone());
        machine.join(MemberTransports::default()).await.unwrap();

        machine.advance_clock_ms(RESTART_POINT_MS - 100);
        machine.keep_alive().await;
        assert!(
            mock.restarted_events.lock().unwrap().is_empty(),
            "not due yet"
        );

        machine.advance_clock_ms(100);
        machine.keep_alive().await;
        assert_eq!(mock.restarted_events.lock().unwrap().len(), 1);
    }

    /// A restart the homeserver cannot answer is retried on the backoff, and
    /// the retry is never planned past the delay's deadline.
    #[tokio::test]
    async fn a_failed_restart_is_retried_before_the_deadline() {
        let mock = Arc::new(MockBackend::new());
        let machine = test_machine(mock.clone());
        machine.join(MemberTransports::default()).await.unwrap();
        *mock.restart_failure.lock().unwrap() = Some(MockDelayFailure::Unreachable);

        let joined_at = machine.now();
        machine.advance_clock_ms(RESTART_POINT_MS);
        machine.keep_alive().await;

        let due = machine.next_due_at_ms().expect("still joined");
        let now = machine.now();
        assert!(due > now, "the retry waits");
        assert!(
            due <= now + 1_500,
            "the first retry is within the first backoff step"
        );
        assert!(due <= joined_at + DEFAULT_KEEP_ALIVE_TIMEOUT_MS);
    }

    /// Past the delay's deadline with no confirmed restart, our leave is out:
    /// the membership is lost, and nothing is sent for it — the homeserver is
    /// most likely unreachable.
    #[tokio::test]
    async fn a_switch_not_restarted_within_its_delay_loses_the_membership() {
        let mock = Arc::new(MockBackend::new());
        let machine = test_machine(mock.clone());
        machine.join(MemberTransports::default()).await.unwrap();
        *mock.restart_failure.lock().unwrap() = Some(MockDelayFailure::Unreachable);
        let delay_id = machine.delayed_event_id().unwrap();
        let sent_before = mock.sticky_events.lock().unwrap().len();

        machine.advance_clock_ms(DEFAULT_KEEP_ALIVE_TIMEOUT_MS);
        machine.keep_alive().await;

        assert_eq!(machine.state(), OwnMembershipState::Lost);
        assert_eq!(machine.next_due_at_ms(), None, "nothing more to do");
        assert_eq!(
            machine.delayed_event_id(),
            Some(delay_id),
            "kept for a rejoin"
        );
        assert_eq!(
            mock.delayed_events.lock().unwrap().len(),
            1,
            "no replacement"
        );
        assert!(
            mock.cancelled_events.lock().unwrap().is_empty(),
            "nothing cancelled"
        );
        assert_eq!(
            mock.sticky_events.lock().unwrap().len(),
            sent_before,
            "no leave sent"
        );
    }

    /// A homeserver behind a partition never answers. A restart waiting on it
    /// must not carry us past the deadline: the membership is lost on time.
    #[tokio::test(start_paused = true)]
    async fn a_restart_that_never_returns_loses_the_membership_at_the_deadline() {
        let mock = Arc::new(MockBackend::new());
        let machine = test_machine(mock.clone());
        machine.join(MemberTransports::default()).await.unwrap();
        mock.restart_hangs.store(true, Ordering::Relaxed);

        machine.advance_clock_ms(RESTART_POINT_MS);
        machine.keep_alive().await;

        assert_eq!(machine.state(), OwnMembershipState::Lost);
        assert_eq!(
            mock.restarted_events.lock().unwrap().len(),
            1,
            "it was tried"
        );
    }

    /// The send timing against a homeserver that refuses every restart: the
    /// first attempt at the restart point, then jittered retries whose waits
    /// never exceed half the time left — denser as the deadline nears — all
    /// before the deadline, and nothing once the membership is lost.
    #[tokio::test]
    async fn failed_restarts_are_retried_ever_denser_until_the_deadline() {
        let mock = Arc::new(MockBackend::new());
        let machine = test_machine(mock.clone());
        machine.join(MemberTransports::default()).await.unwrap();
        *mock.restart_failure.lock().unwrap() = Some(MockDelayFailure::Unreachable);
        let joined_at = machine.now();
        let deadline = joined_at + DEFAULT_KEEP_ALIVE_TIMEOUT_MS;

        let mut attempts = Vec::new();
        while machine.state() == OwnMembershipState::Joined {
            let due = machine.next_due_at_ms().expect("joined");
            machine.advance_clock_ms(due.saturating_sub(machine.now()));
            let before = mock.restarted_events.lock().unwrap().len();
            machine.keep_alive().await;
            if mock.restarted_events.lock().unwrap().len() > before {
                attempts.push(machine.now());
            }
        }

        assert_eq!(machine.state(), OwnMembershipState::Lost);
        assert!(
            attempts[0].abs_diff(joined_at + RESTART_POINT_MS) <= 100,
            "first attempt at the restart point: {attempts:?}",
        );
        assert!(attempts.len() >= 5, "retried, not given up: {attempts:?}");
        assert!(attempts.iter().all(|t| *t < deadline), "{attempts:?}");
        for pair in attempts.windows(2) {
            let (prev, next) = (pair[0], pair[1]);
            let gap = next - prev;
            let ceiling = ((deadline - prev) / 2).max(MIN_RETRY_MS) + 100;
            assert!(gap <= ceiling, "gap {gap} after {prev} exceeds {ceiling}");
            assert!(gap >= 400, "gap {gap} below the backoff's floor");
        }

        let total = mock.restarted_events.lock().unwrap().len();
        machine.advance_clock_ms(60_000);
        machine.keep_alive().await;
        assert_eq!(
            mock.restarted_events.lock().unwrap().len(),
            total,
            "nothing after the loss"
        );
    }

    /// One request at a time: a second wake-up while a restart hangs waits for
    /// it rather than sending another.
    #[tokio::test(start_paused = true)]
    async fn never_two_restarts_in_flight() {
        let mock = Arc::new(MockBackend::new());
        let machine = test_machine(mock.clone());
        machine.join(MemberTransports::default()).await.unwrap();
        mock.restart_hangs.store(true, Ordering::Relaxed);
        machine.advance_clock_ms(RESTART_POINT_MS);

        tokio::join!(machine.keep_alive(), machine.keep_alive());

        assert_eq!(mock.restarted_events.lock().unwrap().len(), 1);
        assert_eq!(machine.state(), OwnMembershipState::Lost);
    }

    /// A restart answered with `M_NOT_FOUND` is hard evidence that the delay
    /// fired: the membership is lost at once.
    #[tokio::test]
    async fn a_restart_the_homeserver_cannot_find_loses_the_membership() {
        let mock = Arc::new(MockBackend::new());
        let machine = test_machine(mock.clone());
        machine.join(MemberTransports::default()).await.unwrap();
        *mock.restart_failure.lock().unwrap() = Some(MockDelayFailure::Gone);

        machine.advance_clock_ms(RESTART_POINT_MS);
        machine.keep_alive().await;

        assert_eq!(machine.state(), OwnMembershipState::Lost);
    }

    /// A join that supersedes a lost one retires the old delay first, so it
    /// cannot fire after the join and end it.
    #[tokio::test]
    async fn a_superseded_delay_is_retired_before_the_join() {
        let mock = Arc::new(MockBackend::new());
        let machine = test_machine(mock.clone());
        machine.supersede_delayed_leave("old-delay".to_owned());

        machine.join(MemberTransports::default()).await.unwrap();

        assert_eq!(
            *mock.sent_now_events.lock().unwrap(),
            vec![("!room:example.org".to_owned(), "old-delay".to_owned())],
            "the old leave is sent, ending the old membership cleanly",
        );
        assert!(mock.cancelled_events.lock().unwrap().is_empty());
        assert_eq!(machine.state(), OwnMembershipState::Joined);
    }

    /// A host that cannot send a delay now cancels it instead: as safe
    /// against the old leave landing after the join.
    #[tokio::test]
    async fn a_host_without_send_now_cancels_the_superseded_delay() {
        // `CancelFailsSender` leaves `send_delayed_event_now` at its default and
        // answers every cancel with a plain error, so reaching that error is
        // the proof the fallback ran.
        let sender = Arc::new(CancelFailsSender::default());
        let machine = OwnMembershipMachine::with_default_timeout(
            sender.clone(),
            "!room:example.org".to_string(),
            "m.call#room".to_string(),
            "alice-device-a".to_string(),
            APPLICATION_TYPE.to_string(),
        );
        machine.supersede_delayed_leave("old-delay".to_owned());

        let error = machine
            .join(MemberTransports::default())
            .await
            .expect_err("the cancel failed, so the join waits");
        assert!(error.to_string().contains("M_NOT_FOUND"), "{error}");
        assert_eq!(*sender.scheduled.lock().unwrap(), 0, "nothing armed");
    }

    /// An old delay the homeserver no longer has has fired: as good as retired.
    #[tokio::test]
    async fn a_superseded_delay_that_already_fired_does_not_stop_the_join() {
        let mock = Arc::new(MockBackend::new());
        *mock.send_now_failure.lock().unwrap() = Some(MockDelayFailure::Gone);
        let machine = test_machine(mock.clone());
        machine.supersede_delayed_leave("old-delay".to_owned());

        machine.join(MemberTransports::default()).await.unwrap();

        assert_eq!(machine.state(), OwnMembershipState::Joined);
    }

    /// While the old delay cannot be retired, joining could be undone by it:
    /// the join fails, sends nothing, and keeps it for the retry.
    #[tokio::test]
    async fn a_join_waits_until_the_superseded_delay_is_retired() {
        let mock = Arc::new(MockBackend::new());
        *mock.send_now_failure.lock().unwrap() = Some(MockDelayFailure::Unreachable);
        let machine = test_machine(mock.clone());
        machine.supersede_delayed_leave("old-delay".to_owned());

        assert!(machine.join(MemberTransports::default()).await.is_err());
        assert_eq!(machine.state(), OwnMembershipState::NotJoined);
        assert!(mock.delayed_events.lock().unwrap().is_empty());
        assert!(mock.sticky_events.lock().unwrap().is_empty());

        // The caller retries with the delay it still holds, as a rejoin does.
        *mock.send_now_failure.lock().unwrap() = None;
        machine.supersede_delayed_leave("old-delay".to_owned());
        machine.join(MemberTransports::default()).await.unwrap();
        assert_eq!(mock.sent_now_events.lock().unwrap().len(), 2, "asked again");
    }

    /// The upkeep sleeps until something is due: the restart while all is
    /// well; the retry, and in any case the deadline, after a failure.
    #[tokio::test]
    async fn next_due_names_the_restart_then_the_retry() {
        let mock = Arc::new(MockBackend::new());
        let machine = test_machine(mock.clone());
        machine.join(MemberTransports::default()).await.unwrap();
        let joined_at = machine.now();

        let due = machine.next_due_at_ms().unwrap();
        assert!(due.abs_diff(joined_at + RESTART_POINT_MS) <= 100, "{due}");

        *mock.restart_failure.lock().unwrap() = Some(MockDelayFailure::Unreachable);
        machine.advance_clock_ms(RESTART_POINT_MS);
        machine.keep_alive().await;
        let retry = machine.next_due_at_ms().unwrap();
        assert!(retry < joined_at + DEFAULT_KEEP_ALIVE_TIMEOUT_MS);
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
    /// trick [`heartbeat_refreshes_the_sticky_membership_once_half_expired`]
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
            .join(MemberTransports::default())
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
            .join(MemberTransports::default())
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
            .join(MemberTransports::default())
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
            .join(MemberTransports::default())
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
            .join(MemberTransports::default())
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
            .join(MemberTransports::default())
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
            .join(MemberTransports::default())
            .await
            .expect("join should succeed");
        assert!(!machine.delayed_leave_supported());

        // Past the first backoff step, then let the homeserver start
        // accepting them.
        machine.advance_clock_ms(2_000);
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
            .join(MemberTransports::default())
            .await
            .expect("join should succeed");

        machine.leave(None).await.expect("leave should succeed");

        assert_eq!(machine.state(), OwnMembershipState::Left);
        assert_eq!(*sender.cancels.lock().unwrap(), 0);
    }

    /// A join that never published a membership is not a join. Leaving the
    /// state at `Joining` would have the heartbeat arming delayed leaves for a
    /// membership nobody can see.
    #[tokio::test]
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

        machine
            .join(MemberTransports::default())
            .await
            .expect_err("the membership send fails");

        assert_eq!(machine.state(), OwnMembershipState::NotJoined);
    }

    /// The sticky entry expires independently of the delayed leave, so a call
    /// running past its lifetime must re-announce the membership.
    #[tokio::test]
    async fn heartbeat_refreshes_the_sticky_membership_once_half_expired() {
        let mock_sender = Arc::new(MockBackend::new());
        // A zero lifetime is always at least half expired.
        let machine = test_machine_with_sticky_duration(mock_sender.clone(), 0);
        machine
            .join(MemberTransports::default())
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
            .join(MemberTransports::default())
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
            .join(MemberTransports::default())
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
            .join(MemberTransports::default())
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
            .join(MemberTransports::default())
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
