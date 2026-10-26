// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The one retry layer for every send the library makes on its own initiative
//! (034 R1), and the signalling health those sends observe (R9, R10).
//!
//! One [`Outbox`] per client: one homeserver, one health. A send names its
//! [`SlotKey`]; a slot holds at most one pending send, a newer send of the same
//! key supersedes it, and never two attempts of one slot are in flight at once.
//! Failures are classified by [`CommandError::is_permanent`] (R3): a refusal is
//! never retried and never touches health; anything else is retried on capped
//! backoff, or after the server's retry-after (R7). Each attempt is bounded by
//! [`ATTEMPT_TIMEOUT`] (R2): the library cannot abort a host's call, only stop
//! waiting for it.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::watch;

use crate::encryption::RtcClock;
use crate::error::CommandError;
use crate::executor::{self, Instant};
use crate::host::backend::BackendError;

#[cfg(test)]
mod tests;

/// How long the library waits for one attempt before counting it as a
/// transient failure with no code (R2).
pub const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);
/// The first backoff step after a transient failure.
pub const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
/// The backoff cap.
pub const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// What a send is, for supersession: one pending send per kind and scope.
///
/// A kind is a slot, so sends that must not overtake one another share one: a
/// leave's membership supersedes a pending refresh and waits for one in flight,
/// and a fresh arm supersedes a pending restart of the old delayed leave.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SendKind {
    /// Our membership event: the join, each refresh and republish, the leave.
    Membership,
    /// Our delayed leave: arming, restarting and cancelling it.
    DelayedLeave,
    /// The leave for a previous participation of this device that never left.
    SupersededLeave {
        sticky_key: String,
    },
    /// The media key to one member.
    KeyDistribution {
        member_id: String,
    },
    Notification,
    Hand,
    Reaction,
    /// The homeserver's `rtc_transports`: a read, retried only as the first
    /// step of a join, under its budget.
    RtcTransports,
}

/// A slot: a [`SendKind`] within a scope, such as one room's slot session.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SlotKey {
    pub scope: String,
    pub kind: SendKind,
}

impl SlotKey {
    pub fn new(scope: impl Into<String>, kind: SendKind) -> Self {
        Self {
            scope: scope.into(),
            kind,
        }
    }
}

/// How long a send may keep trying, and what it can tolerate.
#[derive(Clone, Debug)]
pub struct SendPolicy {
    deadline: Option<Duration>,
    tolerance: Option<Duration>,
    retry_timed_out: bool,
    single_attempt: bool,
}

impl Default for SendPolicy {
    fn default() -> Self {
        Self {
            deadline: None,
            tolerance: None,
            retry_timed_out: true,
            single_attempt: false,
        }
    }
}

impl SendPolicy {
    /// Retries until it lands, is superseded or is cancelled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Gives up `deadline` after the send starts.
    pub fn deadline(mut self, deadline: Duration) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// How long the send can wait before waiting itself does harm, such as
    /// a membership lapsing. A rate limit that outlasts it reports signalling
    /// unreachable (R7); the send is still scheduled at the retry-after.
    pub fn tolerance(mut self, tolerance: Duration) -> Self {
        self.tolerance = Some(tolerance);
        self
    }

    /// An attempt that timed out may have landed: for a send whose duplicate
    /// is wrong (a reaction), stop rather than retry (R4).
    pub fn never_retry_a_timed_out_attempt(mut self) -> Self {
        self.retry_timed_out = false;
        self
    }

    /// One attempt, never retried: a leave (R8). It still waits for the slot's
    /// attempt in flight, so it cannot overtake an earlier send of its kind.
    pub fn single_attempt() -> Self {
        Self {
            single_attempt: true,
            retry_timed_out: false,
            ..Self::default()
        }
    }
}

/// Why a send through the [`Outbox`] did not land.
#[derive(Debug, thiserror::Error)]
pub enum SendFailure {
    /// A permanent failure (R3); never retried.
    #[error("refused: {0}")]
    Refused(CommandError),
    /// A newer send of the same slot took over.
    #[error("superseded by a newer send of the same kind")]
    Superseded,
    /// The slot was cancelled: a leave, or the recipient leaving.
    #[error("cancelled")]
    Cancelled,
    /// The policy's deadline passed first; `last` is the last failure seen.
    #[error("not sent within its deadline")]
    DeadlineElapsed { last: Option<CommandError> },
    /// An attempt did not answer in time and the policy forbids a retry,
    /// because it may have landed.
    #[error("the attempt timed out and may have landed")]
    TimedOut,
    /// The one attempt a single-attempt policy allows failed transiently.
    #[error("failed: {0}")]
    Failed(CommandError),
}

impl SendFailure {
    /// As the error the caller would have had from a single send.
    pub fn into_command_error(self) -> CommandError {
        match self {
            SendFailure::Refused(error) | SendFailure::Failed(error) => error,
            SendFailure::DeadlineElapsed { last: Some(error) } => error,
            other => CommandError::from_message(other.to_string()),
        }
    }

    /// The host's failure behind this, if one was reported.
    pub fn command_error(&self) -> Option<&CommandError> {
        match self {
            SendFailure::Refused(error) | SendFailure::Failed(error) => Some(error),
            SendFailure::DeadlineElapsed { last } => last.as_ref(),
            _ => None,
        }
    }
}

/// Whether the homeserver is answering our sends (R9). One per client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SignallingHealth {
    Reachable,
    /// Since the first transient failure or the host's offline report, in
    /// epoch milliseconds; stable until reachable again.
    Unreachable {
        since_ms: u64,
    },
}

/// The retry layer and health cell. Cheap to clone; clones share everything.
#[derive(Clone)]
pub struct Outbox {
    inner: Arc<Inner>,
}

struct Inner {
    slots: Mutex<HashMap<SlotKey, Slot>>,
    /// Bumped on every slot change and every "online", so a waiting send
    /// re-reads its slot.
    poke: watch::Sender<u64>,
    /// Bumped by each "online" report, so a send in backoff retries at once.
    online_epoch: AtomicU64,
    health: watch::Sender<SignallingHealth>,
    /// Bumped on every unreachable → reachable transition, so a recovery is
    /// never missed when the health watch coalesces a quick flap.
    recoveries: watch::Sender<u64>,
    clock: RtcClock,
}

#[derive(Default)]
struct Slot {
    /// The send that owns the slot; any older one has ended.
    generation: u64,
    /// Sends up to this generation ended by cancellation, not supersession.
    cancelled_through: u64,
    in_flight: bool,
    /// Sends of this slot still running, ended or not.
    waiting: usize,
}

impl Default for Outbox {
    fn default() -> Self {
        Self::new()
    }
}

impl Outbox {
    pub fn new() -> Self {
        Self::with_clock(Arc::new(crate::own_membership::now_ms))
    }

    /// An outbox whose health timestamps come from `clock` (see [`RtcClock`]).
    pub fn with_clock(clock: RtcClock) -> Self {
        Self {
            inner: Arc::new(Inner {
                slots: Mutex::default(),
                poke: watch::Sender::new(0),
                online_epoch: AtomicU64::new(0),
                health: watch::Sender::new(SignallingHealth::Reachable),
                recoveries: watch::Sender::new(0),
                clock,
            }),
        }
    }

    /// The clock health timestamps come from, which a membership machine
    /// sending through this outbox shares.
    pub fn clock(&self) -> RtcClock {
        self.inner.clock.clone()
    }

    /// Signalling health now.
    pub fn health(&self) -> SignallingHealth {
        *self.inner.health.borrow()
    }

    /// Signalling health, current value first.
    pub fn subscribe_health(&self) -> watch::Receiver<SignallingHealth> {
        self.inner.health.subscribe()
    }

    /// Counts the times signalling became reachable again; a change is a
    /// recovery to run (R19), however quickly it followed the loss.
    pub fn subscribe_recoveries(&self) -> watch::Receiver<u64> {
        self.inner.recoveries.subscribe()
    }

    /// The host reports its Matrix client offline or online (R11). Offline
    /// sets signalling unreachable; online sets it reachable and retries every
    /// send in backoff at once (R10). Reporting the current value does nothing.
    pub fn report_connectivity(&self, online: bool) {
        if !online {
            self.mark_unreachable();
        } else if self.mark_reachable() {
            self.inner.online_epoch.fetch_add(1, Ordering::SeqCst);
            self.poke();
        }
    }

    /// Ends the pending send of `key`, if any: it resolves
    /// [`SendFailure::Cancelled`]. An attempt in flight runs to its outcome,
    /// which is ignored.
    pub fn cancel(&self, key: &SlotKey) {
        self.cancel_where(|slot| slot == key);
    }

    /// Ends every pending send of `scope`, as [`Self::cancel`] does: a leave.
    pub fn cancel_scope(&self, scope: &str) {
        self.cancel_where(|slot| slot.scope == scope);
    }

    /// Sends through the slot `key`, retrying transient failures under
    /// `policy`. `attempt` makes one attempt each time it is called.
    ///
    /// Resolves with the first success, or why there was none. Dropping the
    /// future stops the retries; it does not supersede anything.
    pub async fn send<T, F, Fut>(
        &self,
        key: SlotKey,
        policy: SendPolicy,
        attempt: F,
    ) -> Result<T, SendFailure>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, CommandError>>,
    {
        self.begin(key).run(policy, attempt).await
    }

    /// Takes the slot `key` now, superseding its pending send, and returns the
    /// send to [`PendingSend::run`]. Synchronous, so a caller can decide and
    /// register under one lock: a membership machine checks it is still joined
    /// in the same critical section as a leave's [`Self::cancel_scope`].
    pub fn begin(&self, key: SlotKey) -> PendingSend {
        let generation = self.register(&key);
        PendingSend {
            outbox: self.clone(),
            key,
            generation,
        }
    }

    async fn run<T, F, Fut>(
        &self,
        key: &SlotKey,
        generation: u64,
        policy: SendPolicy,
        mut attempt: F,
    ) -> Result<T, SendFailure>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, CommandError>>,
    {
        let key = key.clone();
        let started = Instant::now();
        let deadline = policy.deadline.map(|deadline| started + deadline);
        let tolerance = policy.tolerance.map(|tolerance| started + tolerance);
        let mut poke = self.inner.poke.subscribe();
        let mut backoff = BACKOFF_INITIAL;
        let mut last = None;
        let mut attempts = 0u32;

        loop {
            // One attempt in flight per slot: wait out an older send's.
            loop {
                match self.claim(&key, generation) {
                    Claim::Ours => break,
                    Claim::Ended(failure) => return Err(failure),
                    Claim::Busy => {
                        if !self.wait(&mut poke, deadline).await {
                            return Err(SendFailure::DeadlineElapsed { last });
                        }
                    }
                }
            }
            let budget = deadline.map_or(ATTEMPT_TIMEOUT, |deadline| {
                ATTEMPT_TIMEOUT.min(deadline.saturating_duration_since(Instant::now()))
            });
            attempts += 1;
            let attempt_started = Instant::now();
            let outcome = {
                let _flight = InFlight {
                    outbox: self,
                    key: &key,
                };
                executor::timeout(budget, attempt()).await
            };
            log_attempt(&key, attempts, attempt_started.elapsed(), budget, &outcome);
            if let Some(failure) = self.ended(&key, generation) {
                return Err(failure);
            }

            let delay = match outcome {
                Ok(Ok(value)) => {
                    self.mark_reachable();
                    return Ok(value);
                }
                Ok(Err(error)) if error.is_permanent() => {
                    return Err(SendFailure::Refused(error));
                }
                Ok(Err(error)) if policy.single_attempt => {
                    if !error
                        .backend_error()
                        .is_some_and(BackendError::is_rate_limited)
                    {
                        self.mark_unreachable();
                    }
                    return Err(SendFailure::Failed(error));
                }
                Ok(Err(error)) => {
                    let rate_limited = error
                        .backend_error()
                        .filter(|error| error.is_rate_limited());
                    let delay = match rate_limited {
                        Some(error) => {
                            let delay = error.retry_after_ms.map_or(backoff, Duration::from_millis);
                            // The server answered: only a wait the send cannot
                            // afford makes signalling unreachable (R7).
                            if tolerance.is_some_and(|tolerance| Instant::now() + delay > tolerance)
                            {
                                self.mark_unreachable();
                            }
                            delay
                        }
                        None => {
                            self.mark_unreachable();
                            backoff
                        }
                    };
                    log::debug!("outbox: {key} retrying in {delay:?}");
                    last = Some(error);
                    delay
                }
                Err(_elapsed) => {
                    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                        return Err(SendFailure::DeadlineElapsed { last });
                    }
                    self.mark_unreachable();
                    if !policy.retry_timed_out {
                        return Err(SendFailure::TimedOut);
                    }
                    log::debug!("outbox: {key} retrying in {backoff:?}");
                    backoff
                }
            };
            backoff = (backoff * 2).min(BACKOFF_MAX);

            let wake_at = Instant::now() + delay;
            let online_epoch = self.inner.online_epoch.load(Ordering::SeqCst);
            loop {
                let until_wake = wake_at.saturating_duration_since(Instant::now());
                let until_deadline =
                    deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));
                if until_deadline.is_some_and(|left| left <= until_wake) {
                    // The next attempt would start past the deadline.
                    let left = until_deadline.unwrap_or_default();
                    tokio::select! {
                        _ = executor::sleep(left) => {
                            return Err(SendFailure::DeadlineElapsed { last });
                        }
                        _ = poke.changed() => {}
                    }
                } else {
                    tokio::select! {
                        _ = executor::sleep(until_wake) => break,
                        _ = poke.changed() => {}
                    }
                }
                if let Some(failure) = self.ended(&key, generation) {
                    return Err(failure);
                }
                if self.inner.online_epoch.load(Ordering::SeqCst) != online_epoch {
                    break;
                }
            }
        }
    }

    fn slots(&self) -> MutexGuard<'_, HashMap<SlotKey, Slot>> {
        self.inner
            .slots
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn poke(&self) {
        self.inner.poke.send_modify(|n| *n = n.wrapping_add(1));
    }

    /// Takes the slot for a new send, superseding any pending one.
    fn register(&self, key: &SlotKey) -> u64 {
        let generation = {
            let mut slots = self.slots();
            let slot = slots.entry(key.clone()).or_default();
            slot.generation += 1;
            slot.waiting += 1;
            slot.generation
        };
        self.poke();
        generation
    }

    fn cancel_where(&self, matches: impl Fn(&SlotKey) -> bool) {
        let mut any = false;
        for (_, slot) in self.slots().iter_mut().filter(|(key, _)| matches(key)) {
            slot.cancelled_through = slot.generation;
            slot.generation += 1;
            any = true;
        }
        if any {
            self.poke();
        }
    }

    fn ended(&self, key: &SlotKey, generation: u64) -> Option<SendFailure> {
        let slots = self.slots();
        let slot = slots.get(key)?;
        if slot.generation == generation {
            None
        } else if generation <= slot.cancelled_through {
            Some(SendFailure::Cancelled)
        } else {
            Some(SendFailure::Superseded)
        }
    }

    fn claim(&self, key: &SlotKey, generation: u64) -> Claim {
        if let Some(failure) = self.ended(key, generation) {
            return Claim::Ended(failure);
        }
        let mut slots = self.slots();
        let slot = slots.entry(key.clone()).or_default();
        if slot.in_flight {
            Claim::Busy
        } else {
            slot.in_flight = true;
            Claim::Ours
        }
    }

    /// Waits for a slot change; `false` if the deadline passed first.
    async fn wait(&self, poke: &mut watch::Receiver<u64>, deadline: Option<Instant>) -> bool {
        match deadline {
            Some(deadline) => {
                let left = deadline.saturating_duration_since(Instant::now());
                tokio::select! {
                    _ = poke.changed() => true,
                    _ = executor::sleep(left) => false,
                }
            }
            None => {
                let _ = poke.changed().await;
                true
            }
        }
    }

    /// Drops a slot nobody is using any more.
    fn release(&self, key: &SlotKey, update: impl FnOnce(&mut Slot)) {
        {
            let mut slots = self.slots();
            if let Some(slot) = slots.get_mut(key) {
                update(slot);
                if slot.waiting == 0 && !slot.in_flight {
                    slots.remove(key);
                }
            }
        }
        self.poke();
    }

    fn mark_unreachable(&self) {
        let since_ms = (self.inner.clock)();
        self.inner.health.send_if_modified(|health| {
            if *health == SignallingHealth::Reachable {
                log::info!("outbox: signalling unreachable since {since_ms}");
                *health = SignallingHealth::Unreachable { since_ms };
                true
            } else {
                false
            }
        });
    }

    /// `true` if this changed the health.
    fn mark_reachable(&self) -> bool {
        let recovered = self.inner.health.send_if_modified(|health| {
            if *health == SignallingHealth::Reachable {
                false
            } else {
                log::info!("outbox: signalling reachable again");
                *health = SignallingHealth::Reachable;
                true
            }
        });
        if recovered {
            self.inner
                .recoveries
                .send_modify(|n| *n = n.wrapping_add(1));
        }
        recovered
    }
}

/// One line per attempt, whatever its outcome: kind, attempt number, how long
/// the host took and what it answered. Real calls' rageshakes carry it, so the
/// attempt timeout and backoff can be tuned on client-side latencies rather
/// than the homeserver's own send time.
fn log_attempt<T>(
    key: &SlotKey,
    attempt: u32,
    took: Duration,
    budget: Duration,
    outcome: &Result<Result<T, CommandError>, executor::Elapsed>,
) {
    let took_ms = took.as_millis();
    match outcome {
        Ok(Ok(_)) => log::info!("outbox: {key} attempt {attempt} landed in {took_ms} ms"),
        Ok(Err(error)) => {
            let class = if error.is_permanent() {
                "refused"
            } else {
                "failed"
            };
            log::info!("outbox: {key} attempt {attempt} {class} in {took_ms} ms: {error}");
        }
        Err(_) => log::info!(
            "outbox: {key} attempt {attempt} did not answer within {} ms",
            budget.as_millis()
        ),
    }
}

impl std::fmt::Display for SlotKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            SendKind::KeyDistribution { member_id } => {
                write!(f, "[{}] key to {member_id}", self.scope)
            }
            kind => write!(f, "[{}] {kind:?}", self.scope),
        }
    }
}

enum Claim {
    Ours,
    Busy,
    Ended(SendFailure),
}

/// A send that has taken its slot ([`Outbox::begin`]) and not yet run. It
/// counts on the slot until dropped, so the slot is dropped with the last.
pub struct PendingSend {
    outbox: Outbox,
    key: SlotKey,
    generation: u64,
}

impl PendingSend {
    /// Runs the send; see [`Outbox::send`].
    pub async fn run<T, F, Fut>(self, policy: SendPolicy, attempt: F) -> Result<T, SendFailure>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, CommandError>>,
    {
        self.outbox
            .run(&self.key, self.generation, policy, attempt)
            .await
    }
}

impl Drop for PendingSend {
    fn drop(&mut self) {
        self.outbox.release(&self.key, |slot| {
            slot.waiting = slot.waiting.saturating_sub(1)
        });
    }
}

/// Marks the slot's attempt in flight until the attempt ends or is dropped.
struct InFlight<'a> {
    outbox: &'a Outbox,
    key: &'a SlotKey,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.outbox.release(self.key, |slot| slot.in_flight = false);
    }
}
