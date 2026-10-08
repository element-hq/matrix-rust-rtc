// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! When the dead man's switch is restarted, and how failures are retried.
//!
//! Everything here is decided by comparing timestamps when the session's
//! upkeep wakes, so it holds however punctually that happens.

use rand::Rng;

/// When our own restart of a local switch falls due, as a share of its delay:
/// at 30 % (9 s of the default 30 s). Early enough that a failed restart leaves
/// most of the delay for retries.
pub const LOCAL_RESTART_PERCENT: u64 = 30;

/// An exponential backoff schedule. The defaults are lk-jwt-service's
/// (`ExponentialBackoff::service_default`), so a client and the authorisation
/// service retry alike.
///
/// The jitter is not decoration: when a homeserver comes back after an outage,
/// every client of every call retries at once, and so does the key exchange
/// behind each rejoin. Spreading the attempts is what keeps the homeserver
/// from being hit in lockstep.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BackoffSchedule {
    /// The first wait, in milliseconds.
    pub initial_ms: u64,
    /// Growth per failed attempt.
    pub multiplier: f64,
    /// Jitter: each wait is drawn from `current ± randomization × current`.
    pub randomization: f64,
    /// Longest wait, jitter included, in milliseconds.
    pub max_ms: u64,
}

impl Default for BackoffSchedule {
    fn default() -> Self {
        Self {
            initial_ms: 1_000,
            multiplier: 1.5,
            randomization: 0.5,
            max_ms: 60_000,
        }
    }
}

/// A backoff in progress: how long to wait after each failure, and — for a
/// caller that compares timestamps — when the next attempt is due.
#[derive(Debug, Clone)]
pub struct Backoff {
    schedule: BackoffSchedule,
    current_ms: u64,
    next_attempt_ms: Option<u64>,
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new(BackoffSchedule::default())
    }
}

impl Backoff {
    /// A backoff on `schedule`, with nothing failed yet.
    pub fn new(schedule: BackoffSchedule) -> Self {
        Self {
            schedule,
            current_ms: schedule.initial_ms,
            next_attempt_ms: None,
        }
    }

    /// An attempt succeeded: back to the initial wait, nothing pending.
    pub fn reset(&mut self) {
        self.current_ms = self.schedule.initial_ms;
        self.next_attempt_ms = None;
    }

    /// An attempt failed: the jittered wait before the next one, in
    /// milliseconds. Each call grows the wait by the multiplier, up to the cap.
    pub fn next_wait_ms(&mut self) -> u64 {
        let base = self.current_ms as f64;
        let delta = self.schedule.randomization * base;
        let wait = if delta > 0.0 {
            rand::thread_rng().gen_range((base - delta)..=(base + delta))
        } else {
            base
        };
        self.current_ms =
            ((self.current_ms as f64 * self.schedule.multiplier) as u64).min(self.schedule.max_ms);
        (wait.max(0.0) as u64).min(self.schedule.max_ms)
    }

    /// An attempt failed at `now_ms`: the next one falls due after
    /// [`Self::next_wait_ms`].
    pub fn failed(&mut self, now_ms: u64) {
        let wait = self.next_wait_ms();
        self.next_attempt_ms = Some(now_ms.saturating_add(wait));
    }

    /// Bring the next attempt forward to `deadline_ms` if it would land after
    /// it and the deadline is still ahead: an attempt after the deadline is
    /// pointless, one at it may still count.
    pub fn clamp_to(&mut self, now_ms: u64, deadline_ms: u64) {
        if let Some(next) = self.next_attempt_ms
            && deadline_ms > now_ms
            && next > deadline_ms
        {
            self.next_attempt_ms = Some(deadline_ms);
        }
    }

    /// When a retry is due after a failure; `None` while nothing has failed.
    pub fn retry_due_at_ms(&self) -> Option<u64> {
        self.next_attempt_ms
    }

    /// Whether an attempt may be made at `now_ms`: nothing has failed, or the
    /// retry is due.
    pub fn may_attempt(&self, now_ms: u64) -> bool {
        self.next_attempt_ms.is_none_or(|due| now_ms >= due)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn without_jitter() -> Backoff {
        Backoff::new(BackoffSchedule {
            randomization: 0.0,
            ..BackoffSchedule::default()
        })
    }

    #[test]
    fn backoff_grows_by_the_multiplier_up_to_the_cap() {
        let mut backoff = without_jitter();
        let waits: Vec<u64> = (0..12).map(|_| backoff.next_wait_ms()).collect();
        assert_eq!(&waits[..4], &[1_000, 1_500, 2_250, 3_375]);
        assert_eq!(*waits.last().unwrap(), 60_000, "capped");
    }

    #[test]
    fn jitter_stays_within_the_band_and_the_cap() {
        let mut backoff = Backoff::default();
        let first = backoff.next_wait_ms();
        assert!((500..=1_500).contains(&first), "{first}");
        for _ in 0..20 {
            assert!(backoff.next_wait_ms() <= 60_000);
        }
    }

    #[test]
    fn a_retry_is_brought_forward_to_a_deadline_still_ahead() {
        let mut backoff = Backoff::new(BackoffSchedule {
            initial_ms: 10_000,
            randomization: 0.0,
            ..BackoffSchedule::default()
        });
        backoff.failed(0);
        backoff.clamp_to(0, 4_000);
        assert_eq!(backoff.retry_due_at_ms(), Some(4_000));

        // A deadline already behind us is left alone: clamping to it would
        // make the retry due on every wake-up.
        backoff.failed(5_000);
        backoff.clamp_to(5_000, 4_000);
        assert_eq!(backoff.retry_due_at_ms(), Some(20_000));
    }

    #[test]
    fn a_success_resets_the_schedule() {
        let mut backoff = without_jitter();
        backoff.failed(0);
        backoff.failed(0);
        assert!(!backoff.may_attempt(1_000));
        backoff.reset();
        assert_eq!(backoff.retry_due_at_ms(), None);
        assert!(backoff.may_attempt(0));
        backoff.failed(10);
        assert_eq!(backoff.retry_due_at_ms(), Some(1_010));
    }
}
