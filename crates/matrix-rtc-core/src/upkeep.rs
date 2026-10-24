// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The task that keeps one joined slot session alive, spawned by
//! [`SlotSession::join`](crate::SlotSession::join) on the
//! [`executor`](crate::executor) and stopped by the leave, by dropping the
//! session, or through its [`AbortHandle`](crate::executor::AbortHandle).
//!
//! Its wake-ups: the keep-alive interval (restart the delayed leave, refresh
//! the sticky membership), the moment our membership would lapse (034 R17),
//! signalling coming back (a recovery, R19), a republish the catch-up asked
//! for, a member a key rollout did not reach (one retry each, R32), and the
//! deadline of a coalesced key rotation,
//! followed through the encryption manager's watch so a member who left during
//! a key's window is locked out when the window ends. It holds the membership
//! machine and the encryption manager, never the room: a tick does not stall
//! anything else the room does.

use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tokio::sync::watch;

use crate::encryption::{EncryptionManager, KeyRetry, Undelivered};
use crate::executor::{self, AbortOnDrop, JoinHandleExt};
use crate::host::backend::MatrixBackend;
use crate::outbox::{BACKOFF_INITIAL, BACKOFF_MAX, SendFailure, SendKind, SendPolicy, SlotKey};
use crate::own_membership::{OwnMembershipMachine, OwnMembershipState};

/// What a joined slot session's upkeep needs.
pub(crate) struct SessionUpkeep<T: MatrixBackend + 'static> {
    pub machine: Arc<OwnMembershipMachine<T>>,
    pub encryption: Option<EncryptionManager<T>>,
    pub interval: Duration,
    pub log_tag: String,
}

impl<T: MatrixBackend + 'static> SessionUpkeep<T> {
    /// Starts it, or returns `None` with nowhere to run it (natively, no
    /// current tokio runtime): the host then ticks
    /// [`SlotSession::keep_alive`](crate::SlotSession::keep_alive) itself.
    pub fn spawn(self) -> Option<AbortOnDrop<()>> {
        if !executor::can_spawn() {
            log::warn!(
                "[{}] no runtime to run the keep-alive on; the host must tick it",
                self.log_tag,
            );
            return None;
        }
        Some(executor::spawn(self.run()).abort_on_drop())
    }

    async fn run(self) {
        let Self {
            machine,
            encryption,
            interval,
            log_tag,
        } = self;
        let mut rotation_due = encryption
            .as_ref()
            .map(EncryptionManager::subscribe_rotation_due);
        // The deadline a flush last answered, so one that leaves it in place
        // (the flush failed) waits for the next tick instead of spinning.
        let mut flushed_due: Option<u64> = None;
        let mut beat = pin!(executor::sleep(interval));
        let mut recoveries = machine.outbox().subscribe_recoveries();
        let mut republish_requests = machine.subscribe_republish_requests();
        let mut departures = machine.subscribe_departures();

        // The membership's sends, each retried through the outbox for as long
        // as it takes, run side by side: a refresh retrying through an outage
        // must not hold back the delayed leave's restart, nor the beat.
        let mut restart = pin!(None);
        let mut refresh = pin!(None);
        let mut recover = pin!(None);
        let mut republish = pin!(None);
        let mut departure = pin!(None);

        // One retry per member a rollout did not reach, side by side, so a
        // member who stays unreachable holds back nobody else's key.
        let mut undelivered = encryption
            .as_ref()
            .map(EncryptionManager::subscribe_undelivered);
        let mut key_retries = FuturesUnordered::new();
        let mut retrying = HashSet::new();
        let start_key_retries =
            |undelivered: &mut watch::Receiver<BTreeMap<String, Undelivered>>,
             retrying: &mut HashSet<String>,
             key_retries: &mut FuturesUnordered<_>| {
                let Some(encryption) = &encryption else {
                    return;
                };
                let owed: Vec<String> = undelivered.borrow_and_update().keys().cloned().collect();
                for member_id in owed {
                    if retrying.insert(member_id.clone()) {
                        key_retries.push(retry_key(
                            encryption.clone(),
                            machine.outbox().clone(),
                            machine.scope().to_owned(),
                            member_id,
                        ));
                    }
                }
            };
        if let Some(undelivered) = &mut undelivered {
            start_key_retries(undelivered, &mut retrying, &mut key_retries);
        }

        loop {
            let due = rotation_due
                .as_ref()
                .and_then(|due| *due.borrow())
                .filter(|due| Some(*due) != flushed_due);
            // The manager's deadline is on the outbox's clock, as the machine's is.
            let due_in = due.map(|due| Duration::from_millis(due.saturating_sub(machine.now_ms())));
            let lapse_in = (!machine.is_lapsed())
                .then(|| machine.lapse_at_ms())
                .flatten()
                .map(|at| Duration::from_millis(at.saturating_sub(machine.now_ms())));

            tokio::select! {
                _ = &mut beat => {
                    if machine.state() != OwnMembershipState::Joined {
                        break;
                    }
                    log::trace!("[{log_tag}] keep-alive");
                    // A restart still retrying is left to land; one whose delay
                    // has certainly fired by now gives way to the re-arm.
                    if restart.is_none() || machine.delayed_leave_overdue() {
                        restart.set(Some(machine.restart_delayed_leave()));
                    }
                    if refresh.is_none() {
                        refresh.set(Some(machine.refresh_if_due()));
                    }
                    if let Some(encryption) = &encryption {
                        flush(encryption, &log_tag).await;
                    }
                    machine.update_lapsed();
                    flushed_due = None;
                    beat.set(executor::sleep(interval));
                }
                () = drive(restart.as_mut()) => restart.set(None),
                () = drive(refresh.as_mut()) => refresh.set(None),
                () = drive(recover.as_mut()) => recover.set(None),
                () = drive(republish.as_mut()) => republish.set(None),
                () = drive(departure.as_mut()) => departure.set(None),
                _ = executor::sleep(lapse_in.unwrap_or_default()), if lapse_in.is_some() => {
                    machine.update_lapsed();
                }
                Ok(()) = recoveries.changed() => {
                    recoveries.mark_unchanged();
                    if recover.is_none() {
                        recover.set(Some(machine.recover()));
                    }
                }
                Ok(()) = departures.changed() => {
                    departures.mark_unchanged();
                    if departure.is_none() {
                        departure.set(Some(machine.recover_departure()));
                    }
                }
                Ok(()) = republish_requests.changed() => {
                    republish_requests.mark_unchanged();
                    if republish.is_none() {
                        republish.set(Some(machine.republish()));
                    }
                }
                _ = executor::sleep(due_in.unwrap_or_default()), if due_in.is_some() => {
                    if let Some(encryption) = &encryption {
                        flush(encryption, &log_tag).await;
                    }
                    flushed_due = due;
                }
                changed = changed(&mut rotation_due) => {
                    if !changed {
                        rotation_due = None;
                    }
                }
                Some(member_id) = key_retries.next(), if !key_retries.is_empty() => {
                    retrying.remove(&member_id);
                    if let Some(undelivered) = &mut undelivered {
                        // Still owed when a newer rollout missed them too.
                        start_key_retries(undelivered, &mut retrying, &mut key_retries);
                    }
                }
                changed = changed_map(&mut undelivered) => {
                    match (changed, &mut undelivered) {
                        (true, Some(receiver)) => {
                            start_key_retries(receiver, &mut retrying, &mut key_retries);
                        }
                        _ => undelivered = None,
                    }
                }
            }
        }
        log::debug!("[{log_tag}] upkeep stopped");
    }
}

/// Retries the key owed to one member on capped backoff until it is
/// delivered or nothing is owed any more (034 R32); resolves with the member.
///
/// Each attempt is one outbox attempt in the member's own slot, so a failed
/// host call counts towards signalling health and signalling coming back
/// retries at once. The backoff is ours: a host that answered without
/// delivering to this member says nothing about signalling.
async fn retry_key<T: MatrixBackend + 'static>(
    encryption: EncryptionManager<T>,
    outbox: crate::outbox::Outbox,
    scope: String,
    member_id: String,
) -> String {
    let key = SlotKey::new(
        scope,
        SendKind::KeyDistribution {
            member_id: member_id.clone(),
        },
    );
    let mut owed = encryption.subscribe_undelivered();
    let mut recoveries = outbox.subscribe_recoveries();
    recoveries.mark_unchanged();
    let mut backoff = BACKOFF_INITIAL;
    loop {
        let mut wait = pin!(executor::sleep(backoff));
        loop {
            tokio::select! {
                () = &mut wait => break,
                Ok(()) = recoveries.changed() => break,
                changed = owed.changed() => {
                    if changed.is_err() || !owed.borrow_and_update().contains_key(&member_id) {
                        return member_id;
                    }
                }
            }
        }

        let outcome = outbox
            .send(key.clone(), SendPolicy::single_attempt(), || {
                encryption.retry_undelivered_key(&member_id)
            })
            .await;
        match outcome {
            Ok(KeyRetry::Delivered | KeyRetry::Moot) => return member_id,
            Ok(KeyRetry::Undelivered) => {}
            Err(SendFailure::Failed(error)) => {
                if let Some(retry_after_ms) =
                    error.backend_error().and_then(|error| error.retry_after_ms)
                {
                    backoff = backoff.max(Duration::from_millis(retry_after_ms));
                }
            }
            Err(SendFailure::TimedOut) => {}
            Err(SendFailure::Refused(error)) => {
                log::warn!("key to member {member_id} refused for good, not retried: {error}");
                encryption.forget_undelivered(&member_id);
                return member_id;
            }
            // Cancelled by the leave, or superseded by a newer retry.
            Err(_) => return member_id,
        }
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

/// Runs the operation in `slot`, or never resolves while it is empty.
async fn drive<F: Future<Output = ()>>(slot: Pin<&mut Option<F>>) {
    match slot.as_pin_mut() {
        Some(operation) => operation.await,
        None => std::future::pending().await,
    }
}

/// One keep-alive tick: the membership machine's, then a due rotation, so a
/// missed deadline wake-up makes a rotation late by one tick, never lost.
pub(crate) async fn tick<T: MatrixBackend + 'static>(
    machine: &OwnMembershipMachine<T>,
    encryption: Option<&EncryptionManager<T>>,
    log_tag: &str,
) {
    log::trace!("[{log_tag}] keep-alive");
    machine.keep_alive().await;
    if let Some(encryption) = encryption {
        flush(encryption, log_tag).await;
        // With no upkeep running, the tick is the retry's backoff (034 R32).
        for member_id in encryption.undelivered_members() {
            let key = SlotKey::new(
                machine.scope(),
                SendKind::KeyDistribution {
                    member_id: member_id.clone(),
                },
            );
            let outcome = machine
                .outbox()
                .send(key, SendPolicy::single_attempt(), || {
                    encryption.retry_undelivered_key(&member_id)
                })
                .await;
            if let Err(SendFailure::Refused(_)) = outcome {
                encryption.forget_undelivered(&member_id);
            }
        }
    }
}

async fn flush<T: MatrixBackend + 'static>(encryption: &EncryptionManager<T>, log_tag: &str) {
    if let Err(error) = encryption.flush_due_rotation().await {
        log::warn!("[{log_tag}] a deferred key rotation failed: {error:?}");
    }
}

/// [`changed`] for the undelivered members.
async fn changed_map(
    receiver: &mut Option<watch::Receiver<BTreeMap<String, Undelivered>>>,
) -> bool {
    match receiver {
        Some(receiver) => receiver.changed().await.is_ok(),
        None => std::future::pending().await,
    }
}

/// Resolves on the next change; `false` once the sender is gone. Never
/// resolves without a receiver.
async fn changed(receiver: &mut Option<watch::Receiver<Option<u64>>>) -> bool {
    match receiver {
        Some(receiver) => receiver.changed().await.is_ok(),
        None => std::future::pending().await,
    }
}
