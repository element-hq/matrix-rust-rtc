// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The task that keeps one joined slot session alive, spawned by
//! [`SlotSession::join`](crate::SlotSession::join) on the
//! [`executor`](crate::executor) and stopped by the leave, by dropping the
//! session, or through its [`AbortHandle`](crate::executor::AbortHandle).
//!
//! Two wake-ups: whenever the membership machine has something due (restart
//! the delayed leave, retry, refresh the sticky membership, notice it timed
//! out), at most the keep-alive interval apart; and the deadline of a
//! coalesced key rotation,
//! followed through the encryption manager's watch so a member who left during
//! a key's window is locked out when the window ends. It holds the membership
//! machine and the encryption manager, never the room: a tick does not stall
//! anything else the room does.

use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, watch};

use crate::encryption::EncryptionManager;
use crate::executor::{self, AbortOnDrop, JoinHandleExt};
use crate::host::backend::MatrixBackend;
use crate::own_membership::{OwnMembershipMachine, OwnMembershipState, now_ms};
use crate::session::LeaveReason;

/// What a joined slot session's upkeep needs.
pub(crate) struct SessionUpkeep<T: MatrixBackend + 'static> {
    pub machine: Arc<OwnMembershipMachine<T>>,
    pub encryption: Option<EncryptionManager<T>>,
    /// The longest the upkeep sleeps with nothing due.
    pub interval: Duration,
    pub log_tag: String,
    /// Where the session learns that its membership was lost.
    pub auto_leaves: broadcast::Sender<LeaveReason>,
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
            auto_leaves,
        } = self;
        let mut rotation_due = encryption
            .as_ref()
            .map(EncryptionManager::subscribe_rotation_due);
        // The deadline a flush last answered, so one that leaves it in place
        // (the flush failed) waits for the next tick instead of spinning.
        let mut flushed_due: Option<u64> = None;
        let mut beat = pin!(executor::sleep(machine.next_due_in(interval)));

        loop {
            let due = rotation_due
                .as_ref()
                .and_then(|due| *due.borrow())
                .filter(|due| Some(*due) != flushed_due);
            let due_in = due.map(|due| Duration::from_millis(due.saturating_sub(now_ms())));

            tokio::select! {
                _ = &mut beat => {
                    tick(&machine, encryption.as_ref(), &log_tag, &auto_leaves).await;
                    if machine.state() != OwnMembershipState::Joined {
                        break;
                    }
                    flushed_due = None;
                    beat.set(executor::sleep(machine.next_due_in(interval)));
                }
                _ = executor::sleep(due_in.unwrap_or_default()), if due_in.is_some() => {
                    if let Some(encryption) = &encryption
                        && !flush(&machine, encryption, &log_tag).await
                    {
                        // The delay's deadline passed while the flush waited
                        // on the homeserver: let the machine notice.
                        tick(&machine, None, &log_tag, &auto_leaves).await;
                        if machine.state() != OwnMembershipState::Joined {
                            break;
                        }
                    }
                    flushed_due = due;
                }
                changed = changed(&mut rotation_due) => {
                    if !changed {
                        rotation_due = None;
                    }
                }
            }
        }
        log::debug!("[{log_tag}] upkeep stopped");
    }
}

/// One keep-alive tick: the membership machine's, then a due rotation, so a
/// missed deadline wake-up makes a rotation late by one tick, never lost.
///
/// A membership the tick found lost is reported at once, and nothing else is
/// sent for it: the homeserver is most likely unreachable, and a key flush
/// waiting on it would only delay the news.
pub(crate) async fn tick<T: MatrixBackend + 'static>(
    machine: &OwnMembershipMachine<T>,
    encryption: Option<&EncryptionManager<T>>,
    log_tag: &str,
    auto_leaves: &broadcast::Sender<LeaveReason>,
) {
    log::trace!("[{log_tag}] keep-alive");
    machine.keep_alive().await;
    report_if_lost(machine, auto_leaves);
    if machine.state() != OwnMembershipState::Joined {
        return;
    }
    if let Some(encryption) = encryption
        && !flush(machine, encryption, log_tag).await
    {
        machine.keep_alive().await;
        report_if_lost(machine, auto_leaves);
    }
}

/// Tells the session its membership was lost, once the machine says so: the
/// session ends with [`LeaveCode::MembershipLost`](crate::LeaveCode), carrying
/// the delay a later join must retire. Nothing is sent to the homeserver.
pub(crate) fn report_if_lost<T: MatrixBackend + 'static>(
    machine: &OwnMembershipMachine<T>,
    auto_leaves: &broadcast::Sender<LeaveReason>,
) {
    if machine.take_loss_report() {
        let _ = auto_leaves.send(LeaveReason::membership_lost(
            machine.delayed_event_id(),
            "our membership left the call while we were still in it",
        ));
    }
}

/// Flushes a due key rotation, bounded by the delay's deadline like every
/// request around the keep-alive; `false` if the deadline came first.
async fn flush<T: MatrixBackend + 'static>(
    machine: &OwnMembershipMachine<T>,
    encryption: &EncryptionManager<T>,
    log_tag: &str,
) -> bool {
    match machine
        .within_deadline(encryption.flush_due_rotation())
        .await
    {
        Some(Ok(())) => true,
        Some(Err(error)) => {
            log::warn!("[{log_tag}] a deferred key rotation failed: {error:?}");
            true
        }
        None => {
            log::warn!("[{log_tag}] a key rotation was still waiting at the delay's deadline");
            false
        }
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
