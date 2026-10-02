// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The task that keeps one joined slot session alive, spawned by
//! [`SlotSession::join`](crate::SlotSession::join) on the
//! [`executor`](crate::executor) and stopped by the leave, by dropping the
//! session, or through its [`AbortHandle`](crate::executor::AbortHandle).
//!
//! Two wake-ups: the keep-alive interval (restart the delayed leave, refresh
//! the sticky membership), and the deadline of a coalesced key rotation,
//! followed through the encryption manager's watch so a member who left during
//! a key's window is locked out when the window ends. It holds the membership
//! machine and the encryption manager, never the room: a tick does not stall
//! anything else the room does.

use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::encryption::EncryptionManager;
use crate::executor::{self, AbortOnDrop, JoinHandleExt};
use crate::host::backend::MatrixBackend;
use crate::own_membership::{OwnMembershipMachine, OwnMembershipState, now_ms};

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

        loop {
            let due = rotation_due
                .as_ref()
                .and_then(|due| *due.borrow())
                .filter(|due| Some(*due) != flushed_due);
            let due_in = due.map(|due| Duration::from_millis(due.saturating_sub(now_ms())));

            tokio::select! {
                _ = &mut beat => {
                    tick(&machine, encryption.as_ref(), &log_tag).await;
                    if machine.state() != OwnMembershipState::Joined {
                        break;
                    }
                    flushed_due = None;
                    beat.set(executor::sleep(interval));
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
            }
        }
        log::debug!("[{log_tag}] upkeep stopped");
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
    }
}

async fn flush<T: MatrixBackend + 'static>(encryption: &EncryptionManager<T>, log_tag: &str) {
    if let Err(error) = encryption.flush_due_rotation().await {
        log::warn!("[{log_tag}] a deferred key rotation failed: {error:?}");
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
