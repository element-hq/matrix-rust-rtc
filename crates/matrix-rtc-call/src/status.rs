// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! A joined call's one status ([`CallStatus`]): whether our media is up,
//! whether there is anyone to talk to, and why it ended.
//!
//! Two sources decide it. The call layer knows the participation and, in a DM,
//! the callee; the media layer knows our own connection. A host gating its
//! timer or its system call report on either alone gets it wrong: our media is
//! up seconds before a DM's callee answers. So they are combined here, once,
//! and a host reads the result.

use std::sync::{Arc, Mutex, PoisonError};

use matrix_rtc_core::{LeaveCode, LeaveReason};
use tokio::sync::watch;

use crate::direct_call::{DirectCallEnd, DirectCallState};
use crate::reactions::Clock;

/// Where a joined call is. Moves forward only, except that
/// [`Self::Connected`] goes back to [`Self::Connecting`] while media is
/// detached; [`Self::Ended`] is final.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallStatus {
    /// Our media is not up yet, and there is somebody to talk to: a group
    /// call, or a DM whose callee is already here.
    Connecting,
    /// A DM call waiting for the callee. `media` is how far our own side has
    /// got meanwhile: the ring goes out before our media is up.
    Calling {
        ring: RingPhase,
        media: MediaPhase,
    },
    /// Media up and somebody to talk to, since `since_ms`: the moment both
    /// were first true, which in a DM is the pickup at the earliest.
    Connected {
        since_ms: u64,
        degraded: bool,
    },
    Ended {
        reason: CallEnd,
    },
}

/// How far a DM call's ring has got.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RingPhase {
    /// The ring is being sent. The host shows it is calling, without a
    /// ringback yet.
    Sending,
    /// The ring went out at `sent_at_ms` and lasts `lifetime_ms`.
    Ringing { sent_at_ms: u64, lifetime_ms: u64 },
}

/// How far our own media has got.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MediaPhase {
    Connecting,
    /// `degraded` while a transport reconnects.
    Connected {
        degraded: bool,
    },
}

/// Why a call ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallEnd {
    /// We left, or the room was closed.
    Left,
    /// The callee declined our ring.
    Declined,
    /// Nobody answered before the ring ran out.
    Unanswered,
    /// The ring could not be sent in time.
    RingNotSent,
    /// Everyone else left a DM call.
    OthersLeft,
    /// A DM call joined without a ring had nobody in it.
    Empty,
    /// The slot was closed, which ended our participation without us asking.
    SlotClosed,
    /// The connection to our own focus closed and will not come back.
    ConnectionClosed { message: String },
}

impl From<DirectCallEnd> for CallEnd {
    fn from(value: DirectCallEnd) -> Self {
        match value {
            DirectCallEnd::Declined => Self::Declined,
            DirectCallEnd::Unanswered => Self::Unanswered,
            DirectCallEnd::RingNotSent => Self::RingNotSent,
            DirectCallEnd::OthersLeft => Self::OthersLeft,
            DirectCallEnd::Empty => Self::Empty,
            DirectCallEnd::Left => Self::Left,
        }
    }
}

/// Holds the inputs and publishes the status they make.
pub(crate) struct CallStatusCell {
    inputs: Mutex<Inputs>,
    status: watch::Sender<CallStatus>,
}

struct Inputs {
    clock: Clock,
    media: MediaPhase,
    /// `None` in a group call, which waits for nobody.
    direct: Option<DirectCallState>,
    connected_since_ms: Option<u64>,
    /// The first end reported, by whichever source.
    ended: Option<CallEnd>,
}

impl Inputs {
    fn status(&mut self) -> CallStatus {
        if let Some(reason) = &self.ended {
            return CallStatus::Ended {
                reason: reason.clone(),
            };
        }
        let ring = match &self.direct {
            Some(DirectCallState::Sending) => Some(RingPhase::Sending),
            Some(DirectCallState::Ringing {
                sent_at_ms,
                lifetime_ms,
            }) => Some(RingPhase::Ringing {
                sent_at_ms: *sent_at_ms,
                lifetime_ms: *lifetime_ms,
            }),
            Some(DirectCallState::InCall { .. } | DirectCallState::Ended { .. }) | None => None,
        };
        match (ring, self.media) {
            (Some(ring), media) => CallStatus::Calling { ring, media },
            (None, MediaPhase::Connecting) => CallStatus::Connecting,
            (None, MediaPhase::Connected { degraded }) => CallStatus::Connected {
                since_ms: *self
                    .connected_since_ms
                    .get_or_insert_with(|| (self.clock)()),
                degraded,
            },
        }
    }

    fn set_direct(&mut self, state: DirectCallState) {
        if let DirectCallState::Ended { reason } = &state {
            self.end((*reason).into());
        }
        self.direct = Some(state);
    }

    fn end(&mut self, reason: CallEnd) {
        self.ended.get_or_insert(reason);
    }
}

impl CallStatusCell {
    pub(crate) fn new(clock: Clock, direct: Option<DirectCallState>) -> Arc<Self> {
        let mut inputs = Inputs {
            clock,
            media: MediaPhase::Connecting,
            direct: None,
            connected_since_ms: None,
            ended: None,
        };
        if let Some(direct) = direct {
            inputs.set_direct(direct);
        }
        let status = inputs.status();
        Arc::new(Self {
            inputs: Mutex::new(inputs),
            status: watch::channel(status).0,
        })
    }

    pub(crate) fn status(&self) -> CallStatus {
        self.status.borrow().clone()
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<CallStatus> {
        self.status.subscribe()
    }

    /// Recomputes under the inputs' lock, so two reports cannot publish out
    /// of order.
    fn update(&self, f: impl FnOnce(&mut Inputs)) {
        let mut inputs = self.inputs.lock().unwrap_or_else(PoisonError::into_inner);
        f(&mut inputs);
        let next = inputs.status();
        self.status.send_if_modified(|status| {
            let changed = *status != next;
            *status = next;
            changed
        });
    }

    pub(crate) fn on_direct(&self, state: &DirectCallState) {
        self.update(|inputs| inputs.set_direct(state.clone()));
    }

    /// The participation ended. A reason already published stands: a DM's
    /// own lifecycle publishes its end before it leaves.
    pub(crate) fn on_session_ended(&self, reason: &LeaveReason) {
        let reason = match reason.code {
            LeaveCode::SlotClosed => CallEnd::SlotClosed,
            _ => CallEnd::Left,
        };
        self.update(|inputs| inputs.end(reason));
    }
}

/// What the media layer reports about our own connection. The call layer
/// cannot see it, and the status needs it.
#[derive(Clone)]
pub struct MediaStatusReporter {
    cell: Arc<CallStatusCell>,
}

impl MediaStatusReporter {
    pub(crate) fn new(cell: Arc<CallStatusCell>) -> Self {
        Self { cell }
    }

    /// A group call's reporter with no call behind it, and the status it
    /// drives.
    #[cfg(feature = "testing")]
    pub fn standalone() -> (Self, watch::Receiver<CallStatus>) {
        let cell = CallStatusCell::new(Arc::new(crate::now_ms), None);
        let status = cell.subscribe();
        (Self::new(cell), status)
    }

    /// Our own focus is connected, or there is none to connect: a
    /// receive-only call is up once attached.
    pub fn connected(&self) {
        self.set(MediaPhase::Connected { degraded: false });
    }

    /// Ignored until [`Self::connected`]: a peer's focus can reconnect while
    /// our own is still connecting.
    pub fn degraded(&self, degraded: bool) {
        self.cell.update(|inputs| {
            if let MediaPhase::Connected { .. } = inputs.media {
                inputs.media = MediaPhase::Connected { degraded };
            }
        });
    }

    /// Media was detached while we stay joined.
    pub fn detached(&self) {
        self.set(MediaPhase::Connecting);
    }

    /// The connection to our own focus closed for good, which ends the call.
    pub fn lost(&self, message: impl Into<String>) {
        let reason = CallEnd::ConnectionClosed {
            message: message.into(),
        };
        self.cell.update(|inputs| inputs.end(reason));
    }

    fn set(&self, media: MediaPhase) {
        self.cell.update(|inputs| inputs.media = media);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell(direct: Option<DirectCallState>) -> (Arc<CallStatusCell>, MediaStatusReporter) {
        let clock: Clock = Arc::new(|| 1_000);
        let cell = CallStatusCell::new(clock, direct);
        (cell.clone(), MediaStatusReporter::new(cell))
    }

    #[test]
    fn a_group_call_connects_with_its_media() {
        let (cell, media) = cell(None);
        assert_eq!(cell.status(), CallStatus::Connecting);
        media.connected();
        assert_eq!(
            cell.status(),
            CallStatus::Connected {
                since_ms: 1_000,
                degraded: false
            }
        );
    }

    /// Our media is up long before the callee answers, and the call is not
    /// connected until both are true.
    #[test]
    fn a_dm_call_connects_at_the_pickup_not_with_our_media() {
        let (cell, media) = cell(Some(DirectCallState::Sending));
        assert_eq!(
            cell.status(),
            CallStatus::Calling {
                ring: RingPhase::Sending,
                media: MediaPhase::Connecting
            }
        );
        media.connected();
        cell.on_direct(&DirectCallState::Ringing {
            sent_at_ms: 10,
            lifetime_ms: 90_000,
        });
        assert_eq!(
            cell.status(),
            CallStatus::Calling {
                ring: RingPhase::Ringing {
                    sent_at_ms: 10,
                    lifetime_ms: 90_000
                },
                media: MediaPhase::Connected { degraded: false }
            }
        );
        cell.on_direct(&DirectCallState::InCall { since_ms: 500 });
        assert!(matches!(cell.status(), CallStatus::Connected { .. }));
    }

    #[test]
    fn a_pickup_before_our_media_is_up_is_still_connecting() {
        let (cell, media) = cell(Some(DirectCallState::Sending));
        cell.on_direct(&DirectCallState::InCall { since_ms: 500 });
        assert_eq!(cell.status(), CallStatus::Connecting);
        media.connected();
        assert!(matches!(cell.status(), CallStatus::Connected { .. }));
    }

    /// The duration runs through a reconnect.
    #[test]
    fn degraded_media_keeps_the_connected_time() {
        let (cell, media) = cell(None);
        media.connected();
        media.degraded(true);
        assert_eq!(
            cell.status(),
            CallStatus::Connected {
                since_ms: 1_000,
                degraded: true
            }
        );
    }

    /// A DM's lifecycle publishes its end before it leaves, and the leave's
    /// own reason must not replace it.
    #[test]
    fn the_first_end_stands() {
        let (cell, media) = cell(Some(DirectCallState::Sending));
        cell.on_direct(&DirectCallState::Ended {
            reason: DirectCallEnd::Declined,
        });
        cell.on_session_ended(&LeaveReason::with_reason(LeaveCode::Leave, "declined"));
        media.lost("gone");
        assert_eq!(
            cell.status(),
            CallStatus::Ended {
                reason: CallEnd::Declined
            }
        );
    }

    #[test]
    fn a_closed_slot_and_a_lost_connection_say_so() {
        let (cell, _) = cell(None);
        cell.on_session_ended(&LeaveReason::new(LeaveCode::SlotClosed));
        assert_eq!(
            cell.status(),
            CallStatus::Ended {
                reason: CallEnd::SlotClosed
            }
        );

        let (cell, media) = self::cell(None);
        media.connected();
        media.lost("server went away");
        assert_eq!(
            cell.status(),
            CallStatus::Ended {
                reason: CallEnd::ConnectionClosed {
                    message: "server went away".to_owned()
                }
            }
        );
    }
}
