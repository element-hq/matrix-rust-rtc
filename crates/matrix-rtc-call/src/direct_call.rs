// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! One-to-one calls: what kind of call a join is ([`CallKind`]), and the
//! lifecycle of a call in a DM — ringing, picked up, declined (MSC4310),
//! unanswered, or left by the other person.
//!
//! The lifecycle is decided here, once, rather than by each host: who the
//! callee is, which declines count and when the ring has run out are exactly
//! the things two platforms would otherwise settle differently. A host reads
//! it as part of the call's [`crate::CallStatus`] and draws it; it never
//! computes it. Feature-hq spec 030 is the behaviour this implements.

use std::collections::HashSet;
use std::pin::pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use matrix_rtc_core::compat::DialectBackend;
use matrix_rtc_core::{JoinedMembership, MatrixBackend, RawTimelineEvent, executor};
use tokio::sync::watch;

use crate::notification::{
    Mentions, NOTIFICATION_EVENT_TYPE, NotificationType, NotifyConfig, build_notification_content,
    notification_sticky_duration_ms,
};
use crate::reactions::Clock;
use crate::status::CallStatusCell;

/// MSC4310's decline, under both spellings: hosts deliver the type as it was
/// sent, and clients send either.
pub const DECLINE_EVENT_TYPES: [&str; 2] = ["org.matrix.msc4310.rtc.decline", "m.rtc.decline"];

/// How long after our join the ring may take to go out before the call gives
/// up on it (spec 030 R4).
pub const RING_SEND_DEADLINE: Duration = Duration::from_secs(10);

/// Waits between attempts after a send that failed. The last one repeats.
const RING_RETRY_BACKOFF: [Duration; 3] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
];

/// Declines held while the ring is still being sent, before its event id is
/// known to match them against. A handful is plenty: only the callee's count.
const MAX_HELD_DECLINES: usize = 8;

/// What kind of call a join is. The type allows only what a call may do: a
/// group call cannot ring, and a DM call cannot take a group's notification.
#[derive(Clone, Debug)]
pub enum CallKind {
    /// Not a DM. Never rings; may tell the room with a plain notification.
    Group { notify: Option<GroupNotify> },
    /// A DM call, whose ring and pickup [`crate::CallStatus`] reports. With a
    /// ring, the user chose to call; without one, the call exists and we are
    /// joining it, to answer a ring or because it is in progress.
    Direct { ring: Option<RingConfig> },
}

impl Default for CallKind {
    /// A quiet group join.
    fn default() -> Self {
        Self::Group { notify: None }
    }
}

/// A group call's MSC4075 notification: a visual indication, never a ring.
#[derive(Clone, Debug, Default)]
pub struct GroupNotify {
    /// MSC4196 `m.call.intent`, e.g. `"audio"` or `"video"`.
    pub intent: Option<String>,
    pub lifetime_ms: Option<u64>,
    pub mentions: Mentions,
}

impl From<GroupNotify> for NotifyConfig {
    fn from(value: GroupNotify) -> Self {
        Self {
            notification_type: NotificationType::Notification,
            intent: value.intent,
            lifetime_ms: value.lifetime_ms,
            mentions: value.mentions,
        }
    }
}

/// The ring a DM call sends when the user chose to call.
#[derive(Clone, Debug, Default)]
pub struct RingConfig {
    /// How long the callee's phone rings, and how long we wait. `None` is
    /// [`crate::DEFAULT_RING_LIFETIME_MS`].
    pub lifetime_ms: Option<u64>,
    /// MSC4196 `m.call.intent`, e.g. `"audio"` or `"video"`.
    pub intent: Option<String>,
}

impl RingConfig {
    /// Always the whole room: the mention is not the host's to pick, so no
    /// platform can send a ring that addresses nobody (spec 030 R5).
    fn notify(&self, notification_type: NotificationType) -> NotifyConfig {
        NotifyConfig {
            notification_type,
            intent: self.intent.clone(),
            lifetime_ms: self.lifetime_ms,
            mentions: Mentions::default(),
        }
    }

    pub(crate) fn ring(&self) -> NotifyConfig {
        self.notify(NotificationType::Ring)
    }

    /// What a ring becomes in a DM that is not one-to-one (spec 030 R37).
    pub(crate) fn demoted(&self) -> NotifyConfig {
        self.notify(NotificationType::Notification)
    }
}

/// Why a DM call ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DirectCallEnd {
    /// The callee declined the ring.
    Declined,
    /// The ring ran out of lifetime unanswered.
    Unanswered,
    /// The ring could not be sent in time.
    RingNotSent,
    /// Everyone else left after someone had joined.
    OthersLeft,
    /// We joined an existing call and found nobody in it.
    Empty,
    /// We left, or the call ended for a reason that is not this lifecycle's.
    Left,
}

impl DirectCallEnd {
    /// The reason written on our leave, for logs and peers that show it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Declined => "declined",
            Self::Unanswered => "unanswered",
            Self::RingNotSent => "ring_not_sent",
            Self::OthersLeft => "others_left",
            Self::Empty => "empty",
            Self::Left => "left",
        }
    }
}

/// Where a DM call is. Moves forward only, and [`Self::Ended`] is final.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DirectCallState {
    /// The ring is being sent. The host shows it is calling, without a
    /// ringback yet.
    Sending,
    /// The ring went out at `sent_at_ms` and lasts `lifetime_ms`.
    Ringing {
        sent_at_ms: u64,
        lifetime_ms: u64,
    },
    /// The callee is here, since `since_ms`; for a call that never rang, since
    /// our join.
    InCall {
        since_ms: u64,
    },
    Ended {
        reason: DirectCallEnd,
    },
}

/// The one person a DM call rings: the room's only other joined member, or
/// `None` when there is not exactly one (spec 030 R35, R37).
pub(crate) fn callee_of(
    room_members: Option<&HashSet<String>>,
    own_user_id: &str,
) -> Option<String> {
    let mut others = room_members?.iter().filter(|user| *user != own_user_id);
    let callee = others.next()?;
    others.next().is_none().then(|| callee.clone())
}

/// A DM call's lifecycle, shared by the slot's call state (memberships,
/// declines) and the call's tasks (the ring, its timer, the leave).
pub(crate) struct DirectCall {
    machine: Mutex<Machine>,
    state: watch::Sender<DirectCallState>,
    /// Told every transition, under the machine's lock, so it sees them in
    /// order.
    status: Arc<CallStatusCell>,
}

struct Machine {
    tag: String,
    own_user_id: String,
    callee: String,
    clock: Clock,
    /// Whether anyone but us has been in the call. Only then can the call end
    /// because they all left (spec 030 R26, R28).
    other_user_seen: bool,
    /// Our ring, once sent: what a decline must relate to.
    ring_event_id: Option<String>,
    /// The callee's declines that arrived before `ring_event_id` was known.
    /// The sync carrying one can beat the response carrying the id.
    held_declines: Vec<RawTimelineEvent>,
}

impl DirectCall {
    pub(crate) fn new(
        tag: String,
        own_user_id: String,
        callee: String,
        clock: Clock,
        members: &[JoinedMembership],
        initial: DirectCallState,
        status: Arc<CallStatusCell>,
    ) -> Arc<Self> {
        log::info!("[{tag}] direct call with {callee}: {initial:?}");
        let other_user_seen = members.iter().any(|member| member.sender != own_user_id);
        Arc::new(Self {
            machine: Mutex::new(Machine {
                tag,
                own_user_id,
                callee,
                clock,
                other_user_seen,
                ring_event_id: None,
                held_declines: Vec::new(),
            }),
            state: watch::channel(initial).0,
            status,
        })
    }

    pub(crate) fn state(&self) -> DirectCallState {
        self.state.borrow().clone()
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<DirectCallState> {
        self.state.subscribe()
    }

    /// Every transition runs under the machine's lock, so the state read here
    /// is the one being replaced. An ended call takes none.
    fn update(&self, f: impl FnOnce(&mut Machine, &DirectCallState) -> Option<DirectCallState>) {
        let mut machine = self.machine.lock().unwrap_or_else(PoisonError::into_inner);
        let current = self.state.borrow().clone();
        if matches!(current, DirectCallState::Ended { .. }) {
            return;
        }
        if let Some(next) = f(&mut machine, &current) {
            log::info!("[{}] direct call: {current:?} -> {next:?}", machine.tag);
            self.status.on_direct(&next);
            self.state.send_replace(next);
        }
    }

    /// A slot's joined memberships, as published.
    pub(crate) fn on_roster(&self, members: &[JoinedMembership]) {
        self.update(|machine, current| {
            if members
                .iter()
                .any(|member| member.sender != machine.own_user_id)
            {
                machine.other_user_seen = true;
            }
            match current {
                // Any device of the callee's; never one of ours (R9).
                DirectCallState::Sending | DirectCallState::Ringing { .. }
                    if members.iter().any(|member| member.sender == machine.callee) =>
                {
                    Some(DirectCallState::InCall {
                        since_ms: (machine.clock)(),
                    })
                }
                // Users, not devices: our second device does not keep the call
                // alive after the other person hung up (R26).
                DirectCallState::InCall { .. }
                    if machine.other_user_seen
                        && members
                            .iter()
                            .all(|member| member.sender == machine.own_user_id) =>
                {
                    Some(DirectCallState::Ended {
                        reason: DirectCallEnd::OthersLeft,
                    })
                }
                _ => None,
            }
        });
    }

    /// Room events, timeline or sticky; anything but a decline from the callee
    /// is ignored, so a host may forward without filtering.
    pub(crate) fn on_room_events(&self, events: &[RawTimelineEvent]) {
        self.update(|machine, current| {
            // Only the callee's count (R10): matching the relation alone would
            // let anyone in the room, or another device of ours, end the call.
            let callee = machine.callee.clone();
            let declines = events.iter().filter(|event| {
                DECLINE_EVENT_TYPES.contains(&event.event_type.as_str()) && event.sender == callee
            });
            match current {
                DirectCallState::Sending => {
                    for decline in declines {
                        if machine.held_declines.len() < MAX_HELD_DECLINES
                            && !machine
                                .held_declines
                                .iter()
                                .any(|held| held.event_id == decline.event_id)
                        {
                            machine.held_declines.push(decline.clone());
                        }
                    }
                    None
                }
                DirectCallState::Ringing { .. } => {
                    let ring = machine.ring_event_id.as_deref()?;
                    declines
                        .into_iter()
                        .any(|decline| relates_to(decline, ring))
                        .then_some(DirectCallState::Ended {
                            reason: DirectCallEnd::Declined,
                        })
                }
                _ => None,
            }
        });
    }

    fn on_ring_sent(&self, event_id: String, sent_at_ms: u64, lifetime_ms: u64) {
        self.update(|machine, current| {
            if *current != DirectCallState::Sending {
                return None;
            }
            let declined = machine
                .held_declines
                .drain(..)
                .any(|decline| relates_to(&decline, &event_id));
            machine.ring_event_id = Some(event_id);
            Some(if declined {
                DirectCallState::Ended {
                    reason: DirectCallEnd::Declined,
                }
            } else {
                DirectCallState::Ringing {
                    sent_at_ms,
                    lifetime_ms,
                }
            })
        });
    }

    fn on_ring_not_sent(&self) {
        self.update(|_, current| {
            (*current == DirectCallState::Sending).then_some(DirectCallState::Ended {
                reason: DirectCallEnd::RingNotSent,
            })
        });
    }

    fn on_unanswered(&self) {
        self.update(|_, current| {
            matches!(current, DirectCallState::Ringing { .. }).then_some(DirectCallState::Ended {
                reason: DirectCallEnd::Unanswered,
            })
        });
    }

    /// Our participation ended for a reason that is not this lifecycle's.
    pub(crate) fn finish_left(&self) {
        self.update(|_, _| {
            Some(DirectCallState::Ended {
                reason: DirectCallEnd::Left,
            })
        });
    }
}

/// `m.relates_to` is an `m.reference` to `event_id`.
fn relates_to(event: &RawTimelineEvent, event_id: &str) -> bool {
    event.content.pointer("/m.relates_to/rel_type") == Some(&serde_json::json!("m.reference"))
        && event.content.pointer("/m.relates_to/event_id") == Some(&serde_json::json!(event_id))
}

/// Resolves once the state stops satisfying `stay`; never, if the call is gone.
async fn left_state(
    states: &mut watch::Receiver<DirectCallState>,
    stay: impl Fn(&DirectCallState) -> bool,
) {
    if states.wait_for(|state| !stay(state)).await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// Everything sending the ring needs, gathered at join under the room's lock
/// and run after it is released, so the room's intake — a pickup, a decline —
/// keeps flowing while the ring is retried.
pub(crate) struct RingJob {
    pub tag: String,
    pub room_id: String,
    pub notify: NotifyConfig,
    pub application_type: String,
    /// What the ring relates to (MSC4075).
    pub member_event_id: String,
    pub clock: Clock,
}

impl RingJob {
    /// Sends the ring, then times it. Started as soon as our membership is
    /// sent: nothing waits for media, so the callee's phone rings as early as
    /// it can.
    pub(crate) async fn run<B: MatrixBackend + 'static>(
        self,
        backend: Arc<DialectBackend<B>>,
        direct: Arc<DirectCall>,
    ) {
        let tag = &self.tag;
        let user_id = backend.own_user_id();
        let device_id = backend.own_device_id();
        let lifetime_ms = self.notify.lifetime_ms();
        let mut states = direct.subscribe();
        let mut deadline = pin!(executor::sleep(RING_SEND_DEADLINE));
        let mut attempt = 0;

        let event_id = loop {
            // Stamped per attempt: receivers count the lifetime from it, and a
            // retried ring with the first attempt's stamp rings short.
            let content = build_notification_content(
                &self.notify,
                &self.application_type,
                &user_id,
                &device_id,
                &self.member_event_id,
                (self.clock)(),
            );
            let send = backend.send_sticky_event(
                self.room_id.clone(),
                NOTIFICATION_EVENT_TYPE.to_owned(),
                content,
                notification_sticky_duration_ms(lifetime_ms),
            );
            let result = tokio::select! {
                result = send => result,
                // Never retried: the send may have landed (R3).
                () = &mut deadline => {
                    log::warn!("[{tag}] the ring was not sent within {RING_SEND_DEADLINE:?}");
                    direct.on_ring_not_sent();
                    return;
                }
                () = left_state(&mut states, |state| *state == DirectCallState::Sending) => return,
            };
            match result {
                Ok(event_id) => break event_id,
                // An error means the server did not store it, so a retry
                // cannot ring twice.
                Err(error) => {
                    let backoff = RING_RETRY_BACKOFF[attempt.min(RING_RETRY_BACKOFF.len() - 1)];
                    attempt += 1;
                    log::warn!("[{tag}] the ring was not sent ({error}); retrying in {backoff:?}");
                    tokio::select! {
                        () = executor::sleep(backoff) => {}
                        () = &mut deadline => {
                            log::warn!("[{tag}] the ring was not sent within {RING_SEND_DEADLINE:?}");
                            direct.on_ring_not_sent();
                            return;
                        }
                        () = left_state(&mut states, |state| *state == DirectCallState::Sending) => return,
                    }
                }
            }
        };

        log::info!("[{tag}] ringing ({event_id}) for {lifetime_ms}ms");
        direct.on_ring_sent(event_id, (self.clock)(), lifetime_ms);
        let ringing = |state: &DirectCallState| matches!(state, DirectCallState::Ringing { .. });
        if !ringing(&direct.state()) {
            return;
        }
        // The lifetime that was sent, so both phones stop together (R7).
        tokio::select! {
            () = executor::sleep(Duration::from_millis(lifetime_ms)) => direct.on_unanswered(),
            () = left_state(&mut states, ringing) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_callee_is_the_only_other_joined_member() {
        let members = |users: &[&str]| users.iter().map(|user| (*user).to_owned()).collect();
        let alice = "@alice:example.org";

        assert_eq!(
            callee_of(Some(&members(&[alice, "@bob:example.org"])), alice).as_deref(),
            Some("@bob:example.org")
        );
        assert_eq!(callee_of(Some(&members(&[alice])), alice), None);
        assert_eq!(
            callee_of(
                Some(&members(&[alice, "@bob:example.org", "@carol:example.org"])),
                alice
            ),
            None
        );
        assert_eq!(callee_of(None, alice), None);
    }
}
