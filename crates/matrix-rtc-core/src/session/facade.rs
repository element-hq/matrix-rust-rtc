// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Projects one [`RtcSession`] onto the participation facade
//! (see [`crate::participation`]) and publishes changes to its listener.
//!
//! A child module of `session` so it can read the session's private state
//! without widening any of it. Everything here is a *projection*: the roster,
//! the own-membership machine and the encryption manager stay authoritative,
//! and the snapshot is recomputed from them at every publish point.

use std::sync::{Arc, Mutex};

use super::{JoinCondition, RtcSession};
use crate::commands::RtcCommandSender;
use crate::own_membership::{OwnMembershipState, now_ms};
use crate::participation::{
    DelayedLeaveOutcome, DisconnectCause, EncryptionStatus, JoinExclusionReason, JoinProgress,
    JoinStatus, KeyMap, LeaveStatus, MediaKeyState, MembershipAttribution, MembershipState,
    ParticipationListener, ParticipationSnapshot, RosterPresence, SessionMembership, Status,
    TransportWithMembers, fire_all, fire_changes, media_key_impairments, membership_from,
    own_membership_impairments, sort_impairments, transports_from,
};
use crate::session::LeaveReason;

/// Which host call is in flight, for the two statuses that are only ever
/// observable while one is: `join()` and `leave()` run to completion under
/// the caller's borrow, so the machine's own transient states never reach a
/// publish point, and these are published explicitly instead.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Phase {
    #[default]
    Idle,
    Joining,
    Leaving,
}

/// Latched facts the projection needs that no other component keeps.
#[derive(Default)]
struct Tracking {
    phase: Phase,
    /// Why we are disconnected; `None` reads as [`DisconnectCause::NeverJoined`].
    last_disconnect: Option<DisconnectCause>,
    /// Whether our current membership has been seen in the roster at all —
    /// what distinguishes "echo not back yet" from "gone".
    own_seen_in_roster: bool,
    own_missing_since: Option<u64>,
    /// The key exchange with the members present at join completed once;
    /// later problems are per-tile state, not a return to `Joining`.
    encryption_settled: bool,
    /// How far the last failed join got, for [`DisconnectCause::JoinFailed`].
    join_progress_on_failure: JoinProgress,
}

/// The facade's own state on a session: its listener and what was last
/// published to it, so the next publish can diff against it.
#[derive(Default)]
pub(super) struct FacadeState {
    listener: Option<Arc<dyn ParticipationListener>>,
    /// The snapshot the listener last saw. A `Mutex` because `&self` inputs
    /// (a received key, a due rotation) publish too. Never held across an
    /// await, and never held while the listener runs.
    published: Mutex<ParticipationSnapshot>,
    tracking: Mutex<Tracking>,
}

impl<T: RtcCommandSender + 'static> RtcSession<T> {
    // ---- outputs ---------------------------------------------------------

    /// Every member as a host renders a tile: the joined roster, plus anyone
    /// gone who still holds our current media key. Sorted by member id.
    pub fn memberships(&self) -> Vec<SessionMembership> {
        let own_member_id = self.own_member_id().map(str::to_owned);
        let encryption = self
            .encryption_manager
            .as_ref()
            .filter(|manager| manager.manages_media_keys());
        let mapper = self
            .encryption_manager
            .as_ref()
            .and_then(|manager| manager.identity_mapper());
        let shared_with = encryption
            .and_then(|manager| manager.get_outbound_key())
            .map(|key| key.shared_with)
            .unwrap_or_default();
        let inbound = encryption
            .map(|manager| manager.get_all_inbound_keys())
            .unwrap_or_default();
        let rejections = encryption
            .map(|manager| manager.key_rejections())
            .unwrap_or_default();

        let mut out: Vec<SessionMembership> = self
            .members
            .iter()
            .map(|member| {
                let is_own = own_member_id.as_deref() == Some(member.member_id.as_str());
                let mut tile = membership_from(member, is_own);
                let device_id = match (&self.own_participation, is_own) {
                    (Some(own), true) => Some(own.device_id.as_str()),
                    _ => tile.device_id.as_deref(),
                };
                if let (Some(mapper), Some(device_id)) = (&mapper, device_id) {
                    tile.transport_identity =
                        Some(mapper(&tile.user_id, device_id, &tile.member_id));
                }
                if encryption.is_some() && !is_own {
                    let holds_our_key = shared_with.iter().any(|holder| {
                        holder.member_id == member.member_id
                            && holder.membership_ts == member.membership_ts
                    });
                    let have_their_key = inbound
                        .get(&member.member_id)
                        .is_some_and(|keys| !keys.is_empty());
                    let rejection = if have_their_key {
                        None
                    } else {
                        rejections
                            .get(&member.member_id)
                            .map(|rejected| rejected.reason.clone())
                    };
                    tile.media_key = Some(MediaKeyState {
                        holds_our_key,
                        have_their_key,
                        rejection,
                    });
                }
                tile
            })
            .collect();

        // Whoever was handed our current key and has since left the roster
        // can still decrypt us until the next rotation retires that key.
        for holder in &shared_with {
            let still_joined = self.members.iter().any(|member| {
                member.member_id == holder.member_id && member.membership_ts == holder.membership_ts
            });
            if still_joined || out.iter().any(|tile| tile.member_id == holder.member_id) {
                continue;
            }
            out.push(SessionMembership {
                member_id: holder.member_id.clone(),
                user_id: holder.user_id.clone(),
                device_id: (!holder.device_id.is_empty()).then(|| holder.device_id.clone()),
                attribution: MembershipAttribution::Unknown,
                membership_ts: holder.membership_ts,
                membership_event_id: None,
                application: None,
                published_transports: Vec::new(),
                can_subscribe: Vec::new(),
                is_own: false,
                transport_identity: None,
                state: MembershipState::LeftWithKeys,
                media_key: None,
            });
        }

        out.sort_by(|a, b| a.member_id.cmp(&b.member_id));
        out
    }

    /// One entry per distinct published transport, with the member ids on it.
    pub fn transports(&self) -> Vec<TransportWithMembers> {
        transports_from(&self.memberships())
    }

    /// Every media key held: ours under our member id, each peer's under theirs.
    pub fn key_map(&self) -> KeyMap {
        match self
            .encryption_manager
            .as_ref()
            .filter(|manager| manager.manages_media_keys())
        {
            Some(manager) => KeyMap::from_keys(
                self.own_member_id(),
                manager.get_outbound_key().as_ref(),
                &manager.get_all_inbound_keys(),
            ),
            None => KeyMap::default(),
        }
    }

    /// Where our own participation stands.
    pub fn participation_status(&self) -> Status {
        self.status_with(&self.memberships())
    }

    /// All four outputs at once.
    pub fn participation(&self) -> ParticipationSnapshot {
        self.snapshot()
    }

    /// Installs the change listener, replaying every output to it once.
    ///
    /// Replaces any previous listener. See [`crate::participation`] for the
    /// firing rule.
    pub fn set_participation_listener(&mut self, listener: Arc<dyn ParticipationListener>) {
        let current = self.snapshot();
        fire_all(listener.as_ref(), &current);
        *self.facade.published.lock().unwrap() = current;
        self.facade.listener = Some(listener);
    }

    /// Removes the change listener, if any.
    pub fn clear_participation_listener(&mut self) {
        self.facade.listener = None;
    }

    // ---- publishing ------------------------------------------------------

    /// Recomputes the snapshot and tells the listener about each output that
    /// changed since it last heard. Called at the end of every input that can
    /// move an output; cheap when nothing is listening.
    pub(super) fn publish_participation(&self) {
        let Some(listener) = &self.facade.listener else {
            return;
        };
        let current = self.snapshot();
        let previous = {
            let mut published = self.facade.published.lock().unwrap();
            if *published == current {
                return;
            }
            std::mem::replace(&mut *published, current.clone())
        };
        fire_changes(listener.as_ref(), &previous, &current);
    }

    fn snapshot(&self) -> ParticipationSnapshot {
        let memberships = self.memberships();
        let transports = transports_from(&memberships);
        let key_map = self.key_map();
        let status = self.status_with(&memberships);
        ParticipationSnapshot {
            memberships,
            transports,
            key_map,
            status,
        }
    }

    // ---- phase and cause bookkeeping, driven by join/leave ---------------

    pub(super) fn facade_begin_join(&self) {
        let mut tracking = self.facade.tracking.lock().unwrap();
        tracking.phase = Phase::Joining;
        tracking.own_seen_in_roster = false;
        tracking.own_missing_since = None;
        tracking.encryption_settled = false;
        tracking.join_progress_on_failure = JoinProgress::default();
    }

    /// Records how far a join got before its membership event failed.
    pub(super) fn facade_note_join_progress(&self, progress: JoinProgress) {
        self.facade
            .tracking
            .lock()
            .unwrap()
            .join_progress_on_failure = progress;
    }

    pub(super) fn facade_end_join(&self, error: Option<String>) {
        let mut tracking = self.facade.tracking.lock().unwrap();
        tracking.phase = Phase::Idle;
        if let Some(error) = error
            && self.own_membership_machine.is_none()
        {
            tracking.last_disconnect = Some(DisconnectCause::JoinFailed {
                at_ts: now_ms(),
                progress: tracking.join_progress_on_failure,
                error,
            });
        }
    }

    pub(super) fn facade_begin_leave(&self) {
        self.facade.tracking.lock().unwrap().phase = Phase::Leaving;
    }

    pub(super) fn facade_end_leave(
        &self,
        reason: Option<LeaveReason>,
        delayed_leave: Option<DelayedLeaveOutcome>,
        error: Option<String>,
    ) {
        let mut tracking = self.facade.tracking.lock().unwrap();
        tracking.phase = Phase::Idle;
        tracking.last_disconnect = Some(match error {
            None => DisconnectCause::LeftByHost {
                reason,
                delayed_leave,
            },
            Some(error) => DisconnectCause::LeaveFailed {
                at_ts: now_ms(),
                error,
            },
        });
    }

    // ---- status ----------------------------------------------------------

    fn status_with(&self, memberships: &[SessionMembership]) -> Status {
        let mut tracking = self.facade.tracking.lock().unwrap();
        match tracking.phase {
            Phase::Joining => {
                return Status::Joining(JoinStatus {
                    progress: JoinProgress::default(),
                    encryption: EncryptionStatus::NotManaged,
                    impairments: Vec::new(),
                });
            }
            Phase::Leaving => {
                return Status::Leaving(LeaveStatus {
                    leave_event_sent: false,
                    delayed_leave: None,
                    impairments: Vec::new(),
                });
            }
            Phase::Idle => {}
        }

        let disconnected = |tracking: &Tracking| {
            Status::Disconnected(
                tracking
                    .last_disconnect
                    .clone()
                    .unwrap_or(DisconnectCause::NeverJoined),
            )
        };
        let Some(machine) = self.own_membership_machine.as_ref() else {
            return disconnected(&tracking);
        };
        if machine.state() != OwnMembershipState::Joined {
            return disconnected(&tracking);
        }

        let keep_alive = machine.keep_alive_status();
        let membership = machine.membership_publication();
        let roster = self.roster_presence(machine.sticky_key(), &mut tracking);
        let encryption = self.encryption_status(memberships, &mut tracking);
        let rejections = self
            .encryption_manager
            .as_ref()
            .map(|manager| manager.key_rejections())
            .unwrap_or_default();

        let mut impairments = Vec::new();
        own_membership_impairments(&keep_alive, &membership, &roster, &mut impairments);
        media_key_impairments(
            memberships,
            |member_id| rejections.get(member_id).map(|rejected| rejected.at_ts),
            &mut impairments,
        );
        sort_impairments(&mut impairments);

        Status::Connected(crate::participation::ConnectedStatus {
            member_id: machine.sticky_key().to_owned(),
            membership_event_id: machine.membership_event_id(),
            keep_alive,
            membership,
            roster,
            encryption,
            impairments,
        })
    }

    /// Whether the session projects our own membership, and if not, why.
    fn roster_presence(&self, own_member_id: &str, tracking: &mut Tracking) -> RosterPresence {
        if self
            .members
            .iter()
            .any(|member| member.member_id == own_member_id)
        {
            tracking.own_seen_in_roster = true;
            tracking.own_missing_since = None;
            return RosterPresence::Present;
        }
        if let Some(candidate) = self
            .candidates
            .iter()
            .find(|candidate| candidate.member_id == own_member_id)
        {
            let reason = match self.join_condition(candidate) {
                JoinCondition::SlotClosed => JoinExclusionReason::SlotClosed,
                JoinCondition::UnencryptedInEncryptedRoom => {
                    JoinExclusionReason::UnencryptedInEncryptedRoom
                }
                JoinCondition::SenderNotInRoom => JoinExclusionReason::SenderNotInRoom,
                // A candidate under our current member id cannot be superseded,
                // and a joined one was caught above.
                JoinCondition::Joined | JoinCondition::SupersededOwnParticipation => {
                    return RosterPresence::AwaitingEcho;
                }
            };
            return RosterPresence::Excluded { reason };
        }
        if tracking.own_seen_in_roster {
            let since_ts = *tracking.own_missing_since.get_or_insert_with(now_ms);
            return RosterPresence::Missing { since_ts };
        }
        RosterPresence::AwaitingEcho
    }

    fn encryption_status(
        &self,
        memberships: &[SessionMembership],
        tracking: &mut Tracking,
    ) -> EncryptionStatus {
        let Some(manager) = self
            .encryption_manager
            .as_ref()
            .filter(|manager| manager.manages_media_keys())
        else {
            return EncryptionStatus::NotManaged;
        };

        let peers: Vec<&SessionMembership> = memberships
            .iter()
            .filter(|tile| {
                !tile.is_own && tile.state == MembershipState::Joined && tile.device_id.is_some()
            })
            .collect();
        let key_state = |tile: &SessionMembership, pick: fn(&MediaKeyState) -> bool| {
            tile.media_key.as_ref().is_some_and(pick)
        };
        let has_distributed_initial_keys = peers
            .iter()
            .all(|tile| key_state(tile, |key| key.holds_our_key));
        let has_received_all_member_keys = peers
            .iter()
            .all(|tile| key_state(tile, |key| key.have_their_key));

        if !tracking.encryption_settled {
            if has_distributed_initial_keys && has_received_all_member_keys {
                tracking.encryption_settled = true;
            } else {
                return EncryptionStatus::Joining {
                    has_distributed_initial_keys,
                    has_received_all_member_keys,
                };
            }
        }

        let left_members_with_keys: Vec<String> = memberships
            .iter()
            .filter(|tile| tile.state == MembershipState::LeftWithKeys)
            .map(|tile| tile.member_id.clone())
            .collect();
        let fully_settled = left_members_with_keys.is_empty()
            && has_distributed_initial_keys
            && has_received_all_member_keys
            && !manager.key_switch_pending();
        EncryptionStatus::Connected {
            left_members_with_keys,
            fully_settled,
            last_rotation_ts: manager
                .get_outbound_key()
                .map(|key| key.creation_ts)
                .unwrap_or_default(),
        }
    }
}
