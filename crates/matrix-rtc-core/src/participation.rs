// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The participation facade: what a host programs against.
//!
//! Four outputs — [`SessionMembership`] list, [`TransportWithMembers`] list,
//! [`KeyMap`], [`Status`] — each readable at any time from an [`RtcSession`]
//! or by `(room, slot)` from an [`RtcSessionManager`], and each announced
//! through a [`ParticipationListener`] when, and only when, its value changed.
//! The DTOs here are deliberately flat copies of the session's internal state,
//! so that bindings can mirror them one-to-one without reaching into the
//! roster, the own-membership machine or the encryption manager.
//!
//! Firing rule: listener callbacks run synchronously, on the caller's task,
//! at the end of the input call that changed the value (`join`, `leave`,
//! `heartbeat`, a sticky/slot/room-state update, a received key). A listener
//! must therefore never re-enter the manager synchronously — the bindings hold
//! an async mutex across each call — and should schedule any follow-up work
//! instead. Installing a listener replays the current values once.
//!
//! [`RtcSession`]: crate::RtcSession
//! [`RtcSessionManager`]: crate::RtcSessionManager

use std::collections::BTreeMap;
use std::fmt;

use crate::encryption::types::{InboundEncryptionKey, KeyRejection, OutboundEncryptionKey};
use crate::event::EventOrigin;
use crate::maybe_send::MaybeSend;
use crate::own_membership::transport_to_json;
use crate::session::{JoinedMembership, LeaveReason};
use crate::transport::RtcTransport;

// ---------------------------------------------------------------------------
// Memberships
// ---------------------------------------------------------------------------

/// How a member's `m.rtc.member` event reached us, flattened from
/// [`EventOrigin`] for hosts that only need the verdict.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MembershipAttribution {
    /// Decrypted; the sending device is authenticated.
    Encrypted,
    /// Arrived in the clear.
    Cleartext,
    /// The event named its own device and nothing authenticated the claim.
    Claimed,
    /// The host did not say how it arrived.
    Unknown,
}

impl From<&EventOrigin> for MembershipAttribution {
    fn from(origin: &EventOrigin) -> Self {
        match origin {
            EventOrigin::Encrypted { .. } => Self::Encrypted,
            EventOrigin::Cleartext => Self::Cleartext,
            EventOrigin::Claimed { .. } => Self::Claimed,
            EventOrigin::Unknown => Self::Unknown,
        }
    }
}

/// Whether a listed member is in the call or merely still able to decrypt it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MembershipState {
    /// Satisfies every MSC4143 join condition right now.
    Joined,
    /// Left the roster while holding our current media key — "possibly still
    /// listening" until the next key rotation retires that key.
    LeftWithKeys,
}

/// Whether one member and we can hear each other.
///
/// Two independent booleans on purpose: they fail for different reasons (our
/// to-device send to them vs. theirs to us), clear independently, and a UI
/// renders them in different places.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MediaKeyState {
    /// They hold our current key: they can decrypt us.
    pub holds_our_key: bool,
    /// We hold at least one key of theirs: we can decrypt them.
    pub have_their_key: bool,
    /// Why their most recent key was discarded, while we still lack one from
    /// them. `None` once a key from them is accepted.
    pub rejection: Option<KeyRejection>,
}

/// One member of the session, as a host renders a tile.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionMembership {
    /// `member.id` — unique per join (MSC4143).
    pub member_id: String,
    /// The sending user.
    pub user_id: String,
    /// The sending device, when the event was attributable to one.
    pub device_id: Option<String>,
    /// How the member event reached us.
    pub attribution: MembershipAttribution,
    /// When this participation began, where the dialect states it.
    pub membership_ts: Option<u64>,
    /// Event id of the latest member event of this participation.
    pub membership_event_id: Option<String>,
    /// `content.application.type`.
    pub application: Option<String>,
    /// Transports this member publishes on.
    pub published_transports: Vec<RtcTransport>,
    /// Transport types this member can receive on.
    pub can_subscribe: Vec<String>,
    /// Whether this is our own current participation.
    pub is_own: bool,
    /// The RTC-backend identity the media layer addresses this member by, when
    /// an identity mapper is installed (see
    /// [`RtcIdentityMapper`](crate::RtcIdentityMapper)); `None` until then.
    pub transport_identity: Option<String>,
    /// Joined, or gone but still holding our key.
    pub state: MembershipState,
    /// Per-tile key exchange state; `None` while media keys are not managed,
    /// for our own entry, and for anyone we are not exchanging keys with.
    pub media_key: Option<MediaKeyState>,
}

/// One distinct transport and the members publishing on it — the token-free
/// projection a media layer connects from. Minting a token for a transport is
/// the transport crate's job, not this one's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransportWithMembers {
    pub transport: RtcTransport,
    /// Sorted, so two snapshots with the same members compare equal.
    pub member_ids: Vec<String>,
}

// ---------------------------------------------------------------------------
// Key map
// ---------------------------------------------------------------------------

/// One media key, ours or a peer's.
///
/// `Debug` prints the key's length, never its bytes.
#[derive(Clone, PartialEq, Eq)]
pub struct MediaKey {
    pub member_id: String,
    pub key: Vec<u8>,
    pub key_index: u8,
    pub creation_ts: u64,
}

impl fmt::Debug for MediaKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MediaKey")
            .field("member_id", &self.member_id)
            .field("key_len", &self.key.len())
            .field("key_index", &self.key_index)
            .field("creation_ts", &self.creation_ts)
            .finish()
    }
}

/// Every media key the session holds, by member id — our own outbound key
/// under our member id, and every accepted inbound key under its sender's.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct KeyMap {
    pub keys: BTreeMap<String, Vec<MediaKey>>,
}

impl fmt::Debug for KeyMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.keys.iter().map(|(member, keys)| {
                (
                    member,
                    keys.iter()
                        .map(|key| format!("index {} ({} bytes)", key.key_index, key.key.len()))
                        .collect::<Vec<_>>(),
                )
            }))
            .finish()
    }
}

impl KeyMap {
    /// Builds the map from what the encryption manager holds.
    pub(crate) fn from_keys(
        own_member_id: Option<&str>,
        outbound: Option<&OutboundEncryptionKey>,
        inbound: &std::collections::HashMap<String, Vec<InboundEncryptionKey>>,
    ) -> Self {
        let mut keys: BTreeMap<String, Vec<MediaKey>> = BTreeMap::new();
        if let (Some(member_id), Some(outbound)) = (own_member_id, outbound) {
            keys.insert(
                member_id.to_owned(),
                vec![MediaKey {
                    member_id: member_id.to_owned(),
                    key: outbound.key.clone(),
                    key_index: outbound.key_index,
                    creation_ts: outbound.creation_ts,
                }],
            );
        }
        for (member_id, held) in inbound {
            if held.is_empty() {
                continue;
            }
            let mut entry: Vec<MediaKey> = held
                .iter()
                .map(|key| MediaKey {
                    member_id: member_id.clone(),
                    key: key.key.clone(),
                    key_index: key.key_index,
                    creation_ts: key.creation_ts,
                })
                .collect();
            entry.sort_by_key(|key| key.key_index);
            keys.insert(member_id.clone(), entry);
        }
        Self { keys }
    }
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Where our own participation stands, per mechanism.
///
/// Recoverable conditions are *state* inside the variant (its `impairments`);
/// terminal ones end the participation and show up as a [`DisconnectCause`].
/// Timestamps are unix-ms (`_ts` a point in time, `_ms` a duration).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Status {
    /// Not in the call, and why not.
    Disconnected(DisconnectCause),
    /// `join()` is in flight.
    Joining(JoinStatus),
    /// Our membership is published and kept alive.
    Connected(ConnectedStatus),
    /// `leave()` is in flight.
    Leaving(LeaveStatus),
}

impl Status {
    /// Everything currently wrong, most severe first; empty when disconnected.
    ///
    /// A host that only wants *problem* transitions should diff this rather
    /// than the whole `Status`: `Connected` also changes on every keep-alive
    /// beat (the timestamps move), which is health, not news.
    pub fn impairments(&self) -> &[Impairment] {
        match self {
            Self::Disconnected(_) => &[],
            Self::Joining(status) => &status.impairments,
            Self::Connected(status) => &status.impairments,
            Self::Leaving(status) => &status.impairments,
        }
    }
}

/// How far a join got, step by step.
///
/// The core is host-driven, so the only steps are the two events the join
/// sends and the first heartbeat the host makes afterwards.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct JoinProgress {
    pub has_sent_delayed_leave_event: bool,
    pub has_sent_member_join_event: bool,
    pub has_started_heartbeat: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinStatus {
    pub progress: JoinProgress,
    pub encryption: EncryptionStatus,
    pub impairments: Vec<Impairment>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectedStatus {
    /// Our `member.id` for this join.
    pub member_id: String,
    /// The event id our membership currently lives under; moves on refresh.
    pub membership_event_id: Option<String>,
    pub keep_alive: KeepAlive,
    pub membership: MembershipPublication,
    pub roster: RosterPresence,
    pub encryption: EncryptionStatus,
    /// Everything currently wrong, most severe first. A pure projection of the
    /// fields above.
    pub impairments: Vec<Impairment>,
}

/// The encryption side is deliberately absent: while leaving, keys are being
/// forgotten and any statement about them is about to be false.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaveStatus {
    pub leave_event_sent: bool,
    /// What became of the armed dead man's switch; `None` until the leave
    /// reaches that step, or when none was armed.
    pub delayed_leave: Option<DelayedLeaveOutcome>,
    pub impairments: Vec<Impairment>,
}

/// The dead man's switch (MSC4140) that clears our membership if this client
/// dies. Mutually exclusive states of one mechanism.
///
/// MSC4195 delegation to an SFU is not modelled here because the core never
/// performs it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeepAlive {
    /// Armed, and restarted by every host heartbeat.
    Armed {
        delay_ms: u64,
        last_restart_ts: u64,
        /// When the homeserver publishes our leave if no further restart
        /// lands — `last_restart_ts + delay_ms`.
        fires_at_ts: u64,
    },
    /// Armed, but restarts are failing: unless one succeeds, the homeserver
    /// publishes our leave at `fires_at_ts` and we drop out of the call.
    RestartFailing {
        since_ts: u64,
        fires_at_ts: u64,
        last_error: String,
    },
    /// Its full delay elapsed with no successful restart, so it has in all
    /// likelihood already fired: we are probably out of the call and have
    /// simply not seen the leave come back yet. The next heartbeat arms a
    /// replacement.
    Expired { since_ts: u64 },
    /// None armed. `permanent` means this homeserver refuses delayed events
    /// for good; otherwise the heartbeat re-probes at `next_probe_ts`.
    /// Without a switch, a crashed client leaves a ghost tile until
    /// [`MembershipPublication::expires_at_ts`].
    Unavailable {
        permanent: bool,
        next_probe_ts: Option<u64>,
    },
}

/// Our sticky membership event on the server (MSC4354). It expires unless we
/// re-publish it, so `expires_at_ts` is the deadline a failing refresh runs
/// against.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MembershipPublication {
    /// The lifetime every membership event of this join is published with.
    pub lifetime_ms: u64,
    pub last_published_ts: u64,
    /// `last_published_ts + lifetime_ms`.
    pub expires_at_ts: u64,
    /// Set while refreshes are failing; `None` when healthy.
    pub refresh_failing_since_ts: Option<u64>,
    pub last_refresh_error: Option<String>,
}

/// Whether the session projects our own membership — whether anybody sees us.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RosterPresence {
    /// Our join event was sent and its echo has not come back yet. Not a fault.
    AwaitingEcho,
    Present,
    /// It was in the roster and is gone — the sticky entry lapsed, most likely
    /// because refreshes failed.
    Missing {
        since_ts: u64,
    },
    /// The event is on the server but the session refuses to project it, so
    /// nobody sees us. Only the room state that caused it changing clears it.
    Excluded {
        reason: JoinExclusionReason,
    },
}

/// Why the session excludes our own membership from the roster.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinExclusionReason {
    SlotClosed,
    UnencryptedInEncryptedRoom,
    SenderNotInRoom,
}

/// The media-key side of the participation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EncryptionStatus {
    /// Media keys are not managed for this session.
    NotManaged,
    /// Keys are still being exchanged with the members present at join.
    Joining {
        has_distributed_initial_keys: bool,
        has_received_all_member_keys: bool,
    },
    Connected {
        /// Members who left but still hold the key our media is encrypted
        /// with, until a rotation retires it.
        left_members_with_keys: Vec<String>,
        /// No key switch pending, nobody gone holding our key, and every
        /// current member has sent us a key.
        fully_settled: bool,
        /// Creation of the current outbound key.
        last_rotation_ts: u64,
    },
}

/// What happened to the dead man's switch when we left.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DelayedLeaveOutcome {
    /// Cancelled cleanly; nothing further will be published for us.
    Cancelled,
    /// The cancel failed — most likely because it had already fired. A stray
    /// delayed leave event of ours may still land in the room.
    MayStillFire,
}

/// Why [`Status::Disconnected`] is the current state.
///
/// Terminal by construction: none of these clears on its own, only a host
/// decision (a new `join()`) changes anything.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DisconnectCause {
    /// No join has been attempted on this session.
    NeverJoined,
    /// The host called `leave()` and it went through.
    LeftByHost {
        reason: Option<LeaveReason>,
        /// What became of the dead man's switch; `None` when none was armed.
        delayed_leave: Option<DelayedLeaveOutcome>,
    },
    /// `leave()` failed to publish the leave event. We are no longer tracking
    /// the membership, but the server still holds it until it expires or the
    /// delayed leave fires.
    LeaveFailed { at_ts: u64, error: String },
    /// `join()` failed; the participation never started. Carries how far it
    /// got, so a UI can say "the delayed leave was armed but the membership
    /// event was rejected".
    JoinFailed {
        at_ts: u64,
        progress: JoinProgress,
        error: String,
    },
}

/// How severe an [`Impairment`] is, and therefore where a host renders it.
/// Ordered most severe first, which is also the order `impairments` uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    /// We are, or are about to be, out of the call — or peers cannot use our
    /// media.
    Critical,
    /// Degraded but functioning; a crash or a timeout would now hurt.
    Degraded,
    /// Worth surfacing in diagnostics, not in the call UI.
    Notice,
}

/// A condition that is true *right now* and that the core is still working
/// on. Every variant clears by itself when the underlying operation
/// succeeds — an impairment is never terminal.
///
/// Redundant with the structured fields of [`ConnectedStatus`] and the
/// per-tile [`MediaKeyState`] on purpose: the structured values carry the
/// timestamps a UI renders countdowns from, while this flat, severity-ordered
/// list means a host that renders one warning banner cannot miss a condition
/// it did not know to look for. It never carries a fact the structured fields
/// do not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Impairment {
    /// The dead man's switch could not be restarted. It is still armed: unless
    /// a restart succeeds, the homeserver publishes our leave at `fires_at_ts`.
    KeepAliveRestartFailing {
        since_ts: u64,
        fires_at_ts: u64,
        last_error: String,
    },
    /// The delay's full period elapsed with no successful restart, so the
    /// homeserver has in all likelihood already published our leave.
    KeepAliveExpired { since_ts: u64 },
    /// No dead man's switch is armed. If this client dies, our tile survives
    /// until `membership_expires_at_ts`.
    KeepAliveUnavailable {
        permanent: bool,
        membership_expires_at_ts: u64,
    },
    /// Re-publishing our sticky membership is failing; it expires at
    /// `expires_at_ts` unless a refresh gets through.
    MembershipRefreshFailing {
        since_ts: u64,
        expires_at_ts: u64,
        last_error: String,
    },
    /// Our membership was in the roster and is gone: right now nobody sees us.
    OwnMembershipMissing { since_ts: u64 },
    /// Our membership is on the server but the session refuses to project it.
    OwnMembershipExcluded { reason: JoinExclusionReason },
    /// Our current media key has not reached these members; they cannot
    /// decrypt us. Redelivery is retried on the next rollout.
    MediaKeyNotDelivered { member_ids: Vec<String> },
    /// These members have not sent us a usable key; we cannot decrypt them.
    MediaKeyNotReceived { member_ids: Vec<String> },
    /// A key from this member was discarded and it still has none we can use.
    MediaKeyRejected {
        member_id: String,
        sender_user_id: Option<String>,
        reason: KeyRejection,
        at_ts: u64,
    },
}

impl Impairment {
    pub fn severity(&self) -> Severity {
        match self {
            Self::KeepAliveExpired { .. }
            | Self::OwnMembershipMissing { .. }
            | Self::OwnMembershipExcluded { .. }
            | Self::KeepAliveRestartFailing { .. }
            | Self::MembershipRefreshFailing { .. } => Severity::Critical,
            Self::MediaKeyNotDelivered { .. }
            | Self::MediaKeyNotReceived { .. }
            | Self::MediaKeyRejected { .. }
            | Self::KeepAliveUnavailable { .. } => Severity::Degraded,
        }
    }

    /// Total order for `impairments`: severity, then a fixed variant rank,
    /// then the ids inside. Stable across recomputation with unchanged
    /// inputs, so publish-on-change never flaps on ordering alone.
    fn sort_key(&self) -> (Severity, u8, String) {
        let (rank, discriminator) = match self {
            Self::KeepAliveExpired { .. } => (0, String::new()),
            Self::OwnMembershipMissing { .. } => (1, String::new()),
            Self::OwnMembershipExcluded { .. } => (2, String::new()),
            Self::KeepAliveRestartFailing { .. } => (3, String::new()),
            Self::MembershipRefreshFailing { .. } => (4, String::new()),
            Self::MediaKeyNotDelivered { .. } => (5, String::new()),
            Self::MediaKeyNotReceived { .. } => (6, String::new()),
            Self::MediaKeyRejected { member_id, .. } => (7, member_id.clone()),
            Self::KeepAliveUnavailable { .. } => (8, String::new()),
        };
        (self.severity(), rank, discriminator)
    }
}

/// Sorts impairments into their canonical order.
pub(crate) fn sort_impairments(impairments: &mut [Impairment]) {
    impairments.sort_by_key(Impairment::sort_key);
}

/// The impairments an own-membership state implies.
pub(crate) fn own_membership_impairments(
    keep_alive: &KeepAlive,
    membership: &MembershipPublication,
    roster: &RosterPresence,
    out: &mut Vec<Impairment>,
) {
    match keep_alive {
        KeepAlive::Armed { .. } => {}
        KeepAlive::RestartFailing {
            since_ts,
            fires_at_ts,
            last_error,
        } => out.push(Impairment::KeepAliveRestartFailing {
            since_ts: *since_ts,
            fires_at_ts: *fires_at_ts,
            last_error: last_error.clone(),
        }),
        KeepAlive::Expired { since_ts } => out.push(Impairment::KeepAliveExpired {
            since_ts: *since_ts,
        }),
        KeepAlive::Unavailable { permanent, .. } => out.push(Impairment::KeepAliveUnavailable {
            permanent: *permanent,
            // Without a dead man's switch, the sticky expiry is the only thing
            // that ever clears a ghost tile, so that is the deadline reported.
            membership_expires_at_ts: membership.expires_at_ts,
        }),
    }
    if let Some(since_ts) = membership.refresh_failing_since_ts {
        out.push(Impairment::MembershipRefreshFailing {
            since_ts,
            expires_at_ts: membership.expires_at_ts,
            last_error: membership.last_refresh_error.clone().unwrap_or_default(),
        });
    }
    match roster {
        RosterPresence::Present | RosterPresence::AwaitingEcho => {}
        RosterPresence::Missing { since_ts } => out.push(Impairment::OwnMembershipMissing {
            since_ts: *since_ts,
        }),
        RosterPresence::Excluded { reason } => {
            out.push(Impairment::OwnMembershipExcluded { reason: *reason })
        }
    }
}

/// Aggregated from the same per-tile [`MediaKeyState`] the membership list
/// carries — one source of truth, so a banner and a tile never disagree.
pub(crate) fn media_key_impairments(
    memberships: &[SessionMembership],
    rejected_at: impl Fn(&str) -> Option<u64>,
    out: &mut Vec<Impairment>,
) {
    let (mut undelivered, mut unreceived) = (Vec::new(), Vec::new());
    for membership in memberships {
        // A member gone with keys is not expected to exchange any, and one
        // with no device cannot be sent one at all — neither is ours to fix.
        if membership.state != MembershipState::Joined || membership.device_id.is_none() {
            continue;
        }
        let Some(key) = &membership.media_key else {
            continue;
        };
        if !key.holds_our_key {
            undelivered.push(membership.member_id.clone());
        }
        if !key.have_their_key {
            unreceived.push(membership.member_id.clone());
        }
        if let Some(reason) = &key.rejection {
            out.push(Impairment::MediaKeyRejected {
                member_id: membership.member_id.clone(),
                sender_user_id: Some(membership.user_id.clone()),
                reason: reason.clone(),
                at_ts: rejected_at(&membership.member_id).unwrap_or_default(),
            });
        }
    }
    undelivered.sort();
    unreceived.sort();
    if !undelivered.is_empty() {
        out.push(Impairment::MediaKeyNotDelivered {
            member_ids: undelivered,
        });
    }
    if !unreceived.is_empty() {
        out.push(Impairment::MediaKeyNotReceived {
            member_ids: unreceived,
        });
    }
}

/// Groups the roster's published transports, one entry per distinct
/// transport, members and entries sorted so equal rosters compare equal.
pub(crate) fn transports_from(memberships: &[SessionMembership]) -> Vec<TransportWithMembers> {
    let mut groups: Vec<TransportWithMembers> = Vec::new();
    for membership in memberships {
        if membership.state != MembershipState::Joined {
            continue;
        }
        for transport in &membership.published_transports {
            match groups
                .iter_mut()
                .find(|group| &group.transport == transport)
            {
                Some(group) => group.member_ids.push(membership.member_id.clone()),
                None => groups.push(TransportWithMembers {
                    transport: transport.clone(),
                    member_ids: vec![membership.member_id.clone()],
                }),
            }
        }
    }
    for group in &mut groups {
        group.member_ids.sort();
        group.member_ids.dedup();
    }
    groups.sort_by_cached_key(|group| transport_to_json(&group.transport).to_string());
    groups
}

/// A roster entry as a tile, before the key state is filled in.
pub(crate) fn membership_from(joined: &JoinedMembership, is_own: bool) -> SessionMembership {
    SessionMembership {
        member_id: joined.member_id.clone(),
        user_id: joined.sender.clone(),
        device_id: joined.origin.sender_device_id().map(str::to_owned),
        attribution: MembershipAttribution::from(&joined.origin),
        membership_ts: joined.membership_ts,
        membership_event_id: joined.membership_event_id.clone(),
        application: joined.application.clone(),
        published_transports: joined.transports.clone(),
        can_subscribe: joined.can_subscribe.clone(),
        is_own,
        transport_identity: None,
        state: MembershipState::Joined,
        media_key: None,
    }
}

// ---------------------------------------------------------------------------
// Snapshot and listener
// ---------------------------------------------------------------------------

/// All four outputs at one instant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParticipationSnapshot {
    pub memberships: Vec<SessionMembership>,
    pub transports: Vec<TransportWithMembers>,
    pub key_map: KeyMap,
    pub status: Status,
}

impl Default for ParticipationSnapshot {
    fn default() -> Self {
        Self {
            memberships: Vec::new(),
            transports: Vec::new(),
            key_map: KeyMap::default(),
            status: Status::Disconnected(DisconnectCause::NeverJoined),
        }
    }
}

/// Change notifications for the four outputs.
///
/// Every method is defaulted to a no-op, so a host implements only what it
/// renders. See the module docs for the firing rule; in short: synchronous, on
/// the caller's task, only on change, and never re-enter the manager from
/// inside a callback.
pub trait ParticipationListener: MaybeSend {
    fn on_memberships_change(&self, memberships: &[SessionMembership]) {
        let _ = memberships;
    }
    fn on_transports_change(&self, transports: &[TransportWithMembers]) {
        let _ = transports;
    }
    fn on_key_map_change(&self, key_map: &KeyMap) {
        let _ = key_map;
    }
    fn on_status_change(&self, status: &Status) {
        let _ = status;
    }
}

/// Fires the listener for each output that differs between `previous` and
/// `current`, in a fixed order: memberships, transports, key map, status.
pub(crate) fn fire_changes(
    listener: &dyn ParticipationListener,
    previous: &ParticipationSnapshot,
    current: &ParticipationSnapshot,
) {
    if previous.memberships != current.memberships {
        listener.on_memberships_change(&current.memberships);
    }
    if previous.transports != current.transports {
        listener.on_transports_change(&current.transports);
    }
    if previous.key_map != current.key_map {
        listener.on_key_map_change(&current.key_map);
    }
    if previous.status != current.status {
        listener.on_status_change(&current.status);
    }
}

/// Replays every output once, as installing a listener does.
pub(crate) fn fire_all(listener: &dyn ParticipationListener, current: &ParticipationSnapshot) {
    listener.on_memberships_change(&current.memberships);
    listener.on_transports_change(&current.transports);
    listener.on_key_map_change(&current.key_map);
    listener.on_status_change(&current.status);
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;
    use serde_json::Value;

    use super::*;
    use crate::commands::{
        MockCommandSender, RtcCommandSender, ToDeviceDelivery, ToDeviceRecipient,
    };
    use crate::encryption::types::{KeyOrigin, ReceivedEncryptionKey};
    use crate::error::CommandError;
    use crate::event::{EventOrigin, RawStickyEvent, RawStickyEventContent};
    use crate::join::{JoinSessionParams, LeaveSessionParams};
    use crate::manager::RtcSessionManager;
    use crate::slot::RawSlotEvent;
    use crate::transport::{LiveKitTransport, MemberTransports, RawRtcTransport, RtcTransport};

    const ROOM: &str = "!room:example.org";
    const SLOT: &str = "m.call#ROOM";
    const ALICE: &str = "@alice:example.org";
    const ALICE_DEV: &str = "ALICEDEV";
    const BOB: &str = "@bob:example.org";
    const BOB_DEV: &str = "BOBDEV";
    const URL_A: &str = "https://a.example.org/jwt";
    const URL_B: &str = "https://b.example.org/jwt";

    // ---- doubles -------------------------------------------------------

    /// A command sender whose failures a test switches on and off.
    #[derive(Default)]
    struct ScriptedSender {
        inner: MockCommandSender,
        refuse_delayed: AtomicBool,
        fail_restart: AtomicBool,
        fail_sticky: AtomicBool,
    }

    #[async_trait]
    impl RtcCommandSender for ScriptedSender {
        async fn send_sticky_event(
            &self,
            room_id: String,
            event_type: String,
            content: Value,
            duration_ms: u64,
        ) -> Result<String, CommandError> {
            if self.fail_sticky.load(Ordering::Relaxed) {
                return Err(CommandError::SendError("sticky refused".into()));
            }
            self.inner
                .send_sticky_event(room_id, event_type, content, duration_ms)
                .await
        }
        async fn send_delayed_event(
            &self,
            room_id: String,
            event_type: String,
            content: Value,
            delay_ms: u64,
        ) -> Result<String, CommandError> {
            if self.refuse_delayed.load(Ordering::Relaxed) {
                return Err(CommandError::DelayedEventsNotSupported("disabled".into()));
            }
            self.inner
                .send_delayed_event(room_id, event_type, content, delay_ms)
                .await
        }
        async fn restart_delayed_event(
            &self,
            room_id: String,
            delay_id: String,
        ) -> Result<(), CommandError> {
            if self.fail_restart.load(Ordering::Relaxed) {
                return Err(CommandError::SendError("restart refused".into()));
            }
            self.inner.restart_delayed_event(room_id, delay_id).await
        }
        async fn cancel_delayed_event(
            &self,
            room_id: String,
            delay_id: String,
        ) -> Result<(), CommandError> {
            self.inner.cancel_delayed_event(room_id, delay_id).await
        }
        async fn send_to_device_message(
            &self,
            recipients: Vec<ToDeviceRecipient>,
            message_type: String,
            content: Value,
        ) -> Result<Vec<ToDeviceDelivery>, CommandError> {
            self.inner
                .send_to_device_message(recipients, message_type, content)
                .await
        }
        async fn send_state_event(
            &self,
            room_id: String,
            event_type: String,
            state_key: String,
            content: Value,
        ) -> Result<String, CommandError> {
            self.inner
                .send_state_event(room_id, event_type, state_key, content)
                .await
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Event {
        Memberships(Vec<String>),
        Transports(Vec<TransportWithMembers>),
        KeyMap(Vec<String>),
        Status(Box<Status>),
    }

    #[derive(Default)]
    struct Recorder {
        events: Mutex<Vec<Event>>,
    }

    impl Recorder {
        fn events(&self) -> Vec<Event> {
            self.events.lock().unwrap().clone()
        }
        fn statuses(&self) -> Vec<Status> {
            self.events()
                .into_iter()
                .filter_map(|event| match event {
                    Event::Status(status) => Some(*status),
                    _ => None,
                })
                .collect()
        }
        fn count(&self, pick: fn(&Event) -> bool) -> usize {
            self.events().iter().filter(|event| pick(event)).count()
        }
        fn clear(&self) {
            self.events.lock().unwrap().clear();
        }
    }

    impl ParticipationListener for Recorder {
        fn on_memberships_change(&self, memberships: &[SessionMembership]) {
            self.events.lock().unwrap().push(Event::Memberships(
                memberships.iter().map(|m| m.member_id.clone()).collect(),
            ));
        }
        fn on_transports_change(&self, transports: &[TransportWithMembers]) {
            self.events
                .lock()
                .unwrap()
                .push(Event::Transports(transports.to_vec()));
        }
        fn on_key_map_change(&self, key_map: &KeyMap) {
            self.events
                .lock()
                .unwrap()
                .push(Event::KeyMap(key_map.keys.keys().cloned().collect()));
        }
        fn on_status_change(&self, status: &Status) {
            self.events
                .lock()
                .unwrap()
                .push(Event::Status(Box::new(status.clone())));
        }
    }

    // ---- fixtures ------------------------------------------------------

    fn livekit(url: &str) -> RawRtcTransport {
        let mut extra = BTreeMap::new();
        extra.insert(
            "livekit_service_url".to_owned(),
            Value::String(url.to_owned()),
        );
        RawRtcTransport {
            transport_type: "livekit".to_owned(),
            extra_fields: extra,
        }
    }

    fn joined_event(sender: &str, device: &str, member_id: &str, url: &str) -> RawStickyEvent {
        RawStickyEvent {
            room_id: ROOM.to_owned(),
            event_id: Some(format!("$event-{member_id}")),
            sender: sender.to_owned(),
            origin: EventOrigin::encrypted(Some(device.to_owned())),
            event_type: "m.rtc.member".to_owned(),
            content: RawStickyEventContent::for_join(
                SLOT.to_owned(),
                member_id.to_owned(),
                "m.call".to_owned(),
                MemberTransports::publishing(livekit(url)),
            ),
        }
    }

    fn encrypted_slot() -> RawSlotEvent {
        RawSlotEvent {
            room_id: ROOM.to_owned(),
            slot_id: SLOT.to_owned(),
            content: serde_json::from_str(
                r#"{ "status": "open", "application": { "type": "m.call" },
                     "encryption": { "type": "m.per_member" } }"#,
            )
            .unwrap(),
        }
    }

    type Manager = RtcSessionManager<ScriptedSender>;

    async fn call_manager() -> (Arc<ScriptedSender>, Manager, Arc<Recorder>) {
        let sender = Arc::new(ScriptedSender::default());
        let mut manager = RtcSessionManager::with_command_sender(sender.clone());
        manager.on_room_encryption_received(ROOM, true).await;
        manager
            .on_room_slots_received(ROOM, vec![encrypted_slot()])
            .await;
        let recorder = Arc::new(Recorder::default());
        manager.set_participation_listener(ROOM, SLOT, recorder.clone());
        (sender, manager, recorder)
    }

    fn join_params(member_id: &str) -> JoinSessionParams {
        let mut params = JoinSessionParams::new(
            ALICE.to_owned(),
            ALICE_DEV.to_owned(),
            ROOM.to_owned(),
            SLOT.to_owned(),
            "m.call".to_owned(),
            RtcTransport::LiveKit(LiveKitTransport {
                livekit_service_url: URL_A.to_owned(),
            }),
        );
        params.membership_id = Some(member_id.to_owned());
        params
    }

    async fn join(manager: &mut Manager) {
        manager.join(join_params("alice-a")).await.expect("join");
    }

    async fn roster(manager: &mut Manager, events: Vec<RawStickyEvent>) {
        manager
            .set_current_sticky_state(ROOM, events)
            .await
            .expect("sticky state");
    }

    fn own_event() -> RawStickyEvent {
        joined_event(ALICE, ALICE_DEV, "alice-a", URL_A)
    }

    fn bob_key(origin: KeyOrigin) -> ReceivedEncryptionKey {
        ReceivedEncryptionKey {
            origin,
            room_id: ROOM.to_owned(),
            member_id: "bob-a".to_owned(),
            key_b64: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_owned(),
            key_index: 0,
        }
    }

    fn bob_origin() -> KeyOrigin {
        KeyOrigin::Encrypted {
            sender_user_id: BOB.to_owned(),
            sender_device_id: Some(BOB_DEV.to_owned()),
            sender_is_cross_signed: true,
        }
    }

    fn connected(status: &Status) -> &ConnectedStatus {
        match status {
            Status::Connected(connected) => connected,
            other => panic!("expected Connected, got {other:?}"),
        }
    }

    fn tile<'a>(memberships: &'a [SessionMembership], member_id: &str) -> &'a SessionMembership {
        memberships
            .iter()
            .find(|m| m.member_id == member_id)
            .unwrap_or_else(|| panic!("no tile for {member_id}"))
    }

    // ---- status lifecycle ----------------------------------------------

    #[tokio::test]
    async fn status_walks_the_join_and_leave_lifecycle() {
        let (_, mut manager, recorder) = call_manager().await;
        assert_eq!(
            recorder.statuses(),
            vec![Status::Disconnected(DisconnectCause::NeverJoined)],
            "installing the listener replays the current status"
        );
        recorder.clear();

        join(&mut manager).await;
        let statuses = recorder.statuses();
        assert!(
            matches!(statuses[0], Status::Joining(_)),
            "join publishes Joining first: {statuses:?}"
        );
        let joined = connected(&statuses[1]);
        assert_eq!(joined.member_id, "alice-a");
        assert_eq!(joined.roster, RosterPresence::AwaitingEcho);
        assert!(matches!(joined.keep_alive, KeepAlive::Armed { .. }));
        assert!(
            joined.impairments.is_empty(),
            "alone in the call nothing is wrong: {:?}",
            joined.impairments
        );
        recorder.clear();

        // The host echoes our membership back: now everybody sees us.
        roster(&mut manager, vec![own_event()]).await;
        let status = manager.participation_status(ROOM, SLOT);
        assert_eq!(connected(&status).roster, RosterPresence::Present);
        assert!(tile(&manager.memberships(ROOM, SLOT).unwrap(), "alice-a").is_own);
        recorder.clear();

        manager
            .leave(ROOM.to_owned(), SLOT.to_owned(), LeaveSessionParams::new())
            .await
            .expect("leave");
        let statuses = recorder.statuses();
        assert!(matches!(statuses[0], Status::Leaving(_)), "{statuses:?}");
        assert_eq!(
            statuses[1],
            Status::Disconnected(DisconnectCause::LeftByHost {
                reason: None,
                delayed_leave: Some(DelayedLeaveOutcome::Cancelled),
            })
        );
        assert!(manager.key_map(ROOM, SLOT).unwrap().keys.is_empty());
    }

    #[tokio::test]
    async fn a_failed_join_reports_how_far_it_got() {
        let (sender, mut manager, recorder) = call_manager().await;
        sender.fail_sticky.store(true, Ordering::Relaxed);
        assert!(manager.join(join_params("alice-a")).await.is_err());

        let status = manager.participation_status(ROOM, SLOT);
        let Status::Disconnected(DisconnectCause::JoinFailed {
            progress, error, ..
        }) = &status
        else {
            panic!("expected JoinFailed, got {status:?}");
        };
        assert!(progress.has_sent_delayed_leave_event, "step 1 went through");
        assert!(
            !progress.has_sent_member_join_event,
            "step 2 is what failed"
        );
        assert!(error.contains("sticky refused"), "{error}");
        assert_eq!(recorder.statuses().last(), Some(&status));
    }

    #[tokio::test]
    async fn manager_reports_never_joined_for_a_slot_without_a_session() {
        let manager: RtcSessionManager<ScriptedSender> = RtcSessionManager::new();
        assert_eq!(
            manager.participation_status(ROOM, "m.call#OTHER"),
            Status::Disconnected(DisconnectCause::NeverJoined)
        );
        assert_eq!(manager.memberships(ROOM, "m.call#OTHER"), None);
    }

    // ---- keep-alive ----------------------------------------------------

    #[tokio::test]
    async fn a_failing_keep_alive_restart_is_a_critical_impairment() {
        let (sender, mut manager, recorder) = call_manager().await;
        join(&mut manager).await;
        recorder.clear();

        sender.fail_restart.store(true, Ordering::Relaxed);
        assert!(manager.heartbeat(ROOM, SLOT).await);
        let status = manager.participation_status(ROOM, SLOT);
        let joined = connected(&status);
        assert!(
            matches!(joined.keep_alive, KeepAlive::RestartFailing { ref last_error, .. }
                if last_error.contains("restart refused")),
            "{:?}",
            joined.keep_alive
        );
        assert!(matches!(
            joined.impairments.first(),
            Some(Impairment::KeepAliveRestartFailing { .. })
        ));
        assert_eq!(joined.impairments[0].severity(), Severity::Critical);
        assert!(
            recorder.statuses().last().is_some_and(|s| s == &status),
            "the failure was announced, not just readable"
        );

        sender.fail_restart.store(false, Ordering::Relaxed);
        assert!(manager.heartbeat(ROOM, SLOT).await);
        let joined_again = manager.participation_status(ROOM, SLOT);
        assert!(matches!(
            connected(&joined_again).keep_alive,
            KeepAlive::Armed { .. }
        ));
        assert!(connected(&joined_again).impairments.is_empty());
    }

    #[tokio::test]
    async fn a_homeserver_refusing_delayed_events_shows_keep_alive_unavailable() {
        let (sender, mut manager, _) = call_manager().await;
        sender.refuse_delayed.store(true, Ordering::Relaxed);
        join(&mut manager).await;

        let status = manager.participation_status(ROOM, SLOT);
        let joined = connected(&status);
        assert_eq!(
            joined.keep_alive,
            KeepAlive::Unavailable {
                permanent: true,
                next_probe_ts: None,
            }
        );
        assert!(matches!(
            joined.impairments.as_slice(),
            [Impairment::KeepAliveUnavailable {
                permanent: true,
                ..
            }]
        ));
        assert_eq!(joined.impairments[0].severity(), Severity::Degraded);
        assert_eq!(
            joined.membership.lifetime_ms,
            crate::join::DEFAULT_DEGRADED_LIFETIME_MS,
            "the publication reports the shortened lifetime the join fell back to"
        );
    }

    // ---- roster, transports, keys --------------------------------------

    #[tokio::test]
    async fn memberships_and_transports_follow_the_roster_and_group_by_transport() {
        let (_, mut manager, recorder) = call_manager().await;
        join(&mut manager).await;
        recorder.clear();

        roster(
            &mut manager,
            vec![
                joined_event(BOB, BOB_DEV, "bob-a", URL_A),
                own_event(),
                joined_event("@carol:example.org", "CARDEV", "carol-a", URL_B),
            ],
        )
        .await;

        let memberships = manager.memberships(ROOM, SLOT).unwrap();
        let ids: Vec<&str> = memberships.iter().map(|m| m.member_id.as_str()).collect();
        assert_eq!(ids, ["alice-a", "bob-a", "carol-a"], "sorted by member id");
        assert!(tile(&memberships, "alice-a").is_own);
        assert_eq!(
            tile(&memberships, "alice-a").media_key,
            None,
            "we do not exchange keys with ourselves"
        );
        let bob = tile(&memberships, "bob-a");
        assert_eq!(bob.device_id.as_deref(), Some(BOB_DEV));
        assert_eq!(bob.attribution, MembershipAttribution::Encrypted);
        assert_eq!(
            bob.media_key,
            Some(MediaKeyState {
                holds_our_key: true,
                have_their_key: false,
                rejection: None,
            }),
            "our key went out to bob; his has not arrived"
        );

        let transports = manager.transports(ROOM, SLOT).unwrap();
        assert_eq!(transports.len(), 2);
        assert_eq!(transports[0].member_ids, ["alice-a", "bob-a"]);
        assert_eq!(
            transports[0].transport,
            RtcTransport::LiveKit(LiveKitTransport {
                livekit_service_url: URL_A.to_owned()
            })
        );
        assert_eq!(transports[1].member_ids, ["carol-a"]);

        // We joined alone, so the initial key exchange settled trivially; the
        // arrivals are per-tile problems, not a return to `Joining`.
        let status = manager.participation_status(ROOM, SLOT);
        assert!(
            matches!(
                connected(&status).encryption,
                EncryptionStatus::Connected {
                    fully_settled: false,
                    ..
                }
            ),
            "{:?}",
            connected(&status).encryption
        );
        assert!(matches!(
            connected(&status).impairments.as_slice(),
            [Impairment::MediaKeyNotReceived { member_ids }] if member_ids == &["bob-a", "carol-a"]
        ));

        assert_eq!(recorder.count(|e| matches!(e, Event::Memberships(_))), 1);
        assert_eq!(recorder.count(|e| matches!(e, Event::Transports(_))), 1);
    }

    #[tokio::test]
    async fn encryption_is_joining_until_the_members_present_at_join_have_exchanged_keys() {
        let (_, mut manager, _) = call_manager().await;
        roster(
            &mut manager,
            vec![joined_event(BOB, BOB_DEV, "bob-a", URL_A)],
        )
        .await;
        join(&mut manager).await;

        let status = manager.participation_status(ROOM, SLOT);
        assert_eq!(
            connected(&status).encryption,
            EncryptionStatus::Joining {
                has_distributed_initial_keys: true,
                has_received_all_member_keys: false,
            }
        );

        manager
            .receive_encryption_key(bob_key(bob_origin()))
            .await
            .unwrap();
        let status = manager.participation_status(ROOM, SLOT);
        assert!(matches!(
            connected(&status).encryption,
            EncryptionStatus::Connected {
                fully_settled: true,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn a_peer_key_lands_in_the_key_map_and_settles_encryption() {
        let (_, mut manager, recorder) = call_manager().await;
        join(&mut manager).await;
        roster(
            &mut manager,
            vec![own_event(), joined_event(BOB, BOB_DEV, "bob-a", URL_A)],
        )
        .await;
        recorder.clear();

        manager
            .receive_encryption_key(bob_key(bob_origin()))
            .await
            .expect("receive");

        let key_map = manager.key_map(ROOM, SLOT).unwrap();
        let members: Vec<&String> = key_map.keys.keys().collect();
        assert_eq!(members, ["alice-a", "bob-a"], "ours and bob's");
        assert_eq!(key_map.keys["bob-a"][0].key_index, 0);
        assert_eq!(
            recorder.count(|e| matches!(e, Event::KeyMap(_))),
            1,
            "the key map change was announced once"
        );

        let memberships = manager.memberships(ROOM, SLOT).unwrap();
        assert!(
            tile(&memberships, "bob-a")
                .media_key
                .as_ref()
                .unwrap()
                .have_their_key
        );
        let status = manager.participation_status(ROOM, SLOT);
        assert!(
            matches!(
                connected(&status).encryption,
                EncryptionStatus::Connected {
                    fully_settled: true,
                    ..
                }
            ),
            "{:?}",
            connected(&status).encryption
        );
        assert!(connected(&status).impairments.is_empty());
    }

    #[tokio::test]
    async fn a_rejected_key_is_latched_per_member_until_a_good_one_arrives() {
        let (_, mut manager, _) = call_manager().await;
        join(&mut manager).await;
        roster(
            &mut manager,
            vec![own_event(), joined_event(BOB, BOB_DEV, "bob-a", URL_A)],
        )
        .await;

        manager
            .receive_encryption_key(bob_key(KeyOrigin::Cleartext))
            .await
            .expect("a refused key is not an error");

        let memberships = manager.memberships(ROOM, SLOT).unwrap();
        assert_eq!(
            tile(&memberships, "bob-a")
                .media_key
                .as_ref()
                .unwrap()
                .rejection,
            Some(KeyRejection::Cleartext)
        );
        let status = manager.participation_status(ROOM, SLOT);
        assert!(
            connected(&status).impairments.iter().any(|i| matches!(
                i,
                Impairment::MediaKeyRejected { member_id, reason: KeyRejection::Cleartext, .. }
                    if member_id == "bob-a"
            )),
            "{:?}",
            connected(&status).impairments
        );

        manager
            .receive_encryption_key(bob_key(bob_origin()))
            .await
            .expect("receive");
        let memberships = manager.memberships(ROOM, SLOT).unwrap();
        assert_eq!(
            tile(&memberships, "bob-a")
                .media_key
                .as_ref()
                .unwrap()
                .rejection,
            None,
            "an accepted key clears the latch"
        );
    }

    #[tokio::test]
    async fn a_member_who_left_still_holding_our_key_stays_listed() {
        let (_, mut manager, _) = call_manager().await;
        join(&mut manager).await;
        roster(
            &mut manager,
            vec![own_event(), joined_event(BOB, BOB_DEV, "bob-a", URL_A)],
        )
        .await;
        manager
            .receive_encryption_key(bob_key(bob_origin()))
            .await
            .unwrap();

        // Bob's sticky entry is gone; the key he holds is still fresh, so the
        // rotation that retires it is deferred and he can still decrypt us.
        roster(&mut manager, vec![own_event()]).await;

        let memberships = manager.memberships(ROOM, SLOT).unwrap();
        let bob = tile(&memberships, "bob-a");
        assert_eq!(bob.state, MembershipState::LeftWithKeys);
        assert_eq!(bob.user_id, BOB);
        assert!(
            !manager
                .transports(ROOM, SLOT)
                .unwrap()
                .iter()
                .any(|t| t.member_ids.contains(&"bob-a".to_owned())),
            "a departed member publishes on nothing"
        );
        let status = manager.participation_status(ROOM, SLOT);
        assert_eq!(
            connected(&status).encryption,
            EncryptionStatus::Connected {
                left_members_with_keys: vec!["bob-a".to_owned()],
                fully_settled: false,
                last_rotation_ts: manager.key_map(ROOM, SLOT).unwrap().keys["alice-a"][0]
                    .creation_ts,
            }
        );
    }

    // ---- own membership presence ---------------------------------------

    #[tokio::test]
    async fn our_membership_vanishing_after_it_was_seen_is_missing() {
        let (_, mut manager, recorder) = call_manager().await;
        join(&mut manager).await;
        roster(&mut manager, vec![own_event()]).await;
        recorder.clear();

        roster(&mut manager, vec![]).await;
        let status = manager.participation_status(ROOM, SLOT);
        assert!(matches!(
            connected(&status).roster,
            RosterPresence::Missing { .. }
        ));
        assert!(matches!(
            connected(&status).impairments.first(),
            Some(Impairment::OwnMembershipMissing { .. })
        ));
        assert!(
            recorder.statuses().last().is_some_and(|s| s == &status),
            "announced"
        );
    }

    #[tokio::test]
    async fn our_membership_is_excluded_when_the_slot_closes() {
        let (_, mut manager, _) = call_manager().await;
        join(&mut manager).await;
        roster(&mut manager, vec![own_event()]).await;

        manager.on_room_slots_received(ROOM, vec![]).await;
        let status = manager.participation_status(ROOM, SLOT);
        assert_eq!(
            connected(&status).roster,
            RosterPresence::Excluded {
                reason: JoinExclusionReason::SlotClosed
            }
        );
        assert!(manager.memberships(ROOM, SLOT).unwrap().is_empty());
    }

    // ---- publish-on-change ---------------------------------------------

    #[tokio::test]
    async fn listeners_fire_on_change_only() {
        let (_, mut manager, recorder) = call_manager().await;
        join(&mut manager).await;
        let state = vec![own_event(), joined_event(BOB, BOB_DEV, "bob-a", URL_A)];
        roster(&mut manager, state.clone()).await;
        recorder.clear();

        roster(&mut manager, state.clone()).await;
        manager
            .on_room_members_received(ROOM, vec![ALICE.to_owned(), BOB.to_owned()])
            .await;
        manager
            .on_room_members_received(ROOM, vec![ALICE.to_owned(), BOB.to_owned()])
            .await;
        assert_eq!(
            recorder.events(),
            Vec::<Event>::new(),
            "nothing changed, so nothing was announced"
        );
    }

    #[tokio::test]
    async fn installing_a_listener_replays_every_output_once() {
        let (_, mut manager, _) = call_manager().await;
        join(&mut manager).await;
        roster(&mut manager, vec![own_event()]).await;

        let late = Arc::new(Recorder::default());
        manager.set_participation_listener(ROOM, SLOT, late.clone());
        let events = late.events();
        assert_eq!(events.len(), 4, "{events:?}");
        assert_eq!(events[0], Event::Memberships(vec!["alice-a".to_owned()]));
        assert!(
            matches!(&events[3], Event::Status(status) if matches!(**status, Status::Connected(_)))
        );
    }

    #[tokio::test]
    async fn impairments_are_sorted_by_severity_and_stable() {
        let (sender, mut manager, _) = call_manager().await;
        join(&mut manager).await;
        roster(
            &mut manager,
            vec![own_event(), joined_event(BOB, BOB_DEV, "bob-a", URL_A)],
        )
        .await;
        sender.fail_restart.store(true, Ordering::Relaxed);
        manager.heartbeat(ROOM, SLOT).await;

        let first = manager.participation_status(ROOM, SLOT);
        let impairments = connected(&first).impairments.clone();
        assert!(matches!(
            impairments[0],
            Impairment::KeepAliveRestartFailing { .. }
        ));
        assert!(matches!(
            impairments[1],
            Impairment::MediaKeyNotReceived { .. }
        ));
        assert_eq!(
            manager.participation_status(ROOM, SLOT),
            first,
            "recomputing with unchanged inputs yields the same value"
        );
    }

    // ---- hygiene -------------------------------------------------------

    #[tokio::test]
    async fn key_material_never_appears_in_debug_output() {
        let (_, mut manager, _) = call_manager().await;
        join(&mut manager).await;
        let key_map = manager.key_map(ROOM, SLOT).unwrap();
        let ours = &key_map.keys["alice-a"][0];
        assert_eq!(ours.key.len(), 32);

        let debug = format!("{key_map:?} {ours:?}");
        assert!(debug.contains("32 bytes"), "{debug}");
        assert!(
            !debug.contains(&format!("{:?}", ours.key)),
            "the bytes leaked into Debug: {debug}"
        );
    }
}
