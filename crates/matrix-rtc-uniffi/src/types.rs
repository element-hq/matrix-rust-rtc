// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! FFI DTOs: records and enums mirroring the core's participation types and
//! the inputs a host pushes in.
//!
//! Mirrors rather than re-exports, so the generated bindings depend on nothing
//! but this crate and event content crosses as JSON strings. Every `From`
//! here is a field-for-field copy; the doc of each type is on the core's.

use std::collections::BTreeMap;

use matrix_rtc_core::participation as core;
use matrix_rtc_core::{
    EncryptionConfig, EventOrigin, JoinSessionParams, KeyOrigin, KeyRejection, LeaveCode,
    LeaveReason, LeaveSessionParams, LiveKitTransport, RawSlotEvent, RawStickyEvent,
    ReceivedEncryptionKey, RtcTransport, SlotEncryption, TransportIntent, UnsupportedTransport,
};

use crate::RtcError;

// ---------------------------------------------------------------------------
// Outputs
// ---------------------------------------------------------------------------

/// A transport a member publishes on. `Unsupported` carries the transport's
/// other fields as a JSON object string.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiRtcTransport {
    LiveKit {
        livekit_service_url: String,
    },
    Unsupported {
        transport_type: String,
        extra_json: String,
    },
}

impl From<&RtcTransport> for FfiRtcTransport {
    fn from(transport: &RtcTransport) -> Self {
        match transport {
            RtcTransport::LiveKit(livekit) => Self::LiveKit {
                livekit_service_url: livekit.livekit_service_url.clone(),
            },
            RtcTransport::Unsupported(unsupported) => Self::Unsupported {
                transport_type: unsupported.transport_type.clone(),
                extra_json: serde_json::to_string(&unsupported.extra_fields)
                    .unwrap_or_else(|_| "{}".to_owned()),
            },
        }
    }
}

impl FfiRtcTransport {
    pub(crate) fn into_core(self) -> Result<RtcTransport, RtcError> {
        Ok(match self {
            Self::LiveKit {
                livekit_service_url,
            } => RtcTransport::LiveKit(LiveKitTransport {
                livekit_service_url,
            }),
            Self::Unsupported {
                transport_type,
                extra_json,
            } => {
                let extra_fields: BTreeMap<String, serde_json::Value> =
                    serde_json::from_str(&extra_json).map_err(|e| {
                        RtcError::InvalidInput(format!("transport extra_json: {e}"))
                    })?;
                RtcTransport::Unsupported(UnsupportedTransport {
                    transport_type,
                    extra_fields,
                })
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiMembershipAttribution {
    Encrypted,
    Cleartext,
    Claimed,
    Unknown,
}

impl From<core::MembershipAttribution> for FfiMembershipAttribution {
    fn from(attribution: core::MembershipAttribution) -> Self {
        match attribution {
            core::MembershipAttribution::Encrypted => Self::Encrypted,
            core::MembershipAttribution::Cleartext => Self::Cleartext,
            core::MembershipAttribution::Claimed => Self::Claimed,
            core::MembershipAttribution::Unknown => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiMembershipState {
    Joined,
    LeftWithKeys,
}

impl From<core::MembershipState> for FfiMembershipState {
    fn from(state: core::MembershipState) -> Self {
        match state {
            core::MembershipState::Joined => Self::Joined,
            core::MembershipState::LeftWithKeys => Self::LeftWithKeys,
        }
    }
}

/// Why an inbound media key was refused.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiKeyRejection {
    Cleartext,
    NotCrossSigned,
    RoomMismatch {
        claimed: String,
    },
    SenderMismatch {
        expected: String,
        actual: String,
    },
    UnverifiableDevice,
    DeviceMismatch {
        expected: String,
        actual: Option<String>,
    },
}

impl From<&KeyRejection> for FfiKeyRejection {
    fn from(rejection: &KeyRejection) -> Self {
        match rejection {
            KeyRejection::Cleartext => Self::Cleartext,
            KeyRejection::NotCrossSigned => Self::NotCrossSigned,
            KeyRejection::RoomMismatch { claimed } => Self::RoomMismatch {
                claimed: claimed.clone(),
            },
            KeyRejection::SenderMismatch { expected, actual } => Self::SenderMismatch {
                expected: expected.clone(),
                actual: actual.clone(),
            },
            KeyRejection::UnverifiableDevice => Self::UnverifiableDevice,
            KeyRejection::DeviceMismatch { expected, actual } => Self::DeviceMismatch {
                expected: expected.clone(),
                actual: actual.clone(),
            },
        }
    }
}

/// Whether one member and we can hear each other.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct FfiMediaKeyState {
    pub holds_our_key: bool,
    pub have_their_key: bool,
    pub rejection: Option<FfiKeyRejection>,
}

impl From<&core::MediaKeyState> for FfiMediaKeyState {
    fn from(state: &core::MediaKeyState) -> Self {
        Self {
            holds_our_key: state.holds_our_key,
            have_their_key: state.have_their_key,
            rejection: state.rejection.as_ref().map(FfiKeyRejection::from),
        }
    }
}

/// One member of the session, as a host renders a tile.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct FfiMembership {
    pub member_id: String,
    pub user_id: String,
    pub device_id: Option<String>,
    pub attribution: FfiMembershipAttribution,
    pub membership_ts: Option<u64>,
    pub membership_event_id: Option<String>,
    pub application: Option<String>,
    pub published_transports: Vec<FfiRtcTransport>,
    pub can_subscribe: Vec<String>,
    pub is_own: bool,
    /// The identity the media layer addresses this member by (MSC4195 for
    /// LiveKit); `None` before we joined.
    pub transport_identity: Option<String>,
    pub state: FfiMembershipState,
    /// `None` while media keys are not managed and for our own entry.
    pub media_key: Option<FfiMediaKeyState>,
}

impl From<&core::SessionMembership> for FfiMembership {
    fn from(membership: &core::SessionMembership) -> Self {
        Self {
            member_id: membership.member_id.clone(),
            user_id: membership.user_id.clone(),
            device_id: membership.device_id.clone(),
            attribution: membership.attribution.into(),
            membership_ts: membership.membership_ts,
            membership_event_id: membership.membership_event_id.clone(),
            application: membership.application.clone(),
            published_transports: membership
                .published_transports
                .iter()
                .map(FfiRtcTransport::from)
                .collect(),
            can_subscribe: membership.can_subscribe.clone(),
            is_own: membership.is_own,
            transport_identity: membership.transport_identity.clone(),
            state: membership.state.into(),
            media_key: membership.media_key.as_ref().map(FfiMediaKeyState::from),
        }
    }
}

/// One distinct transport and the members publishing on it. Token-free:
/// minting a token is the host's, from the transport's own type.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct FfiTransportWithMembers {
    pub transport: FfiRtcTransport,
    pub member_ids: Vec<String>,
}

impl From<&core::TransportWithMembers> for FfiTransportWithMembers {
    fn from(group: &core::TransportWithMembers) -> Self {
        Self {
            transport: FfiRtcTransport::from(&group.transport),
            member_ids: group.member_ids.clone(),
        }
    }
}

/// One media key, ours or a peer's. The key map is the flat list of these.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct FfiMediaKey {
    pub member_id: String,
    pub key: Vec<u8>,
    pub key_index: u8,
    pub creation_ts: u64,
}

impl From<&core::MediaKey> for FfiMediaKey {
    fn from(key: &core::MediaKey) -> Self {
        Self {
            member_id: key.member_id.clone(),
            key: key.key.clone(),
            key_index: key.key_index,
            creation_ts: key.creation_ts,
        }
    }
}

pub(crate) fn key_map_from(key_map: &core::KeyMap) -> Vec<FfiMediaKey> {
    key_map
        .keys
        .values()
        .flat_map(|keys| keys.iter().map(FfiMediaKey::from))
        .collect()
}

// ---- status ------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, uniffi::Record)]
pub struct FfiJoinProgress {
    pub has_sent_delayed_leave_event: bool,
    pub has_sent_member_join_event: bool,
    pub has_started_heartbeat: bool,
}

impl From<core::JoinProgress> for FfiJoinProgress {
    fn from(progress: core::JoinProgress) -> Self {
        Self {
            has_sent_delayed_leave_event: progress.has_sent_delayed_leave_event,
            has_sent_member_join_event: progress.has_sent_member_join_event,
            has_started_heartbeat: progress.has_started_heartbeat,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiEncryptionStatus {
    NotManaged,
    Joining {
        has_distributed_initial_keys: bool,
        has_received_all_member_keys: bool,
    },
    Connected {
        left_members_with_keys: Vec<String>,
        fully_settled: bool,
        last_rotation_ts: u64,
    },
}

impl From<&core::EncryptionStatus> for FfiEncryptionStatus {
    fn from(status: &core::EncryptionStatus) -> Self {
        match status {
            core::EncryptionStatus::NotManaged => Self::NotManaged,
            core::EncryptionStatus::Joining {
                has_distributed_initial_keys,
                has_received_all_member_keys,
            } => Self::Joining {
                has_distributed_initial_keys: *has_distributed_initial_keys,
                has_received_all_member_keys: *has_received_all_member_keys,
            },
            core::EncryptionStatus::Connected {
                left_members_with_keys,
                fully_settled,
                last_rotation_ts,
            } => Self::Connected {
                left_members_with_keys: left_members_with_keys.clone(),
                fully_settled: *fully_settled,
                last_rotation_ts: *last_rotation_ts,
            },
        }
    }
}

/// The dead man's switch (MSC4140), as one of its mutually exclusive states.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiKeepAlive {
    Armed {
        delay_ms: u64,
        last_restart_ts: u64,
        fires_at_ts: u64,
    },
    RestartFailing {
        since_ts: u64,
        fires_at_ts: u64,
        last_error: String,
    },
    Expired {
        since_ts: u64,
    },
    Unavailable {
        permanent: bool,
        next_probe_ts: Option<u64>,
    },
}

impl From<&core::KeepAlive> for FfiKeepAlive {
    fn from(keep_alive: &core::KeepAlive) -> Self {
        match keep_alive {
            core::KeepAlive::Armed {
                delay_ms,
                last_restart_ts,
                fires_at_ts,
            } => Self::Armed {
                delay_ms: *delay_ms,
                last_restart_ts: *last_restart_ts,
                fires_at_ts: *fires_at_ts,
            },
            core::KeepAlive::RestartFailing {
                since_ts,
                fires_at_ts,
                last_error,
            } => Self::RestartFailing {
                since_ts: *since_ts,
                fires_at_ts: *fires_at_ts,
                last_error: last_error.clone(),
            },
            core::KeepAlive::Expired { since_ts } => Self::Expired {
                since_ts: *since_ts,
            },
            core::KeepAlive::Unavailable {
                permanent,
                next_probe_ts,
            } => Self::Unavailable {
                permanent: *permanent,
                next_probe_ts: *next_probe_ts,
            },
        }
    }
}

/// Our sticky membership event on the server (MSC4354).
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct FfiMembershipPublication {
    pub lifetime_ms: u64,
    pub last_published_ts: u64,
    pub expires_at_ts: u64,
    pub refresh_failing_since_ts: Option<u64>,
    pub last_refresh_error: Option<String>,
}

impl From<&core::MembershipPublication> for FfiMembershipPublication {
    fn from(publication: &core::MembershipPublication) -> Self {
        Self {
            lifetime_ms: publication.lifetime_ms,
            last_published_ts: publication.last_published_ts,
            expires_at_ts: publication.expires_at_ts,
            refresh_failing_since_ts: publication.refresh_failing_since_ts,
            last_refresh_error: publication.last_refresh_error.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiJoinExclusionReason {
    SlotClosed,
    UnencryptedInEncryptedRoom,
    SenderNotInRoom,
}

impl From<core::JoinExclusionReason> for FfiJoinExclusionReason {
    fn from(reason: core::JoinExclusionReason) -> Self {
        match reason {
            core::JoinExclusionReason::SlotClosed => Self::SlotClosed,
            core::JoinExclusionReason::UnencryptedInEncryptedRoom => {
                Self::UnencryptedInEncryptedRoom
            }
            core::JoinExclusionReason::SenderNotInRoom => Self::SenderNotInRoom,
        }
    }
}

/// Whether the session projects our own membership.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiRosterPresence {
    AwaitingEcho,
    Present,
    Missing { since_ts: u64 },
    Excluded { reason: FfiJoinExclusionReason },
}

impl From<&core::RosterPresence> for FfiRosterPresence {
    fn from(presence: &core::RosterPresence) -> Self {
        match presence {
            core::RosterPresence::AwaitingEcho => Self::AwaitingEcho,
            core::RosterPresence::Present => Self::Present,
            core::RosterPresence::Missing { since_ts } => Self::Missing {
                since_ts: *since_ts,
            },
            core::RosterPresence::Excluded { reason } => Self::Excluded {
                reason: (*reason).into(),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiDelayedLeaveOutcome {
    Cancelled,
    MayStillFire,
}

impl From<core::DelayedLeaveOutcome> for FfiDelayedLeaveOutcome {
    fn from(outcome: core::DelayedLeaveOutcome) -> Self {
        match outcome {
            core::DelayedLeaveOutcome::Cancelled => Self::Cancelled,
            core::DelayedLeaveOutcome::MayStillFire => Self::MayStillFire,
        }
    }
}

/// MSC4143 `leave_reason`: the machine-readable `code` (`leave`,
/// `delayed_leave`, `slot_closed`, or an application-defined string) and an
/// optional human-readable explanation.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct FfiLeaveReason {
    pub code: String,
    pub reason: Option<String>,
}

impl From<&LeaveReason> for FfiLeaveReason {
    fn from(reason: &LeaveReason) -> Self {
        Self {
            code: match &reason.code {
                LeaveCode::Leave => "leave".to_owned(),
                LeaveCode::DelayedLeave => "delayed_leave".to_owned(),
                LeaveCode::SlotClosed => "slot_closed".to_owned(),
                LeaveCode::Other(code) => code.clone(),
            },
            reason: reason.reason.clone(),
        }
    }
}

/// Why we are disconnected. Terminal: only a new `join()` changes it.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiDisconnectCause {
    NeverJoined,
    LeftByHost {
        reason: Option<FfiLeaveReason>,
        delayed_leave: Option<FfiDelayedLeaveOutcome>,
    },
    LeaveFailed {
        at_ts: u64,
        error: String,
    },
    JoinFailed {
        at_ts: u64,
        progress: FfiJoinProgress,
        error: String,
    },
}

impl From<&core::DisconnectCause> for FfiDisconnectCause {
    fn from(cause: &core::DisconnectCause) -> Self {
        match cause {
            core::DisconnectCause::NeverJoined => Self::NeverJoined,
            core::DisconnectCause::LeftByHost {
                reason,
                delayed_leave,
            } => Self::LeftByHost {
                reason: reason.as_ref().map(FfiLeaveReason::from),
                delayed_leave: delayed_leave.map(Into::into),
            },
            core::DisconnectCause::LeaveFailed { at_ts, error } => Self::LeaveFailed {
                at_ts: *at_ts,
                error: error.clone(),
            },
            core::DisconnectCause::JoinFailed {
                at_ts,
                progress,
                error,
            } => Self::JoinFailed {
                at_ts: *at_ts,
                progress: (*progress).into(),
                error: error.clone(),
            },
        }
    }
}

/// How severe an impairment is; most severe first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiSeverity {
    Critical,
    Degraded,
    Notice,
}

impl From<core::Severity> for FfiSeverity {
    fn from(severity: core::Severity) -> Self {
        match severity {
            core::Severity::Critical => Self::Critical,
            core::Severity::Degraded => Self::Degraded,
            core::Severity::Notice => Self::Notice,
        }
    }
}

/// A condition that is true right now and that the core is still working on.
/// Never terminal; see the core's `participation::Impairment`.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiImpairment {
    KeepAliveRestartFailing {
        since_ts: u64,
        fires_at_ts: u64,
        last_error: String,
    },
    KeepAliveExpired {
        since_ts: u64,
    },
    KeepAliveUnavailable {
        permanent: bool,
        membership_expires_at_ts: u64,
    },
    MembershipRefreshFailing {
        since_ts: u64,
        expires_at_ts: u64,
        last_error: String,
    },
    OwnMembershipMissing {
        since_ts: u64,
    },
    OwnMembershipExcluded {
        reason: FfiJoinExclusionReason,
    },
    MediaKeyNotDelivered {
        member_ids: Vec<String>,
    },
    MediaKeyNotReceived {
        member_ids: Vec<String>,
    },
    MediaKeyRejected {
        member_id: String,
        sender_user_id: Option<String>,
        reason: FfiKeyRejection,
        at_ts: u64,
    },
}

impl FfiImpairment {
    /// The same table as the core's `Impairment::severity`.
    pub fn severity(&self) -> FfiSeverity {
        match self {
            Self::KeepAliveRestartFailing { .. }
            | Self::KeepAliveExpired { .. }
            | Self::MembershipRefreshFailing { .. }
            | Self::OwnMembershipMissing { .. }
            | Self::OwnMembershipExcluded { .. } => FfiSeverity::Critical,
            Self::KeepAliveUnavailable { .. }
            | Self::MediaKeyNotDelivered { .. }
            | Self::MediaKeyNotReceived { .. }
            | Self::MediaKeyRejected { .. } => FfiSeverity::Degraded,
        }
    }
}

impl From<&core::Impairment> for FfiImpairment {
    fn from(impairment: &core::Impairment) -> Self {
        match impairment {
            core::Impairment::KeepAliveRestartFailing {
                since_ts,
                fires_at_ts,
                last_error,
            } => Self::KeepAliveRestartFailing {
                since_ts: *since_ts,
                fires_at_ts: *fires_at_ts,
                last_error: last_error.clone(),
            },
            core::Impairment::KeepAliveExpired { since_ts } => Self::KeepAliveExpired {
                since_ts: *since_ts,
            },
            core::Impairment::KeepAliveUnavailable {
                permanent,
                membership_expires_at_ts,
            } => Self::KeepAliveUnavailable {
                permanent: *permanent,
                membership_expires_at_ts: *membership_expires_at_ts,
            },
            core::Impairment::MembershipRefreshFailing {
                since_ts,
                expires_at_ts,
                last_error,
            } => Self::MembershipRefreshFailing {
                since_ts: *since_ts,
                expires_at_ts: *expires_at_ts,
                last_error: last_error.clone(),
            },
            core::Impairment::OwnMembershipMissing { since_ts } => Self::OwnMembershipMissing {
                since_ts: *since_ts,
            },
            core::Impairment::OwnMembershipExcluded { reason } => Self::OwnMembershipExcluded {
                reason: (*reason).into(),
            },
            core::Impairment::MediaKeyNotDelivered { member_ids } => Self::MediaKeyNotDelivered {
                member_ids: member_ids.clone(),
            },
            core::Impairment::MediaKeyNotReceived { member_ids } => Self::MediaKeyNotReceived {
                member_ids: member_ids.clone(),
            },
            core::Impairment::MediaKeyRejected {
                member_id,
                sender_user_id,
                reason,
                at_ts,
            } => Self::MediaKeyRejected {
                member_id: member_id.clone(),
                sender_user_id: sender_user_id.clone(),
                reason: reason.into(),
                at_ts: *at_ts,
            },
        }
    }
}

/// Where our own participation stands.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiStatus {
    Disconnected {
        cause: FfiDisconnectCause,
    },
    Joining {
        progress: FfiJoinProgress,
        encryption: FfiEncryptionStatus,
        impairments: Vec<FfiImpairment>,
    },
    Connected {
        member_id: String,
        membership_event_id: Option<String>,
        keep_alive: FfiKeepAlive,
        membership: FfiMembershipPublication,
        roster: FfiRosterPresence,
        encryption: FfiEncryptionStatus,
        impairments: Vec<FfiImpairment>,
    },
    Leaving {
        leave_event_sent: bool,
        delayed_leave: Option<FfiDelayedLeaveOutcome>,
        impairments: Vec<FfiImpairment>,
    },
}

fn impairments_from(impairments: &[core::Impairment]) -> Vec<FfiImpairment> {
    impairments.iter().map(FfiImpairment::from).collect()
}

impl From<&core::Status> for FfiStatus {
    fn from(status: &core::Status) -> Self {
        match status {
            core::Status::Disconnected(cause) => Self::Disconnected {
                cause: cause.into(),
            },
            core::Status::Joining(joining) => Self::Joining {
                progress: joining.progress.into(),
                encryption: (&joining.encryption).into(),
                impairments: impairments_from(&joining.impairments),
            },
            core::Status::Connected(connected) => Self::Connected {
                member_id: connected.member_id.clone(),
                membership_event_id: connected.membership_event_id.clone(),
                keep_alive: (&connected.keep_alive).into(),
                membership: (&connected.membership).into(),
                roster: (&connected.roster).into(),
                encryption: (&connected.encryption).into(),
                impairments: impairments_from(&connected.impairments),
            },
            core::Status::Leaving(leaving) => Self::Leaving {
                leave_event_sent: leaving.leave_event_sent,
                delayed_leave: leaving.delayed_leave.map(Into::into),
                impairments: impairments_from(&leaving.impairments),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

/// One entry of a room's sticky map, as the host's SDK hands it over.
///
/// `was_encrypted` / `sender_device_id` come from the SDK's decryption
/// metadata, never from the content: MSC4143 identifies a member's device by
/// the device that encrypted the event. `None` for `was_encrypted` means the
/// host did not say, and the rules depending on it are not applied.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiStickyEvent {
    pub room_id: String,
    pub event_id: Option<String>,
    pub sender: String,
    pub sender_device_id: Option<String>,
    pub was_encrypted: Option<bool>,
    /// The wire event type, e.g. `org.matrix.msc4143.rtc.member`.
    pub event_type: String,
    pub content_json: String,
}

impl FfiStickyEvent {
    pub(crate) fn into_core(self) -> Result<RawStickyEvent, RtcError> {
        let content = serde_json::from_str(&self.content_json)
            .map_err(|e| RtcError::InvalidInput(format!("sticky event content: {e}")))?;
        Ok(RawStickyEvent {
            room_id: self.room_id,
            event_id: self.event_id,
            sender: self.sender,
            origin: match self.was_encrypted {
                Some(true) => EventOrigin::encrypted(self.sender_device_id),
                Some(false) => EventOrigin::Cleartext,
                None => EventOrigin::Unknown,
            },
            event_type: self.event_type,
            content,
        })
    }
}

/// One `m.rtc.slot` state event of a room.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiSlotEvent {
    pub room_id: String,
    /// The event's state key, which is the slot id.
    pub slot_id: String,
    pub content_json: String,
}

impl FfiSlotEvent {
    pub(crate) fn into_core(self) -> Result<RawSlotEvent, RtcError> {
        let content = serde_json::from_str(&self.content_json)
            .map_err(|e| RtcError::InvalidInput(format!("slot event content: {e}")))?;
        Ok(RawSlotEvent {
            room_id: self.room_id,
            slot_id: self.slot_id,
            content,
        })
    }
}

/// The `encryption` object of an `m.rtc.slot` to open (`type` plus any other
/// fields as a JSON object string).
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiSlotEncryption {
    pub encryption_type: String,
    pub extra_json: Option<String>,
}

impl FfiSlotEncryption {
    pub(crate) fn into_core(self) -> Result<SlotEncryption, RtcError> {
        let extra: BTreeMap<String, serde_json::Value> = match self.extra_json {
            Some(json) => serde_json::from_str(&json)
                .map_err(|e| RtcError::InvalidInput(format!("slot encryption extra_json: {e}")))?,
            None => BTreeMap::new(),
        };
        Ok(SlotEncryption {
            encryption_type: self.encryption_type,
            extra,
        })
    }
}

/// A decrypted `m.rtc.encryption_key` to-device message.
///
/// The sender fields come from the SDK's Olm decryption metadata; a message
/// that arrived in the clear has `was_encrypted: false` and is refused.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiReceivedEncryptionKey {
    pub room_id: String,
    pub member_id: String,
    pub key_b64: String,
    pub key_index: u8,
    pub was_encrypted: bool,
    pub sender_user_id: Option<String>,
    pub sender_device_id: Option<String>,
    pub sender_is_cross_signed: bool,
}

impl FfiReceivedEncryptionKey {
    pub(crate) fn into_core(self) -> Result<ReceivedEncryptionKey, RtcError> {
        let origin = match (self.was_encrypted, self.sender_user_id) {
            (true, Some(sender_user_id)) => KeyOrigin::Encrypted {
                sender_user_id,
                sender_device_id: self.sender_device_id,
                sender_is_cross_signed: self.sender_is_cross_signed,
            },
            (true, None) => {
                return Err(RtcError::InvalidInput(
                    "an encrypted key must name its sender".to_owned(),
                ));
            }
            (false, _) => KeyOrigin::Cleartext,
        };
        Ok(ReceivedEncryptionKey {
            origin,
            room_id: self.room_id,
            member_id: self.member_id,
            key_b64: self.key_b64,
            key_index: self.key_index,
        })
    }
}

/// MSC4143 key-management knobs; every field defaults to the core's value.
#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct FfiEncryptionConfig {
    pub delay_before_use_ms: Option<u64>,
    pub key_rotation_grace_period_ms: Option<u64>,
    pub max_key_lifetime_ms: Option<u64>,
    pub manage_media_keys: Option<bool>,
    pub require_cross_signed_sender: Option<bool>,
}

impl From<FfiEncryptionConfig> for EncryptionConfig {
    fn from(config: FfiEncryptionConfig) -> Self {
        let defaults = EncryptionConfig::default();
        EncryptionConfig {
            delay_before_use_ms: config
                .delay_before_use_ms
                .unwrap_or(defaults.delay_before_use_ms),
            key_rotation_grace_period_ms: config
                .key_rotation_grace_period_ms
                .unwrap_or(defaults.key_rotation_grace_period_ms),
            max_key_lifetime_ms: config
                .max_key_lifetime_ms
                .unwrap_or(defaults.max_key_lifetime_ms),
            manage_media_keys: config
                .manage_media_keys
                .unwrap_or(defaults.manage_media_keys),
            require_cross_signed_sender: config
                .require_cross_signed_sender
                .unwrap_or(defaults.require_cross_signed_sender),
        }
    }
}

/// What this member does with transports.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum FfiTransportIntent {
    Publish { transport: FfiRtcTransport },
    ReceiveOnly { can_subscribe: Vec<String> },
}

impl FfiTransportIntent {
    pub(crate) fn into_core(self) -> Result<TransportIntent, RtcError> {
        Ok(match self {
            Self::Publish { transport } => TransportIntent::Publish(transport.into_core()?),
            Self::ReceiveOnly { can_subscribe } => TransportIntent::ReceiveOnly { can_subscribe },
        })
    }
}

/// Everything a join needs beyond who and where (which the
/// [`Participation`](crate::Participation) already knows).
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiJoinParams {
    /// The application type, usually `m.call`.
    pub application: String,
    /// Our `member.id` for this join; a fresh random one when `None`. MSC4143
    /// requires a different id on every join.
    pub member_id: Option<String>,
    pub keep_alive_timeout_ms: Option<u64>,
    pub sticky_duration_ms: Option<u64>,
    pub degraded_lifetime_ms: Option<u64>,
    pub encryption: Option<FfiEncryptionConfig>,
}

impl FfiJoinParams {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn into_core(
        self,
        intent: FfiTransportIntent,
        room_id: &str,
        slot_id: &str,
        user_id: &str,
        device_id: &str,
        member_id: String,
    ) -> Result<JoinSessionParams, RtcError> {
        let mut params = JoinSessionParams::with_transport_intent(
            user_id.to_owned(),
            device_id.to_owned(),
            room_id.to_owned(),
            slot_id.to_owned(),
            self.application,
            intent.into_core()?,
        );
        params.membership_id = Some(member_id);
        params.keep_alive_timeout_ms = self.keep_alive_timeout_ms;
        params.sticky_duration_ms = self.sticky_duration_ms;
        params.degraded_lifetime_ms = self.degraded_lifetime_ms;
        params.encryption_config = self.encryption.map(Into::into);
        Ok(params)
    }
}

/// MSC4143 leave reason; `code` defaults to `leave`.
#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct FfiLeaveParams {
    pub code: Option<String>,
    pub reason: Option<String>,
}

impl From<FfiLeaveParams> for LeaveSessionParams {
    fn from(params: FfiLeaveParams) -> Self {
        match params.code {
            Some(code) => LeaveSessionParams::with_leave_reason(LeaveReason {
                code: LeaveCode::from_code(&code),
                reason: params.reason,
            }),
            None => LeaveSessionParams::new(),
        }
    }
}
