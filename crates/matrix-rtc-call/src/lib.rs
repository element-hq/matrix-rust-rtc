// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The call application over `matrix-rtc-core`, which is application-agnostic:
//! Element Call's reactions and raised hand ([`reactions`]), MSC4075 ringing
//! ([`notification`]), and the host-facing [`RtcClient`] → [`RtcRoom`] →
//! [`RtcSession`] / [`RtcCall`].
//!
//! It also holds how a host's `MatrixBackend` reaches the call, none of which
//! needs a Matrix SDK: [`compat`], translation to and from the pre-2026
//! dialects Element Call still speaks, with [`DialectBackend`], the backend
//! wrapper that renders sends in a room's dialect; [`feeder`], which subscribes,
//! seeds, orders and funnels a room into the call; and [`transports`], which
//! transport a join publishes on.

mod client;
pub mod compat;
pub mod feeder;
pub mod notification;
pub mod reactions;
mod room_state;
pub mod transports;

pub use compat::{
    DialectBackend, ElementCallCompat, ElementCallDialect, ElementCallStateDialect,
    LEGACY_KEY_EVENT_TYPE, LegacyKeyMessage, MemberContent, MemberEventRoute, OutboundDialect,
    STATE_MEMBER_EVENT_TYPE, StateMemberEvent, StateMembership,
};
pub use feeder::{
    RoomAlreadyOpen, RoomAttachment, RoomFeeder, RoomFeederRun, RoomRegistry, ToDeviceFeeder,
    ToDeviceFeederRun,
};

pub use client::{
    CallJoinOptions, JoinOptions, RoomOptions, RtcCall, RtcClient, RtcError, RtcRoom, RtcSession,
};
pub use notification::{
    DEFAULT_RING_LIFETIME_MS, MAX_RING_LIFETIME_MS, Mentions, NOTIFICATION_EVENT_TYPE,
    NotificationType, NotifyConfig, build_notification_content, notification_sticky_duration_ms,
    notify_session_started,
};
pub use reactions::{
    ANNOTATION_EVENT_TYPE, ANNOTATION_RELATION_TYPE, DEFAULT_REACTION_ACTIVE_MS, GENERIC_SOUND,
    KNOWN_REACTIONS, RAISED_HAND_KEY, REACTION_EVENT_TYPE, RaisedHand, ReactionError, ReactionKind,
    ReactionSound, ReactionsConfig, ReceivedReaction, RelationLookup, build_raised_hand_content,
    build_reaction_content, first_grapheme, reaction_kind, sound_for,
};

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use web_time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch: a clock read, never a timer.
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
