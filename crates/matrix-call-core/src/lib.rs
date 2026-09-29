// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The `m.call` application layer over [`matrix_rtc_core`].
//!
//! `matrix-rtc-core` speaks generic MSC4143: who is in a slot, our own
//! membership, media keys. Everything Element Call layers on top of that and
//! nothing else does — emoji reactions and the raised hand (`reactions`), the
//! MSC4075 ring/notify sent with a join (`notification`) — lives here, behind
//! [`CallSessionManager`], which wraps an [`RtcSessionManager`] and adds the
//! call behaviour around its inputs. Bindings that expose a *call* hold a
//! `CallSessionManager`; ones that expose bare RTC participation hold the
//! `RtcSessionManager` directly.
//!
//! [`RtcSessionManager`]: matrix_rtc_core::RtcSessionManager

mod commands;
mod manager;
mod notification;
pub mod reactions;

pub use commands::CallCommandSender;
pub use manager::{CallJoinParams, CallSessionManager};
pub use notification::{
    DEFAULT_RING_LIFETIME_MS, MAX_RING_LIFETIME_MS, Mentions, NOTIFICATION_EVENT_TYPE,
    NotificationType, NotifyConfig, build_notification_content, notification_sticky_duration_ms,
};
pub use reactions::{
    ANNOTATION_EVENT_TYPE, DEFAULT_REACTION_ACTIVE_MS, GENERIC_SOUND, KNOWN_REACTIONS,
    RAISED_HAND_KEY, REACTION_EVENT_TYPE, RaisedHand, RawTimelineEvent, ReactionError,
    ReactionKind, ReactionSound, ReactionsConfig, ReceivedReaction, RelationLookup,
    build_raised_hand_content, build_reaction_content, first_grapheme, reaction_kind, sound_for,
};

/// Test doubles for sibling crates, behind the `testing` feature.
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    pub use crate::commands::MockCallCommandSender;
}
