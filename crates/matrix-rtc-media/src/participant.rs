// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The participant roster as applications see it.
//!
//! A [`Participant`] is a MatrixRTC *membership* (one `m.rtc.member` join,
//! keyed by its unique `member_id`) enriched with live media state. The roster
//! is derived from the core's membership snapshots — signalling is the source
//! of truth for who is in the call; transports only attach media to entries
//! that already exist.

/// The kind of media stream a participant publishes.
///
/// Ordered so a set of streams can be reported in a stable order; the
/// declaration order below is what that ordering is.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum MediaStreamKind {
    Microphone,
    Camera,
    ScreenShare,
    ScreenShareAudio,
    Data,
}

/// Live state of one media stream of a participant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamState {
    pub kind: MediaStreamKind,
    /// Whether the publisher has muted the stream.
    pub muted: bool,
}

/// One joined membership of the call, with its current media streams.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Participant {
    /// `member.id` of the membership — unique per join, the roster key.
    pub member_id: String,
    /// Matrix user ID of the member.
    pub user_id: String,
    /// Device that sent (encrypted) the membership event, when attributable.
    pub device_id: Option<String>,
    /// Whether this is our own membership.
    pub is_local: bool,
    /// Whether any registered transport can reach this member's media. A
    /// member publishing only unsupported transports stays in the roster
    /// (signalling truth) but never gets streams.
    pub reachable: bool,
    /// Streams currently published by this participant, in arrival order.
    pub streams: Vec<StreamState>,
    /// When this participant raised their hand (ms since the epoch, by the
    /// server's clock), or `None` while it is down. Sort ascending to queue
    /// speakers in the order they asked; see `matrix_rtc_core::reactions`.
    pub hand_raised_at_ms: Option<u64>,
    /// When this participation began (ms since the epoch), when the dialect
    /// states it. `None` for a native MSC4143 membership, which carries no
    /// join time; see `JoinedMembership::membership_ts`.
    pub joined_at_ms: Option<u64>,
}
