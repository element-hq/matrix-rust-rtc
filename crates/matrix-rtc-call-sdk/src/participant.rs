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

use matrix_rtc_transport::{ConnectionState, MediaStreamKind};

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
    /// The connection carrying this member's media (034 R28); for our own
    /// entry, our own SFU's (R23). `Unreachable` when no registered
    /// transport can reach their media: such a member stays in the roster
    /// (signalling truth) but never gets streams.
    pub connection: ConnectionState,
    /// Whether their media decrypts: false from a frame-encryption failure
    /// until frames decrypt again (034 R33). Always true for our own entry,
    /// which nothing decrypts. `streams` is untouched by it.
    pub decryptable: bool,
    /// Streams currently published by this participant, in arrival order.
    pub streams: Vec<StreamState>,
    /// When this participant raised their hand (ms since the epoch, by the
    /// server's clock), or `None` while it is down. Sort ascending to queue
    /// speakers in the order they asked; see `matrix_rtc_call::reactions`.
    pub hand_raised_at_ms: Option<u64>,
    /// This participant's place in the raised-hand queue, 1 for the earliest,
    /// counting participants on the roster only; `None` while the hand is
    /// down. The core's order (`RtcCall::raised_hands`), so a host never
    /// re-sorts `hand_raised_at_ms` itself.
    pub hand_raised_position: Option<u32>,
    /// When this participation began (ms since the epoch), when the dialect
    /// states it. `None` for a native MSC4143 membership, which carries no
    /// join time; see `JoinedMembership::membership_ts`.
    pub joined_at_ms: Option<u64>,
}
