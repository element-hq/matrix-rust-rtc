// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Which generation of MatrixRTC a room is read in: the feeder's only
//! format-specific decisions, behind [`IngestDialect`]. The core implements it
//! for each [`MembershipFormat`](crate::compat::MembershipFormat); an
//! application feeding through [`RoomFeeder`](super::RoomFeeder) may bring its
//! own.

use crate::{EventIn, MaybeSend, RawStickyEvent, ReceivedEncryptionKey, ToDeviceMessageIn};

/// How one room's membership and media keys are read.
pub trait IngestDialect: MaybeSend {
    /// The room state event types membership is also read from.
    fn membership_state_event_types(&self) -> Vec<String> {
        Vec::new()
    }

    /// Whether the room's slots are read; membership then waits for them, so a
    /// member is never briefly joined to a slot the room's state says is closed.
    fn reads_slots(&self) -> bool {
        true
    }

    /// The room's complete current membership, from its sticky member events
    /// and the latest membership state of [`Self::membership_state_event_types`].
    fn current_membership(
        &self,
        room_id: &str,
        sticky: Vec<EventIn>,
        state: Vec<EventIn>,
    ) -> Vec<RawStickyEvent>;

    /// A media key from a to-device message of a type beyond the spec's, which
    /// the client subscribed to.
    fn parse_key(&self, _message: &ToDeviceMessageIn) -> Option<ReceivedEncryptionKey> {
        None
    }
}
