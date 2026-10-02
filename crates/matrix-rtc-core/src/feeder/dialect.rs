// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Which generation of MatrixRTC a room is read in: the feeder's only
//! dialect-specific decisions, behind [`IngestDialect`]. The core reads the
//! spec ([`SpecDialect`]); an application that also reads older dialects
//! (Element Call's) implements the trait over its own translation, so none of
//! it lives here.

use super::event_origin;
use crate::{
    EventIn, MaybeSend, RawStickyEvent, RawStickyEventContent, ReceivedEncryptionKey,
    ToDeviceMessageIn,
};

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

    /// The to-device types media keys also arrive as, beyond the spec's.
    fn key_event_types(&self) -> Vec<String> {
        Vec::new()
    }

    /// A media key from a message of one of [`Self::key_event_types`].
    fn parse_key(&self, _message: &ToDeviceMessageIn) -> Option<ReceivedEncryptionKey> {
        None
    }
}

/// The spec: `m.rtc.member` sticky events, `m.rtc.slot` state and
/// `m.rtc.encryption_key` to-device messages.
#[derive(Clone, Copy, Debug, Default)]
pub struct SpecDialect;

impl IngestDialect for SpecDialect {
    fn current_membership(
        &self,
        room_id: &str,
        sticky: Vec<EventIn>,
        _state: Vec<EventIn>,
    ) -> Vec<RawStickyEvent> {
        sticky
            .into_iter()
            .filter_map(|event| to_sticky_event(room_id, event))
            .collect()
    }
}

/// An unparseable member event contributes no membership.
fn to_sticky_event(room_id: &str, event: EventIn) -> Option<RawStickyEvent> {
    let content: RawStickyEventContent = serde_json::from_value(event.content)
        .inspect_err(|error| {
            log::warn!(
                "[{room_id}] ignoring an unparseable {} from {}: {error}",
                event.event_type,
                event.sender,
            );
        })
        .ok()?;
    Some(RawStickyEvent {
        room_id: room_id.to_owned(),
        event_id: Some(event.event_id),
        sender: event.sender,
        origin: event_origin(&event.encryption),
        event_type: event.event_type,
        content,
    })
}
