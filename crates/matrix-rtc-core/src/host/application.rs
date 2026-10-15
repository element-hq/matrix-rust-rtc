// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! How a host that drives the stack feeds one room's application beyond
//! membership, without knowing which application it is. Every hook is scoped
//! to the room the intake belongs to, so none of them names one.

use super::backend::MatrixBackend;
use super::event::{RawTimelineEvent, RelationsRequest};
use crate::base_rtc_room::BaseRtcRoom;

/// Every hook defaults to nothing; [`BaseRtcRoom`] implements it that way.
pub trait ApplicationIntake<T: MatrixBackend> {
    fn rtc(&mut self) -> &mut BaseRtcRoom<T>;

    /// The event types to forward to [`Self::on_timeline_events`].
    fn timeline_event_types(&self) -> Vec<String> {
        Vec::new()
    }

    fn on_timeline_events(&mut self, _events: &[RawTimelineEvent]) {}

    /// The sticky (MSC4354) event types to forward to
    /// [`Self::on_sticky_events`]. The core's own member events are never
    /// among them: those go to the membership dialect.
    fn sticky_event_types(&self) -> Vec<String> {
        Vec::new()
    }

    /// The room's current sticky events of [`Self::sticky_event_types`]. A host
    /// hands over the whole sticky set each time, so the same event arrives
    /// again on every delivery; an application handles it idempotently.
    fn on_sticky_events(&mut self, _events: &[RawTimelineEvent]) {}

    fn on_event_redacted(&mut self, _event_id: &str) {}

    fn pending_relations(&self) -> Vec<RelationsRequest> {
        Vec::new()
    }

    /// A failed fetch is not answered; the application asks again.
    fn on_relations_received(&mut self, _target_event_id: &str, _events: &[RawTimelineEvent]) {}
}

impl<T: MatrixBackend + 'static> ApplicationIntake<T> for BaseRtcRoom<T> {
    fn rtc(&mut self) -> &mut BaseRtcRoom<T> {
        self
    }
}
