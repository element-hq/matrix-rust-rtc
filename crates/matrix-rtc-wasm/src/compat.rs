// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Pre-2026 Element Call interoperability, exposed to web hosts.
//!
//! The translation lives in [`matrix_rtc_core::compat`] and is applied by
//! the library's feeder and dialect wrapper; nothing here re-implements a
//! dialect. A page chooses the mode once, when it opens the room
//! (`room`'s `format`), and delivers the same raw events in
//! every mode. This module owns only the mode-string vocabulary
//! (`"current" | "sticky_2025" | "room_state"`).

use matrix_rtc_core::compat::MembershipFormat;
use wasm_bindgen::JsError;

/// The page's mode vocabulary, shared by `room` and `connectMedia` so
/// the two can never disagree by spelling.
pub(crate) fn parse_compat(value: Option<&str>) -> Result<MembershipFormat, JsError> {
    Ok(match value {
        None | Some("current") => MembershipFormat::Current,
        Some("sticky_2025") => MembershipFormat::Sticky2025,
        Some("room_state") => MembershipFormat::RoomState,
        Some(other) => {
            return Err(JsError::new(&format!(
                "unknown format {other:?}: expected current | sticky_2025 | room_state",
            )));
        }
    })
}
