// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Pre-2026 Element Call interoperability, exposed to web hosts.
//!
//! The translation lives in [`matrix_rtc_bridge::compat`] and is applied by
//! the library's feeder and dialect wrapper; nothing here re-implements a
//! dialect. A page chooses the mode once, when it attaches the room
//! (`attachRoom`'s `element_call_compat`), and delivers the same raw events in
//! every mode. This module owns only the mode-string vocabulary
//! (`"off" | "sticky_events" | "state_events"`).

use matrix_rtc_bridge::compat::ElementCallCompat;
use wasm_bindgen::JsError;

/// The page's mode vocabulary, shared by `attachRoom` and `connectMedia` so
/// the two can never disagree by spelling.
pub(crate) fn parse_compat(value: Option<&str>) -> Result<ElementCallCompat, JsError> {
    Ok(match value {
        None | Some("off") => ElementCallCompat::Off,
        Some("sticky_events") => ElementCallCompat::StickyEvents,
        Some("state_events") => ElementCallCompat::StateEvents,
        Some(other) => {
            return Err(JsError::new(&format!(
                "unknown element_call_compat {other:?}: expected off | sticky_events | state_events",
            )));
        }
    })
}
