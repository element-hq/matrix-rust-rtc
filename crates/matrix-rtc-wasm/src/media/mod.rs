// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Media on the web: the participant roster and LiveKit connection lifecycle,
//! without the media itself.
//!
//! The shared `CallEngine` (`matrix-rtc-media`) reconciles the core's
//! membership snapshots with livekit-js room events and owns the multi-focus
//! connection pool — the same code the mobile FFI runs. What differs is the
//! transport: a JS delegate drives livekit-js ([`transport`]), tracks and
//! publishing never cross into Rust, and roster entries carry the livekit-js
//! participant identity so a page can join them to
//! `room.getParticipantByIdentity()`.

mod session;
mod transport;

pub use session::WasmMediaSession;
pub use transport::WasmConnectionEventSink;
