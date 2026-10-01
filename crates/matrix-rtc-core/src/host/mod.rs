// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! What a host sends for the stack (`RtcCommandSender`) and hands back to it
//! (the `Raw*` DTOs, so the core never sees an SDK or FFI type). Some of it —
//! `send_room_event`, `redact_event`, `RawTimelineEvent` — is only for
//! applications built on the core.

pub(crate) mod application;
pub(crate) mod commands;
pub(crate) mod event;
