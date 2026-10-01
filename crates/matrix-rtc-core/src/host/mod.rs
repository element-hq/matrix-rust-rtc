// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! What a host implements for the stack (`MatrixBackend`: sends and
//! subscriptions) and the DTOs on both sides of it — `EventIn` as the client
//! hands an event over, the `Raw*` types as the core takes it in — so the core
//! never sees an SDK or FFI type. Some of it — `send_room_event`,
//! `redact_event`, `RawTimelineEvent` — is only for applications built on the
//! core.

pub(crate) mod application;
pub(crate) mod backend;
pub(crate) mod event;
