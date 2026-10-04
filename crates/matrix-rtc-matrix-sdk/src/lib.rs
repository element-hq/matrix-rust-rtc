// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The matrix-rust-sdk implementation of `matrix-rtc-core`'s `MatrixBackend`.
//!
//! [`SdkMatrixBackend`] turns a `matrix_sdk::Client` into the backend the core and
//! the call layer drive: it sends, subscribes and reads through the SDK, and
//! hands back DTOs so neither depends on SDK types. Everything that does not
//! need the SDK — the Element Call dialects, the feeder, the transport choice —
//! lives in `matrix-rtc-call`.

pub mod sdk;

pub use sdk::{SdkMatrixBackend, TimelineIngest, timeline_ingest_from_raw};
