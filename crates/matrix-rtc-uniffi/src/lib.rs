// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The participation facade of [`matrix_rtc_core`] as a UniFFI 0.31 surface.
//!
//! Built for uniffi-bindgen-react-native, which renders one API for web/wasm
//! and React Native from this crate's uniffi metadata; `web-rtc/` is the npm
//! package that does so. The mobile bindings (`matrix-rtc-ffi`, uniffi 0.29)
//! are a separate, frozen surface and never depend on this crate.
//!
//! DTOs stay local to this crate: records mirror the core's participation
//! types field for field, and event content crosses as JSON strings, so the
//! generated TypeScript never depends on the core's Rust types. The host
//! implements [`RtcCommandSenderCallback`] for the six outbound RTC commands
//! and pushes room state in through [`RtcSessionManager`]; a
//! [`Participation`] is the per-slot handle with the four outputs
//! (memberships, transports, key map, status) and their change listeners.
//!
//! Timings cross as `u64` milliseconds, which uniffi renders as `bigint` in
//! TypeScript.

uniffi::setup_scaffolding!("matrix_rtc");

mod commands;
mod logging;
mod manager;
mod types;

#[cfg(test)]
mod tests;

pub use commands::{
    CommandSenderError, FfiToDeviceDelivery, FfiToDeviceRecipient, RtcCommandSenderCallback,
};
pub use logging::{FfiLogLevel, LogSink};
pub use manager::{
    KeyMapListener, MembershipsListener, Participation, RtcSessionManager, StatusListener,
    TransportsListener,
};
pub use types::*;

/// Errors of the surface's own calls (inputs the host pushes in, join/leave).
#[derive(Debug, Clone, thiserror::Error, uniffi::Error)]
pub enum RtcError {
    /// A DTO could not be read: bad JSON, an unknown transport, a missing
    /// parameter.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// A command the host executes on our behalf failed.
    #[error("command failed: {0}")]
    Command(String),
    #[error("already joined as {0}")]
    AlreadyJoined(String),
    #[error("not joined")]
    NotJoined,
}

impl From<matrix_rtc_core::JoinError> for RtcError {
    fn from(error: matrix_rtc_core::JoinError) -> Self {
        use matrix_rtc_core::JoinError;
        match error {
            JoinError::AlreadyJoined(member_id) => Self::AlreadyJoined(member_id),
            JoinError::MissingParameter(name) => Self::InvalidInput(name.to_owned()),
            JoinError::InvalidTransport => Self::InvalidInput("invalid transport".to_owned()),
            JoinError::CommandError(error) => Self::Command(error.to_string()),
        }
    }
}

impl From<matrix_rtc_core::LeaveError> for RtcError {
    fn from(error: matrix_rtc_core::LeaveError) -> Self {
        use matrix_rtc_core::LeaveError;
        match error {
            LeaveError::NotJoined => Self::NotJoined,
            LeaveError::CommandError(error) => Self::Command(error.to_string()),
        }
    }
}

impl From<matrix_rtc_core::CommandError> for RtcError {
    fn from(error: matrix_rtc_core::CommandError) -> Self {
        Self::Command(error.to_string())
    }
}

/// Routes Rust panics to the JS console (wasm32 only; a no-op on native,
/// where the default hook already prints). Idempotent. The npm package calls
/// this from its `initAsync`.
#[uniffi::export]
pub fn install_panic_hook() {
    #[cfg(target_arch = "wasm32")]
    console_error_panic_hook::set_once();
}

/// How often a host should call [`Participation::heartbeat`] while joined.
///
/// The core arms no timers of its own: the keep-alive restart and the sticky
/// refresh both happen on this call, so the host owns the cadence. A third of
/// the default keep-alive timeout, so two missed beats are survivable.
#[uniffi::export]
pub fn heartbeat_interval_ms() -> u64 {
    matrix_rtc_core::DEFAULT_KEEP_ALIVE_TIMEOUT_MS / 3
}

/// How severe an impairment is, and therefore where a host renders it.
#[uniffi::export]
pub fn impairment_severity(impairment: FfiImpairment) -> FfiSeverity {
    impairment.severity()
}
