// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Error types for the MatrixRTC core crate.
//!
//! This module defines error types used throughout the core crate,
//! particularly for command execution and session operations.

use thiserror::Error;

pub use crate::host::backend::BackendError;

/// Errors that can occur when executing commands via the `MatrixBackend`.
#[derive(Debug, Error)]
pub enum CommandError {
    /// The command was rejected by the client SDK.
    #[error("command rejected by client: {0}")]
    ClientRejected(String),

    /// Failed to serialize event content to JSON.
    #[error("failed to serialize event content: {0}")]
    SerializationError(String),

    /// The room ID is invalid or missing.
    #[error("invalid room ID")]
    InvalidRoomId,

    /// The event type is invalid or unsupported.
    #[error("invalid event type: {0}")]
    InvalidEventType(String),

    /// Failed to send the event (network or SDK error).
    #[error("failed to send event: {0}")]
    SendError(String),

    /// Failed to schedule the delayed event.
    #[error("failed to schedule delayed event: {0}")]
    SchedulingError(String),

    /// Failed to cancel the delayed event (event ID not found or already fired).
    #[error("failed to cancel delayed event: {0}")]
    CancelError(String),

    /// The homeserver will never accept a delayed event, so retrying one is
    /// pointless.
    ///
    /// Two shapes of rejection mean this: the endpoint is not implemented at all
    /// (404 `M_UNRECOGNIZED`), and it is implemented but switched off — which is
    /// what matrix.org answers, a 403 `M_FORBIDDEN` reading "Sending delayed
    /// events has been disallowed".
    #[error("delayed events not supported by the homeserver: {0}")]
    DelayedEventsNotSupported(String),
}

/// Errors that can occur when attempting to join an RTC session.
#[derive(Debug, Error)]
pub enum JoinError {
    /// A command execution error occurred while joining.
    #[error("command error while joining: {0}")]
    CommandError(#[from] CommandError),

    /// The session is already joined with the given membership ID.
    #[error("already joined with membership ID: {0}")]
    AlreadyJoined(String),

    /// Required parameter is missing.
    #[error("missing required parameter: {0}")]
    MissingParameter(&'static str),

    /// Invalid transport configuration.
    #[error("invalid transport configuration")]
    InvalidTransport,
}

/// Errors that can occur when attempting to leave an RTC session.
#[derive(Debug, Error)]
pub enum LeaveError {
    /// A command execution error occurred while leaving.
    #[error("command error while leaving: {0}")]
    CommandError(#[from] CommandError),

    /// The session is not currently joined.
    #[error("not joined")]
    NotJoined,
}

impl CommandError {
    /// Create a generic command error from a string message.
    pub fn from_message(msg: impl Into<String>) -> Self {
        CommandError::SendError(msg.into())
    }

    /// Classifies a delayed-event send failure from what the homeserver said:
    /// `M_UNRECOGNIZED`, or `M_FORBIDDEN` about delayed events, means it will
    /// never accept one. The library decides this, not the host.
    pub fn delayed_event_failure(
        errcode: Option<&str>,
        status: Option<u16>,
        message: impl Into<String>,
    ) -> Self {
        let message = message.into();
        let permanent = match errcode {
            Some("M_UNRECOGNIZED") => true,
            Some("M_FORBIDDEN") => message.to_ascii_lowercase().contains("delayed"),
            _ => status == Some(404),
        };
        if permanent {
            CommandError::DelayedEventsNotSupported(message)
        } else {
            CommandError::SchedulingError(message)
        }
    }

    /// Whether this rejection means the homeserver will never accept a delayed
    /// event, so a client should stop asking rather than retry.
    pub fn is_delayed_events_unsupported(&self) -> bool {
        matches!(self, CommandError::DelayedEventsNotSupported(_))
    }
}

impl From<BackendError> for CommandError {
    fn from(error: BackendError) -> Self {
        CommandError::SendError(error.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_delayed_event_refusal_is_permanent_only_for_the_known_answers() {
        let forbidden = CommandError::delayed_event_failure(
            Some("M_FORBIDDEN"),
            Some(403),
            "Sending delayed events has been disallowed",
        );
        assert!(forbidden.is_delayed_events_unsupported());

        let unrecognized =
            CommandError::delayed_event_failure(Some("M_UNRECOGNIZED"), Some(404), "Unrecognized");
        assert!(unrecognized.is_delayed_events_unsupported());

        let other_forbidden =
            CommandError::delayed_event_failure(Some("M_FORBIDDEN"), Some(403), "not in room");
        assert!(!other_forbidden.is_delayed_events_unsupported());

        let transient = CommandError::delayed_event_failure(None, Some(502), "Bad Gateway");
        assert!(!transient.is_delayed_events_unsupported());
    }
}
