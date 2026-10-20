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

    /// The host's Matrix client failed the send, as it reported it.
    #[error("failed to send event: {0}")]
    Backend(BackendError),

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

    /// The room's slot state holds no open slot of this id, so a membership
    /// would be one every client treats as left. Somebody with the power level
    /// has to open the slot first.
    #[error("slot '{slot_id}' is not open in this room")]
    SlotClosed { slot_id: String },
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
    /// Create a generic command error from a string message: a backend
    /// failure with no code, so transient.
    pub fn from_message(msg: impl Into<String>) -> Self {
        CommandError::Backend(BackendError::new(msg))
    }

    /// The host's failure, when this is one.
    pub fn backend_error(&self) -> Option<&BackendError> {
        match self {
            CommandError::Backend(error) => Some(error),
            _ => None,
        }
    }

    /// Whether retrying cannot help (R3). A backend failure follows
    /// [`BackendError::is_permanent`]; a homeserver that refuses delayed events
    /// and a request the library could not even build are final; a scheduling
    /// or cancel failure carries no code, so it is transient.
    pub fn is_permanent(&self) -> bool {
        match self {
            CommandError::Backend(error) => error.is_permanent(),
            CommandError::DelayedEventsNotSupported(_)
            | CommandError::ClientRejected(_)
            | CommandError::SerializationError(_)
            | CommandError::InvalidRoomId
            | CommandError::InvalidEventType(_) => true,
            CommandError::SchedulingError(_) | CommandError::CancelError(_) => false,
        }
    }

    /// Classifies a delayed-event send failure from what the homeserver said:
    /// `M_UNRECOGNIZED`, or `M_FORBIDDEN` about delayed events, means it will
    /// never accept one. The library decides this, not the host. Anything else
    /// stays the backend's failure, code and all, for the retry layer to
    /// classify.
    pub fn delayed_event_failure(error: BackendError) -> Self {
        let unsupported = match error.errcode.as_deref() {
            Some("M_UNRECOGNIZED") => true,
            Some("M_FORBIDDEN") => error.message.to_ascii_lowercase().contains("delayed"),
            // A bare 404 is a missing endpoint; a 404 with a code is the
            // endpoint answering, such as `M_NOT_FOUND` for a delay that is gone.
            Some(_) => false,
            None => error.status == Some(404),
        };
        if unsupported {
            CommandError::DelayedEventsNotSupported(error.message)
        } else {
            CommandError::Backend(error)
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
        CommandError::Backend(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delayed(errcode: Option<&str>, status: Option<u16>, message: &str) -> CommandError {
        CommandError::delayed_event_failure(BackendError::with_matrix_error(
            errcode.map(str::to_owned),
            status,
            message,
        ))
    }

    fn error(errcode: Option<&str>, status: Option<u16>) -> BackendError {
        BackendError::with_matrix_error(errcode.map(str::to_owned), status, "failed")
    }

    #[test]
    fn a_delayed_event_refusal_is_permanent_only_for_the_known_answers() {
        let forbidden = delayed(
            Some("M_FORBIDDEN"),
            Some(403),
            "Sending delayed events has been disallowed",
        );
        assert!(forbidden.is_delayed_events_unsupported());

        let unrecognized = delayed(Some("M_UNRECOGNIZED"), Some(404), "Unrecognized");
        assert!(unrecognized.is_delayed_events_unsupported());

        let other_forbidden = delayed(Some("M_FORBIDDEN"), Some(403), "not in room");
        assert!(!other_forbidden.is_delayed_events_unsupported());
        assert!(other_forbidden.is_permanent(), "still a refusal, code kept");

        let gone = delayed(Some("M_NOT_FOUND"), Some(404), "Delayed event not found");
        assert!(
            !gone.is_delayed_events_unsupported(),
            "a restart of a fired delay"
        );
        assert!(gone.is_permanent());

        let missing_endpoint = delayed(None, Some(404), "Not Found");
        assert!(missing_endpoint.is_delayed_events_unsupported());

        let transient = delayed(None, Some(502), "Bad Gateway");
        assert!(!transient.is_delayed_events_unsupported());
        assert!(!transient.is_permanent());
    }

    /// R3: the error code decides, never the message.
    #[test]
    fn classification_follows_the_errcode_not_the_message() {
        let cases = [
            (None, None, false),
            (Some("M_LIMIT_EXCEEDED"), Some(429), false),
            (Some("M_UNKNOWN"), Some(502), false),
            (Some("M_FORBIDDEN"), Some(403), true),
            (Some("M_UNRECOGNIZED"), Some(404), true),
            (Some("M_NOT_FOUND"), Some(404), true),
        ];
        for (errcode, status, permanent) in cases {
            assert_eq!(
                error(errcode, status).is_permanent(),
                permanent,
                "{errcode:?} / {status:?}",
            );
        }
        let worded_like_a_refusal = BackendError::new("M_FORBIDDEN: you shall not pass");
        assert!(!worded_like_a_refusal.is_permanent());
    }

    /// R3: the host's mark is final; a status on its own never is.
    #[test]
    fn a_host_mark_is_permanent_and_a_status_alone_is_not() {
        assert!(error(None, None).permanent().is_permanent());
        assert!(!error(None, Some(400)).is_permanent());
        assert!(
            error(Some("M_UNKNOWN"), Some(502))
                .permanent()
                .is_permanent()
        );
        assert!(
            !error(Some("M_FORBIDDEN"), Some(503)).is_permanent(),
            "a code on a 5xx is the server failing, not refusing",
        );
    }
}
