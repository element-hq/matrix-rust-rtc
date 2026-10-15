// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The call's status as the page sees it (`RtcCallStatus` in `ts_types`): a
//! DTO so the serde shape is ours to keep, whatever the call layer's enums
//! become.

use matrix_rtc_call::{CallEnd, CallStatus, MediaPhase, RingPhase};
use serde::Serialize;

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum WasmCallStatus {
    Connecting,
    Calling {
        callee: String,
        ring: WasmRingPhase,
        media: WasmMediaPhase,
    },
    Connected {
        since_ms: u64,
        degraded: bool,
    },
    Ended {
        reason: &'static str,
        /// The transport's description, for `connection_closed` only.
        message: Option<String>,
    },
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum WasmRingPhase {
    Sending,
    Ringing { sent_at_ms: u64, lifetime_ms: u64 },
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum WasmMediaPhase {
    Connecting,
    Connected { degraded: bool },
}

impl From<CallStatus> for WasmCallStatus {
    fn from(value: CallStatus) -> Self {
        match value {
            CallStatus::Connecting => Self::Connecting,
            CallStatus::Calling {
                callee,
                ring,
                media,
            } => Self::Calling {
                callee,
                ring: match ring {
                    RingPhase::Sending => WasmRingPhase::Sending,
                    RingPhase::Ringing {
                        sent_at_ms,
                        lifetime_ms,
                    } => WasmRingPhase::Ringing {
                        sent_at_ms,
                        lifetime_ms,
                    },
                },
                media: match media {
                    MediaPhase::Connecting => WasmMediaPhase::Connecting,
                    MediaPhase::Connected { degraded } => WasmMediaPhase::Connected { degraded },
                },
            },
            CallStatus::Connected { since_ms, degraded } => Self::Connected { since_ms, degraded },
            CallStatus::Ended { reason } => {
                let (reason, message) = match reason {
                    CallEnd::Left => ("left", None),
                    CallEnd::Declined => ("declined", None),
                    CallEnd::Unanswered => ("unanswered", None),
                    CallEnd::RingNotSent => ("ring_not_sent", None),
                    CallEnd::OthersLeft => ("others_left", None),
                    CallEnd::Empty => ("empty", None),
                    CallEnd::SlotClosed => ("slot_closed", None),
                    CallEnd::ConnectionClosed { message } => ("connection_closed", Some(message)),
                };
                Self::Ended { reason, message }
            }
        }
    }
}
