// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The host's outbound Matrix commands, as a uniffi foreign trait.
//!
//! [`RtcCommandSenderCallback`] is what a host implements (matrix-js-sdk on
//! web, matrix-rust-sdk on React Native); [`ForeignCommandSender`] adapts it
//! to the core's [`RtcCommandSender`]. Event types are translated to their
//! wire spelling here, so the host sends what it is given verbatim, and
//! content crosses as a JSON string.

use std::sync::Arc;

use async_trait::async_trait;
use matrix_rtc_core::{
    CommandError, RtcCommandSender, ToDeviceDelivery, ToDeviceRecipient, wire_event_type,
};
use serde_json::Value;

/// Why a command failed, as the host reports it. A host maps its SDK's errors
/// onto these; the core reads `NotSupported` as "stop asking for delayed
/// events on this homeserver".
#[derive(Debug, Clone, thiserror::Error, uniffi::Error)]
pub enum CommandSenderError {
    /// The client SDK refused the command before sending it.
    #[error("rejected by the client: {0}")]
    ClientRejected(String),
    /// The send failed (network, homeserver error).
    #[error("send failed: {0}")]
    SendError(String),
    /// The homeserver does not support delayed events (MSC4140): a 404
    /// `M_UNRECOGNIZED`, or a 403 saying they are disallowed.
    #[error("delayed events are not supported: {0}")]
    NotSupported(String),
}

impl From<CommandSenderError> for CommandError {
    fn from(error: CommandSenderError) -> Self {
        match error {
            CommandSenderError::ClientRejected(message) => Self::ClientRejected(message),
            CommandSenderError::SendError(message) => Self::SendError(message),
            CommandSenderError::NotSupported(message) => Self::DelayedEventsNotSupported(message),
        }
    }
}

/// One device a to-device message is addressed to.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiToDeviceRecipient {
    pub user_id: String,
    pub device_id: String,
}

/// What became of one recipient of a to-device send. `error` is `None` when
/// the message was accepted for that recipient; the core never re-sends a key
/// to a recipient reported as served, so a failure must not be reported as a
/// success.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiToDeviceDelivery {
    pub user_id: String,
    pub device_id: String,
    pub error: Option<String>,
}

/// Host-implemented outbound sends: the six commands a bare RTC participation
/// needs. Every method is async; uniffi renders them as promises / `suspend`.
///
/// (`async_trait` must sit *under* the uniffi attribute: uniffi parses the
/// original `async fn` tokens, `async_trait` then makes the trait
/// dyn-compatible for the Rust side.)
#[uniffi::export(with_foreign)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait RtcCommandSenderCallback: Send + Sync {
    /// Send an MSC4354 sticky event. `event_type` is already the wire
    /// spelling; `duration_ms` must be passed through verbatim — the core
    /// refreshes the entry before it elapses, so a substituted value drops
    /// the membership mid-call or leaves a ghost behind. Returns the event id.
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content_json: String,
        duration_ms: u64,
    ) -> Result<String, CommandSenderError>;

    /// Schedule an MSC4140 delayed event. Returns the *delay id*, which
    /// `restart_delayed_event` and `cancel_delayed_event` take. Throw
    /// `NotSupported` when the homeserver refuses delayed events outright;
    /// the join still succeeds, with a shorter membership lifetime.
    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        content_json: String,
        delay_ms: u64,
    ) -> Result<String, CommandSenderError>;

    async fn restart_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandSenderError>;

    async fn cancel_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandSenderError>;

    /// Send an encrypted to-device message to each recipient, reporting the
    /// outcome per recipient.
    async fn send_to_device_message(
        &self,
        recipients: Vec<FfiToDeviceRecipient>,
        message_type: String,
        content_json: String,
    ) -> Result<Vec<FfiToDeviceDelivery>, CommandSenderError>;

    /// Send a state event (`m.rtc.slot`). Returns the event id.
    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content_json: String,
    ) -> Result<String, CommandSenderError>;
}

/// The core's command sender, backed by the host's callback.
pub struct ForeignCommandSender {
    callback: Arc<dyn RtcCommandSenderCallback>,
}

impl ForeignCommandSender {
    pub fn new(callback: Arc<dyn RtcCommandSenderCallback>) -> Self {
        Self { callback }
    }
}

fn to_json(content: &Value) -> Result<String, CommandError> {
    serde_json::to_string(content).map_err(|e| CommandError::SerializationError(e.to_string()))
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl RtcCommandSender for ForeignCommandSender {
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        duration_ms: u64,
    ) -> Result<String, CommandError> {
        let wire_type = wire_event_type(&event_type).to_owned();
        log::debug!("[{room_id}] sticky {wire_type} ({duration_ms}ms)");
        self.callback
            .send_sticky_event(room_id, wire_type, to_json(&content)?, duration_ms)
            .await
            .map_err(CommandError::from)
    }

    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        delay_ms: u64,
    ) -> Result<String, CommandError> {
        let wire_type = wire_event_type(&event_type).to_owned();
        log::debug!("[{room_id}] delayed {wire_type} ({delay_ms}ms)");
        self.callback
            .send_delayed_event(room_id, wire_type, to_json(&content)?, delay_ms)
            .await
            .map_err(CommandError::from)
    }

    async fn restart_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        self.callback
            .restart_delayed_event(room_id, delay_id)
            .await
            .map_err(CommandError::from)
    }

    async fn cancel_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        self.callback
            .cancel_delayed_event(room_id, delay_id)
            .await
            .map_err(CommandError::from)
    }

    async fn send_to_device_message(
        &self,
        recipients: Vec<ToDeviceRecipient>,
        message_type: String,
        content: Value,
    ) -> Result<Vec<ToDeviceDelivery>, CommandError> {
        let wire_type = wire_event_type(&message_type).to_owned();
        log::debug!("to-device {wire_type} to {} recipient(s)", recipients.len());
        let recipients = recipients
            .into_iter()
            .map(|recipient| FfiToDeviceRecipient {
                user_id: recipient.user_id,
                device_id: recipient.device_id,
            })
            .collect();
        let deliveries = self
            .callback
            .send_to_device_message(recipients, wire_type, to_json(&content)?)
            .await
            .map_err(CommandError::from)?;
        Ok(deliveries
            .into_iter()
            .map(|delivery| ToDeviceDelivery {
                recipient: ToDeviceRecipient::new(delivery.user_id, delivery.device_id),
                error: delivery.error,
            })
            .collect())
    }

    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content: Value,
    ) -> Result<String, CommandError> {
        let wire_type = wire_event_type(&event_type).to_owned();
        log::debug!("[{room_id}] state {wire_type} key={state_key}");
        self.callback
            .send_state_event(room_id, wire_type, state_key, to_json(&content)?)
            .await
            .map_err(CommandError::from)
    }
}
