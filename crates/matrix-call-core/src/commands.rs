// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The outbound commands the call layer needs beyond generic RTC signalling.
//!
//! Reactions and the raised hand are plain room events, and lowering a hand
//! is a redaction; neither is something a bare RTC participation ever sends.
//! They live on their own trait, with [`RtcCommandSender`] as a supertrait so
//! one host object serves both layers.

use async_trait::async_trait;
use matrix_rtc_core::{CommandError, RtcCommandSender};
use serde_json::Value;

/// Room-event commands for the call layer, on top of [`RtcCommandSender`].
///
/// Implemented by the same host object that implements the RTC commands; a
/// [`CallSessionManager`](crate::CallSessionManager) hands one `Arc<T>` to both
/// layers.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait CallCommandSender: RtcCommandSender {
    /// Send a plain room event: message-like, neither sticky nor state.
    ///
    /// Used for the Element Call reactions (`io.element.call.reaction`) and the
    /// raised-hand `m.reaction` annotation. In an encrypted room the event must
    /// go out encrypted like any other message; a client SDK's ordinary send
    /// does that on its own.
    ///
    /// # Arguments
    ///
    /// * `room_id` - The room ID where the event should be sent
    /// * `event_type` - The event type, sent verbatim (nothing here is a
    ///   MatrixRTC type with an unstable alias)
    /// * `content` - The event content as a JSON value
    ///
    /// # Returns
    ///
    /// The event id the homeserver assigned, on the same terms as
    /// [`RtcCommandSender::send_sticky_event`]. A raised hand is lowered by
    /// redacting this very event, so the id has to come back.
    async fn send_room_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
    ) -> Result<String, CommandError>;

    /// Redact one of our own room events.
    ///
    /// Used to lower a raised hand: Element Call has no "hand lowered" event,
    /// the annotation is simply redacted.
    ///
    /// # Arguments
    ///
    /// * `room_id` - The room the event was sent to
    /// * `event_id` - The event to redact, as returned by
    ///   [`send_room_event`](Self::send_room_event)
    /// * `reason` - Optional human-readable reason, put in the redaction's
    ///   content
    async fn redact_event(
        &self,
        room_id: String,
        event_id: String,
        reason: Option<String>,
    ) -> Result<(), CommandError>;
}

/// A recording command sender for tests: the core's [`MockCommandSender`] for
/// the RTC commands, plus the room events and redactions of this layer.
///
/// [`MockCommandSender`]: matrix_rtc_core::testing::MockCommandSender
#[cfg(any(test, feature = "testing"))]
#[derive(Default)]
pub struct MockCallCommandSender {
    /// The RTC half; its recorded events are reachable through `Deref`.
    pub inner: matrix_rtc_core::testing::MockCommandSender,
    /// `(room_id, event_type, content)` of every plain room event sent.
    pub room_events: std::sync::Mutex<Vec<(String, String, Value)>>,
    /// `(room_id, event_id, reason)` of every redaction requested.
    pub redactions: std::sync::Mutex<Vec<(String, String, Option<String>)>>,
}

#[cfg(any(test, feature = "testing"))]
impl MockCallCommandSender {
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(any(test, feature = "testing"))]
impl std::ops::Deref for MockCallCommandSender {
    type Target = matrix_rtc_core::testing::MockCommandSender;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(any(test, feature = "testing"))]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl RtcCommandSender for MockCallCommandSender {
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        duration_ms: u64,
    ) -> Result<String, CommandError> {
        self.inner
            .send_sticky_event(room_id, event_type, content, duration_ms)
            .await
    }

    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        delay_ms: u64,
    ) -> Result<String, CommandError> {
        self.inner
            .send_delayed_event(room_id, event_type, content, delay_ms)
            .await
    }

    async fn restart_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        self.inner.restart_delayed_event(room_id, delay_id).await
    }

    async fn cancel_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        self.inner.cancel_delayed_event(room_id, delay_id).await
    }

    async fn send_to_device_message(
        &self,
        recipients: Vec<matrix_rtc_core::ToDeviceRecipient>,
        message_type: String,
        content: Value,
    ) -> Result<Vec<matrix_rtc_core::ToDeviceDelivery>, CommandError> {
        self.inner
            .send_to_device_message(recipients, message_type, content)
            .await
    }

    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content: Value,
    ) -> Result<String, CommandError> {
        self.inner
            .send_state_event(room_id, event_type, state_key, content)
            .await
    }
}

#[cfg(any(test, feature = "testing"))]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl CallCommandSender for MockCallCommandSender {
    async fn send_room_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
    ) -> Result<String, CommandError> {
        let mut guard = self.room_events.lock().unwrap();
        guard.push((room_id, event_type, content));
        // Numbered by send order, so a test can name the event a redaction is
        // expected to target.
        Ok(format!("$room-{}", guard.len()))
    }

    async fn redact_event(
        &self,
        room_id: String,
        event_id: String,
        reason: Option<String>,
    ) -> Result<(), CommandError> {
        self.redactions
            .lock()
            .unwrap()
            .push((room_id, event_id, reason));
        Ok(())
    }
}
