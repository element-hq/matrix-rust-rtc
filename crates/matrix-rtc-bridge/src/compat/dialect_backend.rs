// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! A backend wrapper that renders every send in the dialect its room speaks
//! and translates event types to their wire spelling, so the host's backend
//! sends what it is given verbatim. One copy of what each binding's command
//! sender used to do on its own.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use matrix_rtc_core::{
    BackendError, CommandError, EventIn, MatrixBackend, OpenIdToken, RoomSink, RoomSubjects,
    Subscription, ToDeviceDelivery, ToDeviceRecipient, ToDeviceSink, wire_event_type,
};
use serde_json::Value;

use super::{MemberEventRoute, OutboundDialect};

/// Wraps a host backend with per-room outbound dialects.
pub struct DialectBackend<B> {
    inner: Arc<B>,
    /// Keyed by room: a to-device media key names only its room. Never held
    /// across an await.
    dialects: Mutex<HashMap<String, OutboundDialect>>,
}

impl<B: MatrixBackend> DialectBackend<B> {
    pub fn new(inner: Arc<B>) -> Self {
        Self {
            inner,
            dialects: Mutex::new(HashMap::new()),
        }
    }

    pub fn inner(&self) -> &Arc<B> {
        &self.inner
    }

    /// Make every later send for `room_id` speak `dialect`, replacing any
    /// previous one.
    pub fn set_dialect(&self, room_id: &str, dialect: OutboundDialect) {
        if let Ok(mut dialects) = self.dialects.lock() {
            match dialect {
                OutboundDialect::None => {
                    dialects.remove(room_id);
                }
                dialect => {
                    dialects.insert(room_id.to_owned(), dialect);
                }
            }
        }
    }

    /// Forget `room_id`'s dialect, after a leave has been rendered in it.
    pub fn clear_dialect(&self, room_id: &str) {
        if let Ok(mut dialects) = self.dialects.lock() {
            dialects.remove(room_id);
        }
    }

    pub fn dialect(&self, room_id: &str) -> OutboundDialect {
        self.dialects
            .lock()
            .ok()
            .and_then(|dialects| dialects.get(room_id).cloned())
            .unwrap_or(OutboundDialect::None)
    }

    /// The dialect a to-device message is rendered in: a media key names its
    /// room inside the content, the only routing information the send has.
    fn dialect_for_content(&self, content: &Value) -> OutboundDialect {
        content
            .get("room_id")
            .and_then(Value::as_str)
            .map(|room_id| self.dialect(room_id))
            .unwrap_or(OutboundDialect::None)
    }
}

fn wire(event_type: String) -> String {
    wire_event_type(&event_type).to_owned()
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl<B: MatrixBackend + 'static> MatrixBackend for DialectBackend<B> {
    fn own_user_id(&self) -> String {
        self.inner.own_user_id()
    }

    fn own_device_id(&self) -> String {
        self.inner.own_device_id()
    }

    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        duration_ms: u64,
    ) -> Result<String, CommandError> {
        let dialect = self.dialect(&room_id);
        let content = dialect.rewrite_notification(&event_type, content);
        match dialect.route_member_event(event_type, content, Some(duration_ms)) {
            MemberEventRoute::Sticky {
                event_type,
                content,
            } => {
                self.inner
                    .send_sticky_event(room_id, wire(event_type), content, duration_ms)
                    .await
            }
            // The pre-sticky generation has no sticky map; `duration_ms` has no
            // meaning there.
            MemberEventRoute::Room {
                event_type,
                content,
            } => {
                self.inner
                    .send_room_event(room_id, wire(event_type), content)
                    .await
            }
            // Room state has no TTL; this dialect states the lifetime inside
            // the content. The type is already the legacy wire id.
            MemberEventRoute::State {
                event_type,
                state_key,
                content,
            } => {
                self.inner
                    .send_state_event(room_id, event_type.to_owned(), state_key, content)
                    .await
            }
        }
    }

    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: Option<String>,
        content: Value,
        delay_ms: u64,
    ) -> Result<String, CommandError> {
        if state_key.is_some() {
            return self
                .inner
                .send_delayed_event(room_id, event_type, state_key, content, delay_ms)
                .await;
        }
        // The delayed leave follows the same routing as the join it pairs
        // with. No lifetime: its legacy content is `{}`.
        match self
            .dialect(&room_id)
            .route_member_event(event_type, content, None)
        {
            MemberEventRoute::Sticky {
                event_type,
                content,
            }
            | MemberEventRoute::Room {
                event_type,
                content,
            } => {
                self.inner
                    .send_delayed_event(room_id, wire(event_type), None, content, delay_ms)
                    .await
            }
            MemberEventRoute::State {
                event_type,
                state_key,
                content,
            } => {
                self.inner
                    .send_delayed_event(
                        room_id,
                        event_type.to_owned(),
                        Some(state_key),
                        content,
                        delay_ms,
                    )
                    .await
            }
        }
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
        recipients: Vec<ToDeviceRecipient>,
        message_type: String,
        content: Value,
    ) -> Result<Vec<ToDeviceDelivery>, CommandError> {
        // A to-device message has one type, so in compat mode the key goes
        // out in the legacy dialect alone.
        let (message_type, content) = self
            .dialect_for_content(&content)
            .rewrite_key_message(&message_type, &content)
            .unwrap_or((message_type, content));
        self.inner
            .send_to_device_message(recipients, wire(message_type), content)
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
            .send_state_event(room_id, wire(event_type), state_key, content)
            .await
    }

    async fn send_room_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
    ) -> Result<String, CommandError> {
        // Verbatim: a reaction is not a MatrixRTC type.
        self.inner
            .send_room_event(room_id, event_type, content)
            .await
    }

    async fn redact_event(
        &self,
        room_id: String,
        event_id: String,
        reason: Option<String>,
    ) -> Result<(), CommandError> {
        self.inner.redact_event(room_id, event_id, reason).await
    }

    async fn subscribe_room(
        &self,
        room_id: String,
        subjects: RoomSubjects,
        sink: Arc<dyn RoomSink>,
    ) -> Result<Arc<dyn Subscription>, BackendError> {
        self.inner.subscribe_room(room_id, subjects, sink).await
    }

    async fn subscribe_to_device(
        &self,
        event_types: Vec<String>,
        sink: Arc<dyn ToDeviceSink>,
    ) -> Result<Arc<dyn Subscription>, BackendError> {
        self.inner.subscribe_to_device(event_types, sink).await
    }

    async fn relations(
        &self,
        room_id: String,
        event_id: String,
        rel_type: String,
        event_type: String,
    ) -> Result<Vec<EventIn>, BackendError> {
        self.inner
            .relations(room_id, event_id, rel_type, event_type)
            .await
    }

    async fn openid_token(&self) -> Result<OpenIdToken, BackendError> {
        self.inner.openid_token().await
    }

    async fn rtc_transports(&self) -> Result<Value, BackendError> {
        self.inner.rtc_transports().await
    }
}
