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

use crate::{
    BackendError, CommandError, EventIn, MatrixBackend, OpenIdToken, RoomSink, RoomSubjects,
    Subscription, ToDeviceDelivery, ToDeviceRecipient, ToDeviceSink, wire_event_type,
};
use async_trait::async_trait;
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

#[cfg(test)]
mod tests {
    use crate::testing::MockBackend;
    use crate::{KEY_MESSAGE_TYPE, ToDeviceRecipient};
    use serde_json::json;

    use super::*;
    use crate::compat::MembershipFormat;
    use crate::compat::ingest::outbound_dialect;

    fn backend() -> (Arc<MockBackend>, DialectBackend<MockBackend>) {
        let mock = Arc::new(MockBackend::new());
        (mock.clone(), DialectBackend::new(mock))
    }

    fn dialect(compat: MembershipFormat, room_id: &str) -> OutboundDialect {
        outbound_dialect(
            compat,
            "@alice:example.org",
            "DEVICE",
            room_id,
            "m.call#room",
        )
    }

    #[tokio::test]
    async fn every_outbound_type_reaches_the_backend_in_its_wire_spelling() {
        let (mock, backend) = backend();
        backend
            .send_sticky_event(
                "!room:example.org".to_owned(),
                "m.rtc.member".to_owned(),
                json!({ "slot_id": "m.call#room" }),
                90_000,
            )
            .await
            .unwrap();
        backend
            .send_state_event(
                "!room:example.org".to_owned(),
                crate::SLOT_EVENT_TYPE.to_owned(),
                "m.call#room".to_owned(),
                json!({ "status": "open" }),
            )
            .await
            .unwrap();
        backend
            .send_to_device_message(
                vec![ToDeviceRecipient::new("@bob:example.org", "BOBDEV")],
                KEY_MESSAGE_TYPE.to_owned(),
                json!({ "room_id": "!room:example.org" }),
            )
            .await
            .unwrap();

        let (_, event_type, _, duration) = mock.last_sticky_event().unwrap();
        assert_eq!(event_type, "org.matrix.msc4143.rtc.member");
        assert_eq!(
            duration, 90_000,
            "the sticky lifetime reaches the backend unchanged"
        );
        assert_eq!(
            mock.state_events.lock().unwrap()[0].1,
            "org.matrix.msc4143.rtc.slot"
        );
        assert_eq!(
            mock.last_to_device_message().unwrap().2,
            "org.matrix.msc4143.rtc.encryption_key"
        );
    }

    /// The pre-sticky wire has no sticky map, so in that mode the notification
    /// is an ordinary room event; in every other mode it stays sticky.
    #[tokio::test]
    async fn a_notification_goes_out_as_a_room_event_in_the_state_dialect() {
        let (mock, backend) = backend();
        backend.set_dialect(
            "!legacy:example.org",
            dialect(MembershipFormat::RoomState, "!legacy:example.org"),
        );
        let notification = json!({
            "application": { "type": "m.call", "notification_type": "ring" },
            "m.mentions": { "user_ids": [], "room": true },
        });

        for room_id in ["!legacy:example.org", "!modern:example.org"] {
            backend
                .send_sticky_event(
                    room_id.to_owned(),
                    "m.rtc.notification".to_owned(),
                    notification.clone(),
                    30_000,
                )
                .await
                .unwrap();
        }

        let room_events = mock.room_events.lock().unwrap();
        let sticky_events = mock.sticky_events.lock().unwrap();
        assert_eq!(room_events.len(), 1);
        assert_eq!(room_events[0].0, "!legacy:example.org");
        assert_eq!(room_events[0].1, "org.matrix.msc4075.rtc.notification");
        assert_eq!(sticky_events.len(), 1);
        assert_eq!(sticky_events[0].0, "!modern:example.org");
        assert_eq!(sticky_events[0].1, "org.matrix.msc4075.rtc.notification");
    }

    /// A to-device message names its room only inside the content, so that is
    /// what decides the dialect — for that room alone.
    #[tokio::test]
    async fn a_media_key_takes_the_dialect_of_its_own_room() {
        let (mock, backend) = backend();
        backend.set_dialect(
            "!legacy:example.org",
            dialect(MembershipFormat::Sticky2025, "!legacy:example.org"),
        );
        let key = |room_id: &str| {
            json!({
                "room_id": room_id,
                "member_id": "MEMBER",
                "media_key": { "index": 0, "key": "AAAA" },
            })
        };
        let recipients = vec![ToDeviceRecipient::new("@bob:example.org", "BOBDEV")];

        for room_id in ["!legacy:example.org", "!modern:example.org"] {
            backend
                .send_to_device_message(
                    recipients.clone(),
                    KEY_MESSAGE_TYPE.to_owned(),
                    key(room_id),
                )
                .await
                .unwrap();
        }

        let types: Vec<String> = mock
            .to_device_messages
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, message_type, _)| message_type.clone())
            .collect();
        assert_eq!(
            types,
            [
                "io.element.call.encryption_keys",
                "org.matrix.msc4143.rtc.encryption_key",
            ]
        );
    }

    /// The pre-sticky delayed leave is a delayed state event: the wrapper
    /// routes it, so a backend only ever sends what it is told.
    #[tokio::test]
    async fn a_pre_sticky_delayed_leave_is_a_delayed_state_event() {
        let (mock, backend) = backend();
        backend.set_dialect(
            "!legacy:example.org",
            dialect(MembershipFormat::RoomState, "!legacy:example.org"),
        );
        backend
            .send_delayed_event(
                "!legacy:example.org".to_owned(),
                "m.rtc.member".to_owned(),
                None,
                json!({ "slot_id": "m.call#room", "msc4354_sticky_key": "k", "member": { "membership": "leave" } }),
                30_000,
            )
            .await
            .unwrap();
        let (_, event_type, state_key, content, _) = mock.last_delayed_event().unwrap();
        assert_eq!(event_type, "org.matrix.msc3401.call.member");
        assert_eq!(
            state_key.as_deref(),
            Some("_@alice:example.org_DEVICE_m.call")
        );
        assert_eq!(content, json!({}));
    }
}
