// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The host-implemented Matrix backend over uniffi: the one contract a host
//! implements (sends and subscriptions), the sink objects it delivers into,
//! and the adapter that turns it into the core's `MatrixBackend`. Content
//! crosses as JSON strings; DTOs keep uniffi shapes out of the core.

use std::sync::Arc;

use async_trait::async_trait;
use matrix_rtc_core::{
    BackendError, CommandError, EventEncryption, EventIn, MatrixBackend as CoreBackend,
    OpenIdToken, RoomSink as CoreRoomSink, RoomSubjects, Subscription as CoreSubscription,
    ToDeviceDelivery, ToDeviceMessageIn, ToDeviceRecipient, ToDeviceSink as CoreToDeviceSink,
};
use serde_json::Value;

/// A failure reported by the host's Matrix client. Carry the Matrix `errcode`
/// and HTTP status when the client has them: the library classifies them (a
/// delayed-event refusal, for one); the host does not.
#[derive(Debug, Clone, thiserror::Error, uniffi::Error)]
pub enum FfiBackendError {
    // `reason`, not `message`: Kotlin's generated exception already has a
    // `message` property, and a field of that name fails to compile.
    #[error("{reason}")]
    Failed {
        errcode: Option<String>,
        status: Option<u16>,
        reason: String,
    },
}

impl FfiBackendError {
    fn into_parts(self) -> (Option<String>, Option<u16>, String) {
        match self {
            Self::Failed {
                errcode,
                status,
                reason,
            } => (errcode, status, reason),
        }
    }

    fn into_backend_error(self) -> BackendError {
        let (errcode, status, message) = self.into_parts();
        BackendError::with_matrix_error(errcode, status, message)
    }

    fn into_send_error(self) -> CommandError {
        CommandError::SendError(self.into_parts().2)
    }

    fn into_delayed_error(self) -> CommandError {
        let (errcode, status, message) = self.into_parts();
        CommandError::delayed_event_failure(errcode.as_deref(), status, message)
    }
}

/// What the client reports about how an event arrived. `encrypted: false`
/// means cleartext; the other two fields then mean nothing. Report only what
/// the client says — a cross-signing status it cannot give stays `null`.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct FfiEventEncryption {
    pub encrypted: bool,
    #[uniffi(default = None)]
    pub sender_device_id: Option<String>,
    #[uniffi(default = None)]
    pub sender_cross_signed: Option<bool>,
}

impl From<FfiEventEncryption> for EventEncryption {
    fn from(value: FfiEventEncryption) -> Self {
        if value.encrypted {
            EventEncryption::Encrypted {
                sender_device_id: value.sender_device_id,
                sender_cross_signed: value.sender_cross_signed,
            }
        } else {
            EventEncryption::Cleartext
        }
    }
}

/// A room event as the client handed it over, content as JSON. The library
/// parses it; the host does not.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiEventIn {
    pub event_id: String,
    pub sender: String,
    pub event_type: String,
    /// Set for state events.
    #[uniffi(default = None)]
    pub state_key: Option<String>,
    #[uniffi(default = 0)]
    pub origin_server_ts: u64,
    /// The whole decrypted `content` object, as JSON.
    pub content_json: String,
    pub encryption: FfiEventEncryption,
}

impl FfiEventIn {
    fn into_core(self) -> Option<EventIn> {
        let content = parse_content(&self.event_type, &self.sender, &self.content_json)?;
        Some(EventIn {
            event_id: self.event_id,
            sender: self.sender,
            event_type: self.event_type,
            state_key: self.state_key,
            origin_server_ts: self.origin_server_ts,
            content,
            encryption: self.encryption.into(),
        })
    }
}

/// A decrypted to-device message as the client handed it over.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiToDeviceMessageIn {
    pub sender: String,
    pub event_type: String,
    pub content_json: String,
    pub encryption: FfiEventEncryption,
}

fn parse_content(event_type: &str, sender: &str, content_json: &str) -> Option<Value> {
    serde_json::from_str(content_json)
        .inspect_err(|error| {
            log::warn!("ignoring a {event_type} from {sender} whose content is not JSON: {error}");
        })
        .ok()
}

fn events_into_core(events: Vec<FfiEventIn>) -> Vec<EventIn> {
    events
        .into_iter()
        .filter_map(FfiEventIn::into_core)
        .collect()
}

/// What the library wants delivered for one room; see [`MatrixBackend::subscribe_room`].
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct FfiRoomSubjects {
    /// Stable and unstable spellings both listed; deliver either.
    pub state_event_types: Vec<String>,
    /// Message-like types to forward as they arrive, redactions included.
    pub timeline_event_types: Vec<String>,
}

impl From<RoomSubjects> for FfiRoomSubjects {
    fn from(value: RoomSubjects) -> Self {
        Self {
            state_event_types: value.state_event_types,
            timeline_event_types: value.timeline_event_types,
        }
    }
}

/// One device a to-device message is addressed to.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiToDeviceRecipient {
    pub user_id: String,
    pub device_id: String,
}

/// What became of one recipient of a to-device send.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiToDeviceDelivery {
    pub user_id: String,
    pub device_id: String,
    /// `null` when delivered; otherwise why not. Surfaced to the logs, not
    /// interpreted.
    pub error: Option<String>,
}

/// A Matrix OpenID token, as returned by
/// `POST /_matrix/client/v3/user/{userId}/openid/request_token`.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiOpenIdToken {
    pub access_token: String,
    pub token_type: String,
    pub matrix_server_name: String,
    pub expires_in_secs: u64,
}

/// Where the host delivers a room's subjects. Every call returns at once.
///
/// For sticky events, state events and joined members each call carries the
/// room's **complete current set** for that subject, and the first call is the
/// current set at subscription time. An empty set means none.
#[derive(uniffi::Object)]
pub struct RoomSink {
    inner: Arc<dyn CoreRoomSink>,
}

#[uniffi::export]
impl RoomSink {
    pub fn on_sticky_events(&self, events: Vec<FfiEventIn>) {
        self.inner.on_sticky_events(events_into_core(events));
    }

    pub fn on_state_events(&self, event_type: String, events: Vec<FfiEventIn>) {
        self.inner
            .on_state_events(event_type, events_into_core(events));
    }

    pub fn on_joined_members(&self, user_ids: Vec<String>) {
        self.inner.on_joined_members(user_ids);
    }

    pub fn on_encryption(&self, encrypted: bool) {
        self.inner.on_encryption(encrypted);
    }

    pub fn on_timeline_events(&self, events: Vec<FfiEventIn>) {
        self.inner.on_timeline_events(events_into_core(events));
    }

    pub fn on_redaction(&self, event_id: String) {
        self.inner.on_redaction(event_id);
    }
}

/// Where the host delivers to-device messages of the subscribed types.
#[derive(uniffi::Object)]
pub struct ToDeviceSink {
    inner: Arc<dyn CoreToDeviceSink>,
}

#[uniffi::export]
impl ToDeviceSink {
    pub fn on_to_device_message(&self, message: FfiToDeviceMessageIn) {
        let Some(content) =
            parse_content(&message.event_type, &message.sender, &message.content_json)
        else {
            return;
        };
        self.inner.on_to_device_message(ToDeviceMessageIn {
            sender: message.sender,
            event_type: message.event_type,
            content,
            encryption: message.encryption.into(),
        });
    }
}

/// A subscription the library ends by calling `cancel`. Cancelling twice is a
/// no-op.
#[uniffi::export(with_foreign)]
pub trait BackendSubscription: Send + Sync {
    fn cancel(&self);
}

/// Everything the library needs from the host's Matrix client. Implement it
/// once, with the host's own client; the library subscribes, orders and parses.
///
/// Sends take the event type already translated to its wire spelling: send it
/// verbatim. Every send returns the homeserver's event id (or, for a delayed
/// send, the MSC4140 delay id).
///
/// (`async_trait` must sit *under* the uniffi attribute: uniffi parses the
/// original `async fn` tokens, `async_trait` then makes the trait
/// dyn-compatible for the Rust side.)
#[uniffi::export(with_foreign)]
#[async_trait]
pub trait MatrixBackend: Send + Sync {
    /// The account the client is logged in as.
    fn own_user_id(&self) -> String;

    /// The device the client is logged in as.
    fn own_device_id(&self) -> String;

    /// `duration_ms` is how long the homeserver keeps the sticky entry; pass
    /// it through verbatim (matrix-sdk-ffi: `Room.sendStickyRaw`). The
    /// library re-sends before it elapses.
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content_json: String,
        duration_ms: u64,
    ) -> Result<String, FfiBackendError>;

    /// With `state_key`, a delayed state event; otherwise a delayed
    /// message-like event. Returns the MSC4140 delay id. A homeserver that
    /// refuses delayed events (`M_UNRECOGNIZED`, or matrix.org's `M_FORBIDDEN`)
    /// does not fail the join: report the `errcode` and the library stops
    /// asking.
    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: Option<String>,
        content_json: String,
        delay_ms: u64,
    ) -> Result<String, FfiBackendError>;

    /// MSC4140's restart action. Never emulate it with cancel-and-resend.
    async fn restart_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), FfiBackendError>;

    async fn cancel_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), FfiBackendError>;

    /// Olm-encrypted, to exactly these devices, with one outcome per
    /// recipient (an unreachable device must not silence the others).
    async fn send_to_device_message(
        &self,
        recipients: Vec<FfiToDeviceRecipient>,
        message_type: String,
        content_json: String,
    ) -> Result<Vec<FfiToDeviceDelivery>, FfiBackendError>;

    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content_json: String,
    ) -> Result<String, FfiBackendError>;

    /// A plain message-like send, encrypted by the client in an encrypted
    /// room.
    async fn send_room_event(
        &self,
        room_id: String,
        event_type: String,
        content_json: String,
    ) -> Result<String, FfiBackendError>;

    async fn redact_event(
        &self,
        room_id: String,
        event_id: String,
        reason: Option<String>,
    ) -> Result<(), FfiBackendError>;

    /// Deliver `subjects` for `room_id` into `sink`: the current sets first,
    /// then every change, until the returned subscription is cancelled.
    ///
    /// Synchronous: register the client's listeners and return; deliver from
    /// the host's own tasks. Sink calls may come from any thread and return at
    /// once. (uniffi cannot hand an object back from an async foreign method.)
    fn subscribe_room(
        &self,
        room_id: String,
        subjects: FfiRoomSubjects,
        sink: Arc<RoomSink>,
    ) -> Result<Arc<dyn BackendSubscription>, FfiBackendError>;

    /// Deliver every decrypted to-device message of `event_types` into `sink`
    /// until the returned subscription is cancelled. Synchronous, as
    /// `subscribe_room`.
    fn subscribe_to_device(
        &self,
        event_types: Vec<String>,
        sink: Arc<ToDeviceSink>,
    ) -> Result<Arc<dyn BackendSubscription>, FfiBackendError>;

    /// `GET /rooms/{room_id}/relations/{event_id}/{rel_type}/{event_type}`,
    /// decrypted.
    async fn relations(
        &self,
        room_id: String,
        event_id: String,
        rel_type: String,
        event_type: String,
    ) -> Result<Vec<FfiEventIn>, FfiBackendError>;

    /// A fresh OpenID token for the account.
    async fn openid_token(&self) -> Result<FfiOpenIdToken, FfiBackendError>;

    /// The `rtc_transports` array of `GET /_matrix/client/v1/rtc/transports`,
    /// as JSON; `[]` when the homeserver lacks the endpoint.
    async fn rtc_transports(&self) -> Result<String, FfiBackendError>;
}

/// Logs what the host made of an outbound command, so "the core decided to
/// send X" and "the host accepted/rejected X" are adjacent in the log.
fn log_command<T>(what: &str, outcome: Result<T, CommandError>) -> Result<T, CommandError> {
    match &outcome {
        Ok(_) => log::debug!("command sent: {what}"),
        Err(error) => log::warn!("command failed: {what}: {error}"),
    }
    outcome
}

/// Kept at `trace`: the content of a to-device message is key material.
fn trace_command_content(what: &str, content_json: &str) {
    log::trace!("command sending: {what} content={content_json}");
}

fn to_json(content: &Value) -> Result<String, CommandError> {
    serde_json::to_string(content).map_err(|e| CommandError::SerializationError(e.to_string()))
}

struct FfiSubscription(Arc<dyn BackendSubscription>);

impl CoreSubscription for FfiSubscription {
    fn cancel(&self) {
        self.0.cancel();
    }
}

/// The core's backend over the host's. Dialects and wire types are the
/// wrapper's business (`DialectBackend`), so this only carries.
pub struct FfiBackend {
    host: Arc<dyn MatrixBackend>,
}

impl FfiBackend {
    pub fn new(host: Arc<dyn MatrixBackend>) -> Self {
        Self { host }
    }
}

#[async_trait]
impl CoreBackend for FfiBackend {
    fn own_user_id(&self) -> String {
        self.host.own_user_id()
    }

    fn own_device_id(&self) -> String {
        self.host.own_device_id()
    }

    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        duration_ms: u64,
    ) -> Result<String, CommandError> {
        let content_json = to_json(&content)?;
        let what = format!("sticky [{room_id}] type={event_type}");
        trace_command_content(&what, &content_json);
        log_command(
            &what,
            self.host
                .send_sticky_event(room_id, event_type, content_json, duration_ms)
                .await
                .map_err(FfiBackendError::into_send_error),
        )
    }

    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: Option<String>,
        content: Value,
        delay_ms: u64,
    ) -> Result<String, CommandError> {
        let content_json = to_json(&content)?;
        let what = format!(
            "delayed [{room_id}] type={event_type} state_key={state_key:?} delay={delay_ms}ms"
        );
        trace_command_content(&what, &content_json);
        log_command(
            &what,
            self.host
                .send_delayed_event(room_id, event_type, state_key, content_json, delay_ms)
                .await
                .map_err(FfiBackendError::into_delayed_error),
        )
    }

    async fn restart_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        let what = format!("restart delayed [{room_id}] delay_id={delay_id}");
        log_command(
            &what,
            self.host
                .restart_delayed_event(room_id, delay_id)
                .await
                .map_err(FfiBackendError::into_delayed_error),
        )
    }

    async fn cancel_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        let what = format!("cancel delayed [{room_id}] delay_id={delay_id}");
        log_command(
            &what,
            self.host
                .cancel_delayed_event(room_id, delay_id)
                .await
                .map_err(FfiBackendError::into_delayed_error),
        )
    }

    async fn send_to_device_message(
        &self,
        recipients: Vec<ToDeviceRecipient>,
        message_type: String,
        content: Value,
    ) -> Result<Vec<ToDeviceDelivery>, CommandError> {
        let content_json = to_json(&content)?;
        let what = format!(
            "to-device type={message_type} to {} recipient(s)",
            recipients.len()
        );
        trace_command_content(&what, &content_json);
        let recipients = recipients
            .into_iter()
            .map(|recipient| FfiToDeviceRecipient {
                user_id: recipient.user_id,
                device_id: recipient.device_id,
            })
            .collect();
        let deliveries = log_command(
            &what,
            self.host
                .send_to_device_message(recipients, message_type, content_json)
                .await
                .map_err(FfiBackendError::into_send_error),
        )?;
        Ok(deliveries
            .into_iter()
            .map(|delivery| {
                let recipient = ToDeviceRecipient::new(delivery.user_id, delivery.device_id);
                match delivery.error {
                    Some(error) => ToDeviceDelivery::failed(recipient, error),
                    None => ToDeviceDelivery::sent(recipient),
                }
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
        let content_json = to_json(&content)?;
        let what = format!("state [{room_id}] type={event_type} state_key={state_key}");
        trace_command_content(&what, &content_json);
        log_command(
            &what,
            self.host
                .send_state_event(room_id, event_type, state_key, content_json)
                .await
                .map_err(FfiBackendError::into_send_error),
        )
    }

    async fn send_room_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
    ) -> Result<String, CommandError> {
        let content_json = to_json(&content)?;
        let what = format!("room event [{room_id}] type={event_type}");
        trace_command_content(&what, &content_json);
        log_command(
            &what,
            self.host
                .send_room_event(room_id, event_type, content_json)
                .await
                .map_err(FfiBackendError::into_send_error),
        )
    }

    async fn redact_event(
        &self,
        room_id: String,
        event_id: String,
        reason: Option<String>,
    ) -> Result<(), CommandError> {
        let what = format!("redact [{room_id}] event_id={event_id}");
        log_command(
            &what,
            self.host
                .redact_event(room_id, event_id, reason)
                .await
                .map_err(FfiBackendError::into_send_error),
        )
    }

    async fn subscribe_room(
        &self,
        room_id: String,
        subjects: RoomSubjects,
        sink: Arc<dyn CoreRoomSink>,
    ) -> Result<Arc<dyn CoreSubscription>, BackendError> {
        log::debug!("[{room_id}] subscribing: {subjects:?}");
        let subscription = self
            .host
            .subscribe_room(room_id, subjects.into(), Arc::new(RoomSink { inner: sink }))
            .map_err(FfiBackendError::into_backend_error)?;
        Ok(Arc::new(FfiSubscription(subscription)))
    }

    async fn subscribe_to_device(
        &self,
        event_types: Vec<String>,
        sink: Arc<dyn CoreToDeviceSink>,
    ) -> Result<Arc<dyn CoreSubscription>, BackendError> {
        log::debug!("subscribing to to-device messages: {event_types:?}");
        let subscription = self
            .host
            .subscribe_to_device(event_types, Arc::new(ToDeviceSink { inner: sink }))
            .map_err(FfiBackendError::into_backend_error)?;
        Ok(Arc::new(FfiSubscription(subscription)))
    }

    async fn relations(
        &self,
        room_id: String,
        event_id: String,
        rel_type: String,
        event_type: String,
    ) -> Result<Vec<EventIn>, BackendError> {
        let events = self
            .host
            .relations(room_id, event_id, rel_type, event_type)
            .await
            .map_err(FfiBackendError::into_backend_error)?;
        Ok(events_into_core(events))
    }

    async fn openid_token(&self) -> Result<OpenIdToken, BackendError> {
        let token = self
            .host
            .openid_token()
            .await
            .map_err(FfiBackendError::into_backend_error)?;
        Ok(OpenIdToken {
            access_token: token.access_token,
            token_type: token.token_type,
            matrix_server_name: token.matrix_server_name,
            expires_in: token.expires_in_secs,
        })
    }

    async fn rtc_transports(&self) -> Result<Value, BackendError> {
        let json = self
            .host
            .rtc_transports()
            .await
            .map_err(FfiBackendError::into_backend_error)?;
        serde_json::from_str(&json)
            .map_err(|error| BackendError::new(format!("rtc_transports is not JSON: {error}")))
    }
}

/// A host backend implemented in Rust, driving the exported surface the way a
/// host would: records every send, keeps the sinks so a test can deliver sets.
#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub(crate) enum Carrier {
        Sticky,
        State,
        DelayedSticky,
        DelayedState,
        Room,
    }

    #[derive(Clone, Debug)]
    pub(crate) struct Send {
        pub carrier: Carrier,
        pub event_type: String,
        pub state_key: Option<String>,
        pub content: Value,
    }

    #[derive(Clone)]
    pub(crate) struct ToDeviceSend {
        pub user_id: String,
        pub device_id: String,
        pub content: Value,
    }

    pub(crate) struct MockSubscription {
        pub cancelled: AtomicBool,
    }

    impl BackendSubscription for MockSubscription {
        fn cancel(&self) {
            self.cancelled.store(true, Ordering::SeqCst);
        }
    }

    #[derive(Default)]
    pub(crate) struct MockHost {
        pub sent_types: Mutex<Vec<String>>,
        pub sticky_duration_ms: Mutex<Option<u64>>,
        pub to_device: Mutex<Vec<ToDeviceSend>>,
        pub sends: Mutex<Vec<Send>>,
        /// Model a homeserver with MSC4140 switched off, the way matrix.org
        /// answers: every delayed send is refused.
        pub refuse_delayed: AtomicBool,
        pub room_sinks: Mutex<HashMap<String, (FfiRoomSubjects, Arc<RoomSink>)>>,
        pub to_device_sink: Mutex<Option<(Vec<String>, Arc<ToDeviceSink>)>>,
        pub transports_json: Mutex<String>,
        pub relations_answers: Mutex<HashMap<String, Vec<FfiEventIn>>>,
    }

    impl MockHost {
        pub fn new() -> Arc<Self> {
            Arc::new(Self {
                transports_json: Mutex::new("[]".to_owned()),
                ..Self::default()
            })
        }

        fn record(&self, event_type: &str) {
            self.sent_types.lock().unwrap().push(event_type.to_owned());
        }

        fn record_send(
            &self,
            carrier: Carrier,
            event_type: &str,
            state_key: Option<String>,
            content_json: &str,
        ) {
            self.sends.lock().unwrap().push(Send {
                carrier,
                event_type: event_type.to_owned(),
                state_key,
                content: serde_json::from_str(content_json).unwrap_or(Value::Null),
            });
        }

        pub fn sends(&self) -> Vec<Send> {
            self.sends.lock().unwrap().clone()
        }

        pub fn sent_types(&self) -> Vec<String> {
            self.sent_types.lock().unwrap().clone()
        }

        pub fn to_device_for(&self, user_id: &str, device_id: &str) -> Vec<Value> {
            self.to_device
                .lock()
                .unwrap()
                .iter()
                .filter(|send| send.user_id == user_id && send.device_id == device_id)
                .map(|send| send.content.clone())
                .collect()
        }

        pub fn clear_to_device(&self) {
            self.to_device.lock().unwrap().clear();
        }

        pub fn room_sink(&self, room_id: &str) -> Arc<RoomSink> {
            self.room_sinks
                .lock()
                .unwrap()
                .get(room_id)
                .map(|(_, sink)| sink.clone())
                .expect("the room should be attached")
        }

        pub fn subjects(&self, room_id: &str) -> Option<FfiRoomSubjects> {
            self.room_sinks
                .lock()
                .unwrap()
                .get(room_id)
                .map(|(subjects, _)| subjects.clone())
        }

        fn delayed_refusal(&self) -> Option<FfiBackendError> {
            self.refuse_delayed
                .load(Ordering::Relaxed)
                .then(|| FfiBackendError::Failed {
                    errcode: Some("M_FORBIDDEN".to_owned()),
                    status: Some(403),
                    reason: "Sending delayed events has been disallowed".to_owned(),
                })
        }
    }

    #[async_trait]
    impl MatrixBackend for MockHost {
        fn own_user_id(&self) -> String {
            "@alice:example.org".to_owned()
        }

        fn own_device_id(&self) -> String {
            "DEVICE".to_owned()
        }

        async fn send_sticky_event(
            &self,
            _room_id: String,
            event_type: String,
            content_json: String,
            duration_ms: u64,
        ) -> Result<String, FfiBackendError> {
            self.record(&event_type);
            *self.sticky_duration_ms.lock().unwrap() = Some(duration_ms);
            self.record_send(Carrier::Sticky, &event_type, None, &content_json);
            Ok(format!("$sticky-{}", self.sends.lock().unwrap().len()))
        }

        async fn send_delayed_event(
            &self,
            room_id: String,
            event_type: String,
            state_key: Option<String>,
            content_json: String,
            _delay_ms: u64,
        ) -> Result<String, FfiBackendError> {
            if let Some(refusal) = self.delayed_refusal() {
                return Err(refusal);
            }
            self.record(&event_type);
            let carrier = if state_key.is_some() {
                Carrier::DelayedState
            } else {
                Carrier::DelayedSticky
            };
            self.record_send(carrier, &event_type, state_key, &content_json);
            Ok(format!("event-{room_id}-{event_type}"))
        }

        async fn restart_delayed_event(
            &self,
            _room_id: String,
            _delay_id: String,
        ) -> Result<(), FfiBackendError> {
            self.record("restart_delayed_event");
            Ok(())
        }

        async fn cancel_delayed_event(
            &self,
            _room_id: String,
            _delay_id: String,
        ) -> Result<(), FfiBackendError> {
            self.record("cancel_delayed_event");
            Ok(())
        }

        async fn send_to_device_message(
            &self,
            recipients: Vec<FfiToDeviceRecipient>,
            message_type: String,
            content_json: String,
        ) -> Result<Vec<FfiToDeviceDelivery>, FfiBackendError> {
            self.record(&message_type);
            let content: Value = serde_json::from_str(&content_json).unwrap_or(Value::Null);
            let mut to_device = self.to_device.lock().unwrap();
            Ok(recipients
                .into_iter()
                .map(|recipient| {
                    to_device.push(ToDeviceSend {
                        user_id: recipient.user_id.clone(),
                        device_id: recipient.device_id.clone(),
                        content: content.clone(),
                    });
                    FfiToDeviceDelivery {
                        user_id: recipient.user_id,
                        device_id: recipient.device_id,
                        error: None,
                    }
                })
                .collect())
        }

        async fn send_state_event(
            &self,
            _room_id: String,
            event_type: String,
            state_key: String,
            content_json: String,
        ) -> Result<String, FfiBackendError> {
            self.record(&event_type);
            self.record_send(Carrier::State, &event_type, Some(state_key), &content_json);
            Ok(format!("$state-{}", self.sends.lock().unwrap().len()))
        }

        async fn send_room_event(
            &self,
            _room_id: String,
            event_type: String,
            content_json: String,
        ) -> Result<String, FfiBackendError> {
            self.record(&event_type);
            self.record_send(Carrier::Room, &event_type, None, &content_json);
            Ok(format!("$room-{}", self.sends.lock().unwrap().len()))
        }

        async fn redact_event(
            &self,
            _room_id: String,
            _event_id: String,
            _reason: Option<String>,
        ) -> Result<(), FfiBackendError> {
            self.record("redact_event");
            Ok(())
        }

        fn subscribe_room(
            &self,
            room_id: String,
            subjects: FfiRoomSubjects,
            sink: Arc<RoomSink>,
        ) -> Result<Arc<dyn BackendSubscription>, FfiBackendError> {
            self.room_sinks
                .lock()
                .unwrap()
                .insert(room_id, (subjects, sink));
            Ok(Arc::new(MockSubscription {
                cancelled: AtomicBool::new(false),
            }))
        }

        fn subscribe_to_device(
            &self,
            event_types: Vec<String>,
            sink: Arc<ToDeviceSink>,
        ) -> Result<Arc<dyn BackendSubscription>, FfiBackendError> {
            *self.to_device_sink.lock().unwrap() = Some((event_types, sink));
            Ok(Arc::new(MockSubscription {
                cancelled: AtomicBool::new(false),
            }))
        }

        async fn relations(
            &self,
            _room_id: String,
            event_id: String,
            _rel_type: String,
            _event_type: String,
        ) -> Result<Vec<FfiEventIn>, FfiBackendError> {
            Ok(self
                .relations_answers
                .lock()
                .unwrap()
                .get(&event_id)
                .cloned()
                .unwrap_or_default())
        }

        async fn openid_token(&self) -> Result<FfiOpenIdToken, FfiBackendError> {
            Ok(FfiOpenIdToken {
                access_token: "token".to_owned(),
                token_type: "Bearer".to_owned(),
                matrix_server_name: "example.org".to_owned(),
                expires_in_secs: 3600,
            })
        }

        async fn rtc_transports(&self) -> Result<String, FfiBackendError> {
            Ok(self.transports_json.lock().unwrap().clone())
        }
    }
}
