// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The one contract a host implements for the stack: `MatrixBackend`.
//!
//! The send half carries the core's events to a Matrix client; the read half
//! lets the library subscribe to a room's sticky events, state, members and
//! encryption, and to to-device messages. Inbound DTOs (`EventIn`,
//! `ToDeviceMessageIn`) carry content as raw JSON with the client's decryption
//! information: the host parses nothing, and the library turns them into the
//! core's typed inputs.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::CommandError;
use crate::maybe_send::MaybeSend;

/// One device a to-device message is addressed to.
///
/// Always one specific device rather than a `*` wildcard: media keys go to the
/// device that published the membership.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToDeviceRecipient {
    pub user_id: String,
    pub device_id: String,
}

impl ToDeviceRecipient {
    pub fn new(user_id: impl Into<String>, device_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            device_id: device_id.into(),
        }
    }
}

/// What became of one recipient of a to-device send.
///
/// The distinction matters beyond reporting: a recipient recorded as served is
/// taken to hold the key and is never re-sent to, so a failure mistaken for a
/// success costs that member the rest of the call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToDeviceDelivery {
    pub recipient: ToDeviceRecipient,
    /// `None` when the message was accepted for this recipient; otherwise why
    /// it was not.
    pub error: Option<String>,
}

impl ToDeviceDelivery {
    /// The message was accepted for this recipient.
    pub fn sent(recipient: ToDeviceRecipient) -> Self {
        Self {
            recipient,
            error: None,
        }
    }

    /// The message could not be delivered to this recipient.
    pub fn failed(recipient: ToDeviceRecipient, error: impl Into<String>) -> Self {
        Self {
            recipient,
            error: Some(error.into()),
        }
    }

    pub fn is_sent(&self) -> bool {
        self.error.is_none()
    }
}

/// A failure reported by the host's Matrix client, with the Matrix error code
/// and HTTP status when it had them. The library classifies; the host does not.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct BackendError {
    pub errcode: Option<String>,
    pub status: Option<u16>,
    pub message: String,
}

impl BackendError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            errcode: None,
            status: None,
            message: message.into(),
        }
    }

    pub fn with_matrix_error(
        errcode: Option<String>,
        status: Option<u16>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            errcode,
            status,
            message: message.into(),
        }
    }

    /// The default answer of a read-half method a backend did not override.
    pub fn not_implemented(method: &str) -> Self {
        Self::new(format!("the backend does not implement {method}"))
    }
}

/// A Matrix OpenID token, as returned by
/// `POST /_matrix/client/v3/user/{userId}/openid/request_token`.
///
/// `Serialize` because a transport's authorisation service receives the whole
/// object verbatim; the library never inspects the fields.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenIdToken {
    pub access_token: String,
    pub token_type: String,
    pub matrix_server_name: String,
    pub expires_in: u64,
}

/// What the client reports about how an event or to-device message arrived.
///
/// Only what a client can honestly say: whether it decrypted the event, which
/// device it attributed it to, and whether it knows that device to be
/// cross-signed. The library derives [`EventOrigin`](crate::EventOrigin) and
/// [`KeyOrigin`](crate::KeyOrigin) from it; a host never builds those.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum EventEncryption {
    /// The event arrived in the clear.
    Cleartext,
    /// The event arrived encrypted and was decrypted by the client.
    Encrypted {
        /// The sending device, as decryption attributed it.
        sender_device_id: Option<String>,
        /// Whether that device is cross-signed (MSC4153); `None` when the
        /// client did not say.
        sender_cross_signed: Option<bool>,
    },
}

impl EventEncryption {
    pub fn encrypted(sender_device_id: Option<String>, sender_cross_signed: Option<bool>) -> Self {
        Self::Encrypted {
            sender_device_id,
            sender_cross_signed,
        }
    }

    pub fn sender_device_id(&self) -> Option<&str> {
        match self {
            Self::Encrypted {
                sender_device_id, ..
            } => sender_device_id.as_deref(),
            Self::Cleartext => None,
        }
    }

    pub fn was_encrypted(&self) -> bool {
        matches!(self, Self::Encrypted { .. })
    }
}

/// A room event as the client handed it over: content as raw JSON.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventIn {
    pub event_id: String,
    pub sender: String,
    pub event_type: String,
    /// Set for state events.
    #[serde(default)]
    pub state_key: Option<String>,
    #[serde(default)]
    pub origin_server_ts: u64,
    /// The whole decrypted `content` object.
    pub content: Value,
    pub encryption: EventEncryption,
}

/// A to-device message as the client handed it over: content as raw JSON.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToDeviceMessageIn {
    pub sender: String,
    pub event_type: String,
    pub content: Value,
    pub encryption: EventEncryption,
}

/// What the library wants delivered for one room, beyond the three subjects
/// every subscription carries: sticky events, joined members and whether the
/// room is encrypted.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomSubjects {
    /// State event types, stable and unstable spellings both listed; each is
    /// delivered under its own type, and the library picks between them.
    pub state_event_types: Vec<String>,
    /// Message-like event types to forward as they arrive, redactions included.
    pub timeline_event_types: Vec<String>,
}

/// Where a host delivers a room's subjects. Every method returns at once; the
/// library does its work elsewhere.
///
/// For sticky events, state events and joined members each call carries the
/// room's **complete current set** for that subject, and the first call is the
/// current set at subscription time. An empty set means none.
pub trait RoomSink: MaybeSend {
    fn on_sticky_events(&self, events: Vec<EventIn>);
    fn on_state_events(&self, event_type: String, events: Vec<EventIn>);
    fn on_joined_members(&self, user_ids: Vec<String>);
    fn on_encryption(&self, encrypted: bool);
    fn on_timeline_events(&self, events: Vec<EventIn>);
    fn on_redaction(&self, event_id: String);
}

/// Where a host delivers to-device messages of the subscribed types.
pub trait ToDeviceSink: MaybeSend {
    fn on_to_device_message(&self, message: ToDeviceMessageIn);
}

/// A subscription the library ends by calling `cancel`. Cancelling twice is a
/// no-op.
pub trait Subscription: MaybeSend {
    fn cancel(&self);
}

/// Everything the library needs from a Matrix client, implemented by the host.
///
/// The send half carries the core's events out; the read half subscribes to
/// what a room says and to to-device messages. Read-half methods have defaults
/// that fail, for backends that only send (test doubles); a backend attached
/// to a room must override every one of them.
///
/// The futures are `Send` everywhere except `wasm32`, where they cannot be: a
/// JS-backed future (`JsFuture` around a `Promise`) is not `Send`, and the
/// browser is single-threaded anyway. Every implementation of this trait needs
/// the same pair of `cfg_attr`s, or it will not satisfy the trait on one of the
/// two targets. Native being `Send` is what lets a uniffi async export return
/// one of these futures. The implementing type is held to the same split by
/// [`MaybeSend`].
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait MatrixBackend: MaybeSend {
    /// The account the client is logged in as.
    fn own_user_id(&self) -> String;

    /// The device the client is logged in as.
    fn own_device_id(&self) -> String;

    // ---- Send half ----

    /// Send a sticky event to a Matrix room.
    ///
    /// # Arguments
    ///
    /// * `room_id` - The room ID where the event should be sent
    /// * `event_type` - The event type (e.g., "m.rtc.member")
    /// * `content` - The event content as a JSON value
    /// * `duration_ms` - How long the server should keep this entry in the
    ///   sticky map. Implementations MUST pass this through rather than
    ///   choosing their own: the caller re-sends the event before this elapses,
    ///   so a different lifetime here silently breaks that refresh.
    ///
    /// # Returns
    ///
    /// The event id the homeserver assigned. Every Matrix send responds with
    /// one, so an implementation that cannot produce it is broken rather than
    /// merely terse — hence no `Option`.
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        duration_ms: u64,
    ) -> Result<String, CommandError>;

    /// Send a delayed event to a Matrix room (MSC4140).
    ///
    /// **One attempt, bounded by a short timeout (a few seconds), for every
    /// delayed-event method here.** The core owns the retries: it backs off
    /// with jitter, knows the delay's deadline, and knows when a retry would be
    /// unsafe. A host that retries underneath it — matrix-sdk does by default —
    /// stretches one attempt past the whole delay, retries in lockstep with
    /// every other client after an outage, and can schedule a second delayed
    /// state event (that request has no transaction id) that nobody restarts.
    ///
    /// With `state_key`, the delayed event is a state event; otherwise a
    /// message-like one. Returns the MSC4140 **delay id** on success — the
    /// handle used to restart or cancel the scheduled send. It is not an event
    /// id: the event has none until it actually fires.
    ///
    /// # Arguments
    ///
    /// * `room_id` - The room ID where the event should be sent
    /// * `event_type` - The event type
    /// * `state_key` - The state key, for a delayed state event
    /// * `content` - The event content as a JSON value
    /// * `delay_ms` - Delay in milliseconds before the event is sent
    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: Option<String>,
        content: Value,
        delay_ms: u64,
    ) -> Result<String, CommandError>;

    /// Restart a previously scheduled delayed event's timer (MSC4140's
    /// `restart` action — "heartbeat ping").
    ///
    /// Resets the scheduled send time to now plus the *original* delay, leaving
    /// the delay id and everything else about the event untouched. This is the
    /// keep-alive primitive: one request, and no moment at which no delayed
    /// leave is armed.
    ///
    /// Do NOT emulate this with cancel-then-reschedule. That leaves a window
    /// with nothing armed, burns the server's `max_scheduled` quota, and a
    /// failed cancel leaks a delay that will fire and mark us as departed while
    /// we are still in the call.
    ///
    /// # Arguments
    ///
    /// * `room_id` - The room ID where the delayed event was scheduled
    /// * `delay_id` - The MSC4140 delay id returned by `send_delayed_event`.
    async fn restart_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError>;

    /// Cancel a previously scheduled delayed event.
    ///
    /// # Arguments
    ///
    /// * `room_id` - The room ID where the delayed event was scheduled
    /// * `delay_id` - The MSC4140 delay id returned by `send_delayed_event`.
    async fn cancel_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError>;

    /// Send a scheduled delayed event now (MSC4140's `send` action).
    ///
    /// Retires the delayed leave of a lost earlier join before joining again:
    /// sending it ends that membership cleanly, where it might otherwise fire
    /// after the new join and end that. `M_NOT_FOUND` means it has fired
    /// already. A host without it answers [`CommandError::NotImplemented`] and
    /// the core cancels the delay instead.
    async fn send_delayed_event_now(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        let _ = (room_id, delay_id);
        Err(CommandError::NotImplemented(
            "send_delayed_event_now".to_owned(),
        ))
    }

    /// Send one to-device message to a set of devices, reporting the outcome
    /// per recipient.
    ///
    /// Used for encryption key distribution (MSC4143). The same `content` goes
    /// to every recipient; each is one specific device, never a `*` fan-out —
    /// media keys go to the device that published the membership, and widening
    /// would hand the key to devices outside the call (and, for our own user, to
    /// this very device, which Olm cannot encrypt to).
    ///
    /// # Why per recipient
    ///
    /// One unreachable device must not silence the others, and the caller has to
    /// know *which* ones were served: a recipient reported as delivered is
    /// recorded as holding the key and never retried, so reporting a failure as
    /// success costs that member the call.
    ///
    /// An `Err` return means the batch could not be attempted at all; the caller
    /// treats every recipient as unserved.
    ///
    /// MSC4143 specifies that encryption keys MUST be sent via encrypted
    /// to-device messages. Keys sent in cleartext SHOULD be discarded by
    /// recipients.
    async fn send_to_device_message(
        &self,
        recipients: Vec<ToDeviceRecipient>,
        message_type: String,
        content: Value,
    ) -> Result<Vec<ToDeviceDelivery>, CommandError>;

    /// Send a state event to a Matrix room.
    ///
    /// Used for `m.rtc.slot`, the only MatrixRTC event that lives in room state.
    /// Sending it usually requires a power level the average member does not
    /// have, so implementations should surface an authorization failure as an
    /// error rather than swallowing it.
    ///
    /// # Returns
    ///
    /// The event id the homeserver assigned, on the same terms as
    /// [`send_sticky_event`](Self::send_sticky_event). No caller of a *slot*
    /// send reads it; it is here because the pre-MSC4354 Element Call dialect
    /// routes the membership through this method, and there the id is what a
    /// notification relates to.
    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content: Value,
    ) -> Result<String, CommandError>;

    /// Send a plain room event: message-like, neither sticky nor state.
    ///
    /// Only applications send these. In an encrypted room the event must go out
    /// encrypted like any other message; a client SDK's ordinary send does that
    /// on its own. `event_type` is sent verbatim.
    ///
    /// # Returns
    ///
    /// The event id the homeserver assigned, on the same terms as
    /// [`send_sticky_event`](Self::send_sticky_event).
    async fn send_room_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
    ) -> Result<String, CommandError>;

    /// Redact one of our own room events. Only applications redact.
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

    // ---- Read half ----

    /// Deliver `subjects` for `room_id` to `sink`, the current sets first, then
    /// every change, until the returned subscription is cancelled.
    async fn subscribe_room(
        &self,
        room_id: String,
        subjects: RoomSubjects,
        sink: Arc<dyn RoomSink>,
    ) -> Result<Arc<dyn Subscription>, BackendError> {
        let _ = (room_id, subjects, sink);
        Err(BackendError::not_implemented("subscribe_room"))
    }

    /// Deliver every decrypted to-device message of `event_types` to `sink`
    /// until the returned subscription is cancelled.
    async fn subscribe_to_device(
        &self,
        event_types: Vec<String>,
        sink: Arc<dyn ToDeviceSink>,
    ) -> Result<Arc<dyn Subscription>, BackendError> {
        let _ = (event_types, sink);
        Err(BackendError::not_implemented("subscribe_to_device"))
    }

    /// `GET /rooms/{room_id}/relations/{event_id}/{rel_type}/{event_type}`,
    /// decrypted.
    async fn relations(
        &self,
        room_id: String,
        event_id: String,
        rel_type: String,
        event_type: String,
    ) -> Result<Vec<EventIn>, BackendError> {
        let _ = (room_id, event_id, rel_type, event_type);
        Err(BackendError::not_implemented("relations"))
    }

    /// A fresh OpenID token for the account.
    async fn openid_token(&self) -> Result<OpenIdToken, BackendError> {
        Err(BackendError::not_implemented("openid_token"))
    }

    /// The homeserver's `rtc_transports` array, raw, from
    /// `GET /_matrix/client/v1/rtc/transports`; an empty array when the
    /// endpoint is missing.
    async fn rtc_transports(&self) -> Result<Value, BackendError> {
        Err(BackendError::not_implemented("rtc_transports"))
    }
}

/// A backend that accepts every send and reads nothing, for unit tests that
/// don't look at what was sent.
#[cfg(test)]
pub struct NoopBackend;

#[cfg(test)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl MatrixBackend for NoopBackend {
    fn own_user_id(&self) -> String {
        "@noop:example.org".to_owned()
    }

    fn own_device_id(&self) -> String {
        "NOOPDEVICE".to_owned()
    }

    async fn send_sticky_event(
        &self,
        _room_id: String,
        _event_type: String,
        _content: Value,
        _duration_ms: u64,
    ) -> Result<String, CommandError> {
        Ok("$mock-sticky-event".to_string())
    }

    async fn send_delayed_event(
        &self,
        _room_id: String,
        _event_type: String,
        _state_key: Option<String>,
        _content: Value,
        _delay_ms: u64,
    ) -> Result<String, CommandError> {
        Ok("mock-event-id".to_string())
    }

    async fn restart_delayed_event(
        &self,
        _room_id: String,
        _delay_id: String,
    ) -> Result<(), CommandError> {
        Ok(())
    }

    async fn cancel_delayed_event(
        &self,
        _room_id: String,
        _delay_id: String,
    ) -> Result<(), CommandError> {
        Ok(())
    }

    async fn send_to_device_message(
        &self,
        recipients: Vec<ToDeviceRecipient>,
        _message_type: String,
        _content: Value,
    ) -> Result<Vec<ToDeviceDelivery>, CommandError> {
        Ok(recipients.into_iter().map(ToDeviceDelivery::sent).collect())
    }

    async fn send_state_event(
        &self,
        _room_id: String,
        _event_type: String,
        _state_key: String,
        _content: Value,
    ) -> Result<String, CommandError> {
        Ok("$mock-state-event".to_string())
    }

    async fn send_room_event(
        &self,
        _room_id: String,
        _event_type: String,
        _content: Value,
    ) -> Result<String, CommandError> {
        Ok("$mock-room-event".to_string())
    }

    async fn redact_event(
        &self,
        _room_id: String,
        _event_id: String,
        _reason: Option<String>,
    ) -> Result<(), CommandError> {
        Ok(())
    }
}

/// One room subscription a [`MockBackend`] handed out.
#[cfg(any(test, feature = "testing"))]
pub struct MockRoomSubscription {
    pub subjects: RoomSubjects,
    pub sink: Arc<dyn RoomSink>,
    pub cancelled: std::sync::atomic::AtomicBool,
}

#[cfg(any(test, feature = "testing"))]
impl Subscription for MockRoomSubscription {
    fn cancel(&self) {
        self.cancelled
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

/// The to-device subscription a [`MockBackend`] handed out.
#[cfg(any(test, feature = "testing"))]
pub struct MockToDeviceSubscription {
    pub event_types: Vec<String>,
    pub sink: Arc<dyn ToDeviceSink>,
    pub cancelled: std::sync::atomic::AtomicBool,
}

#[cfg(any(test, feature = "testing"))]
impl Subscription for MockToDeviceSubscription {
    fn cancel(&self) {
        self.cancelled
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

/// `(room_id, event_type, state_key, content, delay_ms)`.
#[cfg(any(test, feature = "testing"))]
pub type RecordedDelayedEvent = (String, String, Option<String>, Value, u64);

/// How a [`MockBackend`] fails an MSC4140 restart or cancel.
#[cfg(any(test, feature = "testing"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MockDelayFailure {
    /// The homeserver cannot be reached: a plain send error.
    Unreachable,
    /// The homeserver no longer has the delay (`M_NOT_FOUND`).
    Gone,
}

#[cfg(any(test, feature = "testing"))]
impl MockDelayFailure {
    fn error(self) -> CommandError {
        match self {
            Self::Unreachable => CommandError::from_message("homeserver unreachable"),
            Self::Gone => CommandError::DelayedEventNotFound("M_NOT_FOUND".to_owned()),
        }
    }
}

/// A backend that records every send and keeps the sinks it was given, so a
/// test can check what went out and play sets in. Exported under the `testing`
/// feature.
#[cfg(any(test, feature = "testing"))]
pub struct MockBackend {
    pub user_id: String,
    pub device_id: String,
    pub sticky_events: std::sync::Mutex<Vec<(String, String, Value, u64)>>,
    pub delayed_events: std::sync::Mutex<Vec<RecordedDelayedEvent>>,
    pub restarted_events: std::sync::Mutex<Vec<(String, String)>>,
    pub cancelled_events: std::sync::Mutex<Vec<(String, String)>>,
    pub to_device_messages: std::sync::Mutex<Vec<(String, String, String, Value)>>,
    pub state_events: std::sync::Mutex<Vec<(String, String, String, Value)>>,
    /// `(room_id, event_type, content)` of every plain room event sent.
    pub room_events: std::sync::Mutex<Vec<(String, String, Value)>>,
    /// `(room_id, event_id, reason)` of every redaction requested.
    pub redactions: std::sync::Mutex<Vec<(String, String, Option<String>)>>,
    /// Room subscriptions, by room id, in subscription order.
    pub room_subscriptions: std::sync::Mutex<Vec<(String, Arc<MockRoomSubscription>)>>,
    pub to_device_subscriptions: std::sync::Mutex<Vec<Arc<MockToDeviceSubscription>>>,
    /// `(room_id, event_id, rel_type, event_type)` of every relations fetch.
    pub relations_requests: std::sync::Mutex<Vec<(String, String, String, String)>>,
    /// What `relations` answers, by target event id; unknown ids answer empty.
    pub relations_answers: std::sync::Mutex<std::collections::HashMap<String, Vec<EventIn>>>,
    /// What `rtc_transports` answers.
    pub transports: std::sync::Mutex<Result<Value, BackendError>>,
    /// How many times `rtc_transports` was asked.
    pub transports_requests: std::sync::atomic::AtomicUsize,
    /// When set, `subscribe_room` fails with it.
    pub room_subscription_error: std::sync::Mutex<Option<BackendError>>,
    /// When set, sticky sends fail with this message and are not recorded.
    pub sticky_event_error: std::sync::Mutex<Option<String>>,
    /// When set, delayed-event restarts fail this way (and are still recorded).
    pub restart_failure: std::sync::Mutex<Option<MockDelayFailure>>,
    /// When set, delayed-event cancels fail this way (and are still recorded).
    pub cancel_failure: std::sync::Mutex<Option<MockDelayFailure>>,
    /// When set, delayed-event restarts never return, as against a homeserver
    /// behind a partition that drops packets.
    pub restart_hangs: std::sync::atomic::AtomicBool,
    /// Delays sent now (`send_delayed_event_now`), and how such a send fails.
    pub sent_now_events: std::sync::Mutex<Vec<(String, String)>>,
    pub send_now_failure: std::sync::Mutex<Option<MockDelayFailure>>,
}

#[cfg(any(test, feature = "testing"))]
impl Default for MockBackend {
    fn default() -> Self {
        Self {
            user_id: "@mock:example.org".to_owned(),
            device_id: "MOCKDEVICE".to_owned(),
            sticky_events: Default::default(),
            delayed_events: Default::default(),
            restarted_events: Default::default(),
            cancelled_events: Default::default(),
            to_device_messages: Default::default(),
            state_events: Default::default(),
            room_events: Default::default(),
            redactions: Default::default(),
            room_subscriptions: Default::default(),
            to_device_subscriptions: Default::default(),
            relations_requests: Default::default(),
            relations_answers: Default::default(),
            transports: std::sync::Mutex::new(Ok(Value::Array(Vec::new()))),
            transports_requests: Default::default(),
            room_subscription_error: Default::default(),
            sticky_event_error: Default::default(),
            restart_failure: Default::default(),
            cancel_failure: Default::default(),
            restart_hangs: Default::default(),
            sent_now_events: Default::default(),
            send_now_failure: Default::default(),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl MockBackend {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_identity(user_id: impl Into<String>, device_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            device_id: device_id.into(),
            ..Self::default()
        }
    }

    #[allow(dead_code)]
    pub fn last_sticky_event(&self) -> Option<(String, String, Value, u64)> {
        self.sticky_events.lock().unwrap().last().cloned()
    }

    #[allow(dead_code)]
    pub fn last_delayed_event(&self) -> Option<RecordedDelayedEvent> {
        self.delayed_events.lock().unwrap().last().cloned()
    }

    #[allow(dead_code)]
    pub fn last_to_device_message(&self) -> Option<(String, String, String, Value)> {
        self.to_device_messages.lock().unwrap().last().cloned()
    }

    #[allow(dead_code)]
    pub fn to_device_messages_for(&self, user_id: &str, device_id: &str) -> Vec<(String, Value)> {
        self.to_device_messages
            .lock()
            .unwrap()
            .iter()
            .filter(|(u, d, _, _)| u == user_id && d == device_id)
            .map(|(_, _, t, c)| (t.clone(), c.clone()))
            .collect()
    }

    /// The live room subscription for `room_id`, if any.
    pub fn room_subscription(&self, room_id: &str) -> Option<Arc<MockRoomSubscription>> {
        self.room_subscriptions
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|(room, sub)| {
                room == room_id && !sub.cancelled.load(std::sync::atomic::Ordering::SeqCst)
            })
            .map(|(_, sub)| sub.clone())
    }

    /// The live to-device subscription, if any.
    pub fn to_device_subscription(&self) -> Option<Arc<MockToDeviceSubscription>> {
        self.to_device_subscriptions
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|sub| !sub.cancelled.load(std::sync::atomic::Ordering::SeqCst))
            .cloned()
    }
}

#[cfg(any(test, feature = "testing"))]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl MatrixBackend for MockBackend {
    fn own_user_id(&self) -> String {
        self.user_id.clone()
    }

    fn own_device_id(&self) -> String {
        self.device_id.clone()
    }

    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        duration_ms: u64,
    ) -> Result<String, CommandError> {
        if let Some(message) = self.sticky_event_error.lock().unwrap().clone() {
            return Err(CommandError::from_message(message));
        }
        let mut guard = self.sticky_events.lock().unwrap();
        guard.push((room_id, event_type, content, duration_ms));
        // Numbered by send order, so a test can name the event a relation is
        // expected to point at.
        Ok(format!("$sticky-{}", guard.len()))
    }

    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: Option<String>,
        content: Value,
        delay_ms: u64,
    ) -> Result<String, CommandError> {
        self.delayed_events.lock().unwrap().push((
            room_id.clone(),
            event_type.clone(),
            state_key,
            content,
            delay_ms,
        ));
        Ok(format!("delayed-{}-{}", room_id, event_type))
    }

    async fn restart_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        self.restarted_events
            .lock()
            .unwrap()
            .push((room_id, delay_id));
        if self
            .restart_hangs
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            std::future::pending::<()>().await;
        }
        match *self.restart_failure.lock().unwrap() {
            Some(failure) => Err(failure.error()),
            None => Ok(()),
        }
    }

    async fn cancel_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        self.cancelled_events
            .lock()
            .unwrap()
            .push((room_id, delay_id));
        match *self.cancel_failure.lock().unwrap() {
            Some(failure) => Err(failure.error()),
            None => Ok(()),
        }
    }

    async fn send_delayed_event_now(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        self.sent_now_events
            .lock()
            .unwrap()
            .push((room_id, delay_id));
        match *self.send_now_failure.lock().unwrap() {
            Some(failure) => Err(failure.error()),
            None => Ok(()),
        }
    }

    async fn send_to_device_message(
        &self,
        recipients: Vec<ToDeviceRecipient>,
        message_type: String,
        content: Value,
    ) -> Result<Vec<ToDeviceDelivery>, CommandError> {
        let mut guard = self.to_device_messages.lock().unwrap();
        for recipient in &recipients {
            guard.push((
                recipient.user_id.clone(),
                recipient.device_id.clone(),
                message_type.clone(),
                content.clone(),
            ));
        }
        Ok(recipients.into_iter().map(ToDeviceDelivery::sent).collect())
    }

    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content: Value,
    ) -> Result<String, CommandError> {
        let mut guard = self.state_events.lock().unwrap();
        guard.push((room_id, event_type, state_key, content));
        Ok(format!("$state-{}", guard.len()))
    }

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

    async fn subscribe_room(
        &self,
        room_id: String,
        subjects: RoomSubjects,
        sink: Arc<dyn RoomSink>,
    ) -> Result<Arc<dyn Subscription>, BackendError> {
        if let Some(error) = self.room_subscription_error.lock().unwrap().clone() {
            return Err(error);
        }
        let subscription = Arc::new(MockRoomSubscription {
            subjects,
            sink,
            cancelled: Default::default(),
        });
        self.room_subscriptions
            .lock()
            .unwrap()
            .push((room_id, subscription.clone()));
        Ok(subscription)
    }

    async fn subscribe_to_device(
        &self,
        event_types: Vec<String>,
        sink: Arc<dyn ToDeviceSink>,
    ) -> Result<Arc<dyn Subscription>, BackendError> {
        let subscription = Arc::new(MockToDeviceSubscription {
            event_types,
            sink,
            cancelled: Default::default(),
        });
        self.to_device_subscriptions
            .lock()
            .unwrap()
            .push(subscription.clone());
        Ok(subscription)
    }

    async fn relations(
        &self,
        room_id: String,
        event_id: String,
        rel_type: String,
        event_type: String,
    ) -> Result<Vec<EventIn>, BackendError> {
        self.relations_requests.lock().unwrap().push((
            room_id,
            event_id.clone(),
            rel_type,
            event_type,
        ));
        Ok(self
            .relations_answers
            .lock()
            .unwrap()
            .get(&event_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn openid_token(&self) -> Result<OpenIdToken, BackendError> {
        Ok(OpenIdToken {
            access_token: "mock-openid-token".to_owned(),
            token_type: "Bearer".to_owned(),
            matrix_server_name: "example.org".to_owned(),
            expires_in: 3600,
        })
    }

    async fn rtc_transports(&self) -> Result<Value, BackendError> {
        self.transports_requests
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.transports.lock().unwrap().clone()
    }
}
