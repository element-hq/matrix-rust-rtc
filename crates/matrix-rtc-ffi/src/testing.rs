// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Scripted fakes for hosts' own tests (034, contract C16), in the debug
//! artifacts only (the `testing` feature): a [`ScriptedMatrixBackend`] a host
//! hands to `RtcClient` in place of its Matrix client, and a
//! [`ScriptedMediaTransport`] a media session connects through in place of
//! LiveKit. Either can take its side of the call away and give it back, by
//! hand or by fixture key, so the same outage produces the same call on every
//! host. They wrap the core's `MockBackend` and the transport crate's
//! `ScriptedTransport`; the records here are DTOs over those.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use async_trait::async_trait;
use matrix_rtc_core::testing::{
    MockBackend, MockMethod, MockOutcome, MockRoomSubscription, MockToDeviceSubscription,
};
use matrix_rtc_core::{
    BackendError, CommandError, EventEncryption, MatrixBackend as CoreBackend, RoomSubjects,
    ToDeviceMessageIn, ToDeviceRecipient,
};
use matrix_rtc_transport::testing::ScriptedTransport;

use crate::backend::{
    BackendSubscription, FfiBackendError, FfiEventEncryption, FfiEventIn, FfiOpenIdToken,
    FfiRoomSubjects, FfiToDeviceDelivery, FfiToDeviceMessageIn, FfiToDeviceRecipient,
    MatrixBackend, RoomSink, ToDeviceSink, events_into_core, parse_content,
};
use crate::media::{MediaFfiError, MediaSession, MediaSessionConfig};
use crate::runtime::runtime;

/// A fixture key neither fake knows.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum FfiScriptError {
    #[error("unknown fixture {key}: {known}")]
    UnknownFixture { key: String, known: String },
}

/// A backend method whose outcomes can be scripted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiScriptedMethod {
    StickyEvent,
    DelayedEvent,
    RestartDelayedEvent,
    CancelDelayedEvent,
    ToDevice,
    StateEvent,
    RoomEvent,
    Redaction,
    RtcTransports,
}

impl From<FfiScriptedMethod> for MockMethod {
    fn from(value: FfiScriptedMethod) -> Self {
        match value {
            FfiScriptedMethod::StickyEvent => Self::StickyEvent,
            FfiScriptedMethod::DelayedEvent => Self::DelayedEvent,
            FfiScriptedMethod::RestartDelayedEvent => Self::RestartDelayedEvent,
            FfiScriptedMethod::CancelDelayedEvent => Self::CancelDelayedEvent,
            FfiScriptedMethod::ToDevice => Self::ToDevice,
            FfiScriptedMethod::StateEvent => Self::StateEvent,
            FfiScriptedMethod::RoomEvent => Self::RoomEvent,
            FfiScriptedMethod::Redaction => Self::Redaction,
            FfiScriptedMethod::RtcTransports => Self::RtcTransports,
        }
    }
}

impl From<MockMethod> for FfiScriptedMethod {
    fn from(value: MockMethod) -> Self {
        match value {
            MockMethod::StickyEvent => Self::StickyEvent,
            MockMethod::DelayedEvent => Self::DelayedEvent,
            MockMethod::RestartDelayedEvent => Self::RestartDelayedEvent,
            MockMethod::CancelDelayedEvent => Self::CancelDelayedEvent,
            MockMethod::ToDevice => Self::ToDevice,
            MockMethod::StateEvent => Self::StateEvent,
            MockMethod::RoomEvent => Self::RoomEvent,
            MockMethod::Redaction => Self::Redaction,
            MockMethod::RtcTransports => Self::RtcTransports,
        }
    }
}

/// What one scripted send does.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum FfiScriptedOutcome {
    /// Succeeds, as the unscripted backend does.
    Ok,
    /// Fails as the host's client would report it: a `permanent` failure or
    /// a refusal by `errcode` is never retried, anything else is.
    Fail {
        errcode: Option<String>,
        status: Option<u16>,
        retry_after_ms: Option<u64>,
        permanent: bool,
    },
    /// Never answers, as a request the network swallowed.
    Hang,
    /// Succeeds after `delay_ms`.
    Slow { delay_ms: u64 },
}

impl From<FfiScriptedOutcome> for MockOutcome {
    fn from(value: FfiScriptedOutcome) -> Self {
        match value {
            FfiScriptedOutcome::Ok => Self::Ok,
            FfiScriptedOutcome::Fail {
                errcode,
                status,
                retry_after_ms,
                permanent,
            } => {
                let error = BackendError::with_matrix_error(errcode, status, "scripted failure")
                    .with_retry_after_ms(retry_after_ms);
                Self::Fail(if permanent { error.permanent() } else { error })
            }
            FfiScriptedOutcome::Hang => Self::Hang,
            FfiScriptedOutcome::Slow { delay_ms } => {
                Self::After(Duration::from_millis(delay_ms), Box::new(Self::Ok))
            }
        }
    }
}

/// One call the scripted backend received, failed or not.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiScriptedCall {
    pub method: FfiScriptedMethod,
    /// Since the backend was created.
    pub at_ms: u64,
}

/// The backend-side fixture keys (harness/fixtures.md, proposed by 034).
const BACKEND_FIXTURES: &str = "outage.homeserver.30s, outage.homeserver.3m, \
     outage.homeserver.lapse, outage.rate_limit.retry_after_7s";

/// The media-side fixture keys.
const MEDIA_FIXTURES: &str = "outage.own_sfu.recovers_40s, outage.own_sfu.budget, \
     outage.own_sfu.fallback, outage.peer_sfu.1m";

/// A Matrix backend that answers every send itself, records it, and plays
/// what the test delivers into the library's subscriptions. Hand
/// [`Self::backend`] to `RtcClient::new` as you would your own.
#[derive(uniffi::Object)]
pub struct ScriptedMatrixBackend {
    mock: Arc<MockBackend>,
}

#[uniffi::export]
impl ScriptedMatrixBackend {
    #[uniffi::constructor]
    pub fn new(user_id: String, device_id: String) -> Arc<Self> {
        Arc::new(Self {
            mock: Arc::new(MockBackend::with_identity(user_id, device_id)),
        })
    }

    /// This backend as the library takes it.
    pub fn backend(self: Arc<Self>) -> Arc<dyn MatrixBackend> {
        self
    }

    /// What `rtc_transports` answers: the JSON `rtc_transports` array.
    pub fn set_rtc_transports(&self, transports_json: String) {
        let parsed = serde_json::from_str(&transports_json)
            .unwrap_or_else(|_| serde_json::Value::Array(Vec::new()));
        *self.mock.transports.lock().unwrap() = Ok(parsed);
    }

    /// Queues `outcomes` for the next calls to `method`.
    pub fn script(&self, method: FfiScriptedMethod, outcomes: Vec<FfiScriptedOutcome>) {
        self.mock
            .script(method.into(), outcomes.into_iter().map(Into::into));
    }

    /// Every send answers `outcome` until `None`: the homeserver away.
    pub fn set_outage(&self, outcome: Option<FfiScriptedOutcome>) {
        self.mock.set_outage(outcome.map(Into::into));
    }

    /// To-device sends report this device as not delivered while set.
    pub fn set_undeliverable(&self, user_id: String, device_id: String, undeliverable: bool) {
        let mut set = self.mock.undeliverable.lock().unwrap();
        if undeliverable {
            set.insert((user_id, device_id));
        } else {
            set.remove(&(user_id, device_id));
        }
    }

    /// Delivers the room's complete sticky set, as the host's client would.
    pub fn deliver_sticky_events(&self, room_id: String, events: Vec<FfiEventIn>) {
        if let Some(subscription) = self.mock.room_subscription(&room_id) {
            subscription.sink.on_sticky_events(events_into_core(events));
        }
    }

    /// Delivers the room's complete set of `event_type` state.
    pub fn deliver_state_events(
        &self,
        room_id: String,
        event_type: String,
        events: Vec<FfiEventIn>,
    ) {
        if let Some(subscription) = self.mock.room_subscription(&room_id) {
            subscription
                .sink
                .on_state_events(event_type, events_into_core(events));
        }
    }

    pub fn deliver_joined_members(&self, room_id: String, user_ids: Vec<String>) {
        if let Some(subscription) = self.mock.room_subscription(&room_id) {
            subscription.sink.on_joined_members(user_ids);
        }
    }

    pub fn deliver_encryption(&self, room_id: String, encrypted: bool) {
        if let Some(subscription) = self.mock.room_subscription(&room_id) {
            subscription.sink.on_encryption(encrypted);
        }
    }

    pub fn deliver_to_device(&self, message: FfiToDeviceMessageIn) {
        let Some(content) =
            parse_content(&message.event_type, &message.sender, &message.content_json)
        else {
            return;
        };
        if let Some(subscription) = self.mock.to_device_subscription() {
            subscription.sink.on_to_device_message(ToDeviceMessageIn {
                sender: message.sender,
                event_type: message.event_type,
                content,
                encryption: message.encryption.into(),
            });
        }
    }

    /// The content of every sticky event sent to `room_id`, as JSON.
    pub fn sticky_events_sent(&self, room_id: String) -> Vec<String> {
        self.mock
            .sticky_events
            .lock()
            .unwrap()
            .iter()
            .filter(|(room, ..)| *room == room_id)
            .map(|(_, _, content, _)| content.to_string())
            .collect()
    }

    /// The content of every to-device message sent to one device, as JSON.
    pub fn to_device_sent(&self, user_id: String, device_id: String) -> Vec<String> {
        self.mock
            .to_device_messages_for(&user_id, &device_id)
            .into_iter()
            .map(|(_, content)| content.to_string())
            .collect()
    }

    /// Every call received so far, in order.
    pub fn calls(&self) -> Vec<FfiScriptedCall> {
        self.mock
            .calls()
            .into_iter()
            .map(|call| FfiScriptedCall {
                method: call.method.into(),
                at_ms: call.at.as_millis() as u64,
            })
            .collect()
    }

    /// Plays a backend-side fixture, from now: the homeserver away for 30 s,
    /// 3 min, or past our membership's lifetime (6 min), or one
    /// `M_LIMIT_EXCEEDED` with a 7 s `retry_after_ms` on the next send of
    /// each kind.
    pub fn play(&self, fixture: String) -> Result<(), FfiScriptError> {
        let away = |secs: u64| {
            let mock = self.mock.clone();
            mock.set_outage(Some(MockOutcome::Fail(BackendError::new(
                "scripted outage: the homeserver is away",
            ))));
            runtime().spawn(async move {
                tokio::time::sleep(Duration::from_secs(secs)).await;
                mock.set_outage(None);
            });
        };
        match fixture.as_str() {
            "outage.homeserver.30s" => away(30),
            "outage.homeserver.3m" => away(180),
            "outage.homeserver.lapse" => away(360),
            "outage.rate_limit.retry_after_7s" => {
                for method in [
                    MockMethod::StickyEvent,
                    MockMethod::DelayedEvent,
                    MockMethod::RestartDelayedEvent,
                    MockMethod::ToDevice,
                    MockMethod::RoomEvent,
                ] {
                    self.mock.script(
                        method,
                        [MockOutcome::Fail(
                            BackendError::with_matrix_error(
                                Some("M_LIMIT_EXCEEDED".to_owned()),
                                Some(429),
                                "scripted rate limit",
                            )
                            .with_retry_after_ms(Some(7_000)),
                        )],
                    );
                }
            }
            _ => {
                return Err(FfiScriptError::UnknownFixture {
                    key: fixture,
                    known: format!(
                        "this backend plays {BACKEND_FIXTURES}; the media transport plays \
                         {MEDIA_FIXTURES}"
                    ),
                });
            }
        }
        Ok(())
    }
}

/// The core's failure as the host's client would have reported it.
fn ffi_error(error: CommandError) -> FfiBackendError {
    match error.backend_error() {
        Some(backend) => backend_failure(backend.clone()),
        None => FfiBackendError::Failed {
            errcode: None,
            status: None,
            retry_after_ms: None,
            permanent: false,
            reason: error.to_string(),
        },
    }
}

fn backend_failure(error: BackendError) -> FfiBackendError {
    FfiBackendError::Failed {
        errcode: error.errcode,
        status: error.status,
        retry_after_ms: error.retry_after_ms,
        permanent: error.permanent,
        reason: error.message,
    }
}

fn json(content_json: &str) -> Result<serde_json::Value, FfiBackendError> {
    serde_json::from_str(content_json).map_err(|error| FfiBackendError::Failed {
        errcode: None,
        status: None,
        retry_after_ms: None,
        permanent: true,
        reason: format!("content is not JSON: {error}"),
    })
}

/// Cancels a subscription registered with the mock.
struct Cancel(Arc<dyn matrix_rtc_core::Subscription>);

impl BackendSubscription for Cancel {
    fn cancel(&self) {
        self.0.cancel();
    }
}

#[async_trait]
impl MatrixBackend for ScriptedMatrixBackend {
    fn own_user_id(&self) -> String {
        self.mock.user_id.clone()
    }

    fn own_device_id(&self) -> String {
        self.mock.device_id.clone()
    }

    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content_json: String,
        duration_ms: u64,
    ) -> Result<String, FfiBackendError> {
        self.mock
            .send_sticky_event(room_id, event_type, json(&content_json)?, duration_ms)
            .await
            .map_err(ffi_error)
    }

    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: Option<String>,
        content_json: String,
        delay_ms: u64,
    ) -> Result<String, FfiBackendError> {
        self.mock
            .send_delayed_event(
                room_id,
                event_type,
                state_key,
                json(&content_json)?,
                delay_ms,
            )
            .await
            .map_err(ffi_error)
    }

    async fn restart_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), FfiBackendError> {
        self.mock
            .restart_delayed_event(room_id, delay_id)
            .await
            .map_err(ffi_error)
    }

    async fn cancel_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), FfiBackendError> {
        self.mock
            .cancel_delayed_event(room_id, delay_id)
            .await
            .map_err(ffi_error)
    }

    async fn send_to_device_message(
        &self,
        recipients: Vec<FfiToDeviceRecipient>,
        message_type: String,
        content_json: String,
    ) -> Result<Vec<FfiToDeviceDelivery>, FfiBackendError> {
        let recipients = recipients
            .into_iter()
            .map(|recipient| ToDeviceRecipient::new(recipient.user_id, recipient.device_id))
            .collect();
        let deliveries = self
            .mock
            .send_to_device_message(recipients, message_type, json(&content_json)?)
            .await
            .map_err(ffi_error)?;
        Ok(deliveries
            .into_iter()
            .map(|delivery| FfiToDeviceDelivery {
                user_id: delivery.recipient.user_id,
                device_id: delivery.recipient.device_id,
                error: delivery.error,
            })
            .collect())
    }

    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content_json: String,
    ) -> Result<String, FfiBackendError> {
        self.mock
            .send_state_event(room_id, event_type, state_key, json(&content_json)?)
            .await
            .map_err(ffi_error)
    }

    async fn send_room_event(
        &self,
        room_id: String,
        event_type: String,
        content_json: String,
    ) -> Result<String, FfiBackendError> {
        self.mock
            .send_room_event(room_id, event_type, json(&content_json)?)
            .await
            .map_err(ffi_error)
    }

    async fn redact_event(
        &self,
        room_id: String,
        event_id: String,
        reason: Option<String>,
    ) -> Result<(), FfiBackendError> {
        self.mock
            .redact_event(room_id, event_id, reason)
            .await
            .map_err(ffi_error)
    }

    fn subscribe_room(
        &self,
        room_id: String,
        subjects: FfiRoomSubjects,
        sink: Arc<RoomSink>,
    ) -> Result<Arc<dyn BackendSubscription>, FfiBackendError> {
        let subscription = Arc::new(MockRoomSubscription {
            subjects: RoomSubjects {
                state_event_types: subjects.state_event_types,
                timeline_event_types: subjects.timeline_event_types,
            },
            sink: sink.inner.clone(),
            cancelled: AtomicBool::new(false),
        });
        self.mock
            .room_subscriptions
            .lock()
            .unwrap()
            .push((room_id, subscription.clone()));
        Ok(Arc::new(Cancel(subscription)))
    }

    fn subscribe_to_device(
        &self,
        event_types: Vec<String>,
        sink: Arc<ToDeviceSink>,
    ) -> Result<Arc<dyn BackendSubscription>, FfiBackendError> {
        let subscription = Arc::new(MockToDeviceSubscription {
            event_types,
            sink: sink.inner.clone(),
            cancelled: AtomicBool::new(false),
        });
        self.mock
            .to_device_subscriptions
            .lock()
            .unwrap()
            .push(subscription.clone());
        Ok(Arc::new(Cancel(subscription)))
    }

    async fn relations(
        &self,
        room_id: String,
        event_id: String,
        rel_type: String,
        event_type: String,
    ) -> Result<Vec<FfiEventIn>, FfiBackendError> {
        let events = self
            .mock
            .relations(room_id, event_id, rel_type, event_type)
            .await
            .map_err(backend_failure)?;
        Ok(events
            .into_iter()
            .map(|event| FfiEventIn {
                event_id: event.event_id,
                sender: event.sender,
                event_type: event.event_type,
                state_key: event.state_key,
                origin_server_ts: event.origin_server_ts,
                content_json: event.content.to_string(),
                encryption: match event.encryption {
                    EventEncryption::Encrypted {
                        sender_device_id,
                        sender_confirmed,
                    } => FfiEventEncryption {
                        encrypted: true,
                        sender_device_id,
                        sender_confirmed,
                    },
                    EventEncryption::Cleartext => FfiEventEncryption {
                        encrypted: false,
                        sender_device_id: None,
                        sender_confirmed: None,
                    },
                },
            })
            .collect())
    }

    async fn openid_token(&self) -> Result<FfiOpenIdToken, FfiBackendError> {
        Ok(FfiOpenIdToken {
            access_token: "scripted-openid-token".to_owned(),
            token_type: "Bearer".to_owned(),
            matrix_server_name: self
                .mock
                .user_id
                .split_once(':')
                .map_or("example.org", |(_, server)| server)
                .to_owned(),
            expires_in_secs: 3600,
        })
    }

    async fn rtc_transports(&self) -> Result<String, FfiBackendError> {
        self.mock
            .rtc_transports()
            .await
            .map(|transports| transports.to_string())
            .map_err(backend_failure)
    }
}

/// A media transport whose SFUs a test scripts: no media flows, but every
/// connection can be interrupted, closed, restored, refused or made to
/// reject a token. Connect a session through it with
/// [`connect_scripted_media_session`].
#[derive(uniffi::Object)]
pub struct ScriptedMediaTransport {
    inner: ScriptedTransport,
}

#[uniffi::export]
impl ScriptedMediaTransport {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: ScriptedTransport::new(),
        })
    }

    /// The transport reconnecting to `focus` (a `livekit_service_url`).
    pub fn interrupt(&self, focus: String) {
        self.inner.interrupt(&focus);
    }

    /// The transport giving `focus` up.
    pub fn close(&self, focus: String) {
        self.inner.close(&focus);
    }

    /// `focus` works again.
    pub fn restore(&self, focus: String) {
        self.inner.restore(&focus);
    }

    /// `focus` refuses us for good, until restored.
    pub fn refuse(&self, focus: String) {
        self.inner.refuse(&focus);
    }

    /// The next `times` connects to `focus` present a rejected token.
    pub fn reject_token(&self, focus: String, times: u32) {
        self.inner.reject_token(&focus, times);
    }

    /// How many connects `focus` was asked for.
    pub fn connects(&self, focus: String) -> u32 {
        self.inner.connects(&focus)
    }

    /// The focus our media was first connected to.
    pub fn own_focus(&self) -> Option<String> {
        self.inner.own_key()
    }

    /// Every focus our media moved to (034 R25).
    pub fn takeovers(&self) -> Vec<String> {
        self.inner.takeovers()
    }

    /// Plays a media-side fixture, from now: our own SFU closed and back
    /// after 40 s, or never (`budget`, and `fallback`, which a session
    /// connected with `sfu_fallback` and a peer's SFU up turns into a move),
    /// or every peer's SFU interrupted for a minute.
    pub fn play(&self, fixture: String) -> Result<(), FfiScriptError> {
        let own = self.inner.own_key();
        match (fixture.as_str(), own) {
            ("outage.own_sfu.recovers_40s", Some(own)) => {
                self.inner.close(&own);
                let inner = self.inner.clone();
                runtime().spawn(async move {
                    tokio::time::sleep(Duration::from_secs(40)).await;
                    inner.restore(&own);
                });
            }
            ("outage.own_sfu.budget" | "outage.own_sfu.fallback", Some(own)) => {
                self.inner.close(&own);
            }
            ("outage.peer_sfu.1m", own) => {
                let peers: Vec<String> = self
                    .inner
                    .foci()
                    .into_iter()
                    .filter(|focus| Some(focus) != own.as_ref())
                    .collect();
                for peer in &peers {
                    self.inner.interrupt(peer);
                }
                let inner = self.inner.clone();
                runtime().spawn(async move {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    for peer in &peers {
                        inner.restore(peer);
                    }
                });
            }
            _ => {
                return Err(FfiScriptError::UnknownFixture {
                    key: fixture,
                    known: format!(
                        "this transport plays {MEDIA_FIXTURES}, the own-SFU ones once a session \
                         is connected; the backend plays {BACKEND_FIXTURES}"
                    ),
                });
            }
        }
        Ok(())
    }
}

/// [`connect_media_session`](crate::media::connect_media_session) through a
/// [`ScriptedMediaTransport`] instead of LiveKit: the same session, roster,
/// interruptions and call status, over SFUs the test scripts.
#[uniffi::export(async_runtime = "tokio")]
pub async fn connect_scripted_media_session(
    call: Arc<crate::RtcCall>,
    config: MediaSessionConfig,
    transport: Arc<ScriptedMediaTransport>,
) -> Result<Arc<MediaSession>, MediaFfiError> {
    runtime()
        .spawn(async move {
            let format = call.format();
            let mapper = matrix_rtc_transport::livekit::identity_mapper(format);
            transport.inner.set_identity_mapper(mapper.clone());
            let backend = call.backend();
            let key_handler = Arc::new(matrix_rtc_transport::MediaKeyHandler::new());
            let media = matrix_rtc_call_sdk::attach_media(
                call.inner(),
                Arc::new(transport.inner.clone()),
                key_handler.clone(),
                matrix_rtc_call_sdk::AttachOptions {
                    own_user_id: backend.own_user_id(),
                    own_device_id: backend.own_device_id(),
                    identity_mapper: mapper,
                    format,
                    stability: config.stability.map(Into::into).unwrap_or_default(),
                    own_reconnect_budget: config.own_reconnect_budget_ms.map_or(
                        matrix_rtc_transport::pool::OWN_RECONNECT_BUDGET,
                        Duration::from_millis,
                    ),
                    sfu_fallback: config.sfu_fallback,
                },
            )
            .await
            .map_err(crate::media::session_attach_error)?;
            Ok(MediaSession::new(media, key_handler))
        })
        .await
        .map_err(|error| MediaFfiError::Transport(format!("media task panicked: {error}")))?
}

#[cfg(test)]
mod tests {
    //! The fakes driven through the exported surface, as a host's tests
    //! would: real time, so budgets are scaled down.

    use super::*;
    use crate::params::FfiJoinTransport;
    use crate::{
        FfiCallEnd, FfiCallStatus, FfiConnectionLossReason, FfiJoinSessionParams, FfiRoomOptions,
        RtcCall, RtcClient, RtcRoom,
    };

    const ROOM: &str = "!room:example.org";
    const FOCUS: &str = "https://sfu.example.org";

    fn cleartext() -> crate::backend::FfiEventEncryption {
        FfiEventEncryption {
            encrypted: false,
            sender_device_id: None,
            sender_confirmed: None,
        }
    }

    /// Opens the room through the scripted backend, delivers an open slot,
    /// and joins it publishing on [`FOCUS`].
    fn joined(backend: &Arc<ScriptedMatrixBackend>) -> (Arc<RtcRoom>, Arc<RtcCall>) {
        let client = RtcClient::new(backend.clone().backend());
        runtime().block_on(async {
            let open = client
                .clone()
                .room(ROOM.to_owned(), FfiRoomOptions::default());
            tokio::pin!(open);
            let seed = async {
                while backend.mock.room_subscription(ROOM).is_none() {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                backend.deliver_encryption(ROOM.to_owned(), false);
                backend.deliver_state_events(
                    ROOM.to_owned(),
                    matrix_rtc_core::SLOT_EVENT_TYPE.to_owned(),
                    vec![FfiEventIn {
                        event_id: "$slot".to_owned(),
                        sender: "@admin:example.org".to_owned(),
                        event_type: matrix_rtc_core::SLOT_EVENT_TYPE.to_owned(),
                        state_key: Some("m.call#room".to_owned()),
                        origin_server_ts: 1,
                        content_json: r#"{"status":"open","application":{"type":"m.call"}}"#
                            .to_owned(),
                        encryption: cleartext(),
                    }],
                );
                backend.deliver_joined_members(ROOM.to_owned(), vec!["@alice:example.org".into()]);
                backend.deliver_sticky_events(ROOM.to_owned(), Vec::new());
            };
            let (room, ()) = tokio::join!(open, seed);
            let room = room.expect("the room opens");
            let call = room
                .clone()
                .join_call(FfiJoinSessionParams {
                    application_slot_id: None,
                    transport: FfiJoinTransport::Publish {
                        livekit_service_url: FOCUS.to_owned(),
                    },
                    keep_alive_timeout_ms: None,
                    sticky_duration_ms: None,
                    degraded_lifetime_ms: None,
                    encryption_config: None,
                    kind: crate::FfiCallKind::Group { notify: None },
                    reactions: None,
                })
                .await
                .expect("joins");
            (room, call)
        })
    }

    async fn until(mut done: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !done() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("in time");
    }

    /// A call on both fakes: connected once the scripted SFU is up, and
    /// given up after the budget once it is closed, its leave sent first.
    #[test]
    fn a_scripted_call_connects_and_loses_its_own_sfu() {
        let backend = ScriptedMatrixBackend::new("@alice:example.org".into(), "ALICE".into());
        let (_room, call) = joined(&backend);
        let transport = ScriptedMediaTransport::new();
        let _session = runtime()
            .block_on(connect_scripted_media_session(
                call.clone(),
                MediaSessionConfig {
                    own_reconnect_budget_ms: Some(500),
                    ..MediaSessionConfig::default()
                },
                transport.clone(),
            ))
            .expect("connects");
        runtime().block_on(until(|| {
            matches!(call.status(), FfiCallStatus::Connected { .. })
        }));
        assert_eq!(transport.own_focus().as_deref(), Some(FOCUS));

        transport.close(FOCUS.to_owned());
        runtime().block_on(until(|| {
            matches!(call.status(), FfiCallStatus::Ended { .. })
        }));
        let FfiCallStatus::Ended { reason } = call.status() else {
            unreachable!()
        };
        assert!(
            matches!(
                reason,
                FfiCallEnd::ConnectionClosed {
                    reason: FfiConnectionLossReason::Budget,
                    ..
                }
            ),
            "{reason:?}"
        );
        let leave = backend.sticky_events_sent(ROOM.to_owned()).pop().unwrap();
        assert!(leave.contains(r#""membership":"leave""#), "{leave}");
    }

    /// An outage fails every send until it is lifted, and every call is
    /// logged either way.
    #[test]
    fn a_scripted_outage_fails_every_send_until_lifted() {
        let backend = ScriptedMatrixBackend::new("@alice:example.org".into(), "ALICE".into());
        backend.set_outage(Some(FfiScriptedOutcome::Fail {
            errcode: None,
            status: None,
            retry_after_ms: None,
            permanent: false,
        }));
        let failed = runtime().block_on(backend.send_room_event(
            ROOM.to_owned(),
            "m.room.message".to_owned(),
            "{}".to_owned(),
        ));
        assert!(failed.is_err());
        backend.set_outage(None);
        assert!(
            runtime()
                .block_on(backend.send_room_event(
                    ROOM.to_owned(),
                    "m.room.message".to_owned(),
                    "{}".to_owned(),
                ))
                .is_ok()
        );
        assert_eq!(
            backend
                .calls()
                .iter()
                .filter(|call| call.method == FfiScriptedMethod::RoomEvent)
                .count(),
            2
        );
    }

    #[test]
    fn an_unknown_fixture_names_the_known_ones() {
        let backend = ScriptedMatrixBackend::new("@alice:example.org".into(), "ALICE".into());
        let Err(FfiScriptError::UnknownFixture { known, .. }) =
            backend.play("outage.nope".to_owned())
        else {
            panic!("refused");
        };
        assert!(known.contains("outage.homeserver.30s"));
        assert!(known.contains("outage.own_sfu.fallback"));
        assert!(
            backend
                .play("outage.rate_limit.retry_after_7s".to_owned())
                .is_ok()
        );
    }
}
