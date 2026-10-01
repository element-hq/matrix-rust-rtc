// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The `matrix_sdk::Client` implementation of [`MatrixBackend`]: sends as
//! Client-Server requests, room subjects re-read from the SDK on every sync
//! that touches the room, to-device and timeline events from event handlers.
//! Dialects and MatrixRTC parsing are not here: the wrapper and the feeder do
//! those for every backend. Requires the `matrix-sdk` feature.

use std::collections::HashSet;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use matrix_rtc_core::{
    BackendError, CommandError, EventEncryption, EventIn, MatrixBackend, OpenIdToken, RoomSink,
    RoomSubjects, Subscription, ToDeviceDelivery, ToDeviceMessageIn, ToDeviceRecipient,
    ToDeviceSink,
};
use matrix_sdk::deserialized_responses::{
    AlgorithmInfo, DeviceLinkProblem, EncryptionInfo, RawAnySyncOrStrippedState, VerificationLevel,
    VerificationState,
};
use matrix_sdk::encryption::identities::Device;
use matrix_sdk::event_handler::EventHandlerDropGuard;
use matrix_sdk::room::{IncludeRelations, RelationsOptions};
use matrix_sdk::ruma::api::client::delayed_events::update_delayed_event::UpdateAction;
use matrix_sdk::ruma::api::client::delayed_events::{
    DelayParameters, delayed_message_event, delayed_state_event, update_delayed_event,
};
use matrix_sdk::ruma::api::client::state::{get_state_events, send_state_event};
use matrix_sdk::ruma::api::error::ErrorKind;
use matrix_sdk::ruma::events::relation::RelationType;
use matrix_sdk::ruma::events::{
    AnyMessageLikeEventContent, AnyStateEventContent, AnySyncMessageLikeEvent, AnyToDeviceEvent,
    AnyToDeviceEventContent, MessageLikeEventType, StateEventType, TimelineEventType,
};
use matrix_sdk::ruma::serde::Raw;
use matrix_sdk::ruma::{
    DeviceId, EventId, OwnedEventId, OwnedUserId, RoomId, TransactionId, UInt, UserId,
};
use matrix_sdk::{Client, Room, RoomMemberships};
use matrix_sdk_base::crypto::CollectStrategy;
use serde_json::Value;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;

// The sticky duration for `m.rtc.member` comes from the core
// (`JoinSessionParams::sticky_duration_ms`), which re-sends the membership at
// half that interval to stay in the map.
//
// NOTE: the delayed leave is a plain (non-sticky) delayed event, so it clears
// nothing from the sticky map when it fires — crash cleanup relies entirely on
// the membership's own sticky TTL expiring. Not an HTTP-level limitation: both
// `org.matrix.msc4140.delay` and `org.matrix.msc4354.sticky_duration_ms` are
// query parameters on the same `PUT /send`, but ruma's request types cannot
// express both, and neither MSC states that the two compose.

fn command_error(error: impl std::fmt::Display) -> CommandError {
    CommandError::from_message(error.to_string())
}

/// [`command_error`] for the MSC4140 endpoints: `M_UNRECOGNIZED` is a
/// homeserver without the endpoint, `M_FORBIDDEN` one that switched it off
/// (matrix.org's "Sending delayed events has been disallowed"). Either makes
/// the core stop asking for the rest of the session.
fn delayed_command_error(error: matrix_sdk::HttpError) -> CommandError {
    match error.client_api_error_kind() {
        Some(ErrorKind::Unrecognized | ErrorKind::Forbidden) => {
            CommandError::DelayedEventsNotSupported(error.to_string())
        }
        _ => command_error(error),
    }
}

fn backend_error(error: impl std::fmt::Display) -> BackendError {
    BackendError::new(error.to_string())
}

/// Ruma owns the wire spelling: the core's `m.rtc.member` becomes the MSC4143
/// id and follows the stable one once ruma flips.
fn wire_event_type(event_type: String) -> MessageLikeEventType {
    MessageLikeEventType::from(event_type)
}

/// Bounded request behaviour for RTC signalling sends: the SDK default
/// retries without limit, which turns a wedged homeserver into a send that
/// never returns. Five attempts let a rate limit clear (a join is two events,
/// and synapse's default burst is ten) while a dead homeserver still fails in
/// readable time; the waits are the server's own `Retry-After`.
fn rtc_request_config() -> matrix_sdk::config::RequestConfig {
    matrix_sdk::config::RequestConfig::new()
        .timeout(Duration::from_secs(15))
        .retry_limit(5)
}

/// How long wakes are coalesced before a room's subjects are re-read: a sync
/// that touches the room can wake twice, and each re-read is a state fetch.
const WAKE_DEBOUNCE: Duration = Duration::from_millis(250);

/// How long to wait before re-reading a subject whose fetch failed.
const RETRY_AFTER: Duration = Duration::from_secs(5);

/// A [`MatrixBackend`] over a logged-in `matrix_sdk::Client`. Clone-cheap.
#[derive(Clone)]
pub struct SdkBackend {
    client: Client,
}

impl SdkBackend {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    fn room(&self, room_id: &str) -> Result<Room, CommandError> {
        let room_id = RoomId::parse(room_id).map_err(command_error)?;
        self.client
            .get_room(&room_id)
            .ok_or_else(|| CommandError::from_message(format!("room {room_id} not found")))
    }

    fn room_for_read(&self, room_id: &str) -> Result<Room, BackendError> {
        self.room(room_id).map_err(backend_error)
    }

    /// Send a plain message-like room event, bounded like every other RTC send.
    /// `send_raw` encrypts in an encrypted room like any other message send.
    async fn send_room_message(
        &self,
        room: &Room,
        event_type: &str,
        content: &Value,
    ) -> Result<String, CommandError> {
        let response = room
            .send_raw(event_type, content)
            .with_request_config(rtc_request_config())
            .await
            .map_err(command_error)?;
        Ok(response.response.event_id.to_string())
    }
}

/// What the client's decryption metadata says, as the backend reports it.
///
/// MSC4153 asks whether the sending device is cross-signed, not whether we
/// trust its owner: an unverified *identity* still signs its own devices.
/// States that leave the device unattributable count as not cross-signed.
fn event_encryption(info: Option<&EncryptionInfo>) -> EventEncryption {
    let Some(info) = info else {
        return EventEncryption::Cleartext;
    };
    let cross_signed = !matches!(
        info.verification_state,
        VerificationState::Unverified(
            VerificationLevel::UnsignedDevice
                | VerificationLevel::None(_)
                | VerificationLevel::MismatchedSender
        )
    );
    EventEncryption::Encrypted {
        sender_device_id: info.sender_device.as_ref().map(|d| d.to_string()),
        sender_cross_signed: Some(cross_signed),
    }
}

/// Whether the SDK's verdict on the sending device is still provisional: the
/// device was not in the store, or it was (an Olm message carries its own
/// device keys) but the sender's cross-signing identity was not downloaded
/// yet, so "unsigned" may only mean "not checked yet".
async fn sender_trust_pending(client: &Client, info: &EncryptionInfo) -> bool {
    match info.verification_state {
        VerificationState::Unverified(VerificationLevel::None(
            DeviceLinkProblem::MissingDevice,
        )) => true,
        VerificationState::Unverified(VerificationLevel::UnsignedDevice) => client
            .encryption()
            .get_user_identity(&info.sender)
            .await
            .ok()
            .flatten()
            .is_none(),
        _ => false,
    }
}

/// [`event_encryption`] for a device looked up after the fact.
fn device_encryption(device: &Device) -> EventEncryption {
    EventEncryption::Encrypted {
        sender_device_id: Some(device.device_id().to_string()),
        sender_cross_signed: Some(device.is_verified() || device.is_cross_signed_by_owner()),
    }
}

/// The decryption information of a to-device message whose sender trust was
/// pending ([`sender_trust_pending`]), after querying the sender's keys.
///
/// A peer who joins a call right after joining the room sends its media key
/// before our client has queried its keys; reported as-is, the library reads
/// that as "not cross-signed" and refuses the key for good. One keys query
/// for the sender settles it; a failed query reports the message as the SDK
/// did.
async fn resolve_to_device_encryption(client: &Client, info: &EncryptionInfo) -> EventEncryption {
    let AlgorithmInfo::OlmV1Curve25519AesSha2 {
        curve25519_public_key_base64: sender_key,
    } = &info.algorithm_info
    else {
        return event_encryption(Some(info));
    };
    let encryption = client.encryption();
    if let Err(error) = encryption.request_user_identity(&info.sender).await {
        log::warn!(
            "could not query {}'s keys for a to-device message from them ({error}); reporting \
             it as decrypted",
            info.sender,
        );
        return event_encryption(Some(info));
    }
    match encryption.get_user_devices(&info.sender).await {
        Ok(devices) => devices
            .devices()
            .find(|device| {
                device
                    .curve25519_key()
                    .is_some_and(|key| key.to_base64() == *sender_key)
            })
            .map(|device| device_encryption(&device))
            .unwrap_or_else(|| event_encryption(Some(info))),
        Err(_) => event_encryption(Some(info)),
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl MatrixBackend for SdkBackend {
    fn own_user_id(&self) -> String {
        self.client
            .user_id()
            .map(|id| id.to_string())
            .unwrap_or_default()
    }

    fn own_device_id(&self) -> String {
        self.client
            .device_id()
            .map(|id| id.to_string())
            .unwrap_or_default()
    }

    /// `duration_ms` is the core's value, not a constant of ours: it schedules
    /// the refresh against exactly this lifetime.
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        duration_ms: u64,
    ) -> Result<String, CommandError> {
        let room = self.room(&room_id)?;
        let event_type = wire_event_type(event_type).to_string();
        let response = room
            .send_raw(&event_type, &content)
            .with_sticky_duration(Duration::from_millis(duration_ms))
            .with_request_config(rtc_request_config())
            .await
            .map_err(command_error)?;
        Ok(response.response.event_id.to_string())
    }

    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: Option<String>,
        content: Value,
        delay_ms: u64,
    ) -> Result<String, CommandError> {
        let room_id = RoomId::parse(&room_id).map_err(command_error)?;
        let delay = DelayParameters::Timeout {
            timeout: Duration::from_millis(delay_ms),
        };
        let raw = serde_json::value::to_raw_value(&content).map_err(command_error)?;

        // Returns the MSC4140 delay id, which is what restart/cancel take.
        let delay_id = match state_key {
            // NOTE the argument order: `state_key` comes *before* `event_type`
            // here, unlike every other ruma send.
            Some(state_key) => {
                let request = delayed_state_event::unstable::Request::new_raw(
                    room_id,
                    state_key,
                    StateEventType::from(event_type),
                    delay,
                    Raw::<AnyStateEventContent>::from_json(raw),
                );
                self.client
                    .send(request)
                    .with_request_config(rtc_request_config())
                    .await
                    .map_err(delayed_command_error)?
                    .delay_id
            }
            None => {
                let request = delayed_message_event::unstable::Request::new_raw(
                    room_id,
                    TransactionId::new(),
                    wire_event_type(event_type),
                    delay,
                    Raw::<AnyMessageLikeEventContent>::from_json(raw),
                );
                self.client
                    .send(request)
                    .with_request_config(rtc_request_config())
                    .await
                    .map_err(delayed_command_error)?
                    .delay_id
            }
        };
        Ok(delay_id)
    }

    async fn restart_delayed_event(
        &self,
        _room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        // MSC4140's "heartbeat ping": one request, never a moment with no
        // delayed leave armed.
        let request =
            update_delayed_event::unstable_v1::Request::new(delay_id, UpdateAction::Restart);
        self.client
            .send(request)
            .with_request_config(rtc_request_config())
            .await
            .map_err(delayed_command_error)?;
        Ok(())
    }

    async fn cancel_delayed_event(
        &self,
        _room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        let request =
            update_delayed_event::unstable_v1::Request::new(delay_id, UpdateAction::Cancel);
        self.client
            .send(request)
            .with_request_config(rtc_request_config())
            .await
            .map_err(delayed_command_error)?;
        Ok(())
    }

    async fn send_to_device_message(
        &self,
        recipients: Vec<ToDeviceRecipient>,
        message_type: String,
        content: Value,
    ) -> Result<Vec<ToDeviceDelivery>, CommandError> {
        // Olm-encrypted, to exactly the devices that published the memberships;
        // no `"*"` fan-out. One SDK call for the batch, which answers with the
        // devices it could not reach — the per-recipient outcome the core needs.
        let encryption = self.client.encryption();
        let mut devices = Vec::with_capacity(recipients.len());
        let mut unknown = Vec::new();
        // One `/keys/query` per user per batch.
        let mut queried: HashSet<OwnedUserId> = HashSet::new();

        for recipient in &recipients {
            let user = match UserId::parse(&recipient.user_id) {
                Ok(user) => user,
                Err(error) => {
                    unknown.push(ToDeviceDelivery::failed(
                        recipient.clone(),
                        format!("unparseable user id: {error}"),
                    ));
                    continue;
                }
            };
            let device_id = <&DeviceId>::from(recipient.device_id.as_str());

            let mut found = encryption.get_device(&user, device_id).await;

            // Routine on the first key we send a peer, and likely on the
            // pre-sticky path, where nothing has forced a `/keys/query` yet.
            if matches!(found, Ok(None)) && queried.insert(user.clone()) {
                log::debug!(
                    "{user}/{device_id} is not in the crypto store; querying keys before \
                     giving up on delivering a media key to them",
                );
                if let Err(error) = encryption.request_user_identity(&user).await {
                    log::warn!("keys query for {user} failed: {error}");
                }
                found = encryption.get_device(&user, device_id).await;
            }

            match found {
                Ok(Some(device)) => devices.push(device),
                Ok(None) => unknown.push(ToDeviceDelivery::failed(
                    recipient.clone(),
                    "no such known device",
                )),
                Err(error) => unknown.push(ToDeviceDelivery::failed(
                    recipient.clone(),
                    format!("could not look up the device: {error}"),
                )),
            }
        }

        if devices.is_empty() {
            return Ok(unknown);
        }

        let raw: Raw<AnyToDeviceEventContent> =
            Raw::new(&content).map_err(command_error)?.cast_unchecked();
        let failures = encryption
            .encrypt_and_send_raw_to_device(
                devices.iter().collect(),
                &message_type,
                raw,
                // MSC4153: refuse to hand keys to unverified identities.
                CollectStrategy::IdentityBasedStrategy,
            )
            .await
            .map_err(command_error)?;

        let mut deliveries = unknown;
        for device in &devices {
            let recipient =
                ToDeviceRecipient::new(device.user_id().as_str(), device.device_id().as_str());
            let failed = failures
                .iter()
                .any(|(user, dev)| user == device.user_id() && dev == device.device_id());
            deliveries.push(if failed {
                ToDeviceDelivery::failed(recipient, "the homeserver did not accept the message")
            } else {
                ToDeviceDelivery::sent(recipient)
            });
        }

        if !failures.is_empty() {
            log::warn!(
                "to-device {message_type}: {} of {} device(s) did not receive the key",
                failures.len(),
                devices.len(),
            );
        }
        Ok(deliveries)
    }

    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content: Value,
    ) -> Result<String, CommandError> {
        let room = self.room(&room_id)?;
        // Bounded by `rtc_request_config`: the pre-sticky membership comes
        // through here, as time-critical as the sticky one. The type is
        // normalised through ruma, which registers `m.rtc.slot` as
        // an alias of the MSC4143 id.
        let raw = serde_json::value::to_raw_value(&content).map_err(command_error)?;
        let request = send_state_event::v3::Request::new_raw(
            room.room_id().to_owned(),
            StateEventType::from(event_type),
            state_key,
            Raw::<AnyStateEventContent>::from_json(raw),
        );
        let response = self
            .client
            .send(request)
            .with_request_config(rtc_request_config())
            .await
            .map_err(command_error)?;
        Ok(response.event_id.to_string())
    }

    async fn send_room_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
    ) -> Result<String, CommandError> {
        let room = self.room(&room_id)?;
        self.send_room_message(&room, &event_type, &content).await
    }

    async fn redact_event(
        &self,
        room_id: String,
        event_id: String,
        reason: Option<String>,
    ) -> Result<(), CommandError> {
        let room = self.room(&room_id)?;
        let event_id = EventId::parse(&event_id).map_err(command_error)?;
        room.redact(&event_id, reason.as_deref(), None)
            .await
            .map_err(command_error)?;
        Ok(())
    }

    async fn subscribe_room(
        &self,
        room_id: String,
        subjects: RoomSubjects,
        sink: Arc<dyn RoomSink>,
    ) -> Result<Arc<dyn Subscription>, BackendError> {
        let room = self.room_for_read(&room_id)?;

        let mut guards = Vec::new();
        if !subjects.timeline_event_types.is_empty() {
            let handle = register_timeline_receiver(
                &room,
                sink.clone(),
                subjects.timeline_event_types.clone(),
            );
            guards.push(self.client.event_handler_drop_guard(handle));
        }

        let task = tokio::spawn(feed_room_subjects(room, subjects, sink));
        Ok(Arc::new(SdkSubscription::new(task, guards)))
    }

    async fn subscribe_to_device(
        &self,
        event_types: Vec<String>,
        sink: Arc<dyn ToDeviceSink>,
    ) -> Result<Arc<dyn Subscription>, BackendError> {
        // Raw rather than typed: ruma has no event for the MSC4143 rewrite nor
        // for the legacy key type, and a typed handler silently never fires
        // when the content does not match its model.
        let event_types = Arc::new(event_types);
        let client = self.client.clone();
        let handle = self.client.add_event_handler(
            move |event: Raw<AnyToDeviceEvent>, encryption_info: Option<EncryptionInfo>| {
                let sink = sink.clone();
                let event_types = event_types.clone();
                let client = client.clone();
                async move {
                    let Some(event_type) = event.get_field::<String>("type").ok().flatten() else {
                        return;
                    };
                    if !event_types.contains(&event_type) {
                        return;
                    }
                    let sender = encryption_info
                        .as_ref()
                        .map(|info| info.sender.to_string())
                        .or_else(|| event.get_field::<String>("sender").ok().flatten());
                    let (Some(sender), Ok(Some(content))) =
                        (sender, event.get_field::<Value>("content"))
                    else {
                        log::warn!(
                            "ignoring a {event_type} to-device message with no sender or content"
                        );
                        return;
                    };
                    log::debug!(
                        "to-device {event_type} from {sender}: {:?}",
                        encryption_info
                            .as_ref()
                            .map(|info| (&info.verification_state, &info.sender_device))
                    );
                    let deliver = move |encryption| {
                        sink.on_to_device_message(ToDeviceMessageIn {
                            sender,
                            event_type,
                            content,
                            encryption,
                        })
                    };
                    match encryption_info {
                        // A keys query inside the handler would stall the sync
                        // loop; resolve it on the side.
                        Some(info) if sender_trust_pending(&client, &info).await => {
                            tokio::spawn(async move {
                                deliver(resolve_to_device_encryption(&client, &info).await)
                            });
                        }
                        info => deliver(event_encryption(info.as_ref())),
                    }
                }
            },
        );
        let guard = self.client.event_handler_drop_guard(handle);
        Ok(Arc::new(SdkSubscription::new(
            tokio::spawn(std::future::pending()),
            vec![guard],
        )))
    }

    async fn relations(
        &self,
        room_id: String,
        event_id: String,
        rel_type: String,
        event_type: String,
    ) -> Result<Vec<EventIn>, BackendError> {
        let room = self.room_for_read(&room_id)?;
        let target = OwnedEventId::try_from(event_id.as_str()).map_err(backend_error)?;
        let options = RelationsOptions {
            limit: Some(UInt::from(RELATIONS_LOOKUP_LIMIT)),
            include_relations: IncludeRelations::RelationsOfTypeAndEventType(
                RelationType::from(rel_type.as_str()),
                TimelineEventType::from(event_type.as_str()),
            ),
            ..RelationsOptions::default()
        };
        let wanted = std::slice::from_ref(&event_type);
        let relations = room
            .relations(target, options)
            .await
            .map_err(backend_error)?;
        Ok(relations
            .chunk
            .iter()
            .filter_map(|event| {
                match timeline_ingest_from_raw(
                    event.raw(),
                    event.encryption_info().map(Arc::as_ref),
                    wanted,
                ) {
                    Some(TimelineIngest::Event(event)) => Some(event),
                    _ => None,
                }
            })
            .collect())
    }

    async fn openid_token(&self) -> Result<OpenIdToken, BackendError> {
        let response = self
            .client
            .account()
            .request_openid_token()
            .await
            .map_err(backend_error)?;
        Ok(OpenIdToken {
            access_token: response.access_token,
            token_type: response.token_type.to_string(),
            matrix_server_name: response.matrix_server_name.to_string(),
            expires_in: response.expires_in.as_secs(),
        })
    }

    /// Through the SDK's discovery: cached, with a bounded request, and
    /// falling back to the well-known `rtc_foci` when the homeserver lacks the
    /// endpoint. Nothing discovered is an empty array ("none advertised"); a
    /// failed request stays an error rather than reading as "none".
    async fn rtc_transports(&self) -> Result<Value, BackendError> {
        let transports = self
            .client
            .discover_rtc_transports()
            .await
            .map_err(backend_error)?
            .unwrap_or_default();
        serde_json::to_value(transports).map_err(backend_error)
    }
}

/// Relations asked for per event; a call wants one raised hand per member.
const RELATIONS_LOOKUP_LIMIT: u32 = 50;

/// A subscription that aborts its task and drops its handler guards on cancel.
struct SdkSubscription {
    live: StdMutex<Option<(JoinHandle<()>, Vec<EventHandlerDropGuard>)>>,
}

impl SdkSubscription {
    fn new(task: JoinHandle<()>, guards: Vec<EventHandlerDropGuard>) -> Self {
        Self {
            live: StdMutex::new(Some((task, guards))),
        }
    }
}

impl Subscription for SdkSubscription {
    fn cancel(&self) {
        if let Some((task, guards)) = self.live.lock().ok().and_then(|mut live| live.take()) {
            task.abort();
            drop(guards);
        }
    }
}

impl Drop for SdkSubscription {
    fn drop(&mut self) {
        self.cancel();
    }
}

/// Whether a wake source's `recv()` means "keep going": a lagged receiver
/// still means something changed.
fn keep_going<T>(result: Result<T, RecvError>) -> bool {
    matches!(result, Ok(_) | Err(RecvError::Lagged(_)))
}

/// Delivers the room's subjects: the current sets first, then again on every
/// sync that touches the room. Each delivery is the complete set; the core
/// replaces rather than merges. A subject whose read failed is skipped for
/// that round and retried, so a failed slot fetch never reads as "no slots".
async fn feed_room_subjects(room: Room, subjects: RoomSubjects, sink: Arc<dyn RoomSink>) {
    let room_id = room.room_id().to_string();
    log::info!("[{room_id}] room subscription started");

    let mut sticky = room.sticky_events().subscribe();
    let mut updates = room.subscribe_to_updates();

    loop {
        let all_read = emit_room_subjects(&room, &subjects, &sink).await;

        let woken = tokio::select! {
            result = sticky.recv() => keep_going(result),
            result = updates.recv() => keep_going(result),
            _ = tokio::time::sleep(RETRY_AFTER), if !all_read => true,
        };
        if !woken {
            break;
        }
        tokio::time::sleep(WAKE_DEBOUNCE).await;
    }

    log::info!("[{room_id}] room subscription stopped");
}

/// One delivery of every subject, in the order the feeder wants them:
/// encryption, state, members, sticky. Returns whether every read succeeded.
async fn emit_room_subjects(
    room: &Room,
    subjects: &RoomSubjects,
    sink: &Arc<dyn RoomSink>,
) -> bool {
    let room_id = room.room_id().to_string();
    let mut all_read = true;

    match room.latest_encryption_state().await {
        Ok(state) => sink.on_encryption(state.is_encrypted()),
        Err(error) => {
            log::warn!("[{room_id}] failed to read room encryption state: {error}");
            all_read = false;
        }
    }

    // A type the SDK store already holds is read from it; one fetch answers
    // every other requested type, each delivered under its own type: the
    // library tells the spellings apart, not us.
    for event_type in &subjects.state_event_types {
        if held_by_store(event_type) {
            sink.on_state_events(
                event_type.clone(),
                store_state_snapshot(room, event_type).await,
            );
        }
    }
    let fetched: Vec<&String> = subjects
        .state_event_types
        .iter()
        .filter(|event_type| !held_by_store(event_type))
        .collect();
    if !fetched.is_empty() {
        match state_snapshot(room, &fetched).await {
            Some(events) => {
                for event_type in fetched {
                    let of_type = events
                        .iter()
                        .filter(|event| &event.event_type == event_type)
                        .cloned()
                        .collect();
                    sink.on_state_events(event_type.clone(), of_type);
                }
            }
            None => all_read = false,
        }
    }

    match room.members(RoomMemberships::JOIN).await {
        Ok(members) => sink.on_joined_members(
            members
                .into_iter()
                .map(|member| member.user_id().to_string())
                .collect(),
        ),
        Err(error) => {
            log::warn!("[{room_id}] failed to read room members: {error}");
            all_read = false;
        }
    }

    sink.on_sticky_events(sticky_snapshot(room));

    all_read
}

/// The room's live sticky events, raw. The sticky map only files plaintext
/// and successfully decrypted events, so the presence of decryption metadata
/// is exactly whether the event arrived encrypted.
fn sticky_snapshot(room: &Room) -> Vec<EventIn> {
    let room_id = room.room_id().to_string();
    room.sticky_events()
        .live()
        .into_iter()
        .filter_map(|entry| {
            let event_type = entry.key.event_type.to_string();
            let content: Value = match entry.raw().get_field("content") {
                Ok(Some(content)) => content,
                _ => {
                    log::warn!(
                        "[{room_id}] ignoring an {event_type} sticky with no content object \
                         (sticky key {})",
                        entry.key.sticky_key,
                    );
                    return None;
                }
            };
            let origin_server_ts = entry
                .raw()
                .get_field::<u64>("origin_server_ts")
                .ok()
                .flatten()
                .unwrap_or(0);
            Some(EventIn {
                event_id: entry.event_id.to_string(),
                sender: entry.key.sender.to_string(),
                event_type,
                state_key: None,
                origin_server_ts,
                content,
                encryption: event_encryption(entry.encryption_info().map(Arc::as_ref)),
            })
        })
        .collect()
}

/// The room's state events of `event_types` (in practice the `m.rtc.slot`
/// spellings), or `None` when it could not be read.
///
/// Asks the homeserver (`GET /rooms/{id}/state`) instead of the SDK's store:
/// the store only holds the types in sliding sync's `required_state`, which
/// does not include the slot type, so it would report every room as slotless
/// — and that closes every session. A failed fetch is `None`, never "none".
async fn state_snapshot(room: &Room, event_types: &[&String]) -> Option<Vec<EventIn>> {
    let room_id = room.room_id().to_string();
    let request = get_state_events::v3::Request::new(room.room_id().to_owned());
    let response = match room.client().send(request).await {
        Ok(response) => response,
        Err(error) => {
            log::warn!("[{room_id}] failed to fetch room state: {error}");
            return None;
        }
    };

    let events: Vec<EventIn> = response
        .room_state
        .into_iter()
        .filter_map(|raw| {
            let event_type = raw.get_field::<String>("type").ok().flatten()?;
            if !event_types.iter().any(|wanted| **wanted == event_type) {
                return None;
            }
            Some(EventIn {
                event_id: raw.get_field("event_id").ok().flatten()?,
                sender: raw.get_field("sender").ok().flatten()?,
                event_type,
                state_key: raw.get_field("state_key").ok().flatten(),
                origin_server_ts: raw
                    .get_field("origin_server_ts")
                    .ok()
                    .flatten()
                    .unwrap_or(0),
                content: raw
                    .get_field("content")
                    .ok()
                    .flatten()
                    .unwrap_or(Value::Null),
                encryption: EventEncryption::Cleartext,
            })
        })
        .collect();

    log::debug!(
        "[{room_id}] room state fetched: {} event(s) of {event_types:?}",
        events.len(),
    );
    Some(events)
}

/// Whether the SDK store keeps `event_type` current on its own, so it is read
/// from there rather than fetched: `m.call.member` (the pre-sticky Element
/// Call membership) is in sliding sync's default `required_state`.
fn held_by_store(event_type: &str) -> bool {
    StateEventType::from(event_type) == StateEventType::CallMember
}

/// The room's state of `event_type`, raw, from the SDK store. Stripped state
/// belongs to a room we are only invited to and carries no `origin_server_ts`,
/// so it is skipped.
async fn store_state_snapshot(room: &Room, event_type: &str) -> Vec<EventIn> {
    let room_id = room.room_id().to_string();
    let raw = match room
        .get_state_events(StateEventType::from(event_type))
        .await
    {
        Ok(raw) => raw,
        Err(error) => {
            log::warn!("[{room_id}] could not read {event_type} state: {error}");
            return Vec::new();
        }
    };
    raw.into_iter()
        .filter_map(|state| {
            let RawAnySyncOrStrippedState::Sync(raw) = state else {
                return None;
            };
            Some(EventIn {
                event_id: raw.get_field("event_id").ok().flatten()?,
                sender: raw.get_field("sender").ok().flatten()?,
                event_type: event_type.to_owned(),
                state_key: raw.get_field("state_key").ok().flatten(),
                origin_server_ts: raw.get_field("origin_server_ts").ok().flatten()?,
                content: raw.get_field("content").ok().flatten()?,
                encryption: EventEncryption::Cleartext,
            })
        })
        .collect()
}

/// A timeline event as the feeder takes it, or a redaction.
#[derive(Clone, Debug)]
pub enum TimelineIngest {
    Event(EventIn),
    /// A redaction, by the id of the event it redacts.
    Redacted {
        event_id: String,
    },
}

/// A redaction, or an event whose type is in `event_types`; `None` otherwise.
///
/// Generic over the `Raw` payload because the SDK hands the same JSON out under
/// different type parameters. The redaction target is read from both places
/// the room versions put it (`redacts` before v11, `content.redacts` from v11).
pub fn timeline_ingest_from_raw<T>(
    raw: &Raw<T>,
    encryption_info: Option<&EncryptionInfo>,
    event_types: &[String],
) -> Option<TimelineIngest> {
    let event_type: String = raw.get_field("type").ok().flatten()?;
    match event_type.as_str() {
        "m.room.redaction" => {
            let redacts: Option<String> = raw.get_field("redacts").ok().flatten().or_else(|| {
                raw.get_field::<Value>("content")
                    .ok()
                    .flatten()
                    .and_then(|content| content.get("redacts")?.as_str().map(str::to_owned))
            });
            redacts.map(|event_id| TimelineIngest::Redacted { event_id })
        }
        _ if event_types.contains(&event_type) => Some(TimelineIngest::Event(EventIn {
            event_id: raw.get_field("event_id").ok().flatten()?,
            sender: raw.get_field("sender").ok().flatten()?,
            event_type,
            state_key: None,
            origin_server_ts: raw
                .get_field("origin_server_ts")
                .ok()
                .flatten()
                .unwrap_or(0),
            content: raw
                .get_field("content")
                .ok()
                .flatten()
                .unwrap_or(Value::Null),
            encryption: event_encryption(encryption_info),
        })),
        _ => None,
    }
}

/// Forwards redactions and the events of `event_types` to the sink. The
/// handler runs on the sync task; the sink only enqueues.
fn register_timeline_receiver(
    room: &Room,
    sink: Arc<dyn RoomSink>,
    event_types: Vec<String>,
) -> matrix_sdk::event_handler::EventHandlerHandle {
    let event_types = Arc::new(event_types);
    room.add_event_handler(
        move |event: Raw<AnySyncMessageLikeEvent>, encryption_info: Option<EncryptionInfo>| {
            let sink = sink.clone();
            let event_types = event_types.clone();
            async move {
                match timeline_ingest_from_raw(&event, encryption_info.as_ref(), &event_types) {
                    Some(TimelineIngest::Event(event)) => sink.on_timeline_events(vec![event]),
                    Some(TimelineIngest::Redacted { event_id }) => sink.on_redaction(event_id),
                    None => {}
                }
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use matrix_sdk::ruma::events::AnySyncTimelineEvent;

    use super::*;

    fn wanted() -> Vec<String> {
        vec![
            "io.element.call.reaction".to_owned(),
            "m.reaction".to_owned(),
        ]
    }

    fn raw_event(json: &str) -> Raw<AnySyncTimelineEvent> {
        Raw::from_json_string(json.to_owned()).expect("valid json")
    }

    #[test]
    fn a_reaction_event_is_read_into_the_inbound_dto() {
        let raw = raw_event(
            r#"{
                "type": "io.element.call.reaction",
                "event_id": "$reaction",
                "sender": "@bob:example.org",
                "origin_server_ts": 1234,
                "content": {
                    "m.relates_to": { "rel_type": "m.reference", "event_id": "$member" },
                    "emoji": "👏",
                    "name": "clapping"
                }
            }"#,
        );
        let Some(TimelineIngest::Event(event)) = timeline_ingest_from_raw(&raw, None, &wanted())
        else {
            panic!("a reaction must be read as an event");
        };
        assert_eq!(event.event_id, "$reaction");
        assert_eq!(event.sender, "@bob:example.org");
        assert_eq!(event.event_type, "io.element.call.reaction");
        assert_eq!(event.origin_server_ts, 1234);
        assert_eq!(event.encryption, EventEncryption::Cleartext);
        assert_eq!(event.content["emoji"], "👏");
        assert_eq!(event.content["m.relates_to"]["event_id"], "$member");
    }

    #[test]
    fn a_redaction_names_its_target_in_either_room_version_shape() {
        let pre_v11 = raw_event(
            r#"{
                "type": "m.room.redaction",
                "event_id": "$redaction",
                "sender": "@bob:example.org",
                "origin_server_ts": 6,
                "redacts": "$hand",
                "content": {}
            }"#,
        );
        assert!(matches!(
            timeline_ingest_from_raw(&pre_v11, None, &wanted()),
            Some(TimelineIngest::Redacted { event_id }) if event_id == "$hand"
        ));

        let v11 = raw_event(
            r#"{
                "type": "m.room.redaction",
                "event_id": "$redaction",
                "sender": "@bob:example.org",
                "origin_server_ts": 6,
                "content": { "redacts": "$hand" }
            }"#,
        );
        assert!(matches!(
            timeline_ingest_from_raw(&v11, None, &wanted()),
            Some(TimelineIngest::Redacted { event_id }) if event_id == "$hand"
        ));
    }

    #[test]
    fn other_message_like_events_are_not_forwarded() {
        let message = raw_event(
            r#"{
                "type": "m.room.message",
                "event_id": "$msg",
                "sender": "@bob:example.org",
                "origin_server_ts": 7,
                "content": { "msgtype": "m.text", "body": "🖐️" }
            }"#,
        );
        assert!(timeline_ingest_from_raw(&message, None, &wanted()).is_none());
    }

    #[test]
    fn an_application_that_wants_no_timeline_gets_only_redactions() {
        let hand = raw_event(
            r#"{
                "type": "m.reaction",
                "event_id": "$hand",
                "sender": "@bob:example.org",
                "origin_server_ts": 5,
                "content": {}
            }"#,
        );
        assert!(timeline_ingest_from_raw(&hand, None, &[]).is_none());
    }
}
