// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Feeds the manager from a `MatrixBackend`: subscribes to what a room needs
//! in its compatibility mode, seeds room state before membership, funnels the
//! pre-2026 dialects, and forwards timeline events, redactions, relations and
//! media keys. The one place Matrix becomes core input, on every binding.
//!
//! Sinks only enqueue; `run()` does the work under the manager lock and is a
//! plain future the binding spawns where its background work already runs.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use matrix_rtc_core::{
    ApplicationIntake, BackendError, EventEncryption, EventIn, KEY_MESSAGE_TYPE, KeyOrigin,
    MatrixBackend, RawSlotEvent, RawTimelineEvent, ReceivedEncryptionKey, RoomSink, RoomSubjects,
    SLOT_EVENT_TYPE, Subscription, ToDeviceMessageIn, ToDeviceSink,
};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{Mutex, mpsc, watch};

use crate::compat::ingest::{
    LegacyStateMemberEventIn, RawMemberEventIn, merge_current_membership, parse_legacy_key,
};
use crate::compat::{ElementCallCompat, LEGACY_KEY_EVENT_TYPE, STATE_MEMBER_EVENT_TYPE};

#[cfg(test)]
mod tests;

const MEMBER_EVENT_TYPES: [&str; 2] = ["m.rtc.member", "org.matrix.msc4143.rtc.member"];
const SLOT_EVENT_TYPES: [&str; 2] = [SLOT_EVENT_TYPE, "org.matrix.msc4143.rtc.slot"];
const KEY_EVENT_TYPES: [&str; 3] = [
    "m.rtc.encryption_key",
    KEY_MESSAGE_TYPE,
    LEGACY_KEY_EVENT_TYPE,
];

/// How a room is attached.
#[derive(Clone, Debug, Default)]
pub struct AttachOptions {
    pub element_call_compat: ElementCallCompat,
}

/// The compatibility mode of every attached room, shared between the room
/// feeders that register it and the session feeder that binds legacy keys by
/// it.
#[derive(Clone, Default)]
pub struct RoomModes(Arc<StdMutex<HashMap<String, ElementCallCompat>>>);

impl RoomModes {
    pub fn mode(&self, room_id: &str) -> ElementCallCompat {
        self.0
            .lock()
            .ok()
            .and_then(|modes| modes.get(room_id).copied())
            .unwrap_or_default()
    }

    pub fn is_attached(&self, room_id: &str) -> bool {
        self.0
            .lock()
            .map(|modes| modes.contains_key(room_id))
            .unwrap_or(false)
    }

    fn set(&self, room_id: &str, mode: ElementCallCompat) {
        if let Ok(mut modes) = self.0.lock() {
            modes.insert(room_id.to_owned(), mode);
        }
    }

    fn remove(&self, room_id: &str) {
        if let Ok(mut modes) = self.0.lock() {
            modes.remove(room_id);
        }
    }
}

/// What a room needs in `mode`: the slot state only where the generation has
/// slots, the pre-sticky membership state only in that generation.
pub fn subjects_for(mode: ElementCallCompat, timeline_event_types: Vec<String>) -> RoomSubjects {
    let state_event_types = if mode.reads_state_membership() {
        vec![STATE_MEMBER_EVENT_TYPE.to_owned()]
    } else {
        SLOT_EVENT_TYPES.iter().map(|t| (*t).to_owned()).collect()
    };
    RoomSubjects {
        state_event_types,
        timeline_event_types,
    }
}

/// The `EventOrigin` of a message-like event, from what the client reported.
pub fn event_origin(encryption: &EventEncryption) -> matrix_rtc_core::EventOrigin {
    match encryption {
        EventEncryption::Cleartext => matrix_rtc_core::EventOrigin::Cleartext,
        EventEncryption::Encrypted {
            sender_device_id, ..
        } => matrix_rtc_core::EventOrigin::encrypted(sender_device_id.clone()),
    }
}

/// The `KeyOrigin` of a to-device message. A cross-signing status the client
/// did not report counts as not cross-signed.
pub fn key_origin(encryption: &EventEncryption, sender: &str) -> KeyOrigin {
    match encryption {
        EventEncryption::Cleartext => KeyOrigin::Cleartext,
        EventEncryption::Encrypted {
            sender_device_id,
            sender_cross_signed,
        } => KeyOrigin::Encrypted {
            sender_user_id: sender.to_owned(),
            sender_device_id: sender_device_id.clone(),
            sender_is_cross_signed: sender_cross_signed.unwrap_or(false),
        },
    }
}

fn to_timeline_event(room_id: &str, event: EventIn) -> RawTimelineEvent {
    RawTimelineEvent {
        room_id: room_id.to_owned(),
        event_id: event.event_id,
        sender: event.sender,
        origin: event_origin(&event.encryption),
        event_type: event.event_type,
        origin_server_ts: event.origin_server_ts,
        content: event.content,
    }
}

fn to_member_event_in(event: EventIn) -> RawMemberEventIn {
    RawMemberEventIn {
        event_id: Some(event.event_id),
        sender: event.sender,
        sender_device_id: event.encryption.sender_device_id().map(str::to_owned),
        was_encrypted: Some(event.encryption.was_encrypted()),
        event_type: event.event_type,
        content: event.content,
    }
}

fn to_legacy_state_member_event_in(event: EventIn) -> Option<LegacyStateMemberEventIn> {
    Some(LegacyStateMemberEventIn {
        event_id: Some(event.event_id),
        sender: event.sender,
        state_key: event.state_key?,
        origin_server_ts: event.origin_server_ts,
        content: event.content,
    })
}

/// One slot's state event under each spelling of the slot type.
#[derive(Default)]
struct SlotEvents {
    stable: Option<EventIn>,
    unstable: Option<EventIn>,
}

impl SlotEvents {
    /// The stable event where the room has one, else the unstable one: never
    /// both.
    fn current(&self) -> Option<&EventIn> {
        self.stable.as_ref().or(self.unstable.as_ref())
    }

    fn of_type(&mut self, stable: bool) -> &mut Option<EventIn> {
        if stable {
            &mut self.stable
        } else {
            &mut self.unstable
        }
    }
}

/// Replaces every slot's event of one spelling with `events`, the room's
/// complete set under that spelling; the other spelling's events are kept.
fn replace_slot_events(
    slots: &mut HashMap<String, SlotEvents>,
    stable: bool,
    events: Vec<EventIn>,
) {
    for slot in slots.values_mut() {
        *slot.of_type(stable) = None;
    }
    for event in events {
        let Some(slot_id) = event.state_key.clone() else {
            continue;
        };
        *slots.entry(slot_id).or_default().of_type(stable) = Some(event);
    }
    slots.retain(|_, slot| slot.current().is_some());
}

/// An unparseable slot content resolves closed rather than vanishing: an
/// unreadable slot is not an open one.
fn to_slot_event(room_id: &str, event: EventIn) -> Option<RawSlotEvent> {
    Some(RawSlotEvent {
        room_id: room_id.to_owned(),
        slot_id: event.state_key?,
        content: serde_json::from_value(event.content).unwrap_or_default(),
    })
}

enum RoomInput {
    Sticky(Vec<EventIn>),
    State(String, Vec<EventIn>),
    Members(Vec<String>),
    Encryption(bool),
    Timeline(Vec<EventIn>),
    Redaction(String),
    Stop,
}

struct ChannelRoomSink {
    tx: mpsc::UnboundedSender<RoomInput>,
}

impl RoomSink for ChannelRoomSink {
    fn on_sticky_events(&self, events: Vec<EventIn>) {
        let _ = self.tx.send(RoomInput::Sticky(events));
    }

    fn on_state_events(&self, event_type: String, events: Vec<EventIn>) {
        let _ = self.tx.send(RoomInput::State(event_type, events));
    }

    fn on_joined_members(&self, user_ids: Vec<String>) {
        let _ = self.tx.send(RoomInput::Members(user_ids));
    }

    fn on_encryption(&self, encrypted: bool) {
        let _ = self.tx.send(RoomInput::Encryption(encrypted));
    }

    fn on_timeline_events(&self, events: Vec<EventIn>) {
        let _ = self.tx.send(RoomInput::Timeline(events));
    }

    fn on_redaction(&self, event_id: String) {
        let _ = self.tx.send(RoomInput::Redaction(event_id));
    }
}

/// A room the library is feeding. Dropping it does nothing; call
/// [`detach`](Self::detach).
pub struct RoomAttachment {
    room_id: String,
    subscription: Arc<dyn Subscription>,
    stop: mpsc::UnboundedSender<RoomInput>,
    seeded: watch::Receiver<bool>,
    modes: RoomModes,
}

impl RoomAttachment {
    pub fn room_id(&self) -> &str {
        &self.room_id
    }

    /// Resolves once the current sets have been applied, so a join issued
    /// afterwards sees them. Also resolves if the feeder stops first.
    pub async fn seeded(&self) {
        let mut seeded = self.seeded.clone();
        while !*seeded.borrow_and_update() {
            if seeded.changed().await.is_err() {
                return;
            }
        }
    }

    pub fn is_seeded(&self) -> bool {
        *self.seeded.borrow()
    }

    /// Ends the subscription; nothing delivered afterwards is applied.
    pub fn detach(self) {
        log::info!("[{}] detaching", self.room_id);
        self.modes.remove(&self.room_id);
        self.subscription.cancel();
        let _ = self.stop.send(RoomInput::Stop);
    }
}

/// Attaches rooms; see the module docs.
pub struct RoomFeeder;

impl RoomFeeder {
    /// Subscribes to what `room_id` needs in `options.element_call_compat`
    /// and returns the attachment plus the future that applies what arrives.
    /// The caller spawns [`RoomFeederRun::run`].
    pub async fn attach<B, M>(
        backend: Arc<B>,
        manager: Arc<Mutex<M>>,
        modes: RoomModes,
        room_id: String,
        options: AttachOptions,
    ) -> Result<(RoomAttachment, RoomFeederRun<B, M>), BackendError>
    where
        B: MatrixBackend + 'static,
        M: ApplicationIntake<B>,
    {
        let mode = options.element_call_compat;
        let timeline_event_types = manager.lock().await.timeline_event_types();
        let subjects = subjects_for(mode, timeline_event_types);
        log::info!("[{room_id}] attaching in {mode:?} mode: {subjects:?}");

        let (tx, rx) = mpsc::unbounded_channel();
        let sink: Arc<dyn RoomSink> = Arc::new(ChannelRoomSink { tx: tx.clone() });
        modes.set(&room_id, mode);
        let subscription = match backend
            .subscribe_room(room_id.clone(), subjects, sink)
            .await
        {
            Ok(subscription) => subscription,
            Err(error) => {
                modes.remove(&room_id);
                return Err(error);
            }
        };
        let (seeded_tx, seeded_rx) = watch::channel(false);

        let attachment = RoomAttachment {
            room_id: room_id.clone(),
            subscription,
            stop: tx,
            seeded: seeded_rx,
            modes,
        };
        let run = RoomFeederRun {
            backend,
            manager,
            room_id,
            mode,
            rx,
            seeded: seeded_tx,
            state: FeedState::default(),
        };
        Ok((attachment, run))
    }
}

#[derive(Default)]
struct FeedState {
    seen_encryption: bool,
    seen_members: bool,
    seen_slots: bool,
    /// The room's slots by slot id (state key), each under both spellings: a
    /// delivery replaces only its own spelling's events.
    slots: HashMap<String, SlotEvents>,
    /// The latest sticky set, as member events, once one arrived.
    sticky: Option<Vec<RawMemberEventIn>>,
    /// The latest pre-sticky membership state, once one arrived.
    legacy: Option<Vec<LegacyStateMemberEventIn>>,
    membership_dirty: bool,
}

/// The future that applies a room's inputs. Ends on detach.
pub struct RoomFeederRun<B, M> {
    backend: Arc<B>,
    manager: Arc<Mutex<M>>,
    room_id: String,
    mode: ElementCallCompat,
    rx: mpsc::UnboundedReceiver<RoomInput>,
    seeded: watch::Sender<bool>,
    state: FeedState,
}

impl<B, M> RoomFeederRun<B, M>
where
    B: MatrixBackend + 'static,
    M: ApplicationIntake<B>,
{
    pub async fn run(mut self) {
        while let Some(input) = self.rx.recv().await {
            match input {
                RoomInput::Stop => break,
                RoomInput::Encryption(encrypted) => {
                    self.manager
                        .lock()
                        .await
                        .rtc()
                        .on_room_encryption_received(&self.room_id, encrypted)
                        .await;
                    self.state.seen_encryption = true;
                }
                RoomInput::State(event_type, events) => self.on_state(event_type, events).await,
                RoomInput::Members(user_ids) => self.on_members(user_ids).await,
                RoomInput::Sticky(events) => {
                    self.state.sticky = Some(
                        events
                            .into_iter()
                            .filter(|event| MEMBER_EVENT_TYPES.contains(&event.event_type.as_str()))
                            .map(to_member_event_in)
                            .collect(),
                    );
                    self.state.membership_dirty = true;
                }
                RoomInput::Timeline(events) => {
                    let events: Vec<RawTimelineEvent> = events
                        .into_iter()
                        .map(|event| to_timeline_event(&self.room_id, event))
                        .collect();
                    self.manager
                        .lock()
                        .await
                        .on_room_timeline_events(&self.room_id, &events);
                }
                RoomInput::Redaction(event_id) => {
                    self.manager
                        .lock()
                        .await
                        .on_event_redacted(&self.room_id, &event_id);
                }
            }

            if self.state.membership_dirty && self.gate_open() {
                self.apply_membership().await;
            }
        }
        log::info!("[{}] feeder stopped", self.room_id);
    }

    async fn on_state(&mut self, event_type: String, events: Vec<EventIn>) {
        if SLOT_EVENT_TYPES.contains(&event_type.as_str()) {
            replace_slot_events(&mut self.state.slots, event_type == SLOT_EVENT_TYPE, events);
            let slots: Vec<RawSlotEvent> = self
                .state
                .slots
                .values()
                .filter_map(SlotEvents::current)
                .filter_map(|event| to_slot_event(&self.room_id, event.clone()))
                .collect();
            self.manager
                .lock()
                .await
                .rtc()
                .on_room_slots_received(&self.room_id, slots)
                .await;
            self.state.seen_slots = true;
        } else if event_type == STATE_MEMBER_EVENT_TYPE {
            self.state.legacy = Some(
                events
                    .into_iter()
                    .filter_map(to_legacy_state_member_event_in)
                    .collect(),
            );
            self.state.membership_dirty = true;
        } else {
            log::debug!(
                "[{}] ignoring {} state events of a type not asked for: {event_type}",
                self.room_id,
                events.len(),
            );
        }
    }

    /// A joined-members set without the account itself cannot be the room's
    /// current members — the account is in every room it attaches — so it is
    /// a store that has not loaded, and applying it would project everyone
    /// out, us included.
    async fn on_members(&mut self, user_ids: Vec<String>) {
        let own = self.backend.own_user_id();
        if !user_ids.contains(&own) {
            log::warn!(
                "[{}] ignoring a joined-members set of {} that does not contain {own}",
                self.room_id,
                user_ids.len(),
            );
            return;
        }
        self.manager
            .lock()
            .await
            .rtc()
            .on_room_members_received(&self.room_id, user_ids)
            .await;
        self.state.seen_members = true;
    }

    /// Room state before membership: a member is never briefly joined to a
    /// slot the room's state says is closed.
    fn gate_open(&self) -> bool {
        let needs_slots = !self.mode.reads_state_membership();
        self.state.seen_encryption
            && self.state.seen_members
            && (!needs_slots || self.state.seen_slots)
    }

    async fn apply_membership(&mut self) {
        let sticky = self.state.sticky.clone().unwrap_or_default();
        let legacy = self.state.legacy.clone().unwrap_or_default();
        let current = merge_current_membership(&self.room_id, sticky, legacy);
        if let Err(error) = self
            .manager
            .lock()
            .await
            .rtc()
            .set_current_sticky_state(&self.room_id, current)
            .await
        {
            log::warn!(
                "[{}] the current membership was not applied: {error}",
                self.room_id
            );
        }
        self.state.membership_dirty = false;
        if !*self.seeded.borrow() {
            log::info!("[{}] seeded", self.room_id);
            let _ = self.seeded.send(true);
        }
        self.backfill_relations().await;
    }

    /// The relations the application asks for, fetched without holding the
    /// manager lock. A failed fetch is asked for again on the next apply.
    async fn backfill_relations(&self) {
        let requests = self.manager.lock().await.pending_relations(&self.room_id);
        for request in requests {
            match self
                .backend
                .relations(
                    self.room_id.clone(),
                    request.event_id.clone(),
                    request.rel_type.clone(),
                    request.event_type.clone(),
                )
                .await
            {
                Ok(events) => {
                    let events: Vec<RawTimelineEvent> = events
                        .into_iter()
                        .filter(|event| event.event_type == request.event_type)
                        .map(|event| to_timeline_event(&self.room_id, event))
                        .collect();
                    self.manager.lock().await.on_relations_received(
                        &self.room_id,
                        &request.event_id,
                        &events,
                    );
                }
                Err(error) => log::warn!(
                    "[{}] could not fetch the {} relations of {} ({error}); retrying later",
                    self.room_id,
                    request.rel_type,
                    request.event_id,
                ),
            }
        }
    }
}

// ---- To-device keys ----

/// The MSC4143 `m.rtc.encryption_key` content. `format` is not read: the key
/// is always base64.
#[derive(Deserialize)]
struct KeyMessageContent {
    room_id: String,
    member_id: String,
    media_key: MediaKey,
}

#[derive(Deserialize)]
struct MediaKey {
    index: u8,
    key: String,
}

/// A media key from a to-device message, in either generation, or `None` with
/// the reason logged.
pub fn parse_key_message(
    modes: &RoomModes,
    message: ToDeviceMessageIn,
) -> Option<ReceivedEncryptionKey> {
    let origin = key_origin(&message.encryption, &message.sender);
    if message.event_type == LEGACY_KEY_EVENT_TYPE {
        let room_id = message
            .content
            .get("room_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let key = parse_legacy_key(
            modes.mode(room_id),
            &message.sender,
            message.encryption.sender_device_id(),
            &message.content,
        )?;
        return Some(ReceivedEncryptionKey {
            origin,
            room_id: key.room_id,
            member_id: key.member_id,
            key_b64: key.key_b64,
            key_index: key.key_index,
        });
    }

    let content: KeyMessageContent = serde_json::from_value(message.content)
        .inspect_err(|error| {
            log::warn!(
                "ignoring a {} from {} whose content does not parse: {error}",
                message.event_type,
                message.sender,
            );
        })
        .ok()?;
    Some(ReceivedEncryptionKey {
        origin,
        room_id: content.room_id,
        member_id: content.member_id,
        key_b64: content.media_key.key,
        key_index: content.media_key.index,
    })
}

struct ChannelToDeviceSink {
    tx: mpsc::UnboundedSender<Option<ToDeviceMessageIn>>,
}

impl ToDeviceSink for ChannelToDeviceSink {
    fn on_to_device_message(&self, message: ToDeviceMessageIn) {
        let _ = self.tx.send(Some(message));
    }
}

/// The session-wide to-device subscription. Dropping it does nothing; call
/// [`stop`](Self::stop).
pub struct SessionFeeder {
    subscription: Arc<dyn Subscription>,
    stop: mpsc::UnboundedSender<Option<ToDeviceMessageIn>>,
}

impl SessionFeeder {
    /// Subscribes to media keys of both generations and returns the future
    /// that routes them to the manager. The caller spawns
    /// [`SessionFeederRun::run`].
    pub async fn start<B, M>(
        backend: Arc<B>,
        manager: Arc<Mutex<M>>,
        modes: RoomModes,
    ) -> Result<(SessionFeeder, SessionFeederRun<B, M>), BackendError>
    where
        B: MatrixBackend + 'static,
        M: ApplicationIntake<B>,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        let sink: Arc<dyn ToDeviceSink> = Arc::new(ChannelToDeviceSink { tx: tx.clone() });
        let event_types = KEY_EVENT_TYPES.iter().map(|t| (*t).to_owned()).collect();
        let subscription = backend.subscribe_to_device(event_types, sink).await?;
        Ok((
            SessionFeeder {
                subscription,
                stop: tx,
            },
            SessionFeederRun {
                manager,
                modes,
                rx,
                _backend: backend,
            },
        ))
    }

    pub fn stop(self) {
        self.subscription.cancel();
        let _ = self.stop.send(None);
    }
}

/// The future that routes media keys. Ends on stop.
pub struct SessionFeederRun<B, M> {
    manager: Arc<Mutex<M>>,
    modes: RoomModes,
    rx: mpsc::UnboundedReceiver<Option<ToDeviceMessageIn>>,
    _backend: Arc<B>,
}

impl<B, M> SessionFeederRun<B, M>
where
    B: MatrixBackend + 'static,
    M: ApplicationIntake<B>,
{
    pub async fn run(mut self) {
        while let Some(Some(message)) = self.rx.recv().await {
            if !KEY_EVENT_TYPES.contains(&message.event_type.as_str()) {
                continue;
            }
            let Some(key) = parse_key_message(&self.modes, message) else {
                continue;
            };
            if !self.modes.is_attached(&key.room_id) {
                log::debug!(
                    "[{}] dropping a media key for a room that is not attached",
                    key.room_id
                );
                continue;
            }
            if let Err(error) = self
                .manager
                .lock()
                .await
                .rtc()
                .receive_encryption_key(key)
                .await
            {
                log::warn!("a media key was rejected: {error}");
            }
        }
    }
}
