// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Feeds one room's state from a `MatrixBackend`: subscribes to what the room
//! needs in its compatibility mode, seeds room state before membership,
//! funnels the pre-2026 dialects, and forwards timeline events, redactions and
//! relations. Media keys arrive on one to-device subscription per client and
//! are routed to the room they name through the [`RoomRegistry`]. The one place
//! Matrix becomes core input, on every binding.
//!
//! Sinks only enqueue; `run()` does the work under the room's lock and is a
//! plain future the binding spawns where its background work already runs.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, Weak};

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

/// The rooms a client holds, by room id: each one's compatibility mode, which
/// binds a legacy media key, and a weak handle the to-device feeder routes keys
/// to. Holding no strong reference is what lets a dropped room stop receiving
/// keys without anyone unregistering it first.
pub struct RoomRegistry<M>(Arc<StdMutex<HashMap<String, RegisteredRoom<M>>>>);

struct RegisteredRoom<M> {
    mode: ElementCallCompat,
    room: Weak<Mutex<M>>,
}

impl<M> Clone for RoomRegistry<M> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<M> Default for RoomRegistry<M> {
    fn default() -> Self {
        Self(Arc::default())
    }
}

/// Registering a room the registry already holds a live entry for.
#[derive(Debug, thiserror::Error)]
#[error("{0} is already open")]
pub struct RoomAlreadyOpen(pub String);

impl<M> RoomRegistry<M> {
    /// Refused while `room_id` has a live entry; an entry whose room was
    /// dropped without unregistering is replaced.
    pub fn register(
        &self,
        room_id: &str,
        mode: ElementCallCompat,
        room: &Arc<Mutex<M>>,
    ) -> Result<(), RoomAlreadyOpen> {
        let mut rooms = self.lock();
        if rooms
            .get(room_id)
            .is_some_and(|entry| entry.room.strong_count() > 0)
        {
            return Err(RoomAlreadyOpen(room_id.to_owned()));
        }
        rooms.insert(
            room_id.to_owned(),
            RegisteredRoom {
                mode,
                room: Arc::downgrade(room),
            },
        );
        Ok(())
    }

    /// Returns whether the registry is empty afterwards.
    pub fn unregister(&self, room_id: &str) -> bool {
        let mut rooms = self.lock();
        rooms.remove(room_id);
        rooms.is_empty()
    }

    /// The mode `room_id` was opened in, or `Off` for a room not held.
    pub fn mode(&self, room_id: &str) -> ElementCallCompat {
        self.lock()
            .get(room_id)
            .map(|entry| entry.mode)
            .unwrap_or_default()
    }

    /// The live room for `room_id`, if one is held.
    pub fn room(&self, room_id: &str) -> Option<Arc<Mutex<M>>> {
        self.lock().get(room_id)?.room.upgrade()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Never held across an await, so a poisoned lock only means a panic
    /// elsewhere mid-insert; the map itself is still consistent.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, RegisteredRoom<M>>> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
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

/// A room the library is feeding. Dropping it detaches: the subscription
/// ends, and nothing delivered afterwards is applied.
pub struct RoomAttachment {
    room_id: String,
    subscription: Arc<dyn Subscription>,
    stop: mpsc::UnboundedSender<RoomInput>,
    seeded: watch::Receiver<bool>,
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
}

impl Drop for RoomAttachment {
    fn drop(&mut self) {
        log::info!("[{}] detaching", self.room_id);
        self.subscription.cancel();
        let _ = self.stop.send(RoomInput::Stop);
    }
}

/// Attaches rooms; see the module docs.
pub struct RoomFeeder;

impl RoomFeeder {
    /// Subscribes to what `room` needs in `mode` and returns the attachment
    /// plus the future that applies what arrives. The caller spawns
    /// [`RoomFeederRun::run`].
    pub async fn attach<B, M>(
        backend: Arc<B>,
        room: Arc<Mutex<M>>,
        mode: ElementCallCompat,
    ) -> Result<(RoomAttachment, RoomFeederRun<B, M>), BackendError>
    where
        B: MatrixBackend + 'static,
        M: ApplicationIntake<B>,
    {
        let (room_id, timeline_event_types) = {
            let mut room = room.lock().await;
            (room.rtc().room_id().to_owned(), room.timeline_event_types())
        };
        let subjects = subjects_for(mode, timeline_event_types);
        log::info!("[{room_id}] attaching in {mode:?} mode: {subjects:?}");

        let (tx, rx) = mpsc::unbounded_channel();
        let sink: Arc<dyn RoomSink> = Arc::new(ChannelRoomSink { tx: tx.clone() });
        let subscription = backend
            .subscribe_room(room_id.clone(), subjects, sink)
            .await?;
        let (seeded_tx, seeded_rx) = watch::channel(false);

        let attachment = RoomAttachment {
            room_id: room_id.clone(),
            subscription,
            stop: tx,
            seeded: seeded_rx,
        };
        let run = RoomFeederRun {
            backend,
            room,
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

/// The future that applies a room's inputs. Ends when the attachment drops.
pub struct RoomFeederRun<B, M> {
    backend: Arc<B>,
    room: Arc<Mutex<M>>,
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
                    self.room
                        .lock()
                        .await
                        .rtc()
                        .on_encryption_received(encrypted)
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
                    self.room.lock().await.on_timeline_events(&events);
                }
                RoomInput::Redaction(event_id) => {
                    self.room.lock().await.on_event_redacted(&event_id);
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
            self.room.lock().await.rtc().on_slots_received(slots).await;
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
        self.room
            .lock()
            .await
            .rtc()
            .on_members_received(user_ids)
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
            .room
            .lock()
            .await
            .rtc()
            .set_current_sticky_state(current)
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
    /// room's lock. A failed fetch is asked for again on the next apply.
    async fn backfill_relations(&self) {
        let requests = self.room.lock().await.pending_relations();
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
                    self.room
                        .lock()
                        .await
                        .on_relations_received(&request.event_id, &events);
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
    mode_of: impl Fn(&str) -> ElementCallCompat,
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
            mode_of(room_id),
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

/// The client-wide to-device subscription. Dropping it stops it.
pub struct ToDeviceFeeder {
    subscription: Arc<dyn Subscription>,
    stop: mpsc::UnboundedSender<Option<ToDeviceMessageIn>>,
}

impl ToDeviceFeeder {
    /// Subscribes to media keys of both generations and returns the future
    /// that routes each to the room it names. The caller spawns
    /// [`ToDeviceFeederRun::run`].
    pub async fn start<B, M>(
        backend: Arc<B>,
        registry: RoomRegistry<M>,
    ) -> Result<(ToDeviceFeeder, ToDeviceFeederRun<B, M>), BackendError>
    where
        B: MatrixBackend + 'static,
        M: ApplicationIntake<B>,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        let sink: Arc<dyn ToDeviceSink> = Arc::new(ChannelToDeviceSink { tx: tx.clone() });
        let event_types = KEY_EVENT_TYPES.iter().map(|t| (*t).to_owned()).collect();
        let subscription = backend.subscribe_to_device(event_types, sink).await?;
        Ok((
            ToDeviceFeeder {
                subscription,
                stop: tx,
            },
            ToDeviceFeederRun {
                registry,
                rx,
                _backend: backend,
            },
        ))
    }

    pub fn stop(self) {
        drop(self);
    }
}

impl Drop for ToDeviceFeeder {
    fn drop(&mut self) {
        self.subscription.cancel();
        let _ = self.stop.send(None);
    }
}

/// The future that routes media keys. Ends on stop.
pub struct ToDeviceFeederRun<B, M> {
    registry: RoomRegistry<M>,
    rx: mpsc::UnboundedReceiver<Option<ToDeviceMessageIn>>,
    _backend: Arc<B>,
}

impl<B, M> ToDeviceFeederRun<B, M>
where
    B: MatrixBackend + 'static,
    M: ApplicationIntake<B>,
{
    pub async fn run(mut self) {
        while let Some(Some(message)) = self.rx.recv().await {
            if !KEY_EVENT_TYPES.contains(&message.event_type.as_str()) {
                continue;
            }
            let Some(key) = parse_key_message(|room_id| self.registry.mode(room_id), message)
            else {
                continue;
            };
            // A key for a room no live room object holds is dropped: nothing
            // could use it, and keeping it would be state spanning rooms.
            let Some(room) = self.registry.room(&key.room_id) else {
                log::debug!(
                    "[{}] dropping a media key for a room that is not open",
                    key.room_id
                );
                continue;
            };
            if let Err(error) = room.lock().await.rtc().receive_encryption_key(key).await {
                log::warn!("a media key was rejected: {error}");
            }
        }
        log::info!("to-device feeder stopped");
    }
}
