// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! One room's MatrixRTC state: its slots, its room state and a
//! [`SlotSession`] per slot in use.
//!
//! Everything here is scoped to the one room the [`BaseRtcRoom`] was created for;
//! inbound events and keys naming another room are dropped. Nothing in the
//! core spans rooms — routing to-device keys to the right room is the host's.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::watch;

use crate::encryption::types::ReceivedEncryptionKey;
use crate::encryption::{EncryptionKeySignalHandler, RtcIdentityMapper};
use crate::error::{CommandError, JoinError, LeaveError};
use crate::host::backend::MatrixBackend;
use crate::host::event::{EventConversionError, RawStickyEvent};
use crate::join::{JoinSessionParams, LeaveSessionParams};
use crate::membership_listener::{MembershipListener, MembershipListeners, MembershipScope};
use crate::session::{JoinedMembership, RtcMembershipEvent, SlotSession};
use crate::slot::{
    RawSlotEvent, RawSlotEventContent, RoomEncryption, SLOT_EVENT_TYPE, SlotEncryption, SlotState,
};

/// One room's RTC state and the slot sessions in it.
pub struct BaseRtcRoom<T: MatrixBackend> {
    room_id: String,
    /// One per slot in use, keyed by slot id.
    sessions: HashMap<String, SlotSession<T>>,
    backend: Option<Arc<T>>,
    /// Slot events by slot id, kept unresolved because resolving them also
    /// depends on the room's encryption state, which can arrive later or
    /// change. Held here as well as on the sessions so that state arriving
    /// before a session exists still applies when one is created.
    slots: HashMap<String, RawSlotEvent>,
    /// Whether the room's `m.rtc.slot` state has been supplied. Once it has, a
    /// slot with no entry in `slots` is closed, as opposed to unknown.
    slot_state_known: bool,
    /// Users joined to the room, when the host supplies them.
    room_members: Option<HashSet<String>>,
    room_encryption: Option<RoomEncryption>,
    membership_listeners: MembershipListeners,
}

impl<T: MatrixBackend + 'static> BaseRtcRoom<T> {
    /// Creates an empty room without a backend.
    pub fn new(room_id: impl Into<String>) -> Self {
        Self {
            room_id: room_id.into(),
            sessions: HashMap::new(),
            backend: None,
            slots: HashMap::new(),
            slot_state_known: false,
            room_members: None,
            room_encryption: None,
            membership_listeners: MembershipListeners::default(),
        }
    }

    /// Creates an empty room with a backend.
    pub fn with_backend(room_id: impl Into<String>, backend: Arc<T>) -> Self {
        let mut room = Self::new(room_id);
        room.backend = Some(backend);
        room
    }

    /// The room this object is scoped to.
    pub fn room_id(&self) -> &str {
        &self.room_id
    }

    /// Sets the backend for this room.
    pub fn set_backend(&mut self, backend: Arc<T>) {
        self.backend = Some(backend);
    }

    /// For applications, which send through the same backend as the core.
    pub fn backend(&self) -> Option<&Arc<T>> {
        self.backend.as_ref()
    }

    /// Returns true if this room has a backend configured.
    pub fn has_backend(&self) -> bool {
        self.backend.is_some()
    }

    /// Replays every existing slot's joined memberships to it first. Listeners
    /// cannot be removed.
    pub fn add_membership_listener(&mut self, listener: Arc<dyn MembershipListener>) {
        for (slot_id, session) in &self.sessions {
            listener.on_memberships(slot_id, session.members());
        }
        self.membership_listeners.add(listener);
    }

    /// Joins the slot named by `params`.
    ///
    /// Refused while that slot is already joined through this room, and while
    /// the room's slot state is known and the slot is not open.
    ///
    /// Returns the event id of the membership event this join sent; see
    /// [`SlotSession::join`].
    pub async fn join(&mut self, params: JoinSessionParams) -> Result<String, JoinError> {
        let backend = self
            .backend
            .as_ref()
            .ok_or_else(|| {
                log::warn!(
                    "[{}/{}] join rejected: the room has no backend",
                    self.room_id,
                    params.slot_id,
                );
                JoinError::CommandError(CommandError::from_message("no backend configured"))
            })?
            .clone();

        // Only where the room's slot state is known; an unsupplied condition is
        // not enforced, here as in the projection.
        if self
            .slot_state(&params.slot_id)
            .is_some_and(|state| !state.is_open())
        {
            log::warn!(
                "[{}/{}] join rejected: the slot is not open",
                self.room_id,
                params.slot_id,
            );
            return Err(JoinError::SlotClosed {
                slot_id: params.slot_id,
            });
        }

        if let Some(member_id) = self.own_member_id(&params.slot_id) {
            log::warn!(
                "[{}/{}] join rejected: already joined as {member_id}",
                self.room_id,
                params.slot_id,
            );
            return Err(JoinError::AlreadyJoined(member_id));
        }

        let session = self.session_for_slot(&params.slot_id);
        if !session.has_backend() {
            session.set_backend(backend);
        }

        session.join(params).await
    }

    /// Leaves one slot.
    ///
    /// The slot session is **kept**, not removed, and stays usable for a later
    /// join in the same process. See [`SlotSession::leave`] for why, and for
    /// what it does and does not clear — a rejoin therefore starts with the
    /// previous call's joined memberships in place, which
    /// [`SlotSession::join`] accounts for.
    pub async fn leave(
        &mut self,
        slot_id: &str,
        params: LeaveSessionParams,
    ) -> Result<(), LeaveError> {
        let session = self.sessions.get_mut(slot_id).ok_or_else(|| {
            log::warn!(
                "[{}/{slot_id}] leave rejected: no such session",
                self.room_id
            );
            LeaveError::CommandError(CommandError::from_message("session not found"))
        })?;

        session.leave(params).await
    }

    /// Applies the **complete** current sticky state of the room.
    ///
    /// Replaces, rather than merges: a member absent from `events` is gone.
    ///
    /// That is what lets a host hand over what it has without doing any
    /// resolution of its own. An MSC4354 sticky entry lapses when its owner
    /// stops refreshing it — a crashed client — and the entry then simply
    /// disappears from the map. A host that only ever sees the current state
    /// (which is what matrix-sdk-ffi delivers: it collapses the SDK's delta to a
    /// snapshot before it crosses the boundary) has no way to say "this one
    /// expired" beyond its absence. Diffing here means every host does not have
    /// to.
    ///
    /// This is the only way membership reaches the core; there is deliberately
    /// no delta entry point. A delta carried nothing extra — the core flattens
    /// every removal to a plain leave, and an explicit leave arrives inside the
    /// current state anyway, as a leave-shaped sticky replacing the join under
    /// the same key.
    ///
    /// Safe to call repeatedly: re-assert the full state whenever the host's
    /// sticky map changes. Passing an empty list clears the room. An event
    /// naming another room is dropped.
    ///
    /// # One call, at most one key rotation
    ///
    /// Everything the state says is applied before the joined memberships are republished, so
    /// a change of any size costs at most one rotation — three people hanging up
    /// together mint one key between them, not three. Key rotation has no
    /// debounce of its own and cannot have one (the core owns no timer), so this
    /// batching is the only thing standing between a busy call and a key per
    /// event.
    ///
    /// Two consequences for hosts: pass the state **whole**, and do not fan one
    /// snapshot out into several calls. Splitting it is not merely slower — each
    /// call is a complete state, so a partial one reads as everybody missing from
    /// it having left, which rotates the key and re-sends it to every remaining
    /// member.
    pub async fn set_current_sticky_state(
        &mut self,
        events: impl IntoIterator<Item = RawStickyEvent>,
    ) -> Result<(), EventConversionError> {
        let mut batches: HashMap<String, Vec<RtcMembershipEvent>> = HashMap::new();

        for event in events {
            if event.room_id != self.room_id {
                log::warn!(
                    "[{}] dropping a sticky event for another room ({})",
                    self.room_id,
                    event.room_id,
                );
                continue;
            }

            let Some(event) = self.try_convert_membership_event(event)? else {
                continue;
            };

            let slot_id = match &event {
                RtcMembershipEvent::Joined(joined) => joined.slot_id.clone(),
                RtcMembershipEvent::Left(left) => left.slot_id.clone(),
            };

            batches.entry(slot_id).or_default().push(event);
        }

        // Every slot we already track, not just the ones named in `events`: a
        // slot whose last member expired disappears from the payload entirely,
        // and leaving it untouched is precisely the ghost this call exists to
        // prevent. Such a slot gets an empty set, which clears it.
        for slot_id in self.sessions.keys() {
            batches.entry(slot_id.clone()).or_default();
        }

        log::debug!(
            "[{}] current sticky state routed to {} session(s): {}",
            self.room_id,
            batches.len(),
            describe_batches(&batches),
        );

        for (slot_id, batch) in batches {
            self.session_for_slot(&slot_id)
                .set_current_state(batch)
                .await;
        }

        Ok(())
    }

    /// Restarts the keep-alive delayed-leave for one slot.
    ///
    /// Call periodically (e.g. every 15 s) while joined so the dead man's switch
    /// timer keeps getting pushed back. Returns `false` if the slot is not
    /// joined.
    pub async fn heartbeat(&mut self, slot_id: &str) -> bool {
        match self.sessions.get_mut(slot_id) {
            Some(session) => session.heartbeat().await,
            None => false,
        }
    }

    /// Returns the member count of one slot.
    pub fn member_count(&self, slot_id: &str) -> Option<usize> {
        self.sessions.get(slot_id).map(SlotSession::member_count)
    }

    /// Subscribes to membership snapshots of one slot (see
    /// [`SlotSession::subscribe_membership_snapshots`]), or `None` if no
    /// session exists for it yet.
    pub fn subscribe_membership_snapshots(
        &self,
        slot_id: &str,
    ) -> Option<watch::Receiver<Vec<JoinedMembership>>> {
        self.sessions
            .get(slot_id)
            .map(SlotSession::subscribe_membership_snapshots)
    }

    /// Subscribes to membership snapshots of one slot, creating its session if
    /// nobody has been seen in it yet, so a slot can be observed before anyone
    /// — us included — joins it.
    pub fn observe_slot(&mut self, slot_id: &str) -> watch::Receiver<Vec<JoinedMembership>> {
        self.session_for_slot(slot_id)
            .subscribe_membership_snapshots()
    }

    /// Registers a media key signal handler for one slot. Returns `false` if
    /// the slot has no session or has not joined.
    pub fn set_encryption_signal_handler(
        &mut self,
        slot_id: &str,
        handler: Arc<dyn EncryptionKeySignalHandler>,
    ) -> bool {
        self.sessions
            .get_mut(slot_id)
            .is_some_and(|session| session.set_encryption_signal_handler(handler))
    }

    /// Our `member.id` in one slot, or `None` if it is not joined.
    ///
    /// See [`SlotSession::own_member_id`]: it changes on every join, so read
    /// it rather than cache it.
    pub fn own_member_id(&self, slot_id: &str) -> Option<String> {
        self.sessions
            .get(slot_id)
            .and_then(|session| session.own_member_id())
            .map(str::to_owned)
    }

    /// The slots of this room we are currently joined to.
    pub fn joined_slots(&self) -> Vec<String> {
        self.sessions
            .iter()
            .filter(|(_, session)| session.own_member_id().is_some())
            .map(|(slot_id, _)| slot_id.clone())
            .collect()
    }

    /// The event id of our current membership event in one slot, or `None` if
    /// it is not joined.
    ///
    /// See [`SlotSession::own_membership_event_id`]: it moves on every sticky
    /// refresh, so read it at the moment of use.
    pub fn own_membership_event_id(&self, slot_id: &str) -> Option<String> {
        self.sessions
            .get(slot_id)
            .and_then(|session| session.own_membership_event_id())
    }

    /// Re-signals every key one slot already holds to its signal handler.
    ///
    /// Call after installing both the handler and the identity mapper: keys
    /// that arrived before the handler existed were stored but never signalled.
    /// Returns `false` if the slot has no session or has not joined.
    pub async fn replay_encryption_keys(&self, slot_id: &str) -> bool {
        match self.sessions.get(slot_id) {
            Some(session) => session.replay_encryption_keys().await,
            None => false,
        }
    }

    /// When a coalesced key rotation falls due for one slot, if one is owed.
    ///
    /// A consumer with a scheduler drives [`Self::flush_due_key_rotation`] from
    /// this. The deadline is the end of the current key's freshness, which is
    /// *later* than the end of its `delayBeforeUse` — so a consumer that only
    /// reacts to a key coming into use will find nothing due yet and has to come
    /// back at this instant.
    pub fn key_rotation_due_at_ms(&self, slot_id: &str) -> Option<u64> {
        self.sessions
            .get(slot_id)
            .and_then(|session| session.key_rotation_due_at_ms())
    }

    /// Performs a key rotation that was coalesced into a fresh key's window, if one
    /// is owed and the window has closed.
    ///
    /// Membership changes arriving while a rotation is still propagating do not
    /// each mint a key — they are answered by one rotation at the end of the
    /// window (see `EncryptionManager::flush_due_rotation`). Nothing inside the
    /// core can perform it: it holds no timer, so the consumer that *does* enforce
    /// `delayBeforeUse` is the one positioned to call this the moment the window
    /// ends. `matrix-rtc-livekit`'s `MediaKeyBridge` drives it from the same
    /// scheduled wake-up that installs the key.
    ///
    /// A consumer that does not is not left broken, only late: [`Self::heartbeat`]
    /// performs any owed rotation too, so it lands within one heartbeat instead.
    ///
    /// Cheap and idempotent — a no-op unless a rotation is actually due. Returns
    /// `false` if the slot has no session or has not joined.
    pub async fn flush_due_key_rotation(&self, slot_id: &str) -> bool {
        match self.sessions.get(slot_id) {
            Some(session) => session.flush_due_key_rotation().await,
            None => false,
        }
    }

    /// Installs the RTC-backend identity mapper for one slot. Returns `false`
    /// if the slot has no session or has not joined.
    pub fn set_encryption_identity_mapper(
        &mut self,
        slot_id: &str,
        mapper: RtcIdentityMapper,
    ) -> bool {
        self.sessions
            .get_mut(slot_id)
            .is_some_and(|session| session.set_encryption_identity_mapper(mapper))
    }

    /// Routes a media encryption key received from a peer into every slot
    /// session of this room. A key naming another room is dropped.
    ///
    /// The MSC4143 key to-device content carries no `slot_id`, so the key is
    /// fanned out to all slots. This is exact for the common single-slot-per-room
    /// case; multi-slot rooms would receive the key in every slot (harmless —
    /// unmatched keys are buffered/ignored).
    pub async fn receive_encryption_key(
        &self,
        received: ReceivedEncryptionKey,
    ) -> Result<(), CommandError> {
        if received.room_id != self.room_id {
            log::warn!(
                "[{}] dropping a media key for another room ({})",
                self.room_id,
                received.room_id,
            );
            return Ok(());
        }
        for session in self.sessions.values() {
            session.receive_encryption_key(received.clone()).await?;
        }
        Ok(())
    }

    /// Applies the room's `m.rtc.slot` state.
    ///
    /// `slots` must be the room's complete set of `m.rtc.slot` state events:
    /// any slot *not* named by it is taken to be closed. Calling this is what
    /// switches the room from "slot state unknown" (where the MSC4143 open-slot
    /// condition cannot be evaluated, so is not enforced) to enforcing it, so
    /// hosts should call it with whatever they have — an empty list included —
    /// as soon as room state is available, and again on every change. A slot
    /// event naming another room is dropped.
    pub async fn on_slots_received(&mut self, slots: impl IntoIterator<Item = RawSlotEvent>) {
        self.slot_state_known = true;

        // Replace the slots wholesale; a slot that vanished from room state is
        // closed, not merely stale.
        self.slots.clear();
        for slot in slots {
            if slot.room_id != self.room_id {
                log::warn!(
                    "[{}] ignoring a slot event for another room ({})",
                    self.room_id,
                    slot.room_id,
                );
                continue;
            }
            self.slots.insert(slot.slot_id.clone(), slot);
        }
        log::debug!(
            "[{}] room slot state replaced with {} slot(s)",
            self.room_id,
            self.slots.len(),
        );

        self.push_slot_state().await;
    }

    /// Forgets the room's `m.rtc.slot` state, so the open-slot condition stops
    /// being enforced.
    ///
    /// The way back from [`Self::on_slots_received`], and the only one: that
    /// call is otherwise irreversible, because an empty slot list means "no open
    /// slots" rather than "I have nothing to say". This is the second statement,
    /// and it restores the [`SlotKnowledge::Unsupplied`] a room starts in.
    ///
    /// It exists for rooms where the condition is not merely unknown but
    /// *inapplicable* — a MatrixRTC generation older than `m.rtc.slot` itself, in
    /// which no client publishes one and reporting "no slots" would resolve every
    /// session closed and project every member out, the caller included. A host
    /// that simply has not fetched the state yet should say nothing at all rather
    /// than call this.
    ///
    /// [`SlotKnowledge::Unsupplied`]: crate::SlotKnowledge::Unsupplied
    pub async fn forget_slots(&mut self) {
        let known = std::mem::take(&mut self.slot_state_known);
        self.slots.clear();
        if !known {
            return;
        }

        log::info!(
            "[{}] slot state forgotten; the open-slot condition is not enforced",
            self.room_id,
        );
        for session in self.sessions.values_mut() {
            session.forget_slot_state().await;
        }
    }

    /// Reports whether the room is end-to-end encrypted.
    ///
    /// MSC4143 requires RTC encryption in encrypted rooms and forbids it
    /// elsewhere, so this changes how the room's slots resolve. Until a host
    /// calls it neither rule is applied.
    pub async fn on_encryption_received(&mut self, encrypted: bool) {
        let encryption = if encrypted {
            RoomEncryption::Encrypted
        } else {
            RoomEncryption::Unencrypted
        };

        if self.room_encryption.replace(encryption) == Some(encryption) {
            return;
        }

        // Slots already held resolve differently now.
        self.push_slot_state().await;

        for session in self.sessions.values_mut() {
            session.set_room_encryption(encryption).await;
        }
    }

    /// Resolves the room's slots against its current encryption state and
    /// pushes the result to every slot session.
    async fn push_slot_state(&mut self) {
        if !self.slot_state_known {
            log::debug!(
                "[{}] no slot state supplied yet; the open-slot condition stays unenforced",
                self.room_id,
            );
            return;
        }

        let encryption = self.encryption();
        let resolved: HashMap<String, SlotState> = self
            .slots
            .iter()
            .map(|(slot_id, slot)| (slot_id.clone(), slot.resolve(encryption)))
            .collect();

        log::debug!(
            "[{}] slots resolved against encryption={encryption:?}: {}",
            self.room_id,
            resolved
                .iter()
                .map(|(slot_id, state)| format!(
                    "{slot_id}={}",
                    if state.is_open() { "Open" } else { "Closed" },
                ))
                .collect::<Vec<_>>()
                .join(", "),
        );

        for (slot_id, session) in self.sessions.iter_mut() {
            let state = resolved.get(slot_id).cloned().unwrap_or(SlotState::Closed);
            session.set_slot_state(state).await;
        }
    }

    fn encryption(&self) -> RoomEncryption {
        self.room_encryption.unwrap_or_default()
    }

    /// Sets the users currently joined to the room.
    ///
    /// MSC4143 only counts a member event as joined while its sender is still
    /// joined to the room. Until a host calls this, that condition is not
    /// enforced.
    pub async fn on_members_received(&mut self, joined_user_ids: impl IntoIterator<Item = String>) {
        let members: HashSet<String> = joined_user_ids.into_iter().collect();
        self.room_members = Some(members.clone());

        for session in self.sessions.values_mut() {
            session.set_room_members(members.clone()).await;
        }
    }

    /// Opens a slot by sending an `m.rtc.slot` state event.
    ///
    /// The slot id doubles as the state key and MSC4143 requires it to start
    /// with `{application_type}#`, so that is checked here rather than letting
    /// the homeserver accept a slot every client will treat as closed.
    ///
    /// Sending room state usually needs a raised power level; a rejection
    /// surfaces as [`CommandError`].
    pub async fn open_slot(
        &self,
        slot_id: String,
        application_type: String,
        encryption: Option<SlotEncryption>,
    ) -> Result<(), CommandError> {
        if !slot_id.starts_with(&format!("{application_type}#")) {
            return Err(CommandError::from_message(format!(
                "slot id '{slot_id}' does not match application type '{application_type}': \
                 MSC4143 requires the state key to be '{{application_type}}#{{slot}}'"
            )));
        }

        let content = RawSlotEventContent::for_open(application_type, encryption);
        self.send_slot_state(slot_id, content).await
    }

    /// Closes a slot by setting its `m.rtc.slot` status to `closed`.
    ///
    /// Members of the slot become left as soon as the new state is applied.
    pub async fn close_slot(&self, slot_id: String) -> Result<(), CommandError> {
        self.send_slot_state(slot_id, RawSlotEventContent::for_close())
            .await
    }

    async fn send_slot_state(
        &self,
        slot_id: String,
        content: RawSlotEventContent,
    ) -> Result<(), CommandError> {
        let backend = self
            .backend
            .as_ref()
            .ok_or_else(|| CommandError::from_message("no backend configured"))?;

        let content =
            serde_json::to_value(content).expect("m.rtc.slot content is always serializable");

        // The event id goes nowhere: nothing relates to a slot event.
        backend
            .send_state_event(
                self.room_id.clone(),
                SLOT_EVENT_TYPE.to_owned(),
                slot_id,
                content,
            )
            .await
            .map(|_event_id| ())
    }

    /// Everything the room and its slot sessions believe, as JSON.
    ///
    /// Meant to be attached to a bug report or dumped to the log when the joined
    /// memberships look wrong: it answers "which slots exist, what room state do
    /// they have, and why is each candidate in or out" in one shot. Contains no
    /// key material.
    pub fn debug_snapshot(&self) -> serde_json::Value {
        let sessions: serde_json::Map<String, serde_json::Value> = self
            .sessions
            .iter()
            .map(|(slot_id, session)| (slot_id.clone(), session.debug_snapshot()))
            .collect();

        serde_json::json!({
            "room_id": self.room_id,
            "has_backend": self.backend.is_some(),
            "slot_state_known": self.slot_state_known,
            "known_slots": self.slots.keys().collect::<Vec<_>>(),
            "room_encryption": self.room_encryption.map(|encryption| format!("{encryption:?}")),
            "room_members": self.room_members.as_ref().map(HashSet::len),
            "sessions": sessions,
        })
    }

    /// The resolved state of a slot, if the room's slot state has been supplied.
    pub fn slot_state(&self, slot_id: &str) -> Option<SlotState> {
        if !self.slot_state_known {
            return None;
        }
        Some(
            self.slots
                .get(slot_id)
                .map(|slot| slot.resolve(self.encryption()))
                .unwrap_or(SlotState::Closed),
        )
    }

    /// Returns the session for `slot_id`, creating it if needed.
    ///
    /// A newly created session is seeded with whatever room state the room
    /// already holds, so slot state that arrived before the session existed
    /// still governs it. Seeding is synchronous because a session with no
    /// members has nothing to republish.
    fn session_for_slot(&mut self, slot_id: &str) -> &mut SlotSession<T> {
        let encryption = self.encryption();
        let slot = self.slot_state(slot_id);
        let room_members = self.room_members.clone();
        let backend = self.backend.clone();
        let room_id = self.room_id.clone();
        let membership_scope = MembershipScope {
            slot_id: slot_id.to_owned(),
            listeners: self.membership_listeners.clone(),
        };

        self.sessions.entry(slot_id.to_owned()).or_insert_with(|| {
            log::info!(
                "session created [{room_id}/{slot_id}] seeded with slot={} members={:?} encryption={encryption:?}",
                match &slot {
                    Some(state) if state.is_open() => "Open",
                    Some(_) => "Closed",
                    None => "Unsupplied",
                },
                room_members.as_ref().map(HashSet::len),
            );

            let mut session =
                SlotSession::new(room_id, slot_id.to_owned(), backend, membership_scope);
            if let Some(slot) = slot {
                session.seed_slot_state(slot);
            }
            if let Some(room_members) = room_members {
                session.seed_room_members(room_members);
            }
            session.seed_room_encryption(encryption);
            session
        })
    }

    fn try_convert_membership_event(
        &self,
        event: RawStickyEvent,
    ) -> Result<Option<RtcMembershipEvent>, EventConversionError> {
        match event.try_into_membership_event() {
            Ok(event) => Ok(Some(event)),
            // Not an RTC member event at all — the host feeds us its whole
            // sticky map, so this is routine.
            Err(EventConversionError::UnsupportedEventType { .. }) => Ok(None),
            Err(err) => {
                log::warn!(
                    "[{}] dropping a malformed sticky member event: {err}",
                    self.room_id,
                );
                Err(err)
            }
        }
    }
}

/// `slot_id xN` per slot, for the one-line routing summary.
///
/// Which slots a room's sticky events landed in is the first thing to check
/// when the joined memberships look wrong: a typo in `slot_id` silently creates a second,
/// empty session rather than failing.
fn describe_batches(batches: &HashMap<String, Vec<RtcMembershipEvent>>) -> String {
    if batches.is_empty() {
        return "none".to_owned();
    }

    batches
        .iter()
        .map(|(slot_id, batch)| format!("{slot_id} x{}", batch.len()))
        .collect::<Vec<_>>()
        .join(", ")
}
