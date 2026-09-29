// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! [`CallSessionManager`]: an [`RtcSessionManager`] plus everything Element
//! Call does around a session.
//!
//! Composition, not `Deref`: every input that can move a roster or our own
//! membership is shadowed here so the call state (raised hands, the notify
//! decision) is kept in step on the same call, on the caller's task. A future
//! roster input added to the core therefore cannot bypass this layer by
//! accident — it fails to compile until it is forwarded.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use matrix_rtc_core::participation::{
    KeyMap, ParticipationListener, ParticipationSnapshot, SessionMembership, Status,
    TransportWithMembers,
};
use matrix_rtc_core::{
    CommandError, EncryptionKeySignalHandler, EventConversionError, JoinError, JoinSessionParams,
    JoinedMembership, LeaveError, LeaveSessionParams, RawSlotEvent, RawStickyEvent,
    ReceivedEncryptionKey, RtcIdentityMapper, RtcSessionManager, SlotEncryption, SlotState, now_ms,
};
use tokio::sync::{broadcast, watch};

use crate::commands::CallCommandSender;
use crate::notification::{
    NOTIFICATION_EVENT_TYPE, NotifyConfig, build_notification_content,
    notification_sticky_duration_ms,
};
use crate::reactions::{
    ANNOTATION_EVENT_TYPE, REACTION_EVENT_TYPE, RaisedHand, RawTimelineEvent, ReactionError,
    ReactionsConfig, ReactionsState, ReceivedReaction, RelationLookup, build_raised_hand_content,
    build_reaction_content, first_grapheme,
};

/// What a call joins with: the RTC join, plus the call's own options.
#[derive(Clone, Debug)]
pub struct CallJoinParams {
    /// The MSC4143 join.
    pub session: JoinSessionParams,
    /// Ask for an MSC4075 notification to be sent with this join.
    ///
    /// `None` — the default — joins quietly, which is what joining a call
    /// someone else started does. Set it only when the user is *starting* the
    /// call: the notification is still suppressed if somebody is already in the
    /// session, but the intent to summon anyone at all is the application's to
    /// state.
    pub notify: Option<NotifyConfig>,
    /// How this call handles Element Call reactions and the raised hand.
    ///
    /// `None` — the default — is [`ReactionsConfig::default`]: enabled, with
    /// Element Call's three-second window. See [`crate::reactions`].
    pub reactions: Option<ReactionsConfig>,
}

impl CallJoinParams {
    /// A quiet join with default reactions.
    pub fn new(session: JoinSessionParams) -> Self {
        Self {
            session,
            notify: None,
            reactions: None,
        }
    }
}

impl From<JoinSessionParams> for CallJoinParams {
    fn from(session: JoinSessionParams) -> Self {
        Self::new(session)
    }
}

/// Who we are in a session's current join, for as long as we are joined.
#[derive(Clone, Debug)]
struct OwnParticipation {
    room_id: String,
    user_id: String,
    member_id: String,
}

/// The call state of one `(room, slot)` session.
struct CallSession {
    reactions: ReactionsState,
    /// The session's roster, as the core publishes it.
    roster: watch::Receiver<Vec<JoinedMembership>>,
    /// Identity of the current join, or `None` while not joined.
    own: Option<OwnParticipation>,
    /// `room_id/slot_id`, prefixed to log lines.
    log_tag: String,
}

type SessionKey = (String, String);

/// Holds the RTC session manager and the call state of every session in it.
pub struct CallSessionManager<T: CallCommandSender + 'static> {
    rtc: RtcSessionManager<T>,
    command_sender: Option<Arc<T>>,
    calls: HashMap<SessionKey, CallSession>,
}

impl<T: CallCommandSender + 'static> Default for CallSessionManager<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: CallCommandSender + 'static> AsRef<RtcSessionManager<T>> for CallSessionManager<T> {
    fn as_ref(&self) -> &RtcSessionManager<T> {
        &self.rtc
    }
}

impl<T: CallCommandSender + 'static> AsMut<RtcSessionManager<T>> for CallSessionManager<T> {
    fn as_mut(&mut self) -> &mut RtcSessionManager<T> {
        &mut self.rtc
    }
}

impl<T: CallCommandSender + 'static> CallSessionManager<T> {
    /// Creates an empty manager without a command sender.
    pub fn new() -> Self {
        Self {
            rtc: RtcSessionManager::new(),
            command_sender: None,
            calls: HashMap::new(),
        }
    }

    /// Creates an empty manager with a command sender.
    pub fn with_command_sender(command_sender: Arc<T>) -> Self {
        Self {
            rtc: RtcSessionManager::with_command_sender(command_sender.clone()),
            command_sender: Some(command_sender),
            calls: HashMap::new(),
        }
    }

    /// Sets the command sender for this manager and its sessions.
    pub fn set_command_sender(&mut self, command_sender: Arc<T>) {
        self.rtc.set_command_sender(command_sender.clone());
        self.command_sender = Some(command_sender);
    }

    /// Returns true if this manager has a command sender configured.
    pub fn has_command_sender(&self) -> bool {
        self.command_sender.is_some()
    }

    /// The RTC layer underneath. Inputs that move the roster must go through
    /// this manager's own methods, or the call state falls behind.
    pub fn rtc(&self) -> &RtcSessionManager<T> {
        &self.rtc
    }

    /// Mutable access to the RTC layer, for read-side wiring such as
    /// installing a participation listener or a key signal handler.
    pub fn rtc_mut(&mut self) -> &mut RtcSessionManager<T> {
        &mut self.rtc
    }

    // ---- inputs that move the roster or our membership ---------------------

    /// Joins a session and, if asked, notifies the room (MSC4075).
    ///
    /// See [`RtcSessionManager::join`] for the RTC half. The notification is
    /// sent only when nobody else is in the session — every joiner sending one
    /// would ring the room once per participant — and never fails the join.
    pub async fn join(&mut self, params: CallJoinParams) -> Result<(), JoinError> {
        let CallJoinParams {
            session,
            notify,
            reactions,
        } = params;
        let room_id = session.room_id.clone();
        let slot_id = session.slot_id.clone();
        let user_id = session.user_id.clone();
        let device_id = session.device_id.clone();
        let application = session.application.clone();

        self.rtc.join(session).await?;

        let member_id = self
            .rtc
            .own_member_id(&room_id, &slot_id)
            .expect("a successful join has a member id");
        let call = self.call_mut(&room_id, &slot_id);
        call.reactions.configure(reactions.unwrap_or_default());
        call.reactions.reset_own();
        call.own = Some(OwnParticipation {
            room_id: room_id.clone(),
            user_id: user_id.clone(),
            member_id,
        });
        self.sync_rosters();

        if let Some(notify) = notify {
            self.notify_session_started(
                &room_id,
                &slot_id,
                &notify,
                &application,
                &user_id,
                &device_id,
            )
            .await;
        }
        Ok(())
    }

    /// Leaves a session, lowering our hand first while the membership it
    /// annotates still stands. See [`RtcSessionManager::leave`].
    pub async fn leave(
        &mut self,
        room_id: String,
        slot_id: String,
        params: LeaveSessionParams,
    ) -> Result<(), LeaveError> {
        // Best effort: peers drop the hand with the membership anyway, so a
        // failed redaction costs nothing but a stale annotation in the timeline.
        if self.own_raised_hand_of(&room_id, &slot_id).is_some()
            && let Err(error) = self.lower_hand(&room_id, &slot_id).await
        {
            log::warn!(
                "[{room_id}/{slot_id}] the raised hand was not lowered before leaving ({error}); \
                 peers drop it with the membership",
            );
        }

        self.rtc
            .leave(room_id.clone(), slot_id.clone(), params)
            .await?;

        if let Some(call) = self.calls.get_mut(&key(&room_id, &slot_id)) {
            call.own = None;
            call.reactions.reset_own();
        }
        self.sync_rosters();
        Ok(())
    }

    /// Heartbeats a session, then re-raises our hand if the beat refreshed
    /// our membership event. See [`RtcSessionManager::heartbeat`].
    pub async fn heartbeat(&mut self, room_id: &str, slot_id: &str) -> bool {
        let joined = self.rtc.heartbeat(room_id, slot_id).await;
        if joined {
            self.reannotate_hand_if_moved(room_id, slot_id).await;
        }
        joined
    }

    /// See [`RtcSessionManager::set_current_sticky_state`].
    pub async fn set_current_sticky_state(
        &mut self,
        room_id: &str,
        events: impl IntoIterator<Item = RawStickyEvent>,
    ) -> Result<(), EventConversionError> {
        let result = self.rtc.set_current_sticky_state(room_id, events).await;
        self.sync_rosters();
        result
    }

    /// See [`RtcSessionManager::on_room_slots_received`].
    pub async fn on_room_slots_received(
        &mut self,
        room_id: &str,
        slots: impl IntoIterator<Item = RawSlotEvent>,
    ) {
        self.rtc.on_room_slots_received(room_id, slots).await;
        self.sync_rosters();
    }

    /// See [`RtcSessionManager::forget_room_slots`].
    pub async fn forget_room_slots(&mut self, room_id: &str) {
        self.rtc.forget_room_slots(room_id).await;
        self.sync_rosters();
    }

    /// See [`RtcSessionManager::on_room_encryption_received`].
    pub async fn on_room_encryption_received(&mut self, room_id: &str, encrypted: bool) {
        self.rtc
            .on_room_encryption_received(room_id, encrypted)
            .await;
        self.sync_rosters();
    }

    /// See [`RtcSessionManager::on_room_members_received`].
    pub async fn on_room_members_received(
        &mut self,
        room_id: &str,
        joined_user_ids: impl IntoIterator<Item = String>,
    ) {
        self.rtc
            .on_room_members_received(room_id, joined_user_ids)
            .await;
        self.sync_rosters();
    }

    /// Brings every session's call state in line with its roster: members gone
    /// from a roster lose their hand, members whose membership event moved on
    /// get the new id remembered. Runs after every roster input so a dropped
    /// hand is published on the same call that dropped the member.
    fn sync_rosters(&mut self) {
        for (room_id, slot_id) in self.rtc.session_keys() {
            let call = self.call_mut(&room_id, &slot_id);
            if call.roster.has_changed().unwrap_or(false) {
                let members = call.roster.borrow_and_update().clone();
                call.reactions.sync_roster(&members);
            }
        }
    }

    /// The call state of a session, created on first use. Only valid for a
    /// session the RTC layer holds.
    fn call_mut(&mut self, room_id: &str, slot_id: &str) -> &mut CallSession {
        let key = key(room_id, slot_id);
        if !self.calls.contains_key(&key) {
            let roster = self
                .rtc
                .subscribe_membership_snapshots(room_id, slot_id)
                .expect("call state is only created for sessions the RTC layer holds");
            self.calls.insert(
                key.clone(),
                CallSession {
                    reactions: ReactionsState::new(),
                    roster,
                    own: None,
                    log_tag: format!("{room_id}/{slot_id}"),
                },
            );
        }
        self.calls.get_mut(&key).expect("just inserted")
    }

    /// The call state of an existing session, creating it if the RTC layer
    /// holds the session but nothing has asked for its call state yet.
    fn call_for(&mut self, room_id: &str, slot_id: &str) -> Option<&mut CallSession> {
        self.rtc.member_count(room_id, slot_id)?;
        Some(self.call_mut(room_id, slot_id))
    }

    fn calls_in_room(&mut self, room_id: &str) -> Vec<SessionKey> {
        let keys: Vec<SessionKey> = self
            .rtc
            .slots_in_room(room_id)
            .into_iter()
            .map(|slot_id| key(room_id, &slot_id))
            .collect();
        for (room_id, slot_id) in &keys {
            self.call_mut(room_id, slot_id);
        }
        keys
    }

    // ---- MSC4075 -------------------------------------------------------

    /// Sends the notification that summons the room to this session, at the
    /// tail of a join.
    #[allow(clippy::too_many_arguments)]
    async fn notify_session_started(
        &self,
        room_id: &str,
        slot_id: &str,
        notify: &NotifyConfig,
        application: &str,
        user_id: &str,
        device_id: &str,
    ) {
        let Some(call) = self.calls.get(&key(room_id, slot_id)) else {
            return;
        };
        // The question is whether anyone *else* is here, so our own
        // participations have to come out of the count first. Both kinds
        // occur: the host feeds the room's whole sticky map, which contains our
        // own membership as soon as the homeserver echoes it back, and a
        // session outlives `leave()` keeping the previous call's membership as
        // a candidate. Counting either concludes that somebody else started
        // the call and stays silent — the caller hits "call" and no phone
        // rings.
        //
        // The core already drops stale participations of this device from the
        // roster, but only where the sending device is known, and an
        // unencrypted room reports none. So a membership from our own user
        // with no device attributed is treated as ours here too. That is a
        // *wider* rule than the roster's on purpose: it can only misfire on
        // another device of our own user in an unencrypted room, where the
        // cost is one extra ring — against a silent failure to ring at all.
        let others = call
            .roster
            .borrow()
            .iter()
            .filter(|member| {
                !(member.sender == user_id
                    && member
                        .origin
                        .sender_device_id()
                        .is_none_or(|device| device == device_id))
            })
            .count();
        if others != 0 {
            log::info!(
                "[{}] not notifying: {others} member(s) were already in the session, so \
                 somebody else started it",
                call.log_tag,
            );
            return;
        }

        let (Some(command_sender), Some(member_event_id)) = (
            self.command_sender.as_ref(),
            self.rtc.own_membership_event_id(room_id, slot_id),
        ) else {
            return;
        };

        let content = build_notification_content(
            notify,
            application,
            user_id,
            device_id,
            &member_event_id,
            now_ms(),
        );
        log::info!(
            "[{}] notifying the room: {}",
            call.log_tag,
            notify.notification_type.as_str(),
        );
        if let Err(error) = command_sender
            .send_sticky_event(
                room_id.to_owned(),
                NOTIFICATION_EVENT_TYPE.to_owned(),
                content,
                notification_sticky_duration_ms(notify.lifetime_ms()),
            )
            .await
        {
            log::warn!(
                "[{}] the session-started notification was not sent ({error:?}); the call \
                 itself is unaffected",
                call.log_tag,
            );
        }
    }

    // ---- Reactions and raised hands (see `crate::reactions`) -----------
    //
    // Inbound reactions are routed by *room*: a reaction relates to a
    // membership event and names no slot, so every session of the room is
    // offered each event and keeps the ones that relate to its own members.

    /// Applies message-like room events — `io.element.call.reaction` and
    /// `m.reaction` — to every session of `room_id`. Other event types are
    /// ignored, so a host may forward without filtering.
    pub fn on_room_timeline_events(&mut self, room_id: &str, events: &[RawTimelineEvent]) {
        self.sync_rosters();
        for (room_id, slot_id) in self.calls_in_room(room_id) {
            let call = self.call_mut(&room_id, &slot_id);
            let members = call.roster.borrow().clone();
            for event in events {
                call.reactions.ingest(event, &members, false);
            }
        }
    }

    /// Lowers whichever hand the redacted `event_id` raised, in every session
    /// of `room_id`.
    pub fn on_event_redacted(&mut self, room_id: &str, event_id: &str) {
        for (room_id, slot_id) in self.calls_in_room(room_id) {
            let call = self.call_mut(&room_id, &slot_id);
            if call.reactions.on_event_redacted(event_id) {
                log::info!(
                    "[{}] our raised hand was lowered by a redaction from elsewhere",
                    call.log_tag,
                );
            }
        }
    }

    /// Feeds the annotations of one membership event back, as fetched in
    /// answer to [`Self::pending_relation_lookups`]. Only raised hands are
    /// taken from them: an old emoji reaction is not replayed.
    pub fn on_relations_received(
        &mut self,
        room_id: &str,
        target_event_id: &str,
        events: &[RawTimelineEvent],
    ) {
        self.sync_rosters();
        for (room_id, slot_id) in self.calls_in_room(room_id) {
            let call = self.call_mut(&room_id, &slot_id);
            let members = call.roster.borrow().clone();
            call.reactions
                .on_relations_received(target_event_id, events, &members);
        }
    }

    /// Membership events in `room_id` whose annotations have not been fetched
    /// yet, across its sessions. Our own membership is excluded: we know
    /// whether our hand is up.
    ///
    /// Answer each with the event's `/relations` (`rel_type=m.annotation`,
    /// `event_type=m.reaction`) through [`Self::on_relations_received`]; that
    /// is how hands raised before we joined become visible. Asking is what
    /// marks an id as fetched, so a failed fetch is retried by asking again.
    pub fn pending_relation_lookups(&self, room_id: &str) -> Vec<RelationLookup> {
        let mut seen = HashSet::new();
        self.calls
            .iter()
            .filter(|((room, _), _)| room == room_id)
            .flat_map(|((room, slot), call)| {
                let own = self.rtc.own_member_id(room, slot);
                call.reactions
                    .pending_relation_lookups(&call.roster.borrow(), own.as_deref())
            })
            .filter(|lookup| seen.insert(lookup.membership_event_id.clone()))
            .collect()
    }

    /// Sends an emoji reaction from one session, relating it to our current
    /// membership event.
    ///
    /// `name` is what peers select a sound by (see
    /// [`KNOWN_REACTIONS`](crate::reactions::KNOWN_REACTIONS)); an unknown
    /// name plays Element Call's generic sound. Only the first grapheme of
    /// `emoji` is sent. Returns the event id.
    ///
    /// Refused inside the send cooldown: peers would drop the reaction anyway.
    pub async fn send_reaction(
        &mut self,
        room_id: &str,
        slot_id: &str,
        emoji: &str,
        name: &str,
    ) -> Result<String, ReactionError> {
        let emoji = first_grapheme(emoji);
        if emoji.is_empty() {
            return Err(ReactionError::EmptyEmoji);
        }
        let command_sender = self.reaction_command_sender()?;
        let (own, membership_event_id) = self.own_relation_target(room_id, slot_id)?;
        let call = self
            .call_for(room_id, slot_id)
            .ok_or(ReactionError::NoSession)?;
        call.reactions.check_send_allowed()?;

        let event_id = command_sender
            .send_room_event(
                own.room_id,
                REACTION_EVENT_TYPE.to_owned(),
                build_reaction_content(&membership_event_id, emoji, name),
            )
            .await?;
        let call = self.call_mut(room_id, slot_id);
        call.reactions.record_sent();
        log::debug!(
            "[{}] sent reaction {emoji} ({name}) as {event_id}",
            call.log_tag
        );
        Ok(event_id)
    }

    /// Raises our hand in one session: annotates our current membership event
    /// with [`RAISED_HAND_KEY`](crate::reactions::RAISED_HAND_KEY).
    /// Idempotent while it is up. Shows in [`Self::raised_hands`] right away
    /// rather than waiting for the echo.
    pub async fn raise_hand(&mut self, room_id: &str, slot_id: &str) -> Result<(), ReactionError> {
        let call = self
            .call_for(room_id, slot_id)
            .ok_or(ReactionError::NoSession)?;
        if !call.reactions.config().enabled {
            return Err(ReactionError::Disabled);
        }
        if call.reactions.own_raised_hand().is_some() {
            return Ok(());
        }
        let command_sender = self.reaction_command_sender()?;
        let (own, membership_event_id) = self.own_relation_target(room_id, slot_id)?;

        let event_id = command_sender
            .send_room_event(
                own.room_id,
                ANNOTATION_EVENT_TYPE.to_owned(),
                build_raised_hand_content(&membership_event_id),
            )
            .await?;
        let call = self.call_mut(room_id, slot_id);
        log::info!("[{}] hand raised ({event_id})", call.log_tag);
        call.reactions.set_own_raised_hand(
            &own.member_id,
            &own.user_id,
            event_id,
            membership_event_id,
        );
        Ok(())
    }

    /// Lowers our hand in one session by redacting the annotation. A no-op
    /// when it is not up.
    pub async fn lower_hand(&mut self, room_id: &str, slot_id: &str) -> Result<(), ReactionError> {
        let call = self
            .call_for(room_id, slot_id)
            .ok_or(ReactionError::NoSession)?;
        let Some(hand) = call.reactions.own_raised_hand().cloned() else {
            return Ok(());
        };
        let own = call.own.clone().ok_or(ReactionError::NotJoined)?;
        let command_sender = self.reaction_command_sender()?;

        command_sender
            .redact_event(own.room_id, hand.reaction_event_id.clone(), None)
            .await?;
        let call = self.call_mut(room_id, slot_id);
        log::info!(
            "[{}] hand lowered ({} redacted)",
            call.log_tag,
            hand.reaction_event_id
        );
        call.reactions.clear_own_raised_hand(&own.member_id);
        Ok(())
    }

    /// The raised hands of one session, oldest first, or `None` if there is
    /// no such session.
    pub fn raised_hands(&mut self, room_id: &str, slot_id: &str) -> Option<Vec<RaisedHand>> {
        self.call_for(room_id, slot_id)
            .map(|call| call.reactions.raised_hands())
    }

    /// Subscribes to the raised hands of one session — the whole list on
    /// every change — or `None` if there is no such session.
    pub fn subscribe_raised_hands(
        &mut self,
        room_id: &str,
        slot_id: &str,
    ) -> Option<watch::Receiver<Vec<RaisedHand>>> {
        self.call_for(room_id, slot_id)
            .map(|call| call.reactions.subscribe_raised_hands())
    }

    /// Subscribes to the emoji reactions of one session as they arrive, our
    /// own echo included, or `None` if there is no such session. A lagging
    /// subscriber loses the oldest ones, which for a three-second visual is
    /// the right trade.
    pub fn subscribe_reactions(
        &mut self,
        room_id: &str,
        slot_id: &str,
    ) -> Option<broadcast::Receiver<ReceivedReaction>> {
        self.call_for(room_id, slot_id)
            .map(|call| call.reactions.subscribe_reactions())
    }

    /// Where our reactions relate to: our room and current membership event.
    fn own_relation_target(
        &self,
        room_id: &str,
        slot_id: &str,
    ) -> Result<(OwnParticipation, String), ReactionError> {
        let own = self
            .calls
            .get(&key(room_id, slot_id))
            .and_then(|call| call.own.clone())
            .ok_or(ReactionError::NotJoined)?;
        let membership_event_id = self
            .rtc
            .own_membership_event_id(room_id, slot_id)
            .ok_or(ReactionError::NotJoined)?;
        Ok((own, membership_event_id))
    }

    fn reaction_command_sender(&self) -> Result<Arc<T>, ReactionError> {
        self.command_sender.clone().ok_or_else(|| {
            ReactionError::Command(CommandError::from_message("no command sender configured"))
        })
    }

    fn own_raised_hand_of(
        &self,
        room_id: &str,
        slot_id: &str,
    ) -> Option<crate::reactions::OwnRaisedHand> {
        self.calls
            .get(&key(room_id, slot_id))
            .and_then(|call| call.reactions.own_raised_hand().cloned())
    }

    /// Raises our hand again on our new membership event after a sticky
    /// refresh, and redacts the annotation on the old one.
    ///
    /// Element Call drops a raised hand whose membership event has moved on
    /// and looks for one on the new event instead, so a hand that stayed on
    /// the join event would be lowered for us at the first refresh. Peers may
    /// see the hand drop for one round trip in between; that is inherent to
    /// the protocol. A failed re-send is retried on the next heartbeat, since
    /// the ids still differ.
    async fn reannotate_hand_if_moved(&mut self, room_id: &str, slot_id: &str) {
        let Some(hand) = self.own_raised_hand_of(room_id, slot_id) else {
            return;
        };
        let Ok((own, current)) = self.own_relation_target(room_id, slot_id) else {
            return;
        };
        if current == hand.annotated_membership_event_id {
            return;
        }
        let Ok(command_sender) = self.reaction_command_sender() else {
            return;
        };
        let log_tag = format!("{room_id}/{slot_id}");

        log::debug!(
            "[{log_tag}] our membership event moved ({} -> {current}); raising the hand on it \
             again",
            hand.annotated_membership_event_id,
        );
        match command_sender
            .send_room_event(
                own.room_id.clone(),
                ANNOTATION_EVENT_TYPE.to_owned(),
                build_raised_hand_content(&current),
            )
            .await
        {
            Ok(event_id) => {
                self.call_mut(room_id, slot_id)
                    .reactions
                    .set_own_raised_hand(&own.member_id, &own.user_id, event_id, current);
                if let Err(error) = command_sender
                    .redact_event(own.room_id, hand.reaction_event_id.clone(), None)
                    .await
                {
                    log::warn!(
                        "[{log_tag}] the previous raised-hand annotation {} was not redacted \
                         ({error}); it relates to a superseded membership event, so peers ignore \
                         it",
                        hand.reaction_event_id,
                    );
                }
            }
            Err(error) => log::warn!(
                "[{log_tag}] could not raise the hand again on the refreshed membership \
                 ({error}); retrying on the next heartbeat",
            ),
        }
    }

    // ---- test hooks ----------------------------------------------------

    #[cfg(test)]
    pub(crate) fn set_reactions_clock(
        &mut self,
        room_id: &str,
        slot_id: &str,
        clock: crate::reactions::Clock,
    ) {
        self.call_mut(room_id, slot_id).reactions.set_clock(clock);
    }

    #[cfg(test)]
    pub(crate) fn own_raised_hand(
        &self,
        room_id: &str,
        slot_id: &str,
    ) -> Option<crate::reactions::OwnRaisedHand> {
        self.own_raised_hand_of(room_id, slot_id)
    }

    // ---- plain forwarders to the RTC layer -----------------------------

    /// See [`RtcSessionManager::session_count`].
    pub fn session_count(&self) -> usize {
        self.rtc.session_count()
    }

    /// See [`RtcSessionManager::member_count`].
    pub fn member_count(&self, room_id: &str, slot_id: &str) -> Option<usize> {
        self.rtc.member_count(room_id, slot_id)
    }

    /// See [`RtcSessionManager::session_keys`].
    pub fn session_keys(&self) -> Vec<(String, String)> {
        self.rtc.session_keys()
    }

    /// See [`RtcSessionManager::slots_in_room`].
    pub fn slots_in_room(&self, room_id: &str) -> Vec<String> {
        self.rtc.slots_in_room(room_id)
    }

    /// See [`RtcSessionManager::subscribe_membership_snapshots`].
    pub fn subscribe_membership_snapshots(
        &self,
        room_id: &str,
        slot_id: &str,
    ) -> Option<watch::Receiver<Vec<JoinedMembership>>> {
        self.rtc.subscribe_membership_snapshots(room_id, slot_id)
    }

    /// See [`RtcSessionManager::set_encryption_signal_handler`].
    pub fn set_encryption_signal_handler(
        &mut self,
        room_id: &str,
        slot_id: &str,
        handler: Arc<dyn EncryptionKeySignalHandler>,
    ) -> bool {
        self.rtc
            .set_encryption_signal_handler(room_id, slot_id, handler)
    }

    /// See [`RtcSessionManager::own_member_id`].
    pub fn own_member_id(&self, room_id: &str, slot_id: &str) -> Option<String> {
        self.rtc.own_member_id(room_id, slot_id)
    }

    /// See [`RtcSessionManager::own_membership_event_id`].
    pub fn own_membership_event_id(&self, room_id: &str, slot_id: &str) -> Option<String> {
        self.rtc.own_membership_event_id(room_id, slot_id)
    }

    /// See [`RtcSessionManager::replay_encryption_keys`].
    pub async fn replay_encryption_keys(&self, room_id: &str, slot_id: &str) -> bool {
        self.rtc.replay_encryption_keys(room_id, slot_id).await
    }

    /// See [`RtcSessionManager::key_rotation_due_at_ms`].
    pub fn key_rotation_due_at_ms(&self, room_id: &str, slot_id: &str) -> Option<u64> {
        self.rtc.key_rotation_due_at_ms(room_id, slot_id)
    }

    /// See [`RtcSessionManager::flush_due_key_rotation`].
    pub async fn flush_due_key_rotation(&self, room_id: &str, slot_id: &str) -> bool {
        self.rtc.flush_due_key_rotation(room_id, slot_id).await
    }

    /// See [`RtcSessionManager::set_encryption_identity_mapper`].
    pub fn set_encryption_identity_mapper(
        &mut self,
        room_id: &str,
        slot_id: &str,
        mapper: RtcIdentityMapper,
    ) -> bool {
        self.rtc
            .set_encryption_identity_mapper(room_id, slot_id, mapper)
    }

    /// See [`RtcSessionManager::receive_encryption_key`].
    pub async fn receive_encryption_key(
        &self,
        received: ReceivedEncryptionKey,
    ) -> Result<(), CommandError> {
        self.rtc.receive_encryption_key(received).await
    }

    /// See [`RtcSessionManager::open_slot`].
    pub async fn open_slot(
        &self,
        room_id: String,
        slot_id: String,
        application_type: String,
        encryption: Option<SlotEncryption>,
    ) -> Result<(), CommandError> {
        self.rtc
            .open_slot(room_id, slot_id, application_type, encryption)
            .await
    }

    /// See [`RtcSessionManager::close_slot`].
    pub async fn close_slot(&self, room_id: String, slot_id: String) -> Result<(), CommandError> {
        self.rtc.close_slot(room_id, slot_id).await
    }

    /// See [`RtcSessionManager::slot_state`].
    pub fn slot_state(&self, room_id: &str, slot_id: &str) -> Option<SlotState> {
        self.rtc.slot_state(room_id, slot_id)
    }

    /// Everything the RTC layer and this one believe, as JSON, for bug
    /// reports. See [`RtcSessionManager::debug_snapshot`].
    pub fn debug_snapshot(&self) -> serde_json::Value {
        let mut snapshot = self.rtc.debug_snapshot();
        let calls: serde_json::Map<String, serde_json::Value> = self
            .calls
            .values()
            .map(|call| {
                (
                    call.log_tag.clone(),
                    serde_json::json!({
                        "raised_hands": call.reactions.raised_hands().len(),
                        "own_hand_raised": call.reactions.own_raised_hand().is_some(),
                        "reactions_enabled": call.reactions.config().enabled,
                    }),
                )
            })
            .collect();
        snapshot["calls"] = serde_json::Value::Object(calls);
        snapshot
    }

    // ---- participation facade (see `matrix_rtc_core::participation`) ---

    /// See [`RtcSessionManager::memberships`].
    pub fn memberships(&self, room_id: &str, slot_id: &str) -> Option<Vec<SessionMembership>> {
        self.rtc.memberships(room_id, slot_id)
    }

    /// See [`RtcSessionManager::transports`].
    pub fn transports(&self, room_id: &str, slot_id: &str) -> Option<Vec<TransportWithMembers>> {
        self.rtc.transports(room_id, slot_id)
    }

    /// See [`RtcSessionManager::key_map`].
    pub fn key_map(&self, room_id: &str, slot_id: &str) -> Option<KeyMap> {
        self.rtc.key_map(room_id, slot_id)
    }

    /// See [`RtcSessionManager::participation_status`].
    pub fn participation_status(&self, room_id: &str, slot_id: &str) -> Status {
        self.rtc.participation_status(room_id, slot_id)
    }

    /// See [`RtcSessionManager::participation`].
    pub fn participation(&self, room_id: &str, slot_id: &str) -> Option<ParticipationSnapshot> {
        self.rtc.participation(room_id, slot_id)
    }

    /// See [`RtcSessionManager::set_participation_listener`].
    pub fn set_participation_listener(
        &mut self,
        room_id: &str,
        slot_id: &str,
        listener: Arc<dyn ParticipationListener>,
    ) {
        self.rtc
            .set_participation_listener(room_id, slot_id, listener);
    }

    /// See [`RtcSessionManager::clear_participation_listener`].
    pub fn clear_participation_listener(&mut self, room_id: &str, slot_id: &str) {
        self.rtc.clear_participation_listener(room_id, slot_id);
    }
}

fn key(room_id: &str, slot_id: &str) -> SessionKey {
    (room_id.to_owned(), slot_id.to_owned())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use matrix_rtc_core::{
        ApplicationInfo, EventOrigin, JoinSessionParams, LeaveSessionParams, LiveKitTransport,
        MemberInfo, Membership, RawStickyEvent, RawStickyEventContent, RtcTransport,
    };
    use serde_json::{Value, json};
    use tokio::sync::broadcast::error::TryRecvError;

    use super::*;
    use crate::reactions::{
        Clock, ReactionSound, build_raised_hand_content, build_reaction_content,
    };
    use crate::testing::MockCallCommandSender;

    const ROOM: &str = "!room:example.org";
    const SLOT: &str = "m.call#ROOM";
    const ALICE: &str = "@alice:example.org";
    const BOB: &str = "@bob:example.org";
    const BOB_MEMBER: &str = "bob-member-1";

    type Manager = CallSessionManager<MockCallCommandSender>;

    /// A clock the test moves by hand.
    struct TestClock(Arc<AtomicU64>);

    impl TestClock {
        fn new(start: u64) -> Self {
            Self(Arc::new(AtomicU64::new(start)))
        }

        fn clock(&self) -> Clock {
            let time = self.0.clone();
            Arc::new(move || time.load(Ordering::SeqCst))
        }

        fn advance(&self, ms: u64) {
            self.0.fetch_add(ms, Ordering::SeqCst);
        }
    }

    fn member_event(sender: &str, device: &str, member_id: &str, event_id: &str) -> RawStickyEvent {
        RawStickyEvent {
            room_id: ROOM.to_owned(),
            event_id: Some(event_id.to_owned()),
            sender: sender.to_owned(),
            origin: EventOrigin::encrypted(Some(device.to_owned())),
            event_type: "m.rtc.member".to_owned(),
            content: RawStickyEventContent {
                slot_id: SLOT.to_owned(),
                sticky_key: member_id.to_owned(),
                member: MemberInfo {
                    id: Some(member_id.to_owned()),
                    membership: Some(Membership::Join),
                },
                application: ApplicationInfo {
                    application_type: Some("m.call".to_owned()),
                    extra: Default::default(),
                },
                transports: None,
                leave_reason: None,
                created_ts: None,
            },
        }
    }

    fn join_params() -> JoinSessionParams {
        JoinSessionParams::new(
            ALICE.to_owned(),
            "ALICEDEV".to_owned(),
            ROOM.to_owned(),
            SLOT.to_owned(),
            "m.call".to_owned(),
            RtcTransport::LiveKit(LiveKitTransport {
                livekit_service_url: "https://sfu.example.org".to_owned(),
            }),
        )
    }

    /// Alice joined (her membership event is `$sticky-1`), Bob in the roster
    /// with membership event `$bob-member-1`.
    async fn joined_manager(
        params: impl Into<CallJoinParams>,
    ) -> (Manager, Arc<MockCallCommandSender>) {
        let sender = Arc::new(MockCallCommandSender::new());
        let mut manager = CallSessionManager::with_command_sender(sender.clone());
        let mut params: CallJoinParams = params.into();
        let own_member_id = params.session.membership_id();
        params.session.membership_id = Some(own_member_id.clone());
        manager.join(params).await.expect("join succeeds");
        assert_eq!(
            manager.own_membership_event_id(ROOM, SLOT).as_deref(),
            Some("$sticky-1")
        );
        manager
            .set_current_sticky_state(
                ROOM,
                vec![
                    member_event(ALICE, "ALICEDEV", &own_member_id, "$sticky-1"),
                    member_event(BOB, "BOBDEV", BOB_MEMBER, "$bob-member-1"),
                ],
            )
            .await
            .unwrap();
        assert_eq!(manager.member_count(ROOM, SLOT), Some(2));
        (manager, sender)
    }

    /// Feeds one timeline event of `ROOM`, as a host forwarding its timeline does.
    fn ingest(manager: &mut Manager, event: &RawTimelineEvent) {
        manager.on_room_timeline_events(ROOM, std::slice::from_ref(event));
    }

    fn timeline_event(
        event_id: &str,
        sender: &str,
        event_type: &str,
        ts: u64,
        content: Value,
    ) -> RawTimelineEvent {
        RawTimelineEvent {
            room_id: ROOM.to_owned(),
            event_id: event_id.to_owned(),
            sender: sender.to_owned(),
            origin: EventOrigin::Unknown,
            event_type: event_type.to_owned(),
            origin_server_ts: ts,
            content,
        }
    }

    fn bob_reacts(event_id: &str, target: &str, emoji: &str, name: &str) -> RawTimelineEvent {
        timeline_event(
            event_id,
            BOB,
            REACTION_EVENT_TYPE,
            1_000,
            build_reaction_content(target, emoji, name),
        )
    }

    fn bob_raises(event_id: &str, target: &str, ts: u64) -> RawTimelineEvent {
        timeline_event(
            event_id,
            BOB,
            ANNOTATION_EVENT_TYPE,
            ts,
            build_raised_hand_content(target),
        )
    }

    fn hands(manager: &mut Manager) -> Vec<(&'static str, String)> {
        manager
            .raised_hands(ROOM, SLOT)
            .unwrap()
            .into_iter()
            .map(|hand| {
                let who = if hand.sender == BOB { "bob" } else { "alice" };
                (who, hand.reaction_event_id)
            })
            .collect()
    }

    #[tokio::test]
    async fn a_peers_reaction_is_surfaced_with_its_sound() {
        let (mut manager, _) = joined_manager(join_params()).await;
        let mut reactions = manager.subscribe_reactions(ROOM, SLOT).unwrap();

        ingest(
            &mut manager,
            &bob_reacts("$r1", "$bob-member-1", "👏", "clapping"),
        );

        let received = reactions.try_recv().expect("one reaction");
        assert_eq!(received.member_id, BOB_MEMBER);
        assert_eq!(received.sender, BOB);
        assert_eq!(received.emoji, "👏");
        assert_eq!(received.name, "clapping");
        assert_eq!(received.sound, ReactionSound::Named("clap".to_owned()));
    }

    #[tokio::test]
    async fn a_reaction_is_only_accepted_from_the_member_it_relates_to() {
        let (mut manager, _) = joined_manager(join_params()).await;
        let mut reactions = manager.subscribe_reactions(ROOM, SLOT).unwrap();

        // Carol reacting "as" Bob.
        let mut forged = bob_reacts("$r1", "$bob-member-1", "👏", "clapping");
        forged.sender = "@carol:example.org".to_owned();
        ingest(&mut manager, &forged);
        // Bob relating to an event that is nobody's membership.
        ingest(
            &mut manager,
            &bob_reacts("$r2", "$not-a-membership", "👏", "clapping"),
        );
        // Bob raising a hand on Alice's membership.
        ingest(&mut manager, &bob_raises("$h1", "$sticky-1", 5));

        assert_eq!(reactions.try_recv().unwrap_err(), TryRecvError::Empty);
        assert!(manager.raised_hands(ROOM, SLOT).unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_repeat_inside_the_active_window_is_dropped() {
        let (mut manager, _) = joined_manager(join_params()).await;
        let clock = TestClock::new(10_000);
        manager.set_reactions_clock(ROOM, SLOT, clock.clock());
        let mut reactions = manager.subscribe_reactions(ROOM, SLOT).unwrap();

        ingest(
            &mut manager,
            &bob_reacts("$r1", "$bob-member-1", "👏", "clapping"),
        );
        clock.advance(1_000);
        ingest(
            &mut manager,
            &bob_reacts("$r2", "$bob-member-1", "🎉", "party"),
        );
        assert_eq!(reactions.try_recv().unwrap().emoji, "👏");
        assert_eq!(reactions.try_recv().unwrap_err(), TryRecvError::Empty);

        clock.advance(2_000);
        ingest(
            &mut manager,
            &bob_reacts("$r3", "$bob-member-1", "🎉", "party"),
        );
        assert_eq!(reactions.try_recv().unwrap().emoji, "🎉");
    }

    #[tokio::test]
    async fn sending_relates_to_our_membership_and_honours_the_cooldown() {
        let (mut manager, sender) = joined_manager(join_params()).await;
        let clock = TestClock::new(10_000);
        manager.set_reactions_clock(ROOM, SLOT, clock.clock());

        let event_id = manager
            .send_reaction(ROOM, SLOT, "🎉 and more", "party")
            .await
            .expect("first reaction goes out");
        assert_eq!(event_id, "$room-1");
        let sent = sender.room_events.lock().unwrap().clone();
        assert_eq!(
            sent,
            vec![(
                ROOM.to_owned(),
                REACTION_EVENT_TYPE.to_owned(),
                build_reaction_content("$sticky-1", "🎉", "party"),
            )]
        );

        clock.advance(1_000);
        match manager.send_reaction(ROOM, SLOT, "👏", "clapping").await {
            Err(ReactionError::Cooldown { remaining_ms }) => assert_eq!(remaining_ms, 2_000),
            other => panic!("expected a cooldown, got {other:?}"),
        }
        assert_eq!(sender.room_events.lock().unwrap().len(), 1);

        clock.advance(2_000);
        manager
            .send_reaction(ROOM, SLOT, "👏", "clapping")
            .await
            .expect("the cooldown has passed");
        assert_eq!(sender.room_events.lock().unwrap().len(), 2);

        assert!(matches!(
            manager.send_reaction(ROOM, SLOT, "   ", "nothing").await,
            Err(ReactionError::EmptyEmoji)
        ));
    }

    #[tokio::test]
    async fn raising_is_idempotent_and_lowering_redacts_the_annotation() {
        let (mut manager, sender) = joined_manager(join_params()).await;
        let mut watch = manager.subscribe_raised_hands(ROOM, SLOT).unwrap();

        manager.raise_hand(ROOM, SLOT).await.expect("raise");
        let sent = sender.room_events.lock().unwrap().clone();
        assert_eq!(
            sent,
            vec![(
                ROOM.to_owned(),
                ANNOTATION_EVENT_TYPE.to_owned(),
                build_raised_hand_content("$sticky-1"),
            )]
        );
        // Shown locally at once, before any echo.
        assert_eq!(hands(&mut manager), vec![("alice", "$room-1".to_owned())]);
        assert!(watch.has_changed().unwrap());
        assert_eq!(watch.borrow_and_update().len(), 1);

        manager.raise_hand(ROOM, SLOT).await.expect("raise again");
        assert_eq!(
            sender.room_events.lock().unwrap().len(),
            1,
            "nothing re-sent"
        );

        // The echo of our own annotation changes nothing.
        ingest(
            &mut manager,
            &timeline_event(
                "$room-1",
                ALICE,
                ANNOTATION_EVENT_TYPE,
                2_000,
                build_raised_hand_content("$sticky-1"),
            ),
        );
        assert_eq!(hands(&mut manager), vec![("alice", "$room-1".to_owned())]);
        assert!(!watch.has_changed().unwrap());

        manager.lower_hand(ROOM, SLOT).await.expect("lower");
        assert_eq!(
            sender.redactions.lock().unwrap().clone(),
            vec![(ROOM.to_owned(), "$room-1".to_owned(), None)]
        );
        assert!(manager.raised_hands(ROOM, SLOT).unwrap().is_empty());

        manager
            .lower_hand(ROOM, SLOT)
            .await
            .expect("lowering twice is fine");
        assert_eq!(sender.redactions.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_peers_hand_stays_across_their_refresh_and_goes_with_them() {
        let (mut manager, _) = joined_manager(join_params()).await;
        let own_member_id = manager.own_member_id(ROOM, SLOT).unwrap();

        // Before anything was fetched, Bob's membership event wants a lookup
        // and ours does not.
        assert_eq!(
            manager.pending_relation_lookups(ROOM),
            vec![RelationLookup {
                member_id: BOB_MEMBER.to_owned(),
                membership_event_id: "$bob-member-1".to_owned(),
            }]
        );
        manager.on_relations_received(ROOM, "$bob-member-1", &[]);
        assert!(manager.pending_relation_lookups(ROOM).is_empty());

        ingest(&mut manager, &bob_raises("$h1", "$bob-member-1", 5_000));
        assert_eq!(hands(&mut manager), vec![("bob", "$h1".to_owned())]);

        // Bob's sticky refresh moves his membership event on.
        manager
            .set_current_sticky_state(
                ROOM,
                vec![
                    member_event(ALICE, "ALICEDEV", &own_member_id, "$sticky-1"),
                    member_event(BOB, "BOBDEV", BOB_MEMBER, "$bob-member-2"),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            hands(&mut manager),
            vec![("bob", "$h1".to_owned())],
            "a hand outlives a refresh"
        );
        assert_eq!(
            manager.pending_relation_lookups(ROOM),
            vec![RelationLookup {
                member_id: BOB_MEMBER.to_owned(),
                membership_event_id: "$bob-member-2".to_owned(),
            }],
            "the new event's annotations are looked up"
        );

        // A reaction relating to the previous event is still his.
        let mut reactions = manager.subscribe_reactions(ROOM, SLOT).unwrap();
        ingest(
            &mut manager,
            &bob_reacts("$r1", "$bob-member-1", "🐶", "dog"),
        );
        assert_eq!(reactions.try_recv().unwrap().member_id, BOB_MEMBER);

        // Bob leaves: hand gone.
        manager
            .set_current_sticky_state(
                ROOM,
                vec![member_event(ALICE, "ALICEDEV", &own_member_id, "$sticky-1")],
            )
            .await
            .unwrap();
        assert!(manager.raised_hands(ROOM, SLOT).unwrap().is_empty());
    }

    #[tokio::test]
    async fn backfill_restores_hands_but_never_replays_reactions() {
        let (mut manager, _) = joined_manager(join_params()).await;
        let mut reactions = manager.subscribe_reactions(ROOM, SLOT).unwrap();

        manager.on_relations_received(
            ROOM,
            "$bob-member-1",
            &[
                bob_reacts("$old-reaction", "$bob-member-1", "👏", "clapping"),
                bob_raises("$old-hand", "$bob-member-1", 100),
            ],
        );

        assert_eq!(hands(&mut manager), vec![("bob", "$old-hand".to_owned())]);
        assert_eq!(reactions.try_recv().unwrap_err(), TryRecvError::Empty);
    }

    #[tokio::test]
    async fn a_redaction_lowers_the_hand_it_raised() {
        let (mut manager, _) = joined_manager(join_params()).await;
        ingest(&mut manager, &bob_raises("$h1", "$bob-member-1", 5_000));
        assert_eq!(hands(&mut manager).len(), 1);

        manager.on_event_redacted(ROOM, "$something-else");
        assert_eq!(hands(&mut manager).len(), 1);

        manager.on_event_redacted(ROOM, "$h1");
        assert!(manager.raised_hands(ROOM, SLOT).unwrap().is_empty());
    }

    #[tokio::test]
    async fn hands_are_ordered_by_when_they_were_raised() {
        let (mut manager, _) = joined_manager(join_params()).await;
        manager.raise_hand(ROOM, SLOT).await.expect("raise");
        // Bob's hand went up before ours, by the server's clock.
        ingest(&mut manager, &bob_raises("$h1", "$bob-member-1", 1));

        let order: Vec<&str> = hands(&mut manager).iter().map(|(who, _)| *who).collect();
        assert_eq!(order, vec!["bob", "alice"]);
    }

    #[tokio::test]
    async fn the_hand_follows_our_membership_event_across_a_refresh() {
        // A zero lifetime makes every heartbeat refresh the sticky membership.
        let params = JoinSessionParams {
            sticky_duration_ms: Some(0),
            ..join_params()
        };
        let (mut manager, sender) = joined_manager(params).await;
        manager.raise_hand(ROOM, SLOT).await.expect("raise");
        assert_eq!(
            manager
                .own_raised_hand(ROOM, SLOT)
                .unwrap()
                .annotated_membership_event_id,
            "$sticky-1"
        );

        assert!(manager.heartbeat(ROOM, SLOT).await);

        assert_eq!(
            manager.own_membership_event_id(ROOM, SLOT).as_deref(),
            Some("$sticky-2"),
            "the heartbeat refreshed the membership"
        );
        let sent = sender.room_events.lock().unwrap().clone();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[1].2, build_raised_hand_content("$sticky-2"));
        assert_eq!(
            sender.redactions.lock().unwrap().clone(),
            vec![(ROOM.to_owned(), "$room-1".to_owned(), None)],
            "the annotation on the old membership event is redacted"
        );
        assert_eq!(hands(&mut manager), vec![("alice", "$room-2".to_owned())]);
        let own = manager.own_raised_hand(ROOM, SLOT).unwrap();
        assert_eq!(own.annotated_membership_event_id, "$sticky-2");
        assert_eq!(own.reaction_event_id, "$room-2");

        // Lowering redacts the current annotation, not the superseded one.
        manager.lower_hand(ROOM, SLOT).await.expect("lower");
        assert_eq!(sender.redactions.lock().unwrap()[1].1, "$room-2");
    }

    #[tokio::test]
    async fn leaving_lowers_our_hand_first() {
        let (mut manager, sender) = joined_manager(join_params()).await;
        manager.raise_hand(ROOM, SLOT).await.expect("raise");

        manager
            .leave(ROOM.to_owned(), SLOT.to_owned(), LeaveSessionParams::new())
            .await
            .expect("leave");

        assert_eq!(
            sender.redactions.lock().unwrap().clone(),
            vec![(ROOM.to_owned(), "$room-1".to_owned(), None)]
        );
        assert!(manager.own_raised_hand(ROOM, SLOT).is_none());
        assert!(matches!(
            manager.raise_hand(ROOM, SLOT).await,
            Err(ReactionError::NotJoined)
        ));
    }

    #[tokio::test]
    async fn disabled_reactions_neither_send_nor_receive() {
        let params = CallJoinParams {
            reactions: Some(ReactionsConfig {
                enabled: false,
                ..ReactionsConfig::default()
            }),
            ..CallJoinParams::new(join_params())
        };
        let (mut manager, sender) = joined_manager(params).await;
        let mut reactions = manager.subscribe_reactions(ROOM, SLOT).unwrap();

        assert!(matches!(
            manager.send_reaction(ROOM, SLOT, "👏", "clapping").await,
            Err(ReactionError::Disabled)
        ));
        assert!(matches!(
            manager.raise_hand(ROOM, SLOT).await,
            Err(ReactionError::Disabled)
        ));
        assert!(sender.room_events.lock().unwrap().is_empty());

        ingest(
            &mut manager,
            &bob_reacts("$r1", "$bob-member-1", "👏", "clapping"),
        );
        ingest(&mut manager, &bob_raises("$h1", "$bob-member-1", 5_000));
        assert_eq!(reactions.try_recv().unwrap_err(), TryRecvError::Empty);
        assert!(manager.raised_hands(ROOM, SLOT).unwrap().is_empty());
        assert!(manager.pending_relation_lookups(ROOM).is_empty());
    }

    /// Two slots in one room: a reaction names no slot, so the manager offers
    /// it to both sessions and only the one holding the member keeps it.
    #[tokio::test]
    async fn the_manager_routes_a_rooms_reactions_to_the_session_holding_the_member() {
        let sender = Arc::new(MockCallCommandSender::new());
        let mut manager = CallSessionManager::with_command_sender(sender);
        let other_slot = "m.call#OTHER";

        let in_room_slot = join_params();
        let in_other_slot = JoinSessionParams {
            slot_id: other_slot.to_owned(),
            ..join_params()
        };
        let alice_a = in_room_slot.membership_id();
        let alice_b = in_other_slot.membership_id();
        manager
            .join(CallJoinParams::new(JoinSessionParams {
                membership_id: Some(alice_a.clone()),
                ..in_room_slot
            }))
            .await
            .expect("join slot A");
        manager
            .join(CallJoinParams::new(JoinSessionParams {
                membership_id: Some(alice_b.clone()),
                ..in_other_slot
            }))
            .await
            .expect("join slot B");

        manager
            .set_current_sticky_state(
                ROOM,
                vec![member_event(BOB, "BOBDEV", BOB_MEMBER, "$bob-member-1")],
            )
            .await
            .expect("state applies");
        assert_eq!(manager.member_count(ROOM, SLOT), Some(1));
        assert_eq!(manager.member_count(ROOM, other_slot), Some(0));

        let mut slot_a = manager.subscribe_reactions(ROOM, SLOT).unwrap();
        let mut slot_b = manager.subscribe_reactions(ROOM, other_slot).unwrap();

        manager.on_room_timeline_events(
            ROOM,
            &[
                bob_reacts("$r1", "$bob-member-1", "👏", "clapping"),
                bob_raises("$h1", "$bob-member-1", 5_000),
            ],
        );

        assert_eq!(slot_a.try_recv().unwrap().member_id, BOB_MEMBER);
        assert_eq!(slot_b.try_recv().unwrap_err(), TryRecvError::Empty);
        assert_eq!(manager.raised_hands(ROOM, SLOT).unwrap().len(), 1);
        assert!(manager.raised_hands(ROOM, other_slot).unwrap().is_empty());
        assert_eq!(
            manager.pending_relation_lookups(ROOM),
            vec![RelationLookup {
                member_id: BOB_MEMBER.to_owned(),
                membership_event_id: "$bob-member-1".to_owned(),
            }]
        );

        manager.on_event_redacted(ROOM, "$h1");
        assert!(manager.raised_hands(ROOM, SLOT).unwrap().is_empty());

        assert!(matches!(
            manager.raise_hand(ROOM, "m.call#NOWHERE").await,
            Err(ReactionError::NoSession)
        ));
    }

    // ---- MSC4075 notify, through the manager ----

    mod notify {
        use super::*;

        fn open_call_slot() -> matrix_rtc_core::RawSlotEvent {
            matrix_rtc_core::RawSlotEvent {
                room_id: ROOM.to_owned(),
                slot_id: SLOT.to_owned(),
                content: serde_json::from_str(
                    r#"{ "status": "open", "application": { "type": "m.call" } }"#,
                )
                .expect("slot content must parse"),
            }
        }

        /// An open slot prescribing MSC4143 per-member media keys.
        fn encrypted_call_slot() -> matrix_rtc_core::RawSlotEvent {
            matrix_rtc_core::RawSlotEvent {
                room_id: ROOM.to_owned(),
                slot_id: SLOT.to_owned(),
                content: serde_json::from_str(
                    r#"{ "status": "open",
                         "application": { "type": "m.call" },
                         "encryption": { "type": "m.per_member" } }"#,
                )
                .expect("slot content must parse"),
            }
        }

        async fn encrypted_call_manager(sender: Arc<MockCallCommandSender>) -> Manager {
            let mut manager = CallSessionManager::with_command_sender(sender);
            manager.on_room_encryption_received(ROOM, true).await;
            manager
                .on_room_slots_received(ROOM, vec![encrypted_call_slot()])
                .await;
            manager
        }

        /// Feeds the current sticky state containing one peer, the way a host does.
        async fn admit_peer(
            manager: &mut Manager,
            user_id: &str,
            device_id: &str,
            member_id: &str,
        ) {
            manager
                .set_current_sticky_state(
                    ROOM,
                    vec![member_event(user_id, device_id, member_id, "$peer-member")],
                )
                .await
                .unwrap();
        }

        /// Joins as alice, asking for an MSC4075 notification.
        async fn join_and_notify(manager: &mut Manager, member_id: &str, notify: NotifyConfig) {
            let mut session = join_params();
            session.membership_id = Some(member_id.to_owned());
            manager
                .join(CallJoinParams {
                    session,
                    notify: Some(notify),
                    reactions: None,
                })
                .await
                .expect("join should succeed");
        }

        /// Every sticky send of the notification type, as `(content, duration_ms)`.
        fn notifications_sent(sender: &MockCallCommandSender) -> Vec<(Value, u64)> {
            sender
                .sticky_events
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, event_type, _, _)| event_type == NOTIFICATION_EVENT_TYPE)
                .map(|(_, _, content, duration_ms)| (content.clone(), *duration_ms))
                .collect()
        }

        /// Starting a call is what summons the room, and MSC4075 ties the
        /// notification to the membership that justifies it — so the event id the
        /// join's sticky send reported has to come back out as the relation target.
        #[tokio::test]
        async fn starting_a_call_notifies_the_room() {
            let sender = Arc::new(MockCallCommandSender::new());
            let mut manager = encrypted_call_manager(sender.clone()).await;

            let mut notify = NotifyConfig::ring();
            notify.intent = Some("video".to_owned());
            join_and_notify(&mut manager, "alice-a", notify).await;

            let sent = notifications_sent(&sender);
            assert_eq!(sent.len(), 1, "the call starter should notify exactly once");
            let (content, duration_ms) = &sent[0];

            let member_event_id = sender
                .sticky_events
                .lock()
                .unwrap()
                .iter()
                .position(|(_, event_type, _, _)| event_type == "m.rtc.member")
                .map(|index| format!("$sticky-{}", index + 1))
                .expect("the join sends a membership event");
            assert_eq!(
                content.pointer("/m.relates_to/event_id").unwrap(),
                &json!(member_event_id),
                "the relation must name our own membership event"
            );
            assert_eq!(
                content.pointer("/m.relates_to/rel_type").unwrap(),
                "m.reference"
            );
            assert_eq!(content.pointer("/application/type").unwrap(), "m.call");
            assert_eq!(
                content.pointer("/application/notification_type").unwrap(),
                "ring"
            );
            assert_eq!(
                content.pointer("/application/m.call.intent").unwrap(),
                "video"
            );
            assert_eq!(
                content.pointer("/application/device_id").unwrap(),
                "ALICEDEV"
            );
            assert_eq!(
                *duration_ms,
                2 * crate::notification::DEFAULT_RING_LIFETIME_MS,
                "MSC4075: the sticky entry must outlive the ring so acknowledgements can extend it"
            );
        }

        /// "Is anyone already here?" must not be answered `yes` by our own
        /// membership: the host feeds the room's whole sticky map, and once the
        /// homeserver has echoed our membership back that map contains *us*.
        #[tokio::test]
        async fn our_own_membership_does_not_count_as_somebody_else() {
            let sender = Arc::new(MockCallCommandSender::new());
            let mut manager = encrypted_call_manager(sender.clone()).await;

            // The echo of our own membership, under the very id we are about to
            // join with.
            admit_peer(&mut manager, ALICE, "ALICEDEV", "alice-a").await;
            join_and_notify(&mut manager, "alice-a", NotifyConfig::ring()).await;

            assert_eq!(
                notifications_sent(&sender).len(),
                1,
                "the only membership in the session is our own, so we are the one starting the call"
            );
        }

        /// A participation of ours from an earlier call in this process must not
        /// count either — including when the host reported no sending device for
        /// it, which is what an unencrypted room yields.
        #[tokio::test]
        async fn a_stale_participation_of_ours_does_not_count_either() {
            let sender = Arc::new(MockCallCommandSender::new());
            let mut manager = CallSessionManager::with_command_sender(sender.clone());
            manager
                .on_room_slots_received(ROOM, vec![open_call_slot()])
                .await;

            let mut stale = member_event(ALICE, "", "alice-old", "$old");
            stale.origin = EventOrigin::default();
            manager
                .set_current_sticky_state(ROOM, vec![stale])
                .await
                .unwrap();

            join_and_notify(&mut manager, "alice-new", NotifyConfig::ring()).await;

            assert_eq!(
                notifications_sent(&sender).len(),
                1,
                "the session held nothing but our own previous participation"
            );
        }

        /// The other edge of the same rule: our own user on a *different* device is
        /// an ordinary peer, and one already in the call started it.
        #[tokio::test]
        async fn another_device_of_ours_already_in_the_call_counts() {
            let sender = Arc::new(MockCallCommandSender::new());
            let mut manager = encrypted_call_manager(sender.clone()).await;

            admit_peer(&mut manager, ALICE, "ALICELAPTOP", "alice-laptop").await;
            join_and_notify(&mut manager, "alice-a", NotifyConfig::ring()).await;

            assert!(
                notifications_sent(&sender).is_empty(),
                "our laptop was already in the call, so our phone is joining, not starting"
            );
        }

        /// Joining a call someone else started must not ring the room a second
        /// time, even if the host asked for a notification.
        #[tokio::test]
        async fn joining_an_occupied_session_notifies_nobody() {
            let sender = Arc::new(MockCallCommandSender::new());
            let mut manager = encrypted_call_manager(sender.clone()).await;

            admit_peer(&mut manager, BOB, "BOBDEV", "bob-a").await;
            join_and_notify(&mut manager, "alice-a", NotifyConfig::ring()).await;

            assert!(
                notifications_sent(&sender).is_empty(),
                "bob was already in the session, so he started the call, not us"
            );
        }

        #[tokio::test]
        async fn joining_quietly_notifies_nobody() {
            let sender = Arc::new(MockCallCommandSender::new());
            let mut manager = encrypted_call_manager(sender.clone()).await;

            let mut session = join_params();
            session.membership_id = Some("alice-a".to_owned());
            manager.join(session.into()).await.expect("join");

            assert!(notifications_sent(&sender).is_empty());
        }
    }
}
