// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! [`CallRoomState`]: one room's core [`BaseRtcRoom`] plus the call application's
//! state in it — reactions and raised hands per slot, ringing on join. It wraps
//! the three core operations the call acts around (join, leave, keep-alive) and
//! follows each slot's joined memberships through a core
//! [`MembershipListener`](matrix_rtc_core::MembershipListener) registered on
//! that room alone. The feeder writes into it; hosts reach it through
//! [`crate::RtcRoom`] and [`crate::RtcCall`].

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use matrix_rtc_core::{
    ApplicationIntake, BaseRtcRoom, CommandError, JoinError, JoinSessionParams, JoinedMembership,
    LeaveError, LeaveSessionParams, MatrixBackend, RawTimelineEvent, RelationsRequest,
};
use tokio::sync::{broadcast, watch};

use crate::notification::{NotifyConfig, notify_session_started};
use crate::reactions::{
    ANNOTATION_EVENT_TYPE, ANNOTATION_RELATION_TYPE, Clock, REACTION_EVENT_TYPE, RaisedHand,
    ReactionError, ReactionsConfig, ReactionsState, ReceivedReaction, RelationLookup,
    build_raised_hand_content, build_reaction_content, first_grapheme,
};

/// Parameters for joining a call: the core's, plus what the call adds.
#[derive(Clone, Debug)]
pub(crate) struct CallJoinParams {
    /// The MatrixRTC join itself.
    pub rtc: JoinSessionParams,

    /// Ask for an MSC4075 notification to be sent with this join.
    ///
    /// `None` — the default — joins quietly, which is what joining a call
    /// someone else started does. Set it only when the user is *starting* the
    /// call: the notification is still suppressed if somebody is already in the
    /// session, but the intent to summon anyone at all is the application's to
    /// state.
    pub notify: Option<NotifyConfig>,

    /// How this session handles Element Call reactions and the raised hand.
    ///
    /// `None` — the default — is [`ReactionsConfig::default`]: enabled, with
    /// Element Call's three-second window. See [`crate::reactions`].
    pub reactions: Option<ReactionsConfig>,
}

impl CallJoinParams {
    /// A quiet join with default reactions.
    pub fn new(rtc: JoinSessionParams) -> Self {
        Self {
            rtc,
            notify: None,
            reactions: None,
        }
    }

    /// The reactions configuration to use: the configured one or the default.
    pub fn reactions(&self) -> ReactionsConfig {
        self.reactions.clone().unwrap_or_default()
    }
}

impl From<JoinSessionParams> for CallJoinParams {
    fn from(rtc: JoinSessionParams) -> Self {
        Self::new(rtc)
    }
}

/// What the core does not already report about our own participation.
#[derive(Clone, Debug)]
struct OwnCall {
    user_id: String,
}

/// The call-side state of one slot.
struct CallState {
    reactions: ReactionsState,
    /// The joined memberships as last published by the core.
    members: Vec<JoinedMembership>,
    own: Option<OwnCall>,
}

impl CallState {
    fn new(clock: &Clock, members: Vec<JoinedMembership>) -> Self {
        let mut reactions = ReactionsState::new();
        reactions.set_clock(clock.clone());
        reactions.sync_roster(&members);
        Self {
            reactions,
            members,
            own: None,
        }
    }

    fn sync_roster(&mut self, members: &[JoinedMembership]) {
        self.members = members.to_vec();
        self.reactions.sync_roster(members);
    }
}

/// Shared with the membership listener.
struct Shared {
    states: HashMap<String, CallState>,
    /// Handed to every [`ReactionsState`]; replaceable in tests.
    clock: Clock,
}

impl Shared {
    fn state(
        &mut self,
        slot_id: &str,
        members: impl FnOnce() -> Vec<JoinedMembership>,
    ) -> &mut CallState {
        let clock = self.clock.clone();
        self.states
            .entry(slot_id.to_owned())
            .or_insert_with(|| CallState::new(&clock, members()))
    }
}

/// One room's call state: owns the core room; everything the call does not
/// wrap is reached through [`Self::rtc`], [`Self::rtc_mut`] or `Deref`.
pub(crate) struct CallRoomState<T: MatrixBackend> {
    rtc: BaseRtcRoom<T>,
    shared: Arc<Mutex<Shared>>,
}

impl<T: MatrixBackend + 'static> CallRoomState<T> {
    /// `rtc` may already hold slot sessions; their joined memberships are
    /// replayed.
    pub fn new(mut rtc: BaseRtcRoom<T>) -> Self {
        let shared = Arc::new(Mutex::new(Shared {
            states: HashMap::new(),
            clock: Arc::new(crate::now_ms),
        }));
        let listener = {
            let shared = shared.clone();
            move |slot_id: &str, members: &[JoinedMembership]| {
                shared
                    .lock()
                    .unwrap()
                    .state(slot_id, Vec::new)
                    .sync_roster(members);
            }
        };
        rtc.add_membership_listener(Arc::new(listener));
        Self { rtc, shared }
    }

    /// A call over a fresh core room for `room_id`, sending through `backend`.
    pub fn with_backend(room_id: impl Into<String>, backend: Arc<T>) -> Self {
        Self::new(BaseRtcRoom::with_backend(room_id, backend))
    }

    /// The core room, for everything generic.
    pub fn rtc(&self) -> &BaseRtcRoom<T> {
        &self.rtc
    }

    /// Changing joined memberships through it is fine: the listener follows.
    pub fn rtc_mut(&mut self) -> &mut BaseRtcRoom<T> {
        &mut self.rtc
    }

    #[cfg(test)]
    pub(crate) fn set_clock(&self, clock: Clock) {
        let mut shared = self.shared.lock().unwrap();
        for state in shared.states.values_mut() {
            state.reactions.set_clock(clock.clone());
        }
        shared.clock = clock;
    }

    /// `None` when the core holds no session for the slot.
    fn with_state<R>(&self, slot_id: &str, f: impl FnOnce(&mut CallState) -> R) -> Option<R> {
        let members = self.rtc.subscribe_membership_snapshots(slot_id)?;
        let mut shared = self.shared.lock().unwrap();
        let state = shared.state(slot_id, || members.borrow().clone());
        Some(f(state))
    }

    fn for_each_slot(&self, mut f: impl FnMut(&str, &mut CallState)) {
        let mut shared = self.shared.lock().unwrap();
        for (slot_id, state) in shared.states.iter_mut() {
            f(slot_id, state);
        }
    }

    fn room_id(&self) -> String {
        self.rtc.room_id().to_owned()
    }

    fn backend(&self) -> Result<Arc<T>, CommandError> {
        self.rtc
            .backend()
            .cloned()
            .ok_or_else(|| CommandError::from_message("no backend configured"))
    }

    /// The core join, then the notification if we started the call. Returns
    /// the core join's membership event id.
    pub async fn join(&mut self, params: CallJoinParams) -> Result<String, JoinError> {
        let reactions = params.reactions();
        let CallJoinParams { rtc, notify, .. } = params;
        let slot_id = rtc.slot_id.clone();

        let member_event_id = self.rtc.join(rtc.clone()).await?;

        self.with_state(&slot_id, |state| {
            state.reactions.configure(reactions);
            state.reactions.reset_own();
            state.own = Some(OwnCall {
                user_id: rtc.user_id.clone(),
            });
        });

        if let Some(notify) = &notify {
            self.notify_session_started(notify, &rtc, &member_event_id)
                .await;
        }
        Ok(member_event_id)
    }

    /// Lowers our hand first, while the membership it annotates still stands.
    /// Best effort: peers drop the hand with the membership anyway.
    pub async fn leave(
        &mut self,
        slot_id: &str,
        params: LeaveSessionParams,
    ) -> Result<(), LeaveError> {
        let room_id = self.room_id();
        let hand_up = self
            .with_state(slot_id, |state| state.reactions.own_raised_hand().is_some())
            .unwrap_or(false);
        if hand_up
            && self.rtc.own_member_id(slot_id).is_some()
            && let Err(error) = self.lower_hand(slot_id).await
        {
            log::warn!(
                "[{room_id}/{slot_id}] the raised hand was not lowered before leaving ({error}); \
                 peers drop it with the membership",
            );
        }

        self.rtc.leave(slot_id, params).await?;

        self.with_state(slot_id, |state| {
            state.own = None;
            state.reactions.reset_own();
        });
        Ok(())
    }

    /// Then re-annotates our hand if the sticky refresh moved our membership.
    pub async fn keep_alive(&mut self, slot_id: &str) -> bool {
        let joined = self.rtc.keep_alive(slot_id).await;
        if joined {
            self.reannotate_hand_if_moved(slot_id).await;
        }
        joined
    }

    /// Sends the MSC4075 notification that summons the room to this session;
    /// see [`notify_session_started`] for when it is suppressed.
    async fn notify_session_started(
        &self,
        notify: &NotifyConfig,
        params: &JoinSessionParams,
        member_event_id: &str,
    ) {
        let Ok(backend) = self.backend() else {
            return;
        };
        let members = self
            .rtc
            .subscribe_membership_snapshots(&params.slot_id)
            .map(|snapshots| snapshots.borrow().clone())
            .unwrap_or_default();
        notify_session_started(
            backend.as_ref(),
            self.rtc.room_id(),
            notify,
            params,
            &members,
            member_event_id,
        )
        .await;
    }

    // ---- Reactions and raised hands (see `crate::reactions`) ----
    //
    // A reaction relates to a membership event and names no slot, so every
    // slot of the room is offered each event and keeps the ones that relate to
    // its own members.

    /// Applies message-like room events — `io.element.call.reaction` and
    /// `m.reaction` — to every slot. Other event types are ignored, so a host
    /// may forward without filtering.
    pub fn on_timeline_events(&mut self, events: &[RawTimelineEvent]) {
        self.for_each_slot(|_, state| {
            for event in events {
                state.reactions.ingest(event, &state.members, false);
            }
        });
    }

    /// Lowers whichever hand the redacted `event_id` raised, in every slot.
    pub fn on_event_redacted(&mut self, event_id: &str) {
        let room_id = self.room_id();
        self.for_each_slot(|slot_id, state| {
            if state.reactions.on_event_redacted(event_id) {
                log::info!(
                    "[{room_id}/{slot_id}] our raised hand was lowered by a redaction from \
                     elsewhere"
                );
            }
        });
    }

    /// Feeds the annotations of one membership event back, as fetched in
    /// answer to [`Self::pending_relation_lookups`]. Only raised hands are taken
    /// from them: an old emoji reaction is not replayed.
    pub fn on_relations_received(&mut self, target_event_id: &str, events: &[RawTimelineEvent]) {
        self.for_each_slot(|_, state| {
            state
                .reactions
                .on_relations_received(target_event_id, events, &state.members);
        });
    }

    /// Membership events whose annotations the host has not fetched yet,
    /// across the room's slots.
    ///
    /// Answer each with the event's `/relations` (`rel_type=m.annotation`,
    /// `event_type=m.reaction`) through [`Self::on_relations_received`]; that is
    /// how hands raised before we joined become visible. Asking is what marks an
    /// id as fetched, so a failed fetch is retried by asking again.
    pub fn pending_relation_lookups(&self) -> Vec<RelationLookup> {
        let mut seen = HashSet::new();
        let mut lookups = Vec::new();
        self.for_each_slot(|slot_id, state| {
            let own_member_id = self.rtc.own_member_id(slot_id);
            lookups.extend(
                state
                    .reactions
                    .pending_relation_lookups(&state.members, own_member_id.as_deref())
                    .into_iter()
                    .filter(|lookup| seen.insert(lookup.membership_event_id.clone())),
            );
        });
        lookups
    }

    /// Where our reactions relate to: the user we joined as, our `member.id`
    /// and our current membership event.
    fn own_relation_target(
        &self,
        slot_id: &str,
    ) -> Result<(OwnCall, String, String), ReactionError> {
        let own = self
            .with_state(slot_id, |state| state.own.clone())
            .ok_or(ReactionError::NoSession)?
            .ok_or(ReactionError::NotJoined)?;
        let member_id = self
            .rtc
            .own_member_id(slot_id)
            .ok_or(ReactionError::NotJoined)?;
        let membership_event_id = self
            .rtc
            .own_membership_event_id(slot_id)
            .ok_or(ReactionError::NotJoined)?;
        Ok((own, member_id, membership_event_id))
    }

    /// Sends an emoji reaction from one slot, relating
    /// it to our current membership event.
    ///
    /// `name` is what peers select a sound by (see
    /// [`KNOWN_REACTIONS`](crate::reactions::KNOWN_REACTIONS)); an unknown name
    /// plays Element Call's generic sound. Only the first grapheme of `emoji`
    /// is sent. Returns the event id.
    ///
    /// Refused inside the send cooldown: peers would drop the reaction anyway.
    pub async fn send_reaction(
        &mut self,
        slot_id: &str,
        emoji: &str,
        name: &str,
    ) -> Result<String, ReactionError> {
        let room_id = self.room_id();
        self.with_state(slot_id, |_| ())
            .ok_or(ReactionError::NoSession)?;
        let emoji = first_grapheme(emoji);
        if emoji.is_empty() {
            return Err(ReactionError::EmptyEmoji);
        }
        self.with_state(slot_id, |state| state.reactions.check_send_allowed())
            .ok_or(ReactionError::NoSession)??;
        let (_, _, membership_event_id) = self.own_relation_target(slot_id)?;
        let backend = self.backend()?;

        let event_id = backend
            .send_room_event(
                self.room_id(),
                REACTION_EVENT_TYPE.to_owned(),
                build_reaction_content(&membership_event_id, emoji, name),
            )
            .await?;
        self.with_state(slot_id, |state| state.reactions.record_sent());
        log::debug!("[{room_id}/{slot_id}] sent reaction {emoji} ({name}) as {event_id}");
        Ok(event_id)
    }

    /// Raises our hand in one slot: annotates our
    /// current membership event with
    /// [`RAISED_HAND_KEY`](crate::reactions::RAISED_HAND_KEY). Idempotent while
    /// it is up. Shows in [`Self::raised_hands`] right away rather than waiting
    /// for the echo.
    pub async fn raise_hand(&mut self, slot_id: &str) -> Result<(), ReactionError> {
        let room_id = self.room_id();
        let (enabled, already_up) = self
            .with_state(slot_id, |state| {
                (
                    state.reactions.config().enabled,
                    state.reactions.own_raised_hand().is_some(),
                )
            })
            .ok_or(ReactionError::NoSession)?;
        if !enabled {
            return Err(ReactionError::Disabled);
        }
        if already_up {
            return Ok(());
        }
        let (own, member_id, membership_event_id) = self.own_relation_target(slot_id)?;
        let backend = self.backend()?;

        let event_id = backend
            .send_room_event(
                self.room_id(),
                ANNOTATION_EVENT_TYPE.to_owned(),
                build_raised_hand_content(&membership_event_id),
            )
            .await?;
        log::info!("[{room_id}/{slot_id}] hand raised ({event_id})");
        self.with_state(slot_id, |state| {
            state.reactions.set_own_raised_hand(
                &member_id,
                &own.user_id,
                event_id,
                membership_event_id,
            );
        });
        Ok(())
    }

    /// Lowers our hand in one slot by redacting the
    /// annotation. A no-op when it is not up.
    pub async fn lower_hand(&mut self, slot_id: &str) -> Result<(), ReactionError> {
        let room_id = self.room_id();
        let Some(hand) = self
            .with_state(slot_id, |state| state.reactions.own_raised_hand().cloned())
            .ok_or(ReactionError::NoSession)?
        else {
            return Ok(());
        };
        let member_id = self
            .rtc
            .own_member_id(slot_id)
            .ok_or(ReactionError::NotJoined)?;
        let backend = self.backend()?;

        backend
            .redact_event(self.room_id(), hand.reaction_event_id.clone(), None)
            .await?;
        log::info!(
            "[{room_id}/{slot_id}] hand lowered ({} redacted)",
            hand.reaction_event_id
        );
        self.with_state(slot_id, |state| {
            state.reactions.clear_own_raised_hand(&member_id);
        });
        Ok(())
    }

    /// Raises our hand again on our new membership event after a sticky
    /// refresh, and redacts the annotation on the old one.
    ///
    /// Element Call drops a raised hand whose membership event has moved on
    /// and looks for one on the new event instead, so a hand that stayed on
    /// the join event would be lowered for us at the first refresh. Peers may
    /// see the hand drop for one round trip in between; that is inherent to
    /// the protocol. A failed re-send is retried on the next keep-alive tick, since
    /// the ids still differ.
    async fn reannotate_hand_if_moved(&mut self, slot_id: &str) {
        let room_id = self.room_id();
        let Some(Some(hand)) =
            self.with_state(slot_id, |state| state.reactions.own_raised_hand().cloned())
        else {
            return;
        };
        let Ok((own, member_id, current)) = self.own_relation_target(slot_id) else {
            return;
        };
        if current == hand.annotated_membership_event_id {
            return;
        }
        let Ok(backend) = self.backend() else {
            return;
        };

        log::debug!(
            "[{room_id}/{slot_id}] our membership event moved ({} -> {current}); raising the \
             hand on it again",
            hand.annotated_membership_event_id,
        );
        match backend
            .send_room_event(
                self.room_id(),
                ANNOTATION_EVENT_TYPE.to_owned(),
                build_raised_hand_content(&current),
            )
            .await
        {
            Ok(event_id) => {
                self.with_state(slot_id, |state| {
                    state.reactions.set_own_raised_hand(
                        &member_id,
                        &own.user_id,
                        event_id,
                        current,
                    );
                });
                if let Err(error) = backend
                    .redact_event(self.room_id(), hand.reaction_event_id.clone(), None)
                    .await
                {
                    log::warn!(
                        "[{room_id}/{slot_id}] the previous raised-hand annotation {} was not \
                         redacted ({error}); it relates to a superseded membership event, so \
                         peers ignore it",
                        hand.reaction_event_id,
                    );
                }
            }
            Err(error) => log::warn!(
                "[{room_id}/{slot_id}] could not raise the hand again on the refreshed \
                 membership ({error}); retrying on the next keep-alive",
            ),
        }
    }

    /// How one slot handles reactions, as set by the
    /// current join, or `None` if the slot has no session.
    pub fn reactions_config(&self, slot_id: &str) -> Option<ReactionsConfig> {
        self.with_state(slot_id, |state| state.reactions.config().clone())
    }

    /// The raised hands of one slot, oldest first, or
    /// `None` if the slot has no session.
    pub fn raised_hands(&self, slot_id: &str) -> Option<Vec<RaisedHand>> {
        self.with_state(slot_id, |state| state.reactions.raised_hands())
    }

    /// Subscribes to the raised hands of one slot,
    /// oldest first and updated as a whole on every change, or `None` if there
    /// is no such session.
    pub fn subscribe_raised_hands(
        &self,
        slot_id: &str,
    ) -> Option<watch::Receiver<Vec<RaisedHand>>> {
        self.with_state(slot_id, |state| state.reactions.subscribe_raised_hands())
    }

    /// Subscribes to the emoji reactions of one slot,
    /// our own echo included, or `None` if the slot has no session. A lagging
    /// subscriber loses the oldest ones, which for a three-second visual is the
    /// right trade.
    pub fn subscribe_reactions(
        &self,
        slot_id: &str,
    ) -> Option<broadcast::Receiver<ReceivedReaction>> {
        self.with_state(slot_id, |state| state.reactions.subscribe_reactions())
    }

    #[cfg(test)]
    pub(crate) fn own_raised_hand(&self, slot_id: &str) -> Option<crate::reactions::OwnRaisedHand> {
        self.with_state(slot_id, |state| state.reactions.own_raised_hand().cloned())
            .flatten()
    }
}

/// `join`, `leave` and `keep_alive` are inherent, so they win over the core's;
/// `join` takes [`CallJoinParams`], so code written against the core's does not
/// compile rather than silently skipping the notification.
impl<T: MatrixBackend> std::ops::Deref for CallRoomState<T> {
    type Target = BaseRtcRoom<T>;

    fn deref(&self) -> &Self::Target {
        &self.rtc
    }
}

impl<T: MatrixBackend> std::ops::DerefMut for CallRoomState<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rtc
    }
}

impl<T: MatrixBackend + 'static> ApplicationIntake<T> for CallRoomState<T> {
    fn rtc(&mut self) -> &mut BaseRtcRoom<T> {
        &mut self.rtc
    }

    fn timeline_event_types(&self) -> Vec<String> {
        vec![
            REACTION_EVENT_TYPE.to_owned(),
            ANNOTATION_EVENT_TYPE.to_owned(),
        ]
    }

    fn on_timeline_events(&mut self, events: &[RawTimelineEvent]) {
        CallRoomState::on_timeline_events(self, events);
    }

    fn on_event_redacted(&mut self, event_id: &str) {
        CallRoomState::on_event_redacted(self, event_id);
    }

    fn pending_relations(&self) -> Vec<RelationsRequest> {
        self.pending_relation_lookups()
            .into_iter()
            .map(|lookup| RelationsRequest {
                event_id: lookup.membership_event_id,
                rel_type: ANNOTATION_RELATION_TYPE.to_owned(),
                event_type: ANNOTATION_EVENT_TYPE.to_owned(),
            })
            .collect()
    }

    fn on_relations_received(&mut self, target_event_id: &str, events: &[RawTimelineEvent]) {
        CallRoomState::on_relations_received(self, target_event_id, events);
    }
}

#[cfg(test)]
mod tests;
