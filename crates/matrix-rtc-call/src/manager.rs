// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! [`CallSessionManager`]: the call application over a core
//! [`RtcSessionManager`]. It wraps the three core operations the call acts
//! around (join, leave, heartbeat) and follows each session's joined
//! memberships through a core [`MembershipListener`](matrix_rtc_core::MembershipListener).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use matrix_rtc_core::{
    ApplicationIntake, CommandError, JoinError, JoinSessionParams, JoinedMembership, LeaveError,
    LeaveSessionParams, RawTimelineEvent, RelationsRequest, RtcCommandSender, RtcSessionManager,
};
use tokio::sync::{broadcast, watch};

use crate::notification::{
    NOTIFICATION_EVENT_TYPE, NotifyConfig, build_notification_content,
    notification_sticky_duration_ms,
};
use crate::reactions::{
    ANNOTATION_EVENT_TYPE, ANNOTATION_RELATION_TYPE, Clock, REACTION_EVENT_TYPE, RaisedHand,
    ReactionError, ReactionsConfig, ReactionsState, ReceivedReaction, RelationLookup,
    build_raised_hand_content, build_reaction_content, first_grapheme,
};

/// Parameters for joining a call: the core's, plus what the call adds.
#[derive(Clone, Debug)]
pub struct CallJoinParams {
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

type SessionKey = (String, String);

fn key(room_id: &str, slot_id: &str) -> SessionKey {
    (room_id.to_owned(), slot_id.to_owned())
}

/// What the core does not already report about our own participation.
#[derive(Clone, Debug)]
struct OwnCall {
    user_id: String,
}

/// The call-side state of one `(room, slot)` session.
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
    states: HashMap<SessionKey, CallState>,
    /// Handed to every [`ReactionsState`]; replaceable in tests.
    clock: Clock,
}

impl Shared {
    fn state(
        &mut self,
        key: SessionKey,
        members: impl FnOnce() -> Vec<JoinedMembership>,
    ) -> &mut CallState {
        let clock = self.clock.clone();
        self.states
            .entry(key)
            .or_insert_with(|| CallState::new(&clock, members()))
    }
}

/// Owns the core manager; everything the call does not wrap is reached through
/// [`Self::rtc`], [`Self::rtc_mut`] or `Deref`.
pub struct CallSessionManager<T: RtcCommandSender> {
    rtc: RtcSessionManager<T>,
    shared: Arc<Mutex<Shared>>,
}

impl<T: RtcCommandSender + 'static> CallSessionManager<T> {
    /// `rtc` may already hold sessions; their joined memberships are replayed.
    pub fn new(mut rtc: RtcSessionManager<T>) -> Self {
        let shared = Arc::new(Mutex::new(Shared {
            states: HashMap::new(),
            clock: Arc::new(crate::now_ms),
        }));
        let listener = {
            let shared = shared.clone();
            move |room_id: &str, slot_id: &str, members: &[JoinedMembership]| {
                shared
                    .lock()
                    .unwrap()
                    .state(key(room_id, slot_id), Vec::new)
                    .sync_roster(members);
            }
        };
        rtc.add_membership_listener(Arc::new(listener));
        Self { rtc, shared }
    }

    /// A call layer over a fresh core manager sending through `command_sender`.
    pub fn with_command_sender(command_sender: Arc<T>) -> Self {
        Self::new(RtcSessionManager::with_command_sender(command_sender))
    }

    /// The core manager, for everything generic.
    pub fn rtc(&self) -> &RtcSessionManager<T> {
        &self.rtc
    }

    /// Changing joined memberships through it is fine: the listener follows.
    pub fn rtc_mut(&mut self) -> &mut RtcSessionManager<T> {
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

    /// `None` when the core holds no such session.
    fn with_state<R>(
        &self,
        room_id: &str,
        slot_id: &str,
        f: impl FnOnce(&mut CallState) -> R,
    ) -> Option<R> {
        let members = self.rtc.subscribe_membership_snapshots(room_id, slot_id)?;
        let mut shared = self.shared.lock().unwrap();
        let state = shared.state(key(room_id, slot_id), || members.borrow().clone());
        Some(f(state))
    }

    fn for_each_in_room(&self, room_id: &str, mut f: impl FnMut(&str, &mut CallState)) {
        let mut shared = self.shared.lock().unwrap();
        for ((room, slot), state) in shared.states.iter_mut() {
            if room == room_id {
                f(slot, state);
            }
        }
    }

    fn command_sender(&self) -> Result<Arc<T>, CommandError> {
        self.rtc
            .command_sender()
            .cloned()
            .ok_or_else(|| CommandError::from_message("no command sender configured"))
    }

    /// The core join, then the notification if we started the call.
    pub async fn join(&mut self, params: CallJoinParams) -> Result<(), JoinError> {
        let reactions = params.reactions();
        let CallJoinParams { rtc, notify, .. } = params;
        let (room_id, slot_id) = (rtc.room_id.clone(), rtc.slot_id.clone());

        self.rtc.join(rtc.clone()).await?;

        self.with_state(&room_id, &slot_id, |state| {
            state.reactions.configure(reactions);
            state.reactions.reset_own();
            state.own = Some(OwnCall {
                user_id: rtc.user_id.clone(),
            });
        });

        if let Some(notify) = &notify {
            self.notify_session_started(notify, &rtc).await;
        }
        Ok(())
    }

    /// Lowers our hand first, while the membership it annotates still stands.
    /// Best effort: peers drop the hand with the membership anyway.
    pub async fn leave(
        &mut self,
        room_id: String,
        slot_id: String,
        params: LeaveSessionParams,
    ) -> Result<(), LeaveError> {
        let hand_up = self
            .with_state(&room_id, &slot_id, |state| {
                state.reactions.own_raised_hand().is_some()
            })
            .unwrap_or(false);
        if hand_up
            && self.rtc.own_member_id(&room_id, &slot_id).is_some()
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

        self.with_state(&room_id, &slot_id, |state| {
            state.own = None;
            state.reactions.reset_own();
        });
        Ok(())
    }

    /// Then re-annotates our hand if the sticky refresh moved our membership.
    pub async fn heartbeat(&mut self, room_id: &str, slot_id: &str) -> bool {
        let joined = self.rtc.heartbeat(room_id, slot_id).await;
        if joined {
            self.reannotate_hand_if_moved(room_id, slot_id).await;
        }
        joined
    }

    /// Whether a roster entry is this device's own participation — the current
    /// one or an earlier one that is still sticky.
    ///
    /// A candidate whose sending device the host did not report counts as ours
    /// when the *user* matches. See [`Self::notify_session_started`] for why
    /// erring that way is the right trade here — it is the opposite of the rule
    /// the core applies to drop a superseded participation of ours from the
    /// roster, which leaves such a candidate in rather than dropping a genuine
    /// peer.
    fn is_own_participation(member: &JoinedMembership, params: &JoinSessionParams) -> bool {
        member.sender == params.user_id
            && member
                .origin
                .sender_device_id()
                .is_none_or(|device_id| device_id == params.device_id)
    }

    /// Sends the MSC4075 notification that summons the room to this session.
    ///
    /// Called at the tail of [`Self::join`], where the roster has just been
    /// republished. Never fails the join: the user is in the call whether or
    /// not anyone else was told about it.
    async fn notify_session_started(&self, notify: &NotifyConfig, params: &JoinSessionParams) {
        let tag = format!("{}/{}/{}", params.room_id, params.slot_id, params.device_id);

        // MSC4075 leaves who sends the notification open, but every joiner
        // sending one would ring the room once per participant. Only the member
        // who *starts* the session does — matching what Element Call does.
        //
        // The question is whether anyone *else* is here, so our own
        // participations have to come out of the count first. Both kinds occur:
        // the host feeds the room's whole sticky map, which contains our own
        // membership as soon as the homeserver echoes it back, and a session
        // outlives `leave()` keeping the previous call's membership as a
        // candidate. Counting either concludes that somebody else started the
        // call and stays silent — the caller hits "call" and no phone rings.
        //
        // The core already drops the stale ones from the roster, but only where
        // the sending device is known, and an unencrypted room reports none. So
        // a membership from our own user with no device attributed is treated
        // as ours here too. That is a *wider* rule than the roster's on
        // purpose: it can only misfire on another device of our own user in an
        // unencrypted room, where the cost is one extra ring — against a silent
        // failure to ring at all, which is the bug this replaced.
        let members = self
            .rtc
            .subscribe_membership_snapshots(&params.room_id, &params.slot_id)
            .map(|snapshots| snapshots.borrow().clone())
            .unwrap_or_default();
        let others = members
            .iter()
            .filter(|member| !Self::is_own_participation(member, params))
            .count();
        if others > 0 {
            log::info!(
                "[{tag}] not notifying: {others} member(s) were already in the session, so \
                 somebody else started it",
            );
            return;
        }

        // MSC4075 ties the notification to the membership that justifies it.
        let Some(member_event_id) = self
            .rtc
            .own_membership_event_id(&params.room_id, &params.slot_id)
        else {
            log::warn!("[{tag}] not notifying: our membership event id is unknown");
            return;
        };
        let Ok(command_sender) = self.command_sender() else {
            return;
        };

        let content = build_notification_content(
            notify,
            params.application.application_type().unwrap_or_default(),
            &params.user_id,
            &params.device_id,
            &member_event_id,
            crate::now_ms(),
        );

        log::info!(
            "[{tag}] notifying the room: {}",
            notify.notification_type.as_str(),
        );

        if let Err(error) = command_sender
            .send_sticky_event(
                params.room_id.clone(),
                NOTIFICATION_EVENT_TYPE.to_owned(),
                content,
                notification_sticky_duration_ms(notify.lifetime_ms()),
            )
            .await
        {
            log::warn!(
                "[{tag}] the session-started notification was not sent ({error:?}); the call \
                 itself is unaffected",
            );
        }
    }

    // ---- Reactions and raised hands (see `crate::reactions`) ----
    //
    // Inbound reactions are routed by *room*: a reaction relates to a
    // membership event and names no slot, so every session of the room is
    // offered each event and keeps the ones that relate to its own members.

    /// Applies message-like room events — `io.element.call.reaction` and
    /// `m.reaction` — to every session of `room_id`. Other event types are
    /// ignored, so a host may forward without filtering.
    pub fn on_room_timeline_events(&mut self, room_id: &str, events: &[RawTimelineEvent]) {
        self.for_each_in_room(room_id, |_, state| {
            for event in events {
                state.reactions.ingest(event, &state.members, false);
            }
        });
    }

    /// Lowers whichever hand the redacted `event_id` raised, in every session
    /// of `room_id`.
    pub fn on_event_redacted(&mut self, room_id: &str, event_id: &str) {
        self.for_each_in_room(room_id, |slot_id, state| {
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
    pub fn on_relations_received(
        &mut self,
        room_id: &str,
        target_event_id: &str,
        events: &[RawTimelineEvent],
    ) {
        self.for_each_in_room(room_id, |_, state| {
            state
                .reactions
                .on_relations_received(target_event_id, events, &state.members);
        });
    }

    /// Membership events in `room_id` whose annotations the host has not
    /// fetched yet, across its sessions.
    ///
    /// Answer each with the event's `/relations` (`rel_type=m.annotation`,
    /// `event_type=m.reaction`) through [`Self::on_relations_received`]; that is
    /// how hands raised before we joined become visible. Asking is what marks an
    /// id as fetched, so a failed fetch is retried by asking again.
    pub fn pending_relation_lookups(&self, room_id: &str) -> Vec<RelationLookup> {
        let mut seen = HashSet::new();
        let mut lookups = Vec::new();
        self.for_each_in_room(room_id, |slot_id, state| {
            let own_member_id = self.rtc.own_member_id(room_id, slot_id);
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
        room_id: &str,
        slot_id: &str,
    ) -> Result<(OwnCall, String, String), ReactionError> {
        let own = self
            .with_state(room_id, slot_id, |state| state.own.clone())
            .ok_or(ReactionError::NoSession)?
            .ok_or(ReactionError::NotJoined)?;
        let member_id = self
            .rtc
            .own_member_id(room_id, slot_id)
            .ok_or(ReactionError::NotJoined)?;
        let membership_event_id = self
            .rtc
            .own_membership_event_id(room_id, slot_id)
            .ok_or(ReactionError::NotJoined)?;
        Ok((own, member_id, membership_event_id))
    }

    /// Sends an emoji reaction from one `(room_id, slot_id)` session, relating
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
        room_id: &str,
        slot_id: &str,
        emoji: &str,
        name: &str,
    ) -> Result<String, ReactionError> {
        self.with_state(room_id, slot_id, |_| ())
            .ok_or(ReactionError::NoSession)?;
        let emoji = first_grapheme(emoji);
        if emoji.is_empty() {
            return Err(ReactionError::EmptyEmoji);
        }
        self.with_state(room_id, slot_id, |state| {
            state.reactions.check_send_allowed()
        })
        .ok_or(ReactionError::NoSession)??;
        let (_, _, membership_event_id) = self.own_relation_target(room_id, slot_id)?;
        let command_sender = self.command_sender()?;

        let event_id = command_sender
            .send_room_event(
                room_id.to_owned(),
                REACTION_EVENT_TYPE.to_owned(),
                build_reaction_content(&membership_event_id, emoji, name),
            )
            .await?;
        self.with_state(room_id, slot_id, |state| state.reactions.record_sent());
        log::debug!("[{room_id}/{slot_id}] sent reaction {emoji} ({name}) as {event_id}");
        Ok(event_id)
    }

    /// Raises our hand in one `(room_id, slot_id)` session: annotates our
    /// current membership event with
    /// [`RAISED_HAND_KEY`](crate::reactions::RAISED_HAND_KEY). Idempotent while
    /// it is up. Shows in [`Self::raised_hands`] right away rather than waiting
    /// for the echo.
    pub async fn raise_hand(&mut self, room_id: &str, slot_id: &str) -> Result<(), ReactionError> {
        let (enabled, already_up) = self
            .with_state(room_id, slot_id, |state| {
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
        let (own, member_id, membership_event_id) = self.own_relation_target(room_id, slot_id)?;
        let command_sender = self.command_sender()?;

        let event_id = command_sender
            .send_room_event(
                room_id.to_owned(),
                ANNOTATION_EVENT_TYPE.to_owned(),
                build_raised_hand_content(&membership_event_id),
            )
            .await?;
        log::info!("[{room_id}/{slot_id}] hand raised ({event_id})");
        self.with_state(room_id, slot_id, |state| {
            state.reactions.set_own_raised_hand(
                &member_id,
                &own.user_id,
                event_id,
                membership_event_id,
            );
        });
        Ok(())
    }

    /// Lowers our hand in one `(room_id, slot_id)` session by redacting the
    /// annotation. A no-op when it is not up.
    pub async fn lower_hand(&mut self, room_id: &str, slot_id: &str) -> Result<(), ReactionError> {
        let Some(hand) = self
            .with_state(room_id, slot_id, |state| {
                state.reactions.own_raised_hand().cloned()
            })
            .ok_or(ReactionError::NoSession)?
        else {
            return Ok(());
        };
        let member_id = self
            .rtc
            .own_member_id(room_id, slot_id)
            .ok_or(ReactionError::NotJoined)?;
        let command_sender = self.command_sender()?;

        command_sender
            .redact_event(room_id.to_owned(), hand.reaction_event_id.clone(), None)
            .await?;
        log::info!(
            "[{room_id}/{slot_id}] hand lowered ({} redacted)",
            hand.reaction_event_id
        );
        self.with_state(room_id, slot_id, |state| {
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
    /// the protocol. A failed re-send is retried on the next heartbeat, since
    /// the ids still differ.
    async fn reannotate_hand_if_moved(&mut self, room_id: &str, slot_id: &str) {
        let Some(Some(hand)) = self.with_state(room_id, slot_id, |state| {
            state.reactions.own_raised_hand().cloned()
        }) else {
            return;
        };
        let Ok((own, member_id, current)) = self.own_relation_target(room_id, slot_id) else {
            return;
        };
        if current == hand.annotated_membership_event_id {
            return;
        }
        let Ok(command_sender) = self.command_sender() else {
            return;
        };

        log::debug!(
            "[{room_id}/{slot_id}] our membership event moved ({} -> {current}); raising the \
             hand on it again",
            hand.annotated_membership_event_id,
        );
        match command_sender
            .send_room_event(
                room_id.to_owned(),
                ANNOTATION_EVENT_TYPE.to_owned(),
                build_raised_hand_content(&current),
            )
            .await
        {
            Ok(event_id) => {
                self.with_state(room_id, slot_id, |state| {
                    state.reactions.set_own_raised_hand(
                        &member_id,
                        &own.user_id,
                        event_id,
                        current,
                    );
                });
                if let Err(error) = command_sender
                    .redact_event(room_id.to_owned(), hand.reaction_event_id.clone(), None)
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
                 membership ({error}); retrying on the next heartbeat",
            ),
        }
    }

    /// How one `(room_id, slot_id)` session handles reactions, as set by the
    /// current join, or `None` if there is no such session.
    pub fn reactions_config(&self, room_id: &str, slot_id: &str) -> Option<ReactionsConfig> {
        self.with_state(room_id, slot_id, |state| state.reactions.config().clone())
    }

    /// The raised hands of one `(room_id, slot_id)` session, oldest first, or
    /// `None` if there is no such session.
    pub fn raised_hands(&self, room_id: &str, slot_id: &str) -> Option<Vec<RaisedHand>> {
        self.with_state(room_id, slot_id, |state| state.reactions.raised_hands())
    }

    /// Subscribes to the raised hands of one `(room_id, slot_id)` session,
    /// oldest first and updated as a whole on every change, or `None` if there
    /// is no such session.
    pub fn subscribe_raised_hands(
        &self,
        room_id: &str,
        slot_id: &str,
    ) -> Option<watch::Receiver<Vec<RaisedHand>>> {
        self.with_state(room_id, slot_id, |state| {
            state.reactions.subscribe_raised_hands()
        })
    }

    /// Subscribes to the emoji reactions of one `(room_id, slot_id)` session,
    /// our own echo included, or `None` if there is no such session. A lagging
    /// subscriber loses the oldest ones, which for a three-second visual is the
    /// right trade.
    pub fn subscribe_reactions(
        &self,
        room_id: &str,
        slot_id: &str,
    ) -> Option<broadcast::Receiver<ReceivedReaction>> {
        self.with_state(room_id, slot_id, |state| {
            state.reactions.subscribe_reactions()
        })
    }

    #[cfg(test)]
    pub(crate) fn own_raised_hand(
        &self,
        room_id: &str,
        slot_id: &str,
    ) -> Option<crate::reactions::OwnRaisedHand> {
        self.with_state(room_id, slot_id, |state| {
            state.reactions.own_raised_hand().cloned()
        })
        .flatten()
    }
}

/// `join`, `leave` and `heartbeat` are inherent, so they win over the core's;
/// `join` takes [`CallJoinParams`], so code written against the core's does not
/// compile rather than silently skipping the notification.
impl<T: RtcCommandSender> std::ops::Deref for CallSessionManager<T> {
    type Target = RtcSessionManager<T>;

    fn deref(&self) -> &Self::Target {
        &self.rtc
    }
}

impl<T: RtcCommandSender> std::ops::DerefMut for CallSessionManager<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rtc
    }
}

impl<T: RtcCommandSender + 'static> ApplicationIntake<T> for CallSessionManager<T> {
    fn rtc(&mut self) -> &mut RtcSessionManager<T> {
        &mut self.rtc
    }

    fn timeline_event_types(&self) -> Vec<String> {
        vec![
            REACTION_EVENT_TYPE.to_owned(),
            ANNOTATION_EVENT_TYPE.to_owned(),
        ]
    }

    fn on_room_timeline_events(&mut self, room_id: &str, events: &[RawTimelineEvent]) {
        CallSessionManager::on_room_timeline_events(self, room_id, events);
    }

    fn on_event_redacted(&mut self, room_id: &str, event_id: &str) {
        CallSessionManager::on_event_redacted(self, room_id, event_id);
    }

    fn pending_relations(&self, room_id: &str) -> Vec<RelationsRequest> {
        self.pending_relation_lookups(room_id)
            .into_iter()
            .map(|lookup| RelationsRequest {
                event_id: lookup.membership_event_id,
                rel_type: ANNOTATION_RELATION_TYPE.to_owned(),
                event_type: ANNOTATION_EVENT_TYPE.to_owned(),
            })
            .collect()
    }

    fn on_relations_received(
        &mut self,
        room_id: &str,
        target_event_id: &str,
        events: &[RawTimelineEvent],
    ) {
        CallSessionManager::on_relations_received(self, room_id, target_event_id, events);
    }
}

#[cfg(test)]
mod tests;
