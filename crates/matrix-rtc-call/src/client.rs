// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The host-facing objects: an [`RtcClient`] per backend, an [`RtcRoom`] per
//! room the host opens, and an [`RtcSession`] — or, for a call slot, an
//! [`RtcCall`] — per slot it joins.
//!
//! Opening a room, its feeds and the to-device routing are the core's
//! [`BaseRtcClient`], over this crate's room state and read in the room's
//! [`ElementCallCompat`]. Everything room-scoped is on the room, everything
//! about our own participation on the session. The library runs its own
//! background work on the core's [`executor`](matrix_rtc_core::executor): a
//! room's feeds while the room object lives; the core's upkeep (keep-alive,
//! key rotations) and, for a call, the raised hand following our membership
//! event while the session does. Off wasm32 that is the current tokio runtime,
//! so a native host opens rooms and joins from within one.

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard};
use std::time::Duration;

use matrix_rtc_core::{
    ApplicationInfo, BackendError, BaseRtcClient, BaseRtcRoomHandle, CommandError,
    EncryptionConfig, EncryptionKeySignalHandler, JoinError, JoinSessionParams, JoinedMembership,
    LeaveError, LeaveSessionParams, MatrixBackend, OpenError, RtcIdentityMapper, SlotEncryption,
    SlotState, TransportIntent,
    executor::{self, AbortHandle, AbortOnDrop, JoinHandleExt},
};
use tokio::sync::{Mutex, broadcast, watch};

use crate::compat::ingest::{member_id, outbound_dialect};
use crate::compat::{DialectBackend, ElementCallCompat, LEGACY_KEY_EVENT_TYPE};
use crate::notification::NotifyConfig;
use crate::reactions::{RaisedHand, ReactionError, ReactionsConfig, ReceivedReaction};
use crate::room_state::{CallJoinParams, CallRoomState};
use crate::transports;
use matrix_rtc_core::feeder::RoomAlreadyOpen;

type Backend<B> = DialectBackend<B>;
type State<B> = CallRoomState<Backend<B>>;
type SharedState<B> = Arc<Mutex<State<B>>>;

/// Why a client, room or session call failed.
#[derive(Debug, thiserror::Error)]
pub enum RtcError {
    /// R5: the room already has a live room object.
    #[error(transparent)]
    RoomAlreadyOpen(#[from] RoomAlreadyOpen),
    #[error("backend: {0}")]
    Backend(#[from] BackendError),
    #[error(transparent)]
    Join(#[from] JoinError),
    #[error(transparent)]
    Leave(#[from] LeaveError),
    #[error(transparent)]
    Command(#[from] CommandError),
    #[error(transparent)]
    Reaction(#[from] ReactionError),
    /// R9: the session has left, or its room was closed; join again for a new
    /// one.
    #[error("this session is over; join again for a new one")]
    SessionOver,
}

impl From<OpenError> for RtcError {
    fn from(error: OpenError) -> Self {
        match error {
            OpenError::RoomAlreadyOpen(error) => Self::RoomAlreadyOpen(error),
            OpenError::Backend(error) => Self::Backend(error),
        }
    }
}

/// How a room is opened.
#[derive(Clone, Debug, Default)]
pub struct RoomOptions {
    /// Which MatrixRTC generation the room is read and written for: what is
    /// subscribed to, how our sends are rendered, the `member.id` we join
    /// with, how a legacy media key is bound. See [`crate::compat`].
    pub element_call_compat: ElementCallCompat,
}

/// A join into any application's slot.
#[derive(Clone, Debug)]
pub struct JoinOptions {
    /// Must start with `{application_type}#` (MSC4143).
    pub slot_id: String,
    pub application: ApplicationInfo,
    /// `None` publishes on the first LiveKit transport the homeserver
    /// advertises.
    pub transport: Option<TransportIntent>,
    pub encryption_config: Option<EncryptionConfig>,
    pub keep_alive_timeout_ms: Option<u64>,
    /// How often the session restarts its delayed leave; `None` is
    /// [`DEFAULT_KEEP_ALIVE_INTERVAL_MS`](matrix_rtc_core::DEFAULT_KEEP_ALIVE_INTERVAL_MS).
    /// Clamped to half the keep-alive timeout, so one late tick does not end
    /// the membership.
    pub keep_alive_interval_ms: Option<u64>,
    pub sticky_duration_ms: Option<u64>,
    pub degraded_lifetime_ms: Option<u64>,
}

impl JoinOptions {
    pub fn new(slot_id: impl Into<String>, application: impl Into<ApplicationInfo>) -> Self {
        Self {
            slot_id: slot_id.into(),
            application: application.into(),
            transport: None,
            encryption_config: None,
            keep_alive_timeout_ms: None,
            keep_alive_interval_ms: None,
            sticky_duration_ms: None,
            degraded_lifetime_ms: None,
        }
    }
}

/// A join into a call slot: the generic join plus what a call adds.
#[derive(Clone, Debug)]
pub struct CallJoinOptions {
    pub join: JoinOptions,
    /// Ring the room (MSC4075). Suppressed when somebody else is already in
    /// the slot, so it is the intent to summon, not a guarantee of a ring.
    pub notify: Option<NotifyConfig>,
    /// `None` is [`ReactionsConfig::default`].
    pub reactions: Option<ReactionsConfig>,
}

impl CallJoinOptions {
    /// A quiet `m.call` join with default reactions.
    pub fn new(slot_id: impl Into<String>) -> Self {
        Self {
            join: JoinOptions::new(slot_id, "m.call"),
            notify: None,
            reactions: None,
        }
    }
}

/// How long the raised hand waits before retrying a failed re-annotation.
const RETRY: Duration = Duration::from_millis(matrix_rtc_core::DEFAULT_KEEP_ALIVE_INTERVAL_MS);

/// One per backend. Creating it does no I/O (R1).
pub struct RtcClient<B: MatrixBackend + 'static> {
    base: BaseRtcClient<Backend<B>, State<B>>,
}

impl<B: MatrixBackend + 'static> RtcClient<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self {
            base: BaseRtcClient::with_key_event_types(
                Arc::new(DialectBackend::new(backend)),
                vec![LEGACY_KEY_EVENT_TYPE.to_owned()],
            ),
        }
    }

    /// The host's backend behind the dialect wrapper, for what sends outside
    /// the rooms (a media layer's token exchange).
    pub fn backend(&self) -> &Arc<DialectBackend<B>> {
        self.base.backend()
    }

    /// Opens `room_id` (R2) through the core's
    /// [`BaseRtcClient::open_with`], read in the room's compatibility mode.
    /// The feeds run on the executor; await [`RtcRoom::seeded`] before
    /// joining.
    ///
    /// Refused while the room has a live room object (R5). Cancelling the call
    /// part-way leaves nothing behind.
    pub async fn room(
        &self,
        room_id: impl Into<String>,
        options: RoomOptions,
    ) -> Result<RtcRoom<B>, RtcError> {
        let room_id = room_id.into();
        let mode = options.element_call_compat;
        let backend = self.base.backend().clone();
        let state = Arc::new(Mutex::new(CallRoomState::with_backend(
            room_id.clone(),
            backend.clone(),
        )));
        log::info!("client: [{room_id}] opening in {mode:?} mode");
        let handle = self
            .base
            .open_with(room_id.clone(), state.clone(), Arc::new(mode))
            .await?;

        Ok(RtcRoom {
            room_id,
            mode,
            state,
            backend,
            handle: Some(handle),
            sessions: StdMutex::new(HashMap::new()),
        })
    }
}

/// One open room (R3). Dropping it without [`close`](Self::close) ends its
/// subscriptions only (R4): no leave is sent, and a membership left behind
/// expires through its delayed leave.
pub struct RtcRoom<B: MatrixBackend + 'static> {
    room_id: String,
    mode: ElementCallCompat,
    state: SharedState<B>,
    backend: Arc<Backend<B>>,
    /// The core's open room, which feeds `state`; `None` once detached.
    handle: Option<BaseRtcRoomHandle<Backend<B>, State<B>>>,
    /// Whether each slot's current session object is live, by slot. A flag a
    /// session shares, so leaving, dropping it or closing the room ends it.
    sessions: StdMutex<HashMap<String, Arc<AtomicBool>>>,
}

impl<B: MatrixBackend + 'static> RtcRoom<B> {
    pub fn room_id(&self) -> &str {
        &self.room_id
    }

    pub fn element_call_compat(&self) -> ElementCallCompat {
        self.mode
    }

    /// The host's backend behind the dialect wrapper.
    pub fn backend(&self) -> &Arc<DialectBackend<B>> {
        &self.backend
    }

    /// Resolves once the room's current state is applied, so a join issued
    /// afterwards sees it.
    pub async fn seeded(&self) {
        if let Some(handle) = &self.handle {
            handle.seeded().await;
        }
    }

    pub fn is_seeded(&self) -> bool {
        self.handle
            .as_ref()
            .is_some_and(BaseRtcRoomHandle::is_seeded)
    }

    /// Opens a slot by publishing its `m.rtc.slot` state. See
    /// [`matrix_rtc_core::BaseRtcRoom::open_slot`].
    pub async fn open_slot(
        &self,
        slot_id: impl Into<String>,
        application_type: impl Into<String>,
        encryption: Option<SlotEncryption>,
    ) -> Result<(), RtcError> {
        let state = self.state.lock().await;
        Ok(state
            .rtc()
            .open_slot(slot_id.into(), application_type.into(), encryption)
            .await?)
    }

    pub async fn close_slot(&self, slot_id: impl Into<String>) -> Result<(), RtcError> {
        let state = self.state.lock().await;
        Ok(state.rtc().close_slot(slot_id.into()).await?)
    }

    /// `None` until the room's slot state has been supplied.
    pub async fn slot_state(&self, slot_id: &str) -> Option<SlotState> {
        self.state.lock().await.rtc().slot_state(slot_id)
    }

    /// Who is in a slot, without joining it (R7).
    pub async fn member_count(&self, slot_id: &str) -> usize {
        self.observe(slot_id).await.borrow().len()
    }

    /// The slot's joined memberships as they change, without joining it (R7).
    pub async fn observe(&self, slot_id: &str) -> watch::Receiver<Vec<JoinedMembership>> {
        self.state.lock().await.rtc_mut().observe_slot(slot_id)
    }

    /// Everything the room believes, as JSON, for bug reports.
    pub async fn debug_snapshot(&self) -> serde_json::Value {
        self.state.lock().await.rtc().debug_snapshot()
    }

    /// Joins a slot of any application (R6). Refused while the slot is joined
    /// through a live session of this room (R8).
    pub async fn join(&self, options: JoinOptions) -> Result<RtcSession<B>, RtcError> {
        let slot_id = options.slot_id.clone();
        let transport = self.transport(options.transport.clone()).await?;
        let mut state = self.state.lock().await;
        let params = self.prepare_join(&mut state, options, transport).await?;
        let member_id = params.membership_id();
        state.rtc_mut().join(params).await?;
        let upkeep = state.rtc().upkeep_abort_handle(&slot_id);
        drop(state);
        Ok(self.session(slot_id, member_id, upkeep))
    }

    /// Joins a call slot: the generic join, then the call layer — reactions
    /// for this participation, and the ring if asked for.
    pub async fn join_call(&self, options: CallJoinOptions) -> Result<RtcCall<B>, RtcError> {
        let CallJoinOptions {
            join,
            notify,
            reactions,
        } = options;
        let slot_id = join.slot_id.clone();
        let transport = self.transport(join.transport.clone()).await?;
        let mut state = self.state.lock().await;
        let rtc = self.prepare_join(&mut state, join, transport).await?;
        let member_id = rtc.membership_id();
        state
            .join(CallJoinParams {
                rtc,
                notify,
                reactions,
            })
            .await?;
        let upkeep = state.rtc().upkeep_abort_handle(&slot_id);
        let event_moves = state.rtc().subscribe_own_membership_event_id(&slot_id);
        drop(state);
        Ok(RtcCall {
            _hand: event_moves.map(|moves| self.follow_hand(slot_id.clone(), moves)),
            session: self.session(slot_id, member_id, upkeep),
        })
    }

    /// Leaves every slot joined through this room, then ends its
    /// subscriptions (R4). Every session of the room is over afterwards.
    pub async fn close(mut self) {
        self.end_sessions();
        let joined = self.state.lock().await.rtc().joined_slots();
        for slot_id in joined {
            log::info!("[{}/{slot_id}] leaving before closing", self.room_id);
            let result = self
                .state
                .lock()
                .await
                .leave(&slot_id, LeaveSessionParams::new())
                .await;
            if let Err(error) = result {
                log::warn!(
                    "[{}/{slot_id}] leave before close failed: {error}",
                    self.room_id
                );
            }
        }
        self.detach();
    }

    /// The join's own transport, else the first LiveKit one the homeserver
    /// advertises. Asked before the room's lock is taken: it is a request to
    /// the homeserver, and the feed would wait on it otherwise.
    async fn transport(
        &self,
        chosen: Option<TransportIntent>,
    ) -> Result<TransportIntent, RtcError> {
        Ok(transports::resolve(self.backend.as_ref(), chosen).await?)
    }

    /// Everything else a join needs that the host does not say: who we are,
    /// the `member.id` our mode joins with, and our sends' dialect. The state
    /// lock is held so the R8 check and the join cannot interleave with
    /// another join of the slot.
    async fn prepare_join(
        &self,
        state: &mut State<B>,
        options: JoinOptions,
        transport: TransportIntent,
    ) -> Result<JoinSessionParams, RtcError> {
        let JoinOptions {
            slot_id,
            application,
            transport: _,
            encryption_config,
            keep_alive_timeout_ms,
            keep_alive_interval_ms,
            sticky_duration_ms,
            degraded_lifetime_ms,
        } = options;

        if let Some(application_type) = application.application_type()
            && !slot_id.starts_with(&format!("{application_type}#"))
        {
            return Err(CommandError::from_message(format!(
                "slot id '{slot_id}' does not belong to application '{application_type}': \
                 MSC4143 slot ids are '{{application_type}}#{{slot}}'"
            ))
            .into());
        }

        if self.live_session(&slot_id) {
            return Err(JoinError::AlreadyJoined(
                state.rtc().own_member_id(&slot_id).unwrap_or_default(),
            )
            .into());
        }
        // Joined in the core with no live session object: the host dropped
        // it without leaving. Leave that participation first rather than
        // refusing the slot until its delayed leave fires.
        if state.rtc().own_member_id(&slot_id).is_some() {
            log::info!(
                "[{}/{slot_id}] leaving a participation whose session was dropped",
                self.room_id,
            );
            state.leave(&slot_id, LeaveSessionParams::new()).await?;
        }

        let backend = &self.backend;
        let user_id = backend.own_user_id();
        let device_id = backend.own_device_id();
        // Not always a fresh id: see `compat::ingest::member_id` for the one
        // generation where a fresh one makes us mark ourselves departed.
        let membership_id = member_id(self.mode, &user_id, &device_id);

        // Before the join, not after: the join itself sends the membership
        // (and arms the delayed leave), so a dialect registered afterwards
        // would let exactly the two events that announce us go out
        // spec-current.
        backend.set_dialect(
            &self.room_id,
            outbound_dialect(self.mode, &user_id, &device_id, &self.room_id, &slot_id),
        );

        Ok(JoinSessionParams {
            user_id,
            device_id,
            membership_id: Some(membership_id),
            slot_id,
            application,
            transport,
            keep_alive_timeout_ms,
            keep_alive_interval_ms,
            sticky_duration_ms,
            degraded_lifetime_ms,
            encryption_config,
        })
    }

    fn sessions(&self) -> MutexGuard<'_, HashMap<String, Arc<AtomicBool>>> {
        self.sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn live_session(&self, slot_id: &str) -> bool {
        self.sessions()
            .get(slot_id)
            .is_some_and(|live| live.load(Ordering::SeqCst))
    }

    /// The session object for a join that just succeeded. `upkeep` is the
    /// core's, stopped when the session object goes.
    fn session(
        &self,
        slot_id: String,
        member_id: String,
        upkeep: Option<AbortHandle>,
    ) -> RtcSession<B> {
        let live = Arc::new(AtomicBool::new(true));
        self.sessions().insert(slot_id.clone(), live.clone());
        RtcSession {
            room_id: self.room_id.clone(),
            slot_id,
            member_id,
            state: self.state.clone(),
            live,
            upkeep,
        }
    }

    /// Re-raises our hand on each new membership event a sticky refresh puts
    /// in place (see `CallRoomState::reannotate_hand_if_moved`), retrying a
    /// failed re-send every keep-alive interval. Ends when the join's machine
    /// goes, at leave.
    fn follow_hand(
        &self,
        slot_id: String,
        mut moves: watch::Receiver<Option<String>>,
    ) -> AbortOnDrop<()> {
        let state = self.state.clone();
        executor::spawn(async move {
            while moves.changed().await.is_ok() {
                while !state.lock().await.reannotate_hand_if_moved(&slot_id).await {
                    executor::sleep(RETRY).await;
                }
            }
        })
        .abort_on_drop()
    }

    fn end_sessions(&self) {
        for (_, live) in self.sessions().drain() {
            live.store(false, Ordering::SeqCst);
        }
    }
}

impl<B: MatrixBackend + 'static> RtcRoom<B> {
    /// Ends the subscription and forgets the room; idempotent.
    fn detach(&mut self) {
        if let Some(handle) = self.handle.take() {
            self.backend.clear_dialect(&self.room_id);
            drop(handle);
        }
    }
}

impl<B: MatrixBackend + 'static> Drop for RtcRoom<B> {
    fn drop(&mut self) {
        self.detach();
    }
}

/// Our participation in one slot (R6). Over after [`leave`](Self::leave), after
/// its room closes, or once dropped (R9): every call then fails with
/// [`RtcError::SessionOver`] or reports nothing to do, and joining again yields
/// a new session. Dropping it sends no leave; the membership expires through its
/// delayed leave unless the slot is joined again, which leaves it first.
pub struct RtcSession<B: MatrixBackend + 'static> {
    room_id: String,
    slot_id: String,
    member_id: String,
    state: SharedState<B>,
    live: Arc<AtomicBool>,
    /// The core's upkeep for this join, aborted when the session object
    /// drops: a dropped session sends no leave and stops keeping alive.
    upkeep: Option<AbortHandle>,
}

impl<B: MatrixBackend + 'static> RtcSession<B> {
    pub fn room_id(&self) -> &str {
        &self.room_id
    }

    pub fn slot_id(&self) -> &str {
        &self.slot_id
    }

    /// Our `member.id` in this participation.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    pub fn is_live(&self) -> bool {
        self.live.load(Ordering::SeqCst)
    }

    /// The event id of our current membership event. Moves on every sticky
    /// refresh, so read it at the moment of use.
    pub async fn membership_event_id(&self) -> Option<String> {
        if !self.is_live() {
            return None;
        }
        self.state
            .lock()
            .await
            .rtc()
            .own_membership_event_id(&self.slot_id)
    }

    pub async fn member_count(&self) -> usize {
        self.subscribe_memberships().await.borrow().len()
    }

    pub async fn subscribe_memberships(&self) -> watch::Receiver<Vec<JoinedMembership>> {
        self.state
            .lock()
            .await
            .rtc_mut()
            .observe_slot(&self.slot_id)
    }

    pub async fn set_encryption_signal_handler(
        &self,
        handler: Arc<dyn EncryptionKeySignalHandler>,
    ) -> bool {
        self.is_live()
            && self
                .state
                .lock()
                .await
                .rtc_mut()
                .set_encryption_signal_handler(&self.slot_id, handler)
    }

    pub async fn set_encryption_identity_mapper(&self, mapper: RtcIdentityMapper) -> bool {
        self.is_live()
            && self
                .state
                .lock()
                .await
                .rtc_mut()
                .set_encryption_identity_mapper(&self.slot_id, mapper)
    }

    pub async fn replay_encryption_keys(&self) -> bool {
        self.is_live()
            && self
                .state
                .lock()
                .await
                .rtc()
                .replay_encryption_keys(&self.slot_id)
                .await
    }

    /// Leaves the slot; the session is over afterwards (R9). A failed leave
    /// leaves it live, so it can be retried.
    pub async fn leave(&self, params: LeaveSessionParams) -> Result<(), RtcError> {
        if !self.is_live() {
            return Err(RtcError::SessionOver);
        }
        self.state.lock().await.leave(&self.slot_id, params).await?;
        self.live.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn check_live(&self) -> Result<(), RtcError> {
        if self.is_live() {
            Ok(())
        } else {
            Err(RtcError::SessionOver)
        }
    }
}

impl<B: MatrixBackend + 'static> Drop for RtcSession<B> {
    fn drop(&mut self) {
        self.live.store(false, Ordering::SeqCst);
        if let Some(upkeep) = &self.upkeep {
            upkeep.abort();
        }
    }
}

/// Our participation in a call slot: an [`RtcSession`] (reached through
/// `Deref`) plus reactions and the raised hand.
pub struct RtcCall<B: MatrixBackend + 'static> {
    session: RtcSession<B>,
    /// Keeps our raised hand on our current membership event.
    _hand: Option<AbortOnDrop<()>>,
}

impl<B: MatrixBackend + 'static> RtcCall<B> {
    pub fn session(&self) -> &RtcSession<B> {
        &self.session
    }

    /// Sends an emoji reaction relating to our membership. Only the first
    /// grapheme of `emoji` is sent; `name` selects the sound peers play.
    /// Returns the event id.
    pub async fn send_reaction(&self, emoji: &str, name: &str) -> Result<String, RtcError> {
        self.check_live()?;
        Ok(self
            .state
            .lock()
            .await
            .send_reaction(&self.slot_id, emoji, name)
            .await?)
    }

    pub async fn raise_hand(&self) -> Result<(), RtcError> {
        self.check_live()?;
        Ok(self.state.lock().await.raise_hand(&self.slot_id).await?)
    }

    pub async fn lower_hand(&self) -> Result<(), RtcError> {
        self.check_live()?;
        Ok(self.state.lock().await.lower_hand(&self.slot_id).await?)
    }

    /// The slot's raised hands, oldest first.
    pub async fn raised_hands(&self) -> Vec<RaisedHand> {
        self.state
            .lock()
            .await
            .raised_hands(&self.slot_id)
            .unwrap_or_default()
    }

    pub async fn subscribe_raised_hands(&self) -> Option<watch::Receiver<Vec<RaisedHand>>> {
        self.state
            .lock()
            .await
            .subscribe_raised_hands(&self.slot_id)
    }

    pub async fn subscribe_reactions(&self) -> Option<broadcast::Receiver<ReceivedReaction>> {
        self.state.lock().await.subscribe_reactions(&self.slot_id)
    }

    pub async fn reactions_config(&self) -> Option<ReactionsConfig> {
        self.state.lock().await.reactions_config(&self.slot_id)
    }
}

impl<B: MatrixBackend + 'static> Deref for RtcCall<B> {
    type Target = RtcSession<B>;

    fn deref(&self) -> &Self::Target {
        &self.session
    }
}

#[cfg(test)]
mod tests;
