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
//! [`MembershipFormat`]. Everything room-scoped is on the room, everything
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
    LeaveCode, LeaveError, LeaveReason, LeaveSessionParams, MatrixBackend, OpenError,
    ROOM_APPLICATION_SLOT_ID, RtcIdentityMapper, SlotEncryption, SlotState, TransportIntent,
    executor::{self, AbortHandle, AbortOnDrop, JoinHandleExt},
};
use tokio::sync::{Mutex, broadcast, watch};

use crate::notification::NotifyConfig;
use crate::reactions::{RaisedHand, ReactionError, ReactionsConfig, ReceivedReaction};
use crate::room_state::{CallJoinParams, CallRoomState};
use crate::transports::{self, JoinTransport};
use matrix_rtc_core::RoomOptions;
use matrix_rtc_core::compat::{DialectBackend, MembershipFormat};
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

/// A join into any application's slot.
#[derive(Clone, Debug)]
pub struct JoinOptions {
    /// Must start with `{application_type}#` (MSC4143).
    pub slot_id: String,
    pub application: ApplicationInfo,
    /// Defaults to [`JoinTransport::Advertised`].
    pub transport: JoinTransport,
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
    /// Joins `application`'s room-wide slot, `{application}#room`, with
    /// everything else defaulted; each setter overrides one default.
    pub fn application(application: impl Into<ApplicationInfo>) -> Self {
        let application = application.into();
        Self {
            slot_id: slot_id_of(&application, ROOM_APPLICATION_SLOT_ID),
            application,
            transport: JoinTransport::Advertised,
            encryption_config: None,
            keep_alive_timeout_ms: None,
            keep_alive_interval_ms: None,
            sticky_duration_ms: None,
            degraded_lifetime_ms: None,
        }
    }

    /// Joins the application's slot `{application}#{application_slot_id}`.
    pub fn slot(mut self, application_slot_id: impl AsRef<str>) -> Self {
        self.slot_id = slot_id_of(&self.application, application_slot_id.as_ref());
        self
    }

    pub fn transport(mut self, transport: impl Into<JoinTransport>) -> Self {
        self.transport = transport.into();
        self
    }

    pub fn encryption_config(mut self, config: EncryptionConfig) -> Self {
        self.encryption_config = Some(config);
        self
    }

    pub fn keep_alive_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.keep_alive_timeout_ms = Some(timeout_ms);
        self
    }

    pub fn keep_alive_interval_ms(mut self, interval_ms: u64) -> Self {
        self.keep_alive_interval_ms = Some(interval_ms);
        self
    }

    pub fn sticky_duration_ms(mut self, duration_ms: u64) -> Self {
        self.sticky_duration_ms = Some(duration_ms);
        self
    }

    pub fn degraded_lifetime_ms(mut self, lifetime_ms: u64) -> Self {
        self.degraded_lifetime_ms = Some(lifetime_ms);
        self
    }
}

/// `{application_type}#{application_slot_id}`, MSC4143's slot id.
fn slot_id_of(application: &ApplicationInfo, application_slot_id: &str) -> String {
    format!(
        "{}#{application_slot_id}",
        application.application_type().unwrap_or_default()
    )
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
    /// A quiet join of the room-wide call, `m.call#room`, with default
    /// reactions.
    pub fn new() -> Self {
        Self {
            join: JoinOptions::application(CALL_APPLICATION),
            notify: None,
            reactions: None,
        }
    }

    /// Joins the call slot `m.call#{application_slot_id}`.
    pub fn slot(mut self, application_slot_id: impl AsRef<str>) -> Self {
        self.join = self.join.slot(application_slot_id);
        self
    }

    /// See [`JoinOptions::transport`].
    pub fn transport(mut self, transport: impl Into<JoinTransport>) -> Self {
        self.join = self.join.transport(transport);
        self
    }

    /// Rings the room when we start the call.
    pub fn notify(mut self, notify: NotifyConfig) -> Self {
        self.notify = Some(notify);
        self
    }

    pub fn reactions(mut self, reactions: ReactionsConfig) -> Self {
        self.reactions = Some(reactions);
        self
    }
}

impl Default for CallJoinOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// The MSC4143 application type of a call.
const CALL_APPLICATION: &str = "m.call";

/// How long the raised hand waits before retrying a failed re-annotation.
const RETRY: Duration = Duration::from_millis(matrix_rtc_core::DEFAULT_KEEP_ALIVE_INTERVAL_MS);

/// One per backend. Creating it does no I/O (R1).
pub struct RtcClient<B: MatrixBackend + 'static> {
    base: BaseRtcClient<B, State<B>>,
}

impl<B: MatrixBackend + 'static> RtcClient<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self {
            base: BaseRtcClient::new(backend),
        }
    }

    /// The host's backend behind the dialect wrapper, for what sends outside
    /// the rooms (a media layer's token exchange).
    pub fn backend(&self) -> &Arc<DialectBackend<B>> {
        self.base.backend()
    }

    /// Opens `room_id` (R2) through the core's
    /// [`BaseRtcClient::open_with`], in the room's membership format.
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
        let mode = options.format;
        let backend = self.base.backend().clone();
        let state = Arc::new(Mutex::new(CallRoomState::with_backend(
            room_id.clone(),
            backend.clone(),
        )));
        let handle = self
            .base
            .open_with(room_id.clone(), state.clone(), options)
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
    mode: MembershipFormat,
    state: SharedState<B>,
    backend: Arc<Backend<B>>,
    /// The core's open room, which feeds `state`; `None` once detached.
    handle: Option<BaseRtcRoomHandle<B, State<B>>>,
    /// Each slot's current session, shared with it so closing the room ends it.
    sessions: StdMutex<HashMap<String, Arc<SessionEnd>>>,
}

impl<B: MatrixBackend + 'static> RtcRoom<B> {
    pub fn room_id(&self) -> &str {
        &self.room_id
    }

    pub fn format(&self) -> MembershipFormat {
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
        let params = self
            .prepare_join(&mut state, options, transport.clone())
            .await?;
        let member_id = params.membership_id();
        state.rtc_mut().join(params).await?;
        let upkeep = state.rtc().upkeep_abort_handle(&slot_id);
        let auto_leaves = state.rtc().subscribe_auto_leaves(&slot_id);
        drop(state);
        Ok(self.session(slot_id, member_id, transport, upkeep, auto_leaves))
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
        let rtc = self
            .prepare_join(&mut state, join, transport.clone())
            .await?;
        let member_id = rtc.membership_id();
        state
            .join(CallJoinParams {
                rtc,
                notify,
                reactions,
            })
            .await?;
        let upkeep = state.rtc().upkeep_abort_handle(&slot_id);
        let auto_leaves = state.rtc().subscribe_auto_leaves(&slot_id);
        let event_moves = state.rtc().subscribe_own_membership_event_id(&slot_id);
        drop(state);
        Ok(RtcCall {
            _hand: event_moves.map(|moves| self.follow_hand(slot_id.clone(), moves)),
            session: self.session(slot_id, member_id, transport, upkeep, auto_leaves),
        })
    }

    /// Leaves every slot joined through this room, then ends its
    /// subscriptions (R4). Every session of the room is over afterwards.
    pub async fn close(mut self) {
        self.end_sessions(LeaveReason::new(LeaveCode::Leave));
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

    /// What the join's [`JoinTransport`] resolves to. Asked before the room's
    /// lock is taken: it may be a request to the homeserver, and the feed would
    /// wait on it otherwise.
    async fn transport(&self, chosen: JoinTransport) -> Result<TransportIntent, RtcError> {
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

        let mut params = JoinSessionParams {
            membership_id: None,
            slot_id,
            application,
            transport: Some(transport),
            keep_alive_timeout_ms,
            keep_alive_interval_ms,
            sticky_duration_ms,
            degraded_lifetime_ms,
            encryption_config,
        };
        // The `member.id` and our sends' dialect, in the room's format.
        if let Some(handle) = &self.handle {
            handle.prepare_join(&mut params);
        }
        Ok(params)
    }

    fn sessions(&self) -> MutexGuard<'_, HashMap<String, Arc<SessionEnd>>> {
        self.sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn live_session(&self, slot_id: &str) -> bool {
        self.sessions()
            .get(slot_id)
            .is_some_and(|end| end.is_live())
    }

    /// The session object for a join that just succeeded. `upkeep` is the
    /// core's, stopped when the session object goes; `auto_leaves` is the
    /// core's announcement of the leaves it makes on its own, subscribed under
    /// the same lock as the join so none can slip past.
    fn session(
        &self,
        slot_id: String,
        member_id: String,
        transport: TransportIntent,
        upkeep: Option<AbortHandle>,
        auto_leaves: Option<broadcast::Receiver<LeaveReason>>,
    ) -> RtcSession<B> {
        let end = Arc::new(SessionEnd::new());
        self.sessions().insert(slot_id.clone(), end.clone());
        RtcSession {
            _follow_auto_leave: auto_leaves.map(|leaves| follow_auto_leave(leaves, end.clone())),
            room_id: self.room_id.clone(),
            slot_id,
            member_id,
            transport,
            state: self.state.clone(),
            end,
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

    fn end_sessions(&self, reason: LeaveReason) {
        for (_, end) in self.sessions().drain() {
            end.end(reason.clone());
        }
    }
}

/// The one place a participation becomes over and says why, whatever ended it.
struct SessionEnd {
    live: AtomicBool,
    ended: watch::Sender<Option<LeaveReason>>,
}

impl SessionEnd {
    fn new() -> Self {
        Self {
            live: AtomicBool::new(true),
            ended: watch::channel(None).0,
        }
    }

    fn is_live(&self) -> bool {
        self.live.load(Ordering::SeqCst)
    }

    /// `false` if it was already over.
    fn end(&self, reason: LeaveReason) -> bool {
        let live = self.live.swap(false, Ordering::SeqCst);
        if live {
            self.ended.send_replace(Some(reason));
        }
        live
    }

    /// A dropped session sends no leave, so there is no ending to report.
    fn abandon(&self) {
        self.live.store(false, Ordering::SeqCst);
    }
}

/// Ends a session when the core leaves its slot on its own. The core's channel
/// is per slot, so this stops at the first leave rather than adopt a later
/// join's.
fn follow_auto_leave(
    mut leaves: broadcast::Receiver<LeaveReason>,
    end: Arc<SessionEnd>,
) -> AbortOnDrop<()> {
    executor::spawn(async move {
        let reason = loop {
            match leaves.recv().await {
                Ok(reason) => break reason,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return,
            }
        };
        end.end(reason);
    })
    .abort_on_drop()
}

impl<B: MatrixBackend + 'static> RtcRoom<B> {
    /// Ends the subscription and forgets the room; idempotent.
    fn detach(&mut self) {
        drop(self.handle.take());
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
    transport: TransportIntent,
    state: SharedState<B>,
    end: Arc<SessionEnd>,
    /// The core's upkeep for this join, aborted when the session object
    /// drops: a dropped session sends no leave and stops keeping alive.
    upkeep: Option<AbortHandle>,
    _follow_auto_leave: Option<AbortOnDrop<()>>,
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

    /// What this participation publishes on, as its join resolved it.
    pub fn transport(&self) -> &TransportIntent {
        &self.transport
    }

    pub fn is_live(&self) -> bool {
        self.end.is_live()
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

    /// The MSC4140 `delay_id` of the delayed leave protecting this
    /// participation, or `None` once it is over or while nothing is armed.
    /// Replaced when a fired delay is re-armed, so read it at the moment of use.
    pub async fn delayed_leave_id(&self) -> Option<String> {
        if !self.is_live() {
            return None;
        }
        self.state
            .lock()
            .await
            .rtc()
            .own_delayed_leave_id(&self.slot_id)
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

    /// Why this participation ended — the host left, the slot closed
    /// ([`LeaveCode::SlotClosed`]) or the room closed — or `None` while live.
    /// Whatever runs media tears down here. Not set when the session is dropped.
    pub fn subscribe_ended(&self) -> watch::Receiver<Option<LeaveReason>> {
        self.end.ended.subscribe()
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

    /// Leaves the slot; the session is over afterwards (R9), even when the
    /// send fails — the delayed leave then removes the membership.
    pub async fn leave(&self, params: LeaveSessionParams) -> Result<(), RtcError> {
        let reason = params
            .leave_reason
            .clone()
            .unwrap_or_else(|| LeaveReason::new(LeaveCode::Leave));
        // Ended before the send, so media stops publishing at once rather than
        // after a slow or failing one.
        if !self.end.end(reason) {
            return Err(RtcError::SessionOver);
        }
        self.state.lock().await.leave(&self.slot_id, params).await?;
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
        self.end.abandon();
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
