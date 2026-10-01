// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The host-facing objects: an [`RtcClient`] per backend, an [`RtcRoom`] per
//! room the host opens, and an [`RtcSession`] — or, for a call slot, an
//! [`RtcCall`] — per slot it joins.
//!
//! The client holds nothing about a room until one is asked for; the only state
//! spanning rooms is the backend and the routing of to-device media keys to the
//! room they name, through a registry of weak handles. Everything room-scoped
//! is on the room, everything about our own participation on the session. The
//! library spawns nothing: opening a room returns the futures that feed it, and
//! the binding runs them where its background work already runs.

use std::collections::HashMap;
use std::future::Future;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard};

use matrix_rtc_core::{
    ApplicationInfo, BackendError, CommandError, EncryptionConfig, EncryptionKeySignalHandler,
    JoinError, JoinSessionParams, JoinedMembership, LeaveError, LeaveSessionParams, MatrixBackend,
    RtcIdentityMapper, SlotEncryption, SlotState, TransportIntent,
};
use tokio::sync::{Mutex, broadcast, watch};

use crate::compat::ingest::{member_id, outbound_dialect};
use crate::compat::{DialectBackend, ElementCallCompat};
use crate::feeder::{
    RoomAlreadyOpen, RoomAttachment, RoomFeeder, RoomFeederRun, RoomRegistry, ToDeviceFeeder,
    ToDeviceFeederRun,
};
use crate::notification::NotifyConfig;
use crate::reactions::{RaisedHand, ReactionError, ReactionsConfig, ReceivedReaction};
use crate::room_state::{CallJoinParams, CallRoomState};
use crate::transports;

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

/// What every room of a client shares.
struct ClientShared<B: MatrixBackend + 'static> {
    backend: Arc<Backend<B>>,
    registry: RoomRegistry<State<B>>,
    /// Running while at least one room is open. Locked together with the
    /// registry's changes, so a room opening and the last one closing cannot
    /// interleave into an open room with no subscription.
    to_device: StdMutex<Option<ToDeviceFeeder>>,
    /// Serialises opening rooms, so two first rooms do not both subscribe.
    opening: Mutex<()>,
}

impl<B: MatrixBackend + 'static> ClientShared<B> {
    fn to_device(&self) -> MutexGuard<'_, Option<ToDeviceFeeder>> {
        self.to_device
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Forgets `room_id`, and stops the to-device subscription with the last
    /// room.
    fn release(&self, room_id: &str) {
        let mut to_device = self.to_device();
        if self.registry.unregister(room_id)
            && let Some(feeder) = to_device.take()
        {
            log::info!("client: last room closed; to-device subscription stopped");
            feeder.stop();
        }
    }
}

/// One per backend. Creating it does no I/O (R1).
pub struct RtcClient<B: MatrixBackend + 'static> {
    shared: Arc<ClientShared<B>>,
}

impl<B: MatrixBackend + 'static> RtcClient<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self {
            shared: Arc::new(ClientShared {
                backend: Arc::new(DialectBackend::new(backend)),
                registry: RoomRegistry::default(),
                to_device: StdMutex::new(None),
                opening: Mutex::new(()),
            }),
        }
    }

    /// The host's backend behind the dialect wrapper, for what sends outside
    /// the rooms (a media layer's token exchange).
    pub fn backend(&self) -> &Arc<DialectBackend<B>> {
        &self.shared.backend
    }

    /// Opens `room_id` (R2): registers it, subscribes to what its mode needs,
    /// and starts the to-device subscription if this is the first open room.
    /// Spawn [`RoomRuns::into_futures`], then await [`RtcRoom::seeded`].
    ///
    /// Refused while the room has a live room object (R5). Cancelling the call
    /// part-way leaves nothing behind: the room is free again, and the
    /// to-device subscription stops if no other room is open.
    pub async fn room(
        &self,
        room_id: impl Into<String>,
        options: RoomOptions,
    ) -> Result<(RtcRoom<B>, RoomRuns<B>), RtcError> {
        let room_id = room_id.into();
        let mode = options.element_call_compat;
        let shared = &self.shared;
        let _opening = shared.opening.lock().await;

        let state = Arc::new(Mutex::new(CallRoomState::with_backend(
            room_id.clone(),
            shared.backend.clone(),
        )));
        let needs_to_device = {
            let to_device = shared.to_device();
            shared.registry.register(&room_id, mode, &state)?;
            to_device.is_none()
        };
        // From here every early return — an error, or the caller dropping
        // this future at an await — must undo the registration.
        let opening = Opening {
            shared,
            room_id: &room_id,
            opened: false,
        };
        log::info!("client: [{room_id}] opening in {mode:?} mode");

        let to_device = if needs_to_device {
            let (feeder, run) =
                ToDeviceFeeder::start(shared.backend.clone(), shared.registry.clone()).await?;
            *shared.to_device() = Some(feeder);
            log::info!("client: to-device subscription started");
            Some(run)
        } else {
            None
        };

        let (attachment, feed) =
            RoomFeeder::attach(shared.backend.clone(), state.clone(), mode).await?;
        opening.opened();

        let room = RtcRoom {
            room_id,
            mode,
            state,
            attachment: Some(attachment),
            sessions: StdMutex::new(HashMap::new()),
            client: shared.clone(),
        };
        Ok((room, RoomRuns { feed, to_device }))
    }
}

/// Releases a room whose opening did not finish.
struct Opening<'a, B: MatrixBackend + 'static> {
    shared: &'a ClientShared<B>,
    room_id: &'a str,
    opened: bool,
}

impl<B: MatrixBackend + 'static> Opening<'_, B> {
    fn opened(mut self) {
        self.opened = true;
    }
}

impl<B: MatrixBackend + 'static> Drop for Opening<'_, B> {
    fn drop(&mut self) {
        if !self.opened {
            log::info!("client: [{}] opening abandoned", self.room_id);
            self.shared.release(self.room_id);
        }
    }
}

/// The futures that feed an opened room. The binding spawns them.
pub struct RoomRuns<B: MatrixBackend + 'static> {
    feed: RoomFeederRun<Backend<B>, State<B>>,
    to_device: Option<ToDeviceFeederRun<Backend<B>, State<B>>>,
}

impl<B: MatrixBackend + 'static> RoomRuns<B> {
    /// The room's feed, which ends when the room closes or drops, and — for a
    /// client's first open room — the to-device feed, which ends when the last
    /// room does.
    pub fn into_futures(
        self,
    ) -> (
        impl Future<Output = ()> + use<B>,
        Option<impl Future<Output = ()> + use<B>>,
    ) {
        (self.feed.run(), self.to_device.map(ToDeviceFeederRun::run))
    }
}

/// One open room (R3). Dropping it without [`close`](Self::close) ends its
/// subscriptions only (R4): no leave is sent, and a membership left behind
/// expires through its delayed leave.
pub struct RtcRoom<B: MatrixBackend + 'static> {
    room_id: String,
    mode: ElementCallCompat,
    state: SharedState<B>,
    /// `None` once detached.
    attachment: Option<RoomAttachment>,
    /// Whether each slot's current session object is live, by slot. A flag a
    /// session shares, so leaving, dropping it or closing the room ends it.
    sessions: StdMutex<HashMap<String, Arc<AtomicBool>>>,
    client: Arc<ClientShared<B>>,
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
        &self.client.backend
    }

    /// Resolves once the room's current state is applied, so a join issued
    /// afterwards sees it.
    pub async fn seeded(&self) {
        if let Some(attachment) = &self.attachment {
            attachment.seeded().await;
        }
    }

    pub fn is_seeded(&self) -> bool {
        self.attachment
            .as_ref()
            .is_some_and(RoomAttachment::is_seeded)
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
        drop(state);
        Ok(self.session(slot_id, member_id))
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
        drop(state);
        Ok(RtcCall {
            session: self.session(slot_id, member_id),
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
        Ok(transports::resolve(self.client.backend.as_ref(), chosen).await?)
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

        let backend = &self.client.backend;
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

    fn session(&self, slot_id: String, member_id: String) -> RtcSession<B> {
        let live = Arc::new(AtomicBool::new(true));
        self.sessions().insert(slot_id.clone(), live.clone());
        RtcSession {
            room_id: self.room_id.clone(),
            slot_id,
            member_id,
            state: self.state.clone(),
            live,
        }
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
        if let Some(attachment) = self.attachment.take() {
            drop(attachment);
            self.client.backend.clear_dialect(&self.room_id);
            self.client.release(&self.room_id);
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

    /// Restarts the delayed leave; call periodically while joined. `false`
    /// means there is nothing left to keep alive.
    pub async fn heartbeat(&self) -> bool {
        if !self.is_live() {
            return false;
        }
        self.state.lock().await.heartbeat(&self.slot_id).await
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

    /// See [`matrix_rtc_core::BaseRtcRoom::key_rotation_due_at_ms`].
    pub async fn key_rotation_due_at_ms(&self) -> Option<u64> {
        if !self.is_live() {
            return None;
        }
        self.state
            .lock()
            .await
            .rtc()
            .key_rotation_due_at_ms(&self.slot_id)
    }

    /// See [`matrix_rtc_core::BaseRtcRoom::flush_due_key_rotation`].
    pub async fn flush_due_key_rotation(&self) -> bool {
        self.is_live()
            && self
                .state
                .lock()
                .await
                .rtc()
                .flush_due_key_rotation(&self.slot_id)
                .await
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
    }
}

/// Our participation in a call slot: an [`RtcSession`] (reached through
/// `Deref`) plus reactions and the raised hand.
pub struct RtcCall<B: MatrixBackend + 'static> {
    session: RtcSession<B>,
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
