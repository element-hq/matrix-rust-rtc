// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Native UniFFI bindings for the MatrixRTC core.
//!
//! The host implements one [`MatrixBackend`] with its Matrix client and
//! builds an [`RtcClient`] over it; the client opens an [`RtcRoom`] per room,
//! and a room hands out an [`RtcCall`] per slot joined. The library
//! subscribes, orders and feeds itself.
//! This module defines the UniFFI-facing DTOs and object wrappers and converts
//! them into core DTOs so `matrix-rtc-core` stays decoupled from FFI types.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::RwLock as TokioRwLock;
use tokio::sync::watch;

#[cfg(feature = "media")]
use matrix_rtc_call::compat::DialectBackend;
use matrix_rtc_call::{self as call, RtcError};
use matrix_rtc_core::JoinedMembership as CoreJoinedMembership;
#[cfg(feature = "media")]
use matrix_rtc_core::MatrixBackend as CoreBackend;
mod backend;
pub mod compat;
mod logging;
mod params;
mod runtime;
pub use backend::{
    BackendSubscription, FfiBackend, FfiBackendError, FfiEventEncryption, FfiEventIn,
    FfiOpenIdToken, FfiRoomSubjects, FfiToDeviceDelivery, FfiToDeviceMessageIn,
    FfiToDeviceRecipient, MatrixBackend, RoomSink, ToDeviceSink,
};
pub use compat::FfiElementCallCompat;
pub use logging::{
    RtcLogConfig, RtcLogLevel, RtcLogRecord, RtcLogSink, dropped_log_record_count, log_event,
    setup_logging,
};
pub use params::{
    FfiEncryptionConfig, FfiJoinSessionParams, FfiLeaveSessionParams, FfiNotificationType,
    FfiNotifyConfig, FfiReactionsConfig, FfiTransportConfig,
};

/// Participants with observable frame streams, publishing, and constraints —
/// see the module docs. Pulls the LiveKit client (libwebrtc): default off.
#[cfg(feature = "media")]
pub mod media;

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum MatrixRtcFfiError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("internal lock poisoned")]
    InternalLockPoisoned,
    /// A reaction or raised hand could not be sent: not joined, reactions
    /// disabled, inside the send cooldown, or the host's send failed. The
    /// message says which.
    #[error("reaction: {0}")]
    Reaction(String),
    /// The room is open already (for `RtcClient::room`), or has been shut down.
    #[error("room attachment: {0}")]
    Attachment(String),
    /// The call has left, or its room was shut down; join again for a new one.
    #[error("the call is over: {0}")]
    CallOver(String),
    /// The host's backend failed a read.
    #[error("backend: {0}")]
    Backend(String),
}

impl From<matrix_rtc_call::ReactionError> for MatrixRtcFfiError {
    fn from(error: matrix_rtc_call::ReactionError) -> Self {
        Self::Reaction(error.to_string())
    }
}

impl From<RtcError> for MatrixRtcFfiError {
    fn from(error: RtcError) -> Self {
        match error {
            RtcError::RoomAlreadyOpen(error) => Self::Attachment(error.to_string()),
            RtcError::Backend(error) => Self::Backend(error.to_string()),
            RtcError::Reaction(error) => Self::Reaction(error.to_string()),
            error @ RtcError::SessionOver => Self::CallOver(error.to_string()),
            error => Self::InvalidInput(error.to_string()),
        }
    }
}

impl From<matrix_rtc_core::BackendError> for MatrixRtcFfiError {
    fn from(error: matrix_rtc_core::BackendError) -> Self {
        Self::Backend(error.to_string())
    }
}

/// MSC4143 `leave_reason`: a machine-readable `code` plus an optional
/// human-readable `reason`.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct FfiLeaveReason {
    pub code: String,
    pub reason: Option<String>,
}

impl From<FfiLeaveReason> for matrix_rtc_core::LeaveReason {
    fn from(value: FfiLeaveReason) -> Self {
        matrix_rtc_core::LeaveReason {
            code: matrix_rtc_core::LeaveCode::from_code(&value.code),
            reason: value.reason,
        }
    }
}

/// The RTC encryption mechanism an `m.rtc.slot` prescribes for its members.
///
/// Its presence is what turns RTC encryption on for the slot; its absence turns
/// it off. MSC4143 requires it in an encrypted room and forbids it elsewhere, and
/// a slot that gets this wrong resolves *closed* for every client — which reads
/// as the call simply never starting.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiSlotEncryption {
    /// MSC4143 `m.per_member`: each member distributes its own media key. The
    /// only mechanism this SDK implements, and what an encrypted room wants.
    PerMember,
    /// A mechanism named by string. Opening a slot with one this SDK does not
    /// implement means it cannot join that slot itself; it is here so a host can
    /// still publish one rather than being unable to express it.
    Other { encryption_type: String },
}

impl From<FfiSlotEncryption> for matrix_rtc_core::SlotEncryption {
    fn from(value: FfiSlotEncryption) -> Self {
        matrix_rtc_core::SlotEncryption {
            encryption_type: match value {
                FfiSlotEncryption::PerMember => "m.per_member".to_owned(),
                FfiSlotEncryption::Other { encryption_type } => encryption_type,
            },
            extra: std::collections::BTreeMap::new(),
        }
    }
}

/// A transport a member publishes media on (MSC4143 `transports.published`).
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiRtcTransport {
    /// MSC4195 LiveKit transport.
    LiveKit { livekit_service_url: String },
    /// A transport this SDK does not know; kept for forward compatibility.
    Unsupported { transport_type: String },
}

impl From<&matrix_rtc_core::RtcTransport> for FfiRtcTransport {
    fn from(transport: &matrix_rtc_core::RtcTransport) -> Self {
        match transport {
            matrix_rtc_core::RtcTransport::LiveKit(livekit) => FfiRtcTransport::LiveKit {
                livekit_service_url: livekit.livekit_service_url.clone(),
            },
            matrix_rtc_core::RtcTransport::Unsupported(unsupported) => {
                FfiRtcTransport::Unsupported {
                    transport_type: unsupported.transport_type.clone(),
                }
            }
        }
    }
}

/// A member whose hand is up (mirrors `matrix_rtc_call::RaisedHand`).
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct FfiRaisedHand {
    /// `member.id` of the membership the hand belongs to.
    pub member_id: String,
    /// The member's user id.
    pub sender: String,
    /// The `m.reaction` event that raised it.
    pub reaction_event_id: String,
    /// When it was raised (ms since the epoch, by the server's clock). Sort
    /// ascending to order speakers.
    pub raised_at_ms: u64,
}

impl From<matrix_rtc_call::RaisedHand> for FfiRaisedHand {
    fn from(hand: matrix_rtc_call::RaisedHand) -> Self {
        Self {
            member_id: hand.member_id,
            sender: hand.sender,
            reaction_event_id: hand.reaction_event_id,
            raised_at_ms: hand.raised_at_ms,
        }
    }
}

/// One entry of Element Call's reaction catalogue (see [`reaction_catalog`]).
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct FfiReactionKind {
    /// The `name` to send, and the key peers pick a sound by.
    pub name: String,
    /// The emoji Element Call shows for it.
    pub emoji: String,
    /// Base name of the sound asset Element Call plays for it (`clap`,
    /// `party`, …), or `None` for a silent reaction.
    pub sound: Option<String>,
}

/// Element Call's reaction catalogue, in the order its picker shows them.
///
/// The names are the interoperable part: a reaction sent with one of these
/// names plays the same sound on Element Call as on a host that bundles the
/// assets under the `sound` base names here, plus `generic` for names outside
/// the catalogue. The SDK plays nothing itself.
#[uniffi::export]
pub fn reaction_catalog() -> Vec<FfiReactionKind> {
    matrix_rtc_call::KNOWN_REACTIONS
        .iter()
        .map(|kind| FfiReactionKind {
            name: kind.name.to_owned(),
            emoji: kind.emoji.to_owned(),
            sound: kind.sound.map(str::to_owned),
        })
        .collect()
}

/// The sound asset to play for a reaction `name`: a catalogue entry's sound,
/// `generic` for a name outside the catalogue, or `None` for a silent one.
#[uniffi::export]
pub fn reaction_sound_for(name: String) -> Option<String> {
    matrix_rtc_call::sound_for(&name)
        .asset_name()
        .map(str::to_owned)
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct JoinedMembership {
    pub room_id: String,
    pub slot_id: String,
    pub sender: String,
    pub sender_device_id: Option<String>,
    pub sticky_key: String,
    pub member_id: String,
    /// Event id of the member's latest membership event. Moves on every sticky
    /// refresh.
    pub membership_event_id: Option<String>,
    pub application: Option<String>,
    /// Transports this member publishes media on (MSC4143).
    pub transports: Vec<FfiRtcTransport>,
    /// Transport types this member can subscribe to (MSC4143).
    pub can_subscribe: Vec<String>,
}

/// How a room is opened.
#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct FfiRoomOptions {
    /// Which MatrixRTC generation the room is read and written for. Unset (or
    /// `Off`) is spec-current. One decision for the room: what the library
    /// subscribes to, how it renders our sends, the `member.id` we join with,
    /// how an inbound media key is bound, the SFU identity and the token
    /// endpoint. See [`crate::compat`].
    #[uniffi(default = None)]
    pub element_call_compat: Option<FfiElementCallCompat>,
}

/// One per Matrix session, over the host's backend. Creating it does no I/O;
/// the library holds nothing about a room until the host opens one.
#[derive(uniffi::Object)]
pub struct RtcClient {
    client: call::RtcClient<FfiBackend>,
}

#[uniffi::export(async_runtime = "tokio")]
impl RtcClient {
    #[uniffi::constructor]
    pub fn new(backend: Arc<dyn MatrixBackend>) -> Arc<Self> {
        log::info!("client: created over the host backend");
        Arc::new(Self {
            client: call::RtcClient::new(Arc::new(FfiBackend::new(backend))),
        })
    }

    /// Opens a room: the library subscribes to what the room needs in the
    /// given mode and applies its current state. Resolves once that state is
    /// applied, so a `join_call` issued afterwards sees it.
    ///
    /// Opening a room that already has a live room object is an error.
    /// Cancelling the call leaves nothing behind. Dropping the returned room
    /// ends its subscriptions without leaving; `shutdown` leaves first.
    pub async fn room(
        self: Arc<Self>,
        room_id: String,
        options: FfiRoomOptions,
    ) -> Result<Arc<RtcRoom>, MatrixRtcFfiError> {
        let compat = compat::resolve(options.element_call_compat);
        log::info!("client: [{room_id}] opening in {compat:?} mode");
        // On the library's runtime: the room's feeds are spawned from here.
        runtime::on_runtime(async move {
            let room = self
                .client
                .room(
                    room_id.clone(),
                    call::RoomOptions {
                        element_call_compat: compat,
                    },
                )
                .await?;
            room.seeded().await;
            log::info!("client: [{room_id}] open and seeded");
            Ok(Arc::new(RtcRoom {
                room_id,
                room: TokioRwLock::new(Some(room)),
            }))
        })
        .await
    }
}

/// One open room. Everything room-scoped is here; our own participation is on
/// the [`RtcCall`] that `join_call` returns.
#[derive(uniffi::Object)]
pub struct RtcRoom {
    room_id: String,
    /// `None` once shut down.
    room: TokioRwLock<Option<call::RtcRoom<FfiBackend>>>,
}

impl RtcRoom {
    async fn open(
        &self,
    ) -> Result<tokio::sync::RwLockReadGuard<'_, call::RtcRoom<FfiBackend>>, MatrixRtcFfiError>
    {
        tokio::sync::RwLockReadGuard::try_map(self.room.read().await, Option::as_ref).map_err(
            |_| MatrixRtcFfiError::Attachment(format!("{} has been shut down", self.room_id)),
        )
    }

    /// [`Self::join_call`] with the keep-alive interval spelled out, so a test
    /// can beat faster than a call ships with. On the library's runtime: the
    /// call's upkeep is spawned from here.
    pub(crate) async fn join_call_every(
        self: Arc<Self>,
        params: FfiJoinSessionParams,
        interval: Option<Duration>,
    ) -> Result<Arc<RtcCall>, MatrixRtcFfiError> {
        runtime::on_runtime(async move { self.join_on_runtime(params, interval).await }).await
    }

    async fn join_on_runtime(
        &self,
        params: FfiJoinSessionParams,
        interval: Option<Duration>,
    ) -> Result<Arc<RtcCall>, MatrixRtcFfiError> {
        log::info!(
            "room: [{}] join requested {}",
            self.room_id,
            params.summary()
        );
        let mut options = params.into_call().map_err(|error| {
            log::warn!("room: join rejected before it started: {error}");
            MatrixRtcFfiError::InvalidInput(error.to_string())
        })?;
        options.join.keep_alive_interval_ms = interval.map(|interval| interval.as_millis() as u64);
        let room = self.open().await?;
        let joined = room.join_call(options).await.inspect_err(|error| {
            log::warn!("room: [{}] join failed: {error}", self.room_id);
        })?;
        let joined = Arc::new(joined);
        log::info!(
            "room: [{}/{}] joined as {}",
            self.room_id,
            joined.slot_id(),
            joined.member_id()
        );
        Ok(Arc::new(RtcCall {
            call: joined,
            #[cfg(feature = "media")]
            compat: room.element_call_compat(),
            #[cfg(feature = "media")]
            backend: room.backend().clone(),
        }))
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl RtcRoom {
    pub fn room_id(&self) -> String {
        self.room_id.clone()
    }

    /// Opens a slot, by publishing its `m.rtc.slot` state event.
    ///
    /// A room has no slot until somebody with the power level opens one — an
    /// administrator, or the room's creator as initial state — and a join into a
    /// room whose state holds no open slot fails. The library never opens one on
    /// its own; this is the helper for a host that does.
    ///
    /// `slot_id` must start with `{application_type}#` — MSC4143 makes the slot
    /// id the state key and requires that shape, and a slot that ignores it is
    /// one every client treats as closed. Rejected here rather than at the
    /// homeserver, which would accept it.
    ///
    /// `encryption` must be [`FfiSlotEncryption::PerMember`] in an encrypted room
    /// and `null` elsewhere; the mismatch resolves the slot closed for everyone.
    pub async fn open_slot(
        &self,
        slot_id: String,
        application_type: String,
        encryption: Option<FfiSlotEncryption>,
    ) -> Result<(), MatrixRtcFfiError> {
        log::info!(
            "room: [{}/{slot_id}] opening slot: application={application_type} \
             encryption={encryption:?}",
            self.room_id,
        );
        let room = self.open().await?;
        room.open_slot(slot_id, application_type, encryption.map(Into::into))
            .await
            .map_err(|error| {
                log::warn!("room: could not open the slot: {error}");
                MatrixRtcFfiError::InvalidInput(error.to_string())
            })
    }

    /// Closes a slot, by setting its `m.rtc.slot` status to `closed`.
    ///
    /// Every member of it becomes left as soon as clients apply the new state —
    /// this ends the call for everyone, not just for us. Leaving is
    /// [`RtcCall::leave`].
    pub async fn close_slot(&self, slot_id: String) -> Result<(), MatrixRtcFfiError> {
        log::info!("room: [{}/{slot_id}] closing slot", self.room_id);
        let room = self.open().await?;
        room.close_slot(slot_id).await.map_err(|error| {
            log::warn!("room: could not close the slot: {error}");
            MatrixRtcFfiError::InvalidInput(error.to_string())
        })
    }

    /// How many members are joined to a slot, without joining it.
    pub async fn member_count(&self, slot_id: String) -> Result<u64, MatrixRtcFfiError> {
        Ok(self.open().await?.member_count(&slot_id).await as u64)
    }

    /// Observe a slot's joined roster, without joining it.
    ///
    /// The subscription yields the current roster on its first
    /// `nextSnapshot()` and then only on change, so a host can attach at any
    /// point without missing the state it attached to.
    pub async fn subscribe_membership_snapshots(
        &self,
        slot_id: String,
    ) -> Result<Arc<MembershipSnapshotSubscription>, MatrixRtcFfiError> {
        let receiver = self.open().await?.observe(&slot_id).await;
        Ok(MembershipSnapshotSubscription::new(receiver))
    }

    /// A JSON dump of everything the room currently believes: its room state,
    /// and every candidate member of each slot with the reason it is or is not
    /// projected as joined.
    ///
    /// For bug reports and for answering "what does Rust think the state is
    /// right now?" without a debugger. Contains no key material.
    pub async fn debug_snapshot(&self) -> Result<String, MatrixRtcFfiError> {
        Ok(self.open().await?.debug_snapshot().await.to_string())
    }

    /// Joins a call slot, returning our participation in it.
    ///
    /// The SDK generates the `member.id` (read it from [`RtcCall::member_id`]);
    /// hosts do not supply one. MSC4143 requires a fresh `member.id` on every
    /// join, and reusing one is silently destructive: the MSC4195 participant
    /// identity is derived from it, so a repeat join keeps the identity peers
    /// already hold a key for while our key index restarts at 0 — every peer
    /// then decrypts our media with the previous call's key and never recovers.
    ///
    /// The returned call keeps itself alive every 10 seconds, and performs its
    /// key rotations when they fall due, until it leaves or is dropped. Fails
    /// when the room's state holds no open slot of this id, or while the slot
    /// is joined through a live call of this room.
    pub async fn join_call(
        self: Arc<Self>,
        params: FfiJoinSessionParams,
    ) -> Result<Arc<RtcCall>, MatrixRtcFfiError> {
        self.join_call_every(params, None).await
    }

    /// Leaves every slot joined through this room, then ends its
    /// subscriptions. Every call of the room is over afterwards; a second
    /// shutdown is a no-op.
    ///
    /// (Named `shutdown` rather than `close`: uniffi already gives every
    /// object a `close()` — Kotlin's `AutoCloseable` — that frees it, which
    /// for a room is dropping it: its subscriptions end and nothing is left.)
    pub async fn shutdown(&self) {
        let room = self.room.write().await.take();
        match room {
            Some(room) => {
                room.close().await;
                log::info!("room: [{}] shut down", self.room_id);
            }
            None => log::debug!("room: [{}] already shut down", self.room_id),
        }
    }
}

/// Our participation in one call slot. Over after [`leave`](Self::leave), after
/// its room shuts down, or once dropped; joining again yields a new one. Dropping
/// it sends no leave: the keep-alive stops, and the membership expires through
/// its delayed leave unless the slot is joined again, which leaves it first.
#[derive(uniffi::Object)]
pub struct RtcCall {
    call: Arc<call::RtcCall<FfiBackend>>,
    /// The room's mode and backend, for the media layer.
    #[cfg(feature = "media")]
    compat: matrix_rtc_call::compat::ElementCallCompat,
    #[cfg(feature = "media")]
    backend: Arc<DialectBackend<FfiBackend>>,
}

impl RtcCall {
    #[cfg(feature = "media")]
    pub(crate) fn inner(&self) -> &Arc<call::RtcCall<FfiBackend>> {
        &self.call
    }

    #[cfg(feature = "media")]
    pub(crate) fn element_call_compat(&self) -> matrix_rtc_call::compat::ElementCallCompat {
        self.compat
    }

    #[cfg(feature = "media")]
    pub(crate) fn backend(&self) -> Arc<dyn CoreBackend> {
        self.backend.clone()
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl RtcCall {
    pub fn room_id(&self) -> String {
        self.call.room_id().to_owned()
    }

    pub fn slot_id(&self) -> String {
        self.call.slot_id().to_owned()
    }

    /// Our `member.id` in this participation.
    pub fn member_id(&self) -> String {
        self.call.member_id().to_owned()
    }

    pub fn is_live(&self) -> bool {
        self.call.is_live()
    }

    /// The event id of our current membership event, or `None` once over.
    /// Moves on every sticky refresh, so read it at the moment of use.
    pub async fn membership_event_id(&self) -> Option<String> {
        self.call.membership_event_id().await
    }

    pub async fn member_count(&self) -> u64 {
        self.call.member_count().await as u64
    }

    /// The slot's joined roster as it changes; see
    /// [`RtcRoom::subscribe_membership_snapshots`].
    pub async fn subscribe_membership_snapshots(&self) -> Arc<MembershipSnapshotSubscription> {
        MembershipSnapshotSubscription::new(self.call.subscribe_memberships().await)
    }

    // ---- Reactions and raised hands ----
    //
    // Element Call's reactions are ordinary room events relating to the
    // reacting member's membership event. The library reads them from the
    // room (timeline events, redactions and the relations of each membership
    // event); the host plays any sound. Results surface on the media session
    // as `FfiCallEvent::HandRaised` / `HandLowered` / `Reaction` and on
    // `FfiParticipant.hand_raised_at_ms`, and here as `raised_hands`.

    /// Sends an Element Call emoji reaction. `name` is what peers pick a sound
    /// by (see [`reaction_catalog`]); only the first grapheme of `emoji` is
    /// sent. Returns the event id.
    ///
    /// Fails inside the send cooldown (Element Call's three seconds by
    /// default), since peers would drop the reaction anyway.
    pub async fn send_reaction(
        &self,
        emoji: String,
        name: String,
    ) -> Result<String, MatrixRtcFfiError> {
        Ok(self.call.send_reaction(&emoji, &name).await?)
    }

    /// Raises our hand. Idempotent while it is up; the hand follows our
    /// membership across sticky refreshes on its own.
    pub async fn raise_hand(&self) -> Result<(), MatrixRtcFfiError> {
        Ok(self.call.raise_hand().await?)
    }

    /// Lowers our hand by redacting the annotation. A no-op when it is down.
    pub async fn lower_hand(&self) -> Result<(), MatrixRtcFfiError> {
        Ok(self.call.lower_hand().await?)
    }

    /// The slot's raised hands, oldest first.
    pub async fn raised_hands(&self) -> Vec<FfiRaisedHand> {
        self.call
            .raised_hands()
            .await
            .into_iter()
            .map(Into::into)
            .collect()
    }

    /// Leaves the slot; the call is over afterwards. A failed leave leaves it
    /// live, so it can be retried.
    pub async fn leave(&self, params: FfiLeaveSessionParams) -> Result<(), MatrixRtcFfiError> {
        log::info!(
            "call: [{}/{}] leave requested reason={:?}",
            self.call.room_id(),
            self.call.slot_id(),
            params.leave_reason,
        );
        self.call
            .leave(params.into_core())
            .await
            .inspect_err(|error| log::warn!("call: leave failed: {error}"))?;
        log::info!("call: leave succeeded");
        Ok(())
    }
}

struct SubscriptionState {
    receiver: watch::Receiver<Vec<CoreJoinedMembership>>,
    initial_pending: bool,
}

#[derive(uniffi::Object)]
pub struct MembershipSnapshotSubscription {
    state: Mutex<SubscriptionState>,
}

impl MembershipSnapshotSubscription {
    fn new(receiver: watch::Receiver<Vec<CoreJoinedMembership>>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(SubscriptionState {
                receiver,
                initial_pending: true,
            }),
        })
    }
}

#[uniffi::export]
impl MembershipSnapshotSubscription {
    pub fn next_snapshot(&self) -> Result<Option<Vec<JoinedMembership>>, MatrixRtcFfiError> {
        let mut state = lock_mutex(&self.state)?;

        let snapshot = if state.initial_pending {
            state.initial_pending = false;
            Some(state.receiver.borrow().clone())
        } else {
            match state.receiver.has_changed() {
                Ok(true) => Some(state.receiver.borrow_and_update().clone()),
                Ok(false) | Err(_) => None,
            }
        };

        Ok(snapshot.map(|members| {
            members
                .into_iter()
                .map(to_ffi_joined_membership)
                .collect::<Vec<_>>()
        }))
    }
}

fn to_ffi_joined_membership(member: CoreJoinedMembership) -> JoinedMembership {
    JoinedMembership {
        room_id: member.room_id,
        slot_id: member.slot_id,
        sender: member.sender,
        sender_device_id: member.origin.sender_device_id().map(str::to_owned),
        sticky_key: member.sticky_key,
        member_id: member.member_id,
        membership_event_id: member.membership_event_id,
        application: member.application.application_type,
        transports: member.transports.iter().map(Into::into).collect(),
        can_subscribe: member.can_subscribe,
    }
}

/// Locks `mutex`, recovering from poisoning rather than propagating it.
///
/// A panic anywhere inside a handle method used to poison the handle for the
/// rest of the process: every later call — `member_count`, and critically
/// `leave` — returned [`MatrixRtcFfiError::InternalLockPoisoned`] forever, so
/// the host could not even depart the session and its membership stayed live
/// until the dead man's switch expired. One panic permanently disabling the
/// manager, including the ability to leave it, is worse than the panic.
///
/// So we take the guard anyway and clear the flag. The state behind it may have
/// been mid-mutation when the panic unwound, which is why this logs at error
/// level: it is a bug worth reporting, not a condition to handle silently.
///
/// Still returns `Result` — the signature is what ~20 call sites and the
/// `media` module expect, and it keeps room for a future fallible lock — but
/// the error path is now unreachable.
fn lock_mutex<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>, MatrixRtcFfiError> {
    match mutex.lock() {
        Ok(guard) => Ok(guard),
        Err(poisoned) => {
            log::error!(
                "recovering a poisoned lock: an earlier call panicked and its state may be \
                 inconsistent. Please report this with the panic that preceded it."
            );
            mutex.clear_poison();
            Ok(poisoned.into_inner())
        }
    }
}

uniffi::setup_scaffolding!();

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::test_support::{Carrier, MockHost};
    use crate::backend::{FfiEventEncryption, FfiEventIn};
    use matrix_rtc_call::compat::STATE_MEMBER_EVENT_TYPE;

    const ROOM: &str = "!room:example.org";
    const SLOT: &str = "m.call#ROOM";
    const SFU: &str = "https://sfu.example.org";

    fn cleartext() -> FfiEventEncryption {
        FfiEventEncryption {
            encrypted: false,
            sender_device_id: None,
            sender_cross_signed: None,
        }
    }

    fn encrypted(device: &str) -> FfiEventEncryption {
        FfiEventEncryption {
            encrypted: true,
            sender_device_id: Some(device.to_owned()),
            sender_cross_signed: Some(true),
        }
    }

    fn open_slot(encryption: Option<&str>) -> FfiEventIn {
        let mut content =
            serde_json::json!({ "status": "open", "application": { "type": "m.call" } });
        if let Some(mechanism) = encryption {
            content["encryption"] = serde_json::json!({ "type": mechanism });
        }
        FfiEventIn {
            event_id: "$slot".to_owned(),
            sender: "@admin:example.org".to_owned(),
            event_type: matrix_rtc_core::SLOT_EVENT_TYPE.to_owned(),
            state_key: Some(SLOT.to_owned()),
            origin_server_ts: 1,
            content_json: content.to_string(),
            encryption: cleartext(),
        }
    }

    fn member_event(sender: &str, device: &str, member_id: &str, event_id: &str) -> FfiEventIn {
        FfiEventIn {
            event_id: event_id.to_owned(),
            sender: sender.to_owned(),
            event_type: "m.rtc.member".to_owned(),
            state_key: None,
            origin_server_ts: 2,
            content_json: serde_json::json!({
                "slot_id": SLOT,
                "msc4354_sticky_key": member_id,
                "application": { "type": "m.call" },
                "member": { "id": member_id, "membership": "join" },
                "transports": {
                    "published": [{ "type": "livekit", "livekit_service_url": SFU }],
                    "can_subscribe": ["livekit"],
                },
            })
            .to_string(),
            encryption: encrypted(device),
        }
    }

    /// Seeds the room's gating subjects and `sticky`, then opens the room.
    async fn open(
        client: &Arc<RtcClient>,
        mock: &MockHost,
        compat: Option<FfiElementCallCompat>,
        encrypted: bool,
        slots: Vec<FfiEventIn>,
        sticky: Vec<FfiEventIn>,
    ) -> Arc<RtcRoom> {
        // The open runs on the library's runtime, so it may not have
        // subscribed by the time the seeder first looks.
        mock.forget_room_sink(ROOM);
        let open = client.clone().room(
            ROOM.to_owned(),
            FfiRoomOptions {
                element_call_compat: compat,
            },
        );
        tokio::pin!(open);
        // The mock stores the sink inside `subscribe_room`; deliver the current
        // sets once it exists, the way a host does on subscribe.
        let seed = async {
            while mock.subjects(ROOM).is_none() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            let subjects = mock.subjects(ROOM).unwrap();
            let sink = mock.room_sink(ROOM);
            sink.on_encryption(encrypted);
            for event_type in &subjects.state_event_types {
                if event_type == matrix_rtc_core::SLOT_EVENT_TYPE {
                    sink.on_state_events(event_type.clone(), slots.clone());
                }
            }
            sink.on_joined_members(vec![
                "@alice:example.org".to_owned(),
                "@bob:example.org".to_owned(),
                "@carl:example.org".to_owned(),
            ]);
            sink.on_sticky_events(sticky.clone());
            if subjects
                .state_event_types
                .iter()
                .any(|t| t == STATE_MEMBER_EVENT_TYPE)
            {
                sink.on_state_events(STATE_MEMBER_EVENT_TYPE.to_owned(), Vec::new());
            }
        };
        let (result, ()) = tokio::join!(open, seed);
        result.expect("open")
    }

    /// [`open`] with an open slot and nobody in it.
    async fn open_call_room(client: &Arc<RtcClient>, mock: &MockHost) -> Arc<RtcRoom> {
        open(client, mock, None, false, vec![open_slot(None)], Vec::new()).await
    }

    fn join_params() -> FfiJoinSessionParams {
        FfiJoinSessionParams {
            slot_id: SLOT.to_owned(),
            application: "m.call".to_owned(),
            transport: Some(FfiTransportConfig {
                r#type: "livekit".to_owned(),
                livekit_service_url: Some(SFU.to_owned()),
            }),
            receive_only: false,
            can_subscribe: Vec::new(),
            keep_alive_timeout_ms: None,
            sticky_duration_ms: None,
            degraded_lifetime_ms: None,
            encryption_config: None,
            notify: None,
            reactions: None,
        }
    }

    fn no_reason() -> FfiLeaveSessionParams {
        FfiLeaveSessionParams { leave_reason: None }
    }

    fn beats(mock: &MockHost) -> usize {
        mock.sent_types()
            .into_iter()
            .filter(|sent| sent == "restart_delayed_event")
            .count()
    }

    async fn wait_until(mut condition: impl AsyncFnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !condition().await {
            assert!(
                tokio::time::Instant::now() < deadline,
                "condition not met in time"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn a_left_call_has_nothing_to_keep_alive() {
        let mock = MockHost::new();
        let client = RtcClient::new(mock.clone());
        let room = open_call_room(&client, &mock).await;
        let call = room.clone().join_call(join_params()).await.expect("join");

        call.leave(no_reason()).await.expect("leave");
        assert!(!call.is_live());
        assert!(matches!(
            call.leave(no_reason()).await,
            Err(MatrixRtcFfiError::CallOver(_))
        ));
    }

    #[tokio::test]
    async fn a_shut_down_room_refuses_further_calls() {
        let mock = MockHost::new();
        let client = RtcClient::new(mock.clone());
        let room = open_call_room(&client, &mock).await;
        room.shutdown().await;
        assert!(matches!(
            room.clone().join_call(join_params()).await,
            Err(MatrixRtcFfiError::Attachment(_))
        ));
        room.shutdown().await;
    }

    #[tokio::test]
    async fn opening_subscribes_in_the_mode_and_seeds_the_roster() {
        let mock = MockHost::new();
        let client = RtcClient::new(mock.clone());
        let room = open(
            &client,
            &mock,
            None,
            false,
            vec![open_slot(None)],
            vec![member_event("@bob:example.org", "BOBDEV", "bob-a", "$m1")],
        )
        .await;

        let subjects = mock.subjects(ROOM).unwrap();
        assert_eq!(
            subjects.state_event_types,
            vec![
                matrix_rtc_core::SLOT_EVENT_TYPE.to_owned(),
                "org.matrix.msc4143.rtc.slot".to_owned()
            ]
        );
        assert!(mock.to_device_sink.lock().unwrap().is_some());

        let subscription = room
            .subscribe_membership_snapshots(SLOT.to_owned())
            .await
            .unwrap();
        let joined = subscription.next_snapshot().unwrap().unwrap();
        assert_eq!(joined.len(), 1);
        assert_eq!(joined[0].sender, "@bob:example.org");
        assert_eq!(joined[0].sender_device_id.as_deref(), Some("BOBDEV"));
        assert_eq!(joined[0].membership_event_id.as_deref(), Some("$m1"));
        assert_eq!(
            joined[0].transports,
            vec![FfiRtcTransport::LiveKit {
                livekit_service_url: SFU.to_owned(),
            }]
        );
        assert_eq!(joined[0].can_subscribe, vec!["livekit".to_owned()]);
        assert_eq!(
            subscription.next_snapshot().unwrap(),
            None,
            "and then only on change"
        );
    }

    #[tokio::test]
    async fn opening_twice_is_an_error_and_shutting_down_ends_every_subscription() {
        let mock = MockHost::new();
        let client = RtcClient::new(mock.clone());
        let room = open_call_room(&client, &mock).await;

        let again = client
            .clone()
            .room(ROOM.to_owned(), FfiRoomOptions::default())
            .await;
        assert!(matches!(again, Err(MatrixRtcFfiError::Attachment(_))));
        // The refused open subscribed to nothing: the room and to-device.
        assert_eq!(mock.live_subscriptions(), 2);

        room.shutdown().await;
        assert_eq!(
            mock.live_subscriptions(),
            0,
            "the last room took the to-device subscription with it"
        );
    }

    #[tokio::test]
    async fn dropping_the_room_ends_every_subscription() {
        let mock = MockHost::new();
        let client = RtcClient::new(mock.clone());
        let room = open_call_room(&client, &mock).await;
        assert_eq!(mock.live_subscriptions(), 2);

        drop(room);
        assert_eq!(mock.live_subscriptions(), 0);
    }

    #[tokio::test]
    async fn an_open_still_seeding_refuses_a_second_and_cancelling_it_frees_the_room() {
        let mock = MockHost::new();
        let client = RtcClient::new(mock.clone());
        {
            let first = client
                .clone()
                .room(ROOM.to_owned(), FfiRoomOptions::default());
            tokio::pin!(first);
            // Subscribed, never seeded.
            tokio::select! {
                _ = &mut first => panic!("an unseeded open must not resolve"),
                _ = async {
                    while mock.subjects(ROOM).is_none() {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                } => {}
            }
            let second = client
                .clone()
                .room(ROOM.to_owned(), FfiRoomOptions::default())
                .await;
            assert!(matches!(second, Err(MatrixRtcFfiError::Attachment(_))));
        }
        // The first open was dropped (its caller cancelled it): its room
        // subscription ended, the room is free again, and with no room left
        // the to-device subscription went too. Asynchronously: the open runs
        // on the library's runtime, where the cancellation lands a moment later.
        wait_until(async || mock.live_subscriptions() == 0).await;
        let _room = open_call_room(&client, &mock).await;
        assert_eq!(mock.live_subscriptions(), 2);
    }

    /// One room, three generations, one roster: a spec-current peer, a 2025
    /// Element Call peer, and a pre-sticky one carried in room state.
    #[tokio::test]
    async fn membership_from_every_generation_lands_in_one_roster() {
        let mock = MockHost::new();
        let client = RtcClient::new(mock.clone());

        let spec = member_event("@alice:example.org", "ALICEDEV", "alice-a", "$a");
        // No `membership`, no `transports` — that generation states neither.
        let legacy_sticky = FfiEventIn {
            event_id: "$b".to_owned(),
            sender: "@bob:example.org".to_owned(),
            event_type: "m.rtc.member".to_owned(),
            state_key: None,
            origin_server_ts: 2,
            content_json: serde_json::json!({
                "slot_id": SLOT,
                "msc4354_sticky_key": "bob-a",
                "application": { "type": "m.call" },
                "member": { "id": "bob-a", "user_id": "@bob:example.org", "device_id": "BOBDEV" },
                "rtc_transports": [{ "type": "livekit", "livekit_service_url": SFU }],
                "versions": [],
            })
            .to_string(),
            encryption: encrypted("BOBDEV"),
        };
        let pre_sticky = FfiEventIn {
            event_id: "$c".to_owned(),
            sender: "@carl:example.org".to_owned(),
            event_type: STATE_MEMBER_EVENT_TYPE.to_owned(),
            state_key: Some("_@carl:example.org_CARLDEV_m.call".to_owned()),
            origin_server_ts: matrix_rtc_call::compat::element_call_state::now_ms(),
            content_json: serde_json::json!({
                "application": "m.call",
                "call_id": "",
                "device_id": "CARLDEV",
                "expires": 14_400_000_u64,
                "membershipID": "@carl:example.org:CARLDEV",
                "foci_preferred": [{ "type": "livekit", "livekit_service_url": SFU }],
            })
            .to_string(),
            encryption: cleartext(),
        };

        let room = open(
            &client,
            &mock,
            Some(FfiElementCallCompat::StateEvents),
            false,
            Vec::new(),
            vec![spec, legacy_sticky],
        )
        .await;
        mock.room_sink(ROOM)
            .on_state_events(STATE_MEMBER_EVENT_TYPE.to_owned(), vec![pre_sticky]);
        wait_until(async || room.member_count(SLOT.to_owned()).await.unwrap() == 3).await;

        let subscription = room
            .subscribe_membership_snapshots(SLOT.to_owned())
            .await
            .unwrap();
        let mut joined = subscription.next_snapshot().unwrap().unwrap();
        joined.sort_by(|a, b| a.sender.cmp(&b.sender));
        assert_eq!(
            joined
                .iter()
                .map(|member| member.sender_device_id.as_deref())
                .collect::<Vec<_>>(),
            [Some("ALICEDEV"), Some("BOBDEV"), Some("CARLDEV")],
        );
        assert!(
            joined
                .iter()
                .all(|member| member.transports.len() == 1 && member.can_subscribe == ["livekit"]),
            "every generation's SFU must survive the translation: {joined:?}",
        );
    }

    /// The SDK owns the `member.id`, and a rejoin must not reuse the previous
    /// one. The transport comes from the backend when the join names none.
    #[tokio::test]
    async fn every_join_gets_a_fresh_member_id_and_the_advertised_transport() {
        let mock = MockHost::new();
        *mock.transports_json.lock().unwrap() =
            serde_json::json!([{ "type": "livekit", "livekit_service_url": SFU }]).to_string();
        let client = RtcClient::new(mock.clone());
        let room = open_call_room(&client, &mock).await;

        let first = room
            .clone()
            .join_call(FfiJoinSessionParams {
                transport: None,
                ..join_params()
            })
            .await
            .expect("first join");
        let membership = mock
            .sends()
            .into_iter()
            .find(|send| send.carrier == Carrier::Sticky)
            .expect("the membership went out");
        // The host sees the unstable id: peers do not match on `m.rtc.member`.
        assert_eq!(membership.event_type, "org.matrix.msc4143.rtc.member");
        assert_eq!(
            membership
                .content
                .pointer("/transports/published/0/livekit_service_url")
                .unwrap(),
            &serde_json::json!(SFU),
        );

        first.leave(no_reason()).await.expect("leave");
        let second = room.clone().join_call(join_params()).await.expect("rejoin");
        assert_ne!(
            first.member_id(),
            second.member_id(),
            "a rejoin must not reuse the member id"
        );
    }

    #[tokio::test]
    async fn a_second_call_in_a_joined_slot_is_refused() {
        let mock = MockHost::new();
        let client = RtcClient::new(mock.clone());
        let room = open_call_room(&client, &mock).await;
        let _call = room.clone().join_call(join_params()).await.expect("join");
        assert!(matches!(
            room.clone().join_call(join_params()).await,
            Err(MatrixRtcFfiError::InvalidInput(_))
        ));
    }

    #[tokio::test]
    async fn a_join_into_a_room_with_no_open_slot_is_refused() {
        let mock = MockHost::new();
        let client = RtcClient::new(mock.clone());
        let room = open(&client, &mock, None, false, Vec::new(), Vec::new()).await;

        let result = room.clone().join_call(join_params()).await;
        assert!(result.is_err(), "no open slot, no join");
        assert!(mock.sends().is_empty(), "nothing should have been sent");
    }

    #[tokio::test]
    async fn the_keep_alive_driver_restarts_the_delayed_leave() {
        let mock = MockHost::new();
        let client = RtcClient::new(mock.clone());
        let room = open_call_room(&client, &mock).await;

        let _call = room
            .clone()
            .join_call_every(join_params(), Some(Duration::from_millis(50)))
            .await
            .expect("join");
        tokio::time::sleep(Duration::from_millis(300)).await;

        let beats = beats(&mock);
        assert!(
            beats >= 2,
            "saw {beats} beats over 300ms at a 50ms interval"
        );
    }

    #[tokio::test]
    async fn leaving_stops_the_keep_alive_driver() {
        let mock = MockHost::new();
        let client = RtcClient::new(mock.clone());
        let room = open_call_room(&client, &mock).await;

        let call = room
            .clone()
            .join_call_every(join_params(), Some(Duration::from_millis(50)))
            .await
            .expect("join");
        tokio::time::sleep(Duration::from_millis(150)).await;

        call.leave(no_reason()).await.expect("leave");
        let after_leave = beats(&mock);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(beats(&mock), after_leave, "no beat once the call has left");
    }

    #[tokio::test]
    async fn dropping_a_call_stops_its_keep_alive_without_leaving() {
        let mock = MockHost::new();
        let client = RtcClient::new(mock.clone());
        let room = open_call_room(&client, &mock).await;

        let call = room
            .clone()
            .join_call_every(join_params(), Some(Duration::from_millis(50)))
            .await
            .expect("join");
        tokio::time::sleep(Duration::from_millis(150)).await;

        drop(call);
        let after_drop = beats(&mock);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(beats(&mock), after_drop, "no beat once the call is dropped");
        assert!(
            !mock
                .sent_types()
                .iter()
                .any(|sent| sent == "cancel_delayed_event"),
            "dropping sends no leave"
        );
    }

    #[tokio::test]
    async fn shutting_down_leaves_the_joined_call() {
        let mock = MockHost::new();
        let client = RtcClient::new(mock.clone());
        let room = open_call_room(&client, &mock).await;
        let call = room.clone().join_call(join_params()).await.expect("join");

        room.shutdown().await;
        assert!(
            mock.sent_types()
                .iter()
                .any(|sent| sent == "cancel_delayed_event"),
            "the leave cancels the delayed leave: {:?}",
            mock.sent_types()
        );
        assert!(!call.is_live());
    }

    /// Driven through the objects an FFI host holds: the rejoin must
    /// distribute a key to the incumbent even though no sticky event moved.
    #[tokio::test]
    async fn a_rejoin_distributes_keys_without_new_sticky_events() {
        let mock = MockHost::new();
        let client = RtcClient::new(mock.clone());
        let room = open(
            &client,
            &mock,
            None,
            true,
            vec![open_slot(Some("m.per_member"))],
            Vec::new(),
        )
        .await;

        let first = room
            .clone()
            .join_call(join_params())
            .await
            .expect("first join");
        mock.room_sink(ROOM).on_sticky_events(vec![member_event(
            "@bob:example.org",
            "BOBDEV",
            "bob-a",
            "$b",
        )]);
        wait_until(async || !mock.to_device_for("@bob:example.org", "BOBDEV").is_empty()).await;

        first.leave(no_reason()).await.expect("leave");
        mock.clear_to_device();

        let second = room.clone().join_call(join_params()).await.expect("rejoin");
        let sent = mock.to_device_for("@bob:example.org", "BOBDEV");
        assert!(!sent.is_empty(), "the second call distributed no key");
        assert_eq!(
            sent[0].pointer("/member_id").and_then(|v| v.as_str()),
            Some(second.member_id().as_str()),
        );
    }

    #[tokio::test]
    async fn opening_and_closing_a_slot_publishes_the_state_a_peer_reads() {
        let mock = MockHost::new();
        let client = RtcClient::new(mock.clone());
        let room = open(&client, &mock, None, false, Vec::new(), Vec::new()).await;

        room.open_slot(
            SLOT.to_owned(),
            "m.call".to_owned(),
            Some(FfiSlotEncryption::PerMember),
        )
        .await
        .expect("open");
        room.close_slot(SLOT.to_owned()).await.expect("close");

        let sends = mock.sends();
        assert_eq!(sends.len(), 2);
        assert_eq!(sends[0].event_type, "org.matrix.msc4143.rtc.slot");
        assert_eq!(sends[0].state_key.as_deref(), Some(SLOT));
        assert_eq!(
            sends[0].content,
            serde_json::json!({
                "status": "open",
                "application": { "type": "m.call" },
                "encryption": { "type": "m.per_member" },
            }),
        );
        assert_eq!(
            sends[1].content.get("status").unwrap(),
            &serde_json::json!("closed")
        );
    }

    #[tokio::test]
    async fn a_slot_id_that_contradicts_its_application_is_refused() {
        let mock = MockHost::new();
        let client = RtcClient::new(mock.clone());
        let room = open(&client, &mock, None, false, Vec::new(), Vec::new()).await;
        let result = room
            .open_slot(SLOT.to_owned(), "m.something.else".to_owned(), None)
            .await;
        assert!(result.is_err());
        assert!(mock.sends().is_empty());
    }

    // --- Element Call compatibility ------------------------------------------
    //
    // The dialects are tested in `matrix_rtc_call::compat`. Tested here: the
    // mode chosen when the room opens reaches every send, the two of the join
    // included.

    #[tokio::test]
    async fn a_sticky_compat_join_is_readable_by_both_generations() {
        let mock = MockHost::new();
        let client = RtcClient::new(mock.clone());
        let room = open(
            &client,
            &mock,
            Some(FfiElementCallCompat::StickyEvents),
            false,
            vec![open_slot(None)],
            Vec::new(),
        )
        .await;

        let call = room.clone().join_call(join_params()).await.expect("join");
        let membership = mock
            .sends()
            .into_iter()
            .find(|send| send.carrier == Carrier::Sticky)
            .expect("the membership should still be a sticky event");
        assert_eq!(membership.event_type, "org.matrix.msc4143.rtc.member");
        assert_eq!(
            membership.content.pointer("/member/id").unwrap(),
            &serde_json::json!(call.member_id()),
        );
        assert_eq!(
            membership.content.pointer("/member/user_id").unwrap(),
            &serde_json::json!("@alice:example.org"),
        );
        assert_eq!(
            membership.content.pointer("/member/device_id").unwrap(),
            &serde_json::json!("DEVICE"),
        );
        assert_eq!(
            membership
                .content
                .pointer("/rtc_transports/0/livekit_service_url")
                .unwrap(),
            &serde_json::json!(SFU),
        );
    }

    #[tokio::test]
    async fn a_pre_sticky_join_publishes_room_state() {
        let mock = MockHost::new();
        let client = RtcClient::new(mock.clone());
        // No slot state is asked for in this mode: that generation has none.
        let room = open(
            &client,
            &mock,
            Some(FfiElementCallCompat::StateEvents),
            false,
            Vec::new(),
            Vec::new(),
        )
        .await;
        assert_eq!(
            mock.subjects(ROOM).unwrap().state_event_types,
            vec![STATE_MEMBER_EVENT_TYPE.to_owned()]
        );

        let call = room
            .clone()
            .join_call(FfiJoinSessionParams {
                keep_alive_timeout_ms: Some(30_000),
                ..join_params()
            })
            .await
            .expect("join");
        let member_id = call.member_id();
        assert_eq!(member_id, "@alice:example.org:DEVICE");

        let sends = mock.sends();
        assert!(sends.iter().all(|send| send.carrier != Carrier::Sticky));
        let membership = sends
            .iter()
            .find(|send| send.carrier == Carrier::State)
            .expect("the membership should be room state");
        assert_eq!(membership.event_type, "org.matrix.msc3401.call.member");
        assert_eq!(
            membership.state_key.as_deref(),
            Some("_@alice:example.org_DEVICE_m.call"),
        );
        assert_eq!(
            membership.content.get("membershipID").unwrap(),
            &serde_json::json!(member_id),
        );
        let delayed = sends
            .iter()
            .find(|send| send.carrier == Carrier::DelayedState)
            .expect("the delayed leave should be a delayed state event");
        assert_eq!(delayed.state_key, membership.state_key);
        assert_eq!(delayed.content, serde_json::json!({}));
    }

    #[tokio::test]
    async fn a_pre_sticky_join_survives_a_homeserver_without_delayed_events() {
        let mock = MockHost::new();
        mock.refuse_delayed
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let client = RtcClient::new(mock.clone());
        let room = open(
            &client,
            &mock,
            Some(FfiElementCallCompat::StateEvents),
            false,
            Vec::new(),
            Vec::new(),
        )
        .await;

        room.clone()
            .join_call(join_params())
            .await
            .expect("a refused delayed leave must not fail the join");
        let sends = mock.sends();
        assert!(
            sends
                .iter()
                .all(|send| send.carrier != Carrier::DelayedState)
        );
        let membership = sends
            .iter()
            .find(|send| send.carrier == Carrier::State)
            .expect("the membership is still published as room state");
        assert!(
            membership
                .content
                .get("expires")
                .and_then(serde_json::Value::as_u64)
                .is_some_and(|expires| expires > 0)
        );
    }

    /// A mode is per room object: a later spec-current open of the same room
    /// must not inherit the dialect a previous one installed.
    #[tokio::test]
    async fn shutting_down_forgets_the_dialect() {
        let mock = MockHost::new();
        let client = RtcClient::new(mock.clone());
        let room = open(
            &client,
            &mock,
            Some(FfiElementCallCompat::StateEvents),
            false,
            Vec::new(),
            Vec::new(),
        )
        .await;
        let _call = room.clone().join_call(join_params()).await.expect("join");
        room.shutdown().await;
        assert!(
            mock.sends()
                .iter()
                .filter(|send| send.carrier == Carrier::State)
                .any(|send| send.content == serde_json::json!({})),
            "the leave emptied our state membership",
        );

        let room = open_call_room(&client, &mock).await;
        let _call = room.clone().join_call(join_params()).await.expect("rejoin");
        assert!(
            mock.sends()
                .iter()
                .any(|send| send.carrier == Carrier::Sticky
                    && send.event_type == "org.matrix.msc4143.rtc.member"),
            "a spec-current rejoin goes back to a sticky membership",
        );
    }

    /// A panic inside one method must not disable an object forever.
    #[test]
    fn a_poisoned_lock_is_recovered_rather_than_propagated() {
        let mutex = Mutex::new(0_u32);

        let panicked = std::panic::catch_unwind(|| {
            let mut guard = mutex.lock().unwrap();
            *guard = 1;
            panic!("simulates a panic while the guard is held");
        });
        assert!(panicked.is_err());
        assert!(mutex.is_poisoned());

        let guard = lock_mutex(&mutex).expect("a poisoned lock must still be usable");
        assert_eq!(*guard, 1);
        drop(guard);
        assert!(!mutex.is_poisoned());
    }
}
