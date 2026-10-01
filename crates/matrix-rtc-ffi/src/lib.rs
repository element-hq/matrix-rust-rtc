// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Native UniFFI bindings for the MatrixRTC core.
//!
//! The host implements one [`MatrixBackend`] with its Matrix client; the
//! handle attaches rooms, and the library subscribes, orders and feeds itself.
//! This module defines the UniFFI-facing DTOs and object wrappers and converts
//! them into core DTOs so `matrix-rtc-core` stays decoupled from FFI types.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::watch;

use matrix_rtc_bridge::compat::DialectBackend;
use matrix_rtc_bridge::feeder::{
    AttachOptions, RoomAttachment, RoomFeeder, RoomModes, SessionFeeder,
};
use matrix_rtc_bridge::transports;
use matrix_rtc_call::CallSessionManager;
use matrix_rtc_core::{
    JoinedMembership as CoreJoinedMembership, MatrixBackend as CoreBackend, RtcSessionManager,
};
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

type Backend = DialectBackend<FfiBackend>;
type Manager = CallSessionManager<Backend>;

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
    /// The room is not attached (or already is, for `attach_room`).
    #[error("room attachment: {0}")]
    Attachment(String),
    /// The host's backend failed a read.
    #[error("backend: {0}")]
    Backend(String),
}

impl From<matrix_rtc_call::ReactionError> for MatrixRtcFfiError {
    fn from(error: matrix_rtc_call::ReactionError) -> Self {
        Self::Reaction(error.to_string())
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

/// How a room is attached.
#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct FfiAttachOptions {
    /// Which MatrixRTC generation the room is read and written for. Unset (or
    /// `Off`) is spec-current. One decision for the room: what the library
    /// subscribes to, how it renders our sends, the `member.id` we join with,
    /// how an inbound media key is bound, the SFU identity and the token
    /// endpoint. See [`crate::compat`].
    #[uniffi(default = None)]
    pub element_call_compat: Option<FfiElementCallCompat>,
}

/// Aborts the wrapped task when dropped.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A room the handle is feeding.
struct AttachedRoom {
    attachment: RoomAttachment,
    _feed: AbortOnDrop,
}

#[derive(uniffi::Object)]
pub struct RtcSessionManagerHandle {
    /// An async mutex because every entry point is async and holds it across
    /// awaits into the host. `Arc` so a heartbeat driver can hold a `Weak` to it
    /// without keeping the manager alive past the handle.
    inner: Arc<TokioMutex<Manager>>,
    /// The host's backend behind the dialect wrapper. The manager holds the
    /// same `Arc`.
    backend: Arc<Backend>,
    /// One driver per joined session, keyed by `(room_id, slot_id)`. Dropping
    /// the entry stops its task. A `std::sync::Mutex` on purpose: it is only
    /// ever held for a map insert or remove, never across an await.
    heartbeats: Mutex<HashMap<(String, String), HeartbeatDriver>>,
    /// Which generation each attached room is read and written for.
    modes: RoomModes,
    /// The attached rooms, by room id.
    rooms: Mutex<HashMap<String, AttachedRoom>>,
    /// The session-wide to-device subscription, started by the first attach.
    session_feeder: TokioMutex<Option<(SessionFeeder, AbortOnDrop)>>,
}

/// How often the keep-alive is driven.
///
/// Three ticks inside the 30 s default delayed-leave timeout, so a skipped tick
/// (the manager was busy) or one slow round trip cannot let the switch fire.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// Owns the task that drives one session's keep-alive.
struct HeartbeatDriver {
    /// Dropped to ask the task to stop; it observes the closed channel.
    _stop: tokio::sync::mpsc::Sender<()>,
}

/// Runs one session's keep-alive until the session ends or the handle goes away.
async fn run_heartbeat(
    manager: Weak<TokioMutex<Manager>>,
    room_id: String,
    slot_id: String,
    interval: Duration,
    mut stop: tokio::sync::mpsc::Receiver<()>,
) {
    loop {
        tokio::select! {
            // The driver was dropped (leave, rejoin, or the handle died), so a
            // stop takes effect at once rather than at the end of the interval.
            _ = stop.recv() => break,
            _ = tokio::time::sleep(interval) => {}
        }

        let Some(manager) = manager.upgrade() else {
            log::debug!("[{room_id}/{slot_id}] heartbeat: manager gone, stopping");
            break;
        };

        // Skip rather than queue behind an in-flight FFI call: the next tick is
        // 10 s away and the dead man's switch has 30 s, so waiting our turn
        // behind a slow host would only make the beat later than it needs to be.
        let still_joined = match manager.try_lock() {
            Ok(mut guard) => guard.heartbeat(&room_id, &slot_id).await,
            Err(_) => {
                log::debug!("[{room_id}/{slot_id}] heartbeat: manager busy, skipping a tick");
                true
            }
        };

        // `false` means the session is gone or has left — `leave` takes the
        // membership machine, so a beat racing a leave is a no-op and lands
        // here. Nothing left to keep alive.
        if !still_joined {
            log::debug!("[{room_id}/{slot_id}] heartbeat: no longer joined, stopping");
            break;
        }
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

#[uniffi::export(async_runtime = "tokio")]
impl RtcSessionManagerHandle {
    /// One handle per Matrix session, over the host's backend.
    #[uniffi::constructor]
    pub fn new(backend: Arc<dyn MatrixBackend>) -> Arc<Self> {
        log::info!("manager: created over the host backend");
        let backend = Arc::new(DialectBackend::new(Arc::new(FfiBackend::new(backend))));
        Arc::new(Self {
            inner: Arc::new(TokioMutex::new(CallSessionManager::new(
                RtcSessionManager::with_backend(backend.clone()),
            ))),
            backend,
            heartbeats: Mutex::new(HashMap::new()),
            modes: RoomModes::default(),
            rooms: Mutex::new(HashMap::new()),
            session_feeder: TokioMutex::new(None),
        })
    }

    /// Attaches a room: the library subscribes to what the room needs in the
    /// given mode and applies the current state. Resolves once that state is
    /// applied, so a `join` issued afterwards sees it. Attaching an attached
    /// room is an error.
    pub async fn attach_room(
        &self,
        room_id: String,
        options: FfiAttachOptions,
    ) -> Result<(), MatrixRtcFfiError> {
        if lock_mutex(&self.rooms)?.contains_key(&room_id) {
            return Err(MatrixRtcFfiError::Attachment(format!(
                "{room_id} is already attached"
            )));
        }
        let compat = compat::resolve(options.element_call_compat);
        log::info!("manager: [{room_id}] attaching in {compat:?} mode");

        self.ensure_session_feeder().await?;

        let (attachment, run) = RoomFeeder::attach(
            self.backend.clone(),
            self.inner.clone(),
            self.modes.clone(),
            room_id.clone(),
            AttachOptions {
                element_call_compat: compat,
            },
        )
        .await?;
        let feed = AbortOnDrop(runtime::runtime().spawn(run.run()));
        attachment.seeded().await;
        log::info!("manager: [{room_id}] attached and seeded");

        lock_mutex(&self.rooms)?.insert(
            room_id,
            AttachedRoom {
                attachment,
                _feed: feed,
            },
        );
        Ok(())
    }

    /// Detaches a room: leaves any session joined in it, then ends the
    /// subscription. Nothing delivered afterwards is applied. Detaching an
    /// unattached room is a no-op.
    pub async fn detach_room(&self, room_id: String) -> Result<(), MatrixRtcFfiError> {
        let joined: Vec<(String, String)> = lock_mutex(&self.heartbeats)?
            .keys()
            .filter(|(room, _)| *room == room_id)
            .cloned()
            .collect();
        for (room, slot) in joined {
            log::info!("manager: [{room}/{slot}] leaving before detaching");
            if let Err(error) = self
                .leave(room, slot, FfiLeaveSessionParams { leave_reason: None })
                .await
            {
                log::warn!("manager: leave before detach failed: {error}");
            }
        }

        let attached = lock_mutex(&self.rooms)?.remove(&room_id);
        match attached {
            Some(room) => {
                room.attachment.detach();
                self.backend.clear_dialect(&room_id);
                log::info!("manager: [{room_id}] detached");
            }
            None => log::debug!("manager: [{room_id}] detach of a room that is not attached"),
        }
        Ok(())
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
        room_id: String,
        slot_id: String,
        application_type: String,
        encryption: Option<FfiSlotEncryption>,
    ) -> Result<(), MatrixRtcFfiError> {
        log::info!(
            "manager: [{room_id}/{slot_id}] opening slot: application={application_type} \
             encryption={encryption:?}",
        );

        let manager = self.inner.lock().await;
        manager
            .open_slot(
                room_id,
                slot_id,
                application_type,
                encryption.map(Into::into),
            )
            .await
            .map_err(|error| {
                log::warn!("manager: could not open the slot: {error}");
                MatrixRtcFfiError::InvalidInput(error.to_string())
            })
    }

    /// Closes a slot, by setting its `m.rtc.slot` status to `closed`.
    ///
    /// Every member of it becomes left as soon as clients apply the new state —
    /// this ends the call for everyone, not just for us. Leaving is
    /// [`Self::leave`].
    pub async fn close_slot(
        &self,
        room_id: String,
        slot_id: String,
    ) -> Result<(), MatrixRtcFfiError> {
        log::info!("manager: [{room_id}/{slot_id}] closing slot");

        let manager = self.inner.lock().await;
        manager.close_slot(room_id, slot_id).await.map_err(|error| {
            log::warn!("manager: could not close the slot: {error}");
            MatrixRtcFfiError::InvalidInput(error.to_string())
        })
    }

    /// A JSON dump of everything the manager and its sessions currently
    /// believe: sessions, room state per room, and every candidate member with
    /// the reason it is or is not projected as joined.
    ///
    /// For bug reports and for answering "what does Rust think the state is
    /// right now?" without a debugger. Contains no key material.
    pub async fn debug_snapshot(&self) -> Result<String, MatrixRtcFfiError> {
        let manager = self.inner.lock().await;
        Ok(manager.debug_snapshot().to_string())
    }

    pub async fn session_count(&self) -> Result<u64, MatrixRtcFfiError> {
        let manager = self.inner.lock().await;
        Ok(manager.session_count() as u64)
    }

    pub async fn member_count(
        &self,
        room_id: String,
        slot_id: String,
    ) -> Result<Option<u64>, MatrixRtcFfiError> {
        let manager = self.inner.lock().await;
        Ok(manager
            .member_count(&room_id, &slot_id)
            .map(|count| count as u64))
    }

    /// Observe the joined roster of one session.
    ///
    /// Returns `None` if no session exists for `(room_id, slot_id)` — a session
    /// appears when the first member event for that slot arrives, or when this
    /// manager joins it.
    ///
    /// The subscription yields the current roster on its first
    /// `nextSnapshot()` and then only on change, so a host can attach at any
    /// point without missing the state it attached to.
    pub async fn subscribe_membership_snapshots(
        &self,
        room_id: String,
        slot_id: String,
    ) -> Result<Option<Arc<MembershipSnapshotSubscription>>, MatrixRtcFfiError> {
        let manager = self.inner.lock().await;
        Ok(manager
            .subscribe_membership_snapshots(&room_id, &slot_id)
            .map(|receiver| {
                Arc::new(MembershipSnapshotSubscription {
                    state: Mutex::new(SubscriptionState {
                        receiver,
                        initial_pending: true,
                    }),
                })
            }))
    }

    /// Joins a session in an attached room, returning the `member.id` it
    /// joined as.
    ///
    /// The SDK generates that id; hosts do not supply one. MSC4143 requires a
    /// fresh `member.id` on every join, and reusing one is silently destructive:
    /// the MSC4195 participant identity is derived from it, so a repeat join
    /// keeps the identity peers already hold a key for while our key index
    /// restarts at 0 — every peer then decrypts our media with the previous
    /// call's key and never recovers. Read it back with [`Self::own_member_id`].
    ///
    /// Fails when the room is not attached, or when its state holds no open
    /// slot of this id.
    pub async fn join(&self, params: FfiJoinSessionParams) -> Result<String, MatrixRtcFfiError> {
        log::info!("manager: join requested {}", params.summary());

        // Kept for the keep-alive driver, which outlives `params`.
        let room_id = params.room_id.clone();
        let slot_id = params.slot_id.clone();
        if !self.modes.is_attached(&room_id) {
            return Err(MatrixRtcFfiError::Attachment(format!(
                "{room_id} is not attached; attach the room before joining"
            )));
        }
        let compat = self.modes.mode(&room_id);
        let user_id = self.backend.own_user_id();
        let device_id = self.backend.own_device_id();

        // The join's own choice, else the first LiveKit transport the
        // homeserver advertises.
        let chosen = params.transport_intent().map_err(|e| {
            log::warn!("manager: join rejected before it started: {e}");
            MatrixRtcFfiError::InvalidInput(e.to_string())
        })?;
        let transport = match chosen {
            Some(chosen) => chosen,
            None => {
                let advertised = self.backend.rtc_transports().await?;
                transports::choose(&advertised, None).map_err(|e| {
                    log::warn!("manager: join rejected: {e}");
                    MatrixRtcFfiError::InvalidInput(e.to_string())
                })?
            }
        };

        let mut core_params = params
            .into_core(user_id.clone(), device_id.clone(), transport)
            .map_err(|e| {
                log::warn!("manager: join rejected before it started: {e}");
                MatrixRtcFfiError::InvalidInput(e.to_string())
            })?;
        // Not always a fresh id: see `compat::member_id` for the one generation
        // where a fresh one makes us mark ourselves departed on our own join.
        let member_id = compat::member_id(compat, &user_id, &device_id);
        core_params.rtc.membership_id = Some(member_id.clone());

        // Before the join, not after: the join itself sends the membership (and
        // arms the delayed leave), so a dialect registered afterwards would let
        // exactly the two events that announce us go out spec-current.
        self.backend.set_dialect(
            &room_id,
            compat::outbound_dialect(compat, &user_id, &device_id, &room_id, &slot_id),
        );

        // Hold the guard across the join. Serialising is safe because no host
        // callback re-enters a handle: the backend's sinks only enqueue.
        let mut manager = self.inner.lock().await;
        let result = {
            manager.join(core_params).await.map_err(|e| {
                log::warn!("manager: join failed: {e}");
                MatrixRtcFfiError::InvalidInput(e.to_string())
            })
        };

        if result.is_ok() {
            log::info!("manager: join succeeded as {member_id}");
            drop(manager);
            self.start_heartbeat(room_id, slot_id);
        }

        result.map(|_| member_id)
    }

    /// Our `member.id` in one session, or `None` if there is no such session or
    /// it has not joined.
    ///
    /// Changes on every join (MSC4143), so read it when needed rather than
    /// caching what [`Self::join`] returned.
    pub async fn own_member_id(
        &self,
        room_id: String,
        slot_id: String,
    ) -> Result<Option<String>, MatrixRtcFfiError> {
        let manager = self.inner.lock().await;
        Ok(manager.own_member_id(&room_id, &slot_id))
    }

    /// The event id of our current membership event in one session, or `None`
    /// if there is no such session or it has not joined. Moves on every sticky
    /// refresh, so read it at the moment of use.
    pub async fn own_membership_event_id(
        &self,
        room_id: String,
        slot_id: String,
    ) -> Result<Option<String>, MatrixRtcFfiError> {
        let manager = self.inner.lock().await;
        Ok(manager.own_membership_event_id(&room_id, &slot_id))
    }

    // ---- Reactions and raised hands ----
    //
    // Element Call's reactions are ordinary room events relating to the
    // reacting member's membership event. The library reads them from the
    // attached room (timeline events, redactions and the relations of each
    // membership event); the host plays any sound. Results surface on the media
    // session as `FfiCallEvent::HandRaised` / `HandLowered` / `Reaction` and on
    // `FfiParticipant.hand_raised_at_ms`, and here as `raised_hands`.

    /// Sends an Element Call emoji reaction in one session. `name` is what
    /// peers pick a sound by (see [`reaction_catalog`]); only the first
    /// grapheme of `emoji` is sent. Returns the event id.
    ///
    /// Fails inside the send cooldown (Element Call's three seconds by
    /// default), since peers would drop the reaction anyway.
    pub async fn send_reaction(
        &self,
        room_id: String,
        slot_id: String,
        emoji: String,
        name: String,
    ) -> Result<String, MatrixRtcFfiError> {
        let mut manager = self.inner.lock().await;
        Ok(manager
            .send_reaction(&room_id, &slot_id, &emoji, &name)
            .await?)
    }

    /// Raises our hand in one session. Idempotent while it is up; the hand
    /// follows our membership across sticky refreshes on its own.
    pub async fn raise_hand(
        &self,
        room_id: String,
        slot_id: String,
    ) -> Result<(), MatrixRtcFfiError> {
        let mut manager = self.inner.lock().await;
        Ok(manager.raise_hand(&room_id, &slot_id).await?)
    }

    /// Lowers our hand in one session by redacting the annotation. A no-op
    /// when it is down.
    pub async fn lower_hand(
        &self,
        room_id: String,
        slot_id: String,
    ) -> Result<(), MatrixRtcFfiError> {
        let mut manager = self.inner.lock().await;
        Ok(manager.lower_hand(&room_id, &slot_id).await?)
    }

    /// The raised hands of one session, oldest first; empty if there is no
    /// such session.
    pub async fn raised_hands(
        &self,
        room_id: String,
        slot_id: String,
    ) -> Result<Vec<FfiRaisedHand>, MatrixRtcFfiError> {
        let manager = self.inner.lock().await;
        Ok(manager
            .raised_hands(&room_id, &slot_id)
            .unwrap_or_default()
            .into_iter()
            .map(Into::into)
            .collect())
    }

    /// Restarts the keep-alive for one session: reschedules the delayed leave,
    /// and re-sends the membership if its sticky entry is halfway to expiring.
    ///
    /// **Hosts do not need to call this** — [`Self::join`] starts a driver that
    /// does it every 10 seconds, and [`Self::leave`] stops it. It is exported
    /// for hosts that would rather drive the keep-alive from their own scheduler
    /// (a foreground service, a workmanager job), and for tests.
    ///
    /// Returns `false` if there is no joined session for `(room_id, slot_id)`,
    /// which means there is nothing to keep alive.
    pub async fn heartbeat(
        &self,
        room_id: String,
        slot_id: String,
    ) -> Result<bool, MatrixRtcFfiError> {
        let mut manager = self.inner.lock().await;
        Ok(manager.heartbeat(&room_id, &slot_id).await)
    }

    pub async fn leave(
        &self,
        room_id: String,
        slot_id: String,
        params: FfiLeaveSessionParams,
    ) -> Result<(), MatrixRtcFfiError> {
        log::info!(
            "manager: leave requested [{room_id}/{slot_id}] reason={:?}",
            params.leave_reason,
        );

        let core_params = params.into_core();

        // Stop the keep-alive first, so it cannot re-arm a delayed leave after
        // the leave below cancels it. A beat already in flight is harmless: it
        // holds the manager lock we are about to take, and once `leave` has
        // taken the membership machine any later beat is a no-op.
        self.stop_heartbeat(&room_id, &slot_id);

        // Held across the leave, for the reasons in `join` above.
        let mut manager = self.inner.lock().await;
        let result = {
            manager
                .leave(room_id, slot_id, core_params)
                .await
                .map_err(|e| {
                    log::warn!("manager: leave failed: {e}");
                    MatrixRtcFfiError::InvalidInput(e.to_string())
                })
        };

        if result.is_ok() {
            log::info!("manager: leave succeeded");
        }
        // The dialect stays registered: the room is still attached in its
        // mode, and a rejoin in it renders the same way. Detaching clears it.

        result
    }
}

impl RtcSessionManagerHandle {
    /// The session-wide to-device subscription, started once.
    async fn ensure_session_feeder(&self) -> Result<(), MatrixRtcFfiError> {
        let mut slot = self.session_feeder.lock().await;
        if slot.is_some() {
            return Ok(());
        }
        let (feeder, run) =
            SessionFeeder::start(self.backend.clone(), self.inner.clone(), self.modes.clone())
                .await?;
        *slot = Some((feeder, AbortOnDrop(runtime::runtime().spawn(run.run()))));
        log::info!("manager: to-device subscription started");
        Ok(())
    }

    /// Starts (or replaces) the keep-alive driver for one session.
    fn start_heartbeat(&self, room_id: String, slot_id: String) {
        self.start_heartbeat_every(room_id, slot_id, HEARTBEAT_INTERVAL);
    }

    /// [`Self::start_heartbeat`] with the interval spelled out, so a test can
    /// beat faster than a session ships with.
    fn start_heartbeat_every(&self, room_id: String, slot_id: String, interval: Duration) {
        let (stop, stop_rx) = tokio::sync::mpsc::channel(1);
        let manager = Arc::downgrade(&self.inner);
        let key = (room_id.clone(), slot_id.clone());

        // On `runtime()` rather than a thread of its own: the body is a sleep
        // and an await on a mutex, and `tokio::time::sleep` needs a timer to
        // fire at all. Detached — it stops when the `stop` sender below is
        // dropped, or when the manager behind its `Weak` goes away.
        runtime::runtime().spawn(run_heartbeat(manager, room_id, slot_id, interval, stop_rx));

        log::info!(
            "manager: keep-alive driver started for [{}/{}] every {interval:?}",
            key.0,
            key.1,
        );
        // Replaces any previous driver for this session; dropping the old
        // sender stops its task.
        match lock_mutex(&self.heartbeats) {
            Ok(mut drivers) => {
                drivers.insert(key, HeartbeatDriver { _stop: stop });
            }
            Err(error) => log::error!("manager: could not register keep-alive: {error}"),
        }
    }

    /// Stops the keep-alive driver for one session, if any.
    fn stop_heartbeat(&self, room_id: &str, slot_id: &str) {
        let key = (room_id.to_owned(), slot_id.to_owned());
        match lock_mutex(&self.heartbeats) {
            Ok(mut drivers) => {
                if drivers.remove(&key).is_some() {
                    log::debug!("manager: keep-alive driver stopped for [{room_id}/{slot_id}]");
                }
            }
            Err(error) => log::error!("manager: could not stop the keep-alive: {error}"),
        }
    }

    /// The generation `room_id` was attached for, or `Off`.
    ///
    /// `pub(crate)` for the media layer, which derives its SFU identity and picks
    /// its token endpoint from this rather than from a second host-supplied
    /// value — the two disagreeing is not an error but a silence.
    #[cfg(feature = "media")]
    pub(crate) fn element_call_compat_for(
        &self,
        room_id: &str,
    ) -> matrix_rtc_bridge::compat::ElementCallCompat {
        self.modes.mode(room_id)
    }

    /// The host's backend, for the media layer's token exchange.
    #[cfg(feature = "media")]
    pub(crate) fn backend(&self) -> Arc<dyn CoreBackend> {
        self.backend.clone()
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
    use matrix_rtc_bridge::compat::STATE_MEMBER_EVENT_TYPE;

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

    /// Seeds the room's gating subjects and `sticky`, then attaches.
    async fn attach(
        manager: &RtcSessionManagerHandle,
        mock: &MockHost,
        compat: Option<FfiElementCallCompat>,
        encrypted: bool,
        slots: Vec<FfiEventIn>,
        sticky: Vec<FfiEventIn>,
    ) {
        let attach = manager.attach_room(
            ROOM.to_owned(),
            FfiAttachOptions {
                element_call_compat: compat,
            },
        );
        tokio::pin!(attach);
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
        let (result, ()) = tokio::join!(attach, seed);
        result.expect("attach");
    }

    fn join_params() -> FfiJoinSessionParams {
        FfiJoinSessionParams {
            room_id: ROOM.to_owned(),
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
    async fn heartbeat_reports_no_session_when_not_joined() {
        let manager = RtcSessionManagerHandle::new(MockHost::new());
        assert!(
            !manager
                .heartbeat(ROOM.to_owned(), SLOT.to_owned())
                .await
                .expect("the call itself should succeed"),
        );
    }

    #[test]
    fn stopping_an_unknown_heartbeat_is_harmless() {
        let manager = RtcSessionManagerHandle::new(MockHost::new());
        manager.stop_heartbeat(ROOM, SLOT);
        assert!(lock_mutex(&manager.heartbeats).unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_room_must_be_attached_before_joining() {
        let manager = RtcSessionManagerHandle::new(MockHost::new());
        let result = manager.join(join_params()).await;
        assert!(matches!(result, Err(MatrixRtcFfiError::Attachment(_))));
    }

    #[tokio::test]
    async fn attaching_subscribes_in_the_mode_and_seeds_the_roster() {
        let mock = MockHost::new();
        let manager = RtcSessionManagerHandle::new(mock.clone());
        attach(
            &manager,
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

        let subscription = manager
            .subscribe_membership_snapshots(ROOM.to_owned(), SLOT.to_owned())
            .await
            .unwrap()
            .expect("the member event should have created the session");
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
    async fn attaching_twice_is_an_error_and_detaching_ends_the_subscription() {
        let mock = MockHost::new();
        let manager = RtcSessionManagerHandle::new(mock.clone());
        attach(
            &manager,
            &mock,
            None,
            false,
            vec![open_slot(None)],
            Vec::new(),
        )
        .await;

        let again = manager
            .attach_room(ROOM.to_owned(), FfiAttachOptions::default())
            .await;
        assert!(matches!(again, Err(MatrixRtcFfiError::Attachment(_))));

        manager.detach_room(ROOM.to_owned()).await.unwrap();
        // Delivered after detach: not applied.
        mock.room_sink(ROOM).on_sticky_events(vec![member_event(
            "@bob:example.org",
            "BOBDEV",
            "bob-a",
            "$m1",
        )]);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            manager
                .member_count(ROOM.to_owned(), SLOT.to_owned())
                .await
                .unwrap(),
            None
        );
    }

    /// One room, three generations, one roster: a spec-current peer, a 2025
    /// Element Call peer, and a pre-sticky one carried in room state.
    #[tokio::test]
    async fn membership_from_every_generation_lands_in_one_roster() {
        let mock = MockHost::new();
        let manager = RtcSessionManagerHandle::new(mock.clone());

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
            origin_server_ts: matrix_rtc_bridge::compat::element_call_state::now_ms(),
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

        attach(
            &manager,
            &mock,
            Some(FfiElementCallCompat::StateEvents),
            false,
            Vec::new(),
            vec![spec, legacy_sticky],
        )
        .await;
        mock.room_sink(ROOM)
            .on_state_events(STATE_MEMBER_EVENT_TYPE.to_owned(), vec![pre_sticky]);
        wait_until(async || {
            manager
                .member_count(ROOM.to_owned(), SLOT.to_owned())
                .await
                .unwrap()
                == Some(3)
        })
        .await;

        let subscription = manager
            .subscribe_membership_snapshots(ROOM.to_owned(), SLOT.to_owned())
            .await
            .unwrap()
            .expect("the member events should have created the session");
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
        let manager = RtcSessionManagerHandle::new(mock.clone());
        attach(
            &manager,
            &mock,
            None,
            false,
            vec![open_slot(None)],
            Vec::new(),
        )
        .await;

        let first = manager
            .join(FfiJoinSessionParams {
                transport: None,
                ..join_params()
            })
            .await
            .expect("first join");
        assert_eq!(
            manager
                .own_member_id(ROOM.to_owned(), SLOT.to_owned())
                .await
                .unwrap(),
            Some(first.clone()),
        );
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

        manager
            .leave(
                ROOM.to_owned(),
                SLOT.to_owned(),
                FfiLeaveSessionParams { leave_reason: None },
            )
            .await
            .expect("leave");
        let second = manager.join(join_params()).await.expect("rejoin");
        assert_ne!(first, second, "a rejoin must not reuse the member id");
    }

    #[tokio::test]
    async fn a_join_into_a_room_with_no_open_slot_is_refused() {
        let mock = MockHost::new();
        let manager = RtcSessionManagerHandle::new(mock.clone());
        attach(&manager, &mock, None, false, Vec::new(), Vec::new()).await;

        let result = manager.join(join_params()).await;
        assert!(result.is_err(), "no open slot, no join");
        assert!(mock.sends().is_empty(), "nothing should have been sent");
    }

    #[tokio::test]
    async fn the_keep_alive_driver_restarts_the_delayed_leave() {
        let mock = MockHost::new();
        let manager = RtcSessionManagerHandle::new(mock.clone());
        attach(
            &manager,
            &mock,
            None,
            false,
            vec![open_slot(None)],
            Vec::new(),
        )
        .await;

        manager.join(join_params()).await.expect("join");
        manager.start_heartbeat_every(ROOM.to_owned(), SLOT.to_owned(), Duration::from_millis(50));
        tokio::time::sleep(Duration::from_millis(300)).await;

        let beats = mock
            .sent_types()
            .into_iter()
            .filter(|sent| sent == "restart_delayed_event")
            .count();
        assert!(
            beats >= 2,
            "saw {beats} beats over 300ms at a 50ms interval"
        );
    }

    #[tokio::test]
    async fn leaving_stops_the_keep_alive_driver() {
        let mock = MockHost::new();
        let manager = RtcSessionManagerHandle::new(mock.clone());
        attach(
            &manager,
            &mock,
            None,
            false,
            vec![open_slot(None)],
            Vec::new(),
        )
        .await;

        manager.join(join_params()).await.expect("join");
        manager.start_heartbeat_every(ROOM.to_owned(), SLOT.to_owned(), Duration::from_millis(50));
        tokio::time::sleep(Duration::from_millis(150)).await;

        manager
            .leave(
                ROOM.to_owned(),
                SLOT.to_owned(),
                FfiLeaveSessionParams { leave_reason: None },
            )
            .await
            .expect("leave");
        let beats = |mock: &MockHost| {
            mock.sent_types()
                .into_iter()
                .filter(|sent| sent == "restart_delayed_event")
                .count()
        };
        let after_leave = beats(&mock);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            beats(&mock),
            after_leave,
            "no beat once the session has left"
        );
    }

    #[tokio::test]
    async fn detaching_leaves_the_joined_session() {
        let mock = MockHost::new();
        let manager = RtcSessionManagerHandle::new(mock.clone());
        attach(
            &manager,
            &mock,
            None,
            false,
            vec![open_slot(None)],
            Vec::new(),
        )
        .await;
        manager.join(join_params()).await.expect("join");

        manager.detach_room(ROOM.to_owned()).await.unwrap();
        assert!(
            mock.sent_types()
                .iter()
                .any(|sent| sent == "cancel_delayed_event"),
            "the leave cancels the delayed leave: {:?}",
            mock.sent_types()
        );
        assert!(lock_mutex(&manager.heartbeats).unwrap().is_empty());
    }

    /// Driven through the handle an FFI host holds: the rejoin must distribute
    /// a key to the incumbent even though no sticky event moved.
    #[tokio::test]
    async fn a_rejoin_distributes_keys_without_new_sticky_events() {
        let mock = MockHost::new();
        let manager = RtcSessionManagerHandle::new(mock.clone());
        attach(
            &manager,
            &mock,
            None,
            true,
            vec![open_slot(Some("m.per_member"))],
            Vec::new(),
        )
        .await;

        manager.join(join_params()).await.expect("first join");
        mock.room_sink(ROOM).on_sticky_events(vec![member_event(
            "@bob:example.org",
            "BOBDEV",
            "bob-a",
            "$b",
        )]);
        wait_until(async || !mock.to_device_for("@bob:example.org", "BOBDEV").is_empty()).await;

        manager
            .leave(
                ROOM.to_owned(),
                SLOT.to_owned(),
                FfiLeaveSessionParams { leave_reason: None },
            )
            .await
            .expect("leave");
        mock.clear_to_device();

        let second = manager.join(join_params()).await.expect("rejoin");
        let sent = mock.to_device_for("@bob:example.org", "BOBDEV");
        assert!(!sent.is_empty(), "the second call distributed no key");
        assert_eq!(
            sent[0].pointer("/member_id").and_then(|v| v.as_str()),
            Some(second.as_str()),
        );
    }

    #[tokio::test]
    async fn opening_and_closing_a_slot_publishes_the_state_a_peer_reads() {
        let mock = MockHost::new();
        let manager = RtcSessionManagerHandle::new(mock.clone());

        manager
            .open_slot(
                ROOM.to_owned(),
                SLOT.to_owned(),
                "m.call".to_owned(),
                Some(FfiSlotEncryption::PerMember),
            )
            .await
            .expect("open");
        manager
            .close_slot(ROOM.to_owned(), SLOT.to_owned())
            .await
            .expect("close");

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
        let manager = RtcSessionManagerHandle::new(mock.clone());
        let result = manager
            .open_slot(
                ROOM.to_owned(),
                SLOT.to_owned(),
                "m.something.else".to_owned(),
                None,
            )
            .await;
        assert!(result.is_err());
        assert!(mock.sends().is_empty());
    }

    // --- Element Call compatibility ------------------------------------------
    //
    // The dialects are tested in `matrix_rtc_bridge::compat`. Tested here: the
    // mode chosen at attach reaches every send, the two of the join included.

    #[tokio::test]
    async fn a_sticky_compat_join_is_readable_by_both_generations() {
        let mock = MockHost::new();
        let manager = RtcSessionManagerHandle::new(mock.clone());
        attach(
            &manager,
            &mock,
            Some(FfiElementCallCompat::StickyEvents),
            false,
            vec![open_slot(None)],
            Vec::new(),
        )
        .await;

        let member_id = manager.join(join_params()).await.expect("join");
        let membership = mock
            .sends()
            .into_iter()
            .find(|send| send.carrier == Carrier::Sticky)
            .expect("the membership should still be a sticky event");
        assert_eq!(membership.event_type, "org.matrix.msc4143.rtc.member");
        assert_eq!(
            membership.content.pointer("/member/id").unwrap(),
            &serde_json::json!(member_id),
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
        let manager = RtcSessionManagerHandle::new(mock.clone());
        // No slot state is asked for in this mode: that generation has none.
        attach(
            &manager,
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

        let member_id = manager
            .join(FfiJoinSessionParams {
                keep_alive_timeout_ms: Some(30_000),
                ..join_params()
            })
            .await
            .expect("join");
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
        let manager = RtcSessionManagerHandle::new(mock.clone());
        attach(
            &manager,
            &mock,
            Some(FfiElementCallCompat::StateEvents),
            false,
            Vec::new(),
            Vec::new(),
        )
        .await;

        manager
            .join(join_params())
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

    /// A mode is per attachment: a later spec-current attach of the same room
    /// must not inherit the dialect a previous one installed.
    #[tokio::test]
    async fn detaching_forgets_the_dialect() {
        let mock = MockHost::new();
        let manager = RtcSessionManagerHandle::new(mock.clone());
        attach(
            &manager,
            &mock,
            Some(FfiElementCallCompat::StateEvents),
            false,
            Vec::new(),
            Vec::new(),
        )
        .await;
        manager.join(join_params()).await.expect("join");
        manager.detach_room(ROOM.to_owned()).await.unwrap();
        assert!(
            mock.sends()
                .iter()
                .filter(|send| send.carrier == Carrier::State)
                .any(|send| send.content == serde_json::json!({})),
            "the leave emptied our state membership",
        );

        attach(
            &manager,
            &mock,
            None,
            false,
            vec![open_slot(None)],
            Vec::new(),
        )
        .await;
        manager.join(join_params()).await.expect("rejoin");
        assert!(
            mock.sends()
                .iter()
                .any(|send| send.carrier == Carrier::Sticky
                    && send.event_type == "org.matrix.msc4143.rtc.member"),
            "a spec-current rejoin goes back to a sticky membership",
        );
    }

    /// A panic inside one handle method must not disable the handle forever.
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
