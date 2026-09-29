// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The two uniffi objects: [`RtcSessionManager`], where a host pushes room
//! state in, and [`Participation`], the per-slot handle with the four
//! outputs and their listeners.
//!
//! Every method is async because the core manager sits behind a
//! `tokio::sync::Mutex`: blocking is impossible on wasm and unwelcome on a UI
//! thread. Listener callbacks run synchronously inside the input that changed
//! the value, while that lock is held — a listener must schedule any call
//! back into these objects rather than await it in place.

use std::sync::{Arc, Mutex as StdMutex};

use matrix_rtc_core::participation::{
    KeyMap, ParticipationListener, SessionMembership, Status, TransportWithMembers,
};
use matrix_rtc_core::{RtcSessionManager as CoreManager, generate_member_id};
use tokio::sync::Mutex;

use crate::RtcError;
use crate::commands::{ForeignCommandSender, RtcCommandSenderCallback};
use crate::types::{
    FfiJoinParams, FfiLeaveParams, FfiMediaKey, FfiMembership, FfiReceivedEncryptionKey,
    FfiSlotEncryption, FfiSlotEvent, FfiStatus, FfiStickyEvent, FfiTransportIntent,
    FfiTransportWithMembers, key_map_from,
};

type Manager = Arc<Mutex<CoreManager<ForeignCommandSender>>>;

/// Holds every session and takes the room state a host pushes in.
///
/// Inputs are room-scoped, as the host's SDK reports them; the whole sticky
/// map of a room goes in at once, never a partial one (a missing entry reads
/// as a leave).
#[derive(uniffi::Object)]
pub struct RtcSessionManager {
    inner: Manager,
}

// On wasm32 nothing is `Send`/`Sync` (the core's host traits drop the bounds
// there, see `matrix_rtc_core::MaybeSend`), and nothing needs to be: uniffi's
// `wasm-unstable-single-threaded` mode runs everything on the one JS thread.
// The `Arc`s are still what uniffi objects are, so the lint is off for them.
#[cfg_attr(target_arch = "wasm32", allow(clippy::arc_with_non_send_sync))]
#[uniffi::export]
impl RtcSessionManager {
    #[uniffi::constructor]
    pub fn new(command_sender: Arc<dyn RtcCommandSenderCallback>) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(Mutex::new(CoreManager::with_command_sender(Arc::new(
                ForeignCommandSender::new(command_sender),
            )))),
        })
    }

    /// Replaces a room's sticky state with `events`, the room's complete
    /// current sticky map. Non-member event types are ignored.
    pub async fn set_current_sticky_state(
        &self,
        room_id: String,
        events: Vec<FfiStickyEvent>,
    ) -> Result<(), RtcError> {
        let events = events
            .into_iter()
            .map(FfiStickyEvent::into_core)
            .collect::<Result<Vec<_>, _>>()?;
        self.inner
            .lock()
            .await
            .set_current_sticky_state(&room_id, events)
            .await
            .map_err(|e| RtcError::InvalidInput(e.to_string()))
    }

    /// Applies a room's complete `m.rtc.slot` state; a slot absent from it
    /// is closed. Until called, the open-slot condition is not enforced.
    pub async fn on_room_slots_received(
        &self,
        room_id: String,
        slots: Vec<FfiSlotEvent>,
    ) -> Result<(), RtcError> {
        let slots = slots
            .into_iter()
            .map(FfiSlotEvent::into_core)
            .collect::<Result<Vec<_>, _>>()?;
        self.inner
            .lock()
            .await
            .on_room_slots_received(&room_id, slots)
            .await;
        Ok(())
    }

    /// Stops enforcing the open-slot condition in a room.
    pub async fn forget_room_slots(&self, room_id: String) {
        self.inner.lock().await.forget_room_slots(&room_id).await;
    }

    /// Reports whether a room is end-to-end encrypted.
    pub async fn on_room_encryption_received(&self, room_id: String, encrypted: bool) {
        self.inner
            .lock()
            .await
            .on_room_encryption_received(&room_id, encrypted)
            .await;
    }

    /// Sets the users currently joined to a room.
    pub async fn on_room_members_received(&self, room_id: String, joined_user_ids: Vec<String>) {
        self.inner
            .lock()
            .await
            .on_room_members_received(&room_id, joined_user_ids)
            .await;
    }

    /// Feeds a media key received from a peer to every session of its room.
    pub async fn receive_encryption_key(
        &self,
        key: FfiReceivedEncryptionKey,
    ) -> Result<(), RtcError> {
        let key = key.into_core()?;
        self.inner
            .lock()
            .await
            .receive_encryption_key(key)
            .await
            .map_err(RtcError::from)
    }

    /// Opens a slot by sending its `m.rtc.slot` state event.
    pub async fn open_slot(
        &self,
        room_id: String,
        slot_id: String,
        application_type: String,
        encryption: Option<FfiSlotEncryption>,
    ) -> Result<(), RtcError> {
        let encryption = encryption.map(FfiSlotEncryption::into_core).transpose()?;
        self.inner
            .lock()
            .await
            .open_slot(room_id, slot_id, application_type, encryption)
            .await
            .map_err(RtcError::from)
    }

    /// Closes a slot; its members become left once the state applies.
    pub async fn close_slot(&self, room_id: String, slot_id: String) -> Result<(), RtcError> {
        self.inner
            .lock()
            .await
            .close_slot(room_id, slot_id)
            .await
            .map_err(RtcError::from)
    }

    /// The handle for one `(room, slot)` as `user_id`/`device_id`. Any number
    /// of handles may share a manager; the session behind them is created on
    /// first use.
    pub fn participation(
        &self,
        room_id: String,
        slot_id: String,
        user_id: String,
        device_id: String,
    ) -> Arc<Participation> {
        Arc::new(Participation {
            manager: self.inner.clone(),
            room_id,
            slot_id,
            user_id,
            device_id,
            listeners: Arc::new(ListenerHub::default()),
            listeners_installed: StdMutex::new(false),
        })
    }

    /// Everything the manager believes, as JSON, for bug reports. Contains
    /// no key material.
    pub async fn debug_snapshot(&self) -> String {
        self.inner.lock().await.debug_snapshot().to_string()
    }
}

/// Our participation in one slot: join/leave, and the four outputs.
#[derive(uniffi::Object)]
pub struct Participation {
    manager: Manager,
    room_id: String,
    slot_id: String,
    user_id: String,
    device_id: String,
    listeners: Arc<ListenerHub>,
    /// Whether `listeners` is installed on the session yet. Installed lazily
    /// on the first `set_*_listener`, because installing needs the manager
    /// lock and the constructor is synchronous.
    listeners_installed: StdMutex<bool>,
}

#[uniffi::export]
impl Participation {
    /// Joins: arms the delayed leave, publishes our membership, starts key
    /// distribution. Returns our `member.id`. The host then calls
    /// [`Self::heartbeat`] every [`crate::heartbeat_interval_ms`].
    pub async fn join(
        &self,
        intent: FfiTransportIntent,
        params: FfiJoinParams,
    ) -> Result<String, RtcError> {
        let member_id = params.member_id.clone().unwrap_or_else(generate_member_id);
        let params = params.into_core(
            intent,
            &self.room_id,
            &self.slot_id,
            &self.user_id,
            &self.device_id,
            member_id.clone(),
        )?;
        let mut manager = self.manager.lock().await;
        manager.join(params).await?;
        // The MSC4195 pseudonymous identity, so `transport_identity` on the
        // memberships is what the LiveKit SFU knows each member as.
        manager.set_encryption_identity_mapper(
            &self.room_id,
            &self.slot_id,
            matrix_rtc_livekit_proto::identity_mapper(matrix_rtc_bridge_compat_off()),
        );
        Ok(member_id)
    }

    /// Leaves: publishes the leave and cancels the delayed leave.
    pub async fn leave(&self, params: FfiLeaveParams) -> Result<(), RtcError> {
        self.manager
            .lock()
            .await
            .leave(self.room_id.clone(), self.slot_id.clone(), params.into())
            .await
            .map_err(RtcError::from)
    }

    /// Restarts the delayed leave and refreshes the sticky membership when
    /// due. Returns `false` when not joined.
    pub async fn heartbeat(&self) -> bool {
        self.manager
            .lock()
            .await
            .heartbeat(&self.room_id, &self.slot_id)
            .await
    }

    /// Performs a key rotation that was deferred into a switch window, once
    /// it is due. Returns `false` when not joined.
    pub async fn flush_due_key_rotation(&self) -> bool {
        self.manager
            .lock()
            .await
            .flush_due_key_rotation(&self.room_id, &self.slot_id)
            .await
    }

    /// When a deferred key rotation falls due, if one is owed.
    pub async fn key_rotation_due_at_ms(&self) -> Option<u64> {
        self.manager
            .lock()
            .await
            .key_rotation_due_at_ms(&self.room_id, &self.slot_id)
    }

    /// Our `member.id` for the current join.
    pub async fn own_member_id(&self) -> Option<String> {
        self.manager
            .lock()
            .await
            .own_member_id(&self.room_id, &self.slot_id)
    }

    /// Every member as a tile, sorted by member id.
    pub async fn memberships(&self) -> Vec<FfiMembership> {
        self.manager
            .lock()
            .await
            .memberships(&self.room_id, &self.slot_id)
            .unwrap_or_default()
            .iter()
            .map(FfiMembership::from)
            .collect()
    }

    /// One entry per distinct published transport, with its members.
    pub async fn transports(&self) -> Vec<FfiTransportWithMembers> {
        self.manager
            .lock()
            .await
            .transports(&self.room_id, &self.slot_id)
            .unwrap_or_default()
            .iter()
            .map(FfiTransportWithMembers::from)
            .collect()
    }

    /// Every media key held, ours and each peer's.
    pub async fn key_map(&self) -> Vec<FfiMediaKey> {
        key_map_from(
            &self
                .manager
                .lock()
                .await
                .key_map(&self.room_id, &self.slot_id)
                .unwrap_or_default(),
        )
    }

    /// Where our participation stands.
    pub async fn status(&self) -> FfiStatus {
        FfiStatus::from(
            &self
                .manager
                .lock()
                .await
                .participation_status(&self.room_id, &self.slot_id),
        )
    }

    /// Installs the memberships listener, replaying the current value once.
    pub async fn set_memberships_listener(&self, listener: Arc<dyn MembershipsListener>) {
        *self.listeners.memberships.lock().unwrap() = Some(listener.clone());
        let mut manager = self.manager.lock().await;
        if !self.install_hub(&mut manager) {
            listener.on_memberships_change(
                manager
                    .memberships(&self.room_id, &self.slot_id)
                    .unwrap_or_default()
                    .iter()
                    .map(FfiMembership::from)
                    .collect(),
            );
        }
    }

    /// Installs the transports listener, replaying the current value once.
    pub async fn set_transports_listener(&self, listener: Arc<dyn TransportsListener>) {
        *self.listeners.transports.lock().unwrap() = Some(listener.clone());
        let mut manager = self.manager.lock().await;
        if !self.install_hub(&mut manager) {
            listener.on_transports_change(
                manager
                    .transports(&self.room_id, &self.slot_id)
                    .unwrap_or_default()
                    .iter()
                    .map(FfiTransportWithMembers::from)
                    .collect(),
            );
        }
    }

    /// Installs the key map listener, replaying the current value once.
    pub async fn set_key_map_listener(&self, listener: Arc<dyn KeyMapListener>) {
        *self.listeners.key_map.lock().unwrap() = Some(listener.clone());
        let mut manager = self.manager.lock().await;
        if !self.install_hub(&mut manager) {
            listener.on_key_map_change(key_map_from(
                &manager
                    .key_map(&self.room_id, &self.slot_id)
                    .unwrap_or_default(),
            ));
        }
    }

    /// Installs the status listener, replaying the current value once.
    pub async fn set_status_listener(&self, listener: Arc<dyn StatusListener>) {
        *self.listeners.status.lock().unwrap() = Some(listener.clone());
        let mut manager = self.manager.lock().await;
        if !self.install_hub(&mut manager) {
            listener.on_status_change(FfiStatus::from(
                &manager.participation_status(&self.room_id, &self.slot_id),
            ));
        }
    }

    /// Removes every listener of this handle.
    pub async fn clear_listeners(&self) {
        self.listeners.clear();
        let mut manager = self.manager.lock().await;
        manager.clear_participation_listener(&self.room_id, &self.slot_id);
        *self.listeners_installed.lock().unwrap() = false;
    }

    /// This session's state as JSON, for bug reports. Contains no key
    /// material.
    pub async fn debug_snapshot(&self) -> String {
        let manager = self.manager.lock().await;
        let snapshot = manager.debug_snapshot();
        let key = format!("{}/{}", self.room_id, self.slot_id);
        snapshot["sessions"][key].to_string()
    }
}

impl Participation {
    /// Installs the hub on the session if it is not yet, which replays every
    /// output to whichever listeners are set. Returns whether it did (and so
    /// already replayed), so the caller can replay just its own value otherwise.
    fn install_hub(&self, manager: &mut CoreManager<ForeignCommandSender>) -> bool {
        let mut installed = self.listeners_installed.lock().unwrap();
        if *installed {
            return false;
        }
        manager.set_participation_listener(&self.room_id, &self.slot_id, self.listeners.clone());
        *installed = true;
        true
    }
}

/// Element Call compatibility is off on this surface: it speaks spec-current
/// MSC4143 only.
fn matrix_rtc_bridge_compat_off() -> matrix_rtc_bridge::compat::ElementCallCompat {
    matrix_rtc_bridge::compat::ElementCallCompat::Off
}

// ---- listeners ---------------------------------------------------------

/// Called with every member as a tile whenever the list changes.
#[uniffi::export(with_foreign)]
pub trait MembershipsListener: Send + Sync {
    fn on_memberships_change(&self, memberships: Vec<FfiMembership>);
}

/// Called whenever the set of transports or the members on them change.
#[uniffi::export(with_foreign)]
pub trait TransportsListener: Send + Sync {
    fn on_transports_change(&self, transports: Vec<FfiTransportWithMembers>);
}

/// Called whenever a media key is added, replaced or forgotten.
#[uniffi::export(with_foreign)]
pub trait KeyMapListener: Send + Sync {
    fn on_key_map_change(&self, key_map: Vec<FfiMediaKey>);
}

/// Called whenever our participation status changes. `Connected` also
/// changes on every heartbeat (its timestamps move); a host that only wants
/// problem transitions should diff the impairments.
#[uniffi::export(with_foreign)]
pub trait StatusListener: Send + Sync {
    fn on_status_change(&self, status: FfiStatus);
}

/// The one core listener a handle installs, fanning out to whichever of the
/// four foreign listeners are set.
#[derive(Default)]
struct ListenerHub {
    memberships: StdMutex<Option<Arc<dyn MembershipsListener>>>,
    transports: StdMutex<Option<Arc<dyn TransportsListener>>>,
    key_map: StdMutex<Option<Arc<dyn KeyMapListener>>>,
    status: StdMutex<Option<Arc<dyn StatusListener>>>,
}

impl ListenerHub {
    fn clear(&self) {
        *self.memberships.lock().unwrap() = None;
        *self.transports.lock().unwrap() = None;
        *self.key_map.lock().unwrap() = None;
        *self.status.lock().unwrap() = None;
    }
}

impl ParticipationListener for ListenerHub {
    fn on_memberships_change(&self, memberships: &[SessionMembership]) {
        let listener = self.memberships.lock().unwrap().clone();
        if let Some(listener) = listener {
            listener.on_memberships_change(memberships.iter().map(FfiMembership::from).collect());
        }
    }

    fn on_transports_change(&self, transports: &[TransportWithMembers]) {
        let listener = self.transports.lock().unwrap().clone();
        if let Some(listener) = listener {
            listener.on_transports_change(
                transports
                    .iter()
                    .map(FfiTransportWithMembers::from)
                    .collect(),
            );
        }
    }

    fn on_key_map_change(&self, key_map: &KeyMap) {
        let listener = self.key_map.lock().unwrap().clone();
        if let Some(listener) = listener {
            listener.on_key_map_change(key_map_from(key_map));
        }
    }

    fn on_status_change(&self, status: &Status) {
        let listener = self.status.lock().unwrap().clone();
        if let Some(listener) = listener {
            listener.on_status_change(FfiStatus::from(status));
        }
    }
}
