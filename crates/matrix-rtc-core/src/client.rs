// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Opening rooms that feed themselves: a [`BaseRtcClient`] per backend, a
//! [`BaseRtcRoomHandle`] per open room. Opening a room subscribes to it through
//! the [`feeder`](crate::feeder) and runs the feed on the
//! [`executor`](crate::executor) while the handle lives; the client's one
//! to-device subscription runs while any room is open.
//!
//! The only state spanning rooms is the backend and the routing of to-device
//! media keys to the room they name, through a registry of weak handles. A
//! room is any [`ApplicationIntake`]: [`BaseRtcRoom`] for a host of the core
//! alone, an application's room state over it otherwise.

use std::sync::{Arc, Mutex as StdMutex, MutexGuard};

use tokio::sync::{Mutex, watch};

use crate::executor::{self, AbortOnDrop, JoinHandleExt};
use crate::feeder::{
    IngestDialect, RoomAlreadyOpen, RoomAttachment, RoomFeeder, RoomRegistry, SpecDialect,
    ToDeviceFeeder,
};
use crate::{
    ApplicationIntake, BackendError, BaseRtcRoom, JoinError, JoinSessionParams, JoinedMembership,
    LeaveError, LeaveSessionParams, MatrixBackend, MaybeSend,
};

#[cfg(test)]
mod tests;

/// Why a room did not open.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    /// The room already has a live handle.
    #[error(transparent)]
    RoomAlreadyOpen(#[from] RoomAlreadyOpen),
    #[error("backend: {0}")]
    Backend(#[from] BackendError),
}

/// What every room of a client shares.
struct ClientShared<B: MatrixBackend + 'static, M> {
    backend: Arc<B>,
    registry: RoomRegistry<M>,
    /// The to-device types media keys also arrive as, beyond the spec's.
    key_event_types: Vec<String>,
    /// Running while at least one room is open. Locked together with the
    /// registry's changes, so a room opening and the last one closing cannot
    /// interleave into an open room with no subscription.
    to_device: StdMutex<Option<ToDeviceRunning>>,
    /// Serialises opening rooms, so two first rooms do not both subscribe.
    opening: Mutex<()>,
}

/// The client's to-device subscription and the task draining it.
struct ToDeviceRunning {
    feeder: ToDeviceFeeder,
    _task: AbortOnDrop<()>,
}

impl<B: MatrixBackend + 'static, M> ClientShared<B, M> {
    fn to_device(&self) -> MutexGuard<'_, Option<ToDeviceRunning>> {
        self.to_device
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Forgets `room_id`, and stops the to-device subscription with the last
    /// room.
    fn release(&self, room_id: &str) {
        let mut to_device = self.to_device();
        if self.registry.unregister(room_id)
            && let Some(running) = to_device.take()
        {
            log::info!("client: last room closed; to-device subscription stopped");
            running.feeder.stop();
        }
    }
}

/// One per backend, over rooms of type `M`. Creating it does no I/O.
pub struct BaseRtcClient<B: MatrixBackend + 'static, M = BaseRtcRoom<B>> {
    shared: Arc<ClientShared<B, M>>,
}

impl<B: MatrixBackend + 'static, M> Clone for BaseRtcClient<B, M> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<B: MatrixBackend + 'static, M> BaseRtcClient<B, M>
where
    M: ApplicationIntake<B> + MaybeSend + 'static,
{
    pub fn new(backend: Arc<B>) -> Self {
        Self::with_key_event_types(backend, Vec::new())
    }

    /// Also subscribes to media keys of `key_event_types`, which the dialect
    /// of the room each names reads (see [`IngestDialect::parse_key`]).
    pub fn with_key_event_types(backend: Arc<B>, key_event_types: Vec<String>) -> Self {
        Self {
            shared: Arc::new(ClientShared {
                backend,
                registry: RoomRegistry::default(),
                key_event_types,
                to_device: StdMutex::new(None),
                opening: Mutex::new(()),
            }),
        }
    }

    pub fn backend(&self) -> &Arc<B> {
        &self.shared.backend
    }

    /// Opens `room_id` with `room` as its state: registers it, subscribes to
    /// what `dialect` needs, and starts the to-device subscription if this is
    /// the first open room. The feeds run on the executor; await
    /// [`BaseRtcRoomHandle::seeded`] before joining.
    ///
    /// Refused while the room has a live handle. Cancelling the call part-way
    /// leaves nothing behind: the room is free again, and the to-device
    /// subscription stops if no other room is open.
    pub async fn open_with(
        &self,
        room_id: impl Into<String>,
        room: Arc<Mutex<M>>,
        dialect: Arc<dyn IngestDialect>,
    ) -> Result<BaseRtcRoomHandle<B, M>, OpenError> {
        let room_id = room_id.into();
        let shared = &self.shared;
        let _opening = shared.opening.lock().await;

        let needs_to_device = {
            let to_device = shared.to_device();
            shared.registry.register(&room_id, dialect.clone(), &room)?;
            to_device.is_none()
        };
        // From here every early return — an error, or the caller dropping
        // this future at an await — must undo the registration.
        let opening = Opening {
            shared,
            room_id: &room_id,
            opened: false,
        };
        log::info!("client: [{room_id}] opening");

        if needs_to_device {
            let (feeder, run) = ToDeviceFeeder::start(
                shared.backend.clone(),
                shared.registry.clone(),
                shared.key_event_types.clone(),
            )
            .await?;
            *shared.to_device() = Some(ToDeviceRunning {
                feeder,
                _task: executor::spawn(run.run()).abort_on_drop(),
            });
            log::info!("client: to-device subscription started");
        }

        let (attachment, feed) =
            RoomFeeder::attach(shared.backend.clone(), room.clone(), dialect).await?;
        opening.opened();

        Ok(BaseRtcRoomHandle {
            room_id,
            room,
            attached: Some(Attached {
                attachment,
                _feed: executor::spawn(feed.run()).abort_on_drop(),
            }),
            client: shared.clone(),
        })
    }
}

impl<B: MatrixBackend + 'static> BaseRtcClient<B> {
    /// Opens `room_id` as a [`BaseRtcRoom`] read in the spec's dialect. See
    /// [`Self::open_with`].
    pub async fn room(
        &self,
        room_id: impl Into<String>,
    ) -> Result<BaseRtcRoomHandle<B>, OpenError> {
        let room_id = room_id.into();
        let room = Arc::new(Mutex::new(BaseRtcRoom::with_backend(
            room_id.clone(),
            self.shared.backend.clone(),
        )));
        self.open_with(room_id, room, Arc::new(SpecDialect)).await
    }
}

/// Releases a room whose opening did not finish.
struct Opening<'a, B: MatrixBackend + 'static, M> {
    shared: &'a ClientShared<B, M>,
    room_id: &'a str,
    opened: bool,
}

impl<B: MatrixBackend + 'static, M> Opening<'_, B, M> {
    fn opened(mut self) {
        self.opened = true;
    }
}

impl<B: MatrixBackend + 'static, M> Drop for Opening<'_, B, M> {
    fn drop(&mut self) {
        if !self.opened {
            log::info!("client: [{}] opening abandoned", self.room_id);
            self.shared.release(self.room_id);
        }
    }
}

/// A room's subscription and the task feeding it.
struct Attached {
    attachment: RoomAttachment,
    _feed: AbortOnDrop<()>,
}

/// One open room. Dropping it ends its subscription and frees the room id; it
/// sends nothing, so a membership left behind expires through its delayed
/// leave.
pub struct BaseRtcRoomHandle<B: MatrixBackend + 'static, M = BaseRtcRoom<B>> {
    room_id: String,
    room: Arc<Mutex<M>>,
    /// `None` once detached.
    attached: Option<Attached>,
    client: Arc<ClientShared<B, M>>,
}

impl<B: MatrixBackend + 'static, M> BaseRtcRoomHandle<B, M> {
    pub fn room_id(&self) -> &str {
        &self.room_id
    }

    /// The room's state, which the feed writes into.
    pub fn state(&self) -> &Arc<Mutex<M>> {
        &self.room
    }

    pub fn backend(&self) -> &Arc<B> {
        &self.client.backend
    }

    /// Resolves once the room's current state is applied, so a join issued
    /// afterwards sees it.
    pub async fn seeded(&self) {
        if let Some(attached) = &self.attached {
            attached.attachment.seeded().await;
        }
    }

    pub fn is_seeded(&self) -> bool {
        self.attached
            .as_ref()
            .is_some_and(|attached| attached.attachment.is_seeded())
    }

    /// Ends the subscription and forgets the room; idempotent.
    pub fn detach(&mut self) {
        if let Some(attached) = self.attached.take() {
            drop(attached);
            self.client.release(&self.room_id);
        }
    }
}

impl<B: MatrixBackend + 'static, M> Drop for BaseRtcRoomHandle<B, M> {
    fn drop(&mut self) {
        self.detach();
    }
}

impl<B: MatrixBackend + 'static> BaseRtcRoomHandle<B> {
    /// See [`BaseRtcRoom::join`]: the joined slot keeps itself alive until
    /// left.
    pub async fn join(&self, params: JoinSessionParams) -> Result<String, JoinError> {
        self.room.lock().await.join(params).await
    }

    pub async fn leave(&self, slot_id: &str, params: LeaveSessionParams) -> Result<(), LeaveError> {
        self.room.lock().await.leave(slot_id, params).await
    }

    /// The slot's joined memberships as they change, without joining it.
    pub async fn observe(&self, slot_id: &str) -> watch::Receiver<Vec<JoinedMembership>> {
        self.room.lock().await.observe_slot(slot_id)
    }
}
