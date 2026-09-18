// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Transport-agnostic MatrixRTC media model.
//!
//! [`matrix-rtc-core`] answers *who is in the call* (memberships, keys); a
//! transport crate (e.g. `matrix-rtc-livekit`) answers *how bytes flow*. This
//! crate sits between them and gives applications one vocabulary for both:
//!
//! - [`Participant`]s keyed by MatrixRTC `member_id`, each with media
//!   [`StreamState`]s (microphone, camera, screenshare, ...);
//! - frame streams ([`AudioFrame`], [`VideoFrame`]) obtained per participant
//!   through [`RemoteTrackHandle`], with no transport types on the surface;
//! - per-participant [`MediaConstraints`] (visibility, rendered size, quality
//!   cap) that transports translate into subscribe-side simulcast control;
//! - a unified [`CallEvent`] stream merging membership signalling and media
//!   transport state.
//!
//! The [`CallEngine`] ties these together: it watches the core's membership
//! snapshots, maps transport-level participant identities back to memberships,
//! and (from Phase 1) maintains one connection per focus so that MSC4195
//! multi-SFU calls look like a single flat participant set.
//!
//! Transports implement [`MediaTransport`]/[`TransportConnection`]; everything
//! in this crate is `Send`, deliberately on the other side of a channel
//! boundary from the core's `?Send` command futures (the only core input is
//! the `watch` membership snapshot channel, whose payload is plain data).
//!
//! [`matrix-rtc-core`]: matrix_rtc_core

pub mod constraints;
pub mod engine;
pub mod event;
pub mod frame;
pub mod keys;
pub mod local;
pub mod participant;
mod rt;
pub mod stats;
pub mod tile;
pub mod transport;

pub use constraints::{
    Dimensions, MediaConstraints, QualityLimit, ResolvedConstraints, StreamDemand, VideoDetail,
};
pub use engine::{CallEngine, EngineConfig, EngineHandle};
pub use event::{CallEvent, EndedReason, FrameEncryptionDiagnostic, FrameEncryptionState};
pub use frame::{AudioFrame, I420Buffer, VideoFrame, VideoRotation};
pub use keys::{
    FrameKeyRing, KeyDiscardListener, KeyImportListener, LocalKeyIndexHook, MediaKeyHandler,
    ParticipantKey, SwitchCompleteListener,
};
pub use local::{AudioSourceConfig, LocalTrackHandle, PublishOptions, VideoSourceConfig};
pub use participant::{MediaStreamKind, Participant, StreamState};
pub use stats::ReceiveStats;
pub use tile::{CallTile, TileId, Tiles, derive_tiles};
pub use transport::{
    ConnectionContext, ConnectionEvent, MediaTransport, OwnMemberClaims, RemoteTrackHandle,
    SpeakingParticipant, TransportConnection, TransportError,
};
