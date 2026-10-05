// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! How MatrixRTC media flows, for any application on `matrix-rtc-core`.
//!
//! [`matrix_rtc_core`] answers *who is in the session* (memberships, keys);
//! this crate is the contract a media transport implements to carry their
//! bytes, in transport-neutral types:
//!
//! - [`MediaTransport`]/[`TransportConnection`]/[`RemoteTrackHandle`] — reach a
//!   member's advertised focus, map memberships to transport identities, and
//!   report [`ConnectionEvent`]s;
//! - owned frames ([`AudioFrame`], [`VideoFrame`]), publications
//!   ([`PublishOptions`] → [`LocalTrackHandle`]), receive statistics, and
//!   per-stream [`MediaConstraints`];
//! - [`MediaKeyHandler`], the bridge from the core's key signals to a
//!   transport's frame-encryption [`FrameKeyRing`];
//! - [`pool`], the MSC4195 multi-SFU connection pool: one connection per
//!   advertised focus, and media and keys reported per `member_id`;
//! - [`livekit`], the pure MSC4195 control plane (identities, token shapes).
//!
//! No transport IO, no libwebrtc and no call vocabulary: implementations live
//! in `matrix-rtc-livekit` (native) and the wasm binding (livekit-js), the call
//! roster that consumes them in `matrix-rtc-call-sdk`. Compiles for wasm32, where
//! the `Send` bounds vanish ([`matrix_rtc_core::MaybeSend`]).

pub mod connection;
pub mod constraints;
pub mod frame;
pub mod keys;
pub mod livekit;
pub mod local;
pub mod pool;
pub mod stats;
pub mod stream;

pub use connection::{
    ConnectionContext, ConnectionEvent, FrameEncryptionState, FrameStream, MediaTransport,
    OwnFocusTransport, OwnMemberClaims, RemoteTrackHandle, SpeakingParticipant,
    TransportConnection, TransportError,
};
pub use constraints::{
    Dimensions, MediaConstraints, QualityLimit, ResolvedConstraints, StreamDemand, VideoDetail,
};
pub use frame::{AudioFrame, I420Buffer, VideoFrame, VideoRotation};
pub use keys::{
    FrameKeyRing, KeyDiscardListener, KeyImportListener, LocalKeyIndexHook, MediaKeyHandler,
    ParticipantKey, SwitchCompleteListener,
};
pub use local::{AudioSourceConfig, LocalTrackHandle, PublishOptions, VideoSourceConfig};
pub use stats::ReceiveStats;
pub use stream::MediaStreamKind;
