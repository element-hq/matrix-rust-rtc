// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The contract a media transport implements.
//!
//! The MSC4143 *transport* is the description a member advertises — the core's
//! [`RtcTransport`]; this module is how media connects to one. A transport (LiveKit SFU today; P2P or WebTransport pub/sub later) knows
//! how to reach the media a member advertises in the `transports.published`
//! field of their `m.rtc.member` event, and how to map between MatrixRTC
//! memberships and its own participant identities.
//!
//! Members whose [`MediaTransport::connection_key`] is equal share one
//! [`TransportConnection`] — for LiveKit that key is the
//! `livekit_service_url`, matching MSC4195's rule that members on the same
//! focus share a room while every other focus needs its own subscribe-side
//! connection.
//!
//! Everything here is `Send + Sync` off `wasm32`, where transports must never
//! require the engine's tasks to be pinned to a `LocalSet`. On `wasm32` the
//! bounds vanish ([`MaybeSend`]): a browser transport wraps livekit-js through
//! `JsValue`s, which are `!Send`, and the engine runs on the JS event loop.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use matrix_rtc_core::{JoinedMembership, MaybeSend, RtcTransport};
use tokio::sync::mpsc;

use crate::constraints::ResolvedConstraints;
use crate::frame::{AudioFrame, VideoFrame};
use crate::local::{LocalTrackHandle, PublishOptions};
use crate::stats::ReceiveStats;
use crate::stream::MediaStreamKind;

/// Whether the connection carrying someone's media is up (034 R23, R28).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ConnectionState {
    /// Up, or being opened for the first time.
    #[default]
    Connected,
    /// Being re-established after a loss, or failing to connect, for as long
    /// as that takes: a connection is never given up while a member is on it.
    Reconnecting,
    /// No transport can serve what the member advertises.
    Unreachable,
}

/// Whether a participant's frames are encrypting and decrypting cleanly.
///
/// Reported per participant rather than per stream: the transport's frame
/// cryptor is keyed by participant identity, so a failure does not say which
/// of their tracks it came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameEncryptionState {
    /// Frames are being encrypted and decrypted normally.
    Ok,
    /// Frames are arriving with a key index we hold no key for — their media
    /// key has not reached us (or reached us under the wrong identity).
    MissingKey,
    /// We hold a key for the index the frames carry, but it does not decrypt
    /// them. The two sides disagree about the key material itself.
    DecryptionFailed,
    /// Our *outgoing* frames failed to encrypt, so peers receive nothing
    /// usable from us.
    EncryptionFailed,
    /// The transport's cryptor failed internally.
    InternalError,
}

impl FrameEncryptionState {
    /// Whether this state means media is not flowing usably.
    pub fn is_failure(&self) -> bool {
        !matches!(self, Self::Ok)
    }
}

/// A boxed frame stream: `Send` off `wasm32` (frames cross into the engine's
/// tasks), locally boxed on it (one thread, and JS-backed streams are `!Send`).
#[cfg(not(target_arch = "wasm32"))]
pub type FrameStream<'a, T> = futures_core::stream::BoxStream<'a, T>;
/// A boxed frame stream: `Send` off `wasm32` (frames cross into the engine's
/// tasks), locally boxed on it (one thread, and JS-backed streams are `!Send`).
#[cfg(target_arch = "wasm32")]
pub type FrameStream<'a, T> = futures_core::stream::LocalBoxStream<'a, T>;

/// Errors produced by media transports.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// The transport cannot serve the requested connection.
    #[error("unsupported transport: {0}")]
    Unsupported(String),
    /// Establishing the connection failed (auth, network, ...).
    #[error("transport connect failed: {0}")]
    Connect(String),
    /// The connection is gone.
    #[error("transport connection closed: {0}")]
    Closed(String),
    /// The SFU rejected the token a connect presented; a fresh one may
    /// succeed (034 R24).
    #[error("the SFU rejected the token: {0}")]
    TokenRejected(String),
    /// The SFU or the token service refused us for good (a 4xx): retrying
    /// cannot help (034 R24).
    #[error("refused: {0}")]
    Refused(String),
}

/// Our own membership identity, as transports need it for authentication and
/// identity derivation.
#[derive(Clone, Debug)]
pub struct OwnMemberClaims {
    /// `member.id` of our join, unique per join.
    pub member_id: String,
    pub user_id: String,
    pub device_id: String,
}

/// Call-scoped context handed to [`MediaTransport::connect`].
#[derive(Clone, Debug)]
pub struct ConnectionContext {
    pub room_id: String,
    pub slot_id: String,
    pub member: OwnMemberClaims,
}

/// Handle to one subscribed remote track: opens frame streams on demand.
///
/// Handles are cheap `Arc`s; a stream borrows nothing — dropping it stops
/// frame delivery for that consumer only.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait RemoteTrackHandle: MaybeSend {
    fn kind(&self) -> MediaStreamKind;

    /// A stream of decoded audio frames, for audio tracks.
    fn audio_frames(&self) -> Option<FrameStream<'static, AudioFrame>> {
        None
    }

    /// A stream of decoded video frames, for video tracks. Delivery is
    /// latest-frame-wins: slow consumers drop frames instead of buffering.
    fn video_frames(&self) -> Option<FrameStream<'static, VideoFrame>> {
        None
    }

    /// Cumulative receive-side RTP counters for this track.
    ///
    /// `None` from a transport whose RTP layer exposes no counters; the
    /// default. Because frames are produced whether or not RTP arrives, this
    /// is the only way a caller can tell a dead network path from a decrypt
    /// failure — see [`ReceiveStats`].
    async fn receive_stats(&self) -> Option<ReceiveStats> {
        None
    }
}

/// One speaking participant and how loud they are.
///
/// The level travels with the identity because the two come from the same
/// transport event: reporting only *who* is speaking forces a host to meter the
/// PCM itself to answer "how loud", which means decoding audio it may not
/// otherwise need and getting a different answer from the SFU's.
#[derive(Clone, Debug, PartialEq)]
pub struct SpeakingParticipant {
    /// Transport-level participant identity.
    pub identity: String,
    /// Most recent audio level, `0.0` (silent) to `1.0` (loudest). Transports
    /// that do not report one send `0.0`.
    pub level: f32,
}

/// State changes of one transport connection, translated into the
/// transport-neutral vocabulary.
///
/// `identity` is the transport-level participant identity (for LiveKit the
/// MSC4195 pseudonymous identity); the engine maps it back to a `member_id`
/// via [`MediaTransport::remote_identity`].
pub enum ConnectionEvent {
    RemoteJoined {
        identity: String,
    },
    RemoteLeft {
        identity: String,
    },
    TrackAdded {
        identity: String,
        kind: MediaStreamKind,
        track: Arc<dyn RemoteTrackHandle>,
    },
    TrackRemoved {
        identity: String,
        kind: MediaStreamKind,
    },
    TrackMuted {
        identity: String,
        kind: MediaStreamKind,
    },
    TrackUnmuted {
        identity: String,
        kind: MediaStreamKind,
    },
    ActiveSpeakers {
        /// Who is speaking, loudest first where the transport orders them, each
        /// with its most recent audio level.
        speakers: Vec<SpeakingParticipant>,
    },
    /// The frame cryptor's verdict on this participant's media changed.
    EncryptionStateChanged {
        identity: String,
        state: FrameEncryptionState,
    },
    /// The connection lost its transport link and is re-establishing it.
    Reconnecting,
    Reconnected,
    /// The connection ended and will not recover.
    Closed {
        message: String,
    },
}

impl fmt::Debug for ConnectionEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RemoteJoined { identity } => f
                .debug_struct("RemoteJoined")
                .field("identity", identity)
                .finish(),
            Self::RemoteLeft { identity } => f
                .debug_struct("RemoteLeft")
                .field("identity", identity)
                .finish(),
            Self::TrackAdded { identity, kind, .. } => f
                .debug_struct("TrackAdded")
                .field("identity", identity)
                .field("kind", kind)
                .finish_non_exhaustive(),
            Self::TrackRemoved { identity, kind } => f
                .debug_struct("TrackRemoved")
                .field("identity", identity)
                .field("kind", kind)
                .finish(),
            Self::TrackMuted { identity, kind } => f
                .debug_struct("TrackMuted")
                .field("identity", identity)
                .field("kind", kind)
                .finish(),
            Self::TrackUnmuted { identity, kind } => f
                .debug_struct("TrackUnmuted")
                .field("identity", identity)
                .field("kind", kind)
                .finish(),
            Self::ActiveSpeakers { speakers } => f
                .debug_struct("ActiveSpeakers")
                .field("speakers", speakers)
                .finish(),
            Self::EncryptionStateChanged { identity, state } => f
                .debug_struct("EncryptionStateChanged")
                .field("identity", identity)
                .field("state", state)
                .finish(),
            Self::Reconnecting => f.write_str("Reconnecting"),
            Self::Reconnected => f.write_str("Reconnected"),
            Self::Closed { message } => f.debug_struct("Closed").field("message", message).finish(),
        }
    }
}

/// A media transport backend (LiveKit today; P2P/WebTransport later).
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait MediaTransport: MaybeSend {
    /// Stable name of the backend, matching the MSC4143 transport `type`
    /// (e.g. `"livekit"`). Used for logging and `can_subscribe` matching.
    fn transport_type(&self) -> &'static str;

    /// The connection grouping key for a published transport, if this backend
    /// can serve it. Members with equal keys share one connection.
    fn connection_key(&self, transport: &RtcTransport) -> Option<String>;

    /// The identity this member will have on the media plane, used to
    /// reverse-map transport participants to memberships. `None` when the
    /// membership lacks what the derivation needs (e.g. no attributable
    /// sending device).
    fn remote_identity(&self, member: &JoinedMembership) -> Option<String>;

    /// Open the connection for `connection_key` and start translating its
    /// state into [`ConnectionEvent`]s. The returned receiver ends (after a
    /// final [`ConnectionEvent::Closed`], when the transport can tell) when
    /// the connection is gone.
    async fn connect(
        &self,
        connection_key: &str,
        ctx: &ConnectionContext,
    ) -> Result<
        (
            Box<dyn TransportConnection>,
            mpsc::UnboundedReceiver<ConnectionEvent>,
        ),
        TransportError,
    >;
}

/// A live connection to one focus.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait TransportConnection: MaybeSend {
    /// The grouping key this connection was opened for. A handle that took
    /// another over ([`Self::take_over`]) serves that one's from then on,
    /// which its owner keeps track of.
    fn connection_key(&self) -> &str;

    /// Publish a local track on this connection (only ever called on the own
    /// focus). The default rejects publishing, for receive-only transports.
    async fn publish(
        &self,
        _options: PublishOptions,
    ) -> Result<Arc<dyn LocalTrackHandle>, TransportError> {
        Err(TransportError::Unsupported(
            "this transport cannot publish".into(),
        ))
    }

    /// Apply resolved subscription constraints for one of this connection's
    /// remote participants. Transports interpret — LiveKit maps to
    /// enabled/dimensions/quality (`UpdateTrackSettings`); a transport with
    /// no subscription control may ignore them (the default).
    async fn apply_constraints(
        &self,
        _identity: &str,
        _kind: MediaStreamKind,
        _resolved: ResolvedConstraints,
    ) -> Result<(), TransportError> {
        Ok(())
    }

    /// Switch our own outgoing frames to `key_index` (only ever called on the
    /// own focus). Importing a key only fills the transport's ring; the index
    /// our frames carry changes here. The default ignores it, for transports
    /// without frame encryption.
    fn set_local_key_index(&self, _key_index: u8) {}

    /// Establish this connection again behind the same handle, after a loss
    /// or for a first connect (034 R24): the handle, and every local
    /// publication made through it, stays valid and is carried onto the new
    /// connection. `fresh_token` asks for a new token instead of the one the
    /// last connect used. Resolves with the new connection's events.
    ///
    /// Only the own focus is reconnected this way; the default refuses, for
    /// transports whose connections cannot be.
    async fn reconnect(
        &self,
        _fresh_token: bool,
    ) -> Result<mpsc::UnboundedReceiver<ConnectionEvent>, TransportError> {
        Err(TransportError::Unsupported(
            "this connection cannot be re-established".into(),
        ))
    }

    /// This connection's established session, for another handle of the
    /// same transport to [`take_over`](Self::take_over) (034 R25): opaque,
    /// and only meaningful to that transport. Changes nothing by itself; once
    /// taken over, this handle is no longer used. `None` while not connected,
    /// and by default.
    fn hand_over(&self) -> Option<Box<dyn Any>> {
        None
    }

    /// Move this handle onto the session another connection
    /// [handed over](Self::hand_over) (034 R25): its focus becomes this
    /// handle's, every publication made through this handle goes out there,
    /// and a later [`reconnect`](Self::reconnect) goes there too. A reconnect
    /// still in flight is abandoned. Synchronous, so its owner decides
    /// between it and such a reconnect in one step. The default refuses.
    fn take_over(&self, _handover: Box<dyn Any>) -> Result<(), TransportError> {
        Err(TransportError::Unsupported(
            "this connection cannot take another over".into(),
        ))
    }

    /// Close the connection. Idempotent.
    async fn close(&self) -> Result<(), TransportError>;
}

/// A transport whose own-focus connection the caller keeps a typed handle to,
/// next to the engine's: to close it on leave, and for whatever the concrete
/// connection offers beyond [`TransportConnection`].
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait OwnFocusTransport: MediaTransport {
    /// A cheap clonable handle; one clone is handed to the engine.
    type Connection: TransportConnection + Clone + 'static;

    /// [`MediaTransport::connect`], returning the concrete connection.
    async fn connect_own(
        &self,
        connection_key: &str,
        ctx: &ConnectionContext,
    ) -> Result<(Self::Connection, mpsc::UnboundedReceiver<ConnectionEvent>), TransportError>;

    /// The own-focus handle, not connected yet: the pool establishes it
    /// through [`TransportConnection::reconnect`], so a join does not wait
    /// for the SFU (034 R24, contract C10). Publications made through it
    /// before then go out once it connects.
    fn prepare_own(&self, connection_key: &str, ctx: &ConnectionContext) -> Self::Connection;
}
