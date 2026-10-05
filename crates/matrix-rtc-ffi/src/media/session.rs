// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The FFI media session: the host-facing equivalent of the native `LiveKitCall`
//! facade's media half, layered on a call the host has already joined
//! ([`RtcCall`](crate::RtcCall)).

use std::sync::Arc;

use tokio::sync::Mutex as TokioMutex;
use tokio::sync::broadcast;
use tokio::sync::watch;

use matrix_rtc_call_sdk::{AttachError, CallEngine, CallEvent, MediaAttachment};
use matrix_rtc_call_sdk::{LiveKitAttachOptions, LiveKitAttachment, attach_livekit};
use matrix_rtc_core::compat::MembershipFormat;
use matrix_rtc_livekit::{LiveKitTransportConnection, MediaKeyBridge};
use matrix_rtc_transport::{MediaStreamKind, TransportConnection as _};

use super::frames::{AudioFrameStream, FfiLocalTrack, VideoFrameStream};
use super::types::{
    FfiCallEvent, FfiLocalState, FfiMediaConstraints, FfiParticipant, FfiPublishOptions,
    FfiReceiveStats, FfiStabilityConfig, FfiStreamKind, FfiStreamRef, FfiStreamStats, FfiTileId,
    FfiTileRoster, zip_stream_stats,
};
use super::{MediaFfiError, runtime};
use crate::RtcCall;

/// Tuning for a media session. The call says the rest: its room, slot and
/// `member.id`, the focus its join publishes on, and — through the backend —
/// who we are.
#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct MediaSessionConfig {
    /// How much the tile order is damped. `None` takes the defaults.
    #[uniffi(default = None)]
    pub stability: Option<FfiStabilityConfig>,
}

/// Attach media to a joined call: wire frame-key signalling into the core,
/// start the engine (which connects to every peer's focus), and connect the
/// own-focus SFU with per-participant frame E2EE.
///
/// The `member.id` comes from the call's join — the host neither chooses nor
/// passes it. OpenID tokens for the SFU exchange come from the backend.
#[uniffi::export(async_runtime = "tokio")]
pub async fn connect_media_session(
    call: Arc<RtcCall>,
    config: MediaSessionConfig,
) -> Result<Arc<MediaSession>, MediaFfiError> {
    // Everything media lives on the dedicated runtime; hopping onto it here
    // means every internally spawned task (engine actor, pool, IO) inherits
    // the right context regardless of which thread the FFI call came in on.
    runtime()
        .spawn(build_media_session(call, config))
        .await
        .map_err(|error| MediaFfiError::Transport(format!("media task panicked: {error}")))?
}

async fn build_media_session(
    call: Arc<RtcCall>,
    config: MediaSessionConfig,
) -> Result<Arc<MediaSession>, MediaFfiError> {
    // Which MatrixRTC generation this room was joined for, read back from the
    // join rather than taken as a parameter: it decides the participant identity
    // and the token endpoint, and those disagreeing with the membership we
    // already published is not an error but a silence. See `crate::compat`.
    let compat = call.format();
    if compat != MembershipFormat::Current {
        log::info!(
            "media: [{}/{}] connecting in Element Call compatibility mode {compat:?}",
            call.room_id(),
            call.slot_id(),
        );
    }
    let LiveKitAttachment {
        media:
            MediaAttachment {
                engine,
                own_connection,
                own_identity,
                events,
            },
        key_bridge,
    } = attach_livekit(
        call.inner(),
        call.backend(),
        LiveKitAttachOptions {
            format: compat,
            http: None,
            auto_subscribe: true,
            stability: config.stability.map(Into::into).unwrap_or_default(),
        },
    )
    .await
    .map_err(|error| match error {
        AttachError::NotJoined(message) => MediaFfiError::NotJoined(message),
        error => MediaFfiError::Transport(error.to_string()),
    })?;

    let tiles = engine.subscribe_tiles();
    let local = engine.subscribe_local_state();
    Ok(Arc::new(MediaSession {
        engine,
        connection: own_connection,
        _bridge: key_bridge,
        events: TokioMutex::new(events),
        tiles: TokioMutex::new(tiles),
        local: TokioMutex::new(local),
        own_identity,
    }))
}

/// A live media session on a joined slot: the participant roster, the
/// unified event stream, per-stream constraints, frame streams, and local
/// publications — with no transport types on the surface.
///
/// Ends with the call; [`MediaSession::disconnect`] ends it while staying joined.
#[derive(uniffi::Object)]
pub struct MediaSession {
    engine: CallEngine,
    /// The own-focus connection; `None` for a receive-only call.
    connection: Option<LiveKitTransportConnection>,
    /// Keeps the key bridge alive alongside the session for clarity; the
    /// core's encryption manager also holds it.
    _bridge: Arc<MediaKeyBridge>,
    events: TokioMutex<broadcast::Receiver<CallEvent>>,
    tiles: TokioMutex<watch::Receiver<matrix_rtc_call_sdk::TileRoster>>,
    local: TokioMutex<watch::Receiver<Option<matrix_rtc_call_sdk::LocalState>>>,
    own_identity: String,
}

#[uniffi::export(async_runtime = "tokio")]
impl MediaSession {
    /// The next event on the unified call stream. Suspends until one
    /// arrives; `None` means the session is over. Bridge to a Kotlin `Flow`
    /// or Swift `AsyncStream` by looping.
    ///
    /// Events are one-shots and diagnostics: joins and leaves, streams
    /// starting and stopping, key and encryption reports, the connection
    /// degrading, the call ending. **State lives elsewhere**: what to draw,
    /// and whose audio to play, is [`Self::next_roster`] — a latest-value push
    /// whose order is every tile in the call. So a consumer that falls very
    /// far behind (the buffer holds 256) can lose a sound cue or a badge
    /// update, never the roster. Who is speaking is not an event at all: it
    /// is [`FfiCallTile::speaking`].
    pub async fn next_event(&self) -> Option<FfiCallEvent> {
        let mut events = self.events.lock().await;
        loop {
            match events.recv().await {
                Ok(event) => {
                    if let Some(event) = FfiCallEvent::relayed(event) {
                        return Some(event);
                    }
                }
                Err(broadcast::error::RecvError::Lagged(missed)) => {
                    log::warn!("call event consumer lagged; {missed} events dropped");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }

    /// The current participant roster, including ourselves: the transport's
    /// un-joined view, one row per membership. A **diagnostics pull**, not a
    /// live surface — it is the whole call every time, which is the cost the
    /// tile roster's detail window exists to avoid. Read it once to seed what
    /// the tiles do not carry yet (your own row before local state arrives),
    /// and on demand for a readout. Contract C11.
    pub fn participants(&self) -> Vec<FfiParticipant> {
        self.engine
            .participants()
            .into_iter()
            .map(Into::into)
            .collect()
    }

    /// The next tile roster. Suspends until it changes; `None` means the
    /// session is over. Latest-value-wins: a consumer that falls behind gets
    /// the current roster, never a backlog. The first call on a session that
    /// already has tiles returns at once. Contract C7.
    pub async fn next_roster(&self) -> Option<FfiTileRoster> {
        let mut tiles = self.tiles.lock().await;
        tiles.changed().await.ok()?;
        Some(tiles.borrow_and_update().clone().into())
    }

    /// The tile roster as it stands now.
    pub fn roster(&self) -> FfiTileRoster {
        self.engine.tiles().into()
    }

    /// The next change to our own tile or screen-sharing flag. Suspends until
    /// one; skips the state before our membership is on the roster, so the
    /// first value is our first tile. `None` means the session is over.
    pub async fn next_local_state(&self) -> Option<FfiLocalState> {
        let mut local = self.local.lock().await;
        loop {
            local.changed().await.ok()?;
            if let Some(state) = local.borrow_and_update().clone() {
                return Some(state.into());
            }
        }
    }

    /// Our own tile and screen-sharing flag now; `None` until our membership
    /// is on the roster.
    pub fn local_state(&self) -> Option<FfiLocalState> {
        self.engine.local_state().map(Into::into)
    }

    /// Declare which tiles get full records in [`FfiTileRoster::detail`]:
    /// ranks `[offset, offset + len)` plus `also`, wherever those rank — a
    /// tile shown full-screen, a picture-in-picture source. The default is
    /// everything. Declare what you compose, not what is visible. Contract C12.
    pub fn set_detail_window(&self, offset: u32, len: u32, also: Vec<FfiTileId>) {
        self.engine
            .set_detail_window(offset, len, also.into_iter().map(Into::into));
    }

    /// Our participant identity on the media plane (the JWT `sub`; peers import
    /// our media key under it).
    ///
    /// The MSC4195 pseudonymous hash, or — in
    /// [`FfiMembershipFormat::RoomState`](crate::FfiMembershipFormat::RoomState)
    /// — the plain `{user}:{device}` string that generation's authorisation
    /// service mints.
    pub fn local_identity(&self) -> String {
        self.own_identity.clone()
    }

    /// Set the subscription constraints for one stream of one participant.
    /// Debounced and re-applied automatically when the stream (re)appears.
    pub fn set_constraints(
        &self,
        member_id: String,
        kind: FfiStreamKind,
        constraints: FfiMediaConstraints,
    ) {
        self.engine
            .set_constraints(member_id, kind.into(), constraints.into());
    }

    /// The audio frame stream of a participant's stream, once
    /// [`FfiCallEvent::StreamStarted`] announced it. Each call opens an
    /// independent stream.
    pub fn audio_stream(
        &self,
        member_id: String,
        kind: FfiStreamKind,
    ) -> Option<Arc<AudioFrameStream>> {
        let track = self.engine.remote_track(&member_id, kind.into())?;
        Some(Arc::new(AudioFrameStream::new(track.audio_frames()?)))
    }

    /// The video frame stream of a participant's stream (latest-frame-wins).
    pub fn video_stream(
        &self,
        member_id: String,
        kind: FfiStreamKind,
    ) -> Option<Arc<VideoFrameStream>> {
        let track = self.engine.remote_track(&member_id, kind.into())?;
        Some(Arc::new(VideoFrameStream::new(track.video_frames()?)))
    }

    /// Cumulative receive-side RTP counters for a participant's stream.
    ///
    /// `null` while that stream is not subscribed, or before the first RTCP
    /// report has arrived. This is the only way to tell "no RTP arriving" from
    /// "RTP arriving that does not decode": the receive path fabricates frames
    /// at a fixed cadence either way. See [`FfiReceiveStats`] for how to read
    /// the counters — they are totals, so sample twice and compare.
    pub async fn receive_stats(
        &self,
        member_id: String,
        kind: FfiStreamKind,
    ) -> Option<FfiReceiveStats> {
        self.engine
            .receive_stats(&member_id, kind.into())
            .await
            .map(Into::into)
    }

    /// [`MediaSession::receive_stats`] for many streams in one round trip.
    ///
    /// One entry per requested stream, in request order; nothing is omitted
    /// and a duplicate is answered twice. Bound the request to the tiles you
    /// compose — the detail window of contract C12 is the right set, plus the
    /// microphone of each member in it — and call this once per sample rather
    /// than once per stream. The counters are the same cumulative totals as
    /// the single call; see [`FfiReceiveStats`].
    pub async fn receive_stats_for(&self, streams: Vec<FfiStreamRef>) -> Vec<FfiStreamStats> {
        let keys: Vec<(String, MediaStreamKind)> =
            streams.iter().cloned().map(Into::into).collect();
        let results = self.engine.receive_stats_for(&keys).await;
        zip_stream_stats(streams, results)
    }

    /// Publish a local track on our focus; push captured frames into the
    /// returned handle. Retract it with [`MediaSession::unpublish`] — closing
    /// the returned handle does not.
    pub async fn publish(
        &self,
        options: FfiPublishOptions,
    ) -> Result<Arc<FfiLocalTrack>, MediaFfiError> {
        log::info!("media: publishing {options:?}");

        let handle = self.engine.publish(options.into()).await.map_err(|error| {
            log::warn!("media: publish failed: {error}");
            MediaFfiError::Transport(error.to_string())
        })?;
        Ok(Arc::new(FfiLocalTrack::new(handle)))
    }

    /// Mute or unmute one of our own publications.
    ///
    /// Peers are told, so their UI can show it — muting is not the same as
    /// simply not pushing frames, which looks to a peer like a stalled sender.
    /// Our own roster entry and the event stream are updated too
    /// (`StreamMuted`/`StreamUnmuted` against our `member_id`), so a host can
    /// render its own state from the same source it renders everyone else's
    /// instead of keeping a parallel copy.
    ///
    /// Errors if nothing of that kind is currently published.
    pub async fn set_local_muted(
        &self,
        kind: FfiStreamKind,
        muted: bool,
    ) -> Result<(), MediaFfiError> {
        log::info!("media: setting our own {kind:?} muted={muted}");

        self.engine
            .set_local_muted(kind.into(), muted)
            .await
            .map_err(|error| {
                log::warn!("media: local mute failed: {error}");
                MediaFfiError::Transport(error.to_string())
            })
    }

    /// Retract one of our own publications, so peers drop the stream instead
    /// of rendering an empty tile — what a stopped screen share needs, since
    /// unlike a camera a screen has no "off" state a mute could represent.
    ///
    /// On success peers see the stream removed, our own roster entry drops
    /// it (`StreamStopped` against our `member_id`), and the `FfiLocalTrack`
    /// from [`MediaSession::publish`] is dead: `captureAudio` /
    /// `captureVideo` fail with a transport error (they never crash, so a
    /// capture thread still mid-call is safe — it should stop on the first
    /// error). On failure the publication is still live and stays on the
    /// roster, usable and retryable — treat the stream as still visible to
    /// peers. `ScreenShare` and `ScreenShareAudio` are separate
    /// publications; unpublish each. Re-publishing the same kind later is a
    /// fresh [`MediaSession::publish`].
    ///
    /// Errors if nothing of that kind is currently published.
    pub async fn unpublish(&self, kind: FfiStreamKind) -> Result<(), MediaFfiError> {
        log::info!("media: unpublishing our own {kind:?}");

        self.engine.unpublish(kind.into()).await.map_err(|error| {
            log::warn!("media: unpublish failed: {error}");
            MediaFfiError::Transport(error.to_string())
        })
    }

    /// End the media session while staying joined. Not needed after leaving.
    ///
    /// (Named `disconnect` rather than `close`: uniffi already gives every
    /// Kotlin object an `AutoCloseable.close()` for handle disposal, and a
    /// suspend `close()` collides with it.)
    pub async fn disconnect(&self) -> Result<(), MediaFfiError> {
        log::info!("media: disconnecting");
        self.engine.shutdown().await;
        let Some(connection) = &self.connection else {
            return Ok(());
        };
        connection.close().await.map_err(|error| {
            log::warn!("media: own focus did not close cleanly: {error}");
            MediaFfiError::Transport(error.to_string())
        })
    }
}
