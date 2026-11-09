// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Publishing local media.
//!
//! Capture stays platform-side (camera, microphone, screen grabber): the
//! application pushes raw frames *into* the transport through a
//! [`LocalTrackHandle`], and the transport owns encoding, simulcast layering,
//! and pacing. Publications always go to the *own* focus — the one announced
//! in our membership's `transports`, where peers subscribe to us.

use async_trait::async_trait;
use matrix_rtc_core::MaybeSend;

use crate::connection::TransportError;
use crate::frame::{AudioFrame, VideoFrame};
use crate::stats::SendStats;
use crate::stream::MediaStreamKind;

/// PCM format the application will push into an audio publication.
#[derive(Clone, Copy, Debug)]
pub struct AudioSourceConfig {
    pub sample_rate: u32,
    pub num_channels: u32,
}

impl Default for AudioSourceConfig {
    fn default() -> Self {
        Self {
            sample_rate: 48_000,
            num_channels: 1,
        }
    }
}

/// Capture resolution of a video publication. Transports derive encoder and
/// simulcast layer settings from it.
#[derive(Clone, Copy, Debug)]
pub struct VideoSourceConfig {
    pub width: u32,
    pub height: u32,
}

/// Video codec a publisher asks the transport to encode with.
///
/// A preference, not a guarantee: a transport that cannot encode the codec on
/// this platform publishes with its default instead (LiveKit's is VP8). Which
/// codecs are available depends on the platform and on how the media stack was
/// built, so a host offering a choice should ask the transport first. Decode
/// support varies too: not every receiver can decode H.264.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum VideoCodecPreference {
    /// The transport's default.
    #[default]
    Auto,
    Vp8,
    H264,
    Vp9,
    Av1,
}

/// Caps on a video publication's encoder.
///
/// A field left `None` is computed by the transport from the source
/// resolution, as when no limit is given at all. With simulcast, the caps
/// apply to the top layer; the transport derives the lower layers from it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VideoEncodingLimits {
    pub max_bitrate_bps: Option<u64>,
    pub max_framerate: Option<u32>,
}

/// What to publish.
#[derive(Clone, Debug)]
pub struct PublishOptions {
    /// Which stream this is (microphone, camera, screenshare, ...); peers see
    /// it under the same [`MediaStreamKind`].
    pub kind: MediaStreamKind,
    /// PCM format, for audio kinds.
    pub audio: Option<AudioSourceConfig>,
    /// Capture resolution, for video kinds.
    pub video: Option<VideoSourceConfig>,
    /// Publish multiple quality layers so receivers can pick per their
    /// [`MediaConstraints`](crate::constraints::MediaConstraints). Video only;
    /// transports without layered publishing ignore it.
    pub simulcast: bool,
    /// Publish already muted, so peers see a deliberate off from the first
    /// frame on.
    ///
    /// For a lobby that joins muted. Muting after the publish resolves cannot
    /// express that: it leaves a window in which the track is live and unmuted
    /// at the SFU, which for a microphone is a hot mic. The mute travels in the
    /// same request that announces the track, so there is no such window.
    pub muted: bool,
    /// Codec to encode with, for video kinds; see [`VideoCodecPreference`].
    pub video_codec: VideoCodecPreference,
    /// Encoder caps, for video kinds; see [`VideoEncodingLimits`].
    pub video_encoding: VideoEncodingLimits,
}

impl PublishOptions {
    /// A microphone publication with the default PCM format.
    pub fn microphone() -> Self {
        Self {
            kind: MediaStreamKind::Microphone,
            audio: Some(AudioSourceConfig::default()),
            video: None,
            simulcast: false,
            muted: false,
            video_codec: VideoCodecPreference::Auto,
            video_encoding: VideoEncodingLimits::default(),
        }
    }

    /// A camera publication with simulcast enabled.
    pub fn camera(video: VideoSourceConfig) -> Self {
        Self {
            kind: MediaStreamKind::Camera,
            audio: None,
            video: Some(video),
            simulcast: true,
            muted: false,
            video_codec: VideoCodecPreference::Auto,
            video_encoding: VideoEncodingLimits::default(),
        }
    }

    /// A screenshare publication with simulcast enabled.
    pub fn screen_share(video: VideoSourceConfig) -> Self {
        Self {
            kind: MediaStreamKind::ScreenShare,
            audio: None,
            video: Some(video),
            simulcast: true,
            muted: false,
            video_codec: VideoCodecPreference::Auto,
            video_encoding: VideoEncodingLimits::default(),
        }
    }

    /// Publish already muted; see [`PublishOptions::muted`].
    pub fn muted(mut self) -> Self {
        self.muted = true;
        self
    }

    /// Encode with `codec`; see [`VideoCodecPreference`].
    pub fn with_video_codec(mut self, codec: VideoCodecPreference) -> Self {
        self.video_codec = codec;
        self
    }

    /// Cap the encoder; see [`VideoEncodingLimits`].
    pub fn with_video_encoding(mut self, limits: VideoEncodingLimits) -> Self {
        self.video_encoding = limits;
        self
    }
}

/// A live local publication: push captured frames into it.
///
/// Handles are cheap `Arc`s. A publication ends with its connection
/// (leave/close) or when retracted through
/// `CallEngine::unpublish`; after
/// that, `capture_*` calls on the handle error rather than silently
/// succeeding, so a capture loop learns to stop.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait LocalTrackHandle: MaybeSend {
    fn kind(&self) -> MediaStreamKind;

    /// Mute or unmute this publication at the transport.
    ///
    /// Muting stops media reaching peers and tells them why, so their UI can
    /// show it. Distinct from simply not calling `capture_*`: that looks like a
    /// stalled sender to a peer, which is what they see for a wedged client.
    fn set_muted(&self, _muted: bool) -> Result<(), TransportError> {
        Err(TransportError::Unsupported(
            "this publication cannot be muted".into(),
        ))
    }

    /// Retract this publication at the transport, so peers drop the stream
    /// entirely — unlike a mute, which keeps the publication up and tells
    /// them why it is silent. Already-retracted is success (the room may
    /// have closed and taken every publication with it).
    async fn unpublish(&self) -> Result<(), TransportError> {
        Err(TransportError::Unsupported(
            "this publication cannot be unpublished".into(),
        ))
    }

    /// Push a chunk of captured PCM. Paced by the transport: resolves when
    /// the frame has been accepted, applying backpressure to the capture
    /// loop. Errors once the publication is gone.
    async fn capture_audio(&self, _frame: AudioFrame) -> Result<(), TransportError> {
        Err(TransportError::Unsupported(
            "this publication does not accept audio frames".into(),
        ))
    }

    /// Push a captured video frame. Delivery to the encoder is
    /// latest-frame-wins; there is no backpressure.
    fn capture_video(&self, _frame: VideoFrame) -> Result<(), TransportError> {
        Err(TransportError::Unsupported(
            "this publication does not accept video frames".into(),
        ))
    }

    /// Per-layer send statistics for a video publication: what is encoded,
    /// at which size, and which layers dynacast has paused.
    ///
    /// `None` for audio, before the first report, or from a transport that
    /// exposes no send counters; the default. See [`SendStats`].
    async fn send_stats(&self) -> Option<SendStats> {
        None
    }
}
