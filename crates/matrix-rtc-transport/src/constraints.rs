// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Per-participant, per-stream subscription constraints.
//!
//! Applications describe *how they consume* a stream — is it rendered, at
//! what size, under what bandwidth budget — and transports translate the
//! resolved form into their own control surface. The declarative shape is
//! deliberately transport-free so a P2P or WebTransport backend can interpret
//! the same values with its own signalling.
//!
//! The call engine (`matrix-rtc-media`) stores constraints and hands
//! transports only the folded [`ResolvedConstraints`]; transports interpret,
//! they never police. Whatever is asked for is a *cap*: transports and
//! servers still adapt downwards under congestion on their own.
//!
//! Two design points, learned from how SFUs actually behave:
//!
//! - **Not rendering has two strengths.** A stream that is merely
//!   off-screen ([`MediaConstraints::visible`] `= false`) is *paused* —
//!   still subscribed, no data flowing, resumes instantly when scrolled
//!   back. A stream the app does not want at all
//!   ([`MediaConstraints::enabled`] `= false`) may be released as fully as
//!   the transport supports — ideally unsubscribed (bandwidth **and**
//!   decoder freed, resume renegotiates), at minimum paused. The
//!   distinction is in the model so transports can honour it; large calls
//!   need it — a 20-tile grid cannot hold 20 warm video decoders on a
//!   phone.
//! - **Size beats quality as a hint.** The renderer knows its surface size
//!   in pixels; it does not know the publisher's simulcast layer ladder.
//!   [`VideoDetail::Dimensions`] lets the server pick the closest layer and
//!   is the preferred control (LiveKit has deprecated the quality field in
//!   its wire protocol in favour of dimensions). The two are mutually
//!   exclusive by construction.

use crate::stream::MediaStreamKind;

/// A rendered size in pixels.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Dimensions {
    pub width: u32,
    pub height: u32,
}

/// Coarse cap on the quality layer to receive, for callers that don't know
/// their render size. Prefer [`VideoDetail::Dimensions`] when the size is
/// known.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum QualityLimit {
    Low,
    Medium,
    High,
}

/// How much detail to receive for a video stream. The variants are mutually
/// exclusive — a size hint and a quality cap cannot both be in force.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum VideoDetail {
    /// No preference: the transport/server delivers the best it can afford.
    #[default]
    Auto,
    /// The size of the surface this stream is rendered into; the transport
    /// picks the smallest sufficient layer. Preferred.
    Dimensions(Dimensions),
    /// An explicit layer cap, when no render size is known.
    Quality(QualityLimit),
}

/// What the application wants from one stream of one participant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MediaConstraints {
    /// Hard switch. `false` releases the stream as fully as the transport
    /// supports (ideally a full unsubscribe: bandwidth and decoder freed,
    /// resume renegotiates). Use for tiles the user closed, not for
    /// scroll-by invisibility.
    pub enabled: bool,
    /// Whether the stream is currently rendered on screen. `false` pauses
    /// the stream without unsubscribing, so it resumes instantly.
    pub visible: bool,
    /// How much detail to receive (video kinds only).
    pub detail: VideoDetail,
}

impl Default for MediaConstraints {
    fn default() -> Self {
        Self {
            enabled: true,
            visible: true,
            detail: VideoDetail::Auto,
        }
    }
}

/// The demand state of one stream after folding the constraint fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamDemand {
    /// Subscribed, data flowing.
    Active,
    /// Subscribed but paused server-side: no data, instant resume.
    Paused,
    /// Released as fully as the transport supports: ideally unsubscribed
    /// (no bandwidth, no decoder, resume renegotiates), at minimum paused.
    Off,
}

/// The folded form handed to transports: one effective setting with the
/// interactions between the [`MediaConstraints`] fields already resolved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResolvedConstraints {
    pub demand: StreamDemand,
    /// Detail preference; always [`VideoDetail::Auto`] for audio kinds.
    pub detail: VideoDetail,
}

impl MediaConstraints {
    /// Fold the constraint fields into the effective per-stream setting.
    ///
    /// `enabled = false` wins over everything (full unsubscribe);
    /// invisibility pauses.
    pub fn resolve(&self, kind: MediaStreamKind) -> ResolvedConstraints {
        let is_video = kind.is_video();
        let demand = if !self.enabled {
            StreamDemand::Off
        } else if !self.visible {
            StreamDemand::Paused
        } else {
            StreamDemand::Active
        };
        ResolvedConstraints {
            demand,
            detail: if is_video {
                self.detail
            } else {
                VideoDetail::Auto
            },
        }
    }
}

impl ResolvedConstraints {
    /// The call-wide receive switch over a stream's own setting: while video
    /// is not received, a video stream that would be active is paused. `Off`
    /// stays `Off`, and audio kinds are never touched. Applied after
    /// [`MediaConstraints::resolve`] rather than folded into it, so each
    /// stream keeps its own value to return to when video is received again.
    pub fn receiving(self, kind: MediaStreamKind, receive_video: bool) -> Self {
        if receive_video || !kind.is_video() || self.demand != StreamDemand::Active {
            return self;
        }
        Self {
            demand: StreamDemand::Paused,
            ..self
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_keep_streams_active() {
        let resolved = MediaConstraints::default().resolve(MediaStreamKind::Camera);
        assert_eq!(resolved.demand, StreamDemand::Active);
        assert_eq!(resolved.detail, VideoDetail::Auto);
    }

    #[test]
    fn disabled_turns_the_stream_off_and_wins_over_everything() {
        let constraints = MediaConstraints {
            enabled: false,
            visible: true,
            detail: VideoDetail::Quality(QualityLimit::High),
        };
        assert_eq!(
            constraints.resolve(MediaStreamKind::Camera).demand,
            StreamDemand::Off
        );
        assert_eq!(
            constraints.resolve(MediaStreamKind::Microphone).demand,
            StreamDemand::Off
        );
    }

    #[test]
    fn invisible_pauses_instead_of_unsubscribing() {
        let constraints = MediaConstraints {
            visible: false,
            ..Default::default()
        };
        assert_eq!(
            constraints.resolve(MediaStreamKind::Camera).demand,
            StreamDemand::Paused
        );
        assert_eq!(
            constraints.resolve(MediaStreamKind::Microphone).demand,
            StreamDemand::Paused
        );
    }

    #[test]
    fn not_receiving_video_pauses_video_but_keeps_audio() {
        let active = MediaConstraints::default();
        for kind in [MediaStreamKind::Camera, MediaStreamKind::ScreenShare] {
            assert_eq!(
                active.resolve(kind).receiving(kind, false).demand,
                StreamDemand::Paused
            );
            assert_eq!(
                active.resolve(kind).receiving(kind, true).demand,
                StreamDemand::Active
            );
        }
        for kind in [
            MediaStreamKind::Microphone,
            MediaStreamKind::ScreenShareAudio,
        ] {
            assert_eq!(
                active.resolve(kind).receiving(kind, false).demand,
                StreamDemand::Active
            );
        }
    }

    #[test]
    fn not_receiving_video_keeps_off_and_detail() {
        let released = MediaConstraints {
            enabled: false,
            ..Default::default()
        };
        assert_eq!(
            released
                .resolve(MediaStreamKind::Camera)
                .receiving(MediaStreamKind::Camera, false)
                .demand,
            StreamDemand::Off
        );
        let sized = MediaConstraints {
            detail: VideoDetail::Quality(QualityLimit::Low),
            ..Default::default()
        };
        assert_eq!(
            sized
                .resolve(MediaStreamKind::Camera)
                .receiving(MediaStreamKind::Camera, false)
                .detail,
            VideoDetail::Quality(QualityLimit::Low)
        );
    }

    #[test]
    fn detail_only_applies_to_video_kinds() {
        let constraints = MediaConstraints {
            detail: VideoDetail::Dimensions(Dimensions {
                width: 320,
                height: 180,
            }),
            ..Default::default()
        };
        assert_eq!(
            constraints.resolve(MediaStreamKind::Camera).detail,
            VideoDetail::Dimensions(Dimensions {
                width: 320,
                height: 180,
            })
        );
        assert_eq!(
            constraints.resolve(MediaStreamKind::Microphone).detail,
            VideoDetail::Auto
        );
    }
}
