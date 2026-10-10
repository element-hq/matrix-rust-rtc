// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Receive-side statistics for a subscribed remote track, and send-side
//! statistics for each encoded layer of a local video publication.
//!
//! These exist to make one specific failure diagnosable from outside the
//! library. The receive path produces frames at a fixed cadence whether or not
//! RTP is arriving — an audio track with no incoming packets still emits 10 ms
//! buffers, filled by the jitter buffer's concealment (silence). So "the call
//! is silent" and "the call is silent *because nothing is arriving*" look
//! identical at the frame level, and telling them apart used to mean reading
//! this crate's own log output.
//!
//! The counters below are cumulative since subscription and come straight from
//! the transport's RTP layer, so a host can distinguish:
//!
//! - **Nothing arriving** — [`ReceiveStats::packets_received`] flat across two
//!   samples. Network, subscription, or SFU-side problem.
//! - **Arriving but not decrypting** — packets climbing while
//!   [`ReceiveStats::frames_decoded`] (video) stays flat, or
//!   [`ReceiveStats::concealed_samples`] climbs in step with
//!   [`ReceiveStats::total_samples_received`] (audio). Usually a key problem,
//!   corroborated by [`ConnectionEvent::EncryptionStateChanged`].
//! - **Arriving and decoding, but lossy** — packets and frames both climbing
//!   with [`ReceiveStats::packets_lost`] or `jitter` rising.
//!
//! Sample twice and compare: every field is a monotonic total, not a rate.
//!
//! On the send side, [`SendStats`] lists one entry per simulcast layer, so a
//! publisher can see what it encodes: each layer's size and frame rate, whether
//! the SFU still wants it ([`SendLayerStats::active`] — dynacast pauses a layer
//! no subscriber asks for), and why the encoder is cutting quality.
//!
//! [`ConnectionEvent::EncryptionStateChanged`]: crate::ConnectionEvent::EncryptionStateChanged

/// Cumulative receive-side counters for one subscribed track.
///
/// Fields that don't apply to the track's media kind stay `0` (a host reading
/// `concealed_samples` on video learns nothing). Transports report what their
/// RTP layer exposes; see the module docs for how to read them.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReceiveStats {
    /// RTP packets received on this track since subscribing. Flat across two
    /// samples means nothing is arriving at all.
    pub packets_received: u64,
    /// Packets the receiver expected and never got. Signed: reordering can
    /// briefly make it negative.
    pub packets_lost: i64,
    /// Payload bytes received.
    pub bytes_received: u64,
    /// Packet-arrival jitter in seconds.
    pub jitter: f64,
    /// Video frames the decoder produced. Flat while `packets_received`
    /// climbs is the signature of frames arriving but not decrypting.
    pub frames_decoded: u64,
    /// Video frames dropped before rendering (late, or the consumer is slow).
    pub frames_dropped: u64,
    /// Audio samples handed to the output, whether real or concealed.
    pub total_samples_received: u64,
    /// Audio samples the jitter buffer invented because the real ones never
    /// arrived. Climbing in step with `total_samples_received` means the
    /// "audio" being played is entirely fabricated.
    pub concealed_samples: u64,
    /// The subset of `concealed_samples` that was emitted as pure silence
    /// rather than interpolated from neighbouring audio.
    pub silent_concealed_samples: u64,
    /// How many separate times concealment kicked in — a better gap counter
    /// than the sample totals, which one long outage inflates.
    pub concealment_events: u64,
}

/// Why the encoder is currently sending less than it was asked to.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum QualityLimitation {
    #[default]
    None,
    /// The device cannot encode fast enough.
    Cpu,
    /// The network estimate is too low for the requested bitrate.
    Bandwidth,
    Other,
}

/// One encoded layer of a local video publication.
///
/// Counters (`frames_encoded`, `bytes_sent`) are cumulative since publishing;
/// sizes and frame rate are the encoder's current output.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SendLayerStats {
    /// The simulcast layer id (`q`, `h`, `f` on LiveKit), empty for a
    /// single-layer publication.
    pub rid: String,
    pub frame_width: u32,
    pub frame_height: u32,
    pub frames_per_second: f64,
    pub frames_encoded: u64,
    /// Payload bytes sent on this layer.
    pub bytes_sent: u64,
    /// Whether the layer is being encoded. `false` once the SFU reports that
    /// no subscriber wants it (dynacast); flat `frames_encoded` agrees.
    pub active: bool,
    pub quality_limitation: QualityLimitation,
}

/// Send-side statistics for a local video publication, one entry per encoded
/// layer, smallest first.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SendStats {
    pub layers: Vec<SendLayerStats>,
}
