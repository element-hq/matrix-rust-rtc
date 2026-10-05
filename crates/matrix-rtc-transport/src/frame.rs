// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Owned, transport-neutral media frames.
//!
//! Frames are converted to these types at the transport boundary so they can
//! cross task and FFI boundaries without borrowing transport-internal buffers.
//! Video is normalized to I420 — one pixel format across the whole surface;
//! passthrough of other layouts (NV12, textures) is a later optimization.

/// A chunk of interleaved 16-bit PCM audio.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AudioFrame {
    /// Interleaved samples, `samples_per_channel * num_channels` long.
    pub data: Vec<i16>,
    /// Sample rate in Hz (typically 48000).
    pub sample_rate: u32,
    /// Number of interleaved channels.
    pub num_channels: u32,
    /// Samples per channel in `data`.
    pub samples_per_channel: u32,
}

/// Display rotation to apply to a video frame before rendering.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum VideoRotation {
    #[default]
    Deg0,
    Deg90,
    Deg180,
    Deg270,
}

/// A planar I420 (YUV 4:2:0) pixel buffer.
///
/// Strides may exceed the visible width; consumers must honour them. The
/// chroma planes are `(width + 1) / 2` by `(height + 1) / 2`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct I420Buffer {
    pub width: u32,
    pub height: u32,
    pub data_y: Vec<u8>,
    pub stride_y: u32,
    pub data_u: Vec<u8>,
    pub stride_u: u32,
    pub data_v: Vec<u8>,
    pub stride_v: u32,
}

/// A decoded video frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VideoFrame {
    pub buffer: I420Buffer,
    pub rotation: VideoRotation,
    /// Capture timestamp in microseconds, as reported by the transport.
    pub timestamp_us: i64,
}
