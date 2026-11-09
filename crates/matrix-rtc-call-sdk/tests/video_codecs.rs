// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Asks this platform's libwebrtc which video codecs it can encode. An
//! integration test rather than a unit test in `matrix-rtc-livekit` because
//! creating the PeerConnectionFactory needs this crate's `-ObjC` link fix on
//! Apple targets (see `build.rs`); it needs no SFU.

use matrix_rtc_call_sdk::supported_video_codecs;
use matrix_rtc_transport::VideoCodecPreference;

#[test]
fn every_platform_encodes_vp8() {
    let codecs = supported_video_codecs();
    println!("encodable video codecs: {codecs:?}");
    assert!(codecs.contains(&VideoCodecPreference::Vp8));
}
