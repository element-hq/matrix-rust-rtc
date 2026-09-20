// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Apple-only linker fix for the static `libwebrtc` that `livekit` pulls in.
//!
//! WebRTC ships helpers such as `+[NSString stringForAbslStringView:]` as
//! Objective-C *categories*. Category methods live in object files that the
//! linker dead-strips from a static archive unless the whole member is loaded,
//! so at runtime libwebrtc hits "unrecognized selector" and aborts while
//! building its `PeerConnectionFactory`. `-ObjC` forces the linker to load every
//! archive member that defines an Objective-C class or category.
//!
//! `webrtc-sys` can't do this itself: a dependency's build script cannot inject
//! link args into a downstream binary — only the crate that owns the
//! binary/example/test targets can. Hence this lives here.

fn main() {
    // Examples and tests only: this crate ships no bin target (emitting
    // `rustc-link-arg-bins` without one is a hard error). Both kinds of
    // runnable artifact create libwebrtc's PeerConnectionFactory — the
    // `connect` example and the `e2e_call` integration test.
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "macos" || target_os == "ios" {
        println!("cargo:rustc-link-arg-examples=-ObjC");
        println!("cargo:rustc-link-arg-tests=-ObjC");
    }
}
