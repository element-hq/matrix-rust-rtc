// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Android initialisation for libwebrtc.
//!
//! libwebrtc requires the JVM before any peer connection can be created;
//! this `JNI_OnLoad` runs automatically when the host loads the library
//! (same pattern as livekit's own FFI). The AAR must also bundle
//! `libwebrtc.jar` — see `mobile/PACKAGING.md`.
//!
//! Only needed for media; audio/video *device* selection stays with the
//! platform (`AudioManager` etc.) — this SDK only moves raw frames.

use std::os::raw::c_void;

use jni::JavaVM;
use jni::sys::{JNI_VERSION_1_6, jint};

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "C" fn JNI_OnLoad(vm: JavaVM, _reserved: *mut c_void) -> jint {
    log::info!("JNI_OnLoad: initialising libwebrtc for Android");
    matrix_rtc_livekit::android::initialize_android(&vm);
    JNI_VERSION_1_6
}
