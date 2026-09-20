// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

package org.matrix.rtc

import com.sun.jna.CallbackThreadInitializer

/**
 * Media build: pin the callback interfaces that only the `media` feature's
 * bindings generate.
 *
 * `OpenIdTokenProvider` is declared in `crates/matrix-rtc-ffi/src/media/`, so
 * uniffi emits no `uniffiCallbackInterfaceOpenIdTokenProvider` for the slim
 * artifact and naming it from shared code fails that build. The slim twin of
 * this file is a no-op; see [MatrixRtc.initialize] for why any of it happens.
 */
internal fun pinMediaCallbackThreads(initializer: CallbackThreadInitializer) {
    pinVTableCallbacks(uniffiCallbackInterfaceOpenIdTokenProvider.vtable, initializer)
}
