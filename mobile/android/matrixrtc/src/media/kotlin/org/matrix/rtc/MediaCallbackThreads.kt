// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

package org.matrix.rtc

import com.sun.jna.CallbackThreadInitializer

/**
 * Media build: nothing media-only to pin today.
 *
 * The host's `MatrixBackend` (tokens included) is part of the slim surface and
 * is pinned by [MatrixRtc.initialize]. The variant split stays so a callback
 * interface the `media` feature alone generates can be pinned here without
 * naming it from shared code, which would fail the slim build.
 */
@Suppress("UNUSED_PARAMETER")
internal fun pinMediaCallbackThreads(initializer: CallbackThreadInitializer) = Unit
