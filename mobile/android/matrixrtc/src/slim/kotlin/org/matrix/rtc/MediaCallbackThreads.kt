// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

package org.matrix.rtc

import com.sun.jna.CallbackThreadInitializer

/**
 * Signalling-only build: there are no media callback interfaces to pin.
 *
 * The media twin of this file is where a callback interface generated only
 * with the `media` feature would be pinned. Splitting the two by source dir
 * keeps a reference to a type this variant lacks a compile error instead of a
 * reflective lookup that silently finds nothing — and what this pinning
 * prevents is a process abort, so failing quietly is the wrong failure.
 */
@Suppress("UNUSED_PARAMETER")
internal fun pinMediaCallbackThreads(initializer: CallbackThreadInitializer) = Unit
