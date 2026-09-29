/*
 * Copyright 2026 Element Creations Ltd.
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
 * Please see LICENSE in the repository root for full details.
 */

// Public entry point for browsers and bundlers (Vite, webpack 5, Rollup).
//
// `initAsync()` with no argument resolves the wasm next to this module via
// `new URL(..., import.meta.url)`, which every modern bundler rewrites to the
// emitted asset. Pass a source to control it yourself (a CDN URL, a Vite
// `?url` import of `@element-hq/matrix-rtc/wasm`, prefetched bytes).
export * from "./generated/matrix_rtc.js";
export { isInitialized, type WasmSource } from "./init.js";
import { makeInitAsync } from "./init.js";

/** The URL of the wasm binary as this module sees it. */
export function defaultWasmUrl(): URL {
  return new URL("./generated/wasm-bindgen/index_bg.wasm", import.meta.url);
}

/** Load and initialise the bindings. Idempotent; concurrent callers share one load. */
export const initAsync = makeInitAsync(defaultWasmUrl);
