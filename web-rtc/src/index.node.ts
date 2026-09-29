/*
 * Copyright 2026 Element Creations Ltd.
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
 * Please see LICENSE in the repository root for full details.
 */

// Entry point selected by the `node` export condition (Node ≥ 20, vitest,
// bots). Same API as `index.ts`; the only difference is that the default
// source reads the wasm from disk, because Node's `fetch` does not serve
// `file:` URLs.
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
export * from "./generated/matrix_rtc.js";
export { isInitialized, type WasmSource } from "./init.js";
import { makeInitAsync } from "./init.js";

/** The URL of the wasm binary as this module sees it. */
export function defaultWasmUrl(): URL {
  return new URL("./generated/wasm-bindgen/index_bg.wasm", import.meta.url);
}

/** Load and initialise the bindings. Idempotent; concurrent callers share one load. */
export const initAsync = makeInitAsync(async () => {
  const url = defaultWasmUrl();
  return url.protocol === "file:" ? await readFile(fileURLToPath(url)) : url;
});
