/*
Copyright 2026 Element Creations Ltd.

SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
Please see LICENSE in the repository root for full details.
*/

import { defineConfig } from 'vite';

export default defineConfig({
  // Both wasm packages load their .wasm relative to their own JS via
  // `new URL(..., import.meta.url)`; esbuild prebundling would relocate the
  // JS away from the wasm and break that.
  optimizeDeps: {
    exclude: ['matrix-rtc-wasm', '@matrix-org/matrix-sdk-crypto-wasm'],
  },
  // Dev server only (build/preview bundle the wasm as assets): the bindings
  // live in the parent package (`web/pkg/browser`), outside this package's
  // workspace root, and vite's filesystem fence 403s the `/@fs/` fetch for
  // them unless the parent is allowed.
  server: {
    fs: { allow: ['..'] },
  },
  build: {
    // Top-level await appears in matrix-sdk-crypto-wasm's module glue.
    target: 'es2022',
  },
  worker: {
    format: 'es',
  },
});
