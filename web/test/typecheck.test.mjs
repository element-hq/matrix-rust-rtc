/*
Copyright 2026 Element Creations Ltd.

SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
Please see LICENSE in the repository root for full details.
*/

// Type-checks the generated declarations through a real consumer
// (types-smoke.ts). Guards the hand-written TS in the crate's ts_types.rs:
// a declaration that stops parsing, a vanished export, or a signature that
// regresses to `any`-shaped nonsense fails here.

import { describe, it, expect } from 'vitest';
import { execFileSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const smokeFile = fileURLToPath(new URL('./types-smoke.ts', import.meta.url));
const dts = fileURLToPath(new URL('../pkg/browser/matrix_rtc_wasm.d.ts', import.meta.url));

describe('generated TypeScript declarations', () => {
  it('type-check through a consumer', () => {
    if (!existsSync(dts)) {
      console.warn('pkg/browser not built; run `npm run build` first');
      return;
    }
    expect(() =>
      execFileSync(
        'npx',
        ['tsc', '--noEmit', '--strict', '--target', 'es2022', '--module', 'esnext',
         // esnext lib: wasm-bindgen classes declare `[Symbol.dispose]()`.
         '--moduleResolution', 'bundler', '--lib', 'esnext,dom', smokeFile],
        { stdio: 'pipe', encoding: 'utf8' },
      ),
    ).not.toThrow();
  }, 60_000);
});
