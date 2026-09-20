/*
Copyright 2026 Element Creations Ltd.

SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
Please see LICENSE in the repository root for full details.
*/

import { describe, it, expect } from 'vitest';
import { existsSync } from 'node:fs';

const browserBindingUrl = new URL('../pkg/browser/matrix_rtc_wasm.js', import.meta.url);

describe('generated bindings', () => {
  it('browser binding surface is available after build', async () => {
    if (!existsSync(browserBindingUrl)) {
      // Skip if bindings haven't been built
      return;
    }

    const mod = await import(browserBindingUrl.href);

    expect(mod).toBeDefined();
    expect(typeof mod.WasmRtcSession).toBe('function');
    expect(typeof mod.WasmRtcSessionManager).toBe('function');
    expect(typeof mod.WasmMembershipSnapshotSubscription).toBe('function');
  });
});
