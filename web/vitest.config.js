/*
Copyright 2026 Element Creations Ltd.

SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
Please see LICENSE in the repository root for full details.
*/

import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    // Use node environment since we're testing Node.js bindings
    environment: 'node',
    // Global setup for tests
    globals: true,
    // Include test files
    include: ['test/**/*.test.mjs', 'test/**/*.test.js'],
    // Timeout for async tests
    testTimeout: 10000,
  },
});
