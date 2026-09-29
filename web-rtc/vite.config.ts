/*
 * Copyright 2026 Element Creations Ltd.
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
 * Please see LICENSE in the repository root for full details.
 */

import { defineConfig } from "vite";

export default defineConfig({
  // the generated entrypoint imports the .wasm as an asset URL
  assetsInclude: ["**/*.wasm"],
});
