/*
Copyright 2026 Element Creations Ltd.

SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
Please see LICENSE in the repository root for full details.
*/

import { existsSync } from "node:fs";
import { join } from "node:path";

/**
 * The CA the interop stack mints into `demo/backend/data/tls/` at up time, or
 * `undefined` if it is not there (the stack is down, or the certificate came
 * from mkcert, whose CA is already trusted machine-wide).
 */
export function devCaPath(): string | undefined {
  const path = join(__dirname, "..", "..", "demo", "backend", "data", "tls", "local-ca.crt");
  return existsSync(path) ? path : undefined;
}
