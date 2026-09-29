/*
 * Copyright 2026 Element Creations Ltd.
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
 * Please see LICENSE in the repository root for full details.
 */

// Loads the published build (`dist/`) for the acceptance suites, through the
// package's own Node entry point. Warnings and errors only: the crates are
// chatty at info while a test runs.
import { FfiLogLevel, initAsync } from "@element-hq/matrix-rtc";
import { installConsoleLogSink } from "@element-hq/matrix-rtc/log-sink";

let sinkInstalled = false;

export async function initWasm(): Promise<void> {
  await initAsync();
  if (!sinkInstalled) {
    installConsoleLogSink(FfiLogLevel.Warn);
    sinkInstalled = true;
  }
}
