/*
 * Copyright 2026 Element Creations Ltd.
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
 * Please see LICENSE in the repository root for full details.
 */

// The crates log through Rust's `log` facade and have no output of their
// own: a host installs a LogSink. This one writes to the console, prefixed
// with the Rust module the line came from.
import { FfiLogLevel, type LogSink, setLogSink } from "./generated/matrix_rtc.js";

export class ConsoleLogSink implements LogSink {
  log(level: FfiLogLevel, target: string, message: string): void {
    const line = `[matrix-rtc ${target}] ${message}`;
    switch (level) {
      case FfiLogLevel.Error:
        console.error(line);
        break;
      case FfiLogLevel.Warn:
        console.warn(line);
        break;
      case FfiLogLevel.Info:
        console.info(line);
        break;
      default:
        console.debug(line);
    }
  }
}

/** Route the crates' log lines to the console, at `maxLevel` and above. */
export function installConsoleLogSink(maxLevel = FfiLogLevel.Debug): void {
  setLogSink(new ConsoleLogSink(), maxLevel);
}
