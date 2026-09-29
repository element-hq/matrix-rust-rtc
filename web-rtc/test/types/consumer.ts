/*
 * Copyright 2026 Element Creations Ltd.
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
 * Please see LICENSE in the repository root for full details.
 */

// Compiled with `skipLibCheck: false` against dist/ — proves the shipped
// .d.ts files are valid TypeScript and that every subpath export types.
import { initAsync, isInitialized, defaultWasmUrl, heartbeatIntervalMs, RtcSessionManager } from "@element-hq/matrix-rtc";
import { installConsoleLogSink, ConsoleLogSink } from "@element-hq/matrix-rtc/log-sink";
import { MockHost, ROOM_ID, SLOT_ID, OWN_USER_ID, OWN_DEVICE_ID } from "@element-hq/matrix-rtc/testing";

export async function smoke(): Promise<bigint> {
  await initAsync(defaultWasmUrl());
  if (!isInitialized()) throw new Error("not initialised");
  installConsoleLogSink();
  const sink: ConsoleLogSink = new ConsoleLogSink();
  void sink;
  const host: MockHost = new MockHost();
  const manager = new RtcSessionManager(host);
  await host.attach(manager);
  const participation = manager.participation(ROOM_ID, SLOT_ID, OWN_USER_ID, OWN_DEVICE_ID);
  void (await participation.status());
  void (await participation.memberships());
  return heartbeatIntervalMs();
}
