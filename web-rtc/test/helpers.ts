/*
 * Copyright 2026 Element Creations Ltd.
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
 * Please see LICENSE in the repository root for full details.
 */

// Shared fixtures for the wasm acceptance suites.
import { FfiRtcTransport, FfiTransportIntent, RtcSessionManager, type FfiJoinParams } from "@element-hq/matrix-rtc";
import { LK_SERVICE_URL, MockHost, OWN_DEVICE_ID, OWN_USER_ID, ROOM_ID, SLOT_ID } from "@element-hq/matrix-rtc/testing";

export async function newParticipation(opts: { encrypted?: boolean; slotOpen?: boolean } = {}) {
  const host = new MockHost();
  host.encrypted = opts.encrypted ?? false;
  host.slotEncrypted = opts.encrypted ?? false;
  host.slotOpen = opts.slotOpen ?? true;
  const manager = new RtcSessionManager(host);
  await host.attach(manager);
  const participation = manager.participation(ROOM_ID, SLOT_ID, OWN_USER_ID, OWN_DEVICE_ID);
  return { host, manager, participation };
}

export const joinParams: FfiJoinParams = {
  application: "m.call",
  memberId: undefined,
  keepAliveTimeoutMs: 15_000n,
  stickyDurationMs: 240_000n,
  degradedLifetimeMs: undefined,
  encryption: undefined,
};

export const receiveOnly = () => new FfiTransportIntent.ReceiveOnly({ canSubscribe: ["livekit"] });
export const publishLk = (url = LK_SERVICE_URL) =>
  new FfiTransportIntent.Publish({ transport: new FfiRtcTransport.LiveKit({ livekitServiceUrl: url }) });

/** The service URL of a transport, or `undefined` for an unsupported one. */
export const serviceUrl = (t: FfiRtcTransport): string | undefined =>
  FfiRtcTransport.LiveKit.instanceOf(t) ? t.inner.livekitServiceUrl : undefined;
