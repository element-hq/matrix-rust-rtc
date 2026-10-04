/*
Copyright 2026 Element Creations Ltd.

SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
Please see LICENSE in the repository root for full details.
*/

import { describe, it, expect, beforeEach } from 'vitest';
import { existsSync } from 'node:fs';
import { mockBackendHost, openSlotEvent } from './mock-backend-host.mjs';

const nodeBindingUrl = new URL('../pkg/node/matrix_rtc_wasm.js', import.meta.url);

const ROOM_ID = '!RhkzuEOlOxpckXJkhY:synapse.m.localhost';
const SLOT_ID = 'm.call#ROOM';
const ME = '@me:synapse.m.localhost';
const BOB = '@bob:synapse.othersite.m.localhost';
const ALICE = '@alice:synapse.m.localhost';

// Real-world events as a host hands them over: content verbatim, decryption
// information as the client reported it.
function bobJoinEvent() {
  return {
    event_id: '$_ErcrEWx3Hj77_wScF-U4e9aS6cVi37RvFUeq12BiaI',
    event_type: 'org.matrix.msc4143.rtc.member',
    sender: BOB,
    origin_server_ts: 1,
    content: {
      application: { type: 'm.call', 'm.call.intent': 'video' },
      slot_id: SLOT_ID,
      transports: {
        published: [
          { type: 'livekit', livekit_service_url: 'https://matrix-rtc.othersite.m.localhost/livekit/jwt' },
        ],
        can_subscribe: ['livekit'],
      },
      member: { id: 'bcab799f-abae-4d38-bf1b-77238346349a', membership: 'join' },
      msc4354_sticky_key: 'bcab799f-abae-4d38-bf1b-77238346349a',
      sticky_key: 'bcab799f-abae-4d38-bf1b-77238346349a',
    },
    encryption: { kind: 'encrypted', sender_device_id: 'WDQHAPEYDK' },
  };
}

function aliceJoinEvent() {
  return {
    event_id: '$imqekRtWGLcITMI6YuMF0xgpT4S8LMr78eseonO2_Nw',
    event_type: 'org.matrix.msc4143.rtc.member',
    sender: ALICE,
    origin_server_ts: 2,
    content: {
      application: { type: 'm.call', 'm.call.intent': 'video' },
      slot_id: SLOT_ID,
      transports: {
        published: [{ type: 'livekit', livekit_service_url: 'https://matrix-rtc.m.localhost/livekit/jwt' }],
        can_subscribe: ['livekit'],
      },
      member: { id: 'd50437bd-424a-498d-912f-b0f1d2ba7f18', membership: 'join' },
      msc4354_sticky_key: 'd50437bd-424a-498d-912f-b0f1d2ba7f18',
      sticky_key: 'd50437bd-424a-498d-912f-b0f1d2ba7f18',
    },
    encryption: { kind: 'encrypted', sender_device_id: 'VJHNJJCVOA' },
  };
}

function aliceLeaveEvent() {
  return {
    event_id: '$4cN54YJqNgWjtv1g0U1kx_bRhu_kfGV5_6qIAzxUXMA',
    event_type: 'org.matrix.msc4143.rtc.member',
    sender: ALICE,
    origin_server_ts: 3,
    content: {
      slot_id: SLOT_ID,
      member: { id: 'd50437bd-424a-498d-912f-b0f1d2ba7f18', membership: 'leave' },
      leave_reason: { code: 'leave' },
      msc4354_sticky_key: 'd50437bd-424a-498d-912f-b0f1d2ba7f18',
      sticky_key: 'd50437bd-424a-498d-912f-b0f1d2ba7f18',
    },
    encryption: { kind: 'cleartext' },
  };
}

// The feeder applies sets from the sink asynchronously; poll until it has.
async function waitFor(probe, description, attempts = 100) {
  for (let i = 0; i < attempts; i++) {
    if (await probe()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`timed out waiting for ${description}`);
}

describe('RTC manager with real-world data', () => {
  let bindings;

  beforeEach(async () => {
    if (!existsSync(nodeBindingUrl)) {
      return;
    }
    bindings = await import(nodeBindingUrl.href);
  });

  async function attached(sticky) {
    const host = mockBackendHost({
      userId: ME,
      slots: [openSlotEvent(SLOT_ID)],
      members: [ME, BOB, ALICE],
      sticky,
    });
    const manager = new bindings.WasmRtcSessionManager(host);
    await manager.attachRoom(ROOM_ID, undefined);
    return { host, manager };
  }

  // Sticky state is replace-not-merge: every set delivered to the sink carries
  // the room's complete membership, and absence is departure.
  it('ingests realistic sticky events and updates member count', async () => {
    const { host, manager } = await attached([bobJoinEvent()]);
    expect(await manager.session_count()).toBe(1);
    expect(await manager.member_count(ROOM_ID, SLOT_ID)).toBe(1);

    host._sink(ROOM_ID).onStickyEvents([bobJoinEvent(), aliceJoinEvent()]);
    await waitFor(async () => (await manager.member_count(ROOM_ID, SLOT_ID)) === 2, 'alice to join');
    expect(await manager.session_count()).toBe(1);

    host._sink(ROOM_ID).onStickyEvents([bobJoinEvent(), aliceLeaveEvent()]);
    await waitFor(async () => (await manager.member_count(ROOM_ID, SLOT_ID)) === 1, 'alice to leave');
  });

  it('handles unknown transport types as unsupported', async () => {
    const eventWithUnknownTransport = {
      event_id: '$test',
      event_type: 'org.matrix.msc4143.rtc.member',
      sender: '@test:example.org',
      origin_server_ts: 4,
      content: {
        application: { type: 'm.call' },
        slot_id: SLOT_ID,
        transports: { published: [{ type: 'unknown_transport' }], can_subscribe: ['unknown_transport'] },
        member: { id: 'test-id', membership: 'join' },
        sticky_key: 'test-id',
      },
      encryption: { kind: 'cleartext' },
    };
    const { manager } = await attached([eventWithUnknownTransport]);
    expect(await manager.session_count()).toBe(1);
  });
});
