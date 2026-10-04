/*
Copyright 2026 Element Creations Ltd.

SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
Please see LICENSE in the repository root for full details.
*/

// The media roster end-to-end, without livekit: a fake transport delegate
// stands in for livekit-js, so what is under test is everything Rust owns —
// join, membership reconciliation, the multi-focus connection pool, the
// MSC4195 identities on roster entries, and stream state fed through the
// connection-event sink.

import { describe, it, expect } from 'vitest';
import { existsSync } from 'node:fs';
import { mockBackendHost, openSlotEvent, memberEventIn } from './mock-backend-host.mjs';

const nodeBindingUrl = new URL('../pkg/node/matrix_rtc_wasm.js', import.meta.url);

const ROOM_ID = '!call:example.org';
const APPLICATION_SLOT_ID = 'CALL';
const SLOT_ID = `m.call#${APPLICATION_SLOT_ID}`;
const USER_ID = '@me:example.org';
const DEVICE_ID = 'MYDEVICE';
const OWN_FOCUS = 'https://rtc.example.org/livekit/jwt';
const PEER_FOCUS = 'https://rtc.othersite.org/livekit/jwt';

// Stands in for the livekit-js half: resolves token fetches with a canned SFU
// token, records connects and hands their sinks back to the test, and counts
// closes and local key-index switches.
function fakeDelegate() {
  const log = {
    tokenRequests: [],
    connects: [],
    sinks: [],
    keysSet: [],
    localKeyIndexes: [],
    events: [],
    rosters: [],
    closed: 0,
  };
  const delegate = {
    fetchJson: (url, body) => {
      log.tokenRequests.push({ url, body });
      return Promise.resolve({
        status: 200,
        body: JSON.stringify({ jwt: 'the-jwt', url: 'wss://sfu.example.org' }),
      });
    },
    connect: (request, sink) => {
      log.connects.push(request);
      log.sinks.push(sink);
      return Promise.resolve({
        close: () => {
          log.closed += 1;
          return Promise.resolve();
        },
      });
    },
    setKey: (identity, index, key) => {
      log.keysSet.push({ identity, index, length: key.length });
      return Promise.resolve(true);
    },
    setLocalKeyIndex: (index) => {
      log.localKeyIndexes.push(index);
    },
    onEvent: (event) => {
      log.events.push(event);
    },
    onParticipants: (roster) => {
      log.rosters.push(roster);
    },
  };
  return { delegate, log };
}

// The engine's actor runs on the microtask queue; poll until it has caught up.
async function waitFor(probe, description, attempts = 100) {
  for (let i = 0; i < attempts; i++) {
    const value = probe();
    if (value) return value;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`timed out waiting for ${description}`);
}

describe('web media roster over a fake transport delegate', () => {
  it('reconciles memberships, foci, and sink events into the roster', async () => {
    if (!existsSync(nodeBindingUrl)) {
      console.warn('pkg/node not built; run `npm run build:node` first');
      return;
    }
    const bindings = await import(nodeBindingUrl);
    const host = mockBackendHost({
      userId: USER_ID,
      deviceId: DEVICE_ID,
      slots: [openSlotEvent(SLOT_ID)],
      members: [USER_ID, '@peer:example.org', '@far:othersite.org'],
    });
    const client = new bindings.WasmRtcClient(host);
    const room = await client.room(ROOM_ID, undefined);

    // Join, then deliver the sticky state as a server would echo it:
    // ourselves, plus one peer on our focus and one on a second focus.
    const call = await room.joinCall({
      application_slot_id: APPLICATION_SLOT_ID,
      transport: { kind: 'publish', livekit_service_url: OWN_FOCUS },
    });
    const memberId = call.memberId;
    host._sink(ROOM_ID).onStickyEvents([
      memberEventIn({ sender: USER_ID, deviceId: DEVICE_ID, memberId, slotId: SLOT_ID, focus: OWN_FOCUS }),
      memberEventIn({
        sender: '@peer:example.org',
        deviceId: 'PEERDEVICE',
        memberId: 'peer-member-1',
        slotId: SLOT_ID,
        focus: OWN_FOCUS,
      }),
      memberEventIn({
        sender: '@far:othersite.org',
        deviceId: 'FARDEVICE',
        memberId: 'far-member-1',
        slotId: SLOT_ID,
        focus: PEER_FOCUS,
      }),
    ]);

    const { delegate, log } = fakeDelegate();
    const session = await call.connectMedia({}, delegate);

    // The own focus was connected through the token exchange Rust built and
    // the delegate fetched. (No ordering with the pooled connect below: the
    // engine starts reconciling the moment it exists.)
    const ownConnect = log.connects.find((c) => c.connectionKey === OWN_FOCUS);
    expect(ownConnect).toEqual({
      connectionKey: OWN_FOCUS,
      sfuUrl: 'wss://sfu.example.org',
      jwt: 'the-jwt',
    });
    const ownTokenRequest = log.tokenRequests.find(
      (r) => r.url === `${OWN_FOCUS}/get_token`,
    );
    expect(ownTokenRequest.body.member.id).toBe(memberId);

    // The second focus is the engine's to open: one pooled connection for the
    // far member, keyed by its livekit_service_url.
    await waitFor(
      () => log.connects.some((c) => c.connectionKey === PEER_FOCUS),
      'the peer-focus connection',
    );
    expect(log.connects.length).toBe(2);

    // Roster: signalling truth, each entry carrying its MSC4195 identity.
    const roster = await waitFor(() => {
      const participants = session.participants();
      return participants.length === 3 ? participants : null;
    }, 'a three-entry roster');
    const me = roster.find((p) => p.member_id === memberId);
    const peer = roster.find((p) => p.member_id === 'peer-member-1');
    expect(me.is_local).toBe(true);
    expect(me.rtc_identity).toBe(session.ownRtcIdentity());
    expect(peer.is_local).toBe(false);
    expect(peer.reachable).toBe(true);
    // base64(SHA256(...)), unpadded: 43 chars.
    expect(peer.rtc_identity).toHaveLength(43);

    // Media arriving through the sink attaches to the signalled entry.
    const sink = log.sinks[0];
    sink.trackAdded(peer.rtc_identity, 'camera');
    let updated = await waitFor(() => {
      const participants = session.participants();
      const entry = participants.find((p) => p.member_id === 'peer-member-1');
      return entry.streams.length === 1 ? entry : null;
    }, 'the camera stream on the peer');
    expect(updated.streams[0]).toEqual({ kind: 'camera', muted: false });

    sink.trackMuted(peer.rtc_identity, 'camera', true);
    updated = await waitFor(() => {
      const participants = session.participants();
      const entry = participants.find((p) => p.member_id === 'peer-member-1');
      return entry.streams[0].muted ? entry : null;
    }, 'the muted camera stream');

    // The event callback reports the same transitions, and the roster
    // callback carried the same truth.
    const streamEvents = await waitFor(() => {
      const seen = log.events.filter(
        (event) => event.type === 'stream_started' || event.type === 'stream_muted',
      );
      return seen.length >= 2 ? seen : null;
    }, 'the stream events to reach onEvent');
    expect(streamEvents[0]).toEqual({
      type: 'stream_started',
      member_id: 'peer-member-1',
      kind: 'camera',
    });
    expect(streamEvents[1].type).toBe('stream_muted');
    expect(log.rosters.length).toBeGreaterThan(0);

    // A transport-level leave is diagnostics, never roster truth.
    sink.remoteLeft(peer.rtc_identity);
    expect(session.participants().length).toBe(3);

    // Disconnect closes the own focus through the delegate; pooled peer-focus
    // connections close inside the engine.
    await session.disconnect();
    expect(log.closed).toBeGreaterThanOrEqual(1);
  }, 20000);
});
