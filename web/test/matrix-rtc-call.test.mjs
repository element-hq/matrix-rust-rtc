/*
Copyright 2026 Element Creations Ltd.

SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
Please see LICENSE in the repository root for full details.
*/

// MatrixRtcCall over a mocked livekit-client module and the real wasm
// bindings: the RoomEvent translation table, the roster join by
// rtc_identity, and the delegate wiring (token fetch, key provider, close).

import { describe, it, expect } from 'vitest';
import { existsSync } from 'node:fs';
import { mockBackendHost, openSlotEvent, memberEventIn } from './mock-backend-host.mjs';
import { MatrixRtcCall } from '../src/matrix-rtc-call.mjs';

const nodeBindingUrl = new URL('../pkg/node/matrix_rtc_wasm.js', import.meta.url);

const ROOM_ID = '!call:example.org';
const APPLICATION_SLOT_ID = 'CALL';
const SLOT_ID = `m.call#${APPLICATION_SLOT_ID}`;
const USER_ID = '@me:example.org';
const DEVICE_ID = 'MYDEVICE';
const OWN_FOCUS = 'https://rtc.example.org/livekit/jwt';

// --- a livekit-client stand-in -------------------------------------------

const RoomEvent = {
  ParticipantConnected: 'participantConnected',
  ParticipantDisconnected: 'participantDisconnected',
  TrackSubscribed: 'trackSubscribed',
  TrackUnsubscribed: 'trackUnsubscribed',
  LocalTrackPublished: 'localTrackPublished',
  LocalTrackUnpublished: 'localTrackUnpublished',
  TrackMuted: 'trackMuted',
  TrackUnmuted: 'trackUnmuted',
  ActiveSpeakersChanged: 'activeSpeakersChanged',
  Reconnecting: 'reconnecting',
  Reconnected: 'reconnected',
  Disconnected: 'disconnected',
};

class FakeParticipant {
  constructor(identity) {
    this.identity = identity;
  }
}

function fakeLivekit() {
  const instances = [];
  class FakeRoom {
    constructor(options) {
      this.options = options;
      this.handlers = new Map();
      this.remotes = new Map();
      this.localParticipant = new FakeParticipant('local-unset');
      this.disconnected = false;
      instances.push(this);
    }
    on(event, handler) {
      this.handlers.set(event, handler);
      return this;
    }
    async connect(url, token) {
      this.url = url;
      this.token = token;
    }
    async disconnect() {
      this.disconnected = true;
    }
    getParticipantByIdentity(identity) {
      return this.remotes.get(identity);
    }
    emit(event, ...args) {
      this.handlers.get(event)?.(...args);
    }
  }
  class BaseKeyProvider {
    constructor(options) {
      this.options = options;
      this.set = [];
    }
    onSetEncryptionKey(cryptoKey, identity, index) {
      this.set.push({ cryptoKey, identity, index });
    }
  }
  const createKeyMaterialFromBuffer = async (buffer) => ({ fakeKeyFor: buffer.byteLength });
  return {
    module: { Room: FakeRoom, RoomEvent, BaseKeyProvider, createKeyMaterialFromBuffer },
    instances,
  };
}

async function waitFor(probe, description, attempts = 100) {
  for (let i = 0; i < attempts; i++) {
    const value = probe();
    if (value) return value;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`timed out waiting for ${description}`);
}

describe('MatrixRtcCall over a mocked livekit-client', () => {
  it('joins roster entries to livekit participants and translates room events', async () => {
    if (!existsSync(nodeBindingUrl)) {
      console.warn('pkg/node not built; run `npm run build` first');
      return;
    }
    const bindings = await import(nodeBindingUrl);
    const host = mockBackendHost({
      userId: USER_ID,
      deviceId: DEVICE_ID,
      slots: [openSlotEvent(SLOT_ID)],
      members: [USER_ID, '@peer:example.org'],
    });
    const client = new bindings.WasmRtcClient(host);
    const rtcRoom = await client.room(ROOM_ID, undefined);

    const rtcCall = await rtcRoom.joinCall({
      application_slot_id: APPLICATION_SLOT_ID,
      transport: { kind: 'publish', livekit_service_url: OWN_FOCUS },
    });
    const memberId = rtcCall.memberId;
    // The sticky set as the server echoes it: ourselves plus one peer.
    host._sink(ROOM_ID).onStickyEvents([
      memberEventIn({ sender: USER_ID, deviceId: DEVICE_ID, memberId, slotId: SLOT_ID, focus: OWN_FOCUS }),
      memberEventIn({
        sender: '@peer:example.org',
        deviceId: 'PEERDEVICE',
        memberId: 'peer-member-1',
        slotId: SLOT_ID,
        focus: OWN_FOCUS,
      }),
    ]);

    const { module: livekit, instances } = fakeLivekit();
    const rosterUpdates = [];
    const call = new MatrixRtcCall({
      call: rtcCall,
      livekit,
      fetchJson: () =>
        Promise.resolve({
          status: 200,
          body: JSON.stringify({ jwt: 'the-jwt', url: 'wss://sfu.example.org' }),
        }),
    });
    call.onParticipants = (roster) => rosterUpdates.push(roster);

    await call.connect({
      userId: USER_ID,
      deviceId: DEVICE_ID,
      livekitServiceUrl: OWN_FOCUS,
    });

    // One focus in play, so one Room, connected with what the token fetch
    // returned.
    expect(instances).toHaveLength(1);
    const room = instances[0];
    expect(room.url).toBe('wss://sfu.example.org');
    expect(room.token).toBe('the-jwt');

    // Roster entries join to livekit participants by rtc_identity once the
    // room knows them.
    const roster = await waitFor(
      () => (call.participants().length === 2 ? call.participants() : null),
      'a two-entry roster',
    );
    const peer = roster.find((p) => p.member_id === 'peer-member-1');
    expect(peer.livekitParticipant).toBeUndefined();

    const peerParticipant = new FakeParticipant(peer.rtc_identity);
    room.remotes.set(peer.rtc_identity, peerParticipant);
    room.emit(RoomEvent.ParticipantConnected, peerParticipant);
    expect(
      call.participants().find((p) => p.member_id === 'peer-member-1')
        .livekitParticipant,
    ).toBe(peerParticipant);

    // A subscribed track flows through the translation table into the roster
    // and out through onParticipants.
    room.emit(
      RoomEvent.TrackSubscribed,
      {},
      { source: 'camera' },
      peerParticipant,
    );
    await waitFor(() => {
      const latest = rosterUpdates.at(-1);
      const entry = latest?.find((p) => p.member_id === 'peer-member-1');
      return entry?.streams?.length === 1;
    }, 'the camera stream to reach onParticipants');
    const latest = rosterUpdates.at(-1).find((p) => p.member_id === 'peer-member-1');
    expect(latest.streams[0]).toEqual({ kind: 'camera', muted: false });
    expect(latest.livekitParticipant).toBe(peerParticipant);

    await call.disconnect();
    expect(room.disconnected).toBe(true);
  }, 20000);
});
