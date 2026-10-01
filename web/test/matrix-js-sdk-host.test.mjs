/*
Copyright 2026 Element Creations Ltd.

SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
Please see LICENSE in the repository root for full details.
*/

// The real MatrixHost's room subscription against a fake matrix-js-sdk client:
// each subject is delivered on its own change, a slow sticky read never lands
// over a newer one, and a failed read is retried.

import { EventEmitter } from 'node:events';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { MatrixHost, READ_RETRY_MS } from '../src/matrix-js-sdk-host.mjs';

const ROOM = '!room:example.org';
const SLOT = 'm.rtc.slot';

const sdk = {
  RoomStickyEventsEvent: { Update: 'sticky.update' },
  RoomStateEvent: { Events: 'state.events', Members: 'state.members' },
};

function fakeEvent({ id, sender = '@alice:example.org', type, stateKey, content = {} }) {
  return {
    getId: () => id,
    getSender: () => sender,
    getType: () => type,
    getStateKey: () => stateKey,
    getTs: () => 1,
    getContent: () => content,
    getRoomId: () => ROOM,
    isEncrypted: () => false,
    isDecryptionFailure: () => false,
  };
}

const member = (id) => fakeEvent({ id, type: 'm.rtc.member', content: { member: { id } } });

/** A client whose sticky decryption the test can hold open or fail. */
function fakeClient() {
  const client = new EventEmitter();
  const room = new EventEmitter();
  room.roomId = ROOM;
  room.state = { [SLOT]: [fakeEvent({ id: '$slot', type: SLOT, stateKey: 'm.call#ROOM' })] };
  room.members = ['@me:example.org'];
  room.sticky = [];
  room.currentState = {
    getStateEvents: (type, stateKey) =>
      stateKey === undefined ? (room.state[type] ?? []) : (room.state[type]?.[0] ?? null),
  };
  room.getJoinedMembers = () => room.members.map((userId) => ({ userId }));
  room._unstable_getStickyEvents = () => [...room.sticky];

  client.room = room;
  client.getRoom = (roomId) => (roomId === ROOM ? room : null);
  /** Decryptions waiting to be released; empty means they pass at once. */
  client.held = null;
  client.failNext = 0;
  client.decryptEventIfNeeded = () => {
    if (client.failNext > 0) {
      client.failNext -= 1;
      return Promise.reject(new Error('decryption failed'));
    }
    if (!client.held) return Promise.resolve();
    return new Promise((resolve) => client.held.push(resolve));
  };
  return client;
}

function recordingSink() {
  const calls = [];
  const record = (name) => (...args) => calls.push([name, ...args]);
  return {
    calls,
    of: (name) => calls.filter(([called]) => called === name).map(([, ...args]) => args),
    onEncryption: record('encryption'),
    onStateEvents: record('state'),
    onJoinedMembers: record('members'),
    onStickyEvents: record('sticky'),
    onTimelineEvents: record('timeline'),
    onRedaction: record('redaction'),
  };
}

const subjects = { state_event_types: [SLOT], timeline_event_types: [] };
const flush = () => new Promise((resolve) => setTimeout(resolve, 0));
const stickyIds = (sink) => sink.of('sticky').map(([events]) => events.map((event) => event.event_id));

describe('MatrixHost room subscription', () => {
  let client;
  let host;
  let sink;

  beforeEach(() => {
    client = fakeClient();
    host = new MatrixHost({ sdk, client });
    sink = recordingSink();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('delivers every subject on subscribe, room state before membership', async () => {
    client.room.sticky = [member('$alice')];
    host.subscribeRoom(ROOM, subjects, sink);
    await flush();

    expect(sink.calls.map(([name]) => name)).toEqual(['encryption', 'state', 'members', 'sticky']);
    expect(sink.of('state')[0][0]).toBe(SLOT);
    expect(stickyIds(sink)).toEqual([['$alice']]);
  });

  it('re-delivers only the subject that changed', async () => {
    host.subscribeRoom(ROOM, subjects, sink);
    await flush();
    sink.calls.length = 0;

    client.room.state[SLOT] = [];
    const change = fakeEvent({ id: '$closed', type: SLOT, stateKey: 'm.call#ROOM' });
    client.emit(sdk.RoomStateEvent.Events, change);
    client.emit(sdk.RoomStateEvent.Events, change);
    await flush();

    expect(sink.calls).toEqual([['state', SLOT, []]]);
  });

  it('never lets a slow sticky read land over a newer one', async () => {
    client.room.sticky = [member('$alice'), member('$bob')];
    client.held = [];
    host.subscribeRoom(ROOM, subjects, sink);
    await flush();
    const firstRead = client.held.splice(0);

    // Bob leaves while the first read is still decrypting.
    client.room.sticky = [member('$alice')];
    client.room.emit(sdk.RoomStickyEventsEvent.Update);
    await flush();
    for (const release of client.held.splice(0)) release();
    await flush();
    expect(stickyIds(sink)).toEqual([['$alice']]);

    client.held = null;
    for (const release of firstRead) release();
    await flush();
    await flush();
    expect(stickyIds(sink)).toEqual([['$alice']]);
  });

  it('retries a failed read, so a failed first sticky set still arrives', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    client.room.sticky = [member('$alice')];
    client.failNext = 1;
    host.subscribeRoom(ROOM, subjects, sink);
    await vi.advanceTimersByTimeAsync(0);
    expect(sink.of('sticky')).toEqual([]);

    await vi.advanceTimersByTimeAsync(READ_RETRY_MS);
    expect(stickyIds(sink)).toEqual([['$alice']]);
  });

  it('stops delivering and retrying once cancelled', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    client.room.sticky = [member('$alice')];
    client.failNext = 1;
    const subscription = host.subscribeRoom(ROOM, subjects, sink);
    await vi.advanceTimersByTimeAsync(0);

    subscription.cancel();
    client.room.emit(sdk.RoomStickyEventsEvent.Update);
    await vi.advanceTimersByTimeAsync(READ_RETRY_MS * 2);
    expect(sink.of('sticky')).toEqual([]);
    expect(client.listenerCount(sdk.RoomStateEvent.Events)).toBe(0);
    expect(client.room.listenerCount(sdk.RoomStickyEventsEvent.Update)).toBe(0);
  });
});
