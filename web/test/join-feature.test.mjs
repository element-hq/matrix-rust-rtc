/*
Copyright 2026 Element Creations Ltd.

SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
Please see LICENSE in the repository root for full details.
*/

import { describe, it, expect, beforeEach } from 'vitest';
import { existsSync } from 'node:fs';
import { mockBackendHost, openSlotEvent } from './mock-backend-host.mjs';

const nodeBindingUrl = new URL('../pkg/node/matrix_rtc_wasm.js', import.meta.url);

const ROOM_ID = '!test:example.org';
const SLOT_ID = 'm.call#TEST';
const USER_ID = '@alice:example.org';
const DEVICE_ID = 'device123';
const SFU = 'https://example.com/livekit/jwt';

// Helper function to get values from wasm-bindgen serialized content (Map, string, or object)
function getContentValue(content, key) {
  if (content instanceof Map) {
    return content.get(key);
  } else if (typeof content === 'string') {
    const obj = JSON.parse(content);
    return obj[key];
  } else {
    return content[key];
  }
}

const joinParams = {
  slot_id: SLOT_ID,
  application: 'm.call',
  transport: { type: 'livekit', livekit_service_url: SFU },
};

describe('WASM bindings with a mock backend host', () => {
  let bindings;
  let host;

  beforeEach(async () => {
    if (!existsSync(nodeBindingUrl)) {
      // Skip all tests if bindings haven't been built
      return;
    }
    bindings = await import(nodeBindingUrl.href);
    host = mockBackendHost({
      userId: USER_ID,
      deviceId: DEVICE_ID,
      slots: [openSlotEvent(SLOT_ID)],
      transports: [{ type: 'livekit', livekit_service_url: SFU }],
    });
  });

  describe('WasmRtcClient, WasmRtcRoom and WasmRtcCall', () => {
    it('opens a room and subscribes to what it needs', async () => {
      const client = new bindings.WasmRtcClient(host);
      const room = await client.room(ROOM_ID, undefined);
      expect(room.roomId).toBe(ROOM_ID);

      const subjects = host._subjects(ROOM_ID);
      expect(subjects.state_event_types).toEqual(['m.rtc.slot', 'org.matrix.msc4143.rtc.slot']);
      expect(host._toDevice()).not.toBeNull();

      await expect(client.room(ROOM_ID, undefined)).rejects.toThrow(/already open/);
    });

    it('joinCall() without a transport takes the advertised one and sends the membership', async () => {
      const client = new bindings.WasmRtcClient(host);
      const room = await client.room(ROOM_ID, undefined);
      host._clear();

      const { transport, ...withoutTransport } = joinParams;
      void transport;
      const call = await room.joinCall(withoutTransport);
      const memberId = call.memberId;
      expect(memberId).toBeTruthy();
      expect(call.roomId).toBe(ROOM_ID);
      expect(call.slotId).toBe(SLOT_ID);
      expect(call.isLive).toBe(true);

      const stickyEvents = host._getStickyEvents();
      expect(stickyEvents.length).toBe(1);
      expect(stickyEvents[0].roomId).toBe(ROOM_ID);
      // The wire spelling: peers do not match on `m.rtc.member`.
      expect(stickyEvents[0].eventType).toBe('org.matrix.msc4143.rtc.member');

      const content = stickyEvents[0].content;
      expect(getContentValue(content, 'slot_id')).toBe(SLOT_ID);
      // member.id must be unique per join, so it must not be derived from the
      // (stable) user and device IDs.
      const stickyKey = getContentValue(content, 'msc4354_sticky_key');
      expect(stickyKey).toBe(memberId);
      expect(stickyKey).not.toBe(`${USER_ID}-${DEVICE_ID}`);
      expect(getContentValue(getContentValue(content, 'member'), 'membership')).toBe('join');
      // The transport came from the backend.
      const transports = getContentValue(content, 'transports');
      expect(getContentValue(getContentValue(transports, 'published')[0], 'livekit_service_url')).toBe(SFU);

      // The dead man's switch, armed by the join.
      const delayedEvents = host._getDelayedEvents();
      expect(delayedEvents.length).toBe(1);
      expect(delayedEvents[0].eventType).toBe('org.matrix.msc4143.rtc.member');
      expect(delayedEvents[0].stateKey).toBeNull();
      const leaveReason = getContentValue(delayedEvents[0].content, 'leave_reason');
      expect(getContentValue(leaveReason, 'code')).toBe('delayed_leave');
    });

    it('leave() with a leave reason works and ends the call', async () => {
      const client = new bindings.WasmRtcClient(host);
      const room = await client.room(ROOM_ID, undefined);
      const call = await room.joinCall(joinParams);
      host._clear();

      await call.leave({
        leave_reason: { code: 'leave', reason: 'user hung up' },
      });

      const stickyEvents = host._getStickyEvents();
      expect(stickyEvents.length).toBe(1);
      const content = stickyEvents[0].content;
      const leaveReason = getContentValue(content, 'leave_reason');
      expect(getContentValue(leaveReason, 'code')).toBe('leave');
      expect(getContentValue(leaveReason, 'reason')).toBe('user hung up');
      expect(getContentValue(getContentValue(content, 'member'), 'membership')).toBe('leave');

      const cancelledEvents = host._getCancelledEvents();
      expect(cancelledEvents.length).toBe(1);
      expect(cancelledEvents[0].delayId).toBe('delayed-event-0');

      expect(call.isLive).toBe(false);
      await expect(call.leave(undefined)).rejects.toThrow(/over/);
    });

    it('joining a joined slot is refused', async () => {
      const client = new bindings.WasmRtcClient(host);
      const room = await client.room(ROOM_ID, undefined);
      await room.joinCall(joinParams);
      await expect(room.joinCall(joinParams)).rejects.toThrow();
    });

    it('close() leaves a joined call, ends the subscription, and frees the room', async () => {
      const client = new bindings.WasmRtcClient(host);
      const room = await client.room(ROOM_ID, undefined);
      const call = await room.joinCall(joinParams);
      host._clear();

      await room.close();
      expect(host._getCancelledEvents().length).toBe(1);
      expect(host._sink(ROOM_ID)).toBeUndefined();
      expect(call.isLive).toBe(false);
      // A no-op the second time; the room's other methods reject.
      await room.close();
      await expect(room.joinCall(joinParams)).rejects.toThrow(/closed/);
      // The room can be opened again.
      const reopened = await client.room(ROOM_ID, undefined);
      expect(reopened.roomId).toBe(ROOM_ID);
    });

    it('joinCall() into a room with no open slot is refused', async () => {
      const closed = mockBackendHost({ userId: USER_ID, deviceId: DEVICE_ID, slots: [] });
      const client = new bindings.WasmRtcClient(closed);
      const room = await client.room(ROOM_ID, undefined);
      await expect(room.joinCall(joinParams)).rejects.toThrow(/not open/);
      expect(closed._getStickyEvents().length).toBe(0);
    });

    it('a sticky_events room mirrors the legacy fields on the membership', async () => {
      const client = new bindings.WasmRtcClient(host);
      const room = await client.room(ROOM_ID, { element_call_compat: 'sticky_events' });
      await room.joinCall(joinParams);

      const content = host._getStickyEvents()[0].content;
      expect(getContentValue(getContentValue(content, 'member'), 'user_id')).toBe(USER_ID);
      expect(getContentValue(getContentValue(content, 'member'), 'device_id')).toBe(DEVICE_ID);
      expect(getContentValue(content, 'rtc_transports')).toBeDefined();
    });
  });

  describe('Error handling', () => {
    it('join with missing required params throws', async () => {
      const client = new bindings.WasmRtcClient(host);
      const room = await client.room(ROOM_ID, undefined);
      const { slot_id, ...invalidParams } = joinParams;
      void slot_id;
      await expect(room.joinCall(invalidParams)).rejects.toThrow(/invalid join params/);
    });
  });
});
