/*
Copyright 2026 Element Creations Ltd.

SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
Please see LICENSE in the repository root for full details.
*/

// A `MatrixBackendHost` stand-in for the tests: every send resolves and is
// recorded; `subscribeRoom` delivers the configured current sets on subscribe
// and keeps the sink so a test can deliver changes later.

/** One open `m.rtc.slot` state event in the `EventIn` shape. */
export function openSlotEvent(slotId, encryption) {
  const content = { status: 'open', application: { type: 'm.call' } };
  if (encryption) content.encryption = { type: encryption };
  return {
    event_id: '$slot',
    sender: '@admin:example.org',
    event_type: 'm.rtc.slot',
    state_key: slotId,
    origin_server_ts: 1,
    content,
    encryption: { kind: 'cleartext' },
  };
}

/** One `m.rtc.member` sticky event in the `EventIn` shape. */
export function memberEventIn({ sender, deviceId, memberId, slotId, focus, eventId }) {
  return {
    event_id: eventId ?? `$${memberId}`,
    sender,
    event_type: 'm.rtc.member',
    origin_server_ts: 2,
    content: {
      slot_id: slotId,
      // The wire spelling (MSC4354 unstable id), as real events carry it.
      msc4354_sticky_key: memberId,
      application: { type: 'm.call' },
      member: { id: memberId, membership: 'join' },
      transports: {
        published: [{ type: 'livekit', livekit_service_url: focus }],
        can_subscribe: ['livekit'],
      },
    },
    encryption: { kind: 'encrypted', sender_device_id: deviceId, sender_cross_signed: true },
  };
}

export function mockBackendHost({
  userId = '@me:example.org',
  deviceId = 'MYDEVICE',
  encrypted = false,
  slots = [],
  members = [userId],
  sticky = [],
  transports = [],
} = {}) {
  let counter = 0;
  const stickyEventsSent = [];
  const delayedEventsSent = [];
  const cancelledEvents = [];
  const stateEventsSent = [];
  /** roomId -> { subjects, sink } */
  const rooms = new Map();
  let toDevice = null;

  return {
    ownUserId: () => userId,
    ownDeviceId: () => deviceId,
    sendStickyEvent: (roomId, eventType, content) => {
      const eventId = `$sticky-event-${counter++}`;
      stickyEventsSent.push({ roomId, eventType, content, eventId });
      return Promise.resolve({ event_id: eventId });
    },
    sendDelayedEvent: (roomId, eventType, stateKey, content, delayMs) => {
      const delayId = `delayed-event-${delayedEventsSent.length}`;
      delayedEventsSent.push({ roomId, eventType, stateKey, content, delayMs, delayId });
      return Promise.resolve(delayId);
    },
    restartDelayedEvent: () => Promise.resolve(),
    cancelDelayedEvent: (roomId, delayId) => {
      cancelledEvents.push({ roomId, delayId });
      return Promise.resolve();
    },
    sendStateEvent: (roomId, eventType, stateKey, content) => {
      const eventId = `$state-event-${stateEventsSent.length}`;
      stateEventsSent.push({ roomId, eventType, stateKey, content, eventId });
      return Promise.resolve({ event_id: eventId });
    },
    sendToDeviceMessage: () => Promise.resolve(),
    sendRoomEvent: () => Promise.resolve({ event_id: `$room-${counter++}` }),
    redactEvent: () => Promise.resolve(),
    subscribeRoom: (roomId, subjects, sink) => {
      rooms.set(roomId, { subjects, sink });
      // The current sets, the way a host delivers them on subscribe.
      if (subjects.encryption) sink.onEncryption(encrypted);
      for (const type of subjects.state_event_types) {
        if (type === 'm.rtc.slot') sink.onStateEvents(type, slots);
        if (type === 'org.matrix.msc3401.call.member') sink.onStateEvents(type, []);
      }
      if (subjects.joined_members) sink.onJoinedMembers(members);
      if (subjects.sticky_events) sink.onStickyEvents(sticky);
      return { cancel: () => rooms.delete(roomId) };
    },
    subscribeToDevice: (eventTypes, sink) => {
      toDevice = { eventTypes, sink };
      return { cancel: () => (toDevice = null) };
    },
    relations: () => Promise.resolve([]),
    getOpenIdToken: () =>
      Promise.resolve({
        access_token: 'opaque',
        token_type: 'Bearer',
        matrix_server_name: 'example.org',
        expires_in: 3600,
      }),
    rtcTransports: () => Promise.resolve(transports),
    // Test access.
    _sink: (roomId) => rooms.get(roomId)?.sink,
    _subjects: (roomId) => rooms.get(roomId)?.subjects,
    _toDevice: () => toDevice,
    _getStickyEvents: () => stickyEventsSent,
    _getDelayedEvents: () => delayedEventsSent,
    _getCancelledEvents: () => cancelledEvents,
    _getStateEvents: () => stateEventsSent,
    _clear: () => {
      stickyEventsSent.length = 0;
      delayedEventsSent.length = 0;
      cancelledEvents.length = 0;
      stateEventsSent.length = 0;
    },
  };
}
