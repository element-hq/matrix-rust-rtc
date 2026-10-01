/*
Copyright 2026 Element Creations Ltd.

SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
Please see LICENSE in the repository root for full details.
*/

/**
 * Compile-only consumer of the generated TypeScript declarations
 * (`pkg/browser/matrix_rtc_wasm.d.ts`). Never executed: the vitest suite runs
 * `tsc --noEmit` over it, so a declaration that stops parsing, an export that
 * disappears, or a signature that regresses to `any`-shaped nonsense fails the
 * build. The declarations are hand-written in the crate's `ts_types.rs`;
 * field-level drift against the serde structs still needs a human eye there.
 */

import type {
  AttachOptionsIn,
  BackendSubscription,
  EventIn,
  JoinParamsIn,
  MatrixBackendHost,
  MediaDelegate,
  RoomSubjects,
  RtcCallEvent,
  RtcParticipant,
  WasmMediaSession,
  WasmRoomSink,
  WasmRtcSessionManager,
  WasmToDeviceSink,
} from '../pkg/browser/matrix_rtc_wasm';

export function host(client: {
  userId: string;
  deviceId: string;
  send(): Promise<{ event_id: string }>;
}): MatrixBackendHost {
  return {
    ownUserId: () => client.userId,
    ownDeviceId: () => client.deviceId,
    sendStickyEvent: () => client.send(),
    sendStateEvent: () => client.send(),
    sendDelayedEvent: (_room, _type, stateKey: string | null) => Promise.resolve(stateKey ?? 'delay'),
    restartDelayedEvent: () => Promise.resolve(),
    cancelDelayedEvent: () => Promise.resolve(),
    sendToDeviceMessage: () => Promise.resolve(),
    sendRoomEvent: () => client.send(),
    redactEvent: () => Promise.resolve(),
    subscribeRoom: (_roomId: string, subjects: RoomSubjects, sink: WasmRoomSink): BackendSubscription => {
      const slot: EventIn = {
        event_id: '$slot',
        sender: '@admin:hs',
        event_type: 'm.rtc.slot',
        state_key: 'm.call#ROOM',
        content: { status: 'open' },
        encryption: { kind: 'cleartext' },
      };
      sink.onEncryption(false);
      sink.onStateEvents(subjects.state_event_types[0], [slot]);
      sink.onJoinedMembers([client.userId]);
      sink.onStickyEvents([]);
      return { cancel: () => {} };
    },
    subscribeToDevice: (_types: string[], sink: WasmToDeviceSink): BackendSubscription => {
      sink.onToDeviceMessage({
        sender: '@a:hs',
        event_type: 'm.rtc.encryption_key',
        content: {},
        encryption: { kind: 'encrypted', sender_device_id: 'DEV', sender_cross_signed: true },
      });
      return { cancel: () => {} };
    },
    relations: () => Promise.resolve([]),
    getOpenIdToken: () =>
      Promise.resolve({ access_token: 't', token_type: 'Bearer', matrix_server_name: 'hs', expires_in: 1 }),
    rtcTransports: () => Promise.resolve([{ type: 'livekit', livekit_service_url: 'https://sfu' }]),
  };
}

export async function smoke(
  manager: WasmRtcSessionManager,
  delegate: MediaDelegate,
): Promise<void> {
  const options: AttachOptionsIn = { element_call_compat: 'sticky_events' };
  await manager.attachRoom('!r:hs', options);

  const params: JoinParamsIn = {
    room_id: '!r:hs',
    slot_id: 'm.call#ROOM',
    application: 'm.call',
  };
  const memberId: string = await manager.join(params);

  const session: WasmMediaSession = await manager.connectMedia(
    {
      room_id: '!r:hs',
      slot_id: 'm.call#ROOM',
      user_id: '@a:hs',
      device_id: 'DEV',
      livekit_service_url: 'https://sfu',
    },
    delegate,
  );

  const roster: RtcParticipant[] = session.participants();
  for (const participant of roster) {
    // Option fields serialize as null, not absence.
    const identity: string | null = participant.rtc_identity;
    void identity;
  }

  // The event union discriminates on `type`.
  const handle = (event: RtcCallEvent): string => {
    switch (event.type) {
      case 'key_imported':
        return `${event.member_id}@${event.key_index}`;
      case 'active_speakers':
        return event.speakers.map((speaker) => speaker.member_id).join(',');
      case 'ended':
        return event.reason;
      default:
        return event.type;
    }
  };
  void handle;
  void memberId;

  await manager.detachRoom('!r:hs');
}
