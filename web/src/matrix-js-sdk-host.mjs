/*
Copyright 2026 Element Creations Ltd.

SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
Please see LICENSE in the repository root for full details.
*/

/**
 * The Matrix side of a web call: matrix-js-sdk behind the wasm manager's
 * `MatrixBackendHost` contract. One object, both halves:
 * - sends: sticky (MSC4354), delayed events (MSC4140 — restart is the restart
 *   action, never cancel+resend; a delayed STATE event when a state key is
 *   given), state events, plain room events, redactions, and Olm-encrypted
 *   per-device to-device messages;
 * - reads: `subscribeRoom` delivers the room's complete current sets into the
 *   sink the manager hands it — encryption, state events of the requested
 *   types, joined members, the sticky set — first on subscribe and again on
 *   every change, plus timeline events and redactions as they arrive;
 *   `subscribeToDevice` delivers decrypted to-device messages with their Olm
 *   decryption metadata and MSC4153 cross-signing status; `relations`,
 *   `getOpenIdToken` and `rtcTransports` answer on request.
 * The manager parses every MatrixRTC content and applies the compatibility
 * dialects itself; this file hands events over verbatim.
 * `matrix-js-sdk` is not imported here: like `MatrixRtcCall`'s livekit-client,
 * the module is injected (`sdk`), keeping it an optional peer dependency and
 * this file testable against a mock. Requires v42+ (`_unstable_` sticky and
 * delayed-event APIs, rust-crypto).
 */

/**
 * Log in (registering a throwaway user first when `user` is blank — dev
 * homeservers with open registration) and return a crypto-ready, syncing
 * client. Cross-signing is bootstrapped before returning: MSC4153 peers
 * discard media keys from devices that are not cross-signed.
 *
 * @param {object} options
 * @param {object} options.sdk - the `matrix-js-sdk` module.
 */
export async function createMatrixSession({
  sdk,
  homeserverUrl,
  user,
  password,
  displayName = 'Web Peer',
  log = () => {},
}) {
  let localpart = user;
  let pass = password;
  if (!localpart) {
    localpart = `web-${Date.now().toString(16)}${Math.floor(Math.random() * 0xffff).toString(16)}`;
    pass = `test-${localpart}`;
    log(`registering ${localpart}`);
    const response = await fetch(`${homeserverUrl}/_matrix/client/v3/register`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        username: localpart,
        password: pass,
        auth: { type: 'm.login.dummy' },
        inhibit_login: true,
      }),
    });
    if (!response.ok) {
      throw new Error(`registration failed: ${response.status} ${await response.text()}`);
    }
  }

  const bootstrap = sdk.createClient({ baseUrl: homeserverUrl });
  const login = await bootstrap.loginRequest({
    type: 'm.login.password',
    identifier: { type: 'm.id.user', user: localpart },
    password: pass,
  });

  const client = sdk.createClient({
    baseUrl: homeserverUrl,
    accessToken: login.access_token,
    userId: login.user_id,
    deviceId: login.device_id,
  });

  // In-memory, deliberately: the IndexedDB crypto store is origin-scoped and
  // single-account, so switching users (throwaway registrations included)
  // dies with "the account in the store doesn't match". Every login here is a
  // fresh device anyway, so persisted crypto state buys nothing.
  await client.initRustCrypto({ useIndexedDB: false });
  // MSC4153: peers discard our media keys unless this device is cross-signed,
  // so bootstrap before any key can be exchanged — the same order the native
  // interop peer enforces. Uploading the signing keys needs UIA.
  log('bootstrapping cross-signing');
  await client.getCrypto().bootstrapCrossSigning({
    setupNewCrossSigning: true,
    authUploadDeviceSigningKeys: async (makeRequest) => {
      await makeRequest({
        type: 'm.login.password',
        identifier: { type: 'm.id.user', user: login.user_id },
        password: pass,
      });
    },
  });
  await client.setDisplayName(displayName);

  client.startClient();
  await new Promise((resolve, reject) => {
    client.once(sdk.ClientEvent.Sync, (state) =>
      state === 'PREPARED' ? resolve() : reject(new Error(`sync failed: ${state}`)),
    );
  });
  log(`ready as ${login.user_id} (${login.device_id})`);
  return { client, userId: login.user_id, deviceId: login.device_id, password: pass };
}

/**
 * The `MatrixBackendHost` the manager takes: `new WasmRtcSessionManager(host)`.
 */
export class MatrixHost {
  /**
   * @param {object} options
   * @param {object} options.sdk - the `matrix-js-sdk` module.
   * @param {object} options.client - a crypto-ready, syncing MatrixClient.
   * @param {(line: string) => void} [options.log]
   */
  constructor({ sdk, client, log = () => {} }) {
    this.sdk = sdk;
    this.client = client;
    this.log = log;
    /** curve25519 sender key -> device id, per megolm-attributed sender. */
    this.senderDeviceCache = new Map();
  }

  // --- identity ----------------------------------------------------------

  ownUserId() {
    return this.client.getUserId();
  }

  ownDeviceId() {
    return this.client.getDeviceId();
  }

  // --- sends -------------------------------------------------------------

  sendStickyEvent(roomId, eventType, content, durationMs) {
    // The manager chose the duration; pass it through verbatim.
    return this.client._unstable_sendStickyEvent(roomId, durationMs, null, eventType, content);
  }

  sendStateEvent(roomId, eventType, stateKey, content) {
    return this.client.sendStateEvent(roomId, eventType, content, stateKey);
  }

  /** A delayed STATE event when `stateKey` is set (the pre-sticky dialect's leave). */
  async sendDelayedEvent(roomId, eventType, stateKey, content, delayMs) {
    const response =
      stateKey === null || stateKey === undefined
        ? await this.client._unstable_sendDelayedEvent(roomId, { delay: delayMs }, null, eventType, content)
        : await this.client._unstable_sendDelayedStateEvent(
            roomId,
            { delay: delayMs },
            eventType,
            content,
            stateKey,
          );
    // The manager expects the bare MSC4140 delay id.
    return response.delay_id;
  }

  restartDelayedEvent(_roomId, delayId) {
    return this.client._unstable_updateDelayedEvent(delayId, this.sdk.UpdateDelayedEventAction.Restart);
  }

  cancelDelayedEvent(_roomId, delayId) {
    return this.client._unstable_updateDelayedEvent(delayId, this.sdk.UpdateDelayedEventAction.Cancel);
  }

  async sendToDeviceMessage(recipients, messageType, content) {
    // Olm-encrypted, per specific device — never `*`. Resolving with nothing
    // reports every recipient as served; a throw reports the batch
    // unattempted, and the manager re-sends on the next rollout.
    await this.client.encryptAndSendToDevice(messageType, recipients, content);
  }

  /** Reactions and the raised hand: js-sdk encrypts these in an encrypted room. */
  sendRoomEvent(roomId, eventType, content) {
    return this.client.sendEvent(roomId, eventType, content);
  }

  redactEvent(roomId, eventId, reason) {
    return this.client.redactEvent(roomId, eventId, undefined, reason === undefined ? undefined : { reason });
  }

  // --- reads -------------------------------------------------------------

  /**
   * Deliver the room's subjects into `sink`: the current sets now (as soon as
   * they are read), then again on every change, coalesced per tick. Timeline
   * events and redactions go as they arrive. Synchronous by contract: the
   * listeners are registered here, the deliveries happen from them.
   */
  subscribeRoom(roomId, subjects, sink) {
    const room = this.client.getRoom(roomId);
    if (!room) throw new Error(`not joined to ${roomId}`);
    const detachers = [];
    let pending = false;
    let cancelled = false;

    const feed = async () => {
      if (cancelled) return;
      sink.onEncryption(Boolean(room.currentState.getStateEvents('m.room.encryption', '')));
      for (const type of subjects.state_event_types) {
        const events = room.currentState.getStateEvents(type);
        sink.onStateEvents(type, await Promise.all(events.map((ev) => this.eventIn(ev))));
      }
      sink.onJoinedMembers(room.getJoinedMembers().map((member) => member.userId));
      sink.onStickyEvents(await this.stickySnapshot(room));
    };
    const scheduleFeed = () => {
      if (pending) return;
      pending = true;
      queueMicrotask(() => {
        pending = false;
        feed().catch((error) => this.log(`feed failed: ${error}`));
      });
    };

    room.on(this.sdk.RoomStickyEventsEvent.Update, scheduleFeed);
    // State listeners on the CLIENT, not the room: the room-level re-emit is
    // unreliable (it re-arms only when the RoomState instance is swapped, and
    // MSC4222 `state_after` sync churns those), while the client-level one is
    // what js-sdk's own MatrixRTCSessionManager trusts.
    const onStateEvent = (event) => {
      if (event.getRoomId() === roomId) scheduleFeed();
    };
    const onMembers = (_event, _state, member) => {
      if (member.roomId === roomId) scheduleFeed();
    };
    this.client.on(this.sdk.RoomStateEvent.Events, onStateEvent);
    this.client.on(this.sdk.RoomStateEvent.Members, onMembers);
    detachers.push(() => {
      room.off(this.sdk.RoomStickyEventsEvent.Update, scheduleFeed);
      this.client.off(this.sdk.RoomStateEvent.Events, onStateEvent);
      this.client.off(this.sdk.RoomStateEvent.Members, onMembers);
    });

    // Reactions and raised hands are ordinary timeline events, and in an
    // encrypted room they arrive encrypted: the Timeline event fires before
    // decryption, so the Decrypted one is what carries the readable content.
    const { RoomEvent, MatrixEventEvent } = this.sdk;
    if (subjects.timeline_event_types.length > 0 && RoomEvent && MatrixEventEvent) {
      const forward = (event) => {
        if (event.getRoomId() !== roomId) return;
        if (event.isSending?.() || event.isBeingDecrypted?.() || event.isDecryptionFailure?.()) return;
        if (event.isEncrypted?.() && event.getClearContent?.() === undefined && event.getType() === 'm.room.encrypted') return;
        if (!subjects.timeline_event_types.includes(event.getType())) return;
        this.eventIn(event)
          .then((payload) => sink.onTimelineEvents([payload]))
          .catch((error) => this.log(`timeline event failed: ${error}`));
      };
      const onTimeline = (event, _room, toStartOfTimeline, removed) => {
        if (toStartOfTimeline || removed) return;
        forward(event);
      };
      const onRedaction = (event) => {
        if (event.getRoomId() !== roomId) return;
        const target = event.event?.redacts ?? event.getContent()?.redacts;
        if (target) sink.onRedaction(target);
      };
      room.on(RoomEvent.Timeline, onTimeline);
      this.client.on(MatrixEventEvent.Decrypted, forward);
      room.on(RoomEvent.Redaction, onRedaction);
      detachers.push(() => {
        room.off(RoomEvent.Timeline, onTimeline);
        this.client.off(MatrixEventEvent.Decrypted, forward);
        room.off(RoomEvent.Redaction, onRedaction);
      });
    }

    scheduleFeed();
    return {
      cancel: () => {
        cancelled = true;
        for (const detach of detachers.splice(0)) detach();
      },
    };
  }

  /** Decrypted to-device messages of `eventTypes`, with their Olm metadata. */
  subscribeToDevice(eventTypes, sink) {
    const onToDevice = ({ message, encryptionInfo }) => {
      if (!eventTypes.includes(message.type)) return;
      this.toDeviceEncryption(encryptionInfo)
        .then((encryption) =>
          sink.onToDeviceMessage({
            sender: encryptionInfo?.sender ?? message.sender,
            event_type: message.type,
            content: message.content ?? {},
            encryption,
          }),
        )
        .catch((error) => this.log(`to-device message failed: ${error}`));
    };
    this.client.on(this.sdk.ClientEvent.ReceivedToDeviceMessage, onToDevice);
    return {
      cancel: () => this.client.off(this.sdk.ClientEvent.ReceivedToDeviceMessage, onToDevice),
    };
  }

  /** The `/relations` of one event, decrypted, in the `EventIn` shape. */
  async relations(roomId, eventId, relType, eventType) {
    const { events } = await this.client.relations(roomId, eventId, relType, eventType);
    const payloads = [];
    for (const event of events) {
      await this.client.decryptEventIfNeeded(event);
      if (event.isDecryptionFailure()) continue;
      payloads.push(await this.eventIn(event));
    }
    return payloads;
  }

  getOpenIdToken() {
    return this.client.getOpenIdToken();
  }

  /** `GET /_matrix/client/v1/rtc/transports`; `[]` where the homeserver lacks it. */
  async rtcTransports() {
    try {
      const response = await this.client.http.authedRequest(
        this.sdk.Method.Get,
        '/rtc/transports',
        undefined,
        undefined,
        { prefix: '/_matrix/client/v1' },
      );
      return response?.rtc_transports ?? [];
    } catch (error) {
      if (error?.httpStatus === 404) return [];
      throw error;
    }
  }

  // --- shapes ------------------------------------------------------------

  /** One event as the manager takes it: content verbatim, decryption info as reported. */
  async eventIn(event) {
    return {
      event_id: event.getId(),
      sender: event.getSender(),
      event_type: event.getType(),
      state_key: event.getStateKey?.() ?? undefined,
      origin_server_ts: event.getTs(),
      content: event.getContent(),
      encryption: await this.eventEncryption(event),
    };
  }

  /** What js-sdk reports: encrypted or not, and the attributed device. */
  async eventEncryption(event) {
    if (!event.isEncrypted?.()) return { kind: 'cleartext' };
    return { kind: 'encrypted', sender_device_id: await this.senderDeviceOf(event) };
  }

  /** Olm metadata plus MSC4153: the key is only accepted from a cross-signed device. */
  async toDeviceEncryption(encryptionInfo) {
    if (!encryptionInfo) return { kind: 'cleartext' };
    let senderCrossSigned;
    if (encryptionInfo.sender && encryptionInfo.senderDevice) {
      const status = await this.client
        .getCrypto()
        .getDeviceVerificationStatus(encryptionInfo.sender, encryptionInfo.senderDevice);
      senderCrossSigned = status?.signedByOwner ?? false;
    }
    return {
      kind: 'encrypted',
      sender_device_id: encryptionInfo.senderDevice,
      sender_cross_signed: senderCrossSigned,
    };
  }

  /**
   * The room's active sticky events, decrypted. The sticky store does not
   * decrypt and keys encrypted events under `m.room.encrypted`, so iterate
   * everything and decrypt; the manager filters by type.
   */
  async stickySnapshot(room) {
    const events = [];
    for (const event of room._unstable_getStickyEvents()) {
      await this.client.decryptEventIfNeeded(event);
      if (event.isDecryptionFailure()) {
        this.log(`sticky event ${event.getId()} failed to decrypt; skipping`);
        continue;
      }
      events.push(await this.eventIn(event));
    }
    return events;
  }

  /**
   * The device that megalm-encrypted `event`: js-sdk exposes the sender's
   * curve25519 key, and the device list proves which device owns it.
   */
  async senderDeviceOf(event) {
    const senderKey = event.getSenderKey();
    const sender = event.getSender();
    if (!senderKey || !sender) return undefined;
    const cached = this.senderDeviceCache.get(senderKey);
    if (cached) return cached;

    const devices = await this.client.getCrypto().getUserDeviceInfo([sender], true);
    for (const device of devices.get(sender)?.values() ?? []) {
      if (device.getIdentityKey() === senderKey) {
        this.senderDeviceCache.set(senderKey, device.deviceId);
        return device.deviceId;
      }
    }
    this.log(`no device of ${sender} owns sender key ${senderKey}`);
    return undefined;
  }

  /** The `livekit_service_url` the homeserver advertises, from well-known. */
  async discoverFocus() {
    const response = await fetch(
      `${this.client.baseUrl.replace(/\/$/, '')}/.well-known/matrix/client`,
    );
    if (!response.ok) throw new Error(`well-known fetch failed: ${response.status}`);
    const wellKnown = await response.json();
    const foci = wellKnown['org.matrix.msc4143.rtc_foci'] ?? [];
    const livekit = foci.find((focus) => focus.type === 'livekit');
    if (!livekit?.livekit_service_url) {
      throw new Error('well-known advertises no livekit focus');
    }
    return livekit.livekit_service_url;
  }
}
