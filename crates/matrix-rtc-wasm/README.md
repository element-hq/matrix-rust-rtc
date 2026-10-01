# matrix-rtc-wasm

The browser binding for MatrixRTC: `matrix-rtc-core`'s signalling state
machine plus the shared media engine (`matrix-rtc-media`), compiled to
wasm32 and exposed to JavaScript with wasm-bindgen.

The division of labour is deliberate and strict:

- **Rust owns the protocol.** Membership state (MSC4143/MSC4354), the dead
  man's switch (MSC4140), media-key lifecycle and rotation, the participant
  roster, the multi-focus connection pool, MSC4195 identities and token
  request shapes, and the Element Call compatibility dialects.
- **JS owns the IO and the media.** Your Matrix client (matrix-js-sdk) is the
  `MatrixBackendHost`: it sends the events Rust asks it to and delivers the
  room's current state into the sinks Rust subscribes with; livekit-js owns the SFU
  connection, tracks, rendering, and frame encryption — Rust hands it keys and
  reads back its room events. No media bytes ever cross into wasm.

This crate compiles **only** for `wasm32-unknown-unknown` (its futures wrap
JS promises and are `!Send`); on other targets it is an empty crate.

## Building

```sh
# From the repo root; outputs into web/pkg/{browser,node}.
./web/scripts/build-bindings.sh
```

The `web/` npm package wraps the output (`matrix-rtc-wasm` from
`web/package.json`, exports `.` for the bindings and `./call` for the
JS wrapper). See `web/README.md` for the packaging details.

## Get started

The complete, working reference is **`web/demo/`** — a call page over
matrix-js-sdk + livekit-client that is also the interop test peer. What
follows is the shape of it.

### The quick way: the three shipped layers

The package ships every layer of the integration, each dependency an
optional, injected peer: `matrix-rtc-wasm/call` (`MatrixRtcCall`, the
livekit-client half) and `matrix-rtc-wasm/matrix-js-sdk-host`
(`createMatrixSession` + `MatrixHost`, the Matrix half). You write the app
glue.

```js
import init, * as bindings from 'matrix-rtc-wasm';
import { MatrixRtcCall } from 'matrix-rtc-wasm/call';
import { MatrixHost, createMatrixSession } from 'matrix-rtc-wasm/matrix-js-sdk-host';
import * as sdk from 'matrix-js-sdk';
import * as livekit from 'livekit-client';
import E2EEWorker from 'livekit-client/e2ee-worker?worker';

await init();
bindings.initLogging('info', '');

// A crypto-ready, syncing client (cross-signing bootstrapped — MSC4153).
const { client, userId, deviceId } =
  await createMatrixSession({ sdk, homeserverUrl, user, password });

const host = new MatrixHost({ sdk, client });  // the MatrixBackendHost
const rtc = new bindings.WasmRtcClient(host);   // one per Matrix session; no I/O
// Subscribes through the host and resolves once the room's current state is in.
const rtcRoom = await rtc.room(roomId, { element_call_compat: 'off' });

// 1. Publish our membership (starts the keep-alive machinery). The transport
//    comes from the homeserver's /rtc/transports unless you pin one here.
const rtcCall = await rtcRoom.joinCall({ slot_id: 'm.call#ROOM', application: 'm.call' });

// 2. Attach media: roster + LiveKit connection lifecycle.
const call = new MatrixRtcCall({
  call: rtcCall,
  bindings,
  livekit,
  roomOptions: { e2ee: { worker: new E2EEWorker() } },
});
call.onParticipants = (roster) => render(roster); // entries carry rtc_identity
call.onEvent = (event) => console.log(event);     // key_imported, stream_started, ...
await call.connect({ userId, deviceId, livekitServiceUrl: focusUrl });

// 3. Media is livekit-js as usual, via the rooms the wrapper opened.
const room = call.rooms.get(focusUrl);
await room.localParticipant.enableCameraAndMicrophone();

// 4. Hang up: media, then our membership, then the room's subscription.
await call.disconnect();
await rtcCall.leave({});
await rtcRoom.close();
```

The objects are client → room → call. `rtc.room(...)` refuses a room that
already has a live room object. `rtcRoom.close()` leaves every call joined
through it, then unsubscribes; freeing a room or a call (`free()`, or garbage
collection) sends no leave — the membership expires through its delayed
leave. A call is over once left or once its room is closed (`isLive`), and
joining the slot again yields a new one. The library does not detect
incoming calls: a ring reaches the page through its own Matrix client.

Every roster entry is
`{ member_id, user_id, device_id, is_local, reachable, streams, rtc_identity }`;
join `rtc_identity` to `room.getParticipantByIdentity()` for the live
livekit-js participant.

### The host contract

Only needed when you bring your own Matrix stack instead of
`matrix-rtc-wasm/matrix-js-sdk-host` (which implements all of this section;
`web/src/matrix-js-sdk-host.mjs` is the reference).

`new WasmRtcClient(host)` takes a `MatrixBackendHost` (typed in the
generated `.d.ts`): identity, the sends, and the subscriptions Rust feeds
itself from. Contents arrive as plain JS objects; every send returns a
Promise. With matrix-js-sdk (v42+):

| Method | matrix-js-sdk | Resolves to |
| --- | --- | --- |
| `ownUserId()` / `ownDeviceId()` | `getUserId()` / `getDeviceId()` | strings (sync) |
| `sendStickyEvent(roomId, type, content, durationMs)` | `_unstable_sendStickyEvent(roomId, durationMs, null, type, content)` — pass `durationMs` through verbatim | `{event_id}` |
| `sendStateEvent(roomId, type, stateKey, content)` | `sendStateEvent(roomId, type, content, stateKey)` | `{event_id}` |
| `sendDelayedEvent(roomId, type, stateKey, content, delayMs)` | `_unstable_sendDelayedEvent(...)`, or `_unstable_sendDelayedStateEvent(...)` when `stateKey` is not `null` | the bare `delay_id` string |
| `restartDelayedEvent(roomId, delayId)` | `_unstable_updateDelayedEvent(delayId, Restart)` — a true MSC4140 restart, **never** cancel+resend | anything |
| `cancelDelayedEvent(roomId, delayId)` | `_unstable_updateDelayedEvent(delayId, Cancel)` | anything |
| `sendToDeviceMessage(recipients, type, content)` | `encryptAndSendToDevice(type, recipients, content)` — Olm-encrypted, per specific device | `[{userId, deviceId, error?}]`, or nothing = all delivered |
| `sendRoomEvent(roomId, type, content)` / `redactEvent(roomId, eventId, reason)` | `sendEvent` / `redactEvent` | `{event_id}` / anything |
| `subscribeRoom(roomId, subjects, sink)` | register listeners; see below | `{ cancel() }` (sync) |
| `subscribeToDevice(eventTypes, sink)` | `ClientEvent.ReceivedToDeviceMessage` | `{ cancel() }` (sync) |
| `relations(roomId, eventId, relType, eventType)` | `client.relations(...)`, decrypted | `EventIn[]` |
| `getOpenIdToken()` | `getOpenIdToken()` | the token object |
| `rtcTransports()` | `GET /_matrix/client/v1/rtc/transports`; `[]` on 404 | the `rtc_transports` array |

`subscribeRoom` is where the room reaches Rust. Every subscription wants the
room's encryption state, its joined members and its sticky events; `subjects`
adds what the room's mode needs on top (`state_event_types`,
`timeline_event_types`). Deliver each as the room's **complete current set**,
once as soon as you subscribe and again on every change (the first set may be
the one at subscription time, not a later one):

```js
sink.onEncryption(isEncrypted);
sink.onStateEvents(type, events);      // per requested type; the full set, [] included
sink.onJoinedMembers(joinedUserIds);   // must include yourself
sink.onStickyEvents(events);           // the whole sticky map
sink.onTimelineEvents(events);         // as they arrive
sink.onRedaction(redactedEventId);
```

Each event is an `EventIn`:
`{ event_id, sender, event_type, state_key?, origin_server_ts, content, encryption }`
with `content` verbatim (decrypt first) and `encryption` what your client can
honestly report — `{ kind: 'cleartext' }` or
`{ kind: 'encrypted', sender_device_id?, sender_cross_signed? }`, the device
attributed from the decryption metadata (the js-sdk host matches the megolm
sender key against the device list). Rust derives the MSC4143 origin rules and
the MSC4153 key acceptance from that; you never say who is joined. To-device
messages are `{ sender, event_type, content, encryption }` with the Olm sender
metadata and cross-signing status — MSC4153 discards keys from devices that are
not cross-signed, so bootstrap cross-signing before joining anything encrypted.

The page owns every clock: call `rtcCall.heartbeat()` on an interval
(`HEARTBEAT_INTERVAL_MS()`, 10 s) while joined — without it the dead man's
switch fires and peers see you depart mid-call. `MatrixRtcCall.connect` starts
that interval for you.

### Element Call compatibility

Pass `element_call_compat` when opening the room (`rtc.room`) — `"off"` (default, spec-current),
`"sticky_events"` (Element Call "Matrix 2.0", 2025), or `"state_events"`
(what deployed Element Call speaks) — and the library handles the rest:
which room state it subscribes to, the inbound translation, outbound
membership/key rewrites, identities, the token endpoint. Your host delivers
the same raw events in every mode; in `state_events` it also honours the
`stateKey` of `sendDelayedEvent`, because the dead man's switch is a state
event there.

**Both sides of a call must use the same mode.** A mismatch is a silence, not
an error: identities derive differently, so the peer sits in the roster with
no streams and its keys bind to nothing.

## Rules of the road

- **Calls may overlap; frees may not.** Calls on the client, a room and a
  call can be in flight together — each serialises inside Rust. Freeing an
  object while one of its calls is pending throws, as does calling the media
  session while its `disconnect()` runs.
- **Plain objects, not ES `Map`s**, for everything you hand the binding
  (the binding already guarantees the reverse direction).
- The `member.id` is generated per join (`rtcCall.memberId`) — never supply
  or reuse one.
- Frame E2EE on the web means a **per-participant** key provider and
  `room.setE2EEEnabled(true)`; `MatrixRtcCall` does both (livekit-js's
  `ExternalE2EEKeyProvider` is shared-key — wrong for MSC4195).
- The generated `.d.ts` carries **real TypeScript types** for every value
  crossing the boundary (`MatrixBackendHost`, `EventIn`, `JoinParamsIn`, `RtcParticipant`,
  the `RtcCallEvent` union, ...). They are hand-written in `src/ts_types.rs`
  and must be updated alongside the serde structs they describe;
  `web/test/typecheck.test.mjs` type-checks them through a consumer.

## Testing

- `wasm-pack test --node crates/matrix-rtc-wasm` — the binding's own tests.
- `cd web && npm run test:vitest` — the wrapper and roster against fakes.
- `make test-interop` — the real thing: this binding in a browser sharing
  encrypted calls with the native stack and with Element Call (both
  dialects), against a real homeserver and SFU. See `interop/README.md`.
