/*
 * Copyright 2026 Element Creations Ltd.
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
 * Please see LICENSE in the repository root for full details.
 */

// Demo: how a web host drives the participation facade.
//
// Tiles come from the memberships listener, LiveKit rooms to hold from the
// transports listener, keys from the key-map listener, the banner from the
// status listener. The host here is the TS mock (a fake homeserver with
// simulated peers); a matrix-js-sdk host is a later step. A real app would
// also hold LiveKit Room objects — here they are rendered as text.
import { initAsync } from "./index.js";
import { installConsoleLogSink } from "./log-sink.js";
import {
  FfiMembershipState,
  FfiRtcTransport,
  FfiStatus,
  FfiTransportIntent,
  RtcSessionManager,
  heartbeatIntervalMs,
  type FfiMediaKey,
  type FfiMembership,
  type FfiTransportWithMembers,
  type Participation,
} from "./index.js";
import {
  LK_SERVICE_URL,
  MockHost,
  OWN_DEVICE_ID,
  OWN_USER_ID,
  ROOM_ID,
  SLOT_ID,
  type RemotePeer,
} from "./testing/mock-host.js";

const $ = <T extends HTMLElement = HTMLElement>(id: string) => document.getElementById(id) as T;
const logError = (e: unknown) => {
  $("errors").textContent = `${new Date().toISOString().slice(11, 19)} ${String(e)}\n` + $("errors").textContent;
  console.error(e);
};
const guarded = (f: () => void | Promise<void>) => async () => {
  try {
    await f();
  } catch (e) {
    logError(e);
  }
};
const jsonish = (v: unknown) => JSON.stringify(v, (_k, x) => (typeof x === "bigint" ? x.toString() : x), 2);

/** What the UI needs from whichever host is active. */
interface Backend {
  host: MockHost;
  roomId: string;
  slotId: string;
  userId: string;
  deviceId: string;
  lkServiceUrl: string;
  describe: string;
}

let backend: Backend | null = null;
let manager: RtcSessionManager | null = null;
let participation: Participation | null = null;
let heartbeat: ReturnType<typeof setInterval> | null = null;
let keyMap: FfiMediaKey[] = [];
let memberships: FfiMembership[] = [];

function renderTiles() {
  const tiles = $("tiles");
  const keysFor = (memberId: string) =>
    keyMap.filter((k) => k.memberId === memberId).map((k) => `#${k.keyIndex}`).join(" ") || "none";
  tiles.replaceChildren(
    ...memberships.map((m) => {
      const tile = document.createElement("div");
      tile.className = "tile" + (m.state === FfiMembershipState.LeftWithKeys ? " leaving" : "") + (m.isOwn ? " me" : "");
      const initial = m.userId.replace(/^@/, "")[0] ?? "?";
      const keyState = m.mediaKey
        ? `they ${m.mediaKey.holdsOurKey ? "hold" : "lack"} ours · we ${m.mediaKey.haveTheirKey ? "hold" : "lack"} theirs${m.mediaKey.rejection ? ` · refused: ${m.mediaKey.rejection.tag}` : ""}`
        : "—";
      const rows: [string, string][] = [
        ["state", m.state === FfiMembershipState.LeftWithKeys ? "left — may still hold our key" : "in call"],
        ["member id", m.memberId],
        ["user", m.userId],
        ["device", `${m.deviceId ?? "?"} (${m.attribution})`],
        ["application", m.application ?? "?"],
        ["publishes on", m.publishedTransports.map(describeTransport).join(", ") || "nothing (receive-only)"],
        ["can subscribe", m.canSubscribe.join(", ") || "—"],
        ["LK identity", m.transportIdentity ?? "—"],
        ["media keys", keyState],
        ["keys held", keysFor(m.memberId)],
      ];
      if (m.membershipTs) rows.push(["joined", new Date(Number(m.membershipTs)).toLocaleTimeString()]);
      tile.innerHTML = `
        <div class="avatar">${initial.toUpperCase()}</div>
        <div class="name">${m.userId}${m.isOwn ? " <span class='you'>(you)</span>" : ""}</div>
        <dl>${rows.map(([k, v]) => `<dt>${k}</dt><dd>${v}</dd>`).join("")}</dl>`;
      return tile;
    }),
  );
}

function describeTransport(t: FfiRtcTransport): string {
  return FfiRtcTransport.LiveKit.instanceOf(t) ? t.inner.livekitServiceUrl : `${t.inner.transportType} (unsupported)`;
}

function renderTransports(transports: FfiTransportWithMembers[]) {
  // a real app would diff its LiveKit rooms here: connect new service URLs,
  // mint a token for each (the transport type says how), drop gone ones
  $("transports").textContent = transports.length
    ? transports.map((t) => `${describeTransport(t.transport)}\n  members: ${t.memberIds.join(", ") || "none"}`).join("\n")
    : "(none)";
}

function renderKeyMap() {
  $("keymap").textContent = keyMap.length
    ? keyMap.map((k) => `${k.memberId} #${k.keyIndex}: ${k.key.byteLength} bytes`).join("\n")
    : "(empty)";
}

function setStatus(text: string) {
  $("status").textContent = text;
}

async function refreshDebug() {
  if (participation) $("debug").textContent = jsonish(JSON.parse(await participation.debugSnapshot()));
}

function stopHeartbeat() {
  if (heartbeat) clearInterval(heartbeat);
  heartbeat = null;
}

async function teardown() {
  stopHeartbeat();
  if (participation) {
    try {
      if (!FfiStatus.Disconnected.instanceOf(await participation.status())) await participation.leave({ code: "leave", reason: undefined });
    } catch (e) {
      logError(e);
    }
    await participation.clearListeners();
    participation.uniffiDestroy();
    participation = null;
  }
  manager?.uniffiDestroy();
  manager = null;
  backend = null;
  memberships = [];
  keyMap = [];
  renderTiles();
  renderTransports([]);
  renderKeyMap();
  $("outbound").textContent = "";
}

async function start(b: Backend) {
  backend = b;
  $("backend-info").textContent = b.describe;
  manager = new RtcSessionManager(b.host);
  await b.host.attach(manager);
  // The generated interface type; the concrete class is what `participation()` returns.
  const p = manager.participation(b.roomId, b.slotId, b.userId, b.deviceId) as Participation;
  participation = p;
  await p.setMembershipsListener({
    onMembershipsChange: (m) => {
      memberships = m;
      renderTiles();
    },
  });
  await p.setTransportsListener({ onTransportsChange: renderTransports });
  await p.setKeyMapListener({
    onKeyMapChange: (map) => {
      // a real app: lkRooms[..].setKey(identity, key, index) for each new entry
      keyMap = map;
      renderKeyMap();
      renderTiles();
    },
  });
  await p.setStatusListener({
    onStatusChange: (status) => {
      const impairments = FfiStatus.Disconnected.instanceOf(status) ? [] : status.inner.impairments;
      const problems = impairments.length ? ` · ⚠ ${impairments.map((i) => i.tag).join(", ")}` : "";
      setStatus(`${status.tag}${problems}`);
      $("status-detail").textContent = jsonish(status);
      void refreshDebug();
    },
  });
}

function mockBackend(): Backend {
  const host = new MockHost();
  const encrypted = $<HTMLInputElement>("mock-encrypted").checked;
  host.encrypted = encrypted;
  host.slotEncrypted = encrypted;
  host.refuseDelayedEvents = $<HTMLInputElement>("mock-refuse-delayed").checked;
  host.onOutbound = (call) => {
    $("outbound").textContent = `${JSON.stringify(call, (_k, v) => (typeof v === "bigint" ? v.toString() : v))}\n` + $("outbound").textContent;
  };
  return {
    host,
    roomId: ROOM_ID,
    slotId: SLOT_ID,
    userId: OWN_USER_ID,
    deviceId: OWN_DEVICE_ID,
    lkServiceUrl: LK_SERVICE_URL,
    describe: `mock homeserver · ${encrypted ? "encrypted" : "unencrypted"} room · slot ${SLOT_ID} open`,
  };
}

async function main() {
  await initAsync();
  installConsoleLogSink();
  setStatus("wasm loaded — start the mock");

  const joinIntent = () =>
    $<HTMLSelectElement>("intent").value === "publish"
      ? new FfiTransportIntent.Publish({ transport: new FfiRtcTransport.LiveKit({ livekitServiceUrl: backend!.lkServiceUrl }) })
      : new FfiTransportIntent.ReceiveOnly({ canSubscribe: ["livekit"] });

  $("mock-start").onclick = guarded(async () => {
    await teardown();
    await start(mockBackend());
    $("mock-controls").hidden = false;
  });

  $("join").onclick = guarded(async () => {
    if (!participation) throw new Error("start the mock first");
    await participation.join(joinIntent(), {
      application: "m.call",
      memberId: undefined,
      stickyDurationMs: 240_000n,
      keepAliveTimeoutMs: BigInt($<HTMLInputElement>("keep-alive").value || "15000"),
      degradedLifetimeMs: undefined,
      encryption: undefined,
    });
    // The host owns the cadence: nothing in the core arms a timer.
    stopHeartbeat();
    heartbeat = setInterval(() => void participation?.heartbeat().catch(logError), Number(heartbeatIntervalMs()));
  });
  $("leave").onclick = guarded(async () => {
    stopHeartbeat();
    await participation?.leave({ code: "leave", reason: undefined });
  });
  $("open-slot").onclick = guarded(async () => {
    await backend?.host.openSlot($<HTMLInputElement>("mock-encrypted").checked);
  });
  $("close-slot").onclick = guarded(async () => {
    await backend?.host.closeSlot();
  });

  // --- mock-only: simulated peers -----------------------------------------
  let peerCounter = 0;
  const peers: RemotePeer[] = [];
  const mock = () => {
    if (!backend) throw new Error("start the mock first");
    return backend.host;
  };
  $("peer-join").onclick = guarded(async () => {
    peerCounter += 1;
    const peer: RemotePeer = {
      userId: `@peer${peerCounter}:example.org`,
      deviceId: `PEERDEV${peerCounter}`,
      memberId: `m-peer-${peerCounter}`,
      key: new Uint8Array(32).fill(peerCounter),
      autoReply: $<HTMLInputElement>("simulate-keys").checked,
    };
    peers.push(peer);
    mock().addPeer(peer);
    await mock().peerJoins(peer);
  });
  $("peer-leave").onclick = guarded(async () => {
    const peer = peers.pop();
    if (peer) await mock().peerLeaves(peer);
  });
  $("peer-key").onclick = guarded(async () => {
    const peer = peers.at(-1);
    if (peer) await mock().peerSendsKey(peer, Number($<HTMLInputElement>("peer-key-index").value || "0"));
  });
  $("mock-refuse-delayed").onchange = () => {
    if (backend) backend.host.refuseDelayedEvents = $<HTMLInputElement>("mock-refuse-delayed").checked;
  };

  setInterval(() => void refreshDebug(), 2000);
}

main().catch(logError);
