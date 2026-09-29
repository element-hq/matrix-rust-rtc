/*
 * Copyright 2026 Element Creations Ltd.
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
 * Please see LICENSE in the repository root for full details.
 */

// A host implemented in TypeScript, the same seam a matrix-js-sdk-backed
// host implements: it answers the six outbound commands like a homeserver
// that accepts everything, records every call for assertions, **echoes**
// accepted sticky events back into the manager (as sync would, so our own
// membership reaches the roster like anybody else's), and hosts simulated
// remote peers that answer our media key with theirs.
//
// Inputs are push-in: the mock owns the room's sticky map and pushes the
// whole map on every change — a partial push reads as leaves. It never calls
// back into the manager synchronously from inside a command callback; every
// push is scheduled on a macrotask, after the outbound promise resolved.
import {
  CommandSenderError,
  type FfiReceivedEncryptionKey,
  type FfiSlotEvent,
  type FfiStickyEvent,
  type FfiToDeviceDelivery,
  type FfiToDeviceRecipient,
  type RtcCommandSenderCallback,
  type RtcSessionManager,
} from "../generated/matrix_rtc.js";

export const LK_SERVICE_URL = "https://lk.example.org";
export const ROOM_ID = "!room:example.org";
// MSC4143: a slot id is `{application_type}#{id}`; a bare "m.call" resolves closed.
export const SLOT_ID = "m.call#ROOM";
export const OWN_USER_ID = "@me:example.org";
export const OWN_DEVICE_ID = "MYDEV";

export type OutboundCall =
  | { kind: "stickyEvent"; roomId: string; eventType: string; content: any; durationMs: bigint; eventId: string }
  | { kind: "delayedEvent"; roomId: string; eventType: string; content: any; delayMs: bigint; delayId: string }
  | { kind: "restartDelayed"; roomId: string; delayId: string }
  | { kind: "cancelDelayed"; roomId: string; delayId: string }
  | { kind: "toDevice"; recipients: FfiToDeviceRecipient[]; eventType: string; content: any }
  | { kind: "stateEvent"; roomId: string; eventType: string; stateKey: string; content: any };

/** A simulated remote participant. */
export interface RemotePeer {
  userId: string;
  deviceId: string;
  memberId: string;
  /** 32 key bytes; defaults to a constant pattern. */
  key?: Uint8Array;
  /** Answer our media key with theirs (default `true`). */
  autoReply?: boolean;
}

/** One entry of the mock's sticky map. */
interface StickyRecord {
  eventId: string;
  sender: string;
  deviceId: string;
  eventType: string;
  content: any;
}

const DEFAULT_KEY = new Uint8Array(32).fill(7);

function base64(bytes: Uint8Array): string {
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary);
}

/** One macrotask tick: listener callbacks and scheduled echoes land after it. */
export const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

export async function waitFor(what: string, cond: () => boolean | Promise<boolean>, timeoutMs = 3000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await cond())) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for: ${what}`);
    await new Promise((r) => setTimeout(r, 10));
  }
}

export class MockHost implements RtcCommandSenderCallback {
  readonly outbound: OutboundCall[] = [];
  /** Optional observer so a demo can render the outbound log live. */
  onOutbound?: (call: OutboundCall) => void;
  /** Refuse delayed events like a homeserver without MSC4140. */
  refuseDelayedEvents = false;
  /** Echo accepted sticky events back a tick later (as sync would). */
  autoEcho = true;
  /** Simulated peers; those with `autoReply` answer our key with theirs. */
  readonly peers: RemotePeer[] = [];

  // Room state the mock owns, pushed into the manager by `attach()` and on
  // every change.
  encrypted = false;
  slotOpen = true;
  slotEncrypted = false;
  roomMembers: string[] | undefined;

  private manager?: RtcSessionManager;
  private readonly sticky = new Map<string, StickyRecord>();
  private nextEventId = 0;
  private nextDelayId = 0;
  private pendingEcho?: Promise<void>;

  constructor(
    readonly ownUserId = OWN_USER_ID,
    readonly ownDeviceId = OWN_DEVICE_ID,
  ) {}

  /** Wires the manager up and pushes the current room state and sticky map. */
  async attach(manager: RtcSessionManager): Promise<void> {
    this.manager = manager;
    await this.pushRoomState();
    await this.pushSticky();
  }

  calls<K extends OutboundCall["kind"]>(kind: K): Extract<OutboundCall, { kind: K }>[] {
    return this.outbound.filter((c) => c.kind === kind) as Extract<OutboundCall, { kind: K }>[];
  }

  private record(call: OutboundCall) {
    this.outbound.push(call);
    this.onOutbound?.(call);
  }

  private eventId(): string {
    return `$echo-${this.nextEventId++}`;
  }

  // --- RtcCommandSenderCallback ---------------------------------------------

  async sendStickyEvent(roomId: string, eventType: string, contentJson: string, durationMs: bigint): Promise<string> {
    const content = JSON.parse(contentJson);
    const eventId = this.eventId();
    this.record({ kind: "stickyEvent", roomId, eventType, content, durationMs, eventId });
    if (eventType.endsWith("rtc.member")) {
      // The homeserver keeps one entry per sticky key; a leave replaces the
      // join and drops out of the roster when applied.
      this.sticky.set(content.msc4354_sticky_key, {
        eventId,
        sender: this.ownUserId,
        deviceId: this.ownDeviceId,
        eventType,
        content,
      });
      if (this.autoEcho) this.scheduleEcho();
    }
    return eventId;
  }

  async sendDelayedEvent(roomId: string, eventType: string, contentJson: string, delayMs: bigint): Promise<string> {
    const delayId = `delay-${this.nextDelayId++}`;
    this.record({ kind: "delayedEvent", roomId, eventType, content: JSON.parse(contentJson), delayMs, delayId });
    if (this.refuseDelayedEvents) {
      // 404 M_UNRECOGNIZED: "this homeserver will never do delayed events".
      throw new CommandSenderError.NotSupported("M_UNRECOGNIZED: delayed events are not supported");
    }
    return delayId;
  }

  async restartDelayedEvent(roomId: string, delayId: string): Promise<void> {
    this.record({ kind: "restartDelayed", roomId, delayId });
  }

  async cancelDelayedEvent(roomId: string, delayId: string): Promise<void> {
    this.record({ kind: "cancelDelayed", roomId, delayId });
  }

  async sendToDeviceMessage(
    recipients: FfiToDeviceRecipient[],
    eventType: string,
    contentJson: string,
  ): Promise<FfiToDeviceDelivery[]> {
    this.record({ kind: "toDevice", recipients, eventType, content: JSON.parse(contentJson) });
    // Simulated peers answer with their own key, after this call returned.
    for (const recipient of recipients) {
      const peer = this.peers.find((p) => p.userId === recipient.userId && p.deviceId === recipient.deviceId);
      if (peer && peer.autoReply !== false) setTimeout(() => void this.peerSendsKey(peer, 0), 0);
    }
    return recipients.map((recipient) => ({ userId: recipient.userId, deviceId: recipient.deviceId, error: undefined }));
  }

  async sendStateEvent(roomId: string, eventType: string, stateKey: string, contentJson: string): Promise<string> {
    const content = JSON.parse(contentJson);
    this.record({ kind: "stateEvent", roomId, eventType, stateKey, content });
    if (eventType.endsWith("rtc.slot") && stateKey === SLOT_ID) {
      this.slotOpen = content.status === "open";
      this.slotEncrypted = content.encryption?.type === "m.per_member";
      setTimeout(() => void this.pushSlots(), 0);
    }
    return this.eventId();
  }

  // --- pushes ----------------------------------------------------------------

  private require(): RtcSessionManager {
    if (!this.manager) throw new Error("MockHost.attach(manager) first");
    return this.manager;
  }

  /** Pushes the whole sticky map, as a host feeding the room's current state does. */
  async pushSticky(): Promise<void> {
    // Always attributed to a device, whether or not the room is encrypted:
    // the mock stands in for an SDK that decrypted the event, and a member
    // without a device could not be sent a key at all.
    const events: FfiStickyEvent[] = [...this.sticky.values()].map((record) => ({
      roomId: ROOM_ID,
      eventId: record.eventId,
      sender: record.sender,
      senderDeviceId: record.deviceId,
      wasEncrypted: true,
      eventType: record.eventType,
      contentJson: JSON.stringify(record.content),
    }));
    await this.require().setCurrentStickyState(ROOM_ID, events);
  }

  /** Encryption, slots, members — the order a real host feeds them in. */
  async pushRoomState(): Promise<void> {
    const manager = this.require();
    await manager.onRoomEncryptionReceived(ROOM_ID, this.encrypted);
    await this.pushSlots();
    if (this.roomMembers) await manager.onRoomMembersReceived(ROOM_ID, this.roomMembers);
  }

  private async pushSlots(): Promise<void> {
    const slots: FfiSlotEvent[] = this.slotOpen ? [slotEvent({ status: "open", encrypted: this.slotEncrypted })] : [];
    await this.require().onRoomSlotsReceived(ROOM_ID, slots);
  }

  private scheduleEcho() {
    if (this.pendingEcho) return;
    this.pendingEcho = new Promise((resolve) =>
      setTimeout(() => {
        this.pendingEcho = undefined;
        void this.pushSticky().then(resolve, resolve);
      }, 0),
    );
  }

  /** Delivers any pending echo now (or the current map if none is pending). */
  async echo(): Promise<void> {
    if (this.pendingEcho) await this.pendingEcho;
    else await this.pushSticky();
  }

  // --- room state controls -----------------------------------------------------

  async setEncrypted(encrypted: boolean): Promise<void> {
    this.encrypted = encrypted;
    await this.pushRoomState();
  }

  async openSlot(encrypted: boolean): Promise<void> {
    this.slotOpen = true;
    this.slotEncrypted = encrypted;
    await this.pushSlots();
  }

  async closeSlot(): Promise<void> {
    this.slotOpen = false;
    await this.pushSlots();
  }

  async setRoomMembers(userIds: string[]): Promise<void> {
    this.roomMembers = userIds;
    await this.require().onRoomMembersReceived(ROOM_ID, userIds);
  }

  // --- simulated peers -----------------------------------------------------------

  addPeer(peer: RemotePeer): RemotePeer {
    this.peers.push(peer);
    return peer;
  }

  /** The peer's join lands in the sticky map (on `LK_SERVICE_URL` unless given). */
  async peerJoins(peer: RemotePeer, opts: { lkServiceUrl?: string } = {}): Promise<void> {
    this.sticky.set(peer.memberId, {
      eventId: this.eventId(),
      sender: peer.userId,
      deviceId: peer.deviceId,
      eventType: "m.rtc.member",
      content: memberJoinContent({ memberId: peer.memberId, ...opts }),
    });
    await this.pushSticky();
  }

  /** The peer's entry leaves the sticky map (a leave event, or it expired). */
  async peerLeaves(peer: RemotePeer): Promise<void> {
    this.sticky.delete(peer.memberId);
    await this.pushSticky();
  }

  /** The peer sends us its key `index` as an Olm-encrypted to-device message. */
  async peerSendsKey(
    peer: RemotePeer,
    index: number,
    opts: { wasEncrypted?: boolean; senderDeviceId?: string; crossSigned?: boolean } = {},
  ): Promise<void> {
    const key: FfiReceivedEncryptionKey = {
      roomId: ROOM_ID,
      memberId: peer.memberId,
      keyB64: base64(peer.key ?? DEFAULT_KEY),
      keyIndex: index,
      wasEncrypted: opts.wasEncrypted ?? true,
      senderUserId: peer.userId,
      senderDeviceId: opts.senderDeviceId ?? peer.deviceId,
      senderIsCrossSigned: opts.crossSigned ?? true,
    };
    await this.require().receiveEncryptionKey(key);
  }
}

// ---------------------------------------------------------------------------
// Wire shapes the core reads (MSC4143 / MSC4354). Adjust here, not per test.
// ---------------------------------------------------------------------------

export function memberJoinContent(opts: { memberId: string; lkServiceUrl?: string }): any {
  return {
    slot_id: SLOT_ID,
    // MSC4354: the sticky key lives in the content and equals member.id.
    msc4354_sticky_key: opts.memberId,
    member: { id: opts.memberId, membership: "join" },
    application: { type: "m.call" },
    transports: {
      published: [{ type: "livekit", livekit_service_url: opts.lkServiceUrl ?? LK_SERVICE_URL }],
      can_subscribe: ["livekit"],
    },
  };
}

export function slotEvent(opts: { status: "open" | "closed"; encrypted?: boolean } = { status: "open" }): FfiSlotEvent {
  const content: any = { status: opts.status, application: { type: "m.call" } };
  if (opts.encrypted) content.encryption = { type: "m.per_member" };
  return { roomId: ROOM_ID, slotId: SLOT_ID, contentJson: JSON.stringify(content) };
}
