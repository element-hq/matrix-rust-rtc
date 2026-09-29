/*
 * Copyright 2026 Element Creations Ltd.
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
 * Please see LICENSE in the repository root for full details.
 */

// Acceptance tests of the participation facade through the wasm bindings,
// driven entirely through the TS mock host: room state is pushed in,
// outbound commands and the four outputs are asserted. Getters are fresh
// right after a push; listener callbacks run inside the push.
import { beforeAll, describe, expect, it } from "vitest";
import {
  FfiDisconnectCause,
  FfiKeepAlive,
  FfiMembershipState,
  FfiRosterPresence,
  FfiSeverity,
  FfiStatus,
  impairmentSeverity,
  type FfiMembership,
  type FfiTransportWithMembers,
} from "@element-hq/matrix-rtc";
import { LK_SERVICE_URL, OWN_USER_ID, ROOM_ID, tick, waitFor } from "@element-hq/matrix-rtc/testing";
import { joinParams, newParticipation, publishLk, receiveOnly, serviceUrl } from "./helpers";
import { initWasm } from "./wasmInit";

beforeAll(async () => {
  await initWasm();
});

const remote = { userId: "@remote:example.org", deviceId: "RDEV", memberId: "m-1", autoReply: false };

describe("Participation", () => {
  it("starts disconnected with nothing in it", async () => {
    const { participation } = await newParticipation();
    const status = await participation.status();
    if (!FfiStatus.Disconnected.instanceOf(status)) throw new Error("expected Disconnected");
    expect(FfiDisconnectCause.NeverJoined.instanceOf(status.inner.cause)).toBe(true);
    expect(await participation.memberships()).toEqual([]);
    expect(await participation.transports()).toEqual([]);
    expect(await participation.keyMap()).toEqual([]);
    expect(await participation.ownMemberId()).toBeUndefined();
  });

  it("a remote member join shows up as a Joined membership, and the listener hears it", async () => {
    const { host, participation } = await newParticipation();
    const changes: FfiMembership[][] = [];
    await participation.setMembershipsListener({ onMembershipsChange: (m) => changes.push(m) });
    expect(changes).toEqual([[]]); // installing replays the current value

    await host.peerJoins(remote);

    const memberships = await participation.memberships();
    expect(memberships).toHaveLength(1);
    expect(memberships[0].memberId).toBe("m-1");
    expect(memberships[0].userId).toBe(remote.userId);
    expect(memberships[0].deviceId).toBe("RDEV");
    expect(memberships[0].state).toBe(FfiMembershipState.Joined);
    expect(memberships[0].isOwn).toBe(false);
    expect(memberships[0].publishedTransports).toHaveLength(1);
    expect(changes.at(-1)).toEqual(memberships);

    // the same map again changes nothing
    await host.pushSticky();
    expect(changes).toHaveLength(2);
  });

  it("join arms the delayed leave before sending the membership, then goes Joining → Connected", async () => {
    const { host, participation } = await newParticipation();
    const statuses: FfiStatus[] = [];
    await participation.setStatusListener({ onStatusChange: (s) => statuses.push(s) });
    host.autoEcho = false;

    const memberId = await participation.join(receiveOnly(), joinParams);
    expect(memberId).toBeTruthy();
    expect(await participation.ownMemberId()).toBe(memberId);

    // outbound order: dead man's switch first, then the sticky join
    const kinds = host.outbound.map((c) => c.kind);
    const delayedAt = kinds.indexOf("delayedEvent");
    const stickyAt = kinds.indexOf("stickyEvent");
    expect(delayedAt).toBeGreaterThanOrEqual(0);
    expect(stickyAt).toBeGreaterThan(delayedAt);

    const delayed = host.calls("delayedEvent")[0];
    expect(delayed.delayMs).toBe(15_000n);
    expect(delayed.content.leave_reason.code).toBe("delayed_leave");
    expect(delayed.eventType).toBe("org.matrix.msc4143.rtc.member");

    const sticky = host.calls("stickyEvent")[0];
    expect(sticky.roomId).toBe(ROOM_ID);
    expect(sticky.eventType).toBe("org.matrix.msc4143.rtc.member");
    expect(sticky.durationMs).toBe(240_000n);
    expect(sticky.content.member.membership).toBe("join");
    expect(sticky.content.member.id).toBe(memberId);

    // Joining was announced, then Connected while our echo is still out
    expect(statuses.map((s) => s.tag)).toEqual(["Disconnected", "Joining", "Connected"]);
    const connected = statuses[2];
    if (!FfiStatus.Connected.instanceOf(connected)) throw new Error("expected Connected");
    expect(connected.inner.memberId).toBe(memberId);
    expect(FfiRosterPresence.AwaitingEcho.instanceOf(connected.inner.roster)).toBe(true);
    const armed = connected.inner.keepAlive;
    if (!FfiKeepAlive.Armed.instanceOf(armed)) throw new Error("expected Armed");
    expect(armed.inner.firesAtTs).toBe(armed.inner.lastRestartTs + armed.inner.delayMs);
    expect(connected.inner.membership.expiresAtTs).toBe(
      connected.inner.membership.lastPublishedTs + connected.inner.membership.lifetimeMs,
    );
    expect(connected.inner.impairments).toEqual([]);

    // the homeserver echoes our membership: now we are in the roster
    await host.echo();
    const status = await participation.status();
    if (!FfiStatus.Connected.instanceOf(status)) throw new Error("expected Connected");
    expect(FfiRosterPresence.Present.instanceOf(status.inner.roster)).toBe(true);
    const me = (await participation.memberships()).find((m) => m.userId === OWN_USER_ID);
    expect(me?.isOwn).toBe(true);
    expect(me?.memberId).toBe(memberId);
    expect(me?.transportIdentity).toBeTypeOf("string");
  });

  it("transports() lists each service URL with the members publishing on it", async () => {
    const { host, participation } = await newParticipation();
    const changes: FfiTransportWithMembers[][] = [];
    await participation.setTransportsListener({ onTransportsChange: (t) => changes.push(t) });

    await participation.join(publishLk(), joinParams);
    await host.echo();
    await host.peerJoins(remote);
    await host.peerJoins({ ...remote, memberId: "m-2", deviceId: "RDEV2", userId: "@other:example.org" }, {
      lkServiceUrl: "https://lk2.example.org",
    });

    const transports = await participation.transports();
    expect(transports).toHaveLength(2);
    const onDefault = transports.find((t) => serviceUrl(t.transport) === LK_SERVICE_URL);
    expect(onDefault?.memberIds).toEqual(expect.arrayContaining(["m-1", await participation.ownMemberId()]));
    expect(transports.some((t) => serviceUrl(t.transport) === "https://lk2.example.org")).toBe(true);
    expect(changes.at(-1)).toEqual(transports);
  });

  it("heartbeat restarts the delayed leave, and leave cancels it", async () => {
    const { host, participation } = await newParticipation();
    const statuses: FfiStatus[] = [];
    await participation.setStatusListener({ onStatusChange: (s) => statuses.push(s) });
    await participation.join(receiveOnly(), joinParams);
    await host.echo();

    expect(await participation.heartbeat()).toBe(true);
    const delayId = host.calls("delayedEvent")[0].delayId;
    expect(host.calls("restartDelayed").map((c) => c.delayId)).toEqual([delayId]);

    await participation.leave({ code: undefined, reason: undefined });
    expect(host.calls("cancelDelayed").map((c) => c.delayId)).toEqual([delayId]);
    const leave = host.calls("stickyEvent").at(-1)!;
    expect(leave.content.member.membership).toBe("leave");

    expect(statuses.map((s) => s.tag).slice(-2)).toEqual(["Leaving", "Disconnected"]);
    const last = await participation.status();
    if (!FfiStatus.Disconnected.instanceOf(last)) throw new Error("expected Disconnected");
    expect(FfiDisconnectCause.LeftByHost.instanceOf(last.inner.cause)).toBe(true);
    expect(await participation.keyMap()).toEqual([]);
    expect(await participation.heartbeat()).toBe(false);
  });

  it("a homeserver without delayed events degrades the keep-alive into an impairment", async () => {
    const { host, participation } = await newParticipation();
    host.refuseDelayedEvents = true;
    await participation.join(receiveOnly(), joinParams);

    const status = await participation.status();
    if (!FfiStatus.Connected.instanceOf(status)) throw new Error("expected Connected");
    expect(FfiKeepAlive.Unavailable.instanceOf(status.inner.keepAlive)).toBe(true);
    expect(status.inner.impairments.map((i) => i.tag)).toEqual(["KeepAliveUnavailable"]);
    expect(impairmentSeverity(status.inner.impairments[0])).toBe(FfiSeverity.Degraded);
  });

  it("closing the slot excludes our own membership", async () => {
    const { host, participation } = await newParticipation();
    await participation.join(receiveOnly(), joinParams);
    await host.echo();

    await host.closeSlot();
    const status = await participation.status();
    if (!FfiStatus.Connected.instanceOf(status)) throw new Error("expected Connected");
    expect(FfiRosterPresence.Excluded.instanceOf(status.inner.roster)).toBe(true);
    expect(status.inner.impairments.map((i) => i.tag)).toEqual(["OwnMembershipExcluded"]);
    expect(await participation.memberships()).toEqual([]);

    await host.openSlot(false);
    await waitFor("roster back", async () => (await participation.memberships()).length === 1);
  });

  it("listener callbacks land inside the push, on the next tick at the latest", async () => {
    const { host, participation } = await newParticipation();
    const seen: number[] = [];
    await participation.setMembershipsListener({ onMembershipsChange: (m) => seen.push(m.length) });
    const push = host.peerJoins(remote);
    await tick();
    await push;
    expect(seen).toEqual([0, 1]);
  });

  it("debugSnapshot is JSON without key material", async () => {
    const { participation } = await newParticipation({ encrypted: true });
    await participation.join(publishLk(), joinParams);
    const snapshot = JSON.parse(await participation.debugSnapshot());
    expect(snapshot.own_membership).toBe(await participation.ownMemberId());
    expect(snapshot.has_encryption_manager).toBe(true);
    expect(JSON.stringify(snapshot)).not.toContain("key\":");
  });
});
