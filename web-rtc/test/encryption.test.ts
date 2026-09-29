/*
 * Copyright 2026 Element Creations Ltd.
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
 * Please see LICENSE in the repository root for full details.
 */

// Media key exchange through the wasm bindings: distribution to members,
// verification of inbound keys, the per-tile key state, and leaving.
import { beforeAll, describe, expect, it } from "vitest";
import { FfiEncryptionStatus, FfiMembershipState, FfiStatus, type FfiMediaKey } from "@element-hq/matrix-rtc";
import { waitFor } from "@element-hq/matrix-rtc/testing";
import { joinParams, newParticipation, receiveOnly } from "./helpers";
import { initWasm } from "./wasmInit";

beforeAll(async () => {
  await initWasm();
});

const peerA = { userId: "@a:example.org", deviceId: "ADEV", memberId: "m-a", key: new Uint8Array(32).fill(1) };
const peerB = { userId: "@b:example.org", deviceId: "BDEV", memberId: "m-b", key: new Uint8Array(32).fill(2) };

describe("encryption", () => {
  it("joining an encrypted call sends our key to every member's device, and they answer", async () => {
    const { host, participation } = await newParticipation({ encrypted: true });
    await host.peerJoins(host.addPeer(peerA));
    await host.peerJoins(host.addPeer(peerB));
    await participation.join(receiveOnly(), joinParams);

    const batch = host.calls("toDevice")[0];
    expect(batch.eventType).toBe("org.matrix.msc4143.rtc.encryption_key");
    expect(batch.recipients).toEqual(
      expect.arrayContaining([
        { userId: peerA.userId, deviceId: peerA.deviceId },
        { userId: peerB.userId, deviceId: peerB.deviceId },
      ]),
    );
    expect(batch.content.media_key.index).toBe(0);

    // the peers answered: three key rings
    await waitFor("all keys", async () => new Set((await participation.keyMap()).map((k) => k.memberId)).size === 3);
    const status = await participation.status();
    if (!FfiStatus.Connected.instanceOf(status)) throw new Error("expected Connected");
    expect(FfiEncryptionStatus.Connected.instanceOf(status.inner.encryption)).toBe(true);
    const tile = (await participation.memberships()).find((m) => m.memberId === peerA.memberId)!;
    expect(tile.mediaKey).toEqual({ holdsOurKey: true, haveTheirKey: true, rejection: undefined });
  });

  it("a remote key that passes verification lands in the key map and the listener", async () => {
    const { host, participation } = await newParticipation({ encrypted: true });
    const maps: FfiMediaKey[][] = [];
    await participation.setKeyMapListener({ onKeyMapChange: (map) => maps.push(map) });
    await participation.join(receiveOnly(), joinParams);
    await host.peerJoins(peerA); // not a simulated peer: no automatic reply
    const before = maps.length;

    await host.peerSendsKey(peerA, 0);

    const key = (await participation.keyMap()).find((k) => k.memberId === peerA.memberId)!;
    expect(key).toBeDefined();
    expect(Array.from(new Uint8Array(key.key))).toEqual(Array.from(peerA.key));
    expect(key.keyIndex).toBe(0);
    expect(maps).toHaveLength(before + 1);
    expect(maps.at(-1)!.some((k) => k.memberId === peerA.memberId)).toBe(true);
  });

  it("a cleartext key, or one from the wrong device, is refused and shows on the tile", async () => {
    const { host, participation } = await newParticipation({ encrypted: true });
    await participation.join(receiveOnly(), joinParams);
    await host.peerJoins(peerA);

    await host.peerSendsKey(peerA, 0, { wasEncrypted: false });
    expect((await participation.keyMap()).some((k) => k.memberId === peerA.memberId)).toBe(false);
    let tile = (await participation.memberships()).find((m) => m.memberId === peerA.memberId)!;
    expect(tile.mediaKey?.rejection?.tag).toBe("Cleartext");
    let status = await participation.status();
    if (!FfiStatus.Connected.instanceOf(status)) throw new Error("expected Connected");
    expect(status.inner.impairments.map((i) => i.tag)).toContain("MediaKeyRejected");

    await host.peerSendsKey(peerA, 0, { senderDeviceId: "OTHERDEV" });
    expect((await participation.keyMap()).some((k) => k.memberId === peerA.memberId)).toBe(false);
    tile = (await participation.memberships()).find((m) => m.memberId === peerA.memberId)!;
    expect(tile.mediaKey?.rejection?.tag).toBe("DeviceMismatch");

    await host.peerSendsKey(peerA, 0);
    tile = (await participation.memberships()).find((m) => m.memberId === peerA.memberId)!;
    expect(tile.mediaKey).toEqual({ holdsOurKey: true, haveTheirKey: true, rejection: undefined });
    status = await participation.status();
    if (!FfiStatus.Connected.instanceOf(status)) throw new Error("expected Connected");
    expect(status.inner.impairments).toEqual([]);
  });

  it("a key for an unknown member is held until its membership arrives", async () => {
    const { host, participation } = await newParticipation({ encrypted: true });
    await participation.join(receiveOnly(), joinParams);
    await host.peerSendsKey(peerA, 0);
    expect((await participation.keyMap()).some((k) => k.memberId === peerA.memberId)).toBe(false);
    await host.peerJoins(peerA);
    expect((await participation.keyMap()).some((k) => k.memberId === peerA.memberId)).toBe(true);
  });

  it("a member who left while holding our key stays listed as LeftWithKeys", async () => {
    const { host, participation } = await newParticipation({ encrypted: true });
    await host.peerJoins(host.addPeer(peerA));
    await participation.join(receiveOnly(), joinParams);
    await waitFor("keys", async () => (await participation.keyMap()).length === 2);

    await host.peerLeaves(peerA);
    const gone = (await participation.memberships()).find((m) => m.memberId === peerA.memberId);
    expect(gone?.state).toBe(FfiMembershipState.LeftWithKeys);
    const status = await participation.status();
    if (!FfiStatus.Connected.instanceOf(status)) throw new Error("expected Connected");
    if (!FfiEncryptionStatus.Connected.instanceOf(status.inner.encryption)) throw new Error("expected encryption Connected");
    expect(status.inner.encryption.inner.leftMembersWithKeys).toEqual([peerA.memberId]);
    expect(status.inner.encryption.inner.fullySettled).toBe(false);
  });

  it("unencrypted slot: no keys are sent, inbound keys are ignored, key state is absent", async () => {
    const { host, participation } = await newParticipation();
    await host.peerJoins(host.addPeer(peerA));
    await participation.join(receiveOnly(), joinParams);
    expect(host.calls("toDevice")).toEqual([]);
    await host.peerSendsKey(peerA, 0);
    expect(await participation.keyMap()).toEqual([]);
    const tile = (await participation.memberships()).find((m) => m.memberId === peerA.memberId)!;
    expect(tile.mediaKey).toBeUndefined();
    const status = await participation.status();
    if (!FfiStatus.Connected.instanceOf(status)) throw new Error("expected Connected");
    expect(FfiEncryptionStatus.NotManaged.instanceOf(status.inner.encryption)).toBe(true);
  });

  it("leaving forgets every key", async () => {
    const { host, participation } = await newParticipation({ encrypted: true });
    await host.peerJoins(host.addPeer(peerA));
    await participation.join(receiveOnly(), joinParams);
    await waitFor("keys", async () => (await participation.keyMap()).length === 2);
    await participation.leave({ code: undefined, reason: undefined });
    expect(await participation.keyMap()).toEqual([]);
  });
});
