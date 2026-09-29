/*
 * Copyright 2026 Element Creations Ltd.
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
 * Please see LICENSE in the repository root for full details.
 */

// The runtime assumptions the surface rests on under wasm, through the real
// bindings: an exported future awaits a foreign async callback that resolves
// later; `u64` round-trips as bigint; a listener lands during the push.
import { beforeAll, describe, expect, it } from "vitest";
import { heartbeatIntervalMs } from "@element-hq/matrix-rtc";
import { MockHost } from "@element-hq/matrix-rtc/testing";
import { joinParams, newParticipation, receiveOnly } from "./helpers";
import { initWasm } from "./wasmInit";

beforeAll(async () => {
  await initWasm();
});

describe("runtime", () => {
  it("a Rust future waits for a foreign promise that resolves on a timer", async () => {
    const { host, participation } = await newParticipation();
    const original = host.sendDelayedEvent.bind(host);
    let resolvedAt = 0;
    host.sendDelayedEvent = async (...args) => {
      await new Promise((r) => setTimeout(r, 20));
      resolvedAt = Date.now();
      return original(...args);
    };
    const started = Date.now();
    await participation.join(receiveOnly(), joinParams);
    expect(resolvedAt - started).toBeGreaterThanOrEqual(15);
    // the join went on only after the delayed leave was armed
    expect(host.outbound.map((c) => c.kind)).toEqual(["delayedEvent", "stickyEvent"]);
  });

  it("u64 crosses as bigint in both directions", async () => {
    const { host, participation } = await newParticipation();
    await participation.join(receiveOnly(), { ...joinParams, keepAliveTimeoutMs: 12_345n });
    expect(host.calls("delayedEvent")[0].delayMs).toBe(12_345n);
    expect(typeof heartbeatIntervalMs()).toBe("bigint");
  });

  it("the mock host never re-enters the manager from inside a callback", async () => {
    const { host, participation } = await newParticipation();
    // With auto-echo on, the sticky echo is scheduled, not delivered inline.
    expect(host instanceof MockHost).toBe(true);
    await participation.join(receiveOnly(), joinParams);
    expect((await participation.memberships()).length).toBe(0);
    await host.echo();
    expect((await participation.memberships()).length).toBe(1);
  });
});
