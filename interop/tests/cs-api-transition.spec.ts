/*
Copyright 2026 Element Creations Ltd.

SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
Please see LICENSE in the repository root for full details.
*/

/**
 * TEMPORARY (spec 049 R10): the move of LiveKit tokens from `/sfu/get` to the
 * homeserver's `get_token`, between our own two clients (web and native).
 * Both publish a transport naming both `url` and `livekit_service_url` in the
 * room-state dialect Element Call speaks (the MSC format reads only `url`), and
 * our homeserver's `get_token` counts as stable. Each scenario turns the local
 * flag on for one side, both, or neither, and checks the flag decides the
 * identity each side publishes under, and that both directions still see and
 * decrypt each other. Remove with the transition.
 */

import { expect, test } from "@playwright/test";

import { RustPeer } from "../helpers/rust-peer";
import { WebPeer } from "../helpers/web-peer";

const HOMESERVER_URL = process.env.HOMESERVER_URL ?? "https://synapse.m.localhost";
const SFU_URL = process.env.LIVEKIT_SFU_URL ?? "wss://matrix-rtc.m.localhost/livekit/sfu";
const SERVICE_URL =
  process.env.LIVEKIT_SERVICE_URL ?? "https://matrix-rtc.m.localhost/livekit/jwt";

/** `/sfu/get` mints `{user}:{device}`; `get_token` an unpadded base64 hash. */
function expectFlow(identity: string, getToken: boolean, who: string): void {
  if (getToken) {
    expect(identity, `${who} publishes under the hashed identity`).not.toContain("@");
  } else {
    expect(identity, `${who} publishes under {user}:{device}`).toMatch(/^@.+:.+:.+$/);
  }
}

const SCENARIOS = [
  { web: false, rust: false },
  { web: false, rust: true },
  { web: true, rust: false },
  { web: true, rust: true },
];

for (const flags of SCENARIOS) {
  test(`both flows announced — get_token on web: ${flags.web}, on rust: ${flags.rust}`, async ({
    browser,
  }, testInfo) => {
    test.slow();

    const web = await WebPeer.open(browser);
    await web.send({ cmd: "login", homeserver: HOMESERVER_URL });
    const webUser = (await web.waitFor("ready")).user_id as string;

    const peer = RustPeer.spawn({
      ELEMENT_CALL_COMPAT: "state",
      INVITE_USER: webUser,
      ROOM_NAME: `CS API transition ${Date.now().toString(16)}`,
      LIVEKIT_SFU_URL: SFU_URL,
      LIVEKIT_SERVICE_URL: SERVICE_URL,
      ...(flags.rust ? { LIVEKIT_GET_TOKEN: "1" } : {}),
      OUT_WAV: testInfo.outputPath("web-peer-audio.wav"),
      RUST_LOG:
        process.env.RUST_LOG ??
        "info,matrix_rtc_core=debug,matrix_rtc_livekit=debug,matrix_rtc_transport=debug,livekit=warn,webrtc_sys=warn",
    });

    try {
      const roomId = (await peer.waitFor("ready")).room_id as string;
      await web.send({ cmd: "join_room", roomId });
      await web.waitFor("room_joined");

      peer.send("join");
      const peerIdentity = (await peer.waitFor("joined", { timeout: 120_000 }))
        .identity as string;
      expectFlow(peerIdentity, flags.rust, "the Rust peer");

      await web.send({
        cmd: "join",
        roomId,
        compat: "room_state",
        transport: { kind: "publish", url: SFU_URL, livekit_service_url: SERVICE_URL },
        livekitGetToken: flags.web,
        publish: { pattern: true, tone: true },
      });
      const webIdentity = (await web.waitFor("joined", { timeout: 120_000 }))
        .identity as string;
      expectFlow(webIdentity, flags.web, "the web peer");

      // ---- the web client sees and decrypts the Rust client ----------------
      await web.waitFor("track_subscribed", {
        timeout: 90_000,
        predicate: (event) => event.kind === "audio" && event.identity === peerIdentity,
      });
      await web.waitFor("key_imported", {
        timeout: 90_000,
        predicate: (event) => event.identity === peerIdentity,
      });
      await web.waitFor("video_pattern", {
        timeout: 90_000,
        predicate: (event) =>
          event.identity === peerIdentity &&
          (event.left as number) - (event.right as number) >= 40,
      });

      // ---- the Rust client sees and decrypts the web client ----------------
      await peer.waitFor("track_subscribed", {
        timeout: 90_000,
        predicate: (event) => event.kind === "audio" && event.identity === webIdentity,
      });
      await peer.waitFor("key_imported", {
        timeout: 90_000,
        predicate: (event) => event.identity === webIdentity,
      });
      const audio = await peer.waitFor("audio_rms", { timeout: 120_000 });
      expect(audio.value as number).toBeGreaterThan(audio.floor as number);

      await web.send({ cmd: "leave" });
      await web.waitFor("left", { timeout: 60_000 });
      peer.send("leave");
      await peer.waitFor("left", { timeout: 60_000 });
    } finally {
      await web.dispose().catch(() => {});
      await peer.dispose().catch(() => {});
      await testInfo.attach("interop-peer.log", { body: peer.stderr });
      await testInfo.attach("web-peer.log", { body: web.log });
    }
  });
}
