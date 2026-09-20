/*
Copyright 2026 Element Creations Ltd.

SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
Please see LICENSE in the repository root for full details.
*/

/**
 * The test-driver surface: the web analogue of the rust interop peer's stdio
 * protocol (`interop_peer.rs`), for Playwright's `WebPeer` helper.
 * In: `window.webPeer.command(jsonString)` — `{cmd: "login"|"create_room"|
 * "join_room"|"join"|"leave", ...payload}`. Each command resolves after its
 * work completes; failures surface both as a rejected promise and an `error`
 * event. `create_room` takes `{name, invite?, mode?}` — the mode decides
 * whether a slot is opened (the pre-sticky generation has none).
 * Out: every observable event goes to `window.__onWebPeerEvent(jsonString)`
 * (a Playwright `exposeFunction`), and to the on-page log for humans.
 */

export function installPeerApi(app, log) {
  const emitToDriver = (event) => {
    const line = JSON.stringify(event);
    log(line);
    window.__onWebPeerEvent?.(line);
  };
  app.emit = emitToDriver;

  window.webPeer = {
    async command(jsonString) {
      const command = JSON.parse(jsonString);
      try {
        switch (command.cmd) {
          case 'login':
            return await app.login(command);
          case 'create_room':
            return await app.createRoom(command);
          case 'join_room':
            return await app.joinRoom(command.roomId);
          case 'join':
            return await app.join(command);
          case 'leave':
            return await app.leave();
          default:
            throw new Error(`unknown command: ${command.cmd}`);
        }
      } catch (error) {
        emitToDriver({ event: 'error', command: command.cmd, message: String(error) });
        throw error;
      }
    },
  };

  emitToDriver({ event: 'page_ready' });
}
