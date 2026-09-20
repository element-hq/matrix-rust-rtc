/*
Copyright 2026 Element Creations Ltd.

SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
Please see LICENSE in the repository root for full details.
*/

import { WebPeerApp } from './call.mjs';
import { installPeerApi } from './peer-api.mjs';

const params = new URLSearchParams(location.search);
const logElement = document.getElementById('log');

function log(line) {
  const stamped = `${new Date().toISOString().slice(11, 23)} ${line}`;
  console.log(`web-peer: ${line}`);
  logElement.append(stamped, document.createElement('br'));
  logElement.scrollTop = logElement.scrollHeight;
}

const app = new WebPeerApp({
  emit: (event) => log(JSON.stringify(event)),
  log,
  tilesElement: document.getElementById('tiles'),
});

if (params.get('test') === '1') {
  // Headless mode: Playwright drives everything through window.webPeer.
  document.getElementById('controls').hidden = true;
  installPeerApi(app, log);
} else {
  const field = (id) => document.getElementById(id);
  if (params.get('homeserver')) field('homeserver').value = params.get('homeserver');

  document.getElementById('joinBtn').addEventListener('click', async () => {
    field('joinBtn').disabled = true;
    try {
      // One session per page: logging in again with a blank User field would
      // register a NEW throwaway user — who is not invited to any room the
      // previous one created. Reload the page to switch users.
      if (!app.client) {
        await app.login({
          homeserver: field('homeserver').value,
          user: field('user').value || undefined,
          password: field('password').value || undefined,
        });
        field('user').value = app.userId;
      } else {
        log(`reusing session as ${app.userId} (reload the page to switch users)`);
      }
      let roomId = field('room').value.trim();
      if (roomId) {
        await app.joinRoom(roomId);
      } else {
        roomId = await app.createRoom({
          name: `Web call ${new Date().toISOString()}`,
          invite: field('invite').value.trim() || undefined,
          mode: field('mode').value,
        });
        field('room').value = roomId;
      }
      await app.join({
        roomId,
        compat: field('mode').value,
        publish: { devices: true },
      });
      field('leaveBtn').disabled = false;
    } catch (error) {
      log(`join failed: ${error}`);
      field('joinBtn').disabled = false;
    }
  });

  document.getElementById('leaveBtn').addEventListener('click', async () => {
    field('leaveBtn').disabled = true;
    try {
      await app.leave();
    } catch (error) {
      log(`leave failed: ${error}`);
    }
    field('joinBtn').disabled = false;
  });
}
