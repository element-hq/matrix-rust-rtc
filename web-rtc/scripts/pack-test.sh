#!/usr/bin/env bash
# Copyright 2026 Element Creations Ltd.
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
# Please see LICENSE in the repository root for full details.

# Installs the packed tarball into a scratch project and exercises the
# published surface the way a consumer would: the `exports` map, the `node`
# condition and the default wasm lookup in Node, and a Vite production build
# that must emit the .wasm as an asset. Needs network (pulls @ubjs/core and
# vite from the public registry).
set -euo pipefail
cd "$(dirname "$0")/.."

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
tgz="$(npm pack --pack-destination "$tmp" --silent | tail -1)"
echo "packed $tgz"

cd "$tmp"
npm init -y >/dev/null
npm install --silent --no-audit --no-fund "./$tgz"

echo "--- node: default wasm lookup through the node condition"
node --input-type=module -e '
  import { initAsync, isInitialized, heartbeatIntervalMs } from "@element-hq/matrix-rtc";
  import { installConsoleLogSink } from "@element-hq/matrix-rtc/log-sink";
  import { MockHost } from "@element-hq/matrix-rtc/testing";
  if (isInitialized()) throw new Error("initialised before initAsync");
  await initAsync();
  await initAsync(); // idempotent
  installConsoleLogSink();
  new MockHost();
  console.log("ok: initialised, heartbeat interval", heartbeatIntervalMs(), "ms");
'

echo "--- node: explicit source (bytes) works too"
node --input-type=module -e '
  import { readFile } from "node:fs/promises";
  import { createRequire } from "node:module";
  import { initAsync } from "@element-hq/matrix-rtc";
  const wasm = createRequire(import.meta.url).resolve("@element-hq/matrix-rtc/wasm");
  await initAsync(await readFile(wasm));
  console.log("ok: initialised from bytes at", wasm);
'

echo "--- vite: production build emits the wasm as an asset"
npm install --silent --no-audit --no-fund -D vite
mkdir -p app
cat > app/index.html <<'HTML'
<!doctype html><html><body><script type="module" src="./main.js"></script></body></html>
HTML
cat > app/main.js <<'JS'
import { initAsync, heartbeatIntervalMs } from "@element-hq/matrix-rtc";
await initAsync();
document.body.textContent = String(heartbeatIntervalMs());
JS
npx vite build --logLevel warn app --outDir ../out --emptyOutDir >/dev/null
ls out/assets/*.wasm >/dev/null
echo "ok: vite emitted $(ls out/assets/*.wasm)"
