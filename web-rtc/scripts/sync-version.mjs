/*
 * Copyright 2026 Element Creations Ltd.
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
 * Please see LICENSE in the repository root for full details.
 */

// Copies the workspace version from ../Cargo.toml into package.json. The
// package is a build of the crate, so it has no version of its own.
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const pkgDir = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const workspaceToml = readFileSync(resolve(pkgDir, "..", "Cargo.toml"), "utf8");
const version = workspaceToml.match(/^\[workspace\.package\][^[]*?^version = "([^"]+)"/ms)?.[1];
if (!version) throw new Error("no [workspace.package] version in ../Cargo.toml");

const pkgPath = resolve(pkgDir, "package.json");
const pkg = JSON.parse(readFileSync(pkgPath, "utf8"));
if (pkg.version === version) {
  console.log(`package.json already at ${version}`);
} else {
  console.log(`package.json ${pkg.version} -> ${version}`);
  pkg.version = version;
  writeFileSync(pkgPath, JSON.stringify(pkg, null, 2) + "\n");
}
