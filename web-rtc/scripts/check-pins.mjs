/*
 * Copyright 2026 Element Creations Ltd.
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
 * Please see LICENSE in the repository root for full details.
 */

// Fails the build when any of the coupled versions drift. The generated
// bindings call into `@ubjs/core` internals and the wasm-bindgen schema must
// match the CLI that produced the glue, so these are hard errors, not
// warnings.
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const pkgDir = resolve(here, "..");
const repoRoot = resolve(pkgDir, "..");
const require = createRequire(import.meta.url);

const read = (p) => readFileSync(p, "utf8");
const must = (re, text, what) => {
  const m = text.match(re);
  if (!m) throw new Error(`could not find ${what}`);
  return m[1];
};

const pkg = JSON.parse(read(resolve(pkgDir, "package.json")));
const ubrnConfig = read(resolve(pkgDir, "ubrn.config.yaml"));
const crateManifest = must(/^\s*manifestPath:\s*(\S+)/m, ubrnConfig, "manifestPath in ubrn.config.yaml");
const crateToml = read(resolve(repoRoot, crateManifest));
const workspaceToml = read(resolve(repoRoot, "Cargo.toml"));
const patchToml = read(resolve(pkgDir, "Cargo.patch.toml"));

const ubrnDir = dirname(require.resolve("uniffi-bindgen-react-native/package.json"));
const ubrnPkg = JSON.parse(read(resolve(ubrnDir, "package.json")));
// The npm tarball ships ubrn's workspace Cargo.toml but no Cargo.lock, so the
// versions ubrn was built against come from its `[workspace.dependencies]`.
const ubrnToml = read(resolve(ubrnDir, "Cargo.toml"));
const ubrnDep = (name) =>
  must(new RegExp(`^${name} = "=([^"]+)"`, "m"), ubrnToml, `${name} pin in ubrn's Cargo.toml`);
// ubrn pins `uniffi = "=0.31"`, i.e. any 0.31.x: compare on as many components
// as the shorter side states.
const samePrefix = (a, b) => {
  const n = Math.min(a.split(".").length, b.split(".").length);
  return a.split(".").slice(0, n).join(".") === b.split(".").slice(0, n).join(".");
};

const workspaceVersion = must(/^\[workspace\.package\][^[]*?^version = "([^"]+)"/ms, workspaceToml, "workspace version");
// One pin per target table; they must all agree.
const uniffiPins = [...crateToml.matchAll(/^uniffi = (?:"=([^"]+)"|\{ version = "=([^"]+)")/gm)].map((m) => m[1] ?? m[2]);
if (uniffiPins.length === 0) throw new Error("no `uniffi = \"=x.y.z\"` pin in the crate manifest");
if (new Set(uniffiPins).size !== 1) throw new Error(`the uniffi pins disagree: ${uniffiPins.join(", ")}`);
const crateUniffi = uniffiPins[0];
const patchWasmBindgen = must(/wasm-bindgen = "=([^"]+)"/, patchToml, "wasm-bindgen pin in Cargo.patch.toml");

const checks = [
  ["package.json version", pkg.version, "workspace Cargo.toml version", workspaceVersion],
  ["dependencies[@ubjs/core]", pkg.dependencies?.["@ubjs/core"], "devDependencies[uniffi-bindgen-react-native]", pkg.devDependencies?.["uniffi-bindgen-react-native"]],
  ["devDependencies[uniffi-bindgen-react-native]", pkg.devDependencies?.["uniffi-bindgen-react-native"], "installed ubrn", ubrnPkg.version],
  ["crate uniffi", crateUniffi, "ubrn's uniffi", ubrnDep("uniffi"), samePrefix],
  ["Cargo.patch.toml wasm-bindgen", patchWasmBindgen, "ubrn's wasm-bindgen-cli-support", ubrnDep("wasm-bindgen-cli-support")],
];

let failed = false;
for (const [aName, a, bName, b, eq = (x, y) => x === y] of checks) {
  const ok = a !== undefined && b !== undefined && eq(a, b);
  console.log(`${ok ? "ok  " : "FAIL"} ${aName} = ${a}  ${ok ? "==" : "!="}  ${bName} = ${b}`);
  if (!ok) failed = true;
}

if (failed) {
  console.error("\nversion pins drifted — bump them together (see README, 'Gotchas')");
  process.exit(1);
}
