#!/usr/bin/env bash
# Copyright 2026 Element Creations Ltd.
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
# Please see LICENSE in the repository root for full details.

# Build the chaos_peer example for the peer containers and drop it in
# chaos/bin/, which the compose `peer` service mounts.
#
#   chaos/build-peer.sh            # native on Linux, in a container elsewhere
#   chaos/build-peer.sh --docker   # force the container build
#   CARGO_BUILD_JOBS=4 chaos/build-peer.sh   # leave CPU for other work
#
# The binary must be a Linux one matching the runtime image's glibc
# (ubuntu:24.04), so on macOS it is built in the Dockerfile's `builder` stage.
# That build keeps its target dir and cargo registry in named volumes: a
# bind-mounted target is slow on Docker Desktop, and would mix Linux artifacts
# into the host's target/.
set -euo pipefail

cd "$(dirname "$0")/.."

BUILD=(cargo build -p matrix-rtc-call-sdk --features matrix-sdk,testing --example chaos_peer)

mode=native
if [ "${1:-}" = "--docker" ] || [ "$(uname -s)" != "Linux" ]; then
    mode=docker
fi

mkdir -p chaos/bin
if [ "$mode" = native ]; then
    "${BUILD[@]}"
    cp "${CARGO_TARGET_DIR:-target}/debug/examples/chaos_peer" chaos/bin/chaos_peer
else
    docker build -q -t matrix-rtc-chaos-builder --target builder chaos/docker >/dev/null
    docker run --rm \
        -v "$PWD:/src" -w /src \
        -v matrix-rtc-chaos-target:/target \
        -v matrix-rtc-chaos-cargo:/usr/local/cargo/registry \
        -e CARGO_TARGET_DIR=/target \
        ${CARGO_BUILD_JOBS:+-e CARGO_BUILD_JOBS="$CARGO_BUILD_JOBS"} \
        matrix-rtc-chaos-builder \
        bash -c "$(printf '%q ' "${BUILD[@]}") && cp /target/debug/examples/chaos_peer chaos/bin/chaos_peer"
fi
# What it was built from, so the harness can tell a stale binary from a fresh
# one: switching branches does not rebuild it.
git rev-parse HEAD > chaos/bin/chaos_peer.rev
echo "[build-peer] chaos/bin/chaos_peer ready ($mode build of $(cut -c1-8 chaos/bin/chaos_peer.rev))"
