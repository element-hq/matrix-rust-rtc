#!/usr/bin/env bash
# Copyright 2026 Element Creations Ltd.
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
# Please see LICENSE in the repository root for full details.

# Installs a clang new enough to build webrtc-sys on a Linux host, and points
# CC/CXX at it for the later steps of a workflow job.
#
# webrtc-sys compiles against the libc++ shipped inside the prebuilt libwebrtc,
# which needs clang 21 or later (its build script checks); Ubuntu 24.04 has 18
# and no newer package, so this takes LLVM's release tarball, as LiveKit's own
# CI does. Android builds use the NDK's clang and do not need it.
set -euo pipefail

LLVM_VERSION="${LLVM_VERSION:-21.1.8}"
LLVM_ROOT="/opt/llvm-$LLVM_VERSION"

case "$(uname -m)" in
x86_64) arch=X64 ;;
aarch64 | arm64) arch=ARM64 ;;
*)
    echo "install-clang.sh: unsupported architecture $(uname -m)" >&2
    exit 1
    ;;
esac

if [ ! -x "$LLVM_ROOT/bin/clang++" ]; then
    url="https://github.com/llvm/llvm-project/releases/download/llvmorg-$LLVM_VERSION/LLVM-$LLVM_VERSION-Linux-$arch.tar.xz"
    echo "install-clang.sh: fetching $url" >&2
    sudo mkdir -p "$LLVM_ROOT"
    curl --fail --location --silent --show-error --retry 3 "$url" |
        sudo tar -xJ --strip-components=1 -C "$LLVM_ROOT"
fi

"$LLVM_ROOT/bin/clang++" --version

if [ -n "${GITHUB_ENV:-}" ]; then
    {
        echo "CC=$LLVM_ROOT/bin/clang"
        echo "CXX=$LLVM_ROOT/bin/clang++"
    } >>"$GITHUB_ENV"
fi
