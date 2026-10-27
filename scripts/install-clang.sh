#!/usr/bin/env bash
# Copyright 2026 Element Creations Ltd.
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
# Please see LICENSE in the repository root for full details.

# Installs a clang new enough to build webrtc-sys on an Ubuntu host, and points
# CC/CXX at it for the later steps of a workflow job.
#
# webrtc-sys compiles against the libc++ shipped inside the prebuilt libwebrtc,
# which needs clang 21 or later (its build script checks); Ubuntu 24.04 has 18,
# so this takes the clang package alone from LLVM's apt repository. Android
# builds use the NDK's clang and do not need it.
set -euo pipefail

CLANG_VERSION="${CLANG_VERSION:-21}"
CODENAME="$(. /etc/os-release && echo "$VERSION_CODENAME")"
KEYRING=/usr/share/keyrings/apt.llvm.org.gpg

curl --fail --location --silent --show-error --retry 5 --retry-all-errors \
    https://apt.llvm.org/llvm-snapshot.gpg.key | sudo gpg --dearmor --yes -o "$KEYRING"
echo "deb [signed-by=$KEYRING] http://apt.llvm.org/$CODENAME/ llvm-toolchain-$CODENAME-$CLANG_VERSION main" |
    sudo tee /etc/apt/sources.list.d/llvm-"$CLANG_VERSION".list >/dev/null
sudo apt-get update -o Acquire::Retries=5
sudo apt-get install -y -o Acquire::Retries=5 --no-install-recommends "clang-$CLANG_VERSION"

"clang++-$CLANG_VERSION" --version

if [ -n "${GITHUB_ENV:-}" ]; then
    {
        echo "CC=clang-$CLANG_VERSION"
        echo "CXX=clang++-$CLANG_VERSION"
    } >>"$GITHUB_ENV"
fi
