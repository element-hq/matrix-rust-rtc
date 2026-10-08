# Copyright 2026 Element Creations Ltd.
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
# Please see LICENSE in the repository root for full details.

# Two stages, one base. The chaos_peer binary is built either on a CI runner
# (ubuntu-24.04) or in `builder` below, and run in `runtime` — so both must
# share one glibc, which is why this is ubuntu:24.04 and not a slimmer image.

# What a peer needs at run time: libwebrtc's glib, matrix-sdk's sqlite, CA
# roots, and the tools the harness uses to inject faults from inside the peer's
# namespace.
FROM ubuntu:24.04 AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates libglib2.0-0t64 libsqlite3-0 iproute2 iptables \
    && rm -rf /var/lib/apt/lists/*
ENTRYPOINT ["/opt/chaos/chaos_peer"]

# Local builds on a non-Linux host (build-peer.sh --docker). Same toolchain
# prerequisites as the CI jobs that build matrix-rtc-call-sdk.
FROM runtime AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential pkg-config libglib2.0-dev libsqlite3-dev libssl-dev curl git \
    && rm -rf /var/lib/apt/lists/*
ENV RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo PATH=/usr/local/cargo/bin:$PATH
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
ENTRYPOINT []
