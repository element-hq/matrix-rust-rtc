#!/usr/bin/env bash
# Copyright 2026 Element Creations Ltd.
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
# Please see LICENSE in the repository root for full details.

# Run cargo against the MSC4354-capable matrix-rust-sdk fork.
#   scripts/cargo-sticky.sh <cargo subcommand and args>
# The workspace depends on upstream matrix-rust-sdk; this applies the
# `.cargo/experimental-sticky.toml` overlay that redirects it to the fork. The
# caller still passes the features (`experimental-sticky` plus the SDK's own
# `unstable-msc4354` on matrix-sdk and matrix-sdk-ui — see the Makefile's
# STICKY_FEATURES), because a cargo feature cannot forward to an SDK feature
# upstream lacks.
# Two side effects are contained here so the two SDK trees never fight:
# - the build goes to target/sticky (override with CARGO_TARGET_DIR);
# - the redirected SDK re-resolves the lockfile, so Cargo.lock is swapped for
#   Cargo.sticky.lock for the duration and the committed (upstream) lockfile is
#   put back afterwards, whatever the exit status. Cargo.sticky.lock is kept, and
#   committed, so sticky builds resolve the same way every time.
set -euo pipefail

if [ $# -eq 0 ]; then
    echo "usage: $0 <cargo subcommand> [args...]" >&2
    exit 2
fi
subcommand=$1
shift

cd "$(dirname "$0")/.."

mkdir -p target
cp Cargo.lock target/.Cargo.lock.upstream
if [ -f Cargo.sticky.lock ]; then
    cp Cargo.sticky.lock Cargo.lock
fi

restore() {
    # The lockfile cargo just used is the sticky one; keep it for next time.
    cp Cargo.lock Cargo.sticky.lock
    mv -f target/.Cargo.lock.upstream Cargo.lock
}
trap restore EXIT

# `--config` goes AFTER the subcommand, not before it. `cargo clippy` is an
# external subcommand: cargo hands off to `cargo-clippy`, which runs its own
# `cargo check`, and a global `--config` given before the subcommand is not
# forwarded across that hop — so the overlay silently vanished and clippy
# resolved the upstream SDK. Placed after, it is one of the arguments
# `cargo-clippy` passes through; built-in subcommands accept it there too.
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target/sticky}" \
    cargo "$subcommand" --config .cargo/experimental-sticky.toml "$@"
