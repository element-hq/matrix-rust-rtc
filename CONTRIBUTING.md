# Contributing to matrix-rust-rtc

`matrix-rust-rtc` is a Rust implementation of a Matrix RTC client SDK: MSC4143
call-membership signalling, per-participant media-key exchange, and a
transport-agnostic media layer behind Kotlin/Swift (UniFFI) and WebAssembly
bindings. It is consumed by applications rather than run on its own, so its API
surface and wire behaviour are held to a high standard.

Contributions are welcome. This document explains how to contribute effectively.

## Contributor License Agreement

All contributors must sign the
[Element Contributor License Agreement](https://cla-assistant.io/element-hq/matrix-rust-rtc)
before their contribution can be merged. The CLA assistant bot prompts you
automatically when you open a pull request.

This is required because the project is dual licensed (see
[Copyright & License](README.md#copyright--license)): Element needs the right to
distribute your contribution under both the AGPL and the Element Commercial
License. A DCO sign-off is not sufficient for that.

## Issue First

Before writing code for a new feature or an API change, open an issue and agree
the approach with the maintainers. A change that looks reasonable in isolation
can conflict with in-progress work or with the MSCs this crate tracks, and the
issue is where that gets resolved — before anyone writes code.

Bug fixes with a clear, uncontroversial solution can go straight to a pull
request.

## Before You Open a Pull Request

```bash
make quality-check
```

That runs formatting, the license-header check, clippy, the test suite, and a
build check across the workspace. Individually:

```bash
make fmt                 # format
make license-headers     # every source file carries the dual-license header
make clippy              # clippy with -D warnings
make test                # cargo test --all
```

New source files need the license header. `make license-headers-fix` inserts it
for you, in the right comment syntax for the file type.

Wire-format and Element Call compatibility changes should come with coverage in
the interop suite (`interop/`) or the e2e call harness
(`crates/matrix-rtc-livekit/tests/`). See `ARCHITECTURE.md` for how the crates
fit together, and `AGENTS.md` for the repository conventions.

## Spec Changes

This crate tracks several MSCs — notably
[MSC4143](https://github.com/matrix-org/matrix-spec-proposals/pull/4143)
(MatrixRTC), [MSC4195](https://github.com/matrix-org/matrix-spec-proposals/pull/4195)
(LiveKit transport) and
[MSC4354](https://github.com/matrix-org/matrix-spec-proposals/pull/4354)
(sticky events). Where behaviour is specified, implement what the proposal says
and cite it in the code or the pull request; where it is not, say so explicitly
rather than inventing a convention.

## Getting Help

The best place to ask questions about MatrixRTC development is
[#matrixRtc:matrix.org](https://matrix.to/#/#matrixrtc:matrix.org).
