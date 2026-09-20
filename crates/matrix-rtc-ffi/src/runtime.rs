// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The runtime the crate's own background work runs on.
//!
//! The exported handle methods are `async` and driven by uniffi's own Tokio
//! integration (`async_runtime = "tokio"`), so they need nothing from here. Two
//! things do, and one of them is in the slim build, so this is not gated on
//! `media`:
//!
//! - the keep-alive driver a join spawns, which outlives the call that started
//!   it. `tokio::spawn` would put it on whichever runtime uniffi happened to be
//!   polling that call on — async_compat's, an implementation detail of the
//!   binding layer that no entry point promises.
//! - the media layer: `connect_media_session` hops onto this runtime so every
//!   task it spawns afterwards — the engine actor, the connection pool, IO —
//!   inherits the same context regardless of which thread the FFI call arrived
//!   on.
//!
//! It must be multi-threaded. The core arms a `tokio::time::sleep` on the
//! key-rotation path (the MSC4143 `delayBeforeUse` wait), and a current-thread
//! runtime with nothing driving it would leave that timer to never fire.

/// The process-wide runtime backing the keep-alive drivers and the media layer.
pub(crate) fn runtime() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("matrix-rtc")
            .build()
            .expect("failed to build the matrix-rtc tokio runtime")
    })
}
