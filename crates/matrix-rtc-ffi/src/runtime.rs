// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The runtime the library's own background work runs on.
//!
//! The exported handle methods are `async` and driven by uniffi's own Tokio
//! integration (`async_runtime = "tokio"`). The library spawns onto the
//! *current* runtime (`matrix_rtc_core::executor`), and under uniffi that is
//! async_compat's — an implementation detail of the binding layer that no
//! entry point promises. So every export that spawns hops onto this one first
//! ([`on_runtime`]):
//!
//! - opening a room, which spawns its feeds;
//! - joining a call, which spawns its upkeep (keep-alive, key rotations);
//! - the media layer: `connect_media_session` hops here so every task it
//!   spawns afterwards — the engine actor, the connection pool, IO — inherits
//!   the same context regardless of which thread the FFI call arrived on.
//!
//! Multi-threaded, so the library's timers fire with nothing else driving it.

use std::future::Future;

use matrix_rtc_core::executor::JoinHandleExt;

/// The process-wide runtime backing the library's tasks and the media layer.
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

/// Runs `future` on [`runtime`], so whatever it spawns lands there. Dropping
/// the returned future cancels `future`, as awaiting it in place would.
pub(crate) async fn on_runtime<F>(future: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match runtime().spawn(future).abort_on_drop().await {
        Ok(output) => output,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => panic!("matrix-rtc runtime task cancelled: {error}"),
    }
}
