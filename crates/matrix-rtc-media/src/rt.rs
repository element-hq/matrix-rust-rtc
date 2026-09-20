// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The crate's task and timer primitives, per target.
//!
//! Off `wasm32` this is tokio: `spawn` requires `Send` futures and `sleep`
//! rides the runtime's timer driver. On `wasm32` there is no runtime — tasks
//! go to the JS microtask queue via `wasm_bindgen_futures::spawn_local` (no
//! `Send`, one thread) and sleeps are `setTimeout`-backed (`gloo-timers`).
//! `tokio::time` is not an option there: it panics at runtime on
//! `wasm32-unknown-unknown` (`Instant::now` is unimplemented), which is why
//! every engine task and timer goes through this seam instead.

use std::future::Future;
use std::time::Duration;

/// A spawned task, aborted on request. Dropping the handle detaches the task.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct TaskHandle(tokio::task::JoinHandle<()>);

#[cfg(not(target_arch = "wasm32"))]
impl TaskHandle {
    pub(crate) fn abort(&self) {
        self.0.abort();
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn spawn<F>(future: F) -> TaskHandle
where
    F: Future<Output = ()> + Send + 'static,
{
    TaskHandle(tokio::spawn(future))
}

/// Whether [`spawn`] has somewhere to run: a current tokio runtime here, always
/// on wasm (the JS event loop). Callers that can degrade check this instead of
/// letting `spawn` panic.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn can_spawn() -> bool {
    tokio::runtime::Handle::try_current().is_ok()
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
}

/// A spawned task, aborted on request. Dropping the handle detaches the task.
#[cfg(target_arch = "wasm32")]
pub(crate) struct TaskHandle(futures_util::future::AbortHandle);

#[cfg(target_arch = "wasm32")]
impl TaskHandle {
    pub(crate) fn abort(&self) {
        self.0.abort();
    }
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn spawn<F>(future: F) -> TaskHandle
where
    F: Future<Output = ()> + 'static,
{
    let (future, handle) = futures_util::future::abortable(future);
    wasm_bindgen_futures::spawn_local(async move {
        // Aborted is the expected outcome for a task cancelled via the handle.
        let _ = future.await;
    });
    TaskHandle(handle)
}

/// Whether [`spawn`] has somewhere to run: a current tokio runtime off wasm,
/// always here (the JS event loop). Callers that can degrade check this instead
/// of letting `spawn` panic.
#[cfg(target_arch = "wasm32")]
pub(crate) fn can_spawn() -> bool {
    true
}

#[cfg(target_arch = "wasm32")]
pub(crate) async fn sleep(duration: Duration) {
    // setTimeout takes u32 milliseconds; the engine's longest delay is the
    // 30 s backoff cap, nowhere near the ~49-day u32 limit.
    let millis = u32::try_from(duration.as_millis()).unwrap_or(u32::MAX);
    gloo_timers::future::TimeoutFuture::new(millis).await;
}
