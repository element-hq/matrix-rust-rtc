// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The library's task and timer primitives, per target.
//!
//! Off `wasm32` this re-exports tokio: [`spawn`] requires `Send` futures and
//! runs them on the *current* runtime, so a native host calls into the library
//! from within one. On `wasm32` there is no runtime — tasks go to the JS
//! microtask queue via `wasm_bindgen_futures::spawn_local` (no `Send`, one
//! thread) behind a [`JoinHandle`] shaped like tokio's, and sleeps are
//! `setTimeout`-backed (`gloo-timers`): `tokio::time` panics at runtime on
//! `wasm32-unknown-unknown`. Modelled on matrix-sdk-common's `executor`.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
mod sys {
    pub use tokio::task::{AbortHandle, JoinError, JoinHandle, spawn};

    pub fn can_spawn() -> bool {
        tokio::runtime::Handle::try_current().is_ok()
    }

    pub async fn sleep(duration: std::time::Duration) {
        tokio::time::sleep(duration).await;
    }
}

#[cfg(target_arch = "wasm32")]
mod sys {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use futures_util::FutureExt;
    pub use futures_util::future::AbortHandle;
    use futures_util::future::{Abortable, RemoteHandle};

    /// The wasm counterpart of `tokio::task::JoinError`.
    #[derive(Debug)]
    pub enum JoinError {
        Cancelled,
        Panic,
    }

    impl JoinError {
        pub fn is_cancelled(&self) -> bool {
            matches!(self, JoinError::Cancelled)
        }
    }

    impl std::fmt::Display for JoinError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                JoinError::Cancelled => write!(f, "task was cancelled"),
                JoinError::Panic => write!(f, "task panicked"),
            }
        }
    }

    impl std::error::Error for JoinError {}

    /// The wasm counterpart of `tokio::task::JoinHandle`. Dropping it detaches
    /// the task, as tokio's does.
    #[derive(Debug)]
    pub struct JoinHandle<T> {
        remote_handle: Option<RemoteHandle<T>>,
        abort_handle: AbortHandle,
    }

    impl<T> JoinHandle<T> {
        pub fn abort(&self) {
            self.abort_handle.abort();
        }

        pub fn abort_handle(&self) -> AbortHandle {
            self.abort_handle.clone()
        }

        pub fn is_finished(&self) -> bool {
            self.abort_handle.is_aborted()
        }
    }

    impl<T> Drop for JoinHandle<T> {
        fn drop(&mut self) {
            // Dropping a `RemoteHandle` cancels its future; `forget` detaches it.
            if let Some(handle) = self.remote_handle.take() {
                handle.forget();
            }
        }
    }

    impl<T: 'static> Future for JoinHandle<T> {
        type Output = Result<T, JoinError>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.abort_handle.is_aborted() {
                Poll::Ready(Err(JoinError::Cancelled))
            } else if let Some(handle) = self.remote_handle.as_mut() {
                Pin::new(handle).poll(cx).map(Ok)
            } else {
                Poll::Ready(Err(JoinError::Panic))
            }
        }
    }

    pub fn spawn<F, T>(future: F) -> JoinHandle<T>
    where
        F: Future<Output = T> + 'static,
    {
        let (future, remote_handle) = future.remote_handle();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let future = Abortable::new(future, abort_registration);
        wasm_bindgen_futures::spawn_local(async {
            // `Err(Aborted)` is the expected outcome for a cancelled task.
            let _ = future.await;
        });
        JoinHandle {
            remote_handle: Some(remote_handle),
            abort_handle,
        }
    }

    pub fn can_spawn() -> bool {
        true
    }

    pub async fn sleep(duration: std::time::Duration) {
        // setTimeout takes u32 milliseconds (~49 days); longer waits saturate.
        let millis = u32::try_from(duration.as_millis()).unwrap_or(u32::MAX);
        gloo_timers::future::TimeoutFuture::new(millis).await;
    }
}

pub use sys::{AbortHandle, JoinError, JoinHandle, spawn};

/// Whether [`spawn`] has somewhere to run: a current tokio runtime off wasm,
/// always on it (the JS event loop). Callers that can degrade check this
/// instead of letting `spawn` panic.
pub fn can_spawn() -> bool {
    sys::can_spawn()
}

/// Waits for `duration` on the target's timer.
pub async fn sleep(duration: Duration) {
    sys::sleep(duration).await;
}

/// A task aborted when this is dropped.
#[derive(Debug)]
pub struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> AbortOnDrop<T> {
    pub fn new(handle: JoinHandle<T>) -> Self {
        Self(handle)
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T: 'static> Future for AbortOnDrop<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

/// Turns a [`JoinHandle`] into an [`AbortOnDrop`].
pub trait JoinHandleExt<T> {
    fn abort_on_drop(self) -> AbortOnDrop<T>;
}

impl<T> JoinHandleExt<T> for JoinHandle<T> {
    fn abort_on_drop(self) -> AbortOnDrop<T> {
        AbortOnDrop::new(self)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn a_spawned_task_yields_its_output() {
        assert_eq!(spawn(async { 42 }).await.unwrap(), 42);
    }

    #[tokio::test]
    async fn an_aborted_task_reports_cancellation() {
        let handle = spawn(std::future::pending::<()>());
        handle.abort();
        assert!(handle.await.unwrap_err().is_cancelled());
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_an_abort_on_drop_stops_the_task() {
        let ran = Arc::new(AtomicBool::new(false));
        let task = {
            let ran = ran.clone();
            spawn(async move {
                sleep(Duration::from_secs(1)).await;
                ran.store(true, Ordering::SeqCst);
            })
            .abort_on_drop()
        };
        drop(task);
        sleep(Duration::from_secs(2)).await;
        assert!(!ran.load(Ordering::SeqCst));
    }

    #[test]
    fn spawning_needs_a_runtime_off_wasm() {
        assert!(!can_spawn());
    }
}
