// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Where the core's log lines go on this surface.
//!
//! Every crate underneath speaks through the `log` facade and has no output
//! of its own, so nothing is logged until the host installs a [`LogSink`]:
//! its lines then land in the host's log rather than on a console nobody
//! reads.

use std::sync::{Arc, RwLock};

/// The `log` crate's levels, for a [`LogSink`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiLogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl From<log::Level> for FfiLogLevel {
    fn from(level: log::Level) -> Self {
        match level {
            log::Level::Error => Self::Error,
            log::Level::Warn => Self::Warn,
            log::Level::Info => Self::Info,
            log::Level::Debug => Self::Debug,
            log::Level::Trace => Self::Trace,
        }
    }
}

impl From<FfiLogLevel> for log::LevelFilter {
    fn from(level: FfiLogLevel) -> Self {
        match level {
            FfiLogLevel::Error => Self::Error,
            FfiLogLevel::Warn => Self::Warn,
            FfiLogLevel::Info => Self::Info,
            FfiLogLevel::Debug => Self::Debug,
            FfiLogLevel::Trace => Self::Trace,
        }
    }
}

/// A host-supplied destination for log lines. `target` is the Rust module
/// path (`matrix_rtc_core::session`), `message` the formatted line. Called
/// synchronously from wherever the core logs: keep it cheap, and never call
/// back into the bindings from it.
#[uniffi::export(with_foreign)]
pub trait LogSink: Send + Sync {
    fn log(&self, level: FfiLogLevel, target: String, message: String);
}

struct SinkLogger {
    sink: RwLock<Option<Arc<dyn LogSink>>>,
}

static SINK_LOGGER: SinkLogger = SinkLogger {
    sink: RwLock::new(None),
};

impl log::Log for SinkLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &log::Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let sink = self.sink.read().unwrap_or_else(|e| e.into_inner()).clone();
        if let Some(sink) = sink {
            sink.log(
                record.level().into(),
                record.target().to_owned(),
                record.args().to_string(),
            );
        }
    }

    fn flush(&self) {}
}

/// Routes log lines to `sink`, at `max_level` and above. Calling again
/// replaces the sink and the level. The `log` facade accepts one logger per
/// process, so where the host already installed a Rust logger (a native host
/// with `tracing`), that one keeps the lines and the sink stays silent.
#[uniffi::export]
pub fn set_log_sink(sink: Arc<dyn LogSink>, max_level: FfiLogLevel) {
    *SINK_LOGGER.sink.write().unwrap_or_else(|e| e.into_inner()) = Some(sink);
    // Fails only if a logger is installed already, which is the documented case.
    let _ = log::set_logger(&SINK_LOGGER);
    log::set_max_level(max_level.into());
}
