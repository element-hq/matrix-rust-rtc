// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Join functionality for RTC sessions.
//!
//! This module provides the data structures and parameters needed for joining
//! an RTC session: the slot, the application, the transport, and overrides of
//! the timings. Who joins is the backend's account; it is never a parameter.

use crate::encryption::types::EncryptionConfig;
use crate::session::{ApplicationInfo, LeaveCode, LeaveReason};
use crate::transport::RtcTransport;

impl From<RtcTransport> for TransportIntent {
    fn from(transport: RtcTransport) -> Self {
        Self::Publish(transport)
    }
}

/// Default keep-alive timeout in milliseconds (30 seconds).
///
/// This is the delay before the cleanup event would fire if not restarted.
pub const DEFAULT_KEEP_ALIVE_TIMEOUT_MS: u64 = 30_000;

/// Default interval between keep-alive ticks, in milliseconds (10 seconds):
/// three inside the default timeout, so one slow round trip cannot let the
/// delayed leave fire.
pub const DEFAULT_KEEP_ALIVE_INTERVAL_MS: u64 = 10_000;

/// Default sticky-map lifetime for our membership event, in milliseconds
/// (1 hour).
///
/// Distinct from [`DEFAULT_KEEP_ALIVE_TIMEOUT_MS`]: that one arms the delayed
/// leave (the dead man's switch for a client that dies), while this one is how
/// long the homeserver keeps our membership in the sticky map at all. Both are
/// refreshed by [`keep_alive`], the sticky one only once it is halfway to
/// expiry.
///
/// [`keep_alive`]: crate::OwnMembershipMachine::keep_alive
pub const DEFAULT_STICKY_DURATION_MS: u64 = 60 * 60 * 1000;

/// The longest sticky lifetime that is actually honoured (1 hour).
///
/// Homeservers (and matrix-rust-sdk) clamp longer requests down to an hour.
/// That clamp is invisible in the response, so asking for more would have us
/// schedule the refresh against a lifetime the entry does not have — and the
/// membership would lapse before we ever re-sent it. Requests are therefore
/// clamped here, where the refresh interval is derived from the same number.
pub const MAX_STICKY_DURATION_MS: u64 = 60 * 60 * 1000;

/// The membership lifetime to fall back to on a homeserver that refuses MSC4140
/// delayed events, in milliseconds (5 minutes).
///
/// Without a delayed leave there is no dead man's switch, so the only thing that
/// clears a crashed client's membership is the lifetime running out — an hour of
/// ghost with [`DEFAULT_STICKY_DURATION_MS`], four hours in the pre-sticky
/// Element Call dialect. This shortens that to five minutes, at the cost of a
/// membership event every 2½ minutes instead of every 30.
///
/// Five and not less: MSC4354 says a sticky duration "SHOULD NOT be set to below
/// 5 minutes", because a server whose `origin_server_ts` runs behind expires
/// sticky events early and a short duration has no room to absorb that. So this
/// is the floor, not a compromise — the ghost window cannot be tightened further
/// in the sticky dialect however often we refresh.
pub const DEFAULT_DEGRADED_LIFETIME_MS: u64 = 5 * 60 * 1000;

/// The MSC4143 `application_slot_id` of an application's room-wide slot
/// (`m.call#room`).
pub const ROOM_APPLICATION_SLOT_ID: &str = "room";

/// `{application_type}#{application_slot_id}`, MSC4143's slot id.
fn slot_id_of(application: &ApplicationInfo, application_slot_id: &str) -> String {
    format!(
        "{}#{application_slot_id}",
        application.application_type().unwrap_or_default()
    )
}

/// Generates a fresh `member.id` for a join.
///
/// MSC4143 requires the id to be unique per join and suggests it be
/// unpredictable, since transports may use it as entropy when deriving
/// pseudonymous participant identities. 16 random bytes, hex encoded.
pub fn generate_member_id() -> String {
    use rand::RngCore;
    use rand_core::OsRng;

    let mut bytes = [0u8; 16];
    OsRng.fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// What a joining member intends to do with transports.
///
/// Which transport to publish on is the application's decision — discovering
/// what the homeserver offers (`GET /_matrix/client/v1/rtc/transports`) and
/// choosing among them happens above this crate (`matrix-rtc-call` takes the
/// first LiveKit one), and the result is passed in here.
///
/// MSC4143 does not require a member to publish anything — `transports` carries
/// no REQUIRED marker — so a member that only receives, such as a recorder, is
/// a valid participant rather than a degraded one.
#[derive(Clone, Debug)]
pub enum TransportIntent {
    /// Publish on this transport.
    Publish(RtcTransport),

    /// Never publish; only receive. Suits a recorder or any other observer.
    ///
    /// `can_subscribe` is what other members use to pick a transport this one
    /// can actually receive on, so stating it matters even though nothing is
    /// published. An empty list is legal but leaves peers without that cue.
    ReceiveOnly {
        /// Transport types this member can receive on.
        can_subscribe: Vec<String>,
    },
}

/// Parameters for joining an RTC session.
///
/// The slot, application and transport to join with, and overrides of the
/// timings. The user and device are the backend's.
#[derive(Clone, Debug)]
pub struct JoinSessionParams {
    /// The `member.id` (and sticky key) for this join.
    ///
    /// MSC4143 requires this to be unique for *each* join, so that leaving and
    /// rejoining never reuses an identifier. If not provided, a fresh random id
    /// is generated per call to [`JoinSessionParams::membership_id`].
    pub membership_id: Option<String>,

    /// The slot ID for the session (e.g., "m.call#room").
    pub slot_id: String,

    /// `content.application` to publish. A `String` or `&str` converts into a
    /// type-only one.
    pub application: ApplicationInfo,

    /// What this member does with transports. Required: a join without one is
    /// refused ([`Self::transport`](Self::transport()) sets it).
    pub transport: Option<TransportIntent>,

    /// Keep-alive timeout in milliseconds.
    ///
    /// Defaults to `DEFAULT_KEEP_ALIVE_TIMEOUT_MS` if not specified.
    pub keep_alive_timeout_ms: Option<u64>,

    /// The longest the session's upkeep sleeps between keep-alive wake-ups, in
    /// milliseconds; it wakes earlier whenever something is due.
    ///
    /// Defaults to `DEFAULT_KEEP_ALIVE_INTERVAL_MS`; clamped to half the
    /// keep-alive timeout.
    pub keep_alive_interval_ms: Option<u64>,

    /// How long the homeserver should keep our membership in the sticky map,
    /// in milliseconds.
    ///
    /// Defaults to `DEFAULT_STICKY_DURATION_MS` if not specified. The
    /// keep-alive re-sends the membership before this elapses, so a host that
    /// shortens it is choosing a higher signalling rate, not a shorter
    /// presence.
    pub sticky_duration_ms: Option<u64>,

    /// The membership lifetime to use instead of `sticky_duration_ms` once the
    /// homeserver has refused to arm a delayed leave, in milliseconds.
    ///
    /// Defaults to `DEFAULT_DEGRADED_LIFETIME_MS`. Raising it trades a slower
    /// cleanup of a crashed client for less signalling; it only ever applies on
    /// a homeserver without MSC4140.
    pub degraded_lifetime_ms: Option<u64>,

    /// Configuration for encryption key management.
    ///
    /// If not provided, defaults to `EncryptionConfig::default()`.
    pub encryption_config: Option<EncryptionConfig>,

    /// The delayed leave of an earlier join of this membership that ended
    /// [`LeaveCode::MembershipLost`](crate::LeaveCode::MembershipLost), to
    /// retire before joining. The join fails, and can be retried, while the
    /// homeserver cannot be asked.
    pub supersedes_delayed_leave: Option<String>,
}

impl JoinSessionParams {
    /// Joins `application`'s room-wide slot, `{application}#room`, as the
    /// backend's account with a fresh `member.id` and default timings. A join
    /// also needs a [`transport`](Self::transport()); each other setter
    /// overrides one default.
    pub fn application(application: impl Into<ApplicationInfo>) -> Self {
        let application = application.into();
        let slot_id = slot_id_of(&application, ROOM_APPLICATION_SLOT_ID);
        Self {
            membership_id: None,
            slot_id,
            application,
            transport: None,
            keep_alive_timeout_ms: None,
            keep_alive_interval_ms: None,
            sticky_duration_ms: None,
            degraded_lifetime_ms: None,
            encryption_config: None,
            supersedes_delayed_leave: None,
        }
    }

    /// Retires `delay_id` — the delayed leave a lost earlier join of this
    /// membership left armed ([`LeaveReason::delay_id`](crate::LeaveReason)) —
    /// before joining, so it cannot fire after this join and end it.
    pub fn supersedes_delayed_leave(mut self, delay_id: impl Into<String>) -> Self {
        self.supersedes_delayed_leave = Some(delay_id.into());
        self
    }

    /// Joins the application's slot `{application}#{application_slot_id}`.
    pub fn slot(mut self, application_slot_id: impl AsRef<str>) -> Self {
        self.slot_id = slot_id_of(&self.application, application_slot_id.as_ref());
        self
    }

    /// Publishes on (or only receives from) `transport`.
    pub fn transport(mut self, transport: impl Into<TransportIntent>) -> Self {
        self.transport = Some(transport.into());
        self
    }

    /// Joins as `member_id` instead of a fresh one. MSC4143 wants a fresh id
    /// per join; this is for a format that keys membership otherwise.
    pub fn member_id(mut self, member_id: impl Into<String>) -> Self {
        self.membership_id = Some(member_id.into());
        self
    }

    /// Sets the `keep_alive_timeout_ms` field.
    pub fn keep_alive_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.keep_alive_timeout_ms = Some(timeout_ms);
        self
    }

    /// Sets the `keep_alive_interval_ms` field.
    pub fn keep_alive_interval_ms(mut self, interval_ms: u64) -> Self {
        self.keep_alive_interval_ms = Some(interval_ms);
        self
    }

    /// Sets the `sticky_duration_ms` field.
    pub fn sticky_duration_ms(mut self, duration_ms: u64) -> Self {
        self.sticky_duration_ms = Some(duration_ms);
        self
    }

    /// Sets the `degraded_lifetime_ms` field.
    pub fn degraded_lifetime_ms(mut self, lifetime_ms: u64) -> Self {
        self.degraded_lifetime_ms = Some(lifetime_ms);
        self
    }

    /// Sets the `encryption_config` field.
    pub fn encryption_config(mut self, config: EncryptionConfig) -> Self {
        self.encryption_config = Some(config);
        self
    }

    /// Gets the `member.id` (sticky key) to use for this join.
    ///
    /// If a membership_id was explicitly set, returns that. Otherwise generates a
    /// fresh random id: MSC4143 requires a different identifier on every join, so
    /// this must not be derived from stable values like the user and device IDs.
    pub fn membership_id(&self) -> String {
        self.membership_id
            .clone()
            .unwrap_or_else(generate_member_id)
    }

    /// Gets the keep-alive timeout to use.
    ///
    /// Returns the configured timeout or the default.
    pub(crate) fn effective_keep_alive_timeout_ms(&self) -> u64 {
        self.keep_alive_timeout_ms
            .unwrap_or(DEFAULT_KEEP_ALIVE_TIMEOUT_MS)
    }

    /// Gets the keep-alive interval to use: the configured one or the
    /// default, at most half the keep-alive timeout.
    pub(crate) fn effective_keep_alive_interval_ms(&self) -> u64 {
        let interval = self
            .keep_alive_interval_ms
            .unwrap_or(DEFAULT_KEEP_ALIVE_INTERVAL_MS);
        let ceiling = self.effective_keep_alive_timeout_ms() / 2;
        if interval > ceiling {
            log::warn!(
                "[{}] keep-alive interval {interval}ms clamped to {ceiling}ms, half the timeout",
                self.slot_id,
            );
            ceiling
        } else {
            interval
        }
    }

    /// Gets the sticky-map lifetime to use.
    ///
    /// Returns the configured duration or the default.
    pub(crate) fn effective_sticky_duration_ms(&self) -> u64 {
        let requested = self
            .sticky_duration_ms
            .unwrap_or(DEFAULT_STICKY_DURATION_MS);
        if requested > MAX_STICKY_DURATION_MS {
            log::warn!(
                "sticky_duration_ms {requested} exceeds the {MAX_STICKY_DURATION_MS} the server \
                 will honour; using the maximum so the refresh stays ahead of expiry",
            );
            return MAX_STICKY_DURATION_MS;
        }
        requested
    }

    /// Gets the membership lifetime to use once delayed events are known to be
    /// unavailable.
    ///
    /// Returns the configured duration or the default. Clamped to
    /// [`MAX_STICKY_DURATION_MS`] for the same reason
    /// [`Self::effective_sticky_duration_ms`] is, and it is only ever *shorter* than that
    /// in practice.
    pub(crate) fn effective_degraded_lifetime_ms(&self) -> u64 {
        self.degraded_lifetime_ms
            .unwrap_or(DEFAULT_DEGRADED_LIFETIME_MS)
            .min(MAX_STICKY_DURATION_MS)
    }

    /// Gets the encryption configuration to use.
    ///
    /// Returns the configured config or the default.
    pub(crate) fn effective_encryption_config(&self) -> EncryptionConfig {
        self.encryption_config.clone().unwrap_or_default()
    }

    /// Validates the parameters.
    ///
    /// Returns `Ok(())` if all required fields are present and valid.
    /// Returns `Err` with a description of the validation error otherwise.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.slot_id.is_empty() {
            return Err("slot_id is required");
        }
        if self.transport.is_none() {
            return Err("transport is required");
        }
        if self.application.application_type().is_none() {
            return Err("application is required");
        }
        Ok(())
    }
}

/// Parameters for leaving an RTC session.
#[derive(Clone, Debug, Default)]
pub struct LeaveSessionParams {
    /// Optional MSC4143 leave reason. Defaults to `code = leave` when unset.
    pub leave_reason: Option<LeaveReason>,
}

impl LeaveSessionParams {
    /// Creates new leave parameters with no explicit leave reason.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates new leave parameters carrying a leave reason.
    pub fn with_leave_reason(leave_reason: LeaveReason) -> Self {
        Self {
            leave_reason: Some(leave_reason),
        }
    }

    /// Creates new leave parameters from a bare MSC4143 leave code.
    pub fn with_code(code: LeaveCode) -> Self {
        Self::with_leave_reason(LeaveReason::new(code))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{LiveKitTransport, RtcTransport};

    /// MSC4143 requires `member.id` to differ on every join, so a generated id
    /// must not be derived from the (stable) user and device IDs.
    #[test]
    fn test_membership_id_is_unique_per_call() {
        let params = JoinSessionParams::application("m.call").transport(RtcTransport::LiveKit(
            LiveKitTransport {
                livekit_service_url: "https://example.com".to_string(),
            },
        ));

        let first = params.membership_id();
        let second = params.membership_id();

        assert_ne!(first, second);
        assert_eq!(first.len(), 32);
        assert!(!first.contains("@alice:example.org"));
        assert!(!first.contains("device123"));
    }

    #[test]
    fn test_explicit_membership_id() {
        let mut params = JoinSessionParams::application("m.call").transport(RtcTransport::LiveKit(
            LiveKitTransport {
                livekit_service_url: "https://example.com".to_string(),
            },
        ));
        params.membership_id = Some("custom-id".to_string());

        assert_eq!(params.membership_id(), "custom-id");
    }

    #[test]
    fn test_keep_alive_timeout_default() {
        let params = JoinSessionParams::application("m.call").transport(RtcTransport::LiveKit(
            LiveKitTransport {
                livekit_service_url: "https://example.com".to_string(),
            },
        ));

        assert_eq!(
            params.effective_keep_alive_timeout_ms(),
            DEFAULT_KEEP_ALIVE_TIMEOUT_MS
        );
    }

    #[test]
    fn test_keep_alive_timeout_custom() {
        let mut params = JoinSessionParams::application("m.call").transport(RtcTransport::LiveKit(
            LiveKitTransport {
                livekit_service_url: "https://example.com".to_string(),
            },
        ));
        params.keep_alive_timeout_ms = Some(60_000);

        assert_eq!(params.effective_keep_alive_timeout_ms(), 60_000);
    }

    #[test]
    fn test_validate_success() {
        let params = JoinSessionParams::application("m.call").transport(RtcTransport::LiveKit(
            LiveKitTransport {
                livekit_service_url: "https://example.com".to_string(),
            },
        ));

        assert!(params.validate().is_ok());
    }

    #[test]
    fn test_validate_empty_slot_id() {
        let mut params = JoinSessionParams::application("m.call");
        params.slot_id.clear();

        assert_eq!(params.validate(), Err("slot_id is required"));
    }

    #[test]
    fn the_slot_id_is_composed_from_the_application() {
        assert_eq!(
            JoinSessionParams::application("m.call").slot_id,
            "m.call#room"
        );
        assert_eq!(
            JoinSessionParams::application("org.example.board")
                .slot("planning")
                .slot_id,
            "org.example.board#planning"
        );
    }

    #[test]
    fn a_join_names_only_what_it_overrides() {
        let params = JoinSessionParams::application("m.call");
        assert!(params.transport.is_none());
        assert!(params.membership_id.is_none());

        let params = params
            .member_id("mine")
            .keep_alive_interval_ms(5_000)
            .transport(RtcTransport::LiveKit(LiveKitTransport {
                livekit_service_url: "https://example.com".to_string(),
            }));
        assert_eq!(params.membership_id(), "mine");
        assert_eq!(params.effective_keep_alive_interval_ms(), 5_000);
        assert!(matches!(
            params.transport,
            Some(TransportIntent::Publish(_))
        ));
    }
}
