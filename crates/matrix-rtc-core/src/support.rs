// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Whether a homeserver offers what the MSC format needs (spec 038 R45).
//!
//! A pure check over what the host already reads: the `unstable_features` of
//! `/versions` that are on, and the transports discovery advertises. It names
//! each missing piece and never chooses the format: a host that wants to fall
//! back to room-state decides so itself, for the whole call.

use serde_json::Value;

use crate::BackendError;
use crate::compat::MembershipFormat;
use crate::transport::{RawRtcTransport, RtcTransport};

/// MatrixRTC (MSC4143), under its unstable and its stable-ready flags.
const MATRIX_RTC_FEATURES: [&str; 2] = ["org.matrix.msc4143", "org.matrix.msc4143.stable"];
/// Sticky events (MSC4354), which carry MSC memberships.
const STICKY_EVENTS_FEATURE: &str = "org.matrix.msc4354";
/// Delayed events (MSC4140), which carry the delayed leave.
const DELAYED_EVENTS_FEATURE: &str = "org.matrix.msc4140";

/// One thing the MSC format relies on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MscRequirement {
    /// The homeserver advertises MatrixRTC (MSC4143).
    MatrixRtc,
    /// The homeserver supports sticky events (MSC4354).
    StickyEvents,
    /// The homeserver supports delayed events (MSC4140). Without them a call
    /// still works, on a 5 minute membership lifetime (R43).
    DelayedEvents,
    /// Discovery advertises an `m.livekit` transport naming an SFU `url`
    /// (R17).
    LiveKitTransport,
}

/// What [`msc_support`] found missing.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MscSupport {
    /// Without these an MSC call cannot work.
    pub missing: Vec<MscRequirement>,
    /// Without these an MSC call works, worse.
    pub warnings: Vec<MscRequirement>,
}

impl MscSupport {
    /// Whether an MSC call can work here.
    pub fn is_supported(&self) -> bool {
        self.missing.is_empty()
    }
}

/// Checks `server_features` (the `unstable_features` of `/versions` that are
/// on) and `rtc_transports` (what discovery advertises) against what the MSC
/// format needs (spec 038 R45).
pub fn msc_support(server_features: &[String], rtc_transports: &Value) -> MscSupport {
    let has = |feature: &str| server_features.iter().any(|f| f == feature);
    let mut support = MscSupport::default();
    if !MATRIX_RTC_FEATURES.iter().any(|feature| has(feature)) {
        support.missing.push(MscRequirement::MatrixRtc);
    }
    if !has(STICKY_EVENTS_FEATURE) {
        support.missing.push(MscRequirement::StickyEvents);
    }
    let livekit = rtc_transports
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| serde_json::from_value::<RawRtcTransport>(entry.clone()).ok())
        .any(|entry| {
            matches!(
                entry.into_typed_in(MembershipFormat::Msc),
                RtcTransport::LiveKit(_)
            )
        });
    if !livekit {
        support.missing.push(MscRequirement::LiveKitTransport);
    }
    if !has(DELAYED_EVENTS_FEATURE) {
        support.warnings.push(MscRequirement::DelayedEvents);
    }
    support
}

/// TEMPORARY (spec 049): whether a joined call's LiveKit connections take
/// their tokens from `get_token` on transports announcing both `url` and
/// `livekit_service_url`, settled once as media attaches (R12). The default
/// keeps every such transport on `/sfu/get`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LiveKitFlows {
    /// The local flag is on and our homeserver answered `get_token` (R8).
    pub get_token: bool,
    /// The answer, `(SFU url, jwt)`: the token our first own connect uses,
    /// so the probe costs no request of its own.
    pub own_token: Option<(String, String)>,
}

/// TEMPORARY (spec 049 R8): whether a failed `get_token` says our homeserver
/// lacks the endpoint, rather than refusing this one request. No `/versions`
/// flag announces the unstable endpoint, so asking is the only way to know.
pub fn get_token_unsupported(error: &BackendError) -> bool {
    error.status == Some(404)
        || matches!(
            error.errcode.as_deref(),
            Some("M_NOT_FOUND" | "M_UNRECOGNIZED")
        )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn features(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    #[test]
    fn a_homeserver_with_everything_is_supported() {
        let support = msc_support(
            &features(&[
                "org.matrix.msc4143",
                "org.matrix.msc4354",
                "org.matrix.msc4140",
            ]),
            &json!([{ "type": "m.livekit", "url": "wss://sfu" }]),
        );
        assert_eq!(support, MscSupport::default());
        assert!(support.is_supported());
    }

    #[test]
    fn each_missing_piece_is_named() {
        let support = msc_support(
            &[],
            &json!([{ "type": "livekit", "livekit_service_url": "https://jwt" }]),
        );
        assert_eq!(
            support.missing,
            [
                MscRequirement::MatrixRtc,
                MscRequirement::StickyEvents,
                MscRequirement::LiveKitTransport,
            ]
        );
        assert_eq!(support.warnings, [MscRequirement::DelayedEvents]);
        assert!(!support.is_supported());
    }

    #[test]
    fn only_an_absent_endpoint_rules_get_token_out() {
        let answered = |errcode: Option<&str>, status| {
            BackendError::with_matrix_error(errcode.map(str::to_owned), Some(status), "refused")
        };
        assert!(get_token_unsupported(&answered(
            Some("M_UNRECOGNIZED"),
            400
        )));
        assert!(get_token_unsupported(&answered(Some("M_NOT_FOUND"), 404)));
        assert!(get_token_unsupported(&answered(None, 404)));
        assert!(!get_token_unsupported(&answered(Some("M_FORBIDDEN"), 403)));
        assert!(!get_token_unsupported(&answered(
            Some("M_LIMIT_EXCEEDED"),
            429
        )));
        assert!(!get_token_unsupported(&BackendError::new("offline")));
    }

    /// Delayed events only warn: R43 bounds a crash without them.
    #[test]
    fn missing_delayed_events_still_supports_msc() {
        let support = msc_support(
            &features(&["org.matrix.msc4143.stable", "org.matrix.msc4354"]),
            &json!([{ "type": "m.livekit", "url": "wss://sfu" }]),
        );
        assert!(support.is_supported());
        assert_eq!(support.warnings, [MscRequirement::DelayedEvents]);
    }
}
