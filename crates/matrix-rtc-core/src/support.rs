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

use crate::compat::MembershipFormat;
use crate::transport::{RawRtcTransport, RtcTransport};

/// MatrixRTC (MSC4143), under its unstable and its stable-ready flags.
const MATRIX_RTC_FEATURES: [&str; 2] = ["org.matrix.msc4143", "org.matrix.msc4143.stable"];
/// Sticky events (MSC4354), which carry MSC memberships.
const STICKY_EVENTS_FEATURE: &str = "org.matrix.msc4354";
/// Delayed events (MSC4140), which carry the delayed leave.
const DELAYED_EVENTS_FEATURE: &str = "org.matrix.msc4140";
/// MSC4195's `get_token` is stable on this homeserver: what the LiveKit
/// transition checks before taking a token from it (spec 049 R8, R13).
const LIVEKIT_GET_TOKEN_STABLE_FEATURE: &str = "org.matrix.msc4195.stable";

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

/// TEMPORARY (spec 049): which LiveKit token flows a joined call may use on
/// transports announcing both `url` and `livekit_service_url`, read once at
/// join (R12). The default keeps every such transport on `/sfu/get`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LiveKitFlows {
    /// Our homeserver's `get_token` is stable: peers are subscribed through it
    /// (R13).
    pub subscribe_get_token: bool,
    /// We publish through `get_token` too: the local flag is on and the
    /// homeserver's `get_token` is stable (R8).
    pub publish_get_token: bool,
}

impl LiveKitFlows {
    /// From `server_features` (the `unstable_features` of `/versions` that are
    /// on, as [`msc_support`] takes them) and the local flag.
    pub fn new(server_features: &[String], allow_get_token: bool) -> Self {
        let stable = server_features
            .iter()
            .any(|feature| feature == LIVEKIT_GET_TOKEN_STABLE_FEATURE);
        Self {
            subscribe_get_token: stable,
            publish_get_token: stable && allow_get_token,
        }
    }
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
    fn get_token_needs_the_stable_feature_and_publishing_needs_the_flag() {
        let stable = features(&["org.matrix.msc4195.stable"]);
        assert_eq!(LiveKitFlows::new(&[], true), LiveKitFlows::default());
        assert_eq!(
            LiveKitFlows::new(&features(&["org.matrix.msc4195"]), true),
            LiveKitFlows::default()
        );
        assert_eq!(
            LiveKitFlows::new(&stable, false),
            LiveKitFlows {
                subscribe_get_token: true,
                publish_get_token: false,
            }
        );
        assert_eq!(
            LiveKitFlows::new(&stable, true),
            LiveKitFlows {
                subscribe_get_token: true,
                publish_get_token: true,
            }
        );
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
