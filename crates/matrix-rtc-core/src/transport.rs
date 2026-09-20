// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! RTC transport types for MatrixRTC.
//!
//! This module defines the transport types that can appear in `m.rtc.member` events
//! as specified in MSC4143 and MSC4195.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A single RTC transport specification from an m.rtc.member event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RtcTransport {
    /// LiveKit SFU transport as defined in MSC4195.
    LiveKit(LiveKitTransport),
    /// An unsupported or unknown transport type.
    /// Holds the raw transport data for forward compatibility.
    Unsupported(UnsupportedTransport),
}

/// LiveKit-specific transport configuration (MSC4195).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveKitTransport {
    /// URL of the service that issues JWT tokens for connecting to the LiveKit SFU.
    pub livekit_service_url: String,
}

/// An unsupported transport type, storing raw data for forward compatibility.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnsupportedTransport {
    /// The transport type string, e.g., "webrtc", "p2p", etc.
    pub transport_type: String,
    /// The raw JSON fields for this transport (excluding "type").
    pub extra_fields: BTreeMap<String, serde_json::Value>,
}

/// Raw transport data as it appears in the JSON, before parsing into typed variants.
/// Used for deserialization from SDK events.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RawRtcTransport {
    /// The transport type, e.g., "livekit".
    #[serde(rename = "type")]
    pub transport_type: String,
    /// Additional transport-specific fields.
    #[serde(flatten)]
    pub extra_fields: BTreeMap<String, serde_json::Value>,
}

/// The `content.transports` object of an `m.rtc.member` event (MSC4143).
///
/// Replaces the earlier flat `rtc_transports` array: publishing and subscribing
/// are now described separately, so peers can pick a transport that every member
/// can actually receive.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MemberTransports {
    /// Transports this member publishes media on.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub published: Vec<RawRtcTransport>,
    /// Transport types this member is able to subscribe to.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub can_subscribe: Vec<String>,
}

impl MemberTransports {
    /// True when neither list carries anything (used to skip serialization).
    pub fn is_empty(&self) -> bool {
        self.published.is_empty() && self.can_subscribe.is_empty()
    }

    /// Builds the object for a member publishing on `transport`, declaring that
    /// it can also subscribe to that transport type.
    pub fn publishing(transport: RawRtcTransport) -> Self {
        Self {
            can_subscribe: vec![transport.transport_type.clone()],
            published: vec![transport],
        }
    }
}

impl RawRtcTransport {
    /// Convert into a typed RtcTransport.
    /// Known transport types are parsed into their specific variants,
    /// while unknown types become UnsupportedTransport.
    pub fn into_typed(self) -> RtcTransport {
        match self.transport_type.as_str() {
            "livekit" => {
                let url = self
                    .extra_fields
                    .get("livekit_service_url")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                if let Some(url) = url {
                    RtcTransport::LiveKit(LiveKitTransport {
                        livekit_service_url: url,
                    })
                } else {
                    // LiveKit transport without required field -> unsupported
                    RtcTransport::Unsupported(UnsupportedTransport {
                        transport_type: self.transport_type,
                        extra_fields: self.extra_fields,
                    })
                }
            }
            _ => RtcTransport::Unsupported(UnsupportedTransport {
                transport_type: self.transport_type,
                extra_fields: self.extra_fields,
            }),
        }
    }
}
