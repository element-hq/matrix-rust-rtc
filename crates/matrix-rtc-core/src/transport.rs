// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! RTC transport types for MatrixRTC.
//!
//! This module defines the transport types that can appear in `m.rtc.member` events
//! as specified in MSC4143 and MSC4195, including the transitional LiveKit
//! transport that names the legacy JWT service, the SFU, or both.

use serde::{Deserialize, Serialize};

use crate::compat::MembershipFormat;
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

/// The LiveKit transport (MSC4195), as a homeserver advertises it and a member
/// publishes it.
///
/// During the move to the homeserver-issued tokens of the current MSC4195 a
/// transport names the SFU (`url`), the legacy JWT service
/// (`livekit_service_url`), or both. Which of the two is present decides how a
/// token is obtained, and therefore which participant identity it mints — see
/// [`Self::auth_flow`]. At least one of them is always present.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "LiveKitTransportFields", into = "LiveKitTransportFields")]
pub struct LiveKitTransport {
    url: Option<String>,
    livekit_service_url: Option<String>,
    server_name: Option<String>,
}

/// How a token for a [`LiveKitTransport`] is obtained, by publisher and
/// subscriber alike.
///
/// The flow decides the participant identity the token mints, and peers map a
/// participant to a member by that identity, so it is a property of the
/// transport and never of the role or the membership format.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LiveKitAuthFlow<'a> {
    /// An OpenID token exchanged at the legacy JWT service's `/sfu/get`,
    /// minting `{user_id}:{device_id}`. Used whenever the transport still
    /// names the service, even alongside a `url`: clients predating the
    /// homeserver endpoints only look for that identity.
    SfuGet {
        /// The JWT service's base URL.
        service_url: &'a str,
    },
    /// The homeserver's MSC4195 `get_token`, minting
    /// `base64(sha256(canonical_json([sender, member.id])))`.
    CsApi {
        /// The SFU's WebSocket URL.
        url: &'a str,
    },
}

impl LiveKitTransport {
    /// A transport with whichever fields were advertised; `None` when it
    /// names neither an SFU nor a JWT service, or names a `server_name`
    /// without an SFU (the server name only qualifies a `url`).
    pub fn new(
        url: Option<String>,
        livekit_service_url: Option<String>,
        server_name: Option<String>,
    ) -> Option<Self> {
        if url.is_none() && livekit_service_url.is_none() {
            return None;
        }
        if server_name.is_some() && url.is_none() {
            return None;
        }
        Some(Self {
            url,
            livekit_service_url,
            server_name,
        })
    }

    /// A legacy transport naming only the JWT service.
    pub fn service(livekit_service_url: impl Into<String>) -> Self {
        Self {
            url: None,
            livekit_service_url: Some(livekit_service_url.into()),
            server_name: None,
        }
    }

    /// A current MSC4195 transport naming only the SFU.
    pub fn sfu(url: impl Into<String>) -> Self {
        Self {
            url: Some(url.into()),
            livekit_service_url: None,
            server_name: None,
        }
    }

    /// The SFU's WebSocket URL, if advertised.
    pub fn url(&self) -> Option<&str> {
        self.url.as_deref()
    }

    /// The legacy JWT service's base URL, if advertised.
    pub fn livekit_service_url(&self) -> Option<&str> {
        self.livekit_service_url.as_deref()
    }

    /// The homeserver operating the SFU at [`Self::url`], when it is not the
    /// publisher's own.
    pub fn server_name(&self) -> Option<&str> {
        self.server_name.as_deref()
    }

    /// The token flow for this transport: the JWT service whenever one is
    /// named, the homeserver otherwise. Field presence decides, never a URL's
    /// scheme.
    pub fn auth_flow(&self) -> LiveKitAuthFlow<'_> {
        match (&self.livekit_service_url, &self.url) {
            (Some(service_url), _) => LiveKitAuthFlow::SfuGet { service_url },
            (None, Some(url)) => LiveKitAuthFlow::CsApi { url },
            (None, None) => unreachable!("a LiveKit transport names an SFU or a service"),
        }
    }

    /// The homeserver a token for this transport is requested for, when its
    /// publisher is `publisher_user_id`: the advertised `server_name`, else the
    /// publisher's own server.
    pub fn sfu_server_name<'a>(&'a self, publisher_user_id: &'a str) -> &'a str {
        self.server_name
            .as_deref()
            .unwrap_or_else(|| server_name_of(publisher_user_id))
    }
}

/// The shortest media key any transport takes: LiveKit's 32 bytes (spec 038
/// R16). Our keys are never shorter, and an MSC room drops a shorter one; a
/// room-state room takes Element Call's 16-byte keys.
pub const MIN_MEDIA_KEY_LEN: usize = 32;

impl RtcTransport {
    /// The media key length, in bytes, this transport needs from a member
    /// publishing on it; `None` for a transport we do not understand.
    pub fn required_key_len(&self) -> Option<usize> {
        match self {
            RtcTransport::LiveKit(_) => Some(MIN_MEDIA_KEY_LEN),
            RtcTransport::Unsupported(_) => None,
        }
    }

    /// This transport as we would publish it after moving our media onto it
    /// from `publisher_user_id`'s membership (034 R25): an SFU the publisher's
    /// homeserver operates is named with that server, since peers would
    /// otherwise attribute it to ours.
    pub fn as_published_by(&self, publisher_user_id: &str) -> RtcTransport {
        match self {
            RtcTransport::LiveKit(livekit)
                if livekit.url.is_some() && livekit.server_name.is_none() =>
            {
                RtcTransport::LiveKit(LiveKitTransport {
                    server_name: Some(server_name_of(publisher_user_id).to_owned()),
                    ..livekit.clone()
                })
            }
            other => other.clone(),
        }
    }
}

/// The server part of a Matrix user id (`@user:server` → `server`); the whole
/// string when it has none.
pub fn server_name_of(user_id: &str) -> &str {
    user_id
        .split_once(':')
        .map_or(user_id, |(_, server)| server)
}

/// The wire shape of a [`LiveKitTransport`], without the `type`.
#[derive(Serialize, Deserialize)]
struct LiveKitTransportFields {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    livekit_service_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    server_name: Option<String>,
}

impl TryFrom<LiveKitTransportFields> for LiveKitTransport {
    type Error = &'static str;

    fn try_from(fields: LiveKitTransportFields) -> Result<Self, Self::Error> {
        Self::new(fields.url, fields.livekit_service_url, fields.server_name)
            .ok_or("a LiveKit transport needs `url` or `livekit_service_url`, and `server_name` only with `url`")
    }
}

impl From<LiveKitTransport> for LiveKitTransportFields {
    fn from(transport: LiveKitTransport) -> Self {
        Self {
            url: transport.url,
            livekit_service_url: transport.livekit_service_url,
            server_name: transport.server_name,
        }
    }
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

/// The MSC4195 transport type, which the MSC format reads and writes.
pub const MSC_LIVEKIT_TYPE: &str = "m.livekit";
/// The transport type implementations wrote before MSC4195 settled, which the
/// room-state format reads and writes, as Element Call web does.
pub const LEGACY_LIVEKIT_TYPE: &str = "livekit";

impl RawRtcTransport {
    /// Converts into a typed transport the way a room in `format` reads it.
    ///
    /// In MSC only an `m.livekit` entry with a `url` is LiveKit, read by that
    /// `url` alone, so every token comes from the homeserver (spec 038 R19,
    /// R20); anything else is unsupported. Room-state takes either type with
    /// either field, as Element Call web does ([`Self::into_typed`]).
    pub fn into_typed_in(self, format: MembershipFormat) -> RtcTransport {
        if format == MembershipFormat::RoomState {
            return self.into_typed();
        }
        let field = |name: &str| {
            self.extra_fields
                .get(name)
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        };
        let msc = (self.transport_type == MSC_LIVEKIT_TYPE)
            .then(|| LiveKitTransport::new(Some(field("url")?), None, field("server_name")))
            .flatten();
        match msc {
            Some(livekit) => RtcTransport::LiveKit(livekit),
            None => RtcTransport::Unsupported(UnsupportedTransport {
                transport_type: self.transport_type,
                extra_fields: self.extra_fields,
            }),
        }
    }

    /// Convert into a typed RtcTransport, as the room-state format reads it.
    /// Known transport types are parsed into their specific variants,
    /// while unknown types become UnsupportedTransport.
    pub fn into_typed(self) -> RtcTransport {
        match self.transport_type.as_str() {
            // The MSC's `m.livekit`; implementations still write `livekit`.
            "livekit" | "m.livekit" => {
                let field = |name: &str| {
                    self.extra_fields
                        .get(name)
                        .and_then(|v| v.as_str())
                        .map(str::to_owned)
                };
                match LiveKitTransport::new(
                    field("url"),
                    field("livekit_service_url"),
                    field("server_name"),
                ) {
                    Some(livekit) => RtcTransport::LiveKit(livekit),
                    // Malformed: kept for forward compatibility, never connected.
                    None => RtcTransport::Unsupported(UnsupportedTransport {
                        transport_type: self.transport_type,
                        extra_fields: self.extra_fields,
                    }),
                }
            }
            _ => RtcTransport::Unsupported(UnsupportedTransport {
                transport_type: self.transport_type,
                extra_fields: self.extra_fields,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::own_membership::transport_to_json;

    fn typed(value: serde_json::Value) -> RtcTransport {
        serde_json::from_value::<RawRtcTransport>(value)
            .unwrap()
            .into_typed()
    }

    #[test]
    fn each_advertised_shape_selects_its_flow() {
        let RtcTransport::LiveKit(service) =
            typed(json!({ "type": "livekit", "livekit_service_url": "https://jwt" }))
        else {
            panic!("a livekit transport");
        };
        assert_eq!(
            service.auth_flow(),
            LiveKitAuthFlow::SfuGet {
                service_url: "https://jwt"
            }
        );

        let RtcTransport::LiveKit(both) = typed(json!({
            "type": "livekit",
            "url": "wss://sfu",
            "livekit_service_url": "https://jwt",
        })) else {
            panic!("a livekit transport");
        };
        assert_eq!(
            both.auth_flow(),
            LiveKitAuthFlow::SfuGet {
                service_url: "https://jwt"
            },
            "a named service wins over the url"
        );

        let RtcTransport::LiveKit(sfu) = typed(json!({ "type": "m.livekit", "url": "wss://sfu" }))
        else {
            panic!("the MSC's type is a livekit transport too");
        };
        assert_eq!(sfu.auth_flow(), LiveKitAuthFlow::CsApi { url: "wss://sfu" });
    }

    #[test]
    fn a_malformed_livekit_transport_is_kept_but_unsupported() {
        assert!(matches!(
            typed(json!({ "type": "livekit" })),
            RtcTransport::Unsupported(_)
        ));
        assert!(matches!(
            typed(json!({
                "type": "livekit",
                "livekit_service_url": "https://jwt",
                "server_name": "other.org",
            })),
            RtcTransport::Unsupported(_)
        ));
    }

    /// Spec 038 R16: a LiveKit member needs 32-byte keys; an unknown transport
    /// asks for nothing, leaving the floor to decide.
    #[test]
    fn livekit_needs_32_byte_keys() {
        let livekit = typed(json!({ "type": "livekit", "url": "wss://sfu" }));
        assert_eq!(livekit.required_key_len(), Some(MIN_MEDIA_KEY_LEN));
        assert_eq!(MIN_MEDIA_KEY_LEN, 32);
        let other = typed(json!({ "type": "whip", "url": "https://whip" }));
        assert_eq!(other.required_key_len(), None);
    }

    /// Room-state peers read our token flow off the fields we publish, so
    /// they go out exactly as advertised, under Element Call's `livekit`
    /// (spec 038 R39).
    #[test]
    fn room_state_publishes_a_transport_with_exactly_its_fields() {
        for advertised in [
            json!({ "type": "livekit", "livekit_service_url": "https://jwt" }),
            json!({ "type": "livekit", "url": "wss://sfu", "livekit_service_url": "https://jwt" }),
            json!({ "type": "livekit", "url": "wss://sfu", "server_name": "other.org" }),
        ] {
            assert_eq!(
                transport_to_json(&typed(advertised.clone()), MembershipFormat::RoomState),
                advertised
            );
        }
        assert_eq!(
            transport_to_json(
                &typed(json!({ "type": "m.livekit", "url": "wss://sfu" })),
                MembershipFormat::RoomState
            ),
            json!({ "type": "livekit", "url": "wss://sfu" })
        );
    }

    /// MSC writes `{type: "m.livekit", url}`, with the `server_name` of an SFU
    /// that is not ours (spec 038 R17, R23).
    #[test]
    fn msc_publishes_m_livekit_by_its_url() {
        assert_eq!(
            transport_to_json(
                &typed(json!({ "type": "livekit", "url": "wss://sfu" })),
                MembershipFormat::Msc
            ),
            json!({ "type": "m.livekit", "url": "wss://sfu" })
        );
        assert_eq!(
            transport_to_json(
                &typed(
                    json!({ "type": "m.livekit", "url": "wss://sfu", "server_name": "other.org" })
                ),
                MembershipFormat::Msc
            ),
            json!({ "type": "m.livekit", "url": "wss://sfu", "server_name": "other.org" })
        );
    }

    /// Spec 038 R19, R20: in MSC only `m.livekit` with a `url` is LiveKit,
    /// read by the `url` alone; room-state reads either type with either
    /// field.
    #[test]
    fn each_format_reads_its_own_livekit_transports() {
        let read = |value: serde_json::Value, format| {
            serde_json::from_value::<RawRtcTransport>(value)
                .unwrap()
                .into_typed_in(format)
        };
        let both = json!({ "type": "m.livekit", "url": "wss://sfu", "livekit_service_url": "https://jwt" });
        let RtcTransport::LiveKit(msc) = read(both.clone(), MembershipFormat::Msc) else {
            panic!("an m.livekit url is LiveKit in MSC");
        };
        assert_eq!(msc.auth_flow(), LiveKitAuthFlow::CsApi { url: "wss://sfu" });
        let RtcTransport::LiveKit(legacy) = read(both, MembershipFormat::RoomState) else {
            panic!("LiveKit in room-state");
        };
        assert_eq!(
            legacy.auth_flow(),
            LiveKitAuthFlow::SfuGet {
                service_url: "https://jwt"
            }
        );

        for unsupported in [
            json!({ "type": "livekit", "url": "wss://sfu" }),
            json!({ "type": "livekit", "livekit_service_url": "https://jwt" }),
            json!({ "type": "m.livekit", "livekit_service_url": "https://jwt" }),
        ] {
            assert!(
                matches!(
                    read(unsupported.clone(), MembershipFormat::Msc),
                    RtcTransport::Unsupported(_)
                ),
                "{unsupported}"
            );
            assert!(matches!(
                read(unsupported, MembershipFormat::RoomState),
                RtcTransport::LiveKit(_)
            ));
        }
    }

    #[test]
    fn moving_onto_a_peers_sfu_names_its_server() {
        let sfu = RtcTransport::LiveKit(LiveKitTransport::sfu("wss://sfu.other.org"));
        let RtcTransport::LiveKit(moved) = sfu.as_published_by("@bob:other.org") else {
            panic!("still livekit");
        };
        assert_eq!(moved.server_name(), Some("other.org"));

        let service = RtcTransport::LiveKit(LiveKitTransport::service("https://jwt"));
        assert_eq!(service.as_published_by("@bob:other.org"), service);
    }
}
