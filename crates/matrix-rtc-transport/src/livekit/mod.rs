// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The pure half of the MSC4195 LiveKit control plane.
//!
//! Everything a MatrixRTC client must *compute* to reach a LiveKit SFU, with
//! none of the IO: the hash derivations ([`identity`]), the token requests'
//! shapes ([`token`]), and the one rule tying them together: a transport's
//! fields select the token flow ([`LiveKitAuthFlow`]), and the flow decides the
//! participant identity ([`LiveKitTarget`], [`identity_for`]). The native
//! transport (`matrix-rtc-livekit`, which owns libwebrtc and reqwest) builds on
//! this; so does the web binding, where the browser does the fetching and
//! livekit-js does the media — which is why this module must stay free of
//! `livekit`, HTTP clients, and async runtimes, and compile for
//! `wasm32-unknown-unknown`.

pub mod identity;
pub mod token;

pub use token::{SfuToken, TokenServiceError};

use matrix_rtc_core::{
    IdentitySubject, LiveKitAuthFlow, LiveKitTransport, RtcIdentityMapper, RtcTransport,
};
use serde::{Deserialize, Serialize};

/// The participant-identity derivation every LiveKit derivation site shares.
///
/// Call it once per call and share the returned `Arc`: it has four uses (the
/// core's encryption manager, the media transport, our own identity, and the
/// key ring — see `matrix_rtc_call_sdk::attach_media`), and they must not
/// skew. That matters more than it looks, because a divergence is not an error
/// but a silence: peers appear in the roster with no media, their keys land
/// under an identity the SFU never assigned, and nothing anywhere logs a
/// problem.
///
/// This is deliberately a plain Rust closure even on the web:
/// `RtcIdentityMapper` is `Send + Sync`, which a JS-backed function can never
/// be, and the derivation is a hash with no reason to cross the boundary.
pub fn identity_mapper() -> RtcIdentityMapper {
    std::sync::Arc::new(identity_for)
}

/// The identity a member holds on the SFU it publishes on: the one minted by
/// the token flow its first LiveKit transport selects (the transport the pool
/// subscribes to). `None` when it publishes no LiveKit transport, or when
/// `/sfu/get` minted it and its device cannot be attributed.
pub fn identity_for(subject: &IdentitySubject<'_>) -> Option<String> {
    let livekit = first_livekit(subject.transports)?;
    match livekit.auth_flow() {
        LiveKitAuthFlow::SfuGet { .. } => subject
            .device_id
            .map(|device_id| identity::legacy_identity(subject.user_id, device_id)),
        LiveKitAuthFlow::CsApi { .. } => Some(identity::msc4195_identity(
            subject.user_id,
            subject.member_id,
        )),
    }
}

/// The first LiveKit transport of a member's published ones.
pub fn first_livekit(transports: &[RtcTransport]) -> Option<&LiveKitTransport> {
    transports.iter().find_map(|transport| match transport {
        RtcTransport::LiveKit(livekit) => Some(livekit),
        RtcTransport::Unsupported(_) => None,
    })
}

/// Where a LiveKit connection goes and how its token is obtained: a
/// transport's [`LiveKitAuthFlow`], resolved for one publisher.
///
/// Its [`connection_key`](Self::connection_key) groups connections the way
/// Element Call's `keyFor` does: by the flow and what the flow authenticates
/// against. Two members reaching one SFU through the same flow share one
/// connection, so we never connect twice under the same identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "flow", rename_all = "snake_case")]
pub enum LiveKitTarget {
    /// The legacy JWT service's `/sfu/get`, with an OpenID token. The
    /// service's answer names the SFU.
    SfuGet { service_url: String },
    /// The homeserver's MSC4195 `get_token`, for the SFU at `url` operated by
    /// `server_name`.
    CsApi { url: String, server_name: String },
}

impl LiveKitTarget {
    /// The target for `transport` as published by `publisher_user_id`; ours
    /// when we publish on it.
    pub fn for_transport(transport: &LiveKitTransport, publisher_user_id: &str) -> Self {
        match transport.auth_flow() {
            LiveKitAuthFlow::SfuGet { service_url } => Self::SfuGet {
                service_url: service_url.to_owned(),
            },
            LiveKitAuthFlow::CsApi { url } => Self::CsApi {
                url: url.to_owned(),
                server_name: transport.sfu_server_name(publisher_user_id).to_owned(),
            },
        }
    }

    /// The pool's grouping key for this target, from which
    /// [`Self::from_connection_key`] recovers it.
    pub fn connection_key(&self) -> String {
        serde_json::to_string(self).expect("a target is a plain struct of strings")
    }

    /// The target a [`Self::connection_key`] was made from.
    pub fn from_connection_key(key: &str) -> Option<Self> {
        serde_json::from_str(key).ok()
    }

    /// Our identity on this target's SFU: what its token flow mints for us.
    pub fn own_identity(&self, user_id: &str, device_id: &str, member_id: &str) -> String {
        match self {
            Self::SfuGet { .. } => identity::legacy_identity(user_id, device_id),
            Self::CsApi { .. } => identity::msc4195_identity(user_id, member_id),
        }
    }

    /// Whether a token for `other` mints the same identity for us as one for
    /// this: only then can our media move between the two (034 R25).
    pub fn same_flow(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Self::SfuGet { .. }, Self::SfuGet { .. }) | (Self::CsApi { .. }, Self::CsApi { .. })
        )
    }

    /// A short label for logs.
    pub fn describe(&self) -> &str {
        match self {
            Self::SfuGet { service_url } => service_url,
            Self::CsApi { url, .. } => url,
        }
    }
}

/// The connection key for `transport` as published by `publisher_user_id`,
/// when it is a LiveKit one: shared by the native and web transports.
pub fn connection_key(transport: &RtcTransport, publisher_user_id: &str) -> Option<String> {
    match transport {
        RtcTransport::LiveKit(livekit) => {
            Some(LiveKitTarget::for_transport(livekit, publisher_user_id).connection_key())
        }
        RtcTransport::Unsupported(_) => None,
    }
}

/// Configuration identifying the SFU and the MatrixRTC slot to connect to.
#[derive(Clone, Debug)]
pub struct LiveKitTransportConfig {
    /// Where to connect, and how to get the token.
    pub target: LiveKitTarget,
    /// Matrix room ID hosting the `m.rtc.member` event.
    pub room_id: String,
    /// MatrixRTC slot ID.
    pub slot_id: String,
    /// Our `member.id`, for the homeserver's `get_token`.
    pub member_id: String,
    /// Our device, for `/sfu/get`, which mints `{user}:{device}`.
    pub device_id: String,
}

#[cfg(test)]
mod tests;
