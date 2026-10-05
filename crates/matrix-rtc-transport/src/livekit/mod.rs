// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The pure half of the MSC4195 LiveKit control plane.
//!
//! Everything a MatrixRTC client must *compute* to reach a LiveKit SFU, with
//! none of the IO: the hash derivations ([`identity`]), the authorisation
//! service's request/response shapes ([`token`]), and the per-generation
//! dialect choices ([`TokenEndpoint`], [`identity_mapper`]). The native
//! transport (`matrix-rtc-livekit`, which owns libwebrtc and reqwest) builds on
//! this; so does the web binding, where the browser does the fetching and
//! livekit-js does the media — which is why this module must stay free of
//! `livekit`, HTTP clients, and async runtimes, and compile for
//! `wasm32-unknown-unknown`.

pub mod identity;
pub mod token;

pub use token::{MemberClaims, SfuToken, TokenServiceError};

use matrix_rtc_core::compat::{MembershipFormat, room_state};

/// The participant-identity derivation a MatrixRTC generation's authorisation
/// service uses.
///
/// One of the two things `matrix_rtc_core::compat` deliberately cannot own — the
/// other being [`TokenEndpoint`] — because the modern derivation hashes per
/// MSC4195, which is a LiveKit document rather than a Matrix wire format. The
/// `compat` module decides *which generation*; this decides *what that means
/// for an identity*.
///
/// Call it once per call and share the returned `Arc`: it has four uses (the
/// core's encryption manager, the media transport, our own identity, and the
/// key ring — see `matrix_rtc_media::attach_media`), and they must not
/// skew. That matters more than it looks, because a divergence is not an error
/// but a silence: peers appear in the roster with no media, their keys land
/// under an identity the SFU never assigned, and nothing anywhere logs a
/// problem.
///
/// This is deliberately a plain Rust closure even on the web:
/// `RtcIdentityMapper` is `Send + Sync`, which a JS-backed function can never
/// be, and the derivation is a hash with no reason to cross the boundary.
pub fn identity_mapper(compat: MembershipFormat) -> matrix_rtc_core::RtcIdentityMapper {
    use std::sync::Arc;

    match compat {
        MembershipFormat::Current | MembershipFormat::Sticky2025 => {
            Arc::new(identity::pseudonymous_identity)
        }
        // That generation's authorisation service issues the unhashed
        // `{user}:{device}` string, and has no session component at all.
        MembershipFormat::RoomState => {
            Arc::new(|user_id: &str, device_id: &str, _member_id: &str| {
                room_state::participant_identity(user_id, device_id)
            })
        }
    }
}

/// Which authorisation-service dialect to speak.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TokenEndpoint {
    /// MSC4195 `POST /get_token`.
    #[default]
    Msc4195,
    /// Pre-MSC4195 `POST /sfu/get`, for Element Call builds older than MSC4354.
    /// Temporary; see `matrix_rtc_core::compat`.
    LegacyElementCall,
}

impl TokenEndpoint {
    /// The endpoint a MatrixRTC generation's authorisation service speaks.
    /// `/sfu/get` is also where the pre-sticky generation's unhashed identity
    /// comes from — the endpoint mints the identity — so this and
    /// [`identity_mapper`] are one decision, taken from the same format.
    pub fn for_format(compat: MembershipFormat) -> Self {
        match compat {
            MembershipFormat::RoomState => Self::LegacyElementCall,
            MembershipFormat::Current | MembershipFormat::Sticky2025 => Self::Msc4195,
        }
    }
}

/// Configuration identifying the MatrixRTC slot to connect to.
#[derive(Clone, Debug)]
pub struct LiveKitTransportConfig {
    /// `livekit_service_url` advertised by the transport (the authorisation
    /// service base URL, e.g. `https://matrix-rtc.example.com/livekit/jwt`).
    pub livekit_service_url: String,
    /// Matrix room ID hosting the `m.rtc.member` event.
    pub room_id: String,
    /// MatrixRTC slot ID.
    pub slot_id: String,
    /// `member` claims identifying this membership to the authorisation service.
    pub member: MemberClaims,
    /// Which authorisation-service dialect to speak. Defaults to MSC4195.
    pub token_endpoint: TokenEndpoint,
}
