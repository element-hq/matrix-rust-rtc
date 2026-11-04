// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The legacy LiveKit JWT service's token exchange shapes.
//!
//! A transport naming a `livekit_service_url` gets its token from that
//! service's `/sfu/get`: the client `POST`s a Matrix OpenID token and the
//! Matrix room id, and the service answers `{jwt, url}`. A transport naming
//! only an SFU `url` gets its token from the homeserver instead (MSC4195
//! `get_token`), which the host performs through
//! `MatrixBackend::livekit_token` — so that request never passes through here.
//!
//! This module owns the exchange's *shapes* — endpoint URL, request body,
//! response decoding — and nothing sends them: the native transport POSTs the
//! built request with reqwest, the web binding hands it to the browser's
//! `fetch`. Both feed the raw response back through [`parse_sfu_token`], so
//! the wire format has exactly one owner. Obtaining the OpenID token itself is
//! a Client-Server API concern: the host supplies one through its
//! `MatrixBackend::openid_token` (`matrix-rtc-core`).

use serde::{Deserialize, Serialize};

use matrix_rtc_core::OpenIdToken;

/// A token to connect with: the JWT and the SFU URL it is for.
#[derive(Clone, Debug, Deserialize)]
pub struct SfuToken {
    /// JWT to authenticate with the LiveKit SFU.
    pub jwt: String,
    /// WebSocket URL of the LiveKit SFU to connect to for this slot.
    pub url: String,
}

/// The authorisation service's answer could not be turned into an [`SfuToken`].
///
/// Transport-level failures (DNS, TLS, aborted fetch) are not here — they
/// belong to whoever performed the IO and never reach [`parse_sfu_token`].
#[derive(Debug, thiserror::Error)]
pub enum TokenServiceError {
    /// The service rejected the request (non-2xx response).
    #[error("LiveKit authorisation service returned {status}: {body}")]
    Rejected { status: u16, body: String },
    /// A 2xx response whose body is not the `{jwt, url}` shape.
    #[error("LiveKit authorisation service response did not decode: {0}")]
    Decode(#[from] serde_json::Error),
}

/// JSON body of a pre-MSC4195 `POST /sfu/get` request.
#[derive(Serialize)]
struct LegacyGetTokenRequest<'a> {
    /// The LiveKit room name, which the legacy service derives from this string
    /// alone.
    ///
    /// It MUST equal the `livekit_alias` we announce in `foci_preferred` —
    /// Element Call uses the Matrix room id for both — or the two clients end up
    /// in different LiveKit rooms and never see each other, with a perfectly
    /// healthy connection on both sides.
    room: &'a str,
    openid_token: &'a OpenIdToken,
    /// The participant identity this service mints is
    /// `{openid subject}:{device_id}`, which is why that generation's identities
    /// are unhashed and why we cannot choose the shape.
    device_id: &'a str,
}

/// Build the **pre-MSC4195** `POST /sfu/get` request: the endpoint URL and the
/// JSON body to send to it.
///
/// Used, for publishing and subscribing alike, on every transport that names a
/// `livekit_service_url`. There is deliberately **no** fallback between this
/// and the homeserver's `get_token`: the endpoint is not an independent choice,
/// it mints the participant identity peers map us by, and that is decided by
/// the transport we published before any HTTP happens. Succeeding through the
/// other flow produces a fully connected SFU session in which nothing decrypts
/// and nobody appears, so a wrong guess would never surface. A 404 stays loud.
pub fn legacy_token_request(
    livekit_service_url: &str,
    room: &str,
    device_id: &str,
    openid_token: &OpenIdToken,
) -> (String, serde_json::Value) {
    let endpoint = format!("{}/sfu/get", livekit_service_url.trim_end_matches('/'));
    let body = serde_json::to_value(LegacyGetTokenRequest {
        room,
        openid_token,
        device_id,
    })
    .expect("token request bodies are plain structs of strings");
    (endpoint, body)
}

/// Decode a token endpoint's raw response into an [`SfuToken`].
///
pub fn parse_sfu_token(status: u16, body: &str) -> Result<SfuToken, TokenServiceError> {
    if !(200..300).contains(&status) {
        return Err(TokenServiceError::Rejected {
            status,
            body: body.to_owned(),
        });
    }
    Ok(serde_json::from_str(body)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_token() -> OpenIdToken {
        OpenIdToken {
            access_token: "FPkexLLvKbAHKclQhpvgfWxx".to_owned(),
            token_type: "Bearer".to_owned(),
            matrix_server_name: "matrix.example.com".to_owned(),
            expires_in: 3600,
        }
    }

    #[test]
    fn sfu_token_deserializes_per_msc4195() {
        let token = parse_sfu_token(
            200,
            r#"{"jwt": "thejwt", "url": "wss://matrix-rtc.example.com/livekit/sfu"}"#,
        )
        .unwrap();
        assert_eq!(token.jwt, "thejwt");
        assert_eq!(token.url, "wss://matrix-rtc.example.com/livekit/sfu");
    }

    /// The pre-MSC4195 body: a flat `room` instead of `room_id` + `slot_id`, a
    /// bare `device_id` instead of the `member` object, and no member id at all —
    /// which is why that generation's SFU identity carries no session component.
    #[test]
    fn legacy_get_token_request_serializes_for_sfu_get() {
        let token = sample_token();
        let (endpoint, body) = legacy_token_request(
            "https://matrix-rtc.example.com/livekit/jwt",
            "!tDLCaLXijNtYcJZEey:example.com",
            "DEVICEID",
            &token,
        );

        assert_eq!(
            endpoint,
            "https://matrix-rtc.example.com/livekit/jwt/sfu/get"
        );
        let expected = serde_json::json!({
            "room": "!tDLCaLXijNtYcJZEey:example.com",
            "openid_token": {
                "access_token": "FPkexLLvKbAHKclQhpvgfWxx",
                "expires_in": 3600,
                "matrix_server_name": "matrix.example.com",
                "token_type": "Bearer"
            },
            "device_id": "DEVICEID"
        });
        assert_eq!(body, expected);
        // Nothing of the MSC4195 body leaks in: the legacy service rejects a
        // body it cannot parse rather than ignoring the extra keys.
        assert!(body.get("room_id").is_none());
        assert!(body.get("slot_id").is_none());
        assert!(body.get("member").is_none());
    }

    /// The response is the one part the two dialects agree on, so `SfuToken`
    /// serves both and there is no legacy response type.
    #[test]
    fn the_legacy_response_is_the_same_shape() {
        let token =
            parse_sfu_token(200, r#"{"jwt": "thejwt", "url": "wss://sfu.example.com"}"#).unwrap();
        assert_eq!(token.jwt, "thejwt");
    }

    #[test]
    fn a_rejection_keeps_the_status_and_body() {
        let error = parse_sfu_token(403, "no dice").unwrap_err();
        match error {
            TokenServiceError::Rejected { status, body } => {
                assert_eq!(status, 403);
                assert_eq!(body, "no dice");
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[test]
    fn a_malformed_success_body_is_a_decode_error() {
        let error = parse_sfu_token(200, r#"{"jwt": 42}"#).unwrap_err();
        assert!(matches!(error, TokenServiceError::Decode(_)));
    }
}
