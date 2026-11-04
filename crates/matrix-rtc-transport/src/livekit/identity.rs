// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Hash derivations defined by [MSC4195].
//!
//! Both the `livekit_alias` and the LiveKit participant identity the
//! homeserver mints are computed as `base64(SHA256(JSON.serialize([...])))`, where
//! `JSON.serialize` is [Matrix canonical JSON] and `base64` is the unpadded
//! standard alphabet.
//!
//! The `livekit_alias` and SFU URL normally reach the client baked into the JWT
//! returned by the authorisation service, so a connecting client does not need
//! to derive the alias itself. The participant identity, however, is needed to
//! map a received per-participant encryption key (from `matrix-rtc-core`) onto
//! the corresponding LiveKit participant. The legacy JWT service's unhashed
//! identity lives here too, so both derivations have one home.
//!
//! [MSC4195]: https://github.com/matrix-org/matrix-spec-proposals/pull/4195
//! [Matrix canonical JSON]: https://spec.matrix.org/v1.18/appendices/#canonical-json

use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use sha2::{Digest, Sha256};

/// Serialize an array of strings to Matrix canonical JSON.
///
/// MSC4195 only ever hashes arrays whose elements are JSON strings, so the
/// canonical form is exactly the compact, key-less array `serde_json` already
/// emits (no insignificant whitespace, standard string escaping). The function
/// is intentionally restricted to `&[&str]` to make that guarantee explicit.
fn canonical_json_string_array(elements: &[&str]) -> String {
    // `serde_json` serializes a slice of strings with no whitespace, which
    // matches canonical JSON for this restricted shape.
    serde_json::to_string(elements).expect("serializing a string array never fails")
}

/// `base64(SHA256(canonical_json(elements)))` using the unpadded standard
/// base64 alphabet, the primitive shared by all MSC4195 hash derivations.
fn hash_string_array(elements: &[&str]) -> String {
    let canonical = canonical_json_string_array(elements);
    let digest = Sha256::digest(canonical.as_bytes());
    STANDARD_NO_PAD.encode(digest)
}

/// Derive the `livekit_alias` for a MatrixRTC slot, the minimal form from
/// MSC4195: `base64(SHA256(JSON.serialize([room_id, slot_id])))`.
///
/// Production deployments may add server-held random bits for stronger
/// metadata protection; that variant is intentionally not implemented here as
/// the alias normally arrives via the authorisation service's JWT.
pub fn livekit_alias(room_id: &str, slot_id: &str) -> String {
    hash_string_array(&[room_id, slot_id])
}

/// The LiveKit participant identity the homeserver's MSC4195 `get_token`
/// mints: `base64(SHA256(JSON.serialize([sender, member_id])))`.
///
/// This is the `sub` of the JWT, and the value used to associate an inbound
/// `m.rtc.encryption_key` key with a LiveKit participant whose published
/// transport selects that flow.
pub fn msc4195_identity(sender: &str, member_id: &str) -> String {
    hash_string_array(&[sender, member_id])
}

/// The identity the legacy JWT service's `/sfu/get` mints: the unhashed
/// `{user_id}:{device_id}`, which clients predating the homeserver endpoints
/// look for.
pub fn legacy_identity(user_id: &str, device_id: &str) -> String {
    matrix_rtc_core::compat::room_state::participant_identity(user_id, device_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_json_has_no_whitespace() {
        assert_eq!(
            canonical_json_string_array(&["!roomid:example.com", "slot1234"]),
            r#"["!roomid:example.com","slot1234"]"#
        );
    }

    // Verified test vector from the MSC4195 appendix (alias without random bits).
    #[test]
    fn livekit_alias_matches_msc4195_vector() {
        assert_eq!(
            livekit_alias("!roomid:example.com", "slot1234"),
            "O8437W3+jmzMVjoIP3tNwbm+XxHQk2iKpOA7aqw3qSc"
        );
    }

    // Verified test vector from the MSC4195 appendix.
    #[test]
    fn msc4195_identity_matches_msc4195_vector() {
        assert_eq!(
            msc4195_identity("@alice:example.com", "memberABC"),
            "M3VnsLXrkbxIDINXO64u8PZzFyD9ZYFiQULR2dshWYs"
        );
    }

    #[test]
    fn legacy_identity_is_unhashed() {
        assert_eq!(
            legacy_identity("@alice:example.com", "DEVICE"),
            "@alice:example.com:DEVICE"
        );
    }
}
