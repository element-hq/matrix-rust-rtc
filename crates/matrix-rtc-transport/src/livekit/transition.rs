// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! TEMPORARY (spec 049): the move from the standalone JWT service's
//! `/sfu/get` to the homeserver's CS API `get_token`.
//!
//! While a homeserver announces a LiveKit transport with both `url` and
//! `livekit_service_url`, a member publishing it may have taken its token from
//! either flow, depending on its client version, so it may hold either
//! identity. Everything that exists only for that is here, so that it goes away
//! in one piece once homeservers stop announcing `livekit_service_url`.

use matrix_rtc_core::{IdentitySubject, LiveKitTransport, RtcIdentity};

use super::identity;

/// The identities a member may hold when its published transport names both
/// flows: [`RtcIdentity::Transition`]. `None` for a transport naming one flow.
/// When the device cannot be attributed, the `/sfu/get` identity is unknown and
/// only the hashed one is left.
pub fn identity_for_both(
    transport: &LiveKitTransport,
    subject: &IdentitySubject<'_>,
) -> Option<RtcIdentity> {
    transport.url()?;
    transport.livekit_service_url()?;
    let hashed = identity::msc4195_identity(subject.user_id, subject.member_id);
    let Some(device_id) = subject.device_id else {
        return Some(RtcIdentity::Single(hashed));
    };
    Some(RtcIdentity::Transition {
        hashed,
        legacy: identity::legacy_identity(subject.user_id, device_id),
    })
}
