// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Pre-2026 Element Call interoperability, exposed to FFI hosts.
//!
//! The translation lives in [`matrix_rtc_call::compat`] and is applied by the
//! library's feeder and dialect wrapper; nothing here re-implements a dialect.
//! A host chooses the mode once, when it attaches the room
//! ([`FfiAttachOptions::element_call_compat`](crate::FfiAttachOptions::element_call_compat)),
//! and delivers the same raw events in every mode.
//!
//! What the mode decides: which room subjects the library subscribes to (no
//! `m.rtc.slot` in the pre-sticky generation, which has none; its membership
//! state instead), how our sends are rendered, the `member.id` we join with,
//! how an inbound media key is bound, the SFU participant identity and the
//! token endpoint. Those must agree or the call connects and nothing decrypts.

use matrix_rtc_call::compat::ingest;
use matrix_rtc_call::compat::{ElementCallCompat, OutboundDialect};

/// Which MatrixRTC generation a session speaks, for interoperating with Element
/// Call builds that predate the 2026 MSC4143 rewrite.
///
/// Chosen when the room is attached, because it decides more than the wire
/// format of one event: the
/// `member.id` we join with, how an inbound media key is bound to a membership,
/// the SFU participant identity, and which authorisation-service endpoint mints
/// our token. Those must agree or the call connects and nothing decrypts.
///
/// Scaffolding, and meant to be deleted once Element Call catches up. See
/// [`matrix_rtc_call::compat`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, uniffi::Enum)]
pub enum FfiElementCallCompat {
    /// Current MSC4143 + MSC4354 only. The default, and the only mode that
    /// interoperates with spec-current peers.
    #[default]
    Off,
    /// Element Call as of 2025: MSC4354 sticky events carrying the pre-2026
    /// field names alongside the spec ones.
    ///
    /// Joins stay MSC4143-valid, so a spec-current peer still reads us. Leaves
    /// and media keys cannot be additive — a leave becomes the legacy
    /// bare-sticky-key content and keys go out as
    /// `io.element.call.encryption_keys` *instead of* the spec type — so in this
    /// mode keys are exchanged with legacy peers and not with spec-current ones.
    StickyEvents,
    /// Element Call before MSC4354: membership as `org.matrix.msc3401.call.member`
    /// **room state**, plain `{user}:{device}` SFU identities, and the
    /// pre-MSC4195 `/sfu/get` token endpoint.
    ///
    /// Nothing about this mode is additive: a call joined this way is visible to
    /// that generation of Element Call and to nobody else.
    StateEvents,
}

impl From<FfiElementCallCompat> for ElementCallCompat {
    fn from(value: FfiElementCallCompat) -> Self {
        match value {
            FfiElementCallCompat::Off => Self::Off,
            FfiElementCallCompat::StickyEvents => Self::StickyEvents,
            FfiElementCallCompat::StateEvents => Self::StateEvents,
        }
    }
}

/// An absent mode is [`ElementCallCompat::Off`]: hosts that predate this option,
/// and every host not talking to Element Call, leave the field unset.
pub(crate) fn resolve(compat: Option<FfiElementCallCompat>) -> ElementCallCompat {
    compat.unwrap_or_default().into()
}

/// See [`ingest::outbound_dialect`].
pub(crate) fn outbound_dialect(
    compat: ElementCallCompat,
    user_id: &str,
    device_id: &str,
    room_id: &str,
    slot_id: &str,
) -> OutboundDialect {
    ingest::outbound_dialect(compat, user_id, device_id, room_id, slot_id)
}

/// See [`ingest::member_id`].
pub(crate) fn member_id(compat: ElementCallCompat, user_id: &str, device_id: &str) -> String {
    ingest::member_id(compat, user_id, device_id)
}
