// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Pre-2026 Element Call interoperability, exposed to FFI hosts.
//!
//! The translation lives in [`matrix_rtc_core::compat`] and is applied by the
//! library's feeder and dialect wrapper; nothing here re-implements a dialect.
//! A host chooses the mode once, when it opens the room
//! ([`FfiRoomOptions::format`](crate::FfiRoomOptions::format)),
//! and delivers the same raw events in every mode.
//!
//! What the mode decides: which room subjects the library subscribes to (no
//! `m.rtc.slot` in the pre-sticky generation, which has none; its membership
//! state instead), how our sends are rendered, the `member.id` we join with,
//! how an inbound media key is bound, the SFU participant identity and the
//! token endpoint. Those must agree or the call connects and nothing decrypts.

use matrix_rtc_core::compat::MembershipFormat;

/// Which MatrixRTC generation a session speaks, for interoperating with Element
/// Call builds that predate the 2026 MSC4143 rewrite.
///
/// Chosen when the room is opened, because it decides more than the wire
/// format of one event: the
/// `member.id` we join with, how an inbound media key is bound to a membership,
/// the SFU participant identity, and which authorisation-service endpoint mints
/// our token. Those must agree or the call connects and nothing decrypts.
///
/// Scaffolding, and meant to be deleted once Element Call catches up. See
/// [`matrix_rtc_core::compat`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, uniffi::Enum)]
pub enum FfiMembershipFormat {
    /// Current MSC4143 + MSC4354 only. The default, and the only mode that
    /// interoperates with spec-current peers.
    #[default]
    Current,
    /// Element Call as of 2025: MSC4354 sticky events carrying the pre-2026
    /// field names alongside the spec ones.
    ///
    /// Joins stay MSC4143-valid, so a spec-current peer still reads us. Leaves
    /// and media keys cannot be additive — a leave becomes the legacy
    /// bare-sticky-key content and keys go out as
    /// `io.element.call.encryption_keys` *instead of* the spec type — so in this
    /// mode keys are exchanged with legacy peers and not with spec-current ones.
    Sticky2025,
    /// Element Call before MSC4354: membership as `org.matrix.msc3401.call.member`
    /// **room state**, plain `{user}:{device}` SFU identities, and the
    /// pre-MSC4195 `/sfu/get` token endpoint.
    ///
    /// Nothing about this mode is additive: a call joined this way is visible to
    /// that generation of Element Call and to nobody else.
    RoomState,
}

impl From<FfiMembershipFormat> for MembershipFormat {
    fn from(value: FfiMembershipFormat) -> Self {
        match value {
            FfiMembershipFormat::Current => Self::Current,
            FfiMembershipFormat::Sticky2025 => Self::Sticky2025,
            FfiMembershipFormat::RoomState => Self::RoomState,
        }
    }
}

/// An absent mode is [`MembershipFormat::Current`]: hosts that predate this option,
/// and every host not talking to Element Call, leave the field unset.
pub(crate) fn resolve(compat: Option<FfiMembershipFormat>) -> MembershipFormat {
    compat.unwrap_or_default().into()
}
