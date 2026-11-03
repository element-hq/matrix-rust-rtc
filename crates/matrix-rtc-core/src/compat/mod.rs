// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The MatrixRTC membership formats that predate the 2026 MSC4143 rewrite,
//! which matrix-js-sdk — and so Element Call — still speaks. They carry any
//! application; nothing here is about calls beyond the fields those formats
//! put on every membership.
//!
//! # This module is scaffolding, and is meant to be deleted
//!
//! Everything here exists for one reason: the only other MatrixRTC
//! implementation available to test against — Element Call on the JS SDK — still
//! speaks a pre-2026 wire format. Once it catches up, delete this directory, the
//! call sites listed below, and every host's [`MembershipFormat`] option.
//!
//! # Two generations, not one
//!
//! There are two, and they disagree about more than field names:
//!
//! - [`sticky_2025`] — the 2025 format. Already MSC4354 sticky-based; only the
//!   fields inside the member content differ. We only read it now.
//! - [`room_state`] — the format before MSC4354, when membership was
//!   `org.matrix.msc3401.call.member` **room state**. A different carrier, a
//!   different SFU participant identity, and a different token endpoint.
//!
//! [`MembershipFormat`] selects between current MSC4143 and the state format. It
//! is an enum rather than a flag because the generations are mutually exclusive
//! by construction: a membership is a sticky event or a state event, never both.
//!
//! # Why it is shaped this way
//!
//! `BaseRtcRoom` and the rest of the core speak current MSC4143 and nothing
//! else — no legacy field appears in a domain type. The format is confined to
//! this module and the feeder's [`IngestDialect`](crate::feeder::IngestDialect)
//! seam, as JSON funnels at the very edge:
//!
//! - **Inbound for the sticky dialect**
//!   ([`sticky_2025::normalize_member_content`],
//!   [`sticky_2025::parse_key_message`]) is *permissive and always on*. Every
//!   rule is of the form "the modern field is absent and the legacy one is
//!   present", so a spec-shaped event passes through byte-identical and no flag
//!   is needed to protect it. Being always on is deliberate: a legacy path that
//!   only compiles under a flag is a legacy path that rots.
//! - **Everything about the state dialect** is *opt-in*, in both directions.
//!   Reading it is not normalisation — it is a second membership source, in a
//!   different part of the room, with a lifetime the content states rather than
//!   the homeserver enforcing. See [`room_state`] for why that cannot be
//!   always-on the way its sibling is.
//! - **Outbound** ([`OutboundDialect`]) is opt-in for both, because it is the
//!   half that changes what other clients see.
//!
//! # What this module cannot own
//!
//! One thing about the state dialect refuses to be JSON, and is named here so
//! the boundary stays deliberate rather than eroding: the choice of ruma request
//! type (a membership is `PUT .../state/...` rather than a sticky send).
//! [`MemberEventRoute`] exists precisely to keep the *decision* here while the
//! *performing* stays in the `sdk` module.
//!
//! The LiveKit token endpoint and participant identity are not the format's at
//! all: they follow the published transport (a `livekit_service_url` means the
//! JWT service's `/sfu/get` and `{user}:{device}`, a bare SFU `url` the
//! homeserver's MSC4195 `get_token` and a hash), and live wholly in the
//! transport crate (`matrix_rtc_transport::livekit`).
//!
//! # Call sites
//!
//! In this crate:
//!
//! 1. `ingest` — the inbound half for every host: the `IngestDialect` the
//!    feeder reads a room through, which normalises `m.rtc.member` content,
//!    reads state-carried membership in `RoomState`, and parses legacy
//!    to-device keys, all in the format the room was opened in.
//! 2. `dialect_backend::DialectBackend` — the outbound half for every host:
//!    routes and rewrites the sends of the one `MatrixBackend` wrapper the
//!    bindings and the `LiveKitCall` facade all use.
//!
//! In `matrix-rtc-call`: `RtcRoom`'s join — the `member.id` and the outbound
//! dialect for the room's format.
//!
//! In `matrix-rtc-livekit`:
//!
//! 3. `matrix_rtc_call_sdk::LiveKitCall::join` — mode selection and the member id.
//!
//! In `matrix-rtc-ffi` and `matrix-rtc-wasm`, which reach the same dialects from
//! a host that owns its own Matrix stack:
//!
//! 4. `compat` — the binding-shaped mode vocabulary, given at `RtcClient::room`.

use std::borrow::Cow;

use serde_json::Value;

pub mod dialect_backend;
#[cfg(test)]
mod feeder_tests;
pub mod ingest;
pub mod room_state;
pub mod sticky_2025;

pub use dialect_backend::{DialectBackend, DialectHold};
pub use room_state::{
    RoomStateDialect, STATE_MEMBER_EVENT_TYPE, StateMemberEvent, StateMembership,
};
pub use sticky_2025::{LEGACY_KEY_EVENT_TYPE, LegacyKeyMessage, MemberContent};

/// Which MatrixRTC membership format a room is read and written in.
///
/// Mutually exclusive by construction, because the generations disagree about
/// the *carrier* of a membership, not just its fields. Reading the 2025 sticky
/// dialect needs no flag and is always on regardless of this setting; see the
/// module docs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MembershipFormat {
    /// Current MSC4143 + MSC4354 only. The default, and the only mode that
    /// interoperates with spec-current peers.
    #[default]
    Msc,
    /// The format before MSC4354: membership as
    /// `org.matrix.msc3401.call.member` **room state**, published with legacy
    /// foci (`livekit_service_url`), so `/sfu/get` tokens and `{user}:{device}`
    /// SFU identities.
    ///
    /// Nothing about this format is additive — a session joined this way is
    /// visible to that generation of clients and to nobody else. It exists for interop
    /// testing.
    RoomState,
}

impl MembershipFormat {
    /// Whether membership is carried as room state in this mode.
    pub fn reads_state_membership(self) -> bool {
        matches!(self, Self::RoomState)
    }

    /// The slot id to name in an MSC4195 token request. The authorisation
    /// service derives the LiveKit room from it, so it must be the one this
    /// generation's peers name: Element Call spells the room-wide slot
    /// `m.call#ROOM`.
    pub fn token_slot_id(self, slot_id: &str) -> Cow<'_, str> {
        match self {
            Self::Msc => Cow::Borrowed(slot_id),
            Self::RoomState => sticky_2025::legacy_room_slot_id(slot_id),
        }
    }
}

/// Where a member event goes on the wire, and in what shape.
///
/// The dialect decides; `sdk` performs. That split is what keeps every
/// legacy byte inside this module while the ruma request types stay out of it.
#[derive(Clone, Debug)]
pub enum MemberEventRoute {
    /// An MSC4354 sticky timeline event — the only carrier the spec knows, and
    /// the one [`MembershipFormat::Msc`] takes.
    Sticky { event_type: String, content: Value },
    /// A room state event, which is how MatrixRTC membership worked before
    /// MSC4354. The core's sticky `duration_ms` is meaningless here: room state
    /// has no TTL, and the lifetime is stated inside the content instead.
    State {
        event_type: &'static str,
        state_key: String,
        content: Value,
    },
    /// An ordinary message-like room event, with no TTL. What a non-membership
    /// event (the MSC4075 notification) becomes for a generation whose wire
    /// has no sticky map at all — [`MembershipFormat::RoomState`] — so that
    /// a host with no MSC4354 support is never asked for a sticky send.
    Room { event_type: String, content: Value },
}

/// The outbound dialect a command sender speaks: exactly one generation at a
/// time.
#[derive(Clone, Debug)]
pub enum OutboundDialect {
    /// Current MSC4143 + MSC4354, unmodified.
    None,
    /// Element Call before MSC4354.
    State(RoomStateDialect),
}

impl OutboundDialect {
    /// Decide where an outbound event goes and what it says.
    ///
    /// Anything that is not a membership passes through untouched — the legacy
    /// generations differ about memberships, not about everything — but its
    /// *carrier* still follows the generation: a sticky event where there is a
    /// sticky map, an ordinary room event for the pre-sticky one, which
    /// has none. That keeps a host that cannot send sticky events at all (no
    /// MSC4354 in its SDK) able to run a [`MembershipFormat::RoomState`]
    /// call end to end, notification included.
    ///
    /// `lifetime_ms` is how long the membership should stay valid from now. The
    /// sticky routes ignore it — the homeserver is told separately, as the
    /// sticky TTL — but the pre-sticky state route has nowhere else to put a
    /// deadline and writes it into `expires`
    /// ([`RoomStateDialect::member_content`]). `None` means "no opinion",
    /// which is what a delayed leave has.
    pub fn route_member_event(
        &self,
        event_type: String,
        content: Value,
        lifetime_ms: Option<u64>,
    ) -> MemberEventRoute {
        if !sticky_2025::is_member_event(&event_type) {
            return match self {
                Self::State(_) => MemberEventRoute::Room {
                    event_type,
                    content,
                },
                Self::None => MemberEventRoute::Sticky {
                    event_type,
                    content,
                },
            };
        }

        match self {
            Self::None => MemberEventRoute::Sticky {
                event_type,
                content,
            },
            Self::State(dialect) => MemberEventRoute::State {
                event_type: STATE_MEMBER_EVENT_TYPE,
                state_key: dialect.state_key(),
                content: dialect.member_content(&content, lifetime_ms),
            },
        }
    }

    /// Render an outbound MSC4075 notification's content for the target
    /// generation.
    ///
    /// Returns the content unchanged for anything that is not a notification and
    /// for [`OutboundDialect::None`]. The event type never moves — both
    /// generations already agree on it — so only the content is passed.
    ///
    /// The pre-sticky generation reads the 2025 flat shape: it never sends
    /// notifications itself, but a client of that vintage still parses this
    /// shape and there is no other for it to read.
    pub fn rewrite_notification(&self, event_type: &str, content: Value) -> Value {
        let flattened = match self {
            Self::None => None,
            Self::State(_) => sticky_2025::flatten_notification_content(event_type, &content),
        };
        flattened.unwrap_or(content)
    }

    /// Translate an outbound media key into the legacy type and shape, or `None`
    /// to send it untouched.
    ///
    /// The pre-sticky generation uses the 2025 to-device type and content. The
    /// state dialect gets the right `member.id` for free: in that mode the
    /// core's member id already *is* the legacy `membershipID`.
    pub fn rewrite_key_message(
        &self,
        message_type: &str,
        content: &Value,
    ) -> Option<(String, Value)> {
        match self {
            Self::None => None,
            Self::State(dialect) => sticky_2025::rewrite_key_message(
                dialect.own_device_id(),
                dialect.slot_id(),
                message_type,
                content,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn notification() -> Value {
        json!({
            "application": { "type": "m.call", "notification_type": "ring" },
            "m.mentions": { "user_ids": [], "room": true },
        })
    }

    /// `/sfu/get` fixes the slot to `m.call#ROOM`; a room-state peer asking
    /// the homeserver must land in that same LiveKit room.
    #[test]
    fn the_room_state_format_asks_for_tokens_in_element_calls_spelling() {
        let format = MembershipFormat::RoomState;
        assert_eq!(format.token_slot_id("m.call#room"), "m.call#ROOM");
        assert_eq!(format.token_slot_id("m.call#standup"), "m.call#standup");
        assert_eq!(
            MembershipFormat::Msc.token_slot_id("m.call#room"),
            "m.call#room"
        );
    }

    fn state_dialect() -> OutboundDialect {
        OutboundDialect::State(RoomStateDialect::new(
            "@us:example.io",
            "DEVICE",
            "!room:example.io",
            "m.call#room",
        ))
    }

    /// The pre-sticky wire has no sticky map, so a host in that mode must never
    /// be asked for a sticky send — including for the notification.
    #[test]
    fn a_notification_is_a_plain_room_event_in_the_state_dialect() {
        let route =
            state_dialect().route_member_event("m.rtc.notification".into(), notification(), None);
        assert!(
            matches!(route, MemberEventRoute::Room { ref event_type, .. } if event_type == "m.rtc.notification"),
            "got {route:?}"
        );
    }

    /// A format with a sticky map keeps the notification sticky, and the
    /// non-member short-circuit leaves the content alone.
    #[test]
    fn a_notification_stays_sticky_in_the_current_format() {
        let route = OutboundDialect::None.route_member_event(
            "m.rtc.notification".into(),
            notification(),
            None,
        );
        assert!(
            matches!(route, MemberEventRoute::Sticky { ref content, .. } if *content == notification()),
            "got {route:?}"
        );
    }
}
