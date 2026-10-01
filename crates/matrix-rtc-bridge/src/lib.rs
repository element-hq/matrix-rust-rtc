// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Matrix-side bridging for MatrixRTC.
//!
//! [`matrix-rtc-core`] owns *what the protocol says*; a transport crate (e.g.
//! `matrix-rtc-livekit`) owns *how media flows*. This crate owns the third
//! thing: *how the protocol reaches a Matrix homeserver*. It is deliberately
//! transport-free — nothing here knows what a LiveKit SFU is — so a second
//! transport can reuse it unchanged.
//!
//! Four pieces, in increasing order of how much they depend on:
//!
//! - [`compat`] — translation between the current MSC4143 wire format and the
//!   pre-2026 dialects Element Call still speaks. Pure JSON in, pure JSON out;
//!   no Matrix SDK, no async runtime. Available unconditionally, with
//!   [`DialectBackend`], the backend wrapper that renders sends in a room's
//!   dialect.
//! - [`feeder`] — how a `MatrixBackend` feeds the manager: subscribe, seed,
//!   order, funnel. Unconditional; a plain future the binding spawns.
//! - [`transports`] — which transport a join publishes on.
//! - [`sdk`] — the `matrix_sdk::Client` implementation of the backend. Behind
//!   the `matrix-sdk` feature.
//!
//! # Why `matrix-sdk` is off by default
//!
//! [`compat`] is the largest and most-tested part of this crate and needs
//! nothing but `serde_json`. Keeping the SDK optional means its tests build in
//! seconds against no git dependencies — which is the whole reason this crate
//! was split out of the LiveKit transport, where every one of them was trapped
//! behind a `libwebrtc` build.
//!
//! [`matrix-rtc-core`]: matrix_rtc_core

pub mod compat;
pub mod feeder;
pub mod transports;

#[cfg(feature = "matrix-sdk")]
pub mod sdk;

#[cfg(feature = "matrix-sdk")]
pub use sdk::{SdkBackend, TimelineIngest, timeline_ingest_from_raw};

pub use compat::{
    DialectBackend, ElementCallCompat, ElementCallDialect, ElementCallStateDialect,
    LEGACY_KEY_EVENT_TYPE, LegacyKeyMessage, MemberContent, MemberEventRoute, OutboundDialect,
    STATE_MEMBER_EVENT_TYPE, StateMemberEvent, StateMembership,
};
pub use feeder::{
    AttachOptions, AttachedRooms, RoomAttachment, RoomFeeder, RoomFeederRun, RoomModes,
    RoomReservation, ToDeviceFeeder, ToDeviceFeederRun,
};

pub use matrix_rtc_core::OpenIdToken;
