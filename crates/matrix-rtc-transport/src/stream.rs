// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The kinds of media stream a member publishes, the one vocabulary transports,
//! constraints and publications share.

/// The kind of media stream a participant publishes.
///
/// Ordered so a set of streams can be reported in a stable order; the
/// declaration order below is what that ordering is.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum MediaStreamKind {
    Microphone,
    Camera,
    ScreenShare,
    ScreenShareAudio,
    Data,
}
