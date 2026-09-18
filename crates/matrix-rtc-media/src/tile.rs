// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Call tiles: the roster as a UI draws it.
//!
//! One [`CallTile`] per renderable stream — a member sharing their screen is
//! two tiles — ranked so that a UI rendering the list in order is correct.
//! Derived from [`Participant`]s and pure: no engine state, no clock. The
//! speaking set is an input, already damped; hysteresis lives in the engine.

use std::cmp::Reverse;
use std::collections::HashSet;

use crate::participant::{MediaStreamKind, Participant, StreamState};

/// Identity of one tile: the pair `(member_id, kind)`.
///
/// Stable for as long as the tile is in the call — neither component changes
/// when the member mutes, raises a hand, or starts or stops a share.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TileId {
    pub member_id: String,
    /// `Camera` or `ScreenShare`.
    pub kind: MediaStreamKind,
}

/// One renderable stream of one membership, with what a UI needs to place and
/// decorate it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CallTile {
    pub member_id: String,
    pub kind: MediaStreamKind,
    pub user_id: String,
    pub device_id: Option<String>,
    /// Sorts above everything else. A screen share; a pin, once pinning exists.
    pub hero: bool,
    /// This tile's own stream is present and unmuted.
    pub has_video: bool,
    /// The member's microphone is absent or muted — what a mute icon means.
    /// Named for its subject because a tile is itself a stream that can be
    /// muted; that state is `has_video`.
    pub microphone_muted: bool,
    pub speaking: bool,
    pub hand_raised_at_ms: Option<u64>,
    pub joined_at_ms: Option<u64>,
    pub reachable: bool,
}

impl CallTile {
    pub fn id(&self) -> TileId {
        TileId {
            member_id: self.member_id.clone(),
            kind: self.kind,
        }
    }
}

/// The derived tile set.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Tiles {
    /// Everyone else, in rank order.
    pub remote: Vec<CallTile>,
    /// Our own camera tile, beside the list and never in it. `None` until our
    /// own membership is on the roster.
    pub own: Option<CallTile>,
}

/// Derives ranked tiles from the roster.
///
/// `speaking` holds the `member_id`s currently counted as speaking, after
/// whatever damping the caller applies.
pub fn derive_tiles(roster: &[Participant], speaking: &HashSet<String>) -> Tiles {
    let mut remote = Vec::with_capacity(roster.len());
    let mut own = None;
    for participant in roster {
        let camera = tile(participant, MediaStreamKind::Camera, speaking);
        if participant.is_local {
            // R7: beside the list, not in it. R13: our own share is never a
            // tile — we do not subscribe to our own outgoing stream.
            own = Some(camera);
            continue;
        }
        remote.push(camera);
        if stream(participant, MediaStreamKind::ScreenShare).is_some() {
            remote.push(tile(participant, MediaStreamKind::ScreenShare, speaking));
        }
    }
    remote.sort_by_cached_key(rank_key);
    Tiles { remote, own }
}

fn tile(p: &Participant, kind: MediaStreamKind, speaking: &HashSet<String>) -> CallTile {
    CallTile {
        member_id: p.member_id.clone(),
        kind,
        user_id: p.user_id.clone(),
        device_id: p.device_id.clone(),
        // R3: a share is a hero. R12 falls out of derivation: the own tile is
        // only ever built for `Camera`.
        hero: kind == MediaStreamKind::ScreenShare,
        has_video: stream(p, kind).is_some_and(|s| !s.muted),
        microphone_muted: stream(p, MediaStreamKind::Microphone).is_none_or(|s| s.muted),
        speaking: speaking.contains(&p.member_id),
        hand_raised_at_ms: p.hand_raised_at_ms,
        joined_at_ms: p.joined_at_ms,
        reachable: p.reachable,
    }
}

fn stream(p: &Participant, kind: MediaStreamKind) -> Option<&StreamState> {
    p.streams.iter().find(|s| s.kind == kind)
}

/// Sort key, lowest first, so R8's order comes out highest first: hero, raised
/// hand (earliest first), speaking, video, join time (earliest first), then a
/// tie-break that is arbitrary but identical on every client.
///
/// `member_id` is the tie-break rather than anything local, because two
/// clients seeing the same call must produce the same order. It is 16 random
/// bytes as hex — meaningless, and the same everywhere.
#[allow(clippy::type_complexity)]
fn rank_key(
    t: &CallTile,
) -> (
    Reverse<bool>,
    (bool, u64),
    Reverse<bool>,
    Reverse<bool>,
    (bool, u64),
    String,
    u8,
) {
    (
        Reverse(t.hero),
        none_last(t.hand_raised_at_ms),
        Reverse(t.speaking),
        Reverse(t.has_video),
        none_last(t.joined_at_ms),
        t.member_id.clone(),
        match t.kind {
            MediaStreamKind::ScreenShare => 1,
            _ => 0,
        },
    )
}

/// `Option` orders `None` first; a missing hand or join time belongs last.
fn none_last(v: Option<u64>) -> (bool, u64) {
    (v.is_none(), v.unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(id: &str) -> Participant {
        Participant {
            member_id: id.into(),
            user_id: format!("@{id}:example.org"),
            device_id: None,
            is_local: false,
            reachable: true,
            streams: vec![],
            hand_raised_at_ms: None,
            joined_at_ms: None,
        }
    }

    fn publishing(mut p: Participant, kind: MediaStreamKind, muted: bool) -> Participant {
        p.streams.push(StreamState { kind, muted });
        p
    }

    fn order(tiles: &[CallTile]) -> Vec<(&str, MediaStreamKind)> {
        tiles
            .iter()
            .map(|t| (t.member_id.as_str(), t.kind))
            .collect()
    }

    fn silent() -> HashSet<String> {
        HashSet::new()
    }

    #[test]
    fn r1_a_sharer_is_two_tiles_and_a_non_sharer_one() {
        let roster = [
            publishing(member("a"), MediaStreamKind::ScreenShare, false),
            member("b"),
        ];
        let tiles = derive_tiles(&roster, &silent());
        assert_eq!(
            order(&tiles.remote),
            [
                ("a", MediaStreamKind::ScreenShare),
                ("a", MediaStreamKind::Camera),
                ("b", MediaStreamKind::Camera),
            ]
        );
    }

    #[test]
    fn r13_our_own_share_makes_no_tile() {
        let mut me = publishing(member("me"), MediaStreamKind::ScreenShare, false);
        me.is_local = true;
        let tiles = derive_tiles(&[me], &silent());
        assert!(tiles.remote.is_empty());
        assert_eq!(
            tiles.own.as_ref().map(|t| t.kind),
            Some(MediaStreamKind::Camera)
        );
    }

    #[test]
    fn r7_r12_own_tile_is_beside_the_list_and_never_hero() {
        let mut me = member("me");
        me.is_local = true;
        let tiles = derive_tiles(&[member("a"), me], &silent());
        assert_eq!(order(&tiles.remote), [("a", MediaStreamKind::Camera)]);
        let own = tiles.own.expect("own tile");
        assert_eq!(own.member_id, "me");
        assert!(!own.hero);
    }

    #[test]
    fn r3_a_hero_outranks_a_raised_hand() {
        let mut hand = member("hand");
        hand.hand_raised_at_ms = Some(1);
        let sharer = publishing(member("share"), MediaStreamKind::ScreenShare, false);
        let tiles = derive_tiles(&[hand, sharer], &silent());
        assert_eq!(
            tiles.remote[0].id(),
            TileId {
                member_id: "share".into(),
                kind: MediaStreamKind::ScreenShare
            }
        );
        assert!(tiles.remote[0].hero);
    }

    #[test]
    fn r4_the_earlier_hand_ranks_first() {
        let mut late = member("late");
        late.hand_raised_at_ms = Some(200);
        let mut early = member("early");
        early.hand_raised_at_ms = Some(100);
        let tiles = derive_tiles(&[late, early], &silent());
        assert_eq!(
            order(&tiles.remote),
            [
                ("early", MediaStreamKind::Camera),
                ("late", MediaStreamKind::Camera)
            ]
        );
    }

    #[test]
    fn r5_speaking_outranks_silent() {
        let speaking: HashSet<String> = ["b".to_string()].into();
        let tiles = derive_tiles(&[member("a"), member("b")], &speaking);
        assert_eq!(
            order(&tiles.remote),
            [
                ("b", MediaStreamKind::Camera),
                ("a", MediaStreamKind::Camera)
            ]
        );
        assert!(tiles.remote[0].speaking);
    }

    #[test]
    fn r6_video_outranks_no_video() {
        let roster = [
            member("a"),
            publishing(member("b"), MediaStreamKind::Camera, false),
        ];
        let tiles = derive_tiles(&roster, &silent());
        assert_eq!(
            order(&tiles.remote),
            [
                ("b", MediaStreamKind::Camera),
                ("a", MediaStreamKind::Camera)
            ]
        );
        assert!(tiles.remote[0].has_video);
        assert!(!tiles.remote[1].has_video);
    }

    #[test]
    fn r8_full_order_then_join_time_then_member_id() {
        let sharer = publishing(member("share"), MediaStreamKind::ScreenShare, false);
        let mut hand = member("hand");
        hand.hand_raised_at_ms = Some(1);
        let video = publishing(member("video"), MediaStreamKind::Camera, false);
        let mut joined_late = member("z-late");
        joined_late.joined_at_ms = Some(2000);
        let mut joined_early = member("z-early");
        joined_early.joined_at_ms = Some(1000);
        let no_time_b = member("b");
        let no_time_a = member("a");
        let speaking: HashSet<String> = ["talk".to_string()].into();

        let roster = [
            joined_late,
            no_time_b,
            video,
            member("talk"),
            hand,
            no_time_a,
            joined_early,
            sharer,
        ];
        let tiles = derive_tiles(&roster, &speaking);
        assert_eq!(
            order(&tiles.remote),
            [
                ("share", MediaStreamKind::ScreenShare),
                ("hand", MediaStreamKind::Camera),
                ("talk", MediaStreamKind::Camera),
                ("video", MediaStreamKind::Camera),
                ("z-early", MediaStreamKind::Camera),
                ("z-late", MediaStreamKind::Camera),
                // Plain tiles with no join time, by member_id — including the
                // sharer's camera tile, which is not a hero (C4) and publishes
                // no camera here.
                ("a", MediaStreamKind::Camera),
                ("b", MediaStreamKind::Camera),
                ("share", MediaStreamKind::Camera),
            ]
        );
    }

    #[test]
    fn c2_order_is_total_and_deterministic_without_join_times() {
        let roster = [member("c"), member("a"), member("b")];
        let first = derive_tiles(&roster, &silent());
        let again = derive_tiles(&roster, &silent());
        assert_eq!(first, again);
        assert_eq!(
            order(&first.remote),
            [
                ("a", MediaStreamKind::Camera),
                ("b", MediaStreamKind::Camera),
                ("c", MediaStreamKind::Camera)
            ]
        );
    }

    #[test]
    fn r16_camera_identity_survives_a_share_starting_and_stopping() {
        let before = derive_tiles(&[member("a")], &silent());
        let during = derive_tiles(
            &[publishing(member("a"), MediaStreamKind::ScreenShare, false)],
            &silent(),
        );
        let after = derive_tiles(&[member("a")], &silent());
        let camera = |t: &Tiles| {
            t.remote
                .iter()
                .find(|x| x.kind == MediaStreamKind::Camera)
                .map(CallTile::id)
        };
        assert_eq!(camera(&before), camera(&during));
        assert_eq!(camera(&during), camera(&after));
    }

    #[test]
    fn a_muted_remote_share_is_a_hero_without_video() {
        let tiles = derive_tiles(
            &[publishing(member("a"), MediaStreamKind::ScreenShare, true)],
            &silent(),
        );
        let share = &tiles.remote[0];
        assert_eq!(share.kind, MediaStreamKind::ScreenShare);
        assert!(share.hero);
        assert!(!share.has_video);
    }

    #[test]
    fn microphone_muted_covers_absent_and_muted() {
        let unmuted_mic = publishing(member("a"), MediaStreamKind::Microphone, false);
        let muted_mic = publishing(member("b"), MediaStreamKind::Microphone, true);
        let no_mic = member("c");
        let tiles = derive_tiles(&[unmuted_mic, muted_mic, no_mic], &silent());
        let muted: Vec<(&str, bool)> = tiles
            .remote
            .iter()
            .map(|t| (t.member_id.as_str(), t.microphone_muted))
            .collect();
        assert_eq!(muted, [("a", false), ("b", true), ("c", true)]);
    }
}
