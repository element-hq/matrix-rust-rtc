// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! A call's MSC4196 `m.call.intent` (voice or video), read from the
//! memberships already in the call and settled at join.
//!
//! A join takes the intent the host states, the user's own choice (starting a
//! call, or answering a ring that said voice or video); a join that states none
//! (a room's call button) follows the call in progress, else video. The host
//! turns the camera on from the result, so joining a voice call from a button
//! that cannot know never opens the camera.
//! The consensus is matrix-rust-sdk's `CallIntentConsensus`, over sticky and
//! room-state memberships alike.

use matrix_rtc_core::JoinedMembership;

use crate::notification::is_own_participation;

/// The key under `application` (and, in the room-state format, the top level).
pub const CALL_INTENT_KEY: &str = "m.call.intent";

/// MSC4196: whether a call is a voice or a video call.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum CallIntent {
    Audio,
    /// What a join with no intent of its own and no call to follow uses.
    #[default]
    Video,
}

impl CallIntent {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Audio => "audio",
            Self::Video => "video",
        }
    }

    /// `"audio"` or `"video"`; any other value states no intent.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "audio" => Some(Self::Audio),
            "video" => Some(Self::Video),
            _ => None,
        }
    }

    /// The intent a membership states, if it states a known one.
    pub fn of(member: &JoinedMembership) -> Option<Self> {
        member
            .application
            .extra
            .get(CALL_INTENT_KEY)
            .and_then(|value| value.as_str())
            .and_then(Self::parse)
    }
}

/// What the memberships of a call say about its intent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallIntentConsensus {
    /// Every member states this intent.
    Full(CallIntent),
    /// Those that state an intent agree on this one; the others state none.
    Partial {
        intent: CallIntent,
        agreeing: usize,
        total: usize,
    },
    /// Nobody is in the call, nobody states an intent, or members disagree.
    None,
}

impl CallIntentConsensus {
    /// The consensus over `members`.
    pub fn of<'a>(members: impl IntoIterator<Item = &'a JoinedMembership>) -> Self {
        let mut total = 0;
        let mut agreed: Option<(CallIntent, usize)> = None;
        for member in members {
            total += 1;
            let Some(intent) = CallIntent::of(member) else {
                continue;
            };
            match &mut agreed {
                None => agreed = Some((intent, 1)),
                Some((candidate, count)) if *candidate == intent => *count += 1,
                Some(_) => return Self::None,
            }
        }
        match agreed {
            None => Self::None,
            Some((intent, agreeing)) if agreeing == total => Self::Full(intent),
            Some((intent, agreeing)) => Self::Partial {
                intent,
                agreeing,
                total,
            },
        }
    }

    /// The consensus over the members other than this device: another device
    /// of ours counts, our own stale membership does not.
    pub fn of_others(members: &[JoinedMembership], user_id: &str, device_id: &str) -> Self {
        Self::of(
            members
                .iter()
                .filter(|member| !is_own_participation(member, user_id, device_id)),
        )
    }

    /// The intent the call agrees on, fully or partly.
    pub fn intent(self) -> Option<CallIntent> {
        match self {
            Self::Full(intent) | Self::Partial { intent, .. } => Some(intent),
            Self::None => None,
        }
    }

    /// The intent a join uses: `requested`, the user's explicit choice, when
    /// there is one; otherwise the call's when it agrees on one; otherwise
    /// [`CallIntent::Video`].
    pub fn resolve(self, requested: Option<CallIntent>) -> CallIntent {
        requested.or(self.intent()).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use matrix_rtc_core::{ApplicationInfo, EventOrigin};
    use serde_json::json;

    use super::*;

    fn member(sender: &str, device: &str, intent: Option<&str>) -> JoinedMembership {
        let mut application = ApplicationInfo::new("m.call");
        if let Some(intent) = intent {
            application = application.with_extra(CALL_INTENT_KEY, json!(intent));
        }
        JoinedMembership {
            room_id: "!room:example.org".to_owned(),
            slot_id: "m.call#room".to_owned(),
            sender: sender.to_owned(),
            origin: EventOrigin::encrypted(Some(device.to_owned())),
            sticky_key: format!("{sender}{device}"),
            member_id: format!("{sender}{device}"),
            membership_event_id: None,
            membership_ts: None,
            origin_server_ts: None,
            application,
            transports: Vec::new(),
            can_subscribe: Vec::new(),
        }
    }

    #[test]
    fn an_empty_call_has_no_consensus() {
        assert_eq!(CallIntentConsensus::of(&[]), CallIntentConsensus::None);
    }

    #[test]
    fn members_that_all_agree_are_a_full_consensus() {
        let members = [
            member("@a:x", "A", Some("audio")),
            member("@b:x", "B", Some("audio")),
        ];
        assert_eq!(
            CallIntentConsensus::of(&members),
            CallIntentConsensus::Full(CallIntent::Audio)
        );
    }

    #[test]
    fn members_without_an_intent_do_not_vote_but_count() {
        let members = [
            member("@a:x", "A", Some("audio")),
            member("@b:x", "B", None),
            member("@c:x", "C", Some("bogus")),
        ];
        assert_eq!(
            CallIntentConsensus::of(&members),
            CallIntentConsensus::Partial {
                intent: CallIntent::Audio,
                agreeing: 1,
                total: 3,
            }
        );
    }

    #[test]
    fn members_that_disagree_have_no_consensus() {
        let members = [
            member("@a:x", "A", Some("audio")),
            member("@b:x", "B", Some("video")),
        ];
        assert_eq!(CallIntentConsensus::of(&members), CallIntentConsensus::None);
    }

    #[test]
    fn our_own_device_is_left_out_but_our_other_devices_count() {
        let members = [
            member("@me:x", "MINE", Some("video")),
            member("@me:x", "OTHER", Some("audio")),
        ];
        assert_eq!(
            CallIntentConsensus::of_others(&members, "@me:x", "MINE"),
            CallIntentConsensus::Full(CallIntent::Audio)
        );
    }

    #[test]
    fn the_request_wins_over_the_call_and_the_call_over_the_default() {
        let audio = CallIntentConsensus::Full(CallIntent::Audio);
        assert_eq!(audio.resolve(Some(CallIntent::Video)), CallIntent::Video);
        assert_eq!(audio.resolve(None), CallIntent::Audio);
        let none = CallIntentConsensus::None;
        assert_eq!(none.resolve(Some(CallIntent::Audio)), CallIntent::Audio);
        assert_eq!(none.resolve(None), CallIntent::Video);
    }
}
