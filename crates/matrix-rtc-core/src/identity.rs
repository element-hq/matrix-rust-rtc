// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! A member's participant identity on an RTC backend.
//!
//! Usually one string. While homeservers move LiveKit tokens from the
//! standalone JWT service to the CS API (spec 049), a member that publishes
//! both `url` and `livekit_service_url` may hold either of two identities,
//! depending on its client version: [`RtcIdentity::Transition`] carries both.
//! Callers go through [`RtcIdentity::matches`] and [`RtcIdentity::candidates`]
//! so that removing the variant leaves them unchanged.

/// The identity, or identities, a member may hold on its RTC backend.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RtcIdentity {
    /// The one identity the member's token flow mints.
    Single(String),
    /// TEMPORARY (spec 049): a published LiveKit transport with both `url`
    /// and `livekit_service_url`. Either flow may have minted the identity.
    /// Remove once homeservers stop announcing `livekit_service_url`.
    Transition {
        /// The CS API `get_token` identity.
        hashed: String,
        /// The `/sfu/get` identity, `{user_id}:{device_id}`.
        legacy: String,
    },
}

impl RtcIdentity {
    /// Whether a participant with `identity` on the backend is this member.
    pub fn matches(&self, identity: &str) -> bool {
        self.candidates().any(|candidate| candidate == identity)
    }

    /// Every concrete identity the member may hold: what the media layer
    /// indexes the member by and installs its keys under.
    pub fn candidates(&self) -> impl Iterator<Item = &str> {
        let (first, second) = match self {
            Self::Single(identity) => (identity.as_str(), None),
            Self::Transition { hashed, legacy } => (hashed.as_str(), Some(legacy.as_str())),
        };
        std::iter::once(first).chain(second)
    }

    /// The one identity we publish under when this is ours.
    ///
    /// TEMPORARY (spec 049): announcing both fields, we publish via
    /// `/sfu/get` (R7), so ours is the legacy identity.
    pub fn into_own(self) -> String {
        match self {
            Self::Single(identity) => identity,
            Self::Transition { legacy, .. } => legacy,
        }
    }
}

impl From<String> for RtcIdentity {
    fn from(identity: String) -> Self {
        Self::Single(identity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_matches_only_itself() {
        let identity = RtcIdentity::Single("a".into());
        assert!(identity.matches("a"));
        assert!(!identity.matches("b"));
        assert_eq!(identity.candidates().collect::<Vec<_>>(), ["a"]);
    }

    #[test]
    fn transition_matches_either() {
        let identity = RtcIdentity::Transition {
            hashed: "h".into(),
            legacy: "@u:x:D".into(),
        };
        assert!(identity.matches("h"));
        assert!(identity.matches("@u:x:D"));
        assert!(!identity.matches("other"));
        assert_eq!(identity.candidates().collect::<Vec<_>>(), ["h", "@u:x:D"]);
    }
}
