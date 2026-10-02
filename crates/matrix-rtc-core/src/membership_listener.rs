// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Listeners told of every change to a slot's joined memberships (its
//! `m.rtc.member` events considered joined to the slot), so an application's
//! per-member state follows them. Registered on one room, never across rooms.
//! Synchronous, so a listener sees each change in order, under the same lock
//! that applied it.

use std::sync::{Arc, Mutex};

use crate::maybe_send::MaybeSend;
use crate::session::JoinedMembership;

/// Called from inside the publish with a slot's complete joined memberships,
/// including when only an event id moved. Keep it to bookkeeping.
pub trait MembershipListener: MaybeSend {
    fn on_memberships(&self, slot_id: &str, members: &[JoinedMembership]);
}

impl<F> MembershipListener for F
where
    F: Fn(&str, &[JoinedMembership]) + MaybeSend,
{
    fn on_memberships(&self, slot_id: &str, members: &[JoinedMembership]) {
        self(slot_id, members)
    }
}

/// Shared with every slot session of one room, so a listener added later
/// reaches existing ones.
#[derive(Clone, Default)]
pub(crate) struct MembershipListeners(Arc<Mutex<Vec<Arc<dyn MembershipListener>>>>);

impl MembershipListeners {
    pub(crate) fn add(&self, listener: Arc<dyn MembershipListener>) {
        self.0.lock().unwrap().push(listener);
    }

    pub(crate) fn notify(&self, slot_id: &str, members: &[JoinedMembership]) {
        // Cloned out of the lock: a listener may register another.
        let listeners = self.0.lock().unwrap().clone();
        for listener in listeners {
            listener.on_memberships(slot_id, members);
        }
    }
}

/// A slot session's handle on its room's listeners.
#[derive(Clone)]
pub(crate) struct MembershipScope {
    pub(crate) slot_id: String,
    pub(crate) listeners: MembershipListeners,
}

impl MembershipScope {
    pub(crate) fn notify(&self, members: &[JoinedMembership]) {
        self.listeners.notify(&self.slot_id, members);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BaseRtcRoom;
    use crate::host::backend::NoopBackend;
    use crate::host::event::{EventOrigin, RawStickyEvent, RawStickyEventContent};
    use crate::session::{ApplicationInfo, MemberInfo, Membership};

    const ROOM_ID: &str = "!room:example.org";
    const SLOT_ID: &str = "m.call#ROOM";

    fn joined(sender: &str, member_id: &str, event_id: &str) -> RawStickyEvent {
        RawStickyEvent {
            room_id: ROOM_ID.to_owned(),
            event_id: Some(event_id.to_owned()),
            sender: sender.to_owned(),
            origin: EventOrigin::default(),
            event_type: "m.rtc.member".to_owned(),
            content: RawStickyEventContent {
                slot_id: SLOT_ID.to_owned(),
                sticky_key: member_id.to_owned(),
                application: ApplicationInfo::new("m.call"),
                member: MemberInfo {
                    id: Some(member_id.to_owned()),
                    membership: Some(Membership::Join),
                },
                transports: None,
                leave_reason: None,
                created_ts: None,
            },
        }
    }

    type Seen = Arc<Mutex<Vec<(String, Vec<String>)>>>;

    fn recorder() -> (Seen, Arc<dyn MembershipListener>) {
        let seen: Seen = Arc::default();
        let listener = {
            let seen = seen.clone();
            move |slot_id: &str, members: &[JoinedMembership]| {
                seen.lock().unwrap().push((
                    slot_id.to_owned(),
                    members.iter().map(|m| m.member_id.clone()).collect(),
                ));
            }
        };
        (seen, Arc::new(listener))
    }

    fn member_ids(seen: &Seen) -> Vec<Vec<String>> {
        seen.lock()
            .unwrap()
            .iter()
            .map(|(_, ids)| ids.clone())
            .collect()
    }

    #[tokio::test]
    async fn every_published_membership_set_reaches_the_listener_with_its_session() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);
        let (seen, listener) = recorder();
        room.add_membership_listener(listener);

        let alice = joined("@alice:example.org", "alice-a", "$a1");
        let bob = joined("@bob:example.org", "bob-a", "$b1");
        room.set_current_sticky_state(vec![alice.clone(), bob])
            .await
            .unwrap();
        room.set_current_sticky_state(vec![alice]).await.unwrap();

        assert_eq!(
            member_ids(&seen),
            vec![
                vec!["alice-a".to_owned(), "bob-a".to_owned()],
                vec!["alice-a".to_owned()]
            ],
            "the leave must be heard in the same call that caused it",
        );
        assert_eq!(seen.lock().unwrap()[0].0, SLOT_ID);
    }

    #[tokio::test]
    async fn a_moved_event_id_alone_is_published_to_the_listener() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);
        let (seen, listener) = recorder();
        room.add_membership_listener(listener);

        room.set_current_sticky_state(vec![joined("@alice:example.org", "alice-a", "$a1")])
            .await
            .unwrap();
        room.set_current_sticky_state(vec![joined("@alice:example.org", "alice-a", "$a2")])
            .await
            .unwrap();
        // The same state again notifies nobody.
        room.set_current_sticky_state(vec![joined("@alice:example.org", "alice-a", "$a2")])
            .await
            .unwrap();

        assert_eq!(seen.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_late_listener_is_replayed_the_current_memberships() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);
        room.set_current_sticky_state(vec![joined("@alice:example.org", "alice-a", "$a1")])
            .await
            .unwrap();

        let (seen, listener) = recorder();
        room.add_membership_listener(listener);
        assert_eq!(member_ids(&seen), vec![vec!["alice-a".to_owned()]]);

        let mut other_slot = joined("@carol:example.org", "carol-a", "$c1");
        other_slot.content.slot_id = "m.call#OTHER".to_owned();
        room.set_current_sticky_state(vec![
            joined("@alice:example.org", "alice-a", "$a1"),
            other_slot,
        ])
        .await
        .unwrap();
        assert_eq!(
            seen.lock().unwrap().len(),
            2,
            "a slot session created later is covered"
        );
    }

    #[tokio::test]
    async fn every_listener_hears_every_membership_change() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);
        let (first, listener) = recorder();
        room.add_membership_listener(listener);
        let (second, listener) = recorder();
        room.add_membership_listener(listener);

        room.set_current_sticky_state(vec![joined("@alice:example.org", "alice-a", "$a1")])
            .await
            .unwrap();

        assert_eq!(member_ids(&first), member_ids(&second));
        assert_eq!(first.lock().unwrap().len(), 1);
    }
}
