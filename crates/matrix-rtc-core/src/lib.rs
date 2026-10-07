// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Core MatrixRTC domain crate.
//!
//! This crate keeps platform-agnostic RTC behavior and receives data through DTOs
//! (`RawStickyEvent`, `StickyEventsUpdate`). DTOs are used on purpose so the core
//! is decoupled from SDK-specific event types (JS SDK objects, FFI structs, etc.).

mod base_rtc_room;
mod client;
pub mod compat;
mod encryption;
mod error;
pub mod executor;
pub mod feeder;
mod host;
mod join;
mod maybe_send;
mod membership_listener;
mod own_membership;
mod session;
mod slot;
mod transport;
mod upkeep;
mod wire;

/// The SDK release version (the workspace version every crate shares), for a
/// host to show or attach to its log reports.
pub const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");

pub use base_rtc_room::BaseRtcRoom;
pub use client::{BaseRoom, BaseRtcClient, BaseRtcRoomHandle, OpenError, RoomOptions};
pub use encryption::types::{
    EncryptionConfig, InboundEncryptionKey, KeyMaterialSignal, KeyOrigin, KeyRejection,
    OutboundEncryptionKey, OutdatedKeyFilter, ParticipantDeviceInfo, ReceivedEncryptionKey,
};
pub use encryption::{
    DiscardedKey, EncryptionKeySignalHandler, EncryptionManager, KEY_MESSAGE_TYPE, RtcClock,
    RtcIdentityMapper,
};
pub use error::{CommandError, JoinError, LeaveError};
pub use host::application::ApplicationIntake;
pub use host::backend::{
    BackendError, EventEncryption, EventIn, MatrixBackend, OpenIdToken, RoomSink, RoomSubjects,
    Subscription, ToDeviceDelivery, ToDeviceMessageIn, ToDeviceRecipient, ToDeviceSink,
};
pub use host::event::{
    EventConversionError, EventOrigin, RawStickyEvent, RawStickyEventContent, RawStickyEventUpdate,
    RawTimelineEvent, RelationsRequest, StickyEventsUpdate,
};
pub use join::{
    DEFAULT_KEEP_ALIVE_INTERVAL_MS, DEFAULT_KEEP_ALIVE_TIMEOUT_MS, JoinSessionParams,
    LeaveSessionParams, ROOM_APPLICATION_SLOT_ID, TransportIntent, generate_member_id,
};
pub use maybe_send::MaybeSend;
pub use membership_listener::MembershipListener;
pub use own_membership::{
    DelayedLeaveSupport, KeepAliveInfo, MembershipTimings, OwnMembershipMachine,
    OwnMembershipState, transport_to_json,
};
pub use session::{
    ApplicationInfo, JoinedMembership, LeaveCode, LeaveReason, LeftMembership, MemberInfo,
    Membership, RtcMembershipEvent, SlotSession,
};
pub use slot::{
    EncryptionMechanism, OpenSlot, RawSlotEvent, RawSlotEventContent, RoomEncryption,
    SLOT_EVENT_TYPE, SlotEncryption, SlotState, SlotStatus,
};
pub use transport::{
    LiveKitTransport, MemberTransports, RawRtcTransport, RtcTransport, UnsupportedTransport,
};
pub use wire::wire_event_type;

/// Test doubles for crates built on the core, behind the `testing` feature.
#[cfg(feature = "testing")]
pub mod testing {
    pub use crate::host::backend::{MockBackend, MockRoomSubscription, MockToDeviceSubscription};
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::backend::NoopBackend;
    use std::sync::Arc;

    const ROOM_ID: &str = "!room:example.org";
    const EVENT_TYPE_RTC_MEMBER: &str = "m.rtc.member";

    fn sticky_event(
        sender: &str,
        slot_id: &str,
        sticky_key: &str,
        application_type: Option<&str>,
        member: MemberInfo,
        leave_reason: Option<LeaveReason>,
    ) -> RawStickyEvent {
        RawStickyEvent {
            room_id: ROOM_ID.to_owned(),
            event_id: None,
            sender: sender.to_owned(),
            origin: EventOrigin::default(),
            event_type: EVENT_TYPE_RTC_MEMBER.to_owned(),
            content: RawStickyEventContent {
                slot_id: slot_id.to_owned(),
                sticky_key: sticky_key.to_owned(),
                application: ApplicationInfo {
                    application_type: application_type.map(str::to_owned),
                    extra: std::collections::BTreeMap::new(),
                },
                member,
                transports: None,
                leave_reason,
                created_ts: None,
            },
        }
    }

    fn joined_event(sender: &str, slot_id: &str, sticky_key: &str) -> RawStickyEvent {
        sticky_event(
            sender,
            slot_id,
            sticky_key,
            Some("m.call"),
            MemberInfo {
                id: Some(sticky_key.to_owned()),
                membership: Some(Membership::Join),
            },
            None,
        )
    }

    #[allow(dead_code)]
    fn left_event(sender: &str, slot_id: &str, sticky_key: &str) -> RawStickyEvent {
        sticky_event(
            sender,
            slot_id,
            sticky_key,
            None,
            MemberInfo {
                id: Some(sticky_key.to_owned()),
                membership: Some(Membership::Leave),
            },
            Some(LeaveReason::new(LeaveCode::Leave)),
        )
    }

    fn slot_event(slot_id: &str, json: &str) -> RawSlotEvent {
        RawSlotEvent {
            room_id: ROOM_ID.to_owned(),
            slot_id: slot_id.to_owned(),
            content: serde_json::from_str(json).expect("slot content must parse"),
        }
    }

    fn open_call_slot() -> RawSlotEvent {
        slot_event(
            "m.call#room",
            r#"{ "status": "open", "application": { "type": "m.call" } }"#,
        )
    }

    /// An open slot prescribing MSC4143 per-member media keys, so joining it in
    /// an encrypted room turns key distribution on.
    fn encrypted_call_slot() -> RawSlotEvent {
        slot_event(
            "m.call#room",
            r#"{ "status": "open",
                 "application": { "type": "m.call" },
                 "encryption": { "type": "m.per_member" } }"#,
        )
    }

    /// Joins as alice under an explicit `member_id`, so a leave/rejoin pair can
    /// be told apart in the assertions.
    /// The account these tests join as: the backend's, as every join's is.
    fn alice_backend() -> Arc<crate::host::backend::MockBackend> {
        let mut backend = crate::host::backend::MockBackend::new();
        backend.user_id = "@alice:example.org".to_owned();
        backend.device_id = "ALICEDEV".to_owned();
        Arc::new(backend)
    }

    async fn join_as(
        room: &mut BaseRtcRoom<crate::host::backend::MockBackend>,
        member_id: &str,
    ) -> String {
        let mut params = JoinSessionParams::application("m.call").transport(RtcTransport::LiveKit(
            LiveKitTransport {
                livekit_service_url: "https://example.com/jwt".to_owned(),
            },
        ));
        params.membership_id = Some(member_id.to_owned());
        room.join(params).await.expect("join should succeed")
    }

    /// Feeds the current sticky state containing one peer, the way a host does.
    async fn admit_peer(
        room: &mut BaseRtcRoom<crate::host::backend::MockBackend>,
        user_id: &str,
        device_id: &str,
        member_id: &str,
    ) {
        let event = RawStickyEvent {
            origin: EventOrigin::encrypted(Some(device_id.to_owned())),
            ..joined_event(user_id, "m.call#room", member_id)
        };
        room.set_current_sticky_state(vec![event]).await.unwrap();
    }

    async fn leave_call(room: &mut BaseRtcRoom<crate::host::backend::MockBackend>) {
        room.leave("m.call#room", LeaveSessionParams::new())
            .await
            .expect("leave should succeed");
    }

    fn joined_memberships(
        room: &BaseRtcRoom<crate::host::backend::MockBackend>,
    ) -> Vec<crate::session::JoinedMembership> {
        room.subscribe_membership_snapshots("m.call#room")
            .expect("the session should exist")
            .borrow()
            .clone()
    }

    async fn encrypted_call_room(
        sender: Arc<crate::host::backend::MockBackend>,
    ) -> BaseRtcRoom<crate::host::backend::MockBackend> {
        let mut room = BaseRtcRoom::with_backend(ROOM_ID, sender);
        room.on_encryption_received(true).await;
        room.on_slots_received(vec![encrypted_call_slot()]).await;
        room
    }

    /// An MSC4354 sticky entry expires when its owner stops refreshing it — a
    /// crashed client — and the lapse produces no event at all. So the current
    /// state simply arrives smaller, and the member has to go: this call
    /// replaces rather than merges. Merging kept them in the call for good and
    /// pushed expiry detection onto every host.
    #[tokio::test]
    async fn a_member_whose_sticky_entry_expired_is_dropped() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);
        let alice = joined_event("@alice:example.org", "m.call#room", "alice-a");
        let bob = joined_event("@bob:example.org", "m.call#room", "bob-a");

        room.set_current_sticky_state(vec![alice.clone(), bob])
            .await
            .unwrap();
        assert_eq!(room.member_count("m.call#room"), Some(2));

        // Bob's entry lapsed: no leave event, he is simply absent now.
        room.set_current_sticky_state(vec![alice]).await.unwrap();
        assert_eq!(
            room.member_count("m.call#room"),
            Some(1),
            "an expired entry must leave the call, with no leave event to feed in"
        );

        // And an empty state empties the room, rather than being a no-op.
        room.set_current_sticky_state(Vec::new()).await.unwrap();
        assert_eq!(room.member_count("m.call#room"), Some(0));
    }

    /// A slot whose last member expired contributes no events at all, so it
    /// vanishes from the payload entirely. It still has to be cleared, or the
    /// replace has a hole exactly where the ghost would be.
    #[tokio::test]
    async fn a_slot_missing_from_the_current_state_is_cleared_too() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);
        let in_call = joined_event("@alice:example.org", "m.call#room", "alice-a");
        let in_other = joined_event("@bob:example.org", "m.call#OTHER", "bob-a");

        room.set_current_sticky_state(vec![in_call.clone(), in_other])
            .await
            .unwrap();
        assert_eq!(room.member_count("m.call#OTHER"), Some(1));

        // Only the first slot is represented now; the second must empty.
        room.set_current_sticky_state(vec![in_call]).await.unwrap();
        assert_eq!(room.member_count("m.call#room"), Some(1));
        assert_eq!(
            room.member_count("m.call#OTHER"),
            Some(0),
            "a slot absent from the current state is empty, not untouched"
        );
    }

    /// A second call in the same process must distribute a key just as the first
    /// one did.
    ///
    /// The session survives `leave()` on purpose — it is keyed by `(room, slot)`
    /// and a host may still want the joined memberships after hanging up — so the second
    /// join starts with the first call's joined memberships already in place. Nothing else
    /// changes: the incumbent's membership is byte-identical across our leave and
    /// rejoin, so there is no membership change for the second call to ride on.
    /// If distribution only ever happens on a membership *change*, the second
    /// call silently never distributes and the incumbent is left at
    /// `MISSING_KEY` — which is what an Android integration hit, four runs out of
    /// four.
    #[tokio::test]
    async fn a_rejoin_in_the_same_process_distributes_a_key_to_the_incumbent() {
        let sender = alice_backend();
        let mut room = encrypted_call_room(sender.clone()).await;

        // First call: bob arrives after we joined, so the joined memberships change while we
        // hold a key and distribution is triggered.
        join_as(&mut room, "alice-a").await;
        admit_peer(&mut room, "@bob:example.org", "BOBDEV", "bob-a").await;
        assert!(
            !sender
                .to_device_messages_for("@bob:example.org", "BOBDEV")
                .is_empty(),
            "first call should have distributed a key to the incumbent"
        );

        leave_call(&mut room).await;
        sender.to_device_messages.lock().unwrap().clear();

        // Second call: deliberately no further sticky events. Bob has not moved.
        join_as(&mut room, "alice-b").await;

        let sent = sender.to_device_messages_for("@bob:example.org", "BOBDEV");
        assert!(
            !sent.is_empty(),
            "the second call in the same process distributed no key to the incumbent, so its \
             media cannot be decrypted"
        );
        assert_eq!(
            sent.len(),
            1,
            "the incumbent should be handed the key once, not once per code path \
             that noticed the join"
        );
        let (_, content) = &sent[0];
        assert_eq!(
            content.pointer("/member_id").and_then(|v| v.as_str()),
            Some("alice-b"),
            "the key must be advertised under the member id of the current join"
        );
        assert_eq!(
            content.pointer("/media_key/index").and_then(|v| v.as_u64()),
            Some(0),
            "a fresh join starts a fresh key index"
        );
    }

    /// MSC4143 requires a fresh `member.id` per join, so the previous
    /// participation of this very device is not a peer — it is us, one call ago.
    /// Leaving it in the joined memberships gives the media layer a phantom member to open a
    /// receive stream for and to expect a key from.
    #[tokio::test]
    async fn a_rejoin_does_not_advertise_the_previous_participation() {
        let sender = alice_backend();
        let mut room = encrypted_call_room(sender.clone()).await;

        join_as(&mut room, "alice-a").await;
        // The homeserver echoes our own membership back through the sticky map.
        admit_peer(&mut room, "@alice:example.org", "ALICEDEV", "alice-a").await;
        leave_call(&mut room).await;

        join_as(&mut room, "alice-b").await;

        let member_ids: Vec<_> = joined_memberships(&room)
            .into_iter()
            .map(|membership| membership.member_id)
            .collect();
        assert!(
            !member_ids.iter().any(|id| id == "alice-a"),
            "our superseded participation is still joined: {member_ids:?}"
        );
    }

    /// A join hands back the event id of the membership it sent, so an
    /// application can relate its own events to it without asking again.
    #[tokio::test]
    async fn a_join_returns_its_membership_event_id() {
        let sender = alice_backend();
        let mut room = encrypted_call_room(sender.clone()).await;

        let event_id = join_as(&mut room, "alice-a").await;

        assert!(!event_id.is_empty());
        assert_eq!(room.own_membership_event_id("m.call#room"), Some(event_id));
    }

    /// A joined slot is not joined again under a fresh member id: that would
    /// replace the live participation without leaving it.
    #[tokio::test]
    async fn joining_a_joined_slot_is_refused() {
        let sender = alice_backend();
        let mut room = encrypted_call_room(sender.clone()).await;
        join_as(&mut room, "alice-a").await;

        let mut params = JoinSessionParams::application("m.call").transport(RtcTransport::LiveKit(
            LiveKitTransport {
                livekit_service_url: "https://example.com/jwt".to_owned(),
            },
        ));
        params.membership_id = Some("alice-b".to_owned());

        assert!(matches!(
            room.join(params).await,
            Err(JoinError::AlreadyJoined(member_id)) if member_id == "alice-a"
        ));
        assert_eq!(
            room.own_member_id("m.call#room").as_deref(),
            Some("alice-a")
        );
    }

    /// The session outliving a leave is the contract, not an accident: a host
    /// that hangs up may still render "3 people are in this call", and the media
    /// session is torn down separately with no ordering guarantee. Pinned so a
    /// future "just drop the session on leave" refactor has to argue with a test.
    #[tokio::test]
    async fn a_left_session_still_publishes_the_peer_memberships() {
        let sender = alice_backend();
        let mut room = encrypted_call_room(sender.clone()).await;

        join_as(&mut room, "alice-a").await;
        admit_peer(&mut room, "@bob:example.org", "BOBDEV", "bob-a").await;
        leave_call(&mut room).await;

        assert!(
            room.member_count("m.call#room").is_some(),
            "the session should survive"
        );
        assert!(
            joined_memberships(&room)
                .iter()
                .any(|membership| membership.member_id == "bob-a"),
            "the peer memberships should survive our own departure"
        );
        assert_eq!(
            room.member_count("m.call#room"),
            Some(1),
            "the incumbent is still in the call"
        );
        assert_eq!(
            room.own_member_id("m.call#room"),
            None,
            "we are no longer joined, so we have no member id"
        );
    }

    /// Restored after fixing the `BaseRtcRoom::new(ROOM_ID)` recursion that used
    /// to overflow the stack (it was misattributed to watch channels).
    #[tokio::test]
    async fn room_routes_snapshot_and_diff_update_membership() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);
        let joined = joined_event("@alice:example.org", "m.call#room", "alice-device-a");

        room.set_current_sticky_state(vec![joined.clone()])
            .await
            .unwrap();

        assert_eq!(room.member_count("m.call#room"), Some(1));

        // A departure reaches the core as a leave-shaped sticky replacing the
        // join under the same key — and, once that lapses too, as plain absence.
        let left = left_event("@alice:example.org", "m.call#room", "alice-device-a");
        room.set_current_sticky_state(vec![left]).await.unwrap();

        assert_eq!(room.member_count("m.call#room"), Some(0));
    }

    #[tokio::test]
    async fn room_accepts_stable_and_unstable_rtc_member_event_types() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);

        let stable = joined_event("@alice:example.org", "m.call#room", "alice-device-a");
        let unstable = RawStickyEvent {
            event_type: "org.matrix.msc4143.rtc.member".to_owned(),
            ..joined_event("@bob:example.org", "m.call#room", "bob-device-a")
        };

        room.set_current_sticky_state(vec![stable, unstable])
            .await
            .unwrap();

        assert_eq!(room.member_count("m.call#room"), Some(2));
    }

    #[tokio::test]
    async fn room_ignores_non_membership_event_types() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);

        let event = RawStickyEvent {
            event_type: "m.not.rtc.member".to_owned(),
            ..joined_event("@alice:example.org", "m.call#room", "alice-device-a")
        };

        room.set_current_sticky_state(vec![event]).await.unwrap();

        assert_eq!(room.member_count("m.call#room"), None);
    }

    /// Until a host supplies room state the open-slot condition cannot be
    /// evaluated, so it is not enforced; otherwise every existing consumer would
    /// silently see an empty session.
    #[tokio::test]
    async fn members_are_joined_while_slot_state_is_unsupplied() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);

        room.set_current_sticky_state(vec![joined_event(
            "@alice:example.org",
            "m.call#room",
            "alice-a",
        )])
        .await
        .unwrap();

        assert_eq!(room.member_count("m.call#room"), Some(1));
        assert_eq!(room.slot_state("m.call#room"), None);
    }

    /// Re-applying the same sticky state must publish nothing at all.
    ///
    /// `set_current_sticky_state` rebuilds the candidate set from scratch, and
    /// used to refresh after *each* event in the batch. The joined memberships were therefore
    /// republished on the way up — one member, then two, then three — so the
    /// first publication of every tick looked like everyone but one participant
    /// leaving. The encryption room believed it and rotated the key, once per
    /// sticky tick per session, each rotation sending to every remaining member.
    /// In a ten-device call that was a rotation every few seconds; the cost is
    /// quadratic in participants.
    ///
    /// The sticky bridge re-sends the full live set on every tick, so "identical
    /// input publishes nothing" is the property that matters.
    #[tokio::test]
    async fn re_applying_the_same_sticky_state_publishes_nothing() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);
        let members = || {
            vec![
                joined_event("@alice:example.org", "m.call#room", "alice-a"),
                joined_event("@bob:example.org", "m.call#room", "bob-a"),
                joined_event("@carol:example.org", "m.call#room", "carol-a"),
            ]
        };

        room.set_current_sticky_state(members()).await.unwrap();
        assert_eq!(room.member_count("m.call#room"), Some(3));

        let mut snapshots = room
            .subscribe_membership_snapshots("m.call#room")
            .expect("the session exists");
        snapshots.borrow_and_update();

        room.set_current_sticky_state(members()).await.unwrap();

        assert!(
            !snapshots.has_changed().unwrap(),
            "an unchanged sticky state must not republish the joined memberships; every \
             republication is a membership diff the encryption room acts on",
        );
        assert_eq!(room.member_count("m.call#room"), Some(3));
    }

    /// MSC4143: a member event only counts as joined against an *open* slot.
    /// Supplying room state with no slot in it means the slot is closed.
    #[tokio::test]
    async fn members_are_left_when_no_slot_is_open() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);

        room.set_current_sticky_state(vec![joined_event(
            "@alice:example.org",
            "m.call#room",
            "alice-a",
        )])
        .await
        .unwrap();
        room.on_slots_received(Vec::new()).await;

        assert_eq!(room.member_count("m.call#room"), Some(0));
        assert_eq!(room.slot_state("m.call#room"), Some(SlotState::Closed));
    }

    #[tokio::test]
    async fn members_are_joined_against_an_open_slot() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);

        room.on_slots_received(vec![open_call_slot()]).await;
        room.set_current_sticky_state(vec![joined_event(
            "@alice:example.org",
            "m.call#room",
            "alice-a",
        )])
        .await
        .unwrap();

        assert_eq!(room.member_count("m.call#room"), Some(1));
    }

    /// "Clients MUST constantly react to and respect the latest state of the
    /// room": closing a slot mid-session leaves everyone in it, and reopening it
    /// brings back the members whose events are still sticky.
    #[tokio::test]
    async fn closing_and_reopening_a_slot_re_evaluates_members() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);

        room.on_slots_received(vec![open_call_slot()]).await;
        room.set_current_sticky_state(vec![joined_event(
            "@alice:example.org",
            "m.call#room",
            "alice-a",
        )])
        .await
        .unwrap();
        assert_eq!(room.member_count("m.call#room"), Some(1));

        room.on_slots_received(vec![slot_event("m.call#room", r#"{ "status": "closed" }"#)])
            .await;
        assert_eq!(room.member_count("m.call#room"), Some(0));

        room.on_slots_received(vec![open_call_slot()]).await;
        assert_eq!(room.member_count("m.call#room"), Some(1));
    }

    /// Slot state that arrives before the session exists still governs it.
    #[tokio::test]
    async fn slot_state_applies_to_sessions_created_later() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);

        room.on_slots_received(Vec::new()).await;
        room.set_current_sticky_state(vec![joined_event(
            "@alice:example.org",
            "m.call#room",
            "alice-a",
        )])
        .await
        .unwrap();

        assert_eq!(room.member_count("m.call#room"), Some(0));
    }

    /// The way back from "no open slots" to "not my business".
    ///
    /// A room of a MatrixRTC generation older than `m.rtc.slot` contains none, so
    /// a host that fed slot state before learning that must be able to take it
    /// back — otherwise every member of that room, itself included, stays
    /// projected out for the rest of the process.
    #[tokio::test]
    async fn forgetting_a_room_s_slots_stops_the_condition_being_enforced() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);

        // No slot in the room: everyone is projected out.
        room.on_slots_received(Vec::new()).await;
        room.set_current_sticky_state(vec![joined_event(
            "@alice:example.org",
            "m.call#room",
            "alice-a",
        )])
        .await
        .unwrap();
        assert_eq!(room.member_count("m.call#room"), Some(0));

        room.forget_slots().await;
        assert_eq!(
            room.member_count("m.call#room"),
            Some(1),
            "an unenforced condition must not keep a live member out",
        );
        assert_eq!(
            room.slot_state("m.call#room"),
            None,
            "the slot is unknown again, not open",
        );

        // And a session created afterwards is born unenforced too, rather than
        // inheriting the forgotten "no slots".
        room.set_current_sticky_state(vec![
            joined_event("@alice:example.org", "m.call#room", "alice-a"),
            joined_event("@bob:example.org", "m.call#OTHER", "bob-a"),
        ])
        .await
        .unwrap();
        assert_eq!(room.member_count("m.call#OTHER"), Some(1));
    }

    /// Slot state naming another room is dropped, and says nothing about the
    /// same slot id here.
    #[tokio::test]
    async fn slot_events_for_another_room_are_dropped() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);

        room.on_slots_received(vec![RawSlotEvent {
            room_id: "!other:example.org".to_owned(),
            ..open_call_slot()
        }])
        .await;

        assert!(
            room.slot_state("m.call#room")
                .is_some_and(|state| !state.is_open()),
            "the state was supplied, but the only slot in it belonged elsewhere",
        );
    }

    /// Sticky members naming another room are dropped rather than routed into
    /// this room's slots.
    #[tokio::test]
    async fn sticky_events_for_another_room_are_dropped() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);

        room.set_current_sticky_state(vec![
            joined_event("@alice:example.org", "m.call#room", "alice-a"),
            RawStickyEvent {
                room_id: "!other:example.org".to_owned(),
                ..joined_event("@bob:example.org", "m.call#room", "bob-a")
            },
        ])
        .await
        .unwrap();

        assert_eq!(room.member_count("m.call#room"), Some(1));
    }

    /// MSC4143: a member event only counts while its sender is still joined to
    /// the room.
    #[tokio::test]
    async fn members_who_left_the_room_are_not_joined() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);

        room.set_current_sticky_state(vec![
            joined_event("@alice:example.org", "m.call#room", "alice-a"),
            joined_event("@bob:example.org", "m.call#room", "bob-a"),
        ])
        .await
        .unwrap();
        assert_eq!(room.member_count("m.call#room"), Some(2));

        // Bob is no longer in the room, though his member event is still sticky.
        room.on_members_received(vec!["@alice:example.org".to_owned()])
            .await;

        assert_eq!(room.member_count("m.call#room"), Some(1));
    }

    /// MSC4143: in an encrypted room a member event that was not encrypted
    /// "MUST be considered left".
    #[tokio::test]
    async fn cleartext_member_events_are_left_in_an_encrypted_room() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);

        let encrypted = RawStickyEvent {
            origin: EventOrigin::encrypted(Some("ALICEDEV".to_owned())),
            ..joined_event("@alice:example.org", "m.call#room", "alice-a")
        };
        let cleartext = RawStickyEvent {
            origin: EventOrigin::Cleartext,
            ..joined_event("@bob:example.org", "m.call#room", "bob-a")
        };

        room.set_current_sticky_state(vec![encrypted, cleartext])
            .await
            .unwrap();
        // Nothing has reported the room's encryption yet, so neither is judged.
        assert_eq!(room.member_count("m.call#room"), Some(2));

        room.on_encryption_received(true).await;
        assert_eq!(room.member_count("m.call#room"), Some(1));
    }

    /// An unencrypted room imposes no such requirement.
    #[tokio::test]
    async fn cleartext_member_events_are_fine_in_an_unencrypted_room() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);

        let cleartext = RawStickyEvent {
            origin: EventOrigin::Cleartext,
            ..joined_event("@bob:example.org", "m.call#room", "bob-a")
        };
        room.set_current_sticky_state(vec![cleartext])
            .await
            .unwrap();
        room.on_encryption_received(false).await;

        assert_eq!(room.member_count("m.call#room"), Some(1));
    }

    /// A slot with no encryption object is closed in an encrypted room, so its
    /// members are left even though everything else about them is valid.
    #[tokio::test]
    async fn unencrypted_slot_closes_in_an_encrypted_room() {
        let mut room: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);

        room.on_slots_received(vec![open_call_slot()]).await;
        room.set_current_sticky_state(vec![joined_event(
            "@alice:example.org",
            "m.call#room",
            "alice-a",
        )])
        .await
        .unwrap();
        assert_eq!(room.member_count("m.call#room"), Some(1));

        room.on_encryption_received(true).await;

        assert_eq!(room.slot_state("m.call#room"), Some(SlotState::Closed));
        assert_eq!(room.member_count("m.call#room"), Some(0));
    }

    /// Room encryption arriving after the slot re-resolves it, and vice versa;
    /// the room keeps slots unresolved so either order works.
    #[tokio::test]
    async fn slot_resolution_reacts_to_room_encryption_in_either_order() {
        let encrypted_slot = || {
            slot_event(
                "m.call#room",
                r#"{ "status": "open",
                     "application": { "type": "m.call" },
                     "encryption": { "type": "m.per_member" } }"#,
            )
        };

        // Encryption first, then the slot.
        let mut a: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);
        a.on_encryption_received(true).await;
        a.on_slots_received(vec![encrypted_slot()]).await;
        assert!(a.slot_state("m.call#room").unwrap().is_open());

        // Slot first, then encryption.
        let mut b: BaseRtcRoom<NoopBackend> = BaseRtcRoom::new(ROOM_ID);
        b.on_slots_received(vec![encrypted_slot()]).await;
        b.on_encryption_received(true).await;
        assert!(b.slot_state("m.call#room").unwrap().is_open());
    }

    /// The slot's `encryption` object — not local configuration — decides
    /// whether media keys are distributed. Exercised end to end: joining a slot
    /// that prescribes `m.per_member` in an encrypted room produces key
    /// to-device traffic to the other member.
    #[tokio::test]
    async fn slot_encryption_turns_key_distribution_on() {
        let sender = alice_backend();
        let mut room = BaseRtcRoom::with_backend(ROOM_ID, sender.clone());

        room.on_encryption_received(true).await;
        room.on_slots_received(vec![slot_event(
            "m.call#room",
            r#"{ "status": "open",
                         "application": { "type": "m.call" },
                         "encryption": { "type": "m.per_member" } }"#,
        )])
        .await;

        // Local config asks for NO keys; the slot must override it upward.
        join_and_admit_a_peer(&mut room, false).await;

        assert!(
            !sender.to_device_messages.lock().unwrap().is_empty(),
            "keys should be distributed when the slot prescribes a mechanism"
        );
    }

    /// Conversely, MSC4143 forbids RTC encryption in an unencrypted room, so no
    /// keys are distributed there however the client is configured.
    #[tokio::test]
    async fn absent_slot_encryption_turns_key_distribution_off() {
        let sender = alice_backend();
        let mut room = BaseRtcRoom::with_backend(ROOM_ID, sender.clone());

        room.on_encryption_received(false).await;
        room.on_slots_received(vec![open_call_slot()]).await;

        // Local config asks for keys; the slot must override it downward.
        join_and_admit_a_peer(&mut room, true).await;

        assert!(
            sender.to_device_messages.lock().unwrap().is_empty(),
            "no keys should be distributed when the slot prescribes no mechanism"
        );
    }

    /// Joins as alice with `local_manage_media_keys` as the caller's own
    /// preference, then lets bob in so a membership change triggers key
    /// distribution (if it is enabled at all).
    async fn join_and_admit_a_peer(
        room: &mut BaseRtcRoom<crate::host::backend::MockBackend>,
        local_manage_media_keys: bool,
    ) {
        let mut params = JoinSessionParams::application("m.call").transport(RtcTransport::LiveKit(
            LiveKitTransport {
                livekit_service_url: "https://example.com/jwt".to_owned(),
            },
        ));
        params.membership_id = Some("alice-a".to_owned());
        params.encryption_config = Some(EncryptionConfig {
            manage_media_keys: local_manage_media_keys,
            ..EncryptionConfig::default()
        });
        room.join(params).await.expect("join should succeed");

        let bob = RawStickyEvent {
            origin: EventOrigin::encrypted(Some("BOBDEV".to_owned())),
            ..joined_event("@bob:example.org", "m.call#room", "bob-a")
        };
        room.set_current_sticky_state(vec![bob]).await.unwrap();
    }

    /// A member that only receives — a recorder, say — is a valid participant
    /// under MSC4143: `transports` carries no REQUIRED marker, so publishing
    /// nothing is a legitimate choice rather than a broken join.
    #[tokio::test]
    async fn a_receive_only_member_joins_and_publishes_nothing() {
        let sender = alice_backend();
        let mut room = BaseRtcRoom::with_backend(ROOM_ID, sender.clone());

        let params =
            JoinSessionParams::application("m.call").transport(TransportIntent::ReceiveOnly {
                can_subscribe: vec!["livekit".to_owned()],
            });
        room.join(params).await.expect("join should succeed");

        let sticky = sender.sticky_events.lock().unwrap();
        let (_, _, content, _) = sticky.first().expect("a join should have been sent");

        // Nothing published, but peers are still told what it can receive on,
        // so they pick a transport it can actually hear.
        assert!(content.pointer("/transports/published/0").is_none());
        assert_eq!(
            content
                .pointer("/transports/can_subscribe/0")
                .and_then(|v| v.as_str()),
            Some("livekit")
        );
        assert_eq!(
            content
                .pointer("/member/membership")
                .and_then(|v| v.as_str()),
            Some("join")
        );
    }

    /// Stating nothing at all is legal too; the object is then omitted entirely
    /// rather than emitted empty.
    #[tokio::test]
    async fn a_receive_only_member_with_no_cue_omits_transports() {
        let sender = alice_backend();
        let mut room = BaseRtcRoom::with_backend(ROOM_ID, sender.clone());

        let params =
            JoinSessionParams::application("m.call").transport(TransportIntent::ReceiveOnly {
                can_subscribe: Vec::new(),
            });
        room.join(params).await.expect("join should succeed");

        let sticky = sender.sticky_events.lock().unwrap();
        let (_, _, content, _) = sticky.first().expect("a join should have been sent");
        assert!(content.get("transports").is_none());
    }

    /// A publishing member advertises the transport the application chose, and
    /// declares it can receive on that type too.
    #[tokio::test]
    async fn a_publishing_member_advertises_its_transport() {
        let sender = alice_backend();
        let mut room = BaseRtcRoom::with_backend(ROOM_ID, sender.clone());

        let params = JoinSessionParams::application("m.call").transport(RtcTransport::LiveKit(
            LiveKitTransport {
                livekit_service_url: "https://sfu.example.com/jwt".to_owned(),
            },
        ));
        room.join(params).await.expect("join should succeed");

        let sticky = sender.sticky_events.lock().unwrap();
        let (_, _, content, _) = sticky.first().expect("a join should have been sent");
        assert_eq!(
            content
                .pointer("/transports/published/0/livekit_service_url")
                .and_then(|v| v.as_str()),
            Some("https://sfu.example.com/jwt")
        );
        assert_eq!(
            content
                .pointer("/transports/can_subscribe/0")
                .and_then(|v| v.as_str()),
            Some("livekit")
        );
    }

    #[test]
    fn joined_event_with_livekit_transport_is_parsed_correctly() {
        use crate::transport::{RawRtcTransport, RtcTransport};
        use std::collections::BTreeMap;

        let mut extra_fields = BTreeMap::new();
        extra_fields.insert(
            "livekit_service_url".to_owned(),
            serde_json::Value::String("https://example.com/livekit/jwt".to_owned()),
        );

        let event = RawStickyEvent {
            room_id: ROOM_ID.to_owned(),
            event_id: None,
            sender: "@alice:example.org".to_owned(),
            origin: EventOrigin::default(),
            event_type: "m.rtc.member".to_owned(),
            content: RawStickyEventContent {
                slot_id: "m.call#room".to_owned(),
                sticky_key: "alice-device-a".to_owned(),
                application: ApplicationInfo {
                    application_type: Some("m.call".to_owned()),
                    extra: std::collections::BTreeMap::new(),
                },
                member: MemberInfo {
                    id: Some("alice-device-a".to_owned()),
                    membership: Some(Membership::Join),
                },
                transports: Some(MemberTransports::publishing(RawRtcTransport {
                    transport_type: "livekit".to_owned(),
                    extra_fields,
                })),
                leave_reason: None,
                created_ts: None,
            },
        };

        let membership_event = event.try_into_membership_event().unwrap();

        match membership_event {
            RtcMembershipEvent::Joined(joined) => {
                assert_eq!(joined.transports.len(), 1);
                match &joined.transports[0] {
                    RtcTransport::LiveKit(livekit) => {
                        assert_eq!(
                            livekit.livekit_service_url,
                            "https://example.com/livekit/jwt"
                        );
                    }
                    RtcTransport::Unsupported(_) => panic!("Expected LiveKit transport"),
                }
            }
            RtcMembershipEvent::Left(_) => panic!("Expected Joined membership"),
        }
    }

    #[test]
    fn joined_event_with_unknown_transport_is_preserved_as_unsupported() {
        use crate::transport::{RawRtcTransport, RtcTransport};
        use std::collections::BTreeMap;

        let mut extra_fields = BTreeMap::new();
        extra_fields.insert(
            "custom_field".to_owned(),
            serde_json::Value::String("custom_value".to_owned()),
        );

        let event = RawStickyEvent {
            room_id: ROOM_ID.to_owned(),
            event_id: None,
            sender: "@alice:example.org".to_owned(),
            origin: EventOrigin::default(),
            event_type: "m.rtc.member".to_owned(),
            content: RawStickyEventContent {
                slot_id: "m.call#room".to_owned(),
                sticky_key: "alice-device-a".to_owned(),
                application: ApplicationInfo {
                    application_type: Some("m.call".to_owned()),
                    extra: std::collections::BTreeMap::new(),
                },
                member: MemberInfo {
                    id: Some("alice-device-a".to_owned()),
                    membership: Some(Membership::Join),
                },
                transports: Some(MemberTransports::publishing(RawRtcTransport {
                    transport_type: "unknown_transport".to_owned(),
                    extra_fields,
                })),
                leave_reason: None,
                created_ts: None,
            },
        };

        let membership_event = event.try_into_membership_event().unwrap();

        match membership_event {
            RtcMembershipEvent::Joined(joined) => {
                assert_eq!(joined.transports.len(), 1);
                match &joined.transports[0] {
                    RtcTransport::Unsupported(unsupported) => {
                        assert_eq!(unsupported.transport_type, "unknown_transport");
                        assert!(unsupported.extra_fields.contains_key("custom_field"));
                    }
                    RtcTransport::LiveKit(_) => panic!("Expected Unsupported transport"),
                }
            }
            RtcMembershipEvent::Left(_) => panic!("Expected Joined membership"),
        }
    }

    fn restarts(sender: &crate::host::backend::MockBackend) -> usize {
        sender.restarted_events.lock().unwrap().len()
    }

    const TICK: std::time::Duration =
        std::time::Duration::from_millis(DEFAULT_KEEP_ALIVE_INTERVAL_MS);

    /// The core keeps a join alive by itself: nobody ticks it.
    #[tokio::test(start_paused = true)]
    async fn a_joined_slot_keeps_itself_alive_until_it_leaves() {
        let sender = alice_backend();
        let mut room = encrypted_call_room(sender.clone()).await;
        join_as(&mut room, "alice-a").await;
        assert_eq!(
            restarts(&sender),
            0,
            "the join itself armed the delayed leave"
        );

        tokio::time::sleep(TICK * 3 + std::time::Duration::from_millis(1)).await;
        assert_eq!(restarts(&sender), 3, "one restart per interval");

        room.leave("m.call#room", LeaveSessionParams::new())
            .await
            .expect("leave");
        tokio::time::sleep(TICK * 3).await;
        assert_eq!(restarts(&sender), 3, "nothing restarted after the leave");
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_the_room_stops_its_upkeep() {
        let sender = alice_backend();
        let mut room = encrypted_call_room(sender.clone()).await;
        join_as(&mut room, "alice-a").await;

        drop(room);
        tokio::time::sleep(TICK * 3).await;
        assert_eq!(restarts(&sender), 0);
    }

    /// What lets an owner that drops a join without leaving stop its upkeep.
    #[tokio::test(start_paused = true)]
    async fn aborting_the_upkeep_handle_stops_it_and_sends_no_leave() {
        let sender = alice_backend();
        let mut room = encrypted_call_room(sender.clone()).await;
        join_as(&mut room, "alice-a").await;

        room.upkeep_abort_handle("m.call#room")
            .expect("an upkeep runs")
            .abort();
        tokio::time::sleep(TICK * 3).await;
        assert_eq!(restarts(&sender), 0);
        assert!(sender.cancelled_events.lock().unwrap().is_empty());
        assert!(room.own_member_id("m.call#room").is_some(), "still joined");
    }

    /// A slot closing under our own join ends it: we leave with `slot_closed`,
    /// cancel the delayed leave, stop keeping alive, and announce the leave so
    /// the host can tear its media down.
    #[tokio::test(start_paused = true)]
    async fn closing_the_slot_leaves_our_own_join() {
        let sender = alice_backend();
        let mut room = encrypted_call_room(sender.clone()).await;
        join_as(&mut room, "alice-a").await;
        let mut auto_leaves = room
            .subscribe_auto_leaves("m.call#room")
            .expect("joined session");

        room.on_slots_received(vec![slot_event("m.call#room", r#"{ "status": "closed" }"#)])
            .await;

        assert_eq!(
            auto_leaves.try_recv().expect("an auto-leave").code,
            LeaveCode::SlotClosed
        );
        assert_eq!(room.own_member_id("m.call#room"), None);
        let (_, _, leave, _) = sender.last_sticky_event().expect("a leave was sent");
        assert_eq!(leave["leave_reason"]["code"], "slot_closed", "{leave}");
        assert_eq!(sender.cancelled_events.lock().unwrap().len(), 1);
        tokio::time::sleep(TICK * 3).await;
        assert_eq!(restarts(&sender), 0, "nothing kept alive after the leave");
    }

    /// A leave that fails to send still ends our join and is announced, so the
    /// host stops publishing; the delayed leave stays armed to remove us.
    #[tokio::test(start_paused = true)]
    async fn closing_the_slot_ends_our_join_even_when_the_leave_fails() {
        let sender = alice_backend();
        let mut room = encrypted_call_room(sender.clone()).await;
        join_as(&mut room, "alice-a").await;
        let mut auto_leaves = room
            .subscribe_auto_leaves("m.call#room")
            .expect("joined session");
        let sent_before = sender.sticky_events.lock().unwrap().len();
        *sender.sticky_event_error.lock().unwrap() = Some("offline".to_owned());

        room.on_slots_received(vec![slot_event("m.call#room", r#"{ "status": "closed" }"#)])
            .await;

        assert_eq!(
            auto_leaves.try_recv().expect("an auto-leave").code,
            LeaveCode::SlotClosed
        );
        assert_eq!(room.own_member_id("m.call#room"), None);
        assert_eq!(sender.sticky_events.lock().unwrap().len(), sent_before);
        assert!(
            sender.cancelled_events.lock().unwrap().is_empty(),
            "the delayed leave is left to fire"
        );
        tokio::time::sleep(TICK * 3).await;
        assert_eq!(restarts(&sender), 0, "nothing kept alive after the leave");
    }

    /// A host leave that fails to send is reported, but still ends our join:
    /// the machine is gone, so the delayed leave removes the membership.
    #[tokio::test(start_paused = true)]
    async fn a_leave_that_fails_to_send_still_ends_our_join() {
        let sender = alice_backend();
        let mut room = encrypted_call_room(sender.clone()).await;
        join_as(&mut room, "alice-a").await;
        *sender.sticky_event_error.lock().unwrap() = Some("offline".to_owned());

        let result = room.leave("m.call#room", LeaveSessionParams::new()).await;

        assert!(result.is_err(), "the failed send is reported");
        assert_eq!(room.own_member_id("m.call#room"), None);
        assert!(sender.cancelled_events.lock().unwrap().is_empty());
        tokio::time::sleep(TICK * 3).await;
        assert_eq!(restarts(&sender), 0, "nothing kept alive after the leave");
    }

    /// Only the leaves the session makes on its own are announced; a host that
    /// hung up knows it did.
    #[tokio::test(start_paused = true)]
    async fn a_host_leave_is_not_announced_as_an_auto_leave() {
        let sender = alice_backend();
        let mut room = encrypted_call_room(sender.clone()).await;
        join_as(&mut room, "alice-a").await;
        let mut auto_leaves = room
            .subscribe_auto_leaves("m.call#room")
            .expect("joined session");

        leave_call(&mut room).await;
        // Closing the slot now finds nobody of ours to leave.
        room.on_slots_received(vec![slot_event("m.call#room", r#"{ "status": "closed" }"#)])
            .await;

        assert!(auto_leaves.try_recv().is_err());
    }

    /// Without a runtime the join still succeeds; the host ticks it instead.
    #[test]
    fn a_join_off_a_runtime_has_no_upkeep() {
        let sender = alice_backend();
        futures::executor::block_on(async {
            let mut room = encrypted_call_room(sender.clone()).await;
            join_as(&mut room, "alice-a").await;
            assert!(room.upkeep_abort_handle("m.call#room").is_none());
            assert!(room.keep_alive("m.call#room").await);
        });
        assert_eq!(restarts(&sender), 1);
    }
}
