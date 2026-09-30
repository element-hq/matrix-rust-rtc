// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use matrix_rtc_core::testing::MockCommandSender;
use matrix_rtc_core::{
    ApplicationInfo, EventOrigin, JoinSessionParams, LeaveSessionParams, LiveKitTransport,
    MemberInfo, Membership, RawSlotEvent, RawStickyEvent, RawStickyEventContent, RawTimelineEvent,
    RtcTransport,
};
use serde_json::Value;
use tokio::sync::broadcast::error::TryRecvError;

use super::*;
use crate::notification::DEFAULT_RING_LIFETIME_MS;
use crate::reactions::ReactionSound;

const ROOM: &str = "!room:example.org";
const SLOT: &str = "m.call#ROOM";
const ALICE: &str = "@alice:example.org";
const BOB: &str = "@bob:example.org";
const BOB_MEMBER: &str = "bob-member-1";

/// A clock the test moves by hand.
struct TestClock(Arc<AtomicU64>);

impl TestClock {
    fn new(start: u64) -> Self {
        Self(Arc::new(AtomicU64::new(start)))
    }

    fn clock(&self) -> Clock {
        let time = self.0.clone();
        Arc::new(move || time.load(Ordering::SeqCst))
    }

    fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
}

fn member_event(
    sender: &str,
    device: Option<&str>,
    member_id: &str,
    event_id: &str,
) -> RawStickyEvent {
    RawStickyEvent {
        room_id: ROOM.to_owned(),
        event_id: Some(event_id.to_owned()),
        sender: sender.to_owned(),
        origin: match device {
            Some(device) => EventOrigin::encrypted(Some(device.to_owned())),
            None => EventOrigin::Unknown,
        },
        event_type: "m.rtc.member".to_owned(),
        content: RawStickyEventContent {
            slot_id: SLOT.to_owned(),
            sticky_key: member_id.to_owned(),
            member: MemberInfo {
                id: Some(member_id.to_owned()),
                membership: Some(Membership::Join),
            },
            application: ApplicationInfo::new("m.call"),
            transports: None,
            leave_reason: None,
            created_ts: None,
        },
    }
}

fn join_params() -> JoinSessionParams {
    JoinSessionParams::new(
        ALICE.to_owned(),
        "ALICEDEV".to_owned(),
        ROOM.to_owned(),
        SLOT.to_owned(),
        "m.call",
        RtcTransport::LiveKit(LiveKitTransport {
            livekit_service_url: "https://sfu.example.org".to_owned(),
        }),
    )
}

type Manager = CallSessionManager<MockCommandSender>;

async fn set_roster(manager: &mut Manager, events: Vec<RawStickyEvent>) {
    manager
        .rtc_mut()
        .set_current_sticky_state(ROOM, events)
        .await
        .expect("state applies");
}

/// Alice joined (her membership event is `$sticky-1`), Bob in the roster with
/// membership event `$bob-member-1`.
async fn joined_call(mut params: CallJoinParams) -> (Manager, Arc<MockCommandSender>, String) {
    let sender = Arc::new(MockCommandSender::new());
    let mut manager = CallSessionManager::with_command_sender(sender.clone());
    let own_member_id = params.rtc.membership_id();
    params.rtc.membership_id = Some(own_member_id.clone());
    manager.join(params).await.expect("join succeeds");
    assert_eq!(
        manager.rtc().own_membership_event_id(ROOM, SLOT).as_deref(),
        Some("$sticky-1")
    );
    set_roster(
        &mut manager,
        vec![
            member_event(ALICE, Some("ALICEDEV"), &own_member_id, "$sticky-1"),
            member_event(BOB, Some("BOBDEV"), BOB_MEMBER, "$bob-member-1"),
        ],
    )
    .await;
    assert_eq!(manager.rtc().member_count(ROOM, SLOT), Some(2));
    (manager, sender, own_member_id)
}

fn timeline_event(
    event_id: &str,
    sender: &str,
    event_type: &str,
    ts: u64,
    content: Value,
) -> RawTimelineEvent {
    RawTimelineEvent {
        room_id: ROOM.to_owned(),
        event_id: event_id.to_owned(),
        sender: sender.to_owned(),
        origin: EventOrigin::Unknown,
        event_type: event_type.to_owned(),
        origin_server_ts: ts,
        content,
    }
}

fn bob_reacts(event_id: &str, target: &str, emoji: &str, name: &str) -> RawTimelineEvent {
    timeline_event(
        event_id,
        BOB,
        REACTION_EVENT_TYPE,
        1_000,
        build_reaction_content(target, emoji, name),
    )
}

fn bob_raises(event_id: &str, target: &str, ts: u64) -> RawTimelineEvent {
    timeline_event(
        event_id,
        BOB,
        ANNOTATION_EVENT_TYPE,
        ts,
        build_raised_hand_content(target),
    )
}

fn hands(manager: &Manager) -> Vec<(&'static str, String)> {
    manager
        .raised_hands(ROOM, SLOT)
        .expect("the session exists")
        .into_iter()
        .map(|hand| {
            let who = if hand.sender == BOB { "bob" } else { "alice" };
            (who, hand.reaction_event_id)
        })
        .collect()
}

fn ingest(manager: &mut Manager, event: RawTimelineEvent) {
    manager.on_room_timeline_events(ROOM, &[event]);
}

// ---- Reactions and raised hands ----

#[tokio::test]
async fn a_peers_reaction_is_surfaced_with_its_sound() {
    let (mut manager, _, _) = joined_call(join_params().into()).await;
    let mut reactions = manager.subscribe_reactions(ROOM, SLOT).unwrap();

    ingest(
        &mut manager,
        bob_reacts("$r1", "$bob-member-1", "👏", "clapping"),
    );

    let received = reactions.try_recv().expect("one reaction");
    assert_eq!(received.member_id, BOB_MEMBER);
    assert_eq!(received.sender, BOB);
    assert_eq!(received.emoji, "👏");
    assert_eq!(received.name, "clapping");
    assert_eq!(received.sound, ReactionSound::Named("clap".to_owned()));
}

#[tokio::test]
async fn a_reaction_is_only_accepted_from_the_member_it_relates_to() {
    let (mut manager, _, _) = joined_call(join_params().into()).await;
    let mut reactions = manager.subscribe_reactions(ROOM, SLOT).unwrap();

    // Carol reacting "as" Bob.
    let mut forged = bob_reacts("$r1", "$bob-member-1", "👏", "clapping");
    forged.sender = "@carol:example.org".to_owned();
    ingest(&mut manager, forged);
    // Bob relating to an event that is nobody's membership.
    ingest(
        &mut manager,
        bob_reacts("$r2", "$not-a-membership", "👏", "clapping"),
    );
    // Bob raising a hand on Alice's membership.
    ingest(&mut manager, bob_raises("$h1", "$sticky-1", 5));

    assert_eq!(reactions.try_recv().unwrap_err(), TryRecvError::Empty);
    assert!(hands(&manager).is_empty());
}

#[tokio::test]
async fn a_repeat_inside_the_active_window_is_dropped() {
    let (mut manager, _, _) = joined_call(join_params().into()).await;
    let clock = TestClock::new(10_000);
    manager.set_clock(clock.clock());
    let mut reactions = manager.subscribe_reactions(ROOM, SLOT).unwrap();

    ingest(
        &mut manager,
        bob_reacts("$r1", "$bob-member-1", "👏", "clapping"),
    );
    clock.advance(1_000);
    ingest(
        &mut manager,
        bob_reacts("$r2", "$bob-member-1", "🎉", "party"),
    );
    assert_eq!(reactions.try_recv().unwrap().emoji, "👏");
    assert_eq!(reactions.try_recv().unwrap_err(), TryRecvError::Empty);

    clock.advance(2_000);
    ingest(
        &mut manager,
        bob_reacts("$r3", "$bob-member-1", "🎉", "party"),
    );
    assert_eq!(reactions.try_recv().unwrap().emoji, "🎉");
}

#[tokio::test]
async fn sending_relates_to_our_membership_and_honours_the_cooldown() {
    let (mut manager, sender, _) = joined_call(join_params().into()).await;
    let clock = TestClock::new(10_000);
    manager.set_clock(clock.clock());

    let event_id = manager
        .send_reaction(ROOM, SLOT, "🎉 and more", "party")
        .await
        .expect("first reaction goes out");
    assert_eq!(event_id, "$room-1");
    let sent = sender.room_events.lock().unwrap().clone();
    assert_eq!(
        sent,
        vec![(
            ROOM.to_owned(),
            REACTION_EVENT_TYPE.to_owned(),
            build_reaction_content("$sticky-1", "🎉", "party"),
        )]
    );

    clock.advance(1_000);
    match manager.send_reaction(ROOM, SLOT, "👏", "clapping").await {
        Err(ReactionError::Cooldown { remaining_ms }) => assert_eq!(remaining_ms, 2_000),
        other => panic!("expected a cooldown, got {other:?}"),
    }
    assert_eq!(sender.room_events.lock().unwrap().len(), 1);

    clock.advance(2_000);
    manager
        .send_reaction(ROOM, SLOT, "👏", "clapping")
        .await
        .expect("the cooldown has passed");
    assert_eq!(sender.room_events.lock().unwrap().len(), 2);

    assert!(matches!(
        manager.send_reaction(ROOM, SLOT, "   ", "nothing").await,
        Err(ReactionError::EmptyEmoji)
    ));
}

#[tokio::test]
async fn raising_is_idempotent_and_lowering_redacts_the_annotation() {
    let (mut manager, sender, _) = joined_call(join_params().into()).await;
    let mut watch = manager.subscribe_raised_hands(ROOM, SLOT).unwrap();

    manager.raise_hand(ROOM, SLOT).await.expect("raise");
    let sent = sender.room_events.lock().unwrap().clone();
    assert_eq!(
        sent,
        vec![(
            ROOM.to_owned(),
            ANNOTATION_EVENT_TYPE.to_owned(),
            build_raised_hand_content("$sticky-1"),
        )]
    );
    // Shown locally at once, before any echo.
    assert_eq!(hands(&manager), vec![("alice", "$room-1".to_owned())]);
    assert!(watch.has_changed().unwrap());
    assert_eq!(watch.borrow_and_update().len(), 1);

    manager.raise_hand(ROOM, SLOT).await.expect("raise again");
    assert_eq!(
        sender.room_events.lock().unwrap().len(),
        1,
        "nothing re-sent"
    );

    // The echo of our own annotation changes nothing.
    ingest(
        &mut manager,
        timeline_event(
            "$room-1",
            ALICE,
            ANNOTATION_EVENT_TYPE,
            2_000,
            build_raised_hand_content("$sticky-1"),
        ),
    );
    assert_eq!(hands(&manager), vec![("alice", "$room-1".to_owned())]);
    assert!(!watch.has_changed().unwrap());

    manager.lower_hand(ROOM, SLOT).await.expect("lower");
    assert_eq!(
        sender.redactions.lock().unwrap().clone(),
        vec![(ROOM.to_owned(), "$room-1".to_owned(), None)]
    );
    assert!(hands(&manager).is_empty());

    manager
        .lower_hand(ROOM, SLOT)
        .await
        .expect("lowering twice is fine");
    assert_eq!(sender.redactions.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_peers_hand_stays_across_their_refresh_and_goes_with_them() {
    let (mut manager, _, own_member_id) = joined_call(join_params().into()).await;

    // Before anything was fetched, Bob's membership event wants a lookup and
    // ours does not.
    assert_eq!(
        manager.pending_relation_lookups(ROOM),
        vec![RelationLookup {
            member_id: BOB_MEMBER.to_owned(),
            membership_event_id: "$bob-member-1".to_owned(),
        }]
    );
    manager.on_relations_received(ROOM, "$bob-member-1", &[]);
    assert!(manager.pending_relation_lookups(ROOM).is_empty());

    ingest(&mut manager, bob_raises("$h1", "$bob-member-1", 5_000));
    assert_eq!(hands(&manager), vec![("bob", "$h1".to_owned())]);

    // Bob's sticky refresh moves his membership event on.
    set_roster(
        &mut manager,
        vec![
            member_event(ALICE, Some("ALICEDEV"), &own_member_id, "$sticky-1"),
            member_event(BOB, Some("BOBDEV"), BOB_MEMBER, "$bob-member-2"),
        ],
    )
    .await;
    assert_eq!(
        hands(&manager),
        vec![("bob", "$h1".to_owned())],
        "a hand outlives a refresh"
    );
    assert_eq!(
        manager.pending_relation_lookups(ROOM),
        vec![RelationLookup {
            member_id: BOB_MEMBER.to_owned(),
            membership_event_id: "$bob-member-2".to_owned(),
        }],
        "the new event's annotations are looked up"
    );

    // A reaction relating to the previous event is still his.
    let mut reactions = manager.subscribe_reactions(ROOM, SLOT).unwrap();
    ingest(
        &mut manager,
        bob_reacts("$r1", "$bob-member-1", "🐶", "dog"),
    );
    assert_eq!(reactions.try_recv().unwrap().member_id, BOB_MEMBER);

    // Bob leaves, through the core directly: the listener drops his hand.
    let mut watch = manager.subscribe_raised_hands(ROOM, SLOT).unwrap();
    watch.borrow_and_update();
    set_roster(
        &mut manager,
        vec![member_event(
            ALICE,
            Some("ALICEDEV"),
            &own_member_id,
            "$sticky-1",
        )],
    )
    .await;
    assert!(
        watch.has_changed().unwrap(),
        "subscribers hear the hand drop"
    );
    assert!(watch.borrow().is_empty());
}

/// The pre-sticky Element Call dialect reuses `member_id` across joins.
#[tokio::test]
async fn a_hand_does_not_come_back_with_a_rejoin_under_the_same_member_id() {
    let (mut manager, _, own_member_id) = joined_call(join_params().into()).await;
    ingest(&mut manager, bob_raises("$h1", "$bob-member-1", 5_000));
    assert_eq!(hands(&manager).len(), 1);

    let alice = member_event(ALICE, Some("ALICEDEV"), &own_member_id, "$sticky-1");
    set_roster(&mut manager, vec![alice.clone()]).await;
    set_roster(
        &mut manager,
        vec![
            alice,
            member_event(BOB, Some("BOBDEV"), BOB_MEMBER, "$bob-member-9"),
        ],
    )
    .await;

    assert!(hands(&manager).is_empty());
}

#[tokio::test]
async fn backfill_restores_hands_but_never_replays_reactions() {
    let (mut manager, _, _) = joined_call(join_params().into()).await;
    let mut reactions = manager.subscribe_reactions(ROOM, SLOT).unwrap();

    manager.on_relations_received(
        ROOM,
        "$bob-member-1",
        &[
            bob_reacts("$old-reaction", "$bob-member-1", "👏", "clapping"),
            bob_raises("$old-hand", "$bob-member-1", 100),
        ],
    );

    assert_eq!(hands(&manager), vec![("bob", "$old-hand".to_owned())]);
    assert_eq!(reactions.try_recv().unwrap_err(), TryRecvError::Empty);
}

#[tokio::test]
async fn a_redaction_lowers_the_hand_it_raised() {
    let (mut manager, _, _) = joined_call(join_params().into()).await;
    ingest(&mut manager, bob_raises("$h1", "$bob-member-1", 5_000));
    assert_eq!(hands(&manager).len(), 1);

    manager.on_event_redacted(ROOM, "$something-else");
    assert_eq!(hands(&manager).len(), 1);

    manager.on_event_redacted(ROOM, "$h1");
    assert!(hands(&manager).is_empty());
}

#[tokio::test]
async fn hands_are_ordered_by_when_they_were_raised() {
    let (mut manager, _, _) = joined_call(join_params().into()).await;
    manager.raise_hand(ROOM, SLOT).await.expect("raise");
    // Bob's hand went up before ours, by the server's clock.
    ingest(&mut manager, bob_raises("$h1", "$bob-member-1", 1));

    let order: Vec<&str> = hands(&manager).iter().map(|(who, _)| *who).collect();
    assert_eq!(order, vec!["bob", "alice"]);
}

#[tokio::test]
async fn the_hand_follows_our_membership_event_across_a_refresh() {
    // A zero lifetime makes every heartbeat refresh the sticky membership.
    let params = JoinSessionParams {
        sticky_duration_ms: Some(0),
        ..join_params()
    };
    let (mut manager, sender, _) = joined_call(params.into()).await;
    manager.raise_hand(ROOM, SLOT).await.expect("raise");
    assert_eq!(
        manager
            .own_raised_hand(ROOM, SLOT)
            .unwrap()
            .annotated_membership_event_id,
        "$sticky-1"
    );

    assert!(manager.heartbeat(ROOM, SLOT).await);

    assert_eq!(
        manager.rtc().own_membership_event_id(ROOM, SLOT).as_deref(),
        Some("$sticky-2"),
        "the heartbeat refreshed the membership"
    );
    let sent = sender.room_events.lock().unwrap().clone();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[1].2, build_raised_hand_content("$sticky-2"));
    assert_eq!(
        sender.redactions.lock().unwrap().clone(),
        vec![(ROOM.to_owned(), "$room-1".to_owned(), None)],
        "the annotation on the old membership event is redacted"
    );
    assert_eq!(hands(&manager), vec![("alice", "$room-2".to_owned())]);
    let own = manager.own_raised_hand(ROOM, SLOT).unwrap();
    assert_eq!(own.annotated_membership_event_id, "$sticky-2");
    assert_eq!(own.reaction_event_id, "$room-2");

    // Lowering redacts the current annotation, not the superseded one.
    manager.lower_hand(ROOM, SLOT).await.expect("lower");
    assert_eq!(sender.redactions.lock().unwrap()[1].1, "$room-2");
}

#[tokio::test]
async fn leaving_lowers_our_hand_first() {
    let (mut manager, sender, _) = joined_call(join_params().into()).await;
    manager.raise_hand(ROOM, SLOT).await.expect("raise");
    let membership_sends_before = sender.sticky_events.lock().unwrap().len();

    manager
        .leave(ROOM.to_owned(), SLOT.to_owned(), LeaveSessionParams::new())
        .await
        .expect("leave");

    assert_eq!(
        sender.redactions.lock().unwrap().clone(),
        vec![(ROOM.to_owned(), "$room-1".to_owned(), None)]
    );
    assert!(
        sender.sticky_events.lock().unwrap().len() > membership_sends_before,
        "the leave membership went out too"
    );
    assert!(manager.own_raised_hand(ROOM, SLOT).is_none());
    assert!(matches!(
        manager.raise_hand(ROOM, SLOT).await,
        Err(ReactionError::NotJoined)
    ));
}

#[tokio::test]
async fn disabled_reactions_neither_send_nor_receive() {
    let params = CallJoinParams {
        reactions: Some(ReactionsConfig {
            enabled: false,
            ..ReactionsConfig::default()
        }),
        ..join_params().into()
    };
    let (mut manager, sender, _) = joined_call(params).await;
    let mut reactions = manager.subscribe_reactions(ROOM, SLOT).unwrap();

    assert!(matches!(
        manager.send_reaction(ROOM, SLOT, "👏", "clapping").await,
        Err(ReactionError::Disabled)
    ));
    assert!(matches!(
        manager.raise_hand(ROOM, SLOT).await,
        Err(ReactionError::Disabled)
    ));
    assert!(sender.room_events.lock().unwrap().is_empty());

    ingest(
        &mut manager,
        bob_reacts("$r1", "$bob-member-1", "👏", "clapping"),
    );
    ingest(&mut manager, bob_raises("$h1", "$bob-member-1", 5_000));
    assert_eq!(reactions.try_recv().unwrap_err(), TryRecvError::Empty);
    assert!(hands(&manager).is_empty());
    assert!(manager.pending_relation_lookups(ROOM).is_empty());
}

/// Two slots in one room: a reaction names no slot, so the manager offers it
/// to both sessions and only the one holding the member keeps it.
#[tokio::test]
async fn a_rooms_reactions_reach_the_session_holding_the_member() {
    let sender = Arc::new(MockCommandSender::new());
    let mut manager = CallSessionManager::with_command_sender(sender);
    let other_slot = "m.call#OTHER";

    manager
        .join(join_params().into())
        .await
        .expect("join slot A");
    manager
        .join(
            JoinSessionParams {
                slot_id: other_slot.to_owned(),
                ..join_params()
            }
            .into(),
        )
        .await
        .expect("join slot B");

    set_roster(
        &mut manager,
        vec![member_event(
            BOB,
            Some("BOBDEV"),
            BOB_MEMBER,
            "$bob-member-1",
        )],
    )
    .await;
    assert_eq!(manager.rtc().member_count(ROOM, SLOT), Some(1));
    assert_eq!(manager.rtc().member_count(ROOM, other_slot), Some(0));

    let mut slot_a = manager.subscribe_reactions(ROOM, SLOT).unwrap();
    let mut slot_b = manager.subscribe_reactions(ROOM, other_slot).unwrap();

    manager.on_room_timeline_events(
        ROOM,
        &[
            bob_reacts("$r1", "$bob-member-1", "👏", "clapping"),
            bob_raises("$h1", "$bob-member-1", 5_000),
        ],
    );

    assert_eq!(slot_a.try_recv().unwrap().member_id, BOB_MEMBER);
    assert_eq!(slot_b.try_recv().unwrap_err(), TryRecvError::Empty);
    assert_eq!(manager.raised_hands(ROOM, SLOT).unwrap().len(), 1);
    assert!(manager.raised_hands(ROOM, other_slot).unwrap().is_empty());
    assert_eq!(
        manager.pending_relation_lookups(ROOM),
        vec![RelationLookup {
            member_id: BOB_MEMBER.to_owned(),
            membership_event_id: "$bob-member-1".to_owned(),
        }]
    );

    manager.on_event_redacted(ROOM, "$h1");
    assert!(manager.raised_hands(ROOM, SLOT).unwrap().is_empty());

    assert!(matches!(
        manager.raise_hand(ROOM, "m.call#NOWHERE").await,
        Err(ReactionError::NoSession)
    ));
}

#[tokio::test]
async fn the_host_intake_asks_for_annotations_of_membership_events() {
    let (manager, _, _) = joined_call(join_params().into()).await;

    assert_eq!(
        manager.timeline_event_types(),
        vec![
            REACTION_EVENT_TYPE.to_owned(),
            ANNOTATION_EVENT_TYPE.to_owned()
        ]
    );
    assert_eq!(
        manager.pending_relations(ROOM),
        vec![RelationsRequest {
            event_id: "$bob-member-1".to_owned(),
            rel_type: "m.annotation".to_owned(),
            event_type: "m.reaction".to_owned(),
        }]
    );
}

// ---- MSC4075 ----

fn open_slot(encryption: bool) -> RawSlotEvent {
    let json = if encryption {
        r#"{ "status": "open",
             "application": { "type": "m.call" },
             "encryption": { "type": "m.per_member" } }"#
    } else {
        r#"{ "status": "open", "application": { "type": "m.call" } }"#
    };
    RawSlotEvent {
        room_id: ROOM.to_owned(),
        slot_id: SLOT.to_owned(),
        content: serde_json::from_str(json).expect("slot content must parse"),
    }
}

async fn encrypted_call_manager(sender: Arc<MockCommandSender>) -> Manager {
    let mut manager = CallSessionManager::with_command_sender(sender);
    manager
        .rtc_mut()
        .on_room_encryption_received(ROOM, true)
        .await;
    manager
        .rtc_mut()
        .on_room_slots_received(ROOM, vec![open_slot(true)])
        .await;
    manager
}

/// Joins as alice under `member_id`, asking for `notify`.
async fn join_and_notify(manager: &mut Manager, member_id: &str, notify: Option<NotifyConfig>) {
    let mut rtc = join_params();
    rtc.membership_id = Some(member_id.to_owned());
    let params = CallJoinParams {
        notify,
        ..rtc.into()
    };
    manager.join(params).await.expect("join should succeed");
}

/// Every sticky send of the notification type, as `(content, duration_ms)`.
fn notifications_sent(sender: &MockCommandSender) -> Vec<(Value, u64)> {
    sender
        .sticky_events
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, event_type, _, _)| event_type == NOTIFICATION_EVENT_TYPE)
        .map(|(_, _, content, duration_ms)| (content.clone(), *duration_ms))
        .collect()
}

/// Starting a call is what summons the room, and MSC4075 ties the
/// notification to the membership that justifies it — so the event id the
/// join's sticky send reported has to come back out as the relation target.
#[tokio::test]
async fn starting_a_call_notifies_the_room() {
    let sender = Arc::new(MockCommandSender::new());
    let mut manager = encrypted_call_manager(sender.clone()).await;

    let mut notify = NotifyConfig::ring();
    notify.intent = Some("video".to_owned());
    join_and_notify(&mut manager, "alice-a", Some(notify)).await;

    let sent = notifications_sent(&sender);
    assert_eq!(sent.len(), 1, "the call starter should notify exactly once");
    let (content, duration_ms) = &sent[0];

    let member_event_id = sender
        .sticky_events
        .lock()
        .unwrap()
        .iter()
        .position(|(_, event_type, _, _)| event_type == "m.rtc.member")
        .map(|index| format!("$sticky-{}", index + 1))
        .expect("the join sends a membership event");
    assert_eq!(
        content.pointer("/m.relates_to/event_id").unwrap(),
        &serde_json::json!(member_event_id),
        "the relation must name our own membership event"
    );
    assert_eq!(
        content.pointer("/m.relates_to/rel_type").unwrap(),
        "m.reference"
    );
    assert_eq!(content.pointer("/application/type").unwrap(), "m.call");
    assert_eq!(
        content.pointer("/application/notification_type").unwrap(),
        "ring"
    );
    assert_eq!(
        content.pointer("/application/m.call.intent").unwrap(),
        "video"
    );
    assert_eq!(
        content.pointer("/application/device_id").unwrap(),
        "ALICEDEV"
    );
    assert_eq!(
        *duration_ms,
        2 * DEFAULT_RING_LIFETIME_MS,
        "MSC4075: the sticky entry must outlive the ring so acknowledgements can extend it"
    );
}

/// "Is anyone already here?" must not be answered `yes` by our own membership.
///
/// The host feeds the room's whole sticky map, and once the homeserver has
/// echoed our membership back that map contains *us*. A count that includes it
/// concludes somebody else started the call and stays silent — so the caller
/// hits "call" and nobody's phone rings.
#[tokio::test]
async fn our_own_membership_does_not_count_as_somebody_else() {
    let sender = Arc::new(MockCommandSender::new());
    let mut manager = encrypted_call_manager(sender.clone()).await;

    // The echo of our own membership, under the very id we are about to join
    // with.
    set_roster(
        &mut manager,
        vec![member_event(ALICE, Some("ALICEDEV"), "alice-a", "$echo")],
    )
    .await;
    join_and_notify(&mut manager, "alice-a", Some(NotifyConfig::ring())).await;

    assert_eq!(
        notifications_sent(&sender).len(),
        1,
        "the only membership in the session is our own, so we are the one starting the call"
    );
}

/// A participation of ours from an earlier call in this process must not
/// count either — including when the host reported no sending device for it.
///
/// A session outlives `leave()` and keeps its candidates, so the previous
/// call's membership is still there on a rejoin. The core normally drops it
/// as a superseded participation of ours, but that rule needs the sending
/// device, and an unencrypted room supplies none. Left in, it silences every
/// subsequent call in the process until the app restarts.
#[tokio::test]
async fn a_stale_participation_of_ours_does_not_count_either() {
    let sender = Arc::new(MockCommandSender::new());
    let mut manager = CallSessionManager::with_command_sender(sender.clone());
    manager
        .rtc_mut()
        .on_room_slots_received(ROOM, vec![open_slot(false)])
        .await;

    // Our own device, one call ago, with no device reported — exactly what an
    // unencrypted room yields.
    set_roster(
        &mut manager,
        vec![member_event(ALICE, None, "alice-old", "$old")],
    )
    .await;

    join_and_notify(&mut manager, "alice-new", Some(NotifyConfig::ring())).await;

    assert_eq!(
        notifications_sent(&sender).len(),
        1,
        "the session held nothing but our own previous participation"
    );
}

/// The other edge of the same rule: our own user on a *different* device is an
/// ordinary peer, and one already in the call started it.
#[tokio::test]
async fn another_device_of_ours_already_in_the_call_counts() {
    let sender = Arc::new(MockCommandSender::new());
    let mut manager = encrypted_call_manager(sender.clone()).await;

    set_roster(
        &mut manager,
        vec![member_event(
            ALICE,
            Some("ALICELAPTOP"),
            "alice-laptop",
            "$laptop",
        )],
    )
    .await;
    join_and_notify(&mut manager, "alice-a", Some(NotifyConfig::ring())).await;

    assert!(
        notifications_sent(&sender).is_empty(),
        "our laptop was already in the call, so our phone is joining, not starting"
    );
}

/// Joining a call someone else started must not ring the room a second time,
/// even if the host asked for a notification.
#[tokio::test]
async fn joining_an_occupied_session_notifies_nobody() {
    let sender = Arc::new(MockCommandSender::new());
    let mut manager = encrypted_call_manager(sender.clone()).await;

    set_roster(
        &mut manager,
        vec![member_event(BOB, Some("BOBDEV"), "bob-a", "$bob")],
    )
    .await;
    join_and_notify(&mut manager, "alice-a", Some(NotifyConfig::ring())).await;

    assert!(
        notifications_sent(&sender).is_empty(),
        "bob was already in the session, so he started the call, not us"
    );
}

#[tokio::test]
async fn joining_quietly_notifies_nobody() {
    let sender = Arc::new(MockCommandSender::new());
    let mut manager = encrypted_call_manager(sender.clone()).await;

    join_and_notify(&mut manager, "alice-a", None).await;

    assert!(notifications_sent(&sender).is_empty());
}

#[tokio::test]
async fn a_call_layer_over_an_existing_manager_sees_its_rosters() {
    let sender = Arc::new(MockCommandSender::new());
    let mut rtc = matrix_rtc_core::RtcSessionManager::with_command_sender(sender);
    rtc.set_current_sticky_state(
        ROOM,
        vec![member_event(
            BOB,
            Some("BOBDEV"),
            BOB_MEMBER,
            "$bob-member-1",
        )],
    )
    .await
    .unwrap();

    let mut manager = CallSessionManager::new(rtc);
    ingest(&mut manager, bob_raises("$h1", "$bob-member-1", 5_000));

    assert_eq!(hands(&manager), vec![("bob", "$h1".to_owned())]);
}
