// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Scripted outages end to end (034, core-rs § Verification): a whole call
//! through the path a host takes, the homeserver or an SFU taken away and
//! given back on paused time, and what every layer did about it checked
//! together. The per-rule tests live with their modules; these check that
//! the rules compose.

mod harness;

use matrix_rtc_call::{CallEnd, CallStatus, ConnectionLossReason};
use matrix_rtc_call_sdk::{CallEvent, ConnectionState};
use matrix_rtc_core::SignallingHealth;
use matrix_rtc_core::testing::MockMethod;
use serde_json::json;

use harness::*;

fn left(events: &[CallEvent], member: &str) -> usize {
    events
        .iter()
        .filter(|event| {
            matches!(event, CallEvent::ParticipantLeft { member_id, .. } if member_id == member)
        })
        .count()
}

fn joined(events: &[CallEvent], member: &str) -> usize {
    events
        .iter()
        .filter(|event| {
            matches!(event, CallEvent::ParticipantJoined { member_id, .. } if member_id == member)
        })
        .count()
}

/// The homeserver away for 170 s: unreachable at the first failed restart,
/// our membership lapsed at 30 s (the delayed leave fired) with our
/// publications paused. On its return the delay has certainly fired, so it
/// is re-armed and our membership republished once, then the recovery's
/// probe finds the new delay alive; the catch-up set causes nothing more,
/// and the newcomer is announced and keyed.
#[tokio::test(start_paused = true)]
async fn homeserver_outage_round_trip() {
    let mut call = start(&[BOB, CAROL], false, false).await;
    call.publish().await;
    call.drain_events();
    let publishes_before = call.sticky_sends().len();

    call.homeserver_away();
    run_for(11).await;
    assert!(
        matches!(call.health(), SignallingHealth::Unreachable { .. }),
        "unreachable at the first failed restart"
    );
    assert_eq!(
        call.media.engine.interruptions().signalling,
        call.health(),
        "the record copies the client's health"
    );
    assert!(matches!(call.call.status(), CallStatus::Connected { .. }));
    assert_eq!(
        call.media.engine.interruptions().own_media,
        ConnectionState::Connected,
        "media is not signalling"
    );

    run_for(20).await;
    assert!(call.media.engine.interruptions().own_membership_lapsed);
    assert!(call.publications_muted(), "paused while lapsed");

    // Dave joins at the homeserver meanwhile; nothing reaches us.
    run_for(139).await;
    let calls_before = call.mock.calls().len();
    call.homeserver_back();
    call.client.report_connectivity(true);
    run_for(14).await;

    let after: Vec<MockMethod> = call.mock.calls()[calls_before..]
        .iter()
        .map(|call| call.method)
        .filter(|method| *method != MockMethod::ToDevice)
        .collect();
    assert_eq!(
        after[..3],
        [
            MockMethod::DelayedEvent,
            MockMethod::StickyEvent,
            MockMethod::RestartDelayedEvent,
        ],
        "re-arm, republish, then the probe finds the new delay: {after:?}"
    );
    assert!(
        !after[3..]
            .iter()
            .any(|method| matches!(method, MockMethod::DelayedEvent | MockMethod::StickyEvent)),
        "nothing re-armed or republished again: {after:?}"
    );
    assert_eq!(
        call.sticky_sends().len(),
        publishes_before + 1,
        "one republish"
    );
    let republished = call.sticky_sends().last().cloned().unwrap();
    assert_eq!(
        republished["member"],
        call.sticky_sends()[publishes_before - 1]["member"],
        "same member id"
    );

    // The catch-up set: everyone, dave included, and us under the new id.
    call.deliver(
        &[
            Entry::Joined(BOB),
            Entry::Joined(CAROL),
            Entry::Joined(DAVE),
        ],
        true,
    )
    .await;
    run_for(3).await;
    assert_eq!(
        call.sticky_sends().len(),
        publishes_before + 1,
        "no second publish"
    );
    let events = call.drain_events();
    assert_eq!(joined(&events, DAVE.member_id), 1, "dave announced once");
    assert!(
        !call
            .mock
            .to_device_messages_for(DAVE.user, DAVE.device)
            .is_empty(),
        "dave gets our key"
    );
    assert_eq!(call.health(), SignallingHealth::Reachable);
    let interruptions = call.media.engine.interruptions();
    assert!(!interruptions.own_membership_lapsed);
    assert_eq!(interruptions.signalling, SignallingHealth::Reachable);
    assert!(!call.publications_muted(), "publications resumed");
    assert!(call.call.is_live());
}

/// As above on the delayed-leave path: the catch-up set carries the leaves
/// the homeserver fired for bob and carol while it was away; bob returns
/// within the hold and is never reported gone, carol is, ten seconds later.
#[tokio::test(start_paused = true)]
async fn homeserver_outage_with_delayed_leave_flap() {
    let mut call = start(&[BOB, CAROL], false, false).await;
    call.homeserver_away();
    run_for(120).await;
    call.homeserver_back();
    run_for(10).await;
    call.drain_events();
    let keys_to_bob = call.mock.to_device_messages_for(BOB.user, BOB.device).len();

    call.deliver(
        &[
            Entry::Left(BOB, "delayed_leave"),
            Entry::Left(CAROL, "delayed_leave"),
        ],
        true,
    )
    .await;
    run_for(3).await;
    call.deliver(
        &[Entry::Joined(BOB), Entry::Left(CAROL, "delayed_leave")],
        true,
    )
    .await;
    run_for(6).await;
    let events = call.drain_events();
    assert_eq!(left(&events, BOB.member_id), 0, "bob's flap absorbed");
    assert_eq!(left(&events, CAROL.member_id), 0, "carol still held at 9 s");

    run_for(2).await;
    let events = call.drain_events();
    assert_eq!(left(&events, BOB.member_id), 0);
    assert_eq!(left(&events, CAROL.member_id), 1, "carol gone at 10 s");
    run_for(10).await;
    assert_eq!(
        call.mock.to_device_messages_for(BOB.user, BOB.device).len(),
        keys_to_bob + 1,
        "one rotation, for carol's departure"
    );
}

/// Our own SFU lost while bob's is up: at 10 s our media moves to bob's,
/// our membership is republished naming it under the same member id, bob is
/// never reported gone, and the call outlives the budget.
#[tokio::test(start_paused = true)]
async fn own_sfu_loss_with_fallback() {
    let bob = Peer {
        focus: SFU_B,
        ..BOB
    };
    let mut call = start(&[bob], false, true).await;
    call.drain_events();
    let member = call.sticky_sends().last().unwrap()["member"].clone();

    call.transport.close(SFU_A);
    run_for(9).await;
    assert!(call.transport.takeovers().is_empty());
    run_for(2).await;
    assert_eq!(call.transport.takeovers(), [SFU_B]);
    let republished = call.sticky_sends().last().cloned().unwrap();
    assert_eq!(
        republished["transports"]["published"],
        json!([{ "type": "livekit", "livekit_service_url": SFU_B }])
    );
    assert_eq!(republished["member"], member, "same member id");

    run_for(60).await;
    assert!(call.call.is_live());
    assert_eq!(
        call.media.engine.interruptions().own_media,
        ConnectionState::Connected
    );
    assert_eq!(left(&call.drain_events(), bob.member_id), 0);
}

/// Our own SFU and bob's down together: the network is gone, so no
/// fallback; bob's returning at 30 s is the evidence, and our media moves
/// there at once.
#[tokio::test(start_paused = true)]
async fn own_sfu_loss_during_a_network_loss() {
    let bob = Peer {
        focus: SFU_B,
        ..BOB
    };
    let call = start(&[bob], false, true).await;

    call.transport.close(SFU_A);
    call.transport.interrupt(SFU_B);
    run_for(29).await;
    assert!(call.transport.takeovers().is_empty(), "no fallback blind");
    call.transport.restore(SFU_B);
    run_for(1).await;
    assert_eq!(call.transport.takeovers(), [SFU_B]);
    assert!(call.call.is_live());
}

/// Our own SFU never comes back: at 60 s our leave is sent, then the call
/// ends with the budget as its reason, and nothing is sent after.
#[tokio::test(start_paused = true)]
async fn own_sfu_loss_to_the_budget() {
    let call = start(&[BOB], false, false).await;
    let mut ended = call.call.subscribe_ended();

    call.transport.close(SFU_A);
    run_for(59).await;
    assert!(call.call.is_live());
    run_for(2).await;
    assert!(ended.borrow_and_update().is_some(), "publishing stopped");
    let leave = call.sticky_sends().last().cloned().unwrap();
    assert_eq!(leave["member"]["membership"], "leave");
    assert!(matches!(
        call.call.status(),
        CallStatus::Ended {
            reason: CallEnd::ConnectionClosed {
                reason: ConnectionLossReason::Budget,
                ..
            }
        }
    ));
    let sends = call.mock.calls().len();
    run_for(30).await;
    assert_eq!(call.mock.calls().len(), sends, "nothing after the end");
}

/// Joining with our SFU down: the attach returns at once, our media is
/// reconnecting and the call connecting; the SFU up at 25 s connects it with
/// the member id unchanged. Never up: the call ends at 60 s from the request,
/// as never connected.
#[tokio::test(start_paused = true)]
async fn join_with_the_sfu_down() {
    let call = start(&[BOB], true, false).await;
    assert_eq!(call.call.status(), CallStatus::Connecting);
    assert_eq!(
        call.media.engine.interruptions().own_media,
        ConnectionState::Reconnecting
    );
    let member = call.sticky_sends().last().unwrap()["member"].clone();

    run_for(24).await;
    call.transport.restore(SFU_A);
    run_for(16).await;
    assert!(matches!(call.call.status(), CallStatus::Connected { .. }));
    assert_eq!(
        call.media.engine.interruptions().own_media,
        ConnectionState::Connected
    );
    assert_eq!(call.sticky_sends().last().unwrap()["member"], member);

    let never = start(&[BOB], true, false).await;
    run_for(59).await;
    assert!(never.call.is_live());
    run_for(2).await;
    assert!(matches!(
        never.call.status(),
        CallStatus::Ended {
            reason: CallEnd::ConnectionClosed {
                reason: ConnectionLossReason::NeverConnected,
                ..
            }
        }
    ));
}
