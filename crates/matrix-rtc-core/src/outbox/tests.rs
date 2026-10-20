// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::Value;

use super::*;
use crate::host::backend::{BackendError, MatrixBackend, MockBackend, MockMethod, MockOutcome};

/// An outbox whose health timestamps are paused-clock milliseconds since the
/// test started.
fn outbox() -> Outbox {
    let origin = Instant::now();
    Outbox::with_clock(Arc::new(move || origin.elapsed().as_millis() as u64))
}

fn key(kind: SendKind) -> SlotKey {
    SlotKey::new("!room:example.org/m.call#", kind)
}

fn fail() -> MockOutcome {
    MockOutcome::Fail(BackendError::new("offline"))
}

fn fail_with(errcode: &str, status: u16) -> MockOutcome {
    MockOutcome::Fail(BackendError::with_matrix_error(
        Some(errcode.to_owned()),
        Some(status),
        errcode,
    ))
}

fn rate_limited(retry_after_ms: u64) -> MockOutcome {
    MockOutcome::Fail(
        BackendError::with_matrix_error(
            Some("M_LIMIT_EXCEEDED".to_owned()),
            Some(429),
            "slow down",
        )
        .with_retry_after_ms(Some(retry_after_ms)),
    )
}

fn secs(at: &[u64]) -> Vec<Duration> {
    at.iter().copied().map(Duration::from_secs).collect()
}

async fn sticky(
    outbox: &Outbox,
    mock: &Arc<MockBackend>,
    kind: SendKind,
    policy: SendPolicy,
) -> Result<String, SendFailure> {
    outbox
        .send(key(kind), policy, || {
            mock.send_sticky_event("!r".into(), "m.rtc.member".into(), Value::Null, 1000)
        })
        .await
}

async fn reaction(
    outbox: &Outbox,
    mock: &Arc<MockBackend>,
    policy: SendPolicy,
) -> Result<String, SendFailure> {
    outbox
        .send(key(SendKind::Reaction), policy, || {
            mock.send_room_event("!r".into(), "m.reaction".into(), Value::Null)
        })
        .await
}

/// R1: retried on backoff doubling from 1 s, capped at 30 s.
#[tokio::test(start_paused = true)]
async fn a_transient_failure_is_retried_with_capped_backoff() {
    let outbox = outbox();
    let mock = Arc::new(MockBackend::new());
    mock.script(MockMethod::StickyEvent, std::iter::repeat_n(fail(), 7));

    let sent = sticky(&outbox, &mock, SendKind::Membership, SendPolicy::new()).await;

    assert_eq!(sent.unwrap(), "$sticky-1");
    assert_eq!(
        mock.calls_to(MockMethod::StickyEvent),
        secs(&[0, 1, 3, 7, 15, 31, 61, 91]),
    );
}

/// R1: a newer send of the same kind takes the slot; the older one's retry
/// never fires.
#[tokio::test(start_paused = true)]
async fn a_newer_send_of_the_same_kind_supersedes_the_pending_one() {
    let outbox = outbox();
    let mock = Arc::new(MockBackend::new());
    mock.script(MockMethod::StickyEvent, [fail()]);

    let older = sticky(&outbox, &mock, SendKind::Membership, SendPolicy::new());
    let newer = async {
        executor::sleep(Duration::from_millis(500)).await;
        sticky(&outbox, &mock, SendKind::Membership, SendPolicy::new()).await
    };
    let (older, newer) = tokio::join!(older, newer);

    assert!(matches!(older, Err(SendFailure::Superseded)));
    assert_eq!(newer.unwrap(), "$sticky-1");
    executor::sleep(Duration::from_secs(60)).await;
    assert_eq!(
        mock.calls_to(MockMethod::StickyEvent),
        [Duration::ZERO, Duration::from_millis(500)],
    );
}

/// R1, R2: a newer send waits for the older one's attempt in flight; never
/// two attempts of one slot at once.
#[tokio::test(start_paused = true)]
async fn a_newer_send_waits_for_the_attempt_in_flight() {
    let outbox = outbox();
    let mock = Arc::new(MockBackend::new());
    mock.script(
        MockMethod::StickyEvent,
        [MockOutcome::After(
            Duration::from_secs(5),
            Box::new(MockOutcome::Ok),
        )],
    );

    let older = sticky(&outbox, &mock, SendKind::Membership, SendPolicy::new());
    let newer = async {
        executor::sleep(Duration::from_secs(1)).await;
        sticky(&outbox, &mock, SendKind::Membership, SendPolicy::new()).await
    };
    let (older, newer) = tokio::join!(older, newer);

    assert!(
        matches!(older, Err(SendFailure::Superseded)),
        "its outcome is ignored"
    );
    assert!(newer.is_ok());
    assert_eq!(mock.calls_to(MockMethod::StickyEvent), secs(&[0, 5]));
}

/// R1: a leave ends every pending send of its scope; other scopes go on.
#[tokio::test(start_paused = true)]
async fn a_leave_drains_every_slot() {
    let outbox = outbox();
    let mock = Arc::new(MockBackend::new());
    mock.script(MockMethod::StickyEvent, std::iter::repeat_n(fail(), 100));

    let restart = sticky(&outbox, &mock, SendKind::DelayedLeave, SendPolicy::new());
    let refresh = sticky(&outbox, &mock, SendKind::Membership, SendPolicy::new());
    let other_room = outbox.send(
        SlotKey::new("!other:example.org/m.call#", SendKind::Membership),
        SendPolicy::new(),
        || async { Ok::<_, CommandError>(()) },
    );
    let leave = async {
        executor::sleep(Duration::from_millis(1500)).await;
        outbox.cancel_scope("!room:example.org/m.call#");
    };
    let (restart, refresh, other_room, ()) = tokio::join!(restart, refresh, other_room, leave);

    assert!(matches!(restart, Err(SendFailure::Cancelled)));
    assert!(matches!(refresh, Err(SendFailure::Cancelled)));
    assert!(other_room.is_ok());
    let calls = mock.calls_to(MockMethod::StickyEvent).len();
    executor::sleep(Duration::from_secs(120)).await;
    assert_eq!(
        mock.calls_to(MockMethod::StickyEvent).len(),
        calls,
        "nothing after the leave"
    );
}

/// R1, R32: the recipient leaving cancels that member's key slot only.
#[tokio::test(start_paused = true)]
async fn cancelling_one_slot_leaves_the_others() {
    let outbox = outbox();
    let bob = key(SendKind::KeyDistribution {
        member_id: "bob".into(),
    });
    let carol = key(SendKind::KeyDistribution {
        member_id: "carol".into(),
    });
    let carol_attempts = AtomicUsize::new(0);

    let to_bob = outbox.send(bob.clone(), SendPolicy::new(), || async {
        Err::<(), _>(CommandError::from_message("offline"))
    });
    let to_carol = outbox.send(carol, SendPolicy::new(), || {
        let attempt = carol_attempts.fetch_add(1, Ordering::SeqCst);
        async move {
            if attempt < 3 {
                Err(CommandError::from_message("offline"))
            } else {
                Ok(())
            }
        }
    });
    let bob_leaves = async {
        executor::sleep(Duration::from_secs(2)).await;
        outbox.cancel(&bob);
    };
    let (to_bob, to_carol, ()) = tokio::join!(to_bob, to_carol, bob_leaves);

    assert!(matches!(to_bob, Err(SendFailure::Cancelled)));
    assert!(to_carol.is_ok());
}

/// R2: an attempt that has not answered in 30 s is a transient failure.
#[tokio::test(start_paused = true)]
async fn an_attempt_that_does_not_answer_in_30s_is_a_transient_failure() {
    let outbox = outbox();
    let mock = Arc::new(MockBackend::new());
    mock.script(MockMethod::StickyEvent, [MockOutcome::Hang]);
    let mut health = outbox.subscribe_health();

    let sent = sticky(&outbox, &mock, SendKind::Membership, SendPolicy::new());
    let observed = async {
        health.changed().await.unwrap();
        *health.borrow_and_update()
    };
    let (sent, observed) = tokio::join!(sent, observed);

    assert!(sent.is_ok());
    assert_eq!(mock.calls_to(MockMethod::StickyEvent), secs(&[0, 31]));
    assert_eq!(observed, SignallingHealth::Unreachable { since_ms: 30_000 });
    assert_eq!(outbox.health(), SignallingHealth::Reachable);
}

/// R2: an answer that arrives after its attempt timed out is never seen; the
/// retry decides.
#[tokio::test(start_paused = true)]
async fn a_late_answer_to_a_timed_out_attempt_is_ignored() {
    let outbox = outbox();
    let mock = Arc::new(MockBackend::new());
    mock.script(
        MockMethod::StickyEvent,
        [
            MockOutcome::After(Duration::from_secs(40), Box::new(MockOutcome::Ok)),
            MockOutcome::Ok,
        ],
    );

    let sent = sticky(&outbox, &mock, SendKind::Membership, SendPolicy::new()).await;
    executor::sleep(Duration::from_secs(60)).await;

    assert_eq!(sent.unwrap(), "$sticky-1", "the retry's event id");
    assert_eq!(mock.sticky_events.lock().unwrap().len(), 1);
}

/// R3: a refusal is one attempt and leaves health alone.
#[tokio::test(start_paused = true)]
async fn a_permanent_refusal_is_not_retried_and_does_not_touch_health() {
    let outbox = outbox();
    let mock = Arc::new(MockBackend::new());
    mock.script(MockMethod::RoomEvent, [fail_with("M_FORBIDDEN", 403)]);

    let sent = reaction(&outbox, &mock, SendPolicy::new()).await;

    assert!(matches!(sent, Err(SendFailure::Refused(_))));
    assert_eq!(mock.calls_to(MockMethod::RoomEvent).len(), 1);
    assert_eq!(outbox.health(), SignallingHealth::Reachable);
}

/// R3: the host's mark makes a code-less failure a refusal.
#[tokio::test(start_paused = true)]
async fn a_failure_the_host_marks_permanent_is_not_retried() {
    let outbox = outbox();
    let mock = Arc::new(MockBackend::new());
    mock.script(
        MockMethod::StickyEvent,
        [MockOutcome::Fail(
            BackendError::new("not in the room").permanent(),
        )],
    );

    let sent = sticky(&outbox, &mock, SendKind::Membership, SendPolicy::new()).await;

    assert!(matches!(sent, Err(SendFailure::Refused(_))));
    assert_eq!(mock.calls_to(MockMethod::StickyEvent).len(), 1);
    assert_eq!(outbox.health(), SignallingHealth::Reachable);
}

/// R4: a reaction is tried for its deadline, then dropped.
#[tokio::test(start_paused = true)]
async fn a_send_with_a_deadline_gives_up_at_it() {
    let outbox = outbox();
    let mock = Arc::new(MockBackend::new());
    mock.script(MockMethod::RoomEvent, std::iter::repeat_n(fail(), 100));

    let started = Instant::now();
    let sent = reaction(
        &outbox,
        &mock,
        SendPolicy::new().deadline(Duration::from_secs(5)),
    )
    .await;

    assert!(matches!(
        sent,
        Err(SendFailure::DeadlineElapsed { last: Some(_) })
    ));
    assert_eq!(started.elapsed(), Duration::from_secs(5));
    assert_eq!(mock.calls_to(MockMethod::RoomEvent), secs(&[0, 1, 3]));
}

/// R4: an attempt that timed out may have landed, so it is not retried.
#[tokio::test(start_paused = true)]
async fn a_timed_out_attempt_is_not_retried_when_the_policy_says_so() {
    let outbox = outbox();
    let mock = Arc::new(MockBackend::new());
    mock.script(MockMethod::RoomEvent, [MockOutcome::Hang]);

    let sent = reaction(
        &outbox,
        &mock,
        SendPolicy::new().never_retry_a_timed_out_attempt(),
    )
    .await;

    assert!(matches!(sent, Err(SendFailure::TimedOut)));
    executor::sleep(Duration::from_secs(120)).await;
    assert_eq!(mock.calls_to(MockMethod::RoomEvent).len(), 1);
}

/// R7: the server's retry-after replaces the backoff step.
#[tokio::test(start_paused = true)]
async fn a_rate_limit_waits_the_servers_retry_after() {
    let outbox = outbox();
    let mock = Arc::new(MockBackend::new());
    mock.script(MockMethod::StickyEvent, [rate_limited(7_000)]);

    let sent = sticky(&outbox, &mock, SendKind::Membership, SendPolicy::new()).await;

    assert!(sent.is_ok());
    assert_eq!(mock.calls_to(MockMethod::StickyEvent), secs(&[0, 7]));
    assert_eq!(
        outbox.health(),
        SignallingHealth::Reachable,
        "an affordable rate limit is not an outage"
    );
}

/// R7: a retry-after the send cannot afford reports signalling unreachable
/// until the send lands.
#[tokio::test(start_paused = true)]
async fn a_retry_after_past_the_lapse_reports_signalling_unreachable() {
    let outbox = outbox();
    let mock = Arc::new(MockBackend::new());
    mock.script(MockMethod::StickyEvent, [rate_limited(45_000)]);

    let sent = sticky(
        &outbox,
        &mock,
        SendKind::DelayedLeave,
        SendPolicy::new().tolerance(Duration::from_secs(30)),
    );
    let observed = async {
        executor::sleep(Duration::from_secs(1)).await;
        outbox.health()
    };
    let (sent, observed) = tokio::join!(sent, observed);

    assert!(sent.is_ok());
    assert_eq!(observed, SignallingHealth::Unreachable { since_ms: 0 });
    assert_eq!(mock.calls_to(MockMethod::StickyEvent), secs(&[0, 45]));
    assert_eq!(outbox.health(), SignallingHealth::Reachable);
}

/// R9, R10: unreachable from the first transient failure, reachable from the
/// first success, `since` stable in between.
#[tokio::test(start_paused = true)]
async fn health_flips_on_the_first_transient_failure_and_the_first_success() {
    let outbox = outbox();
    let mock = Arc::new(MockBackend::new());
    mock.script(MockMethod::StickyEvent, [fail(), fail()]);
    let mut health = outbox.subscribe_health();

    let sent = async {
        executor::sleep(Duration::from_secs(5)).await;
        sticky(&outbox, &mock, SendKind::Membership, SendPolicy::new()).await
    };
    let transitions = async {
        let mut seen = Vec::new();
        while seen.len() < 2 {
            health.changed().await.unwrap();
            seen.push(*health.borrow_and_update());
        }
        seen
    };
    let (sent, transitions) = tokio::join!(sent, transitions);

    assert!(sent.is_ok());
    assert_eq!(mock.calls_to(MockMethod::StickyEvent), secs(&[5, 6, 8]));
    assert_eq!(
        transitions,
        [
            SignallingHealth::Unreachable { since_ms: 5_000 },
            SignallingHealth::Reachable,
        ],
        "the second failure does not move `since`"
    );
}

/// R10: "online" retries every send in backoff at once.
#[tokio::test(start_paused = true)]
async fn online_retries_every_pending_slot_at_once() {
    let outbox = outbox();
    let mock = Arc::new(MockBackend::new());
    mock.script(
        MockMethod::StickyEvent,
        [
            rate_limited(7_000),
            rate_limited(15_000),
            rate_limited(30_000),
        ],
    );
    outbox.report_connectivity(false);

    let sends = async {
        tokio::join!(
            sticky(&outbox, &mock, SendKind::Membership, SendPolicy::new()),
            sticky(&outbox, &mock, SendKind::DelayedLeave, SendPolicy::new()),
            sticky(&outbox, &mock, SendKind::Notification, SendPolicy::new()),
        )
    };
    let online = async {
        executor::sleep(Duration::from_secs(2)).await;
        outbox.report_connectivity(true);
    };
    let ((a, b, c), ()) = tokio::join!(sends, online);

    assert!(a.is_ok() && b.is_ok() && c.is_ok());
    assert_eq!(
        mock.calls_to(MockMethod::StickyEvent),
        secs(&[0, 0, 0, 2, 2, 2])
    );
    assert_eq!(outbox.health(), SignallingHealth::Reachable);
}

/// R11: offline is unreachable at once, whatever the sends say.
#[tokio::test(start_paused = true)]
async fn offline_sets_signalling_unreachable_at_once() {
    let outbox = outbox();
    executor::sleep(Duration::from_secs(3)).await;

    outbox.report_connectivity(false);
    assert_eq!(
        outbox.health(),
        SignallingHealth::Unreachable { since_ms: 3_000 }
    );

    executor::sleep(Duration::from_secs(3)).await;
    outbox.report_connectivity(false);
    assert_eq!(
        outbox.health(),
        SignallingHealth::Unreachable { since_ms: 3_000 },
        "a repeated report is a no-op"
    );
}

/// R19: a quick offline/online flap still counts one recovery, though the
/// health watch shows no change.
#[tokio::test(start_paused = true)]
async fn a_quick_flap_still_counts_a_recovery() {
    let outbox = outbox();
    let mut health = outbox.subscribe_health();
    let mut recoveries = outbox.subscribe_recoveries();

    outbox.report_connectivity(false);
    outbox.report_connectivity(true);

    assert_eq!(*health.borrow_and_update(), SignallingHealth::Reachable);
    assert!(recoveries.has_changed().unwrap());
    assert_eq!(*recoveries.borrow_and_update(), 1);
}

/// R11: "online" while reachable is a no-op: it wakes nothing.
#[tokio::test(start_paused = true)]
async fn online_while_reachable_wakes_nothing() {
    let outbox = outbox();
    let mock = Arc::new(MockBackend::new());
    mock.script(MockMethod::StickyEvent, [rate_limited(10_000)]);

    let sent = sticky(&outbox, &mock, SendKind::Membership, SendPolicy::new());
    let online = async {
        executor::sleep(Duration::from_secs(2)).await;
        outbox.report_connectivity(true);
    };
    let (sent, ()) = tokio::join!(sent, online);

    assert!(sent.is_ok());
    assert_eq!(mock.calls_to(MockMethod::StickyEvent), secs(&[0, 10]));
}

/// R11: a host that never reports is driven by its sends alone.
#[tokio::test(start_paused = true)]
async fn a_host_that_never_reports_connectivity_is_driven_by_its_sends() {
    let outbox = outbox();
    let mock = Arc::new(MockBackend::new());
    mock.script(MockMethod::RoomEvent, [fail()]);

    let failing = reaction(&outbox, &mock, SendPolicy::new());
    let observed = async {
        executor::sleep(Duration::from_millis(500)).await;
        outbox.health()
    };
    let (sent, observed) = tokio::join!(failing, observed);

    assert!(sent.is_ok());
    assert_eq!(observed, SignallingHealth::Unreachable { since_ms: 0 });
    assert_eq!(outbox.health(), SignallingHealth::Reachable);
}

mod properties {
    //! Random sequences of outcomes, supersessions, cancellations and
    //! connectivity reports, against the invariants R1 and R10 state.

    use std::sync::Mutex as StdMutex;

    use proptest::prelude::*;

    use super::*;

    #[derive(Clone, Debug)]
    enum Outcome {
        Ok,
        Transient,
        Refused,
        RateLimited(u64),
        Hang,
    }

    #[derive(Clone, Debug)]
    enum Op {
        /// A send on slot `0..3`, scripted with these outcomes.
        Send(usize, Vec<Outcome>),
        Cancel(usize),
        Offline,
        Online,
        Wait(u64),
    }

    fn outcome() -> impl Strategy<Value = Outcome> {
        prop_oneof![
            Just(Outcome::Ok),
            Just(Outcome::Transient),
            Just(Outcome::Refused),
            (1u64..40).prop_map(Outcome::RateLimited),
            Just(Outcome::Hang),
        ]
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            (0usize..3, prop::collection::vec(outcome(), 0..6)).prop_map(|(s, o)| Op::Send(s, o)),
            (0usize..3).prop_map(Op::Cancel),
            Just(Op::Offline),
            Just(Op::Online),
            (0u64..60).prop_map(Op::Wait),
        ]
    }

    const KINDS: [SendKind; 3] = [
        SendKind::Membership,
        SendKind::DelayedLeave,
        SendKind::Notification,
    ];

    #[derive(Default)]
    struct Log {
        in_flight: [usize; 3],
        max_in_flight: usize,
        /// Attempts started on a slot after its cancellation, by a send
        /// registered before it.
        after_cancel: usize,
    }

    fn run(ops: Vec<Op>) -> Log {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        runtime.block_on(async move {
            let outbox = outbox();
            let log = Arc::new(StdMutex::new(Log::default()));
            // Per slot, the generation of the latest cancel, and per send its
            // registration order, so a send started before a cancel is caught.
            let cancels = Arc::new(StdMutex::new([0usize; 3]));
            let mut sends = Vec::new();
            let mut order = 0usize;

            for op in ops {
                match op {
                    Op::Send(slot, outcomes) => {
                        order += 1;
                        let registered = order;
                        let outbox = outbox.clone();
                        let log = log.clone();
                        let cancels = cancels.clone();
                        let outcomes = Arc::new(StdMutex::new(outcomes.into_iter()));
                        sends.push(tokio::spawn(async move {
                            let _ = outbox
                                .send(key(KINDS[slot].clone()), SendPolicy::new(), || {
                                    let log = log.clone();
                                    let cancels = cancels.clone();
                                    let next = outcomes.lock().unwrap().next();
                                    async move {
                                        {
                                            let mut log = log.lock().unwrap();
                                            if cancels.lock().unwrap()[slot] >= registered {
                                                log.after_cancel += 1;
                                            }
                                            log.in_flight[slot] += 1;
                                            log.max_in_flight =
                                                log.max_in_flight.max(log.in_flight[slot]);
                                        }
                                        struct Done(Arc<StdMutex<Log>>, usize);
                                        impl Drop for Done {
                                            fn drop(&mut self) {
                                                self.0.lock().unwrap().in_flight[self.1] -= 1;
                                            }
                                        }
                                        let _done = Done(log, slot);
                                        match next.unwrap_or(Outcome::Ok) {
                                            Outcome::Ok => Ok(()),
                                            Outcome::Transient => {
                                                Err(CommandError::from_message("offline"))
                                            }
                                            Outcome::Refused => {
                                                Err(BackendError::with_matrix_error(
                                                    Some("M_FORBIDDEN".into()),
                                                    Some(403),
                                                    "no",
                                                )
                                                .into())
                                            }
                                            Outcome::RateLimited(s) => {
                                                Err(BackendError::with_matrix_error(
                                                    Some("M_LIMIT_EXCEEDED".into()),
                                                    Some(429),
                                                    "slow",
                                                )
                                                .with_retry_after_ms(Some(s * 1000))
                                                .into())
                                            }
                                            Outcome::Hang => {
                                                std::future::pending::<Result<(), CommandError>>()
                                                    .await
                                            }
                                        }
                                    }
                                })
                                .await;
                        }));
                    }
                    Op::Cancel(slot) => {
                        cancels.lock().unwrap()[slot] = order;
                        outbox.cancel(&key(KINDS[slot].clone()));
                    }
                    Op::Offline => outbox.report_connectivity(false),
                    Op::Online => outbox.report_connectivity(true),
                    Op::Wait(s) => executor::sleep(Duration::from_secs(s)).await,
                }
                tokio::task::yield_now().await;
            }
            // Every send ends: each slot's last one succeeds once its script
            // runs out, the others are superseded or cancelled.
            for send in sends {
                send.await.unwrap();
            }
            Arc::try_unwrap(log).ok().unwrap().into_inner().unwrap()
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn never_two_attempts_of_one_slot_in_flight(ops in prop::collection::vec(op(), 1..25)) {
            prop_assert!(run(ops).max_in_flight <= 1);
        }

        #[test]
        fn a_cancelled_send_never_attempts_again(ops in prop::collection::vec(op(), 1..25)) {
            prop_assert_eq!(run(ops).after_cancel, 0);
        }
    }

    /// Health is unreachable exactly when the latest evidence is a transient
    /// failure or an offline report.
    #[tokio::test(start_paused = true)]
    async fn health_follows_the_latest_evidence() {
        let outbox = outbox();
        let attempt = |fails: bool| {
            let outbox = outbox.clone();
            async move {
                outbox
                    .send(
                        key(SendKind::Notification),
                        SendPolicy::new().deadline(Duration::from_millis(1)),
                        || async move {
                            if fails {
                                Err(CommandError::from_message("offline"))
                            } else {
                                Ok(())
                            }
                        },
                    )
                    .await
            }
        };
        let unreachable =
            |outbox: &Outbox| matches!(outbox.health(), SignallingHealth::Unreachable { .. });

        let _ = attempt(true).await;
        assert!(unreachable(&outbox));
        let _ = attempt(false).await;
        assert!(!unreachable(&outbox));
        outbox.report_connectivity(false);
        assert!(unreachable(&outbox));
        outbox.report_connectivity(true);
        assert!(!unreachable(&outbox));
        let _ = attempt(true).await;
        outbox.report_connectivity(true);
        assert!(!unreachable(&outbox));
    }
}
