// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Transient failures on a real stack (034, core-rs § Verification,
//! real-environment): the homeserver restarted under a call, and our own SFU
//! stopped — back in time, taken over by the peer's SFU, or gone with every
//! other one. These are what only a real stack shows: Synapse's restart time
//! and its firing of overdue delayed leaves, LiveKit's own reconnect, and
//! whether a second SFU accepts our token.
//!
//! They stop and start containers of the `demo/backend` stack, so they run one
//! at a time and put the stack back however they end:
//!
//! ```sh
//! make backend-up
//! cargo test -p matrix-rtc-call-sdk --features matrix-sdk,testing \
//!     --test e2e_call -- --ignored --nocapture --test-threads=1 e2e_resilience
//! ```

use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use matrix_rtc_call::{CallEnd, CallStatus, ConnectionLossReason};
use matrix_rtc_call_sdk::{CallEvent, ConnectionState, LiveKitCall, LiveKitCallOptions};
use matrix_rtc_core::compat::MembershipFormat;
use matrix_rtc_core::{LiveKitTransport, SignallingHealth, SlotEncryption};
use matrix_rtc_transport::{AudioFrame, AudioSourceConfig, PublishOptions};

use super::{
    Config, Participant, create_encrypted_room, credentials, login_and_sync, open_slot,
    record_peer_tone, run_to_completion, wait_for_joined_members, wait_for_members, wait_for_room,
};

/// Setup, a minute or two of outage, and teardown.
const DEADLINE: Duration = Duration::from_secs(420);

#[test]
#[ignore = "requires the demo/backend docker stack (make backend-up); restarts its containers"]
fn e2e_resilience_survives_a_homeserver_restart() {
    run_to_completion(DEADLINE, homeserver_restart);
}

#[test]
#[ignore = "requires the demo/backend docker stack (make backend-up); restarts its containers"]
fn e2e_resilience_reconnects_its_own_sfu() {
    run_to_completion(DEADLINE, own_sfu_back_in_time);
}

#[test]
#[ignore = "requires the demo/backend docker stack (make backend-up); restarts its containers"]
fn e2e_resilience_falls_back_to_the_peer_sfu() {
    run_to_completion(DEADLINE, own_sfu_fallback);
}

#[test]
#[ignore = "requires the demo/backend docker stack (make backend-up); restarts its containers"]
fn e2e_resilience_ends_when_every_sfu_is_down() {
    run_to_completion(DEADLINE, every_sfu_down);
}

/// `docker compose` on the demo stack, blocking.
fn compose(args: &[&str]) -> Result<(), Box<dyn Error + Send + Sync>> {
    let file = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../demo/backend/docker-compose.yml");
    println!("[stack] docker compose {}", args.join(" "));
    let status = std::process::Command::new("docker")
        .arg("compose")
        .arg("-f")
        .arg(&file)
        .args(args)
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("docker compose {args:?} exited with {status}").into())
    }
}

async fn compose_async(args: &'static [&'static str]) -> Result<(), Box<dyn Error>> {
    tokio::task::spawn_blocking(move || compose(args))
        .await?
        .map_err(|error| -> Box<dyn Error> { error.to_string().into() })
}

/// Starts `services` again when dropped, so a failing scenario leaves the
/// stack whole for the next one.
struct Restore(&'static [&'static str]);

/// Each SFU with the token service living in its network namespace
/// (`network_mode: service:…`): a token service left running across its
/// SFU's restart keeps the dead namespace and never answers again.
const LIVEKIT: &[&str] = &["livekit", "auth-service"];
const BOTH_LIVEKITS: &[&str] = &["livekit", "auth-service", "livekit2", "auth-service2"];

/// Stops `services` (SFUs with their token services), returning what starts
/// them again.
async fn stop(services: &'static [&'static str]) -> Result<Restore, Box<dyn Error>> {
    let restore = Restore(services);
    let mut args = vec!["stop"];
    args.extend(services);
    tokio::task::spawn_blocking(move || compose(&args))
        .await?
        .map_err(|error| -> Box<dyn Error> { error.to_string().into() })?;
    Ok(restore)
}

impl Drop for Restore {
    fn drop(&mut self) {
        let mut args = vec!["start"];
        args.extend(self.0);
        if let Err(error) = compose(&args) {
            eprintln!("[stack] WARNING: could not restart {:?}: {error}", self.0);
        }
    }
}

/// Publishes a microphone through the engine and feeds it a `freq_hz` sine
/// until the publication closes. Unlike a track published on the raw
/// room, the transport carries it onto every room our media moves to.
async fn publish_tone(
    call: &LiveKitCall,
    freq_hz: f64,
) -> Result<tokio::task::JoinHandle<()>, Box<dyn Error>> {
    let track = call.engine().publish(PublishOptions::microphone()).await?;
    let AudioSourceConfig {
        sample_rate,
        num_channels,
    } = AudioSourceConfig::default();
    let samples_per_channel = sample_rate / 100;
    Ok(tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(10));
        let mut n: u64 = 0;
        loop {
            ticker.tick().await;
            let mut data = Vec::with_capacity((samples_per_channel * num_channels) as usize);
            for _ in 0..samples_per_channel {
                let t = n as f64 / f64::from(sample_rate);
                let sample =
                    (0.5 * f64::from(i16::MAX) * (std::f64::consts::TAU * freq_hz * t).sin())
                        as i16;
                data.extend(std::iter::repeat_n(sample, num_channels as usize));
                n += 1;
            }
            let frame = AudioFrame {
                data,
                sample_rate,
                num_channels,
                samples_per_channel,
            };
            if track.capture_audio(frame).await.is_err() {
                break;
            }
        }
    }))
}

/// Alice and bob in an open slot of a fresh encrypted room, alice publishing
/// on `alice_url` (with the fallback when `alice_fallback`) and bob on
/// `bob_url`, each seeing the other.
async fn two_in_a_call(
    cfg: &Config,
    alice_url: &str,
    bob_url: &str,
    alice_fallback: bool,
) -> Result<(Participant, Participant), Box<dyn Error>> {
    let (alice_creds, bob_creds) = credentials(cfg).await?;
    let alice = login_and_sync(cfg, &alice_creds).await?;
    let bob = login_and_sync(cfg, &bob_creds).await?;
    let bob_id = bob.client.user_id().ok_or("bob has no user id")?.to_owned();
    let room_id = create_encrypted_room(&alice.client, &[&bob_id]).await?;
    let alice_room = wait_for_room(&alice.client, &room_id).await?;
    let bob_room = wait_for_room(&bob.client, &room_id).await?;
    bob_room.join().await?;
    wait_for_joined_members(&alice_room, 2, "alice").await?;
    wait_for_joined_members(&bob_room, 2, "bob").await?;
    open_slot(
        &alice.client,
        room_id.as_str(),
        &cfg.slot_id,
        "m.call",
        Some(SlotEncryption {
            encryption_type: "m.per_member".to_owned(),
            extra: Default::default(),
        }),
    )
    .await?;

    let join = |room: matrix_sdk::Room, url: String, fallback: bool| {
        let http = reqwest::Client::builder()
            .danger_accept_invalid_certs(cfg.insecure_tls)
            .build();
        let slot_id = cfg.slot_id.clone();
        async move {
            LiveKitCall::join(
                &room,
                LiveKitCallOptions {
                    slot_id,
                    livekit_transport: Some(LiveKitTransport {
                        livekit_service_url: url,
                    }),
                    http: Some(http?),
                    format: MembershipFormat::Current,
                    sfu_fallback: fallback,
                    ..LiveKitCallOptions::default()
                },
            )
            .await
            .map_err(|error| -> Box<dyn Error> { error.into() })
        }
    };
    let alice = Participant {
        call: join(alice_room, alice_url.to_owned(), alice_fallback).await?,
        _sync: alice.sync,
    };
    let bob = Participant {
        call: join(bob_room, bob_url.to_owned(), false).await?,
        _sync: bob.sync,
    };
    if !(wait_for_members(&alice.call, 2, "alice").await
        && wait_for_members(&bob.call, 2, "bob").await)
    {
        return Err("the two never saw each other".into());
    }
    Ok((alice, bob))
}

/// Whether `call`'s signalling health ever turns unreachable, from now.
fn watch_unreachable(call: &LiveKitCall) -> Arc<AtomicBool> {
    let seen = Arc::new(AtomicBool::new(false));
    let mut interruptions = call.engine().subscribe_interruptions();
    let flag = seen.clone();
    tokio::spawn(async move {
        loop {
            if matches!(
                interruptions.borrow_and_update().signalling,
                SignallingHealth::Unreachable { .. }
            ) {
                flag.store(true, Ordering::SeqCst);
            }
            if interruptions.changed().await.is_err() {
                return;
            }
        }
    });
    seen
}

/// Waits up to `within` for `done` to hold on `call`'s interruptions record.
async fn until_interruptions(
    call: &LiveKitCall,
    within: Duration,
    done: impl Fn(&matrix_rtc_call_sdk::Interruptions) -> bool,
) -> bool {
    let mut interruptions = call.engine().subscribe_interruptions();
    tokio::time::timeout(within, interruptions.wait_for(|record| done(record)))
        .await
        .is_ok_and(|result| result.is_ok())
}

fn ended(call: &LiveKitCall) -> bool {
    matches!(call.status(), CallStatus::Ended { .. })
}

async fn leave(participants: [Participant; 2]) {
    for participant in participants {
        let _ = tokio::time::timeout(Duration::from_secs(30), participant.call.leave()).await;
    }
}

fn verdict(checks: &[(&str, bool)]) -> Result<(), Box<dyn Error>> {
    println!("\n=== RESULT ===");
    for (what, ok) in checks {
        println!("{what:<56} {ok}");
    }
    if checks.iter().all(|(_, ok)| *ok) {
        Ok(())
    } else {
        Err("resilience scenario failed (see the table above)".into())
    }
}

/// Synapse down for 15 s under a call (R12, R19, R21), long enough for a
/// keep-alive beat to fail (a bare restart can fall between two, and no
/// failed send means nothing to report): media keeps flowing, both
/// clients report signalling unreachable and then reachable again, neither
/// call ends, the membership count is two throughout, and bob never sees
/// alice leave — the delayed leave Synapse may fire on its way back up is
/// absorbed by the hold.
async fn homeserver_restart(cfg: Config) -> Result<(), Box<dyn Error>> {
    let url = cfg.livekit_service_url.clone();
    let (alice, bob) = two_in_a_call(&cfg, &url, &url, false).await?;
    let _tone = publish_tone(&alice.call, 440.0).await?;
    let before = record_peer_tone(&bob.call, "bob", 440.0).await?;
    let alice_member = alice.call.membership_id().to_owned();

    let mut bob_events = bob.call.engine().subscribe_events();
    let alice_unreachable = watch_unreachable(&alice.call);
    let bob_unreachable = watch_unreachable(&bob.call);
    let _restore = Restore(&["synapse"]);
    compose_async(&["stop", "synapse"]).await?;
    let (during, _) = tokio::join!(
        record_peer_tone(&bob.call, "bob-during-restart", 440.0),
        tokio::time::sleep(Duration::from_secs(15)),
    );
    compose_async(&["start", "synapse"]).await?;

    let reachable = |record: &matrix_rtc_call_sdk::Interruptions| {
        record.signalling == SignallingHealth::Reachable && !record.own_membership_lapsed
    };
    let alice_back = until_interruptions(&alice.call, Duration::from_secs(120), reachable).await;
    let bob_back = until_interruptions(&bob.call, Duration::from_secs(120), reachable).await;
    // Past the hold, so a delayed leave fired at startup would have shown.
    tokio::time::sleep(Duration::from_secs(15)).await;
    let mut alice_left = false;
    while let Ok(event) = bob_events.try_recv() {
        alice_left |= matches!(
            event,
            CallEvent::ParticipantLeft { member_id, .. } if member_id == alice_member
        );
    }
    let after = record_peer_tone(&bob.call, "bob-after-restart", 440.0).await?;
    let checks = [
        ("tone alice->bob before the restart", before),
        ("tone alice->bob during the restart", during?),
        ("tone alice->bob after the restart", after),
        (
            "alice reported signalling unreachable",
            alice_unreachable.load(Ordering::SeqCst),
        ),
        (
            "bob reported signalling unreachable",
            bob_unreachable.load(Ordering::SeqCst),
        ),
        ("alice reachable again, membership live", alice_back),
        ("bob reachable again, membership live", bob_back),
        (
            "neither call ended",
            !ended(&alice.call) && !ended(&bob.call),
        ),
        (
            "two members on both sides",
            alice.call.participants().len() == 2 && bob.call.participants().len() == 2,
        ),
        ("bob never saw alice leave", !alice_left),
    ];
    leave([alice, bob]).await;
    verdict(&checks)
}

/// Our SFU stopped and started again inside the budget (R24): our media
/// reconnects behind the same handle with the same member id, and the call
/// never ends.
async fn own_sfu_back_in_time(cfg: Config) -> Result<(), Box<dyn Error>> {
    let url = cfg.livekit_service_url.clone();
    let (alice, bob) = two_in_a_call(&cfg, &url, &url, false).await?;
    let _tone = publish_tone(&alice.call, 440.0).await?;
    let before = record_peer_tone(&bob.call, "bob", 440.0).await?;
    let member = alice.call.membership_id().to_owned();

    let restore = stop(LIVEKIT).await?;
    let reconnecting = until_interruptions(&alice.call, Duration::from_secs(20), |record| {
        record.own_media == ConnectionState::Reconnecting
    })
    .await;
    tokio::time::sleep(Duration::from_secs(25)).await;
    let alive_at_40 = !ended(&alice.call);
    drop(restore);
    let back = until_interruptions(&alice.call, Duration::from_secs(45), |record| {
        record.own_media == ConnectionState::Connected
    })
    .await;
    let after = record_peer_tone(&bob.call, "bob-after-sfu", 440.0).await?;
    let checks = [
        ("tone alice->bob before the stop", before),
        ("our media reported reconnecting", reconnecting),
        ("the call still live 40 s into the outage", alive_at_40),
        ("our media connected again", back),
        ("tone alice->bob after the restart", after),
        ("same member id", alice.call.membership_id() == member),
        ("the call never ended", !ended(&alice.call)),
    ];
    leave([alice, bob]).await;
    verdict(&checks)
}

/// Our SFU stopped for good while bob's is up (R25): at ~10 s our media moves
/// to bob's SFU, whose token service accepts us, and bob hears us again
/// through it; the call outlives the budget.
async fn own_sfu_fallback(cfg: Config) -> Result<(), Box<dyn Error>> {
    let (alice, bob) = two_in_a_call(
        &cfg,
        &cfg.livekit_service_url,
        &cfg.livekit_service_url_2,
        true,
    )
    .await?;
    let _tone = publish_tone(&alice.call, 440.0).await?;
    let before = record_peer_tone(&bob.call, "bob", 440.0).await?;
    let member = alice.call.membership_id().to_owned();

    let _restore = stop(LIVEKIT).await?;
    let moved = until_interruptions(&alice.call, Duration::from_secs(40), |record| {
        record.own_media == ConnectionState::Connected
    })
    .await;
    let through_peer = record_peer_tone(&bob.call, "bob-through-sfu-2", 440.0).await?;
    tokio::time::sleep(Duration::from_secs(50)).await;
    let checks = [
        ("tone alice->bob before the stop", before),
        ("our media connected again, on bob's SFU", moved),
        ("tone alice->bob with our SFU stopped", through_peer),
        ("the call outlived the budget", !ended(&alice.call)),
        ("same member id", alice.call.membership_id() == member),
    ];
    leave([alice, bob]).await;
    verdict(&checks)
}

/// Both SFUs stopped (R24, R25, R26): no peer connection is up, so no
/// fallback; at the end of the budget our leave goes out and the call ends
/// with the budget as its reason.
async fn every_sfu_down(cfg: Config) -> Result<(), Box<dyn Error>> {
    let (alice, bob) = two_in_a_call(
        &cfg,
        &cfg.livekit_service_url,
        &cfg.livekit_service_url_2,
        true,
    )
    .await?;
    let mut status = alice.call.subscribe_status();

    // From the loss as our media reports it: LiveKit drops its connections
    // as its stop begins, seconds before `docker compose stop` returns.
    let mut interruptions = alice.call.engine().subscribe_interruptions();
    let (restore, started) = tokio::join!(stop(BOTH_LIVEKITS), async {
        let _ = interruptions
            .wait_for(|record| record.own_media != ConnectionState::Connected)
            .await;
        tokio::time::Instant::now()
    });
    let _restore = restore?;
    println!(
        "[alice] lost our media {:?} before the stop returned",
        started.elapsed()
    );
    tokio::time::sleep_until(started + Duration::from_secs(50)).await;
    let alive_at_50 = !ended(&alice.call);
    let end = tokio::time::timeout(
        Duration::from_secs(60),
        status.wait_for(|status| matches!(status, CallStatus::Ended { .. })),
    )
    .await
    .ok()
    .and_then(Result::ok)
    .map(|status| status.clone());
    let ended_after = started.elapsed();
    let budget_end = matches!(
        end,
        Some(CallStatus::Ended {
            reason: CallEnd::ConnectionClosed {
                reason: ConnectionLossReason::Budget,
                ..
            }
        })
    );
    println!("[alice] ended after {ended_after:?}: {end:?}");
    let checks = [
        ("the call still live 50 s into the outage", alive_at_50),
        ("ended with the budget as its reason", budget_end),
        (
            "ended within 75 s of the stop",
            ended_after <= Duration::from_secs(75),
        ),
    ];
    leave([alice, bob]).await;
    verdict(&checks)
}
