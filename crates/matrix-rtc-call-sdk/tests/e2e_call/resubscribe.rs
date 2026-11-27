// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Does livekit-rust re-subscribe? The LiveKit transport maps `Off` to a
//! pause because it does not (`transport_impl.rs`, `apply_constraints`).
//! This drives a real unsubscribe and re-subscribe on bob's room, below the
//! engine, several times in one call, and judges each by RTP counters and a
//! decoded frame, not by frames alone (the receive path fabricates frames
//! whether RTP arrives or not).
//!
//! It fails on every livekit up to 0.12.1: the re-subscribed track is never
//! surfaced (see the comment in `apply_constraints` for why). So it runs only
//! when asked, after a livekit bump, and the day it passes `Off` can become a
//! real unsubscribe:
//!
//! ```sh
//! make backend-up
//! E2E_LIVEKIT_RESUBSCRIBE=1 cargo test -p matrix-rtc-call-sdk \
//!     --features matrix-sdk,testing --test e2e_call -- --ignored --nocapture \
//!     e2e_livekit_resubscribes
//! ```

use std::error::Error;
use std::time::Duration;

use futures_util::StreamExt;
use livekit::RoomEvent;
use livekit::prelude::{ParticipantIdentity, RemoteTrackPublication, TrackSource};
use matrix_rtc_call_sdk::LiveKitCall;
use matrix_rtc_transport::MediaStreamKind;
use tokio::sync::mpsc::UnboundedReceiver;

use super::resilience::{leave, two_in_a_call, verdict};
use super::{
    Config, Sfu, halves_mean_luma, publish_pattern_video, run_to_completion, wait_for_remote_track,
};

/// Unsubscribe/re-subscribe rounds in one call: a stranded stream may only
/// show on a later round.
const CYCLES: usize = 3;
const DEADLINE: Duration = Duration::from_secs(300);

#[test]
#[ignore = "requires the demo/backend docker stack (make backend-up)"]
fn e2e_livekit_resubscribes() {
    // Known to fail upstream (see the module doc): CI runs every ignored
    // test, so this one waits to be asked.
    if std::env::var_os("E2E_LIVEKIT_RESUBSCRIBE").is_none() {
        println!("skipped: set E2E_LIVEKIT_RESUBSCRIBE=1 to run");
        return;
    }
    run_to_completion(DEADLINE, resubscribe);
}

async fn resubscribe(cfg: Config) -> Result<(), Box<dyn Error>> {
    let (alice, mut bob) = two_in_a_call(&cfg, Sfu::First, Sfu::First, false).await?;
    let _video = publish_pattern_video(&alice.call).await?;
    let member = alice.call.membership_id().to_owned();
    let identity = ParticipantIdentity::from(alice.call.local_identity().to_owned());

    let first = wait_for_remote_track(
        &bob.call,
        &member,
        MediaStreamKind::Camera,
        tokio::time::Instant::now() + Duration::from_secs(60),
    )
    .await
    .is_some()
        && flowing(&bob.call, &member).await;
    let mut checks = vec![("camera received before any round".to_owned(), first)];

    for cycle in 1..=CYCLES {
        let Some(publication) = camera_publication(&bob.call, &identity) else {
            checks.push((format!("round {cycle}: alice's camera publication"), false));
            break;
        };
        let sid = publication.sid();

        publication.set_subscribed(false);
        let unsubscribed = next_event(bob.call.events(), |event| {
            matches!(event, RoomEvent::TrackUnsubscribed { publication, .. } if publication.sid() == sid)
        })
        .await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let gone = bob
            .call
            .remote_track(&member, MediaStreamKind::Camera)
            .is_none();

        publication.set_subscribed(true);
        let subscribed = next_event(bob.call.events(), |event| {
            matches!(event, RoomEvent::TrackSubscribed { publication, .. } if publication.sid() == sid)
        })
        .await;
        let back = wait_for_remote_track(
            &bob.call,
            &member,
            MediaStreamKind::Camera,
            tokio::time::Instant::now() + Duration::from_secs(10),
        )
        .await
        .is_some();
        let flows = back && flowing(&bob.call, &member).await;
        let pattern = back && pattern_seen(&bob.call, &member).await;
        println!(
            "[bob] round {cycle}: unsubscribed {unsubscribed}, stream gone {gone}, \
             re-subscribed {subscribed}, stream back {back}, RTP {flows}, pattern {pattern}"
        );
        checks.push((format!("round {cycle}: TrackUnsubscribed"), unsubscribed));
        checks.push((format!("round {cycle}: stream removed"), gone));
        checks.push((format!("round {cycle}: TrackSubscribed again"), subscribed));
        checks.push((format!("round {cycle}: RTP arriving again"), flows));
        checks.push((format!("round {cycle}: the pattern decodes again"), pattern));
    }

    leave([alice, bob]).await;
    let checks: Vec<(&str, bool)> = checks
        .iter()
        .map(|(what, ok)| (what.as_str(), *ok))
        .collect();
    verdict(&checks)
}

/// Alice's camera publication as bob's room sees it, subscribed or not.
fn camera_publication(
    call: &LiveKitCall,
    identity: &ParticipantIdentity,
) -> Option<RemoteTrackPublication> {
    call.session()
        .room()
        .remote_participants()
        .get(identity)?
        .track_publications()
        .into_values()
        .find(|publication| publication.source() == TrackSource::Camera)
}

/// Waits up to 10 s for an event matching `wanted`.
async fn next_event(
    events: &mut UnboundedReceiver<RoomEvent>,
    wanted: impl Fn(&RoomEvent) -> bool,
) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match tokio::time::timeout_at(deadline, events.recv()).await {
            Ok(Some(event)) if wanted(&event) => return true,
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => return false,
        }
    }
}

/// Whether RTP packets of `member`'s camera arrive over two seconds.
async fn flowing(call: &LiveKitCall, member: &str) -> bool {
    let Some(before) = call.receive_stats(member, MediaStreamKind::Camera).await else {
        return false;
    };
    tokio::time::sleep(Duration::from_secs(2)).await;
    call.receive_stats(member, MediaStreamKind::Camera)
        .await
        .is_some_and(|after| after.packets_received > before.packets_received)
}

/// Whether a decoded frame of `member`'s camera shows the half-bright,
/// half-dark pattern (fabricated frames are uniform).
async fn pattern_seen(call: &LiveKitCall, member: &str) -> bool {
    let Some(mut frames) = call
        .remote_track(member, MediaStreamKind::Camera)
        .and_then(|track| track.video_frames())
    else {
        return false;
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut seen = 0usize;
    while let Ok(Some(frame)) = tokio::time::timeout_at(deadline, frames.next()).await {
        seen += 1;
        if seen < 5 {
            continue;
        }
        let (left, right) = halves_mean_luma(&frame.buffer);
        return left > 140.0 && right < 90.0;
    }
    false
}
