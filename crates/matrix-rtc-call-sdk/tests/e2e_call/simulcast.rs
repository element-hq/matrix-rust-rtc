// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Simulcast and dynacast, end to end: alice publishes, bob and carol each
//! ask for a size through the constraints API, and the test checks what each
//! of them receives and which layers alice still encodes.
//!
//! Two subscribers because dynacast is decided across all of them: the SFU
//! keeps every layer up to the highest one anyone wants. Only with two can the
//! test show each subscriber getting its own layer at once, and the top layer
//! pausing when the last subscriber wanting it steps down. Assertions stay on
//! outcomes — received frame heights and our own `send_stats` — not on how
//! LiveKit gets there.
//!
//! What a run establishes, one step at a time (each subscriber holds one
//! request per step; layers are alice's, smallest first):
//!
//! | Bob asks          | Carol asks        | Bob gets | Carol gets | Alice encodes            |
//! | ----------------- | ----------------- | -------- | ---------- | ------------------------ |
//! | 240x180           | 960x720           | 240x180  | 960x720    | 180, 360, 720            |
//! | 240x180           | 480x360           | 240x180  | 480x360    | 180, 360 (720 paused)    |
//! | 240x180           | 240x180           | 240x180  | 240x180    | 180 (360, 720 paused)    |
//! | 960x720           | hidden            | 960x720  | nothing    | 180, 360, 720            |
//! | hidden            | hidden            | nothing  | nothing    | nothing (all paused)     |
//! | `Quality::Low`    | `Quality::High`   | 240x180  | 960x720    | 180, 360, 720            |
//! | `Quality::Medium` | `Quality::Medium` | 480x360  | 480x360    | 180, 360 (720 paused)    |
//! | screen 640x360    | screen 1280x720   | 640x360  | 1280x720   | 360 @ 3 fps, 720         |
//! | screen 640x360    | screen 640x360    | 640x360  | 640x360    | 360 @ 3 fps (720 paused) |
//!
//! Dynacast pauses only the layers above the highest one anyone wants; the
//! layers below stay on so the SFU can step a congested subscriber down. The
//! test asserts the top requested layer and everything above it; the lower ones
//! are LiveKit's policy and only logged. Read `active` in `send_stats`, not the
//! frame rate: a paused layer keeps reporting its last rate for a while.
//!
//! ```sh
//! make backend-up
//! cargo test -p matrix-rtc-call-sdk --features matrix-sdk,testing \
//!     --test e2e_call -- --ignored --nocapture e2e_simulcast
//! ```

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use matrix_rtc_call_sdk::LiveKitCall;
use matrix_rtc_core::compat::MembershipFormat;
use matrix_rtc_transport::{
    Dimensions, FrameStream, LocalTrackHandle, MediaConstraints, MediaStreamKind, PublishOptions,
    QualityLimit, SendStats, VideoDetail, VideoFrame, VideoSourceConfig,
};

use super::{
    Config, PATTERN, Participant, create_encrypted_room, credentials_for, join_call,
    login_and_sync, publish_pattern, wait_for_joined_members, wait_for_key, wait_for_members,
    wait_for_remote_track, wait_for_room,
};

/// Three peers to set up, nine steps of up to [`STEP`] each, and teardown.
const DEADLINE: Duration = Duration::from_secs(480);

/// How long one step may take to settle: the SFU's bandwidth estimate ramps
/// up before it forwards a top layer, and dynacast delays a downgrade.
const STEP: Duration = Duration::from_secs(30);

/// Consecutive frames at the expected height before a step counts as settled,
/// so a frame decoded just before a layer switch cannot pass it.
const SETTLED_FRAMES: usize = 3;

/// A screen share at 720p: livekit gives it two layers, 640x360 at 3 fps and
/// the full size.
const SCREEN: VideoSourceConfig = VideoSourceConfig {
    width: 1280,
    height: 720,
};

#[test]
#[ignore = "requires the demo/backend docker stack (make backend-up)"]
fn e2e_simulcast_dynacast() {
    super::run_to_completion(DEADLINE, run);
}

/// What a subscriber asks of alice's stream in one step.
#[derive(Clone, Copy, Debug)]
enum Ask {
    Size(u32, u32),
    Quality(QualityLimit),
    /// Tile scrolled out of view: subscription kept, stream paused.
    Hidden,
}

impl Ask {
    fn constraints(self) -> MediaConstraints {
        match self {
            Ask::Size(width, height) => MediaConstraints {
                detail: VideoDetail::Dimensions(Dimensions { width, height }),
                ..Default::default()
            },
            Ask::Quality(limit) => MediaConstraints {
                detail: VideoDetail::Quality(limit),
                ..Default::default()
            },
            Ask::Hidden => MediaConstraints {
                visible: false,
                ..Default::default()
            },
        }
    }
}

/// One step in time: what bob and carol each ask, the frame height each must
/// then receive (`None`: no frames), and alice's highest active layer
/// (index into her layers, smallest first; `None`: every layer paused).
struct Step {
    bob: Ask,
    carol: Ask,
    bob_gets: Option<u32>,
    carol_gets: Option<u32>,
    top_active: Option<usize>,
}

const CAMERA_LAYERS: usize = 3;
const CAMERA_STEPS: &[Step] = &[
    // Each subscriber gets its own layer; carol keeps the top one alive.
    Step {
        bob: Ask::Size(240, 180),
        carol: Ask::Size(960, 720),
        bob_gets: Some(180),
        carol_gets: Some(720),
        top_active: Some(2),
    },
    // Nobody wants 720 any more: alice stops encoding it.
    Step {
        bob: Ask::Size(240, 180),
        carol: Ask::Size(480, 360),
        bob_gets: Some(180),
        carol_gets: Some(360),
        top_active: Some(1),
    },
    Step {
        bob: Ask::Size(240, 180),
        carol: Ask::Size(240, 180),
        bob_gets: Some(180),
        carol_gets: Some(180),
        top_active: Some(0),
    },
    // A paused subscriber does not hold a layer up, nor does it stop another.
    Step {
        bob: Ask::Size(960, 720),
        carol: Ask::Hidden,
        bob_gets: Some(720),
        carol_gets: None,
        top_active: Some(2),
    },
    // Nobody watches: nothing is encoded.
    Step {
        bob: Ask::Hidden,
        carol: Ask::Hidden,
        bob_gets: None,
        carol_gets: None,
        top_active: None,
    },
    // The quality hint, LiveKit's deprecated path, still selects layers.
    Step {
        bob: Ask::Quality(QualityLimit::Low),
        carol: Ask::Quality(QualityLimit::High),
        bob_gets: Some(180),
        carol_gets: Some(720),
        top_active: Some(2),
    },
    Step {
        bob: Ask::Quality(QualityLimit::Medium),
        carol: Ask::Quality(QualityLimit::Medium),
        bob_gets: Some(360),
        carol_gets: Some(360),
        top_active: Some(1),
    },
];

const SCREEN_LAYERS: usize = 2;
const SCREEN_STEPS: &[Step] = &[
    Step {
        bob: Ask::Size(640, 360),
        carol: Ask::Size(1280, 720),
        bob_gets: Some(360),
        carol_gets: Some(720),
        top_active: Some(1),
    },
    Step {
        bob: Ask::Size(640, 360),
        carol: Ask::Size(640, 360),
        bob_gets: Some(360),
        carol_gets: Some(360),
        top_active: Some(0),
    },
];

async fn run(cfg: Config) -> Result<(), Box<dyn Error>> {
    let compat = MembershipFormat::Current;
    let [alice_creds, bob_creds, carol_creds] =
        credentials_for(&cfg, ["alice", "bob", "carol"]).await?;

    let alice = login_and_sync(&cfg, &alice_creds).await?;
    let bob = login_and_sync(&cfg, &bob_creds).await?;
    let carol = login_and_sync(&cfg, &carol_creds).await?;
    let bob_id = bob.client.user_id().ok_or("bob has no user id")?.to_owned();
    let carol_id = carol
        .client
        .user_id()
        .ok_or("carol has no user id")?
        .to_owned();

    let room_id = create_encrypted_room(&alice.client, &[&bob_id, &carol_id]).await?;
    println!("[alice] created encrypted room {room_id}, invited bob and carol");
    let alice_room = wait_for_room(&alice.client, &room_id).await?;
    let bob_room = wait_for_room(&bob.client, &room_id).await?;
    let carol_room = wait_for_room(&carol.client, &room_id).await?;
    bob_room.join().await?;
    carol_room.join().await?;
    for (room, label) in [
        (&alice_room, "alice"),
        (&bob_room, "bob"),
        (&carol_room, "carol"),
    ] {
        wait_for_joined_members(room, 3, label).await?;
    }

    matrix_rtc_call_sdk::open_slot(
        &alice.client,
        room_id.as_str(),
        &cfg.slot_id,
        "m.call",
        Some(matrix_rtc_core::SlotEncryption {
            encryption_type: "m.per_member".to_owned(),
            extra: Default::default(),
        }),
    )
    .await?;

    let url = cfg.livekit_service_url.clone();
    let alice = join_call(&cfg, alice, alice_room, &alice_creds.user, &url, compat).await?;
    let bob = join_call(&cfg, bob, bob_room, &bob_creds.user, &url, compat).await?;
    let carol = join_call(&cfg, carol, carol_room, &carol_creds.user, &url, compat).await?;

    // Subscribers must hold alice's key before her frames decode at all, or
    // the first step would be measuring key latency.
    let mut ready = true;
    for (participant, label) in [(&alice, "alice"), (&bob, "bob"), (&carol, "carol")] {
        ready &= wait_for_members(&participant.call, 3, label).await;
    }
    for (participant, label) in [(&bob, "bob"), (&carol, "carol")] {
        ready &= wait_for_key(&participant.call, alice.call.local_identity(), label).await;
    }

    let (camera_ok, screen_ok) = if ready {
        let alice_member = alice.call.membership_id();

        println!(
            "[alice] publishing a {}x{} camera",
            PATTERN.width, PATTERN.height
        );
        let camera = publish_pattern(&alice.call, PublishOptions::camera(PATTERN)).await?;
        let camera_ok = verify_layers(
            "camera",
            &camera.track,
            CAMERA_LAYERS,
            [(&bob.call, "bob"), (&carol.call, "carol")],
            alice_member,
            MediaStreamKind::Camera,
            CAMERA_STEPS,
        )
        .await?;

        println!(
            "[alice] publishing a {}x{} screen share",
            SCREEN.width, SCREEN.height
        );
        let screen = publish_pattern(&alice.call, PublishOptions::screen_share(SCREEN)).await?;
        let screen_ok = verify_layers(
            "screen share",
            &screen.track,
            SCREEN_LAYERS,
            [(&bob.call, "bob"), (&carol.call, "carol")],
            alice_member,
            MediaStreamKind::ScreenShare,
            SCREEN_STEPS,
        )
        .await?;
        (camera_ok, screen_ok)
    } else {
        println!("WARNING: the call never settled; skipping the layer checks");
        (false, false)
    };

    let teardown_ok = leave_all([(alice, "alice"), (bob, "bob"), (carol, "carol")]).await;

    println!("\n=== RESULT ===");
    println!("call settled (members + keys):          {ready}");
    println!("camera simulcast + dynacast verified:   {camera_ok}");
    println!("screen share simulcast + dynacast:      {screen_ok}");
    println!("clean teardown:                         {teardown_ok}");
    if ready && camera_ok && screen_ok && teardown_ok {
        println!("SIMULCAST END-TO-END TEST PASSED");
        Ok(())
    } else {
        Err("simulcast end-to-end test failed (see WARNING lines above)".into())
    }
}

/// One subscriber's view of alice's stream.
struct Subscriber<'a> {
    call: &'a LiveKitCall,
    label: String,
    frames: FrameStream<'static, VideoFrame>,
}

impl Subscriber<'_> {
    /// Wait until `deadline` for the stream to settle on `height`
    /// ([`SETTLED_FRAMES`] in a row), or, for `None`, for frames to stop.
    async fn expect(&mut self, height: Option<u32>, deadline: tokio::time::Instant) -> bool {
        match height {
            Some(height) => self.expect_height(height, deadline).await,
            None => self.expect_silence(deadline).await,
        }
    }

    async fn expect_height(&mut self, height: u32, deadline: tokio::time::Instant) -> bool {
        let mut in_a_row = 0;
        let mut last = None;
        while in_a_row < SETTLED_FRAMES {
            match tokio::time::timeout_at(deadline, self.frames.next()).await {
                Ok(Some(frame)) => {
                    let size = (frame.buffer.width, frame.buffer.height);
                    in_a_row = if size.1 == height { in_a_row + 1 } else { 0 };
                    last = Some(size);
                }
                Ok(None) | Err(_) => {
                    println!(
                        "[{}] WARNING: wanted frames {height} tall, last frame was {last:?}",
                        self.label
                    );
                    return false;
                }
            }
        }
        let (width, height) = last.unwrap_or_default();
        println!("[{}] receives {width}x{height}", self.label);
        true
    }

    /// Frames still in flight are fine; paused means a 3 s window with none.
    async fn expect_silence(&mut self, deadline: tokio::time::Instant) -> bool {
        loop {
            match tokio::time::timeout(Duration::from_secs(3), self.frames.next()).await {
                Ok(Some(_)) if tokio::time::Instant::now() < deadline => {}
                Ok(Some(_)) => {
                    println!(
                        "[{}] WARNING: frames kept arriving while paused",
                        self.label
                    );
                    return false;
                }
                Ok(None) | Err(_) => {
                    println!("[{}] receives nothing", self.label);
                    return true;
                }
            }
        }
    }
}

/// Run `steps` against alice's `kind` stream, as seen by two subscribers.
async fn verify_layers(
    what: &str,
    track: &Arc<dyn LocalTrackHandle>,
    layer_count: usize,
    subscribers: [(&LiveKitCall, &str); 2],
    publisher: &str,
    kind: MediaStreamKind,
    steps: &[Step],
) -> Result<bool, Box<dyn Error>> {
    let deadline = tokio::time::Instant::now() + STEP;
    let mut opened = Vec::with_capacity(2);
    for (call, name) in subscribers {
        let label = format!("{name} {what}");
        let Some(remote) = wait_for_remote_track(call, publisher, kind, deadline).await else {
            println!("[{label}] WARNING: alice's stream never arrived");
            return Ok(false);
        };
        let frames = remote
            .video_frames()
            .ok_or("a video track has no frame stream")?;
        opened.push(Subscriber {
            call,
            label,
            frames,
        });
    }
    let [mut bob, mut carol]: [Subscriber; 2] = opened
        .try_into()
        .unwrap_or_else(|_| unreachable!("two subscribers"));

    let mut all_ok = true;
    for (number, step) in steps.iter().enumerate() {
        println!(
            "\n--- {what} step {}: bob {:?}, carol {:?}",
            number + 1,
            step.bob,
            step.carol
        );
        bob.call
            .set_constraints(publisher, kind, step.bob.constraints());
        carol
            .call
            .set_constraints(publisher, kind, step.carol.constraints());

        let deadline = tokio::time::Instant::now() + STEP;
        let (bob_ok, carol_ok, layers_ok) = tokio::join!(
            bob.expect(step.bob_gets, deadline),
            carol.expect(step.carol_gets, deadline),
            wait_for_layers(what, track, layer_count, step.top_active, deadline),
        );
        all_ok &= bob_ok && carol_ok && layers_ok;
    }

    // Leave the stream as a fresh subscriber would find it.
    for subscriber in [&bob, &carol] {
        subscriber
            .call
            .set_constraints(publisher, kind, MediaConstraints::default());
    }
    Ok(all_ok)
}

/// Poll alice's send statistics until she has `count` layers, the one at
/// `top_active` is encoded and every layer above it is paused (`None`: all
/// paused). Layers below are LiveKit's to keep — it holds them so the SFU can
/// step a congested subscriber down — so they are logged, not asserted.
async fn wait_for_layers(
    what: &str,
    track: &Arc<dyn LocalTrackHandle>,
    count: usize,
    top_active: Option<usize>,
    deadline: tokio::time::Instant,
) -> bool {
    let settled = |stats: &SendStats| {
        let layers = &stats.layers;
        if layers.len() != count {
            return false;
        }
        match top_active {
            Some(top) => {
                // Active and actually encoding: a layer reports 0x0 until
                // its first frame.
                layers[top].active
                    && layers[top].frame_height > 0
                    && layers[top + 1..].iter().all(|l| !l.active)
            }
            None => layers.iter().all(|l| !l.active),
        }
    };
    let mut last = None;
    loop {
        if let Some(stats) = track.send_stats().await {
            if settled(&stats) {
                println!("[alice {what}] sends {}", describe(&stats));
                return true;
            }
            last = Some(stats);
        }
        if tokio::time::Instant::now() >= deadline {
            let seen = last.as_ref().map_or("no stats".to_owned(), describe);
            println!(
                "[alice {what}] WARNING: wanted {count} layers with layer {top_active:?} \
                 the highest active, last saw {seen}"
            );
            return false;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn describe(stats: &SendStats) -> String {
    stats
        .layers
        .iter()
        .map(|l| {
            format!(
                "{} {}x{}@{:.0}fps {}{}",
                l.rid,
                l.frame_width,
                l.frame_height,
                l.frames_per_second,
                if l.active { "active" } else { "paused" },
                match l.quality_limitation {
                    matrix_rtc_transport::QualityLimitation::None => String::new(),
                    reason => format!(" (limited: {reason:?})"),
                }
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Leave every peer with a per-leave timeout; any failure fails the test but
/// does not stop the others from leaving.
async fn leave_all<const N: usize>(participants: [(Participant, &str); N]) -> bool {
    let mut ok = true;
    for (participant, label) in participants {
        match tokio::time::timeout(Duration::from_secs(30), participant.call.leave()).await {
            Ok(Ok(())) => println!("[{label}] left cleanly"),
            Ok(Err(error)) => {
                ok = false;
                eprintln!("[{label}] leave failed: {error}");
            }
            Err(_) => {
                ok = false;
                eprintln!("[{label}] WARNING: leave timed out after 30s");
            }
        }
    }
    ok
}
