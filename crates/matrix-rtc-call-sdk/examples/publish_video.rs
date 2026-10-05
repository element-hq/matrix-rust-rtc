// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Join a call through the layered objects (client → room → call), attach
//! LiveKit media, publish a video file as the camera, and receive the other
//! participants' cameras until Ctrl-C.
//!
//! The README quotes [`run_call`]; the rest is login boilerplate. Frames
//! come from `ffmpeg`, which decodes anything it reads to raw I420 at the
//! file's own pace (`-re`), looping.
//!
//! ```sh
//! HOMESERVER_URL=https://synapse.m.localhost MX_USER=alice MX_PASSWORD=secret \
//! ROOM_ID='!room:synapse.m.localhost' VIDEO=clip.mp4 INSECURE_TLS=1 \
//! cargo run -p matrix-rtc-call-sdk --example publish_video --features matrix-sdk
//! ```
//!
//! Optional: `SLOT` (default `room`, i.e. `m.call#room`), `RECOVERY_KEY` — each
//! run is a fresh device, and media-key senders must be cross-signed
//! (MSC4153); the first run per user prints the key to pass on later runs.

use std::env;
use std::error::Error;
use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use matrix_rtc_call::{CallJoinOptions, RtcClient};
use matrix_rtc_call_sdk::{CallEvent, LiveKitAttachOptions, SdkMatrixBackend, attach_livekit};
use matrix_rtc_core::RoomOptions;
use matrix_rtc_transport::{
    Dimensions, I420Buffer, MediaConstraints, MediaStreamKind, PublishOptions, VideoDetail,
    VideoFrame, VideoRotation, VideoSourceConfig,
};
use matrix_sdk::encryption::EncryptionSettings;
use matrix_sdk::ruma::RoomId;
use matrix_sdk_ui::sync_service::SyncService;
use tokio::sync::broadcast::error::RecvError;

const WIDTH: u32 = 1280;
const HEIGHT: u32 = 720;

/// Join `slot` in `room_id`, publish `video` as our camera, and receive the
/// other participants' cameras until Ctrl-C or the call ends.
async fn run_call(
    sdk: matrix_sdk::Client,
    room_id: &str,
    slot: &str,
    video: &str,
) -> Result<(), Box<dyn Error>> {
    let client = RtcClient::new(Arc::new(SdkMatrixBackend::new(sdk)));
    let room = client.room(room_id, RoomOptions::default()).await?;
    room.seeded().await;

    let call = room.join_call(CallJoinOptions::new().slot(slot)).await?; // m.call#<slot>

    // Key exchange, the engine, and the connection to our own focus.
    let media = attach_livekit(
        &call,
        room.backend().clone(),
        LiveKitAttachOptions {
            format: Default::default(), // must match RoomOptions::format
            http: None,
            auto_subscribe: true,
            stability: Default::default(),
        },
    )
    .await?
    .media;

    let camera = media
        .engine
        .publish(PublishOptions::camera(VideoSourceConfig {
            width: WIDTH,
            height: HEIGHT,
        }))
        .await?;

    // `capture_video` is synchronous and latest-frame-wins: a plain thread does.
    let (frames, mut ffmpeg) = ffmpeg_frames(video)?;
    std::thread::spawn(move || {
        for frame in frames {
            // Errors once the publication is gone (left, slot closed, ...).
            if camera.capture_video(frame).is_err() {
                break;
            }
        }
    });

    // Receive: one task per remote camera, logging what arrives. Ctrl-C
    // leaves; the loop ends on `Ended`, which a leave, the slot or the room
    // closing all produce.
    let engine = media.engine;
    let mut events = engine.subscribe_events();
    let hang_up = tokio::signal::ctrl_c();
    tokio::pin!(hang_up);
    let mut left = false;
    loop {
        let event = tokio::select! {
            _ = &mut hang_up, if !left => {
                left = true;
                call.leave(Default::default()).await?;
                continue;
            }
            event = events.recv() => event,
        };
        match event {
            Ok(CallEvent::StreamStarted {
                member_id,
                kind: MediaStreamKind::Camera,
            }) => {
                let Some(mut frames) = engine
                    .remote_track(&member_id, MediaStreamKind::Camera)
                    .and_then(|track| track.video_frames())
                else {
                    continue;
                };
                // How the tile is rendered: the engine picks the simulcast
                // layer that fits, and pauses the stream while not visible.
                engine.set_constraints(
                    &member_id,
                    MediaStreamKind::Camera,
                    MediaConstraints {
                        detail: VideoDetail::Dimensions(Dimensions {
                            width: 320,
                            height: 180,
                        }),
                        ..Default::default()
                    },
                );

                println!("{member_id}: camera started");
                tokio::spawn(async move {
                    let mut received = 0u64;
                    while let Some(frame) = frames.next().await {
                        // A real host renders `frame.buffer`'s I420 planes here.
                        received += 1;
                        if received.is_multiple_of(100) {
                            let (w, h) = (frame.buffer.width, frame.buffer.height);
                            println!("{member_id}: {received} frames, now {w}x{h}");
                        }
                    }
                });
            }
            Ok(CallEvent::Ended { reason }) => {
                println!("call ended: {reason:?}");
                break;
            }
            Ok(_) | Err(RecvError::Lagged(_)) => {}
            Err(RecvError::Closed) => break,
        }
    }
    ffmpeg.kill()?;
    Ok(())
}

/// Decode `video` to I420 frames at its own pace (`-re`), looping, through an
/// `ffmpeg` child; kill the child to stop.
fn ffmpeg_frames(
    video: &str,
) -> Result<(impl Iterator<Item = VideoFrame> + Send + 'static, Child), Box<dyn Error>> {
    let mut ffmpeg = Command::new("ffmpeg")
        .args(["-v", "error", "-re", "-stream_loop", "-1", "-i", video])
        .args(["-an", "-vf", &format!("scale={WIDTH}:{HEIGHT}")])
        .args(["-pix_fmt", "yuv420p", "-f", "rawvideo", "-"])
        .stdout(Stdio::piped())
        .spawn()?;
    let mut raw = ffmpeg.stdout.take().expect("stdout is piped");
    let (luma, chroma) = ((WIDTH * HEIGHT) as usize, (WIDTH * HEIGHT / 4) as usize);
    let mut buf = vec![0u8; luma + 2 * chroma];
    let started = Instant::now();
    let frames = std::iter::from_fn(move || {
        raw.read_exact(&mut buf).ok()?;
        Some(VideoFrame {
            buffer: I420Buffer {
                width: WIDTH,
                height: HEIGHT,
                data_y: buf[..luma].to_vec(),
                stride_y: WIDTH,
                data_u: buf[luma..luma + chroma].to_vec(),
                stride_u: WIDTH / 2,
                data_v: buf[luma + chroma..].to_vec(),
                stride_v: WIDTH / 2,
            },
            rotation: VideoRotation::Deg0,
            timestamp_us: started.elapsed().as_micros() as i64,
        })
    });
    Ok((frames, ffmpeg))
}

fn required(name: &str) -> Result<String, Box<dyn Error>> {
    env::var(name).map_err(|_| format!("missing required env var {name}").into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Both rustls backends are in the tree; pick one before any TLS.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls aws-lc-rs crypto provider");
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let room_id = RoomId::parse(required("ROOM_ID")?)?;
    let video = required("VIDEO")?;
    let slot = env::var("SLOT").unwrap_or_else(|_| "room".to_owned());

    let mut builder = matrix_sdk::Client::builder()
        .homeserver_url(required("HOMESERVER_URL")?)
        .with_encryption_settings(EncryptionSettings {
            auto_enable_cross_signing: true,
            ..EncryptionSettings::default()
        });
    if env::var("INSECURE_TLS").is_ok() {
        builder = builder.disable_ssl_verification();
    }
    let sdk = builder.build().await?;
    sdk.matrix_auth()
        .login_username(&required("MX_USER")?, &required("MX_PASSWORD")?)
        .initial_device_display_name("matrix-rtc publish_video example")
        .send()
        .await?;
    sdk.encryption().wait_for_e2ee_initialization_tasks().await;
    let recovery = sdk.encryption().recovery();
    match env::var("RECOVERY_KEY") {
        Ok(key) => recovery.recover(key.trim()).await?,
        Err(_) => println!(
            "pass RECOVERY_KEY='{}' to future runs",
            recovery.enable().await?
        ),
    }

    // Sliding sync carries the sticky `m.rtc.member` events the room feeds on.
    let sync = SyncService::builder(sdk.clone()).build().await?;
    sync.start().await;
    for _ in 0..60 {
        if sdk.get_room(&room_id).is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    run_call(sdk, room_id.as_str(), &slot, &video).await?;
    sync.stop().await;
    Ok(())
}
