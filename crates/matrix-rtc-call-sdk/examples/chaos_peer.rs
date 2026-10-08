// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! One MatrixRTC participant for the chaos suite: a Rust client driven over
//! stdin, reporting what it observes as JSON lines on stdout.
//!
//! The suite (`chaos/`, see `chaos/README.md`) runs several of these, each in
//! its own container so faults — a network partition, `netem` loss, a
//! `SIGKILL` — can be applied to one participant at a time, while it restarts
//! and degrades the backend underneath them.
//!
//! Unlike `interop_peer`, this process never gives up on its own: a call that
//! drops is reported and the process keeps running, because whether the stack
//! recovers *without help* is the thing under test. There is no deadline; the
//! harness owns time.
//!
//! ## Rejoining
//!
//! When our own membership disappears from the call while the call has not
//! ended — our delayed leave fired while we were cut off from the homeserver —
//! this peer leaves and joins again, as an application would. Only then: a
//! call that *ended* (we left, the slot closed, our SFU connection is gone) is
//! not rejoined. The policy lives here, at the application level, for now; it
//! is meant to move into the call SDK (an attempts limit, and an ending that
//! says the connection was lost).
//!
//! ## Protocol
//!
//! Commands, one per line on **stdin**:
//!
//! | Command | Effect |
//! | ------- | ------ |
//! | `join` | join the call (waits for `EXPECT_ROOM_MEMBERS` room members first) |
//! | `leave` | leave the call cleanly |
//! | `snapshot` | emit the roster and the armed delayed leave |
//! | `quit` | leave if joined, then exit 0 |
//!
//! End of stdin is a `quit`, so a harness that kills this process must
//! `SIGKILL` it before closing stdin, or it leaves cleanly first.
//!
//! Events, one JSON object per line on **stdout** (`{"event": "...", ...}`):
//!
//! | Event | Meaning |
//! | ----- | ------- |
//! | `ready` | logged in, syncing, in the room (`room_id`, `user_id`, `device_id`) |
//! | `joined` | membership published, SFU connected (`membership_id`, `delayed_leave_id`) |
//! | `join_failed` | a join attempt failed (`message`); a rejoin is retried on a backoff |
//! | `status` | every `STATUS_INTERVAL_MS`: roster, member count, armed `delayed_leave_id`, sync state, `rejoins` |
//! | `rejoining` | our membership vanished while the call was live; leaving to join again |
//! | `sync_state` | the sync service changed state |
//! | `sfu` | an SFU connection transition (`reconnecting`, `reconnected`, `disconnected`) |
//! | `track_subscribed` | the SFU forwarded a remote audio track (`identity`) |
//! | `audio` | one window of a peer's decrypted audio (`identity`, `rms`, `tone_440`) |
//! | `call_ended` | the call ended without a `leave` (`reason`); the process stays up |
//! | `left`, `snapshot`, `error` | as named |
//!
//! Human-readable logs go to **stderr**, so stdout stays a clean stream.
//!
//! ## Environment
//!
//! | Variable | Default |
//! | -------- | ------- |
//! | `MX_USER` / `MX_PASSWORD` | *required* — the harness provisions users |
//! | `ROLE` | *required* — `host` creates the room (and opens the slot where the format has one); `guest` joins it |
//! | `INVITE` | host: comma-separated Matrix IDs to invite |
//! | `ROOM_ID` | guest: the room the host created |
//! | `HOMESERVER_URL` | `http://synapse:8008` |
//! | `LIVEKIT_SERVICE_URL` | unset — when set, the focus to publish on instead of the advertised one |
//! | `SLOT_ID` | `LiveKitCallOptions`' default |
//! | `COMPAT` | `state` (room state, what most of the wild speaks); `current` and `sticky` exist but are not exercised yet |
//! | `MEDIA` | `tone` (publish 440 Hz, meter peers' audio) or `none` (signalling only) |
//! | `EXPECT_ROOM_MEMBERS` | `2` |
//! | `STATUS_INTERVAL_MS` | `1000` |
//! | `HEARTBEAT_MS` / `STICKY_DURATION_MS` | the `LiveKitCallOptions` defaults |

use std::cell::RefCell;
use std::env;
use std::error::Error;
use std::rc::Rc;
use std::time::{Duration, Instant};

use livekit::{RoomEvent, track::RemoteTrack};
use matrix_sdk::encryption::EncryptionSettings;
use matrix_sdk::ruma::api::client::room::create_room::v3::Request as CreateRoomRequest;
use matrix_sdk::ruma::events::InitialStateEvent;
use matrix_sdk::ruma::events::room::history_visibility::{
    HistoryVisibility, RoomHistoryVisibilityEventContent,
};
use matrix_sdk::ruma::serde::Raw;
use matrix_sdk::ruma::{OwnedRoomId, OwnedUserId, RoomId};
use matrix_sdk::{Client, RoomMemberships};
use matrix_sdk_ui::sync_service::SyncService;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{broadcast, mpsc};

use matrix_rtc_call_sdk::{CallEvent, LiveKitCall, LiveKitCallOptions, open_slot};
use matrix_rtc_core::compat::MembershipFormat;
use matrix_rtc_core::{LiveKitTransport, SlotEncryption};
use matrix_rtc_livekit::media;

/// Length of one metered audio window. Short enough that a media gap of a few
/// seconds shows up as distinct silent windows.
const AUDIO_WINDOW: Duration = Duration::from_secs(1);

/// How long one wait for a call event lasts before the loop goes back round
/// to emit status.
const POLL: Duration = Duration::from_millis(200);

/// How long our own membership may be missing from a live call before we
/// rejoin. Bridges the moment a state event is replaced, so a refresh is not
/// mistaken for a loss.
const REJOIN_GRACE: Duration = Duration::from_secs(2);

/// First and longest wait between failed rejoin attempts (doubling).
const REJOIN_RETRY_MIN: Duration = Duration::from_secs(1);
const REJOIN_RETRY_MAX: Duration = Duration::from_secs(30);

/// One protocol line, flushed eagerly: the harness sequences faults on it.
fn emit(event: serde_json::Value) {
    use std::io::Write;
    // Not `println!`: a harness that stopped reading must not panic us.
    let mut stdout = std::io::stdout().lock();
    let _ = writeln!(stdout, "{event}");
    let _ = stdout.flush();
}

fn error_chain(error: &(dyn Error + 'static)) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        message.push_str(&format!(": {cause}"));
        source = cause.source();
    }
    message
}

fn main() -> Result<(), Box<dyn Error>> {
    // Both rustls crypto backends are in the tree; see `interop_peer`.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // A `LocalSet` for this tool's own `spawn_local` tasks (stdin, meters,
    // the sync-state watcher).
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let outcome = runtime.block_on(tokio::task::LocalSet::new().run_until(run()));
    if let Err(error) = &outcome {
        emit(serde_json::json!({ "event": "error", "message": error_chain(error.as_ref()) }));
    }
    outcome
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Host,
    Guest,
}

struct Config {
    homeserver: String,
    user: String,
    password: String,
    role: Role,
    invite: Vec<OwnedUserId>,
    room_id: Option<OwnedRoomId>,
    livekit_service_url: Option<String>,
    slot_id: String,
    format: MembershipFormat,
    publish_tone: bool,
    expect_room_members: usize,
    status_interval: Duration,
    keep_alive_interval_ms: Option<u64>,
    sticky_duration_ms: Option<u64>,
}

fn env_parse<T: std::str::FromStr>(name: &str) -> Result<Option<T>, Box<dyn Error>> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map(Some)
            .map_err(|_| format!("{name}={value:?} does not parse").into()),
        Err(_) => Ok(None),
    }
}

impl Config {
    fn from_env() -> Result<Self, Box<dyn Error>> {
        let role = match env::var("ROLE").as_deref() {
            Ok("host") => Role::Host,
            Ok("guest") => Role::Guest,
            other => return Err(format!("ROLE must be host or guest, got {other:?}").into()),
        };
        let invite = env::var("INVITE")
            .unwrap_or_default()
            .split(',')
            .filter(|id| !id.is_empty())
            .map(OwnedUserId::try_from)
            .collect::<Result<_, _>>()?;
        let room_id = env::var("ROOM_ID")
            .ok()
            .map(OwnedRoomId::try_from)
            .transpose()?;
        if role == Role::Guest && room_id.is_none() {
            return Err("ROOM_ID is required for ROLE=guest".into());
        }
        Ok(Config {
            homeserver: env::var("HOMESERVER_URL")
                .unwrap_or_else(|_| "http://synapse:8008".to_owned()),
            user: env::var("MX_USER").map_err(|_| "MX_USER is required")?,
            password: env::var("MX_PASSWORD").map_err(|_| "MX_PASSWORD is required")?,
            role,
            invite,
            room_id,
            livekit_service_url: env::var("LIVEKIT_SERVICE_URL").ok(),
            slot_id: env::var("SLOT_ID").unwrap_or_else(|_| LiveKitCallOptions::default().slot_id),
            format: match env::var("COMPAT").as_deref() {
                Err(_) | Ok("state") => MembershipFormat::RoomState,
                Ok("current") => MembershipFormat::Current,
                Ok("sticky") => MembershipFormat::Sticky2025,
                Ok(other) => return Err(format!("unknown COMPAT {other:?}").into()),
            },
            publish_tone: match env::var("MEDIA").as_deref() {
                Err(_) | Ok("tone") => true,
                Ok("none") => false,
                Ok(other) => return Err(format!("unknown MEDIA {other:?}").into()),
            },
            expect_room_members: env_parse("EXPECT_ROOM_MEMBERS")?.unwrap_or(2),
            status_interval: Duration::from_millis(
                env_parse("STATUS_INTERVAL_MS")?.unwrap_or(1000),
            ),
            keep_alive_interval_ms: env_parse("HEARTBEAT_MS")?,
            sticky_duration_ms: env_parse("STICKY_DURATION_MS")?,
        })
    }
}

/// What the peer is meant to be doing, as opposed to what it is doing: a
/// `join` command makes it want to be in the call until a `leave`, or until
/// the call ends by itself.
#[derive(Default)]
struct Intent {
    want_joined: bool,
    rejoins: u32,
    /// When the next (re)join attempt may run, after a failed one.
    retry_at: Option<Instant>,
    retry_wait: Duration,
}

impl Intent {
    fn joined(&mut self) {
        self.retry_at = None;
        self.retry_wait = REJOIN_RETRY_MIN;
    }

    fn failed(&mut self) {
        self.retry_wait = (self.retry_wait.max(REJOIN_RETRY_MIN / 2) * 2).min(REJOIN_RETRY_MAX);
        self.retry_at = Some(Instant::now() + self.retry_wait);
    }

    fn may_try(&self) -> bool {
        self.retry_at.is_none_or(|at| Instant::now() >= at)
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let cfg = Config::from_env()?;
    let http = reqwest::Client::new();

    // The sync service is bound for the whole run: dropping it stops the
    // membership/key traffic the call depends on.
    let (client, sync) = login_and_sync(&cfg).await?;
    let sync_state = watch_sync_state(&sync);

    let room = match cfg.role {
        Role::Host => {
            let room_id = create_encrypted_room(&client, &cfg.invite).await?;
            let room = wait_for_room(&client, &room_id).await?;
            // The room-state format has no slots; the others need one open,
            // and only the creator has the power level for `m.rtc.slot`.
            if cfg.format != MembershipFormat::RoomState {
                open_slot(
                    &client,
                    room_id.as_str(),
                    &cfg.slot_id,
                    "m.call",
                    Some(SlotEncryption {
                        encryption_type: "m.per_member".to_owned(),
                        extra: Default::default(),
                    }),
                )
                .await?;
            }
            room
        }
        Role::Guest => {
            let room_id = cfg.room_id.as_deref().expect("checked in Config");
            let room = wait_for_room(&client, room_id).await?;
            room.join().await?;
            room
        }
    };

    emit(serde_json::json!({
        "event": "ready",
        "room_id": room.room_id().as_str(),
        "user_id": client.user_id().map(|u| u.to_string()),
        "device_id": client.device_id().map(|d| d.to_string()),
    }));

    let mut commands = stdin_lines();
    let mut joined: Option<Joined> = None;
    let mut intent = Intent::default();
    let mut last_status = Instant::now() - cfg.status_interval;

    loop {
        if last_status.elapsed() >= cfg.status_interval {
            last_status = Instant::now();
            let sync = sync_state.borrow().clone();
            emit_status(joined.as_ref(), &sync, intent.rejoins).await;
        }
        if let Some(active) = joined.as_mut() {
            while let Ok(window) = active.audio_rx.try_recv() {
                emit(window);
            }
            if active.membership_lost() {
                emit(serde_json::json!({
                    "event": "rejoining",
                    "reason": "our membership left the call while it was live",
                }));
                if let Some(active) = joined.take()
                    && let Err(error) = active.leave().await
                {
                    // The membership is already gone; the SFU side may be too.
                    eprintln!(
                        "[peer] leave before rejoin: {}",
                        error_chain(error.as_ref())
                    );
                }
                intent.rejoins += 1;
            }
        }
        if joined.is_none() && intent.want_joined && intent.rejoins > 0 && intent.may_try() {
            match join_call(&cfg, &room, &http).await {
                Ok(active) => {
                    intent.joined();
                    joined = Some(active);
                }
                Err(error) => {
                    intent.failed();
                    emit(serde_json::json!({
                        "event": "join_failed",
                        "message": error_chain(error.as_ref()),
                    }));
                }
            }
        }

        // Handlers run after the select's futures are dropped, so they may use
        // `joined` freely even though the call branch borrows it mutably.
        tokio::select! {
            biased;
            command = commands.recv() => {
                let Some(command) = command else { break };
                match command.trim() {
                    "join" if joined.is_some() => eprintln!("[peer] already joined, ignoring"),
                    "join" => {
                        // Reported, not fatal: the process never gives up on
                        // its own.
                        if let Err(error) =
                            wait_for_joined_members(&room, cfg.expect_room_members).await
                        {
                            emit(serde_json::json!({
                                "event": "join_failed",
                                "message": error_chain(error.as_ref()),
                            }));
                            continue;
                        }
                        intent.want_joined = true;
                        match join_call(&cfg, &room, &http).await {
                            Ok(active) => {
                                intent.joined();
                                joined = Some(active);
                            }
                            // Reported, not fatal: joining while the backend is
                            // degraded is a scenario, and the harness may retry.
                            Err(error) => emit(serde_json::json!({
                                "event": "join_failed",
                                "message": error_chain(error.as_ref()),
                            })),
                        }
                    }
                    "leave" => {
                        intent.want_joined = false;
                        match joined.take() {
                            Some(active) => match active.leave().await {
                                Ok(()) => emit(serde_json::json!({ "event": "left" })),
                                Err(error) => emit(serde_json::json!({
                                    "event": "error",
                                    "message": format!("leave: {}", error_chain(error.as_ref())),
                                })),
                            },
                            None => eprintln!("[peer] not joined, ignoring leave"),
                        }
                    }
                    "snapshot" => {
                        let snapshot = match joined.as_ref() {
                            Some(active) => serde_json::json!({
                                "membership_id": active.call.membership_id(),
                                "delayed_leave_id": active.call.delayed_leave_id().await,
                                "members": roster(&active.call),
                            }),
                            None => serde_json::Value::Null,
                        };
                        emit(serde_json::json!({ "event": "snapshot", "snapshot": snapshot }));
                    }
                    "quit" => break,
                    other => eprintln!("[peer] unknown command {other:?}"),
                }
            }
            event = next_call_event(joined.as_mut()) => {
                match event {
                    None => {}
                    Some(Observed::AudioTrack(identity, audio)) => {
                        emit(serde_json::json!({
                            "event": "track_subscribed",
                            "kind": "audio",
                            "identity": identity,
                        }));
                        let active = joined.as_mut().expect("event came from a joined call");
                        active.meters.push(spawn_audio_meter(
                            identity,
                            audio,
                            active.audio_tx.clone(),
                        ));
                    }
                    Some(Observed::Sfu(state, reason)) => {
                        emit(serde_json::json!({ "event": "sfu", "state": state, "reason": reason }));
                    }
                    Some(Observed::Ended(reason)) => {
                        // Over without us leaving: not ours to undo. Keep the
                        // process (and its Matrix client) up, since what the stack
                        // does next, unprompted, is what the harness watches.
                        intent.want_joined = false;
                        if let Some(active) = joined.take() {
                            active.stop_meters();
                        }
                        emit(serde_json::json!({ "event": "call_ended", "reason": reason }));
                    }
                }
            }
        }
    }

    if let Some(active) = joined {
        match active.leave().await {
            Ok(()) => emit(serde_json::json!({ "event": "left" })),
            Err(error) => emit(serde_json::json!({
                "event": "error",
                "message": format!("leave: {}", error_chain(error.as_ref())),
            })),
        }
    }
    Ok(())
}

/// A live call plus what has to outlive the `join` call.
struct Joined {
    call: LiveKitCall,
    /// The unified call event stream, for the call ending; subscribed at join
    /// so nothing is missed.
    call_events: broadcast::Receiver<CallEvent>,
    _tone: Option<media::ToneHandle>,
    meters: Vec<tokio::task::JoinHandle<()>>,
    audio_tx: mpsc::UnboundedSender<serde_json::Value>,
    audio_rx: mpsc::UnboundedReceiver<serde_json::Value>,
    /// Whether our own membership has reached the roster yet: before it has,
    /// its absence is not a loss.
    seen_self: bool,
    missing_since: Option<Instant>,
}

impl Joined {
    /// Whether our own membership has been missing from this live call for
    /// longer than [`REJOIN_GRACE`], after having been there.
    ///
    /// The roster has an entry for us only while our membership is in the
    /// session, so its absence is the membership's absence.
    fn membership_lost(&mut self) -> bool {
        if self.call.participants().iter().any(|p| p.is_local) {
            self.seen_self = true;
            self.missing_since = None;
            return false;
        }
        if !self.seen_self {
            return false;
        }
        self.missing_since
            .get_or_insert_with(Instant::now)
            .elapsed()
            >= REJOIN_GRACE
    }

    fn stop_meters(&self) {
        for meter in &self.meters {
            meter.abort();
        }
    }

    async fn leave(self) -> Result<(), Box<dyn Error>> {
        self.stop_meters();
        self.call.leave().await?;
        Ok(())
    }
}

/// What a joined call reported, as far as this peer cares.
enum Observed {
    AudioTrack(String, livekit::track::RemoteAudioTrack),
    /// An SFU connection transition, with the reason where there is one.
    Sfu(&'static str, Option<String>),
    /// The call ended without us leaving.
    Ended(String),
}

/// The next event of a joined call worth reporting, or `None` after [`POLL`]
/// (or at once when not joined, after the same pause) so the caller can emit
/// status.
async fn next_call_event(joined: Option<&mut Joined>) -> Option<Observed> {
    let Some(active) = joined else {
        tokio::time::sleep(POLL).await;
        return None;
    };
    let Joined {
        call, call_events, ..
    } = active;
    tokio::select! {
        biased;
        event = call_events.recv() => match event {
            Ok(CallEvent::Ended { reason }) => Some(Observed::Ended(format!("{reason:?}"))),
            Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => None,
            Err(broadcast::error::RecvError::Closed) => {
                Some(Observed::Ended("call event stream closed".to_owned()))
            }
        },
        event = tokio::time::timeout(POLL, call.events().recv()) => match event {
            Err(_) | Ok(None) => None,
            Ok(Some(RoomEvent::TrackSubscribed {
                track: RemoteTrack::Audio(audio),
                participant,
                ..
            })) => Some(Observed::AudioTrack(participant.identity().to_string(), audio)),
            Ok(Some(RoomEvent::Reconnecting)) => Some(Observed::Sfu("reconnecting", None)),
            Ok(Some(RoomEvent::Reconnected)) => Some(Observed::Sfu("reconnected", None)),
            Ok(Some(RoomEvent::Disconnected { reason })) => {
                Some(Observed::Sfu("disconnected", Some(format!("{reason:?}"))))
            }
            Ok(Some(_)) => None,
        },
    }
}

fn roster(call: &LiveKitCall) -> Vec<serde_json::Value> {
    call.participants()
        .into_iter()
        .map(|p| {
            serde_json::json!({
                "user_id": p.user_id,
                "member_id": p.member_id,
                "is_local": p.is_local,
            })
        })
        .collect()
}

async fn emit_status(joined: Option<&Joined>, sync_state: &str, rejoins: u32) {
    let Some(active) = joined else {
        emit(serde_json::json!({
            "event": "status",
            "joined": false,
            "sync": sync_state,
            "rejoins": rejoins,
        }));
        return;
    };
    emit(serde_json::json!({
        "event": "status",
        "joined": true,
        "sync": sync_state,
        "member_count": active.call.member_count().await,
        "members": roster(&active.call),
        "membership_id": active.call.membership_id(),
        "delayed_leave_id": active.call.delayed_leave_id().await,
        "rejoins": rejoins,
    }));
}

/// Meter a peer's audio in back-to-back windows until aborted. A silent window
/// is reported, not skipped: a gap in media is exactly what the suite measures.
fn spawn_audio_meter(
    identity: String,
    audio: livekit::track::RemoteAudioTrack,
    tx: mpsc::UnboundedSender<serde_json::Value>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_local(async move {
        loop {
            let pcm = media::record_track(&audio, AUDIO_WINDOW).await;
            let window = serde_json::json!({
                "event": "audio",
                "identity": identity,
                "rms": rms(&pcm),
                "samples": pcm.len(),
                "tone_440": media::detect_tone(&pcm, media::SAMPLE_RATE, 440.0),
            });
            if tx.send(window).is_err() {
                break;
            }
        }
    })
}

async fn join_call(
    cfg: &Config,
    room: &matrix_sdk::Room,
    http: &reqwest::Client,
) -> Result<Joined, Box<dyn Error>> {
    let call = LiveKitCall::join(
        room,
        LiveKitCallOptions {
            slot_id: cfg.slot_id.clone(),
            livekit_transport: cfg.livekit_service_url.clone().map(|livekit_service_url| {
                LiveKitTransport {
                    livekit_service_url,
                }
            }),
            http: Some(http.clone()),
            format: cfg.format,
            keep_alive_interval_ms: cfg.keep_alive_interval_ms,
            sticky_duration_ms: cfg.sticky_duration_ms,
            ..LiveKitCallOptions::default()
        },
    )
    .await?;
    let call_events = call.subscribe_call_events();
    let tone = if cfg.publish_tone {
        Some(media::publish_tone(call.session(), 440.0).await?)
    } else {
        None
    };
    emit(serde_json::json!({
        "event": "joined",
        "identity": call.local_identity(),
        "membership_id": call.membership_id(),
        "delayed_leave_id": call.delayed_leave_id().await,
    }));
    let (audio_tx, audio_rx) = mpsc::unbounded_channel();
    Ok(Joined {
        call,
        call_events,
        _tone: tone,
        meters: Vec::new(),
        audio_tx,
        audio_rx,
        seen_self: false,
        missing_since: None,
    })
}

/// Log in and start syncing. The account is one device old, so it self-signs
/// and MSC4153's cross-signed-sender requirement holds; see `e2e_call`.
async fn login_and_sync(cfg: &Config) -> Result<(Client, SyncService), Box<dyn Error>> {
    let client = Client::builder()
        .homeserver_url(&cfg.homeserver)
        .with_encryption_settings(EncryptionSettings {
            auto_enable_cross_signing: true,
            ..EncryptionSettings::default()
        })
        .build()
        .await?;
    client
        .matrix_auth()
        .login_username(&cfg.user, &cfg.password)
        .initial_device_display_name("matrix-rtc chaos peer")
        .send()
        .await?;
    client
        .encryption()
        .wait_for_e2ee_initialization_tasks()
        .await;
    eprintln!(
        "[peer] logged in as {:?} (device {:?})",
        client.user_id(),
        client.device_id()
    );

    // Offline mode, as a real app runs it: on a failed sync the service polls
    // `/versions` and restarts itself once the homeserver answers. Without it
    // the service parks in `Error` after the first outage and never recovers,
    // and the suite would be measuring a setup no product ships.
    let sync = SyncService::builder(client.clone())
        .with_offline_mode()
        .build()
        .await?;
    sync.start().await;
    Ok((client, sync))
}

/// Mirror the sync service's state into a cell for `status`, emitting each
/// transition as it happens.
fn watch_sync_state(sync: &SyncService) -> Rc<RefCell<String>> {
    let mut states = sync.state();
    // The subscriber yields changes only; the service is already running.
    let latest = Rc::new(RefCell::new(format!("{:?}", states.get())));
    let cell = latest.clone();
    tokio::task::spawn_local(async move {
        while let Some(state) = states.next().await {
            let state = format!("{state:?}");
            emit(serde_json::json!({ "event": "sync_state", "state": state }));
            *cell.borrow_mut() = state;
        }
    });
    latest
}

async fn create_encrypted_room(
    client: &Client,
    invite: &[OwnedUserId],
) -> Result<OwnedRoomId, Box<dyn Error>> {
    let mut request = CreateRoomRequest::new();
    request.name = Some("Chaos Call".to_owned());
    request.invite = invite.to_vec();
    // `org.matrix.msc3401.call.member` is a *state* event, gated by
    // `state_default` (50), so the invitee could never publish a membership in
    // the room-state format. Real Element Call rooms ship this override.
    request.power_level_content_override = Some(
        Raw::new(&serde_json::json!({
            "events": { matrix_rtc_core::compat::STATE_MEMBER_EVENT_TYPE: 0 },
        }))?
        .cast_unchecked(),
    );
    request.initial_state = vec![
        InitialStateEvent::with_empty_state_key(RoomHistoryVisibilityEventContent::new(
            HistoryVisibility::Shared,
        ))
        .to_raw_any(),
    ];
    let room = client.create_room(request).await?;
    room.enable_encryption().await?;
    Ok(room.room_id().to_owned())
}

async fn wait_for_room(
    client: &Client,
    room_id: &RoomId,
) -> Result<matrix_sdk::Room, Box<dyn Error>> {
    for _ in 0..120 {
        if let Some(room) = client.get_room(room_id) {
            return Ok(room);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!("room {room_id} did not sync within 60s").into())
}

async fn wait_for_joined_members(
    room: &matrix_sdk::Room,
    target: usize,
) -> Result<(), Box<dyn Error>> {
    for _ in 0..240 {
        let count = room
            .members(RoomMemberships::JOIN)
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        if count >= target {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!("room did not reach {target} joined members within 120s").into())
}

fn stdin_lines() -> mpsc::UnboundedReceiver<String> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::task::spawn_local(async move {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    rx
}

fn rms(pcm: &[i16]) -> f64 {
    if pcm.is_empty() {
        return 0.0;
    }
    let sum: f64 = pcm.iter().map(|&s| f64::from(s) * f64::from(s)).sum();
    (sum / pcm.len() as f64).sqrt()
}
