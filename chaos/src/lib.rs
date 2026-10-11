// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Harness for the MatrixRTC chaos suite.
//!
//! Drives the compose stack in `docker/`, runs each participant as a
//! containerised `chaos_peer` (matrix-rtc-call-sdk's example — the real
//! `LiveKitCall` facade over core, matrix-sdk backend and transport), and
//! injects faults around them:
//! homeserver restarts and outages, per-peer partitions and netem, SIGKILL.
//! A second homeserver federating with the first hosts participants who are
//! not on the call's own server.
//!
//! Scenarios live in `tests/scenarios.rs` and judge the stack by what the
//! peers report (their JSON event streams) and by what the homeserver says
//! about their dead man's switches (MSC4140 `GET /delayed_events/{delay_id}`).
//! This crate never links the SDK: the client under test is a black box
//! behind its stdin/stdout protocol. See `README.md`.

mod homeserver;
mod peer;
mod stack;

pub use homeserver::{Account, DelayedEvent, Homeserver, now_ms};
pub use peer::{Partition, Peer};
pub use stack::Stack;

/// How long a fresh call may take to converge. Generous: the first join after
/// a stack start includes Synapse warming up and key upload.
pub const CONVERGE: Duration = Duration::from_secs(120);

use std::time::Duration;

/// Errors are reported, not matched on: a scenario fails with the message.
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

/// The MSC4140 keep-alive the client arms by default
/// (`matrix_rtc_core::DEFAULT_KEEP_ALIVE_TIMEOUT_MS`). Scenarios size their
/// faults around it: shorter than this and the membership must survive,
/// longer and the delayed leave must have fired.
pub const KEEP_ALIVE: Duration = Duration::from_secs(30);

/// How often the client's core restarts its switch by default
/// (`matrix_rtc_core::DEFAULT_KEEP_ALIVE_INTERVAL_MS`).
pub const HEARTBEAT: Duration = Duration::from_secs(10);

/// How the peers keep their membership. Every scenario runs in each mode.
///
/// Only the pre-sticky generation for now: sticky events (MSC4354) are still
/// moving in the spec, Synapse and matrix-rust-sdk, so their modes come once
/// those settle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// The generation most Element Call deployments speak:
    /// `org.matrix.msc3401.call.member` room state, the pre-MSC4195 token
    /// endpoint, no slots.
    State,
}

impl Mode {
    /// The `chaos_peer` `COMPAT` value for this mode.
    pub fn compat(self) -> &'static str {
        match self {
            Mode::State => "state",
        }
    }
}

/// A two-participant call, set up the way a real one is: `host` creates the
/// encrypted room, invites `guest`, opens the slot; `guest` accepts; both
/// join, and both report two members. In [`Mode::State`] there is no slot to
/// open; the peer knows.
pub struct TwoPeerCall {
    pub host: Peer,
    pub guest: Peer,
}

impl TwoPeerCall {
    /// Provision two users, start both peers and join them to one call.
    /// `env` is passed to both peers (e.g. `MEDIA=none`).
    pub async fn start(stack: &Stack, mode: Mode, env: &[(&str, &str)]) -> Result<Self> {
        Self::start_with(stack, mode, env, env).await
    }

    /// [`Self::start`], with each peer's own environment — to run one peer
    /// differently from the other in the same call.
    pub async fn start_with(
        stack: &Stack,
        mode: Mode,
        host_env: &[(&str, &str)],
        guest_env: &[(&str, &str)],
    ) -> Result<Self> {
        let hs = stack.homeserver();
        let host_account = hs.register("host").await?;
        let guest_account = hs.register("guest").await?;
        let invite = guest_account.user_id.clone();
        let (mut host, room_id) = spawn_host(stack, mode, host_account, &invite, host_env).await?;
        let mut guest =
            spawn_guest(stack, mode, "guest", guest_account, &room_id, guest_env).await?;
        join_and_wait(&mut [&mut host, &mut guest], 2).await?;
        Ok(Self { host, guest })
    }
}

/// A call on `synapse` with a third participant from the federated
/// `synapse2`: `host` and `guest` are joined (two members) when this returns;
/// `remote` is in the *room* already — it accepted the invite over
/// federation, so `synapse` was up for that — but joins the *call* only when
/// the scenario sends it `join`. All three share the one SFU.
pub struct FederatedCall {
    pub host: Peer,
    pub guest: Peer,
    pub remote: Peer,
}

impl FederatedCall {
    /// Provision the three users (two on `synapse`, one on `synapse2`), start
    /// all peers, and join `host` and `guest` to the call. `env` goes to all.
    pub async fn start(stack: &Stack, mode: Mode, env: &[(&str, &str)]) -> Result<Self> {
        let hs = stack.homeserver();
        let host_account = hs.register("host").await?;
        let guest_account = hs.register("guest").await?;
        let remote_account = stack.homeserver2().register("remote").await?;
        let invite = format!("{},{}", guest_account.user_id, remote_account.user_id);
        let (mut host, room_id) = spawn_host(stack, mode, host_account, &invite, env).await?;
        let mut guest = spawn_guest(stack, mode, "guest", guest_account, &room_id, env).await?;
        // Its `join` waits until its homeserver sees all three in the room.
        let mut remote_env = env.to_vec();
        remote_env.push(("EXPECT_ROOM_MEMBERS", "3"));
        let remote =
            spawn_guest(stack, mode, "remote", remote_account, &room_id, &remote_env).await?;
        join_and_wait(&mut [&mut host, &mut guest], 2).await?;
        Ok(Self {
            host,
            guest,
            remote,
        })
    }
}

/// Start the peer that creates the encrypted room and invites `invite` (a
/// comma-separated list); returns it once ready, with the room's id.
async fn spawn_host(
    stack: &Stack,
    mode: Mode,
    account: Account,
    invite: &str,
    env: &[(&str, &str)],
) -> Result<(Peer, String)> {
    let mut full = vec![("ROLE", "host"), ("INVITE", invite)];
    full.extend_from_slice(env);
    full.push(("COMPAT", mode.compat()));
    let host = stack.spawn_peer("host", account, &full).await?;
    let ready = host
        .wait(0, CONVERGE, "host ready", |e| is_event(e, "ready"))
        .await?;
    let room_id = ready["room_id"]
        .as_str()
        .ok_or("host's ready event has no room_id")?
        .to_owned();
    Ok((host, room_id))
}

/// Start a peer that accepts its invite to `room_id`; returns it once it is
/// in the room (ready), not yet in the call.
async fn spawn_guest(
    stack: &Stack,
    mode: Mode,
    name: &str,
    account: Account,
    room_id: &str,
    env: &[(&str, &str)],
) -> Result<Peer> {
    let mut full = vec![("ROLE", "guest"), ("ROOM_ID", room_id)];
    full.extend_from_slice(env);
    full.push(("COMPAT", mode.compat()));
    let peer = stack.spawn_peer(name, account, &full).await?;
    peer.wait(0, CONVERGE, &format!("{name} ready"), |e| {
        is_event(e, "ready")
    })
    .await?;
    Ok(peer)
}

/// Send `join` to every peer, then wait until each reports `count` members.
async fn join_and_wait(peers: &mut [&mut Peer], count: u64) -> Result<()> {
    for peer in peers.iter_mut() {
        peer.send("join").await?;
    }
    for peer in peers.iter() {
        // A refused join says so at once; waiting out CONVERGE would only
        // report "no N members".
        let settled = peer
            .wait(0, CONVERGE, &format!("{count} members"), |e| {
                status_member_count(e) == Some(count) || is_event(e, "join_failed")
            })
            .await?;
        if is_event(&settled, "join_failed") {
            return Err(format!("[{}] join failed: {}", peer.name, settled["message"]).into());
        }
    }
    Ok(())
}

/// Whether `event` is a protocol event of this name.
pub fn is_event(event: &serde_json::Value, name: &str) -> bool {
    event["event"] == name
}

/// The member count a `status` event reports, if it is a joined status.
pub fn status_member_count(event: &serde_json::Value) -> Option<u64> {
    if !is_event(event, "status") || event["joined"] != true {
        return None;
    }
    event["member_count"].as_u64()
}
