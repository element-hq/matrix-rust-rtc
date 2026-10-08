// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Chaos scenarios: a two-peer call, one fault, and the invariants the stack
//! must hold through it and after it. See `../README.md` for what each
//! invariant means and how to run these.
//!
//! `#[ignore]`d because they need the docker stack and a built `chaos_peer`.
//! Run them one at a time (`--test-threads=1`): they share the stack, and a
//! homeserver restart in one would be a fault in all the others.

use std::time::Duration;

use matrix_rtc_chaos::{
    HEARTBEAT, Homeserver, KEEP_ALIVE, Mode, Partition, Peer, Result, Stack, TwoPeerCall, is_event,
    now_ms, status_member_count,
};

/// How long the call may take to be whole again once a fault clears: a missed
/// keep-alive window, one heartbeat to notice, and slack for sync to deliver.
const RECOVER: Duration = Duration::from_secs(KEEP_ALIVE.as_secs() + HEARTBEAT.as_secs() + 15);

/// One `#[test]` per scenario and mode, as `<mode>::<scenario>`: every fault
/// is run against each way the client keeps its membership.
macro_rules! scenarios {
    ($($name:ident),* $(,)?) => {
        scenarios!(@mode state, Mode::State, $($name),*);
    };
    (@mode $module:ident, $mode:expr, $($name:ident),*) => {
        mod $module {
            use super::*;
            $(
                #[tokio::test(flavor = "multi_thread")]
                #[ignore = "needs the chaos stack; see chaos/README.md"]
                async fn $name() -> Result<()> {
                    let label = concat!(stringify!($module), "-", stringify!($name));
                    let stack = Stack::up(label).await?;
                    let result = super::$name(&stack, $mode).await;
                    finish(&stack, label, result).await
                }
            )*
        }
    };
}

/// Save backend logs next to the peers', pass or fail, and say where they are.
async fn finish(stack: &Stack, label: &str, result: Result<()>) -> Result<()> {
    stack.dump_logs().await;
    if let Err(error) = &result {
        eprintln!("[{label}] FAILED; logs in {}", stack.out_dir().display());
        eprintln!("[{label}] {error}");
    }
    result
}

scenarios!(
    baseline,
    homeserver_restart,
    homeserver_outage_beyond_keep_alive,
    killed_peer_is_cleaned_up,
    short_partition,
    long_partition_recovers,
    lossy_network,
    homeserver_outage_is_bridged_by_retries,
    homeserver_outage_beyond_two_delays,
    stale_leave_ends_and_rejoins,
);

/// The backend the scenarios assume is the backend that is running: the
/// homeserver advertises both LiveKit transport shapes, and serves MSC4195's
/// C-S delegation endpoint (proxied to lk-jwt-service as an application
/// service). Mode-independent, so outside `scenarios!`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs the chaos stack; see chaos/README.md"]
async fn backend_capabilities() -> Result<()> {
    let stack = Stack::up("backend_capabilities").await?;
    let hs = stack.homeserver();
    let account = hs.register("capabilities").await?;

    let (status, body) = hs
        .get(
            &account,
            "/_matrix/client/unstable/org.matrix.msc4143/rtc/transports",
        )
        .await?;
    let types: Vec<&str> = body["rtc_transports"]
        .as_array()
        .map(|transports| {
            transports
                .iter()
                .filter_map(|t| t["type"].as_str())
                .collect()
        })
        .unwrap_or_default();
    if status != 200 || types != ["m.livekit", "livekit"] {
        return Err(format!("/rtc/transports: {status} {body}").into());
    }

    // Element Call's probe: anything but 404 means the endpoint is served.
    let status = hs
        .post_status_unauthenticated(
            "/_matrix/client/unstable/io.element.msc4195/rtc/livekit/delegate_delayed_leave",
        )
        .await?;
    if status == 404 {
        return Err("the C-S delegate_delayed_leave endpoint is not served".into());
    }
    Ok(())
}

// ---- Invariants ------------------------------------------------------------

/// The peer's dead man's switch is armed on the homeserver, still pending, and
/// was restarted within the last `max_age` — i.e. heartbeats are landing, not
/// just being attempted.
async fn assert_switch_armed(hs: &Homeserver, peer: &Peer, max_age: Duration) -> Result<String> {
    let id = peer
        .delayed_leave_id()
        .ok_or_else(|| format!("[{}] has no delayed leave armed", peer.name))?;
    let delay = hs.delayed_event(&peer.account, &id).await?.ok_or_else(|| {
        format!(
            "[{}] delayed leave {id} unknown to the homeserver",
            peer.name
        )
    })?;
    if !delay.is_pending() {
        return Err(format!(
            "[{}] delayed leave {id} is finalised: {}",
            peer.name, delay.raw
        )
        .into());
    }
    let since = delay
        .delayed_since_ts()
        .ok_or("delayed event has no delayed_since_ts")?;
    let age = now_ms().saturating_sub(since);
    if age > max_age.as_millis() as u64 {
        return Err(format!(
            "[{}] delayed leave {id} last restarted {age}ms ago (limit {max_age:?})",
            peer.name
        )
        .into());
    }
    Ok(id)
}

/// Wait for the peer to report an armed switch that the homeserver agrees is
/// pending and fresh. Re-checked each second: a re-arm lands asynchronously.
async fn wait_switch_armed(hs: &Homeserver, peer: &Peer, timeout: Duration) -> Result<String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match assert_switch_armed(hs, peer, HEARTBEAT + Duration::from_secs(5)).await {
            Ok(id) => return Ok(id),
            Err(error) if tokio::time::Instant::now() > deadline => {
                return Err(format!("within {timeout:?}: {error}").into());
            }
            Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    }
}

/// Wait for the peer to hear the 440 Hz tone, and return the cursor after it:
/// the first window after subscribing is often silent or partial, and counting
/// it would be a gap the stack never had.
async fn wait_tone(peer: &Peer) -> Result<usize> {
    peer.wait(0, RECOVER, "the 440 Hz tone", |e| {
        is_event(e, "audio") && e["tone_440"].as_f64().unwrap_or(0.0) >= 0.5
    })
    .await?;
    Ok(peer.cursor())
}

/// Wait for the homeserver to confirm a restart of the peer's (same) delay
/// made after `after_ms`: proof that heartbeats resumed after a fault, which a
/// restart from just before it would not give.
async fn wait_switch_restarted_after(
    hs: &Homeserver,
    peer: &Peer,
    after_ms: u64,
    timeout: Duration,
) -> Result<String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let id = wait_switch_armed(hs, peer, timeout).await?;
        let since = hs
            .delayed_event(&peer.account, &id)
            .await?
            .and_then(|d| d.delayed_since_ts());
        if since.is_some_and(|since| since > after_ms) {
            return Ok(id);
        }
        if tokio::time::Instant::now() > deadline {
            return Err(format!(
                "[{}] delayed leave {id} not restarted after the fault within {timeout:?}",
                peer.name
            )
            .into());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Every joined status since `since` shows `count` members, and the call never
/// ended: nobody saw anyone leave.
fn assert_membership_stable(peer: &Peer, since: usize, count: u64) -> Result<()> {
    for event in peer.events_since(since) {
        if is_event(&event.value, "call_ended") || is_event(&event.value, "exited") {
            return Err(format!("[{}] call ended: {}", peer.name, event.value).into());
        }
        if is_event(&event.value, "status") {
            match status_member_count(&event.value) {
                Some(n) if n == count => {}
                _ => {
                    return Err(format!(
                        "[{}] membership flapped (expected {count}): {}",
                        peer.name, event.value
                    )
                    .into());
                }
            }
        }
    }
    Ok(())
}

/// The peer reports, after `since`, that its call ended because its
/// membership timed out — within the keep-alive (plus slack), which it can
/// only know locally when the homeserver is out of reach.
async fn wait_membership_lost(peer: &Peer, since: usize) -> Result<()> {
    peer.wait(
        since,
        KEEP_ALIVE + Duration::from_secs(15),
        "membership lost while cut off from the homeserver",
        |e| is_event(e, "call_ended") && e["reason"] == "membership_lost",
    )
    .await
    .map(|_| ())
}

/// How many `name` events the peer reported since `since`.
fn count_events(peer: &Peer, since: usize, name: &str) -> usize {
    peer.events_since(since)
        .iter()
        .filter(|e| is_event(&e.value, name))
        .count()
}

/// Waits until the homeserver confirms a fresh restart of the peer's delayed
/// leave, and returns when it saw it — the moment that delay has the most of
/// its period ahead.
async fn wait_restart(hs: &Homeserver, peer: &Peer) -> Result<tokio::time::Instant> {
    let id = peer
        .delayed_leave_id()
        .ok_or_else(|| format!("[{}] has no delayed leave armed", peer.name))?;
    let since_ts =
        |delay: Option<matrix_rtc_chaos::DelayedEvent>| delay.and_then(|d| d.delayed_since_ts());
    let first = since_ts(hs.delayed_event(&peer.account, &id).await?);
    let deadline = tokio::time::Instant::now() + KEEP_ALIVE;
    while tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let now = since_ts(hs.delayed_event(&peer.account, &id).await?);
        if now.is_some() && now != first {
            return Ok(tokio::time::Instant::now());
        }
    }
    Err(format!(
        "[{}] delayed leave {id} was not restarted within {KEEP_ALIVE:?}",
        peer.name
    )
    .into())
}

/// The peer heard the 440 Hz tone in every audio window since `since`, and in
/// at least `min_windows` of them.
fn assert_audio_continuous(peer: &Peer, since: usize, min_windows: usize) -> Result<()> {
    let windows: Vec<_> = peer
        .events_since(since)
        .into_iter()
        .filter(|e| is_event(&e.value, "audio"))
        .collect();
    if windows.len() < min_windows {
        return Err(format!(
            "[{}] only {} audio windows (wanted {min_windows})",
            peer.name,
            windows.len()
        )
        .into());
    }
    if let Some(gap) = windows
        .iter()
        .find(|w| w.value["tone_440"].as_f64().unwrap_or(0.0) < 0.5)
    {
        return Err(format!("[{}] audio gap: {}", peer.name, gap.value).into());
    }
    Ok(())
}

// ---- Scenarios -------------------------------------------------------------

/// No fault: the reference every other scenario departs from.
async fn baseline(stack: &Stack, mode: Mode) -> Result<()> {
    let hs = stack.homeserver();
    let call = TwoPeerCall::start(stack, mode, &[]).await?;
    let since = wait_tone(&call.guest).await?;
    // At least two full heartbeats, so "fresh" below is a restart, not the
    // arm; and long enough to meter the 20 audio windows asserted below.
    tokio::time::sleep((HEARTBEAT * 2).max(Duration::from_secs(30))).await;
    for peer in [&call.host, &call.guest] {
        assert_switch_armed(&hs, peer, HEARTBEAT + Duration::from_secs(5)).await?;
        assert_membership_stable(peer, since, 2)?;
    }
    assert_audio_continuous(&call.guest, since, 20)
}

/// Synapse restarts (a few seconds down): nobody may notice. Membership holds,
/// heartbeats resume against the same delay, and media — which never touches
/// the homeserver — does not skip a beat.
async fn homeserver_restart(stack: &Stack, mode: Mode) -> Result<()> {
    let hs = stack.homeserver();
    let call = TwoPeerCall::start(stack, mode, &[]).await?;
    let audio_since = wait_tone(&call.guest).await?;
    let since = [call.host.cursor(), call.guest.cursor()];
    let before = [call.host.delayed_leave_id(), call.guest.delayed_leave_id()];

    stack.restart("synapse").await?;
    let restarted_at = now_ms();
    // Long enough for a heartbeat to have gone through post-restart.
    tokio::time::sleep(HEARTBEAT + Duration::from_secs(5)).await;

    for (i, peer) in [&call.host, &call.guest].into_iter().enumerate() {
        assert_membership_stable(peer, since[i], 2)?;
        let id = wait_switch_restarted_after(&hs, peer, restarted_at, RECOVER).await?;
        if before[i].as_deref() != Some(id.as_str()) {
            return Err(format!(
                "[{}] delay changed across a short restart ({:?} -> {id}): \
                 the old one was dropped or leaked",
                peer.name, before[i]
            )
            .into());
        }
    }
    assert_audio_continuous(&call.guest, audio_since, 15)
}

/// Synapse is down for longer than the keep-alive. Both delayed leaves are
/// overdue when it comes back, so the homeserver fires them. Each peer must
/// notice its membership is lost *while the homeserver is still down* — it
/// cannot hear about it — then rejoin and re-arm on its own once it is back,
/// and see the other again. The rejoin is `chaos_peer`'s own, at the
/// application level.
async fn homeserver_outage_beyond_keep_alive(stack: &Stack, mode: Mode) -> Result<()> {
    let hs = stack.homeserver();
    let call = TwoPeerCall::start(stack, mode, &[("MEDIA", "none")]).await?;
    let outage = [call.host.cursor(), call.guest.cursor()];
    stack.stop("synapse").await?;
    for (i, peer) in [&call.host, &call.guest].into_iter().enumerate() {
        wait_membership_lost(peer, outage[i]).await?;
    }
    let since = [call.host.cursor(), call.guest.cursor()];
    stack.start("synapse").await?;

    for (i, peer) in [&call.host, &call.guest].into_iter().enumerate() {
        peer.wait_members(since[i], 2, RECOVER).await?;
        wait_switch_armed(&hs, peer, RECOVER).await?;
    }
    Ok(())
}

/// A peer dies without a word. Its dead man's switch must fire and the
/// survivor must see it leave within the keep-alive (plus sync latency),
/// not linger as a ghost until its membership expires.
async fn killed_peer_is_cleaned_up(stack: &Stack, mode: Mode) -> Result<()> {
    let hs = stack.homeserver();
    let mut call = TwoPeerCall::start(stack, mode, &[("MEDIA", "none")]).await?;
    let delay = call
        .guest
        .delayed_leave_id()
        .ok_or("guest has no delayed leave armed")?;
    let since = call.host.cursor();
    call.guest.kill().await?;

    let deadline = KEEP_ALIVE + Duration::from_secs(15);
    call.host.wait_members(since, 1, deadline).await?;
    // Finalised as sent, or gone: Synapse drops a delay once it is
    // processed rather than retaining it as MSC4140 recommends. The
    // survivor seeing the leave, above, is the real evidence.
    match hs.delayed_event(&call.guest.account, &delay).await? {
        Some(d) if !d.was_sent() => {
            Err(format!("guest's delayed leave did not fire: {}", d.raw).into())
        }
        _ => Ok(()),
    }
}

/// One peer is cut off from everything for less than the keep-alive. When the
/// partition heals the call must be exactly as it was: the other side never
/// saw it leave, and it kept the same delay.
async fn short_partition(stack: &Stack, mode: Mode) -> Result<()> {
    let hs = stack.homeserver();
    let call = TwoPeerCall::start(stack, mode, &[("MEDIA", "none")]).await?;
    let since = call.host.cursor();
    let before = call.guest.delayed_leave_id();

    call.guest.partition(Partition::All).await?;
    tokio::time::sleep(KEEP_ALIVE / 3).await;
    call.guest.heal().await?;
    let healed_at = now_ms();

    let id = wait_switch_restarted_after(&hs, &call.guest, healed_at, RECOVER).await?;
    assert_membership_stable(&call.host, since, 2)?;
    if before.as_deref() != Some(id.as_str()) {
        return Err(format!("guest's delay changed ({before:?} -> {id})").into());
    }
    Ok(())
}

/// One peer is cut off from the homeserver for longer than the keep-alive, so
/// its delayed leave fires and the other side sees it go. Once healed it must
/// come back by itself: the other side sees it again, and it is protected by
/// a fresh, pending delay.
async fn long_partition_recovers(stack: &Stack, mode: Mode) -> Result<()> {
    let hs = stack.homeserver();
    let call = TwoPeerCall::start(stack, mode, &[("MEDIA", "none")]).await?;
    let since = call.host.cursor();

    let cut_off = call.guest.cursor();
    call.guest.partition(Partition::Homeserver).await?;
    wait_membership_lost(&call.guest, cut_off).await?;
    call.host
        .wait_members(since, 1, KEEP_ALIVE + Duration::from_secs(15))
        .await?;
    let healed = call.host.cursor();
    call.guest.heal().await?;

    call.host.wait_members(healed, 2, RECOVER).await?;
    wait_switch_armed(&hs, &call.guest, RECOVER).await?;
    Ok(())
}

/// 20% packet loss on one peer for a minute. Retries must absorb it: the
/// membership never flaps and the switch never lapses.
async fn lossy_network(stack: &Stack, mode: Mode) -> Result<()> {
    let hs = stack.homeserver();
    let call = TwoPeerCall::start(stack, mode, &[("MEDIA", "none")]).await?;
    let since = call.host.cursor();

    call.guest.netem("loss 20%").await?;
    tokio::time::sleep(Duration::from_secs(60)).await;
    // Checked while still lossy: the switch must hold *under* loss.
    assert_switch_armed(&hs, &call.guest, KEEP_ALIVE).await?;
    call.guest.clear_netem().await?;

    assert_membership_stable(&call.host, since, 2)?;
    wait_switch_armed(&hs, &call.guest, RECOVER).await?;
    Ok(())
}

/// Synapse goes away for most of one delay and is back with seconds to spare.
/// The host keeps its membership: its failed restarts are retried, denser as
/// the deadline nears, and one lands in those seconds. The guest runs the
/// restart pacing this one replaced — a restart every 10 s, failures not
/// retried early; its loss detection stays on — and loses its membership in
/// the same outage. Together: the retries are what saved the host's call.
///
/// Timing-sensitive by design: Synapse must be back (it takes a few seconds
/// to start) before the host's deadline, 30 s after its last restart, and
/// still down at the guest's second attempt, 20 s after its last. Slow CI can
/// push the restart past the host's deadline.
async fn homeserver_outage_is_bridged_by_retries(stack: &Stack, mode: Mode) -> Result<()> {
    let hs = stack.homeserver();
    let call = TwoPeerCall::start_with(
        stack,
        mode,
        &[("MEDIA", "none")],
        &[("MEDIA", "none"), ("KEEP_ALIVE_POLICY", "legacy")],
    )
    .await?;
    let host_delay = call.host.delayed_leave_id();
    // Start right after a confirmed restart: the host's deadline is then a
    // full delay away, and the guest's last restart at most 10 s back.
    let restarted = wait_restart(&hs, &call.host).await?;
    let since = [call.host.cursor(), call.guest.cursor()];
    stack.stop("synapse").await?;
    // Back 20 s on, ready a few seconds later: after the guest's second
    // attempt at the latest, with the host's deadline still seconds away.
    tokio::time::sleep_until(restarted + Duration::from_secs(20)).await;
    stack.start("synapse").await?;

    let id = wait_switch_armed(&hs, &call.host, RECOVER).await?;
    if host_delay.as_deref() != Some(id.as_str()) {
        return Err(format!("host's delay changed ({host_delay:?} -> {id}): it was lost").into());
    }
    if count_events(&call.host, since[0], "call_ended") > 0 {
        return Err("the host's call ended: the retries did not bridge the outage".into());
    }
    call.guest
        .wait(
            since[1],
            RECOVER,
            "the legacy policy losing the guest's membership",
            |e| is_event(e, "call_ended") && e["reason"] == "membership_lost",
        )
        .await?;
    Ok(())
}

/// Synapse is gone for well over two delays. Each peer notices that its
/// membership is lost while the homeserver is down, then waits — a messenger
/// knows it is offline, and spends no join attempts on a homeserver it cannot
/// reach — and rejoins once sync is back, retiring its old delay first.
async fn homeserver_outage_beyond_two_delays(stack: &Stack, mode: Mode) -> Result<()> {
    let hs = stack.homeserver();
    let call = TwoPeerCall::start(stack, mode, &[("MEDIA", "none")]).await?;
    let outage = [call.host.cursor(), call.guest.cursor()];
    stack.stop("synapse").await?;
    let down_at = tokio::time::Instant::now();
    for (i, peer) in [&call.host, &call.guest].into_iter().enumerate() {
        wait_membership_lost(peer, outage[i]).await?;
    }
    let lost = [call.host.cursor(), call.guest.cursor()];
    tokio::time::sleep_until(down_at + KEEP_ALIVE * 2 + Duration::from_secs(15)).await;

    // Stage one: offline, so no joins — one attempt is tolerated, for a sync
    // that had not noticed yet.
    for (i, peer) in [&call.host, &call.guest].into_iter().enumerate() {
        let joined = count_events(peer, lost[i], "joined");
        let failed = count_events(peer, lost[i], "join_failed");
        if joined > 0 || failed > 1 {
            return Err(format!(
                "[{}] tried to join while offline ({joined} joined, {failed} failed)",
                peer.name
            )
            .into());
        }
    }

    // Stage two: back online, both rejoin and see each other, freshly armed.
    let back = [call.host.cursor(), call.guest.cursor()];
    stack.start("synapse").await?;
    for (i, peer) in [&call.host, &call.guest].into_iter().enumerate() {
        peer.wait(back[i], RECOVER, "rejoined after the outage", |e| {
            is_event(e, "joined")
        })
        .await?;
        peer.wait_members(back[i], 2, RECOVER).await?;
        wait_switch_armed(&hs, peer, RECOVER).await?;
    }
    Ok(())
}

/// The homeserver is fine, and a delayed leave nobody tracks lands anyway — a
/// leftover of an earlier join, as a homeserver restart can send late. The
/// guest arms one on purpose. When it fires, the room drops the guest's
/// membership while the guest believes it is joined: everybody else counts it
/// as left, so the guest must end its call too, then rejoin by itself.
async fn stale_leave_ends_and_rejoins(stack: &Stack, mode: Mode) -> Result<()> {
    let hs = stack.homeserver();
    let mut call = TwoPeerCall::start(stack, mode, &[("MEDIA", "none")]).await?;
    let since = [call.host.cursor(), call.guest.cursor()];

    call.guest.send("leak_delayed_leave 3000").await?;
    call.guest
        .wait(
            since[1],
            Duration::from_secs(10),
            "the leaked delay armed",
            |e| is_event(e, "leaked_delayed_leave"),
        )
        .await?;
    call.guest
        .wait(
            since[1],
            Duration::from_secs(20),
            "the call ended when the stale leave landed",
            |e| is_event(e, "call_ended") && e["reason"] == "membership_lost",
        )
        .await?;
    let ended = call.guest.cursor();
    call.guest
        .wait(ended, RECOVER, "rejoined after the stale leave", |e| {
            is_event(e, "joined")
        })
        .await?;
    // Cursors from after the rejoin: statuses from before the loss already
    // show two members.
    let rejoined = [call.host.cursor(), call.guest.cursor()];
    for (i, peer) in [&call.host, &call.guest].into_iter().enumerate() {
        peer.wait_members(rejoined[i], 2, RECOVER).await?;
    }
    wait_switch_armed(&hs, &call.guest, RECOVER).await?;
    Ok(())
}
