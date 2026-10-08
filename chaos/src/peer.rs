// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! One containerised `chaos_peer`, and the faults that target it alone.
//!
//! Every stdout line (a JSON protocol event) is kept, timestamped, for the
//! scenario to query, and appended to `<peer>.jsonl` in the scenario's output
//! directory; stderr goes to `<peer>.log`. Those two files are what a failing
//! CI run uploads.
//!
//! Network faults run inside the peer's own namespace via `docker exec`, so
//! they hit this participant and nothing else: iptables for partitions (in a
//! dedicated `CHAOS` chain, so healing is a flush), tc netem for loss and
//! latency.

use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::Notify;

use crate::{Account, Result, status_member_count};

/// What a partition cuts the peer off from.
#[derive(Clone, Copy, Debug)]
pub enum Partition {
    /// Everything: homeserver, SFU, authorisation service.
    All,
    /// The homeserver only; media keeps flowing through the SFU.
    Homeserver,
}

#[derive(Clone, Debug)]
pub struct Event {
    pub at: Instant,
    pub value: Value,
}

pub struct Peer {
    pub name: String,
    pub account: Account,
    container: String,
    stdin: Option<ChildStdin>,
    _child: Child,
    events: Arc<Mutex<Vec<Event>>>,
    arrived: Arc<Notify>,
}

impl Peer {
    pub(crate) async fn spawn(
        mut command: Command,
        name: &str,
        container: String,
        account: Account,
        out_dir: &Path,
    ) -> Result<Self> {
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;

        let events = Arc::new(Mutex::new(Vec::new()));
        let arrived = Arc::new(Notify::new());

        let stdout = child.stdout.take().ok_or("peer stdout not piped")?;
        let mut jsonl = tokio::fs::File::create(out_dir.join(format!("{name}.jsonl"))).await?;
        let (events_w, arrived_w, label) = (events.clone(), arrived.clone(), name.to_owned());
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            loop {
                let line = lines.next_line().await;
                let value = match line {
                    Ok(Some(line)) => {
                        let _ = jsonl.write_all(format!("{line}\n").as_bytes()).await;
                        serde_json::from_str(&line).unwrap_or_else(
                            |_| serde_json::json!({ "event": "garbage", "line": line }),
                        )
                    }
                    // A synthetic last event, so waits fail fast on a dead peer.
                    _ => serde_json::json!({ "event": "exited" }),
                };
                let exited = value["event"] == "exited";
                if value["event"] != "status" && value["event"] != "audio" {
                    eprintln!("[{label}] {value}");
                }
                events_w.lock().unwrap().push(Event {
                    at: Instant::now(),
                    value,
                });
                arrived_w.notify_waiters();
                if exited {
                    break;
                }
            }
            let _ = jsonl.flush().await;
        });

        let stderr = child.stderr.take().ok_or("peer stderr not piped")?;
        let mut log = tokio::fs::File::create(out_dir.join(format!("{name}.log"))).await?;
        tokio::spawn(async move {
            let _ = tokio::io::copy(&mut BufReader::new(stderr), &mut log).await;
        });

        Ok(Self {
            name: name.to_owned(),
            account,
            container,
            stdin: child.stdin.take(),
            _child: child,
            events,
            arrived,
        })
    }

    /// Send one protocol command.
    pub async fn send(&mut self, command: &str) -> Result<()> {
        let stdin = self.stdin.as_mut().ok_or("peer stdin closed")?;
        stdin.write_all(format!("{command}\n").as_bytes()).await?;
        stdin.flush().await?;
        Ok(())
    }

    /// Index of the next event to arrive — pass to [`Peer::wait`] to only
    /// consider what happens from now on.
    pub fn cursor(&self) -> usize {
        self.events.lock().unwrap().len()
    }

    /// Events from `since` onwards.
    pub fn events_since(&self, since: usize) -> Vec<Event> {
        self.events.lock().unwrap()[since..].to_vec()
    }

    /// The last `status` event, if any.
    pub fn latest_status(&self) -> Option<Value> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|e| e.value["event"] == "status")
            .map(|e| e.value.clone())
    }

    /// Wait for the first event at or after `since` matching `pred`.
    pub async fn wait(
        &self,
        since: usize,
        timeout: Duration,
        what: &str,
        pred: impl Fn(&Value) -> bool,
    ) -> Result<Value> {
        let deadline = tokio::time::Instant::now() + timeout;
        // A peer that died before the cursor was taken never answers either.
        let exited_before = {
            let events = self.events.lock().unwrap();
            events[..since.min(events.len())]
                .iter()
                .any(|event| event.value["event"] == "exited")
        };
        if exited_before {
            return Err(format!("[{}] exited before waiting for {what}", self.name).into());
        }
        let mut scanned = since;
        loop {
            // Register before scanning, so an event landing in between wakes us.
            let arrived = self.arrived.notified();
            {
                let events = self.events.lock().unwrap();
                for event in &events[scanned.min(events.len())..] {
                    if pred(&event.value) {
                        return Ok(event.value.clone());
                    }
                    if event.value["event"] == "exited" {
                        return Err(
                            format!("[{}] exited while waiting for {what}", self.name).into()
                        );
                    }
                }
                scanned = events.len();
            }
            if tokio::time::timeout_at(deadline, arrived).await.is_err() {
                return Err(format!(
                    "[{}] no {what} within {timeout:?}; last status: {}",
                    self.name,
                    self.latest_status().unwrap_or(Value::Null),
                )
                .into());
            }
        }
    }

    /// Wait for a joined `status` reporting exactly `count` members.
    pub async fn wait_members(&self, since: usize, count: u64, timeout: Duration) -> Result<Value> {
        self.wait(since, timeout, &format!("{count} members"), |e| {
            status_member_count(e) == Some(count)
        })
        .await
    }

    /// The armed `delay_id` from the latest status, if joined and armed.
    pub fn delayed_leave_id(&self) -> Option<String> {
        self.latest_status()?["delayed_leave_id"]
            .as_str()
            .map(str::to_owned)
    }

    /// SIGKILL the peer: no leave, no cleanup — only the dead man's switch.
    pub async fn kill(&mut self) -> Result<()> {
        // Kill *before* dropping stdin: EOF on stdin is the peer's cue to
        // leave cleanly, which is exactly what a crash must not do.
        docker(&["kill", "--signal", "KILL", &self.container]).await?;
        self.stdin = None;
        Ok(())
    }

    pub async fn partition(&self, what: Partition) -> Result<()> {
        let rules = match what {
            Partition::All => {
                "iptables -A CHAOS -i eth0 -j DROP && iptables -A CHAOS -o eth0 -j DROP"
            }
            Partition::Homeserver => {
                "ip=$(getent hosts synapse | cut -d' ' -f1) && \
                 iptables -A CHAOS -s \"$ip\" -j DROP && iptables -A CHAOS -d \"$ip\" -j DROP"
            }
        };
        self.exec_sh(&format!("{CHAOS_CHAIN} && {rules}")).await
    }

    /// Undo [`Peer::partition`].
    pub async fn heal(&self) -> Result<()> {
        self.exec_sh(&format!("{CHAOS_CHAIN} && iptables -F CHAOS"))
            .await
    }

    /// Apply a netem discipline to the peer's outgoing traffic (a root qdisc
    /// shapes egress only), e.g. `"loss 20%"` or `"delay 300ms 100ms"`.
    /// Replaces any previous one.
    pub async fn netem(&self, spec: &str) -> Result<()> {
        self.exec_sh(&format!("tc qdisc replace dev eth0 root netem {spec}"))
            .await
    }

    /// Undo [`Peer::netem`].
    pub async fn clear_netem(&self) -> Result<()> {
        self.exec_sh("tc qdisc del dev eth0 root 2>/dev/null || true")
            .await
    }

    async fn exec_sh(&self, script: &str) -> Result<()> {
        docker(&["exec", &self.container, "sh", "-c", script]).await?;
        Ok(())
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        // Synchronous on purpose: this also runs while a failed scenario
        // unwinds, and a leaked peer would keep heartbeating into the next one.
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.container])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// Idempotently create the `CHAOS` chain and hook it into INPUT and OUTPUT.
const CHAOS_CHAIN: &str = "{ iptables -N CHAOS 2>/dev/null || true; } && \
     { iptables -C INPUT -j CHAOS 2>/dev/null || iptables -I INPUT -j CHAOS; } && \
     { iptables -C OUTPUT -j CHAOS 2>/dev/null || iptables -I OUTPUT -j CHAOS; }";

pub(crate) async fn docker(args: &[&str]) -> Result<String> {
    let output = Command::new("docker").args(args).output().await?;
    if !output.status.success() {
        return Err(format!(
            "docker {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}
