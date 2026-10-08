// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The compose stack in `docker/`: bringing it up, running peers in it, and
//! the faults that target the backend rather than one peer.
//!
//! The stack is shared between scenarios (they run one at a time) and left
//! running afterwards, so a rerun skips Synapse's boot. Each scenario gets
//! fresh accounts and a fresh room, which is all the isolation it needs; what
//! it must not inherit is a stopped service or a leftover peer, so [`Stack::up`]
//! starts everything and clears old peers every time.

use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::process::Command;

use crate::peer::docker;
use crate::{Account, Homeserver, Peer, Result};

const PROJECT: &str = "matrix-rtc-chaos";
const HOMESERVER: &str = "http://localhost:18008";
const BACKEND: [&str; 3] = ["synapse", "livekit", "auth-service"];

pub struct Stack {
    compose_file: PathBuf,
    run_id: String,
    out_dir: PathBuf,
}

impl Stack {
    /// Bring the backend up (or back up) and prepare `scenario`'s output
    /// directory, `target/chaos/<scenario>/`.
    pub async fn up(scenario: &str) -> Result<Self> {
        let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let binary = crate_dir.join("bin/chaos_peer");
        if !binary.exists() {
            return Err(format!(
                "{} is missing — build it first with chaos/build-peer.sh",
                binary.display()
            )
            .into());
        }
        warn_if_peer_is_stale(&crate_dir);
        let out_dir = crate_dir.join("../target/chaos").join(scenario);
        let _ = tokio::fs::remove_dir_all(&out_dir).await;
        tokio::fs::create_dir_all(&out_dir).await?;

        let stack = Self {
            compose_file: crate_dir.join("docker/compose.yml"),
            run_id: format!(
                "{:x}",
                SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis()
            ),
            out_dir,
        };
        stack.remove_leftover_peers().await?;
        stack
            .compose(&["--profile", "peer", "build", "--quiet", "peer"])
            .await?;
        stack.start_backend().await?;
        Ok(stack)
    }

    pub fn homeserver(&self) -> Homeserver {
        Homeserver::new(HOMESERVER)
    }

    /// Where this scenario's logs go; also written to by [`Stack::dump_logs`].
    pub fn out_dir(&self) -> &std::path::Path {
        &self.out_dir
    }

    /// Start a peer container. `env` is on top of the compose defaults; the
    /// account's credentials are added here.
    pub async fn spawn_peer(
        &self,
        name: &str,
        account: Account,
        env: &[(&str, &str)],
    ) -> Result<Peer> {
        let container = format!("chaos-{name}-{}", self.run_id);
        let mut command = Command::new("docker");
        command
            .args(["compose", "-f"])
            .arg(&self.compose_file)
            .args([
                "-p",
                PROJECT,
                "--profile",
                "peer",
                "run",
                "--rm",
                "-T",
                "--no-deps",
            ])
            .args(["--name", &container])
            .args(["-e", &format!("MX_USER={}", account.localpart)])
            .args(["-e", &format!("MX_PASSWORD={}", account.password)]);
        for (key, value) in env {
            command.args(["-e", &format!("{key}={value}")]);
        }
        command.arg("peer");
        Peer::spawn(command, name, container, account, &self.out_dir).await
    }

    /// `docker restart` a backend service: a clean stop, then a start.
    pub async fn restart(&self, service: &str) -> Result<()> {
        self.compose(&["restart", service]).await?;
        self.wait_ready().await
    }

    /// Stop a backend service (it stays down until [`Stack::start`]).
    pub async fn stop(&self, service: &str) -> Result<()> {
        self.compose(&["stop", service]).await.map(drop)
    }

    pub async fn start(&self, service: &str) -> Result<()> {
        self.compose(&["start", service]).await?;
        self.wait_ready().await
    }

    /// Save the backend's logs next to the peers'. Call at the end of every
    /// scenario, pass or fail — green runs that log oddities are how drift
    /// gets spotted.
    pub async fn dump_logs(&self) {
        let mut args = vec!["logs", "--no-color", "--timestamps"];
        args.extend(BACKEND);
        match self.compose(&args).await {
            Ok(logs) => {
                let _ = tokio::fs::write(self.out_dir.join("backend.log"), logs).await;
            }
            Err(error) => eprintln!("[stack] could not collect backend logs: {error}"),
        }
    }

    async fn start_backend(&self) -> Result<()> {
        let mut args = vec!["up", "-d", "--wait"];
        args.extend(BACKEND);
        self.compose(&args).await?;
        self.wait_ready().await
    }

    /// Synapse's client API from the host, and lk-jwt from inside the SFU's
    /// namespace (its FROM-scratch image has no healthcheck of its own, and it
    /// is deliberately not published).
    async fn wait_ready(&self) -> Result<()> {
        let hs = self.homeserver();
        let deadline = Instant::now() + Duration::from_secs(120);
        while !hs.is_up().await {
            if Instant::now() > deadline {
                return Err("synapse did not come up within 120s".into());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        while self
            .compose(&[
                "exec",
                "-T",
                "livekit",
                "wget",
                "-q",
                "-O",
                "/dev/null",
                "http://localhost:6080/healthz",
            ])
            .await
            .is_err()
        {
            if Instant::now() > deadline {
                return Err("lk-jwt-service did not come up within 120s".into());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok(())
    }

    async fn remove_leftover_peers(&self) -> Result<()> {
        let ids = docker(&[
            "ps",
            "-aq",
            "--filter",
            &format!("label=com.docker.compose.project={PROJECT}"),
            "--filter",
            "label=com.docker.compose.oneoff=True",
        ])
        .await?;
        let ids: Vec<&str> = ids.split_whitespace().collect();
        if !ids.is_empty() {
            let mut args = vec!["rm", "-f"];
            args.extend(ids);
            docker(&args).await?;
        }
        Ok(())
    }

    async fn compose(&self, args: &[&str]) -> Result<String> {
        let compose_file = self.compose_file.to_string_lossy();
        let mut full = vec!["compose", "-f", &compose_file, "-p", PROJECT];
        full.extend_from_slice(args);
        docker(&full).await
    }
}

/// Warn when `bin/chaos_peer` was built from another commit than the checkout's
/// `HEAD`: the binary is mounted, not built, so after a branch switch the suite
/// silently tests the old client. A warning rather than an error, because
/// uncommitted edits (the normal state while working) share the same `HEAD`.
fn warn_if_peer_is_stale(crate_dir: &std::path::Path) {
    let built = std::fs::read_to_string(crate_dir.join("bin/chaos_peer.rev")).unwrap_or_default();
    let head = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(crate_dir)
        .output()
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
        .unwrap_or_default();
    if built.trim() != head {
        eprintln!(
            "[stack] WARNING: chaos_peer was built from {:?}, the checkout is at {head:?}; \
             rerun chaos/build-peer.sh unless that is intended",
            built.trim()
        );
    }
}
