// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The harness's own view of the homeserver, over the published client API.
//!
//! Two jobs: provisioning throwaway accounts, and asking MSC4140 about a
//! peer's dead man's switch. The lookup is by `delay_id` because that is all
//! the merged MSC specifies — listing a user's delayed events moved to
//! MSC4486, so the suite must not depend on it. Each account keeps the access
//! token of the harness's own device: `GET /delayed_events/{delay_id}` is
//! scoped to the user, not to the device that scheduled the delay.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::Result;

#[derive(Clone, Debug)]
pub struct Account {
    pub user_id: String,
    pub localpart: String,
    pub password: String,
    /// The harness's device, for inspection only; the peer logs in separately.
    pub access_token: String,
}

/// One delayed event as `GET /delayed_events/{delay_id}` reports it.
#[derive(Clone, Debug)]
pub struct DelayedEvent {
    pub raw: serde_json::Value,
}

impl DelayedEvent {
    /// Still pending: neither sent, cancelled nor failed.
    pub fn is_pending(&self) -> bool {
        self.raw.get("finalised").is_none()
    }

    /// Finalised by being sent, i.e. the dead man's switch fired.
    pub fn was_sent(&self) -> bool {
        self.raw["finalised"].get("event_id").is_some()
    }

    /// Unix ms of the scheduling or the last restart.
    pub fn delayed_since_ts(&self) -> Option<u64> {
        self.raw["delayed_since_ts"].as_u64()
    }
}

#[derive(Clone)]
pub struct Homeserver {
    base: String,
    http: reqwest::Client,
}

static ACCOUNT_SEQ: AtomicU32 = AtomicU32::new(0);

impl Homeserver {
    pub(crate) fn new(base: &str) -> Self {
        Self {
            base: base.trim_end_matches('/').to_owned(),
            http: reqwest::Client::new(),
        }
    }

    /// Register a fresh user whose localpart starts with `prefix`.
    pub async fn register(&self, prefix: &str) -> Result<Account> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let seq = ACCOUNT_SEQ.fetch_add(1, Ordering::Relaxed);
        let localpart = format!("{prefix}-{nanos:x}-{seq}");
        let password = format!("chaos-{nanos:x}");
        let response = self
            .http
            .post(format!("{}/_matrix/client/v3/register", self.base))
            .json(&serde_json::json!({
                "username": localpart,
                "password": password,
                "auth": { "type": "m.login.dummy" },
                "initial_device_display_name": "chaos harness",
            }))
            .send()
            .await?;
        let status = response.status();
        let body: serde_json::Value = response.json().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!("registering {localpart} failed: {status} {body}").into());
        }
        Ok(Account {
            user_id: body["user_id"]
                .as_str()
                .ok_or("register response has no user_id")?
                .to_owned(),
            access_token: body["access_token"]
                .as_str()
                .ok_or("register response has no access_token")?
                .to_owned(),
            localpart,
            password,
        })
    }

    /// Look up one of `account`'s delayed events; `None` if the homeserver
    /// does not know it (never existed, or its retention ran out).
    pub async fn delayed_event(
        &self,
        account: &Account,
        delay_id: &str,
    ) -> Result<Option<DelayedEvent>> {
        // Unstable prefix: Synapse does not serve the stable v1 path yet.
        let response = self
            .http
            .get(format!(
                "{}/_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delay_id}",
                self.base
            ))
            .bearer_auth(&account.access_token)
            .send()
            .await?;
        let status = response.status();
        let raw: serde_json::Value = response.json().await.unwrap_or_default();
        // A homeserver without the single-delay lookup answers with
        // M_UNRECOGNIZED (405 on Synapse < 1.161, where only POST matches the
        // path), which must not read as "this delay does not exist".
        if raw["errcode"] == "M_UNRECOGNIZED" {
            return Err(format!(
                "the homeserver does not implement GET /delayed_events/{{delay_id}} \
                 (Synapse >= 1.161): {status} {raw}"
            )
            .into());
        }
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(format!("GET delayed event {delay_id}: {status} {raw}").into());
        }
        Ok(Some(DelayedEvent { raw }))
    }

    /// `GET` a client-API path as `account`, returning the status and body.
    pub async fn get(&self, account: &Account, path: &str) -> Result<(u16, serde_json::Value)> {
        let response = self
            .http
            .get(format!("{}{path}", self.base))
            .bearer_auth(&account.access_token)
            .send()
            .await?;
        let status = response.status().as_u16();
        Ok((status, response.json().await.unwrap_or_default()))
    }

    /// The status of an unauthenticated `POST` to a client-API path — enough to
    /// tell a served endpoint (401) from a missing one (404).
    pub async fn post_status_unauthenticated(&self, path: &str) -> Result<u16> {
        let response = self
            .http
            .post(format!("{}{path}", self.base))
            .json(&serde_json::json!({}))
            .send()
            .await?;
        Ok(response.status().as_u16())
    }

    /// Whether the client API answers at all (used around restarts).
    pub async fn is_up(&self) -> bool {
        self.http
            .get(format!("{}/health", self.base))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
    }
}

/// Unix ms now, on the same clock Synapse stamps `delayed_since_ts` with (the
/// containers share the host's kernel clock).
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
