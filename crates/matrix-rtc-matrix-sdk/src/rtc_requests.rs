// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! MSC4143 transport discovery as raw JSON.
//!
//! ruma models a `livekit` transport as `livekit_service_url` only, so its
//! typed discovery drops the SFU `url` a transitional homeserver advertises
//! alongside it, and fails on a homeserver advertising `url` alone. Which of
//! the two fields is present decides how every client gets its LiveKit token,
//! so the core must see the array exactly as served: this GETs the endpoints
//! with the client's own HTTP client and access token and keeps each entry as
//! a JSON value. TODO(U4): drop once ruma models the current MSC4195.

use matrix_rtc_core::BackendError;
use matrix_sdk::Client;
use serde_json::Value;

/// The v1 endpoint and the field its answer carries the array in.
const STABLE: (&str, &str) = ("_matrix/client/v1/rtc/transports", "transports");
/// The unstable endpoint and its field.
const UNSTABLE: (&str, &str) = (
    "_matrix/client/unstable/org.matrix.msc4143/rtc/transports",
    "rtc_transports",
);

/// The homeserver's transports, raw: from the v1 endpoint, else the unstable
/// one. `Ok(None)` when it serves neither (404 or `M_UNRECOGNIZED`).
pub async fn transports(client: &Client) -> Result<Option<Value>, BackendError> {
    for (path, field) in [STABLE, UNSTABLE] {
        if let Some(transports) = get(client, path, field).await? {
            return Ok(Some(transports));
        }
    }
    Ok(None)
}

async fn get(client: &Client, path: &str, field: &str) -> Result<Option<Value>, BackendError> {
    let url = client.homeserver().join(path).map_err(error)?;
    let mut request = client.http_client().get(url);
    if let Some(token) = client.access_token() {
        request = request.bearer_auth(token);
    }
    let response = request.send().await.map_err(error)?;
    let status = response.status().as_u16();
    let body: Value =
        serde_json::from_str(&response.text().await.map_err(error)?).unwrap_or(Value::Null);
    let errcode = body.get("errcode").and_then(Value::as_str);
    if status == 404 || errcode == Some("M_UNRECOGNIZED") {
        return Ok(None);
    }
    if !(200..300).contains(&status) {
        return Err(BackendError::with_matrix_error(
            errcode.map(str::to_owned),
            Some(status),
            format!("GET /{path} answered {status}"),
        ));
    }
    Ok(Some(
        body.get(field)
            .cloned()
            .unwrap_or_else(|| Value::Array(Vec::new())),
    ))
}

fn error(error: impl std::fmt::Display) -> BackendError {
    BackendError::new(error.to_string())
}
