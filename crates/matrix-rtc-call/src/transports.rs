// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Which transport a join publishes on, from what the homeserver advertises.

use matrix_rtc_core::{
    CommandError, LiveKitTransport, MatrixBackend, RtcTransport, TransportIntent,
};
use serde_json::Value;

/// The join's own choice wins; otherwise the first LiveKit entry of the
/// homeserver's `rtc_transports` array, which MSC4143 orders by preference.
/// No entry and no choice is an error rather than a guess.
pub fn choose(
    rtc_transports: &Value,
    chosen: Option<TransportIntent>,
) -> Result<TransportIntent, CommandError> {
    if let Some(chosen) = chosen {
        return Ok(chosen);
    }

    rtc_transports
        .as_array()
        .into_iter()
        .flatten()
        .find(|entry| entry.get("type").and_then(Value::as_str) == Some("livekit"))
        .and_then(|entry| entry.get("livekit_service_url")?.as_str())
        .map(|url| {
            TransportIntent::Publish(RtcTransport::LiveKit(LiveKitTransport {
                livekit_service_url: url.to_owned(),
            }))
        })
        .ok_or_else(|| {
            CommandError::from_message(
                "the homeserver advertises no livekit transport and the join names none",
            )
        })
}

/// [`choose`], asking the backend for the homeserver's transports only when
/// the join names none: a failing or missing endpoint must not fail a join
/// whose own choice overrides it anyway.
pub async fn resolve<B: MatrixBackend + ?Sized>(
    backend: &B,
    chosen: Option<TransportIntent>,
) -> Result<TransportIntent, CommandError> {
    if let Some(chosen) = chosen {
        return Ok(chosen);
    }
    choose(&backend.rtc_transports().await?, None)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use matrix_rtc_core::{BackendError, testing::MockBackend};
    use serde_json::json;

    use super::*;

    fn livekit(url: &str) -> TransportIntent {
        TransportIntent::Publish(RtcTransport::LiveKit(LiveKitTransport {
            livekit_service_url: url.to_owned(),
        }))
    }

    #[tokio::test]
    async fn a_join_that_names_a_transport_does_not_ask_the_homeserver() {
        let backend = MockBackend::new();
        *backend.transports.lock().unwrap() = Err(BackendError::new("homeserver down"));

        let chosen = resolve(&backend, Some(livekit("https://override"))).await;

        assert!(matches!(
            chosen.unwrap(),
            TransportIntent::Publish(RtcTransport::LiveKit(LiveKitTransport { livekit_service_url }))
                if livekit_service_url == "https://override"
        ));
        assert_eq!(backend.transports_requests.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn without_a_choice_the_homeserver_decides_and_its_failure_fails_the_join() {
        let backend = MockBackend::new();
        *backend.transports.lock().unwrap() =
            Ok(json!([{ "type": "livekit", "livekit_service_url": "https://advertised" }]));
        assert!(matches!(
            resolve(&backend, None).await.unwrap(),
            TransportIntent::Publish(RtcTransport::LiveKit(LiveKitTransport { livekit_service_url }))
                if livekit_service_url == "https://advertised"
        ));

        *backend.transports.lock().unwrap() = Err(BackendError::new("homeserver down"));
        assert!(resolve(&backend, None).await.is_err());
    }

    #[test]
    fn the_first_livekit_entry_is_taken_in_homeserver_order() {
        let transports = json!([
            { "type": "org.example.other", "url": "x" },
            { "type": "livekit", "livekit_service_url": "https://first.example.org" },
            { "type": "livekit", "livekit_service_url": "https://second.example.org" },
        ]);
        let TransportIntent::Publish(RtcTransport::LiveKit(livekit)) =
            choose(&transports, None).unwrap()
        else {
            panic!("a livekit transport");
        };
        assert_eq!(livekit.livekit_service_url, "https://first.example.org");
    }

    #[test]
    fn a_join_that_names_a_transport_overrides_the_homeserver() {
        let chosen = TransportIntent::ReceiveOnly {
            can_subscribe: vec!["livekit".to_owned()],
        };
        let transports = json!([{ "type": "livekit", "livekit_service_url": "https://a" }]);
        assert!(matches!(
            choose(&transports, Some(chosen)).unwrap(),
            TransportIntent::ReceiveOnly { .. }
        ));
    }

    #[test]
    fn no_transport_and_no_choice_fails() {
        assert!(choose(&json!([]), None).is_err());
        assert!(choose(&Value::Null, None).is_err());
    }
}
