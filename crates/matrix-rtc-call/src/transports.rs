// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Which transport a join publishes on, from the join's [`JoinTransport`] and
//! what the homeserver advertises. The call only speaks LiveKit, so every
//! member says it can subscribe to `livekit`.

use matrix_rtc_core::{
    CommandError, LiveKitTransport, MatrixBackend, RtcTransport, TransportIntent,
};
use serde_json::Value;

/// The MSC4195 transport type: the only one the call publishes or receives on.
pub const LIVEKIT_TRANSPORT_TYPE: &str = "livekit";

/// What a join does with transports.
#[derive(Clone, Debug, Default)]
pub enum JoinTransport {
    /// Publish on the first LiveKit transport the homeserver advertises
    /// (MSC4143 `rtc_transports`); the join fails if there is none.
    #[default]
    Advertised,

    /// Publish on this LiveKit focus, whatever the homeserver advertises.
    Publish(LiveKitTransport),

    /// Publish nothing and only receive, as a recorder or other observer
    /// does. The membership still says it can subscribe to LiveKit.
    ReceiveOnly,
}

impl From<LiveKitTransport> for JoinTransport {
    fn from(transport: LiveKitTransport) -> Self {
        Self::Publish(transport)
    }
}

fn publish(livekit_service_url: &str) -> TransportIntent {
    TransportIntent::Publish(RtcTransport::LiveKit(LiveKitTransport {
        livekit_service_url: livekit_service_url.to_owned(),
    }))
}

/// The join's own choice wins; [`JoinTransport::Advertised`] takes the first
/// LiveKit entry of the homeserver's `rtc_transports` array, which MSC4143
/// orders by preference. No entry is an error rather than a guess.
pub fn choose(
    rtc_transports: &Value,
    chosen: JoinTransport,
) -> Result<TransportIntent, CommandError> {
    match chosen {
        JoinTransport::Publish(transport) => {
            Ok(TransportIntent::Publish(RtcTransport::LiveKit(transport)))
        }
        JoinTransport::ReceiveOnly => Ok(TransportIntent::ReceiveOnly {
            can_subscribe: vec![LIVEKIT_TRANSPORT_TYPE.to_owned()],
        }),
        JoinTransport::Advertised => rtc_transports
            .as_array()
            .into_iter()
            .flatten()
            .find(|entry| entry.get("type").and_then(Value::as_str) == Some(LIVEKIT_TRANSPORT_TYPE))
            .and_then(|entry| entry.get("livekit_service_url")?.as_str())
            .map(publish)
            .ok_or_else(|| {
                CommandError::from_message("the homeserver advertises no livekit transport")
            }),
    }
}

/// [`choose`], asking the backend for the homeserver's transports only for
/// [`JoinTransport::Advertised`]: a failing or missing endpoint must not fail a
/// join that does not use it.
pub async fn resolve<B: MatrixBackend + ?Sized>(
    backend: &B,
    chosen: JoinTransport,
) -> Result<TransportIntent, CommandError> {
    let rtc_transports = match chosen {
        JoinTransport::Advertised => backend.rtc_transports().await?,
        _ => Value::Null,
    };
    choose(&rtc_transports, chosen)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use matrix_rtc_core::{BackendError, testing::MockBackend};
    use serde_json::json;

    use super::*;

    fn livekit(url: &str) -> JoinTransport {
        JoinTransport::Publish(LiveKitTransport {
            livekit_service_url: url.to_owned(),
        })
    }

    #[tokio::test]
    async fn a_join_that_names_a_transport_does_not_ask_the_homeserver() {
        let backend = MockBackend::new();
        *backend.transports.lock().unwrap() = Err(BackendError::new("homeserver down"));

        let chosen = resolve(&backend, livekit("https://override")).await;
        assert!(matches!(
            chosen.unwrap(),
            TransportIntent::Publish(RtcTransport::LiveKit(LiveKitTransport { livekit_service_url }))
                if livekit_service_url == "https://override"
        ));

        assert!(resolve(&backend, JoinTransport::ReceiveOnly).await.is_ok());
        assert_eq!(backend.transports_requests.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn an_advertised_join_asks_the_homeserver_and_its_failure_fails_the_join() {
        let backend = MockBackend::new();
        *backend.transports.lock().unwrap() =
            Ok(json!([{ "type": "livekit", "livekit_service_url": "https://advertised" }]));
        assert!(matches!(
            resolve(&backend, JoinTransport::Advertised).await.unwrap(),
            TransportIntent::Publish(RtcTransport::LiveKit(LiveKitTransport { livekit_service_url }))
                if livekit_service_url == "https://advertised"
        ));

        *backend.transports.lock().unwrap() = Err(BackendError::new("homeserver down"));
        assert!(resolve(&backend, JoinTransport::Advertised).await.is_err());
    }

    #[test]
    fn the_first_livekit_entry_is_taken_in_homeserver_order() {
        let transports = json!([
            { "type": "org.example.other", "url": "x" },
            { "type": "livekit", "livekit_service_url": "https://first.example.org" },
            { "type": "livekit", "livekit_service_url": "https://second.example.org" },
        ]);
        let TransportIntent::Publish(RtcTransport::LiveKit(livekit)) =
            choose(&transports, JoinTransport::Advertised).unwrap()
        else {
            panic!("a livekit transport");
        };
        assert_eq!(livekit.livekit_service_url, "https://first.example.org");
    }

    #[test]
    fn a_receive_only_join_publishes_nothing_and_subscribes_to_livekit() {
        let transports = json!([{ "type": "livekit", "livekit_service_url": "https://a" }]);
        let TransportIntent::ReceiveOnly { can_subscribe } =
            choose(&transports, JoinTransport::ReceiveOnly).unwrap()
        else {
            panic!("receive only");
        };
        assert_eq!(can_subscribe, ["livekit"]);
    }

    #[test]
    fn an_advertised_join_without_a_livekit_transport_fails() {
        assert!(choose(&json!([]), JoinTransport::Advertised).is_err());
        assert!(choose(&Value::Null, JoinTransport::Advertised).is_err());
    }
}
