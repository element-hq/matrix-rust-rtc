// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Which transport a join publishes on, from the join's [`JoinTransport`] and
//! what the homeserver advertises. The call only speaks LiveKit, so every
//! member says it can subscribe to `livekit`.

use std::time::Duration;

use matrix_rtc_core::{
    BackendError, CommandError, JoinError, LiveKitTransport, MatrixBackend, Outbox, RtcTransport,
    SendKind, SendPolicy, SlotKey, TransportIntent, executor,
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
            // Final: the homeserver's configuration, not a failure to reach it.
            .ok_or_else(|| {
                BackendError::new("the homeserver advertises no livekit transport")
                    .permanent()
                    .into()
            }),
    }
}

/// [`choose`], asking the backend for the homeserver's transports only for
/// [`JoinTransport::Advertised`]: a failing or missing endpoint must not fail a
/// join that does not use it.
///
/// The read is the join's first step, so it runs under the join's budget
/// (034 R6): retried through `outbox` on a transient failure until
/// `deadline`, failed at once on a refusal.
pub async fn resolve<B: MatrixBackend + ?Sized>(
    backend: &B,
    outbox: &Outbox,
    room_id: &str,
    chosen: JoinTransport,
    deadline: executor::Instant,
) -> Result<TransportIntent, JoinError> {
    let rtc_transports = match chosen {
        JoinTransport::Advertised => outbox
            .send(
                SlotKey::new(room_id, SendKind::RtcTransports),
                SendPolicy::new().deadline(remaining(deadline)),
                || async {
                    backend
                        .rtc_transports()
                        .await
                        .map_err(CommandError::Backend)
                },
            )
            .await
            .map_err(JoinError::from_send_failure)?,
        _ => Value::Null,
    };
    choose(&rtc_transports, chosen).map_err(|error| JoinError::Refused {
        errcode: None,
        message: error.to_string(),
    })
}

fn remaining(deadline: executor::Instant) -> Duration {
    deadline.saturating_duration_since(executor::Instant::now())
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

        let chosen = resolve(
            &backend,
            &Outbox::new(),
            "!r",
            livekit("https://override"),
            soon(),
        )
        .await;
        assert!(matches!(
            chosen.unwrap(),
            TransportIntent::Publish(RtcTransport::LiveKit(LiveKitTransport { livekit_service_url }))
                if livekit_service_url == "https://override"
        ));

        assert!(
            resolve(
                &backend,
                &Outbox::new(),
                "!r",
                JoinTransport::ReceiveOnly,
                soon()
            )
            .await
            .is_ok()
        );
        assert_eq!(backend.transports_requests.load(Ordering::SeqCst), 0);
    }

    fn soon() -> executor::Instant {
        executor::Instant::now() + Duration::from_secs(30)
    }

    async fn advertised(backend: &MockBackend) -> Result<TransportIntent, JoinError> {
        resolve(
            backend,
            &Outbox::new(),
            "!r",
            JoinTransport::Advertised,
            soon(),
        )
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn an_advertised_join_asks_the_homeserver() {
        let backend = MockBackend::new();
        *backend.transports.lock().unwrap() =
            Ok(json!([{ "type": "livekit", "livekit_service_url": "https://advertised" }]));
        assert!(matches!(
            advertised(&backend).await.unwrap(),
            TransportIntent::Publish(RtcTransport::LiveKit(LiveKitTransport { livekit_service_url }))
                if livekit_service_url == "https://advertised"
        ));
    }

    /// R6: a transient failure of the read is retried inside the budget; one
    /// that lasts past it times the join out.
    #[tokio::test(start_paused = true)]
    async fn a_failing_read_is_retried_until_the_join_budget_runs_out() {
        let backend = MockBackend::new();
        *backend.transports.lock().unwrap() = Err(BackendError::new("homeserver down"));
        let started = executor::Instant::now();

        assert!(matches!(
            advertised(&backend).await,
            Err(JoinError::TimedOut)
        ));
        assert_eq!(started.elapsed(), Duration::from_secs(30));
        assert!(backend.transports_requests.load(Ordering::SeqCst) > 1);
    }

    /// R6: a refusal, or a homeserver with no LiveKit transport, fails the
    /// join at once.
    #[tokio::test(start_paused = true)]
    async fn a_refused_read_or_no_livekit_transport_fails_the_join_at_once() {
        let backend = MockBackend::new();
        *backend.transports.lock().unwrap() = Err(BackendError::with_matrix_error(
            Some("M_FORBIDDEN".to_owned()),
            Some(403),
            "no",
        ));
        assert!(matches!(
            advertised(&backend).await,
            Err(JoinError::Refused { .. })
        ));
        assert_eq!(backend.transports_requests.load(Ordering::SeqCst), 1);

        *backend.transports.lock().unwrap() = Ok(json!([]));
        let started = executor::Instant::now();
        assert!(matches!(
            advertised(&backend).await,
            Err(JoinError::Refused { .. })
        ));
        assert_eq!(started.elapsed(), Duration::ZERO);
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
