// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Which transport a join publishes on, from the join's [`JoinTransport`],
//! what the homeserver advertises and the room's format. The call only speaks
//! LiveKit, so every member says it can subscribe to it: `m.livekit` in MSC,
//! `livekit` in room-state (spec 038 R17, R39).

use std::time::Duration;

use matrix_rtc_core::compat::MembershipFormat;
use matrix_rtc_core::{
    CommandError, JoinError, LEGACY_LIVEKIT_TYPE, LiveKitTransport, MSC_LIVEKIT_TYPE,
    MatrixBackend, Outbox, RawRtcTransport, RtcTransport, SendKind, SendPolicy, SlotKey,
    TransportIntent, executor,
};
use serde_json::Value;

/// The LiveKit transport type a room in `format` publishes and subscribes
/// on.
pub fn livekit_transport_type(format: MembershipFormat) -> &'static str {
    match format {
        MembershipFormat::Msc => MSC_LIVEKIT_TYPE,
        MembershipFormat::RoomState => LEGACY_LIVEKIT_TYPE,
    }
}

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

/// The join's own choice wins; [`JoinTransport::Advertised`] takes the first
/// LiveKit entry of the homeserver's transports array, which MSC4143 orders by
/// preference, of the room's format's type: in MSC `m.livekit` with a `url`
/// (R17); in room-state `livekit`, with its `url`, `livekit_service_url` or
/// both, as Element Call web picks it (R39). No entry is an error rather than
/// a guess.
pub fn choose(
    rtc_transports: &Value,
    chosen: JoinTransport,
    format: MembershipFormat,
) -> Result<TransportIntent, JoinError> {
    let transport_type = livekit_transport_type(format);
    match chosen {
        JoinTransport::Publish(transport) => {
            Ok(TransportIntent::Publish(RtcTransport::LiveKit(transport)))
        }
        JoinTransport::ReceiveOnly => Ok(TransportIntent::ReceiveOnly {
            can_subscribe: vec![transport_type.to_owned()],
        }),
        JoinTransport::Advertised => rtc_transports
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|entry| serde_json::from_value::<RawRtcTransport>(entry.clone()).ok())
            .filter(|entry| entry.transport_type == transport_type)
            // In room-state, published as advertised: whether it names an SFU
            // `url`, the legacy `livekit_service_url`, or both decides how
            // every client gets its token for it, and so the identity peers
            // expect us under. In MSC, by its `url` alone.
            .find_map(|entry| match entry.into_typed_in(format) {
                RtcTransport::LiveKit(livekit) => {
                    Some(TransportIntent::Publish(RtcTransport::LiveKit(livekit)))
                }
                RtcTransport::Unsupported(_) => None,
            })
            // Final: the homeserver's configuration, not a failure to reach it
            // (R18).
            .ok_or_else(|| JoinError::NoLiveKitTransport {
                transport_type: transport_type.to_owned(),
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
    format: MembershipFormat,
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
    choose(&rtc_transports, chosen, format)
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
        JoinTransport::Publish(LiveKitTransport::service(url.to_owned()))
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
            MembershipFormat::RoomState,
            soon(),
        )
        .await;
        assert!(matches!(
            chosen.unwrap(),
            TransportIntent::Publish(RtcTransport::LiveKit(livekit))
                if livekit.livekit_service_url() == Some("https://override")
        ));

        assert!(
            resolve(
                &backend,
                &Outbox::new(),
                "!r",
                JoinTransport::ReceiveOnly,
                MembershipFormat::Msc,
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
            MembershipFormat::RoomState,
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
            TransportIntent::Publish(RtcTransport::LiveKit(livekit))
                if livekit.livekit_service_url() == Some("https://advertised")
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
        // R18: typed, naming the format's type.
        assert!(matches!(
            advertised(&backend).await,
            Err(JoinError::NoLiveKitTransport { transport_type }) if transport_type == "livekit"
        ));
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    fn advertised_in(transports: &Value, format: MembershipFormat) -> Option<LiveKitTransport> {
        match choose(transports, JoinTransport::Advertised, format).ok()? {
            TransportIntent::Publish(RtcTransport::LiveKit(livekit)) => Some(livekit),
            _ => None,
        }
    }

    #[test]
    fn the_first_entry_of_the_formats_type_is_taken_in_homeserver_order() {
        let transports = json!([
            { "type": "org.example.other", "url": "x" },
            { "type": "livekit", "livekit_service_url": "https://first.example.org" },
            { "type": "m.livekit", "livekit_service_url": "https://no-url.example.org" },
            { "type": "m.livekit", "url": "wss://first.example.org" },
            { "type": "livekit", "livekit_service_url": "https://second.example.org" },
            { "type": "m.livekit", "url": "wss://second.example.org" },
        ]);
        // R39: room-state takes the first `livekit`, as Element Call web does.
        assert_eq!(
            advertised_in(&transports, MembershipFormat::RoomState),
            Some(LiveKitTransport::service("https://first.example.org"))
        );
        // R17: MSC takes the first `m.livekit` naming an SFU.
        assert_eq!(
            advertised_in(&transports, MembershipFormat::Msc),
            Some(LiveKitTransport::sfu("wss://first.example.org"))
        );
    }

    /// Room-state publishes whatever fields the homeserver advertises: they
    /// decide how every client gets a token for the transport. MSC takes the
    /// `url` alone.
    #[test]
    fn room_state_takes_all_the_fields_and_msc_only_the_url() {
        let both = |transport_type: &str| {
            json!([{
                "type": transport_type,
                "url": "wss://sfu.example.org",
                "livekit_service_url": "https://jwt.example.org",
            }])
        };
        let legacy = advertised_in(&both("livekit"), MembershipFormat::RoomState).unwrap();
        assert_eq!(legacy.url(), Some("wss://sfu.example.org"));
        assert_eq!(
            legacy.livekit_service_url(),
            Some("https://jwt.example.org")
        );

        let msc = advertised_in(&both("m.livekit"), MembershipFormat::Msc).unwrap();
        assert_eq!(msc, LiveKitTransport::sfu("wss://sfu.example.org"));
    }

    #[test]
    fn a_receive_only_join_subscribes_to_its_formats_type() {
        for (format, expected) in [
            (MembershipFormat::Msc, "m.livekit"),
            (MembershipFormat::RoomState, "livekit"),
        ] {
            let TransportIntent::ReceiveOnly { can_subscribe } =
                choose(&Value::Null, JoinTransport::ReceiveOnly, format).unwrap()
            else {
                panic!("receive only");
            };
            assert_eq!(can_subscribe, [expected]);
        }
    }

    #[test]
    fn an_advertised_join_without_a_transport_of_its_formats_type_fails() {
        for format in [MembershipFormat::Msc, MembershipFormat::RoomState] {
            assert!(choose(&json!([]), JoinTransport::Advertised, format).is_err());
            assert!(choose(&Value::Null, JoinTransport::Advertised, format).is_err());
        }
        let legacy_only = json!([{ "type": "livekit", "url": "wss://sfu" }]);
        assert!(advertised_in(&legacy_only, MembershipFormat::Msc).is_none());
        let msc_only = json!([{ "type": "m.livekit", "url": "wss://sfu" }]);
        assert!(advertised_in(&msc_only, MembershipFormat::RoomState).is_none());
    }
}
