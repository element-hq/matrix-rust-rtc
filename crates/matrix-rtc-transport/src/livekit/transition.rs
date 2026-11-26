// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! TEMPORARY (spec 049): the move from the standalone JWT service's
//! `/sfu/get` to the homeserver's CS API `get_token`.
//!
//! While a homeserver announces a LiveKit transport with both `url` and
//! `livekit_service_url`, a member publishing it may have taken its token from
//! either flow, depending on its client version, so it may hold either
//! identity. Which flow we take ourselves is chosen per call ([`FlowChoice`]).
//! Everything that exists only for that is here, so that it goes away in one
//! piece once homeservers stop announcing `livekit_service_url`.

use std::collections::HashSet;
use std::sync::Mutex;

use matrix_rtc_core::{IdentitySubject, LiveKitFlows, LiveKitTransport, RtcIdentity};

use super::{LiveKitTarget, SfuToken, identity};

/// The identities a member may hold when its published transport names both
/// flows: [`RtcIdentity::Transition`]. `None` for a transport naming one flow.
/// When the device cannot be attributed, the `/sfu/get` identity is unknown and
/// only the hashed one is left.
pub fn identity_for_both(
    transport: &LiveKitTransport,
    subject: &IdentitySubject<'_>,
) -> Option<RtcIdentity> {
    transport.url()?;
    transport.livekit_service_url()?;
    let hashed = identity::msc4195_identity(subject.user_id, subject.member_id);
    let Some(device_id) = subject.device_id else {
        return Some(RtcIdentity::Single(hashed));
    };
    Some(RtcIdentity::Transition {
        hashed,
        legacy: identity::legacy_identity(subject.user_id, device_id),
    })
}

/// Which flow each of a call's LiveKit connections takes, for transports
/// naming both (R7, R8, R13, R14). One per call, shared by the transport's
/// connection keys, settled from the join's [`LiveKitFlows`].
#[derive(Debug, Default)]
pub struct FlowChoice {
    flows: LiveKitFlows,
    /// The SFU we publish on and our target there, once chosen: a subscription
    /// to that SFU reuses it, whatever its flow (R14).
    own: Mutex<Option<(String, LiveKitTarget)>>,
    /// Servers whose `get_token` failed us: their SFUs are subscribed through
    /// `/sfu/get` for the rest of the call (R13).
    get_token_failed: Mutex<HashSet<String>>,
    /// The probe's token, until our first own connect takes it.
    own_token: Mutex<Option<SfuToken>>,
}

impl FlowChoice {
    pub fn new(mut flows: LiveKitFlows) -> Self {
        let own_token = flows
            .own_token
            .take()
            .map(|(url, jwt)| SfuToken { jwt, url });
        Self {
            flows,
            own_token: Mutex::new(own_token),
            ..Self::default()
        }
    }

    /// The probe's token, once, for our own connection on `connection_key`
    /// when it goes through `get_token` to the SFU the probe asked about.
    pub fn take_own_token(&self, connection_key: &str) -> Option<SfuToken> {
        let Some(LiveKitTarget::CsApi { url, .. }) =
            LiveKitTarget::from_connection_key(connection_key)
        else {
            return None;
        };
        let mut own_token = self.own_token.lock().expect("flow choice poisoned");
        if own_token.as_ref().is_some_and(|token| token.url == url) {
            own_token.take()
        } else {
            None
        }
    }

    /// The target we publish on `transport` through: `get_token` when the
    /// join allowed it (R8), `/sfu/get` otherwise (R7).
    pub fn own_target(&self, transport: &LiveKitTransport, own_user_id: &str) -> LiveKitTarget {
        let target = match transport.url() {
            Some(url) if names_both(transport) && self.flows.get_token => LiveKitTarget::CsApi {
                url: url.to_owned(),
                server_name: transport.sfu_server_name(own_user_id).to_owned(),
            },
            _ => LiveKitTarget::for_transport(transport, own_user_id),
        };
        if let Some(url) = transport.url() {
            *self.own.lock().expect("flow choice poisoned") =
                Some((url.to_owned(), target.clone()));
        }
        target
    }

    /// The target we subscribe to `publisher_user_id`'s `transport` through:
    /// our own connection when it is on that SFU (R14), else `get_token` when
    /// the join settled on it and the SFU's server has not failed us (R13),
    /// else the flow the transport's shape selects.
    pub fn subscribe_target(
        &self,
        transport: &LiveKitTransport,
        publisher_user_id: &str,
    ) -> LiveKitTarget {
        let own = self.own.lock().expect("flow choice poisoned");
        if let (Some(url), Some((own_url, own_target))) = (transport.url(), own.as_ref())
            && url == own_url
        {
            return own_target.clone();
        }
        drop(own);
        if let Some(url) = transport.url()
            && names_both(transport)
            && self.flows.get_token
        {
            let server_name = transport.sfu_server_name(publisher_user_id);
            let failed = self.get_token_failed.lock().expect("flow choice poisoned");
            if !failed.contains(server_name) {
                return LiveKitTarget::CsApi {
                    url: url.to_owned(),
                    server_name: server_name.to_owned(),
                };
            }
        }
        LiveKitTarget::for_transport(transport, publisher_user_id)
    }

    /// A subscription on `connection_key` failed to connect. When it went
    /// through `get_token`, its server is subscribed through `/sfu/get` from
    /// now on, and `true` says its members may now map to another
    /// connection. The SFU's own failures count too: `/sfu/get` reaches the
    /// SFU through the peer's JWT service, and may get in where we did not.
    pub fn subscription_failed(&self, connection_key: &str) -> bool {
        let Some(LiveKitTarget::CsApi { server_name, .. }) =
            LiveKitTarget::from_connection_key(connection_key)
        else {
            return false;
        };
        let newly = self
            .get_token_failed
            .lock()
            .expect("flow choice poisoned")
            .insert(server_name.clone());
        if newly {
            log::warn!(
                "get_token for {server_name}'s SFU failed; subscribing to it through /sfu/get"
            );
        }
        newly
    }
}

fn names_both(transport: &LiveKitTransport) -> bool {
    transport.url().is_some() && transport.livekit_service_url().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ME: &str = "@me:a.org";
    const PEER: &str = "@peer:b.org";

    fn both(url: &str, service: &str) -> LiveKitTransport {
        LiveKitTransport::new(Some(url.into()), Some(service.into()), None).unwrap()
    }

    fn sfu_get(service: &str) -> LiveKitTarget {
        LiveKitTarget::SfuGet {
            service_url: service.into(),
        }
    }

    fn cs_api(url: &str, server_name: &str) -> LiveKitTarget {
        LiveKitTarget::CsApi {
            url: url.into(),
            server_name: server_name.into(),
        }
    }

    fn choice(get_token: bool) -> FlowChoice {
        FlowChoice::new(LiveKitFlows {
            get_token,
            own_token: None,
        })
    }

    #[test]
    fn the_probe_token_serves_our_first_own_connect_only() {
        let flows = FlowChoice::new(LiveKitFlows {
            get_token: true,
            own_token: Some(("wss://a".into(), "jwt".into())),
        });
        let own = flows
            .own_target(&both("wss://a", "https://jwt.a"), ME)
            .connection_key();
        assert_eq!(
            flows.take_own_token(&sfu_get("https://jwt.a").connection_key()),
            None
        );
        assert_eq!(
            flows.take_own_token(&own),
            Some(SfuToken {
                jwt: "jwt".into(),
                url: "wss://a".into()
            })
        );
        assert_eq!(flows.take_own_token(&own), None);
    }

    #[test]
    fn by_default_both_fields_stay_on_sfu_get() {
        let flows = FlowChoice::default();
        let peer = both("wss://b", "https://jwt.b");
        assert_eq!(
            flows.own_target(&both("wss://a", "https://jwt.a"), ME),
            sfu_get("https://jwt.a")
        );
        assert_eq!(
            flows.subscribe_target(&peer, PEER),
            sfu_get("https://jwt.b")
        );
    }

    #[test]
    fn we_publish_through_get_token_only_when_allowed() {
        let ours = both("wss://a", "https://jwt.a");
        assert_eq!(
            choice(false).own_target(&ours, ME),
            sfu_get("https://jwt.a")
        );
        assert_eq!(
            choice(true).own_target(&ours, ME),
            cs_api("wss://a", "a.org")
        );
    }

    /// R13; R14 for our own SFU.
    #[test]
    fn peers_are_subscribed_through_get_token_except_on_our_own_connection() {
        let flows = choice(true);
        let own = flows.own_target(&both("wss://a", "https://jwt.a"), ME);
        assert_eq!(
            flows.subscribe_target(&both("wss://b", "https://jwt.b"), PEER),
            cs_api("wss://b", "b.org")
        );
        assert_eq!(
            flows.subscribe_target(&both("wss://a", "https://jwt.a"), PEER),
            own
        );
        assert_eq!(
            flows.subscribe_target(&LiveKitTransport::sfu("wss://a"), PEER),
            own
        );
    }

    #[test]
    fn a_server_whose_get_token_failed_is_subscribed_through_sfu_get() {
        let flows = choice(true);
        let peer = both("wss://b", "https://jwt.b");
        let failed = flows.subscribe_target(&peer, PEER).connection_key();
        assert!(flows.subscription_failed(&failed));
        assert!(!flows.subscription_failed(&failed));
        assert_eq!(
            flows.subscribe_target(&peer, PEER),
            sfu_get("https://jwt.b")
        );
        assert!(!flows.subscription_failed(&sfu_get("https://jwt.b").connection_key()));
    }

    #[test]
    fn a_transport_naming_one_flow_keeps_it() {
        let flows = choice(true);
        let service = LiveKitTransport::service("https://jwt.b");
        let sfu = LiveKitTransport::sfu("wss://b");
        assert_eq!(
            flows.subscribe_target(&service, PEER),
            sfu_get("https://jwt.b")
        );
        assert_eq!(
            flows.subscribe_target(&sfu, PEER),
            cs_api("wss://b", "b.org")
        );
        assert_eq!(flows.own_target(&service, ME), sfu_get("https://jwt.b"));
    }
}
