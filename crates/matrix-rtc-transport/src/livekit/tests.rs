// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

use matrix_rtc_core::{IdentitySubject, LiveKitTransport, RtcTransport};

use super::*;

const ALICE: &str = "@alice:example.com";

fn both() -> LiveKitTransport {
    LiveKitTransport::new(
        Some("wss://sfu.example.com".into()),
        Some("https://jwt.example.com".into()),
        None,
    )
    .unwrap()
}

fn subject<'a>(transports: &'a [RtcTransport], device: Option<&'a str>) -> IdentitySubject<'a> {
    IdentitySubject {
        user_id: ALICE,
        device_id: device,
        member_id: "memberABC",
        transports,
    }
}

#[test]
fn a_named_service_selects_sfu_get_even_alongside_a_url() {
    for transport in [LiveKitTransport::service("https://jwt.example.com"), both()] {
        assert_eq!(
            LiveKitTarget::for_transport(&transport, ALICE),
            LiveKitTarget::SfuGet {
                service_url: "https://jwt.example.com".into()
            }
        );
    }
}

#[test]
fn a_url_alone_selects_the_homeserver_for_the_publishers_server() {
    assert_eq!(
        LiveKitTarget::for_transport(&LiveKitTransport::sfu("wss://sfu.example.com"), ALICE),
        LiveKitTarget::CsApi {
            url: "wss://sfu.example.com".into(),
            server_name: "example.com".into(),
        }
    );
    let remote = LiveKitTransport::new(
        Some("wss://sfu.other.org".into()),
        None,
        Some("other.org".into()),
    )
    .unwrap();
    assert_eq!(
        LiveKitTarget::for_transport(&remote, ALICE),
        LiveKitTarget::CsApi {
            url: "wss://sfu.other.org".into(),
            server_name: "other.org".into(),
        }
    );
}

#[test]
fn a_key_round_trips_to_its_target() {
    for target in [
        LiveKitTarget::for_transport(&both(), ALICE),
        LiveKitTarget::for_transport(&LiveKitTransport::sfu("wss://sfu.example.com"), ALICE),
    ] {
        assert_eq!(
            LiveKitTarget::from_connection_key(&target.connection_key()),
            Some(target)
        );
    }
}

/// Element Call's `keyFor`: one SFU reached through one flow is one
/// connection, so we never join it twice under the same identity.
#[test]
fn members_share_a_connection_exactly_when_their_flow_and_its_endpoint_agree() {
    let service = RtcTransport::LiveKit(LiveKitTransport::service("https://jwt.example.com"));
    let transitional = RtcTransport::LiveKit(both());
    assert_eq!(
        connection_key(&service, ALICE),
        connection_key(&transitional, "@bob:other.org")
    );

    let sfu = RtcTransport::LiveKit(LiveKitTransport::sfu("wss://sfu.example.com"));
    assert_ne!(
        connection_key(&sfu, ALICE),
        connection_key(&sfu, "@bob:other.org"),
        "the homeserver asked for the token differs"
    );
    assert_ne!(
        connection_key(&sfu, ALICE),
        connection_key(&transitional, ALICE)
    );
}

#[test]
fn the_identity_follows_the_flow_of_the_first_livekit_transport() {
    let transitional = [RtcTransport::LiveKit(both())];
    assert_eq!(
        identity_for(&subject(&transitional, Some("DEVICE"))).as_deref(),
        Some("@alice:example.com:DEVICE")
    );
    // `/sfu/get` mints the device into the identity: no device, no identity.
    assert_eq!(identity_for(&subject(&transitional, None)), None);

    let sfu = [RtcTransport::LiveKit(LiveKitTransport::sfu(
        "wss://sfu.example.com",
    ))];
    assert_eq!(
        identity_for(&subject(&sfu, None)).as_deref(),
        Some("M3VnsLXrkbxIDINXO64u8PZzFyD9ZYFiQULR2dshWYs")
    );

    assert_eq!(identity_for(&subject(&[], Some("DEVICE"))), None);
}

#[test]
fn our_identity_on_a_target_is_what_its_flow_mints() {
    let sfu_get = LiveKitTarget::for_transport(&both(), ALICE);
    let cs_api =
        LiveKitTarget::for_transport(&LiveKitTransport::sfu("wss://sfu.example.com"), ALICE);
    assert_eq!(
        sfu_get.own_identity(ALICE, "DEVICE", "memberABC"),
        "@alice:example.com:DEVICE"
    );
    assert_eq!(
        cs_api.own_identity(ALICE, "DEVICE", "memberABC"),
        "M3VnsLXrkbxIDINXO64u8PZzFyD9ZYFiQULR2dshWYs"
    );
    assert!(!sfu_get.same_flow(&cs_api));
}
