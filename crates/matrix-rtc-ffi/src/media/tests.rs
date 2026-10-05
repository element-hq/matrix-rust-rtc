// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! In-process smoke tests of the media FFI surface, driving the exported
//! functions exactly as a host would (the uniffi traits are implemented in
//! Rust here). Run with `cargo test -p matrix-rtc-ffi --features media` —
//! they need the libwebrtc build, but no SFU and no homeserver.

use crate::backend::test_support::MockHost;
use crate::backend::{FfiEventEncryption, FfiEventIn};
use crate::params::FfiJoinTransport;
use crate::{
    FfiJoinSessionParams, FfiLeaveSessionParams, FfiRoomOptions, RtcCall, RtcClient, RtcRoom,
};
use std::sync::Arc;

use super::session::{MediaSessionConfig, connect_media_session};
use super::types::{
    FfiLocalState, FfiMediaConstraints, FfiStreamKind, FfiStreamRef, FfiTileId, FfiTileRoster,
    FfiVideoDetail, zip_stream_stats,
};
use super::{MediaFfiError, runtime};

/// TCP port 9 (discard) is reliably closed on dev machines: connections are
/// refused immediately, keeping the failure path fast.
const DEAD_SFU_URL: &str = "http://127.0.0.1:9";

fn config() -> MediaSessionConfig {
    MediaSessionConfig::default()
}

fn dead_sfu() -> FfiJoinTransport {
    FfiJoinTransport::Publish {
        livekit_service_url: DEAD_SFU_URL.to_owned(),
    }
}

/// Opens the room with an open slot, the way a host does on subscribe, and
/// joins it with `transport`. The room comes back too: dropping it would end
/// the subscription.
fn joined_call(mock: &Arc<MockHost>, transport: FfiJoinTransport) -> (Arc<RtcRoom>, Arc<RtcCall>) {
    let client = RtcClient::new(mock.clone());
    runtime().block_on(async {
        let open = client
            .clone()
            .room("!room:example.org".to_owned(), FfiRoomOptions::default());
        tokio::pin!(open);
        let seed = async {
            while mock.subjects("!room:example.org").is_none() {
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
            let sink = mock.room_sink("!room:example.org");
            sink.on_encryption(false);
            sink.on_state_events(
                matrix_rtc_core::SLOT_EVENT_TYPE.to_owned(),
                vec![FfiEventIn {
                    event_id: "$slot".to_owned(),
                    sender: "@admin:example.org".to_owned(),
                    event_type: matrix_rtc_core::SLOT_EVENT_TYPE.to_owned(),
                    state_key: Some("m.call#room".to_owned()),
                    origin_server_ts: 1,
                    content_json: r#"{"status":"open","application":{"type":"m.call"}}"#.to_owned(),
                    encryption: FfiEventEncryption {
                        encrypted: false,
                        sender_device_id: None,
                        sender_cross_signed: None,
                    },
                }],
            );
            sink.on_joined_members(vec!["@alice:example.org".to_owned()]);
            sink.on_sticky_events(Vec::new());
        };
        let (room, ()) = tokio::join!(open, seed);
        let room = room.unwrap();
        let call = room
            .clone()
            .join_call(FfiJoinSessionParams {
                application_slot_id: None,
                transport,
                keep_alive_timeout_ms: None,
                sticky_duration_ms: None,
                degraded_lifetime_ms: None,
                encryption_config: None,
                notify: None,
                reactions: None,
            })
            .await
            .unwrap();
        (room, call)
    })
}

#[test]
fn connect_requires_a_live_call() {
    let mock = MockHost::new();
    let (_room, call) = joined_call(&mock, dead_sfu());
    runtime()
        .block_on(call.leave(FfiLeaveSessionParams { leave_reason: None }))
        .unwrap();
    let result = runtime().block_on(connect_media_session(call, config()));
    assert!(
        matches!(result, Err(MediaFfiError::NotJoined(_))),
        "connecting media on a call that has left must fail with NotJoined",
    );
}

#[test]
fn wiring_reaches_the_transport_and_fails_cleanly_without_an_sfu() {
    let mock = MockHost::new();
    let (_room, call) = joined_call(&mock, dead_sfu());

    // Everything up to the SFU works — key bridge registration, engine
    // startup, and the (Rust-implemented) backend's token call — and the dead
    // endpoint surfaces as a clean Transport error, not a hang or a panic.
    let result = runtime().block_on(connect_media_session(call, config()));
    assert!(
        matches!(result, Err(MediaFfiError::Transport(_))),
        "expected a Transport error from the dead SFU endpoint, got {:?}",
        result.as_ref().err(),
    );
}

#[test]
fn an_advertised_join_connects_to_the_advertised_focus() {
    let mock = MockHost::new();
    *mock.transports_json.lock().unwrap() =
        serde_json::json!([{ "type": "livekit", "livekit_service_url": DEAD_SFU_URL }]).to_string();
    let (_room, call) = joined_call(&mock, FfiJoinTransport::Advertised);

    // The host passed no focus: reaching the dead one means it came from the call.
    let result = runtime().block_on(connect_media_session(call, config()));
    assert!(
        matches!(result, Err(MediaFfiError::Transport(_))),
        "expected a Transport error from the advertised dead SFU, got {:?}",
        result.as_ref().err(),
    );
}

#[test]
fn a_receive_only_call_connects_without_a_focus_of_its_own() {
    let mock = MockHost::new();
    let (_room, call) = joined_call(&mock, FfiJoinTransport::ReceiveOnly);

    let session = runtime()
        .block_on(connect_media_session(call, config()))
        .expect("no own focus to connect to");
    runtime()
        .block_on(session.disconnect())
        .expect("disconnect");
}

#[test]
fn constraint_dtos_fold_like_the_core_model() {
    let constraints: matrix_rtc_transport::MediaConstraints = FfiMediaConstraints {
        enabled: true,
        visible: false,
        detail: FfiVideoDetail::Dimensions {
            width: 320,
            height: 180,
        },
        low_bandwidth: false,
    }
    .into();

    assert!(matches!(
        constraints.detail,
        matrix_rtc_transport::VideoDetail::Dimensions(d) if d.width == 320 && d.height == 180
    ));
    let resolved = constraints.resolve(matrix_rtc_transport::MediaStreamKind::Camera);
    assert_eq!(resolved.demand, matrix_rtc_transport::StreamDemand::Paused);
}

// ---- tile DTOs (spec 002) ------------------------------------------------------

fn participant(id: &str) -> matrix_rtc_media::Participant {
    matrix_rtc_media::Participant {
        member_id: id.to_owned(),
        user_id: format!("@{id}:example.org"),
        device_id: None,
        is_local: false,
        reachable: true,
        streams: vec![],
        hand_raised_at_ms: None,
        joined_at_ms: None,
    }
}

#[test]
fn tile_id_round_trips_through_the_ffi() {
    use matrix_rtc_media::TileKind::{Person, ScreenShare};
    for kind in [Person, ScreenShare] {
        let id = matrix_rtc_media::TileId {
            member_id: "m".to_owned(),
            kind,
        };
        let back: matrix_rtc_media::TileId = FfiTileId::from(id.clone()).into();
        assert_eq!(back, id);
    }
}

#[test]
fn stream_ref_converts_every_kind() {
    use FfiStreamKind::{Camera, Data, Microphone, ScreenShare, ScreenShareAudio};
    for kind in [Microphone, Camera, ScreenShare, ScreenShareAudio, Data] {
        let (member_id, back): (String, matrix_rtc_transport::MediaStreamKind) = FfiStreamRef {
            member_id: "m".to_owned(),
            kind,
        }
        .into();
        assert_eq!(member_id, "m");
        assert_eq!(FfiStreamKind::from(back), kind);
    }
}

#[test]
fn stream_stats_dto_zips_request_with_results() {
    let request = vec![
        FfiStreamRef {
            member_id: "bob".to_owned(),
            kind: FfiStreamKind::Microphone,
        },
        FfiStreamRef {
            member_id: "bob".to_owned(),
            kind: FfiStreamKind::Camera,
        },
    ];
    let answered = matrix_rtc_transport::ReceiveStats {
        packets_received: 7,
        ..Default::default()
    };
    let dto = zip_stream_stats(request, vec![Some(answered), None]);

    assert_eq!(dto.len(), 2);
    assert_eq!(dto[0].member_id, "bob");
    assert_eq!(dto[0].kind, FfiStreamKind::Microphone);
    assert_eq!(dto[0].stats.as_ref().map(|s| s.packets_received), Some(7));
    assert_eq!(dto[1].kind, FfiStreamKind::Camera);
    // Asked about and answered with nothing, rather than left out.
    assert!(dto[1].stats.is_none());
}

#[test]
fn tile_roster_dto_preserves_order_and_detail() {
    let roster: Vec<_> = (0..5).map(|i| participant(&format!("m{i}"))).collect();
    let ranked =
        matrix_rtc_media::derive_tiles(&roster, &Default::default(), &Default::default()).remote;
    let last = ranked[4].id();
    let w = matrix_rtc_media::DetailWindow {
        offset: 1,
        len: 2,
        also: [last].into(),
    };
    let dto: FfiTileRoster = matrix_rtc_media::window(&ranked, &w).into();

    assert_eq!(dto.order.len(), 5, "order is never truncated");
    assert_eq!(dto.detail.len(), 3);
    // Detail joins to order by id: ranks 1 and 2, plus the named last tile.
    let detail_ids: Vec<&str> = dto.detail.iter().map(|t| t.member_id.as_str()).collect();
    let expected: Vec<&str> = [1usize, 2, 4]
        .iter()
        .map(|&i| dto.order[i].id.member_id.as_str())
        .collect();
    assert_eq!(detail_ids, expected);
}

#[test]
fn local_state_dto_carries_the_share_flag() {
    let mut me = participant("me");
    me.is_local = true;
    let own = matrix_rtc_media::derive_tiles(&[me], &Default::default(), &Default::default())
        .own
        .expect("own tile");
    let dto: FfiLocalState = matrix_rtc_media::LocalState {
        tile: own,
        is_screen_sharing: true,
    }
    .into();
    assert_eq!(dto.tile.member_id, "me");
    assert!(dto.is_screen_sharing);
}
