// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! In-process smoke tests of the media FFI surface, driving the exported
//! functions exactly as a host would (the uniffi traits are implemented in
//! Rust here). Run with `cargo test -p matrix-rtc-ffi --features media` —
//! they need the libwebrtc build, but no SFU and no homeserver.

use std::sync::Arc;

use crate::commands::{
    CommandSenderCallback, CommandSenderError, FfiToDeviceDelivery, FfiToDeviceRecipient,
    FfiTransportConfig,
};
use crate::{FfiJoinSessionParams, RtcSessionManagerHandle};

use super::session::{MediaSessionConfig, connect_media_session};
use super::types::{
    FfiLocalState, FfiMediaConstraints, FfiOpenIdToken, FfiTileId, FfiTileRoster, FfiVideoDetail,
    OpenIdTokenProvider,
};
use super::{MediaFfiError, runtime};

/// A host command sender that accepts everything (the signalling side is not
/// under test here).
struct NoopCommands;

#[async_trait::async_trait]
impl CommandSenderCallback for NoopCommands {
    async fn send_sticky_event(
        &self,
        _room_id: String,
        _event_type: String,
        _content_json: String,
        _duration_ms: u64,
    ) -> Result<String, CommandSenderError> {
        Ok("$sticky-event-1".to_owned())
    }

    async fn send_delayed_event(
        &self,
        _room_id: String,
        _event_type: String,
        _content_json: String,
        _delay_ms: u64,
    ) -> Result<String, CommandSenderError> {
        Ok("delayed-event-1".to_owned())
    }

    async fn send_state_event(
        &self,
        _room_id: String,
        _event_type: String,
        _state_key: String,
        _content_json: String,
    ) -> Result<String, CommandSenderError> {
        Ok("$state-event-1".to_owned())
    }

    async fn send_delayed_state_event(
        &self,
        _room_id: String,
        _event_type: String,
        _state_key: String,
        _content_json: String,
        _delay_ms: u64,
    ) -> Result<String, CommandSenderError> {
        Ok("delayed-state-event-1".to_owned())
    }

    async fn send_room_event(
        &self,
        _room_id: String,
        _event_type: String,
        _content_json: String,
    ) -> Result<String, CommandSenderError> {
        Ok("$room-event-1".to_owned())
    }

    async fn redact_event(
        &self,
        _room_id: String,
        _event_id: String,
        _reason: Option<String>,
    ) -> Result<(), CommandSenderError> {
        Ok(())
    }

    async fn restart_delayed_event(
        &self,
        _room_id: String,
        _delay_id: String,
    ) -> Result<(), CommandSenderError> {
        Ok(())
    }

    async fn cancel_delayed_event(
        &self,
        _room_id: String,
        _delay_id: String,
    ) -> Result<(), CommandSenderError> {
        Ok(())
    }

    async fn send_to_device_message(
        &self,
        recipients: Vec<FfiToDeviceRecipient>,
        _message_type: String,
        _content_json: String,
    ) -> Result<Vec<FfiToDeviceDelivery>, CommandSenderError> {
        Ok(recipients
            .into_iter()
            .map(|r| FfiToDeviceDelivery {
                user_id: r.user_id,
                device_id: r.device_id,
                error: None,
            })
            .collect())
    }
}

/// A Rust-side implementation of the foreign token provider.
struct DummyTokens;

#[async_trait::async_trait]
impl OpenIdTokenProvider for DummyTokens {
    async fn get_open_id_token(&self) -> Result<FfiOpenIdToken, MediaFfiError> {
        Ok(FfiOpenIdToken {
            access_token: "token".to_owned(),
            token_type: "Bearer".to_owned(),
            matrix_server_name: "example.org".to_owned(),
            expires_in_secs: 3600,
        })
    }
}

/// TCP port 9 (discard) is reliably closed on dev machines: connections are
/// refused immediately, keeping the failure path fast.
const DEAD_SFU_URL: &str = "http://127.0.0.1:9";

fn config() -> MediaSessionConfig {
    MediaSessionConfig {
        room_id: "!room:example.org".to_owned(),
        slot_id: "m.call#ROOM".to_owned(),
        user_id: "@alice:example.org".to_owned(),
        device_id: "DEVICE".to_owned(),
        livekit_service_url: DEAD_SFU_URL.to_owned(),
    }
}

#[test]
fn connect_requires_a_joined_slot() {
    let manager = RtcSessionManagerHandle::new();
    let result = runtime().block_on(connect_media_session(
        manager,
        config(),
        Arc::new(DummyTokens),
    ));
    assert!(
        matches!(result, Err(MediaFfiError::NotJoined(_))),
        "connecting media on an unjoined slot must fail with NotJoined",
    );
}

#[test]
fn wiring_reaches_the_transport_and_fails_cleanly_without_an_sfu() {
    let manager = RtcSessionManagerHandle::new();
    runtime().block_on(async {
        manager
            .set_command_sender(Arc::new(NoopCommands))
            .await
            .unwrap();
        manager
            .join(FfiJoinSessionParams {
                user_id: "@alice:example.org".to_owned(),
                device_id: "DEVICE".to_owned(),
                room_id: "!room:example.org".to_owned(),
                slot_id: "m.call#ROOM".to_owned(),
                application: "m.call".to_owned(),
                transport: Some(FfiTransportConfig {
                    r#type: "livekit".to_owned(),
                    livekit_service_url: Some(DEAD_SFU_URL.to_owned()),
                }),
                can_subscribe: Vec::new(),
                keep_alive_timeout_ms: None,
                sticky_duration_ms: None,
                degraded_lifetime_ms: None,
                encryption_config: None,
                element_call_compat: None,
                notify: None,
                reactions: None,
            })
            .await
            .unwrap();
    });

    // Everything up to the SFU works — key bridge registration, engine
    // startup, and the (Rust-implemented) foreign token provider call — and
    // the dead endpoint surfaces as a clean Transport error, not a hang or
    // a panic.
    let result = runtime().block_on(connect_media_session(
        manager,
        config(),
        Arc::new(DummyTokens),
    ));
    assert!(
        matches!(result, Err(MediaFfiError::Transport(_))),
        "expected a Transport error from the dead SFU endpoint, got {:?}",
        result.as_ref().err(),
    );
}

#[test]
fn constraint_dtos_fold_like_the_core_model() {
    let constraints: matrix_rtc_media::MediaConstraints = FfiMediaConstraints {
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
        matrix_rtc_media::VideoDetail::Dimensions(d) if d.width == 320 && d.height == 180
    ));
    let resolved = constraints.resolve(matrix_rtc_media::MediaStreamKind::Camera);
    assert_eq!(resolved.demand, matrix_rtc_media::StreamDemand::Paused);
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
    use matrix_rtc_media::MediaStreamKind::{Camera, ScreenShare};
    for kind in [Camera, ScreenShare] {
        let id = matrix_rtc_media::TileId {
            member_id: "m".to_owned(),
            kind,
        };
        let back: matrix_rtc_media::TileId = FfiTileId::from(id.clone()).into();
        assert_eq!(back, id);
    }
}

#[test]
fn tile_roster_dto_preserves_order_and_detail() {
    let roster: Vec<_> = (0..5).map(|i| participant(&format!("m{i}"))).collect();
    let ranked = matrix_rtc_media::derive_tiles(&roster, &Default::default()).remote;
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
    let own = matrix_rtc_media::derive_tiles(&[me], &Default::default())
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
