// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Native tests of the surface, with Rust implementations of the foreign
//! traits: what a TypeScript host does, minus the generated glue.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::*;

const ROOM: &str = "!room:example.org";
const SLOT: &str = "m.call#ROOM";
const ALICE: &str = "@alice:example.org";
const ALICE_DEV: &str = "ALICEDEV";
const BOB: &str = "@bob:example.org";
const BOB_DEV: &str = "BOBDEV";
const URL: &str = "https://sfu.example.org/jwt";

/// Records every outbound command; answers like a homeserver that accepts
/// everything.
#[derive(Default)]
struct Host {
    sticky: Mutex<Vec<(String, String, String, u64)>>,
    delayed: Mutex<Vec<(String, String, u64)>>,
    restarted: Mutex<Vec<String>>,
    cancelled: Mutex<Vec<String>>,
    to_device: Mutex<Vec<(Vec<FfiToDeviceRecipient>, String, String)>>,
    state: Mutex<Vec<(String, String, String)>>,
}

#[async_trait]
impl RtcCommandSenderCallback for Host {
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content_json: String,
        duration_ms: u64,
    ) -> Result<String, CommandSenderError> {
        let mut sent = self.sticky.lock().unwrap();
        sent.push((room_id, event_type, content_json, duration_ms));
        Ok(format!("$sticky-{}", sent.len()))
    }
    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        _content_json: String,
        delay_ms: u64,
    ) -> Result<String, CommandSenderError> {
        let mut sent = self.delayed.lock().unwrap();
        sent.push((room_id, event_type, delay_ms));
        Ok(format!("delay-{}", sent.len()))
    }
    async fn restart_delayed_event(
        &self,
        _room_id: String,
        delay_id: String,
    ) -> Result<(), CommandSenderError> {
        self.restarted.lock().unwrap().push(delay_id);
        Ok(())
    }
    async fn cancel_delayed_event(
        &self,
        _room_id: String,
        delay_id: String,
    ) -> Result<(), CommandSenderError> {
        self.cancelled.lock().unwrap().push(delay_id);
        Ok(())
    }
    async fn send_to_device_message(
        &self,
        recipients: Vec<FfiToDeviceRecipient>,
        message_type: String,
        content_json: String,
    ) -> Result<Vec<FfiToDeviceDelivery>, CommandSenderError> {
        let deliveries = recipients
            .iter()
            .map(|recipient| FfiToDeviceDelivery {
                user_id: recipient.user_id.clone(),
                device_id: recipient.device_id.clone(),
                error: None,
            })
            .collect();
        self.to_device
            .lock()
            .unwrap()
            .push((recipients, message_type, content_json));
        Ok(deliveries)
    }
    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        _content_json: String,
    ) -> Result<String, CommandSenderError> {
        self.state
            .lock()
            .unwrap()
            .push((room_id, event_type, state_key));
        Ok("$state-1".to_owned())
    }
}

#[derive(Default)]
struct Recorder {
    memberships: Mutex<Vec<Vec<String>>>,
    statuses: Mutex<Vec<FfiStatus>>,
    key_maps: Mutex<Vec<Vec<String>>>,
    transports: Mutex<Vec<Vec<FfiTransportWithMembers>>>,
}

impl MembershipsListener for Recorder {
    fn on_memberships_change(&self, memberships: Vec<FfiMembership>) {
        self.memberships
            .lock()
            .unwrap()
            .push(memberships.into_iter().map(|m| m.member_id).collect());
    }
}
impl StatusListener for Recorder {
    fn on_status_change(&self, status: FfiStatus) {
        self.statuses.lock().unwrap().push(status);
    }
}
impl KeyMapListener for Recorder {
    fn on_key_map_change(&self, key_map: Vec<FfiMediaKey>) {
        self.key_maps
            .lock()
            .unwrap()
            .push(key_map.into_iter().map(|k| k.member_id).collect());
    }
}
impl TransportsListener for Recorder {
    fn on_transports_change(&self, transports: Vec<FfiTransportWithMembers>) {
        self.transports.lock().unwrap().push(transports);
    }
}

fn sticky_event(sender: &str, device: &str, member_id: &str) -> FfiStickyEvent {
    FfiStickyEvent {
        room_id: ROOM.to_owned(),
        event_id: Some(format!("$event-{member_id}")),
        sender: sender.to_owned(),
        sender_device_id: Some(device.to_owned()),
        was_encrypted: Some(true),
        event_type: "org.matrix.msc4143.rtc.member".to_owned(),
        content_json: serde_json::json!({
            "slot_id": SLOT,
            "msc4354_sticky_key": member_id,
            "member": { "id": member_id, "membership": "join" },
            "application": { "type": "m.call" },
            "transports": {
                "published": [{ "type": "livekit", "livekit_service_url": URL }],
                "can_subscribe": ["livekit"],
            },
        })
        .to_string(),
    }
}

fn encrypted_slot() -> FfiSlotEvent {
    FfiSlotEvent {
        room_id: ROOM.to_owned(),
        slot_id: SLOT.to_owned(),
        content_json: r#"{ "status": "open", "application": { "type": "m.call" },
                           "encryption": { "type": "m.per_member" } }"#
            .to_owned(),
    }
}

async fn setup() -> (
    Arc<Host>,
    Arc<RtcSessionManager>,
    Arc<Participation>,
    Arc<Recorder>,
) {
    let host = Arc::new(Host::default());
    let manager = RtcSessionManager::new(host.clone());
    manager
        .on_room_encryption_received(ROOM.to_owned(), true)
        .await;
    manager
        .on_room_slots_received(ROOM.to_owned(), vec![encrypted_slot()])
        .await
        .unwrap();
    let participation = manager.participation(
        ROOM.to_owned(),
        SLOT.to_owned(),
        ALICE.to_owned(),
        ALICE_DEV.to_owned(),
    );
    let recorder = Arc::new(Recorder::default());
    participation.set_status_listener(recorder.clone()).await;
    participation
        .set_memberships_listener(recorder.clone())
        .await;
    participation.set_key_map_listener(recorder.clone()).await;
    participation
        .set_transports_listener(recorder.clone())
        .await;
    (host, manager, participation, recorder)
}

fn join_params() -> (FfiTransportIntent, FfiJoinParams) {
    (
        FfiTransportIntent::Publish {
            transport: FfiRtcTransport::LiveKit {
                livekit_service_url: URL.to_owned(),
            },
        },
        FfiJoinParams {
            application: "m.call".to_owned(),
            member_id: Some("alice-a".to_owned()),
            keep_alive_timeout_ms: None,
            sticky_duration_ms: None,
            degraded_lifetime_ms: None,
            encryption: None,
        },
    )
}

#[tokio::test]
async fn installing_listeners_replays_the_disconnected_state_once_each() {
    let (_, _, _, recorder) = setup().await;
    assert_eq!(
        recorder.statuses.lock().unwrap().as_slice(),
        [FfiStatus::Disconnected {
            cause: FfiDisconnectCause::NeverJoined
        }]
    );
    assert_eq!(recorder.memberships.lock().unwrap().len(), 1);
    assert_eq!(recorder.key_maps.lock().unwrap().len(), 1);
    assert_eq!(recorder.transports.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn join_walks_joining_then_connected_and_arms_the_delayed_leave_first() {
    let (host, _, participation, recorder) = setup().await;
    let (intent, params) = join_params();

    let member_id = participation.join(intent, params).await.expect("join");
    assert_eq!(member_id, "alice-a");

    let statuses = recorder.statuses.lock().unwrap().clone();
    assert!(
        matches!(statuses[1], FfiStatus::Joining { .. }),
        "{statuses:?}"
    );
    let FfiStatus::Connected {
        member_id,
        keep_alive,
        ..
    } = &statuses[2]
    else {
        panic!("expected Connected, got {:?}", statuses[2]);
    };
    assert_eq!(member_id, "alice-a");
    assert!(matches!(keep_alive, FfiKeepAlive::Armed { .. }));

    let delayed = host.delayed.lock().unwrap().clone();
    let sticky = host.sticky.lock().unwrap().clone();
    assert_eq!(delayed.len(), 1, "one delayed leave armed");
    assert_eq!(
        delayed[0].1, "org.matrix.msc4143.rtc.member",
        "the wire type reaches the host"
    );
    assert_eq!(sticky.len(), 1, "one membership published");
    assert!(
        sticky[0].2.contains("\"membership\":\"join\""),
        "{}",
        sticky[0].2
    );

    let key_map = participation.key_map().await;
    assert_eq!(key_map.len(), 1);
    assert_eq!(key_map[0].member_id, "alice-a");
    assert_eq!(key_map[0].key.len(), 32);
}

#[tokio::test]
async fn the_roster_lands_as_memberships_and_transports_with_identities() {
    let (host, manager, participation, recorder) = setup().await;
    let (intent, params) = join_params();
    participation.join(intent, params).await.expect("join");
    let before = recorder.memberships.lock().unwrap().len();

    manager
        .set_current_sticky_state(
            ROOM.to_owned(),
            vec![
                sticky_event(BOB, BOB_DEV, "bob-a"),
                sticky_event(ALICE, ALICE_DEV, "alice-a"),
            ],
        )
        .await
        .expect("sticky state");

    let memberships = participation.memberships().await;
    let ids: Vec<&str> = memberships.iter().map(|m| m.member_id.as_str()).collect();
    assert_eq!(ids, ["alice-a", "bob-a"]);
    assert!(memberships[0].is_own);
    assert!(
        memberships[1].transport_identity.is_some(),
        "the MSC4195 identity mapper was installed at join"
    );
    assert_eq!(
        memberships[1].media_key,
        Some(FfiMediaKeyState {
            holds_our_key: true,
            have_their_key: false,
            rejection: None,
        })
    );
    assert!(
        host.to_device
            .lock()
            .unwrap()
            .iter()
            .any(|(recipients, _, _)| recipients
                .iter()
                .any(|r| r.user_id == BOB && r.device_id == BOB_DEV)),
        "our key went to bob's device"
    );

    let transports = participation.transports().await;
    assert_eq!(
        transports,
        vec![FfiTransportWithMembers {
            transport: FfiRtcTransport::LiveKit {
                livekit_service_url: URL.to_owned()
            },
            member_ids: vec!["alice-a".to_owned(), "bob-a".to_owned()],
        }]
    );
    assert_eq!(
        recorder.memberships.lock().unwrap().len(),
        before + 1,
        "one memberships change for the whole snapshot"
    );

    // The same snapshot again changes nothing.
    manager
        .set_current_sticky_state(
            ROOM.to_owned(),
            vec![
                sticky_event(BOB, BOB_DEV, "bob-a"),
                sticky_event(ALICE, ALICE_DEV, "alice-a"),
            ],
        )
        .await
        .expect("sticky state");
    assert_eq!(recorder.memberships.lock().unwrap().len(), before + 1);
}

#[tokio::test]
async fn heartbeat_restarts_the_delayed_leave_and_leave_cancels_it() {
    let (host, _, participation, recorder) = setup().await;
    let (intent, params) = join_params();
    participation.join(intent, params).await.expect("join");

    assert!(participation.heartbeat().await);
    assert_eq!(
        host.restarted.lock().unwrap().as_slice(),
        ["delay-1".to_owned()]
    );

    participation
        .leave(FfiLeaveParams::default())
        .await
        .expect("leave");
    assert_eq!(
        host.cancelled.lock().unwrap().as_slice(),
        ["delay-1".to_owned()]
    );
    assert_eq!(
        recorder.statuses.lock().unwrap().last(),
        Some(&FfiStatus::Disconnected {
            cause: FfiDisconnectCause::LeftByHost {
                reason: None,
                delayed_leave: Some(FfiDelayedLeaveOutcome::Cancelled),
            }
        })
    );
    assert!(participation.key_map().await.is_empty());
    assert!(!participation.heartbeat().await, "not joined any more");
}

#[tokio::test]
async fn a_malformed_input_is_an_error_not_a_panic() {
    let (_, manager, _, _) = setup().await;
    let mut bad = sticky_event(BOB, BOB_DEV, "bob-a");
    bad.content_json = "{ not json".to_owned();
    assert!(matches!(
        manager
            .set_current_sticky_state(ROOM.to_owned(), vec![bad])
            .await,
        Err(RtcError::InvalidInput(_))
    ));
}

#[test]
fn impairment_severity_matches_the_core_table() {
    assert_eq!(
        impairment_severity(FfiImpairment::KeepAliveExpired { since_ts: 0 }),
        FfiSeverity::Critical
    );
    assert_eq!(
        impairment_severity(FfiImpairment::MediaKeyNotReceived { member_ids: vec![] }),
        FfiSeverity::Degraded
    );
}
