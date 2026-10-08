// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The web media session: the participant roster and connection lifecycle over
//! livekit-js, layered on a call the page has already joined
//! ([`WasmRtcCall`]).
//!
//! The wiring is `matrix_rtc_call_sdk::attach_media`'s, shared with the FFI;
//! frames, publishing, and constraints stay in livekit-js.
//! The shared `CallEngine` still owns roster reconciliation and the
//! multi-focus connection pool; its actor runs on the JS microtask queue.

use std::sync::Arc;
use std::time::Duration;

use js_sys::{Function, Reflect};
use matrix_rtc_call_sdk::{
    AttachOptions, CallEngine, CallEvent, EndedReason, FrameEncryptionDiagnostic, MediaAttachment,
    Participant, StabilityConfig, attach_media,
};
use matrix_rtc_core::compat::MembershipFormat;
use matrix_rtc_transport::keys::MediaKeyHandler;
use matrix_rtc_transport::livekit::{TokenEndpoint, identity_mapper};
use matrix_rtc_transport::{FrameEncryptionState, TransportConnection as _};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use wasm_bindgen::prelude::*;

use super::transport::{JsFrameKeyRing, JsMediaTransport, JsTransportConnection, stream_kind_str};
use crate::WasmRtcCall;

/// livekit-js's `ExternalE2EEKeyProvider` default `keyringSize`.
const LIVEKIT_JS_DEFAULT_KEY_RING_SIZE: u16 = 16;

/// Tuning for a media session. The call says the rest: its room, slot and
/// `member.id`, the focus its join publishes on, and — through the backend —
/// who we are.
#[derive(Debug, Default, Deserialize)]
struct WasmMediaSessionConfig {
    /// livekit-js key-provider ring size, when configured away from its
    /// default of 16 (`keyringSize`). Keys at or past it are rejected.
    #[serde(default)]
    key_ring_size: Option<u16>,
    /// Element Call compatibility generation this room was joined for:
    /// `"current"` (default), `"sticky_2025"`, or `"room_state"`. Decides the
    /// participant-identity derivation and the token endpoint, so it must
    /// match the membership the page published.
    #[serde(default)]
    format: Option<String>,
    /// How much the tile order is damped. Omitted takes the defaults; so does
    /// any field left out of the object.
    #[serde(default)]
    stability: Option<WasmStabilityConfig>,
}

/// The tile-order damping a page can set (R10, R11). A product decision
/// rather than a protocol one; each field defaults to what
/// `matrix_rtc_call_sdk::StabilityConfig` uses.
#[derive(Debug, Deserialize)]
struct WasmStabilityConfig {
    /// Sustained voice before a member ranks as speaking; the tile flag is not delayed.
    #[serde(default = "default_promote_ms")]
    promote_ms: u64,
    /// Silence before a speaking member stops ranking as one. Raising this above
    /// `promote_ms` leaves a tile at the top of the order after the speaker
    /// stopped, which reads as a stuck UI.
    #[serde(default = "default_demote_ms")]
    demote_ms: u64,
    /// Reorders inside this window are delivered as one.
    #[serde(default = "default_coalesce_ms")]
    coalesce_ms: u64,
}

fn default_promote_ms() -> u64 {
    StabilityConfig::default().promote.as_millis() as u64
}

fn default_demote_ms() -> u64 {
    StabilityConfig::default().demote.as_millis() as u64
}

fn default_coalesce_ms() -> u64 {
    StabilityConfig::default().coalesce.as_millis() as u64
}

impl From<&WasmStabilityConfig> for StabilityConfig {
    fn from(c: &WasmStabilityConfig) -> Self {
        Self {
            promote: Duration::from_millis(c.promote_ms),
            demote: Duration::from_millis(c.demote_ms),
            coalesce: Duration::from_millis(c.coalesce_ms),
        }
    }
}

#[wasm_bindgen]
impl WasmRtcCall {
    /// Attach media to this call: wire frame-key signalling into the core,
    /// start the engine (which connects to every peer's focus), and connect
    /// the own-focus livekit-js room. The `member.id` comes from the join —
    /// the page neither chooses nor passes it.
    ///
    /// `config` is `{ key_ring_size?, format?, stability? }`, and may be
    /// omitted; the focus is the one the join publishes on, none for a
    /// receive-only call;
    /// `delegate` is the object driving livekit-js (see the module docs of
    /// the transport for its required methods). The delegate may additionally
    /// implement `onParticipants(roster)` and `onEvent(event)` — the push
    /// half of the session, invoked from spawned pumps for the life of the
    /// call.
    #[wasm_bindgen(js_name = connectMedia)]
    pub async fn connect_media(
        &self,
        #[wasm_bindgen(unchecked_param_type = "MediaSessionConfigIn")] config: JsValue,
        #[wasm_bindgen(unchecked_param_type = "MediaDelegate")] delegate: JsValue,
    ) -> Result<WasmMediaSession, JsError> {
        let config: WasmMediaSessionConfig = if config.is_undefined() || config.is_null() {
            WasmMediaSessionConfig::default()
        } else {
            serde_wasm_bindgen::from_value(config)
                .map_err(|err| JsError::new(&format!("invalid media session config: {err}")))?
        };

        // Which MatrixRTC generation this room was joined for, read back from
        // the join rather than trusted from this config: it decides the
        // participant identity and the token endpoint, and those disagreeing
        // with the membership we already published is not an error but a
        // silence. The config field is accepted only as a cross-check.
        let compat = self.format();
        if let Some(requested) = config.format.as_deref() {
            let requested = crate::compat::parse_compat(Some(requested))?;
            if requested != compat {
                return Err(JsError::new(&format!(
                    "format {requested:?} disagrees with the mode this room was \
                     joined in ({compat:?}); set the mode on join and drop it here",
                )));
            }
        }
        if compat != MembershipFormat::Current {
            log::info!(
                "media: [{}/{}] connecting in Element Call compatibility mode {compat:?}",
                self.inner().room_id(),
                self.inner().slot_id(),
            );
        }
        // One mapper for the core, the transport, our own identity and the
        // ring: they must not skew.
        let mapper = identity_mapper(compat);
        // Frame encryption: livekit-js owns the key provider; the shared
        // handler forwards every signalled key into it through the delegate.
        let ring = JsFrameKeyRing::new(
            delegate.clone(),
            config
                .key_ring_size
                .unwrap_or(LIVEKIT_JS_DEFAULT_KEY_RING_SIZE),
        );
        // The ring and handler hold `JsValue`s, so they are `!Send` — shared
        // ownership on one thread. The `Arc`s are what the core's and the
        // engine's APIs take.
        #[expect(clippy::arc_with_non_send_sync)]
        let handler = Arc::new(MediaKeyHandler::with_ring(Arc::new(ring)));
        // `allow`, not `expect`: whether clippy fires this depends on the
        // toolchain (1.98 no longer does), and an unfulfilled expectation is
        // itself an error under `-D warnings`.
        let backend = self.backend();
        #[allow(clippy::arc_with_non_send_sync)]
        let transport = Arc::new(JsMediaTransport::new(
            delegate.clone(),
            backend.clone(),
            mapper.clone(),
            TokenEndpoint::for_format(compat),
        ));
        let MediaAttachment {
            engine,
            own_connection,
            own_identity,
            events,
        } = attach_media(
            self.inner(),
            transport,
            handler.clone(),
            AttachOptions {
                own_user_id: backend.own_user_id(),
                own_device_id: backend.own_device_id(),
                identity_mapper: mapper.clone(),
                format: compat,
                stability: config
                    .stability
                    .as_ref()
                    .map(Into::into)
                    .unwrap_or_default(),
            },
        )
        .await
        .map_err(|error| JsError::new(&error.to_string()))?;

        // Roster, event, and switch-complete delivery run as spawned pumps
        // owning their receivers, invoking the delegate's optional callbacks.
        // NOT as async session methods: a wasm-bindgen `&mut self` future
        // holds the object borrowed across its awaits, so a parked long-poll
        // would make every other session call throw ("recursive use of an
        // object"). The pumps borrow nothing from the session.
        if let Some(on_participants) = delegate_callback(&delegate, "onParticipants") {
            let mut participants_rx = engine.subscribe_participants();
            let mapper = mapper.clone();
            wasm_bindgen_futures::spawn_local(async move {
                while participants_rx.changed().await.is_ok() {
                    let roster: Vec<Participant> = participants_rx.borrow_and_update().clone();
                    let roster: Vec<WasmParticipant> = roster
                        .iter()
                        .map(|participant| to_wasm_participant(&mapper, participant))
                        .collect();
                    match serde_wasm_bindgen::to_value(&roster) {
                        Ok(roster) => {
                            let _ = on_participants.call1(&JsValue::NULL, &roster);
                        }
                        Err(error) => log::warn!("media: roster did not serialize: {error}"),
                    }
                }
            });
        }
        if let Some(on_event) = delegate_callback(&delegate, "onEvent") {
            let mut events = events;
            wasm_bindgen_futures::spawn_local(async move {
                loop {
                    match events.recv().await {
                        Ok(event) => {
                            let event = WasmCallEvent::from(event);
                            match serde_wasm_bindgen::to_value(&event) {
                                Ok(event) => {
                                    let _ = on_event.call1(&JsValue::NULL, &event);
                                }
                                Err(error) => {
                                    log::warn!("media: event did not serialize: {error}")
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(missed)) => {
                            log::warn!("media: event consumer lagged, {missed} event(s) dropped");
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            });
        }

        Ok(WasmMediaSession {
            engine,
            own_connection,
            _handler: handler,
            identity_mapper: mapper,
            own_identity,
        })
    }
}

/// An optional delegate callback, by name.
fn delegate_callback(delegate: &JsValue, name: &str) -> Option<Function> {
    Reflect::get(delegate, &JsValue::from_str(name))
        .ok()
        .filter(|method| !method.is_undefined())
        .and_then(|method| method.dyn_into::<Function>().ok())
}

/// A live media session on a joined slot: the participant roster (with the
/// livekit-js identity of each entry). Media itself — tracks, publishing,
/// rendering — stays in livekit-js; join roster entries to
/// `room.getParticipantByIdentity(rtc_identity)`.
///
/// Roster changes and call events arrive through the delegate's
/// `onParticipants` / `onEvent` callbacks, registered at
/// [`WasmRtcCall::connect_media`] time.
///
/// Ends with the call; [`WasmMediaSession::disconnect`] ends it while staying
/// joined.
#[wasm_bindgen]
pub struct WasmMediaSession {
    engine: CallEngine,
    /// The own-focus connection; `None` for a receive-only call.
    own_connection: Option<JsTransportConnection>,
    /// Keeps the key handler alive alongside the session for clarity; the
    /// core's encryption manager also holds it.
    _handler: Arc<MediaKeyHandler>,
    identity_mapper: matrix_rtc_core::RtcIdentityMapper,
    own_identity: String,
}

#[wasm_bindgen]
impl WasmMediaSession {
    /// The current roster. `rtc_identity` is the livekit-js participant
    /// identity, when derivable.
    #[wasm_bindgen(unchecked_return_type = "RtcParticipant[]")]
    pub fn participants(&self) -> Result<JsValue, JsError> {
        let roster: Vec<WasmParticipant> = self
            .engine
            .participants()
            .iter()
            .map(|participant| to_wasm_participant(&self.identity_mapper, participant))
            .collect();
        serde_wasm_bindgen::to_value(&roster).map_err(|err| JsError::new(&err.to_string()))
    }

    /// Our own livekit-js participant identity (the MSC4195 pseudonymous
    /// identity, or the legacy `{user}:{device}` in that compatibility mode).
    #[wasm_bindgen(js_name = ownRtcIdentity)]
    pub fn own_rtc_identity(&self) -> String {
        self.own_identity.clone()
    }

    /// End the media session while staying joined. After leaving it is not
    /// needed, and would call the delegate's `close` a second time.
    pub async fn disconnect(&mut self) -> Result<(), JsError> {
        self.engine.shutdown().await;
        let Some(connection) = &self.own_connection else {
            return Ok(());
        };
        connection
            .close()
            .await
            .map_err(|error| JsError::new(&error.to_string()))
    }
}

fn to_wasm_participant(
    mapper: &matrix_rtc_core::RtcIdentityMapper,
    participant: &Participant,
) -> WasmParticipant {
    // No attributable device, no identity — such a member also cannot be
    // reached on the media plane (same rule as `remote_identity`).
    let rtc_identity = participant
        .device_id
        .as_deref()
        .map(|device_id| mapper(&participant.user_id, device_id, &participant.member_id));
    WasmParticipant {
        member_id: participant.member_id.clone(),
        user_id: participant.user_id.clone(),
        device_id: participant.device_id.clone(),
        is_local: participant.is_local,
        reachable: participant.reachable,
        rtc_identity,
        hand_raised_at_ms: participant.hand_raised_at_ms,
        streams: participant
            .streams
            .iter()
            .map(|stream| WasmStreamState {
                kind: stream_kind_str(stream.kind),
                muted: stream.muted,
            })
            .collect(),
    }
}

/// One published stream on a roster entry.
#[derive(Serialize)]
struct WasmStreamState {
    kind: &'static str,
    muted: bool,
}

/// A roster entry, as JS sees it.
#[derive(Serialize)]
struct WasmParticipant {
    member_id: String,
    user_id: String,
    device_id: Option<String>,
    is_local: bool,
    reachable: bool,
    /// The livekit-js participant identity, when derivable — the join key for
    /// `room.getParticipantByIdentity()`.
    rtc_identity: Option<String>,
    streams: Vec<WasmStreamState>,
    /// When the participant raised their hand (ms since the epoch); `None`
    /// while it is down.
    hand_raised_at_ms: Option<u64>,
}

/// A call event, as JS sees it: `{ type, ...fields }`, snake_case throughout
/// like the rest of this binding.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WasmCallEvent {
    ParticipantJoined {
        member_id: String,
        user_id: String,
    },
    ParticipantLeft {
        member_id: String,
    },
    StreamStarted {
        member_id: String,
        kind: &'static str,
    },
    StreamStopped {
        member_id: String,
        kind: &'static str,
    },
    StreamMuted {
        member_id: String,
        kind: &'static str,
    },
    StreamUnmuted {
        member_id: String,
        kind: &'static str,
    },
    ActiveSpeakers {
        speakers: Vec<WasmSpeaker>,
    },
    HandRaised {
        member_id: String,
        raised_at_ms: u64,
    },
    HandLowered {
        member_id: String,
    },
    /// Transient; `sound` is the asset base name to play, `None` for silence.
    Reaction {
        member_id: String,
        emoji: String,
        name: String,
        sound: Option<String>,
    },
    /// `identity` is the transport identity the key installed under: keys
    /// held before media connects surface while `connectMedia` is still
    /// pending, before the page has a session to look a roster up on.
    KeyImported {
        member_id: String,
        identity: String,
        key_index: u8,
    },
    FrameEncryptionState {
        member_id: String,
        state: &'static str,
        /// Key indices installed for the member, when any are — the half of a
        /// failure diagnosis the media layer knows.
        installed_key_indices: Option<Vec<u8>>,
    },
    KeyDiscarded {
        member_id: String,
        key_index: Option<u8>,
        sender_user_id: Option<String>,
        sender_device_id: Option<String>,
        /// Machine-readable rejection: `cleartext | not_cross_signed |
        /// room_mismatch | sender_mismatch | unverifiable_device |
        /// device_mismatch`.
        reason_code: &'static str,
        /// Human-readable rejection, with the mismatch details.
        reason: String,
    },
    UnknownParticipant {
        identity: String,
    },
    MediaConnectionState {
        degraded: bool,
    },
    Ended {
        /// `left`, or the transport's disconnect description.
        reason: String,
    },
}

#[derive(Serialize)]
struct WasmSpeaker {
    member_id: String,
    level: f32,
}

fn encryption_state_str(state: FrameEncryptionState) -> &'static str {
    match state {
        FrameEncryptionState::Ok => "ok",
        FrameEncryptionState::MissingKey => "missing_key",
        FrameEncryptionState::DecryptionFailed => "decryption_failed",
        FrameEncryptionState::EncryptionFailed => "encryption_failed",
        FrameEncryptionState::InternalError => "internal_error",
    }
}

fn rejection_code(rejection: &matrix_rtc_core::KeyRejection) -> &'static str {
    use matrix_rtc_core::KeyRejection;
    match rejection {
        KeyRejection::Cleartext => "cleartext",
        KeyRejection::NotCrossSigned => "not_cross_signed",
        KeyRejection::RoomMismatch { .. } => "room_mismatch",
        KeyRejection::SenderMismatch { .. } => "sender_mismatch",
        KeyRejection::UnverifiableDevice => "unverifiable_device",
        KeyRejection::DeviceMismatch { .. } => "device_mismatch",
    }
}

impl From<CallEvent> for WasmCallEvent {
    fn from(event: CallEvent) -> Self {
        match event {
            CallEvent::ParticipantJoined { member_id, user_id } => {
                Self::ParticipantJoined { member_id, user_id }
            }
            CallEvent::ParticipantLeft { member_id } => Self::ParticipantLeft { member_id },
            CallEvent::StreamStarted { member_id, kind } => Self::StreamStarted {
                member_id,
                kind: stream_kind_str(kind),
            },
            CallEvent::StreamStopped { member_id, kind } => Self::StreamStopped {
                member_id,
                kind: stream_kind_str(kind),
            },
            CallEvent::StreamMuted { member_id, kind } => Self::StreamMuted {
                member_id,
                kind: stream_kind_str(kind),
            },
            CallEvent::StreamUnmuted { member_id, kind } => Self::StreamUnmuted {
                member_id,
                kind: stream_kind_str(kind),
            },
            CallEvent::ActiveSpeakers { speakers } => Self::ActiveSpeakers {
                speakers: speakers
                    .into_iter()
                    .map(|speaker| WasmSpeaker {
                        member_id: speaker.member_id,
                        level: speaker.level,
                    })
                    .collect(),
            },
            CallEvent::KeyImported {
                member_id,
                identity,
                key_index,
            } => Self::KeyImported {
                member_id,
                identity,
                key_index,
            },
            CallEvent::FrameEncryptionState {
                member_id,
                state,
                diagnostic,
            } => Self::FrameEncryptionState {
                member_id,
                state: encryption_state_str(state),
                installed_key_indices: match diagnostic {
                    FrameEncryptionDiagnostic::KeysInstalled { key_indices } => Some(key_indices),
                    FrameEncryptionDiagnostic::NoKeyInstalled => Some(Vec::new()),
                    FrameEncryptionDiagnostic::NotApplicable => None,
                },
            },
            CallEvent::KeyDiscarded {
                member_id,
                key_index,
                sender_user_id,
                sender_device_id,
                reason,
            } => Self::KeyDiscarded {
                member_id,
                key_index,
                sender_user_id,
                sender_device_id,
                reason_code: rejection_code(&reason),
                reason: reason.to_string(),
            },
            CallEvent::HandRaised {
                member_id,
                raised_at_ms,
            } => Self::HandRaised {
                member_id,
                raised_at_ms,
            },
            CallEvent::HandLowered { member_id } => Self::HandLowered { member_id },
            CallEvent::Reaction {
                member_id,
                emoji,
                name,
                sound,
            } => Self::Reaction {
                member_id,
                emoji,
                name,
                sound,
            },
            CallEvent::UnknownParticipant { identity } => Self::UnknownParticipant { identity },
            CallEvent::MediaConnectionState { degraded } => Self::MediaConnectionState { degraded },
            CallEvent::Ended { reason } => Self::Ended {
                reason: match reason {
                    EndedReason::Left => "left".to_owned(),
                    EndedReason::SlotClosed => "slot_closed".to_owned(),
                    EndedReason::ConnectionClosed { message } => message,
                    EndedReason::MembershipLost { .. } => "membership_lost".to_owned(),
                },
            },
        }
    }
}
