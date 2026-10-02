// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! WebAssembly bindings for the MatrixRTC core.
//!
//! The page implements one `MatrixBackendHost` object (sends and
//! subscriptions) over its Matrix client. A `WasmRtcClient` over it opens a
//! `WasmRtcRoom` per room, which the library feeds itself, and joining a slot
//! returns a `WasmRtcCall`. JS-shaped payloads are converted into core DTOs here
//! so the core stays independent from wasm/JS types.
//!
//! # Only built for `wasm32`
//!
//! The crate body is `cfg`-gated to `wasm32` because it cannot compile anywhere
//! else: its futures wrap JS promises and are therefore `!Send`, while
//! `matrix-rtc-core`'s backend trait is `Send` on every target *but* wasm32
//! (see [`matrix_rtc_core::MatrixBackend`]). On other targets this compiles
//! to an empty crate so a workspace-wide `cargo check`/`clippy` still passes.
//!
//! Check it explicitly:
//!
//! ```sh
//! cargo check -p matrix-rtc-wasm --target wasm32-unknown-unknown
//! ```
#![cfg(target_arch = "wasm32")]

use std::sync::Arc;

use matrix_rtc_call::compat::{DialectBackend, ElementCallCompat};
use matrix_rtc_call::{
    CallJoinOptions, JoinOptions, Mentions, NotificationType, NotifyConfig, RoomOptions,
};
use matrix_rtc_core::{
    EncryptionConfig, LeaveSessionParams, MatrixBackend, RtcTransport, SlotEncryption,
    TransportIntent,
};

mod backend;
mod compat;
mod logging;
mod media;
mod ts_types;
pub use backend::{JsBackend, WasmRoomSink, WasmToDeviceSink};
pub use logging::{init_logging, log_event};
pub use media::{WasmConnectionEventSink, WasmMediaSession};
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, RwLockReadGuard};
use wasm_bindgen::prelude::*;

type Room = matrix_rtc_call::RtcRoom<JsBackend>;
type Call = matrix_rtc_call::RtcCall<JsBackend>;

fn js_error(error: impl std::fmt::Display) -> JsError {
    JsError::new(&error.to_string())
}

#[wasm_bindgen]
/// One per Matrix session, over the page's `MatrixBackendHost`. Creating it
/// does no I/O; [`Self::room`] opens the rooms the page has calls in.
pub struct WasmRtcClient {
    client: matrix_rtc_call::RtcClient<JsBackend>,
}

#[wasm_bindgen]
impl WasmRtcClient {
    #[wasm_bindgen(constructor)]
    pub fn new(#[wasm_bindgen(unchecked_param_type = "MatrixBackendHost")] host: JsValue) -> Self {
        log::info!("client: created over the host backend");
        // `Rc` is not an option: the core takes `Arc<T>` on every target.
        #[allow(clippy::arc_with_non_send_sync)]
        let backend = Arc::new(JsBackend::new(host));
        Self {
            client: matrix_rtc_call::RtcClient::new(backend),
        }
    }

    /// Opens a room: the library subscribes to what the room needs in the
    /// given mode and applies its current state. Resolves once that state is
    /// applied, so a `joinCall` issued afterwards sees it.
    ///
    /// Opening a room that already has a live room object rejects. Freeing the
    /// returned room ends its subscriptions without leaving; `close` leaves
    /// first.
    ///
    /// `options` is `{ element_call_compat?: "off" | "sticky_events" | "state_events" }`.
    pub async fn room(
        &self,
        room_id: String,
        #[wasm_bindgen(unchecked_param_type = "RoomOptionsIn | null | undefined")] options: JsValue,
    ) -> Result<WasmRtcRoom, JsError> {
        let options: Option<WasmRoomOptions> = serde_wasm_bindgen::from_value(options)
            .map_err(|err| JsError::new(&format!("invalid room options: {err}")))?;
        let compat = compat::parse_compat(
            options
                .as_ref()
                .and_then(|options| options.element_call_compat.as_deref()),
        )?;
        log::info!("client: [{room_id}] opening in {compat:?} mode");
        let room = self
            .client
            .room(
                room_id.clone(),
                RoomOptions {
                    element_call_compat: compat,
                },
            )
            .await
            .map_err(js_error)?;
        room.seeded().await;
        log::info!("client: [{room_id}] open and seeded");
        Ok(WasmRtcRoom {
            room_id,
            room: RwLock::new(Some(room)),
        })
    }
}

#[wasm_bindgen]
/// One open room. Everything room-scoped is here; our own participation is on
/// the [`WasmRtcCall`] that `joinCall` returns.
pub struct WasmRtcRoom {
    room_id: String,
    /// `None` once closed.
    room: RwLock<Option<Room>>,
}

impl WasmRtcRoom {
    async fn open(&self) -> Result<RwLockReadGuard<'_, Room>, JsError> {
        RwLockReadGuard::try_map(self.room.read().await, Option::as_ref)
            .map_err(|_| JsError::new(&format!("{} has been closed", self.room_id)))
    }
}

#[wasm_bindgen]
impl WasmRtcRoom {
    #[wasm_bindgen(getter, js_name = roomId)]
    pub fn room_id(&self) -> String {
        self.room_id.clone()
    }

    /// Everything the room believes — its state, and every candidate member
    /// of each slot with why it is or is not joined — as a JSON string.
    #[wasm_bindgen(js_name = debugSnapshot)]
    pub async fn debug_snapshot(&self) -> Result<String, JsError> {
        Ok(self.open().await?.debug_snapshot().await.to_string())
    }

    /// How many members are joined to a slot, without joining it.
    #[wasm_bindgen(js_name = memberCount)]
    pub async fn member_count(&self, slot_id: String) -> Result<u32, JsError> {
        Ok(self.open().await?.member_count(&slot_id).await as u32)
    }

    /// Joins a call slot, returning our participation in it.
    ///
    /// `params`:
    ///   - `slot_id` (e.g. "m.call#ROOM"), `application` (e.g. "m.call")
    ///   - `transport`: the transport to publish on; omit to take the first
    ///     LiveKit one the homeserver advertises (the host's `rtcTransports`)
    ///   - `receive_only`: join without publishing; `can_subscribe` then lists
    ///     the transport types this member can receive on
    ///   - `keep_alive_timeout_ms` (default 30000), `sticky_duration_ms`
    ///     (default 3600000), `degraded_lifetime_ms` (default 300000; not below)
    ///   - `encryption_config`, `notify`, `reactions`
    ///
    /// The call keeps itself alive, and performs its key rotations when they
    /// fall due, until it leaves or is freed. The SDK generates the
    /// `member.id` (MSC4143 requires a fresh one per join).
    /// Rejects when the slot is already joined, or the room's state holds no
    /// open slot of this id.
    #[wasm_bindgen(js_name = joinCall)]
    pub async fn join_call(
        &self,
        #[wasm_bindgen(unchecked_param_type = "JoinParamsIn")] params: JsValue,
    ) -> Result<WasmRtcCall, JsError> {
        let params: WasmJoinSessionParams =
            serde_wasm_bindgen::from_value(params).map_err(|err| {
                log::warn!("room: invalid join params: {err}");
                JsError::new(&format!("invalid join params: {err}"))
            })?;
        log::info!(
            "room: [{}/{}] join requested application={}",
            self.room_id,
            params.slot_id,
            params.application,
        );
        let options = params.into_call()?;
        let room = self.open().await?;
        let call = room.join_call(options).await.map_err(|err| {
            log::warn!("room: [{}] join failed: {err}", self.room_id);
            js_error(err)
        })?;
        log::info!(
            "room: [{}/{}] joined as {}",
            self.room_id,
            call.slot_id(),
            call.member_id()
        );
        Ok(WasmRtcCall {
            compat: room.element_call_compat(),
            backend: room.backend().clone(),
            call,
        })
    }

    /// Opens a slot by publishing its `m.rtc.slot` state event.
    ///
    /// A room has no slot until somebody with the power level opens one — an
    /// administrator, or the room's creator as initial state — and a join into
    /// a room whose state holds no open slot fails. The library never opens one
    /// on its own; this is the helper for a page that does.
    ///
    /// `slot_id` must start with `{application_type}#` (MSC4143 makes the slot
    /// id the state key and requires that shape). `encryption` is the raw
    /// MSC4143 `content.encryption` object — `{ type: "m.per_member" }` in an
    /// encrypted room, `null`/`undefined` elsewhere; the mismatch resolves the
    /// slot closed for everyone.
    #[wasm_bindgen(js_name = openSlot)]
    pub async fn open_slot(
        &self,
        slot_id: String,
        application_type: String,
        #[wasm_bindgen(unchecked_param_type = "SlotEncryptionIn | null | undefined")]
        encryption: JsValue,
    ) -> Result<(), JsError> {
        let encryption: Option<SlotEncryption> = serde_wasm_bindgen::from_value(encryption)
            .map_err(|err| JsError::new(&format!("invalid slot encryption payload: {err}")))?;
        log::info!(
            "room: [{}/{slot_id}] opening slot: application={application_type} \
             encryption={encryption:?}",
            self.room_id,
        );
        self.open()
            .await?
            .open_slot(slot_id, application_type, encryption)
            .await
            .map_err(|err| {
                log::warn!("room: could not open the slot: {err}");
                js_error(err)
            })
    }

    /// Closes a slot, by setting its `m.rtc.slot` status to `closed`.
    ///
    /// Every member of it becomes left as soon as clients apply the new state —
    /// this ends the call for everyone, not just for us. Leaving is
    /// [`WasmRtcCall::leave`].
    #[wasm_bindgen(js_name = closeSlot)]
    pub async fn close_slot(&self, slot_id: String) -> Result<(), JsError> {
        log::info!("room: [{}/{slot_id}] closing slot", self.room_id);
        self.open().await?.close_slot(slot_id).await.map_err(|err| {
            log::warn!("room: could not close the slot: {err}");
            js_error(err)
        })
    }

    /// Leaves every call joined through this room, then ends its
    /// subscriptions. Every call of the room is over afterwards, and the room
    /// can be opened again. Closing a closed room is a no-op.
    pub async fn close(&self) {
        let room = self.room.write().await.take();
        match room {
            Some(room) => {
                room.close().await;
                log::info!("room: [{}] closed", self.room_id);
            }
            None => log::debug!("room: [{}] already closed", self.room_id),
        }
    }
}

#[wasm_bindgen]
/// Our participation in one call slot. Over after `leave`, or once its room is
/// closed: calls then reject or report nothing to do, and joining again yields
/// a new call. Freeing it sends no leave; the membership expires through its
/// delayed leave unless the slot is joined again, which leaves it first.
pub struct WasmRtcCall {
    call: Call,
    /// The mode the room was opened in, for the media layer's identity and
    /// token endpoint.
    compat: ElementCallCompat,
    backend: Arc<DialectBackend<JsBackend>>,
}

impl WasmRtcCall {
    pub(crate) fn inner(&self) -> &Call {
        &self.call
    }

    pub(crate) fn element_call_compat(&self) -> ElementCallCompat {
        self.compat
    }

    /// The page's backend, for the media layer's token exchange.
    pub(crate) fn backend(&self) -> Arc<dyn MatrixBackend> {
        self.backend.clone()
    }
}

#[wasm_bindgen]
impl WasmRtcCall {
    #[wasm_bindgen(getter, js_name = roomId)]
    pub fn room_id(&self) -> String {
        self.call.room_id().to_owned()
    }

    #[wasm_bindgen(getter, js_name = slotId)]
    pub fn slot_id(&self) -> String {
        self.call.slot_id().to_owned()
    }

    /// Our `member.id` in this participation.
    #[wasm_bindgen(getter, js_name = memberId)]
    pub fn member_id(&self) -> String {
        self.call.member_id().to_owned()
    }

    /// `false` once left, or once the room is closed.
    #[wasm_bindgen(getter, js_name = isLive)]
    pub fn is_live(&self) -> bool {
        self.call.is_live()
    }

    /// The event id of our current membership event, or `undefined` once the
    /// call is over. Moves on every sticky refresh, so read it at the moment
    /// of use.
    #[wasm_bindgen(js_name = membershipEventId)]
    pub async fn membership_event_id(&self) -> Option<String> {
        self.call.membership_event_id().await
    }

    /// How many members are joined to this call's slot.
    #[wasm_bindgen(js_name = memberCount)]
    pub async fn member_count(&self) -> u32 {
        self.call.member_count().await as u32
    }

    // ---- Reactions and raised hands ----
    //
    // Element Call's reactions are ordinary room events relating to the
    // reacting member's membership event. The library reads them from the
    // open room (timeline events, redactions and the relations of each
    // membership event); the page plays any sound. Results surface on the
    // media session as `hand_raised` / `hand_lowered` / `reaction` events, on
    // `rtc_participant.hand_raised_at_ms`, and here as `raisedHands`.

    /// Sends an Element Call emoji reaction. `name` is what peers pick a sound
    /// by (see [`reaction_catalog`]); only the first grapheme of `emoji` is
    /// sent. Resolves to the event id; rejects inside the send cooldown.
    #[wasm_bindgen(js_name = sendReaction)]
    pub async fn send_reaction(&self, emoji: String, name: String) -> Result<String, JsError> {
        self.call
            .send_reaction(&emoji, &name)
            .await
            .map_err(js_error)
    }

    /// Raises our hand. Idempotent while it is up.
    #[wasm_bindgen(js_name = raiseHand)]
    pub async fn raise_hand(&self) -> Result<(), JsError> {
        self.call.raise_hand().await.map_err(js_error)
    }

    /// Lowers our hand. A no-op when it is down.
    #[wasm_bindgen(js_name = lowerHand)]
    pub async fn lower_hand(&self) -> Result<(), JsError> {
        self.call.lower_hand().await.map_err(js_error)
    }

    /// The raised hands, oldest first, as `RaisedHand[]`.
    #[wasm_bindgen(js_name = raisedHands, unchecked_return_type = "RaisedHand[]")]
    pub async fn raised_hands(&self) -> Result<JsValue, JsError> {
        serde_wasm_bindgen::to_value(&self.call.raised_hands().await).map_err(js_error)
    }

    /// Leaves the slot; the call is over afterwards. A failed leave leaves it
    /// live, so it can be retried.
    ///
    /// `params` is `{ leave_reason?: { code, reason? } }` — e.g.
    /// `{ code: "leave" }` for an intentional hang-up. Defaults to that.
    pub async fn leave(
        &self,
        #[wasm_bindgen(unchecked_param_type = "LeaveParamsIn | null | undefined")] params: JsValue,
    ) -> Result<(), JsError> {
        let params: Option<WasmLeaveSessionParams> = serde_wasm_bindgen::from_value(params)
            .map_err(|err| JsError::new(&format!("invalid leave params: {err}")))?;
        let params = params.unwrap_or_default();
        log::info!(
            "call: [{}/{}] leave requested reason={:?}",
            self.call.room_id(),
            self.call.slot_id(),
            params.leave_reason,
        );
        self.call
            .leave(params.into_core())
            .await
            .inspect(|()| log::info!("call: leave succeeded"))
            .map_err(|err| {
                log::warn!("call: leave failed: {err}");
                js_error(err)
            })
    }
}

/// How a room is opened.
#[derive(Debug, Default, Deserialize)]
pub struct WasmRoomOptions {
    /// `"off"` (the default), `"sticky_events"` or `"state_events"`. One
    /// decision for the room: what the library subscribes to, how it renders
    /// our sends, the `member.id` we join with, how an inbound media key is
    /// bound, the SFU identity and the token endpoint.
    #[serde(default)]
    pub element_call_compat: Option<String>,
}

/// Element Call's reaction catalogue, as `ReactionKind[]` in the order its
/// picker shows them. The `sound` of each entry is the base name of the asset
/// to bundle and play; `reactionSoundFor` resolves a received `name`.
#[wasm_bindgen(js_name = reactionCatalog, unchecked_return_type = "ReactionKind[]")]
pub fn reaction_catalog() -> Result<JsValue, JsError> {
    #[derive(Serialize)]
    struct Kind {
        name: &'static str,
        emoji: &'static str,
        sound: Option<&'static str>,
    }
    let catalogue: Vec<Kind> = matrix_rtc_call::KNOWN_REACTIONS
        .iter()
        .map(|kind| Kind {
            name: kind.name,
            emoji: kind.emoji,
            sound: kind.sound,
        })
        .collect();
    serde_wasm_bindgen::to_value(&catalogue).map_err(|err| JsError::new(&err.to_string()))
}

/// The sound asset to play for a reaction `name`: a catalogue entry's sound,
/// `"generic"` for a name outside the catalogue, or `undefined` for a silent
/// one.
#[wasm_bindgen(js_name = reactionSoundFor)]
pub fn reaction_sound_for(name: String) -> Option<String> {
    matrix_rtc_call::sound_for(&name)
        .asset_name()
        .map(str::to_owned)
}

/// WASM-friendly join session parameters.
#[derive(Debug, Deserialize)]
pub struct WasmJoinSessionParams {
    pub slot_id: String,
    pub application: String,
    /// The transport to publish on. Omit to take the first LiveKit transport
    /// the homeserver advertises.
    #[serde(default)]
    pub transport: Option<WasmTransportConfig>,
    /// Join without publishing — valid per MSC4143, and what a recorder or
    /// other observer wants. `transport` is then ignored.
    #[serde(default)]
    pub receive_only: bool,
    /// Transport types this member can receive on. Only read when
    /// `receive_only`; a publishing member advertises its own transport's type.
    #[serde(default)]
    pub can_subscribe: Vec<String>,
    #[serde(default)]
    pub keep_alive_timeout_ms: Option<u64>,
    #[serde(default)]
    pub sticky_duration_ms: Option<u64>,
    #[serde(default)]
    pub degraded_lifetime_ms: Option<u64>,
    #[serde(default)]
    pub encryption_config: Option<WasmEncryptionConfig>,
    /// Ask for an MSC4075 notification to be sent with this join, so other
    /// devices in the room ring or show an incoming call. Omit — the default —
    /// to join quietly; pass it only when the user is *starting* the call.
    #[serde(default)]
    pub notify: Option<WasmNotifyConfig>,
    /// How this session handles Element Call reactions and the raised hand.
    /// Omitted is enabled with Element Call's three-second window.
    #[serde(default)]
    pub reactions: Option<WasmReactionsConfig>,
}

/// WASM-friendly reactions configuration (mirrors the core's
/// `ReactionsConfig`; every field defaults).
#[derive(Debug, Deserialize)]
pub struct WasmReactionsConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_reaction_window_ms")]
    pub active_window_ms: u64,
    #[serde(default = "default_reaction_window_ms")]
    pub send_cooldown_ms: u64,
}

fn default_true() -> bool {
    true
}

fn default_reaction_window_ms() -> u64 {
    matrix_rtc_call::DEFAULT_REACTION_ACTIVE_MS
}

impl From<WasmReactionsConfig> for matrix_rtc_call::ReactionsConfig {
    fn from(value: WasmReactionsConfig) -> Self {
        matrix_rtc_call::ReactionsConfig {
            enabled: value.enabled,
            active_window_ms: value.active_window_ms,
            send_cooldown_ms: value.send_cooldown_ms,
        }
    }
}

/// WASM-friendly MSC4075 notification request.
#[derive(Debug, Deserialize)]
pub struct WasmNotifyConfig {
    /// `"ring"` or `"notification"`.
    pub notification_type: String,
    /// MSC4196 `m.call.intent`, e.g. `"audio"` or `"video"`.
    #[serde(default)]
    pub intent: Option<String>,
    /// How long the ring stays valid, in milliseconds (default: 30000, capped
    /// at 120000 because that is what receivers honour).
    #[serde(default)]
    pub lifetime_ms: Option<u64>,
    /// Users named individually in `m.mentions`. Usually empty.
    #[serde(default)]
    pub mention_user_ids: Vec<String>,
    /// Whether the whole room is targeted (default: true).
    #[serde(default = "default_true")]
    pub mention_room: bool,
}

impl WasmNotifyConfig {
    fn into_core(self) -> Result<NotifyConfig, JsError> {
        let notification_type = match self.notification_type.as_str() {
            "ring" => NotificationType::Ring,
            "notification" => NotificationType::Notification,
            other => {
                return Err(JsError::new(&format!(
                    "unknown notification_type {other:?}: expected ring | notification"
                )));
            }
        };
        Ok(NotifyConfig {
            notification_type,
            intent: self.intent,
            lifetime_ms: self.lifetime_ms,
            mentions: Mentions {
                user_ids: self.mention_user_ids,
                room: self.mention_room,
            },
        })
    }
}

/// WASM-friendly encryption configuration.
#[derive(Debug, Deserialize)]
pub struct WasmEncryptionConfig {
    #[serde(default)]
    pub delay_before_use_ms: Option<u64>,
    #[serde(default)]
    pub key_rotation_grace_period_ms: Option<u64>,
    /// Longest a key may be used before it is replaced regardless of
    /// membership (default 1h30).
    #[serde(default)]
    pub max_key_lifetime_ms: Option<u64>,
    #[serde(default)]
    pub manage_media_keys: Option<bool>,
    /// Whether to discard keys from devices that are not cross-signed
    /// (default: true, per MSC4153). Turn it off only where the client cannot
    /// report cross-signing at all.
    #[serde(default)]
    pub require_cross_signed_sender: Option<bool>,
}

impl From<WasmEncryptionConfig> for EncryptionConfig {
    fn from(value: WasmEncryptionConfig) -> Self {
        EncryptionConfig {
            delay_before_use_ms: value.delay_before_use_ms.unwrap_or(5000),
            key_rotation_grace_period_ms: value.key_rotation_grace_period_ms.unwrap_or(10000),
            max_key_lifetime_ms: value.max_key_lifetime_ms.unwrap_or(90 * 60 * 1000),
            manage_media_keys: value.manage_media_keys.unwrap_or(true),
            require_cross_signed_sender: value.require_cross_signed_sender.unwrap_or(true),
        }
    }
}

impl WasmJoinSessionParams {
    /// The transport the join names, if any; `None` leaves the choice to the
    /// library.
    fn transport_intent(&self) -> Result<Option<TransportIntent>, JsError> {
        if self.receive_only {
            return Ok(Some(TransportIntent::ReceiveOnly {
                can_subscribe: self.can_subscribe.clone(),
            }));
        }
        self.transport
            .clone()
            .map(|transport| transport.into_core().map(TransportIntent::Publish))
            .transpose()
    }

    /// The SDK generates the `member.id`: MSC4143 requires a fresh one per
    /// join.
    fn into_call(self) -> Result<CallJoinOptions, JsError> {
        let transport = self.transport_intent()?;
        let mut join = JoinOptions::new(self.slot_id, self.application);
        join.transport = transport;
        join.encryption_config = self.encryption_config.map(Into::into);
        join.keep_alive_timeout_ms = self.keep_alive_timeout_ms;
        join.sticky_duration_ms = self.sticky_duration_ms;
        join.degraded_lifetime_ms = self.degraded_lifetime_ms;
        Ok(CallJoinOptions {
            join,
            notify: self.notify.map(WasmNotifyConfig::into_core).transpose()?,
            reactions: self.reactions.map(Into::into),
        })
    }
}

/// WASM-friendly transport configuration.
#[derive(Clone, Debug, Deserialize)]
pub struct WasmTransportConfig {
    #[serde(rename = "type")]
    pub transport_type: String,
    #[serde(default)]
    pub livekit_service_url: Option<String>,
    #[serde(flatten)]
    pub extra_fields: std::collections::BTreeMap<String, serde_json::Value>,
}

impl WasmTransportConfig {
    pub fn into_core(self) -> Result<RtcTransport, JsError> {
        match self.transport_type.as_str() {
            "livekit" => {
                let url = self.livekit_service_url.ok_or_else(|| {
                    JsError::new("livekit transport requires livekit_service_url")
                })?;
                Ok(RtcTransport::LiveKit(matrix_rtc_core::LiveKitTransport {
                    livekit_service_url: url,
                }))
            }
            _ => {
                let mut extra_fields = self.extra_fields;
                if let Some(url) = self.livekit_service_url {
                    extra_fields.insert(
                        "livekit_service_url".to_string(),
                        serde_json::Value::String(url),
                    );
                }
                Ok(RtcTransport::Unsupported(
                    matrix_rtc_core::UnsupportedTransport {
                        transport_type: self.transport_type,
                        extra_fields,
                    },
                ))
            }
        }
    }
}

/// WASM-friendly leave session parameters.
#[derive(Debug, Deserialize, Default)]
pub struct WasmLeaveSessionParams {
    #[serde(default)]
    pub leave_reason: Option<WasmLeaveReason>,
}

/// MSC4143 `leave_reason`: a machine-readable `code` plus an optional
/// human-readable `reason`.
#[derive(Debug, Deserialize)]
pub struct WasmLeaveReason {
    pub code: String,
    #[serde(default)]
    pub reason: Option<String>,
}

impl From<WasmLeaveReason> for matrix_rtc_core::LeaveReason {
    fn from(value: WasmLeaveReason) -> Self {
        matrix_rtc_core::LeaveReason {
            code: matrix_rtc_core::LeaveCode::from_code(&value.code),
            reason: value.reason,
        }
    }
}

impl WasmLeaveSessionParams {
    pub fn into_core(self) -> LeaveSessionParams {
        LeaveSessionParams {
            leave_reason: self.leave_reason.map(Into::into),
        }
    }
}

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;
    use serde::Serialize;
    use wasm_bindgen_test::*;

    const ROOM: &str = "!room:example.org";
    const SLOT: &str = "m.call#ROOM";

    /// A JS stand-in for the page's backend: every send resolves, the room
    /// subscription delivers one open slot, an unencrypted room, ourselves as
    /// the only joined member and an empty sticky set, so opening the room seeds.
    fn mock_host(sticky_check: &str) -> JsValue {
        let host = js_sys::Object::new();
        let set = |name: &str, args: &str, body: &str| {
            let function = js_sys::Function::new_with_args(args, body);
            js_sys::Reflect::set(&host, &JsValue::from_str(name), &function).unwrap();
        };
        set("ownUserId", "", "return '@alice:example.org';");
        set("ownDeviceId", "", "return 'ALICEDEVICE';");
        // Contents must arrive as plain objects: a real Matrix client
        // `JSON.stringify`s them, and an ES `Map` stringifies to `{}`.
        set(
            "sendStickyEvent",
            "roomId,eventType,content,durationMs",
            &format!(
                "if (JSON.stringify(content) === '{{}}') \
                     return Promise.reject(new Error('content is not a plain object')); \
                 {sticky_check} \
                 return Promise.resolve({{ event_id: '$sticky' }});"
            ),
        );
        set(
            "sendDelayedEvent",
            "roomId,eventType,stateKey,content,delayMs",
            "if (JSON.stringify(content) === '{}') \
                 return Promise.reject(new Error('content is not a plain object')); \
             return Promise.resolve('delay-id');",
        );
        set(
            "restartDelayedEvent",
            "roomId,delayId",
            "globalThis.__restarts = (globalThis.__restarts || 0) + 1; \
             return Promise.resolve();",
        );
        set(
            "cancelDelayedEvent",
            "roomId,delayId",
            "return Promise.resolve();",
        );
        set(
            "sendStateEvent",
            "roomId,eventType,stateKey,content",
            "return Promise.resolve({ event_id: '$state' });",
        );
        set(
            "sendToDeviceMessage",
            "recipients,messageType,content",
            "return Promise.resolve();",
        );
        set(
            "subscribeRoom",
            "roomId,subjects,sink",
            "sink.onEncryption(false); \
             for (const type of subjects.state_event_types) { \
                 if (type === 'm.rtc.slot') sink.onStateEvents(type, [{ \
                     event_id: '$slot', sender: '@admin:example.org', event_type: type, \
                     state_key: 'm.call#ROOM', origin_server_ts: 1, \
                     content: { status: 'open', application: { type: 'm.call' } }, \
                     encryption: { kind: 'cleartext' } }]); \
                 if (type === 'org.matrix.msc3401.call.member') sink.onStateEvents(type, []); \
             } \
             sink.onJoinedMembers(['@alice:example.org']); \
             sink.onStickyEvents([]); \
             return { cancel: () => {} };",
        );
        set(
            "subscribeToDevice",
            "eventTypes,sink",
            "return { cancel: () => {} };",
        );
        set(
            "relations",
            "roomId,eventId,relType,eventType",
            "return Promise.resolve([]);",
        );
        set(
            "getOpenIdToken",
            "",
            "return Promise.resolve({ access_token: 't', token_type: 'Bearer', \
                 matrix_server_name: 'example.org', expires_in: 3600 });",
        );
        set(
            "rtcTransports",
            "",
            "return Promise.resolve([{ type: 'livekit', livekit_service_url: 'https://sfu.example.org' }]);",
        );
        host.into()
    }

    async fn open(client: &WasmRtcClient, compat: Option<&str>) -> WasmRtcRoom {
        // A plain object, as a page passes: the default serializer turns a
        // `json!` map into an ES `Map`, which reads back as no options at all.
        let options = match compat {
            Some(compat) => serde_json::json!({ "element_call_compat": compat })
                .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
                .unwrap(),
            None => JsValue::UNDEFINED,
        };
        client.room(ROOM.to_owned(), options).await.expect("open")
    }

    #[derive(Serialize)]
    struct TestJoinParams {
        slot_id: &'static str,
        application: &'static str,
    }

    fn join_params() -> JsValue {
        serde_wasm_bindgen::to_value(&TestJoinParams {
            slot_id: SLOT,
            application: "m.call",
        })
        .unwrap()
    }

    /// A full join on the wasm target, with the transport taken from the
    /// backend. Also pins the clock: the join path reads wall-clock time,
    /// which on wasm32-unknown-unknown must come from `Date.now()`.
    #[wasm_bindgen_test]
    async fn a_join_succeeds_on_wasm() {
        let client = WasmRtcClient::new(mock_host(""));
        let room = open(&client, None).await;

        let call = room
            .join_call(join_params())
            .await
            .expect("join should succeed");
        assert!(!call.member_id().is_empty());
        assert!(call.is_live());
    }

    fn restarts() -> f64 {
        js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("__restarts"))
            .ok()
            .and_then(|count| count.as_f64())
            .unwrap_or(0.0)
    }

    /// The page ticks nothing: the call restarts its own delayed leave. A
    /// 400 ms timeout caps the interval at 200 ms.
    #[wasm_bindgen_test]
    async fn a_joined_call_keeps_itself_alive() {
        #[derive(Serialize)]
        struct TestJoinParams {
            slot_id: &'static str,
            application: &'static str,
            keep_alive_timeout_ms: u64,
        }
        let client = WasmRtcClient::new(mock_host(""));
        let room = open(&client, None).await;
        let params = serde_wasm_bindgen::to_value(&TestJoinParams {
            slot_id: SLOT,
            application: "m.call",
            keep_alive_timeout_ms: 400,
        })
        .unwrap();
        let call = room.join_call(params).await.expect("join");
        let before = restarts();

        matrix_rtc_core::executor::sleep(std::time::Duration::from_millis(500)).await;
        assert!(restarts() - before >= 2.0, "restarted on its own");
        call.leave(JsValue::UNDEFINED).await.expect("leave");
    }

    #[wasm_bindgen_test]
    async fn a_room_is_opened_once() {
        let client = WasmRtcClient::new(mock_host(""));
        let _room = open(&client, None).await;
        assert!(
            client
                .room(ROOM.to_owned(), JsValue::UNDEFINED)
                .await
                .is_err()
        );
    }

    /// The sticky dialect's outbound rewrite, through the real send path: a
    /// join in a `sticky_events` room must put the EC-2025 mirror fields on
    /// the wire, or that generation cannot see us.
    #[wasm_bindgen_test]
    async fn a_sticky_compat_join_mirrors_the_legacy_fields() {
        #[derive(Serialize)]
        struct TestTransport {
            #[serde(rename = "type")]
            kind: &'static str,
            livekit_service_url: &'static str,
        }
        #[derive(Serialize)]
        struct TestJoinParams {
            slot_id: &'static str,
            application: &'static str,
            transport: TestTransport,
        }

        let client = WasmRtcClient::new(mock_host(
            "if (!Array.isArray(content.rtc_transports) \
                 || !Array.isArray(content.versions) \
                 || !content.member || content.member.user_id === undefined \
                 || content.member.device_id === undefined) \
                 return Promise.reject(new Error('legacy mirror fields missing: ' + JSON.stringify(content)));",
        ));
        let room = open(&client, Some("sticky_events")).await;
        let params = serde_wasm_bindgen::to_value(&TestJoinParams {
            slot_id: SLOT,
            application: "m.call",
            transport: TestTransport {
                kind: "livekit",
                livekit_service_url: "https://sfu.example.org/livekit/jwt",
            },
        })
        .unwrap();

        room.join_call(params)
            .await
            .expect("the rewritten membership should satisfy the strict mock");
    }

    #[wasm_bindgen_test]
    async fn a_left_call_has_nothing_to_keep_alive() {
        let client = WasmRtcClient::new(mock_host(""));
        let room = open(&client, None).await;
        let call = room.join_call(join_params()).await.expect("join");
        call.leave(JsValue::UNDEFINED).await.expect("leave");
        assert!(!call.is_live());
    }
}
