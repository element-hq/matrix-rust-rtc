// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! WebAssembly bindings for the MatrixRTC core.
//!
//! The page implements one `MatrixBackendHost` object (sends and
//! subscriptions) over its Matrix client; the manager attaches rooms and the
//! library feeds itself. JS-shaped payloads are converted into core DTOs here
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

use std::cell::RefCell;
use std::sync::Arc;

use matrix_rtc_bridge::compat::{DialectBackend, ElementCallCompat, ingest};
use matrix_rtc_bridge::feeder::{
    AttachOptions, AttachedRooms, RoomAttachment, RoomFeeder, RoomModes, ToDeviceFeeder,
};
use matrix_rtc_bridge::transports;
use matrix_rtc_call::{
    CallJoinParams, CallSessionManager, Mentions, NotificationType, NotifyConfig,
};
use matrix_rtc_core::{
    EncryptionConfig, JoinSessionParams, LeaveSessionParams, MatrixBackend, RtcSessionManager,
    RtcTransport, SlotEncryption, TransportIntent,
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
use tokio::sync::Mutex;
use wasm_bindgen::prelude::*;

type Backend = DialectBackend<JsBackend>;
type Manager = Arc<Mutex<CallSessionManager<Backend>>>;

#[wasm_bindgen]
/// WebAssembly-facing wrapper around the call layer's `CallSessionManager`,
/// fed from the page's `MatrixBackendHost`.
pub struct WasmRtcSessionManager {
    inner: Manager,
    /// The page's backend behind the dialect wrapper; the manager holds the
    /// same `Arc`.
    backend: Arc<Backend>,
    /// Which generation each attached room is read and written for.
    modes: RoomModes,
    /// The attached rooms, by room id. Freeing the manager detaches them.
    rooms: AttachedRooms<RoomAttachment>,
    /// The session-wide to-device subscription, started by the first attach.
    to_device_feeder: RefCell<Option<ToDeviceFeeder>>,
}

#[wasm_bindgen]
impl WasmRtcSessionManager {
    #[wasm_bindgen(constructor)]
    /// One manager per Matrix session, over the page's backend object (see
    /// `MatrixBackendHost`).
    pub fn new(#[wasm_bindgen(unchecked_param_type = "MatrixBackendHost")] host: JsValue) -> Self {
        log::info!("manager: created over the host backend");
        // `Rc` is not an option: the core takes `Arc<T>` on every target.
        #[allow(clippy::arc_with_non_send_sync)]
        let backend = Arc::new(DialectBackend::new(Arc::new(JsBackend::new(host))));
        #[allow(clippy::arc_with_non_send_sync)]
        let inner = Arc::new(Mutex::new(CallSessionManager::new(
            RtcSessionManager::with_backend(backend.clone()),
        )));
        Self {
            inner,
            backend,
            modes: RoomModes::default(),
            rooms: AttachedRooms::default(),
            to_device_feeder: RefCell::new(None),
        }
    }

    /// Attaches a room: the library subscribes to what the room needs in the
    /// given mode and applies the current state. Resolves once that state is
    /// applied, so a `join` issued afterwards sees it. Attaching a room that
    /// is attached, or still being attached, rejects. Wait for the attach
    /// before detaching the room.
    ///
    /// `options` is `{ element_call_compat?: "off" | "sticky_events" | "state_events" }`.
    #[wasm_bindgen(js_name = attachRoom)]
    pub async fn attach_room(
        &self,
        room_id: String,
        #[wasm_bindgen(unchecked_param_type = "AttachOptionsIn | null | undefined")]
        options: JsValue,
    ) -> Result<(), JsError> {
        let Some(reservation) = self.rooms.reserve(&room_id) else {
            return Err(JsError::new(&format!("{room_id} is already attached")));
        };
        let options: Option<WasmAttachOptions> = serde_wasm_bindgen::from_value(options)
            .map_err(|err| JsError::new(&format!("invalid attach options: {err}")))?;
        let compat = compat::parse_compat(
            options
                .as_ref()
                .and_then(|options| options.element_call_compat.as_deref()),
        )?;
        log::info!("manager: [{room_id}] attaching in {compat:?} mode");

        self.ensure_to_device_feeder().await?;

        let (attachment, run) = RoomFeeder::attach(
            self.backend.clone(),
            self.inner.clone(),
            self.modes.clone(),
            room_id.clone(),
            AttachOptions {
                element_call_compat: compat,
            },
        )
        .await
        .map_err(|err| JsError::new(&err.to_string()))?;
        wasm_bindgen_futures::spawn_local(run.run());
        attachment.seeded().await;
        log::info!("manager: [{room_id}] attached and seeded");
        reservation.fill(attachment);
        Ok(())
    }

    /// Detaches a room: leaves any session joined in it, then ends the
    /// subscription. Nothing delivered afterwards is applied. Detaching an
    /// unattached room is a no-op.
    #[wasm_bindgen(js_name = detachRoom)]
    pub async fn detach_room(&self, room_id: String) -> Result<(), JsError> {
        let joined = self.inner.lock().await.joined_slots(&room_id);
        for slot_id in joined {
            log::info!("manager: [{room_id}/{slot_id}] leaving before detaching");
            if let Err(error) = self
                .inner
                .lock()
                .await
                .leave(room_id.clone(), slot_id, LeaveSessionParams::default())
                .await
            {
                log::warn!("manager: leave before detach failed: {error}");
            }
        }
        let attached = self.rooms.remove(&room_id);
        match attached {
            Some(attachment) => {
                attachment.detach();
                self.backend.clear_dialect(&room_id);
                log::info!("manager: [{room_id}] detached");
            }
            None => log::debug!("manager: [{room_id}] detach of a room that is not attached"),
        }
        Ok(())
    }

    /// Everything the manager and its sessions believe, as a JSON string.
    #[wasm_bindgen(js_name = debugSnapshot)]
    pub async fn debug_snapshot(&self) -> String {
        self.inner.lock().await.debug_snapshot().to_string()
    }

    pub async fn session_count(&self) -> u32 {
        self.inner.lock().await.session_count() as u32
    }

    pub async fn member_count(&self, room_id: String, slot_id: String) -> Option<u32> {
        self.inner
            .lock()
            .await
            .member_count(&room_id, &slot_id)
            .map(|count| count as u32)
    }

    /// Joins an RTC session in an attached room.
    ///
    /// `params`:
    ///   - `room_id`, `slot_id` (e.g. "m.call#ROOM"), `application` (e.g. "m.call")
    ///   - `transport`: the transport to publish on; omit to take the first
    ///     LiveKit one the homeserver advertises (the host's `rtcTransports`)
    ///   - `receive_only`: join without publishing; `can_subscribe` then lists
    ///     the transport types this member can receive on
    ///   - `keep_alive_timeout_ms` (default 30000), `sticky_duration_ms`
    ///     (default 3600000), `degraded_lifetime_ms` (default 300000; not below)
    ///   - `encryption_config`, `notify`, `reactions`
    ///
    /// Refreshing the keep-alive is the page's job: call [`Self::heartbeat`]
    /// on an interval while joined. Resolves to the `member.id` this join used;
    /// the SDK generates it (MSC4143 requires a fresh one per join). Rejects
    /// when the room is not attached or its state holds no open slot of this
    /// id.
    pub async fn join(
        &self,
        #[wasm_bindgen(unchecked_param_type = "JoinParamsIn")] params: JsValue,
    ) -> Result<String, JsError> {
        let params: WasmJoinSessionParams =
            serde_wasm_bindgen::from_value(params).map_err(|err| {
                log::warn!("manager: invalid join params: {err}");
                JsError::new(&format!("invalid join params: {err}"))
            })?;
        let room_id = params.room_id.clone();
        let slot_id = params.slot_id.clone();
        if !self.modes.is_attached(&room_id) {
            return Err(JsError::new(&format!(
                "{room_id} is not attached; attach the room before joining"
            )));
        }
        let mode = self.modes.mode(&room_id);
        let user_id = self.backend.own_user_id();
        let device_id = self.backend.own_device_id();

        log::info!(
            "manager: join requested [{room_id}/{slot_id}] user={user_id} device={device_id} \
             application={} compat={mode:?}",
            params.application,
        );

        // The join's own choice, else the first LiveKit transport the
        // homeserver advertises.
        let transport = match params.transport_intent()? {
            Some(chosen) => chosen,
            None => {
                let advertised = self
                    .backend
                    .rtc_transports()
                    .await
                    .map_err(|err| JsError::new(&err.to_string()))?;
                transports::choose(&advertised, None)
                    .map_err(|err| JsError::new(&err.to_string()))?
            }
        };

        let mut core_params = params.into_core(user_id.clone(), device_id.clone(), transport)?;
        // Not always a fresh id: see `ingest::member_id` for the one generation
        // where a fresh one makes us mark ourselves departed on our own join.
        let member_id = ingest::member_id(mode, &user_id, &device_id);
        core_params.rtc.membership_id = Some(member_id.clone());

        // Before the join, not after: the join itself sends the membership
        // (and arms the delayed leave), so a dialect registered afterwards
        // would let exactly the two events that announce us go out
        // spec-current.
        self.backend.set_dialect(
            &room_id,
            ingest::outbound_dialect(mode, &user_id, &device_id, &room_id, &slot_id),
        );

        let result = self
            .inner
            .lock()
            .await
            .join(core_params)
            .await
            .map_err(|err| JsError::new(&err.to_string()));

        match &result {
            Ok(_) => log::info!("manager: join succeeded as {member_id}"),
            Err(_) => log::warn!("manager: join failed"),
        }

        result.map(|_| member_id)
    }

    /// Our `member.id` in one session, or `undefined` if there is no such
    /// session or it has not joined. Changes on every join, so read it when
    /// needed rather than caching what `join` returned.
    #[wasm_bindgen(js_name = ownMemberId)]
    pub async fn own_member_id(&self, room_id: String, slot_id: String) -> Option<String> {
        self.inner.lock().await.own_member_id(&room_id, &slot_id)
    }

    /// The event id of our current membership event in one session, or
    /// `undefined` if there is no such session or it has not joined. Moves on
    /// every sticky refresh, so read it at the moment of use.
    #[wasm_bindgen(js_name = ownMembershipEventId)]
    pub async fn own_membership_event_id(
        &self,
        room_id: String,
        slot_id: String,
    ) -> Option<String> {
        self.inner
            .lock()
            .await
            .own_membership_event_id(&room_id, &slot_id)
    }

    // ---- Reactions and raised hands ----
    //
    // Element Call's reactions are ordinary room events relating to the
    // reacting member's membership event. The library reads them from the
    // attached room (timeline events, redactions and the relations of each
    // membership event); the page plays any sound. Results surface on the
    // media session as `hand_raised` / `hand_lowered` / `reaction` events, on
    // `rtc_participant.hand_raised_at_ms`, and here as `raisedHands`.

    /// Sends an Element Call emoji reaction in one session. `name` is what
    /// peers pick a sound by (see [`reaction_catalog`]); only the first
    /// grapheme of `emoji` is sent. Resolves to the event id; rejects inside
    /// the send cooldown.
    #[wasm_bindgen(js_name = sendReaction)]
    pub async fn send_reaction(
        &self,
        room_id: String,
        slot_id: String,
        emoji: String,
        name: String,
    ) -> Result<String, JsError> {
        self.inner
            .lock()
            .await
            .send_reaction(&room_id, &slot_id, &emoji, &name)
            .await
            .map_err(|err| JsError::new(&err.to_string()))
    }

    /// Raises our hand in one session. Idempotent while it is up.
    #[wasm_bindgen(js_name = raiseHand)]
    pub async fn raise_hand(&self, room_id: String, slot_id: String) -> Result<(), JsError> {
        self.inner
            .lock()
            .await
            .raise_hand(&room_id, &slot_id)
            .await
            .map_err(|err| JsError::new(&err.to_string()))
    }

    /// Lowers our hand in one session. A no-op when it is down.
    #[wasm_bindgen(js_name = lowerHand)]
    pub async fn lower_hand(&self, room_id: String, slot_id: String) -> Result<(), JsError> {
        self.inner
            .lock()
            .await
            .lower_hand(&room_id, &slot_id)
            .await
            .map_err(|err| JsError::new(&err.to_string()))
    }

    /// The raised hands of one session, oldest first, as `RaisedHand[]`.
    #[wasm_bindgen(js_name = raisedHands, unchecked_return_type = "RaisedHand[]")]
    pub async fn raised_hands(&self, room_id: String, slot_id: String) -> Result<JsValue, JsError> {
        let hands = self
            .inner
            .lock()
            .await
            .raised_hands(&room_id, &slot_id)
            .unwrap_or_default();
        serde_wasm_bindgen::to_value(&hands).map_err(|err| JsError::new(&err.to_string()))
    }

    /// Leaves an RTC session.
    ///
    /// `params` is `{ leave_reason?: { code, reason? } }` — e.g.
    /// `{ code: "leave" }` for an intentional hang-up. Defaults to that.
    pub async fn leave(
        &self,
        room_id: String,
        slot_id: String,
        #[wasm_bindgen(unchecked_param_type = "LeaveParamsIn")] params: JsValue,
    ) -> Result<(), JsError> {
        let params: WasmLeaveSessionParams = serde_wasm_bindgen::from_value(params)
            .map_err(|err| JsError::new(&format!("invalid leave params: {err}")))?;

        log::info!(
            "manager: leave requested [{room_id}/{slot_id}] reason={:?}",
            params.leave_reason,
        );

        let result = self
            .inner
            .lock()
            .await
            .leave(room_id, slot_id, params.into_core())
            .await
            .map_err(|err| JsError::new(&err.to_string()));

        match &result {
            Ok(()) => log::info!("manager: leave succeeded"),
            Err(_) => log::warn!("manager: leave failed"),
        }
        // The dialect stays registered: the room is still attached in its
        // mode. Detaching clears it.
        result
    }

    /// Restarts the keep-alive for one session: reschedules the delayed leave,
    /// and re-sends the membership if its sticky entry is halfway to expiring.
    /// Also flushes a key rotation that has come due.
    ///
    /// The core arms no timers and this binding starts no driver — **the page
    /// must call this on an interval while joined** (`setInterval`,
    /// [`HEARTBEAT_INTERVAL_MS`]), or the dead man's switch fires and peers see
    /// us depart mid-call.
    ///
    /// Resolves to `false` if there is no joined session for
    /// `(room_id, slot_id)`, which means there is nothing to keep alive.
    pub async fn heartbeat(&self, room_id: String, slot_id: String) -> bool {
        self.inner.lock().await.heartbeat(&room_id, &slot_id).await
    }

    /// When the session's next key rotation falls due, in epoch milliseconds,
    /// or `undefined` when none is owed. Diagnostics: the rotation itself is
    /// performed by [`Self::heartbeat`] and by the media layer's
    /// switch-complete signal, not by polling this.
    #[wasm_bindgen(js_name = keyRotationDueAtMs)]
    pub async fn key_rotation_due_at_ms(&self, room_id: String, slot_id: String) -> Option<f64> {
        self.inner
            .lock()
            .await
            .key_rotation_due_at_ms(&room_id, &slot_id)
            .map(|at| at as f64)
    }

    /// Performs the session's key rotation if one has come due; a no-op
    /// otherwise. Resolves to whether a rotation ran.
    #[wasm_bindgen(js_name = flushDueKeyRotation)]
    pub async fn flush_due_key_rotation(&self, room_id: String, slot_id: String) -> bool {
        self.inner
            .lock()
            .await
            .flush_due_key_rotation(&room_id, &slot_id)
            .await
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
        room_id: String,
        slot_id: String,
        application_type: String,
        #[wasm_bindgen(unchecked_param_type = "SlotEncryptionIn | null | undefined")]
        encryption: JsValue,
    ) -> Result<(), JsError> {
        let encryption: Option<SlotEncryption> = serde_wasm_bindgen::from_value(encryption)
            .map_err(|err| JsError::new(&format!("invalid slot encryption payload: {err}")))?;

        log::info!(
            "manager: [{room_id}/{slot_id}] opening slot: application={application_type} \
             encryption={encryption:?}",
        );

        self.inner
            .lock()
            .await
            .open_slot(room_id, slot_id, application_type, encryption)
            .await
            .map_err(|err| {
                log::warn!("manager: could not open the slot: {err}");
                JsError::new(&err.to_string())
            })
    }

    /// Closes a slot, by setting its `m.rtc.slot` status to `closed`.
    ///
    /// Every member of it becomes left as soon as clients apply the new state —
    /// this ends the call for everyone, not just for us. Leaving is
    /// [`Self::leave`].
    #[wasm_bindgen(js_name = closeSlot)]
    pub async fn close_slot(&self, room_id: String, slot_id: String) -> Result<(), JsError> {
        log::info!("manager: [{room_id}/{slot_id}] closing slot");

        self.inner
            .lock()
            .await
            .close_slot(room_id, slot_id)
            .await
            .map_err(|err| {
                log::warn!("manager: could not close the slot: {err}");
                JsError::new(&err.to_string())
            })
    }
}

impl WasmRtcSessionManager {
    /// The session-wide to-device subscription, started once.
    async fn ensure_to_device_feeder(&self) -> Result<(), JsError> {
        if self.to_device_feeder.borrow().is_some() {
            return Ok(());
        }
        let (feeder, run) =
            ToDeviceFeeder::start(self.backend.clone(), self.inner.clone(), self.modes.clone())
                .await
                .map_err(|err| JsError::new(&err.to_string()))?;
        wasm_bindgen_futures::spawn_local(run.run());
        *self.to_device_feeder.borrow_mut() = Some(feeder);
        log::info!("manager: to-device subscription started");
        Ok(())
    }

    /// The mode `room_id` was attached in; an unattached room is spec-current.
    pub(crate) fn element_call_compat_for(&self, room_id: &str) -> ElementCallCompat {
        self.modes.mode(room_id)
    }

    /// The page's backend, for the media layer's token exchange.
    pub(crate) fn backend(&self) -> Arc<dyn MatrixBackend> {
        self.backend.clone()
    }
}

/// How a room is attached.
#[derive(Debug, Default, Deserialize)]
pub struct WasmAttachOptions {
    /// `"off"` (the default), `"sticky_events"` or `"state_events"`. One
    /// decision for the room: what the library subscribes to, how it renders
    /// our sends, the `member.id` we join with, how an inbound media key is
    /// bound, the SFU identity and the token endpoint.
    #[serde(default)]
    pub element_call_compat: Option<String>,
}

/// How often a page should call [`WasmRtcSessionManager::heartbeat`] while
/// joined. Matches the FFI's keep-alive driver interval.
#[wasm_bindgen(js_name = HEARTBEAT_INTERVAL_MS)]
pub fn heartbeat_interval_ms() -> u32 {
    10_000
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
    pub room_id: String,
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

    pub fn into_core(
        self,
        user_id: String,
        device_id: String,
        transport: TransportIntent,
    ) -> Result<CallJoinParams, JsError> {
        let encryption_config = self.encryption_config.map(Into::into);
        let rtc = JoinSessionParams {
            user_id,
            device_id,
            // Filled in by the join entry point, which generates a fresh id per
            // join and returns it.
            membership_id: None,
            room_id: self.room_id,
            slot_id: self.slot_id,
            application: self.application.into(),
            transport,
            keep_alive_timeout_ms: self.keep_alive_timeout_ms,
            sticky_duration_ms: self.sticky_duration_ms,
            degraded_lifetime_ms: self.degraded_lifetime_ms,
            encryption_config,
        };
        Ok(CallJoinParams {
            rtc,
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
    /// the only joined member and an empty sticky set, so `attachRoom` seeds.
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
            "return Promise.resolve();",
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

    async fn attach(manager: &WasmRtcSessionManager, compat: Option<&str>) {
        // A plain object, as a page passes: the default serializer turns a
        // `json!` map into an ES `Map`, which reads back as no options at all.
        let options = match compat {
            Some(compat) => serde_json::json!({ "element_call_compat": compat })
                .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
                .unwrap(),
            None => JsValue::UNDEFINED,
        };
        manager
            .attach_room(ROOM.to_owned(), options)
            .await
            .expect("attach");
    }

    /// A full join on the wasm target, with the transport taken from the
    /// backend. Also pins the clock: the join path reads wall-clock time,
    /// which on wasm32-unknown-unknown must come from `Date.now()`.
    #[wasm_bindgen_test]
    async fn a_join_succeeds_on_wasm() {
        #[derive(Serialize)]
        struct TestJoinParams {
            room_id: &'static str,
            slot_id: &'static str,
            application: &'static str,
        }

        let manager = WasmRtcSessionManager::new(mock_host(""));
        attach(&manager, None).await;

        let params = serde_wasm_bindgen::to_value(&TestJoinParams {
            room_id: ROOM,
            slot_id: SLOT,
            application: "m.call",
        })
        .unwrap();

        let member_id = manager.join(params).await.expect("join should succeed");
        assert!(!member_id.is_empty());
        assert_eq!(
            manager
                .own_member_id(ROOM.to_owned(), SLOT.to_owned())
                .await,
            Some(member_id),
        );
        assert!(manager.heartbeat(ROOM.to_owned(), SLOT.to_owned()).await);
    }

    #[wasm_bindgen_test]
    async fn a_join_needs_an_attached_room() {
        #[derive(Serialize)]
        struct TestJoinParams {
            room_id: &'static str,
            slot_id: &'static str,
            application: &'static str,
        }
        let manager = WasmRtcSessionManager::new(mock_host(""));
        let params = serde_wasm_bindgen::to_value(&TestJoinParams {
            room_id: ROOM,
            slot_id: SLOT,
            application: "m.call",
        })
        .unwrap();
        assert!(manager.join(params).await.is_err());
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
            room_id: &'static str,
            slot_id: &'static str,
            application: &'static str,
            transport: TestTransport,
        }

        let manager = WasmRtcSessionManager::new(mock_host(
            "if (!Array.isArray(content.rtc_transports) \
                 || !Array.isArray(content.versions) \
                 || !content.member || content.member.user_id === undefined \
                 || content.member.device_id === undefined) \
                 return Promise.reject(new Error('legacy mirror fields missing: ' + JSON.stringify(content)));",
        ));
        attach(&manager, Some("sticky_events")).await;
        let params = serde_wasm_bindgen::to_value(&TestJoinParams {
            room_id: ROOM,
            slot_id: SLOT,
            application: "m.call",
            transport: TestTransport {
                kind: "livekit",
                livekit_service_url: "https://sfu.example.org/livekit/jwt",
            },
        })
        .unwrap();

        manager
            .join(params)
            .await
            .expect("the rewritten membership should satisfy the strict mock");
    }

    #[wasm_bindgen_test]
    async fn heartbeat_without_a_session_reports_nothing_to_keep_alive() {
        let manager = WasmRtcSessionManager::new(mock_host(""));
        assert!(!manager.heartbeat(ROOM.to_owned(), SLOT.to_owned()).await);
    }
}
