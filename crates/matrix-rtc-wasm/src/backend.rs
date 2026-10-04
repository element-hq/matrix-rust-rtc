// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The host-implemented Matrix backend over wasm-bindgen: a JS object with
//! the send and subscribe halves (`MatrixBackendHost` in `ts_types`), the
//! sink classes it delivers into, and the adapter to the core's
//! `MatrixBackend`. Content crosses as plain JS objects; DTOs keep JS shapes
//! out of the core.

use std::sync::Arc;

use async_trait::async_trait;
use js_sys::{Array, Function, Reflect};
use matrix_rtc_core::{
    BackendError, CommandError, EventIn, MatrixBackend, OpenIdToken, RoomSink, RoomSubjects,
    Subscription, ToDeviceDelivery, ToDeviceMessageIn, ToDeviceRecipient, ToDeviceSink,
};
use serde::Serialize;
use serde_json::Value;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;

/// Serializes as plain JS objects: a real Matrix client `JSON.stringify`s
/// contents, and an ES `Map` (serde-wasm-bindgen's default for serde maps)
/// stringifies to `{}`.
pub(crate) fn to_plain_js<T: Serialize>(value: &T) -> Result<JsValue, CommandError> {
    value
        .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
        .map_err(|e| CommandError::SerializationError(e.to_string()))
}

fn js_error_message(error: &JsValue) -> String {
    if error.is_undefined() || error.is_null() {
        "unknown error".to_owned()
    } else if let Ok(error) = error.clone().dyn_into::<js_sys::Error>() {
        error.message().into()
    } else if let Some(message) = error.as_string() {
        message
    } else {
        format!("{error:?}")
    }
}

/// A matrix-js-sdk `MatrixError` carries `errcode` and `httpStatus`; read
/// them when present so the library can classify the failure.
fn js_error_parts(error: &JsValue) -> (Option<String>, Option<u16>, String) {
    let field = |name: &str| Reflect::get(error, &JsValue::from_str(name)).ok();
    let errcode = field("errcode").and_then(|v| v.as_string());
    let status = field("httpStatus")
        .and_then(|v| v.as_f64())
        .map(|status| status as u16);
    (errcode, status, js_error_message(error))
}

fn send_error(error: JsValue) -> CommandError {
    let converted = CommandError::SendError(js_error_message(&error));
    log::warn!("command failed: {converted}");
    converted
}

fn delayed_error(error: JsValue) -> CommandError {
    let (errcode, status, message) = js_error_parts(&error);
    let converted = CommandError::delayed_event_failure(errcode.as_deref(), status, message);
    log::warn!("command failed: {converted}");
    converted
}

fn backend_error(error: JsValue) -> BackendError {
    let (errcode, status, message) = js_error_parts(&error);
    BackendError::with_matrix_error(errcode, status, message)
}

/// Reads the event id out of whatever a send method resolved to: matrix-js-sdk's
/// `{ event_id }`, a wrapper's `{ eventId }`, or a bare string. Anything else
/// is a failed send, not a silent success.
fn js_event_id(resolved: &JsValue, method: &str) -> Result<String, CommandError> {
    if let Some(event_id) = resolved.as_string() {
        return Ok(event_id);
    }
    for key in ["event_id", "eventId"] {
        if let Ok(value) = Reflect::get(resolved, &JsValue::from_str(key))
            && let Some(event_id) = value.as_string()
        {
            return Ok(event_id);
        }
    }
    Err(CommandError::SendError(format!(
        "{method} must resolve to the event id (matrix-js-sdk's `{{event_id}}`); got {resolved:?}"
    )))
}

/// Where the host delivers a room's subjects. Every call returns at once.
///
/// For sticky events, state events and joined members each call carries the
/// room's **complete current set** for that subject, and the first call is the
/// current set at subscription time. An empty set means none.
#[wasm_bindgen]
pub struct WasmRoomSink {
    inner: Arc<dyn RoomSink>,
}

fn events_from_js(events: JsValue) -> Result<Vec<EventIn>, JsError> {
    serde_wasm_bindgen::from_value(events)
        .map_err(|err| JsError::new(&format!("invalid event list: {err}")))
}

#[wasm_bindgen]
impl WasmRoomSink {
    #[wasm_bindgen(js_name = onStickyEvents)]
    pub fn on_sticky_events(
        &self,
        #[wasm_bindgen(unchecked_param_type = "EventIn[]")] events: JsValue,
    ) -> Result<(), JsError> {
        self.inner.on_sticky_events(events_from_js(events)?);
        Ok(())
    }

    #[wasm_bindgen(js_name = onStateEvents)]
    pub fn on_state_events(
        &self,
        event_type: String,
        #[wasm_bindgen(unchecked_param_type = "EventIn[]")] events: JsValue,
    ) -> Result<(), JsError> {
        self.inner
            .on_state_events(event_type, events_from_js(events)?);
        Ok(())
    }

    #[wasm_bindgen(js_name = onJoinedMembers)]
    pub fn on_joined_members(&self, user_ids: Vec<String>) {
        self.inner.on_joined_members(user_ids);
    }

    #[wasm_bindgen(js_name = onEncryption)]
    pub fn on_encryption(&self, encrypted: bool) {
        self.inner.on_encryption(encrypted);
    }

    #[wasm_bindgen(js_name = onTimelineEvents)]
    pub fn on_timeline_events(
        &self,
        #[wasm_bindgen(unchecked_param_type = "EventIn[]")] events: JsValue,
    ) -> Result<(), JsError> {
        self.inner.on_timeline_events(events_from_js(events)?);
        Ok(())
    }

    #[wasm_bindgen(js_name = onRedaction)]
    pub fn on_redaction(&self, event_id: String) {
        self.inner.on_redaction(event_id);
    }
}

/// Where the host delivers to-device messages of the subscribed types.
#[wasm_bindgen]
pub struct WasmToDeviceSink {
    inner: Arc<dyn ToDeviceSink>,
}

#[wasm_bindgen]
impl WasmToDeviceSink {
    #[wasm_bindgen(js_name = onToDeviceMessage)]
    pub fn on_to_device_message(
        &self,
        #[wasm_bindgen(unchecked_param_type = "ToDeviceMessageIn")] message: JsValue,
    ) -> Result<(), JsError> {
        let message: ToDeviceMessageIn = serde_wasm_bindgen::from_value(message)
            .map_err(|err| JsError::new(&format!("invalid to-device message: {err}")))?;
        self.inner.on_to_device_message(message);
        Ok(())
    }
}

/// The `{ cancel() }` object a host's subscribe method returned.
struct JsSubscription {
    object: JsValue,
}

impl Subscription for JsSubscription {
    fn cancel(&self) {
        let Ok(method) = Reflect::get(&self.object, &JsValue::from_str("cancel")) else {
            return;
        };
        match method.dyn_into::<Function>() {
            Ok(cancel) => {
                if let Err(error) = cancel.call0(&self.object) {
                    log::warn!("subscription cancel failed: {}", js_error_message(&error));
                }
            }
            Err(_) => log::warn!("the host's subscription has no cancel()"),
        }
    }
}

/// The core's backend over the host's `MatrixBackendHost` object.
pub struct JsBackend {
    host: JsValue,
    /// Optional callback for logging/debugging.
    on_command: Option<Function>,
}

impl JsBackend {
    pub fn new(host: JsValue) -> Self {
        Self {
            host,
            on_command: None,
        }
    }

    pub fn set_debug_callback(&mut self, callback: Function) {
        self.on_command = Some(callback);
    }

    fn log_command(&self, description: &str) {
        log::debug!("command sending: {description}");
        if let Some(callback) = &self.on_command {
            let _ = callback.call1(&JsValue::NULL, &JsValue::from_str(description));
        }
    }

    fn method(&self, name: &str) -> Result<Function, JsValue> {
        let method = Reflect::get(&self.host, &JsValue::from_str(name))?;
        if method.is_undefined() {
            return Err(JsValue::from_str(&format!("host missing method: {name}")));
        }
        method.dyn_into::<Function>()
    }

    /// Calls `name` synchronously and returns what it returned.
    fn call_sync(&self, name: &str, args: Vec<JsValue>) -> Result<JsValue, JsValue> {
        let js_args = Array::new();
        for arg in &args {
            js_args.push(arg);
        }
        Reflect::apply(&self.method(name)?, &self.host, &js_args)
    }

    /// Calls `name` and awaits the returned Promise (a non-Promise result
    /// resolves immediately).
    async fn call_async(&self, name: &str, args: Vec<JsValue>) -> Result<JsValue, JsValue> {
        let result = self.call_sync(name, args)?;
        let promise = if result.is_instance_of::<js_sys::Promise>() {
            result.unchecked_into::<js_sys::Promise>()
        } else {
            js_sys::Promise::resolve(&result)
        };
        wasm_bindgen_futures::JsFuture::from(promise).await
    }

    fn identity(&self, name: &str) -> String {
        match self.call_sync(name, Vec::new()) {
            Ok(value) => value.as_string().unwrap_or_default(),
            Err(error) => {
                log::warn!("host {name}() failed: {}", js_error_message(&error));
                String::new()
            }
        }
    }
}

// Unconditionally `?Send`: this crate is only ever built for wasm32, where the
// core's trait takes the `?Send` shape, and these futures wrap JS promises.
#[async_trait(?Send)]
impl MatrixBackend for JsBackend {
    fn own_user_id(&self) -> String {
        self.identity("ownUserId")
    }

    fn own_device_id(&self) -> String {
        self.identity("ownDeviceId")
    }

    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        duration_ms: u64,
    ) -> Result<String, CommandError> {
        self.log_command(&format!(
            "send_sticky_event: room={room_id}, type={event_type}, duration={duration_ms}ms"
        ));
        let resolved = self
            .call_async(
                "sendStickyEvent",
                vec![
                    JsValue::from_str(&room_id),
                    JsValue::from_str(&event_type),
                    to_plain_js(&content)?,
                    JsValue::from_f64(duration_ms as f64),
                ],
            )
            .await
            .map_err(send_error)?;
        js_event_id(&resolved, "sendStickyEvent")
    }

    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: Option<String>,
        content: Value,
        delay_ms: u64,
    ) -> Result<String, CommandError> {
        self.log_command(&format!(
            "send_delayed_event: room={room_id}, type={event_type}, state_key={state_key:?}, \
             delay={delay_ms}ms"
        ));
        let resolved = self
            .call_async(
                "sendDelayedEvent",
                vec![
                    JsValue::from_str(&room_id),
                    JsValue::from_str(&event_type),
                    match &state_key {
                        Some(state_key) => JsValue::from_str(state_key),
                        None => JsValue::NULL,
                    },
                    to_plain_js(&content)?,
                    JsValue::from_f64(delay_ms as f64),
                ],
            )
            .await
            .map_err(delayed_error)?;
        resolved.as_string().ok_or_else(|| {
            CommandError::SendError("sendDelayedEvent did not return a string delay id".to_owned())
        })
    }

    async fn restart_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        self.log_command(&format!(
            "restart_delayed_event: room={room_id}, delay_id={delay_id}"
        ));
        self.call_async(
            "restartDelayedEvent",
            vec![JsValue::from_str(&room_id), JsValue::from_str(&delay_id)],
        )
        .await
        .map_err(delayed_error)?;
        Ok(())
    }

    async fn cancel_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        self.log_command(&format!(
            "cancel_delayed_event: room={room_id}, delay_id={delay_id}"
        ));
        self.call_async(
            "cancelDelayedEvent",
            vec![JsValue::from_str(&room_id), JsValue::from_str(&delay_id)],
        )
        .await
        .map_err(delayed_error)?;
        Ok(())
    }

    async fn send_to_device_message(
        &self,
        recipients: Vec<ToDeviceRecipient>,
        message_type: String,
        content: Value,
    ) -> Result<Vec<ToDeviceDelivery>, CommandError> {
        self.log_command(&format!(
            "send_to_device_message: {} recipient(s), type={message_type}",
            recipients.len(),
        ));
        // `[{userId, deviceId}, ...]`, mirroring matrix-js-sdk's own to-device
        // shape so a host can pass it straight through.
        let js_recipients = to_plain_js(
            &recipients
                .iter()
                .map(|recipient| {
                    serde_json::json!({
                        "userId": recipient.user_id,
                        "deviceId": recipient.device_id,
                    })
                })
                .collect::<Vec<_>>(),
        )?;
        let result = self
            .call_async(
                "sendToDeviceMessage",
                vec![
                    js_recipients,
                    JsValue::from_str(&message_type),
                    to_plain_js(&content)?,
                ],
            )
            .await
            .map_err(send_error)?;

        // A host that resolves with nothing is taken to have served everyone;
        // one that resolves with `[{userId, deviceId, error?}, ...]` is believed.
        if result.is_undefined() || result.is_null() {
            return Ok(recipients.into_iter().map(ToDeviceDelivery::sent).collect());
        }

        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct JsDelivery {
            user_id: String,
            device_id: String,
            #[serde(default)]
            error: Option<String>,
        }

        let reported: Vec<JsDelivery> = serde_wasm_bindgen::from_value(result).map_err(|e| {
            CommandError::SerializationError(format!(
                "sendToDeviceMessage resolved with something that is not a delivery list: {e}"
            ))
        })?;
        Ok(reported
            .into_iter()
            .map(|delivery| {
                let recipient = ToDeviceRecipient::new(delivery.user_id, delivery.device_id);
                match delivery.error {
                    Some(error) => ToDeviceDelivery::failed(recipient, error),
                    None => ToDeviceDelivery::sent(recipient),
                }
            })
            .collect())
    }

    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content: Value,
    ) -> Result<String, CommandError> {
        self.log_command(&format!(
            "send_state_event: room={room_id}, type={event_type}, state_key={state_key}"
        ));
        let resolved = self
            .call_async(
                "sendStateEvent",
                vec![
                    JsValue::from_str(&room_id),
                    JsValue::from_str(&event_type),
                    JsValue::from_str(&state_key),
                    to_plain_js(&content)?,
                ],
            )
            .await
            .map_err(send_error)?;
        js_event_id(&resolved, "sendStateEvent")
    }

    async fn send_room_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
    ) -> Result<String, CommandError> {
        self.log_command(&format!(
            "send_room_event: room={room_id}, type={event_type}"
        ));
        let resolved = self
            .call_async(
                "sendRoomEvent",
                vec![
                    JsValue::from_str(&room_id),
                    JsValue::from_str(&event_type),
                    to_plain_js(&content)?,
                ],
            )
            .await
            .map_err(send_error)?;
        js_event_id(&resolved, "sendRoomEvent")
    }

    async fn redact_event(
        &self,
        room_id: String,
        event_id: String,
        reason: Option<String>,
    ) -> Result<(), CommandError> {
        self.log_command(&format!(
            "redact_event: room={room_id}, event_id={event_id}"
        ));
        self.call_async(
            "redactEvent",
            vec![
                JsValue::from_str(&room_id),
                JsValue::from_str(&event_id),
                match reason {
                    Some(reason) => JsValue::from_str(&reason),
                    None => JsValue::UNDEFINED,
                },
            ],
        )
        .await
        .map_err(send_error)?;
        Ok(())
    }

    async fn subscribe_room(
        &self,
        room_id: String,
        subjects: RoomSubjects,
        sink: Arc<dyn RoomSink>,
    ) -> Result<Arc<dyn Subscription>, BackendError> {
        log::debug!("[{room_id}] subscribing: {subjects:?}");
        let subjects = to_plain_js(&subjects).map_err(|e| BackendError::new(e.to_string()))?;
        let sink: JsValue = WasmRoomSink { inner: sink }.into();
        let object = self
            .call_sync(
                "subscribeRoom",
                vec![JsValue::from_str(&room_id), subjects, sink],
            )
            .map_err(backend_error)?;
        Ok(Arc::new(JsSubscription { object }))
    }

    async fn subscribe_to_device(
        &self,
        event_types: Vec<String>,
        sink: Arc<dyn ToDeviceSink>,
    ) -> Result<Arc<dyn Subscription>, BackendError> {
        log::debug!("subscribing to to-device messages: {event_types:?}");
        let types = to_plain_js(&event_types).map_err(|e| BackendError::new(e.to_string()))?;
        let sink: JsValue = WasmToDeviceSink { inner: sink }.into();
        let object = self
            .call_sync("subscribeToDevice", vec![types, sink])
            .map_err(backend_error)?;
        Ok(Arc::new(JsSubscription { object }))
    }

    async fn relations(
        &self,
        room_id: String,
        event_id: String,
        rel_type: String,
        event_type: String,
    ) -> Result<Vec<EventIn>, BackendError> {
        let result = self
            .call_async(
                "relations",
                vec![
                    JsValue::from_str(&room_id),
                    JsValue::from_str(&event_id),
                    JsValue::from_str(&rel_type),
                    JsValue::from_str(&event_type),
                ],
            )
            .await
            .map_err(backend_error)?;
        serde_wasm_bindgen::from_value(result)
            .map_err(|e| BackendError::new(format!("relations resolved to a non-event list: {e}")))
    }

    async fn openid_token(&self) -> Result<OpenIdToken, BackendError> {
        let result = self
            .call_async("getOpenIdToken", Vec::new())
            .await
            .map_err(backend_error)?;
        serde_wasm_bindgen::from_value(result)
            .map_err(|e| BackendError::new(format!("getOpenIdToken resolved to a non-token: {e}")))
    }

    async fn rtc_transports(&self) -> Result<Value, BackendError> {
        let result = self
            .call_async("rtcTransports", Vec::new())
            .await
            .map_err(backend_error)?;
        if result.is_undefined() || result.is_null() {
            return Ok(Value::Array(Vec::new()));
        }
        serde_wasm_bindgen::from_value(result)
            .map_err(|e| BackendError::new(format!("rtcTransports resolved to non-JSON: {e}")))
    }
}
