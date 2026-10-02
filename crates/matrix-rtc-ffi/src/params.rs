// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The uniffi records a host passes to join and leave, and their conversion
//! to core types. DTOs keep uniffi shapes out of the core.

/// FFI-friendly transport configuration for join operations.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiTransportConfig {
    /// Transport type (e.g., "livekit")
    pub r#type: String,
    /// LiveKit service URL (required for livekit transport)
    pub livekit_service_url: Option<String>,
}

/// FFI-friendly encryption configuration.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiEncryptionConfig {
    /// Time to wait (ms) before using a newly distributed key (default: 5000ms).
    pub delay_before_use_ms: Option<u64>,
    /// Grace period (ms) for key rotation (default: 10000ms).
    pub key_rotation_grace_period_ms: Option<u64>,
    /// Longest a key may be used before it is replaced regardless of membership
    /// (default: 5400000ms, 1h30). Bounds how much of a long call one recovered
    /// key can decrypt.
    #[uniffi(default = None)]
    pub max_key_lifetime_ms: Option<u64>,
    /// Whether to manage media keys (default: true).
    pub manage_media_keys: Option<bool>,
    /// Whether to discard keys from devices that are not cross-signed
    /// (default: true, per MSC4153).
    pub require_cross_signed_sender: Option<bool>,
}

/// What kind of notification an MSC4075 call notification asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FfiNotificationType {
    /// Ring audibly, for `lifetime_ms`.
    Ring,
    /// Show a visual indication only.
    Notification,
}

/// FFI-friendly MSC4075 notification request, for a join that *starts* a call.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiNotifyConfig {
    /// Ring, or notify silently.
    pub notification_type: FfiNotificationType,
    /// MSC4196 `m.call.intent`, e.g. "audio" or "video". Omitted when unset.
    #[uniffi(default = None)]
    pub intent: Option<String>,
    /// How long the ring stays valid, in milliseconds (default: 30000, capped
    /// at 120000 because that is what receivers honour).
    #[uniffi(default = None)]
    pub lifetime_ms: Option<u64>,
    /// Users named individually in `m.mentions`. Usually empty.
    #[uniffi(default = [])]
    pub mention_user_ids: Vec<String>,
    /// Whether the whole room is targeted (default: true, which is what a call
    /// in a room means). Note that the room's power levels may gate this.
    #[uniffi(default = true)]
    pub mention_room: bool,
}

impl From<FfiNotifyConfig> for matrix_rtc_call::NotifyConfig {
    fn from(value: FfiNotifyConfig) -> Self {
        matrix_rtc_call::NotifyConfig {
            notification_type: match value.notification_type {
                FfiNotificationType::Ring => matrix_rtc_call::NotificationType::Ring,
                FfiNotificationType::Notification => {
                    matrix_rtc_call::NotificationType::Notification
                }
            },
            intent: value.intent,
            lifetime_ms: value.lifetime_ms,
            mentions: matrix_rtc_call::Mentions {
                user_ids: value.mention_user_ids,
                room: value.mention_room,
            },
        }
    }
}

/// FFI-friendly join session parameters.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiJoinSessionParams {
    /// Slot ID (e.g., "m.call#ROOM")
    pub slot_id: String,
    /// Application type (e.g., "m.call")
    pub application: String,
    /// The transport to publish on. `None` — the usual case — takes the first
    /// LiveKit transport the homeserver advertises (`rtcTransports` on the
    /// backend); the join fails if there is none. Set it to pin a specific
    /// focus.
    #[uniffi(default = None)]
    pub transport: Option<FfiTransportConfig>,
    /// Join without publishing — valid per MSC4143, and what a recorder or
    /// other observer wants. `transport` is then ignored.
    #[uniffi(default = false)]
    pub receive_only: bool,
    /// Transport types this member can receive on. Only read when
    /// `receive_only`; a publishing member advertises its own transport's type.
    #[uniffi(default = [])]
    pub can_subscribe: Vec<String>,
    /// Optional keep-alive timeout in milliseconds (default: 30000).
    ///
    /// Arms the delayed leave (the dead man's switch for a client that dies).
    pub keep_alive_timeout_ms: Option<u64>,
    /// Optional sticky-map lifetime for our membership, in milliseconds
    /// (default: 3600000).
    ///
    /// A different clock from `keep_alive_timeout_ms`: this is how long the
    /// homeserver keeps the membership at all. The SDK re-sends the membership
    /// at half this interval, so shortening it buys nothing but traffic.
    pub sticky_duration_ms: Option<u64>,
    /// Optional membership lifetime to fall back to on a homeserver that
    /// refuses MSC4140 delayed events, in milliseconds (default: 300000).
    ///
    /// Only ever used on such a homeserver, where nothing clears a crashed
    /// client's membership except this lifetime running out. Raising it trades a
    /// slower cleanup for less signalling; the SDK re-sends the membership at
    /// half this interval, so 300000 means a membership event every 2½ minutes.
    /// Do not lower it below 300000 — MSC4354 says a sticky duration "SHOULD NOT
    /// be set to below 5 minutes", because a server whose clock runs behind
    /// expires sticky events early and a shorter duration cannot absorb that.
    #[uniffi(default = None)]
    pub degraded_lifetime_ms: Option<u64>,
    /// Optional encryption configuration
    pub encryption_config: Option<FfiEncryptionConfig>,
    /// Ask for an MSC4075 notification to be sent with this join, so other
    /// devices in the room ring or show an incoming call.
    ///
    /// Unset — the default — joins quietly, which is what joining a call
    /// someone else started does. Set it only when the user is *starting* the
    /// call: the SDK still suppresses the notification if anybody is already in
    /// the session, but the intent to summon anyone at all is yours to state.
    #[uniffi(default = None)]
    pub notify: Option<FfiNotifyConfig>,
    /// How this session handles Element Call reactions and the raised hand.
    /// Unset is enabled with Element Call's three-second window.
    #[uniffi(default = None)]
    pub reactions: Option<FfiReactionsConfig>,
}

/// FFI-friendly reactions configuration (mirrors
/// `matrix_rtc_call::ReactionsConfig`).
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiReactionsConfig {
    /// Whether reactions are handled at all. Off, inbound reactions and raised
    /// hands are ignored and sending fails.
    #[uniffi(default = true)]
    pub enabled: bool,
    /// How long a received reaction counts as active, in milliseconds
    /// (default 3000): repeats from the same member inside it are dropped, and
    /// a host should keep the emoji on screen this long.
    #[uniffi(default = 3000)]
    pub active_window_ms: u64,
    /// The least time between two reactions we send, in milliseconds (default
    /// 3000). A send inside it fails without reaching the homeserver.
    #[uniffi(default = 3000)]
    pub send_cooldown_ms: u64,
}

impl From<FfiReactionsConfig> for matrix_rtc_call::ReactionsConfig {
    fn from(value: FfiReactionsConfig) -> Self {
        matrix_rtc_call::ReactionsConfig {
            enabled: value.enabled,
            active_window_ms: value.active_window_ms,
            send_cooldown_ms: value.send_cooldown_ms,
        }
    }
}

/// FFI-friendly leave session parameters.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiLeaveSessionParams {
    /// Optional MSC4143 leave reason. Defaults to `code = "leave"` when unset.
    pub leave_reason: Option<crate::FfiLeaveReason>,
}

/// Conversion from FFI transport config to core transport type.
impl FfiTransportConfig {
    pub fn into_core(self) -> Result<matrix_rtc_core::RtcTransport, matrix_rtc_core::CommandError> {
        use matrix_rtc_core::{LiveKitTransport, RtcTransport, UnsupportedTransport};
        use std::collections::BTreeMap;

        let mut extra_fields = BTreeMap::new();

        match self.r#type.as_str() {
            "livekit" => {
                let url = self.livekit_service_url.ok_or_else(|| {
                    matrix_rtc_core::CommandError::SendError(
                        "livekit transport requires livekit_service_url".to_string(),
                    )
                })?;
                Ok(RtcTransport::LiveKit(LiveKitTransport {
                    livekit_service_url: url,
                }))
            }
            _ => {
                if let Some(url) = self.livekit_service_url {
                    extra_fields.insert("livekit_service_url".to_string(), url.into());
                }
                Ok(RtcTransport::Unsupported(UnsupportedTransport {
                    transport_type: self.r#type,
                    extra_fields,
                }))
            }
        }
    }
}

impl From<FfiEncryptionConfig> for matrix_rtc_core::EncryptionConfig {
    fn from(value: FfiEncryptionConfig) -> Self {
        matrix_rtc_core::EncryptionConfig {
            delay_before_use_ms: value.delay_before_use_ms.unwrap_or(5000),
            key_rotation_grace_period_ms: value.key_rotation_grace_period_ms.unwrap_or(10000),
            max_key_lifetime_ms: value.max_key_lifetime_ms.unwrap_or(90 * 60 * 1000),
            manage_media_keys: value.manage_media_keys.unwrap_or(true),
            require_cross_signed_sender: value.require_cross_signed_sender.unwrap_or(true),
        }
    }
}

/// Conversion from FFI join params to the call layer's join options.
impl FfiJoinSessionParams {
    /// One-line description for logs. Covers everything that decides whether a
    /// join is accepted and how the member is projected — including the
    /// transport intent, which is the field integrators most often get wrong.
    pub(crate) fn summary(&self) -> String {
        let transport = if self.receive_only {
            format!("receive_only:{:?}", self.can_subscribe)
        } else {
            match &self.transport {
                Some(transport) => format!(
                    "publish:{}{}",
                    transport.r#type,
                    transport
                        .livekit_service_url
                        .as_deref()
                        .map(|url| format!("@{url}"))
                        .unwrap_or_default(),
                ),
                None => "publish:advertised".to_owned(),
            }
        };

        format!(
            "[{}] application={} transport={} keep_alive={:?}ms encryption={} notify={:?}",
            self.slot_id,
            self.application,
            transport,
            self.keep_alive_timeout_ms,
            self.encryption_config.is_some(),
            self.notify.as_ref().map(|notify| notify.notification_type),
        )
    }

    /// The transport the join names, if any: what it pins, or receive-only.
    /// `None` leaves the choice to the library.
    pub(crate) fn transport_intent(
        &self,
    ) -> Result<Option<matrix_rtc_core::TransportIntent>, matrix_rtc_core::CommandError> {
        if self.receive_only {
            return Ok(Some(matrix_rtc_core::TransportIntent::ReceiveOnly {
                can_subscribe: self.can_subscribe.clone(),
            }));
        }
        self.transport
            .clone()
            .map(|transport| {
                transport
                    .into_core()
                    .map(matrix_rtc_core::TransportIntent::Publish)
            })
            .transpose()
    }

    /// Who we are, the `member.id` and — when the join names none — the
    /// transport are the library's to decide, so none of them is here. A
    /// host-chosen `member.id` reused across joins would keep the MSC4195
    /// participant identity stable while the key index restarts at 0, so peers
    /// decrypt new media with a stale key and never recover.
    pub(crate) fn into_call(
        self,
    ) -> Result<matrix_rtc_call::CallJoinOptions, matrix_rtc_core::CommandError> {
        let transport = self.transport_intent()?;
        // The binding names the whole slot id; the room checks it belongs to
        // the application.
        let mut join = matrix_rtc_call::JoinOptions {
            slot_id: self.slot_id,
            ..matrix_rtc_call::JoinOptions::application(self.application)
        };
        join.transport = transport;
        join.encryption_config = self.encryption_config.map(Into::into);
        join.keep_alive_timeout_ms = self.keep_alive_timeout_ms;
        join.sticky_duration_ms = self.sticky_duration_ms;
        join.degraded_lifetime_ms = self.degraded_lifetime_ms;
        Ok(matrix_rtc_call::CallJoinOptions {
            join,
            notify: self.notify.map(Into::into),
            reactions: self.reactions.map(Into::into),
        })
    }
}

/// Conversion from FFI leave params to core leave params.
impl FfiLeaveSessionParams {
    pub fn into_core(self) -> matrix_rtc_core::LeaveSessionParams {
        matrix_rtc_core::LeaveSessionParams {
            leave_reason: self.leave_reason.map(Into::into),
        }
    }
}
