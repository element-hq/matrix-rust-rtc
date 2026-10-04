// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The uniffi records a host passes to join and leave, and their conversion
//! to core types. DTOs keep uniffi shapes out of the core.

/// What a join does with transports. Only LiveKit is supported, so every
/// member says it can subscribe to `livekit`.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum FfiJoinTransport {
    /// Publish on the first LiveKit transport the homeserver advertises
    /// (`rtcTransports` on the backend); the join fails if there is none.
    Advertised,
    /// Publish on this LiveKit focus, whatever the homeserver advertises.
    Publish { livekit_service_url: String },
    /// Publish nothing and only receive, as a recorder or other observer does.
    ReceiveOnly,
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
    /// The call slot to join, `m.call#{application_slot_id}`. `None` is the
    /// room-wide call, `room`.
    #[uniffi(default = None)]
    pub application_slot_id: Option<String>,
    /// What the join publishes on, if anything. Usually
    /// [`FfiJoinTransport::Advertised`].
    pub transport: FfiJoinTransport,
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

impl From<FfiJoinTransport> for matrix_rtc_call::JoinTransport {
    fn from(value: FfiJoinTransport) -> Self {
        match value {
            FfiJoinTransport::Advertised => Self::Advertised,
            FfiJoinTransport::Publish {
                livekit_service_url,
            } => Self::Publish(matrix_rtc_core::LiveKitTransport {
                livekit_service_url,
            }),
            FfiJoinTransport::ReceiveOnly => Self::ReceiveOnly,
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
        let transport = match &self.transport {
            FfiJoinTransport::Advertised => "advertised".to_owned(),
            FfiJoinTransport::Publish {
                livekit_service_url,
            } => format!("publish:{livekit_service_url}"),
            FfiJoinTransport::ReceiveOnly => "receive_only".to_owned(),
        };

        format!(
            "[slot {}] transport={} keep_alive={:?}ms encryption={} notify={:?}",
            self.application_slot_id
                .as_deref()
                .unwrap_or(matrix_rtc_core::ROOM_APPLICATION_SLOT_ID),
            transport,
            self.keep_alive_timeout_ms,
            self.encryption_config.is_some(),
            self.notify.as_ref().map(|notify| notify.notification_type),
        )
    }

    /// Who we are, the `member.id` and — when the join names none — the
    /// transport are the library's to decide, so none of them is here. A
    /// host-chosen `member.id` reused across joins would keep the MSC4195
    /// participant identity stable while the key index restarts at 0, so peers
    /// decrypt new media with a stale key and never recover.
    pub(crate) fn into_call(self) -> matrix_rtc_call::CallJoinOptions {
        let mut options = matrix_rtc_call::CallJoinOptions::new();
        if let Some(name) = self.application_slot_id {
            options = options.slot(name);
        }
        let join = &mut options.join;
        join.transport = self.transport.into();
        join.encryption_config = self.encryption_config.map(Into::into);
        join.keep_alive_timeout_ms = self.keep_alive_timeout_ms;
        join.sticky_duration_ms = self.sticky_duration_ms;
        join.degraded_lifetime_ms = self.degraded_lifetime_ms;
        options.notify = self.notify.map(Into::into);
        options.reactions = self.reactions.map(Into::into);
        options
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
