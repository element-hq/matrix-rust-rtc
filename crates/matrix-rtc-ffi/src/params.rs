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
    /// Whether to discard keys from senders the client does not confirm
    /// (default: true, per MSC4153).
    pub require_confirmed_sender: Option<bool>,
}

/// What kind of call a join is. Only a DM call can ring, so a group call
/// cannot express a ring and a DM call cannot take a group's notification.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum FfiCallKind {
    /// Not a DM. Never rings; `notify` tells the room with a visual
    /// notification when we start the call.
    Group { notify: Option<FfiGroupNotify> },
    /// A DM call: the library rings, follows the call and ends it (see
    /// `RtcCall::status`). With `ring`, the user chose to call;
    /// without it, they are answering a ring or joining a call in progress.
    Direct { ring: Option<FfiRingConfig> },
}

/// A group call's MSC4075 notification, for a join that starts the call.
/// A visual indication, never a ring.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiGroupNotify {
    /// MSC4196 `m.call.intent`, e.g. "audio" or "video". Omitted when unset.
    #[uniffi(default = None)]
    pub intent: Option<String>,
    /// How long the notification stays valid, in milliseconds (default: 90000,
    /// capped at 120000).
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

impl From<FfiGroupNotify> for matrix_rtc_call::GroupNotify {
    fn from(value: FfiGroupNotify) -> Self {
        matrix_rtc_call::GroupNotify {
            intent: value.intent,
            lifetime_ms: value.lifetime_ms,
            mentions: matrix_rtc_call::Mentions {
                user_ids: value.mention_user_ids,
                room: value.mention_room,
            },
        }
    }
}

/// The ring a DM call sends when the user chose to call. It always addresses
/// the whole room.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiRingConfig {
    /// How long the callee's phone rings, and how long the call waits for an
    /// answer, in milliseconds (default: 90000, capped at 120000).
    #[uniffi(default = None)]
    pub lifetime_ms: Option<u64>,
    /// MSC4196 `m.call.intent`, e.g. "audio" or "video". Omitted when unset.
    #[uniffi(default = None)]
    pub intent: Option<String>,
}

impl From<FfiCallKind> for matrix_rtc_call::CallKind {
    fn from(value: FfiCallKind) -> Self {
        match value {
            FfiCallKind::Group { notify } => matrix_rtc_call::CallKind::Group {
                notify: notify.map(Into::into),
            },
            FfiCallKind::Direct { ring } => matrix_rtc_call::CallKind::Direct {
                ring: ring.map(|ring| matrix_rtc_call::RingConfig {
                    lifetime_ms: ring.lifetime_ms,
                    intent: ring.intent,
                }),
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
    /// A group call or a DM call. Required: a default of a group call would
    /// let a host that forgot to say DM place a quiet group call nobody
    /// notices.
    pub kind: FfiCallKind,
    /// Optional keep-alive timeout in milliseconds (default: 30000).
    ///
    /// Arms the delayed leave (the dead man's switch for a client that dies).
    pub keep_alive_timeout_ms: Option<u64>,
    /// Optional sticky-map lifetime for our membership, in milliseconds
    /// (default: 3600000).
    ///
    /// A different clock from `keep_alive_timeout_ms`: this is how long the
    /// homeserver keeps the membership at all. The SDK re-sends the membership
    /// once a third of it has passed, so shortening it buys nothing but traffic.
    pub sticky_duration_ms: Option<u64>,
    /// Optional membership lifetime to fall back to on a homeserver that
    /// refuses MSC4140 delayed events, in milliseconds (default: 300000).
    ///
    /// Only ever used on such a homeserver, where nothing clears a crashed
    /// client's membership except this lifetime running out. Raising it trades a
    /// slower cleanup for less signalling; the SDK re-sends the membership once
    /// a third of it has passed, so 300000 means a membership event every 100 s,
    /// and an outage of up to about 3 min 20 s never lapses it.
    /// Do not lower it below 300000 — MSC4354 says a sticky duration "SHOULD NOT
    /// be set to below 5 minutes", because a server whose clock runs behind
    /// expires sticky events early and a shorter duration cannot absorb that.
    #[uniffi(default = None)]
    pub degraded_lifetime_ms: Option<u64>,
    /// Optional encryption configuration
    pub encryption_config: Option<FfiEncryptionConfig>,
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
    /// How old a received reaction may be, in milliseconds (default 10000),
    /// by our clock against its server timestamp. Older ones, such as those a
    /// sync catching up delivers, are dropped as stale.
    #[uniffi(default = 10000)]
    pub max_age_ms: u64,
}

impl From<FfiReactionsConfig> for matrix_rtc_call::ReactionsConfig {
    fn from(value: FfiReactionsConfig) -> Self {
        matrix_rtc_call::ReactionsConfig {
            enabled: value.enabled,
            active_window_ms: value.active_window_ms,
            send_cooldown_ms: value.send_cooldown_ms,
            max_age_ms: value.max_age_ms,
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
            require_confirmed_sender: value.require_confirmed_sender.unwrap_or(true),
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

        let kind = match &self.kind {
            FfiCallKind::Group { notify: None } => "group",
            FfiCallKind::Group { notify: Some(_) } => "group+notify",
            FfiCallKind::Direct { ring: None } => "direct",
            FfiCallKind::Direct { ring: Some(_) } => "direct+ring",
        };
        format!(
            "[slot {}] transport={} keep_alive={:?}ms encryption={} kind={kind}",
            self.application_slot_id
                .as_deref()
                .unwrap_or(matrix_rtc_core::ROOM_APPLICATION_SLOT_ID),
            transport,
            self.keep_alive_timeout_ms,
            self.encryption_config.is_some(),
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
        options.kind = self.kind.into();
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
