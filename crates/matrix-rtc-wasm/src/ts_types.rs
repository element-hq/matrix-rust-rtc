// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The TypeScript declarations for everything that crosses the boundary as a
//! `JsValue`.
//!
//! wasm-bindgen types a `JsValue` parameter or return as `any`; this custom
//! section supplies the real shapes, and the `unchecked_param_type` /
//! `unchecked_return_type` attributes at each call site bind them. The
//! declarations are hand-written and MUST track the serde shapes they
//! describe (the structs are all in this crate — `WasmJoinSessionParams`,
//! `WasmParticipant`, `WasmCallEvent`, the compat carriers, ...); the
//! `web/` package's type-check test catches declarations that stop parsing,
//! but a field rename only a human reads here.
//!
//! Conventions, mirroring the wire: input objects mark serde-defaulted fields
//! `?:`; output objects type `Option` fields `| null` (serde serializes
//! `None` as `null`, not absence).

use wasm_bindgen::prelude::*;

#[wasm_bindgen(typescript_custom_section)]
const TS_TYPES: &'static str = r#"
/** What the client reports about how an event or to-device message arrived. */
export type EventEncryptionIn =
    | { kind: "cleartext" }
    /** `sender_cross_signed` stays absent when the client cannot say; never infer it. */
    | { kind: "encrypted"; sender_device_id?: string; sender_cross_signed?: boolean };

/** A room event as the client handed it over, content verbatim; the library parses it. */
export interface EventIn {
    event_id: string;
    sender: string;
    event_type: string;
    /** Set for state events. */
    state_key?: string;
    origin_server_ts?: number;
    content: Record<string, unknown>;
    encryption: EventEncryptionIn;
}

/** A decrypted to-device message as the client handed it over. */
export interface ToDeviceMessageIn {
    sender: string;
    event_type: string;
    content: Record<string, unknown>;
    encryption: EventEncryptionIn;
}

/** What the library wants delivered for one room (`subscribeRoom`). */
export interface RoomSubjects {
    /** Stable and unstable spellings both listed; deliver each under its own type. */
    state_event_types: string[];
    /** Message-like types to forward as they arrive, redactions included. */
    timeline_event_types: string[];
}

/** What a `subscribeRoom` / `subscribeToDevice` call returns. */
export interface BackendSubscription {
    cancel(): void;
}

/** `WasmRtcClient.room`'s options. */
export interface RoomOptionsIn {
    element_call_compat?: ElementCallCompatMode;
}

/**
 * The page's Matrix backend: the one object `WasmRtcClient` takes.
 * Sends take the event type already in its wire spelling. The read half
 * delivers into the sink classes (`WasmRoomSink`, `WasmToDeviceSink`): for
 * sticky events, state events and joined members every call carries the
 * room's complete current set, the first one on subscribe; an empty set
 * means none.
 */
export interface MatrixBackendHost {
    ownUserId(): string;
    ownDeviceId(): string;
    /** MSC4354 sticky send; pass `durationMs` through verbatim. Never called for a room opened in `state_events` mode, so a host without sticky support may reject it there. */
    sendStickyEvent(roomId: string, eventType: string, content: Record<string, unknown>, durationMs: number): Promise<{ event_id: string } | { eventId: string } | string>;
    sendStateEvent(roomId: string, eventType: string, stateKey: string, content: Record<string, unknown>): Promise<{ event_id: string } | { eventId: string } | string>;
    /** MSC4140 delayed send — a delayed STATE event when `stateKey` is set; resolves with the bare delay id. A rejection carrying `errcode` lets the library tell a homeserver without delayed events apart. */
    sendDelayedEvent(roomId: string, eventType: string, stateKey: string | null, content: Record<string, unknown>, delayMs: number): Promise<string>;
    /** MSC4140 `restart` — never cancel+resend. */
    restartDelayedEvent(roomId: string, delayId: string): Promise<unknown>;
    cancelDelayedEvent(roomId: string, delayId: string): Promise<unknown>;
    /** Olm-encrypted, per specific device. Resolving with nothing reports every recipient served. */
    sendToDeviceMessage(recipients: { userId: string; deviceId: string }[], messageType: string, content: Record<string, unknown>): Promise<{ userId: string; deviceId: string; error?: string }[] | void>;
    /** Plain message-like send (reactions, raised hand); encrypted in an encrypted room. */
    sendRoomEvent(roomId: string, eventType: string, content: Record<string, unknown>): Promise<{ event_id: string } | { eventId: string } | string>;
    /** Redact one of our own events (lowering a raised hand). */
    redactEvent(roomId: string, eventId: string, reason?: string): Promise<unknown>;
    /** Synchronous: register listeners, deliver the current sets (now or soon), return the handle. */
    subscribeRoom(roomId: string, subjects: RoomSubjects, sink: WasmRoomSink): BackendSubscription;
    subscribeToDevice(eventTypes: string[], sink: WasmToDeviceSink): BackendSubscription;
    /** `GET /rooms/{room}/relations/{event}/{relType}/{eventType}`, decrypted. */
    relations(roomId: string, eventId: string, relType: string, eventType: string): Promise<EventIn[]>;
    getOpenIdToken(): Promise<{ access_token: string; token_type: string; matrix_server_name: string; expires_in: number }>;
    /** The `rtc_transports` array of `GET /_matrix/client/v1/rtc/transports`; `[]` when the endpoint is missing. */
    rtcTransports(): Promise<unknown[]>;
}

export type RtcStreamKind = "microphone" | "camera" | "screen_share" | "screen_share_audio" | "data";
export type ElementCallCompatMode = "off" | "sticky_events" | "state_events";

/** One roster entry, as `participants()` returns it. */
export interface RtcParticipant {
    member_id: string;
    user_id: string;
    device_id: string | null;
    is_local: boolean;
    reachable: boolean;
    /** The livekit-js participant identity — the join key for `room.getParticipantByIdentity()`. */
    rtc_identity: string | null;
    streams: { kind: RtcStreamKind; muted: boolean }[];
    /** When the participant raised their hand (ms since the epoch); `null` while it is down. */
    hand_raised_at_ms: number | null;
}

/** An event on the unified call stream (`onEvent`). */
export type RtcCallEvent =
    | { type: "participant_joined"; member_id: string; user_id: string }
    | { type: "participant_left"; member_id: string }
    | { type: "stream_started"; member_id: string; kind: RtcStreamKind }
    | { type: "stream_stopped"; member_id: string; kind: RtcStreamKind }
    | { type: "stream_muted"; member_id: string; kind: RtcStreamKind }
    | { type: "stream_unmuted"; member_id: string; kind: RtcStreamKind }
    | { type: "active_speakers"; speakers: { member_id: string; level: number }[] }
    | { type: "key_imported"; member_id: string; key_index: number }
    | { type: "frame_encryption_state"; member_id: string;
        state: "ok" | "missing_key" | "decryption_failed" | "encryption_failed" | "internal_error";
        installed_key_indices: number[] | null }
    | { type: "key_discarded"; member_id: string; key_index: number | null;
        sender_user_id: string | null; sender_device_id: string | null;
        reason_code: "cleartext" | "not_cross_signed" | "room_mismatch" | "sender_mismatch" | "unverifiable_device" | "device_mismatch";
        reason: string }
    | { type: "hand_raised"; member_id: string; raised_at_ms: number }
    | { type: "hand_lowered"; member_id: string }
    /** Transient: show `emoji` for ~3 s; `sound` is the asset base name to play (`null` = silent). */
    | { type: "reaction"; member_id: string; emoji: string; name: string; sound: string | null }
    | { type: "unknown_participant"; identity: string }
    | { type: "media_connection_state"; degraded: boolean }
    | { type: "ended"; reason: string };

/** `connectMedia`'s configuration; the room and slot are the call's. */
export interface MediaSessionConfigIn {
    user_id: string;
    device_id: string;
    /** The MSC4195 authorisation-service URL of the focus we publish on. */
    livekit_service_url: string;
    /** livekit-js key-provider ring size when configured away from its default of 16. */
    key_ring_size?: number;
    /** Cross-check only: the mode comes from the room. */
    element_call_compat?: ElementCallCompatMode;
    /** How much the tile order is damped; omitted fields take the defaults. */
    stability?: StabilityConfigIn;
}

/**
 * Tile-order damping. A product decision rather than a protocol one, so a
 * page can tune it. A `demote_ms` above `promote_ms` leaves a tile at the top
 * of the order after the speaker stopped, which reads as a stuck UI.
 */
export interface StabilityConfigIn {
    /** Sustained voice before a member ranks as speaking; the tile flag is not delayed. Default 1500. */
    promote_ms?: number;
    /** Silence before a speaking member stops ranking as one. Default 1500. */
    demote_ms?: number;
    /** Reorders inside this window are delivered as one. Default 300. */
    coalesce_ms?: number;
}

/** The object driving livekit-js for `connectMedia`. */
export interface MediaDelegate {
    /** POST `body` as JSON to `url`; resolve with the HTTP status and raw response text. */
    fetchJson(url: string, body: Record<string, unknown>): Promise<{ status: number; body: string }>;
    /** Connect a livekit-js Room and register the RoomEvent translation onto `sink`. */
    connect(request: { connectionKey: string; sfuUrl: string; jwt: string }, sink: WasmConnectionEventSink): Promise<{ close(): Promise<unknown> }>;
    /** Install a media key in livekit-js's key provider (per participant, HKDF material). */
    setKey(identity: string, index: number, key: Uint8Array): Promise<boolean | void>;
    /** Move the local sender onto a rotated key index. */
    setLocalKeyIndex?(index: number): void;
    /** The push half: the roster after each change. */
    onParticipants?(roster: RtcParticipant[]): void;
    /** The push half: the unified call event stream. */
    onEvent?(event: RtcCallEvent): void;
    /** A key's delayBeforeUse window closed: call `flushDueKeyRotation`. */
    onSwitchComplete?(): void;
}

/** `joinCall`'s parameters. */
export interface JoinParamsIn {
    slot_id: string;
    application: string;
    /** Omit to take the first LiveKit transport the homeserver advertises (`rtcTransports`). */
    transport?: { type: string; livekit_service_url?: string; [key: string]: unknown };
    /** Join without publishing; `can_subscribe` then lists what this member can receive on. */
    receive_only?: boolean;
    can_subscribe?: string[];
    keep_alive_timeout_ms?: number;
    sticky_duration_ms?: number;
    degraded_lifetime_ms?: number;
    encryption_config?: {
        delay_before_use_ms?: number;
        key_rotation_grace_period_ms?: number;
        max_key_lifetime_ms?: number;
        manage_media_keys?: boolean;
        require_cross_signed_sender?: boolean;
    };
    notify?: { notification_type?: string; mentions?: Record<string, unknown>; lifetime_ms?: number };
    /** Element Call reactions and raised hand; omitted is enabled with the 3 s window. */
    reactions?: { enabled?: boolean; active_window_ms?: number; send_cooldown_ms?: number };
}

/** `leave`'s parameters (`{}` is a plain hang-up). */
export interface LeaveParamsIn {
    leave_reason?: { code: string; reason?: string };
}

/** A member whose hand is up (`raisedHands`). */
export interface RaisedHand {
    member_id: string;
    sender: string;
    reaction_event_id: string;
    /** ms since the epoch, by the server's clock; sort ascending to order speakers. */
    raised_at_ms: number;
}

/** One entry of Element Call's reaction catalogue (`reactionCatalog()`). */
export interface ReactionKind {
    name: string;
    emoji: string;
    /** Base name of the sound asset Element Call plays for it, or `null` for a silent reaction. */
    sound: string | null;
}

/** MSC4143 `content.encryption` of an `m.rtc.slot` (`openSlot`). */
export interface SlotEncryptionIn {
    type: string;
    [key: string]: unknown;
}

/** One entry of the sink's `activeSpeakers` payload. */
export interface SpeakerIn {
    identity: string;
    /** 0.0 (silent) to 1.0; omit for transports that report no level. */
    level?: number;
}

"#;
