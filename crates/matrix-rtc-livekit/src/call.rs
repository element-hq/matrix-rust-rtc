// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! High-level "join a call" facade over the whole stack.
//!
//! [`LiveKitCall::join`] wires together everything a MatrixRTC participant needs —
//! an [`RtcClient`] over a [`SdkMatrixBackend`], the room it opens and the call
//! joined in it, the MSC4195 token exchange, and an E2EE-enabled SFU
//! connection driven through the transport-agnostic [`matrix_rtc_media`]
//! layer. [`LiveKitCall::leave`] tears
//! all of it down in the right order.
//!
//! Consume the call through the unified stream
//! ([`LiveKitCall::subscribe_call_events`]) and the [`LiveKitCall::participants`] roster;
//! the raw LiveKit accessors ([`LiveKitCall::events`], [`LiveKitCall::session`]) remain for
//! the transition and will go away once frame streams cover their uses.
//!
//! Requires the `matrix-sdk` feature.
//!
//! # Runtime requirements
//!
//! [`LiveKitCall::join`] must be called from within a tokio runtime: the
//! library spawns the room's feeds and the session's keep-alive onto the
//! current one (`matrix_rtc_core::executor`). See `examples/join_and_record.rs`
//! for the runtime skeleton.
//!
//! # Preconditions
//!
//! - the client is logged in and syncing (e.g. `matrix_sdk_ui::sync_service::SyncService`
//!   is running — it enables the sticky-events extension the feeder relies
//!   on);
//! - the user has joined `room`;
//! - the slot is open (an `m.rtc.slot` state event; see [`open_slot`]) —
//!   MSC4143 counts nobody as joined against a closed slot.

use std::sync::Arc;

use livekit::RoomEvent;
use matrix_sdk::{Client, Room};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{broadcast, watch};

use matrix_rtc_call::transports;
use matrix_rtc_call::{
    CallJoinOptions, JoinOptions, JoinTransport, NotifyConfig, RaisedHand, ReactionError,
    ReactionsConfig, RtcCall, RtcClient, RtcError, RtcRoom,
};
use matrix_rtc_core::RoomOptions;
use matrix_rtc_core::compat::{self, MembershipFormat};
use matrix_rtc_core::{
    BaseRtcRoom, EncryptionConfig, LiveKitTransport, MatrixBackend, RtcTransport, SlotEncryption,
    TransportIntent,
};
use matrix_rtc_matrix_sdk::SdkMatrixBackend;
use matrix_rtc_media::{
    CallEngine, CallEvent, ConnectionContext, EngineConfig, LocalTrackHandle, MediaConstraints,
    MediaStreamKind, OwnMemberClaims, Participant, PublishOptions, ReceiveStats, RemoteTrackHandle,
    StabilityConfig,
};

use crate::session::LiveKitSession;
use crate::transport_impl::{LiveKitMediaTransport, LiveKitTransportConnection};
use crate::{
    MediaKeyBridge, TokenEndpoint, identity_mapper, msc4195_key_provider, msc4195_media_key_bridge,
};

/// Errors produced when joining, operating, or leaving a [`LiveKitCall`].
#[derive(Debug, thiserror::Error)]
pub enum LiveKitCallError {
    /// A Matrix client error (login state, room access, ...).
    #[error(transparent)]
    Sdk(#[from] matrix_sdk::Error),

    /// A LiveKit transport error (token exchange, SFU connection).
    #[error(transparent)]
    Transport(#[from] crate::Error),

    /// A media transport error surfaced through the media layer.
    #[error(transparent)]
    Media(#[from] matrix_rtc_media::TransportError),

    /// MatrixRTC signalling through the core failed (membership, slot, keys).
    #[error("MatrixRTC signalling failed: {0}")]
    Signalling(String),

    /// A reaction or raised hand could not be sent (see
    /// [`matrix_rtc_call::reactions`]).
    #[error(transparent)]
    Reaction(#[from] ReactionError),
}

fn signalling_error(error: impl std::fmt::Display) -> LiveKitCallError {
    LiveKitCallError::Signalling(error.to_string())
}

impl From<RtcError> for LiveKitCallError {
    fn from(error: RtcError) -> Self {
        match error {
            RtcError::Reaction(error) => LiveKitCallError::Reaction(error),
            error => signalling_error(error),
        }
    }
}

/// Options for [`LiveKitCall::join`]. `LiveKitCallOptions::default()` matches the common
/// case: the `m.call#room` slot of the `m.call` application, transport
/// discovery via the homeserver, and the core's default encryption policy.
#[derive(Clone, Debug)]
pub struct LiveKitCallOptions {
    /// MatrixRTC slot to join.
    pub slot_id: String,
    /// MatrixRTC application of the slot.
    pub application: String,
    /// The LiveKit transport to publish on, overriding the homeserver's
    /// advertised transports (MSC4143 `GET /rtc/transports`). `None` takes the
    /// first LiveKit one advertised; joining fails if there is none.
    pub livekit_transport: Option<LiveKitTransport>,
    /// Override for the core's media-key policy. `None` keeps the core's
    /// default, which requires key senders to be cross-signed (MSC4153) —
    /// only relax this for test setups whose users have no cross-signing.
    pub encryption_config: Option<EncryptionConfig>,
    /// How often to refresh the dead man's switch delayed leave; `None` is
    /// [`DEFAULT_KEEP_ALIVE_INTERVAL_MS`](matrix_rtc_core::DEFAULT_KEEP_ALIVE_INTERVAL_MS).
    pub keep_alive_interval_ms: Option<u64>,
    /// How long the homeserver keeps our membership in the sticky map. `None`
    /// keeps the core's default of an hour.
    ///
    /// This is what governs how long a **crashed** client lingers as a ghost.
    /// The dead man's switch does not help there: its delayed leave is a plain
    /// event, so it never replaces the sticky entry, and the membership stands
    /// until this elapses. A tool that expects to be killed — a load generator,
    /// a test — wants it short.
    ///
    /// Not free: the keep-alive re-sends the membership once it is halfway to
    /// expiring, so halving this doubles that signalling rate. Keep it well
    /// above twice [`keep_alive_interval_ms`](Self::keep_alive_interval_ms), or the
    /// entry can lapse between ticks.
    pub sticky_duration_ms: Option<u64>,
    /// The membership lifetime to publish instead of
    /// [`sticky_duration_ms`](Self::sticky_duration_ms) when the homeserver
    /// refuses to arm a delayed leave. `None` keeps the core's default of five
    /// minutes, which is also the floor MSC4354 states.
    ///
    /// Only ever reached on a homeserver without MSC4140, where the join
    /// degrades rather than failing. Subject to the same rule as
    /// `sticky_duration_ms`: keep it well above twice
    /// [`keep_alive_interval_ms`](Self::keep_alive_interval_ms).
    pub degraded_lifetime_ms: Option<u64>,
    /// HTTP client used for the token exchange with the authorisation
    /// service. Supply one to control TLS behaviour (e.g. self-signed dev
    /// certs); `None` builds a default client.
    pub http: Option<reqwest::Client>,
    /// Whether to subscribe to peers' media. `false` joins publish-only: the
    /// roster still fills from membership signalling, but no remote track is
    /// ever subscribed, so [`CallEvent::StreamStarted`] and
    /// [`LiveKitCall::remote_track`] never produce anything. Only a load generator
    /// wants this.
    pub auto_subscribe: bool,
    /// Render this call for an older MatrixRTC generation, for interoperating
    /// with Element Call builds that have not caught up with the 2026 MSC4143
    /// rewrite.
    ///
    /// [`MembershipFormat::Sticky2025`] keeps a join MSC4143-valid — the
    /// legacy fields ride alongside. A leave and a media key cannot: a leave
    /// becomes the legacy bare-sticky-key content (that generation has no
    /// `membership` field, and a padded spec leave would read to it as still
    /// joined), and keys go out as `io.element.call.encryption_keys` *instead of*
    /// the spec type, since a to-device message has only one type. A call in that
    /// mode therefore exchanges keys with legacy peers and not with spec-current
    /// ones.
    ///
    /// [`MembershipFormat::RoomState`] goes further and is not additive at
    /// all: the membership moves to `org.matrix.msc3401.call.member` room state,
    /// the SFU participant identity becomes the plain `{user}:{device}` string,
    /// and the token comes from the pre-MSC4195 `/sfu/get` endpoint. Nothing
    /// about such a call is visible to a spec-current peer.
    ///
    /// Reading the 2025 sticky dialect needs no flag and is always on. See
    /// [`crate::compat`], and delete all of it once Element Call catches up.
    pub format: MembershipFormat,
    /// Ask for an MSC4075 notification to be sent with this join, so other
    /// devices in the room ring or show an incoming call.
    ///
    /// `None` — the default — joins quietly, which is what joining a call
    /// someone else started does. Set it only when *starting* the call: the
    /// core still suppresses the notification if anybody is already in the
    /// session, but the intent to summon anyone at all is the caller's.
    pub notify: Option<NotifyConfig>,
    /// How this call handles Element Call reactions and the raised hand.
    ///
    /// `None` — the default — is [`ReactionsConfig::default`]: enabled, with
    /// Element Call's three-second window. Reactions arrive as
    /// [`CallEvent::Reaction`], hands as [`CallEvent::HandRaised`] and on the
    /// roster; send with [`LiveKitCall::send_reaction`], [`LiveKitCall::raise_hand`] and
    /// [`LiveKitCall::lower_hand`].
    pub reactions: Option<ReactionsConfig>,
    /// How much the tile order is damped: how long sustained voice must last
    /// to count as speaking, how long silence must last to stop, and the
    /// window reorders are coalesced into. A product decision rather than a
    /// protocol one; see [`StabilityConfig`].
    pub stability: StabilityConfig,
}

impl Default for LiveKitCallOptions {
    fn default() -> Self {
        Self {
            slot_id: "m.call#room".to_owned(),
            application: "m.call".to_owned(),
            livekit_transport: None,
            encryption_config: None,
            keep_alive_interval_ms: None,
            sticky_duration_ms: None,
            degraded_lifetime_ms: None,
            http: None,
            auto_subscribe: true,
            format: MembershipFormat::default(),
            notify: None,
            reactions: None,
            stability: StabilityConfig::default(),
        }
    }
}

/// A joined MatrixRTC call: live membership signalling plus an E2EE SFU
/// connection.
///
/// Obtained from [`LiveKitCall::join`]; end it with [`LiveKitCall::leave`]. Dropping a
/// `LiveKitCall` without leaving stops the background tasks and the sync-side key
/// handler, but sends no leave event — peers then see this member disappear
/// only when the dead man's switch fires.
pub struct LiveKitCall {
    /// Keeps itself alive and rotates its keys while it lives.
    call: Arc<RtcCall<SdkMatrixBackend>>,
    /// Holds the room's subscription; dropping it detaches.
    room: RtcRoom<SdkMatrixBackend>,
    engine: CallEngine,
    connection: LiveKitTransportConnection,
    raw_events: UnboundedReceiver<RoomEvent>,
    bridge: Arc<MediaKeyBridge>,
    own_identity: String,
}

impl LiveKitCall {
    /// Join the MatrixRTC call on `room` and connect to the SFU with
    /// per-participant frame E2EE.
    ///
    /// Publishes this device's `m.rtc.member` membership as a sticky event,
    /// arms the dead man's switch delayed leave (kept alive by the session),
    /// starts distributing/ingesting media keys over Olm-encrypted
    /// to-device messages, discovers the LiveKit transport, and connects.
    ///
    /// Must run inside a tokio runtime; see the module docs for this and the
    /// other preconditions.
    pub async fn join(
        room: &Room,
        options: LiveKitCallOptions,
    ) -> Result<LiveKitCall, LiveKitCallError> {
        let client = room.client();
        let user_id = client
            .user_id()
            .ok_or_else(|| {
                LiveKitCallError::Signalling("client has no user id (not logged in)".into())
            })?
            .to_string();
        let device_id = client
            .device_id()
            .ok_or_else(|| {
                LiveKitCallError::Signalling("client has no device id (not logged in)".into())
            })?
            .to_string();
        let room_id = room.room_id().to_string();

        match options.format {
            MembershipFormat::Current => {}
            MembershipFormat::Sticky2025 => log::warn!(
                "[{room_id}/{}] joining in pre-2026 Element Call compatibility mode: media keys \
                 go out as {} and will not reach spec-current peers",
                options.slot_id,
                compat::LEGACY_KEY_EVENT_TYPE,
            ),
            MembershipFormat::RoomState => log::warn!(
                "[{room_id}/{}] joining in pre-sticky Element Call compatibility mode: our \
                 membership goes out as {} room state, our SFU identity is the plain \
                 {{user}}:{{device}} string, and the token comes from /sfu/get. Nothing about \
                 this call is visible to a spec-current peer.",
                options.slot_id,
                compat::STATE_MEMBER_EVENT_TYPE,
            ),
        }

        // One client for this call: it opens the room (the feeder subscribes to
        // what the mode needs and seeds room state before membership) and the
        // to-device key subscription; the library runs both feeds.
        let client = RtcClient::new(Arc::new(SdkMatrixBackend::new(client.clone())));
        let room = client
            .room(
                room_id.clone(),
                RoomOptions {
                    format: options.format,
                },
            )
            .await?;
        room.seeded().await;

        // Frame encryption: a single shared KeyProvider handle feeds both the
        // LiveKit room (which encrypts our frames and decrypts peers') and the
        // MediaKeyBridge (which imports every key the core signals). MSC4195
        // per-participant HKDF mode.
        let provider = msc4195_key_provider();
        let bridge = Arc::new(msc4195_media_key_bridge(provider.clone()));
        let identity_mapper = identity_mapper(options.format);

        // The transport is resolved here rather than by the join, because the
        // SFU connection below needs the LiveKit focus it names: the join's own
        // choice, else the first LiveKit one the homeserver advertises.
        let chosen = options
            .livekit_transport
            .clone()
            .map_or(JoinTransport::Advertised, JoinTransport::Publish);
        let TransportIntent::Publish(RtcTransport::LiveKit(livekit)) =
            transports::resolve(room.backend().as_ref(), chosen)
                .await
                .map_err(signalling_error)?
        else {
            return Err(LiveKitCallError::Signalling(
                "the chosen transport is not a LiveKit one".into(),
            ));
        };
        log::info!(
            "[{room_id}/{}] join: focus is {}",
            options.slot_id,
            livekit.livekit_service_url,
        );

        // The join picks the `member.id` this mode joins with (a fresh one per
        // join, except in the pre-sticky generation; see
        // `compat::ingest::member_id`) and renders our sends in the room's
        // dialect.
        let mut join = JoinOptions {
            slot_id: options.slot_id.clone(),
            ..JoinOptions::application(options.application.clone())
        };
        join.transport = JoinTransport::Publish(livekit.clone());
        join.encryption_config = options.encryption_config.clone();
        join.sticky_duration_ms = options.sticky_duration_ms;
        join.degraded_lifetime_ms = options.degraded_lifetime_ms;
        join.keep_alive_interval_ms = options.keep_alive_interval_ms;
        let call = Arc::new(
            room.join_call(CallJoinOptions {
                join,
                notify: options.notify.clone(),
                reactions: options.reactions.clone(),
            })
            .await?,
        );
        let membership_id = call.member_id().to_owned();
        let own_identity = identity_mapper(&user_id, &device_id, &membership_id);
        log::info!(
            "[{room_id}/{}] join: user={user_id} device={device_id} member={membership_id} \
             identity={own_identity}",
            options.slot_id,
        );

        // Wire the encryption manager to the MSC4195 pseudonymous-identity
        // derivation and to our bridge. The same `Arc` that produced
        // `own_identity` above and that the media transport is given below: one
        // value for all of them, so the derivation sites cannot skew — a
        // divergence there is not an error but a silence: peers sit in the
        // roster with no media, their keys land under an identity the SFU never
        // assigned, and nothing logs a problem.
        //
        // The mapper goes in *before* the signal handler. Identities are derived
        // at signal time, so a key signalled in between would be imported under
        // the fallback `user:device` identity. Keys that arrive before the
        // handler are held and replayed below.
        call.set_encryption_identity_mapper(identity_mapper.clone())
            .await;
        if !call.set_encryption_signal_handler(bridge.clone()).await {
            log::warn!(
                "[{room_id}/{}] join: the joined session has no encryption manager",
                options.slot_id,
            );
            return Err(LiveKitCallError::Signalling(
                "failed to register encryption signal handler".into(),
            ));
        }
        let raised_hands = call.subscribe_raised_hands().await;
        let reactions = call.subscribe_reactions().await;
        let memberships = call.subscribe_memberships().await;

        // The media layer: a LiveKit transport sharing the E2EE key provider,
        // and the engine reconciling memberships with connection events. The
        // client is the OpenID token source for the MSC4195 token exchange.
        let http = match options.http {
            Some(http) => http,
            None => reqwest::Client::new(),
        };
        let token_backend: Arc<dyn MatrixBackend> = room.backend().clone();
        let transport = Arc::new(
            LiveKitMediaTransport::new(http, token_backend, provider)
                .with_auto_subscribe(options.auto_subscribe)
                // The same mapper the core got, so our own identity, the peers'
                // and the key ring's all agree.
                .with_identity_mapper(identity_mapper.clone())
                .with_token_endpoint(match options.format {
                    // Pre-MSC4195 `/sfu/get`, which is also where the unhashed
                    // `{user}:{device}` identity above comes from — the two are
                    // one decision, not two.
                    MembershipFormat::RoomState => TokenEndpoint::LegacyElementCall,
                    _ => TokenEndpoint::Msc4195,
                }),
        );
        let ctx = ConnectionContext {
            room_id: room_id.clone(),
            // The token request names the slot as this generation spells it.
            slot_id: options.format.token_slot_id(&options.slot_id).into_owned(),
            member: OwnMemberClaims {
                member_id: membership_id.clone(),
                user_id,
                device_id,
            },
        };
        // The engine owns connections to every peer focus (MSC4195 multi-SFU);
        // only the own focus is connected here, synchronously, so a failed
        // join can be reported (and signalled away) immediately.
        let engine = CallEngine::new(
            EngineConfig {
                transports: vec![transport.clone()],
                own_member_id: membership_id.clone(),
                ctx: ctx.clone(),
                own_connection_key: Some(livekit.livekit_service_url.clone()),
                raised_hands,
                reactions,
                stability: options.stability.clone(),
            },
            memberships,
        );

        // Imported media keys surface as `CallEvent::KeyImported`, and refused
        // ones as `CallEvent::KeyDiscarded` — the only way the reason a key was
        // rejected leaves the core.
        let engine_handle = engine.handle();
        bridge.set_key_import_listener(Box::new(move |key| {
            engine_handle.notify_key_imported(key.rtc_backend_identity.clone(), key.key_index);
        }));
        let engine_handle = engine.handle();
        bridge.set_key_discard_listener(Box::new(move |discarded| {
            engine_handle.notify_key_discarded(discarded);
        }));

        // Re-signal every key held so far, now that the listener above exists.
        //
        // Two things arrive before this point: our own first key, which `join`
        // distributes and signals, and any peer key the sticky bridge has already
        // pumped in. Both were applied to the key provider, but with no listener
        // installed neither produced a `KeyImported` — so the one path a host
        // cannot otherwise observe was also the one it most needed to see. The
        // replay is idempotent at the provider and honours whatever remains of a
        // rotation's `delayBeforeUse`.
        //
        // It runs before `connect_livekit` so the key ring is populated before the
        // first frame can arrive.
        if !call.replay_encryption_keys().await {
            log::warn!(
                "[{room_id}/{}] join: could not replay held keys; peers may stay undecryptable \
                 until the next rotation",
                options.slot_id,
            );
        }

        log::info!(
            "[{room_id}/{}] join: connecting own focus {}",
            options.slot_id,
            livekit.livekit_service_url,
        );
        let (connection, connection_events) = match transport
            .connect_livekit(&livekit.livekit_service_url, &ctx)
            .await
        {
            Ok(connected) => connected,
            Err(error) => {
                log::warn!(
                    "[{room_id}/{}] join: own focus {} refused the connection ({error}); \
                     leaving the slot again",
                    options.slot_id,
                    livekit.livekit_service_url,
                );
                // We are signalled as joined but have no media path; leave so
                // peers don't wait on the dead man's switch to notice.
                if let Err(leave_error) = call.leave(Default::default()).await {
                    log::warn!("leave after failed SFU connect also failed: {leave_error}");
                }
                return Err(error.into());
            }
        };
        engine.adopt_own_connection(Box::new(connection.clone()), connection_events);

        // Now that a room exists, let the bridge move our sender onto each key we
        // rotate to. Importing a key only fills the provider's ring — the index
        // our frames actually carry lives on the frame cryptor, and without this
        // we advertise a rotation to peers and keep encrypting with the previous
        // key. A peer joining after a rotation then holds only the new index and
        // decrypts nothing, and the forward secrecy the rotation exists for is
        // not delivered.
        //
        // Installed after `connect_livekit`, and after the replay above, so the
        // first key is already in the ring; the hook only ever *moves* the index.
        let connection_for_keys = connection.clone();
        bridge.set_local_sender(
            own_identity.clone(),
            Box::new(move |key_index| connection_for_keys.set_local_key_index(key_index)),
        );
        // Adopt whatever index we are already on, rather than assuming 0: a
        // rotation between `join` and here would otherwise be missed, and the
        // connection remembers the value for tracks published later (nothing is
        // published yet, so this only records it).
        if let Some(own_key) = bridge.key_for(&own_identity) {
            connection.set_local_key_index(own_key.key_index);
        }

        log::info!("[{room_id}/{}] join: complete", options.slot_id);

        // Transition-period raw stream; subscribed immediately after connect,
        // so only events racing the connect itself can be missed here.
        let raw_events = connection.session().room().subscribe();

        Ok(LiveKitCall {
            call,
            room,
            engine,
            connection,
            raw_events,
            bridge,
            own_identity,
        })
    }

    /// Sends an Element Call emoji reaction. `name` is what peers pick a sound
    /// by (see [`matrix_rtc_call::KNOWN_REACTIONS`]); only the first grapheme
    /// of `emoji` is sent. Returns the event id.
    ///
    /// Fails with [`ReactionError::Cooldown`] inside the send cooldown, since
    /// peers would drop the reaction anyway.
    pub async fn send_reaction(&self, emoji: &str, name: &str) -> Result<String, LiveKitCallError> {
        Ok(self.call.send_reaction(emoji, name).await?)
    }

    /// Raises our hand. Idempotent while it is up; it follows our membership
    /// across sticky refreshes on its own. Shows on our roster entry at once.
    pub async fn raise_hand(&self) -> Result<(), LiveKitCallError> {
        Ok(self.call.raise_hand().await?)
    }

    /// Lowers our hand by redacting the annotation. A no-op when it is down.
    pub async fn lower_hand(&self) -> Result<(), LiveKitCallError> {
        Ok(self.call.lower_hand().await?)
    }

    /// The raised hands right now, oldest first. The same information is on
    /// each [`Participant::hand_raised_at_ms`] and arrives as
    /// [`CallEvent::HandRaised`] / [`CallEvent::HandLowered`].
    pub async fn raised_hands(&self) -> Vec<RaisedHand> {
        self.call.raised_hands().await
    }

    /// The unified call event stream: membership changes, media streams
    /// starting/stopping, key imports, connection health, call end.
    ///
    /// This is the transport-agnostic replacement for [`LiveKitCall::events`]. Any
    /// number of subscribers may exist; a subscriber that falls far behind
    /// observes a `Lagged` error and should resynchronise from
    /// [`LiveKitCall::participants`].
    pub fn subscribe_call_events(&self) -> broadcast::Receiver<CallEvent> {
        self.engine.subscribe_events()
    }

    /// The current participant roster (including ourselves), derived from
    /// membership signalling and enriched with live media streams.
    pub fn participants(&self) -> Vec<Participant> {
        self.engine.participants()
    }

    /// Watch the participant roster; the receiver always holds the latest
    /// snapshot.
    pub fn subscribe_participants(&self) -> watch::Receiver<Vec<Participant>> {
        self.engine.subscribe_participants()
    }

    /// The frame-stream handle for a participant's subscribed stream, once
    /// [`CallEvent::StreamStarted`] announced it.
    pub fn remote_track(
        &self,
        member_id: &str,
        kind: MediaStreamKind,
    ) -> Option<Arc<dyn RemoteTrackHandle>> {
        self.engine.remote_track(member_id, kind)
    }

    /// Cumulative receive-side RTP counters for a participant's stream, or
    /// `None` while it is not subscribed / before the first RTCP report.
    ///
    /// The only way to distinguish "no RTP arriving" from "RTP arriving that
    /// does not decode": the receive path produces frames at a fixed cadence
    /// either way. See [`ReceiveStats`].
    pub async fn receive_stats(
        &self,
        member_id: &str,
        kind: MediaStreamKind,
    ) -> Option<ReceiveStats> {
        self.engine.receive_stats(member_id, kind).await
    }

    /// Publish a local track (microphone, camera, screenshare) on our focus;
    /// push captured frames into the returned handle.
    pub async fn publish(
        &self,
        options: PublishOptions,
    ) -> Result<Arc<dyn LocalTrackHandle>, LiveKitCallError> {
        Ok(self.engine.publish(options).await?)
    }

    /// Set subscription constraints (visibility, rendered size, quality cap,
    /// low-bandwidth mode) for one stream of one participant. Applied after a
    /// short debounce and re-applied whenever the stream (re)appears.
    pub fn set_constraints(
        &self,
        member_id: &str,
        kind: MediaStreamKind,
        constraints: MediaConstraints,
    ) {
        self.engine.set_constraints(member_id, kind, constraints);
    }

    /// The media engine driving this call's roster and event stream.
    pub fn engine(&self) -> &CallEngine {
        &self.engine
    }

    /// The raw LiveKit room event stream (participants joining, tracks
    /// subscribed, disconnects, ...).
    ///
    /// Transition API: prefer [`LiveKitCall::subscribe_call_events`]; this accessor
    /// goes away once frame-level consumers are served by [`LiveKitCall::remote_track`].
    ///
    /// The stream ending (`recv()` returning `None`) means the call is over:
    /// the room closes its event channel on [`LiveKitCall::leave`] and after any
    /// unrecoverable disconnect (server eviction, reconnects exhausted, ...) —
    /// in the latter case a [`RoomEvent::Disconnected`] carrying the reason is
    /// delivered first, so match it only if the reason matters. Transient
    /// network drops are resumed internally (`Reconnecting`/`Reconnected`
    /// events) and do not end the stream. There is no built-in deadline:
    /// waiting for an event that may never come (e.g. a track from a peer who
    /// never publishes) should be wrapped in a timeout by the caller.
    pub fn events(&mut self) -> &mut UnboundedReceiver<RoomEvent> {
        &mut self.raw_events
    }

    /// The connected SFU session (access the LiveKit room to publish, ...).
    ///
    /// Transition API: media access moves behind [`LiveKitCall::remote_track`] and
    /// the upcoming publish surface.
    pub fn session(&self) -> &LiveKitSession {
        self.connection.session()
    }

    /// Our MSC4195 pseudonymous LiveKit identity (the JWT `sub`). Peers see
    /// this as our participant identity and import our media key under it.
    pub fn local_identity(&self) -> &str {
        &self.own_identity
    }

    /// The `m.rtc.member` membership id of this join.
    pub fn membership_id(&self) -> &str {
        self.call.member_id()
    }

    /// Number of members (including ourselves) currently joined to the slot,
    /// as signalled over sticky membership events.
    pub async fn member_count(&self) -> usize {
        self.call.member_count().await
    }

    /// Whether a media key for the given MSC4195 participant identity has been
    /// received and imported into this call's frame decryptor. See
    /// [`LiveKitCall::local_identity`] for the identity peers know us by.
    pub fn imported_key_for(&self, identity: &str) -> bool {
        self.bridge.key_for(identity).is_some()
    }

    /// Leave the call cleanly: send the leave event (cancelling the delayed
    /// leave) and close the SFU connection.
    ///
    /// The SFU connection is closed even if the Matrix-side leave fails; the
    /// first error wins.
    pub async fn leave(self) -> Result<(), LiveKitCallError> {
        let LiveKitCall {
            call,
            room,
            engine,
            connection,
            ..
        } = self;
        let room_id = room.room_id().to_owned();

        // Step logs bracket every await so a wedged teardown pinpoints itself.
        log::debug!("[{room_id}] leave: sending matrix leave (membership + delayed-event cancel)");
        let leave_result = call
            .leave(Default::default())
            .await
            .map_err(LiveKitCallError::from);
        // After the leave, whose cancel of the delayed event still goes through
        // the backend; the subscriptions are not needed for that.
        drop(room);
        log::debug!(
            "[{room_id}] leave: matrix leave {}; shutting down the media engine",
            if leave_result.is_ok() {
                "sent"
            } else {
                "FAILED"
            },
        );
        // Emits `CallEvent::Ended { reason: Left }` and closes every
        // peer-focus connection; the own-focus close below reports its result.
        engine.shutdown().await;
        log::debug!("[{room_id}] leave: media engine down; closing own SFU connection");
        use matrix_rtc_media::TransportConnection as _;
        let close_result = connection.close().await.map_err(LiveKitCallError::from);
        log::debug!("[{room_id}] leave: complete");
        leave_result.and(close_result)
    }
}

/// Open a MatrixRTC slot in a room by publishing its `m.rtc.slot` state event.
///
/// Requires the power level for `m.rtc.slot` state (by default the room
/// creator). Passing `None` for `encryption` opens an unencrypted slot; calls
/// in encrypted rooms should use `m.per_member` slot encryption.
pub async fn open_slot(
    client: &Client,
    room_id: &str,
    slot_id: &str,
    application: &str,
    encryption: Option<SlotEncryption>,
) -> Result<(), LiveKitCallError> {
    BaseRtcRoom::with_backend(room_id, Arc::new(SdkMatrixBackend::new(client.clone())))
        .open_slot(slot_id.to_owned(), application.to_owned(), encryption)
        .await
        .map_err(signalling_error)
}
