// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Attaching media to a call that is already joined: the one copy of the
//! ordered wiring between the core's encryption manager, the key handler, the
//! [`CallEngine`] and the own-focus connection.
//!
//! The order is the point. Each step out of place does not fail; it silences
//! media (peers in the roster with nothing to decrypt), so every host — the
//! FFI and wasm media sessions, the native `LiveKitCall` facade — goes through
//! [`attach_media`] rather than repeating it. Transport-specific setup (the key
//! ring, the token endpoint) stays with the caller.

use std::sync::Arc;

use matrix_rtc_call::RtcCall;
use matrix_rtc_core::compat::MembershipFormat;
use matrix_rtc_core::{MatrixBackend, RtcIdentityMapper, TransportIntent};
use matrix_rtc_transport::{
    ConnectionContext, MediaKeyHandler, MediaTransport, OwnFocusTransport, OwnMemberClaims,
    TransportConnection, TransportError,
};
use tokio::sync::broadcast;

use crate::engine::{CallEngine, EngineConfig, StabilityConfig};
use crate::event::CallEvent;

/// What [`attach_media`] needs besides the call, the transport and the key
/// handler.
pub struct AttachOptions {
    /// The account and device the call was joined as.
    pub own_user_id: String,
    pub own_device_id: String,
    /// How `(user, device, member_id)` becomes a transport identity. MUST be
    /// the one the transport was built with: the core's encryption manager,
    /// the transport, our own identity and the key ring all derive through
    /// it, and a skew between them is silent.
    pub identity_mapper: RtcIdentityMapper,
    /// The membership format the call's room was opened in: the token request
    /// names the slot as that generation spells it.
    pub format: MembershipFormat,
    /// Damping of the tile order.
    pub stability: StabilityConfig,
}

/// A call with media attached.
pub struct MediaAttachment<C> {
    pub engine: CallEngine,
    /// The own-focus connection; `None` for a receive-only call, which only
    /// connects to its peers' foci (through the engine's pool).
    pub own_connection: Option<C>,
    /// Our transport identity, derived from the join's `member.id`.
    pub own_identity: String,
    /// Subscribed before any key was replayed or connection adopted, so it
    /// holds the `KeyImported` events of keys signalled before media attached.
    pub events: broadcast::Receiver<CallEvent>,
}

#[derive(Debug, thiserror::Error)]
pub enum AttachError {
    /// The call is over, or never had an encryption manager: join again first.
    #[error("{0}")]
    NotJoined(String),
    /// The call publishes on a transport this one cannot serve.
    #[error("{0}")]
    UnsupportedTransport(String),
    /// The own focus refused the connection.
    #[error(transparent)]
    Transport(#[from] TransportError),
}

/// Attach media to `call`: install `identity_mapper` and `key_handler` on its
/// encryption manager, start a [`CallEngine`] over `transport` (which opens
/// connections to every peer's focus), replay the keys signalled so far, and
/// connect the focus the call publishes on.
///
/// The own focus connects here rather than in the engine's pool so a broken
/// SFU fails this call instead of surfacing later as a dead session. On error
/// nothing is left running; leaving the slot stays the caller's.
pub async fn attach_media<B, T>(
    call: &RtcCall<B>,
    transport: Arc<T>,
    key_handler: Arc<MediaKeyHandler>,
    options: AttachOptions,
) -> Result<MediaAttachment<T::Connection>, AttachError>
where
    B: MatrixBackend + 'static,
    T: OwnFocusTransport + 'static,
{
    let (room_id, slot_id) = (call.room_id().to_owned(), call.slot_id().to_owned());
    if !call.is_live() {
        return Err(AttachError::NotJoined(format!(
            "{room_id}/{slot_id} is over — join the slot again first"
        )));
    }
    let own_key = match call.transport() {
        TransportIntent::Publish(published) => {
            Some(transport.connection_key(published).ok_or_else(|| {
                AttachError::UnsupportedTransport(format!(
                    "the call publishes on {published:?}, which is not a {} transport",
                    transport.transport_type(),
                ))
            })?)
        }
        TransportIntent::ReceiveOnly { .. } => None,
    };
    log::info!(
        "media: attaching to [{room_id}/{slot_id}] user={} device={} focus={}",
        options.own_user_id,
        options.own_device_id,
        own_key.as_deref().unwrap_or("none (receive only)"),
    );

    // The `member.id` is the join's rather than the host's: our transport
    // identity derives from it, so a value disagreeing with the published
    // membership would put our media on an identity no peer holds a key for.
    let member_id = call.member_id().to_owned();
    let memberships = call.subscribe_memberships().await;
    let raised_hands = call.subscribe_raised_hands().await;
    let reactions = call.subscribe_reactions().await;

    // Mapper before handler: identities are derived at signal time, and the
    // replay below derives them too, so a handler installed first would import
    // peer keys under the raw `member_id` fallback — an identity the SFU never
    // uses, which is indistinguishable from importing nothing.
    call.set_encryption_identity_mapper(options.identity_mapper.clone())
        .await;
    if !call
        .set_encryption_signal_handler(key_handler.clone())
        .await
    {
        return Err(AttachError::NotJoined(format!(
            "{room_id}/{slot_id} has no encryption manager — join the slot first"
        )));
    }

    let ctx = ConnectionContext {
        room_id: room_id.clone(),
        // The token request names the slot as this generation spells it.
        slot_id: options.format.token_slot_id(&slot_id).into_owned(),
        member: OwnMemberClaims {
            member_id: member_id.clone(),
            user_id: options.own_user_id.clone(),
            device_id: options.own_device_id.clone(),
        },
    };
    let pooled: Arc<dyn MediaTransport> = transport.clone();
    let engine = CallEngine::new(
        EngineConfig {
            transports: vec![pooled],
            own_member_id: member_id.clone(),
            ctx: ctx.clone(),
            own_connection_key: own_key.clone(),
            raised_hands,
            reactions,
            stability: options.stability,
        },
        memberships,
    );
    let events = engine.subscribe_events();

    // Imported keys surface as `CallEvent::KeyImported`, refused ones as
    // `CallEvent::KeyDiscarded` — the only way the reason a key was rejected
    // leaves the core; without it a host sees a `MissingKey` it cannot tell
    // from a key that never arrived.
    let engine_handle = engine.handle();
    key_handler.set_key_import_listener(Box::new(move |key| {
        engine_handle.notify_key_imported(key.rtc_backend_identity.clone(), key.key_index);
    }));
    let engine_handle = engine.handle();
    key_handler.set_key_discard_listener(Box::new(move |discarded| {
        engine_handle.notify_key_discarded(discarded);
    }));

    // Keys signalled between the join and now — our own first key, peer keys
    // already pumped in — were applied but heard by no listener. After the
    // listeners, so a host sees `KeyImported` for exactly the keys it is most
    // likely to be missing; before the connect, so the ring is populated before
    // the first frame can arrive. Idempotent at the ring, and it honours what
    // remains of a rotation's `delayBeforeUse`.
    if !call.replay_encryption_keys().await {
        log::warn!(
            "media: [{room_id}/{slot_id}] could not replay held keys; peers may stay \
             undecryptable until the next rotation",
        );
    }

    let own_identity =
        (options.identity_mapper)(&options.own_user_id, &options.own_device_id, &member_id);
    let own_connection = match &own_key {
        Some(own_key) => {
            let (connection, connection_events) = transport
                .connect_own(own_key, &ctx)
                .await
                .inspect_err(|error| {
                    log::warn!("media: own focus {own_key} refused the connection: {error}");
                })?;
            engine.adopt_own_connection(Box::new(connection.clone()), connection_events);

            // Move our sender onto each key we rotate to. Without this we
            // advertise a rotation to peers and keep encrypting with the
            // previous key, so anyone joining after it decrypts nothing — and
            // the forward secrecy the rotation exists for is not delivered.
            // After the connect and the replay, so the first key is already in
            // the ring; the hook only ever *moves* the index.
            let for_keys = connection.clone();
            key_handler.set_local_sender(
                own_identity.clone(),
                Box::new(move |key_index| for_keys.set_local_key_index(key_index)),
            );
            // Adopt the index we are already on rather than assuming 0: a
            // rotation between the join and here would otherwise be missed.
            if let Some(own) = key_handler.key_for(&own_identity) {
                connection.set_local_key_index(own.key_index);
            }
            Some(connection)
        }
        None => None,
    };

    log::info!("media: attached as member {member_id}, local identity {own_identity}");
    Ok(MediaAttachment {
        engine,
        own_connection,
        own_identity,
        events,
    })
}

#[cfg(test)]
mod tests;
