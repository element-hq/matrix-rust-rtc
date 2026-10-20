// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! A scripted media transport, for tests and debug builds (034, contract
//! C16): no media flows, but every connection's life is the test's to
//! decide. A focus can be interrupted (the transport reconnecting), closed
//! (the transport giving up), restored, made to refuse us, or to reject the
//! next tokens, and whatever the SFU would say can be sent on a live
//! connection. Our own handle supports the fallback's take-over (034 R25).
//!
//! Behind the `testing` feature: the call SDK's scenario tests and the FFI's
//! debug artifacts use it; nothing else should.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use matrix_rtc_core::compat::MembershipFormat;
use matrix_rtc_core::{JoinedMembership, RtcIdentityMapper, RtcTransport};
use tokio::sync::mpsc;

use crate::livekit::identity_mapper;
use crate::{
    ConnectionContext, ConnectionEvent, LocalTrackHandle, MediaStreamKind, MediaTransport,
    OwnFocusTransport, PublishOptions, TransportConnection, TransportError,
};

/// One focus as the script sees it.
#[derive(Default)]
struct Focus {
    /// Connects fail while set.
    down: bool,
    /// Connects are refused while set.
    refused: bool,
    /// How many more connects present a rejected token.
    token_rejections: u32,
    /// Every connect attempt, failed or not.
    connects: u32,
    /// The live connection's events; `None` while there is none.
    events: Option<mpsc::UnboundedSender<ConnectionEvent>>,
}

#[derive(Default)]
struct State {
    foci: HashMap<String, Focus>,
    /// The focus [`OwnFocusTransport::prepare_own`] was last asked for.
    own_key: Option<String>,
    /// Every focus our own handle took over, in order.
    takeovers: Vec<String>,
    /// Every publication made, in order.
    local_tracks: Vec<Arc<ScriptedLocalTrack>>,
}

/// A [`MediaTransport`] whose connections a test scripts. Cheap to clone;
/// clones share every focus.
#[derive(Clone)]
pub struct ScriptedTransport {
    state: Arc<Mutex<State>>,
    identity_mapper: Arc<Mutex<RtcIdentityMapper>>,
}

impl Default for ScriptedTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl ScriptedTransport {
    /// Peers' identities derived as MSC4195 says; see
    /// [`Self::set_identity_mapper`].
    pub fn new() -> Self {
        Self {
            state: Arc::default(),
            identity_mapper: Arc::new(Mutex::new(identity_mapper(MembershipFormat::Current))),
        }
    }

    /// Derive peers' identities with `identity_mapper`, which must be the one
    /// the call's encryption manager uses, as for any transport.
    pub fn set_identity_mapper(&self, identity_mapper: RtcIdentityMapper) {
        *self
            .identity_mapper
            .lock()
            .expect("scripted transport poisoned") = identity_mapper;
    }

    /// Every focus a connection was asked for, ours included.
    pub fn foci(&self) -> Vec<String> {
        let mut foci: Vec<String> = self
            .state
            .lock()
            .expect("scripted transport poisoned")
            .foci
            .keys()
            .cloned()
            .collect();
        foci.sort();
        foci
    }

    fn with_focus<R>(&self, key: &str, f: impl FnOnce(&mut Focus) -> R) -> R {
        let mut state = self.state.lock().expect("scripted transport poisoned");
        f(state.foci.entry(key.to_owned()).or_default())
    }

    /// The transport loses `key` and tries to get it back: connects fail and
    /// a live connection reports [`ConnectionEvent::Reconnecting`].
    pub fn interrupt(&self, key: &str) {
        self.with_focus(key, |focus| {
            focus.down = true;
            if let Some(events) = &focus.events {
                let _ = events.send(ConnectionEvent::Reconnecting);
            }
        });
    }

    /// The transport gives `key` up: connects fail, and a live connection
    /// reports [`ConnectionEvent::Closed`] and ends.
    pub fn close(&self, key: &str) {
        self.with_focus(key, |focus| {
            focus.down = true;
            if let Some(events) = focus.events.take() {
                let _ = events.send(ConnectionEvent::Closed {
                    message: "scripted outage".to_owned(),
                });
            }
        });
    }

    /// `key` works again: connects succeed, and a connection that was only
    /// interrupted reports [`ConnectionEvent::Reconnected`].
    pub fn restore(&self, key: &str) {
        self.with_focus(key, |focus| {
            focus.down = false;
            focus.refused = false;
            if let Some(events) = &focus.events {
                let _ = events.send(ConnectionEvent::Reconnected);
            }
        });
    }

    /// Connects to `key` are refused for good (a 4xx), until restored.
    pub fn refuse(&self, key: &str) {
        self.with_focus(key, |focus| focus.refused = true);
    }

    /// The next `times` connects to `key` present a token the SFU rejects.
    pub fn reject_token(&self, key: &str, times: u32) {
        self.with_focus(key, |focus| focus.token_rejections = times);
    }

    /// Sends `event` on `key`'s live connection; `false` when there is none.
    pub fn send(&self, key: &str, event: ConnectionEvent) -> bool {
        self.with_focus(key, |focus| {
            focus
                .events
                .as_ref()
                .is_some_and(|events| events.send(event).is_ok())
        })
    }

    /// How many connects `key` was asked for, failed or not.
    pub fn connects(&self, key: &str) -> u32 {
        self.with_focus(key, |focus| focus.connects)
    }

    /// The focus our own handle was prepared for.
    pub fn own_key(&self) -> Option<String> {
        self.state
            .lock()
            .expect("scripted transport poisoned")
            .own_key
            .clone()
    }

    /// Every focus our own handle took over, in order (034 R25).
    pub fn takeovers(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("scripted transport poisoned")
            .takeovers
            .clone()
    }

    /// Every publication made through any connection, in order.
    pub fn local_tracks(&self) -> Vec<Arc<ScriptedLocalTrack>> {
        self.state
            .lock()
            .expect("scripted transport poisoned")
            .local_tracks
            .clone()
    }

    fn connection(&self, key: &str) -> ScriptedConnection {
        ScriptedConnection {
            prepared_for: key.to_owned(),
            current: Arc::new(Mutex::new(key.to_owned())),
            transport: self.clone(),
        }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl MediaTransport for ScriptedTransport {
    fn transport_type(&self) -> &'static str {
        "livekit"
    }

    fn connection_key(&self, transport: &RtcTransport) -> Option<String> {
        match transport {
            RtcTransport::LiveKit(livekit) => Some(livekit.livekit_service_url.clone()),
            RtcTransport::Unsupported(_) => None,
        }
    }

    fn remote_identity(&self, member: &JoinedMembership) -> Option<String> {
        let device_id = member.origin.sender_device_id()?;
        let identity_mapper = self
            .identity_mapper
            .lock()
            .expect("scripted transport poisoned")
            .clone();
        Some(identity_mapper(
            &member.sender,
            device_id,
            &member.member_id,
        ))
    }

    async fn connect(
        &self,
        connection_key: &str,
        _ctx: &ConnectionContext,
    ) -> Result<
        (
            Box<dyn TransportConnection>,
            mpsc::UnboundedReceiver<ConnectionEvent>,
        ),
        TransportError,
    > {
        let connection = self.connection(connection_key);
        let events = connection.reconnect(false).await?;
        Ok((Box::new(connection), events))
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl OwnFocusTransport for ScriptedTransport {
    type Connection = ScriptedConnection;

    async fn connect_own(
        &self,
        connection_key: &str,
        ctx: &ConnectionContext,
    ) -> Result<(ScriptedConnection, mpsc::UnboundedReceiver<ConnectionEvent>), TransportError>
    {
        let connection = self.prepare_own(connection_key, ctx);
        let events = connection.reconnect(false).await?;
        Ok((connection, events))
    }

    fn prepare_own(&self, connection_key: &str, _ctx: &ConnectionContext) -> ScriptedConnection {
        self.state
            .lock()
            .expect("scripted transport poisoned")
            .own_key = Some(connection_key.to_owned());
        self.connection(connection_key)
    }
}

/// A handle to one scripted focus; it moves to another on a take-over.
#[derive(Clone)]
pub struct ScriptedConnection {
    prepared_for: String,
    /// The focus this handle is on now.
    current: Arc<Mutex<String>>,
    transport: ScriptedTransport,
}

impl ScriptedConnection {
    fn current(&self) -> String {
        self.current
            .lock()
            .expect("scripted connection poisoned")
            .clone()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl TransportConnection for ScriptedConnection {
    fn connection_key(&self) -> &str {
        &self.prepared_for
    }

    async fn publish(
        &self,
        options: PublishOptions,
    ) -> Result<Arc<dyn LocalTrackHandle>, TransportError> {
        let track = Arc::new(ScriptedLocalTrack {
            kind: options.kind,
            muted: AtomicBool::new(options.muted),
            mutes: Mutex::new(Vec::new()),
        });
        self.transport
            .state
            .lock()
            .expect("scripted transport poisoned")
            .local_tracks
            .push(track.clone());
        Ok(track)
    }

    async fn reconnect(
        &self,
        fresh_token: bool,
    ) -> Result<mpsc::UnboundedReceiver<ConnectionEvent>, TransportError> {
        let key = self.current();
        self.transport.with_focus(&key, |focus| {
            focus.connects += 1;
            if focus.refused {
                return Err(TransportError::Refused(format!("{key} refuses us")));
            }
            if focus.token_rejections > 0 {
                focus.token_rejections -= 1;
                return Err(TransportError::TokenRejected(format!(
                    "{key} rejects the {} token",
                    if fresh_token { "fresh" } else { "cached" },
                )));
            }
            if focus.down {
                return Err(TransportError::Connect(format!("{key} is unreachable")));
            }
            let (events, receiver) = mpsc::unbounded_channel();
            focus.events = Some(events);
            Ok(receiver)
        })
    }

    fn hand_over(&self) -> Option<Box<dyn Any>> {
        Some(Box::new(self.current()))
    }

    fn take_over(&self, handover: Box<dyn Any>) -> Result<(), TransportError> {
        let key = handover.downcast::<String>().map_err(|_| {
            TransportError::Unsupported("not a scripted connection's handover".into())
        })?;
        self.transport
            .state
            .lock()
            .expect("scripted transport poisoned")
            .takeovers
            .push((*key).clone());
        *self.current.lock().expect("scripted connection poisoned") = *key;
        Ok(())
    }

    async fn close(&self) -> Result<(), TransportError> {
        let key = self.current();
        self.transport.with_focus(&key, |focus| focus.events = None);
        Ok(())
    }
}

/// A publication that carries nothing and records its mutes.
pub struct ScriptedLocalTrack {
    kind: MediaStreamKind,
    muted: AtomicBool,
    mutes: Mutex<Vec<bool>>,
}

impl ScriptedLocalTrack {
    /// Whether the transport holds it muted now.
    pub fn is_muted(&self) -> bool {
        self.muted.load(Ordering::SeqCst)
    }

    /// Every `set_muted` the engine made, in order.
    pub fn mutes(&self) -> Vec<bool> {
        self.mutes.lock().expect("scripted track poisoned").clone()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl LocalTrackHandle for ScriptedLocalTrack {
    fn kind(&self) -> MediaStreamKind {
        self.kind
    }

    fn set_muted(&self, muted: bool) -> Result<(), TransportError> {
        self.muted.store(muted, Ordering::SeqCst);
        self.mutes
            .lock()
            .expect("scripted track poisoned")
            .push(muted);
        Ok(())
    }

    async fn unpublish(&self) -> Result<(), TransportError> {
        Ok(())
    }
}
