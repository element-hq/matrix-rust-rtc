// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Key rotation as timed multi-party scenarios.
//!
//! Rotation is a *policy*, and its inputs are the joined memberships and a clock: how many
//! keys a call burns through depends entirely on when people arrive and leave
//! relative to `key_rotation_grace_period_ms` and `delay_before_use_ms`. A
//! single manager checked against a hand-written membership list cannot say
//! anything about that — it cannot tell a necessary rotation from a wasteful
//! one, and it cannot tell whether the other side could still decrypt.
//!
//! So these tests run a whole call: every participant is a real
//! [`EncryptionManager`], to-device messages are routed between them, and both
//! ends are measured.
//!
//! # Two things are asserted, and they pull against each other
//!
//! - **Cost** — how many keys were minted and how many to-device messages that
//!   cost. This is what "over-rotating" means, and the ideal is known exactly:
//!   in a call of `n` where nobody rotates, every ordered pair exchanges one
//!   key, so `n * (n - 1)` sends and one key each.
//! - **Correctness** — at every point, whatever a member is *encrypting with*
//!   must be a key every other member present holds. A cheap policy that leaves
//!   somebody unable to decrypt is not cheaper, it is broken.
//!
//! # No real time passes
//!
//! Both intervals are seconds long, so a test that waited them out would be
//! slow and would race the boundary it is checking. The managers read a clock
//! this file owns and advances by hand ([`EncryptionManager::set_clock`]), which
//! makes "the key is 9.999s old" as easy to state as "30s old", and makes the
//! resulting counts exact rather than approximate.
//!
//! # What the media layer is modelled as
//!
//! [`MediaSim`] stands in for `matrix-rtc-livekit`'s `MediaKeyBridge`: it
//! records every signalled key and honours MSC4143 `delayBeforeUse` by treating
//! a key as usable for *encryption* only from `now + use_after_ms`. Received
//! keys are usable for decryption at once — a frame cryptor keeps a ring of
//! indexes, so holding a key never stops you decrypting an older one.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use matrix_rtc_core::{
    CommandError, EncryptionConfig, EncryptionKeySignalHandler, EncryptionManager, EventOrigin,
    JoinedMembership, KeyMaterialSignal, KeyOrigin, MatrixBackend, ReceivedEncryptionKey,
    ToDeviceDelivery, ToDeviceRecipient,
};
use serde_json::Value;

const ROOM_ID: &str = "!room:example.org";
const SLOT_ID: &str = "m.call#room";

/// Where the scenarios start, so ages and deadlines read as offsets from zero.
const EPOCH: u64 = 1_700_000_000_000;

// ---------------------------------------------------------------------------
// Clock
// ---------------------------------------------------------------------------

/// A clock the test advances by hand.
#[derive(Clone)]
struct TestClock(Arc<AtomicU64>);

impl TestClock {
    fn new() -> Self {
        Self(Arc::new(AtomicU64::new(EPOCH)))
    }

    fn now(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }

    fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }

    fn set(&self, at: u64) {
        self.0.store(at, Ordering::SeqCst);
    }
}

// ---------------------------------------------------------------------------
// The media layer, as far as key material is concerned
// ---------------------------------------------------------------------------

/// One key as the media layer holds it.
#[derive(Clone, Debug)]
struct Install {
    /// Whose media this key belongs to, as the RTC backend names them.
    identity: String,
    index: u8,
    key: Vec<u8>,
    /// When this key may be used to *encrypt* (`now + delayBeforeUse`).
    usable_at_ms: u64,
    /// When it was handed over: for an own key, when it was minted.
    signalled_at_ms: u64,
}

/// Stands in for `MediaKeyBridge`: records signalled keys and when each becomes
/// usable.
struct MediaSim {
    own_identity: String,
    clock: TestClock,
    installs: Mutex<Vec<Install>>,
}

impl MediaSim {
    fn new(own_identity: String, clock: TestClock) -> Self {
        Self {
            own_identity,
            clock,
            installs: Mutex::new(Vec::new()),
        }
    }

    /// The key this member is encrypting with right now: the newest of its own
    /// keys whose `delayBeforeUse` has elapsed.
    fn sending_key(&self, at_ms: u64) -> Option<(u8, Vec<u8>)> {
        self.installs
            .lock()
            .unwrap()
            .iter()
            .rfind(|install| install.identity == self.own_identity && install.usable_at_ms <= at_ms)
            .map(|install| (install.index, install.key.clone()))
    }

    /// Can this member decrypt a frame that `identity` stamped with `index`?
    fn can_decrypt(&self, identity: &str, index: u8, key: &[u8]) -> bool {
        self.installs.lock().unwrap().iter().any(|install| {
            install.identity == identity && install.index == index && install.key == key
        })
    }

    /// Has this member been handed any key at all for `identity`?
    fn holds_anything_from(&self, identity: &str) -> bool {
        self.installs
            .lock()
            .unwrap()
            .iter()
            .any(|install| install.identity == identity)
    }

    /// How many of its *own* keys this member has been handed — one per key it
    /// minted, so `keys_minted() - 1` rotations.
    fn keys_minted(&self) -> usize {
        self.installs
            .lock()
            .unwrap()
            .iter()
            .filter(|install| install.identity == self.own_identity)
            .count()
    }

    /// The own-key indexes handed to the media layer, in order.
    fn own_indexes(&self) -> Vec<u8> {
        self.installs
            .lock()
            .unwrap()
            .iter()
            .filter(|install| install.identity == self.own_identity)
            .map(|install| install.index)
            .collect()
    }

    /// The latest instant at which some pending key becomes usable.
    fn latest_deadline(&self) -> u64 {
        self.installs
            .lock()
            .unwrap()
            .iter()
            .map(|install| install.usable_at_ms)
            .max()
            .unwrap_or(0)
    }
}

#[async_trait]
impl EncryptionKeySignalHandler for MediaSim {
    async fn on_new_key_material(&self, signal: KeyMaterialSignal) {
        self.installs.lock().unwrap().push(Install {
            identity: signal.rtc_backend_identity,
            index: signal.key_index,
            key: signal.key,
            usable_at_ms: self.clock.now() + signal.use_after_ms,
            signalled_at_ms: self.clock.now(),
        });
    }
}

// ---------------------------------------------------------------------------
// The to-device bus
// ---------------------------------------------------------------------------

/// One key message in flight.
struct KeyMessage {
    /// The `member_id` the content names — whose key this is.
    from_member_id: String,
    to: ToDeviceRecipient,
    index: u8,
    key: Vec<u8>,
}

/// One (message, recipient) send attempt.
struct LoggedSend {
    from_member_id: String,
    index: u8,
    to: ToDeviceRecipient,
}

/// The homeserver's to-device channel, and the meter on it.
#[derive(Default)]
struct Bus {
    in_flight: Mutex<Vec<KeyMessage>>,
    /// Every (message, recipient) pair the client tried to send, refused ones
    /// included: a rejected to-device message is a request that was still made,
    /// so counting only the accepted ones would make an unreachable member look
    /// cheap.
    sends: AtomicU64,
    /// The same sends, kept in order with who sent what to which device, so a
    /// scenario can be broken down into "this change cost these keys and these
    /// messages" rather than just a total. Feeds `Call::describe_since`.
    log: Mutex<Vec<LoggedSend>>,
    /// Devices the server refuses to deliver to, as a rate limit or a dead
    /// device would.
    blocked: Mutex<HashSet<(String, String)>>,
}

impl Bus {
    fn drain(&self) -> Vec<KeyMessage> {
        std::mem::take(&mut *self.in_flight.lock().unwrap())
    }

    fn sends(&self) -> u64 {
        self.sends.load(Ordering::SeqCst)
    }
}

/// The backend one participant is given.
struct PeerSender {
    bus: Arc<Bus>,
    own_member_id: String,
}

#[async_trait]
impl MatrixBackend for PeerSender {
    fn own_user_id(&self) -> String {
        "@peer:example.org".to_owned()
    }

    fn own_device_id(&self) -> String {
        "DEVICE".to_owned()
    }

    async fn send_sticky_event(
        &self,
        _room_id: String,
        _event_type: String,
        _content: Value,
        _duration_ms: u64,
    ) -> Result<String, CommandError> {
        Ok("$sticky".to_owned())
    }

    async fn send_delayed_event(
        &self,
        _room_id: String,
        _event_type: String,
        _state_key: Option<String>,
        _content: Value,
        _delay_ms: u64,
    ) -> Result<String, CommandError> {
        Ok("delay-id".to_owned())
    }

    async fn send_room_event(
        &self,
        _room_id: String,
        _event_type: String,
        _content: Value,
    ) -> Result<String, CommandError> {
        Ok("$room".to_owned())
    }

    async fn redact_event(
        &self,
        _room_id: String,
        _event_id: String,
        _reason: Option<String>,
    ) -> Result<(), CommandError> {
        Ok(())
    }

    async fn restart_delayed_event(
        &self,
        _room_id: String,
        _delay_id: String,
    ) -> Result<(), CommandError> {
        Ok(())
    }

    async fn cancel_delayed_event(
        &self,
        _room_id: String,
        _delay_id: String,
    ) -> Result<(), CommandError> {
        Ok(())
    }

    async fn send_to_device_message(
        &self,
        recipients: Vec<ToDeviceRecipient>,
        _message_type: String,
        content: Value,
    ) -> Result<Vec<ToDeviceDelivery>, CommandError> {
        let index = content
            .pointer("/media_key/index")
            .and_then(Value::as_u64)
            .expect("a key message carries an index") as u8;
        let key = general_purpose::STANDARD
            .decode(
                content
                    .pointer("/media_key/key")
                    .and_then(Value::as_str)
                    .expect("a key message carries key material"),
            )
            .expect("key material is base64");
        let from_member_id = content
            .get("member_id")
            .and_then(Value::as_str)
            .expect("a key message names its member")
            .to_owned();

        let blocked = self.bus.blocked.lock().unwrap().clone();
        let mut deliveries = Vec::with_capacity(recipients.len());
        for recipient in recipients {
            self.bus.sends.fetch_add(1, Ordering::SeqCst);
            self.bus.log.lock().unwrap().push(LoggedSend {
                from_member_id: from_member_id.clone(),
                index,
                to: recipient.clone(),
            });
            if blocked.contains(&(recipient.user_id.clone(), recipient.device_id.clone())) {
                deliveries.push(ToDeviceDelivery::failed(recipient, "M_LIMIT_EXCEEDED"));
                continue;
            }
            self.bus.in_flight.lock().unwrap().push(KeyMessage {
                from_member_id: from_member_id.clone(),
                to: recipient.clone(),
                index,
                key: key.clone(),
            });
            deliveries.push(ToDeviceDelivery::sent(recipient));
        }
        debug_assert_eq!(self.own_member_id, from_member_id);
        Ok(deliveries)
    }

    async fn send_state_event(
        &self,
        _room_id: String,
        _event_type: String,
        _state_key: String,
        _content: Value,
    ) -> Result<String, CommandError> {
        Ok("$state".to_owned())
    }
}

// ---------------------------------------------------------------------------
// A participant
// ---------------------------------------------------------------------------

struct Peer {
    name: String,
    user_id: String,
    device_id: String,
    /// Fresh on every join, as MSC4143 requires.
    member_id: String,
    manager: EncryptionManager<PeerSender>,
    media: Arc<MediaSim>,
    in_call: bool,
}

impl Peer {
    /// How the RTC backend names this member. The real one is the MSC4195
    /// pseudonymous identity; all that matters here is that it is stable across
    /// a member's rejoins and identical on both ends.
    fn identity(&self) -> String {
        identity_of(&self.user_id, &self.device_id)
    }

    fn membership(&self) -> JoinedMembership {
        JoinedMembership {
            room_id: ROOM_ID.to_owned(),
            slot_id: SLOT_ID.to_owned(),
            sender: self.user_id.clone(),
            origin: EventOrigin::encrypted(Some(self.device_id.clone())),
            sticky_key: self.member_id.clone(),
            member_id: self.member_id.clone(),
            membership_event_id: None,
            membership_ts: None,
            origin_server_ts: None,
            application: "m.call".into(),
            transports: Vec::new(),
            can_subscribe: Vec::new(),
        }
    }
}

fn identity_of(user_id: &str, device_id: &str) -> String {
    format!("{user_id}/{device_id}")
}

/// A name is enough to address a participant, whether or not they have joined
/// yet — a device can be unreachable before its owner ever arrives.
fn user_id_of(name: &str) -> String {
    format!("@{name}:example.org")
}

fn device_id_of(name: &str) -> String {
    format!("{}DEV", name.to_uppercase())
}

// ---------------------------------------------------------------------------
// The call
// ---------------------------------------------------------------------------

/// One call: shared joined memberships, a shared clock, a to-device bus, and every
/// participant's real encryption manager.
///
/// The joined memberships are shared rather than per-peer on purpose: everybody sees every
/// membership change instantly, which is the friendliest input the policy can
/// get. Anything that goes wrong here is not the network's fault.
struct Call {
    clock: TestClock,
    bus: Arc<Bus>,
    joined: Arc<Mutex<Vec<JoinedMembership>>>,
    peers: Vec<Peer>,
    config: EncryptionConfig,
    /// Bumped per join so a rejoining device gets a fresh `member_id`.
    joins: u32,
}

impl Call {
    fn new(config: EncryptionConfig) -> Self {
        Self {
            clock: TestClock::new(),
            bus: Arc::new(Bus::default()),
            joined: Arc::new(Mutex::new(Vec::new())),
            peers: Vec::new(),
            config,
            joins: 0,
        }
    }

    fn with_defaults() -> Self {
        Self::new(EncryptionConfig::default())
    }

    fn peer(&self, name: &str) -> &Peer {
        self.peers
            .iter()
            .find(|peer| peer.name == name)
            .unwrap_or_else(|| panic!("{name} is not in this call"))
    }

    /// Someone joins, and everybody reacts.
    async fn join(&mut self, name: &str) {
        self.add_peer(name).await;
        self.settle().await;
    }

    /// Several people join and the joined memberships change once.
    async fn join_together(&mut self, names: &[&str]) {
        for name in names {
            self.add_peer(name).await;
        }
        self.settle().await;
    }

    /// A participant's manager joins and its membership is added; nobody has
    /// reacted yet.
    async fn add_peer(&mut self, name: &str) {
        self.joins += 1;
        let user_id = format!("@{name}:example.org");
        let device_id = format!("{}DEV", name.to_uppercase());
        let member_id = format!("{name}-join{}", self.joins);

        // A rejoin reuses the device but never the participation.
        if let Some(existing) = self.peers.iter().position(|peer| peer.name == name) {
            self.peers.remove(existing);
        }

        let media = Arc::new(MediaSim::new(
            identity_of(&user_id, &device_id),
            self.clock.clone(),
        ));
        let joined = self.joined.clone();
        let mut manager = EncryptionManager::new(
            Arc::new(PeerSender {
                bus: self.bus.clone(),
                own_member_id: member_id.clone(),
            }),
            user_id.clone(),
            device_id.clone(),
            member_id.clone(),
            ROOM_ID.to_owned(),
            SLOT_ID.to_owned(),
            move || joined.lock().unwrap().clone(),
        );
        manager.set_config(self.config.clone());
        manager.set_clock({
            let clock = self.clock.clone();
            Arc::new(move || clock.now())
        });
        manager.set_signal_handler(media.clone());
        manager.set_identity_mapper(Arc::new(|user_id, device_id, _member_id| {
            identity_of(user_id, device_id)
        }));
        manager.join().await.expect("join should succeed");

        let peer = Peer {
            name: name.to_owned(),
            user_id,
            device_id,
            member_id,
            manager,
            media,
            in_call: true,
        };
        self.joined.lock().unwrap().push(peer.membership());
        self.peers.push(peer);
    }

    /// Someone leaves, and everybody reacts.
    async fn leave(&mut self, name: &str) {
        let member_id = {
            let peer = self
                .peers
                .iter_mut()
                .find(|peer| peer.name == name)
                .unwrap_or_else(|| panic!("{name} is not in this call"));
            peer.in_call = false;
            peer.manager.leave();
            peer.member_id.clone()
        };
        self.joined
            .lock()
            .unwrap()
            .retain(|membership| membership.member_id != member_id);

        self.settle().await;
    }

    /// Several people leave and the joined memberships change once, which is what a host that
    /// feeds whole sticky-state snapshots produces (`RtcSession::set_current_state`
    /// rebuilds the candidate set and refreshes once).
    async fn leave_together(&mut self, names: &[&str]) {
        for name in names {
            let member_id = {
                let peer = self
                    .peers
                    .iter_mut()
                    .find(|peer| peer.name == *name)
                    .unwrap_or_else(|| panic!("{name} is not in this call"));
                peer.in_call = false;
                peer.manager.leave();
                peer.member_id.clone()
            };
            self.joined
                .lock()
                .unwrap()
                .retain(|membership| membership.member_id != member_id);
        }

        self.settle().await;
    }

    /// The homeserver stops accepting key messages for this member's device.
    fn block_device(&self, name: &str) {
        self.bus
            .blocked
            .lock()
            .unwrap()
            .insert((user_id_of(name), device_id_of(name)));
    }

    fn unblock_device(&self, name: &str) {
        self.bus
            .blocked
            .lock()
            .unwrap()
            .remove(&(user_id_of(name), device_id_of(name)));
    }

    /// Time passes with nothing happening to the joined memberships.
    ///
    /// A rotation coalesced into a switch window falls due at some point during
    /// this, and something has to collect it — in a real client whatever drives
    /// `flush_due_rotation` off `rotation_due_at_ms()`, or failing that the session
    /// heartbeat. This stands in for that, on time, so what the scenarios measure
    /// is the policy rather than a consumer's tick rate.
    async fn advance(&self, ms: u64) {
        self.clock.advance(ms);
        for peer in self.peers.iter().filter(|peer| peer.in_call) {
            peer.manager
                .flush_due_rotation()
                .await
                .expect("a deferred rotation should not fail");
        }
        self.deliver_all().await;
    }

    /// One membership change, delivered to everyone: each participant is told the
    /// memberships moved, and every key message that produces is routed.
    ///
    /// This is exactly one `on_memberships_update()` per participant, which is
    /// what `RtcSession::refresh` does for one change. Deliveries cannot
    /// themselves trigger a rollout, so draining until empty is enough.
    async fn settle(&self) {
        for peer in self.peers.iter().filter(|peer| peer.in_call) {
            peer.manager
                .on_memberships_update()
                .await
                .expect("a membership update should not fail");
        }

        self.deliver_all().await;
    }

    async fn deliver_all(&self) {
        for _ in 0..8 {
            let batch = self.bus.drain();
            if batch.is_empty() {
                break;
            }
            for message in batch {
                self.deliver(message).await;
            }
        }
    }

    async fn deliver(&self, message: KeyMessage) {
        let Some(sender) = self
            .peers
            .iter()
            .find(|peer| peer.member_id == message.from_member_id)
        else {
            return;
        };
        let Some(recipient) = self.peers.iter().find(|peer| {
            peer.user_id == message.to.user_id && peer.device_id == message.to.device_id
        }) else {
            return;
        };
        if !recipient.in_call {
            return;
        }

        recipient
            .manager
            .receive_key(ReceivedEncryptionKey {
                origin: KeyOrigin::Encrypted {
                    sender_user_id: sender.user_id.clone(),
                    sender_device_id: Some(sender.device_id.clone()),
                    sender_is_confirmed: true,
                },
                room_id: ROOM_ID.to_owned(),
                member_id: message.from_member_id.clone(),
                key_b64: general_purpose::STANDARD.encode(&message.key),
                key_index: message.index,
            })
            .await
            .expect("receiving a key should not fail");
    }

    // -- measurements --------------------------------------------------------

    /// Keys this member has rotated to since its first.
    fn rotations(&self, name: &str) -> usize {
        self.peer(name).media.keys_minted().saturating_sub(1)
    }

    /// Every key any participant has ever minted, departed ones included — the
    /// whole call's spend, where `total_rotations` discounts the key each member
    /// arrives with.
    fn keys_minted_total(&self) -> usize {
        self.peers.iter().map(|peer| peer.media.keys_minted()).sum()
    }

    fn total_rotations(&self) -> usize {
        self.peers
            .iter()
            .map(|peer| peer.media.keys_minted().saturating_sub(1))
            .sum()
    }

    /// Every (message, recipient) pair the homeserver accepted.
    fn key_sends(&self) -> u64 {
        self.bus.sends()
    }

    /// Where the send log stands now, to describe what a later step cost.
    fn mark(&self) -> usize {
        self.bus.log.lock().unwrap().len()
    }

    /// What happened since `mark`, as "who sent which key index, how many times".
    ///
    /// This is what turns a total into an account of it: `alice#2 x3` is one
    /// rotation of alice's, broadcast to three peers.
    fn describe_since(&self, mark: usize, label: &str) -> String {
        let log = self.bus.log.lock().unwrap();
        let mut counted: Vec<(String, u8, usize)> = Vec::new();
        for LoggedSend {
            from_member_id: member_id,
            index,
            ..
        } in log.iter().skip(mark)
        {
            let name = self
                .peers
                .iter()
                .find(|peer| &peer.member_id == member_id)
                .map(|peer| peer.name.as_str())
                .unwrap_or("<gone>");
            match counted
                .iter_mut()
                .find(|(seen, seen_index, _)| seen == name && seen_index == index)
            {
                Some((_, _, count)) => *count += 1,
                None => counted.push((name.to_owned(), *index, 1)),
            }
        }

        let total: usize = counted.iter().map(|(_, _, count)| count).sum();
        let detail: Vec<String> = counted
            .iter()
            .map(|(name, index, count)| format!("{name}#{index} x{count}"))
            .collect();
        format!(
            "{label}: {total} send(s){}{}",
            if detail.is_empty() { "" } else { " — " },
            detail.join(", "),
        )
    }

    /// A one-line summary for a failure message.
    fn cost(&self) -> String {
        let per_peer: Vec<String> = self
            .peers
            .iter()
            .filter(|peer| peer.in_call)
            .map(|peer| format!("{}:{:?}", peer.name, peer.media.own_indexes()))
            .collect();
        format!(
            "{} key(s) minted after the first, {} to-device send(s); own key indexes {}",
            self.total_rotations(),
            self.key_sends(),
            per_peer.join(" "),
        )
    }

    /// Who, right now, cannot decrypt whom.
    ///
    /// The question is asked about what each sender is *encrypting with* at this
    /// instant, not about the newest key it holds: a key inside its
    /// `delayBeforeUse` is not in use yet, and one every peer already has is
    /// fine however old it is.
    fn undecryptable_pairs(&self) -> Vec<String> {
        let now = self.clock.now();
        let mut broken = Vec::new();
        for sender in self.peers.iter().filter(|peer| peer.in_call) {
            let Some((index, key)) = sender.media.sending_key(now) else {
                broken.push(format!("{} has no usable key of its own", sender.name));
                continue;
            };
            for receiver in self
                .peers
                .iter()
                .filter(|peer| peer.in_call && peer.name != sender.name)
            {
                if !receiver.media.can_decrypt(&sender.identity(), index, &key) {
                    broken.push(format!(
                        "{} cannot decrypt {} (index {index})",
                        receiver.name, sender.name,
                    ));
                }
            }
        }
        broken
    }

    /// Everybody present can decrypt everybody present, now.
    fn assert_media_flows(&self, when: &str) {
        let broken = self.undecryptable_pairs();
        assert!(
            broken.is_empty(),
            "{when}: media does not flow — {}\n  cost so far: {}",
            broken.join("; "),
            self.cost(),
        );
    }

    /// Everybody can decrypt everybody once every pending `delayBeforeUse` has
    /// elapsed and nothing else has happened.
    ///
    /// This is the weaker, unarguable form of the invariant: whatever the policy
    /// does in the meantime, a settled call must converge on keys everybody
    /// holds.
    async fn assert_media_flows_once_settled(&self, when: &str) {
        let deadline = self
            .peers
            .iter()
            .filter(|peer| peer.in_call)
            .map(|peer| peer.media.latest_deadline())
            .max()
            .unwrap_or(0);
        let resume = self.clock.now();
        self.clock.set(deadline.max(resume));
        let broken = self.undecryptable_pairs();
        self.clock.set(resume);
        assert!(
            broken.is_empty(),
            "{when}: media still does not flow once every delayBeforeUse elapsed — {}\n  \
             cost so far: {}",
            broken.join("; "),
            self.cost(),
        );
    }
}

/// The floor for a call of `n`: every ordered pair exchanges one key, once.
fn ideal_sends(n: u64) -> u64 {
    n * (n - 1)
}

// ===========================================================================
// Scenarios
// ===========================================================================

/// Four people arrive one second apart. Every arrival lands inside the grace
/// period, so nobody should mint a second key: the whole call runs on the four
/// keys created at join, and each pair exchanges exactly one.
#[tokio::test]
async fn arrivals_inside_the_grace_period_cost_no_rotation() {
    let mut call = Call::with_defaults();

    call.join("alice").await;
    for name in ["bob", "carol", "dave"] {
        call.advance(1_000).await;
        call.join(name).await;
    }

    call.assert_media_flows("four staggered arrivals");
    assert_eq!(
        call.total_rotations(),
        0,
        "no arrival was outside the grace period, so no key needed replacing — {}",
        call.cost(),
    );
    assert_eq!(
        call.key_sends(),
        ideal_sends(4),
        "each of the 4 members should hand its one key to the other 3 — {}",
        call.cost(),
    );
}

/// Eight people arriving in bulk, a second apart — a meeting starting.
///
/// The whole burst lands inside one grace period, so it runs on the eight keys
/// they each brought and costs the floor: one key per ordered pair, once.
#[tokio::test]
async fn a_bulk_join_costs_no_rotation() {
    let mut call = Call::with_defaults();
    let names = [
        "alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi",
    ];

    call.join(names[0]).await;
    for name in &names[1..] {
        call.advance(1_000).await;
        call.join(name).await;
    }

    call.assert_media_flows("eight arrivals inside one grace period");
    assert_eq!(
        call.total_rotations(),
        0,
        "the burst fits inside the grace period — {}",
        call.cost(),
    );
    assert_eq!(
        call.key_sends(),
        ideal_sends(8),
        "exactly the floor for a call of eight — {}",
        call.cost(),
    );
}

/// The same four arrivals, but each one lands outside the grace period.
///
/// Every member present mints a new key for every arrival and re-sends it to
/// everyone, so the traffic grows with the square of the call. That is the policy
/// as designed — it mirrors the JS SDK's key manager, which is what Element Call
/// runs — and it is the price of a joiner not being able to read what came before
/// them.
///
/// So this test pins the cost rather than objecting to it: the numbers are the
/// baseline any change to the policy, or to `key_rotation_grace_period_ms`, gets
/// measured against.
#[tokio::test]
async fn arrivals_outside_the_grace_period_rotate_the_whole_call() {
    let mut call = Call::with_defaults();

    call.join("alice").await;
    for name in ["bob", "carol", "dave"] {
        call.advance(30_000).await;
        call.join(name).await;
    }

    call.assert_media_flows_once_settled("four spaced-out arrivals")
        .await;
    assert_eq!(
        call.total_rotations(),
        6,
        "one rotation per member already present, per arrival — {}",
        call.cost(),
    );
    assert_eq!(
        call.key_sends(),
        20,
        "against a floor of {} for a call of four: the extra sends are the re-keying — {}",
        ideal_sends(4),
        call.cost(),
    );
}

/// Three people hang up together, seen as one membership change.
///
/// This is the shape the real ingestion path produces:
/// `BaseRtcRoom::set_current_sticky_state` takes the **whole** sticky state
/// and has deliberately no delta entry point, and `RtcSession::set_current_state`
/// rebuilds the candidate set before refreshing once. So simultaneous hangups
/// reach the policy as a single change and must cost a single key — which is also
/// what makes a debounce unnecessary for this case.
#[tokio::test]
async fn departures_in_one_membership_change_cost_one_rotation() {
    let mut call = Call::with_defaults();

    for name in ["alice", "bob", "carol", "dave", "erin"] {
        call.join(name).await;
    }
    call.advance(30_000).await;
    let before = call.key_sends();

    call.leave_together(&["bob", "carol", "dave"]).await;

    call.assert_media_flows("three departures in one membership change");
    assert_eq!(
        call.rotations("alice"),
        1,
        "one membership change, one new key — {}",
        call.cost(),
    );
    assert_eq!(
        call.key_sends() - before,
        2,
        "alice and erin are what is left: one new key each, handed to the other — {}",
        call.cost(),
    );
}

/// Three departures, each arriving as its own membership change, while the key is fresh.
///
/// The first rotates, because the call had settled. That makes a fresh key, and the
/// two departures landing during its freshness do not each mint one of their own —
/// they are coalesced into the single rotation that closes the window. Two keys for
/// the burst instead of three, and the traffic drops with it, because every rotation
/// avoided is one that would have been broadcast to everybody left.
#[tokio::test]
async fn departures_while_the_key_is_fresh_coalesce_into_one_rotation() {
    let grace = EncryptionConfig::default().key_rotation_grace_period_ms;
    let delay = EncryptionConfig::default().delay_before_use_ms;
    let mut call = Call::with_defaults();

    for name in ["alice", "bob", "carol", "dave", "erin"] {
        call.join(name).await;
    }
    call.advance(30_000).await;
    let before = call.key_sends();

    call.leave("bob").await;
    let after_first_departure = call.rotations("alice");
    call.advance(500).await;
    call.leave("carol").await;
    call.advance(500).await;
    call.leave("dave").await;

    assert_eq!(
        after_first_departure,
        1,
        "the first departure rotates straight away — {}",
        call.cost(),
    );
    assert_eq!(
        call.rotations("alice"),
        1,
        "carol and dave left while that key was still fresh, so they must be coalesced into \
         one rotation rather than minting a key each — {}",
        call.cost(),
    );

    // Freshness ends, and the coalesced rotation happens: one key, answering both
    // departures at once.
    call.advance(grace).await;

    assert_eq!(
        call.rotations("alice"),
        2,
        "the deferred rotation must happen when freshness ends, or carol and dave keep \
         the key they hold — {}",
        call.cost(),
    );
    assert_eq!(
        call.key_sends() - before,
        14,
        "12 sends for the first departure (the 4 members left re-keying to 3 peers each), \
         nothing at all for the two coalesced into its window, and 2 for the rotation that \
         answers them both — against 20 with a rotation per departure — {}",
        call.cost(),
    );

    // And once *that* rotation is in use, none of the three can read the call.
    call.advance(delay).await;
    call.assert_media_flows("after a coalesced burst of departures");
    let now = call.clock.now();
    for departed in ["bob", "carol", "dave"] {
        for remaining in ["alice", "erin"] {
            let peer = call.peer(remaining);
            let (index, key) = peer
                .media
                .sending_key(now)
                .expect("a member in the call has a usable key");
            assert!(
                !call
                    .peer(departed)
                    .media
                    .can_decrypt(&peer.identity(), index, &key),
                "{departed} left, but {remaining} is still encrypting with index {index}, \
                 which {departed} holds — {}",
                call.cost(),
            );
        }
    }
}

/// What a member who leaves can still read, and for exactly how long.
///
/// `delayBeforeUse` keeps everyone remaining encrypting with the outgoing key
/// while its replacement propagates, and that key is the one the leaver holds. So
/// they do keep decrypting for the length of the delay — chosen deliberately,
/// because switching the instant they hang up stalls everybody else's video
/// instead.
///
/// What must hold is that the window is exactly the delay and not a millisecond
/// more: once it expires, every remaining member is encrypting with a key the
/// leaver was never sent.
#[tokio::test]
async fn a_departed_member_is_locked_out_when_the_delay_expires() {
    let mut call = Call::with_defaults();

    call.join("alice").await;
    call.join("bob").await;
    call.join("carol").await;
    call.advance(30_000).await;

    call.leave("carol").await;

    // Inside the window: the accepted trade. Recorded rather than asserted, so
    // that shortening the delay to zero does not fail this test — only widening
    // the exposure past it does.
    let inside = call.clock.now();
    let still_readable: Vec<&str> = ["alice", "bob"]
        .into_iter()
        .filter(|name| {
            let peer = call.peer(name);
            peer.media.sending_key(inside).is_some_and(|(index, key)| {
                call.peer("carol")
                    .media
                    .can_decrypt(&peer.identity(), index, &key)
            })
        })
        .collect();

    call.advance(EncryptionConfig::default().delay_before_use_ms)
        .await;

    let after = call.clock.now();
    for name in ["alice", "bob"] {
        let peer = call.peer(name);
        let (index, key) = peer
            .media
            .sending_key(after)
            .expect("a member in the call has a usable key");
        assert!(
            !call
                .peer("carol")
                .media
                .can_decrypt(&peer.identity(), index, &key),
            "the delay has expired, but {name} is still encrypting with index {index}, which \
             carol holds — she left, and was reading {still_readable:?} until now — {}",
            call.cost(),
        );
    }
}

/// Somebody joins an established call whose keys are no longer fresh.
///
/// Everyone present rotates for them, and keeps encrypting with the old key
/// through `delayBeforeUse` so they themselves never go blind. The newcomer is
/// not handed that old key: it was minted before they joined and would let them
/// read back over everything it encrypted, which is what rotating on their
/// arrival prevents. They decrypt once the new keys come into use, at most
/// `delayBeforeUse` after joining.
#[tokio::test]
async fn a_new_arrival_decrypts_once_the_switch_happens() {
    let mut call = Call::with_defaults();

    call.join("alice").await;
    call.join("bob").await;
    // Long enough that alice and bob will rotate for the next arrival.
    call.advance(30_000).await;

    call.join("carol").await;

    let (index, key) = call
        .peer("alice")
        .media
        .sending_key(call.clock.now())
        .expect("alice is sending");
    assert!(
        !call
            .peer("carol")
            .media
            .can_decrypt(&call.peer("alice").identity(), index, &key),
        "carol holds alice's key from before she joined",
    );
    call.assert_media_flows_once_settled("carol has just joined an established call")
        .await;
}

/// The grace period's edge, from both sides.
///
/// Nothing here is in question — it is the policy as written — but it is the
/// boundary every other cost in this file is measured against, and an off-by-one
/// in it would make those costs mean something else.
#[tokio::test]
async fn the_grace_period_decides_whether_an_arrival_rotates() {
    let grace = EncryptionConfig::default().key_rotation_grace_period_ms;

    let mut just_inside = Call::with_defaults();
    just_inside.join("alice").await;
    just_inside.advance(grace - 1).await;
    just_inside.join("bob").await;
    assert_eq!(
        just_inside.rotations("alice"),
        0,
        "a key 1ms short of the grace period is still fresh — {}",
        just_inside.cost(),
    );

    let mut just_outside = Call::with_defaults();
    just_outside.join("alice").await;
    just_outside.advance(grace).await;
    just_outside.join("bob").await;
    assert_eq!(
        just_outside.rotations("alice"),
        1,
        "a key that has reached the grace period is replaced for an arrival — {}",
        just_outside.cost(),
    );
}

/// Somebody joins while a rotation is still propagating.
///
/// What is on the wire is the key the rotation replaced, minted before they
/// joined. They get only the pending key, fresh, and decrypt once it comes into
/// use — the rest of a window they had nothing to do with, but never history.
#[tokio::test]
async fn an_arrival_during_a_switch_window_is_not_handed_the_old_key() {
    let mut call = Call::with_defaults();

    call.join("alice").await;
    call.join("bob").await;
    // Long enough that carol's arrival rotates, opening a switch window.
    call.advance(30_000).await;
    call.join("carol").await;

    // Dave arrives while that rotation is still waiting to come into use.
    call.advance(1_000).await;
    call.join("dave").await;

    let (index, key) = call
        .peer("alice")
        .media
        .sending_key(call.clock.now())
        .expect("alice is sending");
    assert!(
        !call
            .peer("dave")
            .media
            .can_decrypt(&call.peer("alice").identity(), index, &key),
        "dave holds alice's key from before he joined",
    );
    call.assert_media_flows_once_settled("dave joined mid-rotation")
        .await;
}

/// A long, quiet call still replaces its key.
///
/// Nothing in the joined memberships asks for a rotation, so without a lifetime cap one key
/// would encrypt the whole meeting — and anything that later recovered it would
/// recover all of it. The cap bounds that to `max_key_lifetime_ms`, at one rotation
/// per period.
#[tokio::test]
async fn a_key_expires_even_when_nothing_happens() {
    let lifetime = EncryptionConfig::default().max_key_lifetime_ms;
    let mut call = Call::with_defaults();

    call.join("alice").await;
    call.join("bob").await;
    call.join("carol").await;
    let settled = call.total_rotations();

    // Four hours of conversation and not one membership change.
    for _ in 0..4 {
        call.advance(60 * 60 * 1000).await;
        call.assert_media_flows("a long call in progress");
    }

    let rotations = call.total_rotations() - settled;
    let expected = 3 * (4 * 60 * 60 * 1000 / lifetime) as usize;
    assert_eq!(
        rotations,
        expected,
        "each of the 3 members should replace its key once per {lifetime}ms — {}",
        call.cost(),
    );
}

/// A call whose joined memberships never stop changing still spends only one key per grace
/// period.
///
/// This is not a separate rate limit; it falls out of anchoring the deferral to the
/// key's own age. Each rotation mints a key that is fresh for the grace period, and
/// every departure inside that window is answered by the rotation which closes it.
#[tokio::test]
async fn churn_costs_one_rotation_per_grace_period() {
    let grace = EncryptionConfig::default().key_rotation_grace_period_ms;
    let mut call = Call::with_defaults();

    let names = [
        "alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi",
    ];
    for name in names {
        call.join(name).await;
    }
    call.advance(30_000).await;
    let before_departures = call.rotations("alice");

    // Six departures, one a second. The first rotates because the call had
    // settled; the rest land while that key is fresh.
    for name in &names[1..7] {
        call.leave(name).await;
        call.advance(1_000).await;
    }

    assert_eq!(
        call.rotations("alice") - before_departures,
        1,
        "six departures spread over six seconds, one key so far: the first rotated, the rest \
         are owed against the end of its freshness — {}",
        call.cost(),
    );

    // Freshness ends, and the rotation that closes the window answers all of them.
    call.advance(grace).await;
    assert_eq!(
        call.rotations("alice") - before_departures,
        2,
        "the owed rotation must happen when freshness ends — {}",
        call.cost(),
    );
    call.advance(EncryptionConfig::default().delay_before_use_ms)
        .await;
    call.assert_media_flows("a paced call after a run of departures");

    let now = call.clock.now();
    for departed in &names[1..7] {
        let alice = call.peer("alice");
        let (index, key) = alice
            .media
            .sending_key(now)
            .expect("alice has a usable key");
        assert!(
            !call
                .peer(departed)
                .media
                .can_decrypt(&alice.identity(), index, &key),
            "{departed} left, but alice is still encrypting with index {index}, which they \
             hold — {}",
            call.cost(),
        );
    }
}

/// An arrival inside a switch window is handed the pending key, fresh, and no
/// rotation is left owed for them once the window closes: what they hold is
/// what the call uses from then on.
#[tokio::test]
async fn an_arrival_inside_a_window_leaves_no_rotation_owed() {
    let delay = EncryptionConfig::default().delay_before_use_ms;
    let mut call = Call::with_defaults();

    call.join("alice").await;
    call.join("bob").await;
    call.advance(30_000).await;
    // Carol's arrival rotates and opens a window.
    call.join("carol").await;
    let after_carol = call.rotations("alice");

    // Dave arrives inside it, and is handed the pending key.
    call.advance(1_000).await;
    call.join("dave").await;

    // The window closes. Dave holds what the call is using, so nothing is owed.
    call.advance(delay).await;
    assert_eq!(
        call.rotations("alice"),
        after_carol,
        "an arrival handed the pending key must not cost a further rotation when the \
         window closes — {}",
        call.cost(),
    );
    call.assert_media_flows("the window closed with dave in the call");
}

/// A key message the homeserver refuses is retried — with the same key.
///
/// The member is not recorded as holding it, so the next rollout sees them as
/// newly arrived, which is what makes the retry happen. Minting a fresh key for
/// it would not be: nobody's confidentiality changed because a send failed.
///
/// The grace period is set long enough here that the age rule cannot fire, so a
/// rotation could only come from the retry itself.
#[tokio::test]
async fn a_retry_after_a_failed_delivery_reuses_the_key() {
    let mut call = Call::new(EncryptionConfig {
        key_rotation_grace_period_ms: 60 * 60 * 1_000,
        ..EncryptionConfig::default()
    });

    call.join("alice").await;
    call.block_device("bob");
    call.join("bob").await;

    assert!(
        !call
            .peer("bob")
            .media
            .holds_anything_from(&call.peer("alice").identity()),
        "sanity check on the scenario: bob was unreachable, so alice's key never got to him",
    );

    // The rate limit clears, and something changes the joined memberships again.
    call.unblock_device("bob");
    call.advance(30_000).await;
    call.join("carol").await;

    call.assert_media_flows_once_settled("bob's delivery was retried")
        .await;
    assert_eq!(
        call.rotations("alice"),
        0,
        "a redelivery to a member who never received the key is a re-send, not a re-key — {}",
        call.cost(),
    );
}

/// A member the homeserver never accepts a key for must not make the call more
/// expensive for everybody else.
///
/// They are never recorded as holding the key, so they stay in the "newly
/// arrived" set for good, and every later rollout considers them again. The
/// comparison is against the identical scenario with a reachable device, so what
/// is measured is only the surcharge for the unreachable one.
#[tokio::test]
async fn an_unreachable_member_adds_no_rotations() {
    async fn run(block_bob: bool) -> (usize, u64) {
        let mut call = Call::with_defaults();
        call.join("alice").await;
        if block_bob {
            call.block_device("bob");
        }
        call.join("bob").await;

        // Membership churn a real call has plenty of, each move well outside the
        // grace period.
        for _ in 0..3 {
            call.advance(30_000).await;
            call.join("carol").await;
            call.advance(30_000).await;
            call.leave("carol").await;
        }

        (call.total_rotations(), call.key_sends())
    }

    let (baseline_rotations, baseline_sends) = run(false).await;
    let (rotations, sends) = run(true).await;

    // A ceiling rather than an equality: the unreachable member legitimately
    // makes the call *cheaper* in places, because nobody ends up holding the key
    // we would otherwise have to keep alive while rotating away from it. What
    // must not happen is a surcharge.
    assert!(
        rotations <= baseline_rotations,
        "an unreachable member cost {} extra key(s), which could not reach them either \
         ({rotations} vs {baseline_rotations} reachable)",
        rotations - baseline_rotations,
    );
    assert!(
        sends <= baseline_sends,
        "an unreachable member cost {} extra to-device send(s) ({sends} vs {baseline_sends} \
         reachable)",
        sends - baseline_sends,
    );
}

/// A member whose client dies and comes straight back.
///
/// MSC4143 mints a fresh `member_id` per join, so this reads as one departure
/// and one arrival of the same device. It is one interruption and should cost
/// one key.
#[tokio::test]
async fn a_reconnecting_member_costs_one_rotation() {
    let mut call = Call::with_defaults();

    call.join("alice").await;
    call.join("bob").await;
    call.join("carol").await;
    call.advance(30_000).await;

    call.leave("carol").await;
    call.advance(200).await;
    call.join("carol").await;

    call.assert_media_flows_once_settled("carol reconnected")
        .await;
    assert_eq!(
        call.rotations("alice"),
        1,
        "one reconnection is one interruption — {}",
        call.cost(),
    );
}

/// Eight people arriving over four minutes, one at a time.
///
/// The shape of the "over-rotating" reports, and the reason the grace period is
/// the dial that matters: nothing unusual happens, everybody is well-behaved and
/// well-connected, every arrival is spaced far enough apart to miss the grace
/// period, and the call still spends 28 keys and triple the necessary traffic.
///
/// Scaling is the point — the same policy over a longer meeting with more
/// arrivals costs proportionally more, so this is the number to watch if the
/// grace period is ever widened.
#[tokio::test]
async fn a_long_call_with_a_steady_trickle_of_arrivals() {
    let mut call = Call::with_defaults();
    let names = [
        "alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi",
    ];

    call.join(names[0]).await;
    for name in &names[1..] {
        call.advance(30_000).await;
        call.join(name).await;
    }

    call.assert_media_flows_once_settled("eight arrivals over four minutes")
        .await;
    assert_eq!(
        call.total_rotations(),
        28,
        "seven arrivals, each re-keying everyone already present — {}",
        call.cost(),
    );
    assert_eq!(
        call.key_sends(),
        168,
        "against a floor of {} for a call of eight — {}",
        ideal_sends(8),
        call.cost(),
    );
}

// ===========================================================================
// The account behind KEY_ROTATION.md
// ===========================================================================

/// Every figure quoted in `KEY_ROTATION.md`, with the rotations that make it up.
///
/// The totals are asserted here so the document cannot drift from the policy, and
/// the breakdown is printed so it can be regenerated rather than transcribed by
/// hand:
///
/// ```sh
/// cargo test -p matrix-rtc-core --test key_rotation documented_costs -- --nocapture
/// ```
///
/// `alice#2 x3` reads as "alice sent key index 2 to three peers" — one rotation of
/// hers, broadcast. A step with no sends is a change the policy absorbed.
#[tokio::test]
async fn documented_costs() {
    let grace = EncryptionConfig::default().key_rotation_grace_period_ms;
    let lifetime = EncryptionConfig::default().max_key_lifetime_ms;
    let five = ["alice", "bob", "carol", "dave", "erin"];
    let eight = [
        "alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi",
    ];

    // -- 8 join an empty call, 1s apart -------------------------------------
    println!("\n## 8 join an empty call, 1s apart");
    let mut call = Call::with_defaults();
    call.join(eight[0]).await;
    for name in &eight[1..] {
        call.advance(1_000).await;
        let mark = call.mark();
        call.join(name).await;
        println!("  {}", call.describe_since(mark, &format!("{name} joins")));
    }
    println!("  total: {}", call.cost());
    assert_eq!(
        (
            call.keys_minted_total(),
            call.total_rotations(),
            call.key_sends()
        ),
        (8, 0, 56)
    );

    // -- 8 join, 30s apart --------------------------------------------------
    println!("\n## 8 join, 30s apart (each into a settled call)");
    let mut call = Call::with_defaults();
    call.join(eight[0]).await;
    for name in &eight[1..] {
        call.advance(30_000).await;
        let mark = call.mark();
        call.join(name).await;
        println!("  {}", call.describe_since(mark, &format!("{name} joins")));
    }
    println!("  total: {}", call.cost());
    assert_eq!(
        (
            call.keys_minted_total(),
            call.total_rotations(),
            call.key_sends()
        ),
        (36, 28, 168)
    );

    // -- 3 leave in one membership change ---------------------------------------
    println!("\n## 5-member call, 3 leave in one membership change");
    let mut call = Call::with_defaults();
    for name in five {
        call.join(name).await;
    }
    call.advance(30_000).await;
    let (mark, before) = (call.mark(), call.key_sends());
    let (rotations, keys) = (call.rotations("alice"), call.keys_minted_total());
    call.leave_together(&["bob", "carol", "dave"]).await;
    println!(
        "  {}",
        call.describe_since(mark, "bob, carol and dave leave together")
    );
    assert_eq!(
        (
            call.keys_minted_total() - keys,
            call.rotations("alice") - rotations,
            call.key_sends() - before
        ),
        (2, 1, 2)
    );

    // -- 3 leave 500ms apart, no grace at all -------------------------------
    //
    // The counterfactual the document compares against, as a configuration rather
    // than a memory: with no freshness a key is never young, so every change
    // rotates the instant it arrives.
    println!("\n## 5-member call, 3 leave 500ms apart, no grace");
    let mut call = Call::new(EncryptionConfig {
        delay_before_use_ms: 0,
        key_rotation_grace_period_ms: 0,
        ..EncryptionConfig::default()
    });
    for name in five {
        call.join(name).await;
    }
    call.advance(30_000).await;
    let (start, before, rotations, keys) = (
        call.mark(),
        call.key_sends(),
        call.rotations("alice"),
        call.keys_minted_total(),
    );
    for name in ["bob", "carol", "dave"] {
        let mark = call.mark();
        call.leave(name).await;
        println!("  {}", call.describe_since(mark, &format!("{name} leaves")));
        call.advance(500).await;
    }
    println!("  total: {}", call.describe_since(start, "the burst"));
    assert_eq!(
        (
            call.keys_minted_total() - keys,
            call.rotations("alice") - rotations,
            call.key_sends() - before
        ),
        (9, 3, 20)
    );

    // -- the same, with the grace period ------------------------------------
    println!("\n## 5-member call, 3 leave 500ms apart, with grace");
    let mut call = Call::with_defaults();
    for name in five {
        call.join(name).await;
    }
    call.advance(30_000).await;
    let (start, before, rotations, keys) = (
        call.mark(),
        call.key_sends(),
        call.rotations("alice"),
        call.keys_minted_total(),
    );
    for name in ["bob", "carol", "dave"] {
        let mark = call.mark();
        call.leave(name).await;
        println!("  {}", call.describe_since(mark, &format!("{name} leaves")));
        call.advance(500).await;
    }
    let mark = call.mark();
    call.advance(grace).await;
    println!(
        "  {}",
        call.describe_since(mark, "the key expires, answering all three")
    );
    println!("  total: {}", call.describe_since(start, "the burst"));
    assert_eq!(
        (
            call.keys_minted_total() - keys,
            call.rotations("alice") - rotations,
            call.key_sends() - before
        ),
        (6, 2, 14)
    );

    // -- a long, quiet call --------------------------------------------------
    println!("\n## 3-member call, 4 hours, no membership change");
    let mut call = Call::with_defaults();
    for name in &five[..3] {
        call.join(name).await;
    }
    let (settled, start, before) = (call.total_rotations(), call.mark(), call.key_sends());
    for hour in 1..=4 {
        let mark = call.mark();
        call.advance(60 * 60 * 1000).await;
        println!("  {}", call.describe_since(mark, &format!("hour {hour}")));
    }
    println!("  total: {}", call.describe_since(start, "four hours"));
    assert_eq!(
        (call.total_rotations() - settled, call.key_sends() - before),
        (3 * (4 * 60 * 60 * 1000 / lifetime) as usize, 12)
    );
}

// ===========================================================================
// Properties
// ===========================================================================

/// The policy under arbitrary interleavings of solo and batch arrivals,
/// departures, redials, delivery failures and time, every participant running
/// its own manager. The scenarios above pin costs; these pin what must hold
/// whatever happens, checked after every step:
///
/// 1. **Nobody left out** — every member present can decrypt what every other
///    member present is encrypting with, or already holds the key that member
///    switches to within `delayBeforeUse` (someone reached during a switch
///    window waits for it rather than being handed the old key, see 7); a
///    device refusing deliveries is exempt until a retry after it stops.
/// 2. **Leavers locked out** — `freshness + delayBeforeUse` after a
///    participation left, nothing anyone is encrypting with is a key it holds.
/// 3. **Nothing sent to a device with no participation in the call.**
/// 4. **At most one key per freshness period** — a member's consecutive keys
///    are minted at least `max(grace, delayBeforeUse)` apart, so an arrival
///    never forces a rotation on a fresh key.
/// 5. **Indexes count up** — a participation mints 0, 1, 2, … and never reuses one.
/// 6. **Nothing owed to a leaver** — no undelivered key is kept for someone
///    who left.
/// 7. **No history** — a participation never holds anyone's key minted more
///    than `max(grace, delayBeforeUse)` before it joined: the grace is the one
///    window MSC4143 lets an arrival read back over.
#[cfg(feature = "testing")]
mod properties {
    use std::sync::Arc;

    use proptest::prelude::*;

    use super::*;

    const USERS: [&str; 6] = ["alice", "bob", "carol", "dave", "erin", "frank"];
    const GRACE_MS: u64 = 10_000;
    const DELAY_MS: u64 = 5_000;
    const LIFETIME_MS: u64 = 60_000;
    const FRESHNESS_MS: u64 = if GRACE_MS > DELAY_MS {
        GRACE_MS
    } else {
        DELAY_MS
    };

    #[derive(Clone, Debug)]
    enum Step {
        Join(usize),
        JoinTogether(Vec<usize>),
        Leave(usize),
        LeaveTogether(Vec<usize>),
        Rejoin(usize),
        Block(usize),
        Unblock(usize),
        Advance(u64),
    }

    fn user() -> impl Strategy<Value = usize> {
        0..USERS.len()
    }

    fn users() -> impl Strategy<Value = Vec<usize>> {
        proptest::collection::btree_set(user(), 2..4).prop_map(|set| set.into_iter().collect())
    }

    fn step() -> impl Strategy<Value = Step> {
        prop_oneof![
            4 => user().prop_map(Step::Join),
            2 => users().prop_map(Step::JoinTogether),
            3 => user().prop_map(Step::Leave),
            2 => users().prop_map(Step::LeaveTogether),
            1 => user().prop_map(Step::Rejoin),
            1 => user().prop_map(Step::Block),
            2 => user().prop_map(Step::Unblock),
            // Mostly around the grace and the delay; sometimes past a lifetime.
            5 => (0u64..15_000).prop_map(Step::Advance),
            1 => (LIFETIME_MS..2 * LIFETIME_MS).prop_map(Step::Advance),
        ]
    }

    /// A participation that left, and what its media layer was given.
    struct Leaver {
        name: &'static str,
        member_id: String,
        media: Arc<MediaSim>,
        left_at_ms: u64,
    }

    /// The run's bookkeeping beside the call.
    #[derive(Default)]
    struct Model {
        leavers: Vec<Leaver>,
        /// When each participation joined, by `member_id`.
        joined_at: HashMap<String, u64>,
        /// Refusing deliveries now.
        blocked: HashSet<&'static str>,
        /// Unblocked, but not yet past an `advance` that retries what they
        /// are owed.
        recovering: HashSet<&'static str>,
    }

    fn in_call(call: &Call, name: &str) -> bool {
        call.peers
            .iter()
            .any(|peer| peer.name == name && peer.in_call)
    }

    fn record_leavers(call: &Call, model: &mut Model, names: &[&'static str]) {
        for name in names {
            let peer = call.peer(name);
            model.leavers.push(Leaver {
                name,
                member_id: peer.member_id.clone(),
                media: peer.media.clone(),
                left_at_ms: call.clock.now(),
            });
        }
    }

    /// Time passes the way the session's upkeep sees it: every deferred
    /// rotation is flushed at its own deadline, not at the end of the jump,
    /// and every key a member is owed is retried at each wake-up.
    async fn run_for(call: &Call, model: &mut Model, ms: u64) {
        let target = call.clock.now() + ms;
        loop {
            let now = call.clock.now();
            let next_due = call
                .peers
                .iter()
                .filter(|peer| peer.in_call)
                .filter_map(|peer| peer.manager.rotation_due_at_ms())
                .filter(|due| *due > now && *due <= target)
                .min();
            let Some(due) = next_due else {
                break;
            };
            call.clock.set(due);
            wake(call).await;
        }
        call.clock.set(target);
        wake(call).await;
        model.recovering.clear();
    }

    async fn wake(call: &Call) {
        for peer in call.peers.iter().filter(|peer| peer.in_call) {
            peer.manager
                .flush_due_rotation()
                .await
                .expect("a deferred rotation should not fail");
            for member_id in peer.manager.undelivered_members() {
                peer.manager
                    .retry_undelivered_key_now(&member_id)
                    .await
                    .expect("a retry should not fail");
            }
        }
        call.deliver_all().await;
    }

    fn check(call: &Call, model: &Model, sends_before: usize, after: &Step) {
        let now = call.clock.now();
        let exempt = |name: &str| model.blocked.contains(name) || model.recovering.contains(name);

        // 1. Nobody left out.
        for sender in call.peers.iter().filter(|peer| peer.in_call) {
            let (index, key) = sender
                .media
                .sending_key(now)
                .unwrap_or_else(|| panic!("after {after:?}: {} has no key", sender.name));
            let switching_to: Vec<Install> = sender
                .media
                .installs
                .lock()
                .unwrap()
                .iter()
                .filter(|install| {
                    install.identity == sender.media.own_identity
                        && install.usable_at_ms > now
                        && install.usable_at_ms <= now + DELAY_MS
                })
                .cloned()
                .collect();
            for receiver in call
                .peers
                .iter()
                .filter(|peer| peer.in_call && peer.name != sender.name && !exempt(&peer.name))
            {
                let holds_next = switching_to.iter().any(|next| {
                    receiver
                        .media
                        .can_decrypt(&sender.identity(), next.index, &next.key)
                });
                assert!(
                    holds_next || receiver.media.can_decrypt(&sender.identity(), index, &key),
                    "after {after:?}: {} cannot decrypt {} (index {index}) nor holds what it \
                     switches to; {}",
                    receiver.name,
                    sender.name,
                    call.cost(),
                );
            }
        }

        // 7. No history.
        let minted_at = |identity: &str, index: u8, key: &[u8]| {
            call.peers
                .iter()
                .map(|peer| &peer.media)
                .chain(model.leavers.iter().map(|leaver| &leaver.media))
                .flat_map(|media| {
                    media
                        .installs
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|install| {
                            install.identity == media.own_identity
                                && install.identity == identity
                                && install.index == index
                                && install.key == key
                        })
                        .map(|install| install.signalled_at_ms)
                        .collect::<Vec<_>>()
                })
                .min()
        };
        for receiver in call.peers.iter().filter(|peer| peer.in_call) {
            let joined = model.joined_at[&receiver.member_id];
            let holdings = receiver.media.installs.lock().unwrap().clone();
            for held in &holdings {
                if held.identity == receiver.media.own_identity {
                    continue;
                }
                let Some(minted) = minted_at(&held.identity, held.index, &held.key) else {
                    continue;
                };
                assert!(
                    minted + FRESHNESS_MS >= joined,
                    "after {after:?}: {} ({}) joined at +{} ms and holds {}'s index {}, \
                     minted at +{} ms",
                    receiver.name,
                    receiver.member_id,
                    joined - EPOCH,
                    held.identity,
                    held.index,
                    minted - EPOCH,
                );
            }
        }

        // 2. Leavers locked out.
        for leaver in &model.leavers {
            if now < leaver.left_at_ms + FRESHNESS_MS + DELAY_MS {
                continue;
            }
            for sender in call.peers.iter().filter(|peer| peer.in_call) {
                let Some((index, key)) = sender.media.sending_key(now) else {
                    continue;
                };
                assert!(
                    !leaver.media.can_decrypt(&sender.identity(), index, &key),
                    "after {after:?}: {} ({}), gone since {} ms, still decrypts {} (index {index})",
                    leaver.name,
                    leaver.member_id,
                    now - leaver.left_at_ms,
                    sender.name,
                );
            }
        }

        // 3. Nothing sent to a device with no participation in the call.
        for send in call.bus.log.lock().unwrap().iter().skip(sends_before) {
            assert!(
                call.peers.iter().any(|peer| {
                    peer.in_call
                        && peer.user_id == send.to.user_id
                        && peer.device_id == send.to.device_id
                }),
                "after {after:?}: {}'s key {} went to {} {}, not in the call",
                send.from_member_id,
                send.index,
                send.to.user_id,
                send.to.device_id,
            );
        }

        for peer in call.peers.iter().filter(|peer| peer.in_call) {
            let own: Vec<Install> = peer
                .media
                .installs
                .lock()
                .unwrap()
                .iter()
                .filter(|install| install.identity == peer.media.own_identity)
                .cloned()
                .collect();
            // 4. At most one key per freshness period.
            for pair in own.windows(2) {
                let gap = pair[1].signalled_at_ms - pair[0].signalled_at_ms;
                assert!(
                    gap >= FRESHNESS_MS,
                    "after {after:?}: {} minted index {} {gap} ms after index {}",
                    peer.name,
                    pair[1].index,
                    pair[0].index,
                );
            }
            // 5. Indexes count up.
            let indexes: Vec<u8> = own.iter().map(|install| install.index).collect();
            let expected: Vec<u8> = (0..indexes.len()).map(|n| n as u8).collect();
            assert_eq!(indexes, expected, "after {after:?}: {}", peer.name);
            // 6. Nothing owed to a leaver.
            for owed in peer.manager.undelivered_members() {
                assert!(
                    call.peers
                        .iter()
                        .any(|other| other.in_call && other.member_id == owed),
                    "after {after:?}: {} still owes {owed}, who left",
                    peer.name,
                );
            }
        }
    }

    async fn run(steps: Vec<Step>) {
        let mut call = Call::new(EncryptionConfig {
            delay_before_use_ms: DELAY_MS,
            key_rotation_grace_period_ms: GRACE_MS,
            max_key_lifetime_ms: LIFETIME_MS,
            ..EncryptionConfig::default()
        });
        let mut model = Model::default();
        for step in steps
            .iter()
            .cloned()
            .chain([Step::Advance(FRESHNESS_MS + DELAY_MS)])
        {
            let sends_before = call.mark();
            match &step {
                Step::Join(user) => {
                    let name = USERS[*user];
                    if !in_call(&call, name) {
                        call.join(name).await;
                    }
                }
                Step::JoinTogether(users) => {
                    let names: Vec<&str> = users
                        .iter()
                        .map(|user| USERS[*user])
                        .filter(|name| !in_call(&call, name))
                        .collect();
                    if !names.is_empty() {
                        call.join_together(&names).await;
                    }
                }
                Step::Leave(user) => {
                    let name = USERS[*user];
                    if in_call(&call, name) {
                        record_leavers(&call, &mut model, &[name]);
                        call.leave(name).await;
                    }
                }
                Step::LeaveTogether(users) => {
                    let names: Vec<&'static str> = users
                        .iter()
                        .map(|user| USERS[*user])
                        .filter(|name| in_call(&call, name))
                        .collect();
                    if !names.is_empty() {
                        record_leavers(&call, &mut model, &names);
                        call.leave_together(&names).await;
                    }
                }
                Step::Rejoin(user) => {
                    let name = USERS[*user];
                    if in_call(&call, name) {
                        record_leavers(&call, &mut model, &[name]);
                        call.leave(name).await;
                        call.join(name).await;
                    }
                }
                Step::Block(user) => {
                    let name = USERS[*user];
                    call.block_device(name);
                    model.blocked.insert(name);
                    model.recovering.remove(name);
                }
                Step::Unblock(user) => {
                    let name = USERS[*user];
                    call.unblock_device(name);
                    if model.blocked.remove(name) {
                        model.recovering.insert(name);
                    }
                }
                Step::Advance(ms) => run_for(&call, &mut model, *ms).await,
            }
            for peer in call.peers.iter().filter(|peer| peer.in_call) {
                model
                    .joined_at
                    .entry(peer.member_id.clone())
                    .or_insert(call.clock.now());
            }
            check(&call, &model, sends_before, &step);
        }
    }

    /// Found by the property: a member whose device refused our key is reached
    /// on retry inside a switch window, while we still encrypt with the
    /// previous key. The retry hands over only the pending key — the previous
    /// one was minted before they could receive anything — so they decrypt
    /// once it comes into use.
    #[tokio::test]
    async fn a_member_reached_on_retry_inside_a_switch_window_waits_for_the_switch() {
        let mut call = Call::new(EncryptionConfig {
            delay_before_use_ms: DELAY_MS,
            key_rotation_grace_period_ms: GRACE_MS,
            max_key_lifetime_ms: LIFETIME_MS,
            ..EncryptionConfig::default()
        });
        let mut model = Model::default();
        call.block_device("bob");
        call.join_together(&["bob", "carol"]).await;
        call.join("alice").await;
        run_for(&call, &mut model, GRACE_MS).await;
        // Not fresh any more: dave's arrival rotates everyone, and the new keys
        // wait out delayBeforeUse.
        call.join("dave").await;
        call.unblock_device("bob");
        run_for(&call, &mut model, 0).await;
        let (index, key) = call
            .peer("alice")
            .media
            .sending_key(call.clock.now())
            .expect("alice is sending");
        assert!(
            !call
                .peer("bob")
                .media
                .can_decrypt(&call.peer("alice").identity(), index, &key),
            "bob was handed alice's key from before he could receive it",
        );
        run_for(&call, &mut model, DELAY_MS).await;
        call.assert_media_flows("bob, once the switch window closed");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn key_distribution_holds_whatever_the_roster_does(
            steps in proptest::collection::vec(step(), 1..40),
        ) {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(run(steps));
        }
    }
}
