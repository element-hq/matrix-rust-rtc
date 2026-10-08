# `chaos` — MatrixRTC under faults

An end-to-end suite that runs real MatrixRTC clients against Synapse +
lk-jwt-service + LiveKit, breaks things around them, and checks the invariants
the stack must hold through the fault and after it: the homeserver restarting
or going away, one participant partitioned or on a lossy link, a participant
dying without a word.

It sits one layer above `crates/matrix-rtc-call-sdk/tests/e2e_call` and reuses
its approach — the client is the real `LiveKitCall` facade (core, matrix-sdk
backend, call SDK and LiveKit transport), with no UI and no browser.
What it adds is **isolation per participant**: each one runs in its own
container, so a network fault can hit one peer and not the other, and a
process can be `SIGKILL`ed without taking the harness with it.

## Pieces

| Path | What it is |
| --- | --- |
| `crates/matrix-rtc-call-sdk/examples/chaos_peer.rs` | The client: one participant, driven over stdin, reporting JSON events on stdout (protocol in its module docs). |
| `docker/compose.yml` | The stack. Synapse (config shared with `demo/backend`), lk-jwt, LiveKit, and the `peer` service the harness runs once per participant. |
| `docker/Dockerfile` | `runtime` (what a peer runs in: ubuntu 24.04 + iptables + tc) and `builder` (for non-Linux hosts). |
| `build-peer.sh` | Builds `chaos_peer` into `chaos/bin/` (native on Linux, in `builder` elsewhere). |
| `src/` | The harness: the stack, peers, faults, and the homeserver probes. Never links the SDK. |
| `tests/scenarios.rs` | The scenarios. |

## Running

```sh
chaos/build-peer.sh        # first time: long (matrix-sdk + libwebrtc); then incremental
cargo test -p matrix-rtc-chaos --test scenarios -- --ignored --test-threads=1 --nocapture
# one scenario:
cargo test -p matrix-rtc-chaos --test scenarios -- --ignored --nocapture --exact state::homeserver_restart
```

The harness brings the stack up itself (`matrix-rtc-chaos` compose project,
Synapse on `localhost:18008`, so it coexists with `make backend-up`) and leaves
it running for the next run. Tear down with
`docker compose -f chaos/docker/compose.yml -p matrix-rtc-chaos down -v`.

Every scenario writes `target/chaos/<scenario>/`: `<peer>.jsonl` (the event
stream), `<peer>.log` (the client's own log, `matrix_rtc_*` at debug), and
`backend.log`. Pass or fail — a green run that logs oddities is how drift gets
spotted.

After changing the client, rerun `build-peer.sh`; the binary is mounted, so no
image rebuild.

## Faults

| Fault | How | Scope |
| --- | --- | --- |
| Homeserver restart / outage | `docker compose restart\|stop\|start synapse` | backend |
| Partition (everything, or the homeserver only) | iptables in the peer's namespace (`CHAOS` chain) | one peer |
| Loss, latency, jitter | `tc netem` in the peer's namespace | one peer |
| Crash | `docker kill --signal KILL` | one peer |

## Invariants

The scenarios combine these; each is checked from the outside, by the peers'
reports or by asking the homeserver.

- **Converges.** Every peer reports the expected member count within a bound
  after the fault clears (`RECOVER`: a keep-alive window, a heartbeat, slack).
- **Stable.** Across a fault the stack is meant to absorb, no peer ever
  reports a different member count, and no call ends.
- **Switch armed.** The dead man's switch a peer reports is known to the
  homeserver, still pending, and was restarted within one heartbeat (+5s) —
  i.e. restarts are landing, not just being attempted. Checked with MSC4140's
  `GET /delayed_events/{delay_id}`, the only lookup the merged MSC specifies
  (listing moved to MSC4486, so nothing here depends on it). Synapse serves it
  from 1.161.
- **Switch fires.** For a crashed peer, the survivor sees it leave within the
  keep-alive (+15s), and its last delay is no longer pending. (MSC4140
  recommends keeping finalised delays around for lookup, with their outcome;
  Synapse deletes them once processed, so "finalised as sent" and "gone" are
  both accepted.)
- **Media continuous.** Every 1s audio window a peer meters carries the
  440 Hz tone — for faults that do not touch the media path (a homeserver
  restart) there must be no gap at all.

## Modes

Every scenario runs once per mode, as `<mode>::<scenario>`. For now there is
one:

- `state` — the generation most Element Call deployments speak:
  `org.matrix.msc3401.call.member` room state, `/sfu/get` tokens, no slots
  (`MembershipFormat::RoomState`). The events it puts on the wire match
  matrix-js-sdk's `MembershipManager` field for field; the timings do not —
  js-sdk arms its delayed leave for 8s and restarts it every 5s, where our
  client uses 30s / 10s. The deadlines here are sized for our client, so a
  pass says nothing about how fast Element Call itself is cleaned up.

Sticky events (MSC4354) are not exercised yet: the spec, Synapse and
matrix-rust-sdk are still moving there. The peer accepts `COMPAT=current` and
`COMPAT=sticky`, so a mode is one line once they settle.

## Rejoining

A client cut off from its homeserver for longer than its delayed leave knows
by itself that its membership is gone: the library follows the delay's
lifecycle locally and ends the call with `MembershipLost`, homeserver or not.
`chaos_peer` then rejoins, as an application would — on an exponential backoff
with jitter, since the homeserver is likely still down, and retiring the old
delay first so it cannot end the new membership when it fires late. It rejoins
after a lost call (`MembershipLost`, `ConnectionClosed`), never after a leave or
a closed slot. Reported as `call_ended` (`reason`, `rejoining`), then `joined`,
and a `rejoins` count in `status`. The policy is meant to move into the call
SDK.

## Scenarios

`backend_capabilities` runs first and in no mode: it checks that the stack is
the one the scenarios assume — both transport shapes advertised, and the C-S
delegation endpoint served (Element Call's probe: anything but 404).

| Scenario | Fault | Expects |
| --- | --- | --- |
| `baseline` | none | stable, switch armed, media continuous |
| `homeserver_restart` | Synapse restart | stable, same delay re-armed, media continuous |
| `homeserver_outage_beyond_keep_alive` | Synapse down > keep-alive | both delays fire; both peers rejoin by themselves and re-arm |
| `killed_peer_is_cleaned_up` | SIGKILL one peer | switch fires; survivor sees the leave within keep-alive |
| `short_partition` | one peer cut off < keep-alive | stable; same delay |
| `long_partition_recovers` | one peer cut off from the HS > keep-alive | the other side sees it leave, then return by itself, re-armed |
| `lossy_network` | 20% loss on one peer's outgoing traffic for 60s | stable; switch never lapses |
| `homeserver_outage_is_bridged_by_retries` | Synapse stopped right after a restart, back 20 s later | the host keeps its membership and delay (retries, denser near the deadline); the guest, on the replaced restart pacing (`KEEP_ALIVE_POLICY=legacy`: a restart every 10 s, no early retries; loss detection stays on), loses its — the retries are what saved the call |
| `homeserver_outage_beyond_two_delays` | Synapse down 75 s | both report the loss while it is down, make no join attempts while offline, then rejoin once it is back |
| `stale_leave_ends_and_rejoins` | the guest leaks a 3 s delayed leave of its own membership (`leak_delayed_leave`), homeserver healthy | when it lands, the guest ends its call (`membership_lost`) — everybody else counts it as left — and rejoins by itself |

### Known failures (the hardening backlog)

None: every scenario passes in `state` mode. Scenarios failing for a known
reason are `#[ignore]`d with that reason until the fix lands.

`homeserver_outage_beyond_keep_alive` and `long_partition_recovers` check that
a peer reports its membership lost *while* it is still cut off. Before the
library followed the delay's lifecycle locally, a peer only found out once the
homeserver was back and sync delivered its own leave — and a client that did
not rejoin was left with a session that believed it was joined, no membership
and no switch.

## Next

- **Delegated delayed leave.** With the SFU holding the switch, a
  homeserver-only partition must *not* end the membership, while losing the
  SFU must. Adds a `delegated` mode.
- **Sticky events**, once the spec and the implementations settle.
- **Federation.** A second, federated homeserver; a peer on it joins while the
  first homeserver is down, forcing a key rotation nobody on the first can
  hear about. Invariants: the local pair keeps decrypting each other
  throughout, and every pair decrypts both ways within a bound once the
  homeserver is back.
- **Toxiproxy** in front of Synapse for slow or broken responses (5xx, resets,
  bandwidth caps), distinct from a clean restart.
- **SFU and lk-jwt restarts**; an observer-only peer for ghost-membership
  checks without relying on a participant's own roster.
