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

When a peer's delayed leave fires while it is cut off, its membership leaves
the call. `chaos_peer` then rejoins by itself, as an application would: when
its own roster entry has vanished while the call has not ended. A call that
ended (left, slot closed, SFU connection gone) is not rejoined. The rejoin is
reported as `rejoining` / `joined` events and a `rejoins` count in `status`.
It is meant to move into the call SDK.

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

### Known failures (the hardening backlog)

None: every scenario passes on `main` in `state` mode. Scenarios failing for a
known reason are `#[ignore]`d with that reason until the fix lands.

One weakness is masked rather than absent. In
`homeserver_outage_beyond_keep_alive` both delayed leaves fire, and both peers
pass only because `chaos_peer` rejoins: the fresh join arms a fresh delay.
Inside the old session, `OwnMembershipMachine::rearm_if_certainly_fired` arms
a replacement *while the homeserver is still down*; that fails, marks delayed
events unsupported, and the next attempt waits
`DELAYED_LEAVE_PROBE_INTERVAL_MS` (5 minutes). A client that does not rejoin
is left with a session that believes it is joined, no membership and no
switch.

## Next

- **Keep-alive policy.** Restarts due at a share of the delay, retried on an
  exponential backoff with jitter, and a re-arm only on hard evidence (the
  homeserver no longer knows the delay) — closes the weakness above for
  clients that do not rejoin, and is what a rejoin inside the SDK builds on.
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
