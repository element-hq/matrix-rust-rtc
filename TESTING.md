# Testing

How the test suite is organised and the techniques it relies on. Run it all
with `make test` (or `cargo test`); `CONTRIBUTING.md` has the full pre-PR
checklist.

## Paused time

Everything time-driven — retries and backoff, reconnect budgets, sticky
refreshes, holds, debounces — goes through `matrix_rtc_core::executor` and is
tested under tokio's paused clock (`#[tokio::test(start_paused = true)]`). A
test that waits sixty seconds of call time runs in microseconds, and timings
are asserted exactly ("the fallback at 10 s", "the leave at 60 s") rather than
with tolerances. A test that needs real sleeps is a bug in a seam.

Tests drive the core through fakes rather than mocks of internals:
`MockBackend` (core `testing` feature) scripts the homeserver per method
(`MockMethod`, `MockOutcome::{Ok, Fail, Hang}`) and logs every call with its
time; transport tests use small fake `MediaTransport`s that record connects
and let the test play the SFU's events.

## Scripted outages

`crates/matrix-rtc-call-sdk/tests/resilience/` runs whole calls the way a host
makes one — `RtcClient` → `RtcRoom` → `join_call` → `attach_media` — over
`MockBackend` (the homeserver, which `set_outage` takes away) and the transport
crate's `ScriptedTransport` (the SFUs, behind its `testing` feature), on paused
time: the homeserver away for minutes and back, a delayed-leave flap, our own
SFU lost with and without a fallback, joining with the SFU down. The per-rule
tests live with their modules; these check that the rules compose. The same
two fakes reach hosts' tests through the FFI's `testing` feature
(`ScriptedMatrixBackend`, `ScriptedMediaTransport`), and what only a real
stack shows is in the `e2e_resilience_*` scenarios
(`crates/matrix-rtc-call-sdk/tests/E2E_CALL.md`).

## Property tests (proptest)

A scenario test checks one sequence someone thought of. A property test states
a rule that must hold for *every* sequence and lets
[proptest](https://docs.rs/proptest) generate hundreds of them to break it.
It is a dev-dependency only; nothing ships with it.

### How it works

1. **Strategies** describe the input space: a range, a vector of some
   type, an enum built with `prop_oneof!`.
2. **The property** runs on each generated input and asserts its invariants.
3. **Shrinking**: when an input fails, proptest simplifies it — shorter
   vectors, smaller numbers, simpler variants — while it still fails, and
   reports the minimal case.
4. **Regressions**: the failing seed is written to
   `proptest-regressions/<module>.txt` next to the crate's `src`. Saved seeds
   are replayed before any new input on every run. Commit these files: they
   are how a found edge case stays covered for everyone, CI included.

### The pattern in this repo

Our properties are about state machines driven over time, and they all share
one shape:

- a `Step` enum of what the outside world can do — a send failing, a
  connection dropping, signalling flipping, a membership set arriving, a wait
  of N seconds;
- a strategy generating a `Vec<Step>`, plus any random script the fakes
  replay (which attempts fail, say);
- a fresh current-thread runtime with paused time per case, the steps applied
  in order, and the invariants checked after each one — either rules ("never
  two attempts in flight") or a small model of the expected state compared
  with the real one.

```rust
proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn a_rule_that_always_holds(steps in proptest::collection::vec(step(), 1..30)) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        runtime.block_on(async {
            for step in steps {
                // apply the step, collect what the system reported,
                // assert the invariants
            }
        });
    }
}
```

### Where they are

| Property | File | What it guards |
|---|---|---|
| `never_two_attempts_of_one_slot_in_flight`, `a_cancelled_send_never_attempts_again` | `crates/matrix-rtc-core/src/outbox/tests.rs` | The outbox under random sends, supersessions, cancels, connectivity reports and waits (034 R1) |
| `hold_properties` | `crates/matrix-rtc-core/src/lib.rs` (`tests::hold`) | The delayed-leave hold against a model of who is present (034 R21) |
| `a_fallback_is_taken_only_on_its_terms` | `crates/matrix-rtc-transport/src/pool/tests.rs` (`fallback`) | The SFU fallback under random losses, reconnect outcomes, peer outages and signalling (034 R25) |
| `key_distribution_holds_whatever_the_roster_does` | `crates/matrix-rtc-core/tests/key_rotation.rs` (`properties`) | Media keys under solo and batch joins, leaves, redials, refused deliveries and time, every participant on its own manager: everyone decrypts everyone (or holds the key they switch to), leavers are locked out, nothing reaches a device not in the call, no one holds a key from before they joined |
| `the_identity_mapping_holds_whatever_arrives_first` | `crates/matrix-rtc-transport/src/pool/tests.rs` (`identity`) | Tracks, keys and SFU participants against membership snapshots in any order: streams reported consistently, every current member's live media subscribed, keys and unmappable identities exact |

### What it found

The fallback property first claimed "our media moves at most once". Within
seconds proptest failed it and shrank the failure to five steps:

```
OwnClosed, Wait(10), PeerReconnecting, Wait(6), Wait(4)
```

We fall back to SFU-B at 10 s; SFU-B then drops too, and ten seconds later
our media falls back again, to SFU-C. That is legitimate — the spec only rules
out moving back to our own SFU — so the invariant was corrected, not the code,
and the seed stays in `proptest-regressions/` so that path is replayed on
every run. The hand-written scenarios had never combined those events.

The same mechanism catches real bugs, and two of the properties above did on
their first deep run:

- **Key distribution** shrank to `JoinTogether(alice, bob), Advance(10 001 ms),
  Join(carol)`: carol's arrival rotated everyone, and she was handed the key
  still in use during `delayBeforeUse` — minted before she joined, so its
  whole history came with it. Newcomers now get only the new key and decrypt
  once it is used (`KEY_ROTATION.md`, "Exposure, stated plainly").
- **Identity mapping** shrank to a member publishing, their membership going
  and coming back under the same member id: the pool had dropped their tracks,
  the SFU never re-announces them, so they returned with no media — what every
  peer would see after we republish our membership following an outage. A
  departed member's tracks are now buffered like media that beats a
  membership.

### Running and writing them

- They run with `cargo test` like any test. Each pins its case count in
  `proptest_config`; raise it there for a deeper local run (the
  `PROPTEST_CASES` variable does not override an explicit config).
- A failure prints the minimal input. Fix the code or the invariant, rerun,
  and commit the regressions file with the fix.
- Write properties as *rules*, not expected traces: "only while signalling is
  reachable", "never back to our own focus", "present exactly when the model
  says so". Exact timings and payloads belong in scenario tests next to them —
  the two complement each other.
- Keep each case cheap (paused time, no real I/O) so 64–256 cases cost well
  under a second.
