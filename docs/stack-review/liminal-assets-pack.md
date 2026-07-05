# liminal — Assets Pack (pipeline toolkit)

*What a reviewer, verifier, or implementer agent needs to do liminal work
well. Written for dispatch through the norn/aion pipeline. Companion to
`liminal-ledger.md`.*

## 1. Domain-specific review prompts

**Actor-pattern review** (any change to conversation/, channel/, routing/):
- Link-before-forward: is every new send path preceded by an established
  link (or registered exit notifier) — never the reverse order?
- Is the register/signal race closed under one state lock (exactly-one-
  notification)? Point to the lock.
- Payload discipline: term payloads must NOT travel through local beamr
  mailboxes — data rides shared queues, mailboxes carry wakeup atoms.
  (Exception: remote wire frames as binaries.) Flag violations.
- If a queue push can race a dead pid: is there rollback (the
  `ParticipantChannel::forward` pop-back pattern)?
- Scheduler ownership: does the change assume its own scheduler or accept
  a shared one? Cross-node delivery requires subscribers on the
  distribution-owning scheduler — reject changes that silently re-pin.

**Durability review** (durability/ or anything touching persistence):
- OCC discipline: every append carries `expected_seq`; conflicts surface as
  `SequenceConflict`. Which paths auto-retry (dedup/cursor converge) vs
  surface (conversation/channel)? Preserve that split.
- No partial-persist: in-memory state advances ONLY after successful
  append. Verify ordering in the diff, not the description.
- Cursor contract: "absent == 0", never store physical zero, checkpoints
  refuse regression. Any new cursor-like key follows the same rules.
- Dedup: `release_claim` must never clobber a stored receipt (at-most-once
  guard). Tombstones, not deletes.
- The sync bridge: does the change add a store implementation that might
  not complete on first poll? That breaks every durable path — escalate to
  the async-bridge item (ledger C1), do not merge around it.

**Protocol review** (protocol/, SDK transports, server framing):
- Control frames on stream 0 only; application frames ≥1; `validate()`
  coverage for any new frame type; stable discriminants, never reused.
- Flag bits ride the existing flags byte — no wire-format breaks; unknown
  frame types must remain length-skippable (forward compat).
- Version negotiation touched? Both sync and async paths, and the SDK's
  pinned range.
- Any new id on the wire: negotiated or derived? (FNV-1a name hashing is
  the current derived pattern — flag additions that deepen the collision
  surface.)

**Cluster review**:
- Nothing may register beamr's connection-down hook (single slot, owned by
  pg-purge) until the multi-subscriber hook lands — membership changes
  poll.
- Cross-node sends: external-pid encoding correct? (`Term::try_pid` range
  skips silently drop today — don't extend that pattern.)
- Runtime ownership: new tokio use must not spin per-operation runtimes
  and must drop runtimes on a std::thread (the established
  async-drop-panic workaround).

## 2. Verification methodology

- **Workspace lints are law**: `unsafe_code = "deny"`,
  `unwrap/expect/panic = "deny"` workspace-wide. 500-LOC file limit, **200
  for mod.rs/lib.rs/main.rs** (stricter than beamr — don't import that
  habit).
- **3-way conformance harness** (rust/gleam/typescript,
  `tests/conformance/`, compare.py): any change to connection lifecycle,
  recovery, pressure vocabulary, or conversation lifecycle MUST update
  `scenarios.json` + all three harnesses together. A scenario added to one
  SDK only is a review reject.
- **Crash-link work**: kill-9 the participant process and assert the
  survivor learns via link (not timeout) — the existing conversation tests
  are the template. Timing assertions use the crash-observed `Instant`
  captured inside the link handler, not wall-clock guesses.
- **Cluster work**: two-node e2e with real beamr distribution (the
  services_r5 test shape); simultaneous-dial and node-down-purge cases
  explicitly. Load-test the queue-full / frame-drop path — it is currently
  untested by anyone's admission.
- **External-service tests**: runtime `*_TEST_URL` env gates, never
  `#[ignore]` (CONVENTIONS.toml).
- **Backpressure work (when A1 lands)**: assertions on Defer emission
  under real slow-subscriber load, not on the decision function alone —
  "asserted, not assumed" is frame's acceptance language; adopt it here.

## 3. Design documents required before implementation

| Work item | Prerequisite doc |
|---|---|
| Backpressure wiring (A1) | Defer semantics doc: who buffers, retry/escalation rules, interaction with durable channels and dedup |
| Embedded/frame-conv (B1) | Shared-scheduler ownership doc with frame (who constructs, who supervises, shutdown order) |
| Gleam native surface (B2) | API sketch reviewed against ruling #4/#5 outcomes |
| TS transport (B3) | Joint WS framing doc with beamr's browser-transport brief — one design, two consumers |
| Causal v2 (A4) | Ordering-semantics decision doc (one-hop deprecation vs vector clocks vs auto-orderer stage), cross-checked against aion's ordering |
| Async bridge (C1) | Threading model doc, tracked against haematite's async plans |

## 4. Specialized agents worth having

- **liminal-conformance-runner**: executes the 3-way harness + compare.py
  on demand; blocks any lifecycle/pressure/protocol merge without green
  parity.
- **liminal-durability-reviewer**: carries the OCC/cursor/dedup invariants
  above; reads every durability/ diff.
- **wire-protocol-reviewer**: byte-level review of frame changes against
  the stable-discriminant and forward-compat rules; owns the "did you
  update all three SDK codecs" check (the TS SDK compiles the Rust codec —
  keep it that way rather than reimplementing).
- **crash-link-verifier**: runs the kill-9 scenarios for any
  conversation/dispatch change.

## 5. Implementation constraints (standing orders)

1. Workspace denies stay: no unsafe, no unwrap/expect/panic — design APIs
   so errors are values.
2. The wire protocol evolves additively: new frame types get new
   discriminants; flags extend the flags byte; nothing existing changes
   meaning.
3. Every protocol-visible change lands with conformance scenarios in the
   same PR — the harness is the spec.
4. Preserve the two-plane schema model knowingly: hash negotiation at the
   wire, JSON Schema enforcement at the channel — don't "unify" them
   casually; they serve different trust boundaries.
5. Docs tell the truth: status claims carry the dated ⚠️ convention; if a
   checklist can't be kept current, delete it (stale `done:false` ledgers
   cost real orientation time).
6. The `aion.observability.v1` tap pattern (pinned channel constant +
   default-false notifier method) is the approved shape for new
   cross-layer taps — copy it, don't invent.
