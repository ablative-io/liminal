# SDK participant receive — symmetric conversation participation (dispatch brief)

**Status:** DRAFT for pair tear (Waffles the Terrible), then GATED dispatch —
see §Gate. **Lane:** Cally Ray (boundary pair-agreed 2026-07-12, recorded in
PARK-FLIP-D1-ACTIVATION.md §0: the SDK receive path is this lane;
`liminal-server/src/server/connection/process.rs` is Vesper Lynd's seam,
untouched here). **Reviewer of record:** Waffles the Terrible; **domain-owner
pass:** Hermes Crumpet (standing agreement, 2026-07-12). **Base:** liminal
main @ `68379e8` — the push-reply deadline-on-push merge; every surface this
brief cites was verified present at that commit.

## Gate (named placeholder — this brief is NOT dispatchable until filled)

**`«LIMINAL-BASE-VERSION: unfilled»`** — fill condition: Tom publishes
liminal **0.2.4** (main @ 68379e8 is publish-ready; the trigger is live on
his list). This work is new SDK surface and belongs to the NEXT version
line, not to the fix release: 0.2.4 ships the G7 contract restoration alone,
so its changelog stays one honest sentence, and this leg dispatches against
the published 0.2.4 as its floor. The placeholder is filled with the version
string verified resolving from the registry (anonymous fetch — the
publish-before-pin law), and only then does this brief dispatch. A brief
with the placeholder unfilled is half-armed by design and must not launch.

To be read plainly: **this gate is a SEQUENCING gate wearing a dependency
costume.** The worker builds in this repo on main and nothing here consumes
the crates.io artifact — the gate's real function is release hygiene (0.2.4
exists ⟹ this surface belongs to the next line and 0.2.4's changelog stays
one honest sentence). Nobody should later read "floor" as a build
constraint and go hunting for the pin.

**Deferred obligation (routing freeze, ruled at tear 2026-07-13):** the
liminal domain-owner pass on the worker's diff stands as agreed but its
seat is frozen; the reviewer-of-record's review lands the worker's tree as
**REVIEWED-NOT-MERGED**, and the merge gate stays two-key — the merge waits
for the domain-owner pass, whose trigger is that seat's return from the
routing freeze. Findings relevant to the liminal-uplift design pass are
FLAGGED in the worker's report, never routed to a frozen seat.

## Why (one paragraph)

`liminal-sdk` 0.2.3/main can REQUEST but cannot PARTICIPATE: `receive`
(remote/tcp/connection.rs:139) drains the buffered reply of a prior request,
`receive_conversation_reply` (:254) is request-scoped, lifecycle streaming
is a stub, embedded receive is a stub, and there is no participant resume.
The park-flip/D1 chain built the server side of symmetric delivery
(connection parking, bounded wakeable subscription inboxes, push-reply slots
with deadline-on-push). This leg builds the consumer side: an SDK process
that can be TALKED TO — unsolicited receive, lifecycle observation, ordered
delivery, honest backpressure, resume. It is the enabling work for Frame
F-0c/F-3a, the Meridian exchange's future liminal transport, and the
liminal-as-product uplift (Hermes' design pass names this contract as
consumer input).

## The contract (acceptance frame — F-0c R1, all eight assertions)

This brief's acceptance IS the Frame F-0c R1 contract list (frame repo,
docs/briefs/F-0c-liminal-conversation-spike.md §R1, pinned @ 6a750c8 — the
list was authored to be judged independently of both liminal's shape and
frame's convenience). Each assertion below must move to PROVEN by tests in
THIS repo; F-0c then re-proves them from the outside as a consumer:

1. **OPEN/CREATE** — an SDK participant can open (or join) a conversation
   from the SDK, or the limitation is stated typed and loud (server-created
   conversations only), not discovered.
2. **RECEIVE UNSOLICITED** — a participant receives a message it did not
   request, delivered via the real receive path, proven by content.
3. **REPLY** — and the reply reaches the sender symmetrically.
4. **LIFECYCLE** — join/leave/death of peer participants observable as
   typed events, not inferred from traffic.
5. **ORDERING** — delivery order per conversation is DEFINED and
   documented: what is guaranteed (per-sender FIFO? total per-conversation?)
   and what is not — request-response correlation and workflow steps both
   stand on this answer.
6. **BACKPRESSURE** — the Accept/Defer/Reject surface reaches the
   participant honestly: what a slow consumer experiences is typed, not
   emergent (within the bounds of what the A1 wiring state permits — see
   §Bounds).
7. **RESUME** — a participant that restarts resumes: what is replayed, from
   which cursor, and who owns cursor persistence are all ANSWERED and
   tested; messages-sent-while-dead have a documented fate.
8. **POLL-QUANTUM INDEPENDENCE** — a caller's receive timeout is a pure
   wait quantum; an elapsed receive is a benign re-arm that can never
   change a protocol outcome. **Provenance: proven at the protocol level
   before this brief was drafted** — Hermes' deadline-on-push ruling
   (2026-07-12), implemented on the fix branch and verified by Vesper
   Lynd's pre-merge re-proof (8/8: a four-window handler under 1-second
   polling completed exactly once with the sweeper live). This leg's
   obligation is to not regress it and to extend the same semantics to the
   new receive surfaces — every timeout parameter this brief adds is a wait
   quantum, never a lifetime.

## Requirements

**R1 — Symmetric receive on the remote/TCP participant.** A receive surface
that yields unsolicited conversation messages: blocking-with-quantum
(`receive(timeout) -> Ok(Some(Delivery)) | Ok(None) | Err(typed)`), where
`Delivery` carries conversation id, sender participant, payload, and
delivery sequence. Elapsed quantum = `Ok(None)` = benign re-arm (assertion
8). The typed error surface names ONE discriminating axis explicitly:
**connection-fate vs conversation-fate** — a participant's only real
decision on error is reconnect-or-leave, and an error enum that doesn't
distinguish "this connection is dead" from "this conversation is over"
forces the caller to guess; the worker shapes the variants, the axis is the
contract. The existing request-reply drain remains for compatibility; its
docs gain one sentence naming it request-scoped so nobody mistakes it for
this.

**R2 — Lifecycle events, typed.** Participant join/leave/death arrive as
typed events on the receive surface. **The ordering relationship between
lifecycle events and deliveries is a QUESTION the worker answers with
evidence, not a shape this brief pre-commits** — assertion 5 demands
ordering be DEFINED, and the definition must come from what the server
actually guarantees (per-conversation total? per-sender FIFO?
cross-conversation interleave undefined? lifecycle ordered relative to
deliveries at ingest, transmit, or not at all?). Assertion 5 carries the
same honesty valve as assertion 1: if the strong shape (one ordered stream)
cannot be proven, the documented-weaker answer PASSES by stating its
limitation typed and loud — a true weaker guarantee beats a false stronger
one, and briefs must not win arguments against evidence. Death
distinguishes what the server actually knows: clean leave vs
connection-down (the FIN vs no-FIN distinction stays server-side; the SDK
reports the server's verdict, never invents its own).

**R3 — Resume.** `ResumeRequest` machinery exists in the SDK
(remote/handles.rs:88 `connected()` returns them); this leg completes the
participant story: on reconnect, a participant re-attaches to its
conversations, receives from its last acknowledged cursor, and the
acknowledge surface (:129) is documented as the cursor-advance mechanism.
Messages sent while disconnected: delivered on resume within the server's
retention bounds, and the bound is stated where it can be read (typed
`HistoryCompacted`-shaped outcome when retention was exceeded — the
EventStore precedent, already the engine's honest answer).

**R4 — Backpressure honesty (bounded by A1's real state).** The typed
Accept/Defer/Reject vocabulary exists; live-path wiring is design-landed
but not code-complete upstream (A1). This leg does NOT wire A1. It does:
surface whatever the server actually signals today as typed outcomes on
the participant side, document the current truth (what a slow participant
experiences NOW), and give A1's future wiring a HOME at the type level, not
just a name — the participant-side outcome enum is documented as A1's
landing site, citing A1's design doc, so the wiring lands in a socket
rather than a sentence. No pretending — the F-0c R5 reality-recording
obligation, honored from the inside.

**R5 — Embedded parity is OUT, stated.** The embedded transport's receive
remains a stub; this leg is remote/TCP only. The embedded form arrives as a
later transport optimization behind the same handle shapes (Frame D4's
sequencing). The stub's docs say so — no silent parity implication.

**R6 — Tests, silence-attacked.** Per-assertion tests as the contract
demands, plus the silence-attackers this week's law requires: symmetric
receive of a message sent while the receiver was mid-quantum; lifecycle
event racing a delivery (order defined, asserted); resume across kill -9
(READY-after-durable discipline for cursor persistence if the SDK owns
any); SIGSTOP'd peer (no-FIN) — **the observed bound DERIVES from the
server's named heartbeat/sweep configuration parameters and the test pins
against those parameters, or it cannot fail** ("eventually" is not a test;
a bound without a source is an assertion, not an acceptance); the
eighth-assertion regression pin on every new timeout
parameter; and the two aion-shape tests from the receive-cancel incident
ported to the participant surface (slow-handler-under-short-quantum
completes exactly once).

## Shared-surface coordination (standing agreements)

Root `Cargo.toml`/`Cargo.lock` and `liminal-server/tests/sdk_tcp_e2e.rs`
are coordinate-not-divide with Vesper Lynd — one hand per file, ping before
touching; new e2e cases go in a new file (`sdk_participant_e2e.rs`) unless
coordinated. `connection/process.rs` is not touched by this lane under any
circumstance; if the receive path needs a server-side change in that file,
it is ASKED FOR, not made.

## Gates

House battery, each gate its own command, exit statuses read individually:
`cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`,
`cargo test --workspace`, doc build clean. New files under the repo's line
discipline. No silent failures; every server verdict surfaced typed.
Reviewer of record re-runs everything on final bytes; Hermes Crumpet's
domain-owner pass on the diff stands as agreed; worker tree stays
uncommitted until both passes complete.

## Dispatch

Single unit, one worker, norn-dispatched (Sol, implementation preset,
xhigh — concurrency + protocol surface). Dispatch REQUIRES the §Gate
placeholder filled with the registry-verified published version. Findings
that touch the liminal-uplift design pass (Hermes') are flagged in the
report — this leg is deliberately the first brick of "what ANY participant
needs," and anything it learns belongs to that design as much as to Frame.
