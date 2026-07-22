# Participant-domain wire/server contract — design draft R18

**Status: DRAFT R18 — closes the adversarial post-R17 defects in generated-codec
constructibility, exact Qe=2 occurrence totality, K-classified Leave transfer,
equality-enrollment continuation, and boundary-fixture classification while
preserving R17's movable-claim, completion-order, payload, and frame closures;
not yet ratified.** This redraft
parks at the two-key gate: reviewer-of-record plus the liminal domain-owner pass
(Hermes Crumpet). “Decided-by-draft” below means that R18 selects one contract
shape for those keys to accept or refute; it does not grant implementation
authority before both keys turn.

**Author:** Vesper Lynd. **Drafter:** Sol worker.

## 0. Provenance, authority, and laws

### 0.1 Provenance

This draft is pinned to liminal commit
`ce8814daa748373d8ffc66b3ff1664f1697a5f4e`, confirmed as the merge base of this
design branch. Every repository citation retained from R1–R10 or added in R11 was
opened and re-verified in this checkout against that pin. A bare `path:line`
below therefore means that path and current line at that named commit; no
citation is inherited from an outline or review.

R1 originated from SDK-receive dispatch blocked-upstream, norn session
`256a81a0`, envelope `claude-dev-sdk-receive.8rss9K`. That worker reached the
transport-versus-participant-domain gap, stopped at the brief's valve, and
produced the evidence base retained here. That session remains the **RESUME
VEHICLE** once this contract lands.

R2 is the redraft required by second-key refusal tear `22923c37`, read from the
newest `claude-review-contract-tear.*` envelope. The delta outline decides the
five blockers. Where the tear classified a matter as a decision but the outline
expressly leaves it to an owner or evidence gate, the outline governs and the
tension is recorded in §0.3 and §7 rather than silently erased.

R3 is the targeted redraft required by the second key's refusal ruling of
2026-07-13 and full tear envelope `b1623e31`. That key confirmed that all five
R1 blockers are resolved in R2; they and §0.3 survive. R3 decides only the new
first-attach, compaction, membership, lifecycle-observer, concurrent-holder,
and citation surfaces. Where the tear offered alternatives or suggested a
possible owner gate, the governing R3 outline selects one candidate contract;
those tensions are recorded item by item in §0.4.

R4 is the composition redraft required by second-key refusal tear `5de60e4c`,
read from envelope `claude-review-contract-r3.RMX45H`. The ruling confirms all
six R2 refusal items are resolved in form and mandates decisions—not new
mechanisms—at seven seams among receipts, Leave, observer projection, compaction,
and retention. R4 makes those decisions in place and records them in §0.4.

R5 is the failure-taxonomy redraft required by second-key refusal tear
`be2ca4fb`, read from envelope `claude-review-contract-r4-c.b11qea`. That ruling
confirms all seven R3 seams closed correctly and narrows refusal to five outcome,
bounded-provenance, backpressure, and SDK-transition decisions recorded in §0.4.

R6 is the boundary-completion redraft required by second-key refusal tear
`fb3cc175`, read from envelope `claude-review-contract-r5.Hy31yI`. That ruling
confirms credential-token provenance and all R4 decisions, and narrows refusal
to four enrollment/readiness/finalization/enumeration decisions in §0.4.

R7 is the oldest/newest composition redraft required by second-key refusal tear
`93998bf9`, read from envelope `claude-review-contract-r6.IfAExd`. That ruling
confirms the R6 enrollment index, pending-state bound, ordinary-admission
ambiguity, ack outcomes, size validation, and tombstone precedence, and narrows
refusal to the five sequence/readiness/detach/Retired/drain decisions in §0.4.

R8 is the boundary-field redraft required by max-effort second-key refusal tear
`e51e8e10`, read from envelope `claude-review-contract-r7.LhpOA7`. That ruling
confirms the R7 reserve arithmetic, reconnect linearization, detach cell,
token-bearing `Retired` paths, and unified admission order, and narrows refusal to
six marker/parking/epoch/ack/detach-field/provenance decisions in §0.4.

R9 is the whole-artifact closure redraft required by fresh max-effort second-key
refusal tear `021b672d`, read from envelope
`claude-review-contract-r8-final.HkIBRS`. That examiner was deliberately given no
round history and re-opened the complete wire contract. R9 closes the three
marker/ack/taxonomy blockers and three parked-state/byte-bound/detach-cell majors
recorded in §0.4 rather than treating earlier-round surfaces as settled.

R10 is the dual-final-examiner redraft required after **both** independent fresh
max-effort examiners refused commit `8d8dab0`. The dispatcher hand-verified all
fifteen defects. R10 closes the reserve, restart, precedence, direct-detach,
epoch, single-binding, global-bound, renegotiation, byte-ceiling, counter,
maximum-generation, anchored-recovery, taxonomy, acceptance, and LAW-1 inventory
decisions recorded in its change ledger.

R11 is the final-boundary redraft required by the latest integrated examiner
ruling. That ruling found three executable-acceptance defects and eight semantic
holes spanning sequence exits, closure debt, unbound Leave, finite ordering,
receipt binding, equal-generation secret proof, stale outbound work, binding
epoch identity, and three additional LAW-1 polling families. R11 closes all
items in §0.4 without changing the frozen laws or relaxing the two-key gate.

R12 is the specification-completeness redraft required after both independent
triple-lens examiners and the acceptance-executability lens refused R11 commit
`1e6aa99`. The mandate preserves the E/T reserve formula, case 47 arithmetic,
and all verified polling evidence; R12 closes the thirteen named classes without
re-deriving those surviving decisions. It makes repayment edges closed and
resource-backed, reserves recovery occupancy, makes the polling inventory
reproducible, validates recovery-handshake encoding, closes token fingerprints,
candidate order, Leave drains, restart/incarnation recovery, dispatch-slot
handoff, and all affected executable fixtures.

R13 is the refusal-closure redraft required after two independent max-effort
examiners and one blind executability examiner refused R12 commit `897be59`.
The owner hand-verified all fifteen deduplicated findings and ruled eight repair
classes. R13 separates Q debt from non-borrowable K, makes fenced recovery accept
the marker and terminate the attach/death cycle, gives every edge complete claims
and invalidators, makes composed Leave positional, extends LAW 1 across Rust and
all shipped SDK source, puts the Leave verifier in the tombstone, fixes recovery-
handshake widths/phase precedence, and re-derives every affected fixture. It does
not reopen the thirteen R12 mandate classes beneath those corrections.

R14 is the refusal-closure redraft required after two independent Sol examiners
and one blind human-lens examiner unanimously refused R13 commit `0bbcd56`.
The reviewing seat hand-verified all nineteen findings and consolidated them into
eleven ruled repair classes. R14 makes K componentwise transferable, permits one
quartet per debt episode, separates product-backed marker claims, closes initial
parking outcomes and the floor envelope, serializes every SDK parked-row action,
keeps detach non-secret-bearing, retires Rust/Gleam reconnect-delay re-arm,
canonicalizes sequence-budget payloads, and rebuilds every defective fixture. It
does not reopen the R13 classes beneath those corrections.


R15 is the closure redraft required after the unanimous triple-gate refusal of
R14 commit `f9f9bc2`. The coordinator hand-verified all twenty-one findings with
zero false positives and ruled twelve classes. R15 roots mandatory release in
full-K legality, replaces milestone bits with occurrence plans, gives undelivered
dead-epoch markers a Leave-only successor, closes PCP orderings, removes causal
phantom markers, types nonzero limits, aligns TS pressure, and rebuilds every
reachability/transfer fixture. The thirteen-family/socket structure remains.

R16 is the worklist-closure redraft required after two independent max-effort
examiners refused R15 commit `64bade9`. The coordinator hand-verified and
deduplicated their findings into seven classes. R16 routes pre-delivery and
no-marker fates to encodable Leave-only edges, makes supersession retargeting
constant-space, rebuilds cases 31/43/45/47/48/49/51/56 from complete legal histories,
and gives zero recovery-entry width a deterministic typed disposition. It does
not reopen the sound full-K mandatory-envelope algebra.

R17 is the adversarial closure redraft required by the fresh whole-document exam
after R16 closed that verified worklist. It makes unmaterialized order and
sequence ownership a crash-atomic bounded frontier, moves accepted recovery
blocks only as intact intervals, validates corrupt frontiers, and exhausts every
multi-binding fate/marker/projection/Leave ordering. It also makes cumulative K
exit exposure a producer precondition, replaces an unsafe multi-marker fixture
with an injective occurrence proof, and rebuilds cases 37, 54, and 55 from legal
public histories with exact positional rows. The thirteen LAW-1 prerequisite
families and the socket register remain unchanged. The same fresh pass then
closes payload-field order, bidirectional outcome triggers, exact numeric wire
registries/directions, physical frame-size and recovery-schema bounds, and
pre-commit ordinary-delivery encodability.

The corrected boundary survives R1: the server side has symmetric **transport**,
not a participant-domain contract. At the pin, transport parks on readiness,
has bounded inbox configuration with an installed READY notifier, and owns
correlated push-reply slots
(`crates/liminal-server/src/server/connection/process.rs:193-212`,
`crates/liminal-server/src/config/types.rs:226-236`,
`crates/liminal-server/src/server/connection/apply.rs:378-405`,
`crates/liminal-server/src/server/connection/supervisor.rs:224-240`). Those
surfaces do not jointly specify participant identity, participant cursor
authority, replay, or participant lifecycle verdicts: Push carries correlation
and opaque bytes, while Deliver has only a per-subscription sequence
(`crates/liminal/src/protocol/frame.rs:381-405`,
`crates/liminal/src/protocol/frame.rs:429-444`).

### 0.2 Binding laws

**LAW 1 — NO-POLLING (Tom, 2026-07-13 01:16Z, design constant).** No
application-layer poll loops anywhere in the product. Crash detection stays
event-driven: linked-EXIT in-VM, connection-fate cross-process. The heartbeat
option for no-FIN liveness is dead. The no-FIN bound is designed **inside**
event-driven machinery, not around it. If a design has a timer whose job is
“check whether something changed,” it is wrong: redesign it to be **TOLD**.
This law binds §§2-7, especially §3.

**LAW 2 — BELIEVED STATE IS NOT CITABLE STATE (tear-side law).** Every sentence
asserting the state of the liminal codebase must cite a verified file and line
at the named commit above, or must be a named socket in §7. This law binds the
whole document.

**LAW 3 — TWO-KEY GATE.** Nothing here is ratified until the reviewer key and
Hermes Crumpet's liminal domain-owner key both pass it. “Decided-by-draft” and
“requires” describe the candidate contract presented to those keys, never
implementation authority.

**Silence attack / provenance acceptance.** A reviewer refutes this section by
finding any repository-state sentence without a pinned citation, any citation
that does not support its sentence at the pin, or wording that mistakes a draft
decision for two-key ratification.

### 0.3 R1 → R2 changelog

Driver: second-key refusal tear `22923c37` and its full envelope findings.

| Refusal item | R2 change | Where |
|---|---|---|
| B1 — impossible zero-duplicate application delivery | Rewrote R-C3 in place to **at-least-once delivery, exactly-once marking**; exposed `(conversation_id, delivery_seq)` as the application idempotency key and made crash-window redelivery an acceptance case. | §4 R-C3 and its acceptance frame |
| B2 — unauthorized cursor construction | Closed `«PARTICIPANT-ID-ORIGIN»` by draft decision: the server mints an unforgeable attach secret, returns it exactly once, and authorizes cursor access before attach. | §4 R-C1; §7 |
| B3 — incoherent global sequence/cursor/recipient state | Made v1 conversations broadcast domains, made sequence allocation and record admission one commit, and closed lifecycle recipients. Targeted delivery is excluded. | §4 R-C2–R-C4; §5 R-D1; §6–§7 |
| B4 — lifecycle authority on the wrong trait | Kept `ConnectionNotifier` byte-untouched, introduced participant-domain `ParticipantLifecycle`, and separated worker-registration rollback from participant finalization. **R-A3's trait-widening recommendation is withdrawn.** | §2, especially R-A3 |
| B5 — SIGSTOP cannot be detected by TCP keepalive | Split host/network death from wedged-process liveness. SIGSTOP now must produce no false `ConnectionLost`; cursor stall is the only layer-local observable. | §3 R-B1–R-B5 and acceptance |
| M1 — existing listener violates LAW 1 | Named the current nonblocking-accept/sleep/reap loop and made its retirement a prerequisite of the server-side build. | §1 |
| M2 — protocol-1.0 exclusion was prose only | Required negotiated capability state, close on failed negotiation, and one grep-able outbound participant-frame construction/enforcement choke point. | §5 R-D2 |
| M3 — sockets were decisions in disguise | Audited every R1 socket, decided identity, ack shape, retention units, mux, and recipients, and added a `decided-by-draft / genuinely-open` column. **Recorded outline/tear tension:** the tear sought owner choices for no-FIN defaults and the legacy-resume migration now; the governing outline leaves `«NO-FIN-KERNEL-BOUND»` and `«RESUME-COMMENT-SERVER-MISMATCH»` genuinely open to platform evidence and owner ruling. | §7 |
| M4/minor — keepalive citation overreach | Removed the claim that the read span itself proves HUP and write errors; R-B1 now cites read EOF/error and outbound write failure separately and describes HUP as readiness confirmed by read outcome. | §3 R-B1 |

### 0.4 R10 → R11 changelog

Driver: latest integrated examiner ruling over the complete R10 artifact. Every
item below is closed in normative prose, exhaustive outcomes, and executable
acceptance rather than by an implementation placeholder.

| Refusal item | R11 decision | Where |
|---|---|---|
| E1 — sequence reserve does not pay a detached member's eventual `Left` | Every live member owns one flat exit claim `E=L`; the current checked invariant is `MAX-H >= E+T+M+RS+RT+(L×T)+(L×RT)+(L_other×E)`. Binding terminals, recovery records, replacement terminals, and their marker products remain separately charged, and bind/detach/Leave/recovery transitions consume, transfer, or create the named claims atomically. | §4 R-C2; acceptance 26/46/47/56 |
| E2 — acceptance 37 starts from an unreachable detached-marker state | Replaced the premise with an API-reachable setup using distinct connections, real enrollment/ack/projection/compaction and supersession events, and explicit postcondition checks before the minimum-cap drain. | §4 acceptance 37 |
| E3 — finite-boundary tests invent production mutation APIs | Defined a test-only durability-backend seeding convention for explicitly labelled complete snapshots. Later revisions construct the generation/sequence/transaction/park seams in cases 21/26/31/43/44/47 through their stated legal public histories; only the explicitly labelled allocator/ordering snapshots use the convention, and no production request can set counters. | §4 acceptance preamble and cases 21/26/31/42/43/44/47/48/49/56 |
| S1 — marker acceptance can increase reserve and closure debt can circularly deny recovery | A marker owns one capacity credit from planning through physical compaction; acceptance cannot increase reserve. Detached attach is a mandatory closure class, and nonzero/full debt requires a persisted TOLD repayment edge under absolute-fit, byte/entry, and sequence checks. | §4 R-C4; acceptance 37/45/48/49 |
| S2 — unbound Leave exists only at generation maximum | Any detached live member with exact current generation/secret may tokenized-Leave. If an earlier binding terminal is pending, one atomic transaction appends that terminal first and `Left` second, including at full debt. | §4 R-C1/R-C2/R-C4; §5 R-D1; acceptance 44 |
| S3 — accepted finalizations can outlive `transaction_order` | Active bindings and live members hold consumable major-order claims. Marker candidates share their causal major order, candidate drains allocate no new major, and only work outside those claims can receive `ConversationOrderExhausted`. | §2 R-A2; §4 R-C2; §5 R-D1; acceptance 43 |
| S4 — three production polling families are absent from LAW-1 evidence | Added pinned evidence for channel-reply liveness polling and both SDK TCP reader timeout/stop-flag loops, mandated event races plus explicit shutdown wake, and registered three implementation sockets. | §1; §7; acceptance 50 |
| S5 — a live receipt can report `Bound` after the slot changed | `Bound` now requires the receipt's exact participant binding epoch still occupy the origin slot; otherwise exact replay is `UnboundReceipt`, even on the same physical connection. | §4 R-C0/R-C1; §5 R-D1; acceptance 42 |
| S6 — equal-generation wrong secret has no exhaustive classification | Every live-identity secret-bearing operation compares the secret after tombstone/identity and generation lookup; the exact committed Leave-token exception checks its permanent tombstone verifier first. Either mismatch returns `StaleAuthority` with equal presented/current generation where current is live and no side effect or new oracle. | §4 R-C0/R-C1; §5 R-D1; acceptance 13/53 |
| S7 — queued participant bytes can cross detach/rebind | Queued work is keyed by participant plus binding epoch and revalidated at the sole final construction site; stale work is dropped; constructed bytes precede the same-writer detach/rebind response, while cross-stream handoff atomically retires the old SDK epoch. | §4 R-C5; §5 R-D2; acceptance 42 |
| S8 — connection incarnation does not identify same-connection rotations | Lifecycle, finalization, delivery, and receipt state use immutable `binding_epoch=(connection_incarnation, capability_generation)`, so same-connection rotation fences old work and yields distinct ordered lifecycle facts. | §2 R-A1/R-A2; §4 R-C1/R-C5; acceptance 42 |

### 0.5 R11 → R12 changelog

Driver: the decisions-only r12 refusal mandate over commit `1e6aa99`. R12 left
the then-current E/T formula unchanged; the artifact's current, superseding
eight-term formula is stated only in R-C2 and reproduced in the E1 row above.

| Mandate class | R12 decision | Where |
|---|---|---|
| C1–C2 | Repayment edges are a closed successor machine whose stored fixed-point claims own sequence, candidate position, occupancy, and recovery headroom. | R-C2/R-C4; cases 37/48/49/51 |
| C3 | A reproducible pinned-source grep classifies the total LAW-1 result set; durability bridge, push-reply re-arm, and subscription setup have retirement sockets. | §§1, 3, 7; case 50 |
| C4 | Startup/renegotiation validate the maximum one-shot refusal list against request and wire maxima; no chunk protocol is adopted. | R-A4/R-D1; case 52 |
| C5 | Fenced-recovery death preserves its independent projection edge; detached marker-delivery death has its own arm. | case 45 |
| C6 | Every live token stores a canonical verifier; wrong-secret conflict is `StaleAuthority`, other body conflict is typed. | R-C0/R-C1/R-D1; case 53 |
| C7–C8 | Candidate phases/indexes are normative and every affected finite-boundary fixture is a complete snapshot. | R-A2; cases 26/31/43/47/54 |
| C9 | Globally earlier Leave drains are separately committed and Q-bounded; only the member's own preserved terminal plus `Left` is the two-record atomic core. | R-C2/R-C4; case 55 |
| C10 | Prior-incarnation bindings undergo order/sequence-claim-backed startup recovery; connection incarnation is fixed-width, durable, unique, and exhaustion-safe. | R-A1/R-A2/R-D1; cases 42/56 |
| C11 | Cross-connection handoff retires one SDK dispatch slot, never an unrelated physical reader. | R-C5/R-D2; case 42 |
| C12 | Bound Leave at exact E/T equality spends both claims in one `Left`. | case 47D |
| C13/global | Names, outcomes, idle cost, no-polling, shared definitions, and cross-references receive explicit whole-document audits. | §§5, 7 and final audits |

### 0.6 R12 → R13 changelog

Driver: the decisions-only r13 refusal mandate over commit `897be59`; all
fifteen underlying findings were owner-verified before drafting.

| Mandate class | R13 decision | Where |
|---|---|---|
| C1 | Closure debt borrows only Q; K is explicit, non-borrowable occupancy, with componentwise fit and value assertions. | R-C4; cases 25/37/44/45/48/49/51/56 |
| C2 | Fenced DCR accepts the marker and exits to observer position; complete attach/future-binding claims, optional ordinary-attach refusal, and a decreasing successor plan prevent cycles. | R-A2/R-C2/R-C4; cases 45/51 |
| C3 | The preserved-terminal/Left core is positional; an intervening tuple forces a separate own-terminal drain before one-record Leave. | R-A2/R-C2; case 55 |
| C4 | Every repayment row enumerates every witness invalidator; binding fate and Leave now close `ParticipantCursorProgress`. | R-C4; cases 48/56 |
| C5 | The pinned LAW-1 sweep covers all crate and SDK product source, extended timer/clock/wait/scan vocabulary, TS reconnect, and structural wait shapes. | §§1, 7; case 50 |
| C6 | The bounded tombstone permanently stores and charges the fixed Leave verifier; lookup step 0a is the sole exact-Leave-token exception before ordinary tombstone precedence. | R-C0/R-C1/R-D1; case 53 |
| C7 | Recovery-handshake arithmetic is u128 over u64 inputs and cannot overflow; lifecycle phase alone selects startup versus parked-row incompatibility. | R-A4/R-D1; case 52 |
| C8 | Cases 26/43/45/47/49/51/56 are arithmetically complete, and the fixture audit covers every test seed. | acceptance preamble and named cases |

### 0.7 R13 → R14 changelog

Driver: the decisions-only r14 refusal mandate over commit `0bbcd56`; all
nineteen underlying findings were hand-verified before drafting.

| Mandate class | R14 decision | Where |
|---|---|---|
| E1–E3 | Recovery charge transfers from K_remaining into B; one quartet is consumed once; marker claims name non-product M versus terminal/exit product backing. | R-C2/R-C4; cases 37/45/48/49/51/56 |
| E4–E5 | Initial parking-shape failures have one exhaustive outcome, and `cap_floor` uses exactly the committing transaction class's envelope. | R-A4/R-C4/R-D1; cases 25/37/47 |
| E6–E7 | One SDK-wide domain serializes validation with every parked-row action; detach remains non-secret-bearing with executable verifier arms. | R-A4/R-C0; cases 52/53 |
| E8 | The pinned sweep includes reconnect/retry/backoff/delay vocabulary and separately retires Rust/Gleam caller-side reconnect-delay re-arm. | §§1, 3, 7; case 50 |
| E9 | Every sequence exhaustion uses one canonical payload containing all scalar and product reserve terms. | R-C2/R-D1; cases 21/31/43/47/51/56 |
| E10–E11 | Startup/enrollment, finite drains, complete snapshots, edge fields, floor values, and the partial-handshake sentence are repaired in place. | cases 21/25/26/37/44/47/51/56; R-D1 |


### 0.8 R14 → R15 changelog

| Class | R15 closure | Where |
|---|---|---|
| N1–N3 | Mandatory debt-zero commits prove full-K fit; successor occurrences encode repeated causal events and strictly decrease. | R-C4; cases 26/45/49/51 |
| N4–N7 | Undelivered dead epochs use Leave-only release; PCP ordering owns distinct M; phantom/backing labels are removed. | R-C4; cases 47/49/51/56 |
| N8–N10 | Nonzero parking limits are typed, detach has a complete no-body schema, and TS Defer means accepted/no retry. | R-A4/R-C0/R-D1; cases 25/50/53 |
| N11–N12 | MAX seams have explicit legal-public-prefix or explicitly test-seeded boundary classifications, marker pre-states are history-reachable, and names/digits/field counts are reconciled. | cases 42/44/45/56; final audits |

### 0.9 R15 → R16 changelog

| Worklist class | R16 closure | Where |
|---|---|---|
| W1 | Pre-delivery marker fate selects `DetachedMarkerRelease`; delivered-marker fate selects `DetachedCredentialRecovery`; no-marker cursor fate selects the distinct Leave-only `DetachedCursorRelease`. | R-C4; cases 47/49/51/56 |
| W2 | Case 45 proves the marker-only legal-minimum producer must refuse its unsafe recovery fixed point, then walks the farther-cap success with undelivered retarget, exact one-/two-record K transfers, floor ranges, and full-K release. | case 45 |
| W3 | Case 56 charges a planned marker in S/C only after a strict compaction overtake, retains the extra prefix record needed by the cap-8 success, fixes marker-before-terminal positions, and serializes both event-arrival orders. | case 56 |
| W4 | Supersession retargeting consumes no event occurrence; a persisted episode-churn budget bounds every plan-changing lifecycle cycle, and checked `O_max=1+27×Ce+207×I+234×J` serializes every event/successor alternative. | R-C4; case 54 |
| W5 | Case 31 uses a real ordinary-admission trigger whose class-specific cap floor overtakes the equality cursor and whose resulting marker makes sequence reserve fail. | case 31 |
| W6 | Every refusal-named boundary fixture—public-history or explicitly test-seeded—states all transition inputs, exact occurrence branches, authority, floor, budget, and charge positions. | cases 43/47/48/49/51 |
| W7 | `RE=0` is the eighth and `WF=0` the ninth deterministic `NonzeroLimit` arm in both initial and parked phases. | R-A4/R-D1; case 25 |

### 0.10 R16 → R17 changelog

| Adversarial class | R17 closure | Where |
|---|---|---|
| A1 — identity-fixed reservations fail arbitrary first fate/Leave | Defines movable unmaterialized order and sequence frontiers, moves DCR blocks only as intact intervals, rewrites occurrence backings atomically, and fails corrupt/torn frontiers deterministically. | R-A2/R-C2/R-C4; cases 54/55 |
| A2 — cumulative K exposure can exceed the one stored edge | Requires complete successor-tree cumulative exit charges to fit remaining K and forbids a debt-zero state whose full-K release violates capacity. | R-C4; cases 54/56 |
| A3 — multi-marker churn was asserted through an unsafe fixture | Gives every absent identity-indexed marker fact an injective Dormant-block lower bound and keeps one legal Ce=12 production trigger for EpisodeChurnLimit. | R-C4; case 54 |
| A4 — multi-binding completion schedules were showcased, not exhausted | Replaces the schedule sketch with an event partial order, monotone strict-suffix table, delayed-completion rule, and exact H8/H7/H6 marker/Leave families. | R-A2/R-C4; case 54 |
| A5 — positional/multi-marker fixtures were not public-history exact | Rebuilds case 37's three-marker capacity path and both case 55 terminal/Left positional arms with exact sequence/order vectors. | cases 37/55 |
| A6 — state/taxonomy names and selectors did not cover their claimed domains | Adds `ClaimFrontierInvalid` with exact precedence/fault arms and bounded O(I) validation; totals normal-ack and marker-proof selectors; restricts observer parking to the five constructible scope/dimension pairs; removes the nonexistent SDK-parking scope; and confines `ParticipantUnknown` to participant-id operations. | R-A2/R-C3/R-C4/R-D1; cases 34/54 |
| A7 — exact payload/wire claims left ambiguous or physically impossible arms | Makes every common/replacement payload schema exact; assigns the outer and complete direction-partitioned v1 registries; fixes primitive widths, canonical decode/order, pre-cap response authority, PF/PR/MR and recovery-frame algebra; removes unreachable RowBytes/error-max arms; and bounds ordinary request plus owed delivery before commit. | R-A4/R-C4/R-D1/R-D2; cases 25/32/33/40/43/52 |

### 0.11 R17 → R18 changelog

| Adversarial class | R18 closure | Where |
|---|---|---|
| B1 — stale algebra and index-domain wording | Reproduces the current eight-term reserve equation in historical cross-reference prose, makes participant indexes half-open with I as the non-id sentinel, and replaces Qe-parametric occurrence arithmetic by its generated-Qe=2 closed forms. | changelogs; R-C1/R-C2/R-C4 |
| B2 — impossible RecordTooLarge relation fixtures | Holds the only legal ordinary entry charge at one and varies its configured maximum through 2/1/0, while byte arms vary legal payload lengths below/equal/above one fixed maximum. | R-C4; case 32 |
| B3 — incomplete equality-enrollment continuation | Gives case 25 an exact public prestate, fixed-row charge, debt/K edge, occurrence coordinates, both event orders, floor movement, and the same legal debt-zero/full-K endpoint. | case 25 |
| B4 — fixture-assigned codec byte values | Defines one generated production durability-charge profile and converts every affected fixture to its exact b_u/Bm/Qb values, payload lengths, caps, and componentwise equations. | R-C4; cases 25/31/32/37/43–56 |
| B5 — unreachable occurrence-arithmetic outcomes | Proves every occurrence suboperation total under the accepted cap domains, removes the impossible occurrence-specific Multiply/Add arms while preserving R-A4's reachable configuration product arm, and leaves exactly four bidirectional retention-startup dimensions. | R-C4/R-D1; case 54 |
| B6 — ambiguous Leave/K accounting | Separates E/X-counter-backed lifecycle ownership, ordinary-Q Leaves with zero K charge, and K-claim-backed detached Leaves whose exact appended charge transfers from K_remaining. | R-A2/R-C2/R-C4/R-D1; cases 44–56 |
| B7 — public histories misclassified as test seeds | Enumerates the four actually seeded cases, classifies every other boundary as a stated public history, and removes stale historical/final-audit claims to the contrary. | changelogs; acceptance preamble/cases 42/48/49/56; final audit |

### 0.12 R18 amendment A1 — response/push ordering on one participant connection (2026-07-23)

**Status: amendment to DRAFT R18, authored by the liminal domain owner (Hermes
Crumpet); rides the two-key gate with the draft — the reviewer-of-record key
(Vesper Lynd) ratifies or refutes it at the next redraft.** Provenance: during a
loaded W4 tear battery one run of
`leave_after_detach_reattach_supersession_discharges_unacked_obligation_and_reopens`
read a well-formed unsolicited `ServerPush(ParticipantDelivery)` where it
expected its request's `ServerValue` response. Amplification reproduced the
interleave at will (52/60 iterations under 8-way CPU contention, byte-identical
captures; the response was correctly buffered behind the push — an ordering
artifact, not loss or corruption). Root cause of the ambiguity: R18 nowhere
states whether a request's semantic response and unsolicited pushes on the same
connection are ordered. This amendment closes that gap in prose; it changes no
wire bytes and no server behavior. It adds the `«RESPONSE-PUSH-ORDER»` socket
(decided-by-amendment) and one paragraph in section (c) after the
`conversation_id` demux paragraph.

## 1. The verified gap

The evidence base was re-verified rather than copied:

| Claim | Verified evidence at `ce8814d…` | Contract consequence |
|---|---|---|
| Worker unregistration has neither participant identity nor cause. | `ConnectionNotifier` is explicitly a connection-keyed worker-registration hook and `on_worker_unregistered` carries only `pid` (`crates/liminal-server/src/server/connection/notifier.rs:1-25`, `crates/liminal-server/src/server/connection/notifier.rs:46-52`). | Participant lifecycle needs its own keyed abstraction; §2 does not widen this trait. |
| A notifier unregistration call is also used for rollback while the connection remains open. | If application registration succeeds but registry storage fails, `worker_register_response` calls `on_worker_unregistered(pid)` as compensation and returns a rejection (`crates/liminal-server/src/server/connection/apply.rs:205-213`). | That path must produce no participant event and cannot be assigned a fake close cause. |
| In-VM participant liveness is deliberate event doctrine. | The conversation resource says participant crash arrives through a trapped linked-EXIT notifier, “never by polling, sleeping, or a heartbeat,” and its waiter parks on that event (`crates/liminal-server/src/server/connection/conversation.rs:3-7`, `crates/liminal-server/src/server/connection/conversation.rs:43-49`). | §3 stays inside linked-EXIT, readiness, and connection-fate events. |
| TCP transport resume is refused, while its comment claims re-Subscribe replay. | `TcpRemoteTransport::resume` returns `SdkError::Protocol`, and its comment says re-issued Subscribe triggers durable replay (`crates/liminal-sdk/src/remote/tcp/mod.rs:190-207`). | The migration mismatch remains a named owner ruling; participant resume cannot reuse that claim silently. |
| The existing SDK recovery model is subscription-local and in memory. | `ResumeRequest` carries `subscription_id` and client-held `from_sequence`; `SubscriptionRecovery` stores acknowledgements in a `BTreeMap`/`Vec` and clears them on unsubscribe (`crates/liminal-sdk/src/connection/recovery.rs:33-58`, `crates/liminal-sdk/src/connection/recovery.rs:154-158`). | Participant recovery needs distinct participant/conversation types or an explicit legacy API migration. |
| Current server Subscribe accepts no cursor and unsubscribe resets delivery sequencing. | Subscribe takes stream/channel/schema inputs and constructs a new subscription; unsubscribe removes both delivery counter and held frame (`crates/liminal-server/src/server/connection/apply.rs:349-421`, `crates/liminal-server/src/server/connection/apply.rs:432-443`). | R-C2/R-C3 define a different durable domain and starting convention. |
| Push has no participant envelope; Deliver's sequence is narrower. | Push is `{ flags, stream_id, correlation_id, payload }` with opaque payload; Deliver documents `delivery_seq` as per-subscription, starting at 1 (`crates/liminal/src/protocol/frame.rs:381-405`, `crates/liminal/src/protocol/frame.rs:429-444`). | §5 adds participant frame types without repurposing either key. |
| Authentication cannot by itself authorize a participant cursor. | Server auth is one shared bearer token and expressly “not an ACL system” (`crates/liminal-server/src/config/types.rs:25-31`). | R-C1 adds a participant-scoped capability while leaving connection auth intact. |
| Current negotiation is not an outbound capability boundary. | Connection state records `authenticated` but no selected protocol/capability (`crates/liminal-server/src/server/connection/state.rs:18-59`). `connect_response` sets authenticated before negotiation and answers negotiation failure without closing (`crates/liminal-server/src/server/connection/apply.rs:281-295`). | R-D2 makes negotiation state and the outbound choke point structural. |
| The current listener already has the banned polling shape. | The listener defines sleep backoffs, loops over a nonblocking `accept`, polls shutdown, reaps crashed processes, and sleeps on `WouldBlock` (`crates/liminal-server/src/server/listener.rs:11-12`, `crates/liminal-server/src/server/listener.rs:125-147`). | The server-side build for this contract must retire that loop in favor of readiness/blocking notification plus explicit shutdown and process-exit wakes. Compliance cannot be claimed before then. |
| Cluster membership is polled. | `PollLoop` repeatedly calls `membership.poll_once()` and sleeps for `POLL_INTERVAL` (`crates/liminal-server/src/cluster/membership.rs:198-228`). | `«MEMBERSHIP-EVENT-SOURCE»` must replace it with pushed membership-delta notification plus explicit shutdown wake before conformance. |
| Health accept is polled. | The health worker loops over nonblocking `accept`, reads a shutdown flag, and sleeps on `WouldBlock` (`crates/liminal-server/src/health/endpoint.rs:104-123`). | `«HEALTH-ACCEPT-SHUTDOWN-WAKE»` must use blocking/readiness accept and an explicit shutdown wake before conformance. |
| Shutdown drain and settle are polled. | Drain repeatedly reaps/counts active connections and sleeps; post-force-close repeats the same pattern (`crates/liminal-server/src/server/shutdown.rs:191-225`, `crates/liminal-server/src/server/shutdown.rs:229-244`). | `«SHUTDOWN-DRAIN-NOTIFICATION»` must replace both loops with connection-exit/drain completion notification and an admitted deadline event. |
| Channel command reply liveness is polled. | `LIVENESS_POLL` is 10ms and `poll_reply` repeatedly `recv_timeout`s, queries the process table, and samples the command deadline (`crates/liminal/src/channel/actor/wait.rs:21-24`, `crates/liminal/src/channel/actor/wait.rs:73-95`). | `«CHANNEL-REPLY-EVENT-RACE»` must race the reply, a pushed process-exit notification, and the one admitted command deadline; no repeated liveness query may remain. |
| The SDK push reader polls for local shutdown. | The reader installs a 100ms read timeout, maps timeout to `None`, and loops to re-read an atomic stop flag (`crates/liminal-sdk/src/remote/tcp/push_client.rs:52`, `crates/liminal-sdk/src/remote/tcp/push_client.rs:378-385`, `crates/liminal-sdk/src/remote/tcp/push_client.rs:460-510`). | `«SDK-PUSH-READER-SHUTDOWN-WAKE»` must race blocking/readiness input and an explicit local shutdown wake that interrupts the reader; socket silence is not a clock. |
| The SDK subscription reader polls for local shutdown. | The reader installs the same 100ms timeout and treats timeout as a chance to re-check its stop flag (`crates/liminal-sdk/src/remote/tcp/subscription.rs:47`, `crates/liminal-sdk/src/remote/tcp/subscription.rs:229-233`, `crates/liminal-sdk/src/remote/tcp/subscription.rs:327-382`). | `«SDK-SUBSCRIPTION-READER-SHUTDOWN-WAKE»` must race blocking/readiness input and an explicit local shutdown wake that interrupts the reader; no timeout/flag recheck loop may remain. |
| The synchronous durability bridge repeatedly polls without honoring a waker. | `block_on` constructs a no-op waker, polls up to eight times, and yields after `Pending` (`crates/liminal/src/durability/bridge.rs:29-40`, `crates/liminal/src/durability/bridge.rs:83-99`); production channel and server-dedup callers use it (`crates/liminal/src/channel/types.rs:340-368`, `crates/liminal-server/src/server/connection/services.rs:508-545`). | `«DURABILITY-BRIDGE-WAKE»` permits exactly one poll followed by a loud `Pending` error under the asserted synchronous backend, or a real parked executor honoring the future's waker. No no-op-waker retry/yield loop is conforming. |
| The push-reply public wait contract blesses caller-side timeout re-arm. | `PushReplyAwaiter::receive` describes elapsed polls as benign re-arms and implements timeout/`try_recv` looping (`crates/liminal-server/src/server/connection/supervisor.rs:533-636`); pinned tests repeatedly call it with short quanta (`crates/liminal-server/src/server/connection/supervisor_tests.rs:1211-1225`, `crates/liminal-server/src/server/connection/supervisor_tests.rs:1979-2004`). | `«PUSH-REPLY-AWAITER-EVENT-RACE»` replaces that blessed use with one SDK-call-site race among reply, connection fate, and one admitted deadline. An internal one-shot wait quantum may implement the race, but indefinite caller re-arm is not its contract. |
| Subscription setup samples a total deadline through repeated read timeouts. | `read_one_frame` takes 100ms reads and samples `Instant::now()` against `SETUP_TIMEOUT` (`crates/liminal-sdk/src/remote/tcp/subscription.rs:389-416`). | `«SDK-SUBSCRIPTION-SETUP-DEADLINE-RACE»` races handshake/Subscribe socket input against one total admitted setup deadline; no periodic clock sampling remains. |
| The shipped TypeScript SDK reconnects by an unbounded-default timer retry loop. | `reconnect` iterates attempts, sleeps for backoff, and retries `transport.open()`; omitted or negative `maxAttempts` becomes `Number.POSITIVE_INFINITY`, and default sleep uses `setTimeout` (`sdks/liminal-ts/src/connection.ts:240-264`, `sdks/liminal-ts/src/connection.ts:309-320`, `sdks/liminal-ts/src/connection.ts:336-339`). | `«SDK-RECONNECT-TIMER-LOOP»` replaces automatic timer retry with transport/connection-fate arming; a manual reconnect request remains TOLD, never a retry poll. |
| The Rust SDK's public lifecycle and remote handle compute/forward a reconnect delay and permit caller re-arm. | `ReconnectConfig` defaults from 100ms to 30s and computes capped exponential delay (`crates/liminal-sdk/src/connection/lifecycle.rs:115-195`); lifecycle `reconnect()` accepts `Reconnecting`, advances its attempt counter, and returns another delay (`crates/liminal-sdk/src/connection/lifecycle.rs:277-300`). Public `RemoteHandle::reconnect` delegates directly to that method and returns its delay (`crates/liminal-sdk/src/remote/handles.rs:71-80`). These paths compute or forward only; neither itself waits. | `«SDK-RECONNECT-DELAY-CONTRACT»` requires one fresh transport-fate event per returned delay/attempt and removes repeated timer re-arm through either public entry point as a contract. |
| The Gleam SDK mirrors that reconnect-delay contract. | Its lifecycle stores reconnect configuration/counter and documents exponential backoff (`sdks/liminal-gleam/src/liminal/connection.gleam:47-58`); `reconnect` accepts `Reconnecting`, increments, and returns a delay (`sdks/liminal-gleam/src/liminal/connection.gleam:105-129`). It also performs no wait itself. | The same `«SDK-RECONNECT-DELAY-CONTRACT»` retirement applies; compute-only is not permission for callers to turn each result into a timer re-arm. |
| TypeScript pressure `Defer` blesses producer retry while Rust defines accepted buffering. | `sdks/liminal-ts/src/pressure.ts:32-44,76-86` says buffered delivery but also returns retry delay; Rust says accepted into the bounded buffer and delivered later (`crates/liminal-sdk/src/pressure.rs:11-17`). | This contract classifies `Defer` as **accepted/buffered**: producer MUST NOT retry; delay is only a delivery estimate. Retire contradictory TS wording/tests before conformance; this is sweep-classified retry/delay vocabulary adjacent to, but not counted as, a fourteenth LAW-1 family or socket. |
| The durability API exposes a configured interval sweeper and scan. | `DedupSweeper` stores `sweep_interval` and `sweep_once` scans the store at caller-supplied time (`crates/liminal/src/durability/dedup/sweep.rs:39-76`); production source exports it (`crates/liminal/src/durability/dedup.rs:7-10`, `crates/liminal/src/durability/mod.rs:20`). | `«DEDUP-EXPIRY-EVENT-SOURCE»` replaces interval scanning with keyed admitted-expiry events or explicit mutation-triggered cleanup; no periodic full-store question is conforming. |
| Existing caps have a signed configuration pattern. | `LimitsConfig` defines named hard caps and defaults, rejects zero by field name, and constructs explicit defaults (`crates/liminal-server/src/config/types.rs:193-203`, `crates/liminal-server/src/config/types.rs:257-316`). | Keepalive, retention, receipt lifetime/count, and negotiated SDK parking extend this pattern rather than creating silent unlimited states. |

**`«RESUME-COMMENT-SERVER-MISMATCH»` is broader than a comment edit.** The
legacy public model is subscription-keyed and client-cursored, while R-C1/R-C3
are participant/conversation-keyed and server-cursored. §7 requires an owner to
choose distinct protocols or a versioned deprecation/removal; merely correcting
the comment cannot close the socket.

**LAW-1 prerequisite inventory — reproducible multi-language product sweep.**
This draft derives the inventory from the pin, not reviewer testimony. The product root
is every tracked path below `crates/*/src` and `sdks/**`, including TypeScript,
Gleam, and `wasm-src`. SDK `test/` and `tests/` directories are intentionally
excluded from the **product** claim, not represented as swept clean. In-source
Rust test modules/files remain visible and classify as test-only. These commands
first prove that the pathspecs match real files and print the root lines, then run
every expression independently so overlaps remain visible:

```bash
PIN=ce8814daa748373d8ffc66b3ff1664f1697a5f4e
PATHS=(':(glob)crates/*/src/**' ':(glob)sdks/**'
       ':(exclude,glob)sdks/*/test/**' ':(exclude,glob)sdks/*/tests/**')
git grep -I -l -E '.' "$PIN" -- "${PATHS[@]}" |
  sed -E 's/^[^:]+://' |
  awk -F/ '/^crates\// {print $1"/"$2"/src"; next}
           /^sdks\// {print $1"/"$2}' | sort -u
git grep -I -l -E '.' "$PIN" -- "${PATHS[@]}" | wc -l

for expression in recv_timeout try_recv poll yield_now sleep 'Instant::now' \
  'read_timeout|set_read_timeout|read_one_frame' wait_timeout park_timeout \
  sleep_until 'SystemTime::now' '\.elapsed[[:space:]]*\(' setTimeout setInterval
do
  git grep -n -E "$expression" "$PIN" -- "${PATHS[@]}" || true
done
git grep -n -E 'sleep' "$PIN" -- ':(glob)sdks/**/*.ts' \
  ':(exclude,glob)sdks/*/tests/**' || true
git grep -n -i -E '([[:alnum:]_]*(sweep|scan)[[:alnum:]_]*)' \
  "$PIN" -- "${PATHS[@]}" || true
git grep -n -E 'Condvar|\.wait[[:space:]]*\(' \
  "$PIN" -- "${PATHS[@]}" || true
for word in reconnect retry backoff delay; do
  git grep -n -i -E "([[:alnum:]_]*${word}[[:alnum:]_]*)" \
    "$PIN" -- "${PATHS[@]}" || true
done
```

The root enumeration prints `crates/liminal-sdk/src`,
`crates/liminal-server/src`, `crates/liminal/src`, `sdks/liminal-gleam`, and
`sdks/liminal-ts`; the nonempty-file count is `199`. The old six extended-root
counts are respectively `28, 2, 151, 12, 58, 96`. The timeout-read expression
has `29` results. The added counts in command order are `wait_timeout=0`,
`park_timeout=0`, `sleep_until=0`, `SystemTime::now=5`, `.elapsed(...)=10`,
`setTimeout=2`, and `setInterval=0`; TypeScript `sleep` has `5`. The case-
insensitive structural sweep/scan expression has `119`, and the explicit
Condvar/`.wait(` shape has `15`. The final underscore-tolerant vocabulary counts
are `reconnect=172`, `retry=43`, `backoff=27`, and `delay=109`. All 21 counts were
re-run at the pin over the same five roots with overlaps intentionally retained.
Zero is evidence because the preceding root enumeration proves the pathspecs matched.

The classification below is total over every returned line, including overlaps.
“Lexical” means a doc/comment/identifier/trait implementation or test-only body,
not an executing production wait. Every executable result belongs to exactly one
behavioral row.

| Complete grep-result class | Classification and reason |
|---|---|
| Main listener; cluster membership; health accept; shutdown drain/settle; channel reply wait; SDK push reader; SDK subscription reader | **Nonconforming prerequisite families already evidenced above.** Retire the main listener directly through the readiness/blocking-accept plus shutdown/process-exit rule evidenced above; retire the remaining six through their corresponding §7 event sockets. |
| Durability `bridge::block_on`; `PushReplyAwaiter::receive` caller re-arm; subscription `read_one_frame` setup loop | **Nonconforming prerequisite families added by R12.** Retire through `«DURABILITY-BRIDGE-WAKE»`, `«PUSH-REPLY-AWAITER-EVENT-RACE»`, and `«SDK-SUBSCRIPTION-SETUP-DEADLINE-RACE»`. |
| TypeScript `Connection.reconnect`, its configurable/default sleep, and `setTimeout` implementation | **Nonconforming prerequisite family.** It is one default-infinite timer-driven retry loop; retire it through `«SDK-RECONNECT-TIMER-LOOP»`. |
| Rust `ConnectionLifecycle::reconnect`/`ReconnectConfig` and `RemoteHandle::reconnect`, plus Gleam `connection.reconnect`/`ReconnectConfig` | **Nonconforming public re-arm contract, not an executing wait.** Each lifecycle computes/returns another delay from `Reconnecting`, and the Rust handle exposes that transition directly; retire repeated timer re-arm through `«SDK-RECONNECT-DELAY-CONTRACT»`. |
| All remaining reconnect/retry/backoff/delay vocabulary | **Exhaustively dispositioned, not a hidden wait family.** Remaining SDK pool/remote/recovery and non-reconnect handle matches, plus protocol/Gleam FFI matches, are state, field, conversion vocabulary, or pure delay/jitter calculation; durability's five matches explicitly say “without retrying”; listener backoff belongs to the first nonconforming row; the delivery shed retry is one bounded sticky work item revisited only by the existing READY/write-progress slice, not a timer; pending-reply delay arms one admitted deadline; `set_nodelay`, routing prose, exports, comments, and tests are lexical/test-only. TypeScript/Rust/Gleam reconnect and pressure matches are carved out by their dedicated rows. |
| TypeScript/Rust/Gleam pressure `Defer`, `retry_after`, and `delay` matches | **Semantic mismatch, not a wait permission.** `Defer` means accepted into the bounded buffer, so the producer MUST NOT retry; delay is only a delivery estimate. Retire the contradictory TypeScript retry wording/tests before conformance as the evidence row requires. |
| `DedupSweeper`, `sweep_interval`, `sweep_once`, and the scan it initiates | **Nonconforming prerequisite family.** Retire interval/full-store change detection through `«DEDUP-EXPIRY-EVENT-SOURCE»`; a caller-supplied clock does not make scanning TOLD. |
| `Future::poll`/`Stream::poll_next` implementations in SDK lifecycle/embedded types | **Event-driven conformant.** These are executor-called callbacks honoring the supplied waker, not application loops. |
| One-shot `recv_timeout`/timeout reads in public SDK receive calls, actor request/reply, routing handoff, health request parsing, and connection setup | **Event-driven conformant only as one admitted wait/deadline.** No caller or callee may use timeout return as a re-arm/change-sampling loop; the violations above are carved out explicitly. |
| Conversation EXIT `try_recv`, pending-reply `try_recv`, the pending-reply `send_after(delay)` deadline arm/process timer reads, causal-order/lifecycle/tracing `Instant`/`SystemTime`/`.elapsed` timestamps | **Event-driven conformant.** Each is read only on an already delivered scheduler/domain event, a final park probe, records one timestamp, or arms exactly one admitted operation deadline; none schedules repeated observation. |
| Shutdown `Condvar::wait`; runtime completion wait | **Event-driven conformant.** Each parks until an explicit shutdown/completion notification. Supervisor barrier waits are `#[cfg(test)]`, not product waits. |
| Conversation-close pending-reply sweep; store `scan` trait/adapter and non-dedup scan mentions | **Event-driven/lexical conformant.** The close sweep runs synchronously on the close event; generic scan surfaces do not schedule themselves. The Dedup caller above is the carved-out interval family. |
| All matches under `*_tests.rs`, `*/tests.rs`, an in-file `#[cfg(test)]` module, docs/comments, field/type names, and negative assertions such as “no polling” | **Lexical/test-only.** They remain acceptance affordances. Tests may coordinate with sleeps/yields; no helper is a production liveness mechanism. SDK test directories are outside, and make no contribution to, the product-sweep claim. |

Consequently the participant implementation may inherit none of the thirteen named
nonconforming families. Main/health accepts use readiness plus explicit shutdown;
membership deltas and connection exit are pushed; deadline waits race one admitted
deadline; SDK readers race input with explicit shutdown; push reply races reply,
connection fate, and deadline; subscription setup races input with one total
deadline; and the durability bridge either completes on its sole synchronous poll
or parks on a real waker. SDK reconnect is armed only by a transport/connection-
fate or explicit manual-connect event. A Rust/Gleam returned delay is valid only
for the attempt armed by that fresh event; repeated re-arm without another fate
event returns the exact SDK-local, non-wire
`ReconnectDelayResult = ReconnectArmed { delay_ms:u64 } |
ReconnectNotArmed { state:Reconnecting, required_event:TransportFate }`.
A transport/connection-fate transition into `Reconnecting` stores one
single-use reconnect permit. The first Rust lifecycle or Gleam reconnect call
consumes it and returns `ReconnectArmed`; Rust `RemoteHandle::reconnect`
forwards that same result. A call or caller timer re-arm in `Reconnecting`
without a permit returns `ReconnectNotArmed` and changes no lifecycle state,
attempt counter, computed delay, timer, or network state. Explicit manual
connect is a separate TOLD event, and neither result is an R-D1 wire
discriminant. Dedup expiry is keyed by admitted
expiry/mutation events. No periodic reap, count/check, timeout-as-wake,
atomic-flag recheck, `sleep` backoff, no-op-waker repoll, synthetic wake, or
“temporary” polling adapter is conforming. Independently of spelling, the
structural audit must inspect every production loop/task/callback that combines a
clock, timeout, sleep, atomic/nonblocking probe, retry, sweep, or scan. Any wait
shape lacking a named TOLD trigger fails; a future lexical hit is unclassified
until this table or a new pinned evidence row classifies it.

**Silence attack / gap acceptance.** Refute this section by identifying an
existing frame that jointly carries participant identity, conversation identity,
a durable authorized cursor, replay position, and lifecycle verdict, or by
showing server Subscribe consumes such a cursor. A nearby field with narrower
scope does not close the gap. Conversely, finding the named listener loop does
not refute R12; it proves the explicit retirement prerequisite remains unmet.

## 2. Section (a) — participant lifecycle at the participant boundary

### Proposed contract

**R-A1 — Typed cause, participant-domain owner.** Introduce exact v1 `CloseCause`
and a separate participant-domain observer,
working name `ParticipantLifecycle`. A binding is identified by the immutable
`BindingEpoch { connection_incarnation, capability_generation }` captured by its
attach commit. This is the artifact's sole definition of **binding epoch**; R-C0,
R-C1, R-C5, and restart recovery cite it rather than redefining it. Rotating on
the same physical connection therefore creates a new epoch and can never reuse
the old generation's lifecycle, receipt, finalization, or queued-delivery
identity.

`connection_incarnation` is a fixed-width unsigned 128-bit wire/storage value
`(server_incarnation: u64, connection_ordinal: u64)`. Before participant mode
accepts connections, startup transactionally checked-increments and fsyncs the
persisted `server_incarnation`; it never wraps, rebases, or reuses a value. Before
a connection can negotiate participant capability, the server transactionally
allocates its checked-incrementing `connection_ordinal`. The pair is compared
against every still-live or durably referenced binding, receipt, work item, and
recovery row; a collision is never published and retries the next ordinal.
Ordinals through MAX are allocated at most once. Allocating MAX atomically sets
the fixed header bit `connection_ordinal_exhausted`; encountering a referenced
MAX candidate sets the same bit and publishes no pair. A later mint with that
bit set has no candidate and takes the ordinal-exhaustion arm below.
Exhaustion returns the exact R-D1 payload
`ConnectionIncarnationExhausted { component: ServerIncarnation|
ConnectionOrdinal, current_value, attempted_server_incarnation: Option<u64> }`.
`ServerIncarnation` is selected when the persisted server counter is MAX and
carries `current_value=MAX,attempted_server_incarnation=None`.
`ConnectionOrdinal` is selected when no greater ordinal exists for the current
server incarnation and carries `current_value=MAX,
attempted_server_incarnation=Some(current server incarnation)`. Either arm
refuses startup/new participant connections without changing identity state. The fixed 16-byte
encoding is included in Qb, parked-row, receipt, and frame maxima. Thus the pair
is unique across every simultaneously or durably referenceable connection,
including server restarts, and is minted before first participant use.

Binding-domain facts identify
`(participant_id, conversation_id, binding_epoch)`; each committed lifecycle
record and observer callback also carries its canonical delivery key
`(conversation_id, delivery_seq)`:

- `Attached { participant_id, conversation_id, binding_epoch }`;
- `Detached { participant_id, conversation_id, binding_epoch,
  cause: CloseCause }` for an explicit detach,
  clean Disconnect, authorized superseding attach, or server-directed shutdown;
- `Died { participant_id, conversation_id, binding_epoch,
  cause: CloseCause }` for connection or process
  failure; and
- `Left { participant_id, conversation_id,
  ended_binding_epoch: Option<BindingEpoch> }` for R-C2's explicit, durable,
  terminal membership transition, never for transient detach or connection loss.
  The option is `Some` only when the same commit terminalizes an active binding;
  an already-detached or already-finalized member has no live epoch to invent.

The v1 `CloseCause:u16` registry is exactly these seven non-collapsible classes,
in the displayed one-based order:

- `CleanDeregister`: explicit participant detach or clean protocol Disconnect.
  The current dispatcher classifies Disconnect as Close
  (`crates/liminal-server/src/server/connection/apply.rs:46-53`), and the process
  then follows the normal finish path
  (`crates/liminal-server/src/server/connection/process.rs:280-289`).
- `ConnectionLost`: TCP EOF/FIN without domain detach, kernel keepalive expiry,
  or fatal read/write failure. EOF and read error are distinct paths
  (`crates/liminal-server/src/server/connection/process.rs:238-269`), and fatal
  outbound drain errors tear the connection down
  (`crates/liminal-server/src/server/connection/process.rs:169-182`).
- `ProcessKilled`: trapped linked-EXIT or a locally known supervisor
  termination. Conversation actors already expose the trapped EXIT event
  (`crates/liminal-server/src/server/connection/conversation.rs:173-186`), but
  the external reap path cannot recover beamr's private exit reason
  (`crates/liminal-server/src/server/connection/supervisor.rs:1643-1658`), so
  `«EXTERNAL-EXIT-REASON»` owns only a future versioned detail extension; it does
  not make v1's exact no-detail `ProcessKilled` body ambiguous.
- `ProtocolError`: decode or protocol-state refusal that terminates the
  connection. Decode refusal is distinct in the buffer path
  (`crates/liminal-server/src/server/connection/process.rs:695-710`).
- `Superseded`: an attach authorized by the same participant capability replaces
  an older binding epoch under R-C1, including on one connection incarnation. It is an explicit participant-domain action,
  not evidence that the old transport died.
- `ServerShutdown`: the server deliberately ends an active participant binding
  during shutdown. It is not misreported as participant choice or transport
  failure.
- `UncleanServerRestart { prior_server_incarnation }`: startup recovery found a
  durably active binding owned by a prior server incarnation. It is authoritative
  restart evidence, not a fabricated FIN, clean disconnect, or transport timeout.

The first six variants carry no body; only `UncleanServerRestart` carries its
required u64. In particular v1 `ProcessKilled` carries no unavailable private
exit-reason detail; `«EXTERNAL-EXIT-REASON»` can add typed detail only with a
participant-version bump. No catch-all or optional detail bag exists. EOF is
not proof of clean deregistration. An `Attached` event has no invented close
cause; terminal `Detached` and `Died` binding events do. `Left` is the typed
terminal membership fact and needs no fabricated close cause.

**R-A2 — Event mapping and one finalization authority.** Cause assignment occurs
where the authoritative event is received. The conversation log is the **sole
durable completed lifecycle history**: R-A2's bounded pending state records an
accepted fate only until the participant owner commits exactly one terminal
lifecycle record per active participant binding. Projection to an observer is
governed by R-A4 and is not a second exactly-once effect promise:

| Source event | Participant-domain result |
|---|---|
| Successful R-C1 attach commit | One `Attached` record naming the newly committed binding epoch; a failed/rolled-back attach commits none. |
| Explicit participant detach | `Detached(CleanDeregister)` names exactly that binding epoch; membership and cursor remain durable. |
| Explicit participant Leave | A bound Leave with no older pending terminal emits one ordered `Left` that is both the active binding epoch's terminal record and the terminal membership record. If its own older terminal is pending, apply R-C2's positional rule: compose terminal then Left only when no unrelated tuple intervenes; otherwise drain/link that terminal and every intervening tuple separately before the one-record Leave. Identity, receipts, cursor, and retention claim retire atomically with the eventual `Left`, not necessarily with an earlier terminal drain. |
| Clean protocol Disconnect | `Detached(CleanDeregister)` for every binding still active on that connection. |
| Authorized replacement by a newer binding epoch, including rotation on the same connection incarnation | `Detached(Superseded)` for the exact old epoch, then `Attached` for the exact new epoch in conversation order. |
| TCP FIN/EOF, confirmed read error, fatal write error, or kernel keepalive expiry | `Died(ConnectionLost)` for every active binding on the connection. |
| Trapped linked-EXIT or locally known forced process termination | `Died(ProcessKilled)`; external reason detail remains gated by `«EXTERNAL-EXIT-REASON»`. |
| Terminating decode/protocol-state refusal | `Died(ProtocolError)`. |
| Server shutdown | One authoritative shutdown event batches every still-active binding by conversation. For each conversation, one causal major orders all `Detached(ServerShutdown)` terminals by participant index unless an explicit detach linearized first. The source transaction appends only the longest currently Envelope- and observer-legal terminal prefix, capped at two rows, and persists every remaining terminal as its bounded `PendingFinalization`; Q-bounded drain transactions materialize the rest in that same tuple order before any phase-4 marker. Shutdown is neither participant choice nor inferred transport death. |
| Server startup finds a persisted active binding from a prior `server_incarnation` | One order/sequence-claim-backed startup-recovery transaction revokes authority and records `Died(UncleanServerRestart { prior_server_incarnation })` directly or as `PendingFinalization`; it never invents FIN. |
| Worker-registration compensation in `crates/liminal-server/src/server/connection/apply.rs:205-213` | **No participant event.** The registration never became a participant binding. |

The connection owner publishes one typed connection-termination event to the
participant owner; the participant owner derives the keyed terminal events. It
does not infer cause from later absence and adds no detector, sweep, or timer.

**Prior-incarnation startup recovery.** Before participant traffic or receipt
replay is enabled, startup enumerates the bounded persisted binding slots once as
durable recovery input. For each epoch whose `server_incarnation` is not current,
one idempotent transaction immediately revokes transport/cursor authority,
consumes that epoch's already-owned `A` major and `T` sequence claim, records
`UncleanServerRestart`, and either appends the one terminal record where the
ordinary drain law permits or writes the exact ordered `PendingFinalization`.
The transaction also updates the bounded detach cell, ClosureDebt witness, and
its already-owned edge claims atomically. It creates no reserve domain and is not
refusable at full debt or at sequence/order equality. An exact receipt for the
recovered epoch subsequently fails R-C0's current-slot test and returns
`UnboundReceipt`.

The sole state outcome is R-D1 `BindingRecoveryCommitted { participant_id,
conversation_id, recovered_binding_epoch, cause, assigned_transaction_order,
finalization: Appended { delivery_seq } | Pending { admission_order },
repayment_edge }`; exact restart replay returns the same state and cannot append
a second terminal. A storage-commit failure keeps participant mode closed and
retries startup recovery on the next explicit startup attempt; it is not a wire
semantic outcome. Enumeration is bounded by signed identity slots and runs once
from startup input, never from a periodic scan. Each transaction is TOLD by
startup/storage completion and introduces no timer, poll, or sweep. Its only
durable state is the existing binding/finalization/detach/debt cells plus the
terminal record, so idle cost does not grow.

**Pending finalization and one server-candidate order.** Each R-C0 identity
reservation owns at most one current-or-pending binding slot because replacement
fences the old binding epoch; active plus pending bindings therefore cannot exceed
the signed identity-slot caps. A binding slot is not reusable until its terminal
record commits. When explicit detach has been accepted, clean Disconnect,
EOF/read/write failure, linked-EXIT or local process termination, shutdown, or
protocol error arrives and the log cannot append, one atomic durable transition
changes the binding to `PendingFinalization { participant_id, conversation_id,
binding_epoch, original_cause, event_kind, admission_order }`. For explicit
detach, that transaction also writes R-C0's token-bearing
`detach_replay::Pending` with its server-captured current binding epoch and refusal epoch;
connection-fate candidates have no client token. Transport/cursor authority ends
immediately, but the exact original cause and binding epoch remain durable.

The conversation's serialized admission lane owns a checked fixed-width `u64`
major `transaction_order`. Persist it as `Option<u64>` so the exact number of
allocatable values is derivable in checked `u128`: `2^64` when absent and
`u64::MAX - high` otherwise. Values `0..=u64::MAX` allocate once and the major
never wraps, saturates, aliases, resets, or rebases. Candidate order is
`(transaction_order, candidate_phase, ascending_participant_index)`. Its complete
canonical phase order is:

| Numeric phase | Protocol name | Record/candidate class |
|---:|---|---|
| 0 | `BindingTerminal` | Pending or direct `Detached`/`Died`, including startup recovery. |
| 1 | `MembershipExit` | `Left`; a bound `Left` that also terminalizes its binding appears only here, not twice. |
| 2 | `AttachLifecycle` | `Attached`, including enrollment, supersession, detached attach, and fenced recovery. |
| 3 | `OrdinaryRecord` | One admitted application record owned by the transaction. |
| 4 | `CompactionMarker` | Every `HistoryCompacted` candidate/record induced by the causal transaction. |

All binding terminals in a major therefore precede all attached/ordinary facts
and **all induced markers**; within one phase, ascending `participant_index`
decides order. Transaction-specific impossible classes are absent rather than
assigned another value: enrollment has `AttachLifecycle` then any markers;
supersession has `BindingTerminal`, `AttachLifecycle`, then markers; a multi-
binding fate has every terminal then every marker; and a bound one-record Leave
has `MembershipExit` then any exit markers. A positional composed preserved-terminal Leave uses the preserved old major for
phase 0 and the member's `X` major for phases 1 and 4 only when no unrelated
candidate tuple lies strictly between those majors. Otherwise R-C2 drains the
terminal at its old tuple and later commits one-record `Left` at X.

The numeric ownership of a **still-unmaterialized** lifecycle claim is movable;
the obligation and its count are not. Define the order-claim frontier as the
checked, duplicate-free numeric view of persisted unmaterialized `A/X/RO/RA`
values strictly above `transaction_order` high and every immutable candidate
tuple. A direct record or caller Leave first drains every immutable earlier
candidate, except the same-owner next-gap marker cancellation defined by R-C4;
every other earlier candidate drains.

Before any **E/X-counter-backed** lifecycle fact becomes a direct record or candidate,
the serialized transaction persists one complete relay witness. Its mandatory
fallback transfers the winning logical handle onto the **absolute lowest**
numeric frontier value, even when another class or identity formerly owned that
number, and relays every surviving handle to the smallest greater unused suffix.
The old numeric owner is released when this event invalidates it or receives a
later reserved value when it survives; its logical obligation is never silently
discharged. This absolute-lowest witness must exist for every unrefusable fate or
E/X-counter-backed Leave, so arbitrary completion order cannot run out of suffix.

A transaction may instead retain a later numeric value already owned by the
winning logical class only with an exact **later-handle relocation witness**:
every lower frontier handle is either invalidated by this same fact or moved to
a distinct value above the chosen major; the resulting surviving frontier begins
at chosen-major+1 and is gap-free; every DCR block remains intact; checked
arithmetic proves no value exceeds `u64::MAX`; and the absolute-lowest fallback
would still have been legal. Thus an adjacent sole-owner bound Leave may use its
X major while invalidating the lower A, and a displayed multi-owner arm may use X
only when it names every relocated survivor. No higher-value choice is implicit.
If n handles survive at chosen major v, the checked suffix bound `v+n<=MAX`
is exactly `MAX-v>=n`, so the post-state preserves reserved capacity. Induction
then gives every later mandatory fact another finite suffix; skipped lower numbers
can only make later optional producers refuse sooner. A DCR interval may instead
be consumed or released only as a whole. Persisting the mapping and all occurrence
backing rewrites in this transaction makes the proof crash-atomic.

Eligibility is the logical proof: a binding terminal discharges `A`, `Left`
discharges `X`, recovery Attach discharges `RO`, and the replacement terminal
discharges `RA`; numeric provenance never substitutes or erases that obligation.
A bound one-record Leave discharges X and releases its now-invalid A even when
its chosen numeric major formerly backed another handle. An authoritative fate
that cannot append may instead materialize its
bounded `PendingFinalization` immediately after that immutable prefix; it appends
no record, and the lane later drains the prefix in tuple order. An optional
new-major producer—ordinary record, enrollment, attach, or supersession—instead
preflights a complete relay that makes its required caller major unreserved,
relays the old frontier above that value, consumes the now-unreserved caller
major, and then inserts its exact new claims. It
cannot discharge or erase an old logical A/X obligation as caller capacity; it
may consume a numeric value formerly owned by that claim only after the same
atomic transaction has relayed the logical handle to a greater reserved value,
so no claim count falls. A relay may permute ownership across identities and
across `A`/`X`; this is what lets either of two connections die or either member
Leave first. The old owner is removed before the new owner is written, so the
relay neither allocates an extra major nor fires an occurrence. Once a direct
record or candidate materializes, its complete tuple is immutable and is never
part of a later relay. If more than one surviving-handle mapping satisfies these
rules, the serialized allocator may select one only by persisting the complete
mapping in that same transaction; replay and every later key derive from that
persisted choice. No fixture may depend on a survivor mapping or later-handle
witness that the winning fact did not itself persist and enumerate.

The conversation has at most one DCR claim block. A relay preserves its required
shape as one consecutive `[A,RO,RA]` interval and may move that **whole still-
unmaterialized interval**, never one constituent: if that target's fate wins,
its A uses the frontier major and RO/RA become the next two values; if an
unrelated lifecycle fact wins, that fact uses the frontier major and the intact
DCR block moves immediately after it before the other movable claims. Thus
arbitrary fate/Leave order never steals only A from an accepted recovery plan or
inserts a value into its interval. Every identity-keyed Pending
BindingFateObserved/Leave occurrence names the logical claim handle, while its
serialized CandidatePosition/RecoveryAttachOrder/ReplacementTerminalOrder
backing is the handle's current value. The relay rewrites all affected Pending
backings and edge indices in the same commit. Restart therefore sees either the
entire old frontier or the entire new frontier, never mixed ownership.
After A materializes, the remaining `[RO,RA]` pair is the movable intact DCR
block: an unrelated counter-claim-backed fact may take its current lowest numeric value
only while the same transaction moves both RO and RA after that fact. It may
never move or consume just one member.

Every DCR-capable plan assigns its active `[A,RO,RA]` majors consecutively and
ahead of every unrelated A/X/candidate tuple, or first drains those earlier
candidates separately. After fate, a pending terminal at A and fenced-recovery
`Attached` at RO may compose in one transaction only when the stored fate fixed
point also proves it induces **zero** phase-4 candidates at A; adjacency alone is
insufficient. If that terminal induces any marker, the candidate lane first
commits the terminal and every same-A marker in phase/index order as separate
Q-bounded transactions. The later fenced attach then sees a durable terminal,
appends only Attached at RS/RO, and transfers its exact one-record rather than
two-record charge from `K_remaining`. With zero induced markers, the stored adjacency proves that no tuple
intervenes and the two-record composition is legal. RA remains the reserved
replacement-fate major in both paths. If an optional producer cannot assign this
adjacency and its complete zero-marker/completed-prefix alternatives,
its order/closure preflight refuses before mutation. Later work cannot borrow or
insert into the reserved interval; the whole-frontier relay above is the only
operation that may move the intact still-unmaterialized interval.

The v1 multi-binding-fate producer used by the ordering contract is that
authoritative `ServerShutdown` batch: participants in one conversation may be
bound on distinct connections, but the participant owner receives one shutdown
event and assigns their terminals and induced markers one shared causal major.
The source transaction directly appends the longest Envelope- and observer-legal
ascending-index terminal prefix of length at most `min(2,n)` (possibly zero)
and appends no marker; for every remaining binding it atomically installs the already bounded exact
`PendingFinalization` tuple. Later candidate-lane transactions drain one such
terminal at a time, then every induced phase-4 marker, in the same shared-major
order. Thus the source appends at most two records, every drain appends at most
one, and an arbitrary `n<=I` never enlarges Q. The source consumes the lowest
participating pre-owned A major, releases the other participating A values, and
compacts every surviving claim to fresh greater values in that same transaction;
it never needs an unreserved major. Per-connection FIN/error still produces
only that connection's binding for the conversation and does not silently
acquire batch semantics.

`participant_index` is the permanent ordinal of the identity reservation slot,
assigned once in the half-open range `0..<I` at enrollment. It is unique within the conversation,
persists through live membership and tombstone, and is never reused. All
candidates caused by one transaction—including every marker found by R-C4's
fixed point—share that major without aliasing. **Candidate-key uniqueness** is a
named invariant: no two live candidates or direct records may share the complete
`(transaction_order, candidate_phase, participant_index)` tuple. Commit and
restart reconstruction return R-D1 `ParticipantStateCorrupt { conversation_id,
reason: DuplicateCandidateKey { transaction_order, candidate_phase,
participant_index } }` rather than choosing an order or overwriting bytes.

**Claim-frontier validity** is a second named invariant. For DeliverySeq first
and TransactionOrder second, startup and candidate commit form the numeric union
of immutable assigned candidate values above the counter high plus the movable
claim vector, then sort it; several same-causal-major candidate tuples contribute
one TransactionOrder value. They verify that this union begins at high+1 and is
gap-free, every DeliverySeq candidate value is unique, no movable values
duplicate one another, collide with immutable values, or lie below an immutable
value, every required
logical handle appears exactly once, and every DCR interval has the
exact intact shape above. For DeliverySeq, immutable T/M candidates plus movable
claims together have exactly the R-C2 equation's class counts; for
TransactionOrder, the movable claims alone have exactly `A+X+RO+RA`, because a
materialized candidate has already consumed its claim. The first failure
returns `ParticipantStateCorrupt { conversation_id, reason:
ClaimFrontierInvalid { counter: DeliverySeq | TransactionOrder,
first_bad_position } }`. Select that checked-u128 index deterministically without
assuming identity ownership is canonical: scan actual numeric positions first
and return the lowest position with a value/gap/collision, torn DCR block,
unknown or second duplicate logical handle, or running class count above its
required total. If that scan passes but a handle/class is missing, return actual
length; if a surplus suffix exists, return expected length. For choosing a
missing handle, the non-payload tie order is sequence-budget class order
`E,T,M,RS,RT,L_times_T,L_times_RT,L_other_times_E` or order-claim class order
`A,X,RO,RA`, then ascending source and affected participant index. An empty/
nonzero mismatch is therefore 0. The comparison never forms a counter value
above u64::MAX, so a missing/extra element at the numeric maximum still has one
deterministic index.
No repair, normalization, or traffic occurs. This check makes a higher
materialized candidate above an old lower claim, a torn whole-block relay, a
duplicate, or a gap an explicit corrupt state rather than a legal restart
ordering. A stale Pending occurrence-to-frontier handle is the later
`UnbackedPendingOccurrence` check.

These frontiers add no growable side table. Order handles live in the at-most-I
binding A slots, at-most-I member X slots, and the singleton edge RO/RA fields.
Sequence E/T handles likewise live in identity slots; immutable M/terminal
values live in the at-most-I candidate cells; RS/RT live in the singleton edge;
and each conditional product is a checked range descriptor over the fixed
active-identity ranks, not an expanded `I×I` array. Numeric positions for a
product handle are derived by checked rank arithmetic. Validation merges those
fixed fields/descriptors algebraically and reports the first numeric bad
position without allocating one element per product. Thus movable ownership
changes existing bounded bytes only; its memory is O(I), I is signed and cap-
bounded, and no participant can create an unbounded frontier.

Finite ordering is backed by consumable claims, not physical-longevity prose.
This is the sole **reserved-counter-capacity** definition: every accepted future
obligation owns its required finite counter value before publication, and later
work refuses rather than borrow it. Let `A` be active binding epochs whose terminal fate has no assigned major and let
`X=L` be live members whose tokenized `Left` has no assigned major. Let `RO` be
edge-owned current-operation majors and `RA` edge-owned replacement-binding
terminal majors; both are zero or one because the one stored edge has a fixed
claim set. Leave is never accepted into separate server pending state, so every
stable live member retains its exit claim. Checked arithmetic preserves:

`order_remaining >= A + X + RO + RA`.

A DCR-capable edge owns `RO=1` for the recovery Attach transaction and `RA=1`
for the replacement epoch's future terminal. Recovery Attach consumes RO and
atomically transfers RA into A. No attach, fate, or Leave borrows these values.

One `A` major orders the binding terminal **and every marker candidate in its R-C4
fixed point** by phase/participant minor; one `X` major similarly orders `Left` and
all exit-caused markers. Those marker reconstructions share the causal major and
never allocate another one. A bound Leave consumes one of its `A`/`X` majors and
releases the other; when a binding terminal was assigned earlier, composed Leave
uses that preserved old major for the terminal and consumes `X` for the later
`Left`. Marker append, marker acceptance, normal ack, status, and candidate drain
are continuations of already ordered work and allocate no major.

Any optional allocation whose proposed post-state would make
`order_remaining' < A' + X' + RO' + RA'` returns the one canonical
`ConversationOrderExhausted` outcome. After the exact R-D1 common request
envelope, its exhaustion-specific fields are `counter:TransactionOrder`, `high`,
`next_value`, `order_remaining`, `reserved_claims:A+X+RO+RA`,
`required_majors:1`, `resulting_order_remaining`, and
`resulting_reserved_claims:A'+X'+RO'+RA'`. `next_value` is `None` only when
`high=u64::MAX` and is otherwise exactly `Some(high+1)`; all other fields are
always present. The check uses checked-wide
arithmetic before numeric allocation, mutates nothing, and may not borrow any of
the four claim classes. There is no redundant `scope` field: the common request
discriminant identifies enrollment, credential attach, or ordinary admission,
and each allocates exactly one unreserved caller major. A terminal fate consumes `A` while assigning its
immutable major and cannot be refused for order exhaustion. Supersession pays an
unreserved caller major, uses it for the old terminal/new `Attached` pair, and
transfers old `A` to the new epoch. Enrollment pays its major and creates
`A+1,X+1`; detached attach pays its major and creates `A+1`. Both check the full
resulting four-term expression before mint/bind.

Current pending finalizations and marker candidates already carry immutable
`admission_order`; draining one allocates no second major. Allocation of
`u64::MAX` is legal only when the post-transaction `A+X+RO+RA` is zero. Afterward the
conversation can still drain already ordered work and perform append-free ack/
status, but a new-major operation returns that canonical outcome with
`high:u64::MAX`, `next_value:None`, `order_remaining:0`, `reserved_claims:0`,
`required_majors:1`, `resulting_order_remaining:0`, and the operation's exact
proposed `resulting_reserved_claims:A'+X'+RO'+RA'` before mutation. That last
value is zero for an ordinary admission that creates no obligation, two for a
new enrollment (one `A'` and one `X'`), and otherwise reflects the exact attach/
supersession transfer above; inability to allocate the required major selects
the outcome independently of whether the resulting claim count is zero.
An accepted terminal
fate can never become “unsequencable.” At one billion allocations per second,
`2^64` values last about 584 years; this is physical context, not an infinity
claim, monitor, or substitute for the invariant.

Each identity slot owns at most one pending finalization and one pending marker
cell, so the candidate set is bounded by twice the signed identity-slot cap.
Crash recovery reads it directly. Observer progress or the operator recovery
event wakes the finite set; the lane drains candidates strictly by
`admission_order` before caller **record admission**. Append-free normal/marker
acks remain independently admissible under R-D1 and do not reorder the drain.
Terminal-record append, retention transition, candidate deletion, and binding-
slot release are one durable transaction. No mailbox, absence inference, poll,
sweep, or fabricated later cause participates.

**R-A3 — R1 recommendation explicitly withdrawn.** `ConnectionNotifier` remains
byte-for-byte API-compatible: do not widen `on_worker_unregistered(pid)`, do not
add participant or cause semantics to it, and do not use its compile failures as
a lifecycle inventory. It remains the worker-routing callback its source
specifies (`crates/liminal-server/src/server/connection/notifier.rs:20-52`).
`ParticipantLifecycle` is the only participant lifecycle delivery vehicle.

**R-A4 — Broadcast lifecycle records; honest observer projection.** Each
successful attach and each terminal participant event commits the corresponding
lifecycle record through R-C2. The record is the domain fact. `ParticipantLifecycle`
projects committed records **at least once with exactly-once marking**, keyed by
`(conversation_id, delivery_seq)`: it durably marks projection progress, may
redeliver after a crash, and never changes the key. The callback receives that
key and the same immutable binding epoch as the wire record. Applications requiring an
exactly-once observer effect must durably deduplicate or transactionally commit
against the key, exactly as under R-C3.

Each conversation stores a durable cumulative `observer_progress` initialized to
`0`. The projector examines records in sequence, advances across non-lifecycle
records without a callback, and advances across a lifecycle record only after
its at-least-once projection is durably marked. `observer_progress` is R-C4's
**hard retention claimant**: no compaction commit may remove a sequence above it.
If satisfying either retention cap would require that removal, every operation
listed in R-D1's fail-closed set returns typed `ObserverBackpressure` with the
operation's exact R-D1 common request envelope plus
`backpressure_epoch,observer_progress`, commits no conversation-log record, and consumes no
sequence. An authoritative fate may only commit R-A2's bounded pending state.

Add these signed nonzero limits, each with a signed default advertised in
negotiated participant capability state under the verified `LimitsConfig` pattern
(`crates/liminal-server/src/config/types.rs:193-203`,
`crates/liminal-server/src/config/types.rs:257-316`):

- per conversation: `max_parked_observer_requests_per_conversation` (`N`) and
  `max_parked_observer_bytes_per_conversation` (`C`);
- per SDK: `max_parked_conversations` (`P`),
  `max_parked_observer_requests_per_sdk` (`G`), and
  `max_parked_observer_bytes_per_sdk` (`D`); and
- per row: `max_participant_request_bytes` (`R`) and
  `max_parked_observer_row_bytes` (`B`).

`R` covers the complete encoded participant request. `B` covers the complete
durable serialization: conversation and row keys, storage framing/version/checksum,
state and length fields, encoded request, any separately indexed attempt token,
participant id, presented generation, server-captured binding epoch, refusal epoch, retry metadata, and the
fixed eight-byte `u64 park_order`. Token and identifier widths are protocol
constants. The row is a fixed-layout tagged union: reservation charges the exact
encoded request plus the largest state-variant fields for that row, so later state
transitions cannot grow its charged full-row bytes.
Let `M_row` be the codec-computed maximum non-request metadata bytes of that
negotiated fixed row schema; it includes every field just enumerated and is not
an operator estimate. Protocol v1 names this single tagged-union constant `MR`;
every row charges its exact request plus MR, not a case-selected metadata width.
The v1 row-schema generator enforces `1<=MR<=1_048_576`, so its fixed-width arithmetic fixtures cannot hide an
unbounded metadata term.

Define every recovery-handshake input and limit as an unsigned wire/configuration
`u64`: signed nonzero `P` is in `1..=u64::MAX`; v1 `RF` is exactly 16 because it
contains the ten-byte generic `FrameHeader` and six-byte
`ParticipantFramePrefix` exactly once. The raw advertised `RE`, `R`, and the
operator's raw `configured_WF` field (named `WF` in configuration)
are in `1..=u64::MAX`. After that field passes the nonzero check, the advertised,
stored, and henceforth unqualified symbol `WF` is
`min(configured_WF,10+u32::MAX)`, still an exact u64 complete-frame limit.
Define the SDK/server operation-send ceiling `R_send=min(R,WF)`. The independent
configured R remains the first recovery-request comparison so RequestBytes and
RequestWireFrameBytes stay distinguishable, while every ordinary/tokenized SDK
encode and every parked-row request-size revalidation uses `R_send`; therefore an
accepted SDK request can never be admitted locally only to exceed R-D2's frame
ceiling. `B>=R+MR` deliberately remains the stronger configured-row bound.
The codec-computed maximum complete v1 fixed client request with an empty
ordinary payload is `PR=97`: exact `CredentialAttachRequest` with a Some marker
is `16` header/prefix + `8+8+8+32+16+1+8` body bytes, and direct enumeration of
the other seven request variants is no larger. Let `PF` be the codec-computed maximum complete v1 fixed
frame across those requests, every semantic outcome and fixed recovery error,
pushed control, lifecycle/compaction delivery, empty recovery list, and
zero-payload ordinary delivery. The variable recovery request/status lists are
instead bounded by RH/SH below. Both constants include
the generic header and participant prefix exactly once and lie in
`16..=10+u32::MAX`. Capability configuration requires normalized `WF>=PF` and
`R_send>=PR`; hence every fixed mandatory/result frame is encodable before any
participant operation can commit.
The v1 schema generator enumerates every fixed variant and build-fails unless
`16<=PR<=PF<=1_048_576`; variable opaque payload and recovery-list elements are
excluded from that fixed maximum and receive the separate bounds below. This
finite enumeration is also the proof that the pre-cap ceiling can carry every
typed rejection and that symbolic PF/PR fixtures remain below the physical u32
frame ceiling.
After NonzeroLimit, advertised RE must equal its v1 codec value 16 before any
size formula is used. `RC(P)` is the codec-computed byte width of
the fixed-width network-order u64 list count, hence is exactly 8 in protocol v1
(never an estimated element count). `SE` is exactly 26: three u64 fields
(`conversation_id,refused_epoch,current_observer_progress`) plus the one-byte
`armed` and `progressed` bools. `EE` is exactly 27, the maximum encoded bytes of one
complete recovery-batch error body across `TooManyEntries`,
`DuplicateConversation`, `ConversationUnknown`, `EpochAhead`, and
`ConnectionConversationCapacityExceeded`, including every nested reason tag and
reason field but not the participant inner discriminant already counted in RF:
`EpochAhead` is the maximum at `reason:u16 + conversation_id:u64 +
presented_epoch:u64 + Some-tag:u8 + current:u64 = 27`; the other four bodies are
no larger. Success and error responses reserve
the same fixed envelope/count prefix: accepted `0x0121` carries its actual u64
status count, while error `0x0122|0x0123|0x0124` carries an exact zero u64 count,
after which the body is respectively at most `P×SE` or `EE`. Compute

`RH(P) = u128(RF) + u128(RC(P)) + (u128(P) × u128(RE))`

`SH(P) = u128(RF) + u128(RC(P))
       + max(u128(P) × u128(SE), u128(EE))`

in checked u128. Compare RH to `u128(R)` and then normalized `u128(WF)`, and
compare SH to normalized `u128(WF)`; every dimension-tagged payload's
`wire_frame_limit` is that normalized exact u64 value. Thus a configured u64 WF
above the generic header's physical ceiling never admits an unencodable frame.
Let `U=u64::MAX`. RH is at most `24+16U`; SH is at most
`24+max(26U,27)=24+26U`. Each is strictly below u128::MAX, so arithmetic overflow is therefore
unreachable and has no outcome discriminant. These are codec-schema maxima, not
runtime estimates; their fixed request widths are included in R/B accounting
and their success/error response widths in WF.

Startup and first participant-capability negotiation first reject configuration
shape as `ParticipantParkingConfigurationInvalid { dimension, operands }`. Its
first-precedence dimension is `NonzeroLimit`; its `operands.field` names the
first zero in the fixed field order `N,C,P,G,D,R,B,RE,WF`; that last label means
raw `configured_WF`. Only proposals with all nine fields nonzero continue and
then normalize WF as above. The remaining dimensions are, in order,
`RecoveryEntrySchemaBytes` for `RE!=16`, `WireSchemaBytes` for `WF<PF`,
`RequestSchemaBytes` for `R_send<PR`, `RowSchemaBytes` for `B < R + MR`;
`CheckedProduct` for overflow in any validated product; `RowBytesBound` for
`C > N × B`; `SdkBytesBound` for `D > G × B`; and `RecoverableSlots` for
`P > max_participant_conversations_per_connection`. Its dimension-tagged
`operands` union is exactly one of:

- `NonzeroLimit { field: N|C|P|G|D|R|B|RE|WF, actual: 0,
  required_minimum: 1 }`;
- `RecoveryEntrySchemaBytes { actual: RE, required: 16 }`;
- `WireSchemaBytes { actual: WF, required: PF }`;
- `RequestSchemaBytes { configured_request_limit: R, wire_frame_limit: WF,
  actual: R_send, required: PR }`;
- `RowSchemaBytes { request_limit: R, row_metadata_bytes: MR, actual: B,
  required: u128(R)+u128(MR) }`;
- `CheckedProduct { operation: Multiply, left, right, checked_result: None,
  overflow: true }` for the first `N×B`, then `G×B`, product that is not
  representable as `u64`;
- `RowBytesBound { left: N, right: B, checked_product: N×B, actual: C }`;
- `SdkBytesBound { left: G, right: B, checked_product: G×B, actual: D }`; or
- `RecoverableSlots { actual: P,
  limit: max_participant_conversations_per_connection }`.

The dimension and fixed check order make those variants disjoint; no optional
operand field or untagged operand bag exists. These shape checks precede
recovery-handshake size. Only
a shape-valid proposal then checks `RH(P) > R`, `RH(P) > WF`, and `SH(P) > WF`
in that order as
`ParticipantRecoveryHandshakeTooLarge {
max_entries: P, framing_bytes: u128(RF) + u128(RC(P)),
request_entry_bytes: RE, response_entry_bytes: SE,
error_response_bytes: EE, request_encoded_bytes: RH(P),
response_encoded_bytes: SH(P), request_limit: R, wire_frame_limit: WF,
dimension: RequestBytes | RequestWireFrameBytes | ResponseWireFrameBytes }`.
Thus every accepted configuration can encode the complete one-shot recovery
request and its maximum success or batch-error response. The configured `C` and `D`, not their products, remain
independently binding exact full-row byte ceilings. Checked validation does not
unacceptably constrain legal P: P already means the maximum conversations one
connection must recover atomically, so R12 deliberately adopts **no chunked
handshake** and introduces no per-chunk arming race.

Before sending an operation that can backpressure, the SDK performs one atomic
admission in this exact first-failure order: (1) encode the complete request and
compare its bytes with `R_send`; (2) if this is the conversation's first row, compare
the SDK-wide parked-conversation/interest count with `P`; (3) compare that
conversation's row count with `N`; (4) compare its charged full-row bytes with
`C`; (5) compare SDK-wide row count with `G`; (6) compare SDK-wide charged
full-row bytes with `D`; and (7) allocate `park_order`. A request above `R_send`
returns local `SdkParticipantRequestTooLarge { conversation_id, encoded_bytes,
limit:R_send }`. The first-row slot remains counted until the last row is deleted. A
capacity failure returns
`SdkObserverParkCapacityExceeded { scope: PerConversation | SdkWide, dimension:
Conversations | Rows | Bytes, conversation_id, limit, occupied, requested }`; no
row/frame/server state is created, and an empty newly reserved interest slot is
rolled back. The `requested` byte amount is the exact full-row charge, not request
bytes. Exactly five scope/dimension pairs are constructible:
`PerConversation/Rows`, `PerConversation/Bytes`, `SdkWide/Conversations`,
`SdkWide/Rows`, and `SdkWide/Bytes`; `PerConversation/Conversations` is not a
value of this outcome. Simultaneous failures select the numbered check above. Park-order
exhaustion is last and likewise rolls back a just-reserved first-row slot. A
non-backpressure or terminal unknown-fate transition releases the
row (`RecordAdmissionUnknown` does so); deleting the last row releases its slot.
Tokenized response loss keeps one counted row only while exact token fate remains
recoverable. All table/set/counter storage is therefore bounded by `P`, `G`, `D`,
`N`, and `C`; no connection lifetime can accumulate another unbounded set.

`park_order` is a checked `u64` stored with every row. When a conversation's
parked-row set is empty, the next reservation transaction may reset its durable
counter to zero and allocate zero; this is its **only** reuse rule. Otherwise it
checked-increments. Values through `u64::MAX` are allocated once; allocating MAX
sets an exhausted flag, and the next nonempty-set reservation returns terminal
local `SdkParkOrderExhausted { conversation_id, counter: ParkOrder, value:
u64::MAX }`, reserves nothing, and never wraps, saturates, aliases, or rebases.
At a sustained billion allocations per second, exhausting all `2^64` values takes
about 584 years. Deployment-honest interpretation uses the actual per-conversation
allocation rate and remaining values; this arithmetic justifies a terminal edge,
not “infinite,” and creates no periodic monitor or rebase mechanism.
The reservation transaction persists the complete encoded request, token,
authority, byte accounting, and order in known-never-sent `Reserved`. The other
states are `AwaitingObserverProgress { refused_epoch }`, `RetryAuthorized`,
`InFlight`, and `TokenFateRecovery`.

Normal sending and restart use one lowest-`park_order` sender per conversation.
Before any network write it revalidates current authority and durably changes the
row to `InFlight`. On restart, one transaction changes every `Reserved` row to
`RetryAuthorized`: those rows are known never sent because no write may precede
the `InFlight` commit. They then enter the same ordered sender. Restart drains
`RetryAuthorized`, recovers `InFlight`/`TokenFateRecovery`, and never skips an
earlier blocked row. An `InFlight` crash uses the operation's attempt token; an
untokenized ordinary operation returns `RecordAdmissionUnknown`, deletes its row,
and is never resent. It never assumes that an unobserved write failed.

`ObserverProgressed { conversation_id, refused_epoch, observer_progress }` is a
connection-level control event, not a promise that a particular request now fits.
A backpressure epoch is the immutable `observer_progress` value at refusal in the
same checked non-wrapping sequence domain. Every refusal at an unchanged baseline
returns the same epoch; actual progress makes every later refusal strictly greater.
No historical epoch map is retained. An SDK that has durably consumed progress E
must reject any transition that would demote a row to
`AwaitingObserverProgress(E' <= E)`; it instead performs the serialized status or
operation retry that obtains a current baseline.

The server pushes the event to every live connection that received the epoch's
refusal, including pre-enrollment and unbound replacement-attach connections. One
fixed-size `(conversation, epoch)` interest covers all bounded rows at that
baseline. The SDK's durable interest slot is reserved before its first row, while
the server's connection-local recipient is installed on refusal or reconnect;
connection loss discards only the server copy, never the SDK reservation. The
negotiated signed `max_participant_conversations_per_connection` bounds attached
conversations plus refusal-only recipients; a first live-connection conversation
beyond it returns `ConnectionConversationCapacityExceeded` before mutation.

The exact one-shot `ObserverRecoveryHandshake` wire body is
`{ observer_refusals: [{ conversation_id, refused_epoch }] }`. The whole request
is validated without mutation in this order: a length above P returns
`InvalidObserverEpochList { reason: TooManyEntries, presented_entries,
max_entries: P }`; otherwise the first repeated conversation by request index
returns `InvalidObserverEpochList { reason: DuplicateConversation,
conversation_id, first_index, duplicate_index }`. Then preflight connection-
conversation capacity by traversing the unique list in request-index order from
the current occupancy: an already tracked conversation adds zero; each first
untracked conversation tentatively adds one; and the first entry that would
exceed the limit returns `ConnectionConversationCapacityExceeded` naming that
entry, with no partial slot or arm. Then snapshot and validate every entry in
request order. The first absent server conversation returns
`InvalidObserverEpoch { reason: ConversationUnknown, conversation_id,
presented_epoch, current_observer_progress: None }`; the first known entry whose
presented epoch is newer returns `InvalidObserverEpoch { reason: EpochAhead,
conversation_id, presented_epoch, current_observer_progress: Some(current) }`.
Neither error arms any of the batch. Only after all entries pass does one
transaction install all equal arms and return the single named success
`ObserverRecoveryAccepted { statuses: [ObserverProgressStatus] }`, with statuses
in request order. A valid empty request returns that success with `statuses:[]`.
The list has exactly the request's entry count, so its encoded response is at
most `SH(P)<=WF`; each earlier whole-batch error body is at most `EE` and hence
has the same bound. Every response is one bounded frame and has no
partial-response mode.
Older entries return progressed/unarmed without disturbing a newer pre-existing
arm; equal entries atomically subscribe then snapshot. Because `P` is validated against the
recoverable connection slots, `RH(P)` is validated against both R and WF, and
`SH(P)` is validated against WF,
every Awaiting conversation can be included in one encodable handshake. Equality
linearizes either after progress (older branch) or before it (installed recipient
receives the push), never between both.

One matching signal/status atomically changes the entire matching
`AwaitingObserverProgress` cohort to `RetryAuthorized` before any send. Restart
before that transaction repeats reconnect comparison; restart after it resumes
ordered drain. Terminal results delete rows. Repeated refusal may rewrite a row
only to a strictly newer epoch, without changing `park_order`. Crashes before
cohort marking, between rows, or after writes are determined by durable state.

Current-authority invalidation, stale/expiry, `Retired`, or
`CredentialRecoveryLost` atomically visits **all** bounded row states, including
`Reserved`. Known-never-committed rows terminalize with the typed authority result
and delete; an in-flight tokenized request or accepted pending detach becomes
`TokenFateRecovery` only while exact fate remains recoverable. Stable committed,
stale, retired, expiry, or `StaleOrUnknownReceipt` deletes it. No old request is
silently reissued under new authority. Normal and marker acks never park.

Lifecycle phase, not check order, selects the recovery-handshake outcome. The
**initial phase** is server startup validation or a connection's first participant-
capability negotiation, before it owns any durable parked row; it uses
`ParticipantParkingConfigurationInvalid` for shape and, only after shape passes,
`ParticipantRecoveryHandshakeTooLarge` for RH size. The **parked phase** is restart or
renegotiation whose serialized pre-validation snapshot contains at least one
parked row; it uses only `SdkParkingCapacityIncompatible`. The phase bit, row snapshot, validation, and configuration/handshake commit all
run in one SDK-wide serialization domain. That same domain contains every row
reservation or state mutation, the sender's `Reserved|RetryAuthorized→InFlight`
commit and write authorization, response/progress deletion, and every authority
or expiry visit. None can interleave, so both outcomes can never race for one
proposal and a rejected proposal can send or delete no snapshotted row. Restart/renegotiation with no
parked rows is initial phase.

For one server-startup/first-capability proposal that contains more than one
invalid configuration family, the global pre-participant selector is also
fixed: parking shape, recovery-handshake size, receipt/identity capability,
startup keepalive certification, then retention capacity. The first family's
named outcome wins and its internal order above or in R-C0/R-B2/R-C4 selects its
fields; later families are not evaluated for outcome selection. Refusal occurs
before the participant listener, handshake bytes, identity, row, or conversation
state exists. A parked-phase SDK renegotiation is deliberately narrower: it uses
only the complete `SdkParkingCapacityIncompatible` order below and cannot mask a
server-static startup failure.

In parked phase, validation checks every dimension before sending any row in
this exact order: `NonzeroLimit` in the same fixed nine-field order;
`RecoveryEntrySchemaBytes`, `WireSchemaBytes`, `RequestSchemaBytes`,
`RowSchemaBytes`, `CheckedProduct`, `RowBytesBound`, `SdkBytesBound`, and
`RecoverableSlots`; per-conversation row counts in ascending `conversation_id`;
per-conversation byte counts in that order; SDK parked-conversation count; SDK
row count; SDK byte count; each stored request length against new
`R_send=min(new R,new WF)` in
ascending `(conversation_id,park_order)`; then `RH(P)` against new R, `RH(P)` against new WF,
and `SH(P)` against new WF. The same
shape failures map to the correspondingly named
`SdkParkingCapacityIncompatible` dimensions, with exact operands. Any
violation returns `SdkParkingCapacityIncompatible { dimension, operands }`,
where `operands` is exactly one dimension-tagged variant:

- the nine shape dimensions reuse the complete shape-operand union above;
- `ConversationRows|ConversationBytes { conversation_id, occupied, limit }`;
- `SdkConversations|SdkRows|SdkBytes { occupied, limit }`;
- `RequestBytes { conversation_id, park_order, actual, limit: R_send }`; or
- `RecoveryHandshakeRequestBytes|RecoveryHandshakeRequestWireFrameBytes|
  RecoveryHandshakeResponseWireFrameBytes { max_entries: P,
  framing_bytes: u128(RF)+u128(RC(P)), request_entry_bytes: RE,
  response_entry_bytes: SE, error_response_bytes: EE,
  request_encoded_bytes: RH(P), response_encoded_bytes: SH(P), limit }`, where
  `limit` is respectively `R`, `WF`, or `WF`.

The dimension itself supplies the shape, conversation, SDK-wide, row, or
recovery context; this outcome has no separate `scope` field.
There is deliberately no parked `RowBytes` dimension: after
`RowSchemaBytes` proves `B>=R+MR` and RequestBytes proves each stored
`request_bytes<=R_send<=R`, every exact row is
`request_bytes+MR<=R+MR<=B`. A RowBytes trigger is therefore algebraically
unreachable, and the tighter proof removes rather than names it.
All sum/product and recovery encoded-size fields above retain their stated
`u64`/`u128` domains. Existing parked rows are
preserved even when a downward R/WF change would strand them; no partial
handshake is sent. Every violation preserves every row and sends none until
operator/configuration correction. `Reserved` rows and interest
slots participate. It never grandfather-discard rows. The only request-sized
waiter is this SDK-wide and per-conversation bounded set; every retry is TOLD by
startup, connection readiness, response, observer progress, or authority change,
never a timer, poll, or sweep.

**Idle-cost closure.** Per SDK, parked conversation/interest headers are bounded by
signed P, rows by signed G, and fully serialized bytes by signed D; per-conversation
subsets are additionally bounded by N/C and each row by R/B. Per server
conversation, `ClosureDebt`, its fixed-size edge-claim bitmap/counters, and
`transaction_order` are fixed header fields; K is unavailable occupancy inside the
existing signed cap, not stored bytes. Binding epoch, request verifier, and order/exit-claim tags live in already bounded
identity/receipt cells. Attach/enrollment verifier life is TTL/cap bounded;
detach verifier life is identity-cell bounded; Leave verifier bytes were reserved
in the identity slot and remain inside its permanent bounded tombstone. Startup recovery reuses binding/finalization/detach/
debt cells and creates only the already-owed terminal. Marker/finalization/detach
cells are bounded by signed I; retained log bytes/entries by their signed caps;
and connection-local binding/interest/dispatch maps by signed
`max_participant_conversations_per_connection`. Resettable
`park_order` is one fixed header per currently parked conversation. No new R12
table, set, counter, cell, recipient interest, or recovery token exists outside
one of those bounds.

An arbitrarily long observer outage therefore remains bounded: admissions refuse
and lifecycle facts remain reconstructible. A deterministic callback failure at
sequence K may freeze the conversation indefinitely; replacement/retirement
without advancing past K is the genuine owner socket
`«WEDGED-OBSERVER-POLICY»`. Implementations may not skip K, weaken the hard claim,
or invent automatic observer retirement.

### Silence-attacking acceptance frame

Exercise successful attach, explicit detach, explicit Leave, clean Disconnect,
EOF, read error, write error, kernel expiry, protocol error, linked-EXIT, local
force-close, authorized supersession, external termination, and server shutdown.
Assert exactly one correctly classified ordered lifecycle record for each
participant-domain finalization. Crash after record commit but before observer
notification: after recovery the event is observed at least once, and every
copy has the same `(conversation_id, delivery_seq)`. Crash after callback but
before projection-progress persistence: assert duplicate observation with that
same key and a dedupe-able single effect. Stop observers for two conversations,
admit until both byte and entry caps would be exceeded, and prove further commits
receive typed `ObserverBackpressure` without sequence consumption while every
unprojected lifecycle record remains retained. Recover one observer: its own
progress event permits only its conversation to admit again, with all pending
callbacks delivered at least once; no timer or sweep participates. At the cap,
submit a valid authoritative Leave, lose its `ObserverBackpressure` response, and
replay: prove the same epoch and no `Left`. Recover the observer, receive one
`ObserverProgressed`, retry the same Leave token once, and prove one `Left`.
At the cap, separately trigger explicit detach, Disconnect, EOF, read failure,
write failure, linked-EXIT/process termination, shutdown, and protocol error;
crash before observer recovery. For each, prove the bounded slot is durably
`PendingFinalization` with original cause/binding-epoch/order, the binding has no
remaining authority, progress wake appends exactly one correctly ordered record,
and no absence inference, timer, poll, or duplicate participates. Then make
callback K deterministically fail across restart; prove progress holds at K, all
appending operations fail closed, and only `«WEDGED-OBSERVER-POLICY»` can
authorize replacement—never a silent skip.
Separately force the worker-registration storage rollback at
`crates/liminal-server/src/server/connection/apply.rs:205-213`; assert the
worker compensation callback runs while **zero** participant events or records
are produced. A reviewer refutes this section by finding a fake cause on
rollback, an active binding with no terminal record, two terminal records for
one binding, a redelivery with a changed key, an exactly-once observer-effect
promise, or participant authority routed through `ConnectionNotifier`.

## 3. Section (b) — connection fate, with honest liveness classes

### Constraint first

**NO-POLLING:** no application-layer poll loops anywhere in the product. Crash
detection remains event-driven—linked-EXIT in-VM, connection-fate
cross-process. No heartbeat frame, periodic sweep, scan, synthetic liveness
write, or timer whose job is to ask whether state changed.

R12 retains two different liveness classes:

1. **Class 1 — host/network/TCP-path death.** Host down, network partition or
   black hole, socket close, FIN/EOF, and fatal socket errors are connection-fate
   failures. Kernel machinery may bound silent no-FIN detection.
2. **Class 2 — wedged process with a live TCP stack.** SIGSTOP, deadlock, and
   livelock are **excluded from connection-fate detection by design**. A live
   kernel may continue acknowledging TCP probes while userspace does nothing;
   this layer must not manufacture a death verdict.

### Proposed contract

**R-B1 — Kernel connection fate, Class 1 only.** Enable `SO_KEEPALIVE` on every
participant TCP socket and configure per-socket idle, probe interval, and probe
count. The kernel owns probe scheduling; userspace parks on the existing
readiness path (`crates/liminal-server/src/server/connection/process.rs:193-212`).
Read EOF/error is confirmed on the read path
(`crates/liminal-server/src/server/connection/process.rs:238-269`); fatal write
outcomes come from the outbound writer
(`crates/liminal-server/src/server/connection/outbound.rs:174-235`) and tear down
through the process drain path
(`crates/liminal-server/src/server/connection/process.rs:169-182`). HUP is not
claimed as an independent branch in those spans: readiness must be confirmed by
EOF or error. This corrects R1's citation overreach.

**R-B2 — Owned, certifying socket adapter.** Add a direct production `socket2`
dependency with the explicitly required feature set; the current server lists
`socket2` only under dev-dependencies
(`crates/liminal-server/Cargo.toml:11-36`), though the workspace declares 0.6
(`Cargo.toml:38-46`). One owned adapter configures accepted sockets before
connection-process spawn; current production setup only calls
`set_nonblocking(true)` there
(`crates/liminal-server/src/server/connection/supervisor.rs:744-765`). The
adapter must:

- validate nonzero idle seconds, interval seconds, and probe count against the
  target's range and granularity;
- set `SO_KEEPALIVE` plus all three target options exactly once;
- read back enablement and all three effective values; and
- refuse participant mode at startup or accept when exact certification is
  unavailable—never round, truncate, or silently fall back.

Every refusal is `KeepaliveCertificationFailed { phase, reason }` with exactly
one reason-tagged body:

- `Zero { field: idle_seconds|interval_seconds|probe_count, requested:0,
  required_minimum:1 }`;
- `OutOfRange { field, requested, supported_min, supported_max, platform }`;
- `GranularityMismatch { field, requested, granularity, platform }`;
- `UnsupportedPlatform { platform }`;
- `UnsupportedOption { option: SO_KEEPALIVE|idle|interval|count, platform }`;
- `SetFailed { option, platform, os_error }`;
- `ReadbackFailed { option, platform, os_error }`; or
- `ReadbackMismatch { option, requested, effective, platform }`, where
  `requested`/`effective` are `Enabled(bool)` for `SO_KEEPALIVE` and
  `Unsigned(u64)` for the three numeric options.

The first five reasons require `phase:StartupConfiguration`; the final three
require `phase:AcceptedSocket`. No variant carries any other variant's field,
and no optional/sentinel field bag exists. First test `Zero`
for `idle_seconds`, `interval_seconds`, then `probe_count`; the first zero wins
without needing a platform descriptor. Next resolve the signed participant
keepalive descriptor for the target; absence returns `UnsupportedPlatform` and
no target-specific range predicate is fabricated. On a supported target,
validate fields in that same order, testing `OutOfRange` then
`GranularityMismatch` for one field before the next. Thus simultaneous nonzero
invalid fields select the earliest field and OutOfRange precedes Granularity for
that field. Only after all three values pass, test option support in fixed order
`SO_KEEPALIVE`, `idle`, `interval`, `count`; the first missing option returns
`UnsupportedOption` naming it. Only after all four support checks pass do set
and then readback run in that same fixed option order: the first set OS error is
`SetFailed`; after every set succeeds, each readback tests OS error
`ReadbackFailed` before effective-value `ReadbackMismatch`, then advances to the
next option. This is the complete
selector even when zero, platform, range, option, set, or readback failures
coexist; the first predicate in this paragraph wins.
Startup failure opens no participant listener. Accepted-socket failure closes that
socket before negotiation, binding, receipt, identity, or participant state and
does not fall through to a lifecycle cause.

The fields extend the named/defaulted/nonzero `LimitsConfig` pattern verified in
§1. Exact signed defaults, platform mappings, and macOS/Linux worst-case formula
remain the genuine evidence socket `«NO-FIN-KERNEL-BOUND»` together with
`«KEEPALIVE-PORTABILITY»`; R12 does not manufacture numbers.

**R-B3 — Honest Class-1 bound.** The advertised detection bound is the supported
platform's documented worst case for the read-back configuration, including
option granularity and retry semantics. A black-holed peer is not promised
detection before that machinery expires; an independently authoritative EOF or
socket error may report earlier. Unsupported or uncertifiable platforms refuse
participant mode.

**R-B4 — Ordinary writes are additional events, never probes.** An application
send may surface connection failure immediately; the outbound writer treats a
zero write or unrecoverable error as fatal
(`crates/liminal-server/src/server/connection/outbound.rs:219-235`). The server
must not generate a send merely to test liveness.

**R-B5 — Class-2 truth is cursor stall, not death.** A wedged participant stops
acking, so its R-C3 cursor freezes and retention pressure accrues. If compaction
overtakes it, `HistoryCompacted` on eventual return is the truthful record. This
layer emits no `ConnectionLost`, no wall-clock liveness score, and no synthetic
probe for SIGSTOP/deadlock/livelock. Eviction, alerting, or other wedged-process
semantics belong to the layer above under `«WEDGED-PARTICIPANT-POLICY»`.

**R-B6 — Product polling retirement is a LAW-1 prerequisite.** Every loop named
in §1 must retire: main/health accepts become blocking or readiness-driven with
explicit shutdown wakes; membership deltas are pushed; process/connection exit
signals drain completion; and an admitted shutdown deadline races that
notification. Channel commands race reply, pushed process exit, and their one
admitted deadline. SDK push/subscription readers race socket readiness with an
explicit local shutdown wake, never a read timeout used to sample a stop flag.
Push reply races reply/connection fate/one deadline; subscription setup races
input against one total deadline; and the durability bridge performs one asserted
synchronous poll or uses an executor honoring its waker. Rust/Gleam reconnect-
delay calls additionally require a fresh transport-fate event per attempt and
return the exact local `ReconnectDelayResult`; a call without its single-use
permit returns `ReconnectNotArmed` without mutation. Repeated caller timer re-
arm is nonconforming. The implementation
dependencies are §7 sockets. Retention, replay, lifecycle, startup recovery,
epoch rewrite, detached Leave, fenced recovery, order/debt repayment, and reader
shutdown may not add polling under another name.

### Explicit non-goals

- No application heartbeat frames, heartbeat sweeper, or synthetic liveness send.
- No periodic accept, reap, replay, cursor, or lifecycle scan.
- No wall-clock liveness score.
- No connection-fate claim for a wedged process with a responsive TCP stack.

### Silence-attacking acceptance frame

With certifiable test values, inject a Class-1 black hole/host-loss fault and
assert `ConnectionLost` no later than the documented kernel worst-case bound
plus scheduler/test tolerance; an authoritative earlier socket failure may win.
Then SIGSTOP a participant while its host and socket remain live: assert **no
`ConnectionLost`, no death lifecycle record, and no forced cursor advance**.
Resume it and observe either normal continuation or the same participant's
`HistoryCompacted` outcome if retention passed it. Inventory main/health accepts,
cluster membership, shutdown drain/settle, channel reply waits, both SDK TCP
readers, and every participant thread, timer, task, and write: none may
periodically ask whether state changed; each named §1 loop is absent and its
reply/process-exit/deadline, readiness/shutdown, or domain-event race is exercised. The false-positive assertion for Class 2 is as
load-bearing as Class-1 detection.
Independently exercise each `KeepaliveCertificationFailed` reason, including
zero and simultaneous invalid values (the fixed field order wins), every target
range/granularity boundary, zero on an unsupported platform (Zero wins), nonzero
out-of-range values on an unsupported platform (UnsupportedPlatform wins before
an unavailable range), supported-platform range failure combined with an
unsupported option (the range failure wins), every pair of unsupported options
(fixed option order wins), set/readback errors, and exact readback mismatch.
Assert the complete payload and that no participant
frame, identity, binding, candidate, or lifecycle fact exists.

## 4. Section (c) — authorized registration and server-backed cursor/replay

### Proposed contract

**R-C0 — Write-ahead idempotent participant transactions and bounded receipts.**
Every enrollment attach, credential-bearing attach, explicit detach, and Leave
carries a client-generated, unguessable, single-purpose attempt token. The SDK
**must durably persist the token before sending the request** and re-present that
same token after response loss or a crash. Attach keys are
`(conversation_id, enrollment_token)` before mint and
`(conversation_id, participant_id, attach_attempt_token)` thereafter; detach and
Leave use `(conversation_id, participant_id, detach_attempt_token)` and
`(conversation_id, participant_id, leave_attempt_token)`. All serialize on the
same participant state. A duplicate commits no second capability, binding,
lifecycle record, rotation, cursor transition, or retention claim.

The exact v1 tokenized request schemas are:

- `EnrollmentRequest { conversation_id, enrollment_token }`;
- `CredentialAttachRequest { conversation_id, participant_id,
  capability_generation, attach_secret, attach_attempt_token,
  accept_marker_delivery_seq: Option<DeliverySeq> }`;
- `DetachRequest { conversation_id, participant_id, capability_generation,
  detach_attempt_token }`; and
- `LeaveRequest { conversation_id, participant_id, capability_generation,
  attach_secret, leave_attempt_token }`.

Receipt recovery is not a second wire shape: it is byte-identical retransmission
of the original `EnrollmentRequest` or `CredentialAttachRequest`. Detach and
Leave replay likewise retransmit their exact original frame. The receiving
connection and prospective binding epoch are serialized commit context, never
request fields and never verifier input.

**Canonical live-token verifier.** Every live token record stores a fixed-size
constant-time `request_verifier` over the protocol-versioned canonical request.
Canonicalization is the unique wire-field order with normalized optional tags.
Credential attach and Leave are the only secret-bearing operations: their verifier
combines a constant-time secret-proof verifier with a non-secret fingerprint over
operation discriminant, conversation, participant, presented generation, and
every operation field. Credential attach necessarily covers
`accept_marker_delivery_seq: Option<DeliverySeq>`. Enrollment's complete body is
its lookup key, so it has no same-key body-conflict arm. Detach is deliberately
non-secret-bearing and has no non-key field other than generation; an exact-token
generation mismatch is `StaleAuthority`, not body conflict. Exact binding epoch
plus current generation supplies authority. Adding a detach secret would change
the protocol and is forbidden. Padding, map order, or alternate encodings cannot create a second
canonical body.

While the identity is live, a live receipt, detach cell, or parked token-fate row
checks its verifier before returning that exact-token result. A canonical match
replays the stored result. For a secret-bearing token, secret-proof mismatch is
tested first and returns `StaleAuthority` without revealing a receipt, even when
non-secret fields also differ. With a matching secret proof, attach conflict
classes are tested in fixed order `Generation`, then `MarkerDeliverySequence`;
the first mismatch returns R-D1 `AttemptTokenBodyConflict { token,
operation: CredentialAttachRequest|LeaveRequest, conversation_id,
presented_participant_id, presented_generation,
presented_marker_delivery_seq: Option<DeliverySeq>,
conflict: Generation | MarkerDeliverySequence }`;
participant id and generation are required in both operation variants. The
marker-option field is present only for `CredentialAttachRequest` (where the
Option tag itself is canonical) and absent for `LeaveRequest`; Leave can select
only `Generation`.
It mutates nothing and discloses neither stored body, credential,
nor receipt. `conflict` reports only that first differing field class.

Retirement changes that lookup order. Every enrollment/attach/detach token first
resolves its participant identity and then receives ordinary tombstone `Retired`;
it cannot replay a pre-Leave live result. The sole verifier-before-tombstone
exception is the tombstone's exact committed Leave token. For that token,
secret-proof mismatch first returns the R-D1 Leave `StaleAuthority` union's
`CommittedLeaveTombstone` variant; with matching secret proof,
generation mismatch returns `AttemptTokenBodyConflict` with the complete R-D1
Leave body and `conflict=Generation`;
and only the byte-identical canonical body returns the permanent
`LeaveCommitted`. A different Leave token receives `Retired`. Leave has no marker
field, so `MarkerDeliverySequence` is unreachable for that operation.

After an attach/enrollment live receipt expires, its verifier and secret body are
deleted. The separately bounded credential-attach provenance row is reached only
by its original lookup key `(conversation_id,participant_id,attach_attempt_token)`
and then classifies the exact token fingerprint plus stored request/result
generations. Any retransmission preserving those three key fields receives the
row's `ReceiptExpired` even if generation, secret, or marker option differs; a
changed key field performs a different lookup and cannot reach that row. After
provenance, the same original-key body receives `StaleOrUnknownReceipt`, without
pretending to reverify fields that no longer exist. Enrollment's permanent token mapping likewise keys only on
conversation plus enrollment-token fingerprint. It resolves the participant,
then returns `Retired` if that identity is tombstoned or `EnrollmentKnown` only
while it remains live and non-retired. Thus no provenance-only or permanent-
mapping result is gated by a discarded verifier.

The verifier is stored with the already counted live receipt, detach cell,
bounded parked token-fate row, or successful Leave tombstone. Receipt TTL/caps
bound attach/enrollment verifiers; the identity-slot cell bounds detach. The
permanent retirement slot reserves the fixed maximum Leave verifier bytes at
enrollment, and Leave transfers that charge into its bounded tombstone without a
new capacity decision. The tombstone retains the non-reversible secret-proof
verifier, canonical fingerprint, token, and `LeaveCommitted` fields, never the
secret or body. Its lifetime is exactly the tombstone's permanent lifetime. This
verifier is distinct from R-C0's provenance fingerprint and creates no table,
sweep, or idle-cost dimension.

**Bounded detach replay.** Each reserved identity slot has exactly one fixed-size
durable tagged cell for the current-or-most-recent binding:

- `Empty`;
- `Pending { token, participant_id, request_generation, request_verifier,
  committed_binding_epoch, admission_order, refused_epoch }`; or
- `Committed { token, participant_id, request_generation, request_verifier,
  committed_binding_epoch, detached_delivery_seq }`.

An immediately admissible explicit detach performs terminal append, retention-
floor transition, old-cell replacement, binding release, and
`Empty | Committed(old) → Committed { token, participant_id,
request_generation, request_verifier,
committed_binding_epoch, detached_delivery_seq: allocated }`
in **one** durable transaction. Thus the ordinary success path, not only recovery,
is a normative `Committed` producer; a crash immediately before sees the old
binding/cell, and a crash after sees the complete new record/cell.

If the terminal record cannot yet append, acceptance ends transport/cursor
authority and atomically changes the binding to R-A2 `PendingFinalization` **and**
the cell to `Pending`; no delivery sequence exists or is fabricated. Detach-cell
classification precedes binding lookup: the exact token selects this Pending
status, while a different token against Pending returns only non-secret
`DetachInProgress { conversation_id, participant_id, presented_token,
presented_generation,
committed_binding_epoch }`, commits nothing, and never reveals the stored token. A
nonmatching token against `Committed` receives no special status and continues
through the ordinary tombstone/generation/binding order.

Exact-token Pending replay compares `refused_epoch` with current
`observer_progress` at the serialized point. On equality it returns the current
cell status unchanged. If progress is greater, it first attempts the ordered
candidate drain. Success performs terminal append, floor transition, candidate
removal, binding-slot release, and `Pending → Committed` with the real allocated
sequence in one transaction. If still blocked, one transaction replaces the
cell's `refused_epoch` with current progress, installs that epoch's recipient
interest, and returns `ObserverBackpressure` with the **new** cell epoch. The SDK
must not rewrite a row to an epoch at or below progress it already consumed. A
progress/replay race therefore either drains or arms the current epoch, never
reuses a spent wake. A crash before, during, or after either transaction exposes
only one complete Pending or Committed state.

The cell—not live binding state—is the sole source of every stable
`DetachCommitted` echo, so binding-slot reuse or disappearance cannot alter its
binding epoch or sequence. Stability lasts only until the identity's next
successful attach or Leave. Attach atomically clears the cell and terminalizes
its old token as `StaleAuthority { authority_state:TerminalizedDetachCell,
conversation_id,participant_id,capability_generation,detach_attempt_token,
current_generation,committed_binding_epoch,binding_state }`, where
`binding_state` is either `Bound { current_binding_epoch }` or `Detached`; no
current epoch is fabricated for a gone replacement binding. Leave replaces the
cell result with tombstone-precedence generic R-D1 `Retired` under the replayed
detach envelope plus `retired_generation`.
Thus cycling stores one cell. After response loss the SDK replays while no newer
attach/Leave is durable. A newer `AttachBound` makes the SDK record
`AuthoritySuperseded` and never resend; later stale replay is not evidence detach
failed. `Retired` terminalizes while preserving operator identity.

Every live attach receipt contains `{ participant_id,
request_generation: Option<u64>, capability_generation,
attach_secret, origin_binding_epoch, persisted_cursor,
accepted_marker_delivery_seq: Option<DeliverySeq>, receipt_expires_at,
provenance_expires_at }`. Enrollment stores `request_generation=None`,
`persisted_cursor=0`, and `accepted_marker_delivery_seq=None`; credential attach
stores `request_generation=Some(the presented generation)`, with ordinary attach
storing the existing cursor/`None` and fenced recovery storing the accepted marker
sequence in both the cursor and the `Some` field. Exact replay returns
the replay discriminant `Bound` only when the origin connection/conversation slot
still contains that participant at that exact binding epoch. If the slot is
empty, contains another participant, or contains a later epoch—even on the same
physical connection—replay returns `UnboundReceipt` with the same credential
payload, cursor, marker-acceptance fact, and both deadlines, and never mutates the slot. `Bound` and `UnboundReceipt` are used only by
exact live-receipt replay; fresh commits use `EnrollBound` or `AttachBound`. This
post-receipt comparison is non-secret and occurs after exact-token lookup; it
cannot rotate, detach, or disclose the current occupant. Leave overrides every
enrollment, attach, detach, and provenance result with non-secret `Retired`; only
the exact committed Leave-token verifier exception above precedes it.

**Bounded provenance.** Each committed credential-bearing attach token, and each
enrollment token during its exact-reason window, also has a non-secret fingerprint
record `{ token_fingerprint, participant_id,
request_generation: Option<u64>, result_generation,
terminal_reason: Option<Deadline|Superseded>, provenance_expires_at }`.
Enrollment stores `request_generation=None`; credential attach stores
`Some(the presented generation)`. `terminal_reason` starts `None` while the live
receipt exists and is atomically set to the exact reason when deadline or
supersession deletes that receipt; only the resulting `Some` provenance row may
emit `ReceiptExpired`. While that record
exists, the server may claim exact provenance: an exact committed token whose
body was ended by a newer generation returns the exact `ReceiptExpired` payload
below with reason `Superseded`; one ended by its deadline returns that payload
with reason `Deadline`. At the same old generation, a fresh
token absent from the complete in-window fingerprint set returns
`StaleAuthority`, proving no commit for that token. For a credential-bearing
token after the fingerprint deadline, both an exact old token and an unknown old
token return the exact flattened
`StaleOrUnknownReceipt { conversation_id, token, participant_id,
presented_generation, presented_marker_delivery_seq: Option<DeliverySeq>,
current_generation }`, which
expressly does **not** claim that a transaction committed. Enrollment instead
follows the lifetime mapping below. A retired identity's tombstone still wins and
returns `Retired`.

The exact flattened provenance response
is `ReceiptExpired { conversation_id, token, participant_id,
presented_generation: Option<u64>, presented_marker_delivery_seq,
result_generation,
current_generation, reason: Deadline|Superseded }`. Enrollment uses
`presented_generation=None` because its wire request has no generation;
it also omits `presented_marker_delivery_seq`. Credential attach/receipt replay
uses `Some(the presented generation)` and requires
`presented_marker_delivery_seq: Option<DeliverySeq>` equal to the presented
request Option. The
response echoes the request token, never the stored `token_fingerprint`; that
fingerprint remains an internal bounded lookup value. Tombstone precedence means
`current_generation` is always a live generation in this outcome.

Receipt/provenance cost is signed and known before send. Add nonzero
`attach_receipt_ttl_ms`, `receipt_provenance_ttl_ms` (not shorter than the receipt
TTL), server-wide and per-participant live-receipt caps, and server-wide,
per-conversation, and per-participant provenance-fingerprint caps, all with signed
defaults advertised in negotiated participant capability state. They extend the
verified named/defaulted/nonzero `LimitsConfig` pattern
(`crates/liminal-server/src/config/types.rs:193-203`,
`crates/liminal-server/src/config/types.rs:257-316`). Before participant mode,
validate zero fields in this exact order: `attach_receipt_ttl_ms`,
`receipt_provenance_ttl_ms`, `max_live_attach_receipts_server`,
`max_live_attach_receipts_per_participant`,
`max_receipt_provenance_server`,
`max_receipt_provenance_per_conversation`,
`max_receipt_provenance_per_participant`,
`max_retired_identity_slots_server`, then
`max_retired_identity_slots_per_conversation`. The first zero returns
`ParticipantCapabilityConfigurationInvalid { dimension: NonzeroLimit, field,
actual: 0, required_minimum: 1 }`. Only after all nine pass, a provenance TTL
shorter than the receipt TTL returns
`ParticipantCapabilityConfigurationInvalid { dimension: ReceiptDeadlineOrder,
attach_receipt_ttl_ms, receipt_provenance_ttl_ms,
required_minimum_provenance_ttl_ms: attach_receipt_ttl_ms }`. Both durations and
the admitted monotonic-clock value are u64, while deadlines are encoded as
checked u128 sums, so deadline addition cannot overflow; this is the explicit
tighter proof, not a missing failure arm.

For runtime capacity, check fixed scopes in order `LiveReceiptServer`,
`LiveReceiptParticipant`, `ProvenanceServer`, `ProvenanceConversation`,
`ProvenanceParticipant`. The first complete set unable to admit one returns
R-D1 `ReceiptCapacityExceeded` with the triggering operation's exact common
request envelope and suffix `scope,limit,occupied,requested:1`, before commit.
A later authorized
proof, Leave, or receipt deadline removes the secret body; the non-secret
fingerprint remains only through its provenance deadline. Both cleanups are
admitted durable deadline events plus request-time checks—never a sweep.

**Bounded retirement identity.** Add signed nonzero server-wide and
per-conversation `max_retired_identity_slots`. Enrollment must reserve one slot
in both scopes, testing `Server` before `Conversation`, and returns
R-D1 `IdentityCapacityExceeded` with the enrollment common envelope and suffix
`scope:Server|Conversation,limit,occupied,requested:1` at the first full set. A live participant's
reservation counts against the cap; Leave converts that same reservation into
the permanent non-secret tombstone without another capacity decision. Slots are
not cleaned up in v1, preserving exact `Retired`, no-ghost enrollment replay, and
stable lost-response `LeaveCommitted` forever while bounding total authenticated
enroll/Leave churn. There is no tombstone sweep.

When one enrollment would fail more than one runtime capacity, its fixed selector
is identity scope Server, identity scope Conversation, `LiveReceiptServer`,
`LiveReceiptParticipant`, `ProvenanceServer`, `ProvenanceConversation`, then
`ProvenanceParticipant`. Credential attach starts at `LiveReceiptServer` because
its identity slot already exists. The first full scope returns its named
`IdentityCapacityExceeded` or `ReceiptCapacityExceeded`; no later occupancy is
disclosed. For enrollment, both per-participant occupancies are necessarily zero
for the not-yet-minted identity and their signed limits are nonzero, so
LiveReceiptParticipant and ProvenanceParticipant provably pass and are not
advertised as enrollment refusal scopes. This is R-D1 stage 8's complete
identity/receipt suborder.

**Enrollment mapping exception.** The reserved identity slot owns one indexed
`(conversation_id, enrollment_token_fingerprint) → participant_id` mapping for
the full lifetime of the live identity and its tombstone; it is not provenance-
TTL state. After the secret receipt/provenance window, replay of that committed
token first resolves that participant. A live non-retired identity returns the
complete exact R-D1 `EnrollmentKnown` schema; a tombstone returns the generic
R-D1 enrollment `Retired` envelope plus `participant_id,retired_generation`. Neither remints or
claims an exact expiry reason. For `EnrollmentKnown`, the SDK uses any valid
current-or-newer credential it already holds, otherwise enters
`CredentialRecoveryLost` with the returned identity. A fresh token absent from
this lifetime index may enroll normally until the reserved identity cap.

The negotiated receipt TTL is normatively the deployment's **maximum supported
recovery outage**. If generation G is invalidated by a committed G+1 attach, its
response is lost, and the client returns after the secret body expired, no wire
outcome can restore G+1. On exact `ReceiptExpired` or later
`StaleOrUnknownReceipt` while the SDK has only invalid G, it durably enters
terminal `CredentialRecoveryLost { conversation_id, participant_id,
last_known_generation }`, preserves those identities, disables new attach,
conversation replay, and ack authority, and routes credential repair only to
`«ATTACH-SECRET-LIFECYCLE»` operator re-issue. R-A4's bounded exact-token status
recovery may still learn the fate of an already-sent transaction; it cannot
mutate cursor or membership under the lost credential. It never retries
periodically.

**R-C1 — Retry-safe enrollment, generation-ordered rotation, and binding.** Every
successful attach returns a newly minted secret: enrollment creates generation
`1`; every credential-bearing success checked-increments the fixed-width `u64`
`capability_generation` and rotates the secret. It never wraps, saturates, aliases,
or rebases. No legal state reaches generation `u64::MAX`: generation G requires
at least `2G-1` appended lifecycle records (the enrollment `Attached`, then one
terminal plus one `Attached` per increment). A live bound member also retains at
least `E+T+L×T=3` sequence claims, so `H<=MAX-3` and therefore
`G<=floor((MAX-2)/2)=(MAX-3)/2`. A detached member has already appended its
terminal, so `H>=2G`; its remaining E claim gives `H<=MAX-1`, hence
`G<=(MAX-1)/2`, still strictly below MAX. Every proposed
increment runs the R-C2 resulting-state sequence check before mutation, so
`ConversationSequenceExhausted` wins billions of generations before u64 wrap.
This is an explicit joint-domain proof; `GenerationExhausted` is not a wire
outcome. Any detached live member may use the current credential for R-C2's
tokenized unbound terminal Leave. Connection authentication remains the shared
bearer gate, not an ACL. Participant cursor authority is separate.

`ParticipantId` is exactly the fixed-width u64 permanent
`participant_index` within its conversation; every lookup key also carries
`conversation_id`. The identity reservation stores a monotone
`next_participant_index` in `0..=I`. Enrollment assigns the current value and
checked-increments it only when it is `<I`; slots and ids are never reused.
When it equals I, the already named conversation-scope
`IdentityCapacityExceeded` wins. Successive legal enrollments can therefore mint
every value in `0..<I`, while I itself is the exhausted sentinel and is never a
participant id. Thus distinct accepted enrollment tokens in one
conversation have distinct ids, ids may repeat only across different
conversation keys, and there is no random collision loop or separate id-domain
exhaustion arm.

For every secret-bearing credential attach or Leave, the serialized authority
check is exhaustive. A live exact token first runs R-C0's constant-time verifier:
wrong secret is `StaleAuthority`, while matching-secret non-secret mismatch is
`AttemptTokenBodyConflict`. Otherwise, after tombstone/identity lookup, authority
compares generation and then the presented secret in constant time. A generation
mismatch or an equal-generation secret mismatch returns the one
`StaleAuthority` row carrying presented and current generations. Equality of
those fields identifies either wrong-secret path without a new discriminant or
additional oracle. Every refusal commits no receipt, order, cursor, binding,
lifecycle record, candidate, or retention mutation.

V1 has exactly one participant binding per
`(connection_incarnation, conversation_id)`. Before enrollment mint or credential-
attach binding mutation, the serialized lane checks that slot. Empty permits the
operation; a slot for the same presented participant permits rotation/supersession;
a different participant—or enrollment, whose identity is not minted yet—returns
one of exactly two flat variants:
`ConnectionConversationBindingOccupied { conversation_id,enrollment_token,
presented_participant_id:None }` for enrollment, or
`ConnectionConversationBindingOccupied { conversation_id,participant_id,
capability_generation,attach_attempt_token,accept_marker_delivery_seq,
presented_participant_id:Some(participant_id) }` for credential attach.
It reveals no occupying id, generation, incarnation, or binding state and commits
no receipt, identity, rotation, or record. Exact-token result lookup still
precedes this check. This invariant makes conversation-only delivery demux
unambiguous without adding recipient identity to every delivery frame:

1. Enrollment carries the exact R-C0 `EnrollmentRequest`. There is
   no credential-free bare frame. One commit mints `participant_id`, generation
   `1`, and `attach_secret`; creates R-C2 membership at cursor `0`; binds the
   originating connection at epoch `(connection_incarnation, 1)`; records
   `Attached` with that epoch; stores the enrollment fingerprint and live receipt;
   and returns typed `EnrollBound` with that generation, secret, binding epoch,
   `request_generation=None`,
   `persisted_cursor=0`, `accepted_marker_delivery_seq=None`, and the exact
   receipt/provenance deadlines.
   Subject to the empty connection/conversation binding slot above, distinct
   enrollment tokens create distinct participants; when occupied, refusal happens
   before mint. The permanent slot allocator above has no collision path.
2. Enrollment or attach receipt replay follows R-C0. Recovery is reported as
   attached only while the exact receipt binding epoch still occupies its origin
   slot; every replacement, detach, or later same-connection rotation returns
   `UnboundReceipt`. The SDK first atomically persists its non-stale generation
   and secret, then sends a **new** credential-bearing attach with a fresh
   write-ahead token. Only that new `AttachBound` result enables replay or ack authority
   on the replacement connection.
3. A credential-bearing attach presents the exact R-C0
   `CredentialAttachRequest`, including `conversation_id`. The server
   rechecks generation and secret at the serialized commit point before reading,
   replaying, advancing, or rebinding the cursor. Success increments generation,
   invalidates the presented secret, echoes that presented generation separately
   from the new capability generation, and returns a fresh secret plus the persisted
   cursor, `accepted_marker_delivery_seq`, both deadlines, and new
   `BindingEpoch`, and terminalizes R-C0's prior detach replay cell.
   If a binding epoch is active—even on this same connection incarnation—it
   records exactly one ordered `Detached(Superseded)`/`Attached` handoff naming
   the old/new epochs and fences every old-epoch operation and queued delivery.
4. Lost rotation response is recovered with the invalidated old secret and same
   token only while its receipt is live. R-C0 returns `Bound` only if the receipt's
   exact participant/binding epoch still occupies its origin slot, otherwise
   `UnboundReceipt`, with no second rotation or handoff. After completion/expiry it returns the typed non-secret outcome, never
   treats the old secret as a new attach credential.
5. The SDK persists results in this crash order: compare the received generation
   with the durable one; reject a lower generation as stale; require identical
   secret for an equal generation; for a greater generation atomically persist
   `{ participant_id, request_generation, generation, secret, origin_binding_epoch,
   persisted_cursor, accepted_marker_delivery_seq, receipt_expires_at,
   provenance_expires_at }` **before** marking
   the attempt complete or exposing binding/replay/ack authority. A crash at any
   point replays the write-ahead token. A delayed generation can never overwrite
   a newer secret.
6. A stale generation/secret with a new token is a terminal connection-level
   refusal with no cursor side effect, lifecycle record, broadcast, or retention
   pressure. In a two-holder race, the first serialized valid attach rotates;
   the second is stale. The log contains at most one Superseded/Attached pair per
   genuine handoff.
7. Cursor and replay authority key on the verified current-generation capability,
   authoritative connection binding, and `conversation_id`, never on a receipt
   or the shared bearer token alone.

**Fenced provisional recovery at a retained marker.** This is the only attach path
that may combine cursor abandonment with binding. It is available only when the
presented current generation/secret is valid, no different live binding occupies
the connection/conversation slot, the participant owns the named retained
unaccepted marker, that marker was durably recorded as delivered to its last
authoritative binding epoch, and ordinary handoff fails only for R-C4 closure
headroom. The request must present that marker's exact sequence in
`accept_marker_delivery_seq`; omission or mismatch follows ordinary attach and
cannot acknowledge anything.

One durable transaction revalidates all proofs; accepts the marker by advancing
the cursor to its sequence without claiming abandoned payload delivery; releases
the marker anchor; drains this identity's one earlier `PendingFinalization` when
present; allocates that terminal and `Attached` in global admission order (one or
two records for this identity, never more than `(Qe,Qb)`); rotates
generation/secret; recomputes the floor, marker capacity credit, `ClosureDebt`,
and its repayment edge; and binds the new epoch. It consumes the edge's pre-owned
RS/RO and transfers RT/RA into that epoch's T/A. Absolute fit plus `d'<=Q` is
sufficient even at full incoming debt: because the named marker is accepted, the
successor is exact `ObserverProjection` or `PhysicalCompaction` (or `None` at zero
debt), never `MarkerDelivery`. It returns `AttachBound` with
`accepted_marker_delivery_seq`. No provisional binding is externally visible and
no new record class exists. The marker component allocates no sequence; only the
pending terminal and `Attached` records do. Crash exposes either the entire old
marker/cursor/pending state or the complete new cursor/floor/records/credential/
binding. Invalid proof returns the existing marker/authority outcome with no
mutation. Retry uses the attach token and is driven only by response/connection/
observer events, never a timer.

**Finite generation domain.** The joint record-count proof above is normative:
the sequence domain stops optional attach long before generation can approach
its fixed-width edge. There is no generation rekey, reset, exhaustion outcome,
periodic monitor, or polling policy.

**Exhaustive participant-reference lookup.** At the serialized participant-state
point, every decoded request that names a participant uses this total precedence
before operation-specific checks:

0a. If and only if this is a Leave request whose token equals the presented
    identity tombstone's committed Leave token, run the permanent verifier:
    secret mismatch returns the exact `StaleAuthority` Leave union's
    `CommittedLeaveTombstone` variant; then generation mismatch returns
    the complete R-D1 `AttemptTokenBodyConflict` Leave body with
    `conflict=Generation`; exact canonical body
    returns `LeaveCommitted`.
0b. Resolve any enrollment mapping, attach receipt/provenance fingerprint, or
    detach-cell token to its participant before returning a token status. If
    that identity is tombstoned, return `Retired`; no pre-Leave live result can
    escape. A different Leave token also continues to ordinary tombstone lookup.
0c. Only for a live non-retired identity, run R-C0's phase-specific token state.
    A live attach receipt checks secret first, then conflict classes in order
    Generation/MarkerDeliverySequence, then returns its credential payload plus
    current `Bound`/`UnboundReceipt`. A provenance-only row has no verifier, so
    every exact-token body returns its retained `ReceiptExpired`; after
    provenance it is `StaleOrUnknownReceipt`. Enrollment's lifetime mapping
    returns `EnrollmentKnown`. A detach cell checks its non-secret verifier;
    generation mismatch is `StaleAuthority`, and exact body returns its stable
    result.
0d. A Pending detach cell with a **different** detach token returns
    `DetachInProgress`; this classification precedes binding lookup. A nonmatching
    token against Committed gets no special result and continues below.
1. A tombstone for the **presented** `(conversation_id, participant_id)` returns
   operation-specific `Retired`.
2. No live identity and no tombstone returns the non-secret generic R-D1
   `ParticipantUnknown` outcome with the operation's exact common envelope.
3. A live identity whose current generation differs from the presented generation
   returns `StaleAuthority`. If the operation carries `attach_secret`, an equal-
   generation constant-time mismatch also returns `StaleAuthority`; equal
   presented/current fields classify it without a new oracle. For Leave, either
   live failure uses the exact R-D1 union's `Live` variant; every other operation
   uses its origin-selected common-envelope schema.
4. For a binding-required operation, matching live authority without this
   connection's exact current binding epoch returns generic R-D1 `NoBinding`
   with the operation's exact common envelope.
5. The operation executes its remaining proof/admission checks.

Credential attach, exact receipt/status lookup, and R-C2 terminal Leave by an
already-detached member do not require an existing binding. Normal/marker ack,
new detach, bound Leave, and ordinary record admission do. Detached Leave proves
the exact still-current generation and secret and grants no cursor, replay,
binding, or fresh-credential authority. Server-driven replay runs only inside a successful
`AttachBound`; it is not a request. `ParticipantUnknown` and `NoBinding` leave the
connection open and terminalize the attempt without receipt, cursor, candidate,
identity, record, or other durable state. The explicit identity oracle exists only
after shared connection authentication; neither outcome reveals another holder's
binding epoch or connection incarnation.

This is a participant-scoped continuity capability, **not an ACL system**. The
shared token gates the connection; the current generation and attach secret gate
identity continuity and that participant's cursor. Mapping humans, agents, or
external principals belongs above. If ids later originate externally, the
server still mints the generation/secret; an external id cannot authorize a
cursor. The remaining total-loss/operator re-issue state is named in
`«ATTACH-SECRET-LIFECYCLE»`.

**Domain-owner refutation target.** Hermes may prefer bearer-latest-wins
simplicity over rotation, but that is a different contract: equal holders can
then generate unbounded broadcast handoff traffic and retention pressure. A
ruling for that alternative must add an explicit event-driven admission/rate
bound, accept and specify the denial-of-service consequence, and replace the
rotation and no-record acceptance cases below; it may not silently delete the
fence or introduce a polling limiter.

**R-C2 — One atomic, gap-free conversation log with owed-record reserve.** The
server is the sole writer of a strictly increasing `delivery_seq`. Let `MAX` be
its maximum, `H` the high watermark, `T` active-or-pending bindings with unwritten
terminal records, `M` required-but-unwritten `HistoryCompacted` markers, `L` live
members, `E=L` the flat member-exit claims, and
`L_other = if L == 0 { 0 } else { L - 1 }` computed in the checked wide domain.
Let `RS` be edge-owned sequence values for the current recovery Attach record and
`RT` edge-owned sequence values for the replacement binding's future terminal;
each is zero or one. Every live member owns one value for eventual `Left`; a bound
member also keeps its separate terminal claim in `T`. Checked wide arithmetic
preserves after every commit:

`MAX - H >= E + T + M + RS + RT + (L × T) + (L × RT) + (L_other × E)`.

The canonical `SequenceBudget` payload is `{ high_watermark: H, remaining:
MAX-H, E, T, M, RS, RT, L_times_T: L×T, L_times_RT: L×RT,
L_other_times_E: L_other×E }`. Every `ConversationSequenceExhausted` outcome and
codec vector has the ordinary discriminant/conversation/request envelope and
exactly one nested `sequence_budget` with these ten—and only these ten—fields for
its proposed resulting state. No row may abbreviate a zero term or add a required
cost, scope, or other exhaustion-specific field to that nested payload.

A DCR-capable edge owns `RS=1,RT=1`. Recovery Attach consumes RS and atomically
transfers RT into T; its pre/post right sides differ by exactly the one appended
record. A pre-attach fate plan also owns every conditional M value. No transition
borrows E/T/M/RS/RT or counts one numeric value in two terms.

R14 preserves the E/T/product terms and gives every preplanned marker exactly one
immutable provenance tag. `NonProductM` provenance starts with current sequence
owner `M`. `TerminalProduct { terminal: T | RT, affected_participant }` and
`ExitProduct { exit: E, remaining_participant }` provenance starts with current
sequence owner `ConditionalProduct`: the value remains in `L×T`, `L×RT`, or
`L_other×E` until that causal terminal or exit fires. That transaction
atomically changes the current owner of each marker actually induced to `M`,
retains its immutable `TerminalProduct` or `ExitProduct` provenance, and
releases every unused conditional claim. The `affected_participant` and
`remaining_participant` fields each carry the permanent participant index whose
conditional marker the product backs. `edge_claims.marker_sequence_values`
records the exact value, immutable `provenance_tag`, mutable
`current_sequence_owner`,
causal `transaction_order`, phase-4 candidate position, and participant index.
The only legal owner/tag pairs are `NonProductM/M`, either product provenance
with `ConditionalProduct` before its causal transaction, and that same product
provenance with `M` after it. Thus a value is never simultaneously owned by M
and a product term, even though its immutable provenance survives the transfer;
the transfer is not a later sequence/order allocation. If the
post-state cannot include those claims, the **original optional operation** gets
`ConversationSequenceExhausted`, `ConversationOrderExhausted`, or
`MarkerClosureCapacityExceeded`—whichever first check in R-D1 precedence fails—
before mutation. This cites R-A2's sole **reserved-counter-capacity** definition: an accepted
obligation owns its finite counter values and later ordinary work cannot borrow
them; R-C2 applies that definition to sequence values without redefining it.

Sequence claims use the same movable-frontier rule as order claims. Define its
checked, duplicate-free numeric view over every persisted still-unmaterialized
`E/T/product/RS/RT` value above H and above every immutable pending-terminal or
marker-candidate value. A transaction appending k records assigns those records
the next gap-free values `H+1..H+k`: a sequence-claim-backed record transfers its matching
logical handle, while an optional record uses capacity preflighted outside the
claim count. It then atomically relays every surviving claim to the smallest
strictly greater unused suffix. Ownership may move across identities and among
E, T, and conditional product handles. Before the target fate, one DCR sequence
block is the consecutive `[T,RS,RT]` interval. If that target fate wins with no
induced marker, T uses H+1 and RS/RT become the next two values. If it induces k
markers, T uses H+1, those immutable phase-4 candidates take the next k values,
and the intact RS/RT pair moves after them for the later one-record recovery
Attach. If an unrelated claimed/optional record wins, the whole still-
unmaterialized block moves intact after that record. After T materializes, the remaining `[RS,RT]` pair moves only as one
intact interval. The relay therefore preserves terminal/recovery/replacement
adjacency. It also rewrites every affected Pending occurrence's
MarkerSequenceValue/RecoveryAttachSequence/ReplacementTerminalSequence backing,
candidate index, and product causal reference. Old ownership is removed first;
no value is appended twice, returned to an unowned pool, or counted in two
terms.

An already materialized terminal or marker candidate and its sequence value are
immutable. Every such globally earlier candidate drains before a later caller
record; only the same identity's next-gap unwritten marker may instead be
cancelled by the exact Leave rule below, and only when cancellation leaves no
other candidate earlier than that Leave tuple. A newly blocked authoritative
fate may reserve the first value after the immutable sequence prefix and become
PendingFinalization without appending; remaining movable claims relay above it.
Consequently an arbitrary first
EOF consumes the frontier T value at H+1 even if another identity owned that
number in the preceding snapshot, and all remaining T/E/product claims move
above it crash-atomically. This is reassignment of reserved values, not new
capacity.

`E`, `T`, and `M` are durable owed-record claims. `E` and `T` remain separately
charged for a bound member even though one bound `Left` can discharge both; when a
prior terminal is pending they pay two distinct records. The first product
reserves a possible marker for all `L` members after each binding terminal; the
second reserves one for each of the `L_other` members remaining after an exit.
The joint-history boundary is tighter than the bare `E=1` inequality. A sole
detached member with no marker must come from a bound predecessor, whose
`E+T+L×T=3` requires `H<=MAX-3`. Its terminal can append at MAX-2, release the
unused product, and move E to MAX-1; with no remaining binding, no legal operation
can fill MAX-1 before Leave. Its gap-free `Left` therefore appends at MAX-1 and
MAX stays unallocated. Case 47 constructs this boundary. A pre-commit
`H=MAX-1,E=1` or `H=MAX,E=1` sole-member state is unreachable, not a fixture.
For a candidate costing `k` records, the transaction computes the
resulting membership, exit/terminal claims, floor, and complete R-C4 marker fixed
point, then checks this invariant and R-C4's independent entry/byte closure
invariant. Sequence failure returns `ConversationSequenceExhausted` before an
optional mint, attach, supersession, ordinary record, floor, candidate, or sequence
mutation. It is unreachable for a terminal event already backed by `E` or `T`.
A required marker is never discarded before append, and an appended-unaccepted
marker is never compacted.

The transition accounting is exhaustive:

- Enrollment costs one `Attached`, creates one `E` and one `T`, and includes every
  cursor-0 marker in resulting `M`; it commits only with all resulting claims.
- Attach from detached costs one and creates `T` while preserving that member's
  existing `E`. Supersession costs two contiguous values, consumes old `T`, creates
  replacement `T`, and leaves `E` unchanged. Both are optional and may be refused
  before mutation.
- A binding terminal costs one, consumes `T`, preserves `E`, and converts at most
  `L` released product values into `M`. Active-to-`PendingFinalization` changes no
  sequence count; its eventual append performs that conversion.
- A detached Leave costs one and consumes `E`. A bound Leave with no earlier
  pending terminal appends one `Left` that discharges both `T` and `E`. If its
  own older binding terminal is pending, terminal then Left consume the two
  claims in one composition only when their R-A2 tuples have no unrelated tuple
  between them; otherwise the terminal and intervening tuples drain separately,
  and later one-record `Left` links that terminal sequence. Any exit can create
  markers for the remaining members from the product decrease without invading
  another claim.
- A marker append costs one and changes `M→M-1`. Fenced recovery costs one
  `Attached` plus its prior pending terminal when present; marker acceptance costs
  no sequence, old `T` is consumed when drained, new `T` is created, and `E`
  survives. Ordinary records cost one and every overtaken member enters `M` in the
  same preflight.

If a binding dies while Leave races, serialization yields either bound Leave's
single combined record, or the terminal record followed by detached Leave; it can
never lose or double-spend `E`/`T`. Thus bind→detach→bind→detach cycles preserve
one `E` throughout and create/consume exactly one `T` per binding. Record append,
claim/marker changes, retention transition, and candidate deletion are one commit;
failed optional work consumes no value and sequence never wraps. The current
pump's narrower counter is assigned before enqueue and held with its frame to
preserve order (`crates/liminal-server/src/server/connection/delivery.rs:114-142`);
that fact is not reused as the durable transaction.

A conversation is a **broadcast domain in v1**, with durable membership distinct
from transient attachment:

1. **Membership begins at mint and survives detach.** R-C1's enrollment commit
   makes the participant a conversation member. Attachment is only the
   connection-scoped binding. Any number of detaches, disconnects, deaths, or
   server restarts leave membership, cursor, and retention claim intact.
2. **Initial cursor is `0`.** A newly minted member has acknowledged nothing and
   is entitled from sequence `1`, including history committed before mint. Every
   **member** is entitled to every committed record of the conversation;
   entitlement is membership-scoped and is never attachment-filtered. If the
   retention floor has already passed sequence `1`, the late member's first
   delivery is R-C4's covering `HistoryCompacted` marker and its explicit-accept
   path, by composition of these two rules—not an undefined starting point.
3. **Offline records remain entitled.** Records committed while a member is
   detached are replayed in order on reattach by R-C5's existing
   `(cursor, watermark]` mechanism. The mechanism already supplied the replay;
   this rule supplies the previously missing entitlement.
4. **Leave is terminal and is not detach.** `LeaveRequest { conversation_id, participant_id,
   capability_generation, attach_secret, leave_attempt_token }` is legal either
   from the exact authoritative bound epoch or while the live member is detached.
   Both paths require constant-time proof of the exact current generation/secret;
   shared authentication, an old epoch, a stale generation, or an equal-generation
   wrong secret is insufficient, and v1 defines no operator Leave. The detached
   path grants no binding, cursor, replay, ack, or fresh credential and may only
   retire. The SDK write-aheads the token.

   With no prior binding terminal pending, success uses the member's flat `E`
   claim, appends one ordered `Left` (also discharging active `T` when bound),
   releases every marker anchor plus membership/cursor, invalidates the secret,
   and mints the tombstone atomically. Leave itself never creates a server pending
   cell. If hard observer progress is the blocker, `ObserverBackpressure` leaves
   the member/secret/binding unchanged and only the bounded SDK row waits; any
   independent closure-equation failure is the cross-cutting R-D1
   `MarkerClosureCapacityExceeded` with no automatic retry.

   Leave first drains every globally earlier candidate in a separate committed
   candidate transaction, strictly by the R-A2 tuple. Each transaction is
   independently Q-bounded and crash-visible. A terminal transaction appends that
   terminal and atomically creates its complete same-major marker fixed point;
   those markers then drain in phase-4/index order before any later major. After
   every commit all invariants hold, so restart resumes at the lowest tuple; no
   transaction groups an unbounded set.

   If this member's pending terminal remains after that prefix, the two-record
   atomic core is legal **iff** no unrelated candidate tuple lies strictly between
   the terminal's preserved major and the member's X-major `Left`. In that
   positional case the core is exactly the old-major terminal then X-major `Left`.
   It alone evaluates one combined final floor: no intermediate floor is visible,
   and newly induced exit markers use the Left major. It releases T/E, applies
   floor/credit/debt changes, and writes the tombstone atomically.

   If an unrelated tuple lies between those majors, composition is forbidden.
   The member's own pending terminal drains first as a separate standard terminal
   transaction at its exact tuple, with all same-major markers draining after it;
   every intervening tuple then drains in order. Leave finally commits as the
   existing one-record `Left` transition of an already-detached member whose
   terminal is complete. Each transaction computes its own floor; “one combined
   final floor” does not apply. The stable receipt still records that separately
   committed terminal sequence.

   Observer blockage returns `ObserverBackpressure` before accepting the affected
   transaction; otherwise nonzero debt requires absolute fit and a resource-backed
   successor. The crash-visible order is the exact sequence of separate terminal/
   marker transactions, followed either by the indivisible composed pair or the
   one-record Left, then exit markers. A crash between transactions exposes that
   valid prefix; a crash around a composed core exposes all-old or both records
   plus the tombstone. Exact replay returns one stable `LeaveCommitted {
   conversation_id, leave_attempt_token, participant_id, presented_generation,
   retired_generation, ended_binding_epoch: Option<BindingEpoch>,
   prior_terminal_delivery_seq: Option<DeliverySeq>, left_delivery_seq }`.
   `ended_binding_epoch` is `Some` exactly when this Leave terminalizes the
   active binding and is `None` for detached Leave. Independently,
   `prior_terminal_delivery_seq` is `Some` whenever an older binding terminal
   exists, whether it was committed separately or in the core. This proof grants
   no cursor authority.
5. **The Leave transaction retires everything.** It appends exactly one ordered
   R-A1 `Left`. A prior pending terminal is either in the positional composed core
   or was separately committed before this transaction; no terminal is duplicated.
   Leave terminalizes any active binding and the membership, invalidates current
   secret, converts every
   outstanding enrollment/attach receipt to R-C0
   `Retired`, releases the member cursor/soft retention claim, and permanently
   tombstones `participant_id` in one commit. The tombstone retains the enrollment
   fingerprint and successful Leave token/result but no attach secret; it converts
   the retirement slot reserved by enrollment under R-C0 and cannot exceed either
   signed cap. Tombstone lookup precedes live capability validation: duplicate
   Leave with that token returns the stable `LeaveCommitted {
   conversation_id, leave_attempt_token, participant_id, presented_generation,
   retired_generation, ended_binding_epoch: Option<BindingEpoch>,
   prior_terminal_delivery_seq: Option<DeliverySeq>, left_delivery_seq }` and
   commits no second record even though the secret is now
   invalid; any other
   later enrollment/attach/detach/Leave replay returns
   generic R-D1 `Retired` under that operation's common envelope, with no secret, binding, new
   identity, or record.
6. **Attach/Leave races have one order.** If attach linearizes first, it rotates
   generation and binding epoch; the competing old-epoch Leave fails typed
   `StaleAuthority` with no record. If Leave linearizes first, the participant and
   all receipts are terminal; the competing attach returns `Retired` with no
   binding or record. There is no state in which a stale binding retires the new
   holder. Detach merely ends an attachment; membership, cursor, and its R-B5
   cursor-stall/retention pressure persist.

Records include application payloads, attach, detach, death with R-A1 cause,
Leave, and `HistoryCompacted`. Every member's entitled subsequence is therefore
the full per-conversation sequence, so cumulative gap checking has no legitimate
non-recipient gap. Targeted or filtered delivery inside a conversation is
excluded from v1 under `«TARGETED-DELIVERY»`, not left as a semantics hole.

**R-C3 — At-least-once continuous delivery or explicit abandonment; exactly-once
marking.** The server stores one durable cumulative cursor per authorized
`(conversation_id, participant_id)`. While `cursor + 1 >= physical_floor`, it
offers every entitled retained `(conversation_id, delivery_seq)` at least once
until cumulatively acknowledged; transport loss or reconnect may offer the same
pair again. If `cursor + 1 < physical_floor`, continuous history is impossible:
the server offers R-C4's explicit abandonment marker instead of promising the
retained suffix. This is the deliberate R4 narrowing of R3's unconditional
retained-record offer.

Both ack shapes carry their authority explicitly:

- `ParticipantAck { conversation_id, participant_id,
  capability_generation, through_seq }`; and
- `MarkerAck { conversation_id, participant_id, capability_generation,
  marker_delivery_seq }`.

At the same serialized point as detach and Leave, the server applies R-C1's
presented-id lookup precedence, then requires the presented generation and this
connection's exact current binding epoch to match. Tombstone lookup is therefore
on the id in the ack bytes, not an inferred current connection binding. A delayed
P ack received after Q binds—or after P rotates—on the same connection can only
classify the presented authority; it can never advance the newer cursor. For
continuous history, the server then accepts the normal ack only when every
sequence from cursor + 1 through `through_seq` was made available contiguously to
that matched binding epoch. For broken history, delivery of
`HistoryCompacted { participant_id, abandoned_after, abandoned_through,
physical_floor_at_decision }` authorizes exactly
two versioned routes: a current binding's `MarkerAck`, or R-C1's fenced provisional
attach by the same credential owner when ordinary handoff lacks closure headroom.
Both name the marker's own `delivery_seq`, explicitly abandon the entire interval—including records still physically retained at or above the old floor—and
atomically advance that participant's cursor to the marker sequence. Those
payloads are not asserted or marked as delivered.

For `MarkerAck`, the server requires delivery to the same current binding epoch.
For fenced recovery, it requires the durable delivery fact to the participant's
last authoritative binding epoch plus current-generation/current-secret proof. Every
other attempt spanning abandonment is refused. Until the
client accepts, the cursor holds, no suffix payload flows, and R-B5's cursor-stall
observable remains. The server persists the new cursor before confirmation.
Re-acknowledging the current cursor under the same presented authority is an
idempotent confirmed no-op; an uncovered skip or regression is refused. Each
cursor transition is marked once even if confirmation is retried.

**Total ack/marker-proof selector.** After the R-C1 authority/binding stages,
normal ack compares its requested `through_seq` with the matched durable cursor:

- `< cursor` returns `AckRegression { current_cursor, reason: BelowCursor }`;
- `== cursor` returns `AckNoOp`;
- `> cursor` returns `AckCommitted` exactly when every sequence in
  `cursor+1..=through_seq` was made available contiguously to this binding epoch;
  otherwise it returns `AckGap { current_cursor,
  reason: NotContiguouslyAvailable }`, including
  a requested value above the offered/high watermark.

The complete flattened marker-proof outcome contains exactly one operation-specific
envelope: credential attach serializes `conversation_id,token,participant_id,
capability_generation,requested_marker_delivery_seq`, while `MarkerAck` serializes
`conversation_id,participant_id,capability_generation,
requested_marker_delivery_seq`. Its
marker-specific outcome/reason variant is exactly one of:

- `MarkerMismatch / BelowCursor`: `requested_marker_delivery_seq` and
  `current_cursor`;
- `MarkerMismatch / NoMarkerExpected`: `requested_marker_delivery_seq` only;
- `MarkerMismatch / ExpectedDifferentMarker`:
  `requested_marker_delivery_seq` and `expected_marker_delivery_seq`; or
- `MarkerNotDelivered / NotDeliveredToProofEpoch`:
  `requested_marker_delivery_seq` and `expected_marker_delivery_seq`.

The requested marker is common. `current_cursor` exists only in `BelowCursor`;
`expected_marker_delivery_seq` exists only in the latter two expected-marker
arms. `requested_marker_delivery_seq` is the one echoed request-envelope field,
not a duplicate addition. No optional retained/delivered/current field bag exists.

For a marker-bearing `MarkerAck` or credential attach, the proof epoch is the
current binding epoch for `MarkerAck` and the last authoritative binding epoch
for fenced recovery. A `MarkerAck` equal to the cursor is `AckNoOp` only when the
durable record at that cursor is this participant's already accepted
`HistoryCompacted`; a lower request selects the exact `BelowCursor` body above
with its requested marker and current cursor. Otherwise derive the one expected marker sequence from
the participant's current marker anchor/recovery edge. No expected marker gives
the exact `NoMarkerExpected` body; a different expected sequence gives the exact
`ExpectedDifferentMarker` body with that expected sequence; an exact expected
sequence without a durable delivery fact to the proof epoch gives the exact
`NotDeliveredToProofEpoch` body; and only an exact
sequence with that delivery fact commits `MarkerAckCommitted` or lets fenced
attach continue to its remaining admission checks. An attach with marker option
`None` does not invent a proof: a DCR-required or DMR/DCursor Leave-only edge
selects `RecoveryFence`, while an edge-free ordinary attach continues unless
R-C4's sole-quartet fence applies. These
comparisons are exhaustive and mutate no cursor, anchor, floor, generation, or
binding on refusal.

**Ordinary record admission authority.** Its wire request carries
`{ conversation_id, participant_id, capability_generation, payload }`; it carries
no caller-supplied `sender`. At the same serialized point used by ack, detach, and
Leave, R-C1's exhaustive lookup requires the current generation and this
connection's authoritative binding epoch. Only then may admission derive the
record's `sender = participant_id`, run size/observer/sequence/marker-closure
checks, and commit. A stale, retired, unknown, or unbound request returns that
named outcome and commits nothing. Ordinary admission cannot mint or reserve an
identity, membership, cursor, receipt, candidate, or tombstone and therefore
cannot create durable conversation state outside an already reserved live
identity.

The SDK keeps an in-session confirmed-contiguous watermark. It suppresses a
redelivery only when it can prove `delivery_seq <= watermark`. It hands the
canonical idempotency key `(conversation_id, delivery_seq)` to the application.
After a crash between an application effect and durable ack persistence, the
same record is redelivered with the same key. **Exactly-once application effect
is not promised.** Applications that require it must durably deduplicate or
transactionally commit their effect against that key.

**R-C4 — Monotonic physical floor, soft members, hard observer, and honest
abandonment.** Retention has both
`max_retained_conversation_bytes` and
`max_retained_conversation_entries`, each nonzero with a signed default. Let
`Ce=max_retained_conversation_entries`. The signed per-conversation field
`max_participant_state_bytes=Pstate` is a u64 occurrence-array limit with a
signed default in `1..=u64::MAX`; zero is an invalid configured boundary, not a
parser omission. It limits the complete fixed `successor_milestones` encoding
before any conversation state is allocated and is stored with the same signed
participant-retention configuration. Participant mode also configures signed
raw u64 `max_debt_episode_churn=J_raw`, with default 2. Startup validates
`2..=u32::MAX` before downcast and only then stores fixed-width u32 `J=J_raw`;
the `EpisodeChurnLimit.configured` outcome field is the raw u64. J bounds plan-changing lifecycle cycles while ClosureDebt is
nonzero; it is not a retry count or timer. Bytes bound payload/storage and
entries bound metadata. The fields extend the verified
`LimitsConfig` pattern
(`crates/liminal-server/src/config/types.rs:193-203`,
`crates/liminal-server/src/config/types.rs:257-316`). At participant-mode startup, let `I` be configured per-conversation
`max_retired_identity_slots`; `marker_max=(1,Bm)` be the maximum encoded entry/
byte cost of one `HistoryCompacted`; and `Q=(Qe,Qb)` be the maximum encoded cost
of one transaction in exactly these **mandatory closure classes**: enrollment,
attach from detached, the ordered supersession pair, authoritative multi-binding-
fate source, one-record Leave, the own-
pending-terminal plus `Left` two-record core, one earlier-candidate drain
transaction, immediate detach terminal, marker append, pending-finalization
drain, and R-C1 fenced recovery. Keys, fixed transaction/candidate order, the
16-byte connection incarnation, and storage framing are included.
Their exact entry maxima are enrollment 1, detached attach 1, supersession pair
2, authoritative multi-binding-fate source 2, Leave 1,
own-pending-terminal plus `Left` core 2, earlier-candidate drain 1,
immediate detach terminal 1, marker append 1, pending-finalization drain 1, and
fenced recovery at most 2. Thus every listed transaction appends at most two
records and supersession, a two-binding authoritative source, and the own-
pending-terminal plus `Left` core each reach two, so the generated entry
component is exactly `Qe=2`; it is not a
configuration or fixture input.
`Qb` and `Bm` are likewise generated encoded-byte maxima, never operator- or
test-assigned values.

**Exact v1 durability-charge profile.** To make byte fixtures physical rather
than normalized fiction, the v1 durability generator defines one positive byte
unit `b_u=SR`, where SR is the ordinary durable-row fixed overhead, and
build-checks `10×b_u<=u32::MAX`, `AD+10×b_u<=FRAME_MAX`, and that every unpadded fixed lifecycle or
compaction row is at most `4×b_u`. A charged durable record row includes its conversation/
delivery key, record tag and fields, schema/version/length/checksum/storage
framing, candidate/order metadata, connection incarnation where present, and
per-entry index footprint. Every fixed lifecycle or compaction row is
canonically zero-padded after those fields to exact `Bm=4×b_u`; the decoder
requires the exact length and all-zero padding, and the checksum covers it.
Transaction-only metadata is charged inside those rows, never beside them; the
sum of row charges is the complete transaction charge.
Consequently a one-record mandatory class costs `(1,Bm)`, the reachable
two-record core costs exactly `(2,2Bm)`, and generated `Qb=2Bm=8×b_u`.
Padding is durable-storage-only: it never appears in R-D2 wire bytes or a token
verifier. An ordinary payload of respectively `0`, `3×b_u`, or `10×b_u` bytes
has exact durable charge `b_u`, `4×b_u=Bm`, or `11×b_u`; the shared wire-size
preconditions still apply. Cases call these the one-unit, uniform-Bm, and
eleven-unit ordinary rows. No fixture assigns `SR`, `Bm`, or `Qb`.

An acceptance arm that explicitly invokes the **uniform-Bm fixture convention**
uses only exact `(1,Bm)` retained/planned records in that arm. Within that arm,
every ordinary row has canonical payload `[0x00;3×b_u]`; the convention includes
exact inputs `R_send>=AR+3×b_u` and `WF>=AD+3×b_u`; fixed lifecycle/marker rows use their
canonical padded encoding. Within that arm,
an unparenthesized resource scalar `x` for `S_actual`, `S`, `B`, debt or `d`,
`K`, `K_remaining`, or `configured_cap`/`cap` is exact shorthand for `(x,x×Bm)`;
`C`, `Ce`, and sequence/order counts remain scalars, and any displayed vector
overrides the shorthand. Every such arm still names its entry and byte caps;
the convention never assigns a generated codec value.

Define componentwise transferable **recovery occupancy** `K=(Ke,Kb)=Q`. Q is
the only borrowable mandatory-transaction envelope; K is never debt. With no edge
and zero debt, `K_remaining=(0,0)` and all K is free. A newly stored nonzero-debt
edge holds `K_remaining=K`. When its guaranteed K-funded successor materializes
actual record charge `r=(re,rb)`, the same transaction adds r exactly once to S/B
and subtracts it componentwise from the incoming K claim:

`0 <= r <= K_remaining`, and `K_remaining' = K_remaining - r`.

For a K-funded fenced Attach or **K-claim-backed detached Leave**, r counts only
records newly appended by this transaction. A terminal that
is already durable contributes zero; a still-Pending terminal appended together
with Attached or Left contributes one in addition to that Attached/Left. Live
Leave is outside this transfer equation (`r_K=0`) and funds its appended record
from the ordinary mandatory-Q class; fenced Attach and K-claim-backed detached
Leave use their exact one- or two-record vector. In R-C4, **K-claim-backed** is a
term of art for the detached owner Leave selected either directly from a current
nonzero-debt `DetachedCredentialRecovery`, `DetachedMarkerRelease`, or
`DetachedCursorRelease` edge, or from a current `ObserverProjection` or
`PhysicalCompaction` whose serialized same-owner strict suffix is one of those
three detached edges and whose stored `K_remaining` backs that exit. A detached Leave with `debt=0,edge=None` still
consumes its R-A2/R-C2 E/X counter claims, but it is an ordinary mandatory-Q
Leave with `r_K=0`; it cannot subtract from the required zero
`K_remaining`. No prose label may substitute Q
or all K for this measured charge.

The edge occupancy fields equal exact post-transfer `K_remaining`, never all K by
fiat. Transferred occupancy is already in B and is not free or counted again; no K
is reusable while debt remains. When repayment clears both debt components, the
edge and `K_remaining` clear atomically and all K becomes free. K covers binding
fate plus pending-terminal/recovery Attach while an anchored marker remains retained.

Every fenced Attach and every **K-claim-backed detached Leave** subtracts the exact
entry/byte charge it appends from `K_remaining` in that same transaction. A live
bound Leave is the ordinary mandatory-Q class, not a K-claim-backed detached Leave: it keeps
K held and is distinguished by the durable live-binding fact even when it shares
the bounded Leave event occurrence. For each edge, `exit_charge` is the exact
pre-owned maximum positive charge of its next K-claim-backed detached Leave (one
record when fate already appended its terminal, two when the terminal is still
Pending). A fenced Attach may expose a recovered binding with nonzero debt only
when its post-transfer `K_remaining >= exit_charge` componentwise; otherwise its
producer's fixed-point preflight must prove that the Attach clears debt/full-K
legality atomically or refuse the optional producer before mutation. This is a
stored check, not an assumption that the recovered participant will cooperate.
That fixed-point check is path-sensitive across the complete serialized successor
tree, not one edge at a time. For every ordering that can make several
K-claim-backed detached Leaves eligible before a TOLD debt-lowering event, it accumulates
their exact appended charges against `K_remaining` after each transfer. A
positive-debt candidate floor is not Envelope-valid when any such serialized
prefix would require a charge greater than the then-current `K_remaining` in
either component. The first transaction that would expose that prefix must
instead reach debt zero and full-K legality in the same commit; if that
transaction is optional, its original producer refuses before mutation. Merely
checking the current edge's scalar `exit_charge` is insufficient.
The same producer simulation applies the actual-base-floor Envelope rule to
every later TOLD observer, compaction, cursor, recovered-fate, live-Leave, and
detached-Leave transition. It rejects a plan whose later preferred floor could
enter the raw-debt-zero/full-K-invalid band or whose recovered path could require
a second marker/DCR quartet. A positive-debt commit may not defer discovery of
either failure to a supposedly guaranteed completion.

Checked startup validation uses this first-failure order:

1. compute exact `4 + I` (`2 × Qe + I` with `Qe=2`) in u128, then test the entry cap;
2. compute `(2 × Qb) + (I × Bm)` in u128, then test the byte cap;
3. compare raw u64 `J_raw` to `2..=u32::MAX`, then set u32 `J=J_raw`;
4. compute the proved-in-range `R_max`, `E_max`, `O_max`, `D_cycle`, `B_cycle`,
   and `O_base` values in their displayed dependency order; and
5. compare the fixed occurrence-array encoded bytes against exact configured
   `Pstate=max_participant_state_bytes` (including the invalid zero boundary).

The first two arithmetic expressions cannot overflow and therefore have no
fabricated arithmetic discriminant. Exact `Qe=2` and a
passing entry-cap check imply `I+4<=Ce<=u64::MAX`, hence `I<=MAX-4`.
The entry expression is at most `MAX`; the byte expression is then at most
`(MAX-4)×MAX+2×MAX<MAX²<u128::MAX`. The occurrence formulas are proved below
to fit u128 and therefore have no error outcome. The outcome dimension names the first exact
failed stage (`EntryCapacity`, `ByteCapacity`, `EpisodeChurnLimit`,
or `SuccessorOccurrenceArray`) and carries its exact operands. No later predicate
is evaluated for outcome selection. A failed selectable predicate is
`ParticipantRetentionCapacityInvalid`. At empty equality, base marker reserve
plus free Q plus free K is exactly `I×marker + Q + K = 2Q + I×marker`.
Ordinary work consumes neither Q nor K. A mandatory transaction may borrow Q but
must leave K physically free for, or atomically held by, its edge. Mandatory
records have no reachable `RecordTooLarge`.

Let `SR=b_u` be the generated exact v1 durable-record fixed overhead, represented as
a u64 with generator-enforced domain `1..=u64::MAX-u32::MAX`; schema generation/
build fails rather than emitting a value outside that domain. The wire constants are
exactly `AR=44` (`16` header/prefix + three u64s + u32 payload length) and
`AD=46` (`16` header/prefix + conversation/delivery u64s + record-kind u16 +
sender u64 + u32 payload length). Thus a legal
opaque payload of p bytes has durable record charge `SR+p`, complete
`0x0007 RecordAdmission` size `AR+p`, and complete `0x0201 ParticipantDelivery`
with `record_kind:0x0000 OrdinaryRecord` size `AD+p`.
They include their respective length prefixes and all
non-payload fields; the fixed-schema checks prove `R_send>=AR` and `WF>=AD`.
Startup computes retention headroom
`H_record=configured_cap-(2Q+I×marker)` and the exact wire-backed byte maximum
`wire_record_max_bytes=SR+min(R_send-AR,WF-AD)` in checked u128, then stores
`ordinary_record_max=(H_record.entries,
min(H_record.bytes,wire_record_max_bytes))`. The ordered validation above proves
every subtraction defined. Let `U=u64::MAX,V=u32::MAX`. The body length and
normalized complete-frame ceiling make every legal opaque p and the minimum
term at most V, while `SR<=U-V`; therefore both the widened maximum and every
exact `SR+p` charge are at most U. The addition cannot fail, its byte component
remains the R-D2 u64 scalar width, and no arithmetic outcome or wider payload
field is needed.
Thus any record admitted under this maximum has both
an encodable request and every owed complete delivery before it commits; there is
no post-commit FrameTooLarge dead end. After participant authority and operation
proof but before order/sequence/observer/closure admission, an ordinary record
with exact encoded durable charge `r=(1,encoded_record_bytes)` tests Entries then
Bytes. The first component with `r > ordinary_record_max` returns
`RecordTooLarge { conversation_id, participant_id, capability_generation,
dimension: Entries | Bytes, encoded_record_charge: r,
max_ordinary_record_charge: ordinary_record_max }`. Equality continues. A
smaller record that fails only because of current retained occupancy receives
`MarkerClosureCapacityExceeded`, not `RecordTooLarge`. Acceptance exercises all
three legal relations without inventing an impossible record shape: Bytes holds
one configured maximum fixed and varies legal payload charges below/equal/above
it; Entries holds the only legal ordinary charge `1` fixed and varies the
configured maximum through `2/1/0`. Case 32 is the exact proof. Equality passes
both the retention and wire inequalities. The SDK-local
encoded-frame limit remains the distinct `SdkParticipantRequestTooLarge` gate.

Each conversation header has one fixed-size durable
`ClosureDebt { entry_debt: 0..=Qe, byte_debt: 0..=Qb,
repayment_edge, edge_claims }`, initialized to
zero, `None`, and zero claims. `edge_claims { marker_sequence_values: [(value, provenance_tag, current_sequence_owner, causal_tuple); I],
recovery_attach_sequence: Option<u64>, replacement_terminal_sequence:
Option<u64>, recovery_attach_order: Option<u64>, replacement_terminal_order:
Option<u64>, candidate_positions, successor_milestones, occupancy_entries,
occupancy_bytes, episode_churn_used, episode_churn_limit }` is fixed-width.
`episode_churn_limit=J`, `episode_churn_used` starts at zero with each new debt
episode, survives every edge replacement and restart, and clears only with debt.
Candidate positions are I-bounded.
`successor_milestones` is an occurrence-bounded array indexed by exact
`occurrence_ordinal`; each initialized Pending/Consumed slot carries
`(causal_transaction_order, event_kind, participant_index)`, while Dormant carries
no causal key. Each fixed slot stores `Dormant | Pending | Consumed`. The base producer
serializes all `O_max` slots. A churn transaction may initialize only the next
J-bounded dormant cycle after reserving all of its exact facts. Within one debt-
episode instance, no transaction can grow the array or turn Consumed back into
Pending.

A transaction that makes both debt components zero must retire the entire
episode atomically. Before storing `edge=None`, it marks Consumed every still-
Pending occurrence whose fact was dominated, invalidated, or can no longer
select a non-None closure successor, zeroes every edge claim and
`episode_churn_used`, and replaces the retired instance by the canonical empty
all-Dormant array. There is no state with zero debt and an episode-Pending slot.
A debt-zero transaction retires only the closure-episode occurrence. For every
already-dispatched durable-prefix projection, it atomically transfers the
remaining interest to the ordinary R-A4 event-driven projector. On restart,
the one bounded startup load derives and re-registers that ordinary interest
from `(o,F)` and the retained causal row; it uses neither a retired-key registry
nor a periodic scan or poll.
A later debt producer serializes a new fixed-length instance from scratch; that
replacement is not a Consumed→Pending transition in the retired instance.
Every storage notification carries the stored causal key and `through_seq`; no
retired-key registry is retained. Classification after retirement is derived
from hard observer progress and the retained log. If `through_seq<=o`, the
notification is dominated and is an exact no-op. If `through_seq>o`, hard
retention gives `F<=o+1<=through_seq`, so the exact sequence row and its causal
tuple remain retained. A key match proves that the durable prefix really
materialized and performs the full ordinary R-C4 projection-completion
transaction against current state: set `o'=max(o,through_seq)`, recompute the
actual-base Envelope, and store `F'=max(F,preferred_floor,cap_floor)` with every
resulting removal and marker-credit release while selecting no repayment tag. A
key mismatch proves that cancellation prevented that causal row from
materializing; its future registration was disarmed, so the notification is an
exact no-op and cannot advance o. Either disposition preserves any unrelated
later episode edge and cannot recreate the retired edge or consume a same-
ordinal slot in a later episode. The comparison therefore needs no unbounded
retired-key state and still distinguishes a canceled marker from a different
durable row at the same sequence.

At episode creation, each of the at-most-I current identity slots receives one
pre-endowed **base cycle** covering that binding's unrefusable first fate/Leave
alternatives and every marker/closure alternative already present in the
episode-creator's complete fixed point. The same base cycle may back a marker
that an unrefusable pre-owned successor later materializes, but it is not a free
ticket for a later optional request to introduce a previously absent marker
fact with a different causal transaction. An **episode-churn cycle** is any
later successful enrollment, credential attach or supersession, any marker plan
not already fixed by the current base/churn cycle's causal transaction, or any
other optional transaction which, while debt is nonzero, changes a binding/fate/
marker fact or exact edge payload/range. Preflight computes `delta_cycles` as
the exact number of distinct new binding/fate/closure cycles plus distinct new
marker causal cycles introduced by the transaction. Consequently a later
optional transaction that plans k previously absent markers for k identities
charges k, not one, even when those identities still have unused base fate
alternatives. The episode creator and an unrefusable successor charge zero only
for alternatives their producer already serialized; this is tighter than
counting one transaction because each of the k marker facts can later diverge
under a different identity fate, and the k prescribed occurrence blocks are
the finite proof. The transaction atomically adds `delta_cycles` to
`episode_churn_used` before exposing the changes and pre-endows every charged
cycle's fate/closure alternatives. An ordinary record or ack
whose facts do not change the fixed debt plan consumes no churn; if it would
change a planned range, marker set, or successor payload, it is a churn cycle.
After the `DeliveredMarkerAwaitingAck` fence, an otherwise eligible optional
transaction computes
`u128(episode_churn_used)+u128(delta_cycles)`. Here
`episode_churn_used<=J<=u32::MAX` and one producer charges at most one binding
cycle plus one distinct marker cycle per identity, so
`delta_cycles<=I+1<=u64::MAX-3` by the earlier `I<=MAX-4` proof. The u128 sum
therefore cannot overflow; there is no unreachable overflow arm. If that exact sum
exceeds `u128(J)`, the transaction is refused at R-D1's closure stage, after the earlier order, sequence,
and observer checks but before floor, binding, record, marker, edge, receipt, or provenance mutation,
as the complete R-D1 `MarkerClosureCapacityExceeded` payload with
`scope:EpisodeChurnLimit`, exact `episode_churn_used`, `delta_cycles`,
`episode_churn_limit:J`, and every common field; the Capacity-only fields are
absent. An unrefusable binding-fate
event uses the fate alternative pre-endowed by the binding cycle that exposed
that epoch; it cannot create another binding. Every marker/range change caused
by that fate or by its E/X-counter-backed Leave is already part of the same cycle's
serialized closure alternatives. E/X-counter-backed Leave therefore remains the
ticket-free terminal successor, so the limit creates no dead end. Every base
fixed point reserves the current at-most-I binding cycles, and every accepted
new cycle reserves its own later fate before commit.

Let `R_max=Ce+6×I+7×J`. Ce covers every actual or planned retained entry at
episode creation. For each of the at most `I+J` base-identity or churn cycles,
three deliberately loose Q envelopes cover its record-producing attach or
supersession, fate-terminal, and recovery-or-Leave alternatives. The final J
covers one later marker record per churn cycle. Thus even alternatives that do
not coexist have a serialized record fact, and unrelated ordinary records that
never change the debt plan own no successor occurrence. Projection completion,
physical-compaction completion, and strict cursor progress each consume at least
one previously unconsumed participating record, so each class has at most R_max
unique facts. A cumulative normal ack that advances across several records owns
one `CursorProgressed` fact keyed by the highest newly covered participating
record/boundary; lower covered records do not create extra facts. Consequently
exact, lesser, and greater-than-stored-witness ack orderings all use the same
finite record-indexed coordinates. For each of the `I+J` base-identity or churn cycles there is at most one
unique fact of each remaining kind: marker append, marker delivery, binding
fate, fenced recovery, and Leave. Recovery and Leave are counted
separately because both alternatives must be serialized even though only one
executes. Thus the number of unique event facts is bounded by

`E_max = (3 × R_max) + 5×(I+J)
       = 3×Ce + 23×I + 26×J`.

Pre-delivery supersession transfers one MarkerDelivery plan and charges one
churn cycle, but fires no event occurrence. After that
fact is consumed, the delivered-marker supersession fence below permits only ack,
fate, or Leave, never redelivery to another epoch. A credit cannot own another
marker until physical compaction. Hence each base-identity or churn cycle contributes at
most one marker-delivery fact; retargets share the current cycle's one fact,
including under arbitrary external supersession attempts within J.

One event fact owns one event occurrence irrespective of arrival order. Its
successor is a deterministic function of the other already recorded facts, but
the producer must serialize every alternative that could be selected: at most
the eight complete enum results (`None` plus seven non-None tags). Therefore each
event fact consumes at most one event slot plus eight keyed selection slots; the
producer consumes one initial-selection slot. The deliberately loose checked
bound over the **stored branch tree**, not merely one executed path, is

`O_max = 1 + (9 × E_max)
       = 1 + 27×Ce + 207×I + 234×J`.

This array has a reproducible partition. Let
`D_cycle=234` and
`B_cycle=207` and
`O_base=O_max-(J×D_cycle)=1+27×Ce+I×B_cycle`. Ordinal 0 is the producer's
initial selection. In the next `27×Ce` ordinals, retained/planned record index
`q` in `0..<Ce` owns three nine-slot groups beginning at `1+27×q`: projection at
offset 0, compaction at offset 9, and cursor progress at offset 18. For
alternatives sharing a reserved value, the exact immutable sort key is reserved
`delivery_seq`; `provenance_tag` order
`NonProductM < TerminalProduct(T) < TerminalProduct(RT) < ExitProduct(E)` and
then its affected/remaining permanent participant index; the R-A2 causal tuple
`(transaction_order,candidate_phase,participant_index)`; phase-4 candidate
position; and record participant index. `current_sequence_owner` is
deliberately absent: its legal `ConditionalProduct→M` change cannot relocate an
occurrence coordinate. Base identity index `i` in
`0..<I` owns the block beginning `1+27×Ce+i×B_cycle`; its first five groups are
MarkerAppended, MarkerDelivered, BindingFateObserved, FencedRecoveryCommitted,
and LeaveCommitted, and its remaining 18 groups are projection, compaction,
and cursor progress for each of at most six future record facts in that same
exact sort. These coordinates total exactly `O_base` and replace fixture-local
hand-packing.

All operands in this startup calculation are first widened to u128. The
canonical suboperation order, used to reproduce every derived value, is:

1. `r0=6×I`, `r1=7×J`, `r2=Ce+r0`, `R_max=r2+r1`;
2. `e0=3×Ce`, `e1=23×I`, `e2=26×J`, `e3=e0+e1`,
   `E_max=e3+e2`;
3. `o0=27×Ce`, `o1=207×I`, `o2=234×J`, `o3=1+o0`, `o4=o3+o1`,
   `O_max=o4+o2`;
4. `D_cycle=234` and `B_cycle=207`; and
5. `c0=27×Ce`, `c1=207×I`, `c2=1+c0`, `O_base=c2+c1`.

`O_base` uses the direct final expression; it does not execute a subtraction.
Expanding the preceding definitions gives
`O_max=O_base+J×D_cycle`, which proves the displayed identity cannot underflow;
no subtraction is executed.

No suboperation above can overflow in v1. With `U=u64::MAX` and
`V=u32::MAX`, the closed forms are `D_cycle=234`, `B_cycle=207`, and

`O_max = 1 + 27×Ce + 162×(I+J) + 45×I + 72×J
       = 1 + 27×Ce + 207×I + 234×J
       <= 1 + 234×U + 234×V < 2^72`.

The displayed dependency order also gives `R_max<2^69`, `E_max<2^72`, and
`O_base=1+27×Ce+207×I<2^72`; every intermediate Add/Multiply therefore fits
u128 by a margin of more than 56 bits. The fixed v1 serialized occurrence-slot
width `W_occ` is generator-computed and build-checked in
`1..=min(2^56-1,floor(u64::MAX/811))`, so exact
`encoded_bytes=O_max×W_occ` also fits u128 and Case 54's smallest valid array
fits the `Pstate` u64 domain. These tighter proofs eliminate any
occurrence-specific runtime/configuration arithmetic outcome or occurrence
Add/Multiply reason arm; R-A4's independently reachable
`CheckedProduct { operation:Multiply }` configuration arm is unchanged.

The tail is exactly J contiguous
Dormant blocks: for each block index `j` in `0..<J`, churn block `j` starts at
`O_base+j×D_cycle`. Each block has
26 nine-slot event groups. Groups 0..4 are respectively MarkerAppended,
MarkerDelivered, BindingFateObserved, FencedRecoveryCommitted, and
LeaveCommitted. The remaining 21 groups are, for each of the
cycle's at-most-seven participating record facts in the same exact sort,
ProjectionCompleted, CompactionCompleted, then CursorProgressed. In every group,
offset 0 is the event and offsets 1..8 are selections in the fixed tag order
`None, ObserverProjection, PhysicalCompaction, MarkerDelivery,
ParticipantCursorProgress, DetachedCredentialRecovery,
DetachedMarkerRelease, DetachedCursorRelease`.

The group key, not narrative arrival order, owns these alternatives. If fate or
Leave arrives while an OP or PC witness is current, that lifecycle event consumes
its event slot plus the selection offset for the unchanged OP/PC; the later
storage completion consumes its own event slot plus the offset selected from the
durable fate/Leave fact. The same cross-product applies when ack first replaces
cursor progress by OP/PC and fate or Leave arrives before that storage witness
completes. Thus every legal permutation has two separately keyed selections
(`lifecycle→preserved witness`, then `witness completion→strict suffix`), never
one slot reused by two parent events. Producers mark every tag reachable under
these permutations Pending and every impossible tag Consumed. This rule also
covers Leave-before-existing-OP: Leave preserves that OP, whose completion then
selects the separately indexed post-Leave OP/PC/None payload.

A transaction charging delta cycles atomically maps its exact new cycles to the
next delta Dormant blocks in causal/admission order, writes every exact key/
payload/reserved-value index into the prescribed groups, marks impossible groups
and selections Consumed, and marks the rest Pending. A retarget does not fire an
event occurrence: it retires the superseded active cycle's still-Pending plan
slots as Consumed, transfers every still-fireable sequence/order/position claim,
and activates the next block. Array length never changes. Restart returns
`ParticipantStateCorrupt` with respectively `NonPrefixChurnBlock {
first_bad_block }`, `OccurrenceKeyOutsideGroup { occurrence_ordinal }`,
`ChurnUsedMismatch { used, activated_blocks }`, or
`UnbackedPendingOccurrence { occurrence_ordinal, claim_kind }` for a non-prefix
allocation, key outside its group, unequal counter, or Pending fact without the
exact currently resolved transferred claims.

There is exactly one selection occurrence per `(event occurrence, successor
enum tag)`, not per payload permutation. A selection slot stores the tag and the
indices of already-owned position/range facts; it does not duplicate an OP
`through_seq`, PC range, participant, epoch, or marker value. If live/fate or
arrival-order facts make the same event choose the same tag with different exact
payload fields, one deterministic table lookup over those durable facts and
pre-owned indices constructs the payload while consuming that one tag slot.
Fixtures call such a slot **shared**. A second same-tag slot for the same event is
`ParticipantStateCorrupt { conversation_id, reason: DuplicateSuccessorTag {
event_occurrence_ordinal, successor_tag } }`. This is why eight, rather than the number of payload
permutations, is the exact per-event selection bound.

Candidate-state commit validation and startup decode use one exhaustive
first-corruption selector before overwriting or admitting traffic. Test in this
reason order: `DuplicateCandidateKey` (lowest tuple),
`ClaimFrontierInvalid` (DeliverySeq before TransactionOrder, then lowest
first_bad_position), `NonPrefixChurnBlock` (lowest block), `ChurnUsedMismatch`,
`DuplicateSuccessorTag` (lowest event ordinal, then fixed enum-tag order),
`OccurrenceKeyOutsideGroup` (lowest occurrence ordinal), then
`UnbackedPendingOccurrence` (lowest occurrence ordinal, then claim-kind order).
That final `claim_kind` enum and order is exactly `MarkerSequenceValue`,
`RecoveryAttachSequence`, `ReplacementTerminalSequence`,
`RecoveryAttachOrder`, `ReplacementTerminalOrder`, `CandidatePosition`,
`OccupancyEntries`, then `OccupancyBytes`; these are all occurrence-indexed
backings in `edge_claims`. If one Pending occurrence lacks several, the first in
this list is reported.
The first reason wins and no later predicate is disclosed. In particular, a
second same-tag slot necessarily also violates fixed group placement, but
`DuplicateSuccessorTag` wins before `OccurrenceKeyOutsideGroup`. On candidate
commit the proposed transaction aborts and the conversation fails closed while
the previous durable bytes remain intact; on startup the corrupt bytes remain
intact. Thus the one outcome covers both sites named by R-A2 without pretending
that commit-time validation is startup-only.

`occurrence_ordinal` is a fixed-width u128 in `0..<O_max`; the proved-u128
calculation and the complete serialized `O_max` array size are validated before
participant mode. A serialized array above configured `Pstate`
returns `ParticipantRetentionCapacityInvalid { dimension:
SuccessorOccurrenceArray, O_max, encoded_bytes, limit }` before traffic. The churn counter and J are
serialized in this calculation; supersession itself remains absent from the
event-occurrence kinds under the non-consuming retarget rule below. Marker
values identify their exact R-C2 `NonProductM`, `TerminalProduct`, or
`ExitProduct` immutable provenance tag and current sequence owner; recovery
fields are exactly RS/RT/RO/RA in the two reserve invariants; order/
positions are exact R-A2 tuples; and occupancy equals exact post-transfer
`K_remaining(edge)` in each dimension. Serialization includes every field and fixed maximum byte. No claim is
double-counted or implicit.

The complete `repayment_edge` enum is:

- `None`;
- `ObserverProjection { through_seq }`;
- `PhysicalCompaction { from_floor, through_seq }`;
- `MarkerDelivery { participant_id, binding_epoch, marker_delivery_seq }`;
- `ParticipantCursorProgress { participant_id, binding_epoch, through_seq,
  marker_delivery_seq: Option<DeliverySeq> }`; and
- `DetachedCredentialRecovery { participant_id, marker_delivery_seq,
  prior_binding_epoch }` for a marker durably delivered to that prior epoch; and
- `DetachedMarkerRelease { participant_id, marker_delivery_seq,
  last_dead_binding_epoch }` for a marker not delivered to the named last live
  epoch; and
- `DetachedCursorRelease { participant_id, last_dead_binding_epoch }` for a
  no-marker cursor-progress witness invalidated by that epoch's fate.

Every binding/sequence value in a tag is exact and immutable. A planned marker's
`marker_delivery_seq` is its pre-owned value even before append. On restart the
participant owner re-registers exactly the tagged storage/projection/compaction/
delivery/cursor/recovery interest and continues from its recorded claims; it
never scans for a substitute witness.

Let `S_actual` be the actual encoded retained suffix. Let closure charge `S` equal
actual non-marker retained cost plus `marker_max` for every planned or physically
retained credited marker; therefore `S_actual<=S` in each dimension. Let `C` be distinct reserved
identity slots currently owning a **marker capacity credit**. A credit is acquired when a marker is planned and
survives candidate append, delivery, and acceptance until that exact marker is
physically compacted. It can end earlier only when Leave cancels a still-unwritten
candidate in the same transaction. If that cancelled candidate owns the next
gap-free sequence value, the exact-current ordinary-Q bound Leave or K-claim-backed detached Leave
atomically transfers that never-appended value's current owner from `M` to
its one `Left` record and releases the otherwise-unused E value. The same transfer
is legal when the candidate retains immutable `TerminalProduct` or `ExitProduct`
provenance **after** its causal T/E has fired and changed the current owner to M;
the cancellation changes that current M owner to the Leave's E/Left handle and
releases the already-fired product linkage exactly once. An unfired conditional
product is not a candidate and cannot be cancelled. Thus no numeric value
is appended twice, double-released, or returned to an unowned pool. Cancellation is legal only if
no other candidate remains earlier than the Leave tuple; otherwise that prefix
must drain first and Leave does not fabricate a sequence or admission-order gap.
Thus `C` may temporarily include a retired
identity whose accepted marker remains retained, and always satisfies `0<=C<=I`.
One identity cannot own a second marker candidate/record until physical compaction
(or same-transaction cancellation) releases its credit. Write the componentwise retained baseline as

`B = S + ((I - C) × marker_max)`.

With zero debt, `edge=None`, K is free, and ordinary admission must preserve:

`B + Q + K <= configured_cap`.

Marker acceptance changes only cursor/anchor state: `S_actual`, `S`, `C`, and
remaining marker reserve are identical before and after, so it is append-free and
can never return a capacity refusal. Planned→actual append retains the credit and
its `marker_max` charge in `S`; only `S_actual` changes from no record to actual
encoded cost, already bounded by that charge. Later physical compaction removes
actual cost from `S_actual` and exactly `marker_max` from `S` while releasing the
credit adds exactly one
`marker_max` to remaining reserve. The marker's contribution to closure required
capacity is therefore equal before and after compaction; any co-compacted non-
marker prefix can only reduce it. Debt cannot increase.

Ordinary admission must leave the zero-debt equation true and otherwise returns
`MarkerClosureCapacityExceeded`. Mandatory classes collectively borrow at most Q.
For a mandatory proposed post-state that stores an edge, let `B'` use the formula
above and let `K_remaining'` be its exact post-transfer componentwise occupancy.
Its debt and absolute-fit checks are:

`d' = max(0, B' + Q + K_remaining' - configured_cap)`,

`B' + K_remaining' <= configured_cap`, and `d' <= Q`.

Thus debt is exactly borrowed Q. For an incoming recovery transaction of charge
r, let `B_removed` be the baseline after its exact floor removals. The transaction
uses `B'=B_removed+r` and `K_remaining'=K_remaining-r`, so positive recovery
records fit without double-counting. Edge occupancy is not a free variable: the
serialized `occupancy_entries/occupancy_bytes` claims equal `K_remaining'`
componentwise. A no-edge post-state is legal only with
debt zero and `B'+Q+K<=cap`. The candidate appears once in S', never again as Q
or K. Commit also requires the complete R-C2/R-A2 RS/RT/RO/RA and marker claims.
At startup equality, let enrollment's one actual `Attached` charge be
`q_enroll=(1,b_enroll)` with `b_enroll<=Qb`. Then
`B'=I×marker+q_enroll`, `d'=max(0,B'+Q+K-cap)=q_enroll`, and
`B'+K=cap-Q+q_enroll`; enrollment is not an exact-Q transaction. A genuinely
exact-Q supersession pair reaches debt Q. In either walk the producer stores
`K_remaining=K`; later recovery charge transfers from that claim into B.

While either debt component is nonzero, ordinary caller admission returns
`MarkerClosureCapacityExceeded`. A mandatory transaction may commit at any debt,
including `(Qe,Qb)`, only if absolute fit and the bounded equations hold and its
post-state either lowers debt for everything consumed or stores one valid row of
this closed successor machine with all resources through its finite fixed point
already owned:

| Stored edge | TOLD event and exact witness | Complete invalidators and atomic successor/effect |
|---|---|---|
| `ObserverProjection` | Projection storage completion through exact `through_seq`. | `ProjectionCompleted` advances o and applies the actual-base Envelope in the same transaction. Debt clearance selects `None`; otherwise the exact strict suffix is a strictly later `ObserverProjection`, `PhysicalCompaction`, `MarkerDelivery`, `ParticipantCursorProgress`, `DetachedMarkerRelease`, or `DetachedCursorRelease`. Direct DCR is impossible: a delivered-unaccepted marker makes PCP the strict witness. Each preclaimed `MarkerAppended` occurrence whose sequence extends the projected suffix atomically replaces OP with the exact later OP; the final OP completion, not marker append, selects first delivery. An independently valid cursor/marker ack consumes its own `CursorProgressed` occurrence and preserves this exact OP while debt remains or clears to `None`; fate/Leave likewise preserve it while debt remains. A binding change preserves/retargets only through charged churn, else refuses. These are the complete orderings. |
| `PhysicalCompaction` | Enabled storage compaction of exact `[from_floor, through_seq]`. | `CompactionCompleted`, or an atomic overlapping advance whose new floor is greater than `through_seq`, satisfies the range, releases only removed accepted-marker credits, and selects `None`, a strictly later `ObserverProjection`/`PhysicalCompaction`, `MarkerDelivery`, `ParticipantCursorProgress`, `DetachedMarkerRelease`, or `DetachedCursorRelease`. Direct DCR is impossible for the same strict-witness reason. An advancing cursor/marker ack whose resulting floor does not cover the range consumes its own pre-owned `CursorProgressed` occurrence plus preserve-PC selection and keeps exact PC; one whose floor covers it consumes that ack occurrence and PC atomically, then selects the suffix from the actual post-ack state. A no-op or refused ack consumes neither and preserves PC. Fate and Leave use the same preserve-or-cover rule with their separate exact occurrences. Binding change applies that result inside its charged churn transaction and activated block but fires no event occurrence; optional churn above J refuses. These are the complete orderings. |
| `MarkerDelivery` | Candidate append/storage completion and final-emitter delivery of the exact marker to the exact binding epoch. | Delivery becomes `ParticipantCursorProgress` with `marker_delivery_seq=Some(exact marker)`. A valid lower normal ack, independent `ProjectionCompleted`, or compaction completion below the marker anchor applies Envelope and preserves/retargets exact `MarkerDelivery` while debt remains or clears to `None`; each has its own event/selection occurrence. Detach/death **before that delivery** makes the identity detached and selects `DetachedMarkerRelease`; no fenced proof is fabricated. Supersession atomically retargets exact delivery to its new epoch after full fixed-point preflight; Leave atomically releases the anchor and selects observer/compaction or clears debt. These are the complete invalidators. |
| `ParticipantCursorProgress` | A valid current-epoch normal ack whose contiguously offered requested `through_seq` is at least the stored witness, or the exact marker ack for a stored `marker_delivery_seq:Some(m)` witness. | Equal normal/marker completion consumes the stored PCP occurrence, advances only that cursor, releases the anchor only for exact marker ack, applies Envelope, and selects exactly `None`, `ObserverProjection`, or `PhysicalCompaction`. A greater valid cumulative normal ack consumes the pre-owned record-indexed `CursorProgressed` occurrence for its actual requested boundary, atomically satisfies the lower stored witness, advances once to that greater boundary, and selects the suffix from the actual post-ack state; it never exposes an intermediate equal-witness state. That same transaction marks every lower newly covered `CursorProgressed` event group—including the stored-witness group—and all of their selection slots Consumed, so restart cannot replay a covered lower boundary. A lesser valid advancing normal ack consumes its own pre-owned record-indexed `CursorProgressed` occurrence and preserves/retargets the remaining exact PCP witness; a no-op consumes none and preserves it; `AckGap`/`AckRegression` likewise preserves it without consuming an occurrence. An independently completed observer projection or physical-compaction range applies Envelope and preserves/retargets exact PCP while debt remains or clears to `None`; a compaction range can never pass an unaccepted marker anchor. Detach/death with `Some(m)` plus the durable exact-epoch delivery fact creates DCR; with `None` it creates DCursor. Supersession with `None` retargets; supersession with `Some(m)` refuses `DeliveredMarkerAwaitingAck`. Leave performs its measured effect and replacement. All other successor offsets are Consumed. These are the complete orderings. |
| `DetachedCredentialRecovery` | Exact-current tokenized fenced attach or K-claim-backed detached Leave after prior fate. | Fenced attach atomically accepts the named marker, transfers its actual terminal/Attached charge from K_remaining into B, releases the anchor, consumes the episode's sole RS/RT/RO/RA quartet into Attach/new T/A, and consumes one churn cycle. The terminal is part of that charge only in R-A2's zero-induced-marker composition; otherwise terminal plus same-major marker prefix already drained and the attach charge is exactly one record. Debt-zero/full-K legality selects `None`; otherwise it selects independent `ObserverProjection` or `PhysicalCompaction`, **never `MarkerDelivery`**. If that recovered epoch dies before debt clears, its pre-endowed fate makes the storage suffix select Leave-only `DetachedCursorRelease`; no second recovery quartet is possible. K-claim-backed detached Leave likewise transfers actual charge, spends E/X, releases the anchor, and selects observer/compaction or clears debt. An otherwise-authorized ordinary non-fenced attach while this anchored nonzero-debt edge exists returns the complete generic closure outcome selecting `scope:RecoveryFence`, without mutation. Authority supersession before commit is `StaleAuthority` and leaves the edge current. These are the complete invalidators. |
| `DetachedMarkerRelease` | Exact-current K-claim-backed detached Leave for an undelivered marker whose named binding epoch is dead. | This is an intended Leave-only edge. An otherwise-authorized ordinary attach returns the complete generic closure outcome selecting `scope:RecoveryFence`; an attach presenting this marker returns `MarkerNotDelivered`, because no durable delivery fact exists. K-claim-backed detached Leave transfers its actual charge from K_remaining, spends E/X, releases the undelivered anchor, and selects observer/compaction or clears debt subject to debt-zero full-K legality. Repeat fate is a no-op; supersession is `StaleAuthority`. The occurrence plan contains the exact Leave and successor selections. |
| `DetachedCursorRelease` | Exact-current K-claim-backed detached Leave after the named binding dies with `marker_delivery_seq=None`. | This is an explicitly intended Leave-only dead end. Its sole participant successor is K-claim-backed detached Leave. An otherwise-authorized credential attach with `accept_marker_delivery_seq=None` returns the complete generic closure outcome selecting `scope:RecoveryFence`; one presenting any marker returns `MarkerMismatch` because this edge owns no marker. Normal/marker ack and ordinary admission require a live binding and return `NoBinding`. K-claim-backed detached Leave transfers its exact one- or two-record charge from K_remaining, spends E/X, removes the soft cursor claim, and selects observer/compaction or clears debt subject to full-K legality. Repeat fate is a no-op and supersession is `StaleAuthority`. |

In each detached row, **sole participant successor** constrains the named edge
owner, not a different identity's already-pre-owned lifecycle fact. An unrelated
server fate atomically preserves that exact detached edge or clears debt; an
independently authorized unrelated ordinary-Q live Leave or K-claim-backed detached Leave may release only
its own claims and then preserve the named edge, clear debt, or return the exact
`ObserverBackpressure` selected before mutation. A serialized branch that would
instead need a closure-capacity refusal makes the original optional producer
refuse; an unrefusable fate, ordinary-Q live Leave, or K-claim-backed detached Leave never discovers that failure.
The cumulative `K_remaining` check above includes these cross-identity orderings.
They consume the unrelated identity's existing event occurrence and the existing
shared preserve-current successor tag; they add neither an event kind nor a ninth
successor result to `O_max`.

MarkerAck therefore has two explicit ordinary orderings after the actual-base
Envelope recomputation. Acceptance itself leaves S_actual, S, and C unchanged.
If that recomputation reaches debt zero/full-K legality, the closure edge becomes
`None` even though ordinary projection/compaction work for a retained accepted
marker may continue outside ClosureDebt. Otherwise, o behind the marker selects
`ObserverProjection`, while o already through it selects `PhysicalCompaction`.
Projection without member ack similarly materializes its preclaimed marker and
moves to delivery/cursor progress rather than losing its witness.

A binding-fate event invalidating delivery and any event satisfying projection,
cursor, or compaction performs the named debt effect and successor replacement
in **the same durability transaction**. There is no crash point between witness
invalidation and successor commit. Any other event that can invalidate a witness
is a contract defect unless added as a row; `None` is legal iff both debt
components are zero.

**Resource-backed edge invariant.** Before storing any edge, the transaction
simulates every marker, invalidator, and every post-completion successor-selection
state in its finite plan; assigns each marker's immutable provenance tag,
current sequence owner, credit, causal tuple, the sole
RS/RT/RO/RA quartet, and exact K_remaining; and serializes the remaining-milestone
collection. Event kinds are `ProjectionCompleted`, `CompactionCompleted`,
`MarkerAppended`, `MarkerDelivered`, `CursorProgressed`, `BindingFateObserved`,
`FencedRecoveryCommitted`, `LeaveCommitted`,
and selection of each repayment-edge tag or `None`. Repeated event kinds have
distinct causal tuples/ordinals. A non-churn event consumes exactly one Pending
occurrence, except that one `ProjectionCompleted { through_seq:s }` also marks
Consumed every still-Pending lower `ProjectionCompleted` event group with
`through_seq<=s` and all of those groups' selection slots: durable projection
through s semantically completes the prefix. A later notification for one of
those retired keys follows the ordinary monotone-progress rule above and fires
no second episode event. A successful churn transaction charges its positive exact delta and
may initialize only that many next preallocated Dormant blocks; it never changes array length,
and no event can revive a Consumed slot. A later allocation that would consume an owned value,
position, credit, or byte/entry must transfer a still-fireable suffix atomically or
refuse before mutation. It may not borrow E/T/M/A/X or edge claims. At equality,
an edge fires entirely from those claims or the original optional producer was
refused by its first sequence/order/closure outcome.

`LeaveCommitted` is the debt episode's sole per-identity Leave fact, whether
Leave linearizes while the binding is live or after fate. Its keyed selection
alternatives carry distinct exact live/detached record and floor vectors. The
detached alternative is the K-claim-backed detached class and transfers its displayed
actual charge from K_remaining; the live-bound alternative is the ordinary
mandatory-Q class and keeps K fully held. The durable binding-fate bit selects
the class before capacity/floor preflight, so sharing the event occurrence never
aliases their accounting. Because the alternatives are mutually exclusive,
they do not create a second Leave event occurrence.

**Bounded constant-array supersession retarget.** Supersession fires no event
occurrence and does not claim progress. After the complete churn,
sequence/order/capacity fixed-point preflight succeeds, its normal ordered
terminal/Attached pair and generation/order/sequence changes commit while the
same transaction adds its exact delta, consumes the old plan without firing it,
activates the next churn block(s), and transfers the current target:
`MarkerDelivery` retargets delivery; `ParticipantCursorProgress(None)` retargets
cursor progress. Exact RS/RT/RO/RA and every affected range/position claim move
to the active block. A gap-free supersession may atomically right-shift a
still-unmaterialized claim vector to fresh greater values, remove every old
ownership, and only then use the newly unowned next value for its caller-funded
record/major. That is reassignment of an unmaterialized claim, not firing RS/RO
or borrowing A; no appended sequence or allocated tuple is ever reused. A
`ParticipantCursorProgress(Some(m))` target has consumed its one exact delivery
fact and is not retargetable: credential supersession refuses with the named
`DeliveredMarkerAwaitingAck` closure scope before generation/order/sequence
mutation. When the delivered fence does not apply, the distinct
`EpisodeChurnLimit` closure scope wins if the proposed delta would exceed J before those
same mutations. Array length, marker value, credit, and K_remaining are
byte-identical; active/Consumed/Dormant slot bytes, exact transferred claims,
ordinary lifecycle counters/history, floor when capacity requires it, and the
fixed churn counter change exactly as stated. Restart
reconstructs the single current epoch from that edge; old-epoch delivery/work is
fenced. At most J valid pre-delivery or no-marker churn cycles therefore use
constant array storage, while deliver→supersede→redeliver is not a legal cycle.
The next actual completion, fate, or Leave consumes its Pending strict suffix.
R-D1 classifies `RecoveryFence` as an earlier operation-proof failure. After
order, sequence, and observer checks, the remaining closure order is
`DeliveredMarkerAwaitingAck`, `EpisodeChurnLimit`, then componentwise `Capacity`;
all other outer precedence is exactly R-D1's list.

Each debt episode stores at most one RS/RT/RO/RA quartet and consumes it exactly
once. An ordinary attach that could later need DCR must pre-own that death branch
before the attach commits. If no prior quartet exists, the second-quartet
predicate is false and any numeric shortage reaches R-D1's later order/sequence
stages. If the nonzero-debt episode already has an active or consumed quartet and
the proposed optional operation would require a new quartet instead of
transferring all four still-unused handles, `RecoveryFence` is true and wins at
the earlier operation-proof stage regardless of any simultaneous later numeric
shortage. This predicate is independent of scalar Capacity and forbids the
proposal even if every entry/byte component would fit. Thus no path allocates a
second quartet. Once DCR is anchored, ordinary
non-fenced attach is forbidden. Fenced attach consumes RS/RO and transfers RT/RA
into the new binding's T/A, but marker acceptance ends the DCR cycle and its
observer/compaction successor is independent of later binding fate. K-claim-backed
detached Leave ends it directly. No transition re-endows a quartet, and consumed numeric
values are never reused.

**Closure/induction proof sketch.** Name the lexicographic decreasing measure
`μ=(J-episode_churn_used, unconsumed_successor_occurrences,
entry_debt+byte_debt, M)`. The debt-producing
base commit proves the componentwise Q/K equations and owns at most `O_max`
fixed slots. Every churn cycle strictly lowers μ's first component even if it
initializes Dormant slots; every non-churn table event uses one occurrence and can select only a strict
suffix; a clearing transition leaves the nonzero-debt domain for terminal bottom
`⊥` before the stored counter resets. Within one floor preflight the named
measure ν starts at I and decreases by one whenever a previously unaffected
identity becomes a marker owner. Since ν never drops below zero, at most I steps
close the marker set. Per-row invalidator
audit is complete as stated in the table: observer/storage witnesses have only
completion/range advance; delivery/cursor add fate, constant-space supersession
retarget, and Leave; the three detached tags name their sole attach/Leave sets.
Each consuming invalidation and successor replacement
is one transaction. Induction on μ therefore preserves: nonzero debt has exactly
one valid tag, exact `K_remaining`, complete claims, and no crash-visible invalid witness.

**Anchored recovery occupancy.** Whenever an edge is stored, preflight every
occupancy-consuming operation against the worst binding-fate plus pending-terminal-
and-detached-attach sequence with the marker anchor held. Exact K is unavailable
to supersession, another mandatory transaction, or ordinary work. For
`I=1,Qe=2`, the new legal minimum is `2×2+1=5` entries, not 3. With an anchored
marker, a proposed Qe-sized supersession is refused *before supersession* if its
complete successor fixed point would invade K; it may never commit and discover
after death that recovery cannot fit. The byte dimension is identical with Qb/Bm.

“Repayment edge” promises an ordinary-admission-independent TOLD path, not that an
external participant or observer will cooperate; a deliberate wedge remains the
named §7 policy. The fixed header stores the lexicographically first valid witness
when several exist. It names exact binding/sequence boundaries, is recomputed or
replaced in the same transaction as debt or witness invalidation, and is cleared
when both components reach zero. A mandatory candidate that cannot lower debt or
preserve a row uses its already-owned pending cell until its TOLD event; an optional
producer is refused before mutation. Marker append, append-free ack, observer
progress, cursor progress, and floor compaction may run while debt exists. No
repayment path polls, scans, or requires an ordinary record to make room.

The physical floor remains reproducible. Let `H'` be the candidate watermark;
`F` the current first retained sequence (`H+1` when empty); `m` the minimum member
cursor after membership changes, or `H'` with no members; `o` the hard
`observer_progress`; and `a` the earliest live-member unaccepted marker sequence,
or checked one-past-end `END=MAX+1`. `END` is never allocatable. Define
`preferred_floor=min(m,o)+1`. For candidate floor f, let `B_f` include every
removal and fixed-point marker charge and let `K_f` be exact post-transfer
K_remaining. Define the committing-class predicate
`Envelope(tx,f) = (B_f+Q+K<=cap and debt=0)` for ordinary transactions. For
mandatory transactions let `d_f=max(0,B_f+Q+K_f-cap)` and require
`B_f+K_f<=cap`, `d_f<=Q`, **and** `(d_f!=0 or B_f+Q+K<=cap)`. The final clause is
debt-zero release legality: an edge may clear only where restoring all K also
fits componentwise. In both classes `Envelope(tx,f)` also includes the complete
serialized cumulative-exit predicate above. It may invalidate a numerically
fitting positive-debt floor, but never a debt-zero floor that already proves
full-K legality. `Envelope(tx,f)` also includes the committing class's exact
effect: completion of `PhysicalCompaction { from_floor, through_seq }` requires
`f>=through_seq+1`, whereas an append-free ack contributes no artificial floor.
Let `base_floor=max(F,preferred_floor)`. Because transferring only part of K can
make the mandatory predicate non-monotone (positive debt can be followed by an
illegal raw-debt-zero/full-K gap), the search starts at that actual lower bound:
`cap_floor=min { f>=base_floor | Envelope(tx,f) }`, never the first envelope at
some lower floor and never an envelope from another transaction class. The
published equation remains `F'=max(F,preferred_floor,cap_floor)`; by construction
`F'=cap_floor`, and commit re-evaluates `Envelope(tx,F')` componentwise before
mutation. Thus no preferred-floor jump can land inside the illegal K-release
gap. The result is valid only when `F'<=a`.
Successful commit appends at `H'`, removes exactly `[F,F')`, and stores floor/debt
atomically. Marker acceptance, fenced provisional recovery, or Leave releases its
owner's anchor; nothing else does. Observer hard retention forbids
`cap_floor>o+1` and produces `ObserverBackpressure` before mutation. Member claims
remain soft. No member is ever overtaken by `preferred_floor`, because
`min(m,o)+1 <= m+1`; every `HistoryCompacted` marker is caused by `cap_floor`
pressure or by enrollment whose cursor 0 was already below F. Persisted progress
emits the wake; no poll does.

Before any floor/membership trigger changes state, its finite fixed-point preflight
starts with existing marker owners plus every member the proposed floor overtakes,
simulates candidates in `admission_order`, pins all simulated/unaccepted markers,
and adds each newly overtaken uncovered live identity once. At most `I` strict set
growth steps reach a fixed point. For ordinary work, the postcondition is the
full zero-debt envelope. For every mandatory class, the postcondition is the
absolute-fit plus bounded-`d'` equations above—candidate counted once regardless
of prior debt. For a marker append, one planned slot becomes actual and the debt
cannot grow.

The closure-specific suffix after the triggering operation's exact R-D1 common
request envelope is exact. Every scope serializes these fields from the unchanged
durable pre-request snapshot: `scope`; `marker_capacity_credits=C`;
`marker_anchors`, the number of distinct
delivered-but-unaccepted or undelivered marker anchors; current `entry_debt` and
`byte_debt`; current `repayment_edge`; `edge_sequence_claims` and
`edge_order_position_claims`, the exact counts of still-owned sequence and order
positions in that edge's serialized claim set; `edge_K_remaining`, the current
entry/byte `K_remaining` vector or `(0,0)` when `edge=None`; and
`K_headroom=configured_cap-B`, componentwise in u128. Current absolute fit proves
that subtraction defined. It then serializes current `episode_churn_used`,
`delta_cycles`, and `episode_churn_limit=J`, in that order. `delta_cycles` is zero for the
earlier `RecoveryFence` and `DeliveredMarkerAwaitingAck` scopes, because neither
admits a replacement plan, and is the exact fixed-point cycle charge for
`EpisodeChurnLimit` and `Capacity`. Thus no common field mixes current and
proposed state.

`scope:Capacity` alone adds `dimension:Entries|Bytes`, u128 scalar `required`, and
u128 scalar `limit`. For each reachable node of the completely simulated successor
tree, define its required-capacity vector as `B_v+K_remaining_v` while it stores
an edge, or `B_v+Q+K` when it stores no edge. An ordinary caller trial always
uses its proposed `B_v+Q+K`, because ordinary work may not borrow closure
reserve. The preflight takes the componentwise maximum across the immediate
proposed state and every reachable successor node; this is necessary because
any one of those orderings may occur, and sufficient together with the already
reserved claims because mutually exclusive nodes are not summed. The absolute
fit also proves `d_v=max(0,B_v+Q+K_remaining_v-cap)<=Q`, so there is no hidden
third capacity predicate. Compare Entries first, then Bytes: the first component
whose maximum is greater than the configured component selects the dimension,
puts that maximum in `required`, and puts that configured cap in `limit`.
Equality passes, and Entries wins when both components fail. If neither fails,
Capacity is not selectable.

Failure returns the common envelope plus that complete
`MarkerClosureCapacityExceeded` suffix before
floor, membership, marker credit/anchor, candidate, sequence, debt/edge, or
participant mutation. The other three scopes add no unnamed or Capacity-only
fields.

After preflight and R-C2 sequence checks, one transaction changes floor/membership
and creates one candidate per newly affected member. Simultaneous candidates use checked
`admission_order=(u64 transaction_order, candidate_phase,
ascending_participant_index)`. At append, a candidate recomputes
`abandoned_after`, `abandoned_through`, and `physical_floor_at_decision`, then
appends `HistoryCompacted` and removes itself. The candidate drain precedes caller
records only when that candidate owns an earlier admission tuple. Within one
causal transaction, its phase-3 `OrdinaryRecord` appends before every phase-4
`CompactionMarker` it induces; neither rule permits tuple order to reverse.

**Finite termination under consumable reserve.** The trigger fixed point contains
every marker its own appends induce and absolute-fit reserves all of them. Each
marker append retains one capacity credit and its `marker_max` charge while
materializing one bounded actual record, does not consume Q twice, cannot evict an unaccepted marker, creates no
replacement, and strictly changes `M→M-1` even at full ClosureDebt. With no caller
admission interleaving, `M<=I` reaches zero in at most `I` appends. Acceptance
retains that same credit; only physical compaction releases it under the proven
nonincrease above. Other pending mandatory candidates wait in bounded cells until
a named repayment edge fires; they cannot steal a marker credit. If the equations
cannot prove this before a trigger, refusal occurs before floor change. Earlier/
later finalizations remain ordered by `admission_order`, not wake scheduling.

`abandoned_after` is the held cursor and `abandoned_through` is the pre-marker
watermark; the next sequence is the marker itself. These names deliberately
distinguish physical compaction from client-authorized abandonment. With cursor
`10`, physical floor `20`, and watermark `29`, marker `30` offers abandonment of
`11..29`: records `11..19` are physically gone, while retained entitled records
`20..29` are explicitly included in the offer. If the client accepts marker `30`,
none of `20..29` is delivered and the cursor becomes `30` by authorized
abandonment. If it declines, the cursor remains `10`, no `20..29` payload is
released out of order, and the stall remains visible. This is not a claim that
`20..29` was compacted or seen.

The marker is offered at least once under its canonical key. Delivery loss,
ack-confirmation loss, or reattach redelivers the same retained marker. The floor
cannot remove it before acceptance. Caller commits remain behind the shared
ordered candidate drain; subsequent admissions preserve its anchor and closure
headroom, and live delivery after an appended marker remains queued until marker
acceptance. Once accepted, its cursor transition and anchor release are atomic;
only a later floor transition may then create a new marker for that participant.
No timer retries either path.

Worked floor cases (assume the byte cap is not tighter than the stated entry
cap):

1. **Multiple claims.** At `H=100`, `F=1`, member cursors `{10, 40}`, and
   `o=100`, `preferred_floor=11,base_floor=11`. If the caps require
   `cap_floor=25`, then `F'=max(1,11,25)=25`; one commit removes `1..24`,
   overtakes only cursor `10`, and leaves cursor `40` continuous.
2. **Below-cap Leave.** From `F=11`, cursors `{10, 40}`, and `o=100`, Leave by
   cursor `40` appends `Left` and computes
   `preferred_floor=base_floor=cap_floor=F'=11`; Leave by cursor `10` instead
   leaves cursor `40` as minimum and computes
   `preferred_floor=base_floor=cap_floor=F'=41`.
3. **At-cap final Leave.** At `H=100` with one cursor-`0` member and `o=100`, the
   Leave commit appends `Left` at `101`, removes the released member claim, deletes
   the old retained prefix through `100`, and, with no members `m=H'=101`, uses
   `preferred_floor=base_floor=cap_floor=F'=101`, retaining `Left` for the
   observer. When the observer marks `101`, the empty-log transition uses
   `preferred_floor=base_floor=cap_floor=F'=102`.
4. **Cursor-0 late member at cap.** Mint never lowers `F`. If mint appends
   `Attached` at `101` while `F=25`, its initial computation has
   `preferred_floor=1,base_floor=cap_floor=F'=25`; the new cursor `0` is
   immediately overtaken;
   its first replay outcome is a marker offering abandonment `1..101`. Cap
   eviction may advance `F` further but may not cross `observer_progress + 1`.
   If the unprojected `Attached` blocks marker admission, the marker receives
   typed `ObserverBackpressure` until observer progress emits the retry wake;
   enrollment is never silently rolled back or polled.

**R-C5 — Replay/live cutover and multi-conversation mux.** At attach, the server
establishes a sequencer watermark and subscribes the binding to later committed
records without a race. Normally it emits retained `(cursor, watermark]` in
order, then hands off once to live records `> watermark`. If `cursor + 1` is
below the physical floor—including cursor `0` for a late member—R-C4's
abandonment marker is (re)delivered instead. Its explicit ack abandons through
the marker's pre-commit watermark; only records sequenced after the marker then
flow. Live commits arriving during ordinary replay or marker acceptance are
queued under the same admission bounds and wake the
connection by an event, never a scan. The current READY marker runs one full
slice and the quiet connection parks after a final probe
(`crates/liminal-server/src/server/connection/process.rs:648-658`,
`crates/liminal-server/src/server/connection/process.rs:193-212`). The exact
linearization mechanism remains `«REPLAY-LIVE-CUTOVER»`.

Every retained/live item before encoding is `ParticipantDeliveryWork {
conversation_id, intended_participant_id, intended_binding_epoch, delivery_seq,
record }`. The sole R-D2 participant-frame constructor revalidates immediately
before construction that the connection/conversation slot still contains that
participant at that exact epoch. Mismatch drops the stale work without cursor,
ack, or record mutation; it may never be retargeted to the slot's new occupant.

Detach, Leave, and same-connection rebind install an epoch fence and outbound
response barrier in the serialized handoff. All old-epoch work not yet constructed
fails the final check; bytes already constructed for that epoch are ahead of the
response in that connection's one writer. The operation response follows those
bytes. On successful rebind it names Q's new epoch, and Q's first delivery is
constructed after that response under the same epoch.

A cross-connection handoff claims no impossible byte ordering between two TCP
streams. The commit fences the old final constructor; after generation-ordered
persistence the SDK atomically retires only the old
`DispatchSlot=(connection_incarnation, conversation_id, participant_id,
binding_epoch)` before exposing `AttachBound`. Later origin-stream bytes for that
slot are dropped as old-epoch input, never Q delivery. The physical C1 reader
continues serving every other conversation/dispatch slot; only actual connection
fate drives whole-reader shutdown through the explicit wake. Thus no P work can
be constructed after its slot barrier or retargeted merely because a physical
slot was reused, while unrelated multiplexed delivery is uninterrupted. The
fixed-size dispatch map remains bounded by the negotiated per-connection
conversation cap and adds no unbounded state or polling.

One connection may attach many conversations, but R-C1 permits at most one
participant binding for each `(connection_incarnation, conversation_id)`.
Participant frames are therefore unambiguously demuxed by `conversation_id`; v1
`stream_id` is fixed to zero and carries no identity, ordering, cursor, or
correlation meaning. Per-conversation replay/ack streams may interleave, but each
conversation's order is preserved. A second different participant is refused
before mint/bind under `«MULTI-BINDING-PER-CONVERSATION»`; v1 neither adds recipient
id to deliveries nor silently multiplexes two cursors. Connection teardown
finalizes every active binding through R-A2.

**Response/push ordering (amendment A1).** One participant connection's
server-originated bytes flow through a single FIFO writer, and each
conversation's delivery order is preserved (above) — but the contract promises
**no ordering between a request's `ServerValue` response and unsolicited
`ServerPush` frames**, including pushes of the same conversation: a held
obligation released concurrently with request service may lawfully reach the
wire before the response. A correct client therefore demuxes by frame variant —
`ServerValue` answers the client's single outstanding request; `ServerPush` is
queued/consumed in arrival order — and never assumes the first frame after a
request is that request's response. v1 offers no request-correlation field
(`stream_id` is fixed to zero and carries no correlation meaning, above; R-D3
defers an application-level reply relationship to an explicit future mechanism
rather than overloading `correlation_id`). Reference implementation:
`crates/liminal-sdk/src/remote/participant.rs` `receive()` routes
`ServerPush → Push` and delegates `ServerValue` to the outstanding-operation
slot. Single-outstanding-request discipline is what makes variant demux a
sufficient correlation mechanism in v1; a client that pipelines requests has no
correlation surface and is outside this contract until R-D3's future mechanism
lands.

### Silence-attacking acceptance frame

**Finite-boundary test convention.** “Test-seed” below means a test-only
constructor on the durability backend that installs a version-valid, checksum-
valid complete snapshot before the server starts. It is unavailable to protocol
frames, SDK/public APIs, configuration, and production binaries. The fixture must
state every affected counter, claim, candidate, floor, cursor, debt, and epoch and
must satisfy all invariants on entry; the test then crosses the final boundary
through the ordinary production request/event path. Direct production mutation of
generation, `delivery_seq`, `transaction_order`, or `park_order` is forbidden.
An arm that omits any quantity read by its transition is a **suite defect**, not a
fixture default; H/F/o, L/E/T/M, every cursor/anchor/credit, A/X/order high,
ClosureDebt/edge claims, caps/S occupancy, binding epoch/generation/secret, and
all pending candidates/admission orders must be explicit.
Every acceptance arm uses configured `J=2` unless that arm explicitly varies J;
this sentence supplies a real shared fixture input, not a production default or
an omitted transition value.
Every arm that admits an ordinary payload of exact length p also sets
`R_send>=AR+p` and `WF>=AD+p` and computes its displayed
`ordinary_record_max.bytes>=SR+p`, unless that arm explicitly tests one of those
size boundaries. This is exact shared wire input under R-C4's three overhead
constants, not an assumption that post-commit delivery will fit.

**Legal-prefix receipt hygiene.** In every public-history fixture that repeats
credential attaches—including cases 21, 31, 43, 44, 45, 48, 51, 54, and 56—each
earlier winner's live receipt and provenance row is removed by its already
admitted receipt/provenance deadline events before the next iteration unless the
fixture explicitly retains that row. Detach and token-fate cells are likewise
empty unless named. The final boundary request states its current occupancies.
These one-shot deadline events are TOLD work and use no expiry sweep, polling, or
unbounded quota; thus the enormous legal prefixes do not silently exceed a cap.

**Exact supersession-gate bundle Σ(r,p).** Credential-supersession fixtures below may
cite Σ instead of repeating unrelated quota rows. Let `rΣ` be the codec-exact
byte length of that case's fully stated credential-attach request. Σ means: target
connection/conversation slot empty with negotiated capacity 2 and occupancy 0;
server/participant live-receipt limits 4/4 and exact occupancies r/r;
server/conversation/participant provenance limits 4/4/4 and exact occupancies p/p/p;
receipt/provenance TTLs 1000/2000ms; no parked row or interest; and parking
`N=G=4,P=1,R=max(rΣ,PR,40),B=R+MR,C=D=4B,RE=16,SE=26,EE=27,RF=16,RC(P)=8,
WF=max(PF,R,51)`, so `RH(P)=40<=R<=WF` and `SH(P)=51<=WF`, with every checked
product exact; each citing case gives exact
`0<=r,p<4`. The request's target slot,
token/verifier, source authority, and resulting epoch remain case-specific and
must still be stated. Identity capacity is not reread because supersession uses
its already reserved permanent slot; sequence, order, and retention gates remain
case-specific. These are all non-retention quota inputs read by this producer.

1. Deliver N, let the SDK confirm its contiguous watermark, duplicate N on the
   same session, and prove the guard suppresses the duplicate it can prove.
2. Apply N in the application, crash before ack persistence, reattach, and prove
   N is redelivered **with the identical `(conversation_id, delivery_seq)`**.
   The test expects this residual; it does not hide it behind “exactly once.”
3. Commit/abort every admission failure point and prove successful records are
   gap-free while failed admissions consume no sequence.
4. Attach three participants, commit payload and lifecycle records, and prove
   all three observe the same sequence. No recipient-filter branch exists in v1.
5. Race a live commit against replay's final retained record; prove no loss,
   duplicate marking, or reordering across the handoff.
6. Put cursor `10` below physical floor `20` at watermark `29`; marker `30`
   must name `abandoned_after=10`, `abandoned_through=29`, and floor `20`.
   Prove `20..29` are still physically retained before the choice. Accept marker
   `30`: prove none of `20..29` is delivered, the client explicitly records their
   abandonment, cursor becomes `30`, and later live records flow. Decline it:
   prove cursor remains `10` and `20..29` is not released out of order. Lose
   marker/ack confirmation and prove same-key redelivery with one cursor advance;
   ack `30` without marker delivery is refused. Repeat across compaction and live
   commit races.
7. Attach two conversations to one connection, interleave their records, and
   prove demux by `conversation_id` with independent cumulative cursors.
8. Hold the system silent; replay advances only on attach, storage completion,
   ack, committed-record, observer-progress, and admitted deadline events—never
   because a timer or sweep checks for work.
9. Commit enrollment and lose its response; retry on the origin connection and
   prove the same generation/credential payload returns with exactly one
   `Attached` record and no ghost. On distinct connections with empty conversation
   binding slots, distinct enrollment tokens create distinct participants; the
   occupied-slot case is acceptance 42. Replay after receipt expiry and prove typed `ReceiptExpired`
   cannot mint another participant.
10. Mint a member after N records and prove replay from `1`, or marker-first R-C4
    abandonment if the floor passed it. Detach at a cursor, commit offline
    records, reattach, and prove ordered replay. Leave and prove the formula's
    floor transition, retired id, and no-record refusal.
11. Race two holders of generation G with distinct write-ahead tokens: exactly
    one produces G+1, the loser is stale, and the log has at most one handoff
    pair. Lose the winner's response and replay its live receipt; prove one
    rotation. Try stale G with a new token; prove zero records, broadcasts,
    cursor changes, or retention claims.
12. Commit enrollment, then Leave before any secret-proving reattach; delayed
    exact committed enrollment-token replay returns `Retired` with the same
    participant id and retired generation, but no secret, binding, new identity,
    or record. Separately Leave with a live credential-attach receipt; replay the
    the exact old secret, same token, and old generation and prove `Retired` carries
    that same participant id/retired generation with no secret, binding, or record.
13. While P remains bound on C1, send its exact current-generation/current-secret
    `LeaveRequest` on C2; connection context is not a wire field and the wrong
    binding returns `NoBinding`. Send a lower generation, then equal generation
    with a wrong secret; both return exact `StaleAuthority` (equal presented/current
    generations identifies the latter). Omitting `attach_secret` is not a semantic
    shared-bearer arm: it fails R-D2 canonical frame decode as
    `ParticipantTransportRejected { reason: DecodeFailed {
    decode_class: MissingRequiredField } }`. Each refusal has no
    order/record/cursor side effect. Lose a valid
    Leave response and retry the same write-ahead token; prove one stable
    `LeaveCommitted` and one `Left`. Race valid attach and Leave at the serialized
    state: attach-first makes Leave `StaleAuthority`; Leave-first makes attach and
    all receipt replays `Retired`.
14. Commit attach on C1 and kill C1 before its response. Retry the token on C2:
    assert typed `UnboundReceipt`, persist its higher generation/secret, and
    assert C2 has no replay/ack authority. Send a fresh-token attach from C2,
    obtain `AttachBound`, then prove C2 replays and acks. No response in this sequence
    may ambiguously claim C2 was bound before the final commit.
15. Delay generation G's response until after G+1 is durably installed; prove the
    SDK rejects G and preserves G+1. Crash before and after atomic generation/
    secret persistence and recover by token replay. Exercise the negotiated
    receipt deadline immediately before and after expiry: before returns the live
    typed receipt; after returns `ReceiptExpired`, the secret body is gone, and
    no periodic expiry scan exists. Exceed the signed receipt-count cap and prove
    pre-commit refusal with no rotation.
16. Execute R-C4's four worked floor cases with byte and entry accounting:
    multiple claims `{10,40}`, below-cap Leave of each claimant, at-cap final
    cursor-0 Leave, and cursor-0 late mint at cap. Assert the exact removed prefix,
    monotonic `F'`, which member is overtaken, observer hard-stop refusal, marker
    fields, and the final empty-log floor after observer progress.
17. On C17 for participant P17 at generation G, let exact attach token T with
    `accept_marker_delivery_seq=None` commit G+1 while fresh token U loses.
    Let T's receipt deadline remove its secret body with no G+2 attach. Inside the
    provenance window, T replay must be exact
    `ReceiptExpired { conversation_id:C17,token:T,participant_id:P17,
    presented_generation:Some(G),presented_marker_delivery_seq:None,
    result_generation:G+1,
    current_generation:G+1,reason:Deadline }` while U is `StaleAuthority`. After T's
    fingerprint deadline, both return `StaleOrUnknownReceipt`; neither claims a
    commit, leaks a secret, binds, or writes a record.
18. Churn enroll/Leave until each conversation/server retirement-slot boundary.
    In one conversation the allocator emits exact ordinals `0..I-1`, never reuses
    a retired ordinal, and refuses the next enrollment; a different conversation
    may independently emit numeric id0 because the conversation key differs.
    Every admitted enrollment has a reserved slot; lost Leave response still
    returns stable `LeaveCommitted`, retired enrollment replay cannot ghost-remint,
    and the next enrollment returns `IdentityCapacityExceeded` without minting.
    With limit/occupied/requested `1/1/1` at every reachable failing scope and
    earlier scopes set to `2/1`, first use new enrollment to prove identity Server
    then identity Conversation precede simultaneous LiveReceiptServer/
    ProvenanceServer/ProvenanceConversation failures. Then use an existing live
    participant's credential attach to exercise its five receipt scopes in order:
    LiveReceiptServer, LiveReceiptParticipant, ProvenanceServer,
    ProvenanceConversation, ProvenanceParticipant. New-enrollment per-participant
    occupancies are zero and cannot fail their nonzero limits. Each arm returns
    only its named `IdentityCapacityExceeded` or
    `ReceiptCapacityExceeded` scope and creates no identity, receipt, mapping,
    record, or later-scope disclosure.
19. Force `ObserverBackpressure` separately on enrollment attach, credential
    attach, explicit detach, ordinary record admission, and valid Leave. A known
    refusal parks for one `ObserverProgressed` cycle; normal and marker acks still
    commit/no-op. Lose an ordinary admission response and prove terminal
    `RecordAdmissionUnknown` with no resend. A poison callback reaches the socket.
20. Commit G+1, lose its response, crash the client past `receipt_expires_at`, GC
    the secret, then replay T. Assert non-secret `ReceiptExpired` (or
    `StaleOrUnknownReceipt` after provenance), terminal `CredentialRecoveryLost`
    preserving conversation/participant/G, and no automatic retry.
21. Let `G*=(MAX-3)/2`. Construct—not test-seed—the complete joint
    generation/sequence boundary by enrollment followed by exactly `G*-1`
    successful supersessions of P0. Continuously project, offer, cumulatively ack,
    and compact each pair before the next, so no marker or retained row survives.
    This legal prefix appends exactly `1+2(G*-1)=MAX-4` lifecycle records and
    allocates caller majors `0..G*-1`; it cannot be replaced by independent
    counter injection. `K*` is explicit fixture secret bytes:

    | Quantity | Exact pre-attach value |
    |---|---|
    | H/F/o; sequence budget | `H=MAX-4,F=MAX-3` empty, `o=H`; canonical budget `{ high_watermark:MAX-4,remaining:4,E:1,T:1,M:0,RS:0,RT:0,L_times_T:1,L_times_RT:0,L_other_times_E:0 }` |
    | Cursor/marker/candidates | P0 cursor H; no anchor, credit, candidate, or admission order |
    | Order/debt | `A=X=1,RO=RA=0`, order high `G*-1`; A/X own `G*`/`G*+1`, with ample unreserved order; debt zero, edge `None`, zero edge claims/milestones, K free |
    | Capacity | `I=1,Q=K=(2,2Bm),cap=(16,16Bm)`; `S_actual=S=0,C=0,B=(1,Bm)`, so `B+Q+K=(5,5Bm)`; `preferred_floor=base_floor=cap_floor=F` |
    | Identity/authority | P0 index/id 0, bound at exact epoch e* and generation G*/secret K*; detach cell Empty, no tombstone or live receipt/provenance; same-participant target slot occupancy 1 under connection cap 2 |
    | Request/quota | exact first-use `CredentialAttachRequest { conversation_id:C21,participant_id:P0,capability_generation:G*,attach_secret:K*,attach_attempt_token:U21,accept_marker_delivery_seq:None }`; U21 cell Empty; live-receipt limits server/participant 4/4 and occupancies 0/0; provenance limits server/conversation/participant 4/4/4 and occupancies 0/0/0; TTLs 1000/2000ms |

    Supersession would append terminal/Attached at `MAX-3/MAX-2` and project the
    exact resulting budget `{ high_watermark:MAX-2,remaining:2,E:1,T:1,M:0,
    RS:0,RT:0,L_times_T:1,L_times_RT:0,L_other_times_E:0 }`, whose required 3
    exceeds remaining 2. Order and every earlier gate pass.
    `ConversationSequenceExhausted` returns that exact ten-field nested payload,
    is terminal for U21, and leaves every cell/counter byte-identical. This is the
    reachable generation boundary; no `GenerationExhausted` arm exists.
    Exercise live-receipt and fingerprint-cap `ReceiptCapacityExceeded`, every
    reachable discriminant and every R-D1 row in both directions: each operation
    has only listed outcomes and every listed outcome has a trigger, exact fields,
    retryability, and SDK transition.
22. Keep enrolled P live past receipt and ordinary provenance GC. Replay its
    original enrollment token T: assert `EnrollmentKnown` identifies P/current
    generation without secret/remint. From identical unbound connection state,
    fresh token U enrolls a distinct participant until the identity cap.
23. Backpressure bound and unbound enrollment/replacement connections; prove each
    refusal recipient gets connection-level `ObserverProgressed`. Reconnect and
    obtain non-mutating status by conversation+epoch. Force two refusal/progress
    cycles. Reattach while Leave is parked: the old row terminalizes on its typed
    authority outcome and is not silently reissued under new authority. Prove no
    ack row exists and no polling occurs.
24. For every R-A2 fate while blocked, crash after durable `PendingFinalization`
    and before progress, then inject crashes at every terminal-drain write
    boundary. Recovery preserves original cause/binding-epoch/order and observes
    either the whole atomic append/retention/delete/release transaction or none;
    exactly one ordered terminal record and one released bounded slot result.
25. Exercise the complete initial-configuration matrix before any conversation
    state exists. Let `W0=max(PF,PR)`; every unmentioned schema-size field below
    is valid with normalized `WF=W0`, `R>=PR`, and `RE=16`:

    | Arm | Exact inputs and failed predicate | Required outcome fields |
    |---|---|---|
    | Nonzero limits | independently set each of `N,C,P,G,D,R,B,RE,WF` to 0 while every other field is valid; also set all nine to zero | `ParticipantParkingConfigurationInvalid { dimension:NonzeroLimit, operands:{ field,actual:0,required_minimum:1 } }`; the independent arm names that field and the simultaneous arm names `N` by fixed order |
    | Recovery entry schema | independently `RE=15` and `RE=17`; equality `RE=16` proceeds | `ParticipantParkingConfigurationInvalid { dimension:RecoveryEntrySchemaBytes,operands:{ actual:15,required:16 } }` and the same exact schema with `actual:17`; neither side is silently clamped |
    | Wire schema | raw `configured_WF=PF-1` | `ParticipantParkingConfigurationInvalid { dimension:WireSchemaBytes,operands:{ actual:PF-1,required:PF } }` |
    | Request schema | `R=PR-1,WF=W0`, hence `R_send=PR-1` | `ParticipantParkingConfigurationInvalid { dimension:RequestSchemaBytes,operands:{ configured_request_limit:PR-1,wire_frame_limit:W0,actual:PR-1,required:PR } }` |
    | Row schema | `R=PR,B=PR+MR-1` | `ParticipantParkingConfigurationInvalid { dimension:RowSchemaBytes,operands:{ request_limit:PR,row_metadata_bytes:MR,actual:PR+MR-1,required:u128(PR)+u128(MR) } }` |
    | First product | `N=u64::MAX,G=1,B=PR+MR,R=PR`; first validated product `N×B` has exact factors `u64::MAX` and `PR+MR` | `dimension:CheckedProduct`, `operands:{ operation:Multiply,left:u64::MAX,right:PR+MR,checked_result:None,overflow:true }` |
    | Second product | `N=1,C=PR+MR,G=u64::MAX,B=PR+MR,R=PR`; `N×B=PR+MR` passes and the next `G×B` overflows | `dimension:CheckedProduct`, `operands:{ operation:Multiply,left:u64::MAX,right:PR+MR,checked_result:None,overflow:true }` |
    | Per-conversation bytes | `N=2,R=PR,B=PR+MR,C=2(PR+MR)+1` | `dimension:RowBytesBound`, `operands:{ left:2,right:PR+MR,checked_product:2(PR+MR),actual:2(PR+MR)+1 }` |
    | SDK bytes | `G=3,R=PR,B=PR+MR,D=3(PR+MR)+1` | `dimension:SdkBytesBound`, `operands:{ left:3,right:PR+MR,checked_product:3(PR+MR),actual:3(PR+MR)+1 }` |
    | Recovery slots | `P=6,connection_slots=5` | `dimension:RecoverableSlots`, `operands:{ actual:6,limit:5 }` |

    Independently prove the schema boundaries: normalized WF equal to PF and
    PF+1 both pass WireSchemaBytes; `R_send=PR` and PR+1 pass
    RequestSchemaBytes; and `B=R+MR` and `R+MR+1` pass RowSchemaBytes. RE alone
    is exact-equality schema data, so 15/17 fail and only 16 passes.

    In every arm phase is initial, parked rows/bytes/conversations are zero, and no
    row, interest, handshake, or participant state exists. `NonzeroLimit` precedes all shape dimensions; shape failure precedes
    RH/SH checks. Let `p=max(PF,PR)+1`, `A=24+16p`, and `Z=24+26p`, and set
    exact `connection_slots=p`. Since `PF>=PR` and `PF<=1_048_576`,
    `p<=1_048_577`, `A<=16_777_256`, and `Z<=27_263_026<FRAME_MAX`; both also
    fit u64.
    With shape valid use `P=p,RF=16,RC(P)=8,RE=16,SE=26,EE=27`, hence
    `RH(P)=A` and `SH(P)=Z`, and start with `R=A,WF=Z`.
    Set `(R,WF)` respectively to `(A-1,Z)`, `(A,A-1)`, `(A,Z-1)`, and `(A-1,A-1)`:
    the outcomes are RequestBytes, RequestWireFrameBytes,
    ResponseWireFrameBytes, and RequestBytes under
    `ParticipantRecoveryHandshakeTooLarge`, with exact request/response u128
    operands and `max_entries:p,framing_bytes:u128(24),request_entry_bytes:16,
    response_entry_bytes:26,error_response_bytes:27,
    request_encoded_bytes:A,response_encoded_bytes:Z`.
    In parked phase repeat `NonzeroLimit` for all nine fields plus the
    simultaneous-zero ordering arm, both product triggers, and the other seven
    remaining shape dimensions, and assert the same operands under
    the corresponding `SdkParkingCapacityIncompatible` dimensions. Repeat the
    four RH/SH proposals as RecoveryHandshakeRequestBytes,
    RecoveryHandshakeRequestWireFrameBytes,
    RecoveryHandshakeResponseWireFrameBytes, and
    RecoveryHandshakeRequestBytes, preserving all rows and sending no row,
    partial request, or partial response.

    Independently set each of R-C0's nine capability fields to zero in its fixed
    order while the other eight are valid; an all-zero proposal selects
    `attach_receipt_ttl_ms`. With `attach_receipt_ttl_ms=1000`, provenance TTL
    1000 passes the deadline-order check, while 999 returns exact
    `ParticipantCapabilityConfigurationInvalid { dimension:ReceiptDeadlineOrder,
    attach_receipt_ttl_ms:1000,receipt_provenance_ttl_ms:999,
    required_minimum_provenance_ttl_ms:1000 }`.

    Finally exercise the global selector with all five families invalid, then
    make exactly one earlier family valid at a time. The named outcomes must occur
    in order: parking configuration, recovery-handshake size, capability
    configuration, startup keepalive certification, retention capacity. For
    keepalive, make `idle_seconds=0` also out of range and off-granularity and make
    later fields invalid: `Zero/idle_seconds` wins; with idle valid and interval
    simultaneously out of range/off-granularity, `OutOfRange/interval_seconds`
    wins. Every global arm refuses before listener, bytes, row, identity, or
    conversation state.

    Separately use the generated `I=1,Q=K=(2,Qb),marker=(1,Bm)` and exact cap
    `(5,Bm+2Qb)`; reject either component one below it, then start an empty
    conversation at equality. Its complete prestate is `H=0,F=1,o=0`, no member,
    cursor, candidate, edge, occurrence array, or order/sequence claim,
    `S=(0,0),C=0,B=(1,Bm)`, K free, and `B+Q+K=cap`. Enroll P0 at major0/sequence1
    with cursor0 and epoch e25. The generated fixed-row profile gives that
    Attached record exact charge `q_enroll=(1,Bm)`. The atomic poststate is
    `H=1,F=1,o=0`, exact `[T,E,L×T]` claims at sequences2/3/4, A/X at majors1/2,
    `S'=q_enroll,C'=0,B'=(2,2Bm)`,
    `K_remaining=K`, `d'=q_enroll`, and `B'+K<=cap`; the first lexicographic
    witness is exact `ObserverProjection { through_seq:1 }` and it owns no
    RS/RT/RO/RA or marker claim.

    Here `O_max=811,O_base=343`; record-index0 owns ordinals1..27, P0's base
    block is136..342, and churn blocks343..576/577..810 are Dormant. Producer
    selection0 is Consumed. ProjectionCompleted event1 has exactly its None and
    ParticipantCursorProgress selections2/6 Pending; CursorProgressed event19
    has exactly its None and ObserverProjection selections20/21 Pending; the
    other selections in those groups are Consumed. Compaction group10..18 and
    unused record-index groups28..135 are all Consumed because this fixture's
    removal is atomic with the cursor/OP completion and stores no independent
    physical-compaction edge. P0's base block marks every
    exact table-reachable fate/Leave preserve-or-strict-suffix selection Pending
    and every impossible marker/recovery selection Consumed.

    Complete both legal orders. Projection-first consumes event1/selection6,
    sets `o=1`, and stores `ParticipantCursorProgress { participant_id:P0,
    binding_epoch:e25,through_seq:1,marker_delivery_seq:None }`; the exact ack
    then consumes event19/selection20, sets cursor1, and computes
    `preferred_floor=base_floor=cap_floor=F'=2`. Ack-first consumes
    event19/selection21, sets cursor1, and preserves the OP; its later completion
    consumes event1/selection2 and computes the same floor. Either order removes
    Attached1 and atomically reaches `S=(0,0),C=0,B=(1,Bm),debt=(0,0),edge=None,
    K_remaining=(0,0)`, with full-K equality `B+Q+K=cap`. Crash each side of all
    three commits. Then exercise every detach outcome, prove acks never
    backpressure, and re-run case 21.
26. Construct this complete entry sequence-equality snapshot with `h=MAX-17`.
    Enroll P0/P1/P2, perform respectively 6/4/3 supersessions to generations
    7/5/4, admit `h-30` ordinary records while all three bindings continuously
    project/ack/compact, then take exact socket EOF on P2. The 3 enrollments, 26
    supersession records, ordinary prefix, and P2 terminal total exactly h. The
    3 enrollment callers, 13 supersession callers, ordinary callers, and P2's
    consumed A major leave order high `h-14`; no counter is injected.

    | Quantity | Exact value |
    |---|---|
    | H/F/o and capacity | `H=h,F=h,o=h`; retain only P2's terminal h; `I=3,Q=K=(2,2Bm),cap=(64,64Bm)`, `S=(1,Bm),C=0,B=(4,4Bm)`, debt zero/None, K free |
    | Members/authority | `L=E=3,T=2,M=RS=RT=0`; P0/P1 active at epochs `(7,3;gen7)`/K0 and `(7,4;gen5)`/K1 with cursors h; P2 detached gen4/K2 with cursor h-1 after exact `Died(ConnectionLost)` terminal h from epoch `(7,2;gen4)`; all cells Empty, no anchor/credit/candidate |
    | Budget | `{high:h,remaining:17,E:3,T:2,M:0,RS:0,RT:0,L×T:6,L×RT:0,L_other×E:6}`; `17=3+2+0+0+0+6+0+6` |
    | Sequence positions | T(P0/P1) own h+1/h+2; E(P0/P1/P2) own h+3..h+5; `L×T` owns h+6..h+11 in terminal-owner/affected-index order; `L_other×E` owns h+12..MAX in exit-owner/remaining-index order |
    | Order | high `h-14`; A owns `h-13/h-12`, X owns `h-11/h-10/h-9` in participant-index order; `RO=RA=0`; no successor occurrences |
    | Leave inputs | exact first-use L26-0/1/2 frames carry C26, the named participant, current generation/secret, and distinct tokens; each permanent verifier exists and each Leave-token cell is Empty; receipt/provenance/detach cells are Empty |

    At these ample caps neither terminal nor Leave requires cap pressure. Because
    `preferred_floor=min(m,o)+1=h`, no member is overtaken; therefore every one
    of the six terminal-product and six exit-product claims releases at its causal
    event and no marker materializes. Append P0/P1 terminals at h+1/h+2, then
    detached Leaves at h+3..h+5, pinning `o=h` before each commit. EOF sources are
    the displayed exact epochs and record `event_kind:Died,
    original_cause:ConnectionLost`; every Leave uses its exact L26 frame.
    Through the first four commits `m=h-1`, so
    `preferred_floor=base_floor=cap_floor=F'=h`. Final P2 Leave removes the last
    member: `m=H'=h+5`, `preferred_floor=base_floor=cap_floor=F'=h+1`, which
    compacts retained terminal h. The exact drain is five records,
    `H'=MAX-12`; the twelve
    remaining values are precisely the released product claims. This tighter
    result proves no promised record was dropped and removes every debt/K transfer
    from this sequence-boundary fixture.
27. Race reconnect handshake against observer progress in both linearization
    orders: status is already progressed or the replacement is armed and receives
    the push, never neither. Backpressure two concurrent local requests at one
    baseline: both receive one epoch, one matching signal authorizes one retry of
    each, and refusals after actual progress share a new epoch. Set the negotiated
    connection-conversation limit to 2 and occupy it with C27a/C27b. In one copy,
    submit exact first-use `EnrollmentRequest { conversation_id:C27c,
    enrollment_token:U27 }` with absent mapping/token cell and every earlier gate
    valid. It returns exact semantic
    `ConnectionConversationCapacityExceeded { conversation_id:C27c,
    enrollment_token:U27,limit:2 }`. In another copy C27c is a known server
    conversation with refusal epoch E; reconnect batch entry C27c@E returns the
    complete connection-scoped
    `ConnectionConversationCapacityExceeded { conversation_id:C27c,limit:2 }`.
    Neither installs an arm, recipient, row, participant, or readiness promise.
28. Detach with token T and lose the response. Before another attach, T replay
    returns the same `DetachCommitted`. Successfully reattach with a fresh attach
    token, terminalize T as `AuthoritySuperseded`, then replay T and prove
    `StaleAuthority`, not a stable result or second record. Repeat cycles and prove
    one detach cell; after Leave, the old token returns `Retired`.
29. Decode post-Leave replay of the exact committed enrollment token and an exact
    committed credential token. Both return the same participant id and retired
    generation in `Retired`, and neither returns a secret, binds, mints, or writes.
30. First create a blocked marker candidate, then accept EOF into pending
    finalization, then wake on observer progress: marker appends first from its
    earlier order and recomputes fields before the terminal record. Repeat with
    EOF's candidate ordered first: terminal appends first and the later marker's
    recomputed `abandoned_through` includes it. In both runs the resulting sequence,
    abandonment interval, and cumulative observer projection match that order.
31. Construct both snapshots from public operations. Enrollment arm: enroll Pold,
    perform `sA=(MAX-5)/2` supersessions, then admit one ordinary record at
    caller major `sA+1`. Bound Leave transfers X onto the absolute next frontier
    major `sA+2` and releases A; project/ack/compact continuously. This yields
    `H=2+2sA+1=MAX-2`, order high `sA+2=(MAX-1)/2`, one permanent tombstone,
    and one free identity slot. Ordinary arm: enroll P0, perform
    `sB=(MAX-7)/2` supersessions, then append two ordinary records; retain only
    those last two. This yields `H=1+2sB+2=MAX-4` and order high
    `sB+2=(MAX-3)/2`. Every earlier gate is pinned below:

    | Quantity | Enrollment arm | Ordinary-admission floor-trigger arm |
    |---|---|---|
    | H/F/o | `H=MAX-2,F=MAX-1` empty, `o=H` | `H=MAX-4,F=H-1=MAX-5,o=H`; two retained nonmarkers at F..H |
    | L/E/T/M | `0/0/0/0` | `1/1/1/0`; P0 is bound |
    | Canonical budget | pre `{high_watermark:MAX-2,remaining:2,E:0,T:0,M:0,RS:0,RT:0,L_times_T:0,L_times_RT:0,L_other_times_E:0}`; rejected enrollment projects `{high_watermark:MAX-1,remaining:1,E:1,T:1,M:1,RS:0,RT:0,L_times_T:1,L_times_RT:0,L_other_times_E:0}` | pre `{high_watermark:MAX-4,remaining:4,E:1,T:1,M:0,RS:0,RT:0,L_times_T:1,L_times_RT:0,L_other_times_E:0}`; rejected admission projects `{high_watermark:MAX-3,remaining:3,E:1,T:1,M:1,RS:0,RT:0,L_times_T:1,L_times_RT:0,L_other_times_E:0}`, requiring 4 |
    | Cursors/anchors/credits | none | P0 cursor `F-1=MAX-6`, so `cursor+1=F` and no marker exists before the trigger; no anchor/credit |
    | A/X/order | `0/0`, order high `(MAX-1)/2`, ample remaining | `A=X=1,RO=RA=0`, order high `(MAX-3)/2`; A/X own `(MAX-1)/2` and `(MAX+1)/2` |
    | Debt/edge | `(0,0)`, `None`, zero claims | `(0,0)`, `None`, zero claims |
    | Caps/S/floor | `I=2,Q=K=(2,2Bm)`, caps `(16,16Bm)`, `S=(0,0),C=0,B=(2,2Bm)`, K free | use the uniform-Bm fixture convention; `I=1,Q=K=(2,2Bm),cap=(7,7Bm)`; pre `S_actual=S=(2,2Bm),C=0,B=(3,3Bm)`, K free, `preferred_floor=base_floor=cap_floor=F`; projected append/removal/plan has `S'=(3,3Bm),C'=1,B'=(3,3Bm)`, `preferred_floor=F`, `base_floor=F`, `cap_floor=F+1`, hence `F'=F+1` if it could commit |
    | Identity/binding | Pold tombstone at index/id0; index1 free; exact first-use `EnrollmentRequest { conversation_id:C31A,enrollment_token:U31A }`, absent lifetime mapping/token cell, empty target slot; identity limits server/conversation 4/2 occupied1/1; receipt limits 4/4 occupied0/0 and provenance 4/4/4 occupied0/0/0, receipt/provenance TTLs 1000/2000ms | P0 index/id0, generation `(MAX-5)/2`/secret K31, exact epoch e31, detach and live token cells Empty; exact `RecordAdmission { conversation_id:C31B,participant_id:P0,capability_generation:(MAX-5)/2,payload:[0x00;3×b_u] }` has payload length `3×b_u`, durable charge `(1,Bm)`, `R_send>=AR+3×b_u`, and `WF>=AD+3×b_u` |
    | Candidates | none | none |

    In the first arm use the exact U31A enrollment. Its `Attached` plus resulting E/T,
    cursor-0 `M`, `L×T`, and `L_other×E` claims exceed the one post-append value, so the
    whole mint refuses `ConversationSequenceExhausted` with all ten canonical
    budget fields. In the second arm send the exact displayed ordinary request. Its append
    would make B=4 at F, so that operation's own zero-debt envelope requires
    `cap_floor=F+1`. Removing the first old nonmarker and planning the newly
    overtaken P0 marker yields the displayed `S'=3,C'=1,B'=3`, but only three
    post-append sequence values remain against E+T+M+L_times_T=4. Return
    `ConversationSequenceExhausted` with the displayed ten fields and preserve
    H/F/o, both old records, cursor, binding, every claim, and the empty candidate/
    credit/anchor state byte-for-byte.
32. Let `r32=PR=97` be the exact Some-marker credential-attach request size and
    use `M_row=MR,R=r32,B=r32+MR,
    WF=max(PF,r32)` with exact full-row charge
    `request_bytes+MR` for named conversation C32a. An encoded request of `r32+1` returns
    `SdkParticipantRequestTooLarge { conversation_id:C32a,
    encoded_bytes:r32+1,limit:r32 }` before every
    capacity/counter check; r32 proceeds with charge `b32=r32+MR`. Exercise these otherwise-
    ample fresh snapshots and exact first failures:

    | Arm | Exact occupancy plus request | Required outcome |
    |---|---|---|
    | Per-conversation rows | `N=2,C=2b32`; C32a has two exact b32 rows and requests another b32 row | `SdkObserverParkCapacityExceeded { scope:PerConversation,dimension:Rows,conversation_id:C32a,limit:2,occupied:2,requested:1 }` |
    | Per-conversation bytes | `N=4,C=3b32-1`; C32a has two exact b32 rows and requests another | `SdkObserverParkCapacityExceeded { scope:PerConversation,dimension:Bytes,conversation_id:C32a,limit:3b32-1,occupied:2b32,requested:b32 }` |
    | SDK conversations | `P=2`; two one-b32-row conversations exist and third conversation C32c requests one b32 row | `SdkObserverParkCapacityExceeded { scope:SdkWide,dimension:Conversations,conversation_id:C32c,limit:2,occupied:2,requested:1 }` |
    | SDK rows | `G=2,D=2b32`; two exact b32 rows exist and C32a requests another | `SdkObserverParkCapacityExceeded { scope:SdkWide,dimension:Rows,conversation_id:C32a,limit:2,occupied:2,requested:1 }` |
    | SDK bytes | `G=4,D=3b32-1`; two exact b32 rows exist and C32a requests another | `SdkObserverParkCapacityExceeded { scope:SdkWide,dimension:Bytes,conversation_id:C32a,limit:3b32-1,occupied:2b32,requested:b32 }` |

    A new-conversation request with every aggregate cap full selects SDK
    `Conversations`; on an existing conversation, simultaneous row/byte/global
    failures select per-conversation `Rows`, then `Bytes`, then SDK `Rows`, then
    SDK `Bytes` as each earlier predicate is made valid. No failure allocates
    `park_order`; with both a full earlier cap and exhausted nonempty-set counter,
    that capacity outcome wins, and once every cap is made valid the same request
    returns `SdkParkOrderExhausted`. Every first-row conversation that does commit already owns its
    armable interest slot.

    Exercise static ordinary-record size independently with live P32 bound on
    C32r at generation7, an exact `RecordAdmission` carrying that tuple, and all
    earlier authority/operation-proof gates valid. With
    `I=1,Q=K=(2,Qb),marker_max=(1,Bm)`, set `R_send>=AR+11` and
    `WF>=AD+11`, set `Ce=6`, and hold the retention byte cap at
    `2Qb+Bm+(SR+10)`, so `ordinary_record_max.bytes=SR+10`. Use canonical
    zero-filled payload arrays of exact lengths 9, 10, and 11; their byte charges
    `(1,SR+9)`, `(1,SR+10)`, `(1,SR+11)` are
    respectively below, equal, and
    `RecordTooLarge { conversation_id:C32r,participant_id:P32,
    capability_generation:7,dimension:Bytes,
    encoded_record_charge:(1,SR+11),max_ordinary_record_charge:(1,SR+10) }`.
    Holding that byte cap fixed, choose entry caps 7, 6, and 5, producing exact
    ordinary entry maxima 2, 1, and 0 against the only legal ordinary entry
    charge `(1,SR+10)`: they are below, equal, and
    `RecordTooLarge { conversation_id:C32r,participant_id:P32,
    capability_generation:7,dimension:Entries,
    encoded_record_charge:(1,SR+10),max_ordinary_record_charge:(0,SR+10) }`. No zero- or two-entry ordinary wire
    record exists, so varying the configured maximum is the exhaustive entry
    boundary proof. With maximum `(0,SR+10)` and charge `(1,SR+11)`, Entries wins the
    simultaneous failure and echoes that exact charge/maximum. Every refusal
    mutates nothing; equality continues to later gates.

    Crash **exactly after `Reserved` commit and before first `InFlight`**:
    restart converts it to `RetryAuthorized`, revalidates authority, and sends it
    once in lowest `park_order`. Also crash before cohort mark, between rows,
    after write-before-response, and after repeated refusal; no path exceeds a
    cap, leaks a slot, or polls.
33. With `P=3`, `RF=16,RC(P)=8,RE=16,SE=26,EE=27`, and distinct conversations whose
    current observer epochs are 5, exercise the whole one-shot list selector.
    Set `R=WF=max(PF,PR,128)`: the largest allowed request and response are respectively
    `RH(3)=16+8+3×16=72` and `SH(3)=16+8+max(3×26,27)=102`, while the deliberately
    over-limit four-entry request is still a structurally valid in-frame
    `16+8+4×16=88` bytes. The exact TooManyEntries error body is 18 bytes and
    every other recovery-batch error body is at most EE=27, so their complete
    responses are at most `16+8+27=51<=WF`; no transport-size predicate masks
    any list arm.
    Four unique entries return
    `InvalidObserverEpochList { reason:TooManyEntries,presented_entries:4,
    max_entries:3 }`. A length-3 list with C33a at indices 0 and 2 returns
    `InvalidObserverEpochList { reason:DuplicateConversation,
    conversation_id:C33a,first_index:0,duplicate_index:2 }`; a length-4 list
    that is also duplicate still returns `InvalidObserverEpochList {
    reason:TooManyEntries,presented_entries:4,max_entries:3 }`. A unique
    request-order list `[C33a@4,C33b@5,C33c@6]` returns
    `InvalidObserverEpoch { reason:EpochAhead,conversation_id:C33c,
    presented_epoch:6,current_observer_progress:Some(5) }` and arms none. With
    the newer entry removed, return in request
    order the one top-level `ObserverRecoveryAccepted { statuses:[
    ObserverProgressStatus { conversation_id:C33a,refused_epoch:4,
    current_observer_progress:5,armed:false,progressed:true },
    ObserverProgressStatus { conversation_id:C33b,refused_epoch:5,
    current_observer_progress:5,armed:true,progressed:false }] }`; the older entry
    does not replace a newer pre-existing arm, and equal subscribes before
    snapshot. An empty list returns `ObserverRecoveryAccepted { statuses:[] }`.
    A unique entry naming absent C33x returns `InvalidObserverEpoch {
    reason:ConversationUnknown,conversation_id:C33x,presented_epoch:5,
    current_observer_progress:None }` and arms none. Keep the required `P=3` and
    connection limit3, preoccupy that connection with two distinct tracked
    conversations C33p/C33q, then present unique known
    `[C33a@5,C33b@6]`: index0 tentatively fills the third slot and index1 is the
    first excess, so exact `ConnectionConversationCapacityExceeded {
    conversation_id:C33b,limit:3 }` wins before C33b's newer-epoch comparison
    and installs neither new slot nor arm. Reversing the entries names C33a as
    the second excess. Both batch-capacity refusals encode assigned wire value
    `0x0124 ObserverRecoveryConnectionCapacityExceeded` with structural
    `status_count:u64=0`, then decode to that exact existing
    `ConnectionConversationCapacityExceeded` batch schema. Prove epochs equal
    refusal baselines and never alias after progress.
34. On C34 for P34 at presented generation7, encode normal and marker ack
    requests with the requested boundary. Before Leave, use public records
    to reach cursor10 and offer contiguously through12 to the exact current epoch:
    requests9/10/12 select `AckRegression { conversation_id:C34,
    participant_id:P34,capability_generation:7,through_seq:9,
    current_cursor:10,reason:BelowCursor }`, `AckNoOp { conversation_id:C34,
    participant_id:P34,capability_generation:7,through_seq:10,
    current_cursor:10 }`, and `AckCommitted { conversation_id:C34,
    participant_id:P34,capability_generation:7,through_seq:12,
    current_cursor:12 }`. In an independent copy offered only through11,
    request12 selects `AckGap { conversation_id:C34,participant_id:P34,
    capability_generation:7,through_seq:12,current_cursor:10,
    reason:NotContiguouslyAvailable }`. Then create marker20 through the
    public R-C4 compaction path. With its anchor current but no delivery fact,
    presented20 selects `MarkerNotDelivered { conversation_id:C34,
    participant_id:P34,capability_generation:7,
    requested_marker_delivery_seq:20,reason:NotDeliveredToProofEpoch,
    expected_marker_delivery_seq:20 }` and presented19 selects
    `MarkerMismatch { conversation_id:C34,participant_id:P34,
    capability_generation:7,requested_marker_delivery_seq:19,
    reason:ExpectedDifferentMarker,expected_marker_delivery_seq:20 }`. After exact
    delivery to the current epoch, presented20 returns `MarkerAckCommitted {
    conversation_id:C34,participant_id:P34,capability_generation:7,
    marker_delivery_seq:20,current_cursor:20 }`; its exact replay at accepted
    cursor20 is `AckNoOp { conversation_id:C34,participant_id:P34,
    capability_generation:7,marker_delivery_seq:20,current_cursor:20 }`.
    Presenting marker value 19 is `MarkerMismatch { conversation_id:C34,participant_id:P34,
    capability_generation:7,requested_marker_delivery_seq:19,
    reason:BelowCursor,current_cursor:20 }`, and in an edge-free copy with no
    anchor presented20 returns `MarkerMismatch { conversation_id:C34,
    participant_id:P34,capability_generation:7,
    requested_marker_delivery_seq:20,reason:NoMarkerExpected }`. Run the same proof-epoch selector
    for credential attach: cases 49/51 supply the exact undelivered/no-marker and
    `RecoveryFence` Options, while a delivered DCR accepts only its exact sequence.
    After Leave, deliver both delayed
    forms. Tombstone lookup on the presented id returns `Retired` with that id,
    presented generation/requested boundary, and retired generation, with no
    secret, binding, or live/current cursor; codec vectors match exactly.
35. For an immediately appendable detach, crash immediately before and after the
    one direct append/floor/binding/cell transaction: observe either the old state
    or `Committed` with its allocated sequence, never an absent/partial producer.
    Replay echoes the committed binding epoch/sequence after binding is gone.
    Reattach, terminalize the replacement, then replay old token:
    `StaleAuthority` reports `binding_state: Detached` with no fabricated current
    binding epoch.
36. Separately from case 17, on C36/P36 commit attach token T with
    `accept_marker_delivery_seq=None` at G+1 and then a
    valid G+1→G+2 attach before T's deadline. T replay inside provenance is
    exactly `ReceiptExpired { conversation_id:C36,token:T,participant_id:P36,
    presented_generation:Some(G),presented_marker_delivery_seq:None,
    result_generation:G+1,
    current_generation:G+2,reason:Superseded }`; case 17 alone proves `Deadline`.
37. Invoke the uniform-Bm fixture convention. With `Qe=2,I=3`, reject entry
    cap6 and use `Ce=9,configured_cap=(9,9Bm)`. Construct the public empty checkpoint by enrolling
    P0/P1/P2 at sequences1..3 and majors0..2, then admitting, delivering,
    acknowledging, projecting, and compacting 27 uniform-Bm ordinary records.
    The exact state is `H=30,F=31,o=30`, all three cursors30,
    `S=(0,0),C=0,B=(3,3Bm)`, debt zero, edge None, K free, order high29, and
    sequence claims31..51
    `[T0,T1,T2,E0,E1,E2,L_times_T(0..8),L_other_times_E(0..5)]`.
    Order claims30..35 are `[A0,A1,A2,X0,X1,X2]`. The identities are
    indexes0..2 at generation1 on distinct current epochs with distinct secrets, empty detach
    cells, no candidates, and no receipt/provenance row needed by this arm.

    Admit uniform-Bm ordinary rows31 and32 at caller majors30 and31, shifting only
    still-unmaterialized claims. Project through32 while every cursor remains
    30. This gives `F=31,S=(2,2Bm),C=0,B=(5,5Bm)`; the full ordinary envelope is
    `(5,5Bm)+Q(2,2Bm)+K(2,2Bm)=cap(9,9Bm)`, and all three floor terms remain31. Admit the exact
    third uniform-Bm ordinary row at sequence33/caller major32. Its exact first-use
    wire body is `RecordAdmission { conversation_id:C37,participant_id:P0,
    capability_generation:1,payload:[0x00;3×b_u] }` on P0's current binding,
    has payload length `3×b_u`, durable charge `(1,Bm)`, and uses
    `R_send>=AR+3×b_u,WF>=AD+3×b_u`. Its static retention headroom is
    `cap-(2Q+I×marker)=(2,2Bm)`. At f=31 its tentative
    `B=(6,6Bm)` fails the full envelope; removing row31 at f=32 gives
    `B=(5,5Bm)` and equality. Thus
    `preferred_floor=base_floor=31,cap_floor=F'=32`; every cursor satisfies
    `cursor+1=31<32`, so the same transaction plans exactly three markers
    34..36 with tuples `(32,CompactionMarker,0..2)`. The post-plan positions
    34..57 are exactly
    `[M0,M1,M2,T0,T1,T2,E0,E1,E2,L_times_T(0..8),
    L_other_times_E(0..5)]`, matching the canonical budget
    `{ high_watermark:33,remaining:MAX-33,E:3,T:3,M:3,RS:0,RT:0,
    L_times_T:9,L_times_RT:0,L_other_times_E:6 }`. The order vector
    33..38 is `[A0,A1,A2,X0,X1,X2]`. Position, value, and count therefore
    agree: 24 displayed sequence claims occupy exactly 34..57.

    Planning charges all three markers in S and acquires all three credits:
    rows32/33 plus the planned markers give `S=(5,5Bm),C=3,B=(5,5Bm)`; no marker is
    hidden in the remaining-reserve term. Append markers34,35,36 in candidate
    tuple order. Each `M` transition is credit- and B-neutral, no ClosureDebt
    or recovery reserve exists, and restart reconstructs the exact remaining
    suffix. Deliver/accept them in participant-index order, advance all three
    cursors and the observer through36, and compact. With `m=o=36`,
    `preferred_floor=base_floor=cap_floor=F'=37`; all marker credits release
    and the empty state is `S=0,C=0,B=3,debt=0,edge=None,K_remaining=0`.
    Crash before and after the third admission, each marker append, delivery,
    acceptance, progress, and compaction. This arm proves simultaneous marker
    charging and positional arithmetic without creating a multi-member debt
    episode; Case 54 supplies the separate crash/Leave/K ordering proof.

38. Bind P, offer through N, then Leave P and bind Q on the same connection. Deliver
    delayed bytes for P's normal and marker acks after Q is also offered through N.
    Their explicit P/generation tuple returns P's `Retired` and Q's cursor is
    unchanged. Repeat generation rotation for one participant and obtain
    `StaleAuthority`, never an inferred-current ack.
39. For credential attach, receipt replay, detach, Leave, normal ack, marker ack,
    and ordinary admission, present a never-minted id and assert
    `ParticipantUnknown`. For each binding-required operation present a current id/
    generation on an unbound/wrong connection and assert `NoBinding`; then exercise
    stale and retired precedence. No replay bytes appear before `AttachBound`, and
    unknown/stale/unbound attach outcomes start no server-driven replay. Ordinary
    sender always equals verified id; no failure allocates durable state.
40. Park ordered attach/ordinary/Leave rows in every state, including `Reserved`,
    then invalidate/retire/expire authority; known-no-commit rows delete and only
    exact recoverable fate enters bounded `TokenFateRecovery`. Crash immediately
    after reservation and at every cohort/per-row boundary. Renegotiate downward
    on every dimension using case25's exact nine shape dimensions, case32's five exact
    aggregate occupancies, and case52's three handshake-size dimensions. With all invalid,
    NonzeroLimit/N wins. For aggregate ties, the lowest `conversation_id` selects
    ConversationRows before ConversationBytes, followed by SDK Conversations/Rows/
    Bytes. Let `r40=max(PR+1,AR+1)` be the exact complete size of a
    `RecordAdmission` whose payload length is `r40-AR`; increasing that payload by
    one gives exact request size `r40+1`. Retain request/full-row pairs
    `(r40,b40)` at `(C40a,7)` and `(r40+1,b40+1)` at `(C40b,3)`, where
    `b40=r40+MR`, under initially valid `R>=r40+1,B>=b40+1,
    WF>=max(PF,r40+1)`. Proposed `R=r40-1` leaves
    `R_send=r40-1>=PR` and selects `SdkParkingCapacityIncompatible {
    dimension:RequestBytes,operands:{ conversation_id:C40a,park_order:7,
    actual:r40,limit:r40-1 } }` despite its larger park order. R-A4's proof shows
    no independent RowBytes arm exists once this request check and the MR row
    schema pass. Every refusal preserves all rows/interest slots and sends none. Prove the
    complete fixed order, no
    orphan/old-authority reissue/ack row, and no polling.
41. Backpressure detach T and lose response. Before progress, exact T returns
    Pending/current epoch/no sequence; different U uniquely returns
    `DetachInProgress` before `NoBinding`, without disclosing T. Race live replay T
    against progress in both orders: equality returns unchanged; greater progress
    drains first, or atomically rewrites cell+interest to the newer epoch. SDK never
    parks on a consumed epoch. Crash around every transaction and observe only
    complete Pending or Committed with real sequence. Replay T after commit is
    stable; a nonmatching token against Committed follows ordinary lookup.
42. Bind P to `(C,X)` and retain its live receipt. A fresh enrollment token on
    C/X returns `ConnectionConversationBindingOccupied` with no presented id;
    credential attach for Q returns it with only presented Q. Neither reveals P or
    mints/rotates/replays. Queue P delivery work, rotate P on the same C/X, and
    assert distinct old/new binding epochs plus exactly ordered
    `Detached(Superseded)`/`Attached`; the final constructor drops unconstructed
    old-epoch work and any already-emitted P bytes precede the rotation response
    barrier. Then detach P, bind Q on that same physical slot, and replay P's exact
    still-live receipt there: it is `UnboundReceipt`, never `Bound`. Queue more P
    work across that handoff; prove none is encoded after the barrier or delivered
    as Q and Q's first delivery is keyed to Q's epoch internally. Repeat a P
    credential handoff for conversation X from C1 to C2 while unrelated R on
    C1/conversation Y receives continuously: assert no order between TCP streams,
    atomically persist P's newer epoch, retire only P/X's exact C1 dispatch slot,
    and drop every later C1/X byte before exposing C2's `AttachBound`. C1's reader
    remains live; R/Y delivery is gap-free and no lifecycle event is emitted for R.
    Use two independent allocator-only test seeds, each with the complete live-
    reference set empty and no conversation fixture. In the server arm persist
    `server_incarnation=MAX`; the ordinary startup increment returns
    `ConnectionIncarnationExhausted { component:ServerIncarnation,
    current_value:MAX,attempted_server_incarnation:None }`. In the ordinal arm
    start the server at incarnation7 with `connection_ordinal=MAX` and
    `connection_ordinal_exhausted=true`; the next participant-capability negotiation returns
    `ConnectionIncarnationExhausted { component:ConnectionOrdinal,
    current_value:MAX,attempted_server_incarnation:Some(7) }`. Finally race P/Q into an empty slot: one winner binds and the different-id loser
    gets the typed outcome; codec vectors remain conversation-only. Add fixed-width
    incarnation vectors: same-connection rotation changes only generation; a new
    connection changes ordinal; restart increments server incarnation. A
    non-MAX collision with a durable reference checked-increments to the next
    ordinal and retries before publication; an exhausted component, or attempted
    reuse when no greater ordinal exists, returns
    `ConnectionIncarnationExhausted` without aliasing a live/receipt/work epoch.
43. Use two complete, legally constructed arms and invoke the uniform-Bm fixture
    convention for both. For the transaction-order arm set `I=4`. Enroll P0 at sequence1/major0, then bound-Leave it at sequence2
    using X-major2 and releasing A-major1. Repeat for P1 at sequence3/major3 and
    sequence4/X-major5, and P2 at sequence5/major6 and sequence6/X-major8.
    Each Leave persists the exact later-handle witness: its sole lower frontier
    handle is that same owner's invalidated A, no other claim survives, and the
    post-Leave frontier is empty before the next enrollment.
    Enroll P3 at sequence7/major9, then admit exactly MAX-11 ordinary records while P3
    continuously receives/acks and the observer projects/compacts. The last
    ordinary yields `H=MAX-4`, order high MAX-2, and shifts P3's A/X claims to
    MAX-1/MAX. The three tombstones and live index3 prove the prefix; no counter is
    injected and no I-scaled byte product approaches overflow.

    For the park-order arm let `q=(MAX-3)/4`. Enroll P0, then repeat q times:
    reserve four fresh-token same-generation credential-attach rows under one
    refusal epoch, allocate the next four park-order values without taking the
    optional empty-set reset, consume one TOLD progress event, let the first
    serialized row supersede P0, and delete the other three as `StaleAuthority`.
    Project/ack/compact the two lifecycle rows after each winner. Each cohort is
    bounded by N=G=4 and leaves the row set empty without resetting the retained
    counter. The q cohorts allocate exactly 0..MAX-4. Run two one-row winner
    cohorts at MAX-3/MAX-2, again declining the permitted reset, then reserve
    p43a at MAX-1 and stop before its send. This legal history has q+2
    supersessions, `H=2q+5`, generation q+3, and no injected SDK or server
    counter; each winner's receipt/provenance deadline events run before the next
    cohort, and all wakeups are response/progress or those admitted TOLD deadlines.

    | Quantity | Park-order arm | Transaction-order arm |
    |---|---|---|
    | H/F/o; floor | `H=2q+5,F=H+1,o=H`, retained log empty; `preferred_floor=base_floor=cap_floor=F` | `H=MAX-4,F=MAX-3,o=H`, retained log empty; `preferred_floor=base_floor=cap_floor=F` |
    | Members/budget | `L=E=T=1,M=RS=RT=0`; `{ high_watermark:2q+5,remaining:MAX-(2q+5),E:1,T:1,M:0,RS:0,RT:0,L_times_T:1,L_times_RT:0,L_other_times_E:0 }` | `L=E=T=1,M=RS=RT=0`; `{ high_watermark:MAX-4,remaining:4,E:1,T:1,M:0,RS:0,RT:0,L_times_T:1,L_times_RT:0,L_other_times_E:0 }` |
    | Cursor/marker | P0 cursor H; no anchor, credit, pending marker, or candidate | P3 cursor H; no anchor, credit, pending marker, or candidate |
    | Order | high q+2; A/X own q+3/q+4, `RO=RA=0` | high `MAX-2`; A/X own `MAX-1/MAX`, remaining2, `RO=RA=0` |
    | Debt/edge | zero, `None`, K free, zero claims/occurrences | same |
    | Retention | `I=1,Q=K=(2,2Bm),cap=(16,16Bm)`, `B=(1,Bm)` | `I=4,Q=K=(2,2Bm),cap=(9,9Bm)`, `S=(0,0),C=0,B=(4,4Bm)`; static ordinary maximum is `(1,Bm)` |
    | Identity/authority | P0 index/id0, epoch `(11,2;generation q+3)`, one exact current secret stored in every p43 request row, Empty detach/live cells | tombstones P0/P1/P2 plus P3 index/id3, epoch `(11,4;generation1)`, secret K3, Empty detach/live cells |
    | SDK parking | Let r43 be the codec-exact common size of p43a/p43b/p43c; Reserved p43a at park order `MAX-1`, P0/generation q+3, exact request/full-row bytes `r43/b43` where `b43=r43+MR`; exhausted false | no row/interest; counter0, exhausted false |
    | Parking config | `N=G=4,P=1,R=max(PR,AR+3×b_u,r43),B=R+MR,C=D=4B,RE=16,SE=26,EE=27,RF=16,RC(P)=8,WF=max(PF,AD+3×b_u,R,128)`, `RH(P)=40,SH(P)=51`, occupied rows/conversations/bytes `1/1/b43` | same config, occupied `0/0/0` |

    Reserve equal-size fresh-token p43b at park order MAX, set exhausted, and make
    p43c return `SdkParkOrderExhausted`. Run the ordered sender: p43a commits one
    rotation, then p43b's pre-write revalidation deletes it as `StaleAuthority`.
    With both actual rows now deleted, the empty set leaves the exhausted flag
    unchanged. Fresh current-generation p43d then uses the
    sole empty-set reset, allocates order0, and clears the flag. All rows carry
    exact current P0 authority at reservation time and the displayed codec byte counts.

    In the order arm send exact `RecordAdmission { conversation_id:C43,
    participant_id:P3,capability_generation:1,payload:[0x00;3×b_u] }`, encoded charge
    `(1,Bm)`. Authority, static size, capacity, sequence, and observer gates pass,
    but the caller major would leave `1 < A+X+RO+RA=2`. Return the canonical
    `ConversationOrderExhausted { conversation_id:C43,
    participant_id:P3,capability_generation:1,
    counter:TransactionOrder,high:MAX-2,next_value:Some(MAX-1),order_remaining:2,
    reserved_claims:2,required_majors:1,
    resulting_order_remaining:1,resulting_reserved_claims:2 }` with no mutation.
    Exact EOF on P3's epoch records `Died/ConnectionLost`, consumes A at MAX-1,
    and appends its pre-owned terminal at sequence MAX-3. It stores `B=(5,5Bm)`, debt
    zero/edge None, and `preferred_floor=base_floor=cap_floor=F'=MAX-3`; observer
    projection then sets `o=MAX-3` but detached P3's cursor MAX-4 still pins F.
    Exact first-use `LeaveRequest {
    conversation_id:C43,participant_id:P3,capability_generation:1,
    attach_secret:K3,leave_attempt_token:L43 }` then consumes X at MAX and appends
    `Left` at MAX-2; its permanent verifier exists and token cell is Empty. With
    no members `m=H'=MAX-2`, so `preferred_floor=base_floor=cap_floor=F'=MAX-2`;
    the EOF terminal compacts, `B=(5,5Bm)`, debt remains zero/edge None, and K is free.
    The final state
    has no claims. Every later new-major operation refuses; append-free work
    remains admissible and order never wraps or resets.
44. First prove public detached Leave normally. Then invoke the uniform-Bm
    fixture convention and construct this complete DCR positional checkpoint
    with `h=MAX-6`. Enroll P0, perform
    six supersessions to generation7, and pump ordinary work while continuously
    projecting/acking/compacting. Stop P0 at cursor h-4 with F=h-3 and retain
    h-3/h-2. The final ordinary caller at major h-8 appends h-1; its zero-debt
    search has `preferred_floor=base_floor=h-3,cap_floor=F'=h-2`, removes h-3,
    strictly overtakes P0, and plans/emits marker h with exact
    `(value,provenance_tag,current_sequence_owner,causal_tuple)` value
    `(h,NonProductM,M,(h-8,CompactionMarker,0))`. The retained h-2/h-1/marker h
    have `S=B=(3,3Bm),C=1`, exactly satisfying the `(7,7Bm)` zero-debt envelope.
    Deliver marker h to e7 without acceptance. Exact EOF on e7 then consumes A
    at major h-7 and appends terminal h+1. That mandatory transaction keeps
    `preferred_floor=h-3,base_floor=cap_floor=F'=h-2`, creates debt `(1,Bm)`, and
    selects DCR with the terminal already durable.

    | Quantity | Exact pre-success value |
    |---|---|
    | H/F/o | `H=h+1,F=h-2,o=h`; retained h-2..h+1 |
    | Budget/positions | `{high_watermark:h+1,remaining:5,E:1,T:0,M:0,RS:1,RT:1,L_times_T:0,L_times_RT:1,L_other_times_E:0}`; `[RS,RT,L_times_RT,E]` own h+2..h+5 and MAX is free |
    | Cursor/marker | P0 cursor h-4; delivered-unaccepted marker h with exact anchor/credit |
    | Order | high h-7; `[RO,RA,X]` own h-6/h-5/h-4; no candidate remains |
    | Debt/edge | `DetachedCredentialRecovery { participant_id:P0,marker_delivery_seq:h,prior_binding_epoch:e7 }`; `d=(1,Bm),K_remaining=(2,2Bm),exit_charge=(1,Bm)` |
    | Capacity | `I=1,J=2,Q=K=(2,2Bm),cap=(7,7Bm)`; `S=B=(4,4Bm),C=1`; absolute fit `4+2<=7` |
    | Identity/wire | P0 index/id0 detached generation7/K7; all unrelated cells Empty. Exact L44 is `LeaveRequest { conversation_id:C44,participant_id:P0,capability_generation:7,attach_secret:K7,leave_attempt_token:L44 }`. Exact V44 is `CredentialAttachRequest { conversation_id:C44,participant_id:P0,capability_generation:7,attach_secret:K7,attach_attempt_token:V44,accept_marker_delivery_seq:Some(h) }`; its separately derived target e8 is empty and success produces generation8/K8/e8. Both token cells are Empty and their exact verifiers exist before first use |
    | Occurrences | `O_max=865,O_base=397`; MarkerAppended, MarkerDelivered, and BindingFateObserved are Consumed; exact FencedRecoveryCommitted→None and LeaveCommitted→None facts/selections are Pending; every other base tag is Consumed; blocks 397..630/631..864 are Dormant |

    In the K-claim-backed detached Leave arm, exact L44 appends only Left h+2 because the
    terminal is durable, transfers its exact `(1,Bm)` charge
    `K_remaining:(2,2Bm)→(1,Bm)`, releases E/RS/RT/L×RT and the anchor, and
    retires P0. With no members `m=H'=h+2`; the transaction computes
    `preferred_floor=base_floor=cap_floor=F'=h+1`, removes h-2..h, and leaves
    terminal/Left at `S=(2,2Bm),C=0,B=(3,3Bm)`. Debt is zero and full-K equality
    `3+2+2=7` holds, so it atomically stores `edge=None,K_remaining=(0,0)`.

    In an independent copy of that same publicly constructed DCR checkpoint,
    the fenced arm's exact V44 consumes RS to append only
    Attached h+2, transfers its exact `(1,Bm)` charge
    `K_remaining:(2,2Bm)→(1,Bm)`, accepts marker h, moves
    RT to the new T and RA to the new A, and converts L×RT to L×T. The identical
    `preferred_floor=base_floor=cap_floor=F'=h+1` removal reaches B=3, debt zero,
    full-K equality, and atomically stores `edge=None,K_remaining=(0,0)`. Its
    exact post claims T/L×T/E own h+3/h+4/h+5; A/X own h-5/h-4. Lower generation
    or wrong secret is `StaleAuthority`; post-Leave different-token use is
    `Retired`. Crashes expose only the complete DCR checkpoint or one complete success.
45. Invoke the uniform-Bm fixture convention. Use `h=100`, `I=1,J=2`, and
    `Q=K=(2,2Bm)`. For the `(7,7Bm)` lifecycle caps, construct the public prefix by
    enrollment, six supersessions, and `h-14` ordinary records. Stop at cursor
    h-4 and F=h-3 with h-3/h-2 retained; the final ordinary at major h-8 appends
    h-1, computes `preferred_floor=base_floor=h-3,cap_floor=F'=h-2`, removes
    h-3, and emits marker h. The marker belongs
    to live epoch C1 and has **not** been delivered. P0 is index0,
    generation7/secret K7, with `L=E=T=A=X=1`, all RS/RT/RO/RA zero, empty
    detach/receipt/fingerprint/token cells, and ample sequence/order space.
    Exact U45 wire body is `CredentialAttachRequest { conversation_id:C45,
    participant_id:P0,capability_generation:7,attach_secret:K7,
    attach_attempt_token:U45,accept_marker_delivery_seq:None }` under Σ(0,0);
    its separately derived target C2 slot is empty. Exact V45 wire body is
    `CredentialAttachRequest { conversation_id:C45,participant_id:P0,
    capability_generation:8,attach_secret:K8,attach_attempt_token:V45,
    accept_marker_delivery_seq:Some(h) }` under Σ(1,1), with separate empty C3
    target. Independent optional-cycle copies use exact ordinary requests
    `CredentialAttachRequest { conversation_id:C45,participant_id:P0,
    capability_generation:8,attach_secret:K8,attach_attempt_token:W45,
    accept_marker_delivery_seq:None }` with an empty C3 target and
    `CredentialAttachRequest { conversation_id:C45,participant_id:P0,
    capability_generation:9,attach_secret:K9,attach_attempt_token:X45,
    accept_marker_delivery_seq:None }` with an empty C4 target. Their attempt
    cells are Empty and every receipt/identity quota input is ample. Distinct
    permanent Leave verifiers L45-C2/L45-C3 cover complete
    `LeaveRequest { conversation_id:C45,participant_id:P0,
    capability_generation:8,attach_secret:K8,leave_attempt_token:L45-C2 }` and
    `LeaveRequest { conversation_id:C45,participant_id:P0,
    capability_generation:9,attach_secret:K9,leave_attempt_token:L45-C3 }`
    bodies. Every named
    token cell is empty before first use; target epochs are not verifier input.

    At the legal startup minimum `Ce=5,configured_cap=(5,5Bm)`, the static ordinary maximum is
    componentwise `cap-(2Q+I×marker)=(0,0)`. Enroll Pmin normally, then submit an
    otherwise-authorized uniform-Bm ordinary record whose durable charge is `(1,Bm)`.
    R-D1 checks Entries before Bytes and returns exact
    `RecordTooLarge { conversation_id:C45M,participant_id:Pmin,
    capability_generation:1,dimension:Entries,
    encoded_record_charge:(1,Bm),max_ordinary_record_charge:(0,0) }` before
    order, sequence, closure, floor, record, or marker mutation. Equality at zero
    remains the configured bound. This minimum-cap arm deliberately does not seed
    the impossible uniform-Bm ordinary/marker history; the lifecycle arms below use
    `(7,7Bm)`, whose static maximum admits their uniform-Bm records.

    The successful farther arm uses `Ce=7,configured_cap=(7,7Bm)`, retained nonmarkers h-2/h-1 plus
    marker h, `F=h-2,o=h-3`, cursor h-4 (`cursor+1=h-3<F`),
    `S=B=3,C=1`, and zero debt. The public prefix has exactly six supersession
    extras plus one marker, so H=h implies order high h-8; its A/X claims are
    h-7/h-6. U45 atomically relays the logical A/X claims above h-7, uses the
    freed h-7 as its unreserved caller major for C1→C2 at sequences h+1/h+2,
    transfers logical A to C2, inserts the DCR claims,
    retargets the still-undelivered marker, stores debt Q/K, and selects OP through
    h+2 followed by exact marker delivery to C2. Its post-budget is
    `{ high_watermark:h+2,remaining:MAX-(h+2),E:1,T:1,M:0,RS:1,RT:1,
    L_times_T:1,L_times_RT:1,L_other_times_E:0 }`. Exact sequence positions
    h+3..h+8 are `[T,RS,RT,L_times_T,L_times_RT,E]`; exact order positions
    h-6..h-3 are `[A,RO,RA,X]`. The retained marker claim is
    `(h,NonProductM,M,(h-8,CompactionMarker,0))`. Post-U has
    `F=cap_floor=h-2,o=h-3,B=5,debt=Q,K_remaining=K`; its complete floor vector
    is `preferred_floor=h-3,base_floor=cap_floor=F'=h-2`.

    In every C2 fate arm, exact EOF on binding epoch C2 records
    `event_kind:Died,original_cause:ConnectionLost`. If that fate occurs after
    OP/delivery, major h-6 terminal h+3 appends while the
    actual-base search has `preferred_floor=h-3,base_floor=h-2,
    cap_floor=F'=h-1`, removing only h-2; the state remains
    `B=5,K_remaining=2,debt=2` and selects DCR. V45 then appends only Attached
    h+4, transfers charge `(1,Bm)`
    (`K_remaining:(2,2Bm)→(1,Bm)`), accepts marker h, moves
    F h-1→h+1 with `preferred_floor=base_floor=cap_floor=F'=h+1`, removing
    h-1/marker h, and reaches `B=5,debt=1`. It consumes
    one churn cycle and activates block0 with an exact OP through h+4 plus the
    recovered fate/live-Leave/detached-Leave cross-product; `K_remaining=1`
    equals its exact one-record detached exit charge. After OP and C3 cursor
    progress through h+2, `preferred_floor=base_floor=cap_floor=F'=h+3`
    removes h+1/h+2 and yields
    `B=3,debt=0,edge=None,K_remaining=0`; full K release is `3+2+2=7`.

    If C2 fate precedes initial OP/delivery, its terminal remains Pending and
    BindingFateObserved preserves that OP; OP completion separately selects DMR.
    A K-claim-backed detached Leave before OP appends the pending terminal h+3
    and Left h+4, transfers exact charge `(2,2Bm)`
    (`K_remaining:(2,2Bm)→(0,0)`), preserves OP, and leaves
    `preferred_floor=base_floor=cap_floor=F'=h-2`; OP completion then uses
    `preferred_floor=base_floor=cap_floor=F'=h+3`, removes h-2..h+2, and
    selects None at `B=3,K_remaining=0`. A K-claim-backed detached Leave after
    OP/fate appends only Left h+4, transfers charge `(1,Bm)`
    (`K_remaining:(2,2Bm)→(1,Bm)` before debt-zero release), and reaches the same legal full-K state directly at
    `preferred_floor=base_floor=cap_floor=F'=h+3`, atomically storing
    `edge=None,K_remaining=0`. No fenced token is eligible
    on DMR.

    A live C2 Leave before initial OP is `ObserverBackpressure`: its actual legal
    `cap_floor=h-1` exceeds `o+1=h-2`. After OP it uses the ordinary mandatory-Q
    class (not K-claim-backed), keeps K held, appends Left h+3, and
    `preferred_floor=base_floor=cap_floor=F'=h+3` removes h-2..h+2;
    `B=2,debt=0` and full-K fit `2+2+2<=7` select None and store
    `K_remaining=0`. MarkerAck is eligible only after OP/delivery;
    `preferred_floor=base_floor=cap_floor=F'=h+1` compacts exact h-2..h,
    leaves h+1/h+2 at
    `B=3`, and clears debt/full K at equality with `edge=None,K_remaining=0`.

    Here `O_max=865,O_base=397`; block0/1 are Dormant at 397..630/631..864.
    The base and activated blocks use the normative group coordinates, with all
    reachable witness-preservation tags Pending: fate→existing OP and later
    OP→DMR, Leave→existing OP and later OP→None, MarkerAck→None, and the
    corresponding direct arms.
    Every other tag is Consumed. Crash before/after each producer, OP, delivery,
    fate, recovery, Leave, ack, cursor, and compaction sees one complete vector.

    The optional-cycle closure is explicit. Exact W45's second C2 supersession before the
    initial OP is `ObserverBackpressure`. After OP, its complete later-fate/
    two-record DMR-Leave simulation can remove only h..h+2 with
    no members and `m=H'=h+6`; it computes
    `preferred_floor=base_floor=cap_floor=F'=h+3` and reaches the first no-edge
    node with four retained rows h+3..h+6, `S=(4,4Bm),C=0`, and therefore
    `B=S+((I-C)×marker_max)=(5,5Bm)`. Its full-K required-capacity vector is
    exactly `B+Q+K=(9,9Bm)`. Replay this same public uniform-Bm history in three
    independent component-cap copies. A cap `(7,9Bm)` is entry-driven and has
    post-U `B=(5,5Bm),debt=(2,0),K_remaining=(2,2Bm),
    K_headroom=(2,4Bm)`; the refusal selects
    `scope:Capacity,dimension:Entries,required:9,limit:7`. A cap `(9,7Bm)` is
    byte-driven and has post-U `debt=(0,2Bm),K_headroom=(4,2Bm)`; Entries passes
    at equality and the refusal selects
    `scope:Capacity,dimension:Bytes,required:9Bm,limit:7Bm`. The original cap
    `(7,7Bm)` has `debt=(2,2Bm),K_headroom=(2,2Bm)`; both components fail and
    Entries wins with `required:9,limit:7`. In all three copies the unchanged
    common suffix also has `marker_capacity_credits=1,marker_anchors=1`,
    `repayment_edge=MarkerDelivery { participant_id:P0,binding_epoch:C2,
    marker_delivery_seq:h }`, `edge_sequence_claims=6`,
    `edge_order_position_claims=4`, `edge_K_remaining=(2,2Bm)`,
    `episode_churn_used=0,delta_cycles=1,episode_churn_limit=2`; the exact
    triggering W45 credential-attach envelope precedes it. The optional producer
    mutates none of those fields.
    After V45, exact X45's C3 supersession would overtake cursor h and require a second
    marker/quartet, so the sole-quartet predicate returns the complete closure
    outcome with `scope:RecoveryFence,delta_cycles:0`; every common suffix field
    is the unchanged post-V45 prestate and block1 stays Dormant. Exact C3
    fate appends terminal h+5, stores `B=6,debt=2,K_remaining=1`, and preserves
    OP before selecting DCursor. Its K-claim-backed detached Leave appends h+6, transfers the
    exact remaining charge `(1,Bm)`
    (`K_remaining:(1,Bm)→(0,0)`), and OP/floor completion uses
    `preferred_floor=base_floor=cap_floor=F'=h+5`, leaving B=3 at full-K legality
    and atomically storing `edge=None,K_remaining=0`. Live C3 Leave before that
    OP is `ObserverBackpressure`; after OP it computes the same
    `preferred_floor=base_floor=cap_floor=F'=h+5` with B=2 and stores
    `edge=None,K_remaining=0`.
46. Through public operations enroll P (`E=1,T=1`), detach (`E=1,T=0`), reattach
    (`E=1,T=1`), and detach again (`E=1,T=0`); at each commit recompute every
    reserve term and prove no cycle creates, loses, or double-spends the flat exit
    claim. Then race bound Leave with connection death in both orders. Leave-first
    writes one `Left` and death sees `Retired`; death-first writes its exact-epoch
    terminal and detached Leave writes `Left` second. Inject crashes throughout and
    prove the mid-exit fate is exactly one of those two complete histories.
47. Invoke the uniform-Bm fixture convention and exercise exact sequence
    exhaustion from these complete snapshots; every refusal emits the canonical `SequenceBudget`:

    | Quantity | Arm A | Arm B | Arm C (durable B result) | Arm D |
    |---|---|---|---|---|
    | H/F/o; full floor | `MAX-2/MAX-7/MAX-2`; cursor MAX-8, pre `preferred_floor=base_floor=cap_floor=F` | `h=MAX-6; F=h-4; o=F`; cursor F-1, pre `preferred_floor=base_floor=cap_floor=F` | common post-EOF/post-OP checkpoint `H=h+1,F'=F+1,o=h`; fate computes `preferred_floor=base_floor=F,cap_floor=F'=F+1`, and later OP computes `preferred_floor=F,base_floor=cap_floor=F'=F+1` | `MAX-3/MAX-2/MAX-3` empty; `preferred_floor=base_floor=cap_floor=F` |
    | L/E/T/M | `1/1/0/0`, RS/RT 0 | `1/1/1/0`, RS/RT 1/1 | `1/1/0/1`, RS/RT **0/0** after undelivered fate releases the DCR branch | `1/1/1/0`, RS/RT 0 |
    | Canonical budget | `{high_watermark:MAX-2,remaining:2,E:1,T:0,M:0,RS:0,RT:0,L_times_T:0,L_times_RT:0,L_other_times_E:0}` | `{high_watermark:h,remaining:6,E:1,T:1,M:0,RS:1,RT:1,L_times_T:1,L_times_RT:1,L_other_times_E:0}` | `{high_watermark:h+1,remaining:5,E:1,T:0,M:1,RS:0,RT:0,L_times_T:0,L_times_RT:0,L_other_times_E:0}` | `{high_watermark:MAX-3,remaining:3,E:1,T:1,M:0,RS:0,RT:0,L_times_T:1,L_times_RT:0,L_other_times_E:0}` |
    | Cursor; anchor/credit | P0=MAX-8, so `cursor+1=F`; none/none | P0=F-1, so equality before fate; none/none | unchanged cursor; one planned undelivered P0 anchor/credit | P0=H; none/none |
    | A/X/RO/RA; order | `0/1/0/0`, high MAX-5; X owns MAX-4 | `1/1/1/1`, high h-4, remaining `MAX-(h-4)`, positions h-3..h own `[A,RO,RA,X]` | `0/1/0/0`, high h-3; terminal and marker share causal major h-3 (terminal phase0, marker phase4/index0), X owns h-2, and quartet order claims are released | `1/1/0/0`, high MAX-8; A/X own MAX-7/MAX-6 |
    | Debt/edge/occurrences | `(1,Bm)`; exact `DetachedCursorRelease { participant_id:P0,last_dead_binding_epoch:e47A }`, K_remaining `(2,2Bm)`. For `O_max=946,O_base=478`, base Leave→None is Pending, impossible base tags Consumed, and blocks 478..711/712..945 Dormant | Q; `ObserverProjection { through_seq:h }`, K_remaining 2, `episode_churn_used=0,limit=J`. For `O_max=892,O_base=424`, normative base-coordinate groups keep the exact fate→preserved OP/PC, OP→PC, PC→marker, marker append→DMR, live Leave→preserved OP/PC, and later detached Leave→direct None tags Pending; direct OP→DMR and PC→DMR plus every other impossible base tag are Consumed. Churn blocks 0/1 are Dormant at 424..657/658..891 | Q/K_remaining 2, `episode_churn_used=0,limit=J`; fate's first marker uses the pre-endowed base-identity cycle. Both OP and fate event occurrences are Consumed at this common checkpoint; exact `PhysicalCompaction { from_floor:F+1,through_seq:F+1 }` is current, followed by marker append then DMR. The remaining coordinate groups are Pending/Consumed identically in both orderings; both churn blocks remain Dormant | zero/None/K free; occurrence array empty |
    | Caps/S | `Ce=10,I=1,J=2,Q=K=(2,2Bm),cap=(10,10Bm),S_actual=S=(6,6Bm),C=0,B=(7,7Bm)` | `Ce=8`, cap `(8,8Bm)`, `S_actual=S=5,C=0,B=6` | `S_actual=5,S=6,C=1,B=6` | `Ce=10,I=1,J=2,Q=K=(2,2Bm),cap=(10,10Bm),S_actual=S=0,C=0,B=(1,Bm)` |
    | Identity/cells/candidates | C47A/P0 detached gen3/K3 after public dead binding epoch e47A, no binding; detach/receipt/fingerprint/token cells empty; terminal MAX-2 is durable; no candidate | C47BC/P0 bound `(20,1;gen4)`/K4; cells empty; no candidate | C47BC/P0 detached after exact terminal; cells empty; one marker candidate with value h+2 counted in M while retaining exact `TerminalProduct { terminal:T,affected_participant:P0 }` provenance and its terminal-causal tuple | C47D/P0 bound `(20,2;gen5)`/K5; cells empty; no candidate |

    Arm B has a legal coupled prefix: reach generation3, project/ack/compact
    through h-5, retain ordinary h-4/h-3/h-2, then make the final generation3→4
    exact-Q supersession append h-1/h. Consequently H=h, F=h-4, cursor=h-5,
    order high h-4, and its six sequence positions h+1..MAX are exactly
    `[T,RS,RT,L_times_T,L_times_RT,E]`; consequently
    `recovery_attach_sequence=h+2` and
    `replacement_terminal_sequence=h+3`. Its four order positions h-3..h are
    exactly `[A,RO,RA,X]`, so `recovery_attach_order=h-2` and
    `replacement_terminal_order=h-1`. Fate consumes A=h-3, releases RO/RA for
    the undelivered-marker branch, and atomically shifts X from h to the freed
    next value h-2. In sequence space it consumes T h+1, transfers
    L_times_T h+4 onto M at h+2, releases RS/RT/L_times_RT at h+2/h+3/h+5,
    and atomically shifts E from MAX to h+3. Thus marker h+2 and later L47-C Left h+3 remain
    gap-free; this is also why Arm C can state X=h-2 without an unowned gap.

    Exact fresh Leave tokens L47-A, L47-C, and L47-D have distinct permanent
    non-reversible verifiers over the complete canonical bodies
    `LeaveRequest { conversation_id:C47A,participant_id:P0,
    capability_generation:3,attach_secret:K3,leave_attempt_token:L47-A }`,
    `LeaveRequest { conversation_id:C47BC,participant_id:P0,
    capability_generation:4,attach_secret:K4,leave_attempt_token:L47-C }`, and
    `LeaveRequest { conversation_id:C47D,participant_id:P0,
    capability_generation:5,attach_secret:K5,leave_attempt_token:L47-D }`
    respectively. Each named
    arm presents its matching token/body; its Leave-token cell is empty before
    commit, the request codec is canonical, and every unrelated receipt,
    fingerprint, detach, and token lookup misses exactly as the table states.

    Arm A is also a public history rather than an injected boundary state. Reach
    bound generation3 in exact epoch e47A and, while maintaining the zero-debt envelope, retain five
    uniform-Bm ordinary rows MAX-7..MAX-3 with P0 at cursor MAX-8 and F=MAX-7. At
    `H=MAX-3` the exact bound reserve `[T,L_times_T,E]` owns MAX-2/MAX-1/MAX;
    this proves a bound H=MAX-2 premise would be unreachable. Exact EOF consumes
    T at the pre-owned A major MAX-5, appends terminal MAX-2, releases L×T,
    shifts E to MAX-1, and creates the displayed DCursor debt with
    `preferred_floor=base_floor=cap_floor=F'=MAX-7` and no marker. Exact-current L47-A appends Left at MAX-1, transfers charge
    `(1,Bm)` (`K_remaining:(2,2Bm)→(1,Bm)` before debt-zero release), and—with no members, `m=H'=MAX-1,o=MAX-2`—uses
    `preferred_floor=base_floor=cap_floor=F'=MAX-1`. Removal MAX-7..MAX-2 leaves
    only Left at `B=(2,2Bm)`, satisfies full-K fit, and atomically stores
    `edge=None,K_remaining=(0,0)`. MAX remains deliberately unallocated: the
    reserve proof, rather than wrap or an impossible fixture, is the tighter
    sequence-domain boundary.
    In B the displayed canonical pre-budget owns all
    six remaining values.
    EOF appends at h+1 and transfers the L×T marker into M. Without removal the
    tentative `B'=7` fails mandatory fit `7+K_remaining(2)>8`; removing exactly
    the first retained entry and planning its credited marker yields `B'=6`, fit
    `6+2=8`, debt Q, and exact `preferred_floor=base_floor=F,
    cap_floor=F'=F+1`. C's restart codec asserts
    `provenance_tag=TerminalProduct {
    terminal:T,affected_participant:P0 }`, `current_sequence_owner=M`, and the
    exact terminal-causal tuple; marker append changes only
    S_actual `5→6`, then M `1→0`. Because the marker is not delivered before the
    epoch dies, W1 selects DMR and atomically releases RS/RT/RO/RA and L_times_RT;
    it never preserves a phantom fenced-recovery branch. In the EOF-first
    ordering, EOF reaches an intermediate `o=F` state while OP remains current;
    OP completion then advances o to h and selects PC at F+1. In the OP-first
    ordering, OP first selects PC at F; EOF's exact `cap_floor=F+1` satisfies
    that range and selects the same next PC at F+1. Thus both orderings consume
    distinct event/selection slots, append terminal h+1 before exact marker h+2,
    and converge only at the displayed common C checkpoint with monotonic `o=h`.
    Completing PC `[h-3,h-3]` and appending the marker leaves h-2..h+2 at
    `B=5,debt=1,K_remaining=2`; that completion has
    `preferred_floor=h-4,base_floor=h-3,cap_floor=F'=h-2`.
    K-claim-backed detached L47-C later consumes E through DMR, appends only Left h+3
    because the terminal is durable, and transfers that exact one-record charge
    `(1,Bm)` (`K_remaining:(2,2Bm)→(1,Bm)`). With no members `m=H'=h+3` and `o=h`, it computes
    `preferred_floor=base_floor=cap_floor=F'=h+1`, removes h-2..h, and leaves
    terminal, marker, and Left at `B=3,debt=0`. Full-K release is exact
    `(3,3Bm)+(2,2Bm)+(2,2Bm)=(7,7Bm)<=(8,8Bm)`, so that same commit stores `edge=None,K_remaining=0`; no
    debt-zero partial-K endpoint is visible. Arm D is reached by generation5 followed by enough ordinary
    admissions with continuous ack/projection/compaction to obtain the displayed
    empty H/F state; its four supersession extras force order high MAX-8.
    Arm D remains
    `remaining=3=E+T+L_times_T`; bound Leave appends one Left at MAX-2 and discharges
    E/T. Assert exact payload fields, cap_floor, no wrap, and no hidden default.
48. Invoke the uniform-Bm fixture convention, then test-seed two complete
    observer-order snapshots at `h=100`. The public marker
    history is exact: reach P0 generation5, pump ordinary work while continuously
    acking/projecting/compacting empty through h-4, retain uniform-Bm nonmarkers h-3/h-2,
    and stop P0 at cursor h-4. The four supersession extras plus the marker below
    imply causal-order high h-6. With `F=h-3`, P0 cursor
    h-4 (`cursor+1=F`), and otherwise ample sequence/order authority, valid
    ordinary admission at major h-6 appends h-1. Its zero-debt capacity search
    removes h-3, computes `preferred_floor=base_floor=h-3,
    cap_floor=F'=h-2`, strictly overtakes P0, and plans
    `NonProductM` marker h with exact causal tuple
    `(h-6,CompactionMarker,0)`; the emitter then appends h. Thus the
    retained prefix is nonmarkers h-2/h-1 plus the undelivered credited marker h,
    and `edge_claims` retains that immutable provenance tag, current owner, and
    tuple. Exact credential token U48,
    whose canonical request explicitly carries `accept_marker_delivery_seq:None`,
    then superseded e5→e6 at major h-5, appended terminal h+1/Attached h+2, retargeted
    the still-undelivered marker, and only afterward delivered marker h to e6
    without acceptance. U48's canonical generation5/K5 body, Σ(0,0) quota
    inputs, receipt, provenance, and permanent verifier are all exact. Thus
    `H=h+2,F=h-2`; the five retained rows are h-2..h+2,
    `S_actual=S=B=(5,5Bm),C=1`, and P0 is index0, generation6/K6 at e6.

    Use `Ce=7,configured_cap=(7,7Bm),I=1,J=2,Q=K=(2,2Bm)`, debt Q,
    `K_remaining=K`, `episode_churn_used=0`, and edge
    `ParticipantCursorProgress { participant_id:P0,binding_epoch:e6,
    through_seq:h,marker_delivery_seq:Some(h) }`, with
    `edge_claims.marker_sequence_values[0]=
    (h,NonProductM,M,(h-6,CompactionMarker,0))`. The canonical budget at high
    h+2 has `E=T=RS=RT=L_times_T=L_times_RT=1`, `M=L_other_times_E=0`;
    exact sequence claims h+3..h+8 are `[T,RS,RT,L_times_T,L_times_RT,E]`;
    `A,RO,RA,X` own majors h-4..h-1 after order high h-5. U48's
    receipt/provenance occupy one slot each.
    Exact V48 wire body is `CredentialAttachRequest { conversation_id:C48,
    participant_id:P0,capability_generation:6,attach_secret:K6,
    attach_attempt_token:V48,accept_marker_delivery_seq:Some(h) }` under Σ(1,1).
    Its live verifier covers exactly that body; the separately derived target e7
    slot is empty and success would produce epoch e7 and generation7/K7. Exact
    Leave tokens L48-E6/L48-E7 have distinct permanent verifiers over
    `(P0,generation6,K6)` and `(P0,generation7,K7)`.
    Before each tested first use, P0's detach cell, V48's attempt-token cell, and
    both Leave-token cells are Empty; the e7 target slot is empty. U48 alone is already committed
    with the stated live receipt/provenance, so exact U48 replay reads that cell
    and no V48/Leave arm silently inherits it.
    The behind arm has `o=h-1`; the ahead arm has `o=h`.

    MarkerAck changes no S/C value. Behind has
    `preferred_floor=base_floor=cap_floor=F'=h`,
    removes h-2/h-1, retains the accepted marker plus h+1/h+2, and reaches
    `B=3`; ahead has `preferred_floor=base_floor=cap_floor=F'=h+1`, also removes
    the marker, and likewise reaches `B=3`. Both atomically clear debt, store
    `edge=None,K_remaining=0`, and release full K at equality `3+2+2=7`; any later marker
    projection/compaction is ordinary zero-debt work.

    If exact EOF on binding epoch e6 records
    `event_kind:Died,original_cause:ConnectionLost`, fate wins: major h-4 terminal
    h+3 appends while the actual-base search computes
    `preferred_floor=h-3,base_floor=h-2,cap_floor=F'=h-1`, removing h-2; it remains
    `B=5,K_remaining=2,debt=2` and selects
    DCR. V48 then appends only Attached h+4, transfers exact charge `(1,Bm)`
    (`K_remaining:(2,2Bm)→(1,Bm)`), accepts marker h, and computes
    `preferred_floor=base_floor=cap_floor=F'=h` behind or all three equal h+1
    ahead. Both end at `B=5,debt=1`, consume churn 0→1, and activate block0
    with OP through h+4 and the recovered cursor/fate/Leave cross-product.
    OP plus cursor progress through h+2 later gives
    `preferred_floor=base_floor=cap_floor=F'=h+3`; behind it removes marker h
    plus h+1/h+2, while ahead (which already removed the marker) removes h+1/h+2.
    Both reach
    `B=3,debt=0` with full-K equality `3+2+2=7`; that same completion stores
    `edge=None,K_remaining=0`.

    Before that completion, exact live L48-E7 is the recovered-binding Leave
    ordering. It appends one Left h+5 without a K transfer and uses all-h behind
    or all-(h+1) ahead for its initial floor triple. It reaches
    `B=6,debt=2,K_remaining=1` and selects OP through h+5. Projection with no
    members sets `o=h+5` and
    `preferred_floor=base_floor=cap_floor=F'=h+6`, removes every row, reaches
    `B=1,debt=0`, proves full-K fit `1+2+2=5<=7`, and atomically stores
    `edge=None,K_remaining=0`.

    In the complementary e7 ordering, exact EOF first records
    `Died/ConnectionLost` for epoch e7 and appends terminal h+5 at the same
    respective all-h/all-(h+1) floor. It reaches
    `B=6,debt=2,K_remaining=1` and preserves the current storage suffix, whose
    exact later tag is `DetachedCursorRelease` because marker h was already
    accepted. Exact K-claim-backed detached L48-E7 then appends only Left h+6, transfers its
    one-record charge `(1,Bm)` (`K_remaining:(1,Bm)→(0,0)`), and reaches `B=7,debt=2` at that same
    floor with OP through h+6. Projection sets `o=h+6` and
    `preferred_floor=base_floor=cap_floor=F'=h+7`, removes every row, reaches
    `B=1,debt=0`, proves the same full-K fit5, and stores
    `edge=None,K_remaining=0`. Thus the recovered cursor/fate/Leave cross-product
    named by block0 has both production triggers and no debt-zero partial-K state.

    Exact K-claim-backed detached L48-E6 after that fate appends only Left h+4
    and transfers charge `(1,Bm)`
    (`K_remaining:(2,2Bm)→(1,Bm)`); its floor triple is all h behind or all h+1
    ahead, leaving `B=5,debt=1` and OP through h+4. Completion advances
    `o→h+4` and, with no members `m=H'=h+4`, computes
    `preferred_floor=base_floor=cap_floor=F'=h+5`, removes all remaining rows,
    reaches `B=1,debt=0`, proves full-K fit `1+2+2=5<=7`, and in that same
    commit stores `edge=None,K_remaining=0`. Live L48-E6 is the ordinary
    mandatory-Q class: it appends Left h+3 with K held, uses the same all-h/all-
    h+1 initial floor triples, reaches `B=4,debt=1`, and likewise selects OP.
    Its completion computes `preferred_floor=base_floor=cap_floor=F'=h+4`,
    reaches `B=1,debt=0`, proves the same full-K fit5, and atomically stores
    `edge=None,K_remaining=0`. If ack wins either race, it clears ClosureDebt first and
    the later fate/Leave is an ordinary zero-debt lifecycle transaction.

    Here `O_max=865,O_base=397`; block0/1 are Dormant at 397..630/631..864.
    The base-coordinate groups keep exactly the PCP→None, fate→DCR,
    recovery→OP, live/detached Leave→direct OP, and
    completion→strict-suffix tags Pending; all impossible tags are Consumed.
    Crash both sides of every ack, projection, fate, Leave, recovery, cursor, and
    compaction transaction; no acceptance returns capacity or exposes an invalid
    K/floor witness.
49. Invoke the uniform-Bm fixture convention and test-seed this complete
    undelivered-marker snapshot with `h=100`. Its public causal history is exact. Reach bound P0 generation7 and
    pump/ack/project/compact empty through h-5. Exact-Q supersession e7→e8 at
    major h-11 appends terminal h-4 and Attached h-3. Let e8 receive/ack through
    h-4, then project and compact only the terminal, leaving Attached h-3 at
    `F=h-3` with P0 cursor h-4 (`cursor+1=F`) and the bounded debt edge. Credential
    supersession e8→e9 at major h-10 appends e8's terminal
    h-2 and e9's Attached h-1. At `configured_cap=(5,5Bm)` its actual-base search must remove h-3,
    computes `preferred_floor=base_floor=h-3,cap_floor=F'=h-2`, strictly
    overtakes P0, and plans `NonProductM`
    marker h with exact causal tuple `(h-10,CompactionMarker,0)`.
    The emitter appends h but does not deliver it. Both earlier attach
    receipt/provenance pairs expire at their admitted deadlines before this seed,
    leaving the empty cells stated below. The eight supersession extras plus this
    marker make post-emission order high h-10. Epoch e9's fate at major h-9 then stores
    the exact pending terminal `(h-9,BindingTerminal,0)` with
    `event_kind:Died,original_cause:ConnectionLost,binding_epoch:e9`, and selects
    DMR; it does not fabricate a delivery fact. The complete seed is:

    | Quantity | Exact value |
    |---|---|
    | H/F/o/floor | `H=h,F=h-2,o=h-3`; P0 cursor h-4, so `cursor+1=h-3<F` |
    | Members/budget | `L=E=T=1,M=RS=RT=0`; `{ high_watermark:h,remaining:MAX-h,E:1,T:1,M:0,RS:0,RT:0,L_times_T:1,L_times_RT:0,L_other_times_E:0 }` |
    | Order | `A=0,X=1,RO=RA=0`; the allocated pending-terminal tuple is major h-9, so order high is h-9; X owns h-8 and no tuple intervenes |
    | Capacity | `Ce=5,I=1,J=2,Q=K=(2,2Bm),cap=(5,5Bm)`; `S_actual=S=B=(3,3Bm),C=1`, debt Q, `K_remaining=K` |
    | Edge/occurrences | `DetachedMarkerRelease { participant_id:P0,marker_delivery_seq:h,last_dead_binding_epoch:e9 }`; `edge_claims.marker_sequence_values[0]=(h,NonProductM,M,(h-10,CompactionMarker,0))`; `episode_churn_used=1,limit=J`; for `O_max=811,O_base=343`, the supersession activated block0 at 343..576, whose exact LeaveCommitted→OP, ProjectionCompleted→PC, and CompactionCompleted→None tags remain Pending while its other tags and the superseded base-cycle tags are Consumed; block1 at 577..810 remains Dormant (this Leave-only edge cannot activate it) |
    | Authority/cells | P0 index0 detached, generation 9/secret K9; exact PendingFinalization candidate `(h-9,BindingTerminal,0,event_kind:Died,original_cause:ConnectionLost,binding_epoch:e9)` is the sole candidate; the test request presents Leave token L49, generation9/secret K9, and its canonical body/verifier input; server detach/receipt/fingerprint/token cells and other anchors/credits are empty before commit |

    In independent arms, fresh tokens U49-N/U49-M have Empty attempt cells and
    exact `CredentialAttachRequest` wire bodies carrying C49, P0, generation9,
    K9, their respective token, and the marker option below under Σ(0,0). Their
    live verifiers cover only those bodies; the separately derived e10 target slot
    is empty and all unrelated gates are ample. U49-N presents
    `accept_marker_delivery_seq=None` and returns the generic closure row with
    `scope:RecoveryFence`; U49-M presents `Some(h)` and returns
    `MarkerNotDelivered { conversation_id:C49,token:U49-M,participant_id:P0,
    capability_generation:9,requested_marker_delivery_seq:h,
    reason:NotDeliveredToProofEpoch,expected_marker_delivery_seq:h }`.
    Both preserve the full snapshot. Exact-current K-claim-backed detached Leave
    consumes block0's LeaveCommitted event/OP selection. Before it, the exact
    sequence claims `[T,L_times_T,E]` own h+1/h+2/h+3. It atomically
    consumes T to append terminal h+1, releases unused L×T at h+2, shifts E from
    h+3 to that free h+2, and appends Left h+2. Its
    exact charge `(2,2Bm)` transfers
    `K_remaining:(2,2Bm)→(0,0)`, releases T/E and the marker anchor,
    and yields `S=B=(5,5Bm),C=1,d=Q`, fit 5, with the all-zero post-budget at
    `high_watermark=h+2`. With no members `m=H'=h+2`; because `o=h-3`,
    `preferred_floor=base_floor=cap_floor=F'=h-2` for the Leave and no intermediate floor is
    visible. It selects OP through h+2. After projection sets o=h+2, exact
    compaction `[h-2,h+2]` has `preferred_floor=base_floor=cap_floor=F'=h+3`, removes all
    five charges, and reaches `S=0,C=0,B=(1,Bm),d=0,edge=None,K_remaining=0`,
    where full-K release is exact `1+2+2=5`. Crashes around every occurrence expose only a
    complete prefix, and no arm claims fenced recovery from the undelivered marker.
50. Replace and adversarially test all thirteen sweep-classified LAW-1
    families. Preserve prior arms and distinguish the TypeScript executing timer
    loop from `«SDK-RECONNECT-DELAY-CONTRACT»`: for Rust lifecycle, public
    `RemoteHandle::reconnect`, and Gleam, a fresh transport-fate event may
    authorize exactly one reconnect transition/returned delay. Its single-use
    permit returns exact local `ReconnectArmed { delay_ms }`; a second
    `Reconnecting→reconnect` call through either Rust entry point, the Gleam
    entry point, or caller timer re-arm without a new fate event returns exact
    `ReconnectNotArmed { state:Reconnecting,
    required_event:TransportFate }` and mutates no state, attempt counter,
    delay, timer, or network state. These are the two arms of
    `ReconnectDelayResult`, not wire outcomes.
    Manual connect is separately TOLD. Add dedup expiry notification. Add TS pressure Defer: one accepted buffered item, no producer retry or duplicate publication, and delay interpreted only as delivery estimate. Re-run all 21 §1 counts over the five roots at the pin, including
    `172/43/27/109` for underscore-tolerant reconnect/retry/backoff/delay.
    Independently AST/CFG-audit every clock/timeout/sleep/probe/retry/sweep/scan
    loop or callback; every unmatched wait shape fails.
51. Invoke the uniform-Bm fixture convention and construct the complete
    **pre-attach** state with `h=MAX-7`: reach bound P0
    generation5, pump ordinary work with continuous ack/projection/compaction,
    stop its cursor at h-2, retain ordinary h-1, then take exact EOF and append
    its terminal h. Thus `H=h,F=h-1,o=h-2`; detached P0 has cursor h-2 and
    `cursor+1=F`, so it has never been overtaken. `L=E=1,T=M=RS=RT=0`; canonical budget
    `{ high_watermark:h,remaining:7,E:1,T:0,M:0,RS:0,RT:0,
    L_times_T:0,L_times_RT:0,L_other_times_E:0 }`. Capacity is
    `Ce=7,I=1,J=2,Q=K=(2,2Bm),cap=(7,7Bm),S_actual=S=(2,2Bm),C=0,
    B=(3,3Bm)`, debt zero/None,
    K free. P0 is index0, detached generation 5/secret K5; target binding slot,
    detach cell, receipt/fingerprint/token cells, candidates, anchor, and credit
    are empty. Exact token U51 has wire body `CredentialAttachRequest {
    conversation_id:C51,participant_id:P0,capability_generation:5,
    attach_secret:K5,attach_attempt_token:U51,accept_marker_delivery_seq:None }`;
    its live verifier covers exactly that body. The separately derived target e6
    slot is empty, the server token cell is empty before commit, and success
    persists that cell while producing epoch e6 and generation6/secret K6. Exact Leave token
    L51 and its permanent non-reversible verifier cover canonical body
    `(P0,generation6,secret K6)` for both the live and later detached Leave arms.
    The coupled public history has four supersession extras, so after the terminal
    `A=0,X=1,RO=RA=0`, order high h-5 and X owns h-4. The remaining attach gates are
    exact: connection conversation capacity/occupancy is 2/0; live-receipt
    server/participant limits and occupancies are 2/2 and 0/0; provenance
    server/conversation/participant limits and occupancies are 2/2/2 and 0/0/0;
    receipt/provenance TTLs are 1000/2000ms; and parking is empty under the valid
    `N=G=4,P=1,R=max(PR,40),B=R+MR,C=D=4B,
    RE=16,SE=26,EE=27,RF=16,RC(P)=8,WF=max(PF,R,128)`
    configuration (`RH(P)=40,SH(P)=51`).

    A production detached credential attach appends Attached h+1 (`MAX-6`). Its
    exact post-budget is `{ high_watermark:h+1,remaining:6,E:1,T:1,M:0,RS:1,
    RT:1,L_times_T:1,L_times_RT:1,L_other_times_E:0 }`; post order claims are
    `A=X=RO=RA=1`. It binds exact epoch e6, persists the receipt, and reaches
    `S_actual=S=(3,3Bm),C=0,B=(4,4Bm),d=(1,Bm),K_remaining=K,
    episode_churn_used=0,episode_churn_limit=J` with
    `ObserverProjection { through_seq:h+1 }`. Here
    transaction-order high is h-4; order positions h-3..h are exactly
    `[A,RO,RA,X]`, so `recovery_attach_order=h-2` and
    `replacement_terminal_order=h-1`. Sequence positions h+2..MAX are exactly
    `[T,RS,RT,L_times_T,L_times_RT,E]`, so
    `recovery_attach_sequence=h+3` and
    `replacement_terminal_sequence=h+4`. Exact EOF on binding epoch e6 records
    `event_kind:Died,original_cause:ConnectionLost`; this no-marker fate consumes T/A at
    h+2/h-3, releases RS/RT/RO/RA and both products, and shifts X from h to
    the freed next major h-2 plus E from MAX to the freed next sequence h+3.
    It leaves `S=(4,4Bm),B=(5,5Bm),d=Q,K_remaining=K`. Here
    `preferred_floor=base_floor=cap_floor=F'=h-1`: cursor+1=F' is equality, so M, marker
    credit, anchor, and candidate all remain zero. The same otherwise complete
    checkpoint with `h=MAX-6` is the old equality refusal: the proposed append
    would make `high_watermark=MAX-5,remaining=5` against the same six resulting
    claims and returns `ConversationSequenceExhausted` with the exact common
    credential-attach request envelope plus
    `sequence_budget:{ high_watermark:MAX-5,remaining:5,E:1,T:1,M:0,RS:1,
    RT:1,L_times_T:1,L_times_RT:1,L_other_times_E:0 }` and no other
    exhaustion-specific field before mutation.

    Here `O_max=865,O_base=397`; block0/1 are Dormant at
    397..630/631..864. The base-coordinate groups keep every legal tag Pending:
    ack-before-OP preserves/selects OP then PC; OP-before-ack selects PCP(None)
    then PC; fate before either OP or PC preserves that witness and its completion
    selects DetachedCursorRelease; live or detached Leave before an existing OP/PC
    preserves it and its completion selects the distinct post-Leave suffix. Direct
    fate/Leave arms and every final None are separately keyed. All impossible
    tags are Consumed, so no parent event shares a selection occurrence.

    That already-pinned fate appends the claimed terminal at h+2 without a floor
    move or marker, releases unused RS/RT/RO/RA and both product branches, and
    records `DetachedCursorRelease` as OP's strict suffix. Its post-budget is
    `{ high_watermark:h+2,remaining:5,E:1,T:0,M:0,RS:0,RT:0,
    L_times_T:0,L_times_RT:0,L_other_times_E:0 }`.
    If live bound Leave wins before fate, the ordinary mandatory-Q alternative
    appends one-record Left h+2 with K still fully held. Before initial OP
    completion its `preferred_floor=base_floor=cap_floor=F'=h-1` and
    `B=5,debt=Q` (`m=H'=h+2,o=h-2`). Leave-before-OP preserves the old OP; after
    observer progress through h+2, the successor compaction uses
    `preferred_floor=base_floor=cap_floor=F'=h+3`, removes h-1..h+2, and stores
    `B=1,d=0,edge=None,K_remaining=0` with full-K fit. If the original OP has
    already set o=h+1, the Leave transaction itself uses
    `preferred_floor=base_floor=cap_floor=F'=h+2`, removes h-1..h+1, retains
    Left h+2 at B=2, and clears at full-K fit 6<=7 with
    `edge=None,K_remaining=0`.

    If fate wins first, fresh credential tokens U51-N/U51-M have Empty attempt
    cells and exact `CredentialAttachRequest` wire bodies over C51, P0,
    generation6/K6, their distinct token, and the option below under Σ(1,1).
    Their verifiers cover only those bodies; the separately derived e7 slot is
    empty and every other attach gate ample. U51-N presents
    `accept_marker_delivery_seq=None` and returns the complete generic
    `MarkerClosureCapacityExceeded` row with `scope:RecoveryFence`; U51-M presents
    `Some(h+1)` and returns `MarkerMismatch { conversation_id:C51,token:U51-M,
    participant_id:P0,capability_generation:6,
    requested_marker_delivery_seq:h+1,reason:NoMarkerExpected }`, because this
    DCursor edge owns no marker. Normal/marker ack and ordinary admission return `NoBinding`. Each
    refusal preserves the edge, counters, cells, floor, and claims byte-for-byte.
    Exact-current K-claim-backed detached L51 then appends only Left h+3 because
    the terminal is already durable. It transfers exact charge `(1,Bm)`
    (`K_remaining:(2,2Bm)→(1,Bm)`). Before
    initial OP it stores
    `preferred_floor=base_floor=cap_floor=F'=h-1,B=6,debt=2,K_remaining=1` and
    preserves OP.
    Once o=h+1, the actual-base search uses
    `preferred_floor=base_floor=cap_floor=F'=h+2`, removes ordinary h-1, the old
    terminal h, and Attached h+1, leaving terminal/Left at `B=3,d=0`; the same
    transaction atomically stores `edge=None,K_remaining=0`, and full-K equality
    is `3+2+2=7`. If OP wins before fate, the later fate/Leave transaction reaches
    that same vector directly. This K-claim-backed detached Leave
    is the sole participant successor. Restart assertions cover every branch and
    prove M and marker_delivery_seq remain absent.
52. Validate one-shot recovery at both lifecycle phases. Let
    `p52=max(PF,PR)+1`, `A52=24+16p52`, and `Z52=24+26p52`. Use
    `P=p52,RF=16,RC(P)=8,RE=16,SE=26,EE=27`, so `RH(P)=A52` and
    `SH(P)=Z52`. Let `r52=PR=97` be the exact size of a valid first-use
    credential-attach request with a Some marker and `b52=r52+MR`. Use otherwise-valid
    `N=G=4,R=A52,B=R+MR,M_row=MR,
    C=D=4B,WF=Z52` with exact `connection_slots=p52`. The same PF bound gives
    `A52<=16_777_256`, `Z52<=27_263_026<FRAME_MAX`; the generated fixed MR bound
    also makes `4B<u64::MAX`. In initial
    phase set `(R,WF)` respectively to `(A52-1,Z52)`, `(A52,A52-1)`,
    `(A52,Z52-1)`, and `(A52-1,A52-1)`. In that
    proposal order, the exact outcomes are:

    - `ParticipantRecoveryHandshakeTooLarge { max_entries:p52,
      framing_bytes:u128(24),request_entry_bytes:16,
      response_entry_bytes:26,error_response_bytes:27,
      request_encoded_bytes:A52,response_encoded_bytes:Z52,request_limit:A52-1,
      wire_frame_limit:Z52,dimension:RequestBytes }`;
    - `ParticipantRecoveryHandshakeTooLarge { max_entries:p52,
      framing_bytes:u128(24),request_entry_bytes:16,
      response_entry_bytes:26,error_response_bytes:27,
      request_encoded_bytes:A52,response_encoded_bytes:Z52,request_limit:A52,
      wire_frame_limit:A52-1,dimension:RequestWireFrameBytes }`;
    - `ParticipantRecoveryHandshakeTooLarge { max_entries:p52,
      framing_bytes:u128(24),request_entry_bytes:16,
      response_entry_bytes:26,error_response_bytes:27,
      request_encoded_bytes:A52,response_encoded_bytes:Z52,request_limit:A52,
      wire_frame_limit:Z52-1,dimension:ResponseWireFrameBytes }`; and
    - `ParticipantRecoveryHandshakeTooLarge { max_entries:p52,
      framing_bytes:u128(24),request_entry_bytes:16,
      response_entry_bytes:26,error_response_bytes:27,
      request_encoded_bytes:A52,response_encoded_bytes:Z52,request_limit:A52-1,
      wire_frame_limit:A52-1,dimension:RequestBytes }`.

    In parked phase retain that exact r52/b52 row, so it passes proposed
    `R=A52-1,B=A52+MR` and every aggregate cap. The same four proposals
    return, in order, these exact outcomes:

    - `SdkParkingCapacityIncompatible {
      dimension:RecoveryHandshakeRequestBytes,operands:{ max_entries:p52,
      framing_bytes:u128(24),request_entry_bytes:16,response_entry_bytes:26,
      error_response_bytes:27,request_encoded_bytes:A52,
      response_encoded_bytes:Z52,limit:A52-1 } }`;
    - `SdkParkingCapacityIncompatible {
      dimension:RecoveryHandshakeRequestWireFrameBytes,operands:{ max_entries:p52,
      framing_bytes:u128(24),request_entry_bytes:16,response_entry_bytes:26,
      error_response_bytes:27,request_encoded_bytes:A52,
      response_encoded_bytes:Z52,limit:A52-1 } }`;
    - `SdkParkingCapacityIncompatible {
      dimension:RecoveryHandshakeResponseWireFrameBytes,operands:{ max_entries:p52,
      framing_bytes:u128(24),request_entry_bytes:16,response_entry_bytes:26,
      error_response_bytes:27,request_encoded_bytes:A52,
      response_encoded_bytes:Z52,limit:Z52-1 } }`; and
    - `SdkParkingCapacityIncompatible {
      dimension:RecoveryHandshakeRequestBytes,operands:{ max_entries:p52,
      framing_bytes:u128(24),request_entry_bytes:16,response_entry_bytes:26,
      error_response_bytes:27,request_encoded_bytes:A52,
      response_encoded_bytes:Z52,limit:A52-1 } }`.

    Each preserves the row/interest byte-for-byte and writes no partial request
    or response.

    The other maximum arm is exact but cannot be a later size-refusal trigger:
    at `P=1`, `26<EE=27`, so `SH(1)=51` is error-dominant, while PF already
    includes that complete fixed error frame and therefore `PF>=51`. Any
    `WF<51` proposal selects earlier `WireSchemaBytes`; every shape-valid WF fits
    the error. This explicit dominance proof removes the former invented mutable
    EE fixture. At p52, `26p52>27`, so the reachable response-size refusal above
    independently exercises the success-list product arm.

    Under the one SDK-wide domain race each of admission,
    Reserved/RetryAuthorized→InFlight plus write authorization, response deletion,
    expiry deletion, and authority invalidation against phase latch+validation+commit
    in both orders: a first-row reservation that commits first selects parked-phase
    incompatibility and preserves that row; validation that latches the empty set
    first selects initial-phase handshake-too-large, rejects the proposal, and the
    later row action sees the old valid configuration. For deletion, action-first
    selects initial phase and validation-first selects/preserves the parked
    snapshot until its refusal completes. The analogous state changes are wholly
    before or after the latch. No order sends a row
    or partial request/response under a rejected configuration, resurrects/deletes a
    snapshotted row, or changes the selected phase outcome.
53. For C53/P53, let live credential receipt token U53's canonical request store
    generation7, secret K7, and `accept_marker_delivery_seq:Some(40)`; replay U53
    through every verifier arm. Byte-identical body returns `Bound` when the origin
    epoch still occupies its slot and `UnboundReceipt` when that slot is empty or
    replaced. Wrong secret returns `StaleAuthority`. With K7, changed generation
    alone returns `AttemptTokenBodyConflict { token:U53,
    operation:CredentialAttachRequest,conversation_id:C53,
    presented_participant_id:P53,presented_generation:8,
    presented_marker_delivery_seq:Some(40),conflict:Generation }`; changed
    marker alone returns `AttemptTokenBodyConflict { token:U53,
    operation:CredentialAttachRequest,conversation_id:C53,
    presented_participant_id:P53,presented_generation:7,
    presented_marker_delivery_seq:Some(41),
    conflict:MarkerDeliverySequence }`; changing both returns
    `AttemptTokenBodyConflict { token:U53,
    operation:CredentialAttachRequest,conversation_id:C53,
    presented_participant_id:P53,presented_generation:8,
    presented_marker_delivery_seq:Some(41),conflict:Generation }`. Wrong secret
    plus either or both non-secret changes
    returns `StaleAuthority`, proving secret failure precedes conflict disclosure.
    Every conflict arm uses the complete exact payload and a defined wire field.

    Independently, while P53 remains live at generation7, send a first-use
    `LeaveRequest` with token LL53, presented generation7, and a wrong secret.
    It returns exact `StaleAuthority { authority_state:Live,
    conversation_id:C53,participant_id:P53,presented_generation:7,
    leave_attempt_token:LL53,current_generation:7 }`. A presented generation8
    with any secret selects the same `Live` variant with
    `presented_generation:8,current_generation:7`. On wire those bodies carry
    `originating_request:0x0005` followed by `authority_state:0x0001`; neither
    echoes the secret.

    Expire the live receipt but retain its exact-token provenance row: preserve
    the original `(conversation_id,participant_id,attach_attempt_token)` lookup
    key, alter generation, secret, and marker option, and obtain that row's stored
    `ReceiptExpired`, because no verifier remains. Changing conversation,
    participant, or token instead misses this row and follows that key's ordinary
    lookup taxonomy. After the provenance deadline the same original-key altered
    body returns `StaleOrUnknownReceipt`. For a detach cell, the byte-identical schema returns
    stable `DetachCommitted` (or the stored Pending status). A live generation
    mismatch returns exact `StaleAuthority { authority_state:Live,
    conversation_id:C53,participant_id:P53,capability_generation:8,
    detach_attempt_token:D53,current_generation:7 }`; after attach terminalizes
    D53's old cell, its exact token returns `StaleAuthority {
    authority_state:TerminalizedDetachCell,conversation_id:C53,
    participant_id:P53,capability_generation:7,detach_attempt_token:D53,
    current_generation:8,committed_binding_epoch:e53,
    binding_state:Bound { current_binding_epoch:e53next } }`, with an independent
    empty-slot copy selecting `binding_state:Detached`. On wire these bodies use
    `originating_request:0x0003` followed by `authority_state:0x0001|0x0002`;
    there is no detach secret, marker option, or body-conflict arm to encode.

    For a committed Leave tombstone storing generation8, K8, and token L53, exact
    canonical replay returns the permanent `LeaveCommitted`. With L53 and matching
    K8, changed generation to9 returns
    `AttemptTokenBodyConflict { token:L53,operation:LeaveRequest,
    conversation_id:C53,presented_participant_id:P53,
    presented_generation:9,conflict:Generation }`; wrong secret plus the same
    generation change returns exact `StaleAuthority {
    authority_state:CommittedLeaveTombstone,conversation_id:C53,
    participant_id:P53,presented_generation:9,leave_attempt_token:L53,
    retired_generation:8 }`, encoded after `originating_request:0x0005` with
    `authority_state:0x0002`. A different Leave token returns ordinary `Retired` without
    running the tombstone verifier. That complete Leave conflict payload has no
    marker-option field. Verify fixed verifier charge/lifetime, codec
    normalization, the Generation-before-Marker order, and zero duplicate commit,
    rotation, record, cursor, or receipt mutation in every refusal.
54. First invoke the uniform-Bm fixture convention and construct the complete
    multi-binding ordering arm with `Ce=7,configured_cap=(7,7Bm),I=2,J=2,
    Q=K=(2,2Bm)`. At startup `B=I=2`.
    Enroll P0 at major0/sequence1; it reaches `B=3,debt=0,K_remaining=0` and
    creates no debt episode. Enroll P1 on a distinct connection at
    major1/sequence2; it reaches `B=4,debt=1,K_remaining=2`, creates the episode
    after both identities exist, assigns both base-identity cycles, and leaves
    both churn blocks Dormant with `episode_churn_used=0`. Both members have
    cursor0. P1's producer selects exact
    `ObserverProjection { through_seq:2 }`. Project both Attached rows so
    `H=o=2,F=1`; the floor stays at 1 by
    cursor equality. The keyed ProjectionCompleted occurrence through2 consumes
    its selection to exact `ParticipantCursorProgress { participant_id:P0,
    binding_epoch:e0,through_seq:2,marker_delivery_seq:None }`. Its pre-owned
    strict suffix is the corresponding P1 cursor witness through2, followed by
    the storage suffix selected after both cursors advance. Thus the pre-shutdown
    edge, its participant/epoch/range, and its continuation are not implicit.
    The exact sequence claims occupy 3..12 as
    `[T0,T1,L_times_T(0..3),E0,E1,L_other_times_E0,
    L_other_times_E1]` for `E=2,T=2,L×T=4,L_other×E=2`, while order high1 has
    `[A0,A1,X0,X1]` at 2..5 and `RO=RA=RS=RT=M=0`.
    Both identities are generation1: P0 has epoch e0/secret K54S0 and P1 has
    epoch e1/secret K54S1. Exact first-use requests are
    `LeaveRequest { conversation_id:C54S,participant_id:P0,
    capability_generation:1,attach_secret:K54S0,leave_attempt_token:L54S0 }`
    and `LeaveRequest { conversation_id:C54S,participant_id:P1,
    capability_generation:1,attach_secret:K54S1,leave_attempt_token:L54S1 }`.
    Both token cells are Empty and their permanent verifiers cover exactly those
    canonical bodies.

    The occurrence layout is likewise complete. With
    `O_max=1072,D_cycle=234,O_base=604`, P0's base region is 190..396 and P1's
    is 397..603; both own their exact Pending alternatives because both existed
    at episode creation. Churn blocks0/1 are Dormant at 604..837/838..1071 and
    `episode_churn_used=0`. The exact
    ProjectionCompleted selection just described is Consumed. The two ordered
    CursorProgressed facts and both identities' BindingFateObserved/Leave facts
    are Pending with their displayed sequence/order/occupancy backing; all
    impossible tags are Consumed. Each pending fate lives in its identity's base
    region. Every Pending fact about a currently retained record names
    delivery sequence 1 or 2, its immutable `provenance_tag`,
    `current_sequence_owner`, and participant index. The
    pre-endowed future terminal/marker/Leave record facts separately own their
    logical claim handles, whose current numeric backings are the exact sequence
    claims3..12 and candidate positions displayed below, plus `provenance_tag`
    and `current_sequence_owner`
    and participant indices. Their occurrence statuses are Pending from episode
    creation and never transition from Consumed to Pending; a frontier relay may
    rewrite only the current numeric backing crash-atomically. A causal T/E
    transaction additionally changes `current_sequence_owner` from
    `ConditionalProduct` to `M` as R-C2 requires; neither operation changes
    `provenance_tag`. All use the
    normative coordinate map, so restart has no fixture-local ordinal choice.

    The pre-shutdown state also closes every per-connection fate ordering. It has
    no earlier candidate, so an individual EOF terminal is immediately appendable;
    the pending-terminal-plus-Left two-record core is unreachable in this arm.
    The winning EOF atomically relays its logical T/A handles onto sequence3/
    major2 and compacts every surviving sequence/order handle above them; hence
    P1-first is just as exact as P0-first despite the displayed pre-state labels
    T0/A0 first. Every affected identity-keyed Pending occurrence receives the
    same new backing in that transaction. If both EOFs precede either Leave, the
    first terminal at sequence3 gives
    `B=5,K_remaining=2,debt=2`. The second terminal at sequence4 tentatively
    makes `B=6`; both cursors are still 0, so
    `preferred_floor=base_floor=1,cap_floor=F'=2`; removing Attached1 restores
    B5 and plans the two
    dead-owner markers5/6. Thus it reaches the same
    `H=4,B=5,debt=2,K_remaining=2,OP4`
    retention state as the shutdown batch, with two distinct terminal majors
    instead of one shared major. If the first terminal's K-claim-backed detached Leave wins
    before the other EOF, terminal3/`Left`4 instead give `B=6,K_remaining=1`; the remaining
    terminal5 likewise has `preferred_floor=base_floor=1,cap_floor=F'=2`,
    removes Attached1, plans its marker6, and stays at
    `B=6,K_remaining=1,debt=2`. If marker6 appends first, the last one-record
    K-claim-backed detached Leave at sequence7 transfers its `(1,Bm)` appended charge from
    `K_remaining:1→0`.
    That Leave's path-sensitive non-monotone actual-base search has three exact
    refusal classes and two exact success classes. At `o` in `{2,3,4}`, the raw
    local f3/f4/f5 candidate would expose a later delayed-completion prefix in
    the debt-zero/full-K-illegal gap; the first floor valid across the complete
    stored continuation is f6, which exceeds `o+1`. The token-bearing
    `ObserverBackpressure` therefore mutates nothing. At o=5,
    `preferred_floor=base_floor=cap_floor=F'=6` retains marker6/`Left`7 at `B=3`;
    at the maximum reachable pre-Leave progress o=6,
    `preferred_floor=base_floor=cap_floor=F'=7` compacts marker6 and still has
    B3. Both commits reach debt zero and release full K in the same transaction;
    no `K_remaining=0,debt>0` state or OP7 is exposed. P5 or P6 progress to at
    least5 authorizes the refused request's one retry. An ordinary-Q
    live Leave contributes no K_remaining transfer and only
    reduces this bound.

    The other exact suffix is wire-triggered when the remaining detached owner's
    K-claim-backed detached Leave wins while m6 is its sole earlier candidate. It cancels m6, transfers
    sequence6's already-fired TerminalProduct provenance/current-M owner to
    Left6, moves
    `C:1→0`, transfers exact charge `(1,Bm)` from `K_remaining:1→0`, and has no
    members, so `m=H'=6`. At `o` in `{2,3,4}`, the raw f3/f4/f5 candidate has
    the same unsafe delayed-completion suffix and the first globally valid floor
    f6 exceeds `o+1`; exact token-bearing `ObserverBackpressure` preserves
    `H=5,F=2,B=6,K_remaining=1`. At the maximum
    pre-Leave progress o=5,
    `preferred_floor=base_floor=cap_floor=F'=6` retains only Left6 at B3 and clears by
    `3+2+2=7`. No F3/OP6 positive-debt endpoint exists. Cancellation consumes the marker-specific projection-through6
    occurrence; the pre-endowed remaining E/Leave fact's distinct projection-
    through6 occurrence backs OP6. Thus marker6+`Left`7 and cancelled-marker/`Left`6 both have exact
    crash-visible continuations. Therefore every mix contains at most two durable-terminal
    K-claim-backed detached Leaves, each charged `(1,Bm)`, and cumulative charge
    is at most the initial `K_remaining=K=2`;
    shutdown-before-EOF, EOF-before-shutdown, and both EOF orders are all
    resource-backed.

    Deliver one authoritative `ServerShutdown` event to the participant owner.
    In this exact `I=2,Qe=2` arm, both terminals fit the immediate Q-bounded
    per-conversation batch: its one atomic source transaction consumes lowest
    A-major2, releases A1's old major, and appends direct terminal tuples
    `(2,BindingTerminal,0)` and `(2,BindingTerminal,1)` at sequences3/4. No
    one-terminal or accepted-fate/preappend state is observable. A crash before
    this transaction means ServerShutdown did not linearize; a crash after it
    observes both terminals plus the two marker candidates it creates. The
    fixed-point computation uses
    `preferred_floor=base_floor=1,cap_floor=F'=2`: removing Attached row1 is the
    first absolute-fit envelope, `cursor+1=1<2` strictly overtakes both identities,
    and creates markers at sequences5/6 with tuples
    `(2,CompactionMarker,0/1)`. Terminal sequences3/4 consume T0/T1. The two
    planned markers transfer T0's product claims at sequences5/6 into M0/M1
    with exact immutable provenance tags
    `TerminalProduct { terminal:T0,affected_participant:P0 }` and
    `TerminalProduct { terminal:T0,affected_participant:P1 }`; T1's
    product claims at old values7/8 are unused and release. The immediate
    post-batch snapshot is therefore `H=4,E=2,T=0,M=2,L_other×E=2`, with
    gap-free claims5..10 exactly
    `[M0,M1,E0,E1,L_other_times_E0,L_other_times_E1]`. It retains Attached row2
    plus terminal rows3/4 and has unappended marker candidates5/6,
    `S_actual=3,S=5,C=2,B=5,debt=2,
    K_remaining=2`, and exact edge OP4. The
    tuple order is therefore both atomic-batch terminals in index order, then
    both separately appended markers in index order, with no P0 collision.

    In the same source transaction, the distinct P0-base and P1-base
    BindingFateObserved occurrences consume the old PCP chain, retire its two
    now-impossible cursor events, and select one exact shared-payload
    `ObserverProjection { through_seq:4 }`; each fate owns its own OP selection
    occurrence, so this is not a duplicate same-event tag. The marker candidates
    own Pending MarkerAppended occurrences and exact strict suffixes through5
    and through6. Write Mr for MarkerAppended at r, Pr for
    ProjectionCompleted through r, and `OP_r` for ObserverProjection through r.
    The legal storage schedules are exactly the
    linear extensions of `M5<M6`, `M5<P5`, and `M6<P6`; P4 may complete anywhere.
    A projector may register future OP5/OP6 from a durable candidate, but P5 or
    P6 cannot complete before the named row exists. Cancellation atomically
    marks M6's marker-specific P6 event group and every selection Consumed,
    removes both from this partial order, and retains the distinct pre-endowed
    Leave-record P6 causal key. A late notification carrying the retired marker
    key cannot match or consume that Leave occurrence. The following transition table, applied once per
    event, partitions every such linear extension—including delayed lower
    completions that the former four-row schedule conflated:

    | Event against current strict suffix | Exact transaction result |
    |---|---|
    | Mr with current `OP_s` | Append r and select/preserve OP through max(s,r); M5 may move OP4→OP5 and M6 may move OP4/5→OP6. |
    | Pr with current `OP_s`, `s>r` | Advance `o=max(o,r)` and preserve OP_s; a delayed lower completion never regresses or replaces the edge. |
    | Pr with current `OP_r` | Advance o through r, then select the next durable candidate's OP; if none exists, select the lowest-index surviving undelivered-marker owner's DMR, or None when no such owner/debt remains. |
    | Pr for an actual durable prefix whose episode/key was retired by a higher projection or debt-zero transaction | Fire no episode event. Apply ordinary projection completion: advance `o=max(o,r)`, recompute the actual-base Envelope, and store `F'=max(F,preferred_floor,cap_floor)` with exact removals/credit releases; preserve the current edge—including None—and select no repayment tag. |
    | Pr key invalidated because cancellation prevented its causal marker row from materializing | Exact no-op: cancellation already disarmed the registration; do not advance o, fire an event, select a tag, resurrect an older suffix, or consume a later episode's same ordinal. |
    | K-claim-backed detached Leave appending at l while debt remains against OP or DMR | Release only that identity's anchor, transfer its exact appended charge from K_remaining, and install OP_l; if another owner's DMR was current, OP_l completion restores that surviving DMR. Every delayed P4/P5/P6 uses the preceding preserve row. |

    Each non-retired row consumes the event's own occurrence and one shared tag
    selection; the retired-notification row consumes no episode occurrence.
    durable H, candidate set, retired identities, anchors, and current suffix
    derive the exact payload. Thus `M5<P5<P4<M6`, for example, gives
    OP5→OP6, delayed P4 preserves OP6, and M6 preserves OP6. P4 before M5 can
    select future OP5; after M5, P5 can select future OP6 before M6. There is no
    illegal P5<M5 or P6<M6 arm. No payload permutation allocates a second tag.

    P6 selects `DetachedMarkerRelease { participant_id:P0,
    marker_delivery_seq:5,last_dead_binding_epoch:e0 }` only if OP6 is still
    current and P0 still owns m5. If either exact Leave appends `Left`7 before P6,
    it first installs OP7. Delayed P4/P5/P6 then advances only its own o and
    preserves OP7; it may not resurrect the retired owner's DMR. OP7 selects the
    other surviving owner's DMR (P1/m6 after P0 Left, P0/m5 after P1 Left).
    The final owner cannot retire until that progress is safe, so no branch asks
    OP7 to clear after both owners have already retired. No arm fabricates marker delivery.

    Two and only two candidate-prefix families remain. **Family A:** M5 and M6
    both append before the first Left. This includes every P0-first request,
    because cancelling own m5 would leave the globally earlier m6 tuple before
    X0, and every P1 request after M6. Either exact first-use K-claim-backed
    detached `LeaveRequest` for
    C54S under L54S0/L54S1 then appends
    `Left`7, transfers exact charge `(1,Bm)`
    (`K_remaining:(2,2Bm)→(1,Bm)`), leaves the other cursor at0, uses
    `preferred_floor=1,base_floor=cap_floor=F'=2`, reaches `B=6,debt=2`, and installs OP7 whether or
    not P6 has completed. If OP7 completes first, it sets o=7 and selects the
    remaining DMR; the other exact K-claim-backed detached Leave appends `Left`8,
    transfers `(1,Bm)` from `K_remaining:1→0`, uses
    `m=H'=8` and `preferred_floor=base_floor=cap_floor=F'=8`, removes2..7,
    and leaves `B=3,debt=0,None` with full-K
    equality `3+2+2=7`.

    The same second Leave presented before OP7 must not expose a partial-K
    endpoint. At `o` in `{2,3,4,5,6}`, every lower raw floor has a delayed-
    completion continuation in the debt-zero/full-K-illegal gap; the first
    globally valid floor is f8, which exceeds `o+1`, so the request returns the
    exact token-bearing `ObserverBackpressure`. The two exact o=6 codec arms are
    `ObserverBackpressure { conversation_id:C54S,
    participant_id:P0,capability_generation:1,leave_attempt_token:L54S0,
    backpressure_epoch:6,observer_progress:6,
    prior_terminal_cell_exists:false }` and
    `ObserverBackpressure { conversation_id:C54S,
    participant_id:P1,capability_generation:1,leave_attempt_token:L54S1,
    backpressure_epoch:6,observer_progress:6,
    prior_terminal_cell_exists:false }`. OP7 completion reaches the maximum
    pre-Leave progress o=7 and authorizes one token-stable retry; then
    `preferred_floor=base_floor=cap_floor=F'=8` commits directly at `B=3`.
    No early positive-debt endpoint exists. Every refusal mutates nothing.

    **Family B:** M5 appends, then exact K-claim-backed detached P1 Leave wins while M6 is the
    sole earlier candidate. It cancels its own unwritten m6, atomically transfers
    sequence6's already-fired `TerminalProduct(T0,P1)` provenance/current-M
    owner to Left6,
    consumes X1 after the frontier relay, releases
    that credit, transfers `(1,Bm)` from `K_remaining:2→1`, and with the surviving cursor still 0 uses
    `preferred_floor=1,base_floor=cap_floor=F'=2`, reaching
    `H=6,F=2,B=6,debt=2,OP6`.
    The cancelled marker's ProjectionCompleted-through6 occurrence is consumed;
    the distinct pre-endowed P1 E/Leave record fact owns its own
    ProjectionCompleted-through6 occurrence and backs this OP6, so no occurrence
    is revived or reused.
    A P0 request at this prefix cannot cancel P1's marker and therefore drains
    M6 into Family A. P0's later exact K-claim-backed detached Leave appends
    `Left`7 and transfers `(1,Bm)` from `K_remaining:1→0`.
    At `o` in `{2,3,4,5}`, every lower raw floor has the same unsafe delayed-
    completion suffix and the first globally valid floor7 exceeds `o+1`, so the
    request returns the same exact token-bearing backpressure shape. OP6
    completion advances to o=6 and authorizes one token-stable retry; then
    `preferred_floor=base_floor=cap_floor=F'=7` leaves only `Left`7 at B3 and
    clears by `3+2+2=7`; o>6 before appending `Left`7 is unreachable. No early
    positive-debt endpoint exists. These two families cover a
    request before M5, between M5/M6, after M6, before/after every P4/P5/P6,
    either participant request order, and every crash boundary. Both consume
    the two K-claim-backed detached-Leave charges exactly once, never exceed the
    initial `K_remaining=K=2`, and have a
    named OP completion or progress wake from every positive-debt/refused state.

    Every `B=3,debt=0,edge=None` success above also executes the normative
    episode retirement in that same commit. In the individual marker arm at
    o=5 it retires the still-unneeded marker-P6 and any dominated lower groups;
    in the cancellation arm the cancellation has already consumed marker-P6
    and retirement consumes the distinct still-Pending Leave-P6 group. In
    Families A and B, P7 or P6 respectively has already consumed every lower
    projection group before the final Leave. The retired instance then becomes
    canonical all-Dormant state. A later physical completion for an actual
    durable-prefix old P4, P5, or P6 key is ordinary monotone observer progress,
    fires no episode occurrence, and cannot touch a later instance; the
    cancellation-invalidated marker-P6 key remains the exact no-op already
    specified in the table. Concretely, individual-marker success at
    `o=5,H=7,F=6,C=1,B=3` followed by the actual marker-P6 completion gives
    `o'=6,m=H'=H=7` and
    `preferred_floor=base_floor=cap_floor=F'=7`: it compacts marker6, releases
    `C:1→0`, and leaves `B=3`. Cancellation success at
    `o=5,H=6,F=6,C=0,B=3` makes canceled marker-P6 a no-op, while completion of
    the distinct actual Leave-P6 key gives `o'=6,m=H'=H=6` and
    `preferred_floor=base_floor=cap_floor=F'=7`: it compacts Left6 and changes
    `B:3→2`. Both post-states remain debt-zero/full-K legal.

    Immediately after the batch, the durable snapshot therefore
    contains exactly `H=4` with OP4, `K_remaining=2,debt=2,
    episode_churn_used=0`, both marker
    sequence values/provenance tags/current owners/positions, and every
    OP4→OP5→OP6 selection.
    Appending marker5 consumes its keyed occurrence, advances to `H=5,
    S_actual=4,M=1`, leaves exact claims6..10
    `[M1,E0,E1,L_other_times_E0,L_other_times_E1]`, and selects OP5. Appending
    marker6 consumes the next, advances to `H=6,S_actual=5,M=0`, leaves exact
    claims7..10 `[E0,E1,L_other_times_E0,L_other_times_E1]`, and selects OP6.
    Only this final post-two-marker state has rows2..6 actual and budget
    `H=6,E=2,T=M=0,L_other×E=2`; B/debt/K_remaining remain 5/2/2 through both appends.
    It has `S=5,C=2,B=5,debt=2,K_remaining=2,episode_churn_used=0`, order high2
    with X0/X1 compacted to majors3/4, and exact
    `ObserverProjection { through_seq:6 }`.
    P0 and P1 use their respective base cycles; both churn blocks remain Dormant.
    Crash immediately before/after the atomic terminal batch and before/after
    each marker append; restart reconstructs respectively the exact H2, H4, H5,
    or H6 byte-identical prefix and never exposes only one terminal or reverses
    marker5/marker6.
    Attempt a duplicate complete tuple and classify the candidate state as corrupt
    before overwrite.

    Fault-inject each complete startup-corruption arm into an otherwise valid
    checksum/version snapshot with `Ce=5,I=1,J=2,Qe=2` and therefore
    `O_max=811`; for the last arm its retained/planned record index0 makes ordinal
    1 the ProjectionCompleted event occurrence. Preserve the bytes and fail only
    that conversation before traffic:

    | Injected fault | Exact `ParticipantStateCorrupt.reason` |
    |---|---|
    | two candidates keyed `(16,BindingTerminal,0)` | `DuplicateCandidateKey { transaction_order:16,candidate_phase:BindingTerminal,participant_index:0 }` |
    | in independent otherwise-valid DeliverySeq/TransactionOrder copies, alter only one frontier predicate: with H/high15 leave a movable claim16 below immutable candidate17 (position0); duplicate numeric position16 at position1; skip 17 at position1; duplicate one logical handle at position1; tag position0 with a class whose required total is zero; tear `[T,RS,RT]`/`[A,RO,RA]` at its second position; or store an otherwise gap-free three-handle vector while omitting its required fourth/final handle. Keep every occurrence backing exact for the altered frontier. | For each counter copy, the exact `ClaimFrontierInvalid` reason carries that concrete `counter` enum and respectively `first_bad_position` 0, 1, 1, 1, 0, 1, or 3. If both counter faults are combined, DeliverySeq wins and carries the same concrete numeric position from this mapping. |
    | churn block1 Active while block0 Dormant, with `episode_churn_used=1` matching the one active block | `NonPrefixChurnBlock { first_bad_block:1 }` |
    | occurrence42 carries a key outside its prescribed group | `OccurrenceKeyOutsideGroup { occurrence_ordinal:42 }` |
    | `episode_churn_used=1` with zero activated blocks | `ChurnUsedMismatch { used:1,activated_blocks:0 }` |
    | in eight independent copies, occurrence43 Pending without exactly one of `MarkerSequenceValue`, `RecoveryAttachSequence`, `ReplacementTerminalSequence`, `RecoveryAttachOrder`, `ReplacementTerminalOrder`, `CandidatePosition`, `OccupancyEntries`, or `OccupancyBytes` | The eight exact `UnbackedPendingOccurrence` reasons carry `occurrence_ordinal:43` and, in that same order, `claim_kind:MarkerSequenceValue`, `RecoveryAttachSequence`, `ReplacementTerminalSequence`, `RecoveryAttachOrder`, `ReplacementTerminalOrder`, `CandidatePosition`, `OccupancyEntries`, or `OccupancyBytes`. A copy missing all eight is exact `UnbackedPendingOccurrence { occurrence_ordinal:43,claim_kind:MarkerSequenceValue }`. |
    | ProjectionCompleted event occurrence1 owns two ObserverProjection selection slots (and therefore also violates fixed placement) | `DuplicateSuccessorTag { event_occurrence_ordinal:1,successor_tag:ObserverProjection }` by its earlier reason precedence |

    Each of the first six fault arms changes only the stated predicate. The last
    changes the unavoidable two predicates but proves the fixed selector returns
    `DuplicateSuccessorTag` before `OccurrenceKeyOutsideGroup`; inject a second
    independent corruption into every arm and verify the global reason order.

    At retention startup, `I=1,Qe=2` with entry cap4 returns exact
    `ParticipantRetentionCapacityInvalid { dimension:EntryCapacity,
    required:5,configured:4 }`. With entry cap5, generated Qb/Bm, and configured
    byte cap `2Qb+Bm-1`, startup returns
    `ParticipantRetentionCapacityInvalid { dimension:ByteCapacity,
    required:2Qb+Bm,configured:2Qb+Bm-1 }`.
    Otherwise-valid configurations with raw `J_raw=0` and `J_raw=1` each
    return `ParticipantRetentionCapacityInvalid {
    dimension:EpisodeChurnLimit,configured:J_raw,required_minimum:2,
    required_maximum:u32::MAX }`; raw `J_raw=u32::MAX+1` returns the same
    dimension and bounds. Finally use `Ce=5,I=1,J=2`, generated
    `Q=(2,Qb)`/`marker_max=(1,Bm)`, and byte cap at least `2Qb+Bm`, giving
    `O_max=811`; let `Z=811×W_occ` be the codec's exact serialized array bytes.
    The v1 width bound proves `1<=Z<=u64::MAX`; configure
    `max_participant_state_bytes=Pstate=Z-1`, yielding
    `ParticipantRetentionCapacityInvalid { dimension:SuccessorOccurrenceArray,
    O_max:811,encoded_bytes:Z,limit:Z-1 }`.
    An independent otherwise-identical `Pstate=0` boundary returns the same
    tagged body with `limit:0`; `J=2,Pstate>=Z` proceeds. No participant state or wire traffic
    exists on any refusal, and all four advertised dimensions now have a trigger.

    The multi-marker cycle count needs no second production state. Let U be
    the set of marker causal facts that a later optional transaction would
    introduce but that are absent from every still-active base/churn cycle.
    Each u in U owns a different `(caller_major,CompactionMarker,
    participant_index)` causal tuple, marker sequence claim, candidate
    position, and identity-indexed MarkerAppended/MarkerDelivered/fate
    selection coordinates. If two members of U shared one cycle, consuming
    either fact would force that cycle's one coordinate group both to retain
    and to consume different causal keys; alternatively reviving it for the
    second fact would violate Consumed-never-becomes-Pending. Thus every legal
    serialization needs at least `|U|` new cycles. Assigning one next Dormant
    block to each member is sufficient because its `D_cycle` block contains
    that marker's complete record/event/selection alternatives. Therefore
    the marker contribution to `delta_cycles` is exactly `|U|`: k absent
    marker facts charge k, while an unconsumed marker alternative already
    serialized by the episode creator charges zero. A physically compacted
    marker's consumed coordinates cannot back a later causal fact. The
    single-participant arm below remains the production wire trigger for the
    named limit outcome: after two charged retargets, its third exact request
    has `episode_churn_used=2,delta_cycles=1,J=2` and must refuse.
    Separately let `u=b_u=SR` denote the generated unit and use
    `h=100,r=h-11,Ce=12,I=1,J=2,Q=K=(2,8u)`,
    `marker_max=(1,4u)`, and caps `(12,48u)`. Direct substitution gives
    `O_max=1+27(12)+207(1)+234(2)=1000`,
    `D_cycle=234`, and `O_base=532`.

    The legal public prefix is exact. Before overtake, `H=h-6,F=h-11,o=h-6`,
    P0 cursor is h-12, so `cursor+1=F`. Retained h-11 is an empty-payload
    one-unit ordinary row costing `(1,u)`; h-10 has payload length `10u` and
    costs `(1,11u)`; h-9..h-6 each have payload length `3u` and cost `(1,4u)`.
    Their wire preconditions are the corresponding `R_send>=AR+p` and
    `WF>=AD+p`. Thus `S=(6,28u),C=0,B=(7,32u)` and
    `B+Q+K=(11,48u)`. A `3u`-payload uniform-Bm ordinary record tentatively gives
    B `(8,36u)`. Removing h-11 alone leaves B bytes `35u` and
    `35u+8u+8u=51u>48u`; removing h-11 and h-10 gives B `(6,24u)` and
    `B+Q+K=(10,40u)`. Hence that transaction's
    `preferred_floor=base_floor=h-11`, `cap_floor=F'=h-9`, and
    `cursor+1=h-11<F'` strictly overtakes P0. The causal phase-3 caller record
    appends at h-5 under major h-14, followed by its planned phase-4
    `NonProductM` marker at `m54=h-4` with exact tuple
    `(h-14,CompactionMarker,0)`.
    Ordinary h-3/h-2 then yields eight uniform-Bm retained rows h-9..h-2,
    `C=1,B=(8,32u)`, zero debt, and exact envelope `(12,48u)`. P0 is generation9:
    its eight supersession extras plus this one marker are nine more records than
    majors, so H=h-2 forces order high h-12=r-1. The marker-producing ordinary
    is major h-14 and the last two ordinary callers are h-13/h-12.

    A base exact-Q supersession from generation9 at its prior epoch to generation10/e0 now
    appends terminal h-1 and Attached h at caller major r. Before it, sequence
    high h-2 owns `[T,L×T,E]` at h-1/h/h+1 and order high r-1 owns `[A,X]`
    at r/r+1. The atomic transaction fires T at h-1, moves every surviving/new
    sequence claim to h+1..h+6 in exact `[T,RS,RT,L×T,L×RT,E]` order,
    frees h for Attached, moves/creates `[A,RO,RA,X]` at r+1..r+4, frees r as
    the unreserved caller major, and retargets the still-undelivered marker to
    e0. Old claim ownership is removed before either freed value is appended;
    RS/RO do not fire. The base receipt/provenance expire at their admitted
    deadlines before this snapshot. The result has `F=h-9,o=h`, cursor h-12,
    ten uniform-Bm rows h-9..h, `S_actual=S=B=(10,40u),C=1`,
    `K_remaining=K,debt=Q`, sequence high h, and order high r.

    The current edge is `MarkerDelivery { participant_id:P0,binding_epoch:e0,
    marker_delivery_seq:m54 }` with
    `episode_churn_used=0,episode_churn_limit=2`. Under the normative coordinate
    map, its base-identity groups keep Pending the exact MarkerDelivered→PCP,
    BindingFateObserved→DMR/DCR, FencedRecoveryCommitted→None, and
    LeaveCommitted→None tags plus every record-fact cross-order tag required by
    the witness-preservation rule; MarkerAppended is already Consumed. Every
    other base slot is Consumed. Churn blocks 0/1 are exact Dormant ranges
    532..765 and 766..999. Every tested fate is exact EOF on its named binding:
    e0 for the base arm and e2 after both churn cycles, each recording
    `event_kind:Died,original_cause:ConnectionLost`. On the base recovery branch,
    fate appends terminal
    h+1 and computes `preferred_floor=h-11,base_floor=h-9,
    cap_floor=F'=h-8`; removing h-9 preserves
    `B=(10,40u),K_remaining=(2,8u),debt=(2,8u)`. Recovery then appends only Attached h+2,
    transfers charge `(1,4u)` so `K_remaining:(2,8u)→(1,4u)`, accepts marker m54,
    and its resulting cursor makes
    `preferred_floor=base_floor=cap_floor=F'=h-3`. Removing the then-current h-8..h-4, including m54,
    yields `B=(7,28u),debt=(0,0)`; full-K release is
    `(7,28u)+(2,8u)+(2,8u)=(11,44u)<=cap(12,48u)`, hence the same commit selects
    None and stores `K_remaining=(0,0)`.
    K-claim-backed detached base Leave after that fate appends only Left h+2,
    transfers its exact `(1,4u)` charge `K_remaining:(2,8u)→(1,4u)`, and releases the marker
    anchor. With no members `m=H'=h+2` and `o=h`, it computes
    `preferred_floor=base_floor=cap_floor=F'=h+1`, removes h-8..h, and leaves
    terminal plus Left at `B=(3,12u),debt=(0,0)`; full-K release is
    `(3,12u)+(2,8u)+(2,8u)=(7,28u)<=cap(12,48u)`, so that same commit stores
    `edge=None,K_remaining=(0,0)`. Live base Leave is the distinct ordinary-Q
    arm: with no prior pending terminal it appends only Left h+1, atomically
    discharges T, has `r_K=0` and transfers no charge from `K_remaining`, and
    releases the undelivered anchor.
    With no members `m=H'=h+1` and `o=h`, its
    `preferred_floor=base_floor=cap_floor=F'=h+1` removes h-9..h, including
    m54, and retains only Left h+1. Releasing the marker credit restores the one
    identity reserve, so `S=(1,4u),C=0,B=(2,8u),debt=(0,0)`; full-K release is
    `(2,8u)+(2,8u)+(2,8u)=(6,24u)<=cap(12,48u)`, and the same commit stores
    `edge=None,K_remaining=(0,0)`. Neither arm exposes a debt-zero partial-K
    endpoint.

    Exact attach tokens A54-1/A54-2/A54-3 and their verifiers cover complete
    `CredentialAttachRequest` bodies over C54/P0 with respectively
    generation10/K10/A54-1/None under Σ(0,0) and
    generation11/K11/A54-2/None under Σ(1,1), then
    generation12/K12/A54-3/None under Σ(2,2). Their separately derived target
    slots e1/e2/e3 are empty; the first two successes produce generation11/K11/e1
    and generation12/K12/e2, while A54-3 is used only in refusal copies. For
    k=0,1,2, exact fenced token R54-k covers the complete
    current-generation/current-secret request body with
    `accept_marker_delivery_seq:Some(m54)` under Σ(k,k); its separately derived
    recovery target is empty. Exact Leave token L54-k and its permanent verifier cover that same
    current generation/secret. In each independent first-use arm the named
    A54/R54 attempt-token cell and L54 Leave-token cell are Empty before commit;
    only the explicitly preceding generation's live receipt/provenance occupies
    the Σ(k,k) counts. Thus every active base/block recovery/Leave arm
    has wire authority and exact quota inputs even when another branch is tested.

    Perform two valid credential supersessions e0→e1 and e1→e2 before delivery.
    The first atomically shifts `[A,RO,RA,X]` from r+1..r+4 to r+2..r+5,
    freeing and assigning r+1 as its sole unreserved caller major; A does not
    fire. It fires T at h+1 for the old terminal, shifts every surviving/new
    sequence claim to h+3..h+8 in exact `[T,RS,RT,L×T,L×RT,E]` order, and
    only then appends Attached at freed h+2. In particular RS h+2 moves to h+4,
    RT h+3 to h+5, and E h+6 to h+8; no recovery claim fires. It removes exact
    prefix `[h-9,h-8]`, computes
    `preferred_floor=h-11,base_floor=h-9,cap_floor=F'=h-7`, retires the base
    plan, and activates block0.
    The second similarly shifts the order vector to r+3..r+6 and assigns freed
    caller major r+2 without firing A; it fires T h+3, shifts the sequence vector
    to h+5..h+10, appends Attached at freed h+4, removes `[h-7,h-6]`, computes
    `preferred_floor=h-11,base_floor=h-7,cap_floor=F'=h-5<m54=h-4`, retires
    block0, and activates block1. Each transaction
    keeps `S=B=(10,40u),C=1,K_remaining=(2,8u),debt=(2,8u)`, advances generation/high watermarks,
    and increments churn 0→1→2. Marker/credit are unchanged, but the fixed array
    state and exact claims change as displayed; μ strictly decreases. Crash after
    each and prove restart reconstructs the one active block/edge and fences old
    work. Exact A54-3's third pre-delivery
    e2→e3 attempt returns the common A54-3 envelope plus exact closure suffix
    `scope:EpisodeChurnLimit,marker_capacity_credits:1,marker_anchors:1,
    entry_debt:2,byte_debt:8u,repayment_edge:MarkerDelivery {
    participant_id:P0,binding_epoch:e2,marker_delivery_seq:m54 },
    edge_sequence_claims:6,edge_order_position_claims:4,
    edge_K_remaining:(2,8u),K_headroom:(2,8u),episode_churn_used:2,
    delta_cycles:1,episode_churn_limit:2` before generation/
    sequence/order/floor/history/array mutation and without firing an event
    occurrence. In an independent copy deliver m54 to e2 once, then replay the
    first-use exact A54-3 e2→e3 request: the earlier closure subcheck now returns
    the same unchanged common snapshot with
    `scope:DeliveredMarkerAwaitingAck,delta_cycles:0` and exact
    `repayment_edge=ParticipantCursorProgress { participant_id:P0,
    binding_epoch:e2,through_seq:m54,marker_delivery_seq:Some(m54) }`; every
    generation, counter, history, claim count, and array byte remains identical.
    Then observe e2 fate and complete exact-current K-claim-backed detached Leave through
    DCR. At used=J, fenced recovery is a named `EpisodeChurnLimit` refusal and
    Leave remains the ticket-free legal successor. Fate first appends its
    pre-owned terminal at h+5; the fate transaction's exact
    `preferred_floor=h-11,base_floor=h-5,cap_floor=F'=h-4` removes the unanchored h-5 row, retains marker
    m54 at the new floor, cancels old L×T h+8, relays L×RT h+9→h+8 and E
    h+10→h+9, and keeps `B=(10,40u),K_remaining=(2,8u),debt=(2,8u)` under DCR with gap-free
    surviving claims `[RS h+6,RT h+7,L×RT h+8,E h+9]`. Leave then cancels
    rather than fires RS, atomically relays E h+9→h+6, appends only Left h+6,
    and releases RT h+7 and L×RT h+8. Fate uses
    transferred A r+3; Leave uses X r+6 and releases unused RO/RA r+4/r+5, so no
    candidate tuple lies between. Exact charge `(1,4u)` transfers K_remaining
    from `(2,8u)` to `(1,4u)`.
    With no members `m=H'=h+6`, `o=h`, `preferred_floor=base_floor=h+1`, and the actual-base
    search gives `cap_floor=F'=h+1`; exact removal h-4..h (including the marker)
    leaves h+1..h+6 with `S=(6,24u),C=0,B=(7,28u),debt=(0,0),edge=None,
    K_remaining=(0,0)` and full-K
    release `(7,28u)+(2,8u)+(2,8u)=(11,44u)<=cap(12,48u)`.
    Only delivery, fate, Leave,
    and their suffixes fire event occurrences.
    This exercises repeated retarget, its finite typed bound, and the
    deliver→supersede→redeliver fence; no retry can grow either fixed array or
    retained history past the configured capacity.
55. Invoke the uniform-Bm fixture convention and construct two public positional
    arms with `Ce=16,I=3,Qe=2,Q=K=(2,2Bm)`, and caps `(16,16Bm)`.
    Every harmless ordinary request below has payload `[0x00;3×b_u]`, exact
    charge `(1,Bm)`, `R_send>=AR+3×b_u`, and `WF>=AD+3×b_u`. In the adjacent
    copy enroll P at sequence1/major0 and admit eight such rows at sequences2..9/
    majors1..8. In the intervening copy enroll P/U at sequences1/2 and majors0/1,
    then admit seven such rows at sequences3..9/majors2..8. Acknowledge,
    project, and compact through sequence9. Each copy now has
    `H=9,F=10,o=9`, every existing cursor9, an empty retained log, debt zero,
    edge None, K free, and order high8. Enroll the unaffected marker owner V:
    its caller is major9, Attached is sequence10, its cursor0 is
    already below F, and its exact `NonProductM` candidate owns sequence11 and
    `(9,CompactionMarker,1)` in the adjacent copy or index2 in the intervening
    copy. The enrollment transaction charges the
    planned marker in S and acquires its anchor/credit: actual `Attached`10 plus
    that plan give `S=2,C=1,B=4` and
    `B+Q+K=(8,8Bm)<=(16,16Bm)`, with debt zero, edge None,
    and no active debt-episode occurrences. Receipt and provenance
    rows expire normally before the tested fates; all detach and Leave-token
    cells are Empty.

    In the **adjacent** copy there are exactly P and V. Both are generation1
    at distinct epochs eP/eV with distinct secrets; exact first-use P token L55A verifies
    `LeaveRequest { conversation_id:C55A,participant_id:P,
    capability_generation:1,attach_secret:K55P,leave_attempt_token:L55A }`.
    The post-enrollment sequence claims11..21 are exactly
    `[M_V,T_P,T_V,E_P,E_V,L_times_T(0..3),L_other_times_E(0..1)]`,
    matching `{high_watermark:10,remaining:MAX-10,E:2,T:2,M:1,RS:0,
    RT:0,L_times_T:4,L_times_RT:0,L_other_times_E:2}`. Order high9 owns
    `[A_P,X_P,A_V,X_V]` at majors10..13. Exact EOF on eP records
    `Died(ConnectionLost)` as PendingFinalization at
    `(10,BindingTerminal,0)` because V's earlier marker candidate must
    drain first. L55A arrives before that drain. Once marker11 commits, no
    unrelated tuple lies between P's preserved terminal major10 and X major11,
    so the Q-bounded core atomically appends terminal12 then `Left`13, shifts
    V's still-unmaterialized claims to14..16 as `[T_V,E_V,L_times_T]`,
    retires P, and stores the permanent replay
    `LeaveCommitted { conversation_id:C55A,leave_attempt_token:L55A,
    participant_id:P,presented_generation:1,retired_generation:1,
    ended_binding_epoch:None,prior_terminal_delivery_seq:Some(12),
    left_delivery_seq:13 }`. A crash around the core sees either H11 with the
    pending fate and no tombstone or both records plus the tombstone, never one
    core record.

    In the **intervening** copy enroll unrelated U before the same final V
    enrollment; advance/compact harmless ordinary records so V still uses
    major9 and sequence10. P/U/V have indexes0/1/2, generation1, distinct
    epochs eP/eU/eV and secrets, cursors9/9/0, and the exact sequence positions11..32
    `[M_V,T_P,T_U,T_V,E_P,E_U,E_V,L_times_T(0..8),
    L_other_times_E(0..5)]`. The canonical counts are
    `{high_watermark:10,remaining:MAX-10,E:3,T:3,M:1,RS:0,RT:0,
    L_times_T:9,L_times_RT:0,L_other_times_E:6}`. Order high9 assigns
    `[A_P,A_U,X_P,A_V,X_U,X_V]` to majors10..15; the reserve invariant
    concerns ownership/count, so this explicit order is legal and leaves no
    unowned value. Exact EOFs on P and U create immutable pending tuples
    `(10,BindingTerminal,0)` and `(11,BindingTerminal,1)` behind V's
    `(9,CompactionMarker,2)`. P's exact first-use request is
    `LeaveRequest { conversation_id:C55I,participant_id:P,
    capability_generation:1,attach_secret:K55P-I,
    leave_attempt_token:L55I }`; its distinct permanent verifier exists.

    Candidate order now forces four separate commits: marker11, P terminal12,
    U terminal13, then P's one-record `Left`14 at X-major12. The tuple at major11
    lies strictly between P's terminal and X majors, so composing `{10,12}`
    would skip a live candidate and is forbidden. P's terminal transaction has
    no induced marker in this exact arm; the rule nevertheless drains any
    same-major phase-4 suffix before major11. P's final transaction atomically
    shifts the still-unmaterialized V terminal/remaining claims above H,
    consumes E_P/X_P, retires P, and stores
    `LeaveCommitted { conversation_id:C55I,leave_attempt_token:L55I,
    participant_id:P,presented_generation:1,retired_generation:1,
    ended_binding_epoch:None,prior_terminal_delivery_seq:Some(12),
    left_delivery_seq:14 }`. The exact surviving budget is
    `{high_watermark:14,remaining:MAX-14,E:2,T:1,M:0,RS:0,RT:0,
    L_times_T:2,L_times_RT:0,L_other_times_E:2}`, with positions15..21
    `[T_V,E_U,E_V,L_times_T(0..1),L_other_times_E(0..1)]`; remaining
    order claims13..15 are `[A_V,X_U,X_V]`.

    In both copies the retained marker remains owned by live V,
    `C=1`, and the final baselines are respectively `B=6` and `B=7`;
    their full envelopes are at most `(11,11Bm)<=(16,16Bm)`, debt stays zero,
    and no active debt-episode occurrence plan exists.
    Projecting through the final H, delivering/accepting V's marker, and normal
    cursor/Leave progress give each state a legal continuation. Crash before
    and after every separate drain and replay L55A/L55I: exactly one terminal
    and one Left exist, the optional prior-terminal field names sequence12,
    and no transaction ever groups an unbounded candidate set.

56. Invoke the uniform-Bm fixture convention and test-seed two complete
    PCP-ordering snapshots; every retained/planned record below costs `(1,Bm)`.
    Both arms use this exact later-handle relocation witness for bound P1 Leave.
    Immediately after P1's final supersession caller major r, order high is r and
    `[A_P1,A_P0,X_P1,X_P0]` own r+1..r+4. Leave chooses its already owned
    `X_P1=r+3`, invalidates `A_P1=r+1`, relays surviving `A_P0:r+2→r+4` and
    `X_P0:r+4→r+5`, and stores order high r+3 with gap-free surviving
    `[A_P0,X_P0]` at r+4/r+5. There is no immutable candidate or DCR block, every
    lower handle is enumerated, and the absolute-lowest fallback remains legal.
    For the equality-refusal arm let
    `g=MAX-6`. Construct both identities publicly with P1=index0 at generation1
    and P0=index1 at generation4. At `configured_cap=(7,7Bm)` retain old row g-8 so B=3, then perform
    P1's generation1→2 supersession at major g-11, appending g-7/g-6 and creating exact-Q debt.
    Its generation2→3 supersession appends g-5/g-4; its actual-base search has
    `preferred_floor=base_floor=g-8,cap_floor=F'=g-6`, removes the old row and
    g-7, strictly overtakes P1, and plans/emits marker g-3 under causal
    major g-10. Deliver/accept that marker, project/compact the nonmarkers while
    retaining it at F=g-3, and hold P0 at cursor g-4. Here r=g-10, so bound P1
    Leave uses the witnessed X-major g-7 and appends Left
    g-2. Six supersession/marker record extras plus those two burned majors make
    `H=g-2` imply order high g-7 exactly. Its pre-producer snapshot has
    `H=o=g-2=MAX-8,F=g-3`
    and retains P1's accepted marker g-3 followed by P1's `Left` at g-2. Retired
    identity P1 (index0, retired generation3) stores prior terminal g-5, Left g-2,
    permanent enrollment-token fingerprint mapping `EF56-P1→P1`, leave token
    L56-P1 with its distinct permanent non-reversible Leave verifier, and no
    secret, live binding, detach, receipt, or provenance cell; its retained marker
    owns `C=1`. Live P0 (index1) is bound at e4, generation4/secret K4, with cursor
    g-4, so `cursor+1=F`; `L=E=T=A=X=1` and
    `M=RS=RT=RO=RA=0`. The pre-budget is
    `{ high_watermark:g-2,remaining:8,E:1,T:1,M:0,RS:0,RT:0,
    L_times_T:1,L_times_RT:0,L_other_times_E:0 }`. With
    `Ce=7,I=2,J=2,Q=K=(2,2Bm),cap=(7,7Bm)`, the retained state is
    `S_actual=S=(2,2Bm),C=1,B=(3,3Bm)`, debt zero/None, K free. Order high is g-7,
    remaining `MAX-(g-7)`, and no candidate or successor occurrence exists; both
    P0's detach/receipt/fingerprint/token cells are empty; P1 has exactly the
    tombstone fields just stated and no other durable identity state.
    Exact U56 wire body is `CredentialAttachRequest { conversation_id:C56E,
    participant_id:P0,capability_generation:4,attach_secret:K4,
    attach_attempt_token:U56,accept_marker_delivery_seq:None }` under Σ(0,0),
    and its verifier covers exactly that body. The separately derived e5 target
    slot is empty; success would produce generation5/secret K5/e5.

    The production trigger is exact-Q supersession e4→e5 at major g-6.
    It would append P0's old terminal at g-1 and Attached at g, reserve the one
    RS/RT/RO/RA quartet, and preplan compaction of P1's accepted marker g-3,
    retention of P1's later Left g-2, and P0's `NonProductM` marker at g+1 under
    `(g-6,CompactionMarker,1)`. Its canonical resulting budget is
    `{ high_watermark:g,remaining:6,E:1,T:1,M:1,RS:1,RT:1,
    L_times_T:1,L_times_RT:1,L_other_times_E:0 }`. The six values g+1..MAX
    cannot own those seven displayed positions, so
    the sequence check wins even though every later projection is exact: after
    the two appends plus planned P0 marker, `S_actual=(4,4Bm),S=(5,5Bm),C=2,
    B=(5,5Bm),K_remaining=K,d=Q`, absolute fit is `5+2=7`, and the first edge
    would be `PhysicalCompaction { from_floor:g-3,through_seq:g-3 }`; after that
    removal/credit swap, `S_actual=(3,3Bm),S=(4,4Bm),C=1,B=(5,5Bm)` and the next
    exact range is g-2 alone. Resulting order high would be g-6 with
    `[A,RO,RA,X]` at g-5..g-2.
    `ConversationSequenceExhausted` returns that exact ten-field payload before
    allocating major g-6, appending either record, changing either identity, or
    creating debt, candidates, credits, or occurrences. Every byte of the
    complete pre-snapshot remains unchanged.

    The successful arm uses `h=MAX-7`, `Ce=8,configured_cap=(8,8Bm)`, P1=index0, and P0=index1.
    At zero debt retain old rows h-10/h-9 (B=4). P1's generation1→2 supersession
    at major h-12 appends h-8/h-7 and creates exact-Q debt; generation2→3 at
    major h-11 appends h-6/h-5, computes
    `preferred_floor=base_floor=h-10,cap_floor=F'=h-8`, removes the two old rows,
    and plans/emits marker h-4 under causal major h-11.
    Deliver/accept it and compact the nonmarkers while retaining marker h-4.
    Here r=h-11; bound P1 Leave uses the same witnessed X-major h-8 and appends
    Left h-3. P0 then admits ordinary
    h-2 at major h-7. This clears the earlier debt/full-K state before the
    ordinary admission. Exact-Q supersession e4→e5 at major h-6 appends
    h-1/h and stores
    debt Q/K and a compaction witness for marker h-4. Its completion computes
    `preferred_floor=base_floor=h-4,cap_floor=F'=h-3`, removes h-4, releases
    P1's credit, strictly overtakes P0 at cursor h-5
    (`cursor+1=h-4<F`), planned P0's marker h+1, and atomically selected the next
    exact physical range containing P1's Left h-3 only. This independent arm used empty e5 target slot,
    exact U56-C wire body `CredentialAttachRequest { conversation_id:C56S,
    participant_id:P0,capability_generation:4,attach_secret:K4,
    attach_attempt_token:U56-C,accept_marker_delivery_seq:None }` under Σ(0,0),
    and a verifier over exactly that body. The separately derived e5 target is
    empty; success produces the displayed generation5/secret K5/e5. The complete seed
    after that legal prefix is:

    | Quantity | Exact value |
    |---|---|
    | H/F/o/cursor | `H=h,F=h-3,o=h`; retained P1 Left h-3, ordinary h-2, then supersession h-1/h; P0 cursor h-5, so `cursor+1=h-4<F`; earliest anchor a=h+1 |
    | Marker charge | planned `NonProductM` at sequence h+1/causal major h-6 phase4 index1; `S_actual=(4,4Bm),S=(5,5Bm),C=1,B=(6,6Bm)` because I=2 |
    | Budget/positions | `{ high_watermark:h,remaining:7,E:1,T:1,M:1,RS:1,RT:1,L_times_T:1,L_times_RT:1,L_other_times_E:0 }`; exact ownership is M h+1, then the intact DCR block T h+2/RS h+3/RT h+4, L_times_T h+5, L_times_RT h+6, and E MAX |
    | Member/authority | `L=E=1`; P0 index1 is bound at e5, generation5/secret K5 with detach cell Empty and U56-C live receipt/token verifier plus provenance fingerprint at exact deadlines t0+1000/t0+2000 (post occupancies 1/1); P1 index0 is retired generation3 with prior terminal h-6, compacted marker h-4, Left h-3, permanent enrollment-token fingerprint mapping `EF56-P1-C→P1`, leave token L56-P1-C with its distinct permanent non-reversible Leave verifier, no secret/live cells, and released marker credit |
    | Order/candidates | high h-6, remaining `MAX-(h-6)`, `A=X=RO=RA=1`; exact `[A,RO,RA,X]` own h-5..h-2, marker tuple `(h-6,CompactionMarker,1)` precedes the future fate-terminal major h-5; no other candidate |
    | Capacity/edge | `Ce=8,I=2,J=2,Q=K=(2,2Bm),cap=(8,8Bm)`, debt Q, `K_remaining=K`, `episode_churn_used=0,limit=J`; the first P0 marker belongs to its pre-endowed base-identity cycle; `PhysicalCompaction { from_floor:h-3,through_seq:h-3 }` |
    | Occurrences | `O_max=1099,O_base=631`; normative base-coordinate groups keep PC→marker work, fate→preserved PC/DMR/DCR, marker append/delivery, ack, live/detached Leave, recovery, and every cross-order selection Pending; all impossible base tags Consumed; churn blocks 0/1 Dormant at 631..864/865..1098 |

    Exact fenced V56-C wire body is `CredentialAttachRequest {
    conversation_id:C56S,participant_id:P0,capability_generation:5,
    attach_secret:K5,attach_attempt_token:V56-C,
    accept_marker_delivery_seq:Some(h+1) }` under Σ(1,1), and its verifier covers
    exactly that body. The separately derived e6 target is empty; success produces
    generation6/secret K6/e6.
    Exact Leave token L56-P0-E5 and its permanent verifier cover canonical
    current body `(P0,generation5,K5)` for the tested base live/detached Leave
    arms; this debt-ordering fixture does not instantiate a post-recovery e6
    Leave token. Before each tested first use, V56-C's attempt-token cell and
    L56-P0-E5's Leave-token cell are Empty; P0's
    detach cell is Empty. U56-C alone owns the stated live receipt/provenance.

    Every tested P0 fate is exact EOF on binding epoch e5 and records
    `event_kind:Died,original_cause:ConnectionLost`. PC-first has exact
    `preferred_floor=h-4,base_floor=h-3,cap_floor=F'=h-2`; it removes P1's Left h-3, retains the ordinary
    h-2 and both
    supersession records h-1/h, preserves the planned marker charge/credit, then
    appends marker h+1 before any later terminal.
    If fate arrives first, the tentative terminal gives `B=(7,7Bm)` and absolute
    fit fails because `B+K_remaining=(9,9Bm)>cap=(8,8Bm)`; it persists the exact major h-5
    `PendingFinalization` while its keyed selection retains the storage range. PC then
    completes, marker h+1 appends, and the already ordered terminal drains at
    h+2. If PC completes first, the marker likewise appends at h+1;
    fate-before-delivery appends terminal h+2. Thus both tested arrival orders
    have one fixed assignment and one log order—never the unreserved reverse
    permutation.

    In those two arms the named epoch did not receive the marker, so fate selects
    DMR, never DCR; marker delivery before fate is the separately backed DCR
    alternative. After marker and terminal append the canonical budget is
    `{ high_watermark:h+2,remaining:5,E:1,T:0,M:0,RS:0,RT:0,
    L_times_T:0,L_times_RT:0,L_other_times_E:0 }`: required reserve 1 and four
    free values. Capacity is `S_actual=S=(5,5Bm),C=1,B=(6,6Bm),d=Q,
    K_remaining=K` under DMR.

    Exact K-claim-backed detached Leave appends only Left h+3: DMR fate has already released
    RS/RT and both product claims and relayed E from MAX to the free next value
    h+3, so Leave consumes that exact E claim and transfers charge `(1,Bm)`
    (`K_remaining:(2,2Bm)→(1,Bm)`). With
    no members `m=H'=h+3`,
    `preferred_floor=base_floor=cap_floor=F'=h+1`; exact removal h-2..h leaves
    marker, terminal, and Left at B=4. The transfer computes transient
    K_remaining=1, but debt is zero and full-K equality `4+2+2=8` holds, so the
    same commit stores `edge=None,K_remaining=0`. In the delivered-marker DCR
    alternative, fate consumes T h+2, cancels old L×T h+5, relays L×RT
    h+6→h+5 and E MAX→h+6, and leaves the gap-free edge block `[RS h+3,RT
    h+4,L×RT h+5,E h+6]`. Delivery-win V56-C consumes RS h+3 for Attached,
    transfers the same exact `(1,Bm)` charge
    (`K_remaining:(2,2Bm)→(1,Bm)`), accepts the marker, and reaches the same
    `preferred_floor=base_floor=cap_floor=F'=h+1,B=4` full-K state. It transfers
    RT h+4 in place to new T h+4, L×RT h+5 in place to new L×T h+5, preserves E
    h+6, and atomically stores None/K_remaining=0; recovery
    never exposes a debtful block0 binding. The other DCR participant successor
    is exact K-claim-backed detached L56-P0-E5 after marker delivery then e5 fate. It
    cancels RS h+3, relays E h+6→h+3, appends only Left h+3, transfers its exact
    charge `(1,Bm)` (`K_remaining:(2,2Bm)→(1,Bm)`), and releases RT, every product, and the accepted
    marker anchor. With no members `m=H'=h+3`, it computes
    `preferred_floor=base_floor=cap_floor=F'=h+1`, reaches `B=4,debt=0`, proves
    full-K equality `4+2+2=8`, and in that same distinct Leave occurrence stores
    `edge=None,K_remaining=0`. Thus both reserved DCR completion orderings are
    executable and neither leaves a partial-K debt-zero state.

    The shared live-bound Leave fact uses the ordinary-Q alternative. Before PC it atomically
    cancels the unwritten marker, transfers h+1's current M sequence owner to
    one-record Left,
    has no members (`m=H'=h+1`, o=h), uses
    `preferred_floor=base_floor=cap_floor=F'=h+1`, removes h-3..h, and reaches
    `S=1,C=0,B=3`, debt zero/full-K fit `3+2+2=7<=8`, storing
    `edge=None,K_remaining=0`. After PC but before marker append it
    makes the same transfer and removes h-2..h. After marker
    append it appends Left h+2 (`m=H'=h+2`, o=h), uses
    `preferred_floor=base_floor=cap_floor=F'=h+1`, removes h-2..h and retains
    marker h+1 plus Left h+2 at `B=3`, and consumes the same slot. Each of these
    mutually exclusive result vectors is selected by pre-owned fact indices,
    has `r_K=0` and transfers no record charge from `K_remaining`, proves the same full-K fit, and stores
    `edge=None,K_remaining=0`; only the detached/recovery alternatives transfer
    exact charge from `K_remaining` before their own same-commit zeroing.
    Every from_floor, cap_floor, event/selection occurrence, release, and restart
    codec byte is asserted in both tested arrival orders.

A reviewer refutes R-C0–R-C5 by finding an exactly-once application-effect
claim, changed delivery key, ghost enrollment, `Bound` on an unbound replacement
connection, secret returned after receipt expiry/Leave, stale-generation
overwrite, unauthorized or duplicate-record Leave, cursor mutation before
current binding proof, compaction ack without marker delivery, abandoned payload
called delivered, physical-floor transition that differs from the formula, or
compaction above `observer_progress`. A legitimate non-recipient gap cannot
refute cumulative ack because targeted v1 delivery does not exist.

## 5. Section (d) — participant envelope and structural wire evolution

### Proposed contract

**R-D1 — Exhaustive participant requests and outcomes.** Add versioned request
frames for enrollment, credential attach/receipt replay, explicit detach,
cumulative ack, Leave, marker ack, ordinary record admission, and non-mutating
observer-readiness status. Every decoded conversation-scoped **semantic** wire
outcome carries its named discriminant and, by default, the triggering
operation's complete **common request envelope** below. Table rows then name only
response-specific suffix fields. The envelope is these exact flat request
fields, with no renaming: enrollment
uses `conversation_id,enrollment_token`; credential attach uses
`conversation_id,participant_id,capability_generation,attach_attempt_token,
accept_marker_delivery_seq`; detach uses
`conversation_id,participant_id,capability_generation,detach_attempt_token`;
Leave uses `conversation_id,participant_id,capability_generation,
leave_attempt_token`; normal ack uses
`conversation_id,participant_id,capability_generation,through_seq`; marker ack
uses `conversation_id,participant_id,capability_generation,marker_delivery_seq`;
and ordinary admission uses
`conversation_id,participant_id,capability_generation` but never echoes its
opaque payload. A request's presented `attach_secret` is never echoed by the
common envelope, and a Leave secret is never returned. Complete live-receipt
success/replay schemas may instead carry their newly minted or durably stored
**result** `attach_secret`. Only a row that explicitly calls its listed fields a
**complete exact schema** or **complete exact replacement schema**, or cites R-C0's
complete receipt/verifier schema or R-C3's complete marker-proof schema, replaces
the default envelope. Those exact exceptions deliberately name response fields
`token`, `presented_generation`, `presented_marker_delivery_seq`, or
`requested_marker_delivery_seq` as stated there. Thus a generic row's short
field list is always a suffix, and no outcome leaves a choice between
`capability_generation` and `presented_generation`.
R-D2 `ParticipantTransportRejected` is explicitly presemantic and carries only
its reason-specific transport fields: FrameTooLarge cannot recover a body
conversation/token/id before allocation. The observer-recovery request and its
success/list errors are connection-scoped batch outcomes and carry only their
exact list/status fields; individual status entries carry their conversation
ids. SDK-local, startup, accepted-socket, and internal-recovery rows likewise
carry exactly the fields their table rows name; no nonexistent conversation
sentinel is added. R-C0 defines the four exact tokenized schemas, R-C3 defines the two ack and
ordinary-admission schemas, and R-A4 defines the exact observer-recovery list.
Receipt replay is byte-identical retransmission, not a ninth request shape.

Every incoming participant frame first applies R-D2 stage 1. After successful
decode/version/authentication/capability, an `ObserverRecoveryHandshake` uses
its exhaustive special order: TooManyEntries, DuplicateConversation,
request-index connection-capacity preflight, request-index ConversationUnknown/
EpochAhead, then `ObserverRecoveryAccepted`. Only other decoded conversation-
scoped requests continue through generic stages 2–13 below. The first applicable
stage selects the sole outcome; the table is an inventory, not a second
precedence order:

1. R-D2 `ParticipantTransportRejected`, in its bounded structural order of
   full-frame size, version-independent Framing, structural participant version,
   the remaining four body-decode classes, authentication, then capability;
2. R-C0's exact committed-Leave exception, token-to-identity tombstone
   precedence, then live phase-specific token/verifier result;
3. different-token Pending detach (`DetachInProgress`);
4. presented-id tombstone, missing identity, then generation/secret authority;
5. required current binding (`NoBinding`);
6. connection-conversation capacity and attach binding-slot occupancy;
7. operation proof: ack continuity, explicit marker proof, or `RecoveryFence`;
8. runtime identity/receipt capacity in R-C0's seven-scope order, then static
   `RecordTooLarge` Entries before Bytes;
9. full resulting `ConversationOrderExhausted` check;
10. canonical resulting `ConversationSequenceExhausted` check;
11. `ObserverBackpressure`;
12. remaining closure subchecks in order `DeliveredMarkerAwaitingAck`,
    `EpisodeChurnLimit`, then componentwise `Capacity`; and
13. success.

Simulation may calculate later predicates to construct a fixed point, but may
not select or disclose a later outcome. Unknown, retired, stale, or unbound
authority therefore wins even when a slot is occupied and order, sequence,
observer, and closure limits also fail. The cross-cutting and operation rows
together are exhaustive; no generic “proof/admission refusal” exists.

| Operation | Named outcome discriminant | Required fields | Retryability and required SDK transition |
|---|---|---|---|
| Pre-semantic participant transport gate | `ParticipantTransportRejected` | `reason` plus exactly its R-D2 fields: `FrameTooLarge { complete_frame_bytes, max_frame_bytes }`, `DecodeFailed { decode_class }`, `UnsupportedVersion { presented_version, supported_version }`, `AuthenticationFailed {}`, or `ParticipantCapabilityRequired { required_capability }` | Respond when framing permits, then close. The gate creates no identity, receipt, SDK row, cursor, or semantic-request mutation. When the **server receiver** closes an unbound connection it creates no participant/conversation state; when that receiver closes an active binding, its local refusal is exactly R-A2's terminating protocol event and records one counter-claim-backed `Died(ProtocolError)` (or its bounded PendingFinalization) for that epoch. An SDK-side refusal creates no server fact by assertion; the server classifies only the actual connection event it later receives. These five and only these five R-D2 triggers select this outcome. |
| SDK-local participant encoding | `SdkParticipantRequestTooLarge` | conversation id, encoded request bytes, exact signed effective request-byte limit `R_send=min(R,WF)` | Local terminal refusal for this call; reserve no row, send no frame, create no server state. |
| SDK-local observer-wait admission | `SdkObserverParkCapacityExceeded` | one of exactly five valid scope/dimension pairs: `PerConversation/Rows`, `PerConversation/Bytes`, `SdkWide/Conversations`, `SdkWide/Rows`, or `SdkWide/Bytes`; conversation id, signed limit, occupied, requested full amount | Local terminal refusal; atomically roll back empty first-interest reservation, reserve no row, send no frame, create no server state. `PerConversation/Conversations` is not constructible. |
| SDK-local park-order allocation | `SdkParkOrderExhausted` | conversation id, `counter: ParkOrder`, `value: u64::MAX` | Terminal while set nonempty; reserve/send nothing, preserve ordered rows; only empty-set transaction may reset. |
| SDK restart/renegotiation with parked rows present | `SdkParkingCapacityIncompatible` | offending dimension (`NonzeroLimit`, `RecoveryEntrySchemaBytes`, `WireSchemaBytes`, `RequestSchemaBytes`, `RowSchemaBytes`, `CheckedProduct`, `RowBytesBound`, `SdkBytesBound`, `RecoverableSlots`, `ConversationRows`, `ConversationBytes`, `SdkConversations`, `SdkRows`, `SdkBytes`, `RequestBytes`, `RecoveryHandshakeRequestBytes`, `RecoveryHandshakeRequestWireFrameBytes`, or `RecoveryHandshakeResponseWireFrameBytes`) plus exactly its R-A4 dimension-tagged `operands` variant; no optional operand bag and no separate `scope` | Parked-phase only; preserve every row and interest; send no row and no partial request or response; require correction. The exact variant carries the offending field/factors/product, conversation/park order, actual/limit, or complete u128 recovery-size operands as applicable. |
| Startup/first negotiation configuration shape | `ParticipantParkingConfigurationInvalid` | dimension (`NonzeroLimit`, `RecoveryEntrySchemaBytes`, `WireSchemaBytes`, `RequestSchemaBytes`, `RowSchemaBytes`, `CheckedProduct`, `RowBytesBound`, `SdkBytesBound`, or `RecoverableSlots`) plus exactly its R-A4 dimension-tagged `operands` variant | Initial-phase only; configuration-shape precedence before RH size; refuse before traffic, rows, interests, or handshake bytes. No optional operand bag exists. |
| Startup/first negotiation, or restart/renegotiation with no parked row | `ParticipantRecoveryHandshakeTooLarge` | max entries P, u128 framing bytes `u128(RF)+u128(RC(P))`, request/status-entry/error-response bytes RE/SE/EE, exact u128 `RH(P)`/`SH(P)`, R, WF, dimension (`RequestBytes`, `RequestWireFrameBytes`, or `ResponseWireFrameBytes`) | Initial-phase only; refuse participant mode/configuration before traffic; no rows are created/discarded and no chunk protocol runs. |
| Participant receipt/identity configuration | `ParticipantCapabilityConfigurationInvalid` | one exact flat dimension-tagged schema: `dimension:NonzeroLimit` plus `field,actual:0,required_minimum:1`, or `dimension:ReceiptDeadlineOrder` plus `attach_receipt_ttl_ms,receipt_provenance_ttl_ms,required_minimum_provenance_ttl_ms`; neither arm nests a reason/operands object | Refuse participant mode before traffic; no conversation state exists. Nonzero fields and TTL ordering use R-C0's exact precedence. |
| Keepalive startup/accepted-socket certification | `KeepaliveCertificationFailed` | exact R-B2 `phase` plus one of its eight reason-tagged bodies; no optional field bag | Startup opens no participant listener; accepted-socket failure closes before negotiation and creates no participant/lifecycle state. R-B2 defines first-failure order and exact phase/field variant. |
| Participant retention startup validation | `ParticipantRetentionCapacityInvalid` | one exact tagged body selected in order: `EntryCapacity { required,configured }`, `ByteCapacity { required,configured }`, `EpisodeChurnLimit { configured,required_minimum,required_maximum }`, or `SuccessorOccurrenceArray { O_max,encoded_bytes,limit }`; no optional operand bag | Refuse participant mode before traffic; no conversation state exists; use R-C4's fixed validation order. R-C4's tighter bounds prove every formula suboperation total, so no arithmetic discriminant or trigger exists. |
| Participant connection-incarnation mint | `ConnectionIncarnationExhausted` | exact `component: ServerIncarnation\|ConnectionOrdinal`, `current_value`, and `attempted_server_incarnation: Option<u64>`; server-counter exhaustion requires `MAX/None`, while ordinal exhaustion requires `MAX/Some(current server incarnation)` | Refuse new participant connections/startup before first participant use; preserve all durable identities and never wrap/reuse. |
| Server startup binding recovery | `BindingRecoveryCommitted` | participant/conversation, recovered binding epoch, `UncleanServerRestart` cause/prior server incarnation, assigned major, finalization (`Appended { delivery_seq }` or `Pending { admission_order }`), repayment edge | Internal durable success; exact restart replay is idempotent, receipt status is UnboundReceipt, and no wire retry/poll is created. |
| Conversation participant-state candidate validation or startup decode | `ParticipantStateCorrupt` | conversation id and exactly one of `DuplicateCandidateKey { transaction_order,candidate_phase,participant_index }`, `ClaimFrontierInvalid { counter:DeliverySeq\|TransactionOrder,first_bad_position }`, `NonPrefixChurnBlock { first_bad_block }`, `ChurnUsedMismatch { used,activated_blocks }`, `DuplicateSuccessorTag { event_occurrence_ordinal,successor_tag }`, `OccurrenceKeyOutsideGroup { occurrence_ordinal }`, or `UnbackedPendingOccurrence { occurrence_ordinal,claim_kind }` | Use R-C4's exact first-corruption order. Abort a candidate commit before overwrite or fail startup before traffic; fail the conversation closed, preserve durable bytes, and create no wire retry/poll. Case 54 supplies one exact fault-injection trigger for every reason; its duplicate-tag arm also violates placement but selects DuplicateSuccessorTag by precedence. |
| Live credential-attach receipt or exact committed Leave tombstone token with verified secret but changed non-secret body | `AttemptTokenBodyConflict` | complete exact R-C0 replacement schema: required `token`, `operation:CredentialAttachRequest\|LeaveRequest`, `conversation_id`, `presented_participant_id`, `presented_generation`, and `conflict:Generation\|MarkerDeliverySequence`; `presented_marker_delivery_seq: Option<DeliverySeq>` is required only in the credential-attach variant and absent in the Leave variant | Secret failure is earlier `StaleAuthority`; with a matching secret, Generation is tested before MarkerDeliverySequence. Leave can select only Generation. Enrollment has no same-key non-key field; detach generation mismatch is `StaleAuthority`; provenance has no verifier and returns its phase result only after the original conversation/participant/token lookup key resolves that row. Refusal discloses no stored body, secret, receipt, current slot, or `LeaveCommitted`, and mutates nothing. |
| SDK-local untokenized response loss | `RecordAdmissionUnknown` | exact `RecordAdmissionUnknown { conversation_id, participant_id, capability_generation, operation:OrdinaryRecordAdmission, park_order }` | Atomically delete the row/release last interest when applicable; terminal ambiguity and never resend automatically. |
| First decoded semantic operation for an untracked conversation on this connection | `ConnectionConversationCapacityExceeded` | triggering operation's exact common request envelope plus signed connection-conversation `limit` | No participant mutation, interest arming, or readiness promise; use a connection with a free negotiated slot. |
| Observer-recovery batch preflight for an untracked conversation | `ConnectionConversationCapacityExceeded` | complete connection-scoped schema `ConnectionConversationCapacityExceeded { conversation_id,limit }`; no semantic-operation envelope | The batch simulates unique entries in request-index order and names the first entry that would exceed, with no partial slot or arm. |
| Enrollment or credential attach binding attempt | `ConnectionConversationBindingOccupied` | complete exact replacement schema from R-C1 with two flat variants: enrollment carries `conversation_id,enrollment_token,presented_participant_id:None`; credential attach carries `conversation_id,participant_id,capability_generation,attach_attempt_token,accept_marker_delivery_seq,presented_participant_id:Some(participant_id)`; neither carries occupying identity fields | Terminal for this attempt; no mint/receipt/rotation/replay/binding/record; same-participant rotation remains eligible. |
| Enrollment, credential attach, or ordinary admission requiring an unreserved `transaction_order` major | `ConversationOrderExhausted` | exact common request envelope plus R-A2's exhaustion fields: counter, high, optional next value, order remaining, `reserved_claims=A+X+RO+RA`, `required_majors:1`, resulting remaining, and resulting four-term claims; no redundant scope field | Terminal for this attempt with no wrap/rebase/alias or mutation. The request discriminant identifies the producer. Accepted terminal/exit obligations consume reserved claims; all marker candidates share their causal major, and already ordered candidates drain without another major. |
| Credential attach, participant-id receipt/status replay, detach, Leave, normal ack, marker ack, or ordinary admission | `ParticipantUnknown` | exact common request envelope; no suffix | Terminal semantic outcome for this attempt; connection remains open and no participant/durable state is created. Every operation in this row carries `participant_id`; enrollment-token replay instead follows the lifetime enrollment mapping and cannot select this row. |
| New detach with **no Pending cell**, Leave while a different live binding epoch exists, normal ack, marker ack, or ordinary admission after exact-token lookup misses | `NoBinding` | exact common request envelope; no suffix | Terminal; no other binding disclosed and no state change. Detached exact-credential Leave is eligible; any Pending detach cell is classified at step 0d first. |
| Any listed participant-id operation with a live generation mismatch, any live secret-bearing operation with equal generation and wrong secret, or the exact committed Leave token with wrong secret | `StaleAuthority` | By default, exact common request envelope plus `current_generation` for a live identity. `originating_request:0x0003` instead uses the Detach row's complete tagged replacement union below. For `originating_request:0x0005`, a complete exact replacement tagged union begins with `authority_state:u16`, assigned `0x0001 Live` and `0x0002 CommittedLeaveTombstone`: `Live { conversation_id,participant_id,presented_generation,leave_attempt_token,current_generation }` or `CommittedLeaveTombstone { conversation_id,participant_id,presented_generation,leave_attempt_token,retired_generation }`. No secret-derived detail or common-envelope duplication exists. | Terminal under invalid authority. Live Leave generation/secret failure selects `Live`; the permanent exact-token wrong-secret exception selects `CommittedLeaveTombstone` and exposes only the already-public retired generation. Detach selects its separately tagged live/old-cell state; every other origin is structurally fixed by its common envelope. Each tag is the defined wire trigger that makes same-origin layouts decidable. No state changes. |
| Any listed participant-id operation whose presented id has a tombstone | operation-specific `Retired` | exact common request envelope plus `retired_generation` | Terminal tombstone result; no secret, binding, live cursor, or state change. |
| Any closure-checked admission | `MarkerClosureCapacityExceeded` | triggering operation's exact common request envelope plus R-C4's unchanged-prestate suffix `scope`, `marker_capacity_credits`, `marker_anchors`, `entry_debt`, `byte_debt`, `repayment_edge`, `edge_sequence_claims`, `edge_order_position_claims`, exact entry/byte `edge_K_remaining`, exact `K_headroom=cap-B`, `episode_churn_used`, `delta_cycles`, and `episode_churn_limit`; `scope` is exactly `Capacity`, `RecoveryFence`, `DeliveredMarkerAwaitingAck`, or `EpisodeChurnLimit`, and only `Capacity` adds `dimension:Entries\|Bytes`, simulated maximum `required`, and configured `limit` | No floor, membership, credit, participant, candidate, sequence, edge, receipt/provenance, or debt mutation; terminal for every unaccepted request, with no SDK park or automatic retry. `RecoveryFence` is triggered exactly when otherwise-valid credential attach omits the fenced marker required by DCR, targets DMR/DCursor's declared Leave-only edge without a usable delivered marker, or would allocate a second RS/RT/RO/RA quartet instead of transferring the episode's still-unused quartet; an explicit bad marker instead takes `MarkerNotDelivered`/`MarkerMismatch`. It precedes the other closure scopes and is independent of scalar capacity. `DeliveredMarkerAwaitingAck` fences re-endowment of a consumed exact-epoch delivery occurrence and precedes the budget test. Otherwise `EpisodeChurnLimit` is triggered iff a plan-changing optional lifecycle/marker transaction has `u128(episode_churn_used)+u128(delta_cycles)>u128(J)`; R-C4 proves the sum cannot overflow. Capacity tests Entries before Bytes, and equality passes. Already accepted server candidates, unrefusable fates, pre-owned ordinary-Q Leaves, K-claim-backed detached Leaves, and claimed terminal paths remain event-driven. Marker acceptance itself never has this outcome. |
| Enrollment attach | `EnrollBound` | complete exact replacement schema: `conversation_id,token,participant_id,request_generation:None,capability_generation:1,attach_secret,origin_binding_epoch,persisted_cursor:0,accepted_marker_delivery_seq:None,receipt_expires_at,provenance_expires_at`; `token` echoes the enrollment token | Success; persist the complete canonical live-receipt result before exposing `EnrollBound` and binding authority; replay same token only after unknown response. |
| Enrollment attach | `EnrollmentKnown` | complete exact replacement schema: `conversation_id,token,participant_id,current_generation`; `token` echoes the enrollment token | Post-provenance result only for a live non-retired mapped identity; no secret/binding; use a valid current-or-newer credential or enter `CredentialRecoveryLost`; never remint. A mapped tombstone is `Retired`. |
| Enrollment attach | `ReceiptExpired` | exact R-C0 provenance response with echoed token/participant, `presented_generation:None`, no marker-option field, result/current generations, and `Deadline\|Superseded` reason | Exact only inside the enrollment provenance window; use a valid current-or-newer credential or enter `CredentialRecoveryLost`. |
| Enrollment attach | `Retired` | exact common request envelope plus `participant_id,retired_generation` | Terminal; preserve identity for operator record, no attach. |
| Enrollment attach | `ReceiptCapacityExceeded` | exact common request envelope plus reachable `LiveReceiptServer \| ProvenanceServer \| ProvenanceConversation` scope, limit, occupied, requested 1 | No SDK auto-retry; the first full fixed-order set wins. New-identity per-participant occupancies are zero under nonzero limits and therefore have no unreachable refusal arm. |
| Enrollment attach | `IdentityCapacityExceeded` | exact common request envelope plus `Server \| Conversation` scope, limit, occupied, requested 1 | Terminal for new enrollment; server scope precedes conversation and no participant was minted. |
| Enrollment attach | `ObserverBackpressure` | exact common request envelope plus `backpressure_epoch,observer_progress` | Persist `AwaitingObserverProgress`; retry once after matching `ObserverProgressed` or reconnect status. |
| Enrollment attach | `ConversationSequenceExhausted` | common request envelope plus exactly one nested canonical ten-field `sequence_budget` and no exhaustion-specific field | Terminal for this mint; reserve check includes the flat `Left`, binding terminal, and cursor-0 marker fixed point before identity or sequence mutation. |
| Credential attach | `AttachBound` | complete exact replacement schema: `conversation_id,token,participant_id,request_generation:Some(presented generation),capability_generation,attach_secret,origin_binding_epoch,persisted_cursor,accepted_marker_delivery_seq:Option<DeliverySeq>,receipt_expires_at,provenance_expires_at`; `token` echoes the attach token and result generation is distinct/new | Success; generation-ordered persist of the complete canonical live-receipt result, then expose binding/replay. The marker Option is `Some` only after atomic fenced recovery; no ack parking state exists. |
| Credential attach | `ReceiptExpired` / `StaleOrUnknownReceipt` / `Retired` | `ReceiptExpired` and `StaleOrUnknownReceipt` use the exact R-C0 flattened schemas with presented generation and presented marker Option; `Retired` uses its exact generic row | Never retry same request; preserve newer credential or enter the specified terminal state. |
| Credential attach | `StaleAuthority` | exact common request envelope plus `current_generation` | Terminal for this request; preserve current durable credential. |
| Credential attach with `accept_marker_delivery_seq` | `MarkerNotDelivered` / `MarkerMismatch` | exactly R-C3's flattened marker-proof schema: `conversation_id,token,participant_id,capability_generation,requested_marker_delivery_seq` plus the selected reason's required `current_cursor` or `expected_marker_delivery_seq`; no common-envelope duplication or optional marker-state field bag | Terminal for this attach attempt; no marker accept, cursor, floor, rotation, record, or binding mutation. |
| Credential attach | `ReceiptCapacityExceeded` / `ObserverBackpressure` / `ConversationSequenceExhausted` / `MarkerClosureCapacityExceeded` | exact common request envelope plus respectively the capacity suffix, `backpressure_epoch,observer_progress`, exactly one nested canonical ten-field `sequence_budget`, or R-C4's exact closure suffix | Capacity is surfaced; backpressure enters a shared epoch; reserve exhaustion precedes rotation, binding, floor, and marker changes. Detached attach may consume closure Q but never sequence/order claims it did not reserve; live supersession cannot bypass a recovery fence, re-endow a delivered-but-unaccepted marker occurrence, or exceed J. |
| Receipt replay | `Bound` / `UnboundReceipt` | complete exact replacement schema: `conversation_id,token,participant_id,request_generation:Option<u64>,capability_generation,attach_secret,origin_binding_epoch,persisted_cursor,accepted_marker_delivery_seq:Option<DeliverySeq>,receipt_expires_at,provenance_expires_at` | `Bound` only if the exact participant/epoch still occupies its origin slot; every empty/replaced/later-epoch slot, including on the same connection, is `UnboundReceipt`, then persist-and-fresh-attach. Enrollment uses request-generation `None`; credential replay uses `Some(the originally presented generation)`. |
| Receipt replay | `ReceiptExpired` | exact R-C0 provenance response with echoed request token, participant id, `presented_generation:Some(request generation)`, presented marker Option, result/current generations, and reason; no fingerprint field | Exact only inside provenance window; not retryable; preserve newer credential or `CredentialRecoveryLost`. |
| Receipt replay | `StaleAuthority` | credential request's exact common request envelope plus `current_generation`; use the generic row | Proves no commit for this token; not retryable. |
| Receipt replay | `StaleOrUnknownReceipt` | exact R-C0 flattened schema with conversation/token/participant, presented generation, presented marker Option, and current generation | Post-provenance ambiguity; claims no commit; no automatic retry. |
| Receipt replay | `Retired` | exact common request envelope plus `retired_generation`; use the generic row | Terminal; no secret or binding. |
| `DetachRequest` | `DetachCommitted` | complete exact replacement schema `DetachCommitted { conversation_id,participant_id,capability_generation,detach_attempt_token,committed_binding_epoch,detached_delivery_seq }` | Success; echo comes only from `detach_replay::Committed`, never reused live-binding state; stable until next successful attach/Leave. |
| `DetachRequest` exact-token replay while cell is Pending | `ObserverBackpressure` | exact common request envelope plus committed binding epoch, **current cell `backpressure_epoch` per rewrite rule**, and `observer_progress`; no delivery sequence | Equal returns unchanged; greater progress drains first or atomically rewrites cell+interest to newer refusal epoch. Never park on a consumed epoch or create a second candidate. |
| `DetachRequest` different token while cell is Pending | `DetachInProgress` | complete exact schema `DetachInProgress { conversation_id,participant_id,presented_token,presented_generation,committed_binding_epoch }`; never the stored token | Terminal for the competing attempt; no state change or sequence. |
| `DetachRequest` | `StaleAuthority` | complete exact replacement tagged union beginning with `authority_state:u16`, assigned `0x0001 Live` and `0x0002 TerminalizedDetachCell`: `Live { conversation_id,participant_id,capability_generation,detach_attempt_token,current_generation }` or `TerminalizedDetachCell { conversation_id,participant_id,capability_generation,detach_attempt_token,current_generation,committed_binding_epoch,binding_state:Bound { current_binding_epoch }\|Detached }` | Ordinary live authority mismatch selects `Live`. A token resolved to a terminalized old detach cell selects `TerminalizedDetachCell`; absent live binding is `Detached`, never a sentinel or retained fake current epoch. Both the outer authority tag and nested binding-state tag are required, and no optional field bag exists. |
| `DetachRequest` | `Retired` | exact common request envelope plus `retired_generation` | Terminal tombstone result after Leave; preserve operator identity, no record. |
| `DetachRequest` first accepted while append is blocked | `ObserverBackpressure` | exact common request envelope plus committed binding epoch, `backpressure_epoch,observer_progress` | One atomic transition writes binding-epoch-keyed `PendingFinalization` plus `detach_replay::Pending`; progress wake atomically appends and converts Pending to Committed with the real sequence. |
| `ParticipantAck { conversation_id, participant_id, capability_generation, through_seq }` | `AckCommitted` | common request envelope plus `current_cursor`, equal to the committed resulting cursor | Success only after tuple+binding match; advance SDK watermark. |
| `ParticipantAck` | `AckNoOp` | common request envelope plus unchanged `current_cursor` | Success; idempotent confirmation under the same authority. |
| `ParticipantAck` | `AckGap` / `AckRegression` | common request envelope plus exact R-C3 body: `current_cursor` and respectively `reason:NotContiguouslyAvailable` or `reason:BelowCursor` | Terminal for this ack after authority succeeds; do not advance SDK watermark. Invalid authority uses the generic `StaleAuthority` row and discloses no current cursor. |
| `ParticipantAck` | `Retired` | exact common request envelope plus `retired_generation` | Tombstone lookup uses the presented id; no secret, binding, or live/current cursor is present. |
| Bound `LeaveRequest` with exact current secret | `LeaveCommitted` | complete exact replacement schema `LeaveCommitted { conversation_id,leave_attempt_token,participant_id,presented_generation,retired_generation,ended_binding_epoch:Some(exact active BindingEpoch),prior_terminal_delivery_seq:Option<DeliverySeq>,left_delivery_seq }` | Success; terminal participant state. The prior-terminal option is `Some` iff an older binding terminal exists, whether separately drained or positionally composed. Duplicate token returns the identical result. |
| Detached `LeaveRequest` with exact current secret, at any generation | `LeaveCommitted` | complete exact replacement schema `LeaveCommitted { conversation_id,leave_attempt_token,participant_id,presented_generation,retired_generation,ended_binding_epoch:None,prior_terminal_delivery_seq:Option<DeliverySeq>,left_delivery_seq }` | Terminal success only: positionally compose the prior terminal only with no intervening tuple, otherwise drain/link it separately; release marker/membership/cursor, tombstone, and never bind or grant replay/ack authority. |
| `LeaveRequest` | `StaleAuthority` / `Retired` | `StaleAuthority` uses the cross-cutting row's complete exact `authority_state` two-variant replacement union; `Retired` uses the exact common request envelope plus `retired_generation` | Terminal; live mismatch selects `authority_state:Live`; exact committed-token wrong secret selects `authority_state:CommittedLeaveTombstone`. A different token is `Retired`. Neither returns a secret, binding, or new record. |
| `LeaveRequest` | `ObserverBackpressure` | exact common request envelope plus `backpressure_epoch,observer_progress,prior_terminal_cell_exists` | Preserve valid authority/request only in the bounded SDK row and retry once after progress. Leave creates no server pending cell, whether or not an earlier binding terminal exists. |
| `MarkerAck { conversation_id, participant_id, capability_generation, marker_delivery_seq }` | `MarkerAckCommitted` / `AckNoOp` | common request envelope plus `current_cursor`; committed returns the resulting marker cursor and no-op returns it unchanged | Success only after tuple+binding match; record abandonment or idempotent confirmation. |
| `MarkerAck` | `MarkerNotDelivered` / `MarkerMismatch` | exactly R-C3's flattened schema: `conversation_id,participant_id,capability_generation,requested_marker_delivery_seq` plus the selected reason's required `current_cursor` or `expected_marker_delivery_seq`; no common-envelope duplication | Terminal for this ack after authority succeeds; cursor holds. Invalid authority uses the generic `StaleAuthority` row and discloses no current marker/cursor state. |
| `MarkerAck` | `Retired` | exact common request envelope plus `retired_generation` | Tombstone lookup uses the presented id; no secret, binding, or live/current cursor is present. |
| `RecordAdmission { conversation_id, participant_id, capability_generation, payload }` | `RecordCommitted` | exact common request envelope plus `sender_participant_id` and assigned `delivery_seq` | Success only after tuple+binding match; `sender_participant_id` equals the verified request participant. After response loss enter SDK `RecordAdmissionUnknown` and never resend automatically. |
| Ordinary record admission | `RecordTooLarge` / `ConversationSequenceExhausted` / `MarkerClosureCapacityExceeded` | `RecordTooLarge` carries `dimension: Entries\|Bytes`, exact `encoded_record_charge`, and exact componentwise `max_ordinary_record_charge`; exhaustion carries exactly one nested canonical ten-field `sequence_budget`; closure carries the complete generic `MarkerClosureCapacityExceeded` row above | Terminal for this record; refusal consumes no sequence and preserves floor, identity, every exit/terminal/marker claim, credit, and debt edge. |
| Ordinary record admission | `ObserverBackpressure` | exact common request envelope plus `backpressure_epoch,observer_progress` | A received refusal may park for one progress-cycle retry; a lost response is ambiguous and terminal. |
| Reconnect recovery handshake | `ObserverRecoveryAccepted` | after R-D2's structural status count, exactly one request-ordered `statuses` list whose entries are `ObserverProgressStatus { conversation_id,refused_epoch,current_observer_progress,armed,progressed }` | Single whole-batch success. Older epochs are progressed/unarmed; equal epochs atomically arm then snapshot. Empty input returns an empty list. Matching progress retries every bounded local row at that epoch. |
| Reconnect recovery handshake | `InvalidObserverEpoch` / `InvalidObserverEpochList` | after R-D2's required structural zero status count, exact flat top-level fields with a scalar reason tag: `InvalidObserverEpoch { reason:ConversationUnknown,conversation_id,presented_epoch,current_observer_progress:None }`, `InvalidObserverEpoch { reason:EpochAhead,conversation_id,presented_epoch,current_observer_progress:Some(current) }`, `InvalidObserverEpochList { reason:TooManyEntries,presented_entries,max_entries }`, or `InvalidObserverEpochList { reason:DuplicateConversation,conversation_id,first_index,duplicate_index }`; no nested reason body | Whole-batch typed error; validation arms nothing and mutates no participant state. Length, duplicate, request-order connection-capacity, then request-order epoch validation is exhaustive. |

A valid terminal detach or binding fate never returns
`ConversationSequenceExhausted` because `T` owns its value; any valid bound or
detached Leave never returns it because `E` owns `Left` (and `T` separately owns
an earlier pending terminal). That outcome remains reachable only for optional
enrollment, attach, supersession, ordinary, or floor-triggering candidates before
they invade the reserve. Each owed
marker already owns its `M` value; its physical append is instead protected by
R-C4 closure accounting.
Normal and marker acks never return `ObserverBackpressure`,
`MarkerClosureCapacityExceeded`, or `ConversationOrderExhausted`: they append no
record, allocate no major, and may relieve retention pressure. Ordinary record admission is
deliberately at-most-once-ambiguous after response loss; unlike mandatory-token
operations, the SDK must not convert ambiguity into a duplicate. Applications
requiring deduplication supply and durably interpret an application-level key in
the opaque payload.

`ObserverProgressed` is a pushed connection event, not a polling invitation or
admission guarantee. Receipt and identity capacity refusals are terminal for
that request; no progress event is promised. `HistoryCompacted` carries affected
participant, `abandoned_after`, `abandoned_through`, and
`physical_floor_at_decision`. Every ParticipantDelivery carries the exact common
`conversation_id,delivery_seq,record_kind` prefix. Only `OrdinaryRecord` then
carries `sender_participant_id,payload`; each lifecycle/compaction kind instead
carries its exact affected-participant/epoch/cause or compaction fields from the
R-D2 tagged union, with no optional sender or payload slot. Every record has R-C2
order and member entitlement. `conversation_id`, not `stream_id`, is the mux key.

**R-D2 — One structural protocol-capability choke point.** Leave Push
byte-for-byte untouched and add participant frame discriminants beside it. Push
is a correlated transport with opaque payload
(`crates/liminal/src/protocol/frame.rs:381-405`), so its correlation lifetime is
not the participant log lifetime.

Reserve outer generic `FrameType::Participant=0x1A`, the next free stable u8 value
after `Deliver=0x19`; only that known outer type enters this gate. Its generic
header is exactly `FrameHeader { frame_type:0x1A,flags:0,stream_id:0,
payload_length:u32 }` in the existing ten-byte network-order layout. Every such
frame then has one version-independent structural prefix before its body:
`ParticipantFramePrefix { participant_version_major:u16,
participant_version_minor:u16,participant_discriminant:u16 }`, in that field
order and network byte order. `payload_length` counts the six prefix bytes plus
the body, and the complete encoded-frame size is exactly `10+payload_length`.
Together the first two prefix fields are `ProtocolVersion`; v1 is exactly
`{ major:1,minor:0 }`.

The complete stable v1 inner registry is direction-partitioned, not an open
category description. Client-to-server values are `0x0001 EnrollmentRequest`,
`0x0002 CredentialAttachRequest`, `0x0003 DetachRequest`, `0x0004 ParticipantAck`,
`0x0005 LeaveRequest`, `0x0006 MarkerAck`, `0x0007 RecordAdmission`, and
`0x0008 ObserverRecoveryHandshake`. Receipt replay retransmits byte-identical
`0x0001` or `0x0002` bytes and creates no ninth request shape.

Server-to-client semantic values are this contiguous exact registry:
`0x0100 ParticipantTransportRejected`, `0x0101 AttemptTokenBodyConflict`,
`0x0102 ConnectionConversationCapacityExceeded`,
`0x0103 ConnectionConversationBindingOccupied`,
`0x0104 ConversationOrderExhausted`, `0x0105 ParticipantUnknown`,
`0x0106 NoBinding`, `0x0107 StaleAuthority`, `0x0108 Retired`,
`0x0109 MarkerClosureCapacityExceeded`, `0x010A EnrollBound`,
`0x010B EnrollmentKnown`, `0x010C ReceiptExpired`,
`0x010D ReceiptCapacityExceeded`, `0x010E IdentityCapacityExceeded`,
`0x010F ObserverBackpressure`, `0x0110 ConversationSequenceExhausted`,
`0x0111 AttachBound`, `0x0112 StaleOrUnknownReceipt`,
`0x0113 MarkerNotDelivered`, `0x0114 MarkerMismatch`, `0x0115 Bound`,
`0x0116 UnboundReceipt`, `0x0117 DetachCommitted`,
`0x0118 DetachInProgress`, `0x0119 AckCommitted`, `0x011A AckNoOp`,
`0x011B AckGap`, `0x011C AckRegression`, `0x011D LeaveCommitted`,
`0x011E MarkerAckCommitted`, `0x011F RecordCommitted`,
`0x0120 RecordTooLarge`, `0x0121 ObserverRecoveryAccepted`,
`0x0122 InvalidObserverEpoch`, `0x0123 InvalidObserverEpochList`, and
`0x0124 ObserverRecoveryConnectionCapacityExceeded`. The last wire value decodes
to the existing named `ConnectionConversationCapacityExceeded` outcome with its
complete connection-scoped batch schema; it is not a new semantic outcome.
The pushed/control values are `0x0200 ObserverProgressed` and
`0x0201 ParticipantDelivery`. Lifecycle and compaction records travel inside
`ParticipantDelivery`, whose exact `record_kind:u16` registry is `0x0000 OrdinaryRecord`,
`0x0001 Attached`, `0x0002 Detached`, `0x0003 Died`, `0x0004 Left`, and
`0x0005 HistoryCompacted`. SDK-local, startup/configuration, accepted-socket, and
internal-recovery outcomes in R-D1 are deliberately absent from the wire registry.
No other v1 inner value is assigned; `0x0000`, all gaps, and `0xFFFF` are
unassigned, with `0xFFFF` the permanent unknown-value fixture.

Every semantic response value `0x0101..=0x0120` begins its body with exact
`originating_request:u16`, equal to one of `0x0001..=0x0007`; that structural echo
selects the operation-specific R-D1 common envelope and is serialized once before,
not inside, the exact semantic payload. The outcome/request pairs admitted by the
R-D1 rows are exhaustive; an assigned but impossible pair is InvalidField.
The pair is a routing selector: once both u16 fields exist, an impossible pair
returns that class before any unread suffix byte is interpreted.
Transport rejection has no decodable request, the three recovery outcomes plus
`0x0124` are already request-specific, and pushed values have no originating
request, so those values carry no such echo. This removes same-width ambiguity
between, for example, normal and marker `AckNoOp` or direct and recovery-batch
connection-capacity refusal.
Recovery values have one separate structural count prefix already charged as
R-A4 `RC(P)`: `0x0121` carries `status_count:u64` equal to the following status
list length, while `0x0122`, `0x0123`, and `0x0124` carry required
`status_count:u64=0` before their exact flat R-D1 error payload. Like
`originating_request`, this codec-dispatch prefix is not duplicated inside the
semantic payload schema. For `0x0121`, the count selects exactly that many SE
entries; only after that selected list has exact length does field-domain
validation require the count and request-ordered identities to equal the
outstanding handshake request. For `0x0122..=0x0124`, nonzero is a routing-field
InvalidField before any unread error-body byte. Thus a success count produces
the selected-schema Missing/trailing behavior below, whereas an error count
cannot disguise an alternate error layout.

The `0x0201` body is a complete record-kind-tagged union, not an optional field
bag. Its common prefix is `conversation_id,delivery_seq,record_kind`; then
`OrdinaryRecord` has `sender_participant_id,payload`, `Attached` has
`affected_participant_id,binding_epoch`, `Detached` and `Died` each have
`affected_participant_id,binding_epoch,cause`, `Left` has
`affected_participant_id,ended_binding_epoch:Option<BindingEpoch>`, and
`HistoryCompacted` has `affected_participant_id,abandoned_after,
abandoned_through,physical_floor_at_decision`. Those fields are exactly the R-A1/
R-C3 record fields with the already-common conversation/id names serialized once.

Assignment is receiver-directional: the server admits exactly the eight
client-to-server values, and an SDK with stored participant capability admits
exactly the semantic/control values enumerated above. A value assigned only in the opposite
direction is `UnknownDiscriminant` at that receiver, just like an unassigned
value. Before capability storage the SDK admits only a structurally valid
`0x0100 ParticipantTransportRejected` at version 1.0, so it can consume a
pre-negotiation/auth/version rejection but no other participant frame. An unknown
outer generic FrameType remains the existing generic `Frame::Unknown` behavior
and is not falsely attributed to the participant family.

The six prefix bytes are structural framing, not semantic request fields and not
part of R-D1's common request envelope. The ten-byte generic header and six-byte
prefix are nevertheless included exactly once in every complete encoded-request/
frame size (`R`, `WF`, and recovery `RF`/`RH`/`SH`), every parked encoded-request
charge, and every token verifier's protocol-versioned canonical bytes. Negotiating
the exact capability string `"participant-v1"` fixes and stores participant
version 1.0; ordinary typed outbound construction requires that stored capability
and writes its stored version. R-D2's one-shot rejection permit is the sole
exception described below. A peer can therefore put a different concrete version,
a wrong-direction value, or an unassigned value on an inbound participant frame
and receive one deterministic classification without inventing a body field.

Before semantic dispatch, every participant frame passes one total bounded gate.
Its exact result schema is `ParticipantTransportRejected { reason }`, where
`reason` is exactly one of `FrameTooLarge { complete_frame_bytes,
max_frame_bytes }`, `DecodeFailed { decode_class }`, `UnsupportedVersion {
presented_version, supported_version }`, `AuthenticationFailed {}`, or
`ParticipantCapabilityRequired { required_capability }`. No reason field is
flattened into the top-level outcome and no variant carries another variant's
fields.
Let `FRAME_MAX=10+u32::MAX` complete bytes and let the compile-time, nonzero
pre-capability ceiling be `PRECAP_PARTICIPANT_FRAME_MAX=1_048_576`, which is in
`16..=FRAME_MAX`. The receiver's exact `max_frame_bytes` is stored negotiated
`WF` when participant capability exists and otherwise
`PRECAP_PARTICIPANT_FRAME_MAX`. From the header compute
`complete_frame_bytes=u64(10)+u64(payload_length)`; this cannot overflow and is
the only quantity compared or reported. If it exceeds `max_frame_bytes`, select
the nested `FrameTooLarge { complete_frame_bytes,max_frame_bytes }` before body
allocation. TCP bytes beyond that declared complete frame belong to the next
frame and are never folded into this value.
Before capability no semantic participant request or delivery is legal and the
SDK admits only the typed bounded rejection, whose schema maximum is below this
constant. Therefore the pre-cap ceiling excludes no legal operation and is an
explicit allocation bound rather than an undocumented protocol narrowing.

Otherwise `flags!=0`, `stream_id!=0`, or `payload_length<6` selects
`DecodeFailed { decode_class:Framing }`; all three are version-independent and
may be checked together because they have the identical exact payload. After
Framing passes, decode the two fixed-width **version fields** and compare their
`ProtocolVersion` to `expected_version`, which is stored participant version 1.0
when capability exists and server-supported 1.0 otherwise. Inequality returns
the nested `UnsupportedVersion { presented_version,supported_version:
expected_version }` before any version-dependent discriminant/body decode.

For the supported version, next require `participant_discriminant` to belong to
the receiver-direction set above; an unassigned or opposite-direction value is
`DecodeFailed { decode_class:UnknownDiscriminant }` before its unknowable body is
interpreted. A selected v1 body then uses one exact typed codec: schema fields are
serialized in their declared order. The primitive alias register is fixed:
conversation and participant ids, capability/retired generations, delivery/
transaction/park/occurrence positions, floors, epochs/progress values, counts,
indices, occupancies, and limits are u64; `ConnectionIncarnation` is the two-u64
16-byte value from R-A1; `BindingEpoch` is that value followed by its u64
capability generation, exactly 24 bytes; every enrollment/attach/detach/Leave
attempt token is an opaque fixed 16-byte value; `attach_secret` is an opaque
fixed 32-byte value; receipt/provenance deadlines are u128; and opaque application
payload is the bytes form below. Thus no identifier or credential width is left
to implementation choice. In the canonical ten-field `SequenceBudget`,
`high_watermark,remaining,E,T,M,RS,RT` are u64 and the checked-product fields
`L_times_T,L_times_RT,L_other_times_E` are u128; other fields explicitly declared
u128 in R-A4/R-C4 likewise override the scalar aliases. In
`ConversationOrderExhausted`, `high` is u64, `next_value` is `Option<u64>`,
`required_majors` is u64, and `order_remaining,reserved_claims,
resulting_order_remaining,resulting_reserved_claims` are u128. u16/u32/u64/u128 and those fixed-width identifiers
use their declared widths in network byte order; bool is one byte `0x00|0x01`;
`Option<T>` is one byte `0x00` or `0x01` followed by T only for `0x01`; byte
strings and UTF-8 strings are a u32 network-order byte length followed by exactly
that many bytes; ordinary lists are a u32 network-order element count followed by
exact elements; the recovery refusal/status list alone uses its declared fixed
u64 count (`RC(P)=8`); and a tagged union is a u16 registry value followed by
exactly the selected fields. No decoder preallocates from an untrusted count:
the fixed count-by-element-width product is widened to u128 and compared with the
already bounded remaining frame before allocation, and variable-width elements
are walked within that same remaining-byte bound. An enormous in-frame count
therefore produces the structural MissingRequiredField result below without
allocating count-sized memory. Unless a schema gives an explicit numeric registry,
every enum/tagged-union registry is the one-based u16 ordinal of variants in the
exact left-to-right or top-to-bottom order in which that complete schema lists
them; this rule makes every R-D1 reason, scope, dimension, and nested body
reproducible without a second implicit registry. In particular transport-reason
codes 1..5 are FrameTooLarge, DecodeFailed, UnsupportedVersion,
AuthenticationFailed, and ParticipantCapabilityRequired, while decode-class
codes 1..5 are Framing, UnknownDiscriminant, CanonicalEncoding,
MissingRequiredField, and InvalidField. The body occupies exactly
`payload_length-6` bytes.
Body decoding is one deterministic structural walk, not an impossible flat
precedence assertion. A missing byte before or within the next required fixed
field, selector, length prefix, or the contents promised by a byte/string/list
length selects `MissingRequiredField`. A present routing/variant selector must
name a schema before its suffix can be interpreted: an impossible
`originating_request`/outcome pair, an unassigned Option tag, or an unassigned
tagged-union value selects `InvalidField` immediately and ignores the opaque
remaining bytes, because no alternative suffix shape exists. The explicitly
required-zero recovery error count uses the same routing decision. An ordinary list count and the
`0x0121` status count instead select a concrete list shape. Once every selector
is valid, the codec knows the complete selected shape: a complete shape leaving
any trailing byte selects `CanonicalEncoding`; truncation within that shape
selects `MissingRequiredField`; and only after an exact-length shape, a fixed-
width scalar value outside its declared domain selects `InvalidField`. In
particular every presented `capability_generation` is in `1..=u64::MAX`; zero is
a reproducible fixed-scalar InvalidField value. These rules leave no alternative
integer, tag, field-order, length, or trailing-byte encoding.

The total outer first-failure order is therefore FrameTooLarge, Framing,
UnsupportedVersion, then `UnknownDiscriminant`; the selected body then follows
the structural walk above and reports exactly one of CanonicalEncoding,
MissingRequiredField, or InvalidField at its stated decision point. The five
complete decode classes remain Framing plus those latter four classes. A
structurally decoded supported frame then checks connection authentication and
negotiated participant capability in that order, returning respectively
`AuthenticationFailed {}` or `ParticipantCapabilityRequired {
required_capability:"participant-v1" }`. The sole receive-side exception is an
SDK decoding assigned-direction `0x0100`: after the complete size, framing,
version, discriminant, canonical body, required-field, and field-domain checks
above succeed, it bypasses both the SDK's local authentication-state check and
its participant-capability check and surfaces the typed rejection. It creates no
authority and cannot admit any other participant value. This exact exception is
what lets a server's pre-authentication or pre-capability refusal cross the same
gate instead of being masked by a locally synthesized refusal. These five
top-level reasons—counting the five-class `DecodeFailed`
selector as one reason—are the complete bidirectional trigger set for the one
transport outcome; its reason-specific payload contains no fields from another
arm. When framing permits, a server receiver sends the typed rejection then
closes; an SDK receiver surfaces the same local outcome and closes without
echoing it, preventing rejection loops. Every arm creates no semantic operation
state. At the server receiver, an unbound close leaves participant/conversation
state untouched, while an active close feeds exactly one R-A2
`Died(ProtocolError)` finalization and no second transport-gate effect. At the
SDK receiver, the local refusal/close fabricates no server-side cause; absent an
earlier authoritative event, the server's ensuing EOF is R-A2
`Died(ConnectionLost)`, not an inferred ProtocolError.
This gate is why malformed bearer-only Leave bytes are not a semantic
`StaleAuthority` arm.

The rejection frame itself uses outer `FrameType::Participant=0x1A`, assigned v1
inner `0x0100 ParticipantTransportRejected`, flags/stream zero, and prefix version
equal to stored participant version when it exists and server-supported 1.0
otherwise. Thus Framing, UnsupportedVersion, and AuthenticationFailed have a
defined response header/prefix even before successful negotiation is stored. A
one-shot `TransportRejectionPermit` is minted only by this gate and authorizes
only that typed rejection; it cannot construct delivery, lifecycle, success, or
other semantic participant frames.

The server must store successful negotiated protocol/capabilities only after
negotiation succeeds. Failed authentication or participant-frame version
validation responds and closes; it never leaves an application-admitted connection. Protocol 1.0 is the
only currently supported version
(`crates/liminal-server/src/server/connection/apply.rs:20-21`), and preserving an
unknown discriminant as `Frame::Unknown` is parsing behavior only
(`crates/liminal/src/protocol/codec/known.rs:43-54`).

Outbound enforcement is structural:

1. There is exactly one grep-able server construction site for outbound
   participant frames, adjacent to the outbound emitter.
2. For ordinary output that function requires the connection's unforgeable negotiated
   participant capability or R-D2's one-shot `TransportRejectionPermit`, plus a
   typed participant outbound value, never an unrestricted `Frame`. The permit
   type accepts only `ParticipantTransportRejected`, supplies server version 1.0
   when no stored version exists, and is consumed by one construction. Exactly
   one of negotiated capability or permit is present. For delivery work the
   negotiated-capability path performs R-C5's final exact participant/binding-
   epoch slot check immediately before constructing the discriminant; mismatch is
   a stale-work drop, not retargeting.
3. The constructor accepts only the sender-direction registry, so a server cannot
   emit a client request value and an SDK cannot emit a server outcome/control
   value. The generic outbound enqueue path rejects every raw outer
   `FrameType::Participant`, regardless of inner value, so no producer can bypass
   the construction site.
4. Replay, live delivery, lifecycle, compaction, attach responses, and close
   verdicts all call that site. Pre-handshake/auth/version rejection can obtain
   only the one-shot rejection permit; force-close and nonparticipant paths can
   obtain neither authority.
5. Same-connection detach/rebind inserts R-C5's writer barrier at this boundary:
   old-epoch constructed bytes precede the response, stale queued values fail
   revalidation, and the new epoch's first construction follows. Cross-connection
   handoff instead fences the old constructor and relies on SDK epoch retirement;
   it asserts no ordering between independent TCP streams.

This choke point sits at the common emission boundary: ordinary applied
responses currently reach `OutboundWriter::enqueue_frame` from `process_buffer`
(`crates/liminal-server/src/server/connection/process.rs:713-730`), delivery also
uses `enqueue_frame` (`crates/liminal-server/src/server/connection/delivery.rs:128-142`),
and that method is where frames are encoded
(`crates/liminal-server/src/server/connection/outbound.rs:124-153`). A second
participant-frame constructor or unchecked encode path is a contract failure.

**R-D3 — Correlation remains correlation.** `correlation_id` remains the Push ↔
PushReply key; PushReply echoes it and the server owns the one-shot reply slot
(`crates/liminal/src/protocol/frame.rs:381-405`,
`crates/liminal-server/src/server/connection/supervisor.rs:224-233`). Participant
`delivery_seq` and attach credentials never reuse it. A future participant
application reply needs an explicit relationship rather than overloading either
key.

**R-D4 — Payload opacity.** Payload remains uninterpreted application bytes. The
participant record wraps those bytes; it does not inspect or derive identity,
recipient, ordering, or lifecycle facts from them. That preserves the verified
opacity of Push payload (`crates/liminal/src/protocol/frame.rs:381-394`).

### Silence-attacking acceptance frame

From captured wire bytes alone, an SDK must attribute every participant record
to `(conversation_id, delivery_seq)`, identify sender or affected participant,
distinguish payload from lifecycle, and obtain the canonical idempotency key.
Exercise all nine concrete transport-trigger arms directly—the five top-level
reasons with `DecodeFailed` split into its five-class selector:

1. with valid stored `WF=PF`, a header with exact
   `payload_length=PF-9`, hence complete size PF+1, returns
   `FrameTooLarge { complete_frame_bytes:PF+1,max_frame_bytes:PF }` before
   allocation; `PF>=16` makes the u32 payload length defined. Independently,
   before capability, `payload_length=1_048_567` yields complete 1_048_577 and
   reports the exact pre-cap maximum 1_048_576;
2. independent in-limit headers with respectively `flags:0x01`, `stream_id:1`,
   and `payload_length:5` each return `DecodeFailed { decode_class:Framing }`;
3. an in-limit exact prefix `{ participant_version_major:2,
   participant_version_minor:0,participant_discriminant:0x0005 }` against
   expected 1.0 returns `UnsupportedVersion { presented_version:{major:2,
   minor:0},supported_version:{major:1,minor:0} }`;
4. exact supported prefix `{1,0,0xFFFF}` returns
   `DecodeFailed { decode_class:UnknownDiscriminant }`; the server also returns
   that class for opposite-direction `0x0100`, and a capable SDK does so for
   opposite-direction `0x0005`;
5. a complete canonical `0x0005 LeaveRequest` body followed by one extra
   `0x00`, with `payload_length` increased by one, returns
   `DecodeFailed { decode_class:CanonicalEncoding }`;
6. a canonical `0x0005 LeaveRequest` prefix/body ending immediately before or
   anywhere within required `attach_secret` returns
   `DecodeFailed { decode_class:MissingRequiredField }`;
7. a complete canonical `0x0002 CredentialAttachRequest` whose exact
   `capability_generation` bytes encode zero returns
   `DecodeFailed { decode_class:InvalidField }`; independently, a canonical
   server `0x011E MarkerAckCommitted` body with impossible
   `originating_request:0x0001` returns that same class at the SDK; the canonical
   `0x0107`/`originating_request:0x0003` Case-53 vectors decode
   `authority_state:0x0001` as `Live` and `0x0002` as
   `TerminalizedDetachCell`; and the two canonical
   `0x0107`/`originating_request:0x0005` Case-53 vectors decode
   `authority_state:0x0001` as `Live` and `0x0002` as
   `CommittedLeaveTombstone`. For either origin, `0x0000` or `0x0003` returns
   that same InvalidField class before any variant suffix is interpreted.
   Repeat the impossible originating-request and invalid-tag vectors with no
   suffix, a truncated would-be suffix, and arbitrary trailing bytes; all return
   routing InvalidField. For each of `0x0122..=0x0124`, nonzero
   `status_count` likewise returns routing InvalidField with an absent, exact, or
   trailing error body. By contrast, for an outstanding two-entry handshake,
   `0x0121/status_count:2` plus one status is MissingRequiredField, two statuses
   plus one byte is CanonicalEncoding, exact two statuses proceeds, and
   `status_count:1` plus one exact status is fixed-domain InvalidField because it
   does not equal the outstanding request;
8. a canonical supported `0x0001 EnrollmentRequest` before authentication returns
   `AuthenticationFailed {}`; and
9. the same authenticated frame without stored `"participant-v1"` returns
   `ParticipantCapabilityRequired { required_capability:"participant-v1" }`.

The unauthenticated, pre-capability SDK independently consumes a fully canonical
valid 1.0/`0x0100` rejection and closes without requiring either local state; a
malformed rejection still selects the first structural/decode arm above rather
than receiving this exception. Combine all applicable failures and
make each earlier predicate valid in turn. For body precedence, an unknown value
with arbitrary trailing bytes remains UnknownDiscriminant; a known complete
generation-zero body plus a trailing byte is CanonicalEncoding; removing the
trailing byte and truncating during the next required field is
MissingRequiredField; adding all required fields exposes InvalidField. An
unsupported version combined with any body fault selects UnsupportedVersion;
changing only both version fields to 1.0 exposes the first body fault. The R-D2
order selects exactly the stated arm each time. Run every arm first on an
unbound connection and prove all participant/conversation stores byte-identical.
Then send to the server, after one active binding, representatives of every
reason still triggerable in that state—size, framing, version, and body decode.
Authentication and capability refusal are pre-binding by construction and are
not fabricated in this arm. The gate commits no decoded-request effect, while
its one server-local close event produces exactly the counter-claim-backed R-A2
`Died(ProtocolError)` terminal (or complete bounded PendingFinalization);
retry/repeated close observation produces no second lifecycle fact. In the
opposite direction, feed an active SDK malformed server bytes: it surfaces the
local typed rejection and closes without echo, and the server's resulting EOF
produces exactly one `Died(ConnectionLost)` unless an earlier authoritative fate
already won; neither side invents the other's decode cause.
For every attach/detach/Leave exchange it must also recover the echoed attempt
token, capability generation, applicable receipt deadline, and typed
binding/terminal status; an
`UnboundReceipt` can never decode as `Bound`. Mixed-version tests exercise every
producer class—attach, Leave, replay, live,
lifecycle, compaction, auth/version refusal, force-close, and pre-handshake—and
prove each emitted participant value is in the exact sender-direction v1 registry
and force-close emits no participant frame. A source-structure
test/grep proves there is exactly one outbound participant-frame construction
site, that it performs the final participant/binding-epoch revalidation, and that
generic enqueue refuses every raw outer `FrameType::Participant`. On one connection, queue P work,
cross detach/rebind, and bind Q: only constructed P bytes precede the response,
stale work drops, and Q's first construction uses Q's epoch. Repeat across two
connections: assert no cross-stream order, atomically retire P's exact old SDK
dispatch slot before exposing Q's `AttachBound`, reject later old-slot delivery,
and keep the physical reader serving unrelated conversations until connection fate.
Push/PushReply codec vectors remain byte-identical.

## 6. Interactions and non-goals

### Interactions

**R-I1 — Resume becomes buildable, not silently compatible.** The current TCP
resume method is a typed refusal
(`crates/liminal-sdk/src/remote/tcp/mod.rs:190-207`). R-C0/R-C1/R-C3 and R-D1/R-D2
supply write-ahead attach, authorized cursor, replay, idempotency key, and verdict
facts for a later SDK surface. They do not authorize routing participant recovery
through the legacy subscription-local `ResumeRequest`; `«RESUME-COMMENT-SERVER-MISMATCH»`
must rule distinct protocols versus migration/deprecation first.

**R-I2 — Cally/F-3a gate.** The checked-in receive brief assigns the lane and
parks merge at reviewer-plus-domain-owner keys
(`docs/design/SDK-PARTICIPANT-RECEIVE.md:3-10`,
`docs/design/SDK-PARTICIPANT-RECEIVE.md:35-41`). It requires receive facts
including conversation, sender, payload, and sequence
(`docs/design/SDK-PARTICIPANT-RECEIVE.md:96-110`) and reattach from a server
cursor with typed compaction (`docs/design/SDK-PARTICIPANT-RECEIVE.md:128-136`).
The future version carrying that receive path therefore remains gated on this
contract and its implementation.

`«LIMINAL-SDK-VERSION»` is the unfilled future-release dependency gate. It is
distinct from the brief's filled sequencing gate
`«LIMINAL-BASE-VERSION: 0.2.4»`
(`docs/design/SDK-PARTICIPANT-RECEIVE.md:12-33`).

**R-I3 — Resume vehicle.** After both keys pass and the contract lands, norn
session `256a81a0` / envelope `claude-dev-sdk-receive.8rss9K` is the designated
resume vehicle. Until then it remains blocked upstream.

### Non-goals and explicit exclusions

- No implementation, schedule, or release promise in this document.
- No SDK API/ergonomics design beyond facts the wire must expose.
- No application payload schema.
- No ACL or human/agent identity mapping; that belongs above the participant
  continuity capability.
- No exactly-once application effect guarantee.
- No lifecycle outbox or unbounded secret-bearing receipt, provenance-fingerprint,
  or retired-identity store.
- No application heartbeat, poll, sweep, scan, synthetic probe, receipt-expiry
  sweep, observer retry loop, or wall-clock liveness score.
- No connection-fate detection for SIGSTOP/deadlock/livelock.
- No targeted or filtered delivery within a v1 conversation. V1 is broadcast;
  `«TARGETED-DELIVERY»` is a named future protocol design, not a current hole.
- Wire and server semantics only.

### Silence-attacking acceptance frame

A reviewer refutes this section by finding a hidden ACL claim, SDK ergonomics
commitment, exactly-once effect promise, targeted-delivery branch, schedule, or
liveness detector. The receive brief must be implementable from explicit
current-contract facts, except where §7 names a genuine gate.

## 7. Named sockets and decision register

R18 distinguishes decisions made by the draft from genuine unknowns. A
**decided-by-draft** row has one candidate answer and may change only by explicit
key refusal/revision. A **genuinely-open** row names unavailable platform
evidence, an implementation linearization mechanism, a layer-above policy, or
an owner migration decision. Open rows are not permission for implementations
to choose incompatible semantics silently.

| Socket | Status | R18 answer or reason openness is genuine | Closure/refutation evidence |
|---|---|---|---|
| `«ATTACH-TRANSACTION»` | **decided-by-draft** | Attach, detach, and Leave use mandatory write-ahead tokens and one serialized participant state. The identity slot's detach cell is `Empty`, token-bearing `Pending` without a sequence, or `Committed` with the exact binding epoch/sequence; it is the sole stable-echo source until next attach/Leave. | All send/commit/response windows, same/different token while Pending, crash around Pending→Committed, echo after binding removal, stale replay after replacement death, detach loss across attach/Leave, replacement recovery, and terminal receipt tests. |
| `«RECEIPT-LIFETIME»` | **decided-by-draft** | Signed receipt TTL/count and non-secret provenance TTL/count caps bound both bodies and classifiers. Cleanup uses admitted deadlines; TTL is the maximum supported recovery outage and expiry may enter `CredentialRecoveryLost`. | Delayed generation, crash order, exact/unknown before/after provenance, cap exhaustion, composed outage, and no-sweep tests. |
| `«RETIRED-IDENTITY-BOUND»` | **decided-by-draft** | Enrollment reserves signed server/conversation identity slots; each slot owns the enrollment-token mapping for the live+tombstone lifetime, and Leave converts it to a permanent tombstone. Cap exhaustion refuses before mint, bounding churn without weakening `EnrollmentKnown`, `Retired`, or duplicate `LeaveCommitted`. | Post-GC enrollment T versus fresh U, boundary churn, no-ghost replay, lost Leave response, and pre-mint capacity refusal. |
| `«PARTICIPANT-ID-ORIGIN»` | **decided-by-draft** | Server mints the next permanent ordinal id `0..I-1`, generation 1, and initial secret inside tokenized enrollment; ids are never reused in one conversation and the same numeric ordinal may occur only under a different conversation key. Later attach proves current generation/secret and atomically increments/rotates before cursor access. External ids never authorize a cursor. | Authorization-before-cursor, ordinal boundaries/no reuse, cross-conversation equal ordinals, enrollment replay, generation ordering, rotation loss, and reconnect tests. |
| `«MEMBERSHIP-BOUNDARY»` | **decided-by-draft** | Membership begins at mint with cursor `0`, survives detach, entitles full history and offline commits, and ends only with authoritative tokenized Leave that retires id, receipts, and soft retention claim. | Late join, offline replay, Leave authority/idempotency/races, and floor-transition tests. |
| `«LEAVE-AUTHORITY»` | **decided-by-draft** | Exact current generation/secret authorizes tokenized terminal Leave bound or detached. A pending own terminal composes with Left only if no unrelated tuple lies between their majors; otherwise terminal/intermediates drain separately and one-record Left links the terminal sequence. Detached Leave grants no cursor/binding authority; v1 has no operator Leave. | Authority refusals, duplicate stable result, attach/death races, both positional permutations and crash boundaries, full debt, and generation maximum. |
| `«ACK-SHAPE»` | **decided-by-draft** | Normal `ParticipantAck { conversation_id, participant_id, capability_generation, through_seq }` and `MarkerAck { conversation_id, participant_id, capability_generation, marker_delivery_seq }` carry presented authority. At one serialized point the server looks up the presented id, matches generation plus the exact binding epoch, then validates continuity or named abandonment. Post-Leave uses the presented id's tombstone and never a live cursor. | P-ack-after-Q-bind ambiguity, same-id rotation, unknown/unbound/stale/Retired codec vectors, normal gap/regression, marker-delivery refusal, and proof abandoned payload is not marked delivered. |
| `«COMPACTION-EXIT»` | **decided-by-draft** | A pending marker shares the durable server-candidate admission order with finalizations; at append it recomputes `abandoned_after..abandoned_through` and the physical-floor snapshot. Acking abandons through that pre-marker watermark and advances to marker sequence. | Both marker/finalization orders, append-time fields, retained-suffix choice, marker loss/redelivery, later independent floor episode, concurrent-live, and post-ack flow tests. |
| `«RETENTION-UNITS»` | **decided-by-draft** | Caps start at `2Q+I×marker`: debt borrows only Q; K=Q is componentwise transferable recovery occupancy. Recovery charge moves from K_remaining into B; fit uses exact post-transfer K_remaining. One quartet and the fixed occurrence/selection array back the complete successor plan; anchored DCR forbids ordinary attach. | Empty equality with one-record enrollment; exact S/B/K_remaining transfers; marker ack/compaction; fenced DCR/Leave; equality refusal and farther-max quartet; crash-atomic successor tests. |
| `«MULTI-CONVERSATION-MUX»` | **decided-by-draft** | Yes. One connection carries many conversations, demuxed by `conversation_id`; participant `stream_id = 0` and has no semantic role. | Cross-conversation interleaving and independent-cursor tests. |
| `«RESPONSE-PUSH-ORDER»` | **decided-by-amendment (A1, 2026-07-23; pending reviewer-of-record key)** | No ordering is promised between a request's `ServerValue` response and unsolicited `ServerPush` frames on one connection; per-conversation push order is preserved; correct clients demux by frame variant with single-outstanding-request discipline; v1 has no request-correlation field (R-D3 defers it). | Amplified interleave reproduction (52/60 under 8-way contention, byte captures) plus harness demux fix as fail-first pair; SDK `receive()` variant-demux as reference; cross-conversation interleaving tests of `«MULTI-CONVERSATION-MUX»` unaffected. |
| `«MULTI-BINDING-PER-CONVERSATION»` | **decided-by-draft (excluded in v1)** | At most one participant binds each `(connection_incarnation, conversation_id)`. A different-id enrollment/attach gets R-C1's exact request-echo variant, which adds only `presented_participant_id:None\|Some(request participant)` and never an occupying identity field; same-id rotation is allowed. | Empty-slot race, enrollment with no presented id, Q-after-P refusal without P disclosure, same-P rotation, and conversation-only delivery codec. |
| `«LIFECYCLE-VERDICT-RECIPIENTS»` | **decided-by-draft** | Every member is entitled to every lifecycle/compaction record in total order, including while detached, unless it explicitly accepts a named abandonment after compaction broke continuity. | Three-party lifecycle/compaction races, offline replay, and explicit-abandonment tests. |
| `«LIFECYCLE-OBSERVER-DELIVERY»` | **decided-by-draft** | The log is sole completed lifecycle history and observer progress hard. Signed per-conversation and SDK-wide conversation/row/full-byte caps plus request/row maxima bound parking; durable first-row interest slots make all Awaiting conversations armable. `Reserved` restart, checked u64 order, cohort marking, authority loss, epoch monotonicity, and all-dimension renegotiation are explicit. Acks never park. | Independent row/byte/global cap hits, interest-slot recovery, Reserved crash, per-row downward incompatibility, near-max order, epoch races, credential loss, and no orphan/ack/poll. |
| `«SUPERSESSION-FENCE»` | **decided-by-draft** | Every credential-bearing successful attach checked-increments generation, rotates the secret, and creates a distinct immutable binding epoch even on one physical connection. Final outbound construction drops old-epoch work behind the response barrier; stale proof is a no-record refusal. | Two-holder and same-connection rotation races, single handoff pair, exact-epoch receipt recovery, queued-work barrier, and stale-proof zero-record tests. |
| `«RESUME-COMMENT-SERVER-MISMATCH»` | **genuinely-open** | Owner must choose separate legacy subscription recovery versus version/deprecate/delete it for participant attach. Outline governs over the earlier tear's preference to decide now; R12 nevertheless makes “comment-only fix” insufficient. | Owner ruling covering types, cursor owner, starting convention, persistence, release boundary, and removal/compatibility tests. |
| `«MEMBERSHIP-EVENT-SOURCE»` | **genuinely-open implementation dependency** | External behavior is fixed: membership deltas must be pushed and shutdown must wake the source. The selected membership backend's non-poll event API is not yet chosen. | Backend event subscription/callback with ordered delta and explicit shutdown tests; delete `PollLoop`, `poll_once` cadence, and sleep. |
| `«HEALTH-ACCEPT-SHUTDOWN-WAKE»` | **genuinely-open implementation dependency** | External behavior is fixed: health accept blocks/uses readiness and explicit shutdown interrupts it. The concrete cross-platform wake mechanism is not selected. | Per-target accept/shutdown race tests with no WouldBlock sleep or shutdown-flag sampling loop. |
| `«SHUTDOWN-DRAIN-NOTIFICATION»` | **genuinely-open implementation dependency** | External behavior is fixed: connection exit updates a completion primitive raced against one admitted deadline; force-close uses the same notification. Concrete supervisor API is not selected. | Exit/drain notification API, crash/force-close/deadline races, and deletion of reap/count/sleep loops. |
| `«CHANNEL-REPLY-EVENT-RACE»` | **genuinely-open implementation dependency** | External behavior is fixed: a command wait parks on the reply, pushed process exit, and one admitted command deadline. The scheduler-to-waiter exit-notification primitive is not selected. | Reply/exit/deadline races in every order; delete `LIVENESS_POLL`, repeated `recv_timeout`, and process-table sampling. |
| `«SDK-PUSH-READER-SHUTDOWN-WAKE»` | **genuinely-open implementation dependency** | External behavior is fixed: the push reader blocks on/readiness-waits for socket input and explicit local shutdown interrupts that wait. The portable wake primitive is not selected. | Silent-socket data/EOF/error/shutdown races and join tests; delete `READER_POLL_TIMEOUT`, timeout-as-`None`, and stop-flag rechecks. |
| `«SDK-SUBSCRIPTION-READER-SHUTDOWN-WAKE»` | **genuinely-open implementation dependency** | External behavior is fixed: the subscription reader blocks on/readiness-waits for socket input and explicit local shutdown interrupts that wait. The portable wake primitive is not selected. | Silent-socket data/EOF/error/shutdown races and join tests; delete `READER_POLL_TIMEOUT`, timeout-as-`None`, and stop-flag rechecks. |
| `«DURABILITY-BRIDGE-WAKE»` | **genuinely-open implementation dependency** | External behavior is fixed: asserted synchronous backends receive one poll and loud Pending error, or a real executor parks and honors the waker. Backend integration selects which shape. | Ready/one-Pending/real-wake cases; delete `MAX_POLLS`, `NoopWaker`, repeated `poll`, and `yield_now`. |
| `«PUSH-REPLY-AWAITER-EVENT-RACE»` | **genuinely-open implementation dependency** | External behavior is fixed: the SDK call site waits once on reply, connection fate, and one admitted deadline. A one-shot internal quantum may remain only as mechanism, never a blessed caller re-arm loop. | All race orders and cancellation/drop; delete indefinite `receive` reinvocation contract and repeated short-poll tests. |
| `«SDK-SUBSCRIPTION-SETUP-DEADLINE-RACE»` | **genuinely-open implementation dependency** | External behavior is fixed: handshake/Subscribe setup races socket input with one total deadline. Portable readiness/deadline composition is implementation-owned. | Input/deadline/EOF/error races; delete 100ms `read_one_frame` re-arm and `Instant::now` loop sampling. |
| `«SDK-RECONNECT-TIMER-LOOP»` | **genuinely-open implementation dependency** | Reconnect is armed by a transport/connection-fate event or explicit manual request; failure waits for another event and never timer-retries. Concrete platform event wiring is implementation-owned. | All event/failure/manual races; delete backoff loop, default-infinite attempts, sleep, and setTimeout. |
| `«SDK-RECONNECT-DELAY-CONTRACT»` | **genuinely-open implementation dependency** | Rust/Gleam delay computation and Rust `RemoteHandle::reconnect` forwarding are not themselves waits. A fresh transport-fate transition creates one permit and returns exact local `ReconnectArmed { delay_ms }` when consumed; a call or timer re-arm without that permit returns `ReconnectNotArmed { state:Reconnecting,required_event:TransportFate }` without mutation. These are the exhaustive `ReconnectDelayResult` arms and are not wire outcomes. Concrete event/API wiring is implementation-owned. | Fresh-event/`ReconnectArmed`, no-fresh-event/`ReconnectNotArmed`, attempt/delay, and manual-connect races through lifecycle and `RemoteHandle` in Rust and through the Gleam lifecycle; remove every public repeated-delay re-arm contract while preserving pure bounded calculation if useful. |
| `«DEDUP-EXPIRY-EVENT-SOURCE»` | **genuinely-open implementation dependency** | Dedup expiry is keyed by admitted deadline or mutation notification, never periodic full-store scan. Deadline-queue/storage integration is implementation-owned. | Expiry/mutation/crash races; delete sweep interval and timer-driven `sweep_once` scan. |
| `«EXTERNAL-EXIT-REASON»` | **genuinely-open** | Current external reap cannot read the private beamr exit reason; complete `ProcessKilled` detail needs event/API plumbing, not inference. | Typed event payload or nonblocking termination-reason API; no scan substitute. |
| `«NO-FIN-KERNEL-BOUND»` | **genuinely-open** | Exact signed defaults and macOS/Linux worst-case formulas require platform evidence. The contract shape and refusal policy are decided; numbers are not invented. | Platform documentation, readback, and black-hole fault tests proving lower/worst-case behavior. |
| `«KEEPALIVE-PORTABILITY»` | **genuinely-open** | Supported target option/range/granularity mapping and refusal matrix require target validation. | Per-target set/readback and bounded fault tests; unsupported targets refuse. |
| `«REPLAY-LIVE-CUTOVER»` | **genuinely-open** | External behavior is fixed, but the atomic sequencer/storage/binding linearization mechanism depends on the selected durability backend. | Named linearization point and adversarial attach/replay/live/crash tests. |
| `«ATTACH-SECRET-LIFECYCLE»` | **genuinely-open (narrowed)** | Receipt lifetime is decided and is not credential revocation. The negotiated TTL is the maximum recovery outage; lost response plus expiry normatively produces SDK `CredentialRecoveryLost` with preserved identity. Operator re-issue, post-receipt capability expiry, revocation, and the server-brief ground-pack consequence of undelivered dead-epoch `DetachedMarkerRelease` remain open. | Threat model and authorized operator re-issue from `CredentialRecoveryLost`; atomic expiry/revocation preserving generation, Retired, and no-polling rules. |
| `«WEDGED-PARTICIPANT-POLICY»` | **genuinely-open** | Connection-fate exclusion is decided. Alerting/eviction for cursor stall belongs to the layer above and must not feed a false lifecycle verdict back into this layer. | Layer-owner policy demonstrating no polling detector and no false `ConnectionLost`. |
| `«WEDGED-OBSERVER-POLICY»` | **genuinely-open** | A deterministic callback failure at sequence K truthfully freezes the conversation. Owner must define operator replacement or retirement that resumes from K without silently advancing observer progress, skipping lifecycle, polling, or weakening the hard claim. | Operator-authority ruling, same-key replay/replacement evidence, poison recovery tests, and no-skip audit. |
| `«TARGETED-DELIVERY»` | **genuinely-open (future only)** | V1 excludes it. A future version must redesign entitlement, cursor scanning, ack validation, and capability negotiation rather than weakening broadcast implicitly. | New versioned contract and mixed-version/recipient-gap tests; not a v1 implementation gate. |
| `«LIMINAL-SDK-VERSION-GATE-NAME»` | **decided-by-draft (owner-closed)** | Preserve both real gates: filled `«LIMINAL-BASE-VERSION: 0.2.4»` and unfilled future `«LIMINAL-SDK-VERSION»`. | Existing owner reconciliation and exact names in §6. |

**LAW 2 closes the evidence escape hatch:** an unresolved dependency is one of
the grep-able genuinely-open rows or it cannot support a codebase-state claim.
**LAW 1 closes the mechanism escape hatch:** none may be “solved” by a timer,
poll, sweep, scan, heartbeat, listener backoff, periodic reap, read-timeout wake,
stop-flag sampling, or synthetic write whose job is to ask whether state changed.

### Silence-attacking acceptance frame

Extract every guillemet-delimited socket name and require each result to appear
in this register or be one of the two SDK gate names explained here. For decided rows, search for
one normative answer in §§2-6. For genuinely-open rows, require the named owner
or evidence and refuse implementation where the gap is load-bearing. Search all
timers, wakes, loops, callbacks, and tasks—including all thirteen §1 families and
startup recovery—and prove each is driven by admitted work, reply/data readiness, kernel
connection fate, explicit shutdown/process exit, one admitted deadline, or an
existing domain event rather than change detection.

### R18 define-what-you-name and coherence audits

The drafter ran the standing name audit over this complete artifact. Extraction
between Unicode U+00AB/U+00BB yields 38 unique guillemet names: 36 exact register
rows plus exactly the two §6 SDK gate names permitted above; there is no
unregistered socket. The typed-name audit requires every declaration-shaped use
to resolve to its normative field/phase/outcome table. The retained R15 delta
resolves u64/u128 RF/RC/RE/WF/RH and lifecycle phase in R-A4; B/K_remaining,
marker provenance/current owner, canonical SequenceBudget, RS/RT/RO/RA,
successor occurrences,
edge tags, and decreasing measures in R-A2/R-C2/R-C4; the non-secret detach
verifier in R-C0; and every reachable outcome/dimension in R-D1. R16 additionally
resolves the three detached-edge fate classes, Ce/J, the persisted episode-churn
counter, Dormant occurrence slots, checked `O_max`, supersession retargeting, all W6 boundary-fixture inputs and charge positions, and RE in both phase-specific
nonzero-limit taxonomies. R17 resolves movable claim handles and intact DCR
blocks, `ClaimFrontierInvalid`, cumulative exit-charge/K exposure, fired-product
marker cancellation, delayed projection suffixes, and every H8/H7/H6 Case54
continuation. It makes the normal-ack and marker-proof selectors total, fixes the
five constructible observer-parking scope/dimension pairs, gives SDK-parking
incompatibility no fictional scope field, and confines `ParticipantUnknown` to
requests that actually carry `participant_id`. It also resolves `RecoveryFence` as the exact closure
scope for an otherwise-authorized no-marker credential attach against DMR or
DCursor, with concrete wire arms in cases 49 and 51.
It additionally resolves the exact outer/inner wire registries, sender direction,
primitive widths, canonical decode classes, pre-capability rejection authority,
PF/PR/MR schema bounds, fixed recovery widths, and ordinary request/delivery
headroom. R18 resolves the generated `b_u=SR`, `Bm=4b_u`, and `Qb=8b_u`
durability profile and every affected fixture; exact `Qe=2`, `q_enroll`, and
occurrence closed forms; `Pstate=max_participant_state_bytes` and the four-and-
only-four retention-startup dimensions; the half-open participant-index range
with I as a non-identity exhausted sentinel; the disjoint ordinary-Q versus
K-claim-backed detached Leave classes; and the public-history versus four-case
test-seed classification.

Shared concepts have one definition and citations elsewhere:

| Shared concept | Sole normative definition |
|---|---|
| Binding epoch | R-A1 `BindingEpoch`; R-C0/R-C1/R-C5/startup recovery cite it. |
| Reserved counter capacity | R-A2's accepted-obligation claim principle; R-C2 and R-C4 apply it. |
| Movable claim frontier | R-A2 defines order ownership/validation; R-C2 applies the same relay to sequence claims; R-C4 stores only bounded existing handles/descriptors. |
| Marker capacity credit | R-C4 from planning through physical compaction; cases 45/48 cite it. |

The exhaustive-outcome pass searched every `returns`, `return`, `outcome`, and
new discriminant. `ParticipantParkingConfigurationInvalid` has nine dimension
classes (including all nine deterministically ordered `NonzeroLimit` fields)
and case-25 arms; parked
equivalents use explicit dimensions. Reconnect
delay has the fresh-fate/`ReconnectArmed` and no-fresh-fate/
`ReconnectNotArmed` triggers in case 50. Every trigger has an outcome and no
new outcome lacks a trigger/case. `ClaimFrontierInvalid` has value, gap, count,
logical-handle, whole-block, missing-suffix, counter-precedence, and exact-index
fault arms in case 54. `SdkObserverParkCapacityExceeded` has exactly
`PerConversation/Rows`, `PerConversation/Bytes`, `SdkWide/Conversations`,
`SdkWide/Rows`, and `SdkWide/Bytes`; `PerConversation/Conversations` is not
constructible. Case 34 supplies public histories for every normal-ack and
marker-proof selector branch. `RecoveryFence` is a scope of the existing
closure outcome, not a new family: cases 49/51 trigger it with exact tokens,
bodies, targets, Options, and quota inputs, while explicit bad-marker requests
take `MarkerNotDelivered`/`MarkerMismatch`. Detach has no unencodable secret arm.

Acceptance references remain contiguous 1–56. Every use of the test-seed
convention is enumerated here: case 42's two allocator-only header seeds, case
48's two observer-order snapshots after its stated public marker history, case
49's complete undelivered-marker snapshot, and case 56's two PCP-ordering
snapshots. Cases 21, 26, 31, 37, 43, 44, 45, 47, 51, 54, and 55 instead
construct their displayed boundary states through the stated legal public
histories; case 44's second branch is an independent copy of that public
checkpoint, not a seed. Each fixture names every quantity read by its
transition. Re-derived cases 21/25/26/31/37/43/44/45/47/48/
49/51/54/55/56 were arithmetically re-run against the displayed pre/post formulas; every
floor-moving step states cap_floor. The row-by-row invalidator, guillemet-socket,
and define-what-you-name audits find no dangling tag, outcome, case, section,
quantity, provenance tag, current owner, quartet, or superseded minimum. Idle
cost is closed, and
the 21-count LAW-1 sweep plus case 50 covers lexical and structural waits.
---

**Gate posture:** DESIGN DRAFT R18. All earlier-round decisions remain in force.
R18 preserves R17's A1–A7, R16's W1–W7, and R15's N1–N12 closures, and
additionally closes generated-codec constructibility, exact Qe=2 occurrence
totality, K-classified Leave accounting, the equality-enrollment continuation,
and public-history/test-seed classification. R17 introduced
movable order/sequence ownership, intact DCR intervals, cumulative multi-identity
K exposure, exact multi-marker churn charging, and the Case54 completion
framework; R18 closes its residual delayed-completion, episode-retirement, and
provenance/current-owner defects. R17 also supplies exact bidirectional payload/wire registries, physical
frame/recovery bounds, and pre-commit ordinary-delivery fit. R16's closures remain: exact pre-delivery,
delivered-marker, and no-marker fate edges; reachable minimum/farther full-K
release arms; planned-marker positional accounting; a checked occurrence bound
with a fixed-array, J-bounded supersession retarget plan; a production-triggered floor arm;
complete boundary snapshots with explicit public-history/test-seed
classification; and RE/WF in the nine-field nonzero-limit taxonomy.
Reviewer key plus Hermes Crumpet's liminal domain-owner key are still required;
until both turn, this document is not ratified and grants no implementation
authority.
