# P0-39 — the stage-8 receipt-capacity hybrid

**Lane:** p0-39 · **Branch:** `p0-39-caps-hybrid` off `77e4845` · **Status:**
built, gated, contract amendment PROPOSED (two-key process pending at the seat).

Tom's governing sentence, which this whole design is an attempt to obey
literally:

> **no configured number refuses an honest arrival.**

---

## 1. What was wrong

Stage 8 held five receipt/provenance scopes, each a hard admission gate that
refused with a typed `ReceiptCapacityExceeded` when full. Two failure shapes
came out of that, and they are different failures needing different repairs —
which is why the answer is a HYBRID rather than one uniform rule.

**Shape A — the shared pools refuse a stranger.** `LiveReceiptServer`,
`ProvenanceServer` and `ProvenanceConversation` are pooled across parties. An
honest third party, having consumed nothing, meets a number that somebody
else's churn filled. There is no version of that which is defensible: the
refused party cannot have caused it, cannot observe it, and cannot act on it.

**Shape B — the per-participant pools wedge their own owner.** The field
specimen: a participant's own committed rotations fill its own provenance
window, and the (N+1)th attach of *that same participant* is refused. Worse on
a cold boot, where the residue is replayed out of durable state and the
refusal greets the client at reconnect. In the live-receipt case the wedge is
even starker — the attach refused by the enrollment receipt is the very
operation that ENDS that receipt.

## 2. The ruled shape

| Scope | Before | After |
|---|---|---|
| `LiveReceiptServer` | gate | TTL-bounded, reporting tripwire |
| `ProvenanceServer` | gate | TTL-bounded, reporting tripwire |
| `ProvenanceConversation` | gate | TTL-bounded, reporting tripwire |
| `LiveReceiptParticipant` | gate | displacement window |
| `ProvenanceParticipant` | gate | displacement window |
| identity Server / Conversation | gate | **unchanged — still a gate** |

**TTL-only for the shared pools.** Retention is bounded by
`attach_receipt_ttl_ms` and `receipt_provenance_ttl_ms` alone. The reasoning
that has to survive this document: the shared pools are where an honest third
party meets a number someone else's churn consumed, and no configured refusal
is tolerable there. They get a runaway tripwire that REPORTS. Never a wall.

**Displacement for the per-participant scopes.** The configured number becomes
a WINDOW SIZE, not a refusal threshold. At a full window the OLDEST in-window
entry is displaced and the new entry always lands. The reasoning: per-participant
pressure is self-inflicted — your own churn displaces your own oldest
fingerprint — so the number bounds memory without refusing. This is also what
kills the field specimen: N committed rotations no longer refuse the N+1th
attach of the same participant, including on cold replay from durable state.

**Identity is out of lane.** `identity_slots`,
`max_retired_identity_slots_server` and every `IdentityCapacityExceeded`
behaviour are untouched beyond what the shrinking of the receipt algebra
mechanically required. The identity gate is a bound on permanent ordinals, not
on retention, and refusing to mint a *new* identity is not refusing an honest
arrival its own history.

## 3. Values — set at the seat, premises beside them

**The build ships MECHANISM ONLY.** The TTL durations, the two window sizes and
the three tripwire thresholds all survive as configured numbers, and they stay
config-owned: no in-tree constants, no serde defaults, every field required
(`config/types.rs:567@77e4845` — the house ships no defaults).

*History of this section:* the original ruling routed the concrete values to
Tom's desk for ratification before lock. Tom's word of 2026-08-13 (relayed by
the sequencer) struck that gate: the values are a technical call at this seat,
and "configured values with written rationale IS the lock." This section is
that lock. A minted value owns its premises — what it is derived from, when,
and what growth re-derives it.

| Field | Value | Premise | Re-derivation trigger |
|---|---|---|---|
| `attach_receipt_ttl_ms` | 3,600,000 (1h) | Deployed estate value, unchanged. Bounds the secret-bearing receipt window; long enough for any observed reconnect/recovery cycle (field recoveries settle in seconds–minutes), short enough that a leaked receipt body ages out within an operator's working hour. | A field recovery observed to need a receipt beyond 1h. |
| `receipt_provenance_ttl_ms` | 7,200,000 (2h) | Deployed estate value, unchanged. Must be ≥ receipt TTL (validated order); 2× gives classification a full receipt-lifetime of hindsight after the receipt itself expires. | A recovering client observed presenting a receipt older than 2h that deserved an exact terminal reason. |
| `max_live_attach_receipts_per_participant` (window) | 8 | Deployed estate value, semantics now a displacement window. A participant legitimately holds 2 live receipts (enrollment + current attach); 8 = 4× headroom for rotation bursts. Under displacement, exceeding it costs classification precision, never admission. | Displacement counters showing routine steady-state displacement in this scope for honest clients. |
| `max_receipt_provenance_per_participant` (window) | 256 | Deployed estate value (raised 64→256 on 2026-08-06 after the boot-wedge). The worst observed crash-loop specimen minted 234 generations in one replay burst; 256 covers it. Under displacement the number no longer gates boot — a burst past 256 displaces oldest fingerprints (coarser classification, counted loudly) instead of wedging. | The displacement counter firing on a burst that a *recovering* client then paid for in lost exact-reason classification. |
| `live_receipt_server_report_threshold` | 1,024 | The value the old design considered server live-receipt capacity (`max_live_attach_receipts_server`, deleted). Crossing what used to wall is exactly the churn-storm signal the tripwire exists to report. | Estate growth: more than ~128 routinely-live participants (1,024 / 8). |
| `receipt_provenance_server_report_threshold` | 4,096 | The old server provenance cap's value, same reasoning: the threshold at which the old design would have begun refusing third parties is precisely the level worth a loud report. | Estate growth: routine in-window provenance approaching 4,096 without any crash loop present. |
| `receipt_provenance_per_conversation_report_threshold` | 256 | The old per-conversation cap's value. The fossil specimen showed a single crash-looping participant can pace ~468 inserts through one 2h window — this threshold fires roughly halfway through such a burst, early enough to matter. | A legitimate (non-crash-loop) conversation observed crossing 256 in-window. |

Derived 2026-08-13 at the seat, from: the deployed estate config read at the
sequencer's bytes (2026-08-12), the 64/64 boot-wedge specimen and its 08-06
raise, and the fossil churn specimen (234 generations / 468 inserts, single
replay burst). The premises are the lock; changing a value without rewriting
its premise row is the silent-fallback failure this lane exists to end.

## 4. Design decisions this lane owned

### 4.1 The three shared config fields: DELETED, and replaced by named tripwires

Ruling carried into the build: *"a retained-but-ignored config field is a silent
fallback wearing a schema, and `deny_unknown_fields` refusing a stale estate
file is loud-and-correct."*

The measurement the ruling asked for — *is the field truly dead?* — came back
**no, but not under its own name.** The runaway tripwire needs a threshold per
pool, so a number survives for each of the three. What does not survive is the
NAME. `max_live_attach_receipts_server` says *maximum*; a field that no longer
refuses anything and merely decides when to log would be lying in its own
identifier, which is the same silent-fallback failure the ruling names, one
level down. So:

| Deleted | Added |
|---|---|
| `max_live_attach_receipts_server` | `live_receipt_server_report_threshold` |
| `max_receipt_provenance_server` | `receipt_provenance_server_report_threshold` |
| `max_receipt_provenance_per_conversation` | `receipt_provenance_per_conversation_report_threshold` |

Retained with their names unchanged, because *maximum* stays literally true —
occupancy never exceeds them — while the behaviour at the bound changes from
refusal to displacement:

* `max_live_attach_receipts_per_participant`
* `max_receipt_provenance_per_participant`

**Deployment consequence, stated plainly.** This is a config-compatibility
break on BOTH halves. A stale estate file fails to load because
`deny_unknown_fields` rejects the three removed keys, and fails again because
the three new keys are required and absent. Both failures are loud, typed and
name the field. The sequencer edits the live estate config in the same
estate-quiet restart window that already holds two other enablements, so the
estate file never meets the new binary without the matching edit.

### 4.2 Displacement order, and why it is clock-free

Displacement order is **oldest deadline first**, tie-broken by kind
(enrollment fingerprint before attach fingerprint) then by token bytes. That is
byte-for-byte the order the server ledger's own `OccupancyEntry` set already
sorts by within one participant, which is what lets the ledger and the slot
apply ONE plan and land on the same retained set.

Two structural guarantees hold the ledger and the slot together, rather than
two implementations agreeing by inspection:

* `Slot::incoming_provenance_member` — one definition of *which fingerprint
  this commit retains*, called by both the ledger's plan and the slot's commit.
* `Slot::plan_provenance_displacement` — one definition of *which members go*,
  called by both, over the same pre-commit slot.

**The plan bounds the PHYSICALLY retained set, not the in-window count, and it
never reads the clock.** Cold replay re-executes the same committed attaches in
the same durable order but under a much later clock; a plan consulting `now`
could not reproduce the live retained set. This one does — it is a function of
durable structure and the configured window alone.

The in-window survivors match either way, and this is the load-bearing
argument: expiry and displacement remove members in the SAME order. An expired
member has a smaller deadline than every in-window member, so oldest-first
sheds expired fingerprints before it ever touches a live one. A replayed slot
may therefore physically hold an expired fingerprint that a live slot had
already pruned, and still answer every classification identically. Pinned by
`a_cold_replay_retains_the_identical_window`, which compares the retained sets
member by member as SETS — two sets of equal size that disagree about which
fingerprint survived is exactly the drift a count would miss.

**A lowered window can no longer wedge a boot.** The old over-limit arm existed
because a cap lowered beneath restored durable occupancy had to refuse rather
than admit past a signed number. A window lowered beneath durable occupancy
simply displaces more on its next insert. The over-limit machinery survives for
identity only, where it is still the right answer.

### 4.3 The live-receipt window needs no eviction of its own

Worth writing down because it looks like a gap. A participant's live-receipt
occupancy is structurally at most one: `slot.attach.is_some()` implies
`enrollment_receipt_ended.is_some()`, both written only by
`install_attach_receipt`. And every live receipt that window counts is in the
committing attach's own `retire` set. So the entries a full live-receipt window
would displace are exactly the ones the commit already retires, post-commit
occupancy is exactly one whatever the window size is, and the algebra's
`Displaced` arm is satisfied by work the commit was doing anyway.

That is also the exact anatomy of the wedge: the old wall refused an attach
because of a receipt that same attach was about to end.

### 4.4 Classification degradation is a documented, pinned behaviour

An evicted fingerprint stops answering with its exact terminal reason and falls
through to the coarser classification. Written into the code the way board #37
wrote *"occupancy is not classification"*, and pinned explicitly:

* a displaced **enrollment** fingerprint degrades from `ReceiptExpired` (with
  its exact `Superseded`/`Deadline` reason) to the permanent lifetime mapping's
  `EnrollmentKnown`;
* a displaced **attach** fingerprint degrades from `ReceiptExpired` to the
  intentionally ambiguous `StaleOrUnknownReceipt`, which claims no commit
  proof — and NEVER to `StaleAuthority`, which would assert that no commit
  happened. For a displaced-but-committed rotation that assertion is a lie, and
  it is the one degradation worth guarding.

One trap avoided, and it is worth naming because the obvious refactor walks
into it: the displacement test at the enrollment classification site is
deliberately NOT the occupancy predicate. Occupancy additionally requires proven
possession (board #37), while a receipt that died by its own deadline with no
attach at all must still classify `Deadline` exactly as before. Folding the two
predicates together silently changes an unrelated contract row.

### 4.5 No sweeps

R-C0's never-a-sweep rule is a design law inside the code, not only a contract
sentence. There is no background eviction task. Every displacement happens at
insert time, inside the committing operation, under the same admitted clock
read that drives the operation.

## 5. Visibility — silent to experience, loud to record

Displacement is invisible to the arriving client BY DESIGN. The old wall, for
all its faults, was loud: a typed refusal the client could read. **A bound that
neither refuses nor discloses hides exactly what the wall at least made loud**,
so the replacement discloses to the operator instead.

| Family | Labels | Fires when |
|---|---|---|
| `liminal_receipt_displacements_total` | `scope` ∈ {`live_receipt_participant`, `provenance_participant`} | any entry is displaced |
| `liminal_receipt_pool_runaway_total` | `pool` ∈ {`live_receipt_server`, `provenance_server`, `provenance_conversation`} | an admitted operation observes that pool at or above its threshold |

Both follow the #56 pattern exactly: private name constants in `metrics.rs`,
bounded enum-derived label vocabularies, every label value pre-registered at
`init` so the exposition carries explicit zeros, cached handles indexed by
discriminant, and recording helpers that return early when idle.

**Counting versus warning.** The counter moves on EVERY observation — its rate
is the storm's rate. The `warn` fires only on the RISING EDGE, so a sustained
storm costs one log line rather than one per request, and a pool that recovers
and runs away again warns again. That trade is pinned rather than left to be
discovered (`a_sustained_storm_warns_once_but_keeps_counting`), because an
operator who sees one warning and a climbing counter needs to know they are
seeing a storm and not a single event.

The tripwire is an OCCUPANCY OBSERVATION, never a gate. The operation that
triggered it has already been admitted by the time the numbers are looked at.
`a_pool_below_its_threshold_reports_nothing` is the negative control: without
it, every green is consistent with a tripwire that fires unconditionally, which
is a counter that means nothing.

## 6. What is NOT in this lane

* **No wire change.** `ReceiptCapacityScope`, `EnrollmentReceiptCapacityScope`
  and the `ReceiptCapacityExceeded` rows stay defined and registry-assigned.
  The receipt scopes become *defined but unemitted*. Deleting or reordering a
  `u16_registry!` variant is wire-breaking and was never on the table.
* **No identity change.**
* **No new durable state.** The displacement flag on the slot is derived and
  re-derived by replay; nothing about displacement is persisted.

## 7. Honest gaps

1. **The values are unratified.** Mechanism only, by instruction. Until Tom
   rules the numbers, no deployment can be configured from this document.
2. **The rising-edge warn is per-process for the server pools and per-live-
   conversation for the conversation pool.** A conversation evicted from the
   registry and re-loaded re-arms its edge and may warn again for a storm it
   already warned about. Counted correctly either way; the log may repeat.
3. **The displacement counter is process-global and additive only.** It cannot
   answer *which* participant displaced — that is on the paired `debug` line,
   deliberately, because a per-participant label is unbounded cardinality on a
   surface a scraper keeps forever.
4. **One pin became unconstructible rather than merely vacuous.** The frozen
   first-full precedence ACROSS the model boundary (an in-model full scope
   answering before a later over-limit scope) can no longer be built: the only
   surviving later scope is identity Conversation, and lowering `identity_slots`
   beneath already-minted ordinals makes the conversation refuse to REPLAY —
   the protocol's slot allocator rejects an ordinal outside `0..I` during
   restore, long before stage-8 capacity is consulted. Measured, not assumed.
   The in-model half of the law is pinned in its place, and the unreachability
   is recorded at the pin's own bytes so no future reader mistakes its absence
   for an oversight.

## 8. Contract amendment

PARTICIPANT-CONTRACT.md §0.17 (R18 amendment A6), written as PROPOSED with a
status block under the house two-key pattern. The seat runs the key process
after this build. Row-by-row disposition lives in that section.

The advertisement mandate at :2176-2180 is STRUCK by ruling. The evidence row
records the measurement it rests on — **re-measured at this lane's bytes, and
corrected**, because the sizing pass's phrasing ("zero occurrences of the nine
field names in the protocol crate, 26-hit positive control") is not what a
fresh measurement returns:

* **Direct structural evidence, which is stronger than any grep.**
  `NegotiatedParticipantCapability` (`wire/codec.rs:85-88@77e4845`) has exactly
  two fields — `protocol_version` and `max_frame_bytes`. None of the nine is
  advertised in negotiated participant capability state, because that state has
  no room for them. The mandate was never built.
* **Grep corroboration, stated accurately.** Seven of the nine names have zero
  occurrences anywhere in `crates/liminal-protocol/{src,tests}`. The remaining
  two — `attach_receipt_ttl_ms` and `receipt_provenance_ttl_ms` — occur twelve
  times each, but ONLY as function parameters and validation-error fields in
  the deadline algebra (`lifecycle/operations/enrollment_operation.rs`,
  `outcome/startup.rs`), never in capability state. "Zero occurrences of the
  nine" is therefore false as written; the claim it was reaching for is true.
* **Positive control**, so the absence is a measurement and not a broken
  instrument: `ReceiptCapacityScope` returns 54 hits over the same two trees.

Surviving informational values disclose through deployment config and the truth
report, not through negotiated capability state.

## Revision record

| Rev | Date | Author | Change |
|---|---|---|---|
| r1 | 2026-08-13 | seat/lane p0-39 | Initial design record for the ruled hybrid: TTL-only shared pools with reporting tripwires, per-participant displacement windows, the delete-and-rename config decision with its deployment consequence, clock-free replay-equivalent displacement ordering, the pinned classification degradation, and the visibility surface that replaces the wall. |
| r2 | 2026-08-13 | seat (Hermes) | §3 rewritten: the values-to-Tom ratification gate struck by Tom's own word (relayed 2026-08-13); values set at the seat with premises and re-derivation triggers beside them — configured values with written rationale is the lock. All seven values derived; none changed from deployed/prior numbers. |
