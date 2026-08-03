# F8 — a drained compaction marker permanently poisons the participant log

**Design pass.** Author: Hermes Crumpet (liminal seat — designs, cannot compile).
Incident owner: Waffles the Terrible (Manifold spine, 2026-07-31); diagnosis by
their Opus agent, store excavated to the row level. Execution goes to an
executor seat; **the code is the authority where this note and the code
disagree — stop and report, do not improvise.**

**ANCHOR — `09cfa49`.** Every coordinate below was re-derived AT THE BYTES of
that commit (repo main, F8B repo chain closed), never patched by arithmetic.
The original measurement pass ran at `92e65ce`; that commit stays cited as the
historical record of when each claim was first read, not as a live coordinate —
history is not edited, the anchor moves. The re-anchor's own control:
`git diff --numstat 92e65ce 09cfa49 -- crates/liminal-protocol` returns EMPTY,
so the whole protocol crate is byte-identical across the two commits and every
protocol coordinate here is unchanged by construction rather than by luck. The
server-side coordinates were each re-read individually, because
`crates/liminal-server/` did move.

Versions: liminal-server 0.5.1 published · liminal-protocol 0.3.2 · haematite
0.7.0. Repo main is AHEAD of the published server at `09cfa49` — the F8B chain
(boot drain, R-BOOT-VERDICT, typed-refusal carrier, R-SEAL) is landed and
unreleased.

---

## 1. THE INCIDENT, ONE PARAGRAPH

A compaction marker for P1 is minted and drained (retained record at delivery
seq M in `ClaimFrontiers::marker_records`); P1 never MarkerAcks (killed), its
cursor stays below M. P0 departs; its Ordinary fate succeeds because P1's cursor
still holds the floor below M — but the departing participant's cursor is set to
the high watermark, now **past M**. P1 then drops: the server **durably appends
P1's Died row carrying an open Ordinary intent, and only then measures** — the
floor computation (blind to markers) yields a floor past M, the transition
refuses it for crossing the marker (`Precedence`), and the refusal is collapsed
to `OwnerTransition` with the cause discarded. **The Died row is already
durable with an undischargeable intent. Every subsequent boot dies in
`repair_pending_specific_fates` (called at `production/handler.rs:482`, inside
`replay_and_repair`; surfaced as `ServerError::ParticipantStartupRestore` at
`connection/services.rs:391`) before the server reaches listen.** One-row
excision is a trap: `repair_unclean_server_restart` re-mints the same poisoned
Died on the next boot. No in-band remedy exists.

## 2. THE TWO DEFECTS — MEASURED

**(A) INVARIANT SPLIT — the floor is computed blind to the markers the
transition refuses to cross.** `validate_binding_fate_floor`
(`crates/liminal-protocol/src/lifecycle/operations/binding_fate.rs:382`,
**private fn** — the declaration is a bare `fn`, no `pub`, re-read at `09cfa49`)
computes `minimum_remaining_cursor` over the *other* participants (`:428-435`)
and calls `floor_transition(..)` at `:436-442` with **no marker input
anywhere**: its five arguments are `retained_floor`, `minimum_remaining_cursor`,
`candidate_high_watermark`, `hard_observer_progress`, `retained_floor` again —
not one of them is a marker record. Downstream, the live-frontier transition
refuses a floor that crosses a retained marker (`LiveFrontierError::Precedence`,
`live_frontier.rs:1052`). Two halves of one invariant, each enforcing its half
against the other; **neither clamps.**

**(B) COMMIT-BEFORE-VALIDATE.** In
`crates/liminal-server/src/server/participant/production/connection_fate.rs`,
the durable append (`appender.append(&completed.operation, ..)`, **`:256`**)
precedes the fate measurement (`authority.complete_pending_specific_fate(..)`,
**`:368`**). The ordering is not merely numeric and was re-walked at `09cfa49`:
`complete_target` appends at `:256`, then calls `open_specific_fate` at `:296`,
which is where `:368` lives — one call path, append first. A refusal at `:368`
is therefore permanent by construction: the intent it refuses to discharge is
already durable.

**(TAX) THE CAUSE IS DISCARDED AT THE BOUNDARY.** `binding_fate.rs:373` is
literally `.map_err(|_| BindingFateMeasurementError::OwnerTransition)` — five
distinct `LiveFrontierError` causes collapse to one name. The five, counted at
the bytes of `prepare_binding_fate_transition`
(`crates/liminal-protocol/src/lifecycle/operations/live_frontier/binding_fate_transition.rs:15-58`):
`RetainedCharge` (`:34`), `ClosureAccounting` (`:53`), and — through
`map_frontier_error` at `:45` — `Authority`, `Precedence`, `Frontier`. This is
what turned a one-minute diagnosis into a store excavation.

## 3. THE FIX — THREE PIECES

### 3.1 Markers PIN the floor (defect A)

Include the **minimum retained marker-record sequence** as a cap in the floor
computation, so the measured floor can never cross a retained marker. The
purpose of a retained marker is precisely that its record stays replayable
until acked — **pinning is the marker's meaning; unsatisfiability was the bug.**

Implementable with existing surface, MEASURED at `09cfa49`: the floor fn already
holds `owner: &LiveFrontierOwner`; `LiveFrontierOwner::frontiers()` is public
(`pub const fn`, `live_frontier.rs:162`) and
`ClaimFrontiers::retained_marker_records()` is public (`pub fn`,
`claim_frontier.rs:2214`). `validate_binding_fate_floor` already reaches
`owner.frontiers()` six times in its own body (`:391`, `:395`, `:424`, `:429`,
`:437`, `:441`), so the reach costs nothing new. The cap is:

```
floor_cap = min over retained marker records of (marker seq)   // if any
resulting_floor = min(resulting_floor, floor_cap)              // clamp
```

**The `Precedence` refusal STAYS, as a backstop invariant** — after this fix it
should be unreachable from this path, and a reachable backstop firing is a bug
report, not control flow.

### 3.2 Append conditional on successful prepare (defect B)

Reorder `connection_fate.rs` so the binding-fate measurement/prepare runs
**before** the durable Died append, and the append happens **only on a
successful prepare**. A refused measurement must leave **no durable residue** —
the connection drop is then re-processable rather than poisonous.

Builder note: the current order may exist because the Died row is the durable
*source* under which the fate completes. If the code shows a hard reason the
source append must precede measurement, **stop and report** — the fallback
design is that a refusal must actively discharge or annul the intent it
strands, but that is a different, worse design and it is not to be built
silently.

**⚡ AMENDMENT (2026-08-03, Cally 62b5ceb2 / b5644caa) — THE MECHANISM IS A
SPLIT, NOT A REORDER. The requirement above is UNCHANGED; only how it is built
moves.** Struck text stays readable: the "reorder" instruction above is
superseded, not erased, and the builder note above is RESOLVED rather than
deleted, because its resolution is a measurement.

**THE BUILDER NOTE'S STOP CONDITION WAS TESTED AND DID NOT FIRE.** It asked
whether the code shows a hard reason the source append must precede
measurement. Measured at `binding_fate_completion.rs:86-89`:
`prepare_binding_fate` takes exactly three inputs — the owner from
`take_frontier()`, `pending.token`, and `self.observer_progress`. All three are
in memory; it reads **no durable state**, so the measurement cannot depend on a
row it never consults. Corroborating, the refusal path at `:92-111` already
restores the frontier and re-inserts the pending fate: recoverable by
construction, which is only coherent if measurement is pure. The stop-and-report
instruction was followed and the answer is recorded here rather than removed.

**WHAT THE STOP DID SURFACE — a second constraint the original text did not
anticipate.** `binding_fate_completion.rs:52-53` states the invariant in the
code's own words: the specific row "appends its exact specific row **after the
owning Died source is durable**." That binds the COMPLETION APPEND, not the
measurement. The two are welded into one function, `complete_pending_specific_fate`,
which has THREE callers:

| caller | path | Died row at call time |
| --- | --- | --- |
| `connection_fate.rs:368` | LIVE — §3.2's target | NOT yet durable, once reordered |
| `binding_fate_completion.rs:318` | boot, `repair_pending_specific_fates` | already durable |
| `binding_fate_completion.rs:390` | boot | already durable |

The live path needs measure-then-append; both boot paths need the combined form
over an already-durable row, where it is **correct and stays**. A pure reorder
cannot serve both. Hence: SPLIT.

**THE PRESCRIPTION.** `admit_terminal` already returns `admitted.owner` as a
VALUE, so the measurement runs on that value before any install and before any
append:

1. compute allocations
2. `admit_terminal` → `admitted`
3. MEASURE on `admitted.owner` — pure, durable-free
4. **REFUSED** ⇒ return `Err` with **NOTHING appended and NOTHING installed**
5. **PREPARED** ⇒ append the Died row, append the completion row, **THEN**
   install the prepared owner

**THE SHAPE'S OWN INVARIANT: APPENDS PRECEDE INSTALLS, THROUGHOUT.** Step 5 is
ordered that way deliberately. Installing transitioned state before its durable
row would be the MIRROR of the defect this section exists to remove — memory
ahead of disk instead of disk ahead of validation — and trading one for the
other is not a fix. The boot callers keep the combined form unchanged.

⛔ **SUPERSESSION, scoped honestly.** What is superseded is this section's
MECHANISM only: "reorder `connection_fate.rs`" → "split the measurement from the
completion append." §3.2's requirement sentence — *a refused measurement must
leave no durable residue* — is untouched, and so is every other section. In
particular this amendment makes **no claim about §3.3's carrier idiom**, which
is NOT new to the re-anchor: `OwnerTransition → OwnerTransition(LiveFrontierError)`
is present in the pre-re-anchor text of this document with its `Copy`/derives
measurement and its `live_frontier.rs:1541` citation. What the re-anchor added
to §3.3 was the RULING form — the landed-carrier table and the
do-not-mint-a-second-idiom instruction — not the payload itself. Three of four
stated absences were measured; the fourth was inferred from a section title and
is corrected here rather than carried.

### 3.3 Carry the cause — RIDE THE LANDED CARRIER, DO NOT MINT A SECOND IDIOM

`OwnerTransition` → `OwnerTransition(LiveFrontierError)`. **This is a named
requirement of the fix, not a nicety.**

**The substance, re-verified at `09cfa49`.** The payload is free: the derives
survive because `LiveFrontierError` is `Copy` (`live_frontier.rs:1047`, the
enum at `:1048` deriving `Clone, Copy, Debug, PartialEq, Eq`), and so is
`BindingFateMeasurementError` itself (`binding_fate.rs:132`, the same five
derives — checked, because a payload is only one line if BOTH sides are
`Copy`). And the inner error already reaches the boundary: `map_frontier_error`
maps `LiveFrontierTransitionError::Precedence → LiveFrontierError::Precedence`
at `live_frontier.rs:1541`, that value travels out of
`prepare_binding_fate_transition`, and `binding_fate.rs:373` throws it away one
line later. The cause is not being *recovered* by this fix; it is being
*stopped from being discarded*.

**The form is no longer an open question — F8B landed the idiom at this exact
seam, and the builder copies it rather than inventing a parallel one.** The
landed carrier, `BindingTerminalAdmissionRefused`, runs a four-site chain that
this fix mirrors one-for-one:

| F8B's landed carrier (`09cfa49`) | site | F8's counterpart |
| --- | --- | --- |
| protocol reason CAPTURED at the refusal seam, not formatted | `connection_fate.rs:469-471` (`refused.error()` goes straight into the variant) | `binding_fate.rs:373`, where today the closure argument is `\|_\|` |
| server error variant carries it BY TYPE | `production/state.rs:290` (`StateError::BindingTerminalAdmissionRefused { error: BindingTerminalAdmitError }`) | the `OwnerTransition` payload |
| preserving conversion, not a format string | `production/handler.rs:676-677` (`state_error` returns the typed variant BEFORE the `Internal { message: format!(..) }` fallback at `:686-688`) | same seam, same shape |
| semantic-boundary variant the consumer branches on | `participant/dispatch.rs:192` (doc rationale at `:182-190`, cross-reference at `:145-148`) | same |

The discipline the pattern establishes, in its own words at `state.rs:286-288`:
the reason "must survive this seam by type, because the park-versus-fatal
decision downstream is not allowed to read it back out of a formatted message."
The live consumer demonstrates why — `connection_fate_dispatch.rs:39-44`
matches on `BindingTerminalAdmissionRefused { error:
BindingTerminalAdmitError::Precedence }` structurally, which is exactly the
decision F8's collapsed `OwnerTransition` makes impossible today.

So the build is: **add the payload at the protocol boundary**
(`BindingFateMeasurementError::OwnerTransition` gains the inner
`LiveFrontierError` — that is the §4.1 major), and **carry it server-side
through the conversion discipline already in the tree**, not through a new
mechanism. `handler.rs`'s `state_error` is the one function that decides which
refusals stay typed; F8's reason joins the two that already do, and any new
`StateError`/`ParticipantSemanticError` variant is shaped after `state.rs:290`
and `dispatch.rs:192`.

**Test precedent comes with it.** `production/tests_f8b_typed_refusal.rs` (297
lines) is the shape of §5's §3.3 red-first unit: one positive pole (`:208-209`,
the refusal reaches the recovery consumer typed) and TWO negative poles —
`:230-231`, a failure that is not an admission refusal must not wear the
carrier, and `:276-277`, a non-`Precedence` refusal keeps its own reason. A
carrier that answers "yes" to everything is not a carrier, and the positive
pole alone cannot detect that.

## 4. VERSION CONSEQUENCES — AND THE TWO PURCHASES ARE SEPARABLE

### 4.1 liminal-protocol 0.3.2 → 0.4.0 — the MAJOR is §3.3, nothing else

- §3.1 is a **private fn** — no API surface moves. Free.
- §3.2 is **server-side only**. Free for the protocol crate.
- §3.3 changes a variant of `BindingFateMeasurementError`: **public enum
  (`binding_fate.rs:133`), re-exported at two levels (`operations/mod.rs:37`,
  `lifecycle/mod.rs:213`), still NO `#[non_exhaustive]`** — the only attribute
  above it at `:132` is the derive ⇒ major, per the toolchain rule ("adding new
  fields to an enum variant").

**Mitigation, census RE-RUN at `09cfa49` WITH ITS CONTROL — and the control
itself was corrected.** `git grep -n "BindingFateMeasurementError" 09cfa49 --
crates sdks tests scripts config` — every tracked code path, all four crates
plus both SDKs — returns **20 hits in exactly 3 files**: 18 in the defining
file `crates/liminal-protocol/src/lifecycle/operations/binding_fate.rs`, and
one apiece on the two re-export lines above (`lifecycle/mod.rs:213`,
`operations/mod.rs:37`). The ref the census saw is named because a census that
does not name its refs is not a census: the pathspec is resolved against the
commit `09cfa49`, not the working tree. **Zero consumers outside
`crates/liminal-protocol/`, and zero even in the protocol crate's own tests.**
(Widening to the whole tree adds only this document's own citations.)

This was the claim most at risk from the re-anchor, because F8B landed
**+1,956 / −82 lines across 19 files under `crates/liminal-server/`** between
`92e65ce` and `09cfa49` (`git diff --numstat`), any of which could have become
the first consumer. It survived.

The control — a census that finds nothing has to prove it can find something —
is **`BindingFateTerminal`, which the identical search style finds used in
server files** (`production/binding_fate_completion.rs:4`, `:451`, `:454`,
`:459`, `:462`). **Membership predicate, stated because two honest counters
disagreed on it:** those five are WHOLE-IDENTIFIER matches (`git grep -nw`).
A substring counter returns 8 raw server lines, because it sweeps in
`BindingFateTerminalRestore` (`fenced_attach_codec.rs:5`, `:311`, `:321`) — a
DISTINCT type that merely carries this one's name as a prefix. A count of a
spelling is not a count of a construct; the control's liveness — the only
thing this number is for — is robust under either predicate, but the
enumeration above is the identifier's, not the spelling's. (The main census is
directionally safe against the same trap: a substring sweep can only
OVERcount consumers, and it still found zero.) ⚠️ The pre-`09cfa49` text of this section also named
`BindingFateMeasurementRefused` as a control. **That was wrong and is
withdrawn.** Re-run at both commits, `BindingFateMeasurementRefused` appears
in server files at NEITHER (`git grep -n BindingFateMeasurementRefused 92e65ce
-- crates/liminal-server` exits 1, and so does the same search at `09cfa49`) —
it is protocol-internal exactly like the enum it was supposed to discriminate
against, so as a positive control it was dead, and a dead control licenses
nothing. One live control remains, which is enough; the false one is named here
rather than quietly deleted.

The break is formal, not felt.

### 4.2 The rider: the judged `#[non_exhaustive]` pass comes due in this cut

A breaking liminal-protocol bump fires `scripts/exhaustiveness-gate.py`, which
refuses the cut without an attribute decision on file. This is task #34's
window: **268 public enums, 0 attributes.**

That figure is not a historical citation — **it was RE-MEASURED at `09cfa49`
during this re-anchor** by running the gate itself, which needs no cargo (plain
`python3`; it reads the manifest with `tomllib`, censuses with `git grep` at
`HEAD`, and fetches the registry index over `urllib`). Verdict at the cut:

```
census controls: POSITIVE 336 `pub struct` found | NEGATIVE 0 hits for an
impossible token | PARTITION 5/5
  in tree   0.3.2  (breaking series (0, 3))
  published 0.3.2  (breaking series (0, 3)), 5 unyanked of 5
  268 `pub enum` declarations, 0 `#[non_exhaustive]`
PASS: not a breaking bump ((0, 3) <= (0, 3)). The ride is still available.
```

Exit 0. The gate PASSES today precisely because the bump has not happened yet —
**§3.3 is the change that flips it**, and at that moment 268 enums carrying 0
attributes becomes a RED with no decision on file. The pass is JUDGED, not
blanket — wire-codec enums whose exhaustive matching is load-bearing get a
written refusal at `docs/gates/EXHAUSTIVENESS-REFUSAL.md`, not the attribute;
the gate accepts either, and accepts silence from neither.

**⚠️ SEPARABILITY (Waffles' review constraint, adopted): the two-defect fix
(§3) and the attribute pass each stand on their own evidence and are reviewed
independently, so a question about one cannot stall the other. They ship under
the one 0.4.0 because the major is the moment both are free — but they are two
deliverables, two review threads, one release.**

### 4.3 liminal-server — bump class OPEN, with the check named

The §3.2 reorder is an internal behaviour fix (patch-class alone). Consuming
protocol 0.4.0 forces a server major **only if a changed protocol type appears
in server's public API**. The zero-consumers measurement suggests it does not,
but the builder runs the leak check at the crate surface before the version is
chosen. **Do not inherit this note's suggestion as the decision.**

## 5. VERIFICATION — THE BEAT HAS RUN, AND IT IS RED

**This section no longer predicts. A live beat fired on 2026-08-01 (fire order
Waffles `2f5dc27e`) against both preserved incident stores, on both binaries,
and MEASURED what the earlier text expected.** What follows is the beat's
record; the design's obligations are restated underneath it as the build leg's
predicate.

### 5.1 What was run, and against what

The preserved stores — the spine-poison store
(`apps/manifold/.manifold-backup-20260731-spine-poison/`, excavated to the row
level: `oplog-extraction.json`, 194 rows decoded, poison at physical rows
166-170 of conversation 1) and the incident-2 intent-82 store — give a
before/after pair no synthetic fixture can match, because they are the actual
incidents. **The beat ran on handoff COPIES, never on those originals**, with
fixture identity fixed by deterministic tree hash before and after. Those paths
are cited from the incident owner's record and are deliberately NOT re-read at
this seat: the store trees hold sensitive residue, and identity here is carried
by hash, which is the stronger claim anyway:

| fixture | tree hash | files |
| --- | --- | --- |
| spine-poison-copy | `86ae3a8f5edf0362cadfcdf6e5aed78a4696587d4f97fefda7e429086d7f55c1` | 852 |
| intent82-copy | `f7effebf8b85bbaaa1e6291608f8cedc12135f56ffa09d697ff852a939b34101` | 682 |

**Post-beat re-hash matched on both — the fixtures were not mutated by the
runs.** Instrument note, load-bearing: these hashes reproduce over `shasum`
FULL output lines (digest **and** `./relpath`), never over a bare digest list.
A digest-only census silently survives a file being renamed or moved, which is
exactly the mutation a forensic fixture must not be allowed to hide.

Binaries under test, both identified by sha256 and size:

| binary | sha256 | bytes |
| --- | --- | --- |
| liminal-server 0.5.1 (published) | `f3fdb80f4ba58eda6a1c05426c58e3ff8d80c6e057a2fdccc28571ea995538c7` | 17,999,264 |
| fixed-for-F8B, built at `09cfa49` | `9498e21775351addadda1ef62a66bce71698b1cdc6b0bc7af24d5b8b4ea077e9` | 18,020,656 |

### 5.2 Negative control — PASSED, so item 1 below is MEASURED, not predicted

On 0.5.1, each store, **two runs each, deterministic**:

- **intent82** refused with `ParticipantIncarnation` / "Open 82 failed before
  Complete" / binding-terminal admission refused: `Precedence` — incident 2's
  own chain.
- **spine** refused in `ParticipantStartupRestore` — **this document's
  incident-1 chain, §1's exact failure.**

The control did its job: the copies still carry their poison, and the harness
can see it. A RED from the fixed binary therefore cannot be a dead harness.

### 5.3 The fixed-for-F8B binary: BOTH STORES RED ON `OwnerTransition`

Two runs each, structural lines byte-stable across runs:

```
intent82:
Error: participant incarnation unclean-server-restart repair failed: participant semantic service failed: participant production operation failed: participant production invariant violated: binding-fate measurement refused: OwnerTransition

spine:
Error: participant startup restore failed: participant semantic service failed: participant production operation failed: participant production invariant violated: binding-fate measurement refused: OwnerTransition
```

Read the intent82 line carefully, because it is the most informative result of
the beat. **F8B's boot drain FIRED** (`drains=1 sealed=false`) and boot advanced
PAST incident-2's failure stage into unclean-server-restart repair — where it
hit **this** document's defect class. That is F8B working exactly as designed
and then handing the store to a defect F8B was never scoped to fix.
**F8B is necessary but not sufficient; F8's fix is now the last blocker for
BOTH stores.**

Note also what the two lines have in common and what it costs: both terminate
on the bare name `OwnerTransition`, with no cause attached. §3.3 is not the
tax's cosmetic tail — it is why two different stores, failing through two
different boot stages, produce error text that cannot tell an operator which of
five `LiveFrontierError` causes fired.

### 5.4 The red predicate for the build leg

**Those two lines ARE the red.** The acceptance observation re-runs the same
protocol — same handoff copies, same reference tree hashes, fresh duplicates
per run — with a binary carrying **F8B + F8**, and:

1. **Old binary (0.5.1), copy of the store: fails restore IDENTICALLY.**
   MEASURED 2026-08-01 (§5.2), not assumed. Re-run it anyway as the negative
   control of the acceptance beat: a control is a thing you run, not a thing
   you cite.
2. **Fixed binary, both stores: boot to LISTENING** — pending intent
   discharged, P1's marker still retained until acked. This is the green.
3. **If the fixed binary REFUSES instead of restoring, that is a STOP** — it
   means repair does not re-measure the way this note believes, the ruled
   direction is wrong at the mechanism, and the design comes back here. The
   ruling is unchanged by the beat; a REFUSES outcome is still a STOP, not a
   partial credit.

**RULED DIRECTION — the fixed binary RESTORES the poisoned store.** Basis,
re-verified at `09cfa49` rather than carried over: boot repair
`repair_pending_specific_fates` (`binding_fate_completion.rs:296`) calls
`complete_pending_specific_fate` at `:318`, and that method has exactly ONE
definition (`binding_fate_completion.rs:40`) — the same one the live path calls
at `connection_fate.rs:368`. Boot repair and the live path are not two
implementations that happen to agree; they are one function. So §3.1's cap
makes the previously-unsatisfiable measurement satisfiable on the boot path by
construction: the floor clamps to the marker, the intent discharges, boot
reaches listen.

Plus red-first units, one per piece: §3.1 — a fixture with a retained
unacked marker and a departing peer must measure a floor ≤ M (fails today with
`Precedence`-turned-`OwnerTransition`); §3.2 — a refused measurement must
leave **zero appended rows** (fails today: Died row present); §3.3 — a
`Precedence` refusal must surface `OwnerTransition(Precedence)` (fails today:
payload absent), tested in the three-pole shape §3.3 borrows from
`tests_f8b_typed_refusal.rs`. And the no-new-poison property: after §3.1+§3.2,
the incident sequence replayed from clean produces a discharged fate and a live
boot.

### 5.5 Config closure — the residual caveat is DISCHARGED

The beat's boot config was reconstructed from the manifold app source, which
left a standing caveat: a RED could in principle have been an artefact of a
harness config that diverged from the live server's. **That caveat was
discharged on 2026-08-01 by the incident owner (Waffles, message `2ba71e19`):
the live spine config's `[participant]` block was diffed against the beat
harness block — 24/24 lines identical, zero drift, and stable across the live
config and both forensic backups.** Nothing about the RED verdict is
attributable to config divergence.

### 5.6 ⚠️ SCOPE OF THIS GREEN — MANDATORY NOTE

**F8B is LANDED.** The repo chain closed at `09cfa49` (see
[`F8B-INTENT-DEADLOCK.md`](F8B-INTENT-DEADLOCK.md) §6.6 R-SEAL and §9). That
changes the SCOPE note's tense but not its rule.

Everything in §§5.1-5.4 is evidence about **INCIDENT 1's defect class**. It
does not cover the connection-fate intent deadlock (incident 2), which fails at
a different boot stage (`ConnectionIncarnationAuthority::startup`, not handler
construction), surfaces a different error (`ParticipantIncarnation`, not
`ParticipantStartupRestore`), and refuses on a guard with NO FLOOR INPUT — so
§3.1's marker cap is provably inert on that path and §3.3's repair never reads
the immutable-candidate lane. That separation is what the beat confirmed from
the other side: F8B's landed drain cleared incident-2's stage on intent82 and
the store STILL died, on F8's defect.

The two documents remain **separate review threads** with separate evidence.
What the beat changed is that **the acceptance observation is now shared**: the
green is the F8-CHAIN-complete observation — *both stores boot to LISTENING on
one binary carrying F8B+F8* — and that is the observation on which the estate's
release pins unkey. **A green here must never be claimed from F8B evidence
alone, and a green there must never be claimed from F8 evidence alone.** One
shared observation is not one shared proof.

## 6. WHAT THIS NOTE IS NOT

Not compiled, not tested, written at a seat that can do neither — and the
re-anchor pass that produced the `09cfa49` coordinates was likewise doc-class:
every construct was read at the bytes, no cargo command of any kind was run.
The one instrument executed was `scripts/exhaustiveness-gate.py`, which needs
no toolchain (§4.2).

**Line numbers are MEASURED at `09cfa49` — the re-anchor HAS run, so the
builder does not inherit stale coordinates from `92e65ce`.** What has not
changed is the reason the instruction exists: any commit after `09cfa49` drifts
them again, and this document's coordinates are a map, never an authority. The
builder re-verifies each named construct at its own base before building, and
where a construct has moved crates or changed shape rather than merely moved
lines, that is a STOP and a report — not a patch to this note.

Downstream: manifold-node re-pins on landing (Waffles' committed sweep —
protocol 0.3.2 / server 0.5.1 / sdk 0.5.1 move on the design landing, manifold
is the willing early adopter as the live victim). The pins unkey on the
F8-CHAIN-complete observation of §5.6, not on this document landing.
