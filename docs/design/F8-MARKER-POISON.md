# F8 — a drained compaction marker permanently poisons the participant log

**Design pass.** Author: Hermes Crumpet (liminal seat — designs, cannot compile).
Incident owner: Waffles the Terrible (Manifold spine, 2026-07-31); diagnosis by
their Opus agent, store excavated to the row level. **Every claim below marked
MEASURED was independently re-verified at this seat at `92e65ce`** (server and
protocol files untouched by the open feature branch). Execution goes to an
executor seat; **the code is the authority where this note and the code
disagree — stop and report, do not improvise.**

Versions: liminal-server 0.5.1 · liminal-protocol 0.3.2 · haematite 0.7.0.

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
`repair_pending_specific_fates` (`production/handler.rs:461`,
`ParticipantStartupRestore`) before the server reaches listen.** One-row
excision is a trap: `repair_unclean_server_restart` re-mints the same poisoned
Died on the next boot. No in-band remedy exists.

## 2. THE TWO DEFECTS — MEASURED

**(A) INVARIANT SPLIT — the floor is computed blind to the markers the
transition refuses to cross.** `validate_binding_fate_floor`
(`liminal-protocol/src/lifecycle/operations/binding_fate.rs:382`, **private
fn**) computes `minimum_remaining_cursor` over the *other* participants and
calls `floor_transition(..)` with **no marker input anywhere** (body read at
`:424-441`). Downstream, the live-frontier transition refuses a floor that
crosses a retained marker (`LiveFrontierError::Precedence`,
`live_frontier.rs:1052`). Two halves of one invariant, each enforcing its half
against the other; **neither clamps.**

**(B) COMMIT-BEFORE-VALIDATE.** In
`liminal-server/src/server/participant/production/connection_fate.rs`, the
durable append (`appender.append(&completed.operation, ..)`, **`:256`**)
precedes the fate measurement (`authority.complete_pending_specific_fate(..)`,
**`:368`**). A refusal at `:368` is therefore permanent by construction: the
intent it refuses to discharge is already durable.

**(TAX) THE CAUSE IS DISCARDED AT THE BOUNDARY.** `binding_fate.rs:373` is
literally `.map_err(|_| BindingFateMeasurementError::OwnerTransition)` — five
distinct `LiveFrontierError` causes collapse to one name. This is what turned a
one-minute diagnosis into a store excavation.

## 3. THE FIX — THREE PIECES

### 3.1 Markers PIN the floor (defect A)

Include the **minimum retained marker-record sequence** as a cap in the floor
computation, so the measured floor can never cross a retained marker. The
purpose of a retained marker is precisely that its record stays replayable
until acked — **pinning is the marker's meaning; unsatisfiability was the bug.**

Implementable with existing surface, MEASURED: the floor fn already holds
`owner: &LiveFrontierOwner`; `owner.frontiers()` is public
(`live_frontier.rs:162`) and `ClaimFrontiers::retained_marker_records()` is
public (`claim_frontier.rs:2214`). The cap is:

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

### 3.3 Carry the cause (the tax)

`OwnerTransition` → `OwnerTransition(LiveFrontierError)`. **One-line payload,
derives survive** (MEASURED: `LiveFrontierError` is `Copy`,
`live_frontier.rs:1047`; the carrying enum derives
`Clone, Copy, Debug, PartialEq, Eq`). The inner error already reaches the
boundary (`LiveFrontierTransitionError::Precedence → LiveFrontierError::
Precedence`, `live_frontier.rs:1541`); today it is thrown away one line later.
**This is a named requirement of the fix, not a nicety.**

## 4. VERSION CONSEQUENCES — AND THE TWO PURCHASES ARE SEPARABLE

### 4.1 liminal-protocol 0.3.2 → 0.4.0 — the MAJOR is §3.3, nothing else

- §3.1 is a **private fn** — no API surface moves. Free.
- §3.2 is **server-side only**. Free for the protocol crate.
- §3.3 changes a variant of `BindingFateMeasurementError`: **public enum,
  re-exported at two levels (`operations/mod.rs:37`, `lifecycle/mod.rs:213`),
  NO `#[non_exhaustive]`** ⇒ major, per the toolchain rule ("adding new fields
  to an enum variant").

Mitigation, MEASURED WITH A CONTROL: **zero in-estate consumers reference the
enum** — and the emptiness is real, because the same search finds its sibling
re-exports (`BindingFateMeasurementRefused`, `BindingFateTerminal`) in server
files where they *are* used. The break is formal, not felt.

### 4.2 The rider: the judged `#[non_exhaustive]` pass comes due in this cut

A breaking liminal-protocol bump fires `scripts/exhaustiveness-gate.py`, which
refuses the cut without an attribute decision on file. This is task #34's
window: **268 public enums, 0 attributes today.** The pass is JUDGED, not
blanket — wire-codec enums whose exhaustive matching is load-bearing get a
written refusal, not the attribute.

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

## 5. VERIFICATION — THE FORENSIC PAIR IS THE SPINE

The preserved store
(`apps/manifold/.manifold-backup-20260731-spine-poison/`, with
`oplog-extraction.json`, 194 rows decoded, poison at physical rows 166-170 of
conversation 1) gives a before/after pair no synthetic fixture can match,
because it is the actual incident.

**RULED DIRECTION — the fixed binary RESTORES the poisoned store.** Basis,
MEASURED: boot repair (`binding_fate_completion.rs:296`) calls the **same**
`complete_pending_specific_fate` as the live path, so §3.1's cap makes the
previously-unsatisfiable measurement satisfiable — the floor clamps to the
marker, the intent discharges, boot reaches listen. Therefore:

1. **Old binary (0.5.1), copy of the store: fails restore IDENTICALLY** —
   `ParticipantStartupRestore`, every boot. (Negative control: proves the
   store copy still carries the poison.)
2. **Fixed binary, same copy: reaches listen**, the pending intent discharged,
   P1's marker still retained until acked. (The fix, at the incident's bytes.)
3. **If the fixed binary REFUSES instead of restoring, that is a STOP** — it
   means repair does not re-measure the way this note believes, the ruled
   direction is wrong at the mechanism, and the design comes back here.

Plus red-first units, one per piece: §3.1 — a fixture with a retained
unacked marker and a departing peer must measure a floor ≤ M (fails today with
`Precedence`-turned-`OwnerTransition`); §3.2 — a refused measurement must
leave **zero appended rows** (fails today: Died row present); §3.3 — a
`Precedence` refusal must surface `OwnerTransition(Precedence)` (fails today:
payload absent). And the no-new-poison property: after §3.1+§3.2, the incident
sequence replayed from clean produces a discharged fate and a live boot.

## 6. WHAT THIS NOTE IS NOT

Not compiled, not tested, written at a seat that can do neither. Line numbers
are MEASURED at `92e65ce` and will drift; the builder re-anchors before
building. Downstream: manifold-node re-pins on landing (Waffles' committed
sweep — protocol 0.3.2 / server 0.5.1 / sdk 0.5.1 move on the design landing,
manifold is the willing early adopter as the live victim).
