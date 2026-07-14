# CONTRACT-WORKLIST — starting defects for the goal session (over R15 @ 64bade9)

Provenance: R15 was refused by two independent max-effort adversarial exams
(2026-07-14). Every finding below was hand-verified in bytes against 64bade9 by
the coordinator seat before entering this list; the two exams' findings deduped
to these seven classes. Close every item, then enter the fresh self-exam loop
defined in your instructions. Delete this file in the same commit that closes
its final item.

Stability note (verified): the N1 mandatory-envelope algebra itself re-derives
correctly — both exams independently re-ran the full-K formula on the complete
arithmetic shown in cases 48/49 and it holds. Do not restructure the normative
envelope; every defect below is wiring or fixture instantiation.

## W1 — Pre-delivery and None-witness fates still route to DCR (orphaning DMR)
The R15-added `DetachedMarkerRelease` edge exists (:1765-66) but the transitions
that must reach it were never rewired. The `MarkerDelivery` row (:1763) still
sends detach/death to `DetachedCredentialRecovery` — yet MarkerDelivery IS the
pre-delivery state, fenced DCR requires a durable delivery fact (:1272-80,
:1591-94), and case 49 (:2445-49) demands fate-before-delivery select DMR.
Second unencodable arm: `ParticipantCursorProgress.marker_delivery_seq` is
`Option` (:1688-89) but its fate transition (:1764) unconditionally constructs
DCR whose `marker_delivery_seq` is mandatory (:1690-91) — the None witness has
no legal tag (case 51 supplies the reachable M=0 setup). Rewire both rows:
undelivered/None fates select DMR (or a stated no-marker fate arm), delivered
fates select DCR. Re-derive every fixture that walks these transitions.

## W2 — Case 45: both arms invalid (:2367-90)
(a) Boundary arm unreachable: the retained log is gap-free and F is its first
retained sequence, so F=h−2 with retained marker h forces at least three
retained entries; "marker-only S=C=B=(1,Bm)" is impossible. If the state is
marker-only, F=h. (b) Farther arm arithmetic: after recovery B=7 at cap 7,
removing four prefix records removes the accepted marker, releasing its credit:
S=3, C=0, B=3+(1−0)×marker=4, and full-K release is 4+2+2=8>7 — the exact
release N1 forbids. Reaching B=3 needs a fifth removal, which advances the floor
past cursor+1 and forces a new marker/credit/candidate plan the fixture never
accounts for. (c) The farther arm pins neither h nor the producer class; with
h=MAX−6 inherited, an exact-Q producer leaves 4 sequence values against a DCR
branch needing 6, so sequence refusal precedes the claimed capacity commit.
Rebuild both arms reachable and re-derived end to end.

## W3 — Case 56 commit arm: charge positions, causation, and release vector (:2507-23)
(a) A planned marker is charged in S and acquires its credit at planning
(:1701-12): with two retained nonmarkers and I=1, the state must be S=3, C=1,
B=3, not the displayed S=2, C=0, B=3 — the total matches only by placing the
marker charge in the wrong positions. (b) Causation: cursor h−2 with F=h−1 is
the never-overtaken equality; the pre-state cannot already own a pending
marker/M value by any legal history. (c) The fate-first prose says RS/RT and
their order/product claims release unused (:2519-20) but the positional vector
`5=1+0+1+1+1+0+1+0` (:2521) retains RS=1, RT=1, L×RT=1; the correct post-release
reserve is E=1, M=1, everything else zero — required 2, four free values.
Rebuild the arm with correct charging, a reachable cause, and a vector that
matches its own declared releases.

## W4 — Occurrence array has no computable bound for repeated supersession (:1664-80, :1782-95)
`occurrence_ordinal` has no range or maximum formula; the only bound is the
phrase "finite I/phase fixed point". Supersession retargets MarkerDelivery/PCP
witnesses (:1763-64), each retarget consuming a distinct `SupersessionObserved`
occurrence, and supersessions are generation-driven — not bounded by I or phase.
With I=1 and an undelivered anchored marker, supersessions can exhaust any
pre-endowed occurrence count while remaining legal, and no transition may append
an occurrence. Either supersession retargeting does not consume an occurrence
(rule change with an explicit proof that restart reconstruction survives), or
supersession count gets a typed bound with a named refusal outcome. Then restore
a fixture that actually exercises repetition: R15 rebuilt case 26 into an
M=0/no-occurrence drain (:2158-76), so no acceptance case now walks a repeated
marker/Leave/supersession plan through the representation.

## W5 — Case 31 floor arm has no legal production transition (:2200-18)
At the pinned state: cursor+1=F, o=H, so preferred_floor=F; B+Q+K=3+2+2=7=cap
means the envelope holds at F, so cap_floor=F and F'=max(F,F,F)=F. No production
trigger can move the floor or overtake the member, and an ordinary append would
change high/remaining, contradicting the projected payload (:2204). Repin the
pre-state so a real trigger legally overtakes (or re-target what the arm
proves), and correct the projected payload.

## W6 — Seed-convention totality (:1993-2004 vs the seeded set)
The convention makes any omitted transition input a suite defect. Case 49's seed
(:2434-43) omits H/F/o, cursor, L/E/T/M/RS/RT, the canonical budget, A/X/RO/RA
and order high, authority/generation/secret, the pending-terminal tuple, marker
cause/backing, and the occurrence array — and "State cap_floor and
preferred_floor at both commits" is an instruction, not values. Case 51
(:2459-70) is near-MAX (h=MAX−7) with no Test-seed declaration. Audit cases 43,
47, and 48 under the same convention (48 introduces a delivered marker with no
named causation or backing enum). Then make the self-audit sentence (:2864-68)
true instead of aspirational.

## W7 — RE=0 has no typed disposition in either phase (:684-707, :816-27)
The schema requires RE>0 (:686) but `NonzeroLimit` enumerates only N/C/P/G/D/R/B
(:697), the parked-phase list repeats the same seven (:817-18), and case 25
tests only those. RE=0 with all other predicates valid makes RH(P)=RF+RC(P),
which can pass every check — a declared-invalid configuration with no outcome.
Add RE to the NonzeroLimit dimension in both phases, extend case 25 with the
RE=0 arm, and update the audit enumeration.
