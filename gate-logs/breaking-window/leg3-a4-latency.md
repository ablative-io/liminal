# A4 §0.15 obligation 2 — the latency differential, measured

Obligation text (`docs/design/PARTICIPANT-CONTRACT.md` §0.15, build obligation
2): *"Latency is named as a channel and measured nowhere yet. A
presenter-scoped lookup that is faster than the wide one is a timing signal in
principle. The build must measure the differential and bound or unify it; this
line is the named-not-measured marker until then."*

**Verdict: BOUNDED, not unified.** The differential is real and reproducible at
the map primitive (~11-16 ns, sign depending on hit vs miss) and is bounded
below observability by a shared durable-append cost roughly six million times
larger. Numbers, controls, and the reasoning are below. This file is what
retires the named-not-measured marker.

## Instruments

Two `#[ignore]`d measurements in
`crates/liminal-server/src/server/participant/production/tests_a4_body_conflict.rs`:

| Test | Question |
|---|---|
| `a4_presenter_scoped_versus_wide_lookup_latency` | the obligation's LITERAL question: presenter-scoped range vs wide range |
| `a4_cross_participant_collision_is_not_observable_in_response_time` | the question the obligation exists for: can the presenter SEE a cross-participant collision in its own response time |

Both are ignored deliberately: they are measurements, not pins. Neither asserts
a wall-clock bound, because a timing assertion on shared hardware is a flake
generator and would be a worse instrument than the recorded numbers. The first
one does assert the two probes agree on their ANSWER, which is a real
invariant and not a timing claim.

Run both:

```
cargo test -p liminal-server --release --lib -- --ignored --nocapture a4_
```

⛔ Release profile is not optional. A debug-profile number here measures
`rustc -O0`, not the shipped path.

## Measurement 1 — presenter-scoped range vs wide range

**Setup.** 10,200 committed identities in one `BTreeMap<CommittedAdmissionKey,
DeliverySeq>` — 200 participants x 50 private tokens each, plus one HOT token
committed by all 200 participants. Over the hot token the wide range spans 200
entries while the presenter-scoped range spans exactly 1: the worst case for a
differential. 200,000 timed probes per class after a 10,000-probe warm pass.

**Scope justification.** The two arms differ in EXACTLY one operation — the
`range(..)` bounds. Decode, authority classification, envelope construction and
response encode are byte-identical between them and all run before the branch.
Measuring the map primitive therefore measures the entire differential and
nothing else. Dispatching 10k admissions instead would bury it under the
durable append (see measurement 2, where that is the finding).

**Results** (Darwin 25.3.0, Apple silicon, `--release`, two independent runs):

| Probe class | run 1 | run 2 |
|---|---|---|
| presenter-scoped HIT (hot token, 1 of 200) | 77.57 ns | 69.30 ns |
| wide HIT (hot token, 200 spanned) | 93.89 ns | 83.69 ns |
| presenter-scoped MISS (uncommitted token) | 66.72 ns | 55.43 ns |
| wide MISS (uncommitted token) | 54.92 ns | 44.23 ns |
| **HIT differential** (presenter − wide) | **−16.31 ns** | **−14.39 ns** |
| **MISS differential** (presenter − wide) | **+11.80 ns** | **+11.20 ns** |

**Reading it honestly.** The differential is NOT noise: both magnitude and sign
reproduce across runs. But it is also not the signal the obligation feared. The
obligation's hypothesis was "presenter-scoped is faster than wide"; measured,
the presenter-scoped probe is ~15 ns FASTER on a hit and ~11 ns SLOWER on a
miss. That is the structure of the range descent (a tighter start bound lands
closer to the answer on a hit and further from an early exit on a miss), not a
monotone advantage a caller could read as "this token is mine".

## Measurement 2 — is any of it observable at the presenter?

This is the sharper question, and the one that decides whether the outlawed
probe channel exists in practice. A4 leaves A2's cross-participant arm
warn-and-fall-through, so a presenter whose token happens to be held by SOMEBODY
ELSE pays the wide range's HIT instead of its MISS, plus a `tracing::warn!`. If
that were observable at the presenter, the channel would exist through timing
even though no byte of it reaches the wire.

**Setup.** Two enrolled participants on one conversation, real disk store. The
victim first spends 400 tokens. The prober then commits 400 records under
tokens NOBODY holds, then 400 under the victim's tokens, then 400 more fresh
ones. The third pass is the control on the first: the store grows with every
commit, so a monotone drift must be separated from the differential rather than
attributed to it.

**Results** (`--release`, 400 dispatches per class):

| Class | ns/dispatch |
|---|---|
| fresh token, pass 1 | 89,574,928 |
| COLLIDING token (wide hit + `warn!`) | 97,502,449 |
| fresh token, pass 2 | 99,448,102 |
| store drift across the two identical fresh passes | **+9,873,175** |
| collision excess vs the fresh mean | **+2,990,934** |

**Reading it.** The collision excess (2.99 ms) is 3.3x SMALLER than the drift
measured between two IDENTICAL control passes (9.87 ms). The collision case is
not distinguishable from ordinary store growth at the dispatch seam. The
per-dispatch cost is ~90 ms, dominated by the durable append both paths pay.

## Verdict

**BOUNDED.** Ratio of the map-primitive differential to one dispatch:

```
15 ns / 90,000,000 ns  =  1.7 x 10^-7
```

The differential is ~6 million times smaller than the shared cost every
admission pays regardless of which arm it takes, and end to end it is smaller
than the control's own drift. A presenter cannot resolve it.

**Not unified, and deliberately so.** Unifying the two probes means giving the
refusal the wide range — which is the one-widened-arm build §0.15 obligation 1
outlaws outright, "regardless of its test results". Unification would trade a
1.7e-7 timing differential for the actual disclosure channel. The two ranges
stay.

## Cross-reference — the finding this measurement sits beside

§0.15 obligation 1 prescribes the presenter-scoped range literally as
`(token, [0x00;32], presenter) ..= (token, [0xFF;32], presenter)`. Over the key
layout that shipped with A2 — `(token, fingerprint, participant)` — that range
is NOT presenter-scoped: `BTreeMap` compares lexicographically, the fingerprint
sits in the middle, and endpoints spanning every fingerprint return every
participant's entries. A build following the obligation's text literally
therefore refuses cross-participant presentations: the exact probe channel the
same obligation outlaws. Measured red at
`gate-logs/breaking-window/leg3-a4-red2-literal-contract-range.log`.

The build closes this by ordering the key `(token, participant, fingerprint)`,
which makes the prescribed range actually presenter-scoped and lets the probe
touch ZERO foreign entries — which is also why measurement 1's presenter-scoped
column is a genuine 1-entry span rather than a 200-entry one wearing a label.
The key order is in-memory only; no durable or wire byte encodes it. The
contract TEXT still carries the unimplementable literal and should be corrected
at the register — flagged to the seat, not patched here.
