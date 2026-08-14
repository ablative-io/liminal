# Marker-ownership lane — #76: non-owner marker obligation

Lane branch `marker-ownership-76`, cut from landed main `60530fd`. One leg.

**Authority.** Tom's fix-everything word (meridian `d3c7f892`) opened #76; the
lane paused when the seat's first diagnosis was falsified (withdrawal `95c9165`,
measurements in `gate-logs/fossil-reclaim/leg2-STOP-measurements.log`). The
recast is now CONFIRMED IN THE FIELD (Waffles msgs `fa343b8d`/`527e127f`):

- Boot-1 log (`.manifold/kernel-boot-20260814-1547.log`, untouched since the
  failure): line 759 — marker at delivery 829 names PARTICIPANT 5; lines
  1120/2181 — the seat holding the outstanding MarkerAck at 829 is the
  REGISTRY, participant 0; line 1020 — the fatal invariant. Non-owner offered,
  non-owner acked, invariant fatal.
- Live control (kernel-boot-20260814-1922.log, healthy 0.8.1 cycle): the
  structurally identical pair fired again TODAY — `[846] HISTORY COMPACTED for
  participant 3` / `[registry] 0: waiting for the answer to marker
  acknowledgement at 846` — and resolved as a re-sync only because the client
  carve-out caught it. The shape recurs on ordinary compaction traffic; which
  way it ends depends on which side catches it.

**Contract classification (why this is a COMPLIANCE fix, not an amendment).**
R-C3 @60530fd: every member's entitled subsequence is the full per-conversation
sequence — `HistoryCompacted` is delivered to every member AS A RECORD (~line
3153). But the MarkerAck route is authorized only by "delivery of
`HistoryCompacted { participant_id, ... }`" naming the acker's own broken
history: both authorized routes "atomically advance THAT participant's cursor
to the marker sequence" (~3215-3230), an ack "can never advance the newer
cursor" of another binding, and "every other attempt spanning abandonment is
refused." A non-owner has no abandonment to span — its history over the
marker's seq is continuous, covered by ordinary cumulative `ParticipantAck`.
The contract already answers the recast question: a non-owner holds NO marker
obligation. No text change; the sitting may ratify the reading, the mechanism
does not wait.

**Defect site** (@60530fd): `ConversationOutbox::is_marker_obligation`,
`crates/liminal-server/src/server/participant/production/outbox/selection.rs:126-139`
— the doc comment says "Whether this exact participant still OWNS the named
marker obligation"; the body checks `record.recipients.contains(&participant_id)`
plus record-type only, never the marker's own affected `participant_id`. The
comment already claims what the body must enforce. Sole consumer of the
marker-flavored answer on the offer path:
`record_publication_offer`, `handler_semantic.rs:453-498` (obligation guard
:482-484, `offered_markers` insert :492). Sole-consumer status verified by
census at the second key's hands (Waffles, msg `0fa20050`): `git grep
is_marker_obligation 60530fd -- crates/` returns exactly two lines — the
definition and handler_semantic.rs:483.

**Fix shape:**

1. Ownership condition in `is_marker_obligation`: the record must be
   `HistoryCompacted` AND its affected `participant_id` must equal the queried
   participant. Record delivery UNTOUCHED — recipients unchanged, non-owners
   still receive the record; their delivery becomes an ordinary obligation
   covered by cumulative ack.
2. ⛔ THE CONSUMER'S ARM (key-holder amendment, Waffles msg `0fa20050` — the
   fold is mandatory, the placement is the implementer's call):
   `record_publication_offer` runs for EVERY `HistoryCompacted` publication
   and treats `!current || !obligation` as an INTERNAL ERROR (:485). If the
   ownership condition lives ONLY inside `is_marker_obligation`, every lawful
   non-owner delivery of the record hits obligation=false and ERRORS — record
   delivery to survivors breaks, worse than the defect. The ownership question
   decides marker-vs-ordinary BEFORE that guard: a `HistoryCompacted`
   publication whose affected `participant_id` is not the recipient is an
   ORDINARY delivery — return Ok with no `offered_markers` insert. The
   Internal error stays reserved for OWNER-marker publications that genuinely
   lost binding or obligation. Early-return in `record_publication_offer` or a
   hoisted predicate — implementer's choice; the distinction "not a marker for
   this recipient" ≠ "lost authority" is not negotiable.
3. Consequence to verify by pin, not assume: with the offer never
   marker-flagged for non-owners, a legacy non-owner MarkerAck lands the
   offered=None arm and answers the existing benign `NoMarkerExpected`
   re-sync — never the invariant.
4. The invariant `marker_progress.rs:76-78` is UNMODIFIED (standing law,
   carried from the fossil-reclaim brief). It is correct; the fix removes the
   unlawful offer that walked a non-owner into it.
5. SDK measurement (report, possibly zero delta): does the published client
   send MarkerAck for a marker naming ANOTHER participant? If yes, the client
   half is the same ownership condition (additive); flag any SDK delta to the
   seat BEFORE building it — release-shape question.

**Pins:**

1. Field shape RED at the unfixed tree: multi-member conversation, compaction
   induces a marker for participant A, participant B receives it, offer path
   marker-flags B, B's MarkerAck dies at the exact invariant string ("stored
   MarkerAck has no matching marker delivery authority"), TRUE EXIT. Post-fix:
   B's flow carries the record as ordinary, cumulative ack covers it, no
   marker offer minted.
2. Owner path unchanged: A's own marker still offers, A's MarkerAck advances
   A's cursor to the marker seq — existing pins stay green unmodified.
3. Survivor-delivery pins (the four in the `e2e_cold_all_shapes.rs:403`
   family) STAY GREEN — they pin record delivery to survivors, which the fix
   preserves. ⛔ If any existing pin asserts a non-owner marker OFFER or
   MarkerAck obligation, that pin encodes the defect: STOP and flag to the
   seat with the pin named; never silently rewrite a pin.
4. Legacy-client arm: a non-owner that sends MarkerAck anyway is answered
   benignly (NoMarkerExpected re-sync), never fatally — pinned on at least one
   real transport.
5. Restart parity: a store whose history holds markers for A with B live loads
   clean first boot, no invariant, and B is never marker-flagged on the replay
   path either (the predicate must govern replay-derived offers identically).
   Measure whether pre-fix stores hold any durable poison needing a healing
   story — offers are believed volatile; MEASURE, then say so either way.
   The 950M field fixture IS a pre-fix store carrying the poison shape, so
   this measurement has a field arm waiting: if the answer is volatile-only,
   the fixture's clean first boot (operator-side, post-consume) proves it in
   production bytes.
6. On-disk arm for pin 5 with the instrument control (the standing discipline:
   prove the disk store engaged).

**STOP clauses:** any protocol-crate delta (`git diff --stat 60530fd..HEAD --
crates/liminal-protocol` EMPTY at gate); any pin conflict per pin 3; any need
to touch the invariant or any refusal's semantics; any measurement suggesting
the contract reading above is wrong at the bytes — all return to the seat as
flags, never silent choices.

**Gates:** full workspace battery at lane tip, baseline 2081/2/3 over 56 plus
exactly the new pins reconciled by name; failures = the declared F8 pair BY
NAME only; `cargo clippy --all-targets` clean; cargo's exit captured DIRECTLY
(never through a pipeline); no `git checkout --` in red drivers (file copies
only); logs in-tree under `gate-logs/marker-ownership/`; red proofs committed
before their fixes' greens.

**Field cross-check after land + consume** (operator-side, not a lane gate):
Waffles's preserved 950M store copy
(`manifold/.manifold-evidence-20260814-marker-authority`) boots clean FIRST
boot on the fixed tree, no invariant — the original failed-boot store becomes
the closing fixture. His box, his hands.
