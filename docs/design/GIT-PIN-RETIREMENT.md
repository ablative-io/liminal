# Git-pin retirement lane — consumers off liminal git revs, onto published crates

**Authority.** Tom's off-the-pins ask, held here as RELAYED: Waffles (stack
key holder) routed the lane start at meridian msg `ff3a4fba` (2026-08-15
09:03Z) — "sanctioned and unblocked since the v0.6.0 publish". No liminal
code changes; this is a coordination lane: census, published-road proof,
advisories, receipts. Standing rules recorded from the same relay: consumer
advisories at BOTH ends before any pin flips; protocol changes land at both
ends reader-first; manifold's consume rides Waffles's landing desk (he owns
its lock).

**Census** (2026-08-15, this box: walked `/Users/tom/Developer/ablative` at
maxdepth 5, `Cargo.lock` + `Cargo.toml`, pattern
`git+https://github.com/ablative-io/liminal` / `git.*ablative-io/liminal`,
excluding `target/` and `.claude/`; positive control = manifold's lock, known
to carry git refs, found with 4):

1. **manifold main lock @ `46fdbe1`** — the git quartet at liminal rev
   `0c3bacc` (liminal-protocol 0.4.0, liminal-rs 0.5.1, liminal-sdk 0.5.1,
   liminal-server 0.5.1). Sole intake: `frame-host 0.4.1`, itself git-pinned
   at frame rev `9b32c02`, whose manifest declared the git deps.
   manifold-node's own liminal family is already registry-current
   (server 0.8.2, protocol 0.7.0, sdk 0.7.0). The duplicate
   liminal-server pair (0.5.1 git + 0.8.2 registry) in one binary is the
   proven cause of the 08-15 four-hour deaf-listener incident (board #9,
   attribution confirmed three legs, Waffles msg `e807f232`).
2. **manifold worktrees** — six carry the same quartet via frame-host;
   `autoprov-1` ADDITIONALLY declares its own manifest git quartet at
   liminal rev `33c995f` (8 lock refs).
3. **frame main @ `17130cc`** — ALREADY REGISTRY (protocol 0.4.1, rs/sdk/
   server 0.5.3). Frame retired its own git pins after `9b32c02`; what
   remains there is version staleness, not a git pin.
4. No other walked repo on this box git-pins liminal. LIMIT NAMED: this
   census saw one box. Checkouts on other boxes (annabel-box) need their
   own census by their own seats.

**Published-road proof** (rev→tag containment, measured at this repo):
`0c3bacc` is contained in every tag from `liminal-v0.5.2` up; `33c995f` in
every tag from `liminal-v0.6.0` up. Nothing any consumer pinned for is
absent from published crates. **No liminal-side publish is required; the
road is complete as of the 0.8.2 family** (liminal-rs 0.5.5 /
liminal-protocol 0.7.0 / liminal-sdk 0.7.0 / liminal-server 0.8.2, all
registry-verified 08-14).

**Migration legs** (each at its owner's desk; liminal's seat holds the
advisories and closes on receipts):

- **Leg 1 — frame republish + manifold frame-pin advance (Waffles's desk,
  rides the K9 rebuild+restart window, = board #9's consume).** Frame bumps
  its registry family 0.4.1/0.5.3 → current, republishes frame-host;
  manifold advances the frame pin; the duplicate liminal-server pair
  collapses. ⚠ ADVISORY: the span 0.5.3→current crosses the 0.8.0 breaking
  window (A5 wrapper condition-2, A4 attempt-token two-ranges,
  StateUnavailable{source}) plus behavioral deltas a host embedder feels:
  #56 latch semantics (terminal→recoverable hold), #63 `receive_within`
  typed silence, #76 marker ownership. Reader-first ordering applies.
- **Leg 2 — autoprov-1 worktree (Waffles's desk).** Its manifest quartet at
  `33c995f` flips to registry ≥0.6.0 family; recommend current. Same
  breaking-span advisory if it targets current. Must not land carrying git
  pins.
- **Leg 3 — liminal seat (this lane).** Census + road proof + advisories
  (this document), lane record on the `ablative/stack/liminal` topic,
  closure verification on receipts.

**2026-08-16 addendum — Leg 1's FRAME HALF IS DONE, executed by Tom
himself 08-15 evening** (Athena's owner report, msg `d6ddf26e`, 7 commits
`37ac84f..db6bc7c` inclusive; every byte claim below re-measured at this
seat on frame `origin/main = 251b74f`): frame's lock carries the current
family (liminal-protocol 0.7.0 / liminal-rs 0.5.5 / liminal-sdk 0.7.0 /
liminal-server 0.8.2), zero `git+…ablative-io/liminal` refs, exactly one
`liminal-server` entry, beamr SINGLE at 0.16.3 through the bump.
frame-host is now **0.5.0** (manifest at `251b74f`; registry index per
Athena's cache-mtime receipt) — so **manifold's frame-pin advance targets
0.5.0, not the 0.4.1 named above**. The census row "frame main @ 17130cc"
was true at its moment and is superseded at `251b74f` (sha-pinned both
ways). Remaining to close leg 1: the manifold half only — frame-pin
advance + the manifold receipts below (Waffles's desk). Athena also
owner-confirmed the frame census row (zero git refs) — the walked-from-
outside row is now confirmed at the owning seat. Frame-side record:
frame `docs/notes/beamr-017-wave-frame-is-last.md` §2026-08-16.

**Receipts that close the lane** (byte-level, not attestations of intent):
manifold lock shows exactly one `liminal-server` (`grep -c 'name =
"liminal-server"' Cargo.lock` = 1); the estate-walk grep for
`git+…ablative-io/liminal` returns zero across walked repos incl. worktrees;
`strings` on the rebuilt manifold binary finds zero pre-#56 refusal strings
(#9's receipt). Frame's lock carries the current family.

**2026-08-16 closure ruling (Tom's word, relayed Waffles msg `4b2f8320`):
annabel-box is not connected and the estate is this computer for now.** The
lane therefore **closes on the K9 receipts alone** (the manifold half
above; frame's receipts already hold at `251b74f`). The annabel-box census
is carried as a **NAMED BLOCKED RESIDUE** — it fires the day a second box
answers ssh (Athena holds the spec: both patterns, positive control through
the same predicate, search space named in the output) — and does NOT gate
the close.
