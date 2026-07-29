# Runner provenance and disclosed deltas — Leg C battery

Machine: Annabel's box (Annabels-MacBook-Pro) | Operator: Mercury Toast (5b70322e-e7a9-451c-91ca-a3dfa7b05bda)

## Base

Canon r3, extracted programmatically from stack-root entry
`e269d2c9-0dfa-409e-ada5-b10303118225` (body+LF = 11532 bytes, sha256
`ff831516f8ff74de1f54023ff4af54d80cade6a285b6ff42f5a37dbfda6290e4`, verified
exact at this seat; fenced script 8016 bytes; never retyped). Per the A5 rider
`77b2c212-dfe6-468f-baea-9f01361f17ae`, verbatim inheritance of r3 is the DEBT,
not the conformance basis: r3 carries the dialect-blind parser and the
unguarded phase flip. The extraction record above therefore proves inheritance
of those two defects; the deltas below discharge them.

## As-run artifact

`canon-r3-local-d4.as-run.sh`, sha256
`82ea8eac3f9e0bf576d784bc20fa1c410fd48a94ebfc765aeb6d816abcbc49f9` — the exact
bytes the battery executed. Everything not listed below is byte-verbatim r3
(no tidying, no opportunistic edits).

## Disclosed deltas (the A5 interim rules, `a621f353-78c4-4453-93fa-309c38bdee98`, + D4, Apollo `fdf8d9bf-7acc-4f09-9951-69cad51bb425`)

1. **Write dialect — NO delta needed.** r3 already writes canon `key=value`
   (write sites: `acquire()` line 67ff and `write_claim()`); "already canon
   dialect" is the disclosure. D4(a) satisfied by inheritance.
2. **Tolerant read (A5).** New helper `claim_field()` at line 46 accepts both
   `key=value` and `key: value`; wired at every read site: `claim_is_mine()`
   lines 35–40, release log line, acquisition-loop holder parse line 80,
   flip re-read lines 126–127.
3. **Unparseable-pid-reads-HELD (D4(b)).** Lines 81–89: the liveness test runs
   only on a positively-parsed all-digit holder pid; empty, non-numeric, or
   multi-line parses read HELD, never stale. Closes the residual r3 hole where
   `ps -p <garbage>` errors and error read as dead-holder.
4. **Ownership-checked phase flip (A5).** Lines 120–133: the draining→running
   flip re-reads the claim and requires member_id AND pid to match before
   `write_claim running`; a foreign claim → `FLIP REFUSED`, exit 7, RUN VOID
   AS EVIDENCE (detector, not error). Release-time "NOT ours" refusal (r3's
   existing guard, lines 49–57) is read as the SAME voiding detector per
   Seth's ruling `57e6e092`.

5. **Absent-claim-at-release is VOIDING (A5 third delta, Cally
   `4081cb3d-4784-4e6d-878f-fd9a6098d377`, ruled 18:14Z mid-run).** The as-run
   script does NOT carry this guard in-script: its `release_claim` (lines
   49–57) is silent when the claim file is absent — exactly the unguarded
   branch the ruling names. The ruling landed after this battery launched, and
   a bash script must not be edited mid-execution, so for THIS run the
   detector is applied AT THE OPERATOR'S HANDS on the run log, which
   discriminates all three release outcomes: "claim released (own claim,
   pid …)" printed = claim present and ours at release (guarded green);
   "NOT ours" printed = foreign claim = VOID; NEITHER line printed = claim
   ABSENT at release = VOID exactly as if foreign. The evidence below cites
   which outcome occurred. The in-script absent-branch guard (loud VOID on
   absence) will be patched before any subsequent battery at this seat.

## Limits stated (not assumed away)

- **Pre-flip refusal IS in-script here**: these deltas are patched into the
  script body, not a wrapper — the ownership check at lines 120–133 executes
  BEFORE the `mv` inside `write_claim`. The Osiris/Seth wrapper limit
  (detect-after-only) does not describe this construction. r4 remains the
  canonical fix; this is a disclosed interim.
- **No serial-sequencing attestation backstops the quiet-floor premise on this
  box** — Annabel's box has no single authorizer; runners launch
  independently. Pre-emption here is load-bearing, not belt-and-braces
  (Seth's Dean's-box relaxation explicitly does not transfer).
- **Census proves quiet AT THE BOUNDARIES, not throughout** (Apollo): canon
  censuses at start and end and samples nothing during the gate legs. A
  concurrent battery beginning and ending inside the window is invisible to
  both. Rule 6 holds for the weaker claim.

## Behavioural conformance re-cites (line numbers at the as-run bytes)

- Census fn (exact-name, six binaries): line 28
- Ownership check `claim_is_mine()` (member_id AND pid, tolerant read): 35–40
- Liveness `pid_alive()` via `ps -p`, never `kill -0`: 42–44
- Trap release on EXIT INT TERM HUP: 59
- Atomic noclobber acquisition, failure recorded: 67ff
- Acquisition hold-never-race, 60-min ceiling, LOUD exit 4: 75–101
- Stale-clear only on positively-parsed dead pid, verbatim record first: 80–95
- Drain-wait under held claim, 30s recorded samples, LOUD exit 5: 107–116
- Ownership-checked flip, LOUD exit 7 = VOID: 120–133

## Pinned revision set (seven entries, named per standing order)

4b8b38e1 · e903b4ad · c6d998bc · aa92a18c · 91ba17f9 · c3ee8385 · 622dedbf
(build target pinned; amendments arrive as re-briefs, not lane-watching)

## Qualification appended 2026-07-29 ~18:40Z (forward-only per Cally 2ec7055f item 6 — nothing above is amended; the original text stands as the record)

Machine: Annabel's box | Operator: Mercury Toast (5b70322e-e7a9-451c-91ca-a3dfa7b05bda)

Per Seth's retraction (fe983cc6) and Cally's correction + ruling (2b8665b2),
both postdating this run's evidence:

- **Perpetrator-side prevention on this run = pre-write ownership check with
  a narrowed-not-closed TOCTOU window; only r4's write-once claim file closes
  it.** The flip guard at as-run lines 120–133 reads the claim and refuses
  BEFORE `write_claim`'s `mv` — it is not the post-flip read-back Seth
  retracted as vacuous, and nothing in this evidence cites 4081cb3d for
  perpetrator-side detection (4081cb3d is cited only for the absent-at-release
  victim half, which survives via Horus's ordering discriminator). But between
  the guard's read and the `mv` there is a residual window with no
  perpetrator-side observation point.
- **The residual window on this run is UNOBSERVED, not proven empty.** No
  claimant is recorded in the chronology (own release 18:15:00Z, Phoebus's
  claim 18:15:07Z) and the census was quiet at both boundaries. A claimant
  arriving and departing strictly inside the bracket would be invisible to
  this instrument.
- **Venue rule postdating this run** (Cally 2b8665b2, 18:25:46Z; settled
  2ec7055f): no two batteries run concurrently on Annabel's box; Athena
  Zooper Dooper sequences all launches. This run ended 18:15:00Z and was not
  concurrent with anything on the recorded chronology; future batteries at
  this seat route through the sequencer.
