# Ephemeral-store reclamation — design pass (task #3, design leg)

**Status: DESIGN DRAFT for review — not a dispatch.** No executor, no GO; any
build waits on the freeze doctrine like everything else.
**Author:** Hermes Crumpet (liminal seat).
**Evidence base:** task #3's metadata holds the full investigation record —
the 571-dir partition, the pre-registered redeploy experiment (fired
2026-07-31 08:23Z), and the rulings this design binds to. This document is
the mechanism; that record is the why.

> ⚠️ **READ THIS AT HEAD.** A commit hash citing this file names the tree it
> read, not the current ruling.

---

## 1. THE TWO FINDINGS, AS RULED

**1a. The ephemeral branch is INTENDED.** `open_ephemeral`
(`crates/liminal/src/durability/store.rs:407`) is ungated production API,
re-exported at `durability/mod.rs`, and the server reaches it at
`build_durable_store` (`crates/liminal-server/src/server/connection/services.rs:901-905`,
`None => open_ephemeral(..)` when no `persistence_path` is configured).
`durable = false` is a **retention contract, not an
implementation-medium declaration** — a disk-backed store honouring it is
legitimate. The design below does not remove this branch.

**1b. Crash residue breaks the promise WITH PAYLOADS.** Conversation payloads
reach the store with no durability gate in the append plumbing
(`crates/liminal-server/src/server/participant/aggregate.rs:228`). A store
that outlives its process is `durable=false` message content persisting on
disk — privacy-shaped, and measured live: the 2026-07-31 kill arm left
`dBddZ4` as a full-size orphan (140,840 KiB, present, no holder). Today the
residue bound is **unbounded**. The design's deliverable is an honest bound:
**ephemeral residue survives at most until the next server boot.**

## 2. WHAT THE MEASUREMENTS BIND

Three constraints, each traceable to a datum in task #3:

1. **Liveness is decided by the writer lock and NOTHING else.** The orphan
   `dBddZ4` SHRANK during shutdown (168,008 → 140,840 KiB over ~40 s) with
   its guard never dropping — orphans are not byte-frozen, so **quiescence
   heuristics (stable size, mtime age) are structurally invalid** liveness
   tests. Haematite's writer lock is an OS advisory lock on
   `<data_dir>/writer.lock` (haematite `db/lock.rs:7`, `LOCK_FILE` at `:37`),
   released on fd close — **the kernel drops it on any process death,
   including SIGKILL.** That makes lock-based liveness exact where every
   heuristic is approximate.
2. **The mechanism is signal-agnostic.** The kill-arm datum licenses only
   "the TempDir guard does not drop under the harness stop path"; the
   clean-SIGTERM leg is unmeasured. Boot-time reclamation never asks WHY an
   orphan exists, so its safety story does not depend on the unmeasured leg —
   which is an argument FOR boot-time and AGAINST exit-path hardening, whose
   correctness would depend on exactly that signal inventory.
3. **A reclaimer must OWN its population.** The estate's no-sweep order
   exists because a prefix match over shared system temp cannot distinguish
   our residue from a stranger's directory, and two of the "leaked" dirs were
   a LIVE DATABASE. **The automatic sweep must be scoped to a root only we
   write under — never the shared system temp dir.**

## 3. THE MECHANISM

### 3.1 The claim-then-delete primitive (haematite's own discipline, reused)

Haematite's own module doc states the liveness thesis in its own words
(second-reader verification, `db/lock.rs`): release is kernel-owned on fd
close by design — "a crashed writer leaves only an inert, unlocked,
content-free lockfile whose presence means nothing." And the primitive is
well-defined over the real residue population, measured (names-only listing,
within the evidence rules): **the husks contain `writer.lock` +
`config.json`** — Arm 2's husk path is real, not hopeful.

For a candidate directory, probe `<dir>/writer.lock` and classify into
exactly one of **five verdicts** — the first three are the common cases, the
last two exist so a race or a fault can never masquerade as either:

- **`live-skipped`** — lock held elsewhere (`DataDirLocked`-shaped refusal)
  ⇒ a live writer owns it ⇒ leave it, count it.
- **`reclaimed`** — lock acquired ⇒ no live writer exists ⇒ **delete the
  directory while still holding the lock**, then release. This is precisely
  haematite's failed-create discipline (`db.rs:130-132`: removal happens
  "while still holding the writer lock — it can never delete a concurrent
  writer's live dir"), so the claim and the delete are one atomic ownership,
  no TOCTOU.
- **`no-lockfile-skipped`** — `writer.lock` ABSENT. This is the pre-create
  window: the guard TempDir exists but `Database::create` has not yet
  reached its lock acquisition. The dir must NOT be reclaimed, and —
  binding — **the probe must never open-with-create**: haematite's own
  acquisition opens creating-if-absent (`db/lock.rs:71`), so a reclaimer
  that O_CREAT-locks the path would make the legitimate creator's acquire
  fail `DataDirLocked` at mint. Probe existence without creating; absent ⇒
  skip and count under this verdict's own name.
- **`vanished`** — ENOENT mid-probe: the owner finished its teardown, or a
  concurrent reclaimer won the claim. Benign; its own verdict, **distinct
  from `refused`**, so the §5 zero-refused clause cannot convert a benign
  race into a STOP.
- **`refused`** — a real error (permissions, IO fault, unrecognised state).
  Never silent, always carries the error text.

Reclamation obeys the sensitive-residue rules: the reclaimer **stats, locks,
and deletes — it never opens store contents** (existence-and-holder only),
and disposition is **deletion, never archive** (the residue is message
content under a `durable=false` promise).

Each candidate produces exactly one loud log line: path and verdict, with
the error text on refusals. A sweep summary counts all five verdicts. **No
silent outcomes** — a reclaimer that cannot look must say so, not report a
clean world (the blocked-instrument law).

### 3.2 Arm 1 — automatic boot sweep over a server-owned root

Production ephemeral stores move from system temp to a **server-owned
ephemeral root** — an explicit `ephemeral_root` config value, refuse-at-boot
when absent, no silent fallback to system temp (RULED, §6.1; the
sibling-of-persistence shape is dead code in the only case that mints and is
not an option). At boot, before minting its own store, the
server sweeps `liminal-durability-*` entries under that root with the §3.1
primitive. Population ownership makes the sweep safe by construction: nothing
else writes there, so prefix-match false positives are structurally excluded
rather than heuristically unlikely.

This requires the rooted mint in production — which is the deferred
root-ownership question at `store.rs:419-428`: `open_ephemeral_rooted` is
test-gated (`:433`) because a rooted store's PARENT lifetime is the caller's
problem, "deferred until a real embedder need arrives." **The need has now
arrived twice** (frame, and the server itself). Ruled shape: an
`EphemeralRoot` handle — created once per server process, non-`Clone`, owns
the root directory for process lifetime, is the ONLY mint path
(`EphemeralRoot::open_store(..)`), and carries the boot sweep
(`EphemeralRoot::reclaim_orphans()`). The token's existence proves the parent
outlives every store minted from it, which is the D3-adjacent invariant the
`:419-428` comment demands. Frame consumes the same type; its consumer-facing
doc lands in `frame-host`'s module docs (frame's thread, not this one).

**The mid-create race, named:** on a shared root the sweep can meet a
sibling store MID-CREATE (§5's own acceptance runs a second live server
under the same root). The §3.1 `no-lockfile-skipped` rule IS the
resolution: in the pre-create window the dir has no `writer.lock` and the
sweep skips it without touching the path; from the moment the creator's
lock acquisition lands, the dir reads `live-skipped`. There is no instant
at which a legitimately-minting store is claimable — so the acceptance's
"second server survives" clause covers mid-create, not just post-create.

**Interplay with the designed panic leak:** `EphemeralGuard::drop` leaks the
directory deliberately when the store's drop panics (`store.rs:284`,
`dir.keep()`) because removal under possibly-live workers is corruption. The
boot sweep completes that story instead of contradicting it: while any
panicked process's workers still hold the lock, the sweep skips the dir; once
the process dies, the fd closes and the next boot reclaims it. The leak
comment's "visible residue is diagnosable" window becomes **one boot cycle**,
which is the same honest bound as §1b.

### 3.3 Arm 2 — explicit legacy reclaim over a NAMED path

The boot sweep must never touch system temp (§2.3) — but the existing
residue (567 husks + `dBddZ4`) lives exactly there, minted by pre-design
binaries. Arm 2 is a one-time, **operator-invoked** reclaim: the operator
names the population root explicitly (flag or subcommand — surface for
review), and the tool applies the identical §3.1 primitive with the identical
logging. The operator's explicit naming is the authorisation the automatic
arm derives from population ownership; **there is no silent path by which the
automatic sweep widens to system temp.** Arm 2 is how the standing no-sweep
order eventually lifts: not an `rm -rf`, but a lock-disciplined tool run
under operator authority, leaving live stores standing by proof rather than
by luck.

### 3.4 The lock-protocol pin — cross-crate, so it must be a CONTROL

`writer.lock` is haematite's internal contract (`LOCK_FILE` is
`pub(super)`); liminal cannot call haematite's lock module. The preferred
resolution is a haematite-exposed primitive — **asked, and RULED by Apollo
2026-07-31 (his tracker's #67, next natural cut, 0.8.0-class, no executor
floor so no landing date; both cites verified at his bytes, main @
`72176a8`):**

- **The name follows the mechanism, not the wish: `delete_if_unlocked`, NOT
  `reclaim_if_orphaned`.** Apollo's finding, and it binds this document too:
  *unlocked does not imply orphaned.* A cleanly-closed durable store leaves
  an inert unlocked `writer.lock` byte-identical to a crashed ephemeral
  one — and he measured that **no durability field exists in haematite's
  on-disk config**, so the orphan/dormant discriminator does not exist on
  disk today. The *is-this-an-orphan* judgment is therefore the CALLER's,
  made from context haematite lacks — which is exactly what this design's
  population ownership supplies: haematite tests locks; liminal's owned
  root + `liminal-durability-` mint provenance is what upgrades "unlocked"
  to "orphan". **Neither half alone licenses a deletion.** (For Arm 2 the
  same division holds: the operator's explicit naming of the population is
  the orphan judgment, and only `ephemeral_tempdir` ever writes that prefix
  under system temp.)
- **Returns a `#[non_exhaustive]` enum, never a bool** — distinct arms for
  deleted / live-writer / refused-no-anchor / dir-vanished / typed errors,
  matching this document's five-verdict discipline from the other side.
- **The anchor discipline transfers to the INTERIM probe, binding:** a
  `writer.lock` that is not a regular file (symlink, special) is refused
  BEFORE any open (haematite `lock.rs:76-80`) — a reclaimer that follows a
  symlink deletes something outside the dir it thinks it is clearing. The
  §3.1 probe inherits this as a `refused` arm.
- A durability field in `config.json` — making *orphaned* genuinely testable
  from the directory alone — is a separate, larger haematite change, noted
  and not bundled.

Interim until #67 lands: liminal performs the same advisory-lock acquisition
on `<dir>/writer.lock` via the same std file-locking API haematite uses. That
interim carries a pin — the lock file's name and location — which no code
would mechanically compare, and a pin nobody compares is an instruction, not
a control. **Binding: a conformance test mints a real store and asserts
`writer.lock` exists at the expected path within it**, so a haematite rename
breaks a test instead of silently un-arming the reclaimer.

## 4. WHAT THIS DESIGN DOES NOT DO

- Does not remove or gate the ephemeral branch (§1a — intended).
- Does not touch the battery-minted dirs problem: liminal's own tests routing
  through `open_ephemeral_rooted` under `target/` is the **mechanical leg**,
  tracked separately in task #3 — and its brief has an unread input (the
  aion-awl record at `e866f723`) that must be read before that leg moves; the
  collective resolver is unreachable from this seat, so that read is not
  claimable here and is named rather than skipped.
- Does not decide the version class of the server/liminal changes — the
  `EphemeralRoot` API is additive but the config surface may not be;
  classification happens at the change, per the version-bump three-classes
  rule.
- Does not lift the no-sweep order. Only a green Arm-2 acceptance run does
  that, and only for the population it names.

## 5. ACCEPTANCE — the forensic pair that exists on disk today

**Fixture (claimed, ruled in task #3):** `dBddZ4` — a genuine ~140 MiB
production-scale orphan of exactly the reclaimable class — plus its two live
neighbours as in-situ negative controls.

**Arm 2 acceptance, on the box where the fixture lives, post-freeze, under
named GO:** operator invokes legacy reclaim naming the system-temp
population. PASS requires ALL of:

- `dBddZ4`: **reclaimed** — absent afterwards, its log line naming the path.
- Every dir with a live holder (the manifold surface's store, the current
  boot's store): **live-skipped** — present afterwards, holders intact.
- The husk population: reclaimed, counted, the count reported against the
  census taken immediately before the run — and **the census and the tool's
  population must share a DOMAIN: same predicate, same root** (the
  count-domain law), or the comparison silently compares two populations.
- Zero `refused` verdicts — **a refusal instead of a reclaim is a STOP back
  to design** (F8's clause, same reason: a mechanism that cannot handle the
  real case is not amended in the field). `vanished` and
  `no-lockfile-skipped` are NOT refusals (§3.1) and do not trip this
  clause.

The passing run IS the fixture's disposition: deletion-never-archive,
satisfied by the mechanism under test. **Before/after censuses are taken by
existence-and-holder only.**

**Arm 1 acceptance, synthetic, any venue:** under a fresh `EphemeralRoot` —
clean drop leaves nothing; SIGKILL leaves an orphan which the next boot
reclaims; a second live server's store under the same root survives the first
server's boot sweep; a panic-path `keep()` dir is reclaimed at next boot only
after its process dies. Red-first, loud teed logs, no silencing redirections,
suite-count tell before trusting green.

## 6. RULED (Cally's second-reader pass, 2026-07-31)

1. **Root config: explicit `ephemeral_root`, and the server REFUSES at boot
   when ephemeral minting is needed and it is absent.** Sibling-of-persistence
   is dead code in the only case that mints — the ephemeral branch fires
   exactly when `persistence_path` is `None`, so there is nothing to be
   sibling of. A derived default root is the same bug one level up: an
   un-chosen location for `durable=false` payload bytes. The refusal message
   names the one-line fix. ⚠️ **This changes bare-boot behaviour, so it is
   flagged to Tom as a veto-window item at the change's landing** — not
   decided silently. Frame picks the config up at its re-pin, the
   already-ruled inheritance path.
2. **The haematite ask is filed with Apollo now** — filing starts his clock
   without coupling ours; the §3.4 interim + conformance pin carries
   independently until his next natural cut. Explicitly NOT tied to the
   corrected re-pin trigger (both #31 and #56), which is about error-type
   survival and moves on its own evidence.
3. **Arm 2 is a server subcommand.** Config-parsing inheritance beats
   standalone's only advantage, since a subcommand runs without a live
   server anyway.
