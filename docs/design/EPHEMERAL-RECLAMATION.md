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
2. **The mechanism is signal-agnostic — and the signal question is now
   MEASURED CLOSED (2026-07-31 11:16Z, upgrading this section).** The
   kill-arm datum (`dBddZ4`, harness stop) had left the clean-SIGTERM leg
   open. It is open no longer: the gallery app-host on 6010 received a
   SIGTERM and drained **provably gracefully** — logged component stop,
   "durable state flushed", "graceful shutdown sequence complete" — and
   `liminal-durability-XkxShj` is PRESENT, NO HOLDER, FULL SIZE
   (331,620 KiB; Waffles ~5 min post-shutdown, Cally independently at
   ~11:22Z, same number to the kilobyte, lsof empty both times).
   **Shutdown flavor does not discriminate: no observed exit path cleans.**
   Two different host binaries (frame-host, app-host), same embedded
   liminal, same outcome — so this is not one host's quirk. ⇒ Boot-time
   reclamation is not belt-and-braces; **it is the only cleaner over every
   OBSERVED exit path** (Waffles' framing with the tested-path qualifier —
   the source has a clean path that cleans, and liminal's lifecycle tests
   exercise it; the claim is about the measured population, not every path
   that exists).
   **The mechanism census, CLOSED 2026-07-31 (three candidate mechanisms,
   three seats, one afternoon):** *(a) drop-ran-and-panicked* — REFUTED on
   the graceful leg: the panic path's sanctioned leak prints a signature
   (`store.rs:270-293`, `dir.keep()` at `:284`, log line "ephemeral store
   drop panicked; leaking its directory…"), and that line is ABSENT from
   the app-host log (instrument credible: 141 raw lines, lower-severity
   tracing present throughout). *(b) drop-never-ran* — REFUTED on BOTH
   legs, structurally: a live control store holds `config.json` + shards +
   `writer.lock`; **both real orphans hold shards ONLY** — deletion ran and
   stopped partway, so Drop ran. *(c) drop-ran, removal failed SILENTLY,
   partial* — **CONFIRMED on both legs.** `tempfile`'s `TempDir::drop` is
   `let _ = remove_dir_all(self.path())` (tempfile-3.27.0 `dir/mod.rs`,
   Result DISCARDED — verified independently at three seats); `close()`
   exists for callers who want the error. The absent grep is thereby
   positively explained: `let _` swallows the error, no line by
   construction. **The upstream fix — the clean path calls
   `TempDir::close()` explicitly and LOGS the error — is task #5, a
   confirmed-defect fix with two production receipts**: the impl's doc says
   the directory "is removed"; the mechanism says "removal is attempted and
   errors vanish." Even with that fix landed, SIGKILL and panic-keep
   remain, and a host exit fix only shrinks the population — so the
   reclaimer is load-bearing on every branch, **including the measured one
   where every destructor runs perfectly and production data is silently
   half-deleted.**
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

⚠️ **REVISED 2026-07-31 after the mechanism census closed — the earlier
five-verdict set had a defect the measurements exposed: it read
absence-of-lock as the pre-create window and skipped such dirs forever.
But confirmed mechanism (c) means REAL orphans are PARTIALLY DELETED
stores — both real-world exhibits are shards-only, `writer.lock` and
`config.json` already gone — so the old rule would have skipped the
design's own acceptance fixtures unreclaimed, permanently.** Absence of
the lock is not absence of an owner: partial deletion removes the lockfile
while a live writer's flock survives on the unlinked inode, invisible to
any new probe. Two disciplines therefore exist, selected by what the probe
finds:

**Lock-present discipline** (`writer.lock` exists as a regular file — the
husk class measured earlier is exactly this):

- **`live-skipped`** — lock held elsewhere (`DataDirLocked`-shaped refusal)
  ⇒ a live writer owns it ⇒ leave it, count it.
- **`reclaimed`** — lock acquired ⇒ no live writer ⇒ **delete the directory
  while still holding the lock**, then release: haematite's failed-create
  discipline (`db.rs:130-132`), atomic ownership, no TOCTOU.

**Lock-absent discipline** (no `writer.lock` — the partially-deleted orphan
class, which both real-world exhibits inhabit):

- ⛔ **LOCK-ABSENCE IS NEVER PERMISSION — the damage is self-confirming**
  (Athena's sharpening, adopted as the discipline's first rule). Partial
  deletion MANUFACTURES exactly the signature a lockfile-keyed reclaimer
  would read as "no owner": `remove_dir_all` deletes the lockfile early in
  its walk, while shard workers can still be live behind it. A reclaimer
  that fires on lock-absence performs **removal under live workers** —
  precisely the corruption `store.rs:261-263` deliberately leaks to avoid,
  undone at exactly the moment the author's choice exists to protect.
- The positive liveness predicate is therefore: **any open fd under the
  directory = ALIVE — never the absence of a file that removal deletes
  first.** Holder present ⇒ **`live-skipped`**. No holder ⇒
  **`reclaimed-lockless`** — its own verdict name, so the counts always
  show which discipline fired. The check is an existence-and-holder
  measurement (lsof-class), never a content read.
- **The census instrument's OWN failure is a named arm (R4): if holder
  enumeration is unavailable or errors, the verdict is `refused` — never
  assumed-no-holder.** The blocked-instrument law applies everywhere, but
  this is the ONE site where a silent false "no holder" performs removal
  under live workers, so it gets its own arm rather than a general
  citation: no lsof-class answer, no deletion, ever.
- **Evidence status, labelled honestly (Athena's own labelling, kept):**
  the fd predicate is **DERIVED FROM MECHANISM, NOT DEMONSTRATED BY
  SAMPLE.** On every observable store the fd predicate and the unsound
  lockfile predicate AGREE (live control: fds present, lock present; both
  orphans: fds absent, lock absent) — the state where they differ, lock
  deleted while shard workers still hold fds, is the transient window
  inside a failing drop and has never been sampled. The derivation: fds
  are what the kernel tracks, and the mid-deletion window provably has
  them while provably lacking the lockfile. Build against the derivation;
  do not cite the agreeing sample as proof of superiority.
- Deletion without a lock to hold is safe ONLY under the per-arm mint
  guarantee (§3.2's root-level serialization for Arm 1; operator-vouched
  dead population for Arm 2) — this discipline is never valid over a
  population where a legitimate creator could be mid-mint.
- **Binding, unchanged: the probe must never open-with-create** —
  haematite's acquisition opens creating-if-absent (`db/lock.rs:71`), so an
  O_CREAT probe would make a legitimate creator's acquire fail
  `DataDirLocked` at mint.

**Both disciplines share:**

- **`vanished`** — ENOENT mid-probe: the owner finished its teardown, or a
  concurrent reclaimer won. Benign; **distinct from `refused`**, so the §5
  zero-refused clause cannot convert a benign race into a STOP.
- **`refused`** — a real error (permissions, IO fault, a `writer.lock` that
  is not a regular file per the symlink-anchor rule, unrecognised state).
  Never silent, always carries the error text.

**The pre-create window is no longer inferred from the dir's contents** —
a freshly-minted empty dir and an almost-fully-deleted orphan are
indistinguishable from the outside, so the protection moves up a level:
Arm 1 serializes mint against sweep at the root (§3.2); Arm 2's population
contains no live creators by the operator's explicit vouching. **This
paragraph is NEW DESIGN absorbed under measurement pressure and needs its
own review pass** — flagged, not slipped in.

Reclamation obeys the sensitive-residue rules: the reclaimer **stats, locks,
and deletes — it never opens store contents** (existence-and-holder only),
and disposition is **deletion, never archive** (the residue is message
content under a `durable=false` promise).

Each candidate produces exactly one loud log line: path and verdict, with
the error text on refusals. A sweep summary reports **five verdict names in
six buckets** — `reclaimed` / `reclaimed-lockless` / `live-skipped` (split
per-discipline, two buckets) / `vanished` / `refused` — so the count and
its domain agree on the page. **No silent outcomes** — a reclaimer that
cannot look must say so, not report a clean world (the blocked-instrument
law).

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

**The production receipt for this arm (2026-07-31, the JJuaBk case):** a
live production server on Tom's own box held its runtime ephemeral store in
a system-temp TempDir — the exact class this section removes — and the
mechanism-(c) exit path was live on it. Blast radius stayed at 4 KiB only
because that store happened to be empty (the real data was safe on durable
disk; measured by full lsof, closed at zero residual cost, insurance-copied
by Waffles). **The class was live on production and the absence of written
data is the only thing that kept it free** — that is why a server must
never hold its runtime root in a TempDir, stated with a case rather than a
hypothesis. The class defect rides to Tom as a design item with this arm
as the named remedy.

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

**The mid-create race, named — resolution REVISED with the §3.1 rework:**
on a shared root the sweep can meet a sibling store MID-CREATE (§5's own
acceptance runs a second live server under the same root). The earlier
resolution (skip any lockless dir) died with the old verdict set — real
orphans are lockless, so it protected the mint window by abandoning the
mission. Replacement, **new design, flagged for review with the §3.1
rework:** `EphemeralRoot` carries a ROOT-LEVEL lock that serializes mint
against sweep — a mint holds it from TempDir creation until the store's
own `writer.lock` acquisition lands; the sweep holds it exclusively for
the sweep's duration. A dir observed lockless DURING a sweep is therefore
provably not mid-mint, which is what licenses the lock-absent discipline
inside Arm 1. The root lock is held across the mint WINDOW only (sub-second),
never across store lifetime, so it serializes nothing in steady state.
"Second server survives" then covers mid-create by mechanism rather than
by verdict-set accident.

**The root lock is PINNED, not gestured at (R3)** — this document refuses
unpinned locks one section down, so its own second lock meets the same bar:

- **(a) Mechanism class:** an OS advisory fd lock, kernel-released on
  process death, and **RAII-released on EVERY mint-window failure path** —
  panic, failed `writer.lock` acquisition, any error return. Without that
  property a wedged mint silently deadlocks every future boot sweep; the
  release-on-all-paths behaviour is itself a red-first test.
- **(b) Creation discipline:** created by `EphemeralRoot` at root creation,
  exactly once — **no later actor ever probes it with O_CREAT**, the same
  rule the store lockfile carries.
- **(c) Name and location:** at the root, under a name the sweep's
  `liminal-durability-*` candidate predicate structurally cannot match
  (e.g. `.ephemeral-root.lock` — dotted, un-prefixed; final name at
  implementation, the non-matching property is the requirement).
- **(d) Anchor discipline:** the regular-file refusal applies — a root
  lock that is a symlink or special file is `refused` before any open.
- **(e) Conformance pin:** the same test class as `writer.lock`'s (§3.4) —
  a test creates a root and asserts the lock file's path, so a rename
  breaks a test instead of silently un-serializing mint from sweep.

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

**The vouching has teeth (R5) — it is a RUN CONDITION, not a sentiment: no
ephemeral-minting process starts or restarts for the run's duration.** The
threat is concrete and has a name: 99844-class pre-design binaries mint
into exactly the swept population on restart. A restart mid-run creates a
fresh lockless, fd-less dir inside its mid-mint window — with no root lock
to protect it, the lock-absent discipline would reclaim it out from under
the creator. So the acceptance run RECORDS the condition as checked: a
process census (which ephemeral-minting processes exist) taken before and
after the run, **sharing its count-domain with the population census** —
same box, same predicate class — so "no creator started" is a measured
sentence, not an assumed one.

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

**Fixtures (claimed, ruled in task #3):** `dBddZ4` (~140 MiB, kill-arm
provenance) and `XkxShj` (~331 MiB, graceful-SIGTERM provenance, preserved
under the same no-sweep ledger entry) — the two exit-path flavors, so the
acceptance run proves the reclaimer over BOTH observed orphan classes — plus
the live neighbours as in-situ negative controls. **Both fixtures are
DAMAGED ARTIFACTS, and that is what makes them faithful:** confirmed
mechanism (c) means the real reclaim class is partially-deleted stores
(both exhibits are shards-only, lock and config already gone), and they
exercise the §3.1 lock-absent discipline — the one the original verdict
set could not serve. **Any SYNTHETIC fixture must likewise be a partially
deleted store, not a complete one** — a complete-store fixture tests the
easy discipline and licenses nothing about the real population, which is
the sampling law wearing filesystem clothes.

**Arm 2 acceptance, on the box where the fixture lives, post-freeze, under
named GO:** operator invokes legacy reclaim naming the system-temp
population. PASS requires ALL of:

- `dBddZ4`: **`reclaimed-lockless`, via the lock-absent discipline** —
  absent afterwards, its log line naming path, verdict, and discipline. It
  is a shards-only orphan (measured at three hands), so a run reporting
  plain `reclaimed` for it has either misclassified or collapsed the count
  separation the verdict split exists for — **the expected verdict names
  the expected discipline, per fixture.**
- `XkxShj`: **`reclaimed-lockless`, via the lock-absent discipline** — same
  clause, its own bullet; the graceful-provenance fixture must be SEEN to
  exercise the same discipline, not assumed covered by its sibling.
- The husk population (lock + config, no shards): **`reclaimed`, via the
  lock-present discipline** — the atomic claim-then-delete's own receipt.
- Every dir with a live holder (the manifold surface's store, the current
  boot's store): **`live-skipped`** — present afterwards, holders intact.
- The husk count reported against the census taken immediately before the
  run — and **the census and the tool's population must share a DOMAIN:
  same predicate, same root** (the count-domain law), or the comparison
  silently compares two populations.
- Zero `refused` verdicts — **a refusal instead of a reclaim is a STOP back
  to design** (F8's clause, same reason: a mechanism that cannot handle the
  real case is not amended in the field). `vanished` is NOT a refusal
  (§3.1) and does not trip this clause; `live-skipped` outcomes are the
  negative controls passing, not failures.

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
