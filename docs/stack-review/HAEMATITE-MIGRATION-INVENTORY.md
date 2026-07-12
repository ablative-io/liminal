<!-- Brief-prep evidence for task: liminal haematite 0.4.1→next-line migration brief.
     Produced 2026-07-12 by a read-only inventory agent under Hermes Crumpet's
     liminal seat; spot-verification and the brief itself are the seat's work.
     Claims marked UNVERIFIED require a released haematite artifact to settle. -->

# Liminal → haematite migration inventory (for the 0.4.1 → next-line brief)

**Repos:** liminal `/Users/annabel/Developer/ablative/stack/liminal` (main tree, `liminal-rs`/`liminal-server` v0.2.3). haematite `/Users/annabel/Developer/ablative/stack/haematite` — working tree is **v0.6.0-class dev state** (`crates/haematite/Cargo.toml:3` says `version = "0.5.0"`, on branch `incident-fence-chain` with unreleased FENCE-CHAIN/recovery-boot/CSOT work layered on top). **Local tags stop at `haematite-v0.4.1`** — there is no `0.5.0` or `0.6.0` tag in this tree, so all 0.5.0-state claims below are read from git history commits and the in-tree CHANGELOG/Cargo.toml, marked where inferential.

## §1 — API touchpoint table

Every direct `haematite::` type/call in liminal. All durability API flows through liminal's own `DurableStore` trait (`crates/liminal/src/durability/store.rs:22-58`); the concrete impl and error mapping are the only haematite leak points.

| # | Site (file:line) | haematite item | liminal role | Behind `DurableStore`? |
|---|---|---|---|---|
| 1 | `crates/liminal/src/durability/store.rs:4` | `import ApiError, Database, DatabaseConfig, Event, EventStore` | module-level imports | — |
| 2 | `store.rs:67,73` | `Arc<EventStore>` in `HaematiteStore` field + `HaematiteStore::new(event_store: Arc<EventStore>)` **(pub const fn)** | store construction | concrete impl of trait |
| 3 | `store.rs:93-96` | `EventStore::append(key, payload, expected_seq)` | event append (CAS on seq) | yes |
| 4 | `store.rs:112-115` | `EventStore::read_from(key, offset)` | event read | yes |
| 5 | `store.rs:137-139,162-165` | `EventStore::read_value(key)` | CAS value read | yes |
| 6 | `store.rs:157-159` | `EventStore::cas(key, expected: Option<u64>, new)` | compare-and-swap (cursor/dedup) | yes |
| 7 | `store.rs:174-183` | `EventStore::scan(pred on meta.stream_key)` + `EventStore::read(stream_key)` | prefix scan | yes |
| 8 | `store.rs:190` | `EventStore::flush()` | durable flush | yes |
| 9 | `store.rs:98-100` | `ApiError::CorruptEvent(..)` | contract-violation error synth | yes |
| 10 | `store.rs:264,267` | `Database` (field), `EventStore::new(database)` | ephemeral store construction | concrete impl |
| 11 | `store.rs:401-408` | `Database::create(DatabaseConfig{ data_dir, shard_count, sweep_interval:None, distributed:None })` | ephemeral open/create | construction |
| 12 | `store.rs:411-418` | `From<Event> for StoredEntry` reads `event.payload/.seq/.timestamp` | event field projection | yes |
| 13 | `store.rs:426-437` | `From<ApiError>`: matches `SequenceConflict / CasMismatch / CorruptEvent / Storage / HistoryCompacted` | error taxonomy mapping | yes |
| 14 | `durability/error.rs:10` | `DurabilityError::StoreError(haematite::ApiError)` **(pub enum variant)** | error carrier | — (leak, see §2) |
| 15 | `error.rs:65-72` | `From<haematite::SequenceConflict>` (`.expected/.actual: u64`) | error mapping | — |
| 16 | `error.rs:74-85` | `From<haematite::CasMismatch>` (`.expected/.actual: Option<u64>`) | error mapping | — |
| 17 | `liminal-server .../services.rs:7` | `import Database, DatabaseConfig, EventStore` | server construction imports | — |
| 18 | `services.rs:862-866` | persistent path: `EventStore::new(database)` + `HaematiteStore::new(Arc::new(..))` | durability store construction | via `Arc<dyn DurableStore>` |
| 19 | `services.rs:871-885` (`open_or_create_database`) | `Database::open(data_dir)` / `Database::create(DatabaseConfig{..})` | startup/recovery (open-or-create) | construction |
| 20 | `services.rs:855` | `EphemeralHaematiteStore` in factory closure type | ephemeral lifecycle | — |
| 21 | `liminal/src/durability/store.rs:347,374` | `open_ephemeral(shard_count)` / `open_ephemeral_rooted` (test seam) → returns `EphemeralHaematiteStore` | ephemeral store lifecycle | wrapper |
| 22 | test files: `cursor/tests.rs:337-338`, `recovery/tests.rs:436-437`, `replay/tests.rs:299-300`, `dedup/tests.rs:525-526`, `conversation/tests.rs:333-334` | `ApiError::Storage(DatabaseError::IoError(..))` | fault injection in tests | — |
| 23 | `liminal-server .../services_r5_tests.rs:6,21-28` | `Database::create`, `EventStore::new`, `HaematiteStore::new` | test store construction | — |

**Roles covered:** durability store construction (2,10,18), append (3), read (4,7), CAS (5,6), scan (7), flush (8), startup/recovery open-or-create (19), ephemeral store lifecycle (10,11,20,21), error taxonomy (9,13,14–16,22).

**Note:** `recovery.rs`, `replay.rs`, `cursor.rs`, `dedup.rs`, `channel/storage.rs` mention haematite only in **comments** — they operate through the `DurableStore` trait and leak no haematite type. Confirmed: `grep "haematite::" recovery.rs` (non-comment) returns nothing.

## §2 — Boundary verdict

**Refined from "no":** haematite types are **confined to the durability layer** and do **not** reach liminal's top-level prelude, wire protocol, envelope, channel/conversation surface, or the SDK — but they are **not** fully hidden; they cross in three precise places, two of which are public and one cross-crate.

- **liminal top-level public API is clean.** `crates/liminal/src/lib.rs:13-21` re-exports only `channel`, `conversation`, `envelope`, `error`, `metrics`, `tracing` types — no haematite type. **liminal-sdk has zero haematite references** (grep empty).
- **`liminal::durability` public module DOES expose haematite types:**
  - `HaematiteStore::new(event_store: Arc<haematite::EventStore>)` — pub, re-exported at `durability/mod.rs:33-35`.
  - `DurabilityError::StoreError(haematite::ApiError)` — pub enum variant (`error.rs:10`), re-exported (`mod.rs:22`), plus public `From<haematite::SequenceConflict>` / `From<haematite::CasMismatch>` impls.
- **Cross-crate crossing:** `liminal-server` names haematite types directly — `use haematite::{Database, DatabaseConfig, EventStore}` (`services.rs:7`) and calls `Database::create/open`, `EventStore::new`, `HaematiteStore::new` (`services.rs:862-885`). This is why `liminal-server/Cargo.toml:15` carries its own `haematite` dep. So haematite types cross **from liminal into liminal-server**, but always collapse to `Arc<dyn DurableStore>` at the service boundary (`services.rs` `durable_store() -> Result<Arc<dyn DurableStore>>`).
- **The `DurableStore` trait itself is haematite-free** — its signatures use only liminal-owned `StoredEntry` + `DurabilityError` (`store.rs:22-58`). The abstraction is real; the leak is only the concrete impl + error carrier.

**Critical for the beamr-unification release note — NO beamr type crosses into liminal at all.** The haematite API liminal touches (`Database`, `DatabaseConfig`, `EventStore`, `Event`, `ApiError`) exposes no beamr type. Verified: `DatabaseConfig.distributed: Option<DistributedDatabaseConfig>` (`haematite db/config.rs:34`), and `DistributedDatabaseConfig` uses `SyncNodeId` (haematite's own type), not a beamr type. beamr types appear in haematite's public surface **only** in `sync/transport_glue.rs` (`ConnectionManager`, `Atom`, `Arc<DistConnection>` — `transport_glue.rs:31-98`), which liminal never calls. So haematite 0.4.1's transitive beamr 0.13 is fully encapsulated behind types liminal uses.

## §3 — Cargo / features snapshot

**Version reqs (liminal):**
- `Cargo.toml:39` → `haematite = "0.4.1"` (workspace dep, propagated via `{ workspace = true }` in `crates/liminal/Cargo.toml:27` and `crates/liminal-server/Cargo.toml:15`).
- `Cargo.toml:30` → `beamr = { version = "0.14.0", features = ["readiness"] }` (direct).
- `crates/liminal/Cargo.toml:20` → `beamr` features `["cooperative", "json"]`. `crates/liminal-server/Cargo.toml` → beamr features `["json"]`.
- No features enabled on `haematite` (default only). liminal's own `test-support` feature gates `open_ephemeral_rooted`.

**Lock resolution (`Cargo.lock`):** one `haematite v0.4.1` (registry, line 822-823); **two beamr** — `beamr v0.13.0` (line 147-149) and `beamr v0.14.0` (line 177-179).

**Dual-beamr graph (verified via cargo tree):**
- `cargo tree -e features -i beamr` → *"multiple `beamr` packages… ambiguous: beamr@0.13.0, beamr@0.14.0"* (confirms the split).
- `beamr@0.13.0` reached **only** transitively: `beamr v0.13.0 → feature "default" → haematite v0.4.1 → liminal-rs / liminal-server`. Features pulled: haematite's default set = `embedded, fs, jit, net, std, threads`.
- `beamr@0.14.0` reached **directly** by `liminal-rs` and `liminal-server`. Features: `cooperative` (liminal-rs only), `default`, `json`, `readiness`, `std`.
- `cargo tree -p liminal-server -e normal` shows `haematite v0.4.1 → beamr v0.13.0` as a distinct subtree beneath the direct `beamr v0.14.0`.

**Benign-today evidence:** the two beamr copies share no type edge (§2). They even carry disjoint feature sets (0.13: threads/jit/embedded; 0.14: cooperative/readiness/json), so they are genuinely independent compilation units, not a fightable unification-by-features.

## §4 — Kinds adoption map

**What the "kinds" line is:** haematite 0.5.0's headline feature is **branch kinds** (persisted, engine-enforced branch namespace boundary), *not* an EventStore-API feature. Evidence (git log, reachable in this tree):
- `a8c4430` "release: haematite 0.5.0 — branch kinds make the merge surface kind-aware (deliberate break)"
- `bd95436` Merge `branch-kind-marker`: "persisted branch kinds + engine-checked namespace boundary — haematite 0.5.0"
- `e7c49c8` "feat(branch): persist and enforce branch kinds"
- `e18f259` design brief: "Namespace/Work kinds, engine-checked boundary, legible refusals" (Frame D6 storage half)
- `b4c63ba` mentions `fork_from_with_kind` — the API is on the **branch/fork/merge** surface.

**Liminal call sites that would change: NONE (verified).** Liminal uses only the `EventStore` append/read/cas/scan primitive and `Database` open/create. It does **not** use branch/fork/merge/`fork_from_with_kind` — grep for `branch`, `fork`, `merge`, `kind` against haematite usage in liminal returns nothing beyond the durability primitive. The branch-kind marker types (`Namespace`/`Work` kinds) sit on an API liminal never touches.

**What adoption would buy liminal:** essentially nothing *forced*. The "kinds" adoption in the migration brief would be **optional namespace hygiene** — if liminal ever wanted its durable streams tagged with a kind marker for engine-checked namespace separation (e.g. channel-partition streams vs cursor/dedup CAS keys, currently separated only by string key prefixes formatted in `channel/storage.rs:152`, `cursor.rs:235`, `dedup.rs:137`). Benefits *if adopted*: engine-enforced namespace boundary (vs today's convention-only key prefixes), legible refusals on cross-namespace access. It does **not** change wire format of stored payloads. **This is a "may adopt" not a "must adopt"** — the version bump does not require it. Mark the type-safety/wire-format specifics **UNVERIFIED** against a real EventStore-level kind API: the 0.5.0 kind marker is a *branch* concept, and whether haematite exposes any stream/key-level kind on the append/cas primitive liminal uses is **not evidenced** in this tree.

## §5 — Risk register

1. **API drift 0.4.1 → 0.5.0 on liminal's primitives — LOW, UNVERIFIED-clean.** The in-tree CHANGELOG (`crates/haematite/CHANGELOG.md`) has **no `## 0.5.0` section** (jumps `Unreleased` → `0.4.0` → `0.3.0`), so the 0.5.0 changes are not documented there; 0.5.0 is described only by release commit `a8c4430` as a *branch-surface* "deliberate break." No evidence that `EventStore::{append,read_from,read,cas,read_value,scan,flush}`, `Database::{create,open}`, `DatabaseConfig`, `Event`, or `ApiError` variants changed between 0.4.1 and 0.5.0. **Action for brief author: diff the `haematite-v0.4.1` tag against the 0.5.0 release commit for these exact symbols before finalizing.** Mark UNVERIFIED until done.
2. **`ApiError` variant set is load-bearing.** Liminal exhaustively matches `SequenceConflict | CasMismatch | CorruptEvent | Storage | HistoryCompacted` (`store.rs:426-437`). Any added/renamed `ApiError` variant is a compile break at that match and at `error.rs:10`. `CasMismatch.actual/.expected` are `Option<u64>` and `SequenceConflict.expected/.actual` are `u64` — field-shape changes break `error.rs:65-85`.
3. **`DatabaseConfig` field set is load-bearing.** Liminal constructs it literally with `{ data_dir, shard_count, sweep_interval: None, distributed: None }` at `store.rs:401-408` and `services.rs:875-880`. A new non-`Default` field (0.5.0+ CSOT work is adding cluster-members state per the `## Unreleased` CHANGELOG) breaks these literals. The tree already shows active `db/config.rs` churn.
4. **wasm dual-beamr (flagged for Apollo's audit) — CONFIRMED.** haematite's `wasm-runtime` feature pulls `beamr-wasm = "0.5.0"` (`haematite Cargo.toml:56`), and `beamr-wasm 0.5.0` depends on **`beamr 0.11.0`** (haematite `Cargo.lock:148-160`, dependency list line `"beamr 0.11.0"`). haematite's native build pins `beamr = "0.13.0"` (`Cargo.toml:45`, dev-dep `:112`). So haematite *itself* carries a 0.13 (native) / 0.11 (wasm) beamr split, independent of liminal's 0.13/0.14 split. liminal does not enable `wasm-runtime`, so this 0.11 line does **not** enter liminal's graph today — but it is a second unification front the 0.6.0 unified release must also resolve. Cite for Apollo.
5. **Liminal tests pinning haematite-specific behavior — several, all in `store.rs` ephemeral-lifecycle module (`store.rs:463+`):**
   - `ephemeral_store_open_failure_leaves_zero_residue` (~`store.rs:555-575`): pre-seeds a `config.json` into the guard dir to force `Database::create` to **refuse** — pins haematite 0.4.1's "create refuses on pre-existing `config.json`, and removes only a dir it created, never a pre-existing one" behavior (documented at `store.rs:338-342`). A 0.5.0 change to create-on-nonempty-dir semantics or the config filename would flip this test.
   - `ephemeral_dir_removed_after_last_handle_drops` / `..._survives_until_last_store_clone_drops` (`store.rs:495-524`): pin database-close → shard-actor-join → writer-lock-release ordering on `Drop`.
   - `repeated_ephemeral_cycles_each_own_distinct_dir_zero_residue` (`store.rs:580`), `guard_leaks_directory_when_store_drop_panics` (`store.rs:626`): pin residue/leak semantics.
   - Startup/open-or-create partial-open: **liminal has no dedicated startup partial-open test** — `open_or_create_database` (`services.rs:871`) branches on `config.json` existence; no test injects a partially-written database dir. Mark: **no partial-open coverage exists (gap, not a pinned behavior).**
   - Fault-injection tests (§1 row 22) construct `ApiError::Storage(DatabaseError::IoError(..))` directly — depend on `DatabaseError::IoError` existing and being constructible; a `DatabaseError` refactor breaks 5 test files.

## §6 — Release-note line draft (liminal 0.2.4)

> **Durability graph carries two beamr copies (0.13 via haematite 0.4.1, 0.14 direct) — deliberate and benign.** liminal depends directly on `beamr 0.14.0` (channel/connection scheduler, `readiness`/`cooperative` features) while `haematite 0.4.1` pulls its own `beamr 0.13.0` transitively for the durable event store. The two never exchange a type: haematite fully encapsulates its beamr behind `EventStore`/`Database`/`ApiError`, none of which expose a beamr type across liminal's boundary (beamr types live only in haematite's unused `sync::transport_glue`), and the copies compile with disjoint feature sets — so there is no runtime cost, no idle resident state, and no correctness surface to the split. Re-unification onto a single beamr line is a haematite-side change (beamr types cross haematite's public sync surface, making it a major/0.6.0-class bump) and rides the next haematite release; it is intentionally deferred, not overlooked.

---

## Condensed summary

- **§1:** ~23 direct haematite touchpoints, all in the durability layer. The full append/read/cas/scan/flush primitive is used via `EventStore` behind liminal's own `DurableStore` trait (`crates/liminal/src/durability/store.rs:22-58`). Construction is split: ephemeral (`store.rs:347-418`, `open_ephemeral`) and persistent open-or-create (`liminal-server .../services.rs:871-885`, `Database::open`/`create`). `recovery/replay/cursor/dedup` touch haematite only through the trait.
- **§2:** liminal's top-level API + SDK are haematite-free, but haematite types **do** cross in three spots: `HaematiteStore::new(Arc<EventStore>)` (pub), `DurabilityError::StoreError(haematite::ApiError)` + its `From` impls (pub), and `liminal-server` naming `Database/DatabaseConfig/EventStore` directly (cross-crate). All collapse to `Arc<dyn DurableStore>` at the service boundary. **No beamr type crosses into liminal anywhere** (verified: `DatabaseConfig.distributed` uses haematite's `SyncNodeId`, not beamr).
- **§3:** `haematite = "0.4.1"` (default features); direct `beamr 0.14.0` (`cooperative/json/readiness`). Lock has both `beamr 0.13.0` (only via haematite) and `beamr 0.14.0` (direct). `cargo tree` confirms the split; disjoint feature sets, no shared type edge.
- **§4:** haematite 0.5.0 "kinds" = **branch kinds** (fork/merge namespace markers, commits `a8c4430`/`e7c49c8`), on an API liminal **does not use**. Zero forced liminal call-site changes. Adoption would be optional namespace hygiene over today's string-prefix stream keys; stream/key-level kind API is **UNVERIFIED** (0.5.0 kind is a branch concept).
- **§5:** Main risks — `ApiError` exhaustive match + `DatabaseConfig` struct literal + `DatabaseError::IoError` construction are load-bearing across ~7 files; 0.5.0 EventStore-API drift is **UNVERIFIED** (in-tree CHANGELOG has no 0.5.0 section — diff the 0.4.1 tag vs release commit `a8c4430`); ephemeral `config.json`-refusal + drop-ordering tests pin 0.4.1 behavior; **no startup partial-open test exists**. wasm dual-beamr **confirmed**: `beamr-wasm 0.5.0 → beamr 0.11.0` (haematite `Cargo.lock:148-160`), separate from liminal's split, for Apollo's audit.
- **§6:** Release-note line drafted above — states both beamr versions, why benign (no type crossing, disjoint features, no idle cost), and that re-unification is a haematite 0.6.0-class change riding the next line.
- **Environment caveat:** the haematite tree is an unreleased **0.5.0-versioned dev state** (branch `incident-fence-chain`); only tags ≤ `v0.4.1` exist locally. All 0.5.0/0.6.0 statements come from git history + in-tree files, flagged UNVERIFIED where a released artifact would be needed.
