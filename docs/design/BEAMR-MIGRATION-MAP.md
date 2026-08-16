<!-- SEAT VERIFICATION (Hermes Crumpet, 2026-08-16, at main ec38f21 / beamr tags as cited):
Six load-bearing claims re-measured at the dispatcher's own hands before this
landed: (1) scheduler/suspension.rs blob-identical v0.16.3=v0.17.0=v0.18.2
(git rev-parse blob f4496260 all three) — the "suspend surface exists only
from 0.17" premise is FALSE; (2) ProcessHandle { pid: u64 } private at
v0.18.2:crates/beamr/src/process/registry.rs:22-24; (3) both Waiting sites
confirmed — NOTE the second is interpreter/opcodes/trampoline.rs (~:295), not
scheduler/trampoline.rs; (4) Scheduler::new + natives: NativeBifs delta
confirmed at both tags (v0.16.3 mod.rs:1058 vs v0.18.2 mod.rs:1066);
(5) liminal LIVENESS_POLL at wait.rs:24 and subscribe_exit_events at
supervisor.rs:1169 confirmed @ec38f21; (6) the §2.6 changelog advisory
confirmed verbatim at v0.18.2:CHANGELOG.md. On §2.6's urgency for liminal:
already SETTLED by the standing two-legged exposure verdict (08-15, beamr
road post dd03e0fe thread) — liminal's hand-built modules carry empty
function_tables and are JIT-invisible by construction, so a compiled `!`
cannot exist in liminal regardless of feature flags; the advisory argues the
migration for OTHER embedders and for estate hygiene, not from liminal
exposure. Open item 4 (cargo tree -e features) is thereby moot for the
exposure question, still worth running at branch-cut for the record. -->

# beamr migration map — liminal workspace off 0.16.3

**Status: DRAFT for dispatcher verification. Every claim below is to be re-checked at the dispatcher's own hands.**

| field | value |
|---|---|
| liminal rev (all liminal citations) | `93d8cc747b953514c3de2c11cea8fde0085dfeb3` (main) |
| liminal working tree at census | clean except untracked `.claude/skills/onboarding/`, `.claude/skills/profile-builder/` |
| beamr repo (read-only) | `/Users/tom/Developer/ablative/stack/beamr`, HEAD `1965650d1a003fb7076047fdeee114cca7905b32` (never checked out; all reads via `git show <tag>:` / `git grep -n <tag>`) |
| current pin | `Cargo.toml:45 @ 93d8cc7` — `beamr = { version = "0.16.3", features = ["readiness"] }` |
| resolved | `Cargo.lock:147-149 @ 93d8cc7` — `name = "beamr"` / `version = "0.16.3"` / `source = registry+https://github.com/rust-lang/crates.io-index` |
| migration target | 0.18.2-or-later; this map keys on **API signatures**, not the version number |

**Per-crate feature pins (these differ from the workspace line and matter — the union is what must survive):**

| crate | manifest line @ 93d8cc7 | features |
|---|---|---|
| workspace | `Cargo.toml:45` | `readiness` |
| `liminal` | `crates/liminal/Cargo.toml:25` | `cooperative`, `json` |
| `liminal-server` | `crates/liminal-server/Cargo.toml:13` | `json` |

Union of enabled features = **`readiness`, `cooperative`, `json`**.

---

## 1. TOUCHPOINT CENSUS

### 1.1 Predicates used, and the positive control

All greps run from `/Users/tom/Developer/ablative/stack/liminal` at `93d8cc7`. `target/` is not matched (no `.rs` under it is reachable from `crates/`); `.claude/` is excluded by scoping every grep to `crates/`.

| # | predicate | purpose | result |
|---|---|---|---|
| A | `grep -rn --include="*.rs" "beamr" crates/` | broadest possible net | 325 lines, 54 files |
| B | `grep -rn --include="*.rs" -E "beamr::\|use beamr" crates/` | **API touchpoints only** | **136 lines, 33 files** |
| C | `grep -rn --include="*.rs" "beamr" crates/ \| grep -vE "beamr::\|use beamr"` | prose/comment mentions | 189 lines |
| D | `grep -rn --include="*.rs" -E "^\s*impl(<[^>]*>)?\s+.*\bTRAIT\b\s+for\b" crates/` | trait impls that never spell `beamr::` | see §1.4 |

**Positive control (run BEFORE trusting any zero).** Predicate B was run against `crates/liminal/src/channel/actor/beam.rs`, the channel actor's known beamr consumer, and returned its import block (`beam.rs:17-24 @ 93d8cc7`, e.g. `:17 use beamr::atom::Atom;`). The predicate detects a known-present case, so its zeros are informative.

⚠ **A zsh trap that produced a false zero, recorded so the dispatcher does not repeat it.** The first run of predicate A was written unquoted (`--include=*.rs`); zsh glob-expanded `*.rs` against the cwd, `grep` errored `no matches found`, and the pipeline printed **`0`**. An unquoted `--include` under zsh yields a confident, wrong zero. Every predicate above quotes it.

### 1.2 Instrument disagreement, measured not argued

`grep` (predicate A) reported **325** lines; an independent Python attributor over the same file set reported **324**. Per the measure-don't-argue rule the single differing line was isolated by `comm` on the two sorted `file:line` sets:

- Only in grep: `crates/liminal-server/src/server/connection/supervisor_tests.rs:267 @ 93d8cc7`
- Only in Python: none

Line 267 is `fn spawning_connections_creates_distinct_beamr_processes()` — the enclosing item is the test function itself. The Python regex used `beamr\b`, and `beamr_processes` has no word boundary after `beamr`, so it declined the line. **Both instruments are correct**: grep counts a substring occurrence, Python counts a word occurrence, and the line is a *test function name*, not an API touchpoint. It is correctly excluded from the 136 API sites.

### 1.3 Totals

| quantity | count | how derived |
|---|---|---|
| API touchpoint sites (lines) | **136** | predicate B |
| files containing an API touchpoint | **33** | predicate B, `-l` |
| files mentioning beamr in prose only | **21** | 54 (A) − 33 (B) |
| API sites in `liminal` | **83** | `grep -c "^crates/liminal/"` |
| API sites in `liminal-server` | **53** | `grep -c "^crates/liminal-server/"` |
| distinct imported symbols | **59** | brace-group expander over every `use beamr…;` statement |
| distinct symbols incl. never-imported fully-qualified | **63** | 59 + `CoopSenderHandle`, `ets::OwnedTerm`, `distribution::connection::DistConnection`, `distribution::DEFAULT_COOKIE` |
| distinct `Scheduler` methods called | **18** | see §1.5 |

**`liminal-sdk` and `liminal-protocol` are beamr-free.** Predicate B over `crates/liminal-sdk/` returns exit 1 (no match); the same predicate over `crates/liminal-server/` returns 53 lines (positive control). Neither `crates/liminal-sdk/Cargo.toml` nor `crates/liminal-protocol/Cargo.toml` names beamr (both exit 1). The two beamr mentions in the SDK are prose: `crates/liminal-sdk/src/conversation.rs:13` and `crates/liminal-sdk/src/remote/websocket/web_socket/adapter.rs:56 @ 93d8cc7`. **The migration cannot reach the SDK or the protocol crate.**

### 1.4 Trait impls — conformance sites that never spell `beamr::`

These break on any trait method-set or signature change and are invisible to a `beamr::` grep. All @ `93d8cc7`.

| site | impl | method set as implemented |
|---|---|---|
| `crates/liminal-server/src/server/connection/process.rs:1065` | `impl<T: ConnectionTransport> NativeHandler for TransportConnectionProcess<T>` | `fn handle(&mut self, ctx: &mut NativeContext<'_>) -> NativeOutcome` (`:1066`) |
| `crates/liminal-server/src/server/connection/websocket/process.rs:1030` | `impl NativeHandler for WebSocketConnectionProcess` | `fn handle(…) -> NativeOutcome` (`:1031`) |
| `crates/liminal/src/conversation/actor/watcher.rs:93` | `impl NativeHandler for ActorExitWatcher` | `fn handle(…) -> NativeOutcome` (`:118`) |
| `crates/liminal/src/conversation/participant.rs:294` | `impl NativeHandler for ParticipantProcess` | `fn handle(…) -> NativeOutcome` (`:295`) |
| `crates/liminal/src/channel/subscription.rs:455` | `impl NativeHandler for SubscriberProcess` | `fn handle(…) -> NativeOutcome` (`:456`) |
| `crates/liminal/src/routing/function/execute/actor.rs:100` | `impl Actor for RoutingInvocationActor` | `type Call/Reply/Cast = i64` (`:101-103`), `fn handle_call(&mut self, request: Self::Call, ctx: &mut ActorContext<'_,'_>) -> Self::Reply` (`:105`), `fn handle_cast(…)` (`:127`), `fn store(&self, outcome: InvocationOutcome)` (`:139`) |
| `crates/liminal-server/src/cluster/discovery.rs:65` | `impl NodeResolver for ClusterResolver` | `fn resolve<'a>(&'a self, name: &'a str) -> ResolveFuture<'a>` (`:66`) |

**Total trait conformance sites: 7** (5 × `NativeHandler`, 1 × `Actor`, 1 × `NodeResolver`).

Predicate D returned **empty** for `NativeHandlerFactory` and for `Resolver`. Positive control: the identical predicate returned the five `NativeHandler` impls above, so it detects a known-present case. Both are used as **types**, not implemented:
- `NativeHandlerFactory` — a boxed-closure alias: `crates/liminal-server/src/server/connection/supervisor.rs:1237` `let factory: NativeHandlerFactory = Box::new(move || {…})`, plus return-type positions at `supervisor.rs:561,605,1290` and `websocket/supervisor.rs:352`.
- `Resolver` — a parameter type at `crates/liminal/src/channel/supervisor.rs:131` (`resolver: Resolver`).

### 1.5 `Scheduler` method surface consumed

Extracted with `grep -rhno --include="*.rs" -E "scheduler(\(\))?\.[a-z_]+\(" crates/` (receiver-anchored, so it also catches `self.scheduler.`). **18 distinct beamr methods.**

⚠ **Do not use a bare method-name predicate here.** `grep -rn "\.shutdown(" crates/` returns **240** lines because liminal has its own `shutdown` methods on many types; the receiver-anchored counts below are the sound ones. They are still a *lower* bound — a receiver bound to a differently-named local (e.g. `sched`) is not matched.

| method | receiver-anchored sites | notes |
|---|---|---|
| `process_table()` | 40 | **the F7 polling probe** — see §4 |
| `terminate_process()` | 27 | |
| `spawn_test_process()` | 18 | test-only surface |
| `is_linked()` | 6 | |
| `enqueue_atom_message()` | 4 | |
| `shutdown()` | 3 | |
| `spawn_native()` | 2 | |
| `readiness_deregister()` | 2 (+1 as `scheduler().`) | **`readiness` feature** |
| `pg_registry()` | 2 | |
| `atom_table()` | 2 | |
| `timers()` | 1 | |
| `take_exit_outcome()` | 1 | `supervisor.rs:2799` |
| `subscribe_exit_events()` | 1 | `supervisor.rs:1169` — **the existing TOLD surface** |
| `start_distribution_listener()` | 1 | |
| `spawn_native_trap_exit()` | 1 | `subscription.rs:591` |
| `peek_exit_reason()` | 2 | `supervisor.rs:2761`, `:2985` |
| `thread_count()` | 1 (as `scheduler().`) | |
| `service_inventory()` | 1 (as `scheduler().`) | |

Excluded as std smart-pointer methods, verified by reading the sites: `.upgrade()` (8, `Weak::upgrade` — e.g. `supervisor.rs:1068,1783,2750`), `.borrow_mut()` (4, `RefCell`), `.strong_count()` (1, `Arc`).

**Constructor / entry-ladder sites (the before-image for §2b):**

| site @ 93d8cc7 | enclosing item | call |
|---|---|---|
| `crates/liminal-server/src/server/connection/supervisor.rs:1131` | `SupervisorInner::new` (ctor region) | `Scheduler::with_services(SchedulerConfig{…}, SchedulerServices::from_config().owned_readiness(), registry)` |
| `crates/liminal/src/channel/supervisor.rs:152` | channel supervisor ctor | `Scheduler::new(…)` |
| `crates/liminal/src/conversation/actor.rs:289` | conversation actor ctor | `Scheduler::new(…)` |
| `crates/liminal/src/channel/subscription.rs:712` | `mod tests::cooperative_scheduler` | `WasmScheduler::new(atom_table, modules, bifs)` |
| `crates/liminal/src/routing/function/execute/actor.rs:219` | `mod tests::cooperative_scheduler` | `WasmScheduler::new(atom_table, modules, bifs)` |

### 1.6 Distinct symbol index (59 imported)

Counts are import-statement sites, from the brace-group expander (which correctly handles the multi-line `use` at `supervisor.rs:19-21`).

| symbol | import sites | symbol | import sites |
|---|---|---|---|
| `beamr::process::ExitReason` | 16 | `beamr::distribution::connection::AcceptHandle` | 2 |
| `beamr::atom::Atom` | 13 | `beamr::distribution::connection_events::ConnectionEvent` | 2 |
| `beamr::scheduler::Scheduler` | 11 | `beamr::distribution::pg::PgRegistry` | 2 |
| `beamr::atom::AtomTable` | 10 | `beamr::distribution::resolver::NodeResolver` | 2 |
| `beamr::term::Term` | 6 | `beamr::distribution::resolver::ResolveError` | 2 |
| `beamr::native::native_process::NativeContext` | 6 | `beamr::distribution::resolver::StaticResolver` | 2 |
| `beamr::distribution::connection::ConnectionManager` | 5 | `beamr::loader::Instruction` | 2 |
| `beamr::module::ModuleRegistry` | 5 | `beamr::loader::decode::Operand` | 2 |
| `beamr::native::ProcessContext` | 5 | `beamr::module::Module` | 2 |
| `beamr::native::native_process::NativeHandler` | 5 | `beamr::module::ModuleOrigin` | 2 |
| `beamr::native::native_process::NativeOutcome` | 5 | `beamr::module::ResolvedImport` | 2 |
| `beamr::scheduler::Interest` | 3 | `beamr::module::ResolvedImportTarget` | 2 |
| `beamr::scheduler::ReadinessToken` | 3 | `beamr::native::BifRegistryImpl` | 2 |
| `beamr::scheduler::SchedulerConfig` | 3 | `beamr::native::Capability` | 2 |
| `beamr::timer::TimerRef` | 3 | `beamr::native::NativeEntry` | 2 |
| `beamr::constant_pool::ConstantPool` | 2 | `beamr::native::native_process::NativeHandlerFactory` | 2 |
| `beamr::scheduler::WasmScheduler` | 2 | `beamr::term::boxed::Tuple` | 2 |

Singletons (1 import site each): `beamr::Actor`, `beamr::ActorContext`, `beamr::ActorError`, `beamr::CallFuture`, `beamr::spawn_actor`, `beamr::spawn_actor_cooperative`, `beamr::distribution::DistributionConfig`, `beamr::distribution::Resolver`, `beamr::distribution::connection_events::ConnectionGeneration`, `beamr::distribution::connection_events::SubscriberId`, `beamr::distribution::control::encode_pg_update_frame`, `beamr::distribution::control::encode_send_frame`, `beamr::distribution::pg::PgUpdate`, `beamr::distribution::pg::RemoteMember`, `beamr::distribution::resolver::ResolveFuture`, `beamr::distribution::resolver::Resolver`, `beamr::ets::copy_term_to_ets`, `beamr::process::heap::Heap`, `beamr::process::registry::ProcessHandle`, `beamr::scheduler::ExitEvent`, `beamr::scheduler::ExitEventSubscription`, `beamr::scheduler::SchedulerServices`, `beamr::term::binary_ref::BinaryRef`, `beamr::term::shared_binary::SharedBinary`, `beamr::term::shared_binary::write_proc_bin`.

⚠ **A brace-group trap the dispatcher should know about.** A naive `grep -o "beamr::[A-Za-z0-9_:]*"` **misses every member of a multi-line brace group**, capturing only the prefix. That is how `ExitEvent`, `ExitEventSubscription` and `SchedulerServices` — imported at `crates/liminal-server/src/server/connection/supervisor.rs:19-21 @ 93d8cc7` — were absent from the first symbol extraction. The 59-symbol index above comes from a brace-group expander that joins continuation lines to the `;`. **These three symbols are load-bearing for §3 and §4.**

Never imported, only fully qualified at the use site (4): `beamr::CoopSenderHandle` (doc-link, `execute/actor.rs:172`), `beamr::ets::OwnedTerm` (`subscription.rs:718`, in `fn frame_as_owned_binary`), `beamr::distribution::connection::DistConnection` (`cluster/sync.rs:269`, in `fn write_raw_blocking`), `beamr::distribution::DEFAULT_COOKIE` (doc-link, `config/types.rs:140`).

⚠ Two of the four are **rustdoc intra-doc links**, not code. They still fail the build under a `broken_intra_doc_links` deny, so they are migration sites — but they are invisible to a compile-error-driven migration until docs are built.

### 1.7 Full per-site census

**`crates/liminal-server/src/cluster/discovery.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 26 | `<module scope>` | `beamr::atom::AtomTable` |
| 27 | `<module scope>` | `beamr::distribution::connection::ConnectionManager` |
| 28 | `<module scope>` | `beamr::distribution::resolver::NodeResolver`, `beamr::distribution::resolver::ResolveError`, `beamr::distribution::resolver::ResolveFuture`, `beamr::distribution::resolver::Resolver` |
| 184 | `mod tests` | `beamr::distribution::resolver::NodeResolver`, `beamr::distribution::resolver::ResolveError` |

**`crates/liminal-server/src/cluster/membership.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 77 | `<module scope>` | `beamr::atom::Atom`, `beamr::atom::AtomTable` |
| 78 | `<module scope>` | `beamr::distribution::connection::AcceptHandle`, `beamr::distribution::connection::ConnectionManager` |
| 79 | `<module scope>` | `beamr::distribution::connection_events::ConnectionEvent`, `beamr::distribution::connection_events::SubscriberId` |
| 80 | `<module scope>` | `beamr::scheduler::Scheduler` |
| 646 | `mod tests` | `beamr::atom::AtomTable` |
| 647 | `mod tests` | `beamr::distribution::connection::AcceptHandle`, `beamr::distribution::connection::ConnectionManager` |
| 648 | `mod tests` | `beamr::distribution::connection_events::ConnectionEvent`, `beamr::distribution::connection_events::ConnectionGeneration` |
| 649 | `mod tests` | `beamr::distribution::resolver::StaticResolver` |

**`crates/liminal-server/src/cluster/sync.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 27 | `<module scope>` | `beamr::atom::Atom`, `beamr::atom::AtomTable` |
| 28 | `<module scope>` | `beamr::distribution::connection::ConnectionManager` |
| 29 | `<module scope>` | `beamr::distribution::control::encode_pg_update_frame`, `beamr::distribution::control::encode_send_frame` |
| 30 | `<module scope>` | `beamr::distribution::pg::PgRegistry`, `beamr::distribution::pg::PgUpdate`, `beamr::distribution::pg::RemoteMember` |
| 31 | `<module scope>` | `beamr::native::ProcessContext` |
| 32 | `<module scope>` | `beamr::term::Term` |
| 159 | `impl ClusterSync > fn send_to_member` | `beamr::atom::Atom::OK` |
| 269 | `fn write_raw_blocking` | `beamr::distribution::connection::DistConnection` |
| 300 | `mod tests` | `beamr::atom::AtomTable` |
| 301 | `mod tests` | `beamr::distribution::connection::ConnectionManager` |
| 302 | `mod tests` | `beamr::distribution::pg::PgRegistry` |
| 303 | `mod tests` | `beamr::distribution::resolver::StaticResolver` |

**`crates/liminal-server/src/config/types.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 140 | `struct RoutingRuleDef` | `beamr::distribution::DEFAULT_COOKIE` |

**`crates/liminal-server/src/server/connection/loopback/process.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 19 | `<module scope>` | `beamr::native::native_process::NativeContext` |
| 20 | `<module scope>` | `beamr::scheduler::Interest` |

**`crates/liminal-server/src/server/connection/pending_reply.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 44 | `<module scope>` | `beamr::timer::TimerRef` |

**`crates/liminal-server/src/server/connection/process.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 6 | `<module scope>` | `beamr::atom::Atom` |
| 7 | `<module scope>` | `beamr::native::native_process::NativeContext`, `beamr::native::native_process::NativeHandler`, `beamr::native::native_process::NativeOutcome` |
| 8 | `<module scope>` | `beamr::process::ExitReason` |
| 9 | `<module scope>` | `beamr::scheduler::Interest`, `beamr::scheduler::ReadinessToken` |
| 10 | `<module scope>` | `beamr::term::Term` |

**`crates/liminal-server/src/server/connection/process_terminal_tests.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 9 | `<module scope>` | `beamr::process::ExitReason` |

**`crates/liminal-server/src/server/connection/supervisor.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 15 | `<module scope>` | `beamr::atom::Atom`, `beamr::atom::AtomTable` |
| 16 | `<module scope>` | `beamr::module::ModuleRegistry` |
| 17 | `<module scope>` | `beamr::native::native_process::NativeHandlerFactory` |
| 18 | `<module scope>` | `beamr::process::ExitReason` |
| 19 | `<module scope>` | `beamr::scheduler::ExitEvent`, `beamr::scheduler::ExitEventSubscription`, `beamr::scheduler::ReadinessToken`, `beamr::scheduler::Scheduler`, `beamr::scheduler::SchedulerConfig`, `beamr::scheduler::SchedulerServices` |
| 22 | `<module scope>` | `beamr::timer::TimerRef` |

**`crates/liminal-server/src/server/connection/supervisor_tests.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 10 | `<module scope>` | `beamr::process::ExitReason` |
| 2693 | `fn wait_for_process_count` | `beamr::scheduler::Scheduler` |

**`crates/liminal-server/src/server/connection/wake.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 25 | `<module scope>` | `beamr::atom::Atom` |
| 26 | `<module scope>` | `beamr::scheduler::Scheduler` |

**`crates/liminal-server/src/server/connection/websocket/process.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 21 | `<module scope>` | `beamr::atom::Atom` |
| 22 | `<module scope>` | `beamr::native::native_process::NativeContext`, `beamr::native::native_process::NativeHandler`, `beamr::native::native_process::NativeOutcome` |
| 23 | `<module scope>` | `beamr::process::ExitReason` |
| 24 | `<module scope>` | `beamr::scheduler::Interest`, `beamr::scheduler::ReadinessToken` |
| 25 | `<module scope>` | `beamr::term::Term` |
| 26 | `<module scope>` | `beamr::timer::TimerRef` |

**`crates/liminal-server/src/server/connection/websocket/supervisor.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 19 | `<module scope>` | `beamr::native::native_process::NativeHandlerFactory` |

**`crates/liminal-server/tests/cluster_two_node.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 472 | `fn node_b_remote_members` | `beamr::distribution::pg::RemoteMember` |

**`crates/liminal-server/tests/server_conversation_supervisor.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 14 | `<module scope>` | `beamr::process::ExitReason` |

**`crates/liminal/src/channel/actor/beam.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 17 | `<module scope>` | `beamr::atom::Atom` |
| 18 | `<module scope>` | `beamr::constant_pool::ConstantPool` |
| 19 | `<module scope>` | `beamr::loader::Instruction` |
| 20 | `<module scope>` | `beamr::loader::decode::Operand` |
| 21 | `<module scope>` | `beamr::module::Module`, `beamr::module::ModuleOrigin`, `beamr::module::ResolvedImport`, `beamr::module::ResolvedImportTarget` |
| 22 | `<module scope>` | `beamr::native::Capability`, `beamr::native::NativeEntry`, `beamr::native::ProcessContext` |
| 23 | `<module scope>` | `beamr::term::Term` |
| 24 | `<module scope>` | `beamr::term::boxed::Tuple` |

**`crates/liminal/src/channel/actor/mod.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 29 | `mod wait` | `beamr::atom::Atom` |
| 30 | `mod wait` | `beamr::native::ProcessContext` |
| 31 | `mod wait` | `beamr::scheduler::Scheduler` |

**`crates/liminal/src/channel/actor/wait.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 13 | `<module scope>` | `beamr::scheduler::Scheduler` |

**`crates/liminal/src/channel/subscription.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 24 | `<module scope>` | `beamr::native::native_process::NativeContext`, `beamr::native::native_process::NativeHandler`, `beamr::native::native_process::NativeOutcome` |
| 25 | `<module scope>` | `beamr::process::ExitReason` |
| 26 | `<module scope>` | `beamr::scheduler::Scheduler` |
| 27 | `<module scope>` | `beamr::term::binary_ref::BinaryRef` |
| 668 | `impl Drop for SubscriptionInner > fn drop` | `beamr::scheduler::WasmScheduler` |
| 693 | `mod cooperative_smoke` | `beamr::atom::AtomTable` |
| 694 | `mod cooperative_smoke` | `beamr::ets::copy_term_to_ets` |
| 695 | `mod cooperative_smoke` | `beamr::module::ModuleRegistry` |
| 696 | `mod cooperative_smoke` | `beamr::native::BifRegistryImpl` |
| 697 | `mod cooperative_smoke` | `beamr::process::heap::Heap` |
| 698 | `mod cooperative_smoke` | `beamr::scheduler::WasmScheduler` |
| 699 | `mod cooperative_smoke` | `beamr::term::shared_binary::SharedBinary`, `beamr::term::shared_binary::write_proc_bin` |
| 718 | `mod cooperative_smoke` | `beamr::ets::OwnedTerm` |
| 762 | `mod cooperative_smoke > fn real_subscriber_process_delivers_a_published_envelope_cooperatively` | `beamr::native::native_process::NativeHandler` |

**`crates/liminal/src/channel/supervisor.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 19 | `<module scope>` | `beamr::atom::Atom`, `beamr::atom::AtomTable` |
| 20 | `<module scope>` | `beamr::distribution::DistributionConfig`, `beamr::distribution::Resolver` |
| 21 | `<module scope>` | `beamr::module::ModuleRegistry` |
| 22 | `<module scope>` | `beamr::scheduler::Scheduler`, `beamr::scheduler::SchedulerConfig` |

**`crates/liminal/src/channel/tests.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 14 | `<module scope>` | `beamr::process::ExitReason` |

**`crates/liminal/src/channel/types.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 599 | `impl ChannelHandle` | `beamr::scheduler::Scheduler` |

**`crates/liminal/src/conversation/actor.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 6 | `<module scope>` | `beamr::atom::Atom`, `beamr::atom::AtomTable` |
| 7 | `<module scope>` | `beamr::module::ModuleRegistry` |
| 8 | `<module scope>` | `beamr::scheduler::Scheduler`, `beamr::scheduler::SchedulerConfig` |
| 114 | `impl ConversationSupervisor > fn spawn_with_participant` | `beamr::process::ExitReason::Normal` |
| 329 | `impl SupervisorInner > fn spawn_participant` | `beamr::native::native_process::NativeHandler` |
| 414 | `impl SupervisorInner > fn rollback_actor_attempt` | `beamr::process::ExitReason::Normal` |
| 417 | `impl SupervisorInner > fn rollback_actor_attempt` | `beamr::process::ExitReason::Normal` |
| 459 | `impl SupervisorInner > fn spawn_watcher` | `beamr::native::native_process::NativeHandler` |
| 473 | `impl SupervisorInner > fn spawn_watcher` | `beamr::process::ExitReason::Normal` |

**`crates/liminal/src/conversation/actor/beam.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 6 | `<module scope>` | `beamr::atom::Atom` |
| 7 | `<module scope>` | `beamr::constant_pool::ConstantPool` |
| 8 | `<module scope>` | `beamr::loader::Instruction` |
| 9 | `<module scope>` | `beamr::loader::decode::Operand` |
| 10 | `<module scope>` | `beamr::module::Module`, `beamr::module::ModuleOrigin`, `beamr::module::ResolvedImport`, `beamr::module::ResolvedImportTarget` |
| 11 | `<module scope>` | `beamr::native::Capability`, `beamr::native::NativeEntry`, `beamr::native::ProcessContext` |
| 12 | `<module scope>` | `beamr::process::ExitReason` |
| 13 | `<module scope>` | `beamr::term::Term` |
| 14 | `<module scope>` | `beamr::term::boxed::Tuple` |

**`crates/liminal/src/conversation/actor/core.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 13 | `<module scope>` | `beamr::atom::Atom` |
| 14 | `<module scope>` | `beamr::native::ProcessContext` |
| 15 | `<module scope>` | `beamr::process::ExitReason` |
| 16 | `<module scope>` | `beamr::term::Term` |

**`crates/liminal/src/conversation/actor/tests.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 3 | `<module scope>` | `beamr::process::ExitReason` |
| 84 | `fn wait_until_process_gone` | `beamr::scheduler::Scheduler` |

**`crates/liminal/src/conversation/actor/watcher.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 23 | `<module scope>` | `beamr::native::native_process::NativeContext`, `beamr::native::native_process::NativeHandler`, `beamr::native::native_process::NativeOutcome` |
| 24 | `<module scope>` | `beamr::process::ExitReason` |

**`crates/liminal/src/conversation/participant.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 20 | `<module scope>` | `beamr::atom::Atom` |
| 21 | `<module scope>` | `beamr::native::native_process::NativeContext`, `beamr::native::native_process::NativeHandler`, `beamr::native::native_process::NativeOutcome` |
| 22 | `<module scope>` | `beamr::scheduler::Scheduler` |
| 159 | `impl ParticipantRuntime > fn reap_orphans` | `beamr::process::ExitReason::Normal` |

**`crates/liminal/src/conversation/patterns.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 80 | `mod tests` | `beamr::process::ExitReason` |

**`crates/liminal/src/conversation/types.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 4 | `<module scope>` | `beamr::process::ExitReason` |
| 5 | `<module scope>` | `beamr::process::registry::ProcessHandle` |

**`crates/liminal/src/routing/dispatch/tests.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 4 | `<module scope>` | `beamr::process::ExitReason` |
| 58 | `fn has_link_to` | `beamr::scheduler::Scheduler` |
| 66 | `fn wait_for_link` | `beamr::scheduler::Scheduler` |

**`crates/liminal/src/routing/function/execute.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 12 | `<module scope>` | `beamr::scheduler::Scheduler` |

**`crates/liminal/src/routing/function/execute/actor.rs`**

| line | enclosing item | beamr symbol(s) consumed |
|---|---|---|
| 3 | `<module scope>` | `beamr::process::ExitReason` |
| 4 | `<module scope>` | `beamr::scheduler::Scheduler` |
| 5 | `<module scope>` | `beamr::Actor`, `beamr::ActorContext`, `beamr::spawn_actor` |
| 52 | `impl BeamrInvocation > fn execute` | `beamr::ActorError::Spawn` |
| 53 | `impl BeamrInvocation > fn execute` | `beamr::ActorError::Spawn` |
| 55 | `impl BeamrInvocation > fn execute` | `beamr::ActorError::Timeout` |
| 171 | `fn lock_or_recover<T` | `beamr::scheduler::WasmScheduler` |
| 172 | `fn lock_or_recover<T` | `beamr::CoopSenderHandle::call_async` |
| 181 | `fn lock_or_recover<T` | `beamr::CallFuture` |
| 199 | `mod cooperative_smoke` | `beamr::atom::AtomTable` |
| 200 | `mod cooperative_smoke` | `beamr::module::ModuleRegistry` |
| 201 | `mod cooperative_smoke` | `beamr::native::BifRegistryImpl` |
| 202 | `mod cooperative_smoke` | `beamr::scheduler::WasmScheduler` |
| 203 | `mod cooperative_smoke` | `beamr::ActorError`, `beamr::CallFuture`, `beamr::spawn_actor_cooperative` |

---

## 2. SIGNATURE DELTA TABLE

Tag revs: `v0.16.3` = `1a77d5c480a7d75e87dff13756431d2fac744592`; `v0.18.2` = `74e532fe6df90397be3ab6c98664f9cb0f881460`. All beamr paths below are under `crates/beamr/src/`.

**Method note.** Comparison was driven by **blob identity** (`git rev-parse <tag>:<path>`) before reading any line. A file with the same blob hash at both tags has provably identical declarations *and* identical line numbers, so a single line number is cited as holding at both tags. That is a tree comparison, not a path comparison — it satisfies "a path is not a tree".

### 2.0 Headline

**Of the 63 distinct symbols liminal consumes, 62 are UNCHANGED and 1 family is SIGNATURE-CHANGED.** The entire compile-forced break surface for this workspace is **the `Scheduler` constructors**, which gained a required `NativeBifs` argument. Nothing liminal hand-builds changed at all.

### 2.1 SIGNATURE-CHANGED — the only break that reaches liminal

| symbol | v0.16.3 | v0.18.2 | liminal sites |
|---|---|---|---|
| `Scheduler::new` | `scheduler/mod.rs:1058` — `(config, module_registry) -> Result<Self, String>` | `scheduler/mod.rs:1066` — `(config, module_registry, natives: NativeBifs) -> Result<Self, String>` | 2 |
| `Scheduler::with_services` | `scheduler/mod.rs:1135` — `(config, services, module_registry) -> Result<Self, String>` | `scheduler/mod.rs:1143` — `(config, services, module_registry, natives: NativeBifs) -> Result<Self, String>` | 1 |
| `Scheduler::new_replay` | `scheduler/mod.rs:1180` | `scheduler/mod.rs:1193` — `+ natives: NativeBifs` | 0 |
| `Scheduler::new_replay_with_registry` | `scheduler/mod.rs:1195` | `scheduler/mod.rs:1218` — `+ natives: NativeBifs` | 0 |

**`NativeBifs` is new at v0.18.2** — `scheduler/services.rs:56`, re-exported `scheduler/mod.rs:156`. `git grep -c 'NativeBifs' v0.16.3 -- crates/` returns **zero occurrences anywhere in the v0.16.3 tree** (positive control: the same `pub struct X` predicate finds `SchedulerConfig` at `scheduler/mod.rs:281 @ v0.16.3`). One private field `registry: Arc<BifRegistryImpl>`; constructors `NativeBifs::none()` (`services.rs:71`) and `const NativeBifs::registry(Arc<BifRegistryImpl>)` (`services.rs:86`).

⚠ **This — not `Module` — is where "empty-bundle unrepresentability" actually landed.** `NativeBifs` has **no `Default` and no `From`, deliberately**; its own rustdoc says the intent is that *"this scheduler resolves no native BIFs"* is *"a value the call site had to write down rather than a default it inherited."* So the migration cannot be completed by adding `..Default::default()`; every call site must state its answer. **This makes all three liminal sites SEMANTIC, not mechanical** — see §5.

`SchedulerConfig` itself is **UNCHANGED**: `scheduler/mod.rs:281 @ v0.16.3` → `:285 @ v0.18.2`, same 11 public fields. All five fields liminal sets survive at both tags — `thread_count` (`:282`/`:286`), `nif_private_data` (`:313`/`:317`), `node_name` (`:302`/`:306`), `creation` (`:303`/`:307`), `distribution` (`:304`/`:308`) — and liminal spreads `..SchedulerConfig::default()`, so added fields could not have broken it anyway.

### 2.2 MOVED — same public path, no source edit required

| symbol | v0.16.3 | v0.18.2 | note |
|---|---|---|---|
| `process::ExitReason` | `process/types.rs:229` | `process/types.rs:257` | +28-line shift from an inserted `impl RawStackEntry`. **All 6 variants identical**: `Normal`, `Kill`, `Killed`, `Error`, `NoConnection`, `NoProc`. Re-exported wholesale by `pub use types::*;` at `process/mod.rs:13`, both tags. |
| `connection::ConnectionManager` | `distribution/connection.rs:684` | `distribution/connection/mod.rs:307` | `connection.rs` (2964 lines) split into a directory module; `distribution/mod.rs` is **byte-identical**, so `beamr::distribution::connection::…` still resolves. |
| `connection::AcceptHandle` | `distribution/connection.rs:419` | `distribution/connection/link.rs:382`, re-exported `connection/mod.rs:40` | same split |
| `connection::DistConnection` | `distribution/connection.rs:123` | `distribution/connection/link.rs:86`, re-exported `connection/mod.rs:40` | same split |

The connection module's public-fn set across the split: **0 removed, 2 added** (`inbound_accepts_refused`, `inbound_residency_bytes`).

### 2.3 UNCHANGED — everything else liminal consumes

Byte-identical defining blobs unless noted; line numbers hold at both tags.

| module | symbols | site(s) |
|---|---|---|
| `atom` | `Atom`, `Atom::OK`, `AtomTable` | `atom/table.rs:11`, `:14`, `:186`; re-export `atom/mod.rs:4` |
| `constant_pool` | `ConstantPool` | `constant_pool/mod.rs:42` |
| `loader` | `Instruction` (**75 variants**), `decode::Operand` (12 variants) | `loader/decode/instruction.rs:4`; `loader/decode/compact.rs:11` |
| `module` | `Module`, `ModuleOrigin`, `ResolvedImport`, `ResolvedImportTarget`, `ModuleRegistry` | `module.rs:108`, `:82`, `:69`, `:25`, `:372` — whole file blob `d7d9345f…`, **identical** |
| `native` | `Capability` (6 variants), `NativeEntry` (3 fields), `ProcessContext` (121 public methods), `BifRegistryImpl` | `native/capability.rs:10`; `native/mod.rs:134`; `native/context/mod.rs:337`; `native/mod.rs:330` |
| `native::native_process` | `NativeContext` (15 public methods), `NativeHandler`, `NativeOutcome`, `NativeHandlerFactory` | `native/native_process.rs:103`, `:48`, `:56`, `:40` |
| `process` | `registry::ProcessHandle`, `heap::Heap` | `process/registry.rs:22`; `process/heap.rs:250` |
| `scheduler` | `Scheduler` (type), `SchedulerConfig`, `WasmScheduler`, `Interest`, `ReadinessToken`, `SchedulerServices`, `ExitEvent`, `ExitEventSubscription` | `scheduler/mod.rs:986`→`:990`; `scheduler/wasm.rs:119`; `scheduler/readiness/types.rs:5`,`:48`; `scheduler/services.rs:326`; `scheduler/exit_events.rs:24`, `:67` |
| `term` | `Term`, `boxed::Tuple`, `binary_ref::BinaryRef`, `shared_binary::{SharedBinary, write_proc_bin}` | `term/mod.rs:63`; `term/boxed/accessors.rs:10`; `term/binary_ref.rs:11`; `term/shared_binary.rs:35`, `:93` |
| `timer` | `TimerRef` | `timer.rs:19` |
| `ets` | `OwnedTerm`, `copy_term_to_ets` | `ets/copy.rs:17`, `:86` |
| crate root | `Actor`, `ActorContext`, `ActorError`, `CallFuture`, `CoopSenderHandle`, `spawn_actor`, `spawn_actor_cooperative` | `native/actor.rs:125`, `:150`, `:313`, `:453`; `native/actor_cooperative.rs:207`, `:103`, `:342` — `actor.rs` blob `11aad05a…` **identical** |
| `distribution` | `DistributionConfig`, `Resolver`, `DEFAULT_COOKIE`, `connection_events::{ConnectionEvent, ConnectionGeneration, SubscriberId}`, `resolver::{NodeResolver, ResolveError, ResolveFuture, Resolver, StaticResolver}`, `control::{encode_send_frame, encode_pg_update_frame}`, `pg::{RemoteMember, PgUpdate, PgRegistry}` | `distribution/mod.rs:59`, `:33`, `:36`; `connection_events.rs:260`,`:210`,`:325`; `resolver.rs:46`,`:25`,`:17`,`:21`,`:53`; `control.rs:185`,`:230`; `pg.rs:23`,`:34`,`:81` — all defining blobs **identical** |

**Trait conformance — all 7 liminal impls (§1.4) are safe.**
- `NativeHandler` — `native/native_process.rs:48`, method set unchanged; liminal's 5 impls need no edit.
- `Actor` — `native/actor.rs:125`; assoc types `Call`/`Reply`/`Cast` and `handle_call`/`handle_cast` unchanged.
- `NodeResolver` — `resolver.rs:46-49`, file byte-identical: exactly one required method `fn resolve<'a>(&'a self, name: &'a str) -> ResolveFuture<'a>`, no provided methods, no new supertraits, not sealed. Liminal's `ClusterResolver` impl needs no edit.
- `ActorError` has **exactly 2 variants at both tags** (`Spawn`, `Timeout`, `native/actor.rs:311-318`) and is **not** `#[non_exhaustive]`, so liminal's matches at `execute/actor.rs:52,55` stay exhaustive.
- `ConnectionEvent` (`connection_events.rs:258-265`) has 2 variants at both tags and **was already `#[non_exhaustive]` at v0.16.3** — no newly-required wildcard.

### 2.4 The four named claims, adjudicated at the bytes

**(a) "the runtime entry ladder reportedly became `run_with_native_services`" — HALF RIGHT, and it does not touch liminal.**
The ladder did collapse, but `run_with_native_services` is **not new**: it exists at `interpreter/mod.rs:234 @ v0.16.3` with a **byte-identical signature**, and sits at `interpreter/mod.rs:210 @ v0.18.2`. What changed is that its three siblings were **REMOVED**: `run` (`:204 @ v0.16.3`), `run_with_registry` (`:210`), `run_with_timer_services` (`:220`). Positive control for the removals: the predicate `git grep -nE 'pub fn run\(|pub fn run_with_registry|pub fn run_with_timer_services'` still returns `ets/match_spec.rs:161 pub fn run(` at v0.18.2 — the instrument is live at that tag and simply finds nothing in `interpreter/`.
⚠ **This is a non-event for liminal.** `grep -rn --include="*.rs" -E "beamr::interpreter|interpreter::run|run_with_native_services|run_with_registry|run_with_timer_services|NativeServices" crates/` → **exit 1, no output** (positive control, same predicate shape: `beamr::scheduler` returns 23 lines). Liminal never touches the interpreter entry ladder; it reaches beamr only through `Scheduler`.

**(b) "an empty-bundle unrepresentability change" — REAL, but NOT where the prompt places it.**
It is **not** on `Module`/`function_table`. `module.rs` is blob `d7d9345f3ee7cb0ab6df91937e0634292bb46561` at **both** tags — I re-derived both hashes myself — and `Module` has **14 public fields at both tags** (counted by `awk`, not by hand), with `function_table: Vec<(usize, Atom, u8)>` at `module.rs:122` still a **plain `Vec`**: not `NonEmpty`, not private, no constructor requirement. An empty bundle remains representable by struct literal. The unrepresentability pattern landed instead on **`NativeBifs`** (§2.1) — the `Scheduler` constructor axis.

**(c) "`spawn_link_dirty` was removed in 0.17.0" — CONFIRMED, and liminal is unexposed. Re-measured, not inherited.**
Present at `scheduler/spawning.rs:380 @ v0.16.3` (`impl Scheduler`); **absent at v0.17.0 and every later tag**. The v0.16.3 body was a pure one-line delegation to `spawn_link`, and its own doc already announced the removal. **Replacement: `Scheduler::spawn_link`**, `spawning.rs:295` at both tags.
Liminal exposure: **none.** `grep -rn --include="*.rs" "spawn_link_dirty" crates/` → exit 1 (positive control, same predicate: `spawn_native_trap_exit` returns `subscription.rs:583,591`).
⚠ **And the adjacent field risk I raised in §5.4 resolves CLEAN.** `NativeEntry.dirty_kind` — which liminal writes at `channel/actor/beam.rs:151` and `conversation/actor/beam.rs:172` — **still exists at v0.18.2**: `native/mod.rs:138`, `pub dirty_kind: Option<DirtySchedulerKind>`, unchanged. `NativeEntry` has the same 3 public fields at both tags. The dirty *config* fields (`dirty_cpu_threads`, `dirty_io_threads`, `dirty_queue_depth`) also survive on `SchedulerConfig`. **Only the `spawn_link_dirty` alias was removed; the dirty-native mechanism stayed.** So the prior ruling holds, and holds for the right reason.

**(d) the `readiness` feature — STILL EXISTS at v0.18.2, unchanged, and still in `default`.**
`crates/beamr/Cargo.toml` differs between the two tags by **exactly one line** — the `version` string (blobs `5d2980fc…` vs `55e18e14…`, `diff` returns only `3c3`). The `[features]` table (`Cargo.toml:68-86`) is **identical**:
- `readiness = ["threads", "dep:mio"]` (`:76`) — **in `default`** at both tags; 64 gate sites across 14 files at both tags. Gates the mio FD-readiness service: `ReadinessChoice` (`scheduler/services.rs:127`) and the `owned_readiness`/`shared_readiness`/`disable_readiness` builders (`:504`/`:511`/`:518`) — i.e. exactly liminal's `SchedulerServices::from_config().owned_readiness()` at `supervisor.rs:1136`, plus `Interest`/`ReadinessToken`.
- `cooperative = ["std", "dep:crossbeam-queue"]` (`:74`) — not in `default` at either tag; 18 gate sites / 4 files at both. Gates `CallFuture`/`CoopSenderHandle`/`spawn_actor_cooperative` re-exports.
- `json = ["dep:base64", "dep:serde_json"]` (`:81`) — not in `default` at either tag; 1 gate site at both.
**All three of liminal's features are safe byte-for-byte.**

⚠ **Naming trap for the dispatcher:** liminal-server's `ReadinessState` / `SharedReadinessState` / `readiness_check` (`crates/liminal-server/src/health/checks.rs:350` etc. @ 93d8cc7) are **liminal's own health types and have nothing to do with beamr's `readiness` feature**. A grep for `readiness` conflates them. Beamr's readiness surface in liminal is only `ReadinessToken`, `Interest`, `Scheduler::readiness_deregister`, and `owned_readiness`.

### 2.5 Two beamr-side changes liminal is NOT exposed to (checked, not assumed)

- **`EtsRegistry::new` gained an `Arc<AtomTable>` parameter and lost its `Default` impl** at v0.18.2. Liminal exposure: **none** — `grep -rn --include="*.rs" "EtsRegistry" crates/` → exit 1 (positive control, same predicate: `copy_term_to_ets|OwnedTerm` returns `subscription.rs:694,718,728`). Liminal uses only `ets::copy_term_to_ets` and `ets::OwnedTerm`, both in `ets/copy.rs`, byte-identical.
- **`WasmScheduler::new` is UNCHANGED** — `scheduler/wasm.rs` is blob `45979cdc1ffd9898ff24ef555f28ca9de2acc404` at **both** tags; `pub fn new(atom_table: Arc<AtomTable>, module_registry: Arc<ModuleRegistry>, bif_registry: Arc<BifRegistryImpl>) -> Self` at `wasm.rs:166-170`. Liminal's two call sites (`subscription.rs:712`, `execute/actor.rs:219`) pass exactly those three and need no edit.

### 2.6 ⚠ A correctness advisory that bears on WHETHER to migrate, not just how

`git show v0.18.2:CHANGELOG.md:3-75` reports that **JIT-compiled code silently dropped every message sent to another process** in all versions **0.4.0 through 0.18.1**, fixed only in **0.18.2**. `CHANGELOG.md:12-14` states the **0.16.x line is unpatched and a backport was considered and ruled against**. `jit` is in beamr's `default` feature set (`Cargo.toml:69`).

**This is an argument for the migration, not a footnote to it.** The dispatcher should determine whether liminal's dependency enables `jit` (it comes in via `default`, and liminal's manifests do not appear to set `default-features = false` — *unverified, flagged in §6*) and whether liminal's `jit_threshold` config leaves it reachable. A separate **still-open** accumulator-rooting class is recorded at `CHANGELOG.md:131-145` as unfixed in every released version through 0.18.1; **its status at 0.18.2 was not established** — see §6.

---

## 3. NEW SURFACE — PARK/WAKE AND SUSPEND (S5 liveness)

Tag revs as above. **The whole family is gated by exactly one feature, `threads`** (`lib.rs:25-26` for `hook`; `scheduler/mod.rs:120-121` for `exit_events`; `:175-176` for `suspension`). `Scheduler` itself is threads-gated. Liminal enables `threads` transitively via `readiness` (`readiness = ["threads", …]`) and via `default`. **The `cooperative`/wasm path has no hook seam, no exit events, no exit watches, and no resume/wake API** — relevant because liminal's `WasmScheduler` test paths cannot use any of this.

### 3.1 The background claim, adjudicated

> "0.17.0 introduced a suspend surface and `watch_exit`."

- **`watch_exit` — TRUE**, and precisely dated. Absent at v0.16.3 (`git grep -n "watch_exit" v0.16.3 -- crates/beamr/src/` → **true exit 1**, unpiped; positive control through the same predicate/tag/pathspec: `subscribe_exit_events` → `scheduler/execution.rs:213 @ v0.16.3`, true exit 0). Arrives at **`scheduler/execution.rs:251 @ v0.17.0`** and is at the **same line** at v0.18.2.
- **"suspend surface" — FALSE.** `scheduler/suspension.rs` is blob `f4496260…` and `hook.rs` is blob `f6c169c1…` at **all three** tags. The suspend/hook surface did not change by one byte between v0.16.3 and v0.18.2.
- `ExitEvent` / `ExitEventSubscription` / `subscribe_exit_events` **predate v0.16.3** (CHANGELOG dates them to 0.15.3) — which is why liminal already consumes them today (§4.2).
- **Nothing in this family changed between v0.17.0 and v0.18.2.** The 0.18.x work is JIT-side.
- ⚠ Documentation hole: **`v0.17.1` has no CHANGELOG entry** — the string `0.17.1` appears nowhere in the file, though the tag exists and the file asserts of itself that "a version is patched only when it appears in this file with its own entry saying so" (`CHANGELOG.md:15-16 @ v0.18.2`).

### 3.2 Types

**Exit family** — `scheduler/exit_events.rs @ v0.18.2`, re-exported `scheduler/mod.rs:126-130`:

| item | line | detail |
|---|---|---|
| `EXIT_EVENT_CAPACITY: usize = 1_024` | `:20` | subscriber queue bound |
| `enum ExitEvent` | `:24` | `Exited { pid: u64, reason: ExitReason }` (`:27`); `Lagged` (`:39`) |
| `enum ExitEventRecvError` | `:44` | `Disconnected` (`:45`); `Timeout` (`:47`) |
| `struct ExitEventSubscription` | `:67` | broadcast handle |
| `enum ExitWatchState` | `:117` | `Live(ExitWatch)` (`:121`); `AlreadyExited(ExitReason)` (`:126`); `NoRecord` (`:131`) |
| `struct ExitWatch` | `:151` | one-shot per-pid handle |

**Park-state types** (mostly unreachable by an embedder — see §3.5):

| item | site @ v0.18.2 | variants/fields |
|---|---|---|
| `enum ProcessStatus` | `process/types.rs:347` | `New`, `Running`, `Yielded`, `Waiting`, `Suspended`, `Exited(ExitReason)` |
| `enum SuspensionKind` | `process/mod.rs:64` | `HostAwait`, `DirtyCall`, `Hook` |
| `struct SuspensionRecord` | `process/mod.rs:105` | `call_id`, `kind`, `position`, `wake_on_message`, `continuation` |
| `struct SuspendRequest` | `native/context/mod.rs:274` | `timeout_ms`, `wake_on_message`, `call_id` |
| `enum ProcessInfoStatus` | `native/process_info_bifs.rs:98` | `Running`, `Waiting`, `Suspended` |
| `struct ProcessHandle` | `process/registry.rs:22` | **one private field `pid: u64`** — load-bearing |
| `struct HookEvent` | `hook.rs:14` | `pid`, `module`, `function`, `arity`, `reductions_consumed` |
| `enum HookDecision` | `hook.rs:29` | `Continue` (`:32`); `Suspend` (`:34`) |

### 3.3 Functions — every one is synchronous; there is no `async fn` in this family

`impl Scheduler`, `scheduler/execution.rs @ v0.18.2`:

```rust
pub fn wake_notifier(&self, pid: u64) -> impl Fn() + Send + Sync + 'static   // :23
pub fn wake_process(&self, pid: u64)                                         // :29
pub fn resume_process(&self, pid: u64) -> bool                               // :51
pub fn run_until_exit(&self, pid: u64) -> (ExitReason, OwnedTerm)            // :142  ⚠ POLLS
pub fn peek_exit_reason(&self, pid: u64) -> Option<ExitReason>               // :177
pub fn take_exit_outcome(&self, pid: u64) -> Option<(ExitReason, OwnedTerm)> // :194
pub fn subscribe_exit_events(&self) -> Option<ExitEventSubscription>         // :213
pub fn watch_exit(&self, pid: u64) -> ExitWatchState                         // :251  ← NEW in 0.17.0
pub fn wake_with_result(&self, pid: u64, result: Term) -> bool               // :303
pub fn wake_with_result_for(&self, pid, call_id: u64, result: Term) -> bool  // :325
pub fn terminate_process(&self, pid: u64, reason: ExitReason)                // :352
```

`impl ExitEventSubscription` (`exit_events.rs:72`): `recv()` (`:74`, blocking), `recv_timeout(Duration)` (`:84`).
`impl ExitWatch` (`exit_events.rs:167`): `pid()` (`:170`), `recv()` (`:175`, blocking), `recv_timeout()` (`:182`); `impl Drop` (`:192`) deregisters.
`impl Hook` (`hook.rs:44`): `new`, `register<F>`, `unregister`, `is_registered`, `invoke`.
`impl ProcessContext`: `request_suspend(timeout_ms)` (`native/context/mod.rs:1596`, `wake_on_message = true`), `request_await_suspend(timeout_ms)` (`:1609`, `wake_on_message = false`), `cancel_requested_suspend` (`:1638`), `take_suspend` (`:1659`).

⚠ **`Scheduler::suspend_process` and `park_process` DO NOT EXIST.** Positive control for the absence: `resume_process` returns hits in 5 files through the same predicate. Suspension is requestable only from **inside a hook callback** by returning `HookDecision::Suspend`, or from **inside a native** via `ProcessContext::request_*_suspend`.

⚠ **`Scheduler::run_until_exit` (`execution.rs:142-157`) is itself a 10 ms poll loop** (`wait_timeout(guard, Duration::from_millis(10))` at `:154-155`). **Under liminal's NO-POLLING law it must never be adopted.** `peek_exit_reason`/`take_exit_outcome` are non-blocking samples — sound as post-notification reads, unsound as detectors.

### 3.4 How an embedder is TOLD

**(a) Process exit — genuinely TOLD. Two push mechanisms, both blocking channels, no polling.**

1. **Broadcast** — `subscribe_exit_events()` → `Option<ExitEventSubscription>` → `.recv()` blocks on a `crossbeam_channel::Receiver<ExitEvent>`. Bounded at 1,024; overflow surfaces as a typed `ExitEvent::Lagged`, never a silent drop. ⚠ **Exclusive: one subscription per scheduler lifetime, enforced by `OnceLock`** — hence the `Option` return. This is exactly what liminal-server relies on today.
2. **Per-pid one-shot** — `watch_exit(pid)` → `ExitWatchState`. **Unlimited watchers**, `bounded(1)` channel each, dropping the handle deregisters it. The typed registration answer is the valuable part: `AlreadyExited(reason)` and `NoRecord` mean **the caller can never block on an answer that cannot arrive** (`exit_events.rs:127-131`).

Ordering is guaranteed (`exit_tombstones.rs:273-281`, `insert_inner`): outcome installed → `events.publish(ExitEvent::Exited)` → `watches.fire(pid, reason)`. So a watcher woken by a fire can immediately `take_exit_outcome`. Fire is `try_send` into one-slot channels — non-blocking, no user code on the exit path.

**(b) Park state — NOT TOLD. There is no park event, no park subscription, no park callback, and no park-state read.**

Positive control for this absence: the predicate `git grep -nE '(enum|struct) [A-Za-z]*(Park|Suspend|Suspension|Wait|Wake)[A-Za-z]*(Event|Notification|Subscription|Watch|Observer|Listener)'` returns nothing at v0.18.2, while `enum ExitEvent` returns 2 hits in `exit_events.rs` through the same instrument. `fn subscribe*` across the whole crate yields exactly three surfaces — exit events, distribution connection events, and their internals — **nothing for park**.

The hook is **not** a park notification: `HookEvent` (`hook.rs:14`) carries `pid, module, function, arity, reductions_consumed` and **no status, no park reason**, and it fires at a reduction boundary of a *running* slice. It is the embedder **causing** a suspend, not being **told** of a park.

### 3.5 ⛔ PARK-KIND DISCRIMINATION — THE VERDICT

**beamr at v0.18.2 CANNOT discriminate RECEIVE-PARK from SUSPEND-PARK. This is a plain finding, verified at the bytes, not a gap in the survey. It is two failures stacked.**

**Failure 1 — the embedder cannot observe park state at all.**
There is no `Scheduler::process_status(pid)`, no `is_parked`, no status snapshot. The only process-directed read is `Scheduler::process_table()` (`scheduler/mod.rs:1886`) → `ProcessTable::get(pid)` → `ProcessHandle`, whose **complete definition at `process/registry.rs:22-24 @ v0.18.2`** is:

```rust
pub struct ProcessHandle {
    pid: u64,          // :23 — the only field, and it is PRIVATE
}
```

with a single accessor `pub const fn pid(self) -> u64` (`:29`). **That is liveness and nothing else** — which is precisely all liminal's F7 loop gets from it today (§4.1). `Process::status()` (`process/mod.rs:342`) and `Process::suspension()` (`:753`) are `pub`, but `Process` is deliberately `!Send`/`!Sync` and lives inside a mutex-protected slot the scheduler owns; an embedder holding a `Scheduler` can never obtain a `&Process`.

**Failure 2 — even internally, the two kinds collapse onto one status.**

| path | site @ v0.18.2 (enclosing fn) | `ProcessStatus` written | `SuspensionRecord`? |
|---|---|---|---|
| **RECEIVE-PARK** (BEAM `wait`/`wait_timeout`) | `interpreter/opcodes/messaging.rs:339` in **`fn transition_to_waiting`** (decl `:332`) | **`Waiting`** | **NONE** |
| **SUSPEND-PARK, host-await** (`request_suspend` / `request_await_suspend`, incl. `sleep_forever`) | `interpreter/opcodes/trampoline.rs:295` in **`fn handle_suspend`** | **`Waiting`** ← *same variant* | `SuspensionRecord{kind: HostAwait}` set at `:280-286` |
| **SUSPEND-PARK, hook** (`HookDecision::Suspend`) | `scheduler/execution/core.rs:713` in **`fn execute_slice_with_budget`** | `Suspended` | `SuspensionRecord{kind: Hook}` |
| dirty call in flight | `scheduler/execution/core.rs:853` | `Suspended` | `kind: DirtyCall` |

I read both decisive sites myself. `messaging.rs:338-340` is `process.transition_to(ProcessStatus::Waiting)` with **no `set_suspension` anywhere in the receive path**; `trampoline.rs:280` sets `SuspensionRecord{ kind: SuspensionKind::HostAwait, … }` and then `:294-296` performs the **identical** `process.transition_to(ProcessStatus::Waiting)`.

**`ProcessStatus::Waiting` is the single variant that both a plain receive-park and the embedder-facing host-await suspend-park write.** This propagates to the only rendered view: `status_from_process` (`scheduler/mod.rs:2319`) maps `Waiting → ProcessInfoStatus::Waiting`, and `status_atom` (`process_info_bifs.rs:287`) renders the atom `waiting`. A process parked in `sleep_forever` — which `tests/sleep_forever_parks.rs @ v0.18.2` documents as parking through the host-await facility — reports `waiting`, **indistinguishable from a process blocked in a receive**. And that view is `pub(super)`, reachable only from the `erlang:process_info/2` BIF *inside* the VM, never from the embedder.

**The discriminator exists internally and is deliberately sealed.** `scheduler/suspension.rs:183-186` states it outright — *"A process without a mirror is plain-receive parked and is always wakeable."* But `mod suspension;` is **private** (`scheduler/mod.rs:176`, no `pub`, absent from every `pub use`), `SuspensionMirror` is `pub(super)`, and the `suspensions` map is a private field. `SuspensionKind` is nameable from outside, but **no API ever hands one back**.

**Per-API answer to the required extra column:**

| API @ v0.18.2 | which park does it represent/report? | discriminates the two? |
|---|---|---|
| `watch_exit` / `ExitWatch` / `ExitEvent` | **neither — exit only**, not park | n/a |
| `Scheduler::process_table()` → `ProcessHandle` | neither — **liveness only** (one private `pid`) | ✗ |
| `wake_process(pid)` | receive-park wake (delivery-driven) | ✗ (returns `()`) |
| `resume_process(pid) -> bool` | **SUSPEND-PARK, `Hook` kind only** | ⚠ leaks kind via `bool`, but **mutating — never use as a probe** |
| `wake_with_result*(pid, …) -> bool` | **SUSPEND-PARK, `HostAwait` kind only** | ⚠ same: mutating |
| `HookDecision::Suspend` | causes SUSPEND-PARK (`Hook`) | embedder knows only what it caused |
| `ProcessContext::request_*_suspend` | causes SUSPEND-PARK (`HostAwait`) | in-native only, not an observer seam |

⚠ **Do not probe with the wake calls.** They *are* kind-specific — `resume_process` refuses anything but `SuspensionKind::Hook` (`execution.rs:53-55`); `wake_with_result` publishes only against `HostAwait` (`execution.rs:307-311`) — so their `bool` returns do leak the kind. But they **mutate**, and `resume_process` carries a documented footgun (`execution.rs:43-50`): a resume for a pid that is not hook-suspended arms a **wildcard** (`RESUME_ANY_HOOK`, `suspension.rs:23`) that stays armed until that process's next hook suspension or its exit, and then **silently cancels that future suspend**. Probing corrupts the thing it measures.

⚠ **Test-only instruments — do not design against them.** `Scheduler::idle_park_count` (`scheduler/mod.rs:2059`), `observed_park_timeout_millis` (`:2069`), `suspension_mirror_registration_count` (`:2088`) are all `#[cfg(any(test, feature = "test-support"))]` and absent from production builds. **`idle_park_count` is a specific trap for this design: it counts *scheduler-thread* idle parks, not process parks** — a different meaning of the word.

### 3.6 What this means for the downstream emitter

**The emitter cannot ask beamr which park a process is in. The only park-kind knowledge available at v0.18.2 is what the embedder caused itself**, and it must maintain that in its own map:
- a hook callback that returned `HookDecision::Suspend` for a pid knows that pid is suspend-parked (`Hook`);
- a native that called `request_await_suspend` knows that pid is suspend-parked (`HostAwait`) — **and beamr's own status will call that `Waiting`, colliding with receive-park**;
- everything else observed as parked is, by elimination, receive-park — an inference from the embedder's own bookkeeping, never a beamr read.

**If the S5 design requires the emitter to *report* park kind, that discrimination must be built and owned in liminal, or requested upstream from beamr.** It cannot be derived from the v0.18.2 API. Two candidate upstream asks, in ascending cost: (i) make `SuspensionKind` readable per-pid via a non-mutating `Scheduler` accessor; (ii) split `ProcessStatus::Waiting` so host-await parks are not conflated with receive parks (a breaking change to a public enum).

---

## 4. F7 RETIREMENT — the `LIVENESS_POLL` / `poll_reply` sampling loop

### 4.1 It still exists at main. Verified at the bytes.

`crates/liminal/src/channel/actor/wait.rs @ 93d8cc7` (96 lines total, `grep -c ""`):

| line | item | content |
|---|---|---|
| `:13` | `<module scope>` | `use beamr::scheduler::Scheduler;` — the file's **only** beamr touchpoint |
| `:19` | `<module scope>` | `pub(super) const COMMAND_TIMEOUT: Duration = Duration::from_secs(5);` |
| `:24` | `<module scope>` | `const LIVENESS_POLL: Duration = Duration::from_millis(10);` |
| `:27-34` | `enum WaitFailure` | `Disconnected` / `Dead` / `TimedOut` |
| `:38` | `fn wait_live<T>` | delegates to `poll_reply` (`:43`) |
| `:58` | `fn wait_schema_live` | delegates to `poll_reply` (`:63`) |
| `:76` | `fn poll_reply<R>` | **the sampling loop** |

The loop body, `fn poll_reply` at `wait.rs:82-95 @ 93d8cc7`:

```rust
let deadline = Instant::now() + COMMAND_TIMEOUT;
loop {
    match response.recv_timeout(LIVENESS_POLL) {
        Ok(reply) => return Ok(reply),
        Err(mpsc::RecvTimeoutError::Disconnected) => return Err(WaitFailure::Disconnected),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            if scheduler.process_table().get(pid).is_none() {   // :87  ← THE SAMPLE
                return Err(WaitFailure::Dead);
            }
            if Instant::now() >= deadline {                      // :90
                return Err(WaitFailure::TimedOut);
            }
        }
    }
}
```

`LIVENESS_POLL` occurs at exactly 2 sites (`:24` declaration, `:83` use); `poll_reply` at exactly 3 (`:43`, `:63`, `:76` declaration). **The wake-up is a 10 ms timer and the liveness verdict is a table sample at `:87` — this is the F7 violation, and it is live at main.**

The stated reason is at `wait.rs:3-8 @ 93d8cc7`: the reply `SyncSender` is moved into the actor's command queue, which **outlives** the beamr process, so the sender is never dropped when the actor dies and a plain `recv_timeout(COMMAND_TIMEOUT)` would block the full 5 s. **Any replacement must still solve exactly that: an actor death that does not close the reply channel.**

### 4.2 The replacement pattern already exists in this workspace, at 0.16.3

This is the most consequential finding in §4, and it **narrows the beamr dependency of F7 retirement**.

`crates/liminal-server/src/server/connection/supervisor.rs @ 93d8cc7` already consumes a fully TOLD exit surface **on the current pin**:

- `:19-21` — `use beamr::scheduler::{ExitEvent, ExitEventSubscription, ReadinessToken, Scheduler, SchedulerConfig, SchedulerServices};`
- `:1169` in the supervisor ctor — `match scheduler.subscribe_exit_events() { Some(subscription) => … }`
- `:1054` — `fn run_reclaim_reactor(subscription: &ExitEventSubscription, scheduler: &Weak<Scheduler>, runtime: &Weak<ConnectionRuntime>)`
- `:1060` — `match subscription.recv()` → `Ok(ExitEvent::Exited { pid, reason })` (`:1061`) / `Ok(ExitEvent::Lagged)` (`:1067`) / `Err(_) => return` (`:1074`)

Its own doc comment at `:1039-1046` states it "blocks on beamr's sole exit-event subscription — never polling, never timed", and `:1161-1168` calls it "the TOLD source". The design record agrees and dates the surface *earlier* than the current pin: `docs/design/W4-LAW1-POLLING-RETIREMENT.md:564 @ 93d8cc7` records that "pinned beamr 0.15.4 already carries the public exit surface (`scheduler/execution.rs` `subscribe_exit_events` `:213` / `take_exit_outcome` `:194` / `peek_exit_reason` `:177`)".

**Consequences for the dispatcher:**
1. F7 retirement in `wait.rs` is **not blocked on 0.18.2**. A `subscribe_exit_events`-based rewrite is expressible on the current 0.16.3 pin, and the in-tree reactor is a working reference implementation.
2. ⚠ **`subscribe_exit_events()` returns an `Option` and liminal treats itself as the SOLE subscriber.** `supervisor.rs:1041-1043 @ 93d8cc7` says it drains the outcome store because "we are the sole subscriber and therefore the sole drainer". If `wait.rs` adds a *second* subscriber on the same scheduler, that sole-drainer assumption is the thing that breaks. **This is the central semantic decision of F7 retirement** (see §5), and it is a liminal-side design question, not a beamr version question — *unless* 0.18.2 changes the subscription model to multi-subscriber. That is the single most important thing to confirm in §2/§3.
3. The channel-actor scheduler (`channel/supervisor.rs:152`, `Scheduler::new`) is a **different scheduler instance** from the connection scheduler, so the sole-subscriber conflict may not actually arise. **Unverified — the dispatcher must confirm which scheduler instance `wait.rs`'s `scheduler` argument belongs to before designing the fix.**

### 4.3 `watch_exit` is absent from liminal at main

`grep -rn --include="*.rs" -E "ExitEvent|ExitEventSubscription|SchedulerServices|subscribe_exit|watch_exit" crates/` returns 9 lines, none containing `watch_exit`. Positive control: the same predicate, same run, returned the `subscribe_exit_events` site at `supervisor.rs:1169`. So the zero for `watch_exit` is a real absence in liminal, measured by an instrument proven to fire.

### 4.4 Other polling constructs found (context, not all in scope)

`crates/liminal/src/durability/bridge.rs:51 @ 93d8cc7` — `const MAX_POLLS: usize = 8;`, used at `:85` in a bounded `for _ in 0..MAX_POLLS` future-poll loop. This is a **bounded future driver**, not a liveness sampler, and does not touch beamr. Retired poll families are asserted *absent* by tests at `crates/liminal-server/src/cluster/membership.rs:937 fn membership_source_has_no_retired_poll_family` (naming `POLL_INTERVAL`, `poll_once`, `run_poll_loop`) — evidence the workspace already runs anti-polling pins.

---

## 5. MIGRATION SIZE ESTIMATE

### 5.1 Headline: 136 touchpoints, 3 edits

**Compile-forced edit sites for 0.16.3 → 0.18.2: exactly 3.** Every other one of the 136 API touchpoints compiles unchanged, because 62 of the 63 consumed symbols are UNCHANGED and the 4 MOVED symbols kept their public paths.

| site @ 93d8cc7 | enclosing item | current call | required edit | class |
|---|---|---|---|---|
| `crates/liminal/src/channel/supervisor.rs:152` | channel supervisor ctor | `Scheduler::new(SchedulerConfig{…}, registry)` | add 3rd arg `NativeBifs` | **SEMANTIC** |
| `crates/liminal/src/conversation/actor.rs:289` | conversation actor ctor | `Scheduler::new(SchedulerConfig{…}, registry)` | add 3rd arg `NativeBifs` | **SEMANTIC** |
| `crates/liminal-server/src/server/connection/supervisor.rs:1131` | `SupervisorInner` ctor | `Scheduler::with_services(SchedulerConfig{…}, SchedulerServices::from_config().owned_readiness(), registry)` | add 4th arg `NativeBifs` | **SEMANTIC** |

### 5.2 MECHANICAL vs SEMANTIC

**MECHANICAL (rename / re-signature, no behaviour choice): 0 sites.**

There is no mechanical tier in this migration. The two candidates both evaporated on measurement:
- `spawn_link_dirty` → `spawn_link` would have been mechanical, but liminal never calls it (§2.4c).
- `interpreter::run*` → `run_with_native_services` would have been mechanical, but liminal never touches the interpreter (§2.4a).

**SEMANTIC (behaviour choice required): 3 sites — all the same decision, asked three times.**

> **THE DECISION: for each of liminal's three schedulers, does it resolve native BIFs — `NativeBifs::none()` or `NativeBifs::registry(Arc<BifRegistryImpl>)`?**

beamr removed the ability to answer this by inheritance: `NativeBifs` has **no `Default` and no `From`**, by explicit design, so each call site must write its answer down. Each of the three is a genuinely independent answer.

**Evidence bearing on the answer (for the dispatcher to complete, not to inherit):**
- Both `Scheduler::new` sites feed a `ModuleRegistry` containing exactly one module, built by `actor_module(...)` (§5.3). That module's only external call is `Instruction::CallExt{ import: Operand::Unsigned(0) }` (`channel/actor/beam.rs:111-114`), which resolves through `resolved_imports[0]` to `ResolvedImportTarget::Native(NativeEntry{ function: process_command_nif, … })`. **That is module-level native resolution, not a BIF-registry lookup** — which points at `NativeBifs::none()`.
- ⚠ **But that is an inference from the bytecode I read, not a proof about the runtime.** The dispatcher must confirm that nothing reachable from these schedulers performs a BIF lookup — including the native processes spawned via `spawn_native` / `spawn_native_trap_exit` and their `NativeHandler::handle` bodies, which run arbitrary liminal code with a `NativeContext`. A wrong `none()` fails at runtime as an unresolved-BIF error, **not at compile time** — this is the one place in the migration where the compiler will not catch a mistake.
- Liminal's only `BifRegistryImpl::new()` constructions today are in `mod tests` (`channel/subscription.rs:711`, `routing/function/execute/actor.rs:218`), both feeding `WasmScheduler::new` — i.e. **production liminal currently builds no BIF registry at all**, which is consistent with `none()`.

### 5.3 Hand-built beamr structures — the fixture-compile risk

**Two builders construct `Module` as a COMPLETE struct literal with no `..Default::default()` fallback.** Any field **added** to `Module` breaks both; any field **removed** or **renamed** breaks both.

| builder | site @ 93d8cc7 | enclosing fn |
|---|---|---|
| channel actor | `crates/liminal/src/channel/actor/beam.rs:134-158` | `pub fn actor_module(module_name: Atom, entry_function: Atom, command_function: Atom) -> Module` (`:103`) |
| conversation actor | `crates/liminal/src/conversation/actor/beam.rs:155-179` | fn ending `-> Module` at `:124` |

⚠ These are **not test fixtures** — `actor_module` is `pub` and is the production module body for the channel actor. A break here is a production break, not a test break.

The 14 fields both literals set, in order (`channel/actor/beam.rs:135-157`; the conversation copy at `:156-178` is field-identical):

`name`, `generation`, `origin`, `exports`, `label_index`, `code`, `function_table`, `line_table`, `literals`, `constant_pool`, `resolved_imports`, `lambdas`, `string_table`, `line_info`.

Both set **`function_table: Vec::new()`** (`channel …beam.rs:141`, `conversation …beam.rs:162`) and **`literals: Vec::new()`**, `line_table: Vec::new()`, `lambdas: Vec::new()`, `string_table: Vec::new()`, `line_info: Vec::new()` — i.e. **six empty vectors**. This is precisely the shape an "empty-bundle unrepresentability" change would outlaw. **If any of these six moved to a non-empty-by-construction type, both builders fail to compile and the fix is a semantic one** (what is the module's true function table?), not a rename.

Nested literals, same exposure:
- `ResolvedImport { module, function, arity, target }` — `channel …beam.rs:145-154`, `conversation …beam.rs:166-175`
- `ResolvedImportTarget::Native(NativeEntry { function, dirty_kind, capability })` — `channel …beam.rs:149-153`, `conversation …beam.rs:170-174`
- `ConstantPool::default()` — `channel …beam.rs:144`, `conversation …beam.rs:165`
- `Instruction::{Label, LoopRec, RemoveMessage, CallExt, CallOnly, Wait}` with `Operand::{Label, X, Unsigned}` — `channel …beam.rs:104-123`

### 5.4 ⚠ `spawn_link_dirty`: the exposure I flagged, and how it resolved

The prior ruling ("liminal is unexposed to beamr 0.17.0's `spawn_link_dirty` removal") was **not inherited; it was re-measured**, as instructed.

- **The function: confirmed unexposed.** `grep -rn --include="*.rs" "spawn_link_dirty" crates/` → exit 1, no output. Positive control through the *same* predicate: `grep -rn --include="*.rs" "spawn_native_trap_exit" crates/` returns `crates/liminal/src/channel/subscription.rs:583` and `:591`. The instrument fires on a known-present beamr spawn API, so the zero is real.
- ⚠ **But a dirty-scheduler-adjacent FIELD is exposed.** `grep -rn --include="*.rs" "dirty_kind\|dirty\|Dirty" crates/` returns exactly **2** lines, both `dirty_kind: None,` inside the `NativeEntry` literals: `crates/liminal/src/channel/actor/beam.rs:151` and `crates/liminal/src/conversation/actor/beam.rs:172 @ 93d8cc7`.

**"Unexposed to `spawn_link_dirty`" does not entail "unexposed to the dirty-scheduler removal."** Had the 0.17.0 dirty removal also dropped `NativeEntry::dirty_kind`, both production module builders would fail to compile. That is a **field-level** exposure the function-level ruling does not cover, and it was the highest-value single byte-check in the migration.

✅ **RESOLVED CLEAN at the bytes (§2.4c).** `native/mod.rs:138 @ v0.18.2` still declares `pub dirty_kind: Option<DirtySchedulerKind>`, and `NativeEntry` has the same 3 public fields at both tags. Only the `spawn_link_dirty` **alias** was removed; the dirty-native **mechanism** stayed (`SchedulerConfig` also keeps `dirty_cpu_threads`/`dirty_io_threads`/`dirty_queue_depth`). Both `dirty_kind: None` literals compile unchanged.

**The methodological point survives the clean result, and is the reason to keep this subsection:** the prior ruling was true but under-scoped, and a migration that had inherited it rather than re-measuring would have carried an unexamined field-level risk on two *production* code paths. The ruling's scope was the function; the exposure was a struct field.

---

### 5.5 Fixture/builder verdict — ALL SAFE

The hand-built structures catalogued in §5.3 and the `dirty_kind` exposure in §5.4 were the largest identified risk. **Both resolve clean**, verified at blob level:
- `module.rs` is blob `d7d9345f3ee7cb0ab6df91937e0634292bb46561` at **both** tags → `Module` (14 pub fields), `ModuleOrigin` (4 variants), `ResolvedImport` (4 fields), `ResolvedImportTarget` (5 variants) all unchanged. Both `actor_module` builders compile as written.
- `native/mod.rs` → `NativeEntry` keeps its 3 public fields including `pub dirty_kind: Option<DirtySchedulerKind>` (`native/mod.rs:138 @ v0.18.2`). Both `dirty_kind: None` literals compile.
- `native/capability.rs` → `Capability` keeps 6 variants; `Capability::ProcessLocal` valid.
- `loader/decode/instruction.rs` → `Instruction` keeps **75 variants**; `loader/decode/compact.rs` → `Operand` keeps 12. Every opcode both builders emit is still present.
- `ConstantPool` (`constant_pool/mod.rs:42`) — ⚠ note for the record: **all three of its fields are private at both tags**, so it was never struct-literal constructible cross-crate. Liminal correctly uses `ConstantPool::default()` (`channel/actor/beam.rs:144`, `conversation/actor/beam.rs:165`). No change, no edit.
- `WasmScheduler::new` unchanged (`scheduler/wasm.rs` blob-identical), so both `mod tests` cooperative-scheduler fixtures compile.

**No liminal test or fixture that hand-builds a beamr structure gains or loses a field across the tags.**

### 5.6 F7 retirement — a separate, larger workstream

F7 retirement is **not forced by the version bump** and is **not blocked by it either**. Sizing it separately:

| item | finding |
|---|---|
| sites | 1 file, `crates/liminal/src/channel/actor/wait.rs @ 93d8cc7` — 3 functions (`wait_live:38`, `wait_schema_live:58`, `poll_reply:76`), 1 const (`LIVENESS_POLL:24`), 1 enum (`WaitFailure:27`) |
| class | **SEMANTIC** |
| the decision | which TOLD surface replaces the `process_table().get(pid)` sample at `wait.rs:87` |

**The mapped replacement is `Scheduler::watch_exit(pid)`** (`scheduler/execution.rs:251 @ v0.18.2`), and it is a materially better fit than the `subscribe_exit_events` pattern liminal already runs:

1. **It is per-pid and unlimited**, whereas `subscribe_exit_events` is **`OnceLock`-exclusive, one subscription per scheduler lifetime**. `wait.rs` is a per-command, concurrent wait path — it could never have taken a second broadcast subscription. `watch_exit` is the surface that makes a per-command TOLD wait expressible at all.
2. **`ExitWatchState` closes the registration race that `wait.rs` exists to handle.** The file's whole reason for polling is that the reply sender outlives the process, so a dead actor never disconnects the channel. `watch_exit` answers `AlreadyExited(reason)` for a process that died **before** the watch was armed, and `NoRecord` for a pid that never ran — so the caller is *never* left blocking on an answer that cannot arrive (`exit_events.rs:127-131`). That is exactly the failure mode `LIVENESS_POLL` was papering over.
3. **Ordering is guaranteed**: outcome installed → event published → watch fired (`exit_tombstones.rs:273-281`), so a woken waiter can immediately read the reason.
4. **Dropping the watch deregisters it** (`impl Drop`, `exit_events.rs:192`), so a timed-out or errored command leaves nothing behind.

The shape becomes: arm `watch_exit(pid)` once before waiting, then block on **both** the reply receiver and the watch — resolving whichever fires first, with `COMMAND_TIMEOUT` as the only deadline and **no 10 ms wakeup**.

⚠ Two cautions carried forward: **never** adopt `Scheduler::run_until_exit` (it is itself a 10 ms poll loop, `execution.rs:154-155`), and the `subscribe_exit_events` sole-subscriber assumption documented at `supervisor.rs:1041-1043 @ 93d8cc7` must not be disturbed — using `watch_exit` in `wait.rs` leaves that assumption intact, which is a further argument for it.

⚠ **Unresolved and required before designing the fix (§4.2 item 3):** which scheduler instance `wait.rs`'s `scheduler` argument belongs to. Liminal constructs at least three distinct schedulers (`channel/supervisor.rs:152`, `conversation/actor.rs:289`, `liminal-server …supervisor.rs:1131`). This is a liminal-side question, unanswered here.

### 5.7 Size summary

| bucket | sites | notes |
|---|---|---|
| MECHANICAL | **0** | no rename or pure re-signature applies to liminal |
| SEMANTIC — forced by the bump | **3** | the `NativeBifs` decision, ×3 schedulers |
| SEMANTIC — elective (F7) | **1 file / 3 fns** | `watch_exit` rewrite of `wait.rs`; not forced by the bump |
| fixture/builder breakage | **0** | verified at blob level |
| trait-impl breakage | **0** | all 7 conformance sites safe |
| feature breakage | **0** | `readiness`/`cooperative`/`json` byte-identical |

**The version bump itself is a 3-line change. The engineering judgement is concentrated entirely in one repeated question (`NativeBifs`), and the risk is concentrated in the fact that a wrong answer to it fails at runtime rather than at compile time.**

---

## 6. WHAT I COULD NOT VERIFY, AND WHY

Stated plainly so the dispatcher does not mistake silence for confirmation.

1. **The migration target is not pinned.** This map is built against **v0.18.2**. The prompt states the target is "0.18.2-or-later" and in motion; a 0.19.0 breaking cut could invalidate any UNCHANGED verdict here. **Every row in §2 must be re-run against the actual chosen tag.** The map keys on signatures, as instructed, so re-running is mechanical.
2. **Which scheduler instance `wait.rs` receives** — not established (§4.2, §5.6). It determines whether the `subscribe_exit_events` sole-subscriber assumption is even in tension with F7 work. Liminal-side question; I did not trace the call graph into `wait_live`'s callers.
3. **Whether `NativeBifs::none()` is correct for each of the three schedulers** — I gathered evidence pointing that way (§5.2) but did **not** prove it. Proving it means showing no BIF lookup is reachable from any of the three, including from inside `NativeHandler::handle` bodies. **A wrong answer fails at runtime, not at compile time.**
4. **Whether liminal's beamr dependency actually enables `jit`.** `jit` is in beamr's `default` features (`Cargo.toml:69 @ v0.18.2`), and liminal's three manifest lines do not visibly set `default-features = false` — but **I did not confirm the effective resolved feature set** (that needs `cargo tree -e features` or equivalent, and I was instructed to run no cargo command). This gates how urgently §2.6's JIT message-drop advisory applies to liminal on the current 0.16.3 pin.
5. **The status at 0.18.2 of the accumulator-rooting defect class** recorded at `CHANGELOG.md:131-145 @ v0.18.2` as unfixed through 0.18.1. Out of scope of the three dispatches; **not established either way**.
6. **No build or test was run** anywhere, per instruction. Every compile-safety claim in §5 is a **bytes-level inference from declarations**, not a compiler verdict. In particular "0 fixture breakage" means *no field changed*, not *it compiled*.
7. **`v0.17.1` has no CHANGELOG entry** (§3.1) — so anything that landed in that tag is undocumented in the file the project treats as authoritative for patch status. I compared v0.16.3 against v0.18.2 directly, so this does not affect the delta table, but it does mean the CHANGELOG cannot be relied on as a complete narrative of the range.
8. **Prose-only mentions were not audited for staleness.** 189 lines mention beamr in comments/docs (§1.3), including version-specific claims such as `channel/subscription.rs:583 @ 93d8cc7` ("0.16.1 `spawn_native_trap_exit`") and `docs/design/W4-LAW1-POLLING-RETIREMENT.md:564` (beamr 0.15.4 line/file citations). **These are historical citations, not live claims**, and under the version-bump three-classes rule they must NOT be swept by a repo-wide version edit. I did not enumerate which are which.
9. **Symbol counts are lower bounds in one respect**: the `Scheduler` method census (§1.5) is receiver-anchored on the literal string `scheduler.`, so a scheduler bound to a differently-named local would be missed. I found no such case by inspection but did not prove their absence.
