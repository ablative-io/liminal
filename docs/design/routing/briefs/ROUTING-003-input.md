# ROUTING-003: Implement predicate compiler and routing function execution

**Cluster:** routing
**Repo:** `/Users/tom/Developer/ablative/liminal`
**Clone URL:** https://github.com/ablative-io/liminal.git
**Base ref:** `main`
**Reviewers:** Waffles the Terrible
**Depends on:** ROUTING-001

---

## Purpose

Delivers the unification layer of the two-tier routing model (ADR-003): predicates compile to routing functions, and the engine always executes functions. This brief builds the compiler that transforms Predicate types into optimized routing functions, and the execution harness that loads and runs Gleam routing functions in supervised beamr processes with fault isolation and hot deployment.

## Task

Create routing/compiler.rs with the predicate-to-function compiler, and routing/function.rs with the routing function execution harness. The compiler transforms Predicate types into routing functions, applying index-aware optimizations (field extraction ordering, short-circuit layout). Compiled functions must produce identical results to direct predicate evaluation. The function execution harness loads Gleam routing functions via content-hash module loading into beamr, runs them as supervised processes, and supports hot deployment. A routing function receives the message and a consumer state view (capacity, affinity tags). A crashing routing function must not affect other channels. This brief does not implement the dispatch conversation or consumer group model — those belong to ROUTING-004.

## Requirements

### R1: Implement predicate-to-routing-function compiler

**Spec:** WHEN a Predicate is submitted to the compiler, THE SYSTEM SHALL produce a compiled routing function representation that encodes the predicate logic. The compiled form SHALL be executable by the routing function harness. THE SYSTEM SHALL NOT modify the original Predicate during compilation.

**Acceptance criteria:**
- compile(Predicate::Comparison { field: "amount", op: Gt, value: Integer(1000) }) produces a CompiledFunction.
- compile(Predicate::And([p1, p2])) produces a CompiledFunction encoding the conjunction.
- compile(Predicate::Not(Box::new(p))) produces a CompiledFunction encoding the negation.
- The original Predicate is unchanged after compilation (borrow, not consume).

**Files:**
- `crates/liminal/src/routing/compiler.rs` (create)

*Checklist: C8*

### R2: Ensure compiled predicates produce identical routing decisions

**Spec:** WHEN a compiled predicate function is executed against a message, THE SYSTEM SHALL produce the identical routing decision as direct predicate evaluation for all input combinations. THE SYSTEM SHALL NOT exhibit behavioral divergence between compiled and interpreted paths.

**Acceptance criteria:**
- For 100 randomized predicate + message combinations, compiled execution and direct evaluation produce identical boolean results.
- Edge cases — empty And/Or, deeply nested Not, missing fields, type mismatches — produce identical results in both paths.
- A property-based or exhaustive comparison test covers all predicate variants.

**Files:**
- `crates/liminal/src/routing/compiler.rs` (modify)

*Checklist: C9*

### R3: Apply index-aware optimizations in the compiler

**Spec:** WHEN compiling a predicate with multiple field references, THE SYSTEM SHALL order field extraction to minimize redundant access (fields used in more clauses are extracted first). WHEN compiling boolean combinators, THE SYSTEM SHALL arrange clauses for optimal short-circuit behavior (cheapest or most selective first where determinable). THE SYSTEM SHALL NOT reorder clauses in a way that changes observable evaluation semantics.

**Acceptance criteria:**
- A compiled And predicate with an Exists check and a Comparison extracts the Exists field first when it is cheaper to evaluate.
- The compiler reorders And clauses to place field-existence checks before value comparisons.
- Reordering does not change the boolean result for any input — verified by the R2 equivalence tests.

**Files:**
- `crates/liminal/src/routing/compiler.rs` (modify)

*Checklist: C10*

### R4: Define routing function loading via content-hash modules

**Spec:** The routing function module SHALL define a RoutingFunction trait or type representing executable routing logic loaded from a Gleam module. WHEN a routing function module is loaded, THE SYSTEM SHALL load it via content-hash module loading into beamr. THE SYSTEM SHALL NOT load modules by mutable path references.

**Acceptance criteria:**
- RoutingFunction type wraps a reference to a loaded beamr module.
- Loading a routing function module by content hash succeeds and produces an executable RoutingFunction.
- Loading the same content hash twice returns the already-loaded module without duplication.

**Files:**
- `crates/liminal/src/routing/function.rs` (create)

*Checklist: C11 | Stories: S3*

### R5: Execute routing functions in supervised beamr processes

**Spec:** WHEN a routing function is invoked, THE SYSTEM SHALL execute it in a supervised beamr process. The function SHALL receive the message and a ConsumerStateView containing per-consumer capacity and affinity tags. THE SYSTEM SHALL return the routing decision produced by the function. THE SYSTEM SHALL NOT execute routing functions on the calling thread.

**Acceptance criteria:**
- Routing function execution spawns a beamr process.
- The spawned process receives a ConsumerStateView with capacity and affinity fields.
- The function's routing decision (selected consumer) is returned to the caller.
- ConsumerStateView implements Debug and contains current_in_flight, max_in_flight, buffer_depth, and affinity_tags fields.

**Files:**
- `crates/liminal/src/routing/function.rs` (modify)

*Checklist: C12, C13 | Stories: S3*

### R6: Implement fault isolation for routing function execution

**Spec:** IF a routing function panics or enters an infinite loop, THEN THE SYSTEM SHALL terminate the offending process without affecting routing evaluations for other channels. THE SYSTEM SHALL NOT propagate a routing function crash to the calling channel's evaluation pipeline. THE SYSTEM SHALL return an error to the caller indicating the function failed.

**Acceptance criteria:**
- A routing function that panics is caught by the supervisor; the caller receives an error result.
- After a routing function crash on channel A, routing evaluation on channel B proceeds normally.
- A routing function exceeding a supervision timeout is terminated and returns an error.

**Files:**
- `crates/liminal/src/routing/function.rs` (modify)

*Checklist: C14*

### R7: Implement hot deployment for routing functions

**Spec:** WHEN a new version of a routing function is deployed, THE SYSTEM SHALL load the new module via content-hash loading and replace the active function reference. In-flight evaluations using the previous version SHALL complete normally. THE SYSTEM SHALL NOT drop connections or pause dispatch during hot deployment.

**Acceptance criteria:**
- Deploying a new routing function version while the old version is in use does not interrupt in-flight evaluations.
- After deployment, the next routing evaluation uses the new function version.
- The old module version remains loaded until all in-flight references are released.
- No connections are dropped during the deployment transition.

**Files:**
- `crates/liminal/src/routing/function.rs` (modify)

*Checklist: C15 | Stories: S4*

## Boundaries

- SHALL NOT implement the dispatch conversation — that belongs to ROUTING-004.
- SHALL NOT implement consumer group logic — that belongs to ROUTING-004.
- SHALL NOT define backpressure signals or capacity tracking — those belong to ROUTING-005.
- SHALL NOT implement pressure policy enforcement — that belongs to ROUTING-006.
- SHALL NOT expose beamr types in the routing function public API — the boundary module wraps beamr.

## Verification

- cargo clippy -p liminal --all-targets -- -D warnings passes clean.
- cargo test -p liminal passes with all compiler and function execution tests green.
- cargo fmt --check passes clean.
- compiler.rs and function.rs are each under 500 lines including tests (CN1).
- Compiled predicate equivalence test covers all Predicate variants.
- All public types implement Debug (CN4).
- No .unwrap() or .expect() in library code (CN2).

## Architecture Decision Records

### ADR-003: Two-tier routing: predicates for the common case, functions for the flexible case

**Decision:** Two routing tiers: declarative predicates (WHERE clauses, indexable, optimizable by the bus) for the 90% case, and imperative routing functions (Gleam code running in beamr, hot-deployable, supervised) for the 10% case. Predicates compile to functions internally — one runtime model. The rejected alternative is predicates-only (insufficiently flexible for load-aware and state-dependent routing) or functions-only (not analyzable or optimizable).
**Decided by:** Frodo + Bono alignment

## Constraints

- **CN1:** No file over 500 lines. If a module approaches this limit, extract into submodules.
- **CN2:** No panics in production code. .unwrap() and .expect() only in tests.
- **CN3:** mod.rs contains ONLY pub mod declarations and pub use re-exports. No logic.
- **CN4:** All public types implement Debug. All error types implement std::error::Error.
- **CN5:** Predicate evaluation must not allocate per-message. Predicates evaluate against borrowed message fields with no heap allocation on the hot path.
- **CN6:** Backpressure signals must be delivered to producers within the same publish round-trip. No asynchronous notification after the fact.
- **CN7:** Consumer crash detection via process link must trigger re-routing within one millisecond. No polling or heartbeat-based detection.
- **CN8:** Routing function execution is supervised and isolated. A panic or infinite loop in one routing function must not block evaluation for other channels.
- **CN9:** Adding or removing a consumer from a group must not pause, block, or restart dispatch for other consumers in the same group.

## Checklist Items

- **C8:** Predicates compile to routing functions via routing::compiler.
- **C9:** Compiled predicate functions produce identical routing decisions to direct predicate evaluation for all input combinations.
- **C10:** The compiler applies index-aware optimizations: field extraction ordering and short-circuit layout.
- **C11:** Routing functions are Gleam modules loaded into beamr via content-hash module loading.
- **C12:** Each routing function executes in a supervised beamr process with fault isolation.
- **C13:** A routing function receives the message and a consumer state view containing capacity and affinity tags.
- **C14:** Routing function crash does not affect routing evaluations for other channels.
- **C15:** Routing functions are hot-deployable: a new version replaces the old without dropping connections or pausing dispatch.

## User Stories

- **S3:** As an application developer, I want to deploy a custom routing function in Gleam so that I can implement load-aware or state-dependent routing logic that predicates cannot express.
- **S4:** As an application developer, I want to hot-deploy an updated routing function so that I can change routing logic in production without restarting the bus or dropping connections.

## Design Intention

When this cluster is complete, a developer configures routing as a declaration -- predicate subscriptions for the common case, routing functions for the flexible case -- and the bus handles the rest. Backpressure is not an afterthought bolted onto the consumer; it is a protocol primitive that producers see and react to in real time. Consumer groups do not rebalance. A dispatch conversation selects a consumer, links to it, observes completion, and re-routes on failure -- per message, with sub-millisecond crash detection via beamr process links. The experience should feel like the bus is an intelligent intermediary that understands both what messages mean and how much work consumers can absorb, rather than a dumb pipe that delivers bytes and hopes for the best.

## Workflow Config

- Isolation: worktree
- Verify-fix cap: 3
- Review cap: 1
