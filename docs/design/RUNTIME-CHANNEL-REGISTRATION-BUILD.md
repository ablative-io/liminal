# Runtime channel registration — the layer-1 build brief

**Hermes Crumpet, 2026-08-03. The design is
`RUNTIME-CHANNEL-REGISTRATION.md` Parts I–II at `9d2dbc6` — accepted, amended
(v1a), and closed at the design gate. This brief adds no design. It fixes the
build order, the binding constraints an executor may not re-litigate, and the
verification that makes the build real. Registration of this brief rides the
lane's discipline: it goes to the stack lead's desk before any dispatch fires.**

## Scope

Three crates, every change additive, each version implication decided at its
own cut (design §II.10):

| Crate | Change |
| --- | --- |
| `liminal-protocol` | `CHANNEL_NOT_REGISTERED_CODE` (`0x0101`), `CHANNEL_QUIESCED_CODE` (`0x0102`), and the three-band map recorded beside them, same commit (L1 + its condition) |
| `liminal` | `ChannelHandle::is_actor_spawned` — never spawns, reads `core.get().is_some()` only, doc-comment states both (L6 + conditions) |
| `liminal-server` | the `channel_registry` module (types + two error enums), the `RwLock` roster, `build_configured_channel` extraction, the four APIs, `admit_channel` + `apply.rs` consultation, `limits.max_channels` + validation, the three pinned tests |

Out of scope, refused by name in the design (§II.11): removal, un-quiesce, any
wire admin surface, per-channel authorisation, schema evolution through
registration, metrics.

## Build order

Five steps, each a commit that compiles and passes the tier-1 suite on its own.
The order is the dependency order, and it front-loads the pieces later steps
consume:

1. **Protocol consts + band map** (`liminal-protocol`). No consumer yet — lands
   first so every later step can name the codes instead of carrying literals.
   liminal-server already depends on liminal-protocol (its Cargo.toml:18,
   verified at the gate), so no new dependency edge exists anywhere in this
   lane.
2. **`is_actor_spawned`** (`liminal`). Additive, self-contained, and test 3's
   instrument — built before the code it will observe, so the idle-cost test
   can land WITH step 4 rather than after it.
3. **The roster refactor** (`liminal-server`, no new public API, no new
   state): the field becomes `RwLock<HashMap<String, Arc<ConfiguredChannel>>>`,
   the boot loop's body is extracted to `build_configured_channel`, and
   `flush_durable_state` adopts the clone-then-drop-guard iteration (§II.4).
   Pure structure — behaviour-identical by intent, and the existing suite is
   the check that it is. This is the step where drift would hide, so it lands
   alone, not folded into step 4.
   **Re-scoped at registration (stack lead, 2026-08-03, binding):**
   `ConfiguredChannel`'s new fields do NOT land here — a field with no reader
   is dead-state debt in exactly the step designed to carry zero noise. The
   registered premise ("`-D warnings` fails the build") was corrected at the
   bytes — no warnings-deny exists in this workspace, and the tree already
   carries two *tracked* dead-code warnings that build green — but the cure
   stands on the truer ground: the estate's no-silent-tradeoffs rule bars
   deliberately minting new warning-debt, tracked or not, and a
   behaviour-identical step must be byte-quiet to mean what it claims.
4. **The APIs + the cap** (`liminal-server`): `ConfiguredChannel` gains
   `origin`/`state`/`quiesce_reason` in the same commit as their readers
   (the step-3 re-scope, binding), plus the `channel_registry` module,
   `register_channel` / `quiesce_channel` / `channel_status` /
   `registered_channels`, `limits.max_channels` with `Some(0)` in
   `collect_errors`' non-zero rule, and the three pinned tests (§II.9) landing
   in the same commit as the surface they pin. Test 1's reason-code assertion
   is satisfiable HERE, without the wire leg: `ChannelAccessError` and its
   `reason_code()` land in this step and the consts landed in step 1, so the
   test asserts the funnel's typed refusal directly — the WIRE carrying the
   code is step 5's claim, not test 1's (confirmed on the record at
   registration).
5. **The wire leg**: `ChannelOperation` + defaulted `admit_channel` on the
   trait, the `LiminalConnectionServices` override, `apply.rs` consultation in
   `publish_response`/`subscribe_response` (§II.5(d)). Last because it consumes
   everything, and its blast surface (every connection frame) deserves the
   shortest exposure to an unfinished lane.

## Binding constraints — an executor may not re-litigate these

1. The refusal STRING is preserved byte-for-byte (`channel '<name>' is not
   configured`); discrimination lives in the reason code only (§II.10 watch-1).
2. No new `ServerError` variant; no field added to `ListenerAccept`; no
   message-inspection anywhere, in any helper, under any name (F-v1-1).
3. The service's inner roster read STAYS. "Already checked upstream" is the
   exact deletion §II.5(d) Ground 2 forbids.
4. `admit_channel`'s default body admits. Only `LiminalConnectionServices`
   overrides. `WorkerFrontDoorServices` is not touched.
5. Typed codes (`0x0101`/`0x0102`) are emitted ONLY from the admission
   decision. The admitted-then-`Err` path carries `SERVER_ERROR_CODE` + the
   preserved string — degraded, never lying.
6. Quiesce is a CAS on the entry's `AtomicU8`, reason written to the `OnceLock`
   strictly before the flip; one-way; never touches the actor; never walks
   subscribers.
7. `channel_status` and `registered_channels` touch no actor. Any path through
   `core()` is a defect, not a shortcut.
8. The cap counts `RuntimeRegistered` entries only; absent cap refuses
   `CapNotConfigured { cap: "limits.max_channels" }`; no default value is
   invented anywhere.
9. Origin never flips (L4). Identity is the three-field set of §II.6, compared
   exactly as written (id AND document, opposite failure directions).
10. No `#[non_exhaustive]` on any new enum (L7, judged extension on the
    record). Zero `_ =>` arms, zero new `#[allow]` — estate bar, no exceptions.
11. The §II.4 flip measurement (publish throughput, N ≥ 16 publishers) is
    PRE-STATED, not run. Nobody runs it as part of this lane.

## Verification

- **Per step:** the step's own tests plus the tier-1 suite, teed logs, rc
  captured and form-checked before citation. Step 3 additionally cites the
  existing channel-path tests it is claiming behaviour-identity against.
- **Lane close:** one full workspace battery at the final tree
  (`--no-fail-fast` form per the run-22 precedent), priced and observed under
  the standing string-round discipline before it fires. The three pinned tests
  are present in its census: `runtime_registered_channels_are_absent_after_restart`
  (with its resumed-log positive control),
  `quiesce_admits_a_subscribe_that_read_active` (both-direction arms),
  `registered_idle_channels_spawn_no_actor` (both arms; honest claim is
  "registration spawns nothing" — the occupancy census stays blocked on
  beamr's inventory API, said aloud in the design).
- **Review floor:** ≥1 named Sol/Fable review per delegated piece before the
  landing word, per standing law.
- **Semver check at close:** each crate's diff re-read against its predicted
  class (all three additive-minor per §II.10's table); any surprise is a STOP,
  raw to the stack lead, before any cut is proposed.

## Dispatch shape

Steps are dispatched singly (2–3 concurrent max under the standing usage
doctrine; this lane needs no parallelism — the order is a chain). Each dispatch
carries: this brief, the design doc's governing sections by number, the
worktree path, the commit-message law (compose via Write, `--cleanup=verbatim
-F`, `sed`+`cmp` audit, Opus trailer for delegated commits), and the
constraint list above verbatim. Every executor claim is verified at the
dispatcher's own bytes before the next step fires. Branch stays unpushed until
the lane's landing word; nothing is published from this lane under any
circumstances — version cuts and publishes belong to their own gates.
