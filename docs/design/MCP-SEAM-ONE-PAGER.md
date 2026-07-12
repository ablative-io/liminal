# The MCP seam — joint one-pager (liminal × haematite)

**Status:** TORN AND FOLDED — Hermes Crumpet first-clear draft (5e0e098),
Apollo Biscuit's tear 2026-07-12 (verdict: structure stands; six amendments
T1–T6, folded in this revision — T2/T3/T4 are his layer's load-bearing
walls, worded by him). Joint names: Hermes Crumpet (liminal) + Apollo
Biscuit (haematite). Feeds BOTH repos' console/MCP design docs.
**Inputs:** liminal console scout (norn session b4fad82a, envelope
`~/.norn/delegations/claude-scout-console.GMHHYy`, observed at liminal main
3c3aa10); Cally Ray's F-0c §R1 consumer contract (frame `6a750c8`,
docs/briefs/F-0c-liminal-conversation-spike.md); Tom's two directives
(2026-07-12 via Vesper): consoles are eventually rewritten in Frame, so the
MCP seam is the DURABLE artifact and console UI is interim chrome; and the
liminal-uplift north star — liminal as a product (agent registers mailbox
with liminal / workflows with aion / dispatch with norn, MCP on all).

## 0. What the seam is

The seam is a **per-service MCP contract**: a named, versioned set of MCP
resources and tools each stack service commits to, with typed refusals for
everything outside it. It is NOT a console, NOT a listener, and NOT a
gateway — those are hosts and consumers of the seam. Every claim below is
written so a future Frame console, a CLI, or an agent hitting the surface
directly gets identical semantics. Contract-heavy here, chrome-light
everywhere else, per directive (a).

## (i) Transport: per-service vs gateway — argued, not assumed

**Position: the contract is per-service; the listener is per-host-process.
A stack gateway is a consumer of seams, never the seam.**

The argument rests on an asymmetry the gateway shape cannot absorb:

- **liminal-server is a process** with host-side state and an existing
  observability listener (health worker: /health /ready /metrics,
  bind-address-scoped, single thread). Everything its MCP surface serves is
  in-process, event-maintained host state — a gateway would need a second
  protocol to extract that state, which either re-invents this seam one
  layer down or degrades to scraping /metrics text and losing types.
- **haematite is a library.** It has no process and no listener of its own;
  its MCP surface can only be served by whatever process embeds it
  (liminal-server, aion, a standalone inspector). A "haematite MCP
  endpoint" as a deployable is a category error; a "haematite MCP contract
  hosted by the embedder" is exactly implementable. **(T1, torn and
  confirmed):** the standalone-inspector host is not hypothetical — the
  `crates/haem` stats CLI is a real standalone host today, and the
  haematite contract's FIRST host may be a local `haem` process speaking
  MCP over **stdio**: zero listener, zero idle cost, auth = OS
  process-spawn rights. The seam contract is **transport-identical** over
  stdio and an embedder's HTTP listener — same resources, same refusals,
  same versioning.

So: **each service defines its contract; each host process mounts the
contracts of the services it embeds** (liminal-server mounts liminal's, and
— if the embedder opts in — haematite's, namespaced). Failure honesty falls
out for free: an unreachable surface means THAT process is unreachable, not
an aggregator hiccup. Product honesty too: liminal-as-product speaks MCP
itself, not through a chaperone. A stack-level gateway (one place to point
an agent at everything) stays legitimate as a thin MCP *client* that
re-exports mounted seams — a Frame/console concern, out of scope here.

Transport binding for liminal v1: the MCP surface rides the EXISTING health
listener process-side (same bind-address exposure model, no new thread), or
a sibling listener in the same process if the pair rules the head-of-line
risk (serial handler, 2s read timeout) disqualifies sharing. Either way the
seam contract is identical — that's the point of the seam.

## (ii) Auth: composing liminal's H4 token gate + haematite's trust boundary

**Bind-address control is exposure management, not authentication.** The
health listener today has NO auth (verified: no token, header, or TLS check;
routing parses only the request line) — and the console/MCP surface does NOT
inherit H4, because H4 authenticates *wire-protocol clients* (frame-level
token gate on every frame), a different audience from *operators/agents*
reading the service.

Contract:

1. **Separate credential, same discipline.** MCP access carries its own
   bearer token, configured independently of the H4 client token
   (`[console] auth_token` class, exact key at design-doc level). One
   credential class per audience; rotating operator access never touches
   client auth, and vice versa. **(T1):** where the transport is stdio (the
   `haem`-class local host), the bearer-token clause is **N/A by
   construction and says so** — auth is the OS's process-spawn boundary;
   nobody bolts a token onto a pipe.
2. **haematite's trust boundary is the embedder — and it splits into TWO
   read modes with OPPOSITE lock semantics that the seam must never blur
   (T2, Apollo's wording, load-bearing):**
   - **(a) LIVE observer:** the blessed `ReadOnlyDatabase` path — takes NO
     lock, cannot block a writer. This is the ONLY mode a live host mounts.
   - **(b) OFFLINE inspector:** the `vacuum_stats` class — takes the A4
     WRITER LOCK by design (exclusive store access is its correctness
     precondition). Offline tools NEVER mount on a live host's seam; an
     attempt is a typed refusal in its own class
     (`refused:requires-exclusive-store`, §(iii)) whose remedy names the
     offline runbook. Without this split, someone eventually wires
     `haem stats` behind an MCP tool on liminal-server and the console
     acquires the writer lock against a live database — the storage twin of
     the busy-spin resurrection the liminal birth-rule kills.
   The embedder's MCP auth IS haematite's auth; what the embedder is
   licensed to expose is redacted stats and lifecycle facts, never store
   contents: no raw key/value read, no scan, no checkout, no path
   disclosure beyond what the embedder's own config already names.
3. **Redaction by default, allowlist to widen.** Raw fds, auth token bytes,
   cluster cookies, and filesystem paths are redacted from every MCP
   response by default. Widening is an explicit config act with a named
   operator decision, never a default.
4. **Unauthenticated surface is a typed refusal, not a silent 404** — see
   (iii); an agent must be able to distinguish "wrong credential" from
   "surface not mounted".

## (iii) v1 is READ-ONLY — as a typed-refusal contract, not an absence

v1 ships no mutating tools. But per house doctrine (**refusals name their
remedy**), read-only is a CONTRACT SHAPE, not a missing feature: the seam
declares the mutation classes it does not serve, and a mutation attempt
gets a typed refusal naming the class, the version gate, and the remedy
("not in v1; mutation class X is design-gated on <doc>"). **Four** distinct
outcomes, never conflated (T2 adds the fourth):
`refused:unauthenticated`, `refused:read-only`, `refused:not-mounted`,
`refused:requires-exclusive-store` (an offline-inspector capability asked
of a live host — remedy names the offline runbook). A consumer can build
against v1 and know exactly which wall it hit.

**liminal v1 views** (each field arrives at design-doc level with the full
per-field line: owner / update-event / read-primitive / lock-behavior /
staleness / cardinality / redaction / idle-cost):

- **connections** — active count, admissions, per-connection peer/worker/
  readiness posture from OpsState snapshots (never the records mutex).
- **channels** — configured-channel metadata + lifecycle-maintained
  subscriber gauges (never ChannelRegistry::list, which wakes every actor).
- **subscriptions** — inbox budget occupancy, overflow flags, depth gauges
  (event-maintained host atomics; never the inbox queue lock).
- **conversations** — actor/participant gauges, pending-reply/tombstone
  totals, armed-timer count (event-maintained; never registry mutexes or
  actor queries).
- **caps/pressure** — the §5 LimitsConfig matrix as config-vs-occupancy
  pairs, refusal counters by typed-refusal class.
- **inventory** — beamr `service_inventory()` lines (host-side, verified
  no actor wake) + scheduler census; the same lines the census pins assert.

All of it rides three read primitives and nothing else: the /metrics
registry snapshot, `service_inventory()`, and OpsState-class
event-maintained snapshots (immutable redacted config + atomics +
Arc-swapped copy-on-write). The R7 slice counters ride only via their
signed promotion (per-connection AtomicU64, active-slice cost signed at the
pair's desk).

**haematite v1 views (T4, Apollo's half):**

- **store identity** — data_dir AS CONFIGURED, format_version, shard_count
  (config echo; the vacuum TRUST_BOUNDARY precedent applies verbatim).
- **shard gauges** — materialised count, per-shard committed seq
  (embedder-maintained).
- **branch/snapshot metadata counts** — names redacted by default per
  (ii)(3): count + kind only, until allowlisted.
- **last VacuumReport AS A DOCUMENT** — if the operator ran one; the report
  is already serde-Serialize and redaction-shaped. The seam serves the
  artifact, never runs the tool.

NO content reads, NO scan, NO checkout — the mutation-class registry lists
vacuum/sweep (unit 2) as design-gated from birth.

### Versioning (T5 — load-bearing, given consoles-rewritten-in-Frame)

The contract version is a **resource the consumer reads FIRST**. Additive =
new views/fields. Breaking = any removal, rename, redaction-widening
reversal, or refusal-class change; a breaking change bumps the major, and
the old contract's refusals name the migration. The seam is the durable
artifact, so its version discipline is the whole ballgame — and it is the
part other seats' seams inherit verbatim (§out-of-scope).

## (iv) The shared idle clause

One clause, two halves, both falsifiable — this is the seam's inheritance
from the incident, stated so no console can resurrect the busy-spin:

- **No-wake (liminal's half):** serving any MCP read never wakes a beamr
  process, scheduler, or mailbox — no actor command, no
  `enqueue_atom_message`, no process-table execution. Reads ride
  event-maintained host state; writers pay at their existing lifecycle
  events. Pinned by the permanent regression: a console scrape does not
  advance a parked connection's slice counter (the R7 tombstone's sibling).
- **No-writer-lock (haematite's half — T3, Apollo's wording):** "Serving
  any MCP read acquires zero locks that any write/flush/sweep path
  acquires, touches zero store fds for writing, and performs zero node
  writes — pinned fail-first." The caveat that rides WITH it:
  `ReadOnlyDatabase`'s read is WAL-replay-per-call (recorded under LEDGER
  A2) — a polling console re-replays the WAL every scrape. So the haematite
  v1 views ride embedder-maintained gauges and config-echo wherever
  possible, and any observer-backed view carries its per-call cost as a
  lens line PLUS a staleness field (as-of-last-replay) in the response
  itself. A view that hides its polling cost is the incident wearing a
  dashboard.

Honest caveat, stated not hidden: the serving THREAD itself is not free —
the health worker already wakes every 10ms polling accept, and an HTTP/MCP
request executes on it. That is a pre-existing, separately-signed idle cost
of the transport, not of the seam; any new listener thread this design adds
is a rule-2 item (bound + pinning test + sign-off) from birth. (The
stdio-host transport, T1, carries zero idle cost by construction.)

**Idle-cost lens answers travel WITH the seam:** every view added to the
contract answers the four lens questions (idle cost + ceiling / aggregate
ceiling / quiescence test / by-design⇒signed) in the design doc, per the
campaign's standing rule 5.

## Out of scope (named so the tear can confirm the fence — confirmed, T6)

Mutating tools (v2+, design-gated; haematite's vacuum/sweep listed in the
mutation-class registry from birth); the Frame console itself (consumer);
the stack gateway (consumer); aion/norn MCP surfaces — their seats, same
shape expected, **and the §Versioning paragraph is the part they inherit
verbatim, so four seams don't invent four version disciplines** (T6);
F-0c's conversation-participant contract (Cally's R1 list is the
*client-protocol* acceptance surface — this seam is the *operator*
surface; they meet only at the conversations view's field list).
