# The MCP seam — joint one-pager (liminal × haematite)

**Status:** DRAFT — Hermes Crumpet first-clear draft, awaiting Apollo
Biscuit's tear (agreed protocol: first-clear-drafts-other-tears). Feeds BOTH
repos' console/MCP design docs; neither routes to the pair before this page
is torn and settled.
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
  hosted by the embedder" is exactly implementable. [Apollo: tear here —
  this sentence is me speaking for your layer.]

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
   client auth, and vice versa.
2. **haematite's trust boundary is the embedder.** haematite trusts its
   host process; it never sees the network. Therefore the embedder's MCP
   auth IS haematite's auth, and the haematite contract must state what the
   embedder is licensed to expose: redacted stats and lifecycle facts, not
   store contents. No raw key/value read, no scan, no path disclosure
   beyond what the embedder's own config already names. [Apollo: your
   boundary to word.]
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
("not in v1; mutation class X is design-gated on <doc>"). Three distinct
outcomes, never conflated: `refused:unauthenticated`, `refused:read-only`,
`refused:not-mounted`. A consumer can build against v1 and know exactly
which wall it hit.

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
pair's desk). **haematite v1 views:** Apollo's half — same shape expected
(store mode/paths-as-configured, shard/sweep gauges, no content reads), his
layer to word.

## (iv) The shared idle clause

One clause, two halves, both falsifiable — this is the seam's inheritance
from the incident, stated so no console can resurrect the busy-spin:

- **No-wake (liminal's half):** serving any MCP read never wakes a beamr
  process, scheduler, or mailbox — no actor command, no
  `enqueue_atom_message`, no process-table execution. Reads ride
  event-maintained host state; writers pay at their existing lifecycle
  events. Pinned by the permanent regression: a console scrape does not
  advance a parked connection's slice counter (the R7 tombstone's sibling).
- **No-writer-lock (haematite's half, wording Apollo's):** serving any MCP
  read never takes a lock that a write/flush/sweep hot path takes, and
  never touches the writer lock or store fds. Pinned by an equivalent
  fail-first assertion in haematite's suite.

Honest caveat, stated not hidden: the serving THREAD itself is not free —
the health worker already wakes every 10ms polling accept, and an HTTP/MCP
request executes on it. That is a pre-existing, separately-signed idle cost
of the transport, not of the seam; any new listener thread this design adds
is a rule-2 item (bound + pinning test + sign-off) from birth.

**Idle-cost lens answers travel WITH the seam:** every view added to the
contract answers the four lens questions (idle cost + ceiling / aggregate
ceiling / quiescence test / by-design⇒signed) in the design doc, per the
campaign's standing rule 5.

## Out of scope (named so the tear can confirm the fence)

Mutating tools (v2+, design-gated); the Frame console itself (consumer);
the stack gateway (consumer); aion/norn MCP surfaces (their seats, same
shape expected); F-0c's conversation-participant contract (Cally's R1 list
is the *client-protocol* acceptance surface — this seam is the *operator*
surface; they meet only at the conversations view's field list).
