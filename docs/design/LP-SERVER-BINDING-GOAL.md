# liminal-server binding — goal session (phase B)

You are binding `liminal-server` to `crates/liminal-protocol` — the shared
participant-lifecycle crate extracted from the frozen design document
`docs/design/PARTICIPANT-CONTRACT.md` @ `55856ae`. The crate owns the rules:
typed states, typed transitions, typed outcomes, wire types, capacity/floor
algebra. The server owns the bindings: durable storage of the typed states,
transport, and transaction boundaries. You implement bindings; you NEVER
re-implement, duplicate, or shadow a rule the crate already owns. If a rule
seems missing from the crate, you report it in your declaration — you do not
add lifecycle logic to the server.

Prerequisite: phase A checkpoint 1 is merged or available on
`feature/liminal-protocol` (crate compiles, state machine + wire register +
algebra present). Read `docs/design/LP-EXTRACTION-GOAL.md` (the crate's brief)
and `docs/design/LP-ROADMAP.md` before starting.

## Ground rules

Identical to LP-EXTRACTION-GOAL.md's ground rules, with these substitutions:
worktree `.worktrees/lp-server-binding`, branch `feature/lp-server-binding`
(branched from the branch/commit carrying the crate — state which in your first
declaration). First commit: this brief file. You may modify `liminal-server`
and workspace manifests only. No new prose documents. Gates: build, clippy
`-D warnings`, tests. Commit and push per completed unit.

## Stage 1 — discovery (checkpoint, no code changes yet)

Read the current `liminal-server` source and produce, IN YOUR DECLARATION TEXT
(not a new document): (1) where participant attach/detach/leave/crash handling
currently lives (files, functions, line ranges); (2) what storage engine and
schema the server uses for participant/conversation state and where
transactions are drawn; (3) what wire protocol the server currently speaks and
whether it matches the crate's register — if it does not, STOP after this
declaration; migration vs cutover is a human ruling, do not choose silently;
(4) every reconnect/retry/backoff/delay/polling site touching participant
state (the contract's LAW-1 sweep classified 13 families — cite what you find
against `connection/process.rs`, `supervisor.rs`, and wherever else they
live); (5) your binding plan as a short ordered list of units. PAUSE for
review of this declaration before stage 2.

## Stage 2 — binding

- Storage: persist the crate's typed states (binding states, the four-variant
  detach cell, membership/tombstones, the seven stored-edge kinds) with
  transaction boundaries matching the document's atomicity claims (e.g. detach
  = terminal append + floor transition + cell replacement + binding release in
  ONE durable transaction; attach = clear-to-Terminalized atomically). The
  crate's transition functions decide; the server persists what they return.
- Transport: wire handlers decode requests with the crate's wire types, call
  the crate's transitions, encode the crate's outcomes. No handler constructs
  an outcome by hand.
- No polling anywhere in what you touch (LAW-1): wake on events, never on
  timers-as-polling. Retirement of pre-existing polling sites outside your
  touched paths is out of scope (phase D) — but never ADD one.
- Tests: server-level integration tests exercising attach/detach/leave/crash/
  replay through storage — at minimum the two blocker scenarios end-to-end
  (post-attach detach-token replay returns TerminalizedDetachCell with the old
  epoch from a COLD start, proving it's durable, not in-memory; two
  participants acking the same suffix through the server). Use the crate's
  acceptance-case tests as the source of expected values.

## Final declaration

Commit hash, files touched, test counts, gate outputs, the list of any crate
API gaps you hit (reported, not patched around), and any rule you found
yourself tempted to duplicate in the server — name it and where the crate
version lives instead.
