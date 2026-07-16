# liminal-sdk receive + release prep — goal session (phase C)

You are completing `liminal-sdk`'s receive side against `crates/liminal-protocol`
and preparing (NOT executing) a release. The crate owns the lifecycle rules and
wire types; the SDK owns the client-side bindings: transport, reconnect
behavior, surfacing outcomes to callers. Same law as the server phase: you
never re-implement a rule the crate owns; a missing rule is reported, not
patched in locally.

Prerequisites: phase A checkpoint 1 reviewed; phase B stage-1 discovery done
(so wire compatibility is settled). Read `docs/design/LP-EXTRACTION-GOAL.md`
and `docs/design/LP-ROADMAP.md` first. The existing receive-side skeleton in
`liminal-sdk` is your starting point — extend it, don't rewrite it
(copy-don't-rewrite applies to working code).

## Ground rules

Identical to LP-EXTRACTION-GOAL.md's ground rules, substituting: worktree
`.worktrees/lp-sdk`, branch `feature/lp-sdk`, first commit this brief. You may
modify `liminal-sdk` and workspace manifests only. Gates: build, clippy
`-D warnings`, tests.

## Scope

- Decode server messages with the crate's wire types; drive a client-side
  typed lifecycle (attach → bound → detach/leave, crash/resume) using the
  crate's states; surface every outcome the register defines — no outcome
  swallowed, no silent fallback, no generic error arm hiding a typed one.
- Reconnect per the contract's client rules: replay detach while no newer
  attach/Leave is durable; on `AuthoritySuperseded`, record and never resend;
  handle `TerminalizedDetachCell` and `DetachInProgress` as the terminal
  statuses they are. No polling — reconnection is event/backoff-free per
  LAW-1's client obligations as encoded in the crate.
- Tests: client-side lifecycle tests against a mocked/loopback transport using
  the crate's types, including token-replay after response loss, crash-resume,
  and the ack selector thresholds (`AckRegression`/`AckNoOp`/`AckCommitted`/
  `AckGap`).
- Release PREP only: changelog entries, version bumps staged in a commit,
  publish checklist in the declaration. You never run `cargo publish`, never
  tag, never push tags — publish and tags are lead-gated (Waffles or Tom).

## Final declaration

Commit hash, test counts, gate outputs, the staged version bumps, the publish
checklist, and any crate API gaps found.
