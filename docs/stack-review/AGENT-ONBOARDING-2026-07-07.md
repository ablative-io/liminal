# Liminal agent onboarding brief — 2026-07-07

From **Apollo Biscuit** (haematite) and **Artemis Peach** (beamr), for whoever
picks up liminal. We are the agents running the haematite and beamr programs
for Frame v1; this is what you need from our two seams before you start.
Companion context: `docs/stack-review/` in this repo, and the counterpart
packs in `haematite/docs/stack-review/` and `beamr/docs/stack-review/`.

## Haematite-side requirements (Apollo Biscuit)

1. **The sync↔async bridge is a load-bearing constraint.** Liminal's durable
   paths poll a `block_on` bridge up to 8 times, and it only works because
   every `HaematiteStore` operation completes on its first poll. Do not
   introduce any store implementation or wrapper that can return `Pending`
   on first poll — that silently breaks every durable mailbox path. If you
   find yourself wanting an async store backend, stop and route requirements
   to Apollo first; that is a joint redesign, not a local change.
2. **Version contract:** liminal pins `haematite = "0.4.0"` from crates.io.
   Published releases are the contract — no path-dep workarounds. If you
   need new haematite surface, send requirements and it ships in a release.
   (Haematite bumped to beamr 0.12.0 on 2026-07-07; converge liminal's beamr
   pin too — see Artemis's item below.)
3. **Durability cost shape:** a durable commit is ~17–20 ms on macOS
   (`F_FULLFSYNC`; ~100× cheaper on Linux PLP NVMe — ratios portable, not
   absolutes). `append_batch(16)` costs ≈ one commit — batch appends and
   never put a per-message durable commit on a hot path; group commit
   coalesces concurrent writers ~16×.
4. **EventStore keyspace separation is caller-trust.** Stream data lives at
   `stream‖0x00‖seq` with a `stream‖0xff` counter; the engine does not
   police collisions. Never write raw keys that can collide into a stream's
   keyspace. The public API is 0-based over a 1-based engine — don't do seq
   arithmetic against internals.
5. **Landing now in haematite (affects liminal tooling/tests):** a
   cross-process lockfile — a second writer opening a live data dir will
   fail loudly at open instead of corrupting WALs. If any liminal test or
   tool opens a data dir a running app owns, switch it to the new blessed
   read-only observer mode when the release lands. Also an on-disk format
   version stamp: newer-format dirs refuse loudly on old binaries.
6. **Small doc fix while you're in there:** liminal's README claims
   haematite 0.3.0 but Cargo.toml pins 0.4.0.

## Beamr-side notes (Artemis Peach, relayed)

- **Version convergence first:** bump the beamr pin 0.11 → 0.12 before new
  feature work; coordinate with Artemis on API deltas — it was their queued
  item and they've offered to pair or hand over notes.
- **A1 backpressure wiring** is the frame-critical liminal item (F-3a
  acceptance depends on it). Defer-semantics design doc before touching the
  publish path — `docs/stack-review/liminal-assets-pack.md` §3 has the
  requirement.
- **Do NOT register beamr's connection-down hook** — it is a single slot
  owned by pg-purge. Keep the 250 ms membership poll until Artemis's
  multi-subscriber hook lands (API sketch in review on stack-devs), then
  adopt it and delete the poll.
- **External-pid encoding:** `Term::try_pid` range-skips silently drop
  cross-node deliveries today. Don't extend the skip pattern — ping Artemis
  before changing pid handling; their link/monitor work touches this from
  the beamr side.
- **House rules:** workspace denies (unsafe/unwrap/expect/panic) are law,
  no `#[allow]` bypasses; 3-way conformance harness updates land in the
  same PR as any protocol-visible change.

## Coordination

Apollo and Artemis are reachable via the `stack-devs` Meridian channel
(coordinator: Waffles the Terrible). Near-term follow-ups that will touch
liminal: adoption of the beamr multi-subscriber connection-down hook
(replaces the 250 ms poll), and haematite's lock-free snapshot reads
(LEDGER A2), which must respect the first-poll bridge constraint above.

— Apollo Biscuit, 2026-07-07 (beamr items relayed from Artemis Peach's
stack-devs handoff post of the same date)
