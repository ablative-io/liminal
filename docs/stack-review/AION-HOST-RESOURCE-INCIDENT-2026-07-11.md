# Aion host resource incident — Liminal handoff (2026-07-11)

For Vesper and the implementing team. This is a focused incident note against
Liminal `main` at `288b997` / published 0.2.3. It covers the connection CPU
failure and the unnecessary services constructed by Aion's worker front door.

## Executive finding

Liminal is the primary source of Aion's chronic CPU burn. Every live TCP
connection remains runnable when completely idle because socket `WouldBlock`
returns `NativeOutcome::Continue`. Beamr correctly interprets `Continue` as
immediate requeue. A four-thread connection scheduler therefore consumes about
four cores as soon as enough idle connections exist.

The previous Beamr FIFO repair is present and correct. It prevents one spinner
from starving its peers; it cannot make any connection park. The Liminal
connection process needs readiness-driven wakeup, not sleeps or a smaller
scheduler.

A second, independent issue magnifies Aion's footprint: the worker-only server
constructs channel, conversation and Haematite services that it never uses.

## Incident evidence

The host investigation found:

- A freshly relaunched Aion server stabilized at approximately 350–427% CPU
  with 11 connected but idle workers.
- Four connection-scheduler workers dominated the process; the hottest four
  represented 89.7% of accumulated Aion thread CPU in the captured stackshot.
- Ten Aion CPU-resource reports from 7–11 July sampled the Liminal connection
  path.
- Post-relaunch logs showed all 11 workers registered, followed by almost seven
  minutes with no useful server activity while the four-core burn continued.
- Heartbeats do not explain it: Liminal's `Ping` handling is reactive, and Aion
  dispatches use a zero heartbeat window.

The WindowServer watchdog termination had a separate immediate call-path. The
continuous Aion load made the host much less resilient, but this note does not
claim that Liminal directly crashed WindowServer.

## Critical: idle sockets are permanent work

The current behavior is explicit in source:

- `ConnectionProcess` documents a "no-sleep `Continue` discipline" and returns
  `Continue` at the end of every normal slice
  (`crates/liminal-server/src/server/connection/process.rs:86-125`).
- A nonblocking read returning `WouldBlock` deliberately does not park and flows
  back to `Continue` (`process.rs:136-158`).
- The connection scheduler has a fixed four normal workers
  (`connection/supervisor.rs:22,352-380`).
- The subscription delivery pump relies on the same loop: its module comment
  says no wakeup plumbing is needed because the connection already runs every
  slice (`connection/delivery.rs:1-12`).

Commit `ff8d863` replaced a 10 ms sleep with permanent requeue to stop one
connection from blocking a scheduler thread. Commit `bb81724` subsequently
made that busy loop the delivery pump. The repository ledger already calls the
process "busy-polls by design" (`docs/stack-review/liminal-ledger.md:170-178`).

This explains the observed shape exactly: 11 idle runnable processes are
shared fairly across four workers, so all four remain saturated.

## Required readiness contract

A sleep, periodic poll or lower worker count is not a fix. Use one
supervisor-wide readiness reactor for all connection sockets:

1. Register readable, HUP and error interest for each live socket. Register
   writable interest only while outbound bytes remain after `WouldBlock`.
2. Drain bounded socket and internal work. Return `Continue` only when known
   in-memory work remains after the slice budget is exhausted.
3. Before returning `NativeOutcome::Wait`, arm/rearm readiness and perform the
   final nonblocking probe. This closes the readiness-before-park window. If
   that probe observes work, drain it or remain runnable; never return `Wait`
   while known work is pending.
4. Deliver readiness to the owning Beamr PID with a durable atom/mailbox marker,
   not a bare edge wake. Beamr 0.13's `enqueue_atom_message` plus its
   register-then-mailbox-recheck park path preserve the execute-to-wait race.
5. Key registrations by PID plus connection generation. Deregister on every
   termination path and discard stale events after PID/socket reuse.
6. Coalesced or duplicate readiness markers must be harmless.

Two additional wake sources must participate:

- `OutboundWriter::drain` currently returns the same `Ok(())` for an empty
  buffer and a blocked non-empty buffer (`connection/outbound.rs:156-192`). It
  must report those states separately so writable interest is armed only when
  required.
- A parked subscriber needs notification when its channel inbox becomes
  non-empty. Install that notifier before the final empty recheck so publish
  cannot race registration. The old assumption that the connection will run
  another slice is precisely the defect being removed.

Existing control traffic already queues state and sends a wake atom through the
supervisor (`connection/supervisor.rs:430-466`); preserve that path.

## High: worker-only Aion constructs unused runtimes

Aion passes zero channels, no cluster and no persistence to
`LiminalConnectionServices::from_config`. Reserved liveness, capability and
transcript publishes are consumed by its notifier before ordinary channel
dispatch. Nevertheless `from_config` unconditionally constructs:

- An eight-shard Haematite database
  (`crates/liminal-server/src/server/connection/services.rs:227-230,500-527`).
- A channel supervisor (`services.rs:243-247`), including a separate Beamr
  scheduler (`crates/liminal/src/channel/supervisor.rs:152-172`).
- A conversation supervisor (`services.rs:288-299`), including another Beamr
  scheduler (`crates/liminal/src/conversation/actor.rs:218-238`).
- The actually required four-thread connection supervisor.

The fix should be an explicit capability-scoped construction path, not special
case checks scattered through the full service implementation. Provide a
notifier/worker-front-door `ConnectionServices` implementation (or equivalent
constructor) that supports registration, correlated push/reply and
notifier-consumed reserved publishes without constructing channels,
conversations, dedup or Haematite. It must reject unsupported ordinary
publish/subscribe/conversation frames explicitly.

Full server mode should keep its services, preferably with injected/shared
scheduler and store ownership made explicit.

## Moderate: ephemeral durability is not lifecycle-owned

When persistence is absent, `build_durable_store` creates a plain path named
`liminal-durability-<pid>-<counter>` below the system temp directory
(`services.rs:512-527`). No `TempDir` guard or cleanup owner survives with the
store, so the directory persists after normal process exit.

This host currently has 276 such directories. They total only about 1.1 MiB
because most were unused, so they are not the 3.5 GiB Aion database problem.
They are still evidence that "ephemeral" has no enforced lifecycle, and a
written instance can accumulate real data.

Use an owned temporary-directory guard for truly ephemeral stores and verify
cleanup on normal drop, partial startup failure and repeated start/stop. If a
process-lifetime store must survive a component restart, name and manage that
policy explicitly rather than leaking it accidentally.

## Acceptance gates

- Eleven connected idle sockets settle in `Wait`; test-only slice counters stop
  increasing without an event.
- A parked connection wakes for readable data, returns `Pong`, drains to
  `WouldBlock`, rearms and parks again.
- `push_to_connection` wakes a parked process and completes its correlated
  reply.
- Publishing wakes a parked subscriber without periodic polling.
- A full send buffer parks on writable readiness and later flushes the exact
  remaining bytes in order.
- EOF/HUP wakes, deregisters and removes the connection.
- Deterministic barrier tests inject readiness, control and subscription events
  before wait registration and immediately after it; none are lost.
- Duplicate readiness markers do not duplicate frame application.
- Connect/write/disconnect churn with more connections than scheduler workers
  remains quiescent between events.
- Worker-front-door construction creates no channel scheduler, conversation
  scheduler, Haematite scheduler/store or temp directory.
- Unsupported worker-front-door frames fail explicitly.
- Requested ephemeral storage is removed on normal shutdown and startup
  rollback.
- A macOS soak with 11 idle workers returns CPU to host baseline; establish the
  operational threshold with Tom rather than inventing a hidden code limit.

Do not revert Beamr's FIFO repair, add polling sleeps, or merely reduce the four
connection workers. Those change fairness or cap the visible burn while leaving
the permanent-runnable invariant intact.
