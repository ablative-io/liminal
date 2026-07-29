---
name: worker-liveness-deadlines
description: House-wide standing dispatch pattern (Waffles-banked 2026-07-22) — completion monitoring alone is insufficient; every dispatched worker gets an output-liveness deadline watchdog
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 6bfc46aa-2bfa-43f8-a284-0ba8a3713539
  modified: 2026-07-23T00:34:12.930Z
---

Minted after death mode #13 (W2 leg 1f, 2026-07-21/22): a worker whose stream goes silent while the process stays alive is invisible to completion-based monitoring — the leg-1f worker hung for ~7 hours before a Waffles status check surfaced it. He hit the same family three times the same day on his side (builders that backgrounded gate batteries and stopped producing, alive but never finishing). Four instances across two ecosystems in one day = standing pattern, not a fluke. Banked house-wide at the coordination seat.

**Why:** provider deaths announce themselves (`response.failed`, exits); silent stalls don't. A hang costs wall-clock equal to whatever notices it — a watchdog makes that minutes, a human status check makes it hours.

**How to apply:** alongside EVERY worker dispatch, launch a watchdog loop (10-minute poll) that checks liveness mtimes and flags >30 minutes of silence while the process is still alive; on flag → GROUND-TRUTH FIRST (teed worktree logs, git status, ps for cargo — the logs are authoritative), and only then kill/salvage per the salvage rule ([[liminal-repo-state]] — tracked diff AND untracked files separately), reset, relaunch on the unchanged boundary. Related: the fine-granularity commit mandate makes the killed worker's cost near-zero.

Third witnessed mode (2026-07-23): a subagent's transcript can be LOST after context compaction — SendMessage resume fails with "No transcript found for agent ID". Salvage = fresh dispatch with a fully self-contained mandate rebuilt from ground truth (branch state + the worker's last report); keep worker reports comprehensive precisely so this is cheap.

Two witnessed false-positive/hygiene modes (2026-07-23): (1) a RESUMED subagent's task-output JSONL can go stale for 35+ min while the worker actively writes teed logs — watch the UNION of the worktree logs dir mtime and the task output file, not the JSONL alone; (2) watchdogs outlive their workers unless explicitly stopped at fold time — stopping the watchdog is part of closing a lane, else stale loops fire alarms for finished work (three fired at once for Annabel, two of them stale).
