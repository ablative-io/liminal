---
name: meridian-idle-delivery-gap
description: Meridian bridge does not deliver messages into an idle Claude session — silent seat ≠ seat that saw the work; ground-truth check before reading silence as a stall
metadata: 
  node_type: memory
  type: project
  originSessionId: 6bfc46aa-2bfa-43f8-a284-0ba8a3713539
  modified: 2026-07-22T10:08:37.997Z
---

Found 2026-07-22 (Apollo liveness check at Waffles' request): the Meridian bridge does NOT deliver messages into an idle Claude session — Apollo's session process was alive but turn-idle for 4+ hours, and Waffles' dispatch + liveness DM never appeared in his transcript at all. The messages were never received, not declined.

**Why:** channel messages inject only into active turns; an idle session at the prompt gets nothing and fires no wake.

**How to apply:** (1) Never read seat silence as "saw the work and stalled" — run a ground-truth check first (process alive? transcript mtime/growth? last entry types — unanswered user-side entries with no assistant turn = idle-never-received; working-dir/git activity?). (2) Expect my own idle periods to have the same gap — check ts fields on arrival, answer status checks promptly (see [[liminal-repo-state]] bridge-lag note). (3) Defect class "idle-session delivery / wake-on-message" is on Waffles' board as infrastructure work.
