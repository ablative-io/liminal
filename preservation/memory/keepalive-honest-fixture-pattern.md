---
name: keepalive-honest-fixture-pattern
description: "Waffles-flagged house pattern from W2 brief §5.1 — idle fixtures must prove the unrelated timer/transport counters GROW while the unit-under-test counters stay flat, so the test cannot pass by hiding the timer"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 6bfc46aa-2bfa-43f8-a284-0ba8a3713539
  modified: 2026-07-21T12:37:15.240Z
---

From the W2 brief tear (2026-07-21, Waffles' tear record, "I want that pattern remembered"): when a design claims an idle-cost bound in the presence of an unavoidable unrelated periodic mechanism (e.g. WebSocket keepalive pings), the pinning fixture must assert BOTH sides — the unrelated counters (transport slices, ping writes) VISIBLY GROW during the window, AND the unit-under-test counters stay flat. A fixture that only asserts flatness can pass by accidentally disabling the timer; forcing the growth assertion proves the idle window was real.

**Why:** an idle-bound test that passes with the noisy neighbor turned off proves nothing; requiring the neighbor's counters to grow makes the fixture self-authenticating.

**How to apply:** in any idle-cost pin (see [[no-silent-tradeoffs-rules]]), pair every "X stays flat" assertion with a "the surrounding activity Y grew" assertion derived from the same observed window. Related: W2 brief §5.1; the W1b OperationFlush-barrier synchronization precedent (real barriers, no settle windows).
