---
name: usage-doctrine-coordinate-not-execute
description: "Tom's binding usage doctrine (2026-07-13): my session COORDINATES, workers EXECUTE — dispatch heavy legs to Sol/norn workers, fan-outs route through Waffles' seat"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 5b70322e-e7a9-451c-91ca-a3dfa7b05bd9
---

Tom's usage doctrine, 2026-07-13 ~01:21Z, full text relayed verbatim by Waffles 2026-07-14. Binding, permanent (lane assignments around it were superseded by the stand-by order; the doctrine was not).

**Your session COORDINATES, workers EXECUTE.** Claude-side context is the scarce resource and hands-on work in your own session eats it — the heavy legs of every assignment route to norn/Sol workers and workflow dispatches, not my own hands.

**Why:** context is the seat's scarce resource; spending it on keystrokes crowds out the judgment work only the seat can do.

**How to apply:**
- Bulk reading → a Sol research/scout session with an evidence-pack schema.
- Drafting → Sol drafts from MY outline and evidence pack; my pass is the judgment layer (shape, laws, tear-readiness), not the typing.
- Implementation → norn dev workers as always; my seat reviews.
- **Tripwire: reading more than a few files by hand, or writing more than an outline by hand → stop and dispatch it.**
- Coordination beyond ONE worker — fan-outs, multi-angle sweeps, adversarial verification panels — routes through WAFFLES' seat: send him the shape, he runs it as a workflow. (Annabel has separately told me dynamic workflows/fan-out in-session are fine — reconcile per task; Waffles-routed is the doctrine's default for multi-worker jobs.)
- Report session IDs and envelopes in every handoff — delegated work stays auditable.

The craft stays mine — the keystrokes don't.

**MODEL-TIER CONSTRAINT (Tom's word, relayed by Waffles on #stack-devs 2026-07-18 03:27Z; binds all seats' dispatches from here):** be sensible with the top tier — dev/build workers run on **Opus-class or below**; reserve the **top tier (Fable)** for judgment folds and anything genuinely **phase-decisive**. My norn dev/build workers already run gpt-5.6-sol (not Fable) and my judgment passes sit at my own seat, so I'm compliant by default — but when I next spawn workers (norn or Agent-tool with a model override), keep build/mechanical legs on the cheaper tier and only reach for the top tier on a decisive fold. Same principle as the context-scarcity doctrine above: spend the expensive resource only where the seat's judgment actually lands.

**DELEGATION-ECONOMY RULE (Annabel, 2026-07-18; text received 04:10Z via Artemis's ATTRIBUTED VERBATIM RELAY on #stack-devs — Annabel's direct confirmation still PENDING, asked in-session):** Annabel's two in-session messages to Artemis's seat, quoted verbatim by him: (1) "Anything you can do utilising norn Sub agents using the norn-skill Would be appreciated from this point forward. You don't have to stop anything you're doing now but it'll just help us stretch out the usage a little bit further." (2) "You can also use as much opus for sub agents as you like We just need to make sure that we're having at least GPT 5.6 sol Or fable review". Operational reading (Artemis's, adopted provisionally): delegation defaults to Norn workers to stretch Claude usage; Opus for Claude subagents unrestricted; HARD floor = every piece of delegated work gets at least a GPT-5.6 Sol or Fable review before it counts, declaration names which. Composes with Tom's model-tier rule above — my norn Sol dev workers + Fable-seat judgment already satisfy both. Waffles ruled (04:10Z): dispatching on a verbatim attributed relay is NOT inference; note "Annabel's confirmation pending" in declarations until she confirms. **RIDER CLOSED (Annabel in-session, 2026-07-18 ~05:40Z, her words verbatim): "Yeah, so I don't want anything deferred just to be clear on that but yeah, just whatever's gonna give us the absolute best possible outcome."** Operator reading, banked: (1) NO DEFERRALS reaffirmed DIRECTLY by the operator — no longer only a relayed law; (2) best-possible-outcome-first — the relayed rule stands as the mechanical default (norn workers to stretch usage; Opus unrestricted for subagents; hard floor = at least one Sol or Fable review per delegated piece, named per declaration), and where economy and quality tension, QUALITY WINS. Declarations no longer carry the "Annabel-confirmation-pending" rider.

**HOUSE STANDARD (Waffles ruled 2026-07-18 03:53Z, binds every future lane) — PRE-REVIEW DECIDED-TEXT vs THE BYTES between fold and build:** Artemis's Sol pre-review (03:52Z) caught three amendment-grade defects in torn+folded EMB briefs that brief-time review AND the tear both missed — anchors were accurate, but two acceptance criteria were unimplementable at the pin and one doc claim was false. THE GAP: brief-time review + tear validate that anchors are accurate, but neither re-derives whether the acceptance criteria are *implementable AT those anchors*. THE LENS: after a brief is torn/folded and BEFORE dispatching the build worker, run one cheap INDEPENDENT pass (a Sol review session) checking every acceptance criterion + file:line claim against the actual bytes at the base commit. Cost ≈ one Sol session vs a rebuild-later. Any amendment it finds lands IN THE BRIEF ARTIFACTS (committed), never prompt-only to the worker. Binding on my torn-brief lanes (e.g. the post-demo SDK-completion brief, LAW-1 impl briefs). Maps onto my gate-integrity / [[no-silent-tradeoffs-rules]] discipline.

Related: [[no-silent-tradeoffs-rules]], [[liminal-repo-state]]

**STANDING DOCTRINE (Waffles ratified 2026-07-18 15:04Z, msg fd4776ef — binds EVERY future leg, all arcs) — THE IRON COMMIT DISCIPLINE:** every build/dev leg commits and pushes EVERY coherent green sub-unit IMMEDIATELY; never more than ONE sub-unit uncommitted at any moment; leg scope capped at 1-2 slices (never a whole multi-slice plan in one session). Earned by Unit 2 leg 1 (died of context exhaustion at ~80min with ZERO commits, ~2k coherent lines orphaned); same rule Waffles' Grunk legs run (one commit per requirement, push on completion) — "a leg that can die without losing more than one sub-unit is the only kind of leg that belongs in an overnight schedule." Corollary for SALVAGE of inherited-uncommitted work: no commit-time provenance ⇒ STRICT ownership — every inherited line survives verification against the brief at the bytes or is deleted; compiling ≠ conforming; the tear treats salvaged slices with exactly that suspicion. Smaller legs ⇒ more, smaller declarations: slice-level checkpoints to the coordinator are welcome (he spot-checks rather than waiting for the full table).
