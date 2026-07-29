---
name: ablative-stack-layout
description: The six ablative-stack layers under ~/Developer/ablative/stack and how they compose
metadata: 
  node_type: memory
  type: project
  originSessionId: 5b70322e-e7a9-451c-91ca-a3dfa7b05bd9
  modified: 2026-07-22T18:25:52.618Z
---

The ablative stack lives at /Users/annabel/Developer/ablative/stack/ (six repos): **beamr** (BEAM VM in Rust, repo at 0.12.0 while liminal pins 0.11.0; liminal consumes it as scheduler + native-process API + links/monitors/ExitReason + distribution::pg, NOT as a Gleam interpreter), **haematite** (prolly-tree CAS engine 0.4.0; liminal uses ONLY the EventStore surface: append/read_from/cas/read_value/scan — never fork/merge/KV), **liminal** (messaging bus, middle layer), **aion** (durable workflow engine 0.8.0, 17 crates; consumes liminal as optional push-based activity-dispatch transport behind `liminal-transport` feature; aion-worker/src/runtime/liminal.rs is the worker half), **norn** (agent runtime; deliberately does NOT depend on aion/liminal — aion drives norn via aion-integration-norn over JSON-RPC), **frame** (composition layer, v0.1.0-dev scaffold only; frame-conv will consume liminal).

Maturity: beamr ~1,700 tests, haematite ~650, aion ~2,500 (single-node), norn ~3,750, frame ~0.

Liminal repo conventions: CONVENTIONS.toml (norn post-mutation checks) advises ≤500 LOC per .rs file (200 for mod/lib/main), no #[allow]/#[expect], no TODO markers. Workspace lints deny unwrap/expect/panic + pedantic clippy; `module_name_repetitions` allowed as documented workspace policy. Crate publishes as `liminal-rs`, imported as `liminal`. Design process: docs/design/<cluster>/ briefs (yggdrasil/orchestrated-dev workflow), dispatch waves in docs/DISPATCH-WAVES.md.

Team coordination (as of 2026-07-07): agents per domain in the Meridian channel `stack-devs` — Waffles the Terrible (coordinator/tear seat), Artemis Peach (beamr), Apollo Biscuit (haematite), Vesper Lynd (aion/norn-infra); I hold liminal as Hermes Crumpet. Handoff + sequencing live in liminal `docs/stack-review/` (AGENT-HANDOFF.md: beamr 0.12 pin bump first, A2 schema wiring warm-up, then A1 backpressure with design-doc-before-code; never register beamr's single-slot connection-down hook; C1 first-poll bridge constraint). Task decisions route through Annabel.

Beamr pin law (LIVE, Artemis): ANY liminal beamr-pin bump routes through Artemis' seat to walk the delta BEFORE taking bytes — exercised 2026-07-22 for 0.15.4→0.16.0 (she CLEARED it; the load-bearing exit surface subscribe_exit_events/take_exit_outcome/peek_exit_reason survives 0.16.0 at identical signatures; spawn_link_dirty was NOT removed in 0.16.0 despite the earlier ~0.16.0 estimate — still present in scheduler/spawning.rs). Liminal's uptake profile in her inventory (my correction, her words "better clearance than the one I issued"): **"bytecode-loading, fixed-paths-unreachable by instruction census"** — NOT "no bytecode": both actor tiers run hand-assembled BEAM receive loops (Label/LoopRec/RemoveMessage/CallExt/CallOnly/Wait; channel/actor/beam.rs + conversation twin); sole CallExt resolves ResolvedImportTarget::Native → process_command NIF, never erlang:send/2; zero send/func_info/if_end opcodes assembled. Durable consequence: liminal is the ONE consumer exercising beamr's interpreter receive family (LoopRec/Wait) outside beamr's own tests — any beamr interpreter-receive change has exactly one external witness: liminal's battery. No beamr facility trait impls in liminal; no beamr-wasm in the workspace. (Versions elsewhere in this memory are 2026-07-07-era and stale: haematite now 0.6.x, liminal 0.3.x published.)

Related: [[liminal-repo-state]]
