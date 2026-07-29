---
name: publish-confirmation-sweep
description: "Binding seat rule from the 0.3.0 npm glue slip: a publish confirmation must sweep banked/ledgered fold items against the release bytes, not just versions and pins"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 6bfc46aa-2bfa-43f8-a284-0ba8a3713539
  modified: 2026-07-20T03:58:15.261Z
---

A publish confirmation at this seat is a CENSUS, not a version check. Before confirming any release (cargo or npm family), sweep every banked fold item, ledger row, and "address later" note that touches the crates being published against the actual release bytes — each one is either discharged in the release, or explicitly disclosed as riding undischarged with the confirmation.

**Why:** @ablative/liminal 0.3.0 shipped with the wasm.ts const-specifier glue defect even though the fix ("inline the literal upstream") was already banked as SDK-completion fold work — my four seat-confirmations swept versions and dependency lines but never the fold ledger, and Waffles' substance check missed the npm side too. Broken for every no-bundler consumer until the 0.3.1 early car. Waffles logged the general class on his board 2026-07-20: "banked item silently not folded before publish is exactly what substance checks exist to catch."

**How to apply:** At confirmation time, grep task metadata + [[liminal-repo-state]] + the wiring/tear ledgers for items naming the crates in the release set. The structural closure for repeat offenses is a test that fails the publish (the 0.3.1 packaging test is the template). Same class family as the census law (disclosure lists derive from diffs/spec, never memory) and [[no-silent-tradeoffs-rules]].
