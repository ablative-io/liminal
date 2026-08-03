# The `#[non_exhaustive]` refusal — 268 public enums stay exhaustive

**Decision: REFUSED. Ruled by Cally Ray, 2026-08-03T07:43:35Z (DM message
`53c5576a-5e19-4447-b070-8e65696a5207`). Recorded by Hermes Crumpet at the
liminal-protocol 0.3.2 → 0.4.0 cut.**

`scripts/exhaustiveness-gate.py` accepts the attribute or a written refusal, and
accepts silence from neither. This file is the refusal. It is not a deferral and
it carries no expiry: the ride this cut offered was inspected and deliberately
not taken.

## What was decided

The 268 public enums in `crates/liminal-protocol/src` do **not** get
`#[non_exhaustive]`. They stay exhaustive, and every downstream consumer keeps
being forced to name every variant it handles.

## Why this cut is the one that asked

The trigger is §3.3 of `docs/design/F8-MARKER-POISON.md`: the
`BindingFateMeasurementError::OwnerTransition` variant gained a payload, so the
protocol's own refusal cause now travels **by type** rather than collapsing into
a bare marker. The variant and its construction site live in
`crates/liminal-protocol/src/lifecycle/operations/binding_fate.rs` — at the tree
`5f1ae7d` where §3.3 landed, the declaration is `:155` and the `map_err` that
mints it is `:379`. Adding a field to an enum variant is a major change under
the cargo semver reference, and for a `0.x` crate the breaking boundary is the
minor, so the crate moves to a new breaking series. The attribute can ride only
a breaking bump. This is one, which is why the question came due here rather
than on somebody's schedule.

## The three grounds

**(a) The estate's designs make causes travel by type, and decisions turn on
structure.** Park-versus-fatal is read off the shape of a value, not off a
formatted string. An enum that forces downstream `_ =>` arms un-makes exactly
the discipline that caught the F8 boot-identity drift. A wildcard arm sitting in
a fatal-policy consumer is a silent policy change waiting for its first new
variant — it will compile, it will run, and it will route a cause nobody
considered into whichever branch the wildcard happened to land in.

**(b) The estate bar is zero `_ =>`.** `#[non_exhaustive]` would force on every
consumer precisely what we forbid in ourselves. A rule we enforce internally and
export the inverse of is not a rule.

**(c) The proof case is the `Precedence` symmetry trap** named in the Sol
landing review. Measurement-side `OwnerTransition(Precedence)` must latch fatal,
while admission-side `Precedence` must park. Same spelling, opposite
obligations, and the distinction is only legible where the match is exhaustive.
Under `#[non_exhaustive]` a consumer's wildcard absorbs one of those two poles
without a diagnostic, and the trap closes quietly.

## The price, named

Version majors are the honest price of new refusal classes. Each future cause
that needs to travel by type will cost a breaking bump, and consumers will be
told rather than surprised. That is the trade being bought here on purpose: the
break is formal and the compiler makes it loud, which is strictly better than a
wildcard that makes it silent.

## The census this was decided against

`docs/design/F8-MARKER-POISON.md` §4.2 measured **268 `pub enum` declarations
carrying 0 `#[non_exhaustive]`** at `09cfa49`, by running the gate itself. The
gate re-measured the same figure at this cut — 268 and 0, with its positive
control finding 336 `pub struct`, its negative control returning zero for an
impossible token, and its 5/5 leading-token partition intact. The debt is an
upper bound on exposed surface: module visibility and re-export reachability
are not resolved by that number, and it is quoted here as the bound it is.

## Scope

This refusal covers the liminal-protocol public enums at the 0.4.0 cut. It is a
judged pass, not a blanket one. A future enum whose exhaustive matching is not
load-bearing may be introduced with the attribute **at birth**, which is the
cargo reference's own mitigation and costs nothing; what is refused here is
retrofitting it onto 268 enums whose exhaustiveness is doing work.
