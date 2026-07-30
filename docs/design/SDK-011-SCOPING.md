# SDK-011 — setup-deadline API: scoping before build

**Status:** scoping complete, build BLOCKED on two answers from frame (§5).
**Task:** #30. **Assigned:** Cally, 2026-07-30, as the one open item gated on nothing.
**Measured at:** `liminal` main `8ce8e3c`, rustc/cargo 1.97.1.

---

## 1. THE FACT THAT DECIDES THE SHAPE

```
crates/liminal-sdk/src/error.rs:9    pub enum SdkError    NO #[non_exhaustive]
```

Cited from the toolchain's own reference — `$(rustc --print sysroot)/share/doc/rust/html/cargo/reference/semver.html`:

> **Major: adding new enum variants (without `non_exhaustive`)**

⇒ **the typed timeout variant is a BREAKING change**, taking `liminal-sdk`
0.5.1 → **0.6.0**. For a `0.x` crate the **MINOR is the breaking boundary**, so
`^0.5.1` never resolves to 0.6.0.

### 1.1 This inverts the "land it before the wave" argument

The assignment reasoned that an API frame wants is cheapest to land *before* the
crates that consume it publish irreversibly. **That holds for additive work and
fails for breaking work.** If frame's wave one publishes against `liminal-sdk`
0.5.1 as staged, **SDK-011's breaking half is invisible to it regardless of when
it lands** — frame would need an explicit re-pin, i.e. a further liminal release
*and* a second eleven-crate re-pin. Exactly the cost the early landing was
meant to avoid.

### 1.2 Task #34's window is still open, measured

Task #34 is marked **completed**, but the attribute is **absent from `SdkError`
today**. Whatever #34 closed, it was not this. **Flagged rather than reopened —
that is Cally's call, not mine.**

---

## 2. WHAT A SETUP TIMEOUT LOOKS LIKE TO A CALLER TODAY

`crates/liminal-sdk/src/remote/tcp/push_client.rs:747`:

```rust
return Err(SdkError::Connection {
    description: "push connection timed out waiting for a control-frame reply".to_string(),
});
```

**A caller can distinguish a setup timeout from a refused connection only by
matching on that string.** This is the actual defect SDK-011 exists to fix, and
it is the same shape as haematite #56 (Apollo's finding): **the type is
destroyed at a catch-all, leaving a stringly-typed path.**

Current deadline: `crates/liminal-sdk/src/remote.rs:64` —
`pub(crate) const SETUP_TIMEOUT = 5s`, consumed at six call sites across the TCP
and WebSocket transports, and pinned by name in three tests.

---

## 3. THE SPLIT

> ### ⚠️ 3.0 CORRECTION — THE FIRST DRAFT OF THIS SECTION WAS WRONG
>
> §3 originally read *"configurable setup deadline (builder/**config**…)"* and
> called it additive. **Putting the deadline on `RemoteConfig` is BREAKING**, so
> that draft had both halves major and no additive half at all.
>
> `crates/liminal-sdk/src/remote.rs:93` — `RemoteConfig` has **four public
> fields and no private field**, no `#[non_exhaustive]`. Semver reference:
> **"Major: adding a public field when no private field exists."** Adding a
> *private* field is equally breaking here, because the rule that makes it minor
> requires **at least one private field to already exist**. Struct literals break
> either way. `ConnectionPoolConfig` (`connection/pool.rs:16`) has the same
> all-public shape and the same constraint.
>
> **⇒ THE API SHAPE DECIDES THE SEMVER CLASS. That turns question (c) from a
> matter of taste into a release-planning question.**
>
> - deadline as an **argument at a new entry point** → **adding a public fn is
>   MINOR** → genuinely additive, ships 0.5.2.
> - deadline as a **field on `RemoteConfig`** → **MAJOR** → no additive half
>   exists; the whole of SDK-011 joins 0.6.0.

| half | change | semver | ships as |
|---|---|---|---|
| **additive** | setup deadline as an argument on a **new entry point**; every existing type keeps its construction shape; default stays the ratified 5 s | minor | **0.5.2** — caret consumers float to it |
| **breaking** | `#[non_exhaustive]` on `SdkError` **+** the typed timeout variant | **major** | **0.6.0** — every consumer re-pins |

### 3.2 AND FRAME NOT BREAKING IS A STATE, NOT A LICENCE

At the venue, `crates/frame-conv/src/handle/attach.rs:33` builds config via
**`RemoteConfig::new(...)`, not a struct literal** — so a new field would not
break *frame* in practice. **That is not permission.** It is semver-major for
every consumer we do not control, and "the one consumer I checked happens to be
safe" is exactly the reasoning behind a claim I withdrew last week: **a true
observation with an invented rule is a false claim wearing evidence.**

### ★ 3.1 THE TWO BREAKING ITEMS MUST RIDE THE SAME BUMP

Both are breaking. **If the variant lands without the attribute, the bump is
spent and the attribute is still absent — so the next variant breaks again, and
the one after that.** One bump buys the variant *and* makes every future variant
additive permanently. **This is the cheapest moment that window will ever
close.**

---

## 3.3 🔴 AN INERT KNOB ALREADY EXISTS, AND IT IS IN THIS EXACT AREA

`ConnectionPoolConfig::timeout_millis` (`connection/pool.rs:20`) is
**WRITE-ONLY**. Its only three occurrences in the Rust workspace are its
declaration, its `new()` parameter, and its assignment. **It is never read —
anywhere.** Verified with a positive control (`max_connections`, which *is* read
elsewhere, proving the search can find reads).

⇒ **a caller who sets `timeout_millis` reasonably believes they configured a
timeout, and nothing happens.** No error, no warning, no effect. Same family as
everything else this week: **a mechanism that appears to work and is silent.**

This matters to SDK-011's remit: shipping a *second* timeout knob beside a dead
one would leave two fields, one live and one inert, distinguishable only by
reading the source. **Either wire it or delete it as part of this work** —
deleting a public field is major and would ride 0.6.0; wiring it is a behaviour
change on a field nobody can currently be depending on for effect.

> ⚠️ A search for `timeout_millis` across the tree also returns hits in
> `sdks/liminal-gleam` — **a different field of the same name in another SDK.**
> Namespace confusion again, caught only because the control was in place.

---

## 3.4 🔴 FROZEN STRINGS — AND THE TOKEN IS ALREADY AMBIGUOUS

Athena found that frame string-matches liminal's error **text**. Verified at
frame's own bytes, venue `3def5a1`,
`crates/frame-conv/src/seam.rs:38-52` — `classify_receive_error` branches on
`sdk_error.to_string().contains("timed out")` (with `"os error 35"` and
`"Resource temporarily unavailable"`) to choose **`QuantumElapsed`**, documented
there as *"an elapse is NOT a connection fate — the same connection serves
afterwards."* Everything else falls to `ConnectionFate`.

**Census of `"timed out"` in `liminal-sdk` (non-test):** five messages.

| # | location | path |
|---|---|---|
| 1 | `websocket/connection/exchange.rs:183` | exchange |
| 2 | `websocket/connection/exchange.rs:200` | exchange |
| 3 | `websocket/subscription.rs:363` (`setup_exchange`) | **SETUP** |
| 4 | `tcp/subscription.rs:427` (`read_one_frame`) | **SETUP** |
| 5 | `tcp/push_client.rs:757` (`read_one_frame`) | **SETUP** |

`read_one_frame` is called only from handshake call sites
(`subscription.rs:267,304`, `push_client.rs:583,614`), so 3–5 are setup-only.

### ★ THE DEFECT IS NOT FRAGILITY, IT IS AMBIGUITY

**`"timed out"` is doing discriminating work while carrying two opposite
meanings**: a steady-state quantum elapse (*connection is fine, re-arm*) and a
**setup failure** (*the connection never established*). Frame's classifier reads
both as `QuantumElapsed`. `participant.rs` carries reconnect-permit machinery
(`RemoteReconnectPermitOutcome`, `reconnect_attempt`), which is precisely the
route by which a **post-reconnect setup failure** could surface into the
receive-side classifier.

**I could not resolve reachability by static reading, and this seat cannot run
tests — so this is stated as an OPEN HAZARD, not a confirmed bug.** But the
direction of the risk is one-way: a failed setup misread as a benign elapse
means frame keeps using a connection that never came up.

⇒ **NO REWORDING DISCIPLINE CAN FIX THIS.** "Do not change error text" preserves
the ambiguity exactly as faithfully as it preserves the meaning. **Only the typed
seam removes it** — which is a second, independent argument for putting
`#[non_exhaustive]` with ASK-2's variant.

### CONSTRAINT ON THIS WORK — BINDING

1. **Do not modify strings 1–5.** They are load-bearing in a consumer, with no
   semver protection: a reword shipped as a patch is a silent behaviour change.
2. **No new message may contain `"timed out"`** unless it denotes a genuine
   benign elapse on a path that can reach `Transport`.
3. The additive deadline must change **when** a timeout fires, never **what it
   says**.

---

## 4. WHAT IS SAFE TO BUILD NOW

**The additive half only.** It is correct under either answer in §5 and cannot
become wasted work. **`SdkError` is not to be touched until (a) is answered.**

---

## 5. BLOCKING QUESTIONS — FRAME'S HALF, NOT TO BE INFERRED

**(a) DECIDES EVERYTHING.** Does frame need to distinguish a setup timeout
**programmatically**, or is a configurable duration enough? A knob alone ⇒
SDK-011 is wholly additive, ships as 0.5.2, and **never touches the wave**. A
type branch ⇒ breaking, and it belongs after wave one.

**(b)** *"retry-or-budget"* is a name, not a semantic. **Per-attempt deadline, or
a total budget across retries?** Different APIs; guessing builds the wrong one.

**(c)** What value does frame want, and is it fixed per venue or per connection?
Is 5 s *wrong* for them, or merely un-tunable?

**(d)** **Which frame crate is the consumer?** On the tree visible from this
seat the pinning style is **mixed** — `frame-conv` pins `liminal-sdk` **exactly**
(`=`), while `frame-host` and `frame-view` use caret. **An exact pin does not
float even for a patch**, so if the consumer is `frame-conv`, even the additive
0.5.2 needs an explicit edit in frame and the "no re-pin" benefit evaporates.

> ⚠️ **COORDINATE CAVEAT.** Those pins were read at
> `frame @ docs/pulse-v1-decisions / 4abdb27`, **not** the readiness branch;
> Minerva's re-pin is not on that tree. **The PINNING STYLE is the finding, not
> the versions.** Confirm both against the venue branch.

---

## 6. A GREP TRAP IN THIS EXACT AREA

`grep non_exhaustive crates/liminal-sdk/src/` returns **seven hits**, and **every
one is `finish_non_exhaustive()`** — a `Debug` formatter method, unrelated to the
attribute. **Zero are the attribute.**

A census counting those would report the SDK as already covered and **close the
#34 window on a false green.** Same family as the package-name trap, one layer
down: **the identifier you searched for is not the construct you meant.** Any
mechanical check here must match the attribute form `#[non_exhaustive]` at a
line start, never the bare token.
