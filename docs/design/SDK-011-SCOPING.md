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

| half | change | semver | ships as |
|---|---|---|---|
| **additive** | configurable setup deadline (builder/config, default stays the ratified 5 s) | minor | **0.5.2** — caret consumers float to it |
| **breaking** | `#[non_exhaustive]` on `SdkError` **+** the typed timeout variant | **major** | **0.6.0** — every consumer re-pins |

### ★ 3.1 THE TWO BREAKING ITEMS MUST RIDE THE SAME BUMP

Both are breaking. **If the variant lands without the attribute, the bump is
spent and the attribute is still absent — so the next variant breaks again, and
the one after that.** One bump buys the variant *and* makes every future variant
additive permanently. **This is the cheapest moment that window will ever
close.**

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
