# Liminal

Conversation-native messaging built on [beamr](https://github.com/ablative-io/beamr) and [haematite](https://github.com/ablative-io/haematite).

## What it is

A messaging system where conversations — not individual messages — are the fundamental unit. Every channel is a lightweight BEAM process, every subscription is a monitor, and backpressure is supervision rather than configuration. Built for AI-agent coordination, where messages need to survive crashes and never get lost.

## Status

**v0.5.0** (`liminal-rs` / `liminal-server` / `liminal-sdk` 0.5.0; `liminal-protocol` 0.3.2). Core messaging, channels, schema validation, durable mailboxes, and the wire protocol are implemented and tested. Backpressure and predicate routing exist as tested subsystems not yet wired into the delivery path; the Aion integration is a set of protocol-level seams (worker registration, push dispatch, observability drain) consumed by the external `aion` crates. See `CHANGELOG.md` for the release record and `docs/stack-review/` for the honest state map.

## Install

The crate is published on crates.io as **`liminal-rs`** (the bare `liminal` name is taken by an unrelated crate), but it is imported as `liminal`. Use the package alias:

```toml
[dependencies]
liminal = { package = "liminal-rs", version = "0.5.0" }
```

```rust
use liminal::channel::ChannelHandle;
use liminal::conversation::ConversationHandle;
```

## Crates

| Crate (crates.io) | `use` as | Version | License | Description |
|-------------------|----------|---------|---------|-------------|
| `liminal-rs` | `liminal` | 0.5.0 | AGPL-3.0-only | Core library — channels, conversations, durability, routing, backpressure, protocol |
| `liminal-protocol` | `liminal_protocol` | 0.3.2 | Apache-2.0 | Shared wire and lifecycle types (no_std-capable) |
| `liminal-sdk` | `liminal_sdk` | 0.5.0 | Apache-2.0 | Application-facing SDK traits for building liminal clients (no_std-capable) |
| `liminal-server` | `liminal_server` | 0.5.0 | AGPL-3.0-only | Standalone server for the liminal bus |

## SDKs

- **TypeScript** (`sdks/liminal-ts/`) — browser and Node.js client
- **Gleam** (`sdks/liminal-gleam/`) — native BEAM client

## Features

- **Conversation-native** — conversations have participants, lifecycles, and crash recovery; not just pipes.
- **BEAM-native channels** — every channel is a beamr process with crash isolation.
- **Durable mailboxes** — messages backed by haematite's content-addressed storage.
- **Backpressure** — slow consumers throttle producers; Accept / Defer / Reject are protocol primitives.
- **Schema validation** — messages validated against JSON Schema before delivery.
- **Causal ordering & tracing** — causal metadata and trace context carried on the envelope.
- **Aion integration** — workflow steps publish, subscribe, and coordinate through conversations.

## Architecture

```
crates/liminal/src/
├── channel/       — pub/sub channels with schemas and supervision
├── conversation/  — first-class conversations with participants
├── durability/    — crash-safe message persistence (haematite-backed)
├── routing/       — message routing
├── pressure/      — backpressure / flow control
├── protocol/      — wire protocol
├── causal/        — causal ordering metadata
├── metrics/       — metrics registry
└── tracing/       — trace context propagation
```

## Requirements

- Rust 1.85+ (edition 2024)
- Depends on beamr 0.16.1 (with the `readiness` feature) and haematite 0.7.0

## License

Liminal is **split-licensed** — the client surface is Apache-2.0, the servers are
AGPL-3.0-only:

- **Apache-2.0** — `liminal-protocol`, `liminal-sdk`, and the TypeScript SDK
  `@ablative/liminal` with its WASM protocol bridge. These link into *your*
  application without copyleft obligations. Full text ships in each package
  (`LICENSE-APACHE` in the crates, `LICENSE` in the npm package).
- **AGPL-3.0-only** — `liminal-rs` (the `crates/liminal` core) and
  `liminal-server`. Full text at [`LICENSE`](LICENSE).

Commercial licensing for the server components is available from Ablative —
contact <tom@ablative.com.au>. The full rule, and the reasoning behind it, is in
[`LICENSING.md`](LICENSING.md).
