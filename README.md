# Liminal

Conversation-native messaging built on [beamr](https://github.com/ablative-io/beamr) and [haematite](https://github.com/ablative-io/haematite).

## What it is

A messaging system where conversations — not individual messages — are the fundamental unit. Every channel is a lightweight BEAM process, every subscription is a monitor, and backpressure is supervision rather than configuration. Built for AI-agent coordination, where messages need to survive crashes and never get lost.

## Status

**v0.2.0.** Core messaging, channels, backpressure, schema validation, durable mailboxes, routing, and Aion integration are implemented and tested.

## Install

The crate is published on crates.io as **`liminal-rs`** (the bare `liminal` name is taken by an unrelated crate), but it is imported as `liminal`. Use the package alias:

```toml
[dependencies]
liminal = { package = "liminal-rs", version = "0.2.0" }
```

```rust
use liminal::channel::ChannelHandle;
use liminal::conversation::ConversationHandle;
```

## Crates

| Crate (crates.io) | `use` as | Description |
|-------------------|----------|-------------|
| `liminal-rs` | `liminal` | Core library — channels, conversations, durability, routing, backpressure, protocol |
| `liminal-sdk` | `liminal_sdk` | Application-facing SDK traits for building liminal clients (no_std-capable) |
| `liminal-server` | `liminal_server` | Standalone server for the liminal bus |

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
├── tracing/       — trace context propagation
└── aion/          — Aion workflow-engine integration
```

## Requirements

- Rust 1.85+
- Depends on beamr 0.11.0 and haematite 0.3.0

## License

AGPL-3.0-only
