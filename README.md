# Liminal

Conversation-native messaging built on [beamr](https://github.com/tomWhiting/beamr) and [haematite](https://github.com/ablative-io/haematite).

## What it is

A messaging system where conversations — not messages — are the fundamental unit. Every channel is a lightweight BEAM process. Every subscription is a monitor. Backpressure is supervision, not configuration. Built for AI agent coordination where messages need to survive crashes and never get lost.

## Status

**v0.2.0** — Core messaging, channels, back pressure, schema validation, and Aion integration are implemented and tested. Conversation replay from durable state is in development.

## Quick start

Add liminal to your `Cargo.toml`:

```toml
[dependencies]
liminal = "0.2.0"
```

## Crates

| Crate | Description |
|-------|-------------|
| `liminal` | Core library — channels, conversations, durability, routing, back pressure |
| `liminal-sdk` | Rust SDK for building liminal clients |
| `liminal-server` | Server with cluster support |

## SDKs

- **TypeScript** (`sdks/liminal-ts/`) — Browser and Node.js client
- **Gleam** (`sdks/liminal-gleam/`) — Native BEAM client

## Features

- **Conversation-native** — Conversations have participants, lifecycles, and crash recovery. Not just pipes.
- **BEAM-native channels** — Every channel is a beamr process with crash isolation.
- **Durable mailboxes** — Messages backed by haematite's content-addressed storage.
- **Back pressure** — Slow consumers throttle producers. Accept, Defer, Reject are protocol primitives.
- **Schema validation** — Messages validated against JSON schemas before delivery.
- **Aion integration** — Workflow steps publish, subscribe, and coordinate through conversations.

## Architecture

```
liminal/
├── channels/       — Pub/sub with schemas and supervision
├── conversations/  — First-class conversations with participants
├── durability/     — Crash-safe message persistence
├── routing/        — Message routing logic
├── backpressure/   — Flow control
├── protocol/       — Wire protocol
├── aion/           — Workflow engine integration
└── cluster/        — Multi-node support
```

## Requirements

- Rust 1.85+
- Depends on beamr 0.11.0 and haematite 0.1.0

## License

AGPL-3.0
