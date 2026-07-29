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

## Usage

Build the standalone server and run it against the shipped example configuration:

```bash
cargo build --release -p liminal-server
./target/release/liminal-server --config config/liminal.example.toml
```

`--config` is required: there is no implicit configuration path, and the five
mandatory keys (`listen_address`, `health_listen_address`, `drain_timeout_ms`,
`channels`, `routing_rules`) carry no defaults.
[`config/liminal.example.toml`](config/liminal.example.toml) is a complete,
commented starting point — copy it, edit it, keep it under version control. The
schema is strict: an unknown key is a startup error, not a warning. Note that
`routing_rules` is mandatory and fully validated, but is not yet consumed by the
delivery path (see Status).

The example binds the client wire protocol on `127.0.0.1:8080` and the health
endpoints on `127.0.0.1:8081`; the two must be different ports, so probe traffic
stays isolated from client traffic. `SIGINT` or `SIGTERM` begins a graceful drain
bounded by `drain_timeout_ms`.

### Health and metrics

The health listener speaks plain HTTP on `health_listen_address` and serves three
`GET` routes. Any other path answers `404`; any other method on these paths
answers `405`.

| Route | Response |
|-------|----------|
| `GET /health` | Liveness. Always `200` with `{"status":"healthy","message":null}` — if the process can answer, it is alive. |
| `GET /ready` | Readiness. `200` with `{"ready":true,"unmet_conditions":[]}` once configuration has loaded, the wire listener is bound, and — when `[cluster]` is configured — membership is established. Otherwise `503`, naming the unmet conditions. |
| `GET /metrics` | Prometheus text exposition (`text/plain; version=0.0.4`) of the process metrics registry. Always `200`; the body is empty when no registry is installed, so a scraper still observes a live target. |

### Environment overrides

Thirteen `LIMINAL_*` variables override file values. They are applied after the
file is parsed and before validation runs, so an override is held to exactly the
same rules as a value written in the file.

| Variable | Overrides |
|----------|-----------|
| `LIMINAL_LISTEN_ADDRESS` | `listen_address` |
| `LIMINAL_HEALTH_LISTEN_ADDRESS` | `health_listen_address` |
| `LIMINAL_DRAIN_TIMEOUT_MS` | `drain_timeout_ms` |
| `LIMINAL_PERSISTENCE_PATH` | `persistence_path` |
| `LIMINAL_AUTH_TOKEN` | `auth.token` |
| `LIMINAL_CLUSTER_NODE_NAME` | `cluster.node_name` |
| `LIMINAL_CLUSTER_LISTEN_ADDRESS` | `cluster.listen_address` |
| `LIMINAL_CLUSTER_SEED_NODES` | `cluster.seed_nodes` (comma-separated) |
| `LIMINAL_CLUSTER_COOKIE` | `cluster.cookie` |
| `LIMINAL_WEBSOCKET_LISTEN_ADDRESS` | `websocket.listen_address` |
| `LIMINAL_WEBSOCKET_PATH` | `websocket.path` |
| `LIMINAL_WEBSOCKET_ALLOWED_ORIGINS` | `websocket.allowed_origins` (comma-separated) |
| `LIMINAL_WEBSOCKET_PING_INTERVAL_MS` | `websocket.ping_interval_ms` |

Two asymmetries are deliberate. `LIMINAL_AUTH_TOKEN` *may* create an absent
`[auth]` section — a single scalar secret belongs in the environment rather than
a committed file. The cluster and websocket variables refuse to fabricate a
section the file did not declare, because a partially specified listener is worse
than no listener.

### Durable state

`persistence_path` is optional. Absent, durable channels use an ephemeral store
that leaves no residue and survives no restart. Set it and startup requires the
directory to **already exist** and be writable — it is never created for you, so
a path typo fails startup with `path is unreachable` instead of quietly minting a
new directory. The store itself is created one level below, at
`<persistence_path>/durability`. The example ships this key commented out so it
validates from any fresh checkout.

### Logging

The server logs through `tracing`, filtered by `RUST_LOG`. When the variable is
unset the default filter is:

```
warn,liminal_server=info,liminal=info
```

An **empty** `RUST_LOG` is not "use the default". `RUST_LOG=""` is a valid but
empty directive set, and it means total silence — including `error` events. That
is upstream `env-filter` semantics, kept deliberately. Unset the variable rather
than emptying it if you want the default back.

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
