# Liminal

A conversation-based messaging bus built on the beamr BEAM virtual machine.

## What it is

Liminal is a messaging bus where conversations, not messages, are the fundamental unit. Every message delivery, every backpressure decision, every durable replay happens through a conversation actor that owns the interaction lifecycle. The code looks like a message bus but the execution model is actor-per-conversation, not queue-per-topic.

A conversation is a supervised beamr process that mediates a structured exchange between participants. When the bus crashes, conversations resume from committed state. When a participant crashes, the bus detects it within microseconds via process links. When load exceeds capacity, backpressure propagates through the conversation, not through a separate flow-control channel.

> ⚠️ **Status (2026-06):** partial — conversation actors are supervised and restart after a crash, but the actor holds in-memory state and respawns empty; there is no committed-state replay yet.

## Why it exists

Existing messaging systems (NATS, Kafka, RabbitMQ, gRPC streams) treat the bus as a dumb pipe. Messages go in, messages come out. The bus doesn't know what a conversation is, doesn't know when a participant crashes, doesn't know whether a message sequence makes semantic sense. All of that is the application's problem.

Liminal makes it the bus's problem:

- **Crash detection is immediate.** Participant processes are linked to conversation actors. Death triggers an EXIT signal on the same scheduler tick. No heartbeat interval. No timeout. No health check endpoint.
- **Durable conversations resume from committed state.** Backed by haematite's content-addressed storage. Branch on conversation start, commit on completion, discard on failure. Conversation state is an append-only event log, not a mutable record.

  > ⚠️ **Status (2026-06):** not yet implemented — describes intended design. The conversation actor holds in-memory state and respawns empty on crash; there is no committed-state replay, and the embedded haematite crate is an in-memory mock (see `crates/haematite`).
- **Schema validation happens at publish time.** Bad messages never enter the system. The channel owns the schema and rejects non-conforming payloads before any subscriber sees them.
- **Backpressure is a protocol primitive.** Accept, Defer, Reject are first-class frame types in the wire protocol, not application-level conventions.
- **Per-causal-chain ordering, not per-topic.** Two independent conversations on the same channel don't need total ordering. This is what lets it scale horizontally without coordination overhead.
- **Exactly-once as an explicit three-party contract.** Both sides opt in, the bus mediates the commit. Not a hidden implementation detail that breaks under partition.

## How it works

### Conversations as actors

Every conversation is a beamr process. It has a mailbox, it has links to its participants, it has a state machine (Created, Active, Completing, Closed, Failed). Sending a message to a conversation is a message send. Receiving is a selective receive. The BEAM process model gives you supervision, crash isolation, and fair scheduling for free.

### Typed channels

Channels are the routing surface. Each channel owns a JSON Schema that defines the message contract. Publish-time validation means subscribers never receive structurally invalid messages. Schema evolution (add a field with a default) doesn't disconnect existing subscribers.

### Two-tier routing

Predicates match messages to subscribers without code: "all orders where amount > 1000". When predicates aren't enough, Gleam routing functions run as supervised beamr processes with fault isolation and hot deployment. The predicate compiler optimises declarative predicates into the same execution model as custom functions. One routing engine, two input formats.

> ⚠️ **Status (2026-06):** not yet implemented — describes intended design. Routing functions are native Rust closures today; there are zero `.gleam` files in the repo and no beamr process execution. Module bytecode is only content-hashed for dedup, never executed (see `crates/liminal/src/routing/function/loader.rs`).

### Custom wire protocol

A purpose-built binary protocol with typed frames, connection lifecycle state machine, protocol version negotiation, stream multiplexing, and causal context embedded in every message envelope. The bus extracts ordering information from the frame without deserialising the application payload.

### The eight clusters

| Cluster | What it builds | Briefs |
|---|---|---|
| **core** | Channel actors, conversation actors, schema validation, envelopes, causal ordering | LIM-001 through LIM-006 |
| **routing** | Predicate evaluation, routing tables, predicate compiler, function execution, dispatch, backpressure | ROUTING-001 through ROUTING-006 |
| **durability** | Durable channel storage, consumer cursors, dedup, event-sourced conversations, crash recovery | DUR-001 through DUR-006 |
| **protocol** | Frame types, codec, envelope types, lifecycle state machine, multiplexing, schema negotiation | PROTO-001 through PROTO-006 |
| **aion** | Activity dispatch, signal delivery, workflow history, worker registration | AION-001 through AION-005 |
| **sdk** | Rust SDK traits, Gleam bindings, TypeScript WASM bridge, conformance tests | SDK-001 through SDK-009 |
| **server** | Binary entry point, config loading, network listener, graceful shutdown, clustering, health endpoints | SRV-001 through SRV-006 |
| **observability** | Metrics registry, channel/conversation/pressure metrics, Prometheus export, structured logging | OBS-001 through OBS-007 |

### Architecture decisions

- **ADR-001:** Conversations as the fundamental unit. Not channels, not topics, not queues.
- **ADR-002:** Typed channels with compile-time Gleam enforcement. Schema in the bus, not beside it.
- **ADR-003:** Two-tier routing. Predicates for declarative matching, functions for complex logic.
- **ADR-004:** Backpressure as a protocol primitive. Accept/Defer/Reject at the frame level.
- **ADR-005:** Per-causal-chain ordering. Independent conversations are independent.
- **ADR-006:** Two exactly-once strategies. Idempotency keys for simple cases, three-party commit for distributed transactions.
- **ADR-007:** Conversation-mediated dispatch. Aion activities route through conversations, not direct function calls.
- **ADR-008:** Custom binary wire protocol. Purpose-built for conversation lifecycle, not adapted from HTTP/gRPC.
- **ADR-009:** Haematite storage backend. Content-addressed append log for durable conversations.
- **ADR-010:** Embedded-first architecture. Zero-hop in-process dispatch with no serialisation boundary. Same code path works distributed.

## What makes it different

Most messaging systems are stateless pipes with optional persistence bolted on. Liminal is a stateful conversation engine with messaging as the transport.

The closest analogues are:

- **Temporal** for durable workflows, but Liminal mediates the communication directly instead of wrapping gRPC. Crash detection is microseconds, not heartbeat intervals.
- **Phoenix Channels** for real-time pub/sub, but Liminal's conversations have durable state, schema validation, and exactly-once guarantees.
- **NATS** for lightweight messaging, but Liminal's predicate routing and conversation lifecycle go beyond subject-based pub/sub.

The key insight: BEAM was designed for Erlang, a dynamically typed language. Every BEAM runtime throws away the types that Gleam's compiler proves at compile time. beamr is the first BEAM runtime that preserves and uses those types. Liminal builds on that by making typed channels a protocol-level guarantee, not an application-level convention.

**Make your processing resumable and idempotency is free.**

## Current status

19 of 51 briefs landed. ~188 tests. The core channel and conversation actors, predicate routing with compiled functions, wire protocol with versioning and typed envelopes, durability foundation with partition-aware storage, and Aion activity dispatch integration are complete. Consumer cursors, stream multiplexing, and the SDK surface are next.

> ⚠️ **Status (2026-06):** partial — the Aion integration is a designed seam with all-no-op defaults and no `aion` crate dependency yet (dispatch uses a default embedded context; see `crates/liminal/src/aion/dispatch.rs`). "Routing with compiled functions" runs native Rust closures, not Gleam/beamr (see note above). Durability "resumes from committed state" is not yet implemented (see above).
