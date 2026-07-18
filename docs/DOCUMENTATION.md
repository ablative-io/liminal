# Liminal — Documentation

## What is Liminal?

Liminal is a messaging system — it moves information between different parts of your application. What makes it different: the conversation is the fundamental unit, not individual messages. A conversation knows who's in it, what's been said, and what state it's in. If something crashes, the conversation picks up where it left off.

## Why does Liminal exist?

Most messaging systems are just pipes — they move data from point A to point B without understanding what's flowing through them. If something goes wrong, the message is lost and you have to build retry logic, dead-letter queues, and monitoring on top.

Liminal is different:

- **Conversations, not messages** — A conversation is a managed interaction between participants. It tracks who joined, what was said, and what state it's in. It's the difference between passing notes in class and having an actual meeting with an agenda.
- **Crash-proof** — Messages are stored durably. If the system crashes mid-conversation, it replays from the last known state and continues. No lost messages.
- **Schema validation** — Messages are checked against a schema before delivery. If someone sends the wrong type of data, it's rejected immediately — not discovered later when something breaks.
- **Backpressure** — If a recipient can't keep up, Liminal slows down the sender automatically. No overloaded queues, no dropped messages.

## How does Liminal fit in the Ablative Stack?

Liminal is the communication layer. It's how every part of the stack talks to every other part:

```
Component A needs to tell Component B something
            ↓
Liminal creates a conversation between them
            ↓
Messages are validated, delivered, and stored
            ↓
If anything crashes, the conversation resumes automatically
```

Aion uses Liminal to coordinate workflows. Frame uses it instead of REST APIs — components talk through conversations, not HTTP requests. AI agents join conversations as participants, just like humans.

## Current Status

**Version 0.2.2** — Core messaging, channels, schema validation, durable mailboxes, and the wire protocol are implemented and tested. 45,000 lines of code, 188 tests. The TypeScript and Gleam SDKs are available.

## Getting Started

### What you'll need

- **Rust** — Liminal's core is a Rust crate. Install Rust from [rustup.rs](https://rustup.rs) if you don't have it.

### Add Liminal to your Rust project

In your `Cargo.toml`:

```toml
[dependencies]
liminal = { package = "liminal-rs", version = "0.2.2" }
```

Note: The crate is published on crates.io as `liminal-rs` (the bare `liminal` name was already taken), but you import it as `liminal` in your code.

### Create a conversation

```rust
use liminal::channel::ChannelHandle;
use liminal::conversation::ConversationHandle;

// Create a channel
let channel = ChannelHandle::new("updates");

// Subscribe to the channel
channel.subscribe(|message| {
    println!("Received: {:?}", message);
});

// Send a message
channel.send("Hello from Liminal!");
```

### TypeScript SDK

If you're working in TypeScript (browser or Node.js):

```
npm install @ablative/liminal
```

```typescript
import { Conversation } from '@ablative/liminal';

const conv = new Conversation('updates');

conv.on('message', (msg) => {
    console.log('Received:', msg);
});

conv.send({ text: 'Hello from the browser!' });
```

## Key Concepts

**Conversations** — The core abstraction. A conversation is a supervised process with participants, state, and a message history. Unlike a WebSocket channel (which is just a pipe), a conversation understands what's happening inside it.

**Channels** — A broadcast mechanism — one sender, many subscribers. Useful for events like "a new order came in" that multiple parts of your system need to know about.

**Durable mailboxes** — Messages are stored so they survive crashes. When a process restarts, it can replay messages it missed. This means you don't need to build your own retry logic.

**Schema validation** — Every message can be validated against a schema before delivery. If the data doesn't match the expected shape, it's rejected at the gate rather than causing errors downstream.

**Process links** — Liminal uses BEAM-style process links (via Beamr) for crash detection. If a participant in a conversation crashes, the conversation knows immediately — no heartbeat polling, no timeouts.

## Learn More

- [Ablative Stack overview](https://ablative.dev) — See how Liminal fits with the other components

## License

AGPL-3.0 — free to use and modify. If you distribute a modified version or run it as a service, you must share your changes under the same license.
