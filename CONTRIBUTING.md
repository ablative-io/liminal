# Contributing to Liminal

## Prerequisites

- **Rust 1.85+** (check with `rustc --version`)
- **Cargo** (ships with Rust)
- For the TypeScript SDK: **Node.js 20+** and **npm**
- For the Gleam SDK: **Gleam 1.x** and **Erlang/OTP 26+**

## Building

```sh
# Build the entire workspace
cargo build --workspace

# Build a specific crate
cargo build -p liminal-rs
cargo build -p liminal-server
cargo build -p liminal-sdk
cargo build -p liminal-protocol
```

## Testing

```sh
# Run all workspace tests
cargo test --workspace

# Run tests for a specific crate
cargo test -p liminal-rs
cargo test -p liminal-server

# Run the conformance suite
cargo test -p liminal-rs --test conformance
```

## Linting

Both must pass clean before any commit:

```sh
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```

If clippy fires, fix the code. `#[allow(...)]` and `#[expect(...)]` are
bypasses, not fixes.

## Repository layout

```
liminal/
├── crates/
│   ├── liminal/             Core library (channels, conversations,
│   │   └── src/               durability, routing, backpressure, protocol)
│   │       ├── channel/       Pub/sub channels with schemas and supervision
│   │       ├── conversation/  First-class conversations with participants
│   │       ├── durability/    Crash-safe message persistence (haematite-backed)
│   │       ├── routing/       Message routing and predicate matching
│   │       ├── pressure/      Backpressure / flow control
│   │       ├── protocol/      Wire protocol
│   │       ├── causal/        Causal ordering metadata
│   │       ├── metrics/       Metrics registry
│   │       └── tracing/       Trace context propagation
│   ├── liminal-protocol/    Protocol algebra, lifecycle, outcome, wire format
│   ├── liminal-sdk/         Application-facing SDK traits (no_std-capable)
│   └── liminal-server/      Standalone server binary
├── sdks/
│   ├── liminal-ts/          TypeScript SDK (browser and Node.js)
│   └── liminal-gleam/       Gleam SDK (native BEAM client)
├── tests/
│   └── conformance/         Cross-implementation conformance suite
├── docs/
│   ├── design/              Design documents and decision records
│   └── stack-review/        Honest state map of what works and what doesn't
├── scripts/                 Build and utility scripts
├── CHANGELOG.md             Release history
├── CONVENTIONS.toml         Norn post-mutation check configuration
├── VISION.md                Architecture vision and design rationale
└── LICENSE                  AGPL-3.0-only
```

## Crate publishing

The workspace publishes three crates to crates.io in lockstep:

| crates.io name | `use` as | Purpose |
|---|---|---|
| `liminal-rs` | `liminal` | Core library (the bare `liminal` name is taken) |
| `liminal-sdk` | `liminal_sdk` | SDK traits for building liminal clients |
| `liminal-server` | `liminal_server` | Standalone server |

## Code style

- **No god files.** Keep modules under 500 lines of code (excluding tests,
  comments, whitespace).
- **`mod.rs` is declarations only.** Logic goes in named files.
- **Error handling:** `thiserror` for library errors, `anyhow` only in the
  binary for top-level reporting. Never `.unwrap()` or `.expect()` in
  library code.
- **No silent failures.** Every error handled, logged, or propagated.
- Strict clippy lints are enforced workspace-wide in `Cargo.toml`:
  `unsafe_code = "deny"`, pedantic enabled, `unwrap_used`/`expect_used`/`panic`
  denied.

## Commit messages

Follow the conventional commits style used in this repo:

```
feat(server): short description of the change
fix(protocol): what was broken and how it's fixed
refactor(channel): what was restructured
docs(design): what documentation changed
test(server): what test coverage was added
```

## License

By contributing, you agree that your contributions will be licensed under
AGPL-3.0-only, consistent with the project license.
