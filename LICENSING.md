# Licensing

Liminal is split-licensed. The rule is simple: everything your code links
against is Apache-2.0; the servers are AGPL-3.0-only.

## Apache-2.0 — the client surface

- **`liminal-protocol`** (crates.io) — the shared wire and lifecycle types
- **`liminal-sdk`** (crates.io) — the application-facing client SDK
- **`@ablative/liminal`** (npm) — the TypeScript SDK and its WASM protocol bridge

These are the pieces that get compiled into *your* application. You can link
them into closed-source, commercial, or any other software without your code
taking on copyleft obligations. Each package carries the full license text
(`LICENSE-APACHE` in the crates, `LICENSE` in the npm package).

Apache-2.0 rather than MIT because of its explicit patent grant: every
contributor licenses any patents their contribution practices, so building a
client on liminal can never become a patent trap.

## AGPL-3.0-only — the servers

- **`liminal-rs`** (the `crates/liminal` core: broker, codec, transports)
- **`liminal-server`** (the runnable server binary)

The servers stay open. AGPL's network clause means anyone who runs a modified
liminal server for others must publish their modifications — improvements to
the infrastructure flow back instead of being captured behind a proprietary
hosted fork. The full text is in [`LICENSE`](LICENSE) at the repo root.

If AGPL doesn't work for your deployment, commercial licensing for the server
components is available from Ablative — contact <tom@ablative.com.au>.

## In practice

| You want to… | License that applies to you |
| --- | --- |
| Build an app or service that connects to a liminal server | Apache-2.0 (client packages only) |
| Run a liminal server, modified or not, for yourself or others | AGPL-3.0-only |
| Run a modified server without publishing your changes | Commercial license from Ablative |
