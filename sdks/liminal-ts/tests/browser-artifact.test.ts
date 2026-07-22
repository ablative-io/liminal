/**
 * Smoke proof for the self-contained browser artifact
 * (`dist/browser/liminal.js`) — asserting on the exact bytes that ship in the
 * npm tarball and get vendored by frame scaffolds.
 *
 * Walls, in order:
 *  1. the artifact embeds the protocol WASM binary's bytes (aligned base64
 *     prefix of `wasm/liminal_protocol_wasm_bg.wasm`);
 *  2. the artifact text carries no static imports, no dynamic `import()`, and
 *     no `node:` specifiers — nothing left for an import map to miss;
 *  3. the artifact loads as a real ES module with `fetch` poisoned;
 *  4. its export surface is exactly the package root surface;
 *  5. the embedded WASM actually runs: a `LiminalFeedSource` over a stub
 *     WebSocket encodes a Connect frame through the inlined binary — with
 *     `fetch` still poisoned and never called.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";

import * as referenceSurface from "../src/index.js";

const packageRoot = fileURLToPath(new URL("../..", import.meta.url));
const artifactPath = path.join(packageRoot, "dist", "browser", "liminal.js");
const wasmBinaryPath = path.join(packageRoot, "wasm", "liminal_protocol_wasm_bg.wasm");
const HANDSHAKE_TIMEOUT_MS = 30_000;

interface StubSentFrame {
  readonly frame: Uint8Array;
}

class StubWebSocket {
  binaryType = "blob";
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: unknown }) => void) | null = null;
  onerror: (() => void) | null = null;
  onclose: ((event: unknown) => void) | null = null;
  readyState = 0;
  readonly sent: StubSentFrame[] = [];
  private notifySend: (() => void) | undefined;

  send(frame: Uint8Array): void {
    this.sent.push({ frame });
    this.notifySend?.();
  }

  close(_code?: number, _reason?: string): void {
    this.readyState = 3;
  }

  open(): void {
    this.readyState = 1;
    assert.ok(this.onopen, "feed transport must install an onopen handler");
    this.onopen();
  }

  async nextFrame(failure: () => Error | undefined): Promise<Uint8Array> {
    const deadline = Date.now() + HANDSHAKE_TIMEOUT_MS;
    while (this.sent.length === 0) {
      const error = failure();
      if (error !== undefined) throw error;
      if (Date.now() > deadline) {
        throw new Error("Timed out waiting for the stub WebSocket to receive a frame");
      }
      await new Promise<void>((resolve) => {
        this.notifySend = resolve;
        setTimeout(resolve, 50);
      });
      this.notifySend = undefined;
    }
    const sent = this.sent[0];
    assert.ok(sent);
    return sent.frame;
  }
}

interface BrowserFeedSource {
  subscribe(receiver: (envelopeBytes: string) => void): () => void;
  onError(listener: (error: Error) => void): () => void;
}

interface BrowserSurface {
  readonly LiminalFeedSource: new (
    serverUrl: string,
    authToken: string,
    channel?: string,
    options?: { webSocketFactory?: (endpoint: string) => unknown },
  ) => BrowserFeedSource;
  readonly DEFAULT_FEED_CHANNEL: string;
}

test("browser artifact embeds the protocol WASM binary and stays self-contained", () => {
  const artifactText = readFileSync(artifactPath, "utf8");
  const wasmBytes = readFileSync(wasmBinaryPath);
  assert.ok(wasmBytes.byteLength > 0, "protocol WASM binary must not be empty");
  // 510 is divisible by 3, so this is a strict prefix of the full base64 text.
  const alignedPrefix = wasmBytes.subarray(0, 510).toString("base64");
  assert.ok(
    artifactText.includes(alignedPrefix),
    "artifact must embed the protocol WASM binary as base64",
  );
  assert.ok(
    artifactText.length > wasmBytes.byteLength,
    "artifact must be larger than the WASM binary it embeds",
  );

  const staticImport = /^\s*(?:import\b|export\s+[^;\n]*?\bfrom\b)/m.exec(artifactText);
  assert.equal(
    staticImport,
    null,
    `artifact must have no static module dependencies, found ${JSON.stringify(staticImport?.[0])}`,
  );
  assert.ok(!/\bimport\s*\(/.test(artifactText), "artifact must have no dynamic import() calls");
  assert.ok(!/["'`]node:/.test(artifactText), "artifact must not reference node: builtins");
});

test("browser artifact loads as ESM offline and matches the root export surface", async () => {
  const artifact = await importArtifactWithFetchPoisoned();
  const artifactExports = new Set(Object.keys(artifact));
  const referenceExports = new Set(Object.keys(referenceSurface));
  assert.deepEqual(
    [...artifactExports].sort(),
    [...referenceExports].sort(),
    "artifact exports must equal the package root surface exactly",
  );
  for (const name of [
    "createChannel",
    "Connection",
    "openConversation",
    "generate",
    "LiminalFeedSource",
    "LiminalFeedSourceError",
    "SdkError",
  ]) {
    assert.equal(typeof artifact[name], "function", `artifact must export function ${name}`);
  }
  // `Channel` is the package's frozen helper namespace, not a class.
  assert.equal(typeof artifact["Channel"], "object", "artifact must export the Channel namespace");
  assert.ok(Object.isFrozen(artifact["Channel"]), "Channel namespace must arrive frozen");
  assert.equal(artifact["DEFAULT_FEED_CHANNEL"], "frame.demo.graph-view");
});

test("embedded WASM encodes a Connect frame with fetch poisoned", async () => {
  const artifact = await importArtifactWithFetchPoisoned();
  const surface = artifact as unknown as BrowserSurface;
  const fetchCalls: unknown[] = [];
  const poisonedFetch: typeof fetch = (...args) => {
    fetchCalls.push(args);
    throw new Error("network access is forbidden in the browser artifact smoke test");
  };
  const realFetch = globalThis.fetch;
  globalThis.fetch = poisonedFetch;
  try {
    const socket = new StubWebSocket();
    const source = new surface.LiminalFeedSource("ws://127.0.0.1:9", "smoke-token", undefined, {
      webSocketFactory: () => socket,
    });
    let sourceError: Error | undefined;
    source.onError((error) => {
      sourceError = error;
    });
    const unsubscribe = source.subscribe(() => {});
    assert.equal(socket.binaryType, "arraybuffer", "feed transport must request binary frames");
    socket.open();
    const connectFrame = await socket.nextFrame(() => sourceError);
    assert.ok(connectFrame instanceof Uint8Array, "Connect frame must be raw bytes");
    assert.ok(connectFrame.byteLength > 0, "Connect frame must not be empty");
    assert.equal(sourceError, undefined, "handshake must not surface a feed error");
    assert.equal(fetchCalls.length, 0, "embedded WASM instantiation must never call fetch");
    unsubscribe();
  } finally {
    globalThis.fetch = realFetch;
  }
});

async function importArtifactWithFetchPoisoned(): Promise<Record<string, unknown>> {
  const fetchCalls: unknown[] = [];
  const poisonedFetch: typeof fetch = (...args) => {
    fetchCalls.push(args);
    throw new Error("network access is forbidden while importing the browser artifact");
  };
  const realFetch = globalThis.fetch;
  globalThis.fetch = poisonedFetch;
  try {
    const artifact = (await import(pathToFileURL(artifactPath).href)) as Record<string, unknown>;
    assert.equal(fetchCalls.length, 0, "importing the artifact must never call fetch");
    return artifact;
  } finally {
    globalThis.fetch = realFetch;
  }
}
