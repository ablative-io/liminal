import assert from "node:assert/strict";
import { test } from "node:test";

import {
  LiminalFeedSource,
  LiminalFeedSourceError,
  RESERVED_OBSERVABILITY_CHANNEL,
} from "../src/index.js";
import type {
  WasmProtocolBindings,
  WasmReceivedFrame,
  WasmReceiveOptions,
} from "../src/wasm.js";

const encoder = new TextEncoder();

class FakeWebSocket {
  binaryType: BinaryType = "blob";
  readyState = 0;
  onopen: ((event: Event) => void) | null = null;
  onmessage: ((event: MessageEvent) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;
  onclose: ((event: CloseEvent) => void) | null = null;
  readonly sent: Uint8Array[] = [];
  closeCalled = false;

  open(): void {
    this.readyState = 1;
    this.onopen?.({} as Event);
  }

  message(bytes: Uint8Array): void {
    this.onmessage?.({ data: bytes.buffer } as MessageEvent);
  }

  send(data: ArrayBufferView): void {
    this.sent.push(new Uint8Array(data.buffer, data.byteOffset, data.byteLength).slice());
  }

  close(): void {
    this.closeCalled = true;
    this.readyState = 3;
    this.onclose?.({ code: 1000 } as CloseEvent);
  }
}

function bindings(): WasmProtocolBindings {
  return {
    connect: () => ({ connectionId: 0, frame: Uint8Array.of(1) }),
    send: () => ({ frame: Uint8Array.of(9) }),
    subscribe: () => ({ frame: Uint8Array.of(2) }),
    unsubscribe: () => ({ frame: Uint8Array.of(3) }),
    close: () => ({ frame: Uint8Array.of(4) }),
    receive: (options) => fakeReceive(options),
  };
}

function fakeReceive(options: WasmReceiveOptions): WasmReceivedFrame {
  const code = options.frame[0];
  const base = {
    channel: options.channel ?? "",
    streamId: code === 10 ? 0 : 1,
    consumedBytes: options.frame.byteLength,
    payload: options.frame.slice(1),
  };
  if (code === 10) return { ...base, frameType: 0x02 };
  if (code === 11) return { ...base, frameType: 0x06, subscriptionId: 42n };
  if (code === 12) return { ...base, frameType: 0x19 };
  return { ...base, frameType: 0x04 };
}

function deliver(envelope: string): Uint8Array {
  const payload = encoder.encode(envelope);
  const frame = new Uint8Array(payload.byteLength + 1);
  frame[0] = 12;
  frame.set(payload, 1);
  return frame;
}

async function flush(): Promise<void> {
  for (let turn = 0; turn < 8; turn += 1) await Promise.resolve();
}

async function waitUntil(predicate: () => boolean): Promise<void> {
  for (let turn = 0; turn < 100; turn += 1) {
    if (predicate()) return;
    await Promise.resolve();
  }
  assert.fail("asynchronous feed operation did not settle within 100 microtasks");
}

test("LiminalFeedSource rejects the reserved observability channel", () => {
  assert.throws(
    () => new LiminalFeedSource("ws://127.0.0.1/liminal", "token", RESERVED_OBSERVABILITY_CHANNEL),
    (error: unknown) => error instanceof LiminalFeedSourceError && error.code === "RESERVED_CHANNEL",
  );
});

test("LiminalFeedSource handshakes, delivers exact bytes, dedupes snapshots, and tears down", async () => {
  const socket = new FakeWebSocket();
  const source = new LiminalFeedSource("127.0.0.1:9000/liminal", "secret", undefined, {
    wasm: { bindings: bindings() },
    webSocketFactory: () => socket as unknown as WebSocket,
  });
  const received: string[] = [];
  const unsubscribe = source.subscribe((bytes) => received.push(bytes));

  socket.open();
  await waitUntil(() => socket.sent.length === 1);
  assert.deepEqual(socket.sent, [Uint8Array.of(1)]);
  socket.message(Uint8Array.of(10));
  await waitUntil(() => socket.sent.length === 2);
  assert.deepEqual(socket.sent, [Uint8Array.of(1), Uint8Array.of(2)]);
  const ready = source.whenSubscribed();
  socket.message(Uint8Array.of(11));
  await ready;

  const snapshot =
    '{"componentId":"graph-demo","contractId":"frame:graph-view@v1","generation":1,"kind":"snapshot","payload":"{\\"edges\\":[],\\"nodes\\":[]}","seq":0}';
  socket.message(deliver(snapshot));
  await waitUntil(() => received.length === 1);
  assert.deepEqual(received, [snapshot]);
  assert.equal(await source.requestSnapshot(), snapshot);

  let duplicateSettled = false;
  const duplicate = source.requestSnapshot().then((bytes) => {
    duplicateSettled = true;
    return bytes;
  });
  await flush();
  assert.equal(duplicateSettled, false, "the same installed snapshot cut must stay quiet");

  const nextSnapshot = snapshot.replace('"generation":1', '"generation":2');
  socket.message(deliver(nextSnapshot));
  assert.equal(await duplicate, nextSnapshot);

  unsubscribe();
  await waitUntil(() => socket.sent.length === 4 && socket.closeCalled);
  assert.deepEqual(socket.sent, [
    Uint8Array.of(1),
    Uint8Array.of(2),
    Uint8Array.of(3),
    Uint8Array.of(4),
  ]);
  assert.equal(socket.closeCalled, true);
});

test("receiver exceptions surface as typed errors without terminating delivery", async () => {
  const socket = new FakeWebSocket();
  const source = new LiminalFeedSource("ws://127.0.0.1/liminal", "", undefined, {
    wasm: { bindings: bindings() },
    webSocketFactory: () => socket as unknown as WebSocket,
  });
  const errors: LiminalFeedSourceError[] = [];
  source.onError((error) => errors.push(error));
  const unsubscribe = source.subscribe(() => {
    throw new Error("receiver fault");
  });
  socket.open();
  await waitUntil(() => socket.sent.length === 1);
  socket.message(Uint8Array.of(10));
  await waitUntil(() => socket.sent.length === 2);
  socket.message(Uint8Array.of(11));
  await source.whenSubscribed();

  socket.message(deliver('{"generation":1,"kind":"delta","seq":1}'));
  await waitUntil(() => errors.length === 1);

  assert.equal(errors[0]?.code, "RECEIVER_FAILED");
  assert.equal(source.lastError?.code, "RECEIVER_FAILED");
  unsubscribe();
});
