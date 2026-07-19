import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import {
  DEFAULT_FEED_CHANNEL,
  LiminalFeedSource,
  LiminalFeedSourceError,
} from "../src/index.js";
import type { FeedPublishReceipt } from "../src/index.js";
import { receive as decodeFrame, send as encodePublish } from "../src/wasm.js";
import type { WasmLoadOptions } from "../src/wasm.js";

const sdkRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const wasmPath = join(sdkRoot, "wasm", "liminal_protocol_wasm_bg.wasm");

const CONNECT_ACK = 0x02;
const SUBSCRIBE_ACK = 0x06;
const PUBLISH = 0x09;
const PUBLISH_ACK = 0x0a;
const PUBLISH_ERROR = 0x0b;
const HEADER_LEN = 10;
const PUBLISH_STREAM_ID = 1;

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

/** Builds one canonical liminal frame: 10-byte header plus payload. */
function frameBytes(frameType: number, streamId: number, payload: Uint8Array): Uint8Array {
  const bytes = new Uint8Array(HEADER_LEN + payload.byteLength);
  const view = new DataView(bytes.buffer);
  view.setUint8(0, frameType);
  view.setUint8(1, 0);
  view.setUint32(2, streamId);
  view.setUint32(6, payload.byteLength);
  bytes.set(payload, HEADER_LEN);
  return bytes;
}

/** ConnectAck: selected version (1.0) plus a zero capability bitset. */
function connectAckFrame(): Uint8Array {
  return frameBytes(CONNECT_ACK, 0, Uint8Array.of(0, 1, 0, 0, 0, 0, 0, 0));
}

/** SubscribeAck: subscription id plus the selected 32-byte schema id. */
function subscribeAckFrame(streamId: number, subscriptionId: bigint): Uint8Array {
  const payload = new Uint8Array(8 + 32);
  new DataView(payload.buffer).setBigUint64(0, subscriptionId);
  return frameBytes(SUBSCRIBE_ACK, streamId, payload);
}

/** PublishAck: the server-assigned message id. */
function publishAckFrame(streamId: number, messageId: bigint): Uint8Array {
  const payload = new Uint8Array(8);
  new DataView(payload.buffer).setBigUint64(0, messageId);
  return frameBytes(PUBLISH_ACK, streamId, payload);
}

/** PublishError: reason code plus a present optional refusal string. */
function publishErrorFrame(streamId: number, reasonCode: number, message: string): Uint8Array {
  const text = encoder.encode(message);
  const payload = new Uint8Array(2 + 1 + 4 + text.byteLength);
  const view = new DataView(payload.buffer);
  view.setUint16(0, reasonCode);
  view.setUint8(2, 1);
  view.setUint32(3, text.byteLength);
  payload.set(text, 7);
  return frameBytes(PUBLISH_ERROR, streamId, payload);
}

async function loadWasm(): Promise<WasmLoadOptions> {
  return { source: new Uint8Array(await readFile(wasmPath)) };
}

/**
 * Waits for `predicate` under a bounded deadline. Timer-based rather than
 * microtask-spinning because the first use of the real wasm codec instantiates
 * the WebAssembly module, which settles on a macrotask.
 */
async function waitUntil(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() > deadline) {
      assert.fail("asynchronous feed operation did not settle within five seconds");
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}

interface SubscribedHarness {
  readonly socket: FakeWebSocket;
  readonly source: LiminalFeedSource;
  readonly wasm: WasmLoadOptions;
  readonly delivered: string[];
  readonly errors: LiminalFeedSourceError[];
  readonly unsubscribe: () => void;
}

/** Drives the real-wasm handshake to `active` over a fake WebSocket. */
async function subscribedHarness(): Promise<SubscribedHarness> {
  const wasm = await loadWasm();
  const socket = new FakeWebSocket();
  const source = new LiminalFeedSource("ws://127.0.0.1:9000/liminal", "token", undefined, {
    wasm,
    webSocketFactory: () => socket as unknown as WebSocket,
  });
  const delivered: string[] = [];
  const errors: LiminalFeedSourceError[] = [];
  source.onError((error) => errors.push(error));
  const unsubscribe = source.subscribe((bytes) => delivered.push(bytes));

  socket.open();
  await waitUntil(() => socket.sent.length === 1);
  socket.message(connectAckFrame());
  await waitUntil(() => socket.sent.length === 2);
  const ready = source.whenSubscribed();
  socket.message(subscribeAckFrame(1, 77n));
  await ready;

  return { socket, source, wasm, delivered, errors, unsubscribe };
}

function expectFeedError(
  code: LiminalFeedSourceError["code"],
): (error: unknown) => boolean {
  return (error: unknown) =>
    error instanceof LiminalFeedSourceError && error.code === code;
}

test("publish before subscribe is refused loudly with NOT_SUBSCRIBED", async () => {
  const wasm = await loadWasm();
  const source = new LiminalFeedSource("ws://127.0.0.1:9000/liminal", "token", undefined, {
    wasm,
    webSocketFactory: () => new FakeWebSocket() as unknown as WebSocket,
  });
  await assert.rejects(source.publish("{}"), expectFeedError("NOT_SUBSCRIBED"));
});

test("publish before the connect handshake completes is refused loudly, not queued", async () => {
  const wasm = await loadWasm();
  const socket = new FakeWebSocket();
  const source = new LiminalFeedSource("ws://127.0.0.1:9000/liminal", "token", undefined, {
    wasm,
    webSocketFactory: () => socket as unknown as WebSocket,
  });
  source.onError(() => {});
  const unsubscribe = source.subscribe(() => {});
  socket.open();
  await waitUntil(() => socket.sent.length === 1);
  // Connect frame sent, no ConnectAck yet: the handshake is incomplete.
  await assert.rejects(source.publish("{}"), expectFeedError("NOT_SUBSCRIBED"));
  assert.equal(socket.sent.length, 1, "a refused publish must transmit nothing");
  unsubscribe();
});

test("publish transmits the exact canonical frame on the subscription socket and resolves with the PublishAck receipt", async () => {
  const harness = await subscribedHarness();
  const { socket, source, wasm } = harness;
  const envelope = '{"componentId":"demo","kind":"delta","seq":9}';

  const receiptPromise = source.publish(envelope);
  await waitUntil(() => socket.sent.length === 3);

  // Byte-exactness: the transmitted frame is exactly the wasm encoder's
  // canonical publish frame for this channel, stream id, and payload.
  const { frame: expected } = await encodePublish(
    {
      connectionId: 0,
      streamId: PUBLISH_STREAM_ID,
      channel: DEFAULT_FEED_CHANNEL,
      payload: encoder.encode(envelope),
    },
    wasm,
  );
  assert.deepEqual(socket.sent[2], expected);

  // Round-trip through the real wasm decoder: canonical Publish, stream id 1,
  // byte-identical envelope payload.
  const decoded = await decodeFrame(
    { connectionId: 0, frame: socket.sent[2] ?? new Uint8Array() },
    wasm,
  );
  assert.equal(decoded.frameType, PUBLISH);
  assert.equal(decoded.streamId, PUBLISH_STREAM_ID);
  assert.deepEqual(decoded.payload, encoder.encode(envelope));

  socket.message(publishAckFrame(PUBLISH_STREAM_ID, 424_242n));
  const receipt: FeedPublishReceipt = await receiptPromise;
  assert.deepEqual(receipt, { messageId: 424_242n, streamId: PUBLISH_STREAM_ID });
  assert.deepEqual(harness.errors, [], "an acknowledged publish must not surface errors");
  harness.unsubscribe();
});

test("a server PublishError rejects only the publish with the typed refusal; the subscription survives", async () => {
  const harness = await subscribedHarness();
  const { socket, source } = harness;

  const refused = source.publish('{"kind":"delta"}');
  await waitUntil(() => socket.sent.length === 3);
  socket.message(publishErrorFrame(PUBLISH_STREAM_ID, 0x1234, "channel is not declared"));

  await assert.rejects(refused, (error: unknown) => {
    assert.ok(error instanceof LiminalFeedSourceError);
    assert.equal(error.code, "PUBLISH_REJECTED");
    assert.match(error.message, /reason 4660/);
    assert.match(error.message, /channel is not declared/);
    assert.deepEqual(error.details, { reasonCode: 0x1234 });
    return true;
  });
  assert.deepEqual(harness.errors, [], "a refused publish must not fail the subscription");

  // The socket and state machine stay healthy: a follow-up publish succeeds.
  const retried = source.publish('{"kind":"delta","seq":2}');
  await waitUntil(() => socket.sent.length === 4);
  socket.message(publishAckFrame(PUBLISH_STREAM_ID, 7n));
  assert.deepEqual(await retried, { messageId: 7n, streamId: PUBLISH_STREAM_ID });
  assert.equal(socket.closeCalled, false);
  harness.unsubscribe();
});

test("teardown rejects an in-flight publish with CONNECTION_CLOSED and closes the socket cleanly", async () => {
  const harness = await subscribedHarness();
  const { socket, source } = harness;

  const inFlight = source.publish('{"kind":"delta","seq":3}');
  await waitUntil(() => socket.sent.length === 3);
  harness.unsubscribe();

  await assert.rejects(inFlight, expectFeedError("CONNECTION_CLOSED"));
  await waitUntil(() => socket.closeCalled);
  // Connect, Subscribe, Publish, then the teardown's Unsubscribe + Disconnect.
  await waitUntil(() => socket.sent.length === 5);
});

test("an unsolicited PublishError still terminates the subscription as a protocol breach", async () => {
  const harness = await subscribedHarness();
  const { socket } = harness;

  socket.message(publishErrorFrame(PUBLISH_STREAM_ID, 0x0007, "stray refusal"));
  await waitUntil(() => harness.errors.length === 1);
  assert.equal(harness.errors[0]?.code, "PROTOCOL_ERROR");
  assert.match(harness.errors[0]?.message ?? "", /subscriber received PublishError/);
  assert.equal(socket.closeCalled, true);
});

test("a PublishAck with no publish in flight terminates the subscription as a protocol breach", async () => {
  const harness = await subscribedHarness();
  const { socket } = harness;

  socket.message(publishAckFrame(PUBLISH_STREAM_ID, 1n));
  await waitUntil(() => harness.errors.length === 1);
  assert.equal(harness.errors[0]?.code, "PROTOCOL_ERROR");
  assert.match(harness.errors[0]?.message ?? "", /no publish in flight/);
  assert.equal(socket.closeCalled, true);
});
