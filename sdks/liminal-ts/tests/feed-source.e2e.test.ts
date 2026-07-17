import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { test } from "node:test";

import { DEFAULT_FEED_CHANNEL, LiminalFeedSource } from "../src/index.js";
import {
  connect as encodeConnect,
  receive as decodeFrame,
  send as encodePublish,
} from "../src/wasm.js";
import type { WasmLoadOptions } from "../src/wasm.js";
import { ProtocolTcpClient, startLiminalServer, withTimeout } from "./live-server.js";

const AUTH_TOKEN = "splice-live-token";
const CONNECT_ACK = 0x02;
const PUBLISH_ACK = 0x0a;
const encoder = new TextEncoder();

interface ReceivedFeed {
  readonly envelopes: string[];
  readonly receiver: (envelope: string) => void;
  waitForCount(count: number): Promise<void>;
}

function receivedFeed(): ReceivedFeed {
  const envelopes: string[] = [];
  const waiters: Array<{ readonly count: number; readonly resolve: () => void }> = [];
  return {
    envelopes,
    receiver(envelope): void {
      envelopes.push(envelope);
      for (let index = waiters.length - 1; index >= 0; index -= 1) {
        if (envelopes.length >= (waiters[index]?.count ?? Number.POSITIVE_INFINITY)) {
          waiters.splice(index, 1)[0]?.resolve();
        }
      }
    },
    waitForCount(count): Promise<void> {
      if (envelopes.length >= count) return Promise.resolve();
      return new Promise<void>((resolve) => waiters.push({ count, resolve }));
    },
  };
}

test(
  "LiminalFeedSource receives byte-identical demo envelopes through real TCP publish and WebSocket Deliver",
  { timeout: 120_000 },
  async (context) => {
    const server = await startLiminalServer(DEFAULT_FEED_CHANNEL, AUTH_TOKEN);
    context.after(() => server.close());
    const wasmBytes = new Uint8Array(await readFile(server.wasmPath));
    const wasm: WasmLoadOptions = { source: wasmBytes };
    const feed = receivedFeed();
    const source = new LiminalFeedSource(
      `ws://127.0.0.1:${server.websocketPort}/liminal`,
      AUTH_TOKEN,
      DEFAULT_FEED_CHANNEL,
      { wasm },
    );
    const unsubscribe = source.subscribe(feed.receiver);
    context.after(unsubscribe);
    await withTimeout(source.whenSubscribed(), 10_000, "WebSocket subscribe handshake");

    const publisher = await ProtocolTcpClient.open(server.tcpPort);
    context.after(() => publisher.close());
    const { frame: connectFrame } = await encodeConnect(
      { endpoint: `tcp://127.0.0.1:${server.tcpPort}`, authToken: encoder.encode(AUTH_TOKEN) },
      wasm,
    );
    const connectReply = await publisher.exchange(connectFrame);
    const connected = await decodeFrame({ connectionId: 0, frame: connectReply }, wasm);
    assert.equal(connected.frameType, CONNECT_ACK);

    const snapshotPayload =
      '{"graph":{"edges":[],"format":"graph-input","nodes":[]},"values":[]}';
    const firstDeltaPayload = '{"nodeId":"core","type":"value-changed","value":21}';
    const gapDeltaPayload = '{"nodeId":"relay","type":"value-changed","value":51}';
    const nextSnapshotPayload =
      '{"graph":{"edges":[],"format":"graph-input","nodes":[]},"values":[{"nodeId":"core","value":21}]}';
    const expected = [
      envelope("snapshot", 1, 0, snapshotPayload),
      envelope("delta", 1, 1, firstDeltaPayload),
      envelope("delta", 1, 3, gapDeltaPayload),
      envelope("snapshot", 2, 0, nextSnapshotPayload),
    ];

    await publishAndConfirm(publisher, wasm, expected[0] ?? "", 1);
    await withTimeout(feed.waitForCount(1), 10_000, "initial snapshot Deliver");
    await publishAndConfirm(publisher, wasm, expected[1] ?? "", 3);
    await withTimeout(feed.waitForCount(2), 10_000, "first delta Deliver");
    await publishAndConfirm(publisher, wasm, expected[2] ?? "", 5);
    await withTimeout(feed.waitForCount(3), 10_000, "gap delta Deliver");

    assert.deepEqual(feed.envelopes.slice(0, 3).map(kindOf), ["snapshot", "delta", "delta"]);
    assert.equal(await source.requestSnapshot(), expected[0]);

    let duplicateSettled = false;
    const duplicate = source.requestSnapshot().then((snapshot) => {
      duplicateSettled = true;
      return snapshot;
    });
    await Promise.resolve();
    await Promise.resolve();
    assert.equal(duplicateSettled, false, "same installed snapshot must not churn resync state");

    await publishAndConfirm(publisher, wasm, expected[3] ?? "", 7);
    await withTimeout(feed.waitForCount(4), 10_000, "generation-bump snapshot Deliver");
    assert.equal(await withTimeout(duplicate, 10_000, "strictly newer cached snapshot"), expected[3]);

    assert.equal(feed.envelopes.length, 4);
    assert.deepEqual(feed.envelopes, expected, "Deliver payload content must match publisher content");
    assert.deepEqual(
      feed.envelopes.map((value) => encoder.encode(value)),
      expected.map((value) => encoder.encode(value)),
      "received UTF-8 bytes must be identical to the RFC-8785 canonical envelopes",
    );
  },
);

async function publishAndConfirm(
  publisher: ProtocolTcpClient,
  wasm: WasmLoadOptions,
  envelopeBytes: string,
  streamId: number,
): Promise<void> {
  const { frame } = await encodePublish(
    {
      connectionId: 0,
      channel: DEFAULT_FEED_CHANNEL,
      payload: encoder.encode(envelopeBytes),
      streamId,
    },
    wasm,
  );
  const reply = await publisher.exchange(frame);
  const acknowledgement = await decodeFrame({ connectionId: 0, frame: reply }, wasm);
  assert.equal(acknowledgement.frameType, PUBLISH_ACK, "TCP publish must receive PublishAck");
}

function envelope(kind: "snapshot" | "delta", generation: number, seq: number, payload: string): string {
  return `{"componentId":"graph-view-demo","contractId":"frame:graph-view@v1","generation":${generation},"kind":"${kind}","payload":${JSON.stringify(payload)},"seq":${seq}}`;
}

function kindOf(envelopeBytes: string): unknown {
  return (JSON.parse(envelopeBytes) as { readonly kind?: unknown }).kind;
}
