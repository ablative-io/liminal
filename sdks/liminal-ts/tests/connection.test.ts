import assert from "node:assert/strict";
import { test } from "node:test";

import { Connection, backoffDelay } from "../src/connection.js";
import type {
  ConnectionStateChange,
  ConnectionTransport,
} from "../src/connection.js";

test("backoffDelay grows exponentially and is capped at maxDelay", () => {
  const config = { baseDelay: 100, maxDelay: 1000, jitter: 0, random: () => 0 };
  assert.equal(backoffDelay(0, config), 100);
  assert.equal(backoffDelay(1, config), 200);
  assert.equal(backoffDelay(2, config), 400);
  assert.equal(backoffDelay(3, config), 800);
  // 100 * 2^4 = 1600 -> capped at 1000.
  assert.equal(backoffDelay(4, config), 1000);
  assert.equal(backoffDelay(10, config), 1000);
});

test("backoffDelay adds jitter bounded by jitter fraction of the base", () => {
  const config = { baseDelay: 100, maxDelay: 10_000, jitter: 0.5, random: () => 1 };
  // base 100 + random(1) * (100 * 0.5) = 150.
  assert.equal(backoffDelay(0, config), 150);
});

test("connect transitions disconnected -> connecting -> connected", async () => {
  const transport: ConnectionTransport = {
    async open(): Promise<void> {},
    async close(): Promise<void> {},
  };
  const connection = new Connection(transport);
  const states: string[] = [];
  connection.onStateChange((change) => states.push(change.current));
  await connection.connect();
  assert.deepEqual(states, ["connecting", "connected"]);
  assert.equal(connection.currentState, "connected");
});

test("a failing open enters reconnecting (exponential, not fixed) then connects", async () => {
  let attempts = 0;
  const transport: ConnectionTransport = {
    async open(): Promise<void> {
      attempts += 1;
      if (attempts < 3) {
        throw new Error("refused");
      }
    },
    async close(): Promise<void> {},
  };
  const delays: number[] = [];
  const connection = new Connection(transport, {
    baseDelay: 10,
    maxDelay: 1000,
    jitter: 0,
    random: () => 0,
    sleep: async (ms) => {
      delays.push(ms);
    },
  });
  const changes: ConnectionStateChange[] = [];
  connection.onStateChange((change) => changes.push(change));

  await connection.connect();

  assert.equal(connection.currentState, "connected");
  // First open fails -> two reconnecting attempts (idx 0, 1) before success.
  assert.deepEqual(delays, [10, 20]);
  const reconnecting = changes.filter((c) => c.current === "reconnecting");
  assert.deepEqual(
    reconnecting.map((c) => c.attempt),
    [0, 1],
  );
});

test("reconnection gives up after maxAttempts and ends disconnected", async () => {
  const transport: ConnectionTransport = {
    async open(): Promise<void> {
      throw new Error("always down");
    },
    async close(): Promise<void> {},
  };
  const connection = new Connection(transport, {
    baseDelay: 1,
    maxAttempts: 3,
    jitter: 0,
    random: () => 0,
    sleep: async () => {},
  });
  await assert.rejects(() => connection.connect());
  assert.equal(connection.currentState, "disconnected");
});

test("subscriptions resume from their last acknowledged sequence", async () => {
  const transport: ConnectionTransport = {
    async open(): Promise<void> {},
    async close(): Promise<void> {},
  };
  const connection = new Connection(transport);
  await connection.connect();
  const cursor = connection.registerSubscription("orders");
  assert.equal(cursor.lastAckedSequence, -1);
  connection.acknowledge("orders", 5);
  connection.acknowledge("orders", 3); // non-monotonic, ignored
  assert.equal(cursor.lastAckedSequence, 5);
  const resumed = connection.resumeCursors();
  assert.equal(resumed.length, 1);
  assert.equal(resumed[0]?.lastAckedSequence, 5);
});
