import assert from "node:assert/strict";
import { test } from "node:test";

import {
  accept,
  defer,
  fromTransportResult,
  isDefer,
  kindForState,
  reject,
  withPressure,
} from "../src/pressure.js";
import type { PressureResponse } from "../src/pressure.js";
import { createChannel } from "../src/channel.js";
import type { ChannelTransport, PublishResult } from "../src/channel.js";

test("constructors build the three discriminated variants", () => {
  assert.deepEqual(accept(), { kind: "accept" });
  assert.deepEqual(defer(250), { kind: "defer", delay: 250 });
  assert.deepEqual(defer(250, "buffered"), {
    kind: "defer",
    delay: 250,
    reason: "buffered",
  });
  assert.deepEqual(reject("overwhelmed"), {
    kind: "reject",
    reason: "overwhelmed",
  });
});

test("defer clamps negative or non-finite delays to zero", () => {
  assert.equal(defer(-5).delay, 0);
  assert.equal(defer(Number.NaN).delay, 0);
});

test("discriminated union narrows on kind", () => {
  const response: PressureResponse = defer(100);
  if (response.kind === "defer") {
    // Compiles only because narrowing exposes `delay`.
    assert.equal(response.delay, 100);
  } else {
    assert.fail("expected defer");
  }
});

test("kindForState maps wire pressure state to producer kind", () => {
  assert.equal(kindForState("normal"), "accept");
  assert.equal(kindForState("deferred"), "defer");
  assert.equal(kindForState("rejecting"), "reject");
});

test("fromTransportResult never converts defer/reject to accept", () => {
  assert.deepEqual(fromTransportResult({ kind: "defer", delay: 12 }), {
    kind: "defer",
    delay: 12,
  });
  assert.deepEqual(fromTransportResult({ state: "rejecting", reason: "full" }), {
    kind: "reject",
    reason: "full",
  });
  // Gleam/FFI tag shapes.
  assert.equal(
    fromTransportResult({ Defer: true, delay_millis: 7 }).kind,
    "defer",
  );
  assert.equal(fromTransportResult({ Reject: true, reason: "x" }).kind, "reject");
});

test("fromTransportResult treats an absent signal as a plain ack (accept)", () => {
  assert.deepEqual(fromTransportResult(undefined), { kind: "accept" });
  assert.deepEqual(fromTransportResult({}), { kind: "accept" });
});

test("withPressure maps a channel publish result into a PressureResponse", async () => {
  const transport: ChannelTransport = {
    async publish(): Promise<PublishResult> {
      return { state: "deferred", delay: 30 };
    },
    subscribe(): AsyncIterable<unknown> {
      return (async function* () {})();
    },
    async requestReply(): Promise<unknown> {
      return undefined;
    },
  };
  const channel = createChannel<{ id: number }>({
    name: "orders",
    schema: { type: "object", properties: { id: { type: "integer" } } },
    transport,
  });
  const pressured = withPressure(channel);
  const response = await pressured.publish({ id: 1 });
  assert.ok(isDefer(response));
  assert.equal(response.kind === "defer" ? response.delay : -1, 30);
});
