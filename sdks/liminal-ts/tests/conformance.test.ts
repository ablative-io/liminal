import assert from "node:assert/strict";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { test } from "node:test";

import { createChannel } from "../src/channel.js";
import type { ChannelTransport, JsonSchema, PublishResult } from "../src/channel.js";
import { Connection } from "../src/connection.js";
import type { ConnectionTransport } from "../src/connection.js";
import { openConversation } from "../src/conversation.js";
import type {
  ConversationEvent,
  ConversationTransport,
} from "../src/conversation.js";
import { withPressure } from "../src/pressure.js";
import type { PressureResponse } from "../src/pressure.js";

interface ScenarioSuite {
  readonly scenarios: readonly Scenario[];
}

interface Scenario {
  readonly name: string;
  readonly expected: ObservedValue;
}

type ObservedValue =
  | null
  | boolean
  | number
  | string
  | readonly ObservedValue[]
  | { readonly [key: string]: ObservedValue };

interface ScenarioResult {
  readonly scenario: string;
  readonly pass: boolean;
  readonly expected: ObservedValue;
  readonly observed: ObservedValue;
}

interface Message {
  readonly id: number;
}

test("typescript SDK conformance scenarios match shared expectations", async () => {
  const suite = loadScenarios();
  const results: ScenarioResult[] = [];

  for (const scenario of suite.scenarios) {
    const observed = await observeScenario(scenario.name);
    const pass = deepEqual(observed, scenario.expected);
    results.push({
      scenario: scenario.name,
      pass,
      expected: scenario.expected,
      observed,
    });
    assert.deepEqual(observed, scenario.expected, scenario.name);
  }

  const output = { sdk: "typescript", results };
  const text = JSON.stringify(output, null, 2);
  console.log(text);
  writeResultIfRequested("typescript", text);
});

function loadScenarios(): ScenarioSuite {
  const primary = resolve(process.cwd(), "../../tests/conformance/scenarios.json");
  const fallback = resolve(process.cwd(), "tests/conformance/scenarios.json");
  const path = existsSync(primary) ? primary : fallback;
  return JSON.parse(readFileSync(path, "utf8")) as ScenarioSuite;
}

async function observeScenario(name: string): Promise<ObservedValue> {
  switch (name) {
    case "connection.normal_connect":
      return observeNormalConnect();
    case "connection.reconnect_after_drop":
      return observeReconnectAfterDrop();
    case "connection.clean_disconnect":
      return observeCleanDisconnect();
    case "subscription.resume_from_last_acknowledged":
      return observeSubscriptionRecovery();
    case "backpressure.publish_variants":
      return observeBackpressureVariants();
    case "conversation.open_message_close":
      return observeConversationLifecycle();
    default:
      throw new Error(`unknown conformance scenario ${name}`);
  }
}

async function observeNormalConnect(): Promise<ObservedValue> {
  const connection = new Connection(successfulTransport(), deterministicConfig());
  const states: string[] = [];
  connection.onStateChange((change) => states.push(change.current));

  await connection.connect();

  return {
    state_transitions: states,
    final_state: connection.currentState,
  };
}

async function observeReconnectAfterDrop(): Promise<ObservedValue> {
  const connection = new Connection(successfulTransport(), deterministicConfig());
  const states: string[] = [];
  const attempts: number[] = [];
  connection.onStateChange((change) => {
    states.push(change.current);
    if (change.current === "reconnecting" && change.attempt !== undefined) {
      attempts.push(change.attempt);
    }
  });

  await connection.connect();
  await connection.handleDrop(new Error("link dropped"));

  return {
    state_transitions: states,
    final_state: connection.currentState,
    reconnect_attempts: attempts,
  };
}

async function observeCleanDisconnect(): Promise<ObservedValue> {
  const connection = new Connection(successfulTransport(), deterministicConfig());
  const states: string[] = [];
  connection.onStateChange((change) => states.push(change.current));

  await connection.connect();
  await connection.close();

  return {
    state_transitions: states,
    final_state: connection.currentState,
    // Read the disconnect reason from the SDK rather than asserting a literal,
    // so this scenario genuinely constrains close()'s observable behaviour.
    disconnect_reason: connection.lastDisconnectReason ?? "none",
  };
}

async function observeSubscriptionRecovery(): Promise<ObservedValue> {
  const connection = new Connection(successfulTransport(), deterministicConfig());
  await connection.connect();
  const cursor = connection.registerSubscription("orders");
  connection.acknowledge("orders", 5);

  return {
    subscription: cursor.channel,
    last_acknowledged_sequence: cursor.lastAckedSequence,
    from_sequence: cursor.lastAckedSequence + 1,
  };
}

async function observeBackpressureVariants(): Promise<ObservedValue> {
  const responses: PublishResult[] = [
    { kind: "accept" },
    { kind: "defer", delay: 250 },
    { kind: "reject", reason: "consumer overloaded" },
  ];
  const transport = scriptedChannelTransport(responses);
  const channel = withPressure(
    createChannel<Message>({
      name: "orders",
      schema: messageSchema,
      transport,
    }),
  );

  const observed: ObservedValue[] = [];
  observed.push(normalizePressure(await channel.publish({ id: 1 })));
  observed.push(normalizePressure(await channel.publish({ id: 2 })));
  observed.push(normalizePressure(await channel.publish({ id: 3 })));

  return { responses: observed };
}

function normalizePressure(response: PressureResponse): ObservedValue {
  switch (response.kind) {
    case "accept":
      return { kind: "accept" };
    case "defer":
      return { kind: "defer", delay: response.delay };
    case "reject":
      return { kind: "reject", reason: response.reason };
  }
}

async function observeConversationLifecycle(): Promise<ObservedValue> {
  const transport = scriptedConversationTransport([{ id: 1 }]);
  const conversation = openConversation<Message>({ id: "conv-1", transport });
  const lifecycle = collectKinds(conversation.lifecycle());

  await conversation.send({ id: 0 });
  for await (const _message of conversation.receive()) {
    // Drive the receive loop to completion through the public async iterable.
  }

  return { events: await lifecycle };
}

function successfulTransport(): ConnectionTransport {
  return {
    async open(): Promise<void> {},
    async close(): Promise<void> {},
  };
}

function deterministicConfig(): {
  readonly baseDelay: number;
  readonly jitter: number;
  readonly random: () => number;
  readonly sleep: (ms: number) => Promise<void>;
} {
  return {
    baseDelay: 1,
    jitter: 0,
    random: () => 0,
    sleep: async (ms) => {
      assert.equal(ms, 1);
    },
  };
}

function scriptedChannelTransport(
  responses: readonly PublishResult[],
): ChannelTransport {
  const pending = [...responses];
  return {
    async publish(): Promise<PublishResult> {
      const response = pending.shift();
      assert.notEqual(response, undefined);
      return response;
    },
    subscribe(): AsyncIterable<unknown> {
      return (async function* emptyMessages() {})();
    },
    async requestReply(): Promise<unknown> {
      return undefined;
    },
  };
}

function scriptedConversationTransport(
  messages: readonly Message[],
): ConversationTransport<Message> {
  const sent: Message[] = [];
  return {
    async send(message: Message): Promise<void> {
      sent.push(message);
    },
    async *receive(): AsyncIterable<Message> {
      for (const message of messages) {
        yield message;
      }
    },
    async close(): Promise<void> {},
  };
}

async function collectKinds<T>(
  events: AsyncIterable<ConversationEvent<T>>,
): Promise<string[]> {
  const kinds: string[] = [];
  for await (const event of events) {
    kinds.push(event.kind);
  }
  return kinds;
}

function writeResultIfRequested(sdk: string, text: string): void {
  const directory = process.env.CONFORMANCE_RESULTS_DIR;
  if (directory === undefined) {
    return;
  }
  mkdirSync(directory, { recursive: true });
  writeFileSync(resolve(directory, `${sdk}.json`), text);
}

function deepEqual(left: ObservedValue, right: ObservedValue): boolean {
  try {
    assert.deepEqual(left, right);
    return true;
  } catch {
    return false;
  }
}

const messageSchema = {
  type: "object",
  required: ["id"],
  properties: {
    id: { type: "integer" },
  },
} satisfies JsonSchema;
