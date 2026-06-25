import assert from "node:assert/strict";
import { test } from "node:test";

import { openConversation } from "../src/conversation.js";
import type {
  ConversationEvent,
  ConversationTransport,
} from "../src/conversation.js";

interface Msg {
  readonly text: string;
}

function scriptedTransport(messages: readonly Msg[]): {
  transport: ConversationTransport<Msg>;
  sent: Msg[];
  closed: { value: boolean };
} {
  const sent: Msg[] = [];
  const closed = { value: false };
  const transport: ConversationTransport<Msg> = {
    async send(message: Msg): Promise<void> {
      sent.push(message);
    },
    async *receive(): AsyncIterable<Msg> {
      for (const message of messages) {
        yield message;
      }
    },
    async close(): Promise<void> {
      closed.value = true;
    },
  };
  return { transport, sent, closed };
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

test("lifecycle emits opened -> message* -> closing -> closed on completion", async () => {
  const { transport } = scriptedTransport([{ text: "a" }, { text: "b" }]);
  const convo = openConversation<Msg>({ id: "c1", transport });
  const lifecycle = collectKinds(convo.lifecycle());

  const received: Msg[] = [];
  for await (const message of convo.receive()) {
    received.push(message);
  }

  assert.deepEqual(received, [{ text: "a" }, { text: "b" }]);
  assert.deepEqual(await lifecycle, [
    "opened",
    "message",
    "message",
    "closing",
    "closed",
  ]);
});

test("close() emits closing then closed with a closed reason", async () => {
  const { transport, closed } = scriptedTransport([]);
  const convo = openConversation<Msg>({ id: "c2", transport });
  const events: ConversationEvent<Msg>[] = [];
  const drain = (async () => {
    for await (const event of convo.lifecycle()) {
      events.push(event);
    }
  })();

  await convo.close();
  await drain;

  assert.equal(closed.value, true);
  const terminal = events.at(-1);
  assert.equal(terminal?.kind, "closed");
  assert.equal(
    terminal?.kind === "closed" ? terminal.reason.kind : "?",
    "closed",
  );
});

test("send after close is rejected and never silently dropped", async () => {
  const { transport } = scriptedTransport([]);
  const convo = openConversation<Msg>({ id: "c3", transport });
  await convo.close();
  await assert.rejects(() => convo.send({ text: "late" }));
});

test("a transport receive error emits error before closed with a typed reason", async () => {
  const transport: ConversationTransport<Msg> = {
    async send(): Promise<void> {},
    async *receive(): AsyncIterable<Msg> {
      yield { text: "ok" };
      throw new Error("link reset");
    },
  };
  const convo = openConversation<Msg>({ id: "c4", transport });
  const events: ConversationEvent<Msg>[] = [];
  const drain = (async () => {
    for await (const event of convo.lifecycle()) {
      events.push(event);
    }
  })();

  await assert.rejects(async () => {
    for await (const _ of convo.receive()) {
      // drive the receive loop until it throws
    }
  });
  await drain;

  const kinds = events.map((event) => event.kind);
  assert.deepEqual(kinds, ["opened", "message", "error", "closing", "closed"]);
  const errorEvent = events.find((event) => event.kind === "error");
  assert.equal(
    errorEvent?.kind === "error" ? errorEvent.reason.message : "?",
    "link reset",
  );
});

test("a late lifecycle subscriber still observes every event in order", async () => {
  const { transport } = scriptedTransport([{ text: "x" }]);
  const convo = openConversation<Msg>({ id: "c5", transport });
  // Drive to completion first, then subscribe.
  for await (const _ of convo.receive()) {
    // consume
  }
  const kinds = await collectKinds(convo.lifecycle());
  assert.deepEqual(kinds, ["opened", "message", "closing", "closed"]);
});
