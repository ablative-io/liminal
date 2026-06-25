import assert from "node:assert/strict";
import { test } from "node:test";

import { generate } from "../src/codegen.js";
import type { ChannelDefinition } from "../src/codegen.js";

test("generates an interface with mapped field types from a schema", () => {
  const definition: ChannelDefinition = {
    channel: "orders",
    version: "1.0.0",
    schema: {
      type: "object",
      required: ["name", "count"],
      properties: {
        name: { type: "string" },
        count: { type: "integer" },
      },
    },
  };
  const source = generate([definition], { banner: false });
  assert.match(source, /export interface Orders \{/);
  assert.match(source, /name: string;/);
  assert.match(source, /count: number;/);
  // integer maps to number, matching the brief's acceptance example.
  assert.doesNotMatch(source, /count: integer/);
});

test("records source channel name and schema version in JSDoc", () => {
  const source = generate([
    {
      channel: "telemetry",
      version: "2.3.1",
      schema: { type: "object", properties: { ok: { type: "boolean" } } },
    },
  ]);
  assert.match(source, /Generated from liminal channel `telemetry`/);
  assert.match(source, /Schema version: 2\.3\.1/);
});

test("optional fields are marked when not in required", () => {
  const source = generate([
    {
      channel: "user",
      schema: {
        type: "object",
        required: ["id"],
        properties: {
          id: { type: "string" },
          nickname: { type: "string" },
        },
      },
    },
  ]);
  assert.match(source, /id: string;/);
  assert.match(source, /nickname\?: string;/);
});

test("maps arrays, nested objects, enums and PascalCases the channel name", () => {
  const source = generate([
    {
      channel: "order-events",
      schema: {
        type: "object",
        properties: {
          tags: { type: "array", items: { type: "string" } },
          status: { type: "string", enum: ["open", "closed"] },
          meta: {
            type: "object",
            properties: { region: { type: "string" } },
          },
        },
      },
    },
  ]);
  assert.match(source, /export interface OrderEvents \{/);
  assert.match(source, /tags\?: string\[\];/);
  assert.match(source, /status\?: "open" \| "closed";/);
  assert.match(source, /meta\?: \{/);
  assert.match(source, /region\?: string;/);
});

test("produces a type alias for a non-object schema", () => {
  const source = generate(
    [{ channel: "tick", schema: { type: "number" } }],
    { banner: false },
  );
  assert.match(source, /export type Tick = number;/);
});
