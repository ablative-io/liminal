/**
 * TypeScript type generator for liminal channel definitions (story S8 / C19).
 *
 * Given one or more channel definitions — a channel name, an optional schema
 * version, and the channel's message {@link JsonSchema} — this module emits
 * TypeScript `interface` source with correct field names, mapped types, and
 * JSDoc comments naming the source channel and schema version. The mapping
 * follows the JSON Schema vocabulary used by `schema.ts`:
 *
 *   string  -> string        integer -> number       number  -> number
 *   boolean -> boolean        null    -> null         array   -> T[]
 *   object  -> nested interface / Record / inline shape
 *
 * Usable both programmatically (`generate(...)`) and as a CLI
 * (`node codegen.js <defs.json> [out.ts]`).
 */

import type { JsonSchema } from "./schema.js";

/** A single channel's type definition fed to the generator. */
export interface ChannelDefinition {
  /** Source channel name, e.g. `"orders"`. */
  readonly channel: string;
  /** Message schema for the channel. */
  readonly schema: JsonSchema;
  /** Optional schema version recorded in the generated JSDoc. */
  readonly version?: string;
  /**
   * Optional explicit interface name. Defaults to a PascalCase rendering of the
   * channel name (e.g. `"order-events"` -> `OrderEvents`).
   */
  readonly typeName?: string;
}

/** Options controlling generated output. */
export interface GenerateOptions {
  /** Indentation unit (default two spaces). */
  readonly indent?: string;
  /** Banner comment emitted once at the top of the file (default on). */
  readonly banner?: boolean;
}

const DEFAULT_INDENT = "  ";

/**
 * Generates TypeScript interface source for the supplied channel definitions.
 *
 * @returns A single TypeScript source string. Each definition yields one
 * exported interface; object properties become fields, and nested objects are
 * inlined as anonymous object types.
 */
export function generate(
  definitions: readonly ChannelDefinition[],
  options: GenerateOptions = {},
): string {
  const indent = options.indent ?? DEFAULT_INDENT;
  const blocks = definitions.map((definition) =>
    renderInterface(definition, indent),
  );
  const body = blocks.join("\n\n");
  if (options.banner === false) {
    return `${body}\n`;
  }
  return `${fileBanner()}\n\n${body}\n`;
}

/** Renders one channel definition into an exported interface block. */
function renderInterface(definition: ChannelDefinition, indent: string): string {
  const name = definition.typeName ?? pascalCase(definition.channel);
  const doc = interfaceDoc(definition);
  const schema = definition.schema;
  if (typeof schema !== "boolean" && isObjectSchema(schema)) {
    const fields = renderObjectFields(schema, indent, indent);
    const bodyText = fields.length === 0 ? "" : `\n${fields.join("\n")}\n`;
    return `${doc}export interface ${name} {${bodyText}}`;
  }
  // Non-object schema (e.g. a tuple/array/scalar message): emit a type alias.
  return `${doc}export type ${name} = ${renderType(schema, indent, "")};`;
}

/** Renders the JSDoc block that precedes a generated interface. */
function interfaceDoc(definition: ChannelDefinition): string {
  const lines = [
    ` * Generated from liminal channel \`${definition.channel}\`.`,
  ];
  if (definition.version !== undefined) {
    lines.push(` * Schema version: ${definition.version}.`);
  }
  const description = readString(definition.schema, "description");
  if (description !== undefined) {
    lines.push(" *", ` * ${description}`);
  }
  return `/**\n${lines.join("\n")}\n */\n`;
}

/** Renders the `name: Type;` field lines of an object schema. */
function renderObjectFields(
  schema: Record<string, unknown>,
  indent: string,
  currentIndent: string,
): string[] {
  const properties = isRecord(schema.properties) ? schema.properties : {};
  const required = new Set(readStringList(schema.required));
  const lines: string[] = [];
  for (const [field, child] of Object.entries(properties)) {
    if (!isJsonSchema(child)) {
      continue;
    }
    const fieldDoc = renderFieldDoc(child, currentIndent);
    if (fieldDoc !== undefined) {
      lines.push(fieldDoc);
    }
    const optional = required.has(field) ? "" : "?";
    const type = renderType(child, indent, currentIndent);
    lines.push(`${currentIndent}${propertyKey(field)}${optional}: ${type};`);
  }
  return lines;
}

/** Emits a single-line JSDoc for a field when the schema has a description. */
function renderFieldDoc(
  schema: JsonSchema,
  currentIndent: string,
): string | undefined {
  const description = readString(schema, "description");
  if (description === undefined) {
    return undefined;
  }
  return `${currentIndent}/** ${description} */`;
}

/** Maps a JSON Schema node to a TypeScript type expression. */
function renderType(
  schema: JsonSchema,
  indent: string,
  currentIndent: string,
): string {
  if (typeof schema === "boolean") {
    return schema ? "unknown" : "never";
  }
  if (!isRecord(schema)) {
    return "unknown";
  }
  const enumType = renderEnum(schema);
  if (enumType !== undefined) {
    return enumType;
  }
  const types = readStringList(schema.type);
  if (types.length > 1) {
    return types.map((type) => renderScalarType(type, schema, indent, currentIndent)).join(" | ");
  }
  const type = types[0];
  if (type === undefined) {
    return "unknown";
  }
  return renderScalarType(type, schema, indent, currentIndent);
}

/** Renders a `const`/`enum` schema as a TypeScript literal union. */
function renderEnum(schema: Record<string, unknown>): string | undefined {
  if (Object.hasOwn(schema, "const")) {
    return literal(schema.const);
  }
  if (Array.isArray(schema.enum) && schema.enum.length > 0) {
    return schema.enum.map(literal).join(" | ");
  }
  return undefined;
}

/** Maps a single JSON Schema `type` keyword to a TypeScript type. */
function renderScalarType(
  type: string,
  schema: Record<string, unknown>,
  indent: string,
  currentIndent: string,
): string {
  switch (type) {
    case "string":
      return "string";
    case "integer":
    case "number":
      return "number";
    case "boolean":
      return "boolean";
    case "null":
      return "null";
    case "array":
      return renderArrayType(schema, indent, currentIndent);
    case "object":
      return renderInlineObject(schema, indent, currentIndent);
    default:
      return "unknown";
  }
}

/** Renders an array schema as `T[]` (or `unknown[]` when items are unknown). */
function renderArrayType(
  schema: Record<string, unknown>,
  indent: string,
  currentIndent: string,
): string {
  if (!isJsonSchema(schema.items)) {
    return "unknown[]";
  }
  const itemType = renderType(schema.items, indent, currentIndent);
  return needsArrayParens(itemType) ? `Array<${itemType}>` : `${itemType}[]`;
}

/** Renders an inline (nested) object schema. */
function renderInlineObject(
  schema: Record<string, unknown>,
  indent: string,
  currentIndent: string,
): string {
  if (!isRecord(schema.properties) || Object.keys(schema.properties).length === 0) {
    if (isJsonSchema(schema.additionalProperties) && schema.additionalProperties !== true) {
      const valueType = renderType(schema.additionalProperties, indent, currentIndent);
      return `Record<string, ${valueType}>`;
    }
    return "Record<string, unknown>";
  }
  const nextIndent = currentIndent + indent;
  const fields = renderObjectFields(schema, indent, nextIndent);
  if (fields.length === 0) {
    return "Record<string, unknown>";
  }
  return `{\n${fields.join("\n")}\n${currentIndent}}`;
}

/** Renders a JSON literal as a TypeScript literal type. */
function literal(value: unknown): string {
  if (typeof value === "string") {
    return JSON.stringify(value);
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (value === null) {
    return "null";
  }
  return "unknown";
}

/** Quotes a property key only when it is not a valid bare identifier. */
function propertyKey(field: string): string {
  return /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(field) ? field : JSON.stringify(field);
}

/** PascalCases a channel name for use as an interface identifier. */
function pascalCase(name: string): string {
  const parts = name.split(/[^A-Za-z0-9]+/).filter((part) => part.length > 0);
  const pascal = parts
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join("");
  return /^[A-Za-z_$]/.test(pascal) ? pascal : `Channel${pascal}`;
}

function needsArrayParens(type: string): boolean {
  return type.includes("|") || type.includes(" ");
}

function isObjectSchema(schema: Record<string, unknown>): boolean {
  const types = readStringList(schema.type);
  if (types.length > 0) {
    return types.includes("object");
  }
  return isRecord(schema.properties);
}

function readString(schema: JsonSchema, key: string): string | undefined {
  if (typeof schema === "boolean" || !isRecord(schema)) {
    return undefined;
  }
  const value = schema[key];
  return typeof value === "string" ? value : undefined;
}

function readStringList(value: unknown): string[] {
  if (typeof value === "string") {
    return [value];
  }
  return Array.isArray(value) && value.every((entry) => typeof entry === "string")
    ? value
    : [];
}

function isJsonSchema(value: unknown): value is JsonSchema {
  return typeof value === "boolean" || isRecord(value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function fileBanner(): string {
  return [
    "/**",
    " * AUTO-GENERATED by the liminal TypeScript SDK type generator.",
    " * Do not edit by hand; regenerate from the channel schema definitions.",
    " */",
  ].join("\n");
}

/**
 * Reads channel definitions from a JSON file and writes generated TypeScript.
 *
 * Usable programmatically and from the CLI wrapper in `codegen-cli.ts`. The
 * input JSON must be a {@link ChannelDefinition} or an array of them; when
 * `outputPath` is omitted the source is written to stdout.
 */
export async function generateFromFile(
  inputPath: string,
  outputPath?: string,
): Promise<string> {
  const { readFile, writeFile } = await import("node:fs/promises");
  const raw = await readFile(inputPath, "utf8");
  const parsed: unknown = JSON.parse(raw);
  const definitions = Array.isArray(parsed)
    ? (parsed as ChannelDefinition[])
    : [parsed as ChannelDefinition];
  const source = generate(definitions);
  if (outputPath !== undefined) {
    await writeFile(outputPath, source, "utf8");
  }
  return source;
}
