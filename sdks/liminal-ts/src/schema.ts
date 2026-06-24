export type JsonSchema = boolean | { readonly [keyword: string]: unknown };

export interface ValidationIssue {
  readonly path: string;
  readonly message: string;
  readonly keyword?: string;
}

export interface ValidationResult {
  readonly valid: boolean;
  readonly errors: readonly ValidationIssue[];
}

export type SchemaValidator = (
  value: unknown,
  schema: JsonSchema,
) => boolean | ValidationResult;

export function validateJsonSchema(value: unknown, schema: JsonSchema): ValidationResult {
  const errors = validateAgainstSchema(value, schema, "$", "schema");
  return { valid: errors.length === 0, errors };
}

function validateAgainstSchema(
  value: unknown,
  schema: JsonSchema,
  path: string,
  keyword: string,
): ValidationIssue[] {
  if (typeof schema === "boolean") {
    return schema ? [] : [issue(path, "boolean schema rejected value", keyword)];
  }
  if (!isRecord(schema)) {
    return [issue(path, "schema must be a JSON Schema object or boolean", keyword)];
  }

  const errors: ValidationIssue[] = [];
  const expectedTypes = readStringList(schema.type);
  if (schema.type !== undefined && expectedTypes.length === 0) {
    errors.push(issue(path, "schema type must be a string or string array", "type"));
  } else if (expectedTypes.length > 0 && !expectedTypes.some((type) => matchesType(value, type))) {
    errors.push(issue(path, `expected ${expectedTypes.join(" or ")}`, "type"));
    return errors;
  }

  validateEnumAndConst(value, schema, path, errors);
  validateString(value, schema, path, errors);
  validateNumber(value, schema, path, errors);
  validateArray(value, schema, path, errors);
  validateObject(value, schema, path, errors);
  validateCombinators(value, schema, path, errors);
  return errors;
}

function validateEnumAndConst(
  value: unknown,
  schema: Record<string, unknown>,
  path: string,
  errors: ValidationIssue[],
): void {
  if (Array.isArray(schema.enum) && !schema.enum.some((entry) => deepEqual(value, entry))) {
    errors.push(issue(path, "value is not one of the allowed enum entries", "enum"));
  }
  if (Object.hasOwn(schema, "const") && !deepEqual(value, schema.const)) {
    errors.push(issue(path, "value does not match const", "const"));
  }
}

function validateString(
  value: unknown,
  schema: Record<string, unknown>,
  path: string,
  errors: ValidationIssue[],
): void {
  if (typeof value !== "string") {
    return;
  }
  const minLength = readNumber(schema.minLength);
  const maxLength = readNumber(schema.maxLength);
  if (minLength !== undefined && value.length < minLength) {
    errors.push(issue(path, `string is shorter than ${minLength}`, "minLength"));
  }
  if (maxLength !== undefined && value.length > maxLength) {
    errors.push(issue(path, `string is longer than ${maxLength}`, "maxLength"));
  }
  if (typeof schema.pattern === "string" && !matchesPattern(value, schema.pattern)) {
    errors.push(issue(path, "string does not match pattern", "pattern"));
  }
}

function validateNumber(
  value: unknown,
  schema: Record<string, unknown>,
  path: string,
  errors: ValidationIssue[],
): void {
  if (typeof value !== "number") {
    return;
  }
  const minimum = readNumber(schema.minimum);
  const maximum = readNumber(schema.maximum);
  if (minimum !== undefined && value < minimum) {
    errors.push(issue(path, `number is less than ${minimum}`, "minimum"));
  }
  if (maximum !== undefined && value > maximum) {
    errors.push(issue(path, `number is greater than ${maximum}`, "maximum"));
  }
}

function validateArray(
  value: unknown,
  schema: Record<string, unknown>,
  path: string,
  errors: ValidationIssue[],
): void {
  if (!Array.isArray(value)) {
    return;
  }
  const minItems = readNumber(schema.minItems);
  const maxItems = readNumber(schema.maxItems);
  if (minItems !== undefined && value.length < minItems) {
    errors.push(issue(path, `array has fewer than ${minItems} items`, "minItems"));
  }
  if (maxItems !== undefined && value.length > maxItems) {
    errors.push(issue(path, `array has more than ${maxItems} items`, "maxItems"));
  }
  if (isJsonSchema(schema.items)) {
    const itemSchema = schema.items;
    value.forEach((item, index) => {
      errors.push(...validateAgainstSchema(item, itemSchema, `${path}/${index}`, "items"));
    });
  }
}

function validateObject(
  value: unknown,
  schema: Record<string, unknown>,
  path: string,
  errors: ValidationIssue[],
): void {
  if (!isRecord(value)) {
    return;
  }
  const properties = isRecord(schema.properties) ? schema.properties : {};
  for (const name of readStringList(schema.required)) {
    if (!Object.hasOwn(value, name)) {
      errors.push(issue(`${path}/${name}`, "required property is missing", "required"));
    }
  }
  for (const [name, childSchema] of Object.entries(properties)) {
    if (Object.hasOwn(value, name) && isJsonSchema(childSchema)) {
      errors.push(...validateAgainstSchema(value[name], childSchema, `${path}/${name}`, "properties"));
    }
  }
  validateAdditionalProperties(value, properties, schema.additionalProperties, path, errors);
}

function validateAdditionalProperties(
  value: Record<string, unknown>,
  properties: Record<string, unknown>,
  additionalProperties: unknown,
  path: string,
  errors: ValidationIssue[],
): void {
  for (const key of Object.keys(value)) {
    if (Object.hasOwn(properties, key)) {
      continue;
    }
    if (additionalProperties === false) {
      errors.push(issue(`${path}/${key}`, "additional property is not allowed", "additionalProperties"));
    } else if (isJsonSchema(additionalProperties) && additionalProperties !== true) {
      errors.push(...validateAgainstSchema(value[key], additionalProperties, `${path}/${key}`, "additionalProperties"));
    }
  }
}

function validateCombinators(
  value: unknown,
  schema: Record<string, unknown>,
  path: string,
  errors: ValidationIssue[],
): void {
  for (const child of readSchemaList(schema.allOf)) {
    errors.push(...validateAgainstSchema(value, child, path, "allOf"));
  }
  const anyOf = readSchemaList(schema.anyOf);
  if (anyOf.length > 0 && !anyOf.some((child) => validateAgainstSchema(value, child, path, "anyOf").length === 0)) {
    errors.push(issue(path, "value does not match any anyOf schema", "anyOf"));
  }
  const oneOf = readSchemaList(schema.oneOf);
  if (oneOf.length > 0) {
    const matches = oneOf.filter((child) => validateAgainstSchema(value, child, path, "oneOf").length === 0);
    if (matches.length !== 1) {
      errors.push(issue(path, "value must match exactly one oneOf schema", "oneOf"));
    }
  }
}

function matchesType(value: unknown, type: string): boolean {
  switch (type) {
    case "array":
      return Array.isArray(value);
    case "boolean":
      return typeof value === "boolean";
    case "integer":
      return typeof value === "number" && Number.isInteger(value);
    case "null":
      return value === null;
    case "number":
      return typeof value === "number" && Number.isFinite(value);
    case "object":
      return isRecord(value);
    case "string":
      return typeof value === "string";
    default:
      return false;
  }
}

function readStringList(value: unknown): string[] {
  if (typeof value === "string") {
    return [value];
  }
  return Array.isArray(value) && value.every((entry) => typeof entry === "string")
    ? value
    : [];
}

function readSchemaList(value: unknown): JsonSchema[] {
  return Array.isArray(value) ? value.filter(isJsonSchema) : [];
}

function readNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function matchesPattern(value: string, pattern: string): boolean {
  try {
    return new RegExp(pattern).test(value);
  } catch {
    return false;
  }
}

function isJsonSchema(value: unknown): value is JsonSchema {
  return typeof value === "boolean" || isRecord(value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function deepEqual(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) {
    return true;
  }
  if (Array.isArray(left) && Array.isArray(right)) {
    return left.length === right.length && left.every((entry, index) => deepEqual(entry, right[index]));
  }
  if (isRecord(left) && isRecord(right)) {
    const leftKeys = Object.keys(left);
    const rightKeys = Object.keys(right);
    return leftKeys.length === rightKeys.length && leftKeys.every((key) => Object.hasOwn(right, key) && deepEqual(left[key], right[key]));
  }
  return false;
}

function issue(path: string, message: string, keyword?: string): ValidationIssue {
  return keyword === undefined ? { path, message } : { path, message, keyword };
}
