import { validateJsonSchema } from "./schema.js";
import type {
  JsonSchema,
  SchemaValidator,
  ValidationIssue,
  ValidationResult,
} from "./schema.js";

export type {
  JsonSchema,
  SchemaValidator,
  ValidationIssue,
  ValidationResult,
} from "./schema.js";

export type SdkErrorCode =
  | "Connection"
  | "Protocol"
  | "Serialization"
  | "TypeValidation"
  | "Wasm";

export interface ProtocolOperationResult {
  readonly [field: string]: unknown;
}

export type PublishResult = ProtocolOperationResult | undefined;

export interface RequestReplyMetadata {
  readonly requestSchema: JsonSchema;
  readonly responseSchema: JsonSchema;
}

export interface ChannelTransport {
  publish(
    channel: string,
    message: unknown,
    schema: JsonSchema,
  ): Promise<PublishResult>;
  subscribe(channel: string, schema: JsonSchema): AsyncIterable<unknown>;
  requestReply(
    channel: string,
    message: unknown,
    metadata: RequestReplyMetadata,
  ): Promise<unknown>;
}

export interface ChannelConfig<T> {
  readonly name: string;
  readonly schema: JsonSchema;
  readonly validator?: SchemaValidator;
  readonly transport?: ChannelTransport;
}

export interface RequestReplyOptions<Req, Resp> {
  readonly requestSchema?: JsonSchema;
  readonly responseSchema?: JsonSchema;
  readonly requestValidator?: SchemaValidator;
  readonly responseValidator?: SchemaValidator;
}

export interface Channel<T> {
  readonly name: string;
  readonly schema: JsonSchema;
  publish(message: T): Promise<PublishResult>;
  subscribe(): AsyncIterable<T>;
  requestReply<Req = T, Resp = T>(
    message: Req,
    options?: RequestReplyOptions<Req, Resp>,
  ): Promise<Resp>;
}

export class SdkError extends Error {
  readonly code: SdkErrorCode;
  readonly details?: unknown;

  constructor(
    code: SdkErrorCode,
    message: string,
    options: { readonly cause?: unknown; readonly details?: unknown } = {},
  ) {
    super(message, { cause: options.cause });
    this.name = "SdkError";
    this.code = code;
    if (options.details !== undefined) {
      this.details = options.details;
    }
  }
}

export function createChannel<T>(config: ChannelConfig<T>): Channel<T> {
  return new ValidatedChannel(config);
}

export const Channel = Object.freeze({
  create: createChannel,
});

class ValidatedChannel<T> implements Channel<T> {
  readonly name: string;
  readonly schema: JsonSchema;
  private readonly transport: ChannelTransport;
  private readonly validator: SchemaValidator;

  constructor(config: ChannelConfig<T>) {
    this.name = config.name;
    this.schema = config.schema;
    this.validator = config.validator ?? validateJsonSchema;
    this.transport = config.transport ?? missingTransport;
  }

  async publish(message: T): Promise<PublishResult> {
    const payload = validateOrThrow<T>(
      message,
      this.schema,
      this.validator,
      this.name,
      "published message",
    );
    return this.transport.publish(this.name, payload, this.schema);
  }

  async *subscribe(): AsyncIterable<T> {
    for await (const payload of this.transport.subscribe(this.name, this.schema)) {
      yield validateOrThrow<T>(
        payload,
        this.schema,
        this.validator,
        this.name,
        "incoming message",
      );
    }
  }

  async requestReply<Req = T, Resp = T>(
    message: Req,
    options: RequestReplyOptions<Req, Resp> = {},
  ): Promise<Resp> {
    const requestSchema = options.requestSchema ?? this.schema;
    const responseSchema = options.responseSchema ?? this.schema;
    const requestValidator = options.requestValidator ?? this.validator;
    const responseValidator = options.responseValidator ?? this.validator;
    const request = validateOrThrow<Req>(
      message,
      requestSchema,
      requestValidator,
      this.name,
      "request message",
    );
    const response = await this.transport.requestReply(this.name, request, {
      requestSchema,
      responseSchema,
    });
    return validateOrThrow<Resp>(
      response,
      responseSchema,
      responseValidator,
      this.name,
      "response message",
    );
  }
}

function validateOrThrow<T>(
  value: unknown,
  schema: JsonSchema,
  validator: SchemaValidator,
  channel: string,
  label: string,
): T {
  const result = normalizeValidationResult(validator(value, schema));
  if (result.valid) {
    return value as T;
  }
  throw new SdkError("TypeValidation", `${label} failed JSON Schema validation`, {
    details: { channel, errors: result.errors },
  });
}

function normalizeValidationResult(
  result: boolean | ValidationResult,
): ValidationResult {
  if (typeof result === "boolean") {
    return { valid: result, errors: result ? [] : [fallbackIssue()] };
  }
  return result;
}

function fallbackIssue(): ValidationIssue {
  return { path: "$", message: "schema rejected value" };
}

const missingTransport: ChannelTransport = {
  async publish(): Promise<PublishResult> {
    throw missingTransportError();
  },
  subscribe(): AsyncIterable<unknown> {
    return missingTransportStream();
  },
  async requestReply(): Promise<unknown> {
    throw missingTransportError();
  },
};

async function* missingTransportStream(): AsyncIterable<unknown> {
  throw missingTransportError();
}

function missingTransportError(): SdkError {
  return new SdkError(
    "Connection",
    "Channel transport is not configured; connect the SDK before using transport operations",
  );
}
