export type LiminalFeedSourceErrorCode =
  | "INVALID_ENDPOINT"
  | "INVALID_CHANNEL"
  | "RESERVED_CHANNEL"
  | "ALREADY_SUBSCRIBED"
  | "NOT_SUBSCRIBED"
  | "CONNECTION_FAILED"
  | "CONNECTION_CLOSED"
  | "CONNECT_REJECTED"
  | "SUBSCRIBE_REJECTED"
  | "PUBLISH_REJECTED"
  | "PROTOCOL_ERROR"
  | "WASM_ERROR"
  | "RECEIVER_FAILED"
  | "ERROR_LISTENER_FAILED";

/** Typed failure surfaced by {@link LiminalFeedSource}. */
export class LiminalFeedSourceError extends Error {
  readonly code: LiminalFeedSourceErrorCode;
  readonly details?: unknown;

  constructor(
    code: LiminalFeedSourceErrorCode,
    message: string,
    options: { readonly cause?: unknown; readonly details?: unknown } = {},
  ) {
    super(message, { cause: options.cause });
    this.name = "LiminalFeedSourceError";
    this.code = code;
    if (options.details !== undefined) this.details = options.details;
  }
}

export function feedError(
  code: LiminalFeedSourceErrorCode,
  message: string,
  cause?: unknown,
  details?: unknown,
): LiminalFeedSourceError {
  return new LiminalFeedSourceError(code, message, { cause, details });
}
