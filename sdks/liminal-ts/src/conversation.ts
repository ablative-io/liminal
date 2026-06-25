/**
 * Conversation lifecycle for the TypeScript SDK.
 *
 * Per ADR-001 the conversation — not the message — is the fundamental unit: a
 * supervised exchange between parties that mediates lifecycle and failure
 * handling. This module mirrors the Rust `Conversation` model
 * (`crates/liminal/src/conversation/`) and the protocol lifecycle ordering in
 * `crates/liminal/src/protocol/lifecycle.rs`, exposing it as a TypeScript-
 * idiomatic handle with an observable lifecycle stream.
 *
 * It is kept aligned with the Gleam SDK's conversation contract
 * (`sdks/liminal-gleam/src/liminal/conversation.gleam`): the `TerminateReason`
 * variants (`Completed` / `Closed` / `TimedOut` / `Failed`) map onto the typed
 * close/error reasons here.
 */

import { SdkError } from "./channel.js";

/**
 * Reason a conversation terminated, mirroring the Gleam `TerminateReason`.
 *
 * - `completed` — the exchange finished normally.
 * - `closed` — the local side closed the conversation.
 * - `timed-out` — the conversation exceeded its bound and the runtime closed it.
 * - `failed` — the conversation failed with an error (see {@link ConversationError}).
 */
export type TerminateReason =
  | { readonly kind: "completed" }
  | { readonly kind: "closed" }
  | { readonly kind: "timed-out" }
  | { readonly kind: "failed"; readonly error: ConversationError };

/**
 * Typed reason carried by an `error` lifecycle event.
 *
 * Mirrors `LiminalError` failure surfaces relevant to a conversation
 * (participant crash, timeout, transport/protocol failure) without coupling the
 * SDK to the full Rust error enum.
 */
export interface ConversationError {
  /** Stable machine-readable failure category. */
  readonly code: ConversationErrorCode;
  /** Human-readable description of the failure. */
  readonly message: string;
  /** Underlying cause, when one is available. */
  readonly cause?: unknown;
}

/** Machine-readable conversation failure categories. */
export type ConversationErrorCode =
  | "ParticipantCrashed"
  | "Timeout"
  | "Transport"
  | "Protocol"
  | "Application";

/**
 * Lifecycle event emitted by a {@link Conversation}.
 *
 * Discriminated by `kind`. The runtime guarantees the ordering
 * `opened -> message* -> closing -> closed`, with an optional `error` emitted
 * (carrying a typed reason) immediately before `closed` on failure. Lifecycle
 * transitions are never silently dropped.
 */
export type ConversationEvent<T> =
  | { readonly kind: "opened"; readonly conversationId: string }
  | { readonly kind: "message"; readonly message: T }
  | { readonly kind: "closing"; readonly reason: TerminateReason }
  | { readonly kind: "closed"; readonly reason: TerminateReason }
  | { readonly kind: "error"; readonly reason: ConversationError };

/** Discriminants of {@link ConversationEvent}. */
export type ConversationEventKind = ConversationEvent<unknown>["kind"];

/** Transport that drives a single conversation's send/receive/close. */
export interface ConversationTransport<T> {
  /** Sends a message into the conversation. */
  send(message: T): Promise<void>;
  /** Yields inbound messages until the conversation ends. */
  receive(): AsyncIterable<T>;
  /** Closes the conversation; resolves once the close is acknowledged. */
  close?(): Promise<void>;
}

/** Configuration for {@link openConversation}. */
export interface ConversationConfig<T> {
  /** Stable identifier for the conversation (the conversation span name). */
  readonly id: string;
  /** Transport driving message flow for this conversation. */
  readonly transport: ConversationTransport<T>;
}

/**
 * Handle to an open conversation.
 *
 * `T` is the message payload type. The handle exposes the
 * `send` / `receive` / `close` triad plus an observable `lifecycle` stream.
 */
export interface Conversation<T> {
  /** Stable conversation identifier. */
  readonly id: string;
  /** Sends a message into the conversation. */
  send(message: T): Promise<void>;
  /** Async iterable of inbound messages. */
  receive(): AsyncIterable<T>;
  /** Closes the conversation, emitting `closing` then `closed`. */
  close(): Promise<void>;
  /** Observable lifecycle event stream, in transition order. */
  lifecycle(): AsyncIterable<ConversationEvent<T>>;
}

/**
 * Opens a conversation over `config.transport` and returns a {@link Conversation}
 * handle. The `opened` lifecycle event is emitted synchronously on open; the
 * receive loop drives `message` events, and `close()` / failures drive the
 * terminal `closing` -> (`error`?) -> `closed` transitions.
 */
export function openConversation<T>(
  config: ConversationConfig<T>,
): Conversation<T> {
  return new TransportConversation(config);
}

/**
 * Drives one conversation's lifecycle and fans events out to any number of
 * concurrent `lifecycle()` consumers via an async broadcast queue.
 */
class TransportConversation<T> implements Conversation<T> {
  readonly id: string;
  private readonly transport: ConversationTransport<T>;
  private readonly events = new EventChannel<ConversationEvent<T>>();
  private receiving = false;
  private terminated = false;

  constructor(config: ConversationConfig<T>) {
    this.id = config.id;
    this.transport = config.transport;
    this.events.push({ kind: "opened", conversationId: this.id });
  }

  async send(message: T): Promise<void> {
    if (this.terminated) {
      throw new SdkError(
        "Protocol",
        "cannot send on a closed conversation",
        { details: { conversationId: this.id } },
      );
    }
    try {
      await this.transport.send(message);
    } catch (cause) {
      await this.fail(toConversationError(cause, "Transport"));
      throw cause;
    }
  }

  async *receive(): AsyncIterable<T> {
    if (this.receiving) {
      throw new SdkError(
        "Protocol",
        "conversation receive() may only be consumed once",
        { details: { conversationId: this.id } },
      );
    }
    this.receiving = true;
    try {
      for await (const message of this.transport.receive()) {
        this.events.push({ kind: "message", message });
        yield message;
      }
    } catch (cause) {
      await this.fail(toConversationError(cause, "Transport"));
      throw cause;
    }
    await this.finish({ kind: "completed" });
  }

  async close(): Promise<void> {
    await this.terminate({ kind: "closed" }, async () => {
      await this.transport.close?.();
    });
  }

  lifecycle(): AsyncIterable<ConversationEvent<T>> {
    return this.events.subscribe();
  }

  /** Emits the terminal sequence for a normal (non-error) completion. */
  private async finish(reason: TerminateReason): Promise<void> {
    await this.terminate(reason);
  }

  /** Emits `error` then the terminal `closing`/`closed` for a failure. */
  private async fail(error: ConversationError): Promise<void> {
    if (this.terminated) {
      return;
    }
    this.events.push({ kind: "error", reason: error });
    await this.terminate({ kind: "failed", error });
  }

  /**
   * Runs the terminal transition exactly once: `closing` -> (action) ->
   * `closed`. Subsequent calls are no-ops so transitions are never duplicated
   * or dropped.
   */
  private async terminate(
    reason: TerminateReason,
    action?: () => Promise<void>,
  ): Promise<void> {
    if (this.terminated) {
      return;
    }
    this.terminated = true;
    this.events.push({ kind: "closing", reason });
    if (action !== undefined) {
      try {
        await action();
      } catch (cause) {
        const error = toConversationError(cause, "Transport");
        this.events.push({ kind: "error", reason: error });
        const failedReason: TerminateReason = { kind: "failed", error };
        this.events.push({ kind: "closed", reason: failedReason });
        this.events.close();
        return;
      }
    }
    this.events.push({ kind: "closed", reason });
    this.events.close();
  }
}

/** Converts an arbitrary thrown value into a typed {@link ConversationError}. */
function toConversationError(
  cause: unknown,
  fallback: ConversationErrorCode,
): ConversationError {
  if (cause instanceof SdkError) {
    return {
      code: mapSdkErrorCode(cause.code, fallback),
      message: cause.message,
      cause,
    };
  }
  if (cause instanceof Error) {
    return { code: fallback, message: cause.message, cause };
  }
  return { code: fallback, message: String(cause), cause };
}

function mapSdkErrorCode(
  code: SdkError["code"],
  fallback: ConversationErrorCode,
): ConversationErrorCode {
  switch (code) {
    case "Connection":
      return "Transport";
    case "Protocol":
      return "Protocol";
    case "Serialization":
    case "TypeValidation":
      return "Application";
    case "Wasm":
      return "Transport";
    default:
      return fallback;
  }
}

/**
 * Single-producer, multi-consumer async broadcast queue.
 *
 * Each `subscribe()` call gets its own buffered cursor, so a lifecycle consumer
 * that attaches after `opened` was pushed still observes every event in order.
 * This guarantees lifecycle transitions are never silently dropped.
 */
class EventChannel<E> {
  private readonly buffer: E[] = [];
  private closed = false;
  private readonly waiters = new Set<() => void>();

  push(event: E): void {
    if (this.closed) {
      return;
    }
    this.buffer.push(event);
    this.wake();
  }

  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.wake();
  }

  async *subscribe(): AsyncIterable<E> {
    let cursor = 0;
    while (true) {
      while (cursor < this.buffer.length) {
        yield this.buffer[cursor] as E;
        cursor += 1;
      }
      if (this.closed) {
        return;
      }
      await this.next();
    }
  }

  private next(): Promise<void> {
    return new Promise<void>((resolve) => {
      this.waiters.add(resolve);
    });
  }

  private wake(): void {
    for (const resolve of this.waiters) {
      resolve();
    }
    this.waiters.clear();
  }
}
