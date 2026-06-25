import type { Channel } from "./channel.js";

/**
 * Producer-visible backpressure as a TypeScript discriminated union.
 *
 * This mirrors the Rust source of truth in two layers:
 *
 * - `crates/liminal/src/pressure/signal.rs` — the `PressureSignal` enum with
 *   `Accept` / `Defer` / `Reject` variants returned during a publish round-trip.
 * - `crates/liminal/src/protocol/backpressure.rs` — `PressureState`
 *   (`Normal` / `Deferred` / `Rejecting`) plus the `Defer` / `Reject` payloads
 *   that carry an optional human-readable reason.
 *
 * It is also kept semantically aligned with the Gleam SDK's `PressureResponse`
 * (`Accept | Defer(delay_millis) | Reject(reason)` in
 * `sdks/liminal-gleam/src/liminal/channel.gleam`) so the SDK-009 conformance
 * gate can compare all three SDKs.
 *
 * Per ADR-004, backpressure is a wire-protocol primitive: every signal must
 * reach the application and must never be silently converted to `accept`.
 */

/**
 * Message delivered to the consumer; processing has begun.
 *
 * Corresponds to `PressureSignal::Accept` / `PressureState::Normal`.
 */
export interface PressureAccept {
  readonly kind: "accept";
}

/**
 * Message buffered; it will be delivered when consumer capacity frees.
 *
 * Corresponds to `PressureSignal::Defer` / `PressureState::Deferred`.
 *
 * `delay` is the producer's hint, in milliseconds, for how long to wait before
 * retrying. The wire `DeferPayload` carries an optional human-readable reason,
 * preserved here as the optional `reason` field.
 */
export interface PressureDefer {
  readonly kind: "defer";
  /** Suggested wait, in milliseconds, before the producer retries. */
  readonly delay: number;
  /** Optional human-readable deferral reason from the consumer. */
  readonly reason?: string;
}

/**
 * Consumer is overwhelmed; the message has been shed.
 *
 * Corresponds to `PressureSignal::Reject` / `PressureState::Rejecting`.
 */
export interface PressureReject {
  readonly kind: "reject";
  /** Human-readable rejection reason from the consumer. */
  readonly reason: string;
}

/**
 * Producer-visible backpressure response, discriminated by `kind`.
 *
 * TypeScript narrowing on `kind` exposes each variant's fields:
 * `if (response.kind === "defer") { response.delay }`.
 */
export type PressureResponse = PressureAccept | PressureDefer | PressureReject;

/** The set of valid `PressureResponse` discriminants. */
export type PressureKind = PressureResponse["kind"];

/** Constructs an `accept` response. */
export function accept(): PressureAccept {
  return { kind: "accept" };
}

/**
 * Constructs a `defer` response.
 *
 * @param delay Suggested wait before retry, in milliseconds (clamped to >= 0).
 * @param reason Optional human-readable deferral reason.
 */
export function defer(delay: number, reason?: string): PressureDefer {
  const normalizedDelay = Number.isFinite(delay) && delay > 0 ? delay : 0;
  return reason === undefined
    ? { kind: "defer", delay: normalizedDelay }
    : { kind: "defer", delay: normalizedDelay, reason };
}

/** Constructs a `reject` response. */
export function reject(reason: string): PressureReject {
  return { kind: "reject", reason };
}

/** Narrows a `PressureResponse` to its `accept` variant. */
export function isAccept(response: PressureResponse): response is PressureAccept {
  return response.kind === "accept";
}

/** Narrows a `PressureResponse` to its `defer` variant. */
export function isDefer(response: PressureResponse): response is PressureDefer {
  return response.kind === "defer";
}

/** Narrows a `PressureResponse` to its `reject` variant. */
export function isReject(response: PressureResponse): response is PressureReject {
  return response.kind === "reject";
}

/**
 * Pressure state observed on a stream, mirroring the Rust protocol
 * `PressureState` enum (`Normal` / `Deferred` / `Rejecting`).
 */
export type PressureState = "normal" | "deferred" | "rejecting";

/**
 * Maps a wire-level {@link PressureState} to the producer-visible
 * {@link PressureResponse.kind}, matching the Rust state-to-signal mapping.
 */
export function kindForState(state: PressureState): PressureKind {
  switch (state) {
    case "normal":
      return "accept";
    case "deferred":
      return "defer";
    case "rejecting":
      return "reject";
  }
}

/**
 * Normalizes an arbitrary transport result into a {@link PressureResponse}.
 *
 * Per ADR-004 / CN6, this NEVER silently converts a `defer` or `reject` signal
 * into `accept`. A `defer` or `reject` shape is only produced when the transport
 * explicitly signalled it. An absent or unrecognised result (e.g. the SDK-007
 * `PublishResult` of `undefined`) is treated as a plain delivery acknowledgement
 * — i.e. `accept` — because the transport carried no pressure signal at all.
 *
 * Recognised input shapes:
 * - `{ kind: "accept" | "defer" | "reject", ... }` — a TS `PressureResponse`.
 * - `{ state: "normal" | "deferred" | "rejecting", ... }` — a wire pressure state.
 * - Gleam/FFI tags: `{ Accept } | { Defer, delay_millis } | { Reject, reason }`.
 */
export function fromTransportResult(result: unknown): PressureResponse {
  if (!isRecord(result)) {
    return accept();
  }
  const direct = fromDiscriminatedKind(result);
  if (direct !== undefined) {
    return direct;
  }
  const byState = fromStateField(result);
  if (byState !== undefined) {
    return byState;
  }
  const byTag = fromGleamTag(result);
  if (byTag !== undefined) {
    return byTag;
  }
  return accept();
}

function fromDiscriminatedKind(
  result: Record<string, unknown>,
): PressureResponse | undefined {
  switch (result.kind) {
    case "accept":
      return accept();
    case "defer":
      return defer(readNumber(result.delay) ?? 0, readString(result.reason));
    case "reject":
      return reject(readString(result.reason) ?? "consumer rejected message");
    default:
      return undefined;
  }
}

function fromStateField(
  result: Record<string, unknown>,
): PressureResponse | undefined {
  const state = result.state;
  if (state === "normal") {
    return accept();
  }
  if (state === "deferred") {
    return defer(readNumber(result.delay) ?? 0, readString(result.reason));
  }
  if (state === "rejecting") {
    return reject(readString(result.reason) ?? "consumer rejected message");
  }
  return undefined;
}

function fromGleamTag(
  result: Record<string, unknown>,
): PressureResponse | undefined {
  if (Object.hasOwn(result, "Accept")) {
    return accept();
  }
  if (Object.hasOwn(result, "Defer")) {
    return defer(
      readNumber(result.delay_millis) ?? readNumber(result.delay) ?? 0,
      readString(result.reason),
    );
  }
  if (Object.hasOwn(result, "Reject")) {
    return reject(readString(result.reason) ?? "consumer rejected message");
  }
  return undefined;
}

/**
 * A channel whose `publish` surfaces the backpressure signal to the producer.
 *
 * Distinct from the SDK-007 {@link Channel} (whose `publish` returns the raw
 * `PublishResult`): here `publish` returns `Promise<PressureResponse>` so the
 * producer can react to `defer` / `reject` per ADR-004. `subscribe` and
 * `requestReply` are unchanged from the wrapped channel.
 */
export interface PressureChannel<T> {
  readonly name: string;
  /** Publishes a message and returns the producer-visible pressure signal. */
  publish(message: T): Promise<PressureResponse>;
  subscribe(): AsyncIterable<T>;
  requestReply<Req = T, Resp = T>(message: Req): Promise<Resp>;
}

/**
 * Wraps an SDK-007 {@link Channel} so `publish` returns a {@link PressureResponse}.
 *
 * This does NOT duplicate the channel's publish/subscribe logic — it delegates
 * to the wrapped channel and maps its `PublishResult` through
 * {@link fromTransportResult}, preserving every pressure signal (CN6 / ADR-004).
 */
export function withPressure<T>(channel: Channel<T>): PressureChannel<T> {
  return {
    name: channel.name,
    async publish(message: T): Promise<PressureResponse> {
      return fromTransportResult(await channel.publish(message));
    },
    subscribe(): AsyncIterable<T> {
      return channel.subscribe();
    },
    requestReply<Req = T, Resp = T>(message: Req): Promise<Resp> {
      return channel.requestReply<Req, Resp>(message);
    },
  };
}

function readNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function readString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
