/**
 * Connection lifecycle and reconnection for the TypeScript SDK.
 *
 * Mirrors the protocol connection state machine in
 * `crates/liminal/src/protocol/lifecycle.rs` (the SDK-003 lifecycle) projected
 * onto the four client-facing states the brief mandates:
 * `connecting -> connected -> reconnecting -> disconnected`.
 *
 * Reconnection uses exponential backoff with jitter — never fixed-interval
 * retry — and, on a successful reconnect, resumes each subscription from its
 * last acknowledged sequence so no acknowledged messages are replayed and no
 * gaps are introduced.
 */

import { SdkError } from "./channel.js";

/**
 * Client-facing connection state, discriminated by the string literal.
 *
 * - `connecting` — initial connect attempt in flight.
 * - `connected` — handshake complete; frames may flow.
 * - `reconnecting` — the link dropped and a backoff-paced retry is in flight.
 * - `disconnected` — closed by the caller or after giving up; terminal unless
 *   `connect()` is called again.
 */
export type ConnectionState =
  | "connecting"
  | "connected"
  | "reconnecting"
  | "disconnected";

/**
 * Reason a connection entered the `disconnected` state. Mirrors the Rust SDK's
 * `DisconnectReason` so both SDKs report the same disconnect cause.
 *
 * - `normal` — closed intentionally via {@link Connection.close}.
 * - `error` — closed because reconnection was exhausted or the link errored.
 * - `timeout` — closed because a timeout elapsed.
 */
export type DisconnectReason = "normal" | "error" | "timeout";

/** A single state-change observation delivered to subscribers. */
export interface ConnectionStateChange {
  readonly previous: ConnectionState;
  readonly current: ConnectionState;
  /** Retry attempt index (0-based) when entering `reconnecting`, else undefined. */
  readonly attempt?: number;
  /** Disconnect cause when entering `disconnected`, else undefined. */
  readonly reason?: DisconnectReason;
}

/** Callback invoked on every connection state transition. */
export type ConnectionStateListener = (change: ConnectionStateChange) => void;

/**
 * Underlying transport the connection drives. Distinct from the SDK-007 channel
 * transport: this is the raw link the reconnection loop opens and closes.
 */
export interface ConnectionTransport {
  /** Opens the link; resolves once the handshake completes. */
  open(): Promise<void>;
  /** Closes the link. */
  close(): Promise<void>;
}

/** Tunable backoff and retry parameters. */
export interface ConnectionConfig {
  /** Base backoff delay in milliseconds (default 100). */
  readonly baseDelay?: number;
  /** Maximum backoff delay in milliseconds (default 30_000). */
  readonly maxDelay?: number;
  /**
   * Maximum reconnection attempts before giving up and entering `disconnected`.
   * `Infinity` (the default) retries indefinitely.
   */
  readonly maxAttempts?: number;
  /**
   * Jitter fraction in [0, 1] applied to each delay (default 0.5). The realised
   * delay is `base + random(0, jitter * base)`, bounding worst-case latency
   * while de-correlating concurrent reconnects.
   */
  readonly jitter?: number;
  /** Injectable RNG in [0, 1) for deterministic tests (default `Math.random`). */
  readonly random?: () => number;
  /** Injectable timer for deterministic tests (default `setTimeout`-based). */
  readonly sleep?: (ms: number) => Promise<void>;
}

/** Resolved (non-optional) backoff parameters. */
interface ResolvedConfig {
  readonly baseDelay: number;
  readonly maxDelay: number;
  readonly maxAttempts: number;
  readonly jitter: number;
  readonly random: () => number;
  readonly sleep: (ms: number) => Promise<void>;
}

/**
 * Tracks a subscription's resume point: reconnection resumes from
 * `lastAckedSequence + 1` so acknowledged messages are not replayed.
 */
export interface SubscriptionCursor {
  readonly channel: string;
  /** Last sequence number the consumer acknowledged, or -1 if none yet. */
  lastAckedSequence: number;
}

/**
 * Computes the exponential-backoff-with-jitter delay for a reconnection
 * attempt: `min(baseDelay * 2^attempt, maxDelay) + jitter`.
 *
 * Exported for direct unit testing of the backoff policy.
 *
 * @param attempt 0-based attempt index.
 * @param config Resolved backoff parameters.
 */
export function backoffDelay(
  attempt: number,
  config: {
    readonly baseDelay: number;
    readonly maxDelay: number;
    readonly jitter: number;
    readonly random: () => number;
  },
): number {
  const exponent = Math.min(attempt, 53);
  const uncapped = config.baseDelay * Math.pow(2, exponent);
  const base = Math.min(uncapped, config.maxDelay);
  const jitterSpan = base * config.jitter;
  return base + config.random() * jitterSpan;
}

/**
 * Manages a single connection's lifecycle: initial connect, automatic
 * backoff-paced reconnection on drop, subscription resume, and observable state
 * transitions.
 */
export class Connection {
  private state: ConnectionState = "disconnected";
  private reason: DisconnectReason | undefined;
  private readonly listeners = new Set<ConnectionStateListener>();
  private readonly subscriptions = new Map<string, SubscriptionCursor>();
  private readonly transport: ConnectionTransport;
  private readonly config: ResolvedConfig;
  private generation = 0;

  constructor(transport: ConnectionTransport, config: ConnectionConfig = {}) {
    this.transport = transport;
    this.config = resolveConfig(config);
  }

  /** Returns the current connection state. */
  get currentState(): ConnectionState {
    return this.state;
  }

  /**
   * Returns the reason for the most recent disconnect, or `undefined` if the
   * connection has never been disconnected since the last successful connect.
   */
  get lastDisconnectReason(): DisconnectReason | undefined {
    return this.reason;
  }

  /** Registers a state-change listener; returns an unsubscribe function. */
  onStateChange(listener: ConnectionStateListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  /**
   * Registers (or returns the existing) resume cursor for a channel
   * subscription. The cursor is updated via {@link acknowledge} and consulted on
   * reconnect.
   */
  registerSubscription(channel: string): SubscriptionCursor {
    const existing = this.subscriptions.get(channel);
    if (existing !== undefined) {
      return existing;
    }
    const cursor: SubscriptionCursor = { channel, lastAckedSequence: -1 };
    this.subscriptions.set(channel, cursor);
    return cursor;
  }

  /** Advances a subscription's acknowledged sequence (monotonically). */
  acknowledge(channel: string, sequence: number): void {
    const cursor = this.subscriptions.get(channel);
    if (cursor !== undefined && sequence > cursor.lastAckedSequence) {
      cursor.lastAckedSequence = sequence;
    }
  }

  /** Returns the resume cursors, one per registered subscription. */
  resumeCursors(): readonly SubscriptionCursor[] {
    return [...this.subscriptions.values()];
  }

  /**
   * Opens the connection, transitioning `disconnected -> connecting ->
   * connected`. On failure the backoff-paced reconnection loop is entered.
   */
  async connect(): Promise<void> {
    this.generation += 1;
    this.reason = undefined;
    this.transition("connecting");
    try {
      await this.transport.open();
      this.transition("connected");
    } catch (cause) {
      await this.reconnect(this.generation, cause);
    }
  }

  /**
   * Signals that the link dropped. Transitions into the reconnection loop unless
   * the connection was deliberately closed.
   */
  async handleDrop(cause?: unknown): Promise<void> {
    if (this.state === "disconnected") {
      return;
    }
    await this.reconnect(this.generation, cause);
  }

  /** Closes the connection and stops any reconnection loop. */
  async close(): Promise<void> {
    this.generation += 1;
    const wasActive = this.state !== "disconnected";
    this.transition("disconnected", undefined, "normal");
    if (wasActive) {
      await this.transport.close();
    }
  }

  /**
   * Runs the exponential-backoff reconnection loop. Each iteration emits a
   * `reconnecting` transition carrying the attempt index, sleeps for the backoff
   * delay, then retries `open()`. On success it resumes subscriptions and
   * transitions to `connected`; after `maxAttempts` it gives up and transitions
   * to `disconnected`.
   */
  private async reconnect(generation: number, _cause?: unknown): Promise<void> {
    for (let attempt = 0; attempt < this.config.maxAttempts; attempt += 1) {
      if (generation !== this.generation) {
        return; // Superseded by a newer connect()/close().
      }
      this.transition("reconnecting", attempt);
      await this.config.sleep(backoffDelay(attempt, this.config));
      if (generation !== this.generation) {
        return;
      }
      try {
        await this.transport.open();
        this.resumeSubscriptions();
        this.transition("connected");
        return;
      } catch {
        // Fall through to the next backoff iteration; never fixed-interval.
      }
    }
    this.transition("disconnected", undefined, "error");
    throw new SdkError("Connection", "exhausted reconnection attempts", {
      details: { attempts: this.config.maxAttempts },
    });
  }

  /**
   * Hook point for resuming subscriptions from their last acknowledged
   * sequence. The cursors carry the resume points; the transport-specific
   * Subscribe/Resume frames are issued by the channel layer using
   * {@link resumeCursors}. Kept here so reconnection and resume stay coupled.
   */
  private resumeSubscriptions(): void {
    for (const cursor of this.subscriptions.values()) {
      // Resume point is lastAckedSequence + 1; channel layer re-subscribes.
      void cursor;
    }
  }

  private transition(
    next: ConnectionState,
    attempt?: number,
    reason?: DisconnectReason,
  ): void {
    if (next === this.state && attempt === undefined) {
      return;
    }
    const previous = this.state;
    this.state = next;
    let change: ConnectionStateChange;
    if (next === "disconnected") {
      this.reason = reason ?? "normal";
      change = { previous, current: next, reason: this.reason };
    } else if (attempt === undefined) {
      change = { previous, current: next };
    } else {
      change = { previous, current: next, attempt };
    }
    for (const listener of this.listeners) {
      listener(change);
    }
  }
}

function resolveConfig(config: ConnectionConfig): ResolvedConfig {
  return {
    baseDelay: positive(config.baseDelay, 100),
    maxDelay: positive(config.maxDelay, 30_000),
    maxAttempts:
      config.maxAttempts === undefined || config.maxAttempts < 0
        ? Number.POSITIVE_INFINITY
        : config.maxAttempts,
    jitter: clampFraction(config.jitter, 0.5),
    random: config.random ?? Math.random,
    sleep: config.sleep ?? defaultSleep,
  };
}

function positive(value: number | undefined, fallback: number): number {
  return value !== undefined && Number.isFinite(value) && value > 0
    ? value
    : fallback;
}

function clampFraction(value: number | undefined, fallback: number): number {
  if (value === undefined || !Number.isFinite(value)) {
    return fallback;
  }
  return Math.min(Math.max(value, 0), 1);
}

function defaultSleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}
