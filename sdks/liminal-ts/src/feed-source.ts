import { feedError, LiminalFeedSourceError } from "./feed-source-error.js";
import { FeedSnapshotCache } from "./feed-snapshot-cache.js";
import {
  FeedWebSocketSubscription,
  type FeedPublishReceipt,
  type FeedWebSocketFactory,
} from "./feed-websocket.js";
import type { WasmLoadOptions } from "./wasm.js";

export type { FeedPublishReceipt } from "./feed-websocket.js";

export const DEFAULT_FEED_CHANNEL = "frame.demo.graph-view";
export const RESERVED_OBSERVABILITY_CHANNEL = "aion.observability.v1";

/** The frame demo's swappable transport boundary, mirrored locally. */
export interface FeedSource {
  subscribe(receiver: (envelopeBytes: string) => void): () => void;
  requestSnapshot(): Promise<string>;
}

export interface LiminalFeedSourceOptions {
  /** Override the generated codec only for embedding or deterministic tests. */
  readonly wasm?: WasmLoadOptions;
  /** Browser WebSocket by default; injectable for browser-compatible test hosts. */
  readonly webSocketFactory?: FeedWebSocketFactory;
}

export type LiminalFeedSourceErrorListener = (error: LiminalFeedSourceError) => void;

interface ReadyWaiter {
  readonly resolve: () => void;
  readonly reject: (error: LiminalFeedSourceError) => void;
}

/**
 * A browser WebSocket implementation of the frame demo's `FeedSource` seam.
 *
 * `requestSnapshot()` serves each cached snapshot cut at most once. If the
 * cache still contains the installed cut, the request stays quiet and waits
 * for a strictly newer in-stream generation baseline. The demo publisher emits
 * that baseline every `SNAPSHOT_PERIOD`, so any wait/spin is bounded by that
 * period (currently twenty deltas).
 */
export class LiminalFeedSource implements FeedSource {
  readonly endpoint: string;
  readonly channel: string;

  private readonly authToken: Uint8Array;
  private readonly wasm: WasmLoadOptions;
  private readonly createWebSocket: FeedWebSocketFactory;
  private readonly snapshots = new FeedSnapshotCache();
  private readonly errorListeners = new Set<LiminalFeedSourceErrorListener>();
  private readonly readyWaiters: ReadyWaiter[] = [];
  private session: FeedWebSocketSubscription | undefined;
  private receiver: ((envelopeBytes: string) => void) | undefined;
  private ready = false;
  private latestError: LiminalFeedSourceError | undefined;

  constructor(
    serverUrl: string | URL,
    authToken: string | Uint8Array,
    channel = DEFAULT_FEED_CHANNEL,
    options: LiminalFeedSourceOptions = {},
  ) {
    this.endpoint = normalizeEndpoint(serverUrl);
    validateChannel(channel);
    this.channel = channel;
    this.authToken =
      typeof authToken === "string" ? new TextEncoder().encode(authToken) : authToken.slice();
    this.wasm = options.wasm ?? {};
    this.createWebSocket = options.webSocketFactory ?? ((endpoint) => new WebSocket(endpoint));
  }

  subscribe(receiver: (envelopeBytes: string) => void): () => void {
    if (this.session !== undefined) {
      throw feedError("ALREADY_SUBSCRIBED", "LiminalFeedSource supports one active receiver");
    }
    this.receiver = receiver;
    this.ready = false;
    let session: FeedWebSocketSubscription;
    session = new FeedWebSocketSubscription(
      this.endpoint,
      this.authToken,
      this.channel,
      this.wasm,
      this.createWebSocket,
      {
        ready: () => this.markReady(session),
        deliver: (bytes) => this.deliver(session, bytes),
        failed: (error) => this.fail(session, error),
      },
    );
    this.session = session;
    session.start();

    let active = true;
    return () => {
      if (!active) return;
      active = false;
      if (this.session !== session) return;
      this.session = undefined;
      this.receiver = undefined;
      this.ready = false;
      const closed = feedError("CONNECTION_CLOSED", "Liminal feed subscription was closed");
      this.rejectReady(closed);
      this.snapshots.rejectPending(closed);
      void session.close().catch((cause: unknown) => {
        this.emitError(
          cause instanceof LiminalFeedSourceError
            ? cause
            : feedError("CONNECTION_FAILED", "Liminal feed teardown failed", cause),
        );
      });
    };
  }

  /** Resolves when ConnectAck and SubscribeAck have both been received. */
  whenSubscribed(): Promise<void> {
    if (this.ready) return Promise.resolve();
    if (this.session === undefined) {
      return Promise.reject(feedError("NOT_SUBSCRIBED", "Liminal feed is not subscribed"));
    }
    return new Promise<void>((resolve, reject) => this.readyWaiters.push({ resolve, reject }));
  }

  requestSnapshot(): Promise<string> {
    if (this.session === undefined) {
      return Promise.reject(feedError("NOT_SUBSCRIBED", "Snapshot requested without a subscription"));
    }
    return this.snapshots.request();
  }

  /**
   * Publishes one envelope to this source's channel over the SAME WebSocket
   * the subscription rides — the server applies `Publish` and `Subscribe`
   * frames on one authenticated connection, so no second socket is opened.
   *
   * Resolves with the server's `PublishAck` receipt. Rejects with
   * `PUBLISH_REJECTED` carrying the server's `PublishError` reason when the
   * publish is refused, and with `NOT_SUBSCRIBED` when called before
   * `subscribe()` or before the handshake completes (await
   * {@link whenSubscribed} first) — a premature publish is refused loudly,
   * never queued.
   */
  async publish(envelope: string | Uint8Array): Promise<FeedPublishReceipt> {
    const session = this.session;
    if (session === undefined) {
      throw feedError(
        "NOT_SUBSCRIBED",
        "Publish requires an active subscription; call subscribe() first",
      );
    }
    const bytes = typeof envelope === "string" ? new TextEncoder().encode(envelope) : envelope;
    return session.publish(bytes);
  }

  onError(listener: LiminalFeedSourceErrorListener): () => void {
    this.errorListeners.add(listener);
    return () => this.errorListeners.delete(listener);
  }

  get lastError(): LiminalFeedSourceError | undefined {
    return this.latestError;
  }

  private markReady(session: FeedWebSocketSubscription): void {
    if (this.session !== session) return;
    this.ready = true;
    const waiters = this.readyWaiters.splice(0);
    waiters.forEach(({ resolve }) => resolve());
  }

  private deliver(session: FeedWebSocketSubscription, bytes: string): void {
    if (this.session !== session) return;
    this.snapshots.observe(bytes);
    try {
      this.receiver?.(bytes);
    } catch (cause) {
      this.emitError(feedError("RECEIVER_FAILED", "Feed receiver threw while handling Deliver", cause));
    }
  }

  private fail(session: FeedWebSocketSubscription, error: LiminalFeedSourceError): void {
    if (this.session !== session) return;
    this.session = undefined;
    this.receiver = undefined;
    this.ready = false;
    this.rejectReady(error);
    this.snapshots.rejectPending(error);
    this.emitError(error);
  }

  private rejectReady(error: LiminalFeedSourceError): void {
    const waiters = this.readyWaiters.splice(0);
    waiters.forEach(({ reject }) => reject(error));
  }

  private emitError(error: LiminalFeedSourceError): void {
    this.latestError = error;
    this.errorListeners.forEach((listener) => {
      try {
        listener(error);
      } catch (cause) {
        this.latestError = feedError(
          "ERROR_LISTENER_FAILED",
          "Liminal feed error listener threw",
          cause,
          { surfacedError: error },
        );
      }
    });
  }
}

function normalizeEndpoint(input: string | URL): string {
  const candidate = input instanceof URL ? new URL(input.href) : parseEndpoint(input);
  if (candidate.protocol === "http:") candidate.protocol = "ws:";
  if (candidate.protocol === "https:") candidate.protocol = "wss:";
  if (candidate.protocol !== "ws:" && candidate.protocol !== "wss:") {
    throw feedError("INVALID_ENDPOINT", `Liminal feed requires a ws:// or wss:// URL, got ${candidate.protocol}`);
  }
  return candidate.href;
}

function parseEndpoint(input: string): URL {
  try {
    return new URL(input.includes("://") ? input : `ws://${input}`);
  } catch (cause) {
    throw feedError("INVALID_ENDPOINT", `Invalid liminal WebSocket endpoint ${JSON.stringify(input)}`, cause);
  }
}

function validateChannel(channel: string): void {
  if (channel.length === 0) throw feedError("INVALID_CHANNEL", "Liminal feed channel cannot be empty");
  if (channel === RESERVED_OBSERVABILITY_CHANNEL) {
    throw feedError("RESERVED_CHANNEL", `Liminal feed cannot subscribe to reserved channel ${channel}`);
  }
}
