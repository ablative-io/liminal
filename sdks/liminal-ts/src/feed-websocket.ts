import { feedError, LiminalFeedSourceError } from "./feed-source-error.js";
import {
  close as encodeDisconnect,
  connect as encodeConnect,
  receive as decodeFrame,
  subscribe as encodeSubscribe,
  unsubscribe as encodeUnsubscribe,
} from "./wasm.js";
import type { WasmLoadOptions, WasmReceivedFrame } from "./wasm.js";

const CONNECT_ACK = 0x02;
const CONNECT_ERROR = 0x03;
const DISCONNECT = 0x04;
const SUBSCRIBE_ACK = 0x06;
const SUBSCRIBE_ERROR = 0x07;
const PUBLISH_ERROR = 0x0b;
const DELIVER = 0x19;
const SUBSCRIPTION_STREAM_ID = 1;
const SOCKET_OPEN = 1;

export type FeedWebSocketFactory = (endpoint: string) => WebSocket;

export interface FeedWebSocketHandlers {
  readonly ready: () => void;
  readonly deliver: (envelopeBytes: string) => void;
  readonly failed: (error: LiminalFeedSourceError) => void;
}

type Phase = "opening" | "connect" | "subscribe" | "active" | "closing" | "closed";

/** One browser WebSocket carrying one canonical liminal frame per binary message. */
export class FeedWebSocketSubscription {
  private socket: WebSocket | undefined;
  private phase: Phase = "opening";
  private subscriptionId: bigint | undefined;
  private inbound: Promise<void> = Promise.resolve();
  private readonly decoder = new TextDecoder("utf-8", { fatal: true });

  constructor(
    private readonly endpoint: string,
    private readonly authToken: Uint8Array,
    private readonly channel: string,
    private readonly wasm: WasmLoadOptions,
    private readonly createWebSocket: FeedWebSocketFactory,
    private readonly handlers: FeedWebSocketHandlers,
  ) {}

  start(): void {
    try {
      const socket = this.createWebSocket(this.endpoint);
      this.socket = socket;
      socket.binaryType = "arraybuffer";
      socket.onopen = () => void this.opened();
      socket.onmessage = (event) => this.enqueue(event.data);
      socket.onerror = () => this.fail(feedError("CONNECTION_FAILED", "Liminal WebSocket failed"));
      socket.onclose = (event) => this.closed(event);
    } catch (cause) {
      this.fail(feedError("CONNECTION_FAILED", "Failed to open the liminal WebSocket", cause));
    }
  }

  async close(): Promise<void> {
    if (this.phase === "closing" || this.phase === "closed") return;
    this.phase = "closing";
    const socket = this.socket;
    try {
      if (socket?.readyState === SOCKET_OPEN) {
        if (this.subscriptionId !== undefined) {
          const { frame } = await encodeUnsubscribe(
            { connectionId: 0, streamId: SUBSCRIPTION_STREAM_ID, subscriptionId: this.subscriptionId },
            this.wasm,
          );
          socket.send(frame);
        }
        const { frame } = await encodeDisconnect({ connectionId: 0 }, this.wasm);
        socket.send(frame);
      }
      socket?.close(1000, "liminal feed unsubscribed");
    } catch (cause) {
      socket?.close();
      throw feedError("CONNECTION_FAILED", "Failed to tear down the liminal subscription", cause);
    }
  }

  private async opened(): Promise<void> {
    if (this.phase !== "opening") return;
    try {
      const { frame } = await encodeConnect(
        { endpoint: this.endpoint, authToken: this.authToken },
        this.wasm,
      );
      if (this.phase !== "opening") return;
      this.phase = "connect";
      this.requireOpenSocket().send(frame);
    } catch (cause) {
      this.fail(feedError("WASM_ERROR", "Failed to encode the liminal Connect frame", cause));
    }
  }

  private enqueue(data: unknown): void {
    this.inbound = this.inbound
      .then(async () => this.handleMessage(await messageBytes(data)))
      .catch((cause: unknown) => {
        this.fail(
          cause instanceof LiminalFeedSourceError
            ? cause
            : feedError("PROTOCOL_ERROR", "Failed to process a liminal WebSocket frame", cause),
        );
      });
  }

  private async handleMessage(bytes: Uint8Array): Promise<void> {
    if (this.phase === "closing" || this.phase === "closed") return;
    let frame: WasmReceivedFrame;
    try {
      frame = await decodeFrame({ connectionId: 0, frame: bytes, channel: this.channel }, this.wasm);
    } catch (cause) {
      throw feedError("WASM_ERROR", "Failed to decode a liminal WebSocket frame", cause);
    }
    if (frame.consumedBytes !== bytes.byteLength) {
      throw feedError("PROTOCOL_ERROR", "WebSocket message was not exactly one liminal frame");
    }
    await this.handleFrame(frame);
  }

  private async handleFrame(frame: WasmReceivedFrame): Promise<void> {
    if (frame.frameType === CONNECT_ACK && this.phase === "connect") {
      const { frame: subscribeFrame } = await encodeSubscribe(
        { connectionId: 0, streamId: SUBSCRIPTION_STREAM_ID, channel: this.channel },
        this.wasm,
      );
      this.phase = "subscribe";
      this.requireOpenSocket().send(subscribeFrame);
      return;
    }
    if (frame.frameType === SUBSCRIBE_ACK && this.phase === "subscribe") {
      if (frame.subscriptionId === undefined) {
        throw feedError("PROTOCOL_ERROR", "SubscribeAck omitted its subscription id");
      }
      this.subscriptionId = frame.subscriptionId;
      this.phase = "active";
      this.handlers.ready();
      return;
    }
    if (frame.frameType === DELIVER && (this.phase === "subscribe" || this.phase === "active")) {
      this.handlers.deliver(this.decodeUtf8(frame.payload));
      return;
    }
    if (frame.frameType === CONNECT_ERROR) {
      throw this.rejection("CONNECT_REJECTED", "server rejected Connect", frame);
    }
    if (frame.frameType === SUBSCRIBE_ERROR) {
      throw this.rejection("SUBSCRIBE_REJECTED", "server rejected Subscribe", frame);
    }
    if (frame.frameType === PUBLISH_ERROR) {
      throw this.rejection("PROTOCOL_ERROR", "subscriber received PublishError", frame);
    }
    if (frame.frameType === DISCONNECT) {
      throw feedError("CONNECTION_CLOSED", "Liminal server disconnected the feed");
    }
    throw feedError("PROTOCOL_ERROR", `Unexpected liminal frame type 0x${frame.frameType.toString(16)}`);
  }

  private rejection(
    code: "CONNECT_REJECTED" | "SUBSCRIBE_REJECTED" | "PROTOCOL_ERROR",
    summary: string,
    frame: WasmReceivedFrame,
  ): LiminalFeedSourceError {
    const detail = frame.payload.byteLength === 0 ? "no detail" : this.decodeUtf8(frame.payload);
    return feedError(code, `${summary} (reason ${frame.reasonCode ?? 0}): ${detail}`, undefined, {
      reasonCode: frame.reasonCode,
    });
  }

  private decodeUtf8(bytes: Uint8Array): string {
    try {
      return this.decoder.decode(bytes);
    } catch (cause) {
      throw feedError("PROTOCOL_ERROR", "Deliver payload was not valid UTF-8", cause);
    }
  }

  private requireOpenSocket(): WebSocket {
    if (this.socket?.readyState !== SOCKET_OPEN) {
      throw feedError("CONNECTION_CLOSED", "Liminal WebSocket closed during setup");
    }
    return this.socket;
  }

  private closed(event: CloseEvent): void {
    if (this.phase === "closing" || this.phase === "closed") {
      this.phase = "closed";
      return;
    }
    this.fail(feedError("CONNECTION_CLOSED", `Liminal WebSocket closed (${event.code})`));
  }

  private fail(error: LiminalFeedSourceError): void {
    if (this.phase === "closed" || this.phase === "closing") return;
    this.phase = "closed";
    this.socket?.close();
    this.handlers.failed(error);
  }
}

async function messageBytes(data: unknown): Promise<Uint8Array> {
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data)) {
    const bytes = new Uint8Array(data.byteLength);
    bytes.set(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
    return bytes;
  }
  if (data instanceof Blob) return new Uint8Array(await data.arrayBuffer());
  throw feedError("PROTOCOL_ERROR", "Liminal WebSocket received a non-binary message");
}
