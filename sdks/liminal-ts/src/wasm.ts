import { SdkError } from "./channel.js";

export interface WasmConnectOptions {
  readonly endpoint: string;
  readonly authToken?: Uint8Array;
}

export interface WasmSendOptions {
  readonly connectionId: number;
  readonly channel: string;
  readonly payload: unknown;
  readonly schemaId?: Uint8Array;
  readonly streamId?: number;
}

export interface WasmSubscribeOptions {
  readonly connectionId: number;
  readonly channel: string;
  readonly acceptedSchemas?: readonly Uint8Array[];
  readonly streamId?: number;
}

export interface WasmUnsubscribeOptions {
  readonly connectionId: number;
  readonly subscriptionId: bigint;
  readonly streamId?: number;
}

export interface WasmReceiveOptions {
  readonly connectionId: number;
  readonly frame: Uint8Array;
  readonly channel?: string;
}

export interface WasmCloseOptions {
  readonly connectionId: number;
}

export interface WasmConnected {
  readonly connectionId: number;
  readonly frame: Uint8Array;
}

export interface WasmSendResult {
  readonly frame: Uint8Array;
}

export interface WasmFrameResult {
  readonly frame: Uint8Array;
}

export interface WasmReceivedFrame {
  readonly channel: string;
  readonly frameType: number;
  readonly streamId: number;
  readonly subscriptionId?: bigint;
  /** Server-assigned message id, present only on `PublishAck` (0x0a) frames. */
  readonly messageId?: bigint;
  readonly reasonCode?: number;
  /** Exact payload bytes; no JSON decode/re-encode is performed. */
  readonly payload: Uint8Array;
  readonly consumedBytes: number;
}

/** @deprecated Use {@link WasmReceivedFrame}; retained as a source-compatible alias. */
export type WasmMessagePayload = WasmReceivedFrame;

export interface WasmCloseResult {
  readonly frame: Uint8Array;
}

export interface WasmProtocolBindings {
  connect(options: WasmConnectOptions): Promise<WasmConnected> | WasmConnected;
  send(options: WasmSendOptions): Promise<WasmSendResult> | WasmSendResult;
  subscribe(options: WasmSubscribeOptions): Promise<WasmFrameResult> | WasmFrameResult;
  unsubscribe(options: WasmUnsubscribeOptions): Promise<WasmFrameResult> | WasmFrameResult;
  receive(options: WasmReceiveOptions): Promise<WasmReceivedFrame> | WasmReceivedFrame;
  close(options: WasmCloseOptions): Promise<WasmCloseResult> | WasmCloseResult;
}

export type WasmSource = URL | string | ArrayBuffer | Uint8Array | WebAssembly.Module;

export interface WasmLoadOptions {
  readonly source?: WasmSource;
  readonly bindings?: WasmProtocolBindings | (() => Promise<WasmProtocolBindings> | WasmProtocolBindings);
}

type GeneratedWasmModule = {
  default(input?: unknown): Promise<unknown>;
  connect(authToken: Uint8Array): Uint8Array;
  send(streamId: number, channel: string, schemaId: Uint8Array, payload: Uint8Array): Uint8Array;
  subscribe(streamId: number, channel: string, acceptedSchemas: Uint8Array): Uint8Array;
  unsubscribe(streamId: number, subscriptionId: bigint): Uint8Array;
  receive(frame: Uint8Array): Uint8Array;
  close(): Uint8Array;
};

const textEncoder = new TextEncoder();
const RECEIVE_PREFIX_LENGTH = 19;
let activeBindings: Promise<WasmProtocolBindings> | undefined;

export async function connect(
  options: WasmConnectOptions,
  loadOptions: WasmLoadOptions = {},
): Promise<WasmConnected> {
  const bindings = await loadBindings(loadOptions);
  return bindings.connect(options);
}

export async function send(
  options: WasmSendOptions,
  loadOptions: WasmLoadOptions = {},
): Promise<WasmSendResult> {
  const bindings = await loadBindings(loadOptions);
  return bindings.send(options);
}

export async function subscribe(
  options: WasmSubscribeOptions,
  loadOptions: WasmLoadOptions = {},
): Promise<WasmFrameResult> {
  const bindings = await loadBindings(loadOptions);
  return bindings.subscribe(options);
}

export async function unsubscribe(
  options: WasmUnsubscribeOptions,
  loadOptions: WasmLoadOptions = {},
): Promise<WasmFrameResult> {
  const bindings = await loadBindings(loadOptions);
  return bindings.unsubscribe(options);
}

export async function receive(
  options: WasmReceiveOptions,
  loadOptions: WasmLoadOptions = {},
): Promise<WasmReceivedFrame> {
  const bindings = await loadBindings(loadOptions);
  return bindings.receive(options);
}

export async function close(
  options: WasmCloseOptions,
  loadOptions: WasmLoadOptions = {},
): Promise<WasmCloseResult> {
  const bindings = await loadBindings(loadOptions);
  return bindings.close(options);
}

export function configureWasmBridge(options: WasmLoadOptions): void {
  activeBindings = loadBindings(options);
}

export async function loadBindings(
  options: WasmLoadOptions = {},
): Promise<WasmProtocolBindings> {
  if (options.bindings !== undefined) {
    return resolveBindings(options.bindings);
  }
  activeBindings ??= instantiateBindings(options.source);
  return activeBindings;
}

async function resolveBindings(
  bindings: WasmLoadOptions["bindings"],
): Promise<WasmProtocolBindings> {
  if (bindings === undefined) {
    throw wasmError("WASM protocol bindings were not provided");
  }
  const resolved = typeof bindings === "function" ? await bindings() : bindings;
  assertBindings(resolved);
  return resolved;
}

async function instantiateBindings(source?: WasmSource): Promise<WasmProtocolBindings> {
  const module = await loadGeneratedModule(source);
  return {
    connect(options) {
      return { connectionId: 0, frame: module.connect(options.authToken ?? new Uint8Array()) };
    },
    send(options) {
      return {
        frame: module.send(
          options.streamId ?? 1,
          options.channel,
          options.schemaId ?? emptySchemaId(),
          encodePayload(options.payload),
        ),
      };
    },
    subscribe(options) {
      return {
        frame: module.subscribe(
          options.streamId ?? 1,
          options.channel,
          flattenSchemaIds(options.acceptedSchemas ?? []),
        ),
      };
    },
    unsubscribe(options) {
      return { frame: module.unsubscribe(options.streamId ?? 1, options.subscriptionId) };
    },
    receive(options) {
      const decoded = module.receive(options.frame);
      return { channel: options.channel ?? "", ...decodeReceivedFrame(decoded) };
    },
    close() {
      return { frame: module.close() };
    },
  };
}

async function loadGeneratedModule(source?: WasmSource): Promise<GeneratedWasmModule> {
  try {
    const module = await import("../wasm/liminal_protocol_wasm.js") as GeneratedWasmModule;
    if (source === undefined) {
      // In browsers wasm-bindgen resolves its sibling asset relative to the
      // generated glue module. Node callers can provide bytes via `source`.
      await module.default();
    } else {
      await module.default({ module_or_path: await wasmInitInput(source) });
    }
    return module;
  } catch (cause) {
    throw wasmError("Failed to load liminal protocol WASM module", cause);
  }
}

async function wasmInitInput(source: WasmSource): Promise<WasmSource> {
  if (source instanceof WebAssembly.Module) {
    return source;
  }
  return readWasmBytes(source);
}

async function readWasmBytes(source: URL | string | ArrayBuffer | Uint8Array): Promise<ArrayBuffer> {
  if (source instanceof ArrayBuffer) {
    return source;
  }
  if (source instanceof Uint8Array) {
    return copyBytes(source);
  }
  let url: URL;
  try {
    url = typeof source === "string" ? new URL(source) : source;
  } catch (cause) {
    throw wasmError(
      "String WASM sources must be absolute URLs; use URL or Uint8Array for relative assets",
      cause,
    );
  }
  if (url.protocol === "file:" && isNodeRuntime()) {
    const { readFile } = await import("node:fs/promises");
    return copyBytes(await readFile(url));
  }
  if (typeof fetch !== "function") {
    throw wasmError("No fetch implementation is available to load the WASM module");
  }
  const response = await fetch(url);
  if (!response.ok) {
    throw wasmError(`WASM module request failed with HTTP ${response.status}`);
  }
  return response.arrayBuffer();
}

function encodePayload(payload: unknown): Uint8Array {
  if (payload instanceof Uint8Array) {
    return payload;
  }
  const serialized = JSON.stringify(payload);
  if (serialized === undefined) {
    throw wasmError("Payload cannot be serialized for protocol framing");
  }
  return textEncoder.encode(serialized);
}

function flattenSchemaIds(schemaIds: readonly Uint8Array[]): Uint8Array {
  const flattened = new Uint8Array(schemaIds.length * 32);
  schemaIds.forEach((schemaId, index) => {
    if (schemaId.byteLength !== 32) {
      throw wasmError("Accepted schema ids must be exactly 32 bytes");
    }
    flattened.set(schemaId, index * 32);
  });
  return flattened;
}

function decodeReceivedFrame(decoded: Uint8Array): Omit<WasmReceivedFrame, "channel"> {
  if (decoded.byteLength < RECEIVE_PREFIX_LENGTH) {
    throw wasmError("Decoded protocol frame did not include typed receive metadata");
  }
  const view = new DataView(decoded.buffer, decoded.byteOffset, decoded.byteLength);
  const consumedBytes = view.getUint32(0);
  const frameType = view.getUint8(4);
  const streamId = view.getUint32(5);
  // The bridge's u64 slot carries the SubscribeAck subscription id or the
  // PublishAck message id (zero otherwise); expose each under its own name.
  const identifier = view.getBigUint64(9);
  const reasonCode = view.getUint16(17);
  return {
    consumedBytes,
    frameType,
    streamId,
    ...(frameType === 0x06 ? { subscriptionId: identifier } : {}),
    ...(frameType === 0x0a ? { messageId: identifier } : {}),
    ...([0x03, 0x07, 0x0b, 0x0f].includes(frameType) ? { reasonCode } : {}),
    payload: decoded.slice(RECEIVE_PREFIX_LENGTH),
  };
}

function emptySchemaId(): Uint8Array {
  return new Uint8Array(32);
}

function copyBytes(bytes: Uint8Array): ArrayBuffer {
  const copy = new Uint8Array(bytes.byteLength);
  copy.set(bytes);
  return copy.buffer;
}

function assertBindings(value: unknown): asserts value is WasmProtocolBindings {
  if (!isRecord(value)) {
    throw wasmError("Loaded WASM bridge did not expose protocol bindings");
  }
  const missing = ["connect", "send", "subscribe", "unsubscribe", "receive", "close"].filter(
    (name) => typeof value[name] !== "function",
  );
  if (missing.length > 0) {
    throw wasmError(`Loaded WASM bridge is missing protocol function(s): ${missing.join(", ")}`);
  }
}

function isNodeRuntime(): boolean {
  return typeof process !== "undefined" && process.versions?.node !== undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function wasmError(message: string, cause?: unknown): SdkError {
  return new SdkError("Wasm", message, { cause });
}
