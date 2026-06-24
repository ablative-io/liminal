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

export interface WasmMessagePayload {
  readonly channel: string;
  readonly payload: unknown;
  readonly consumedBytes: number;
}

export interface WasmCloseResult {
  readonly frame: Uint8Array;
}

export interface WasmProtocolBindings {
  connect(options: WasmConnectOptions): Promise<WasmConnected> | WasmConnected;
  send(options: WasmSendOptions): Promise<WasmSendResult> | WasmSendResult;
  receive(options: WasmReceiveOptions): Promise<WasmMessagePayload | undefined> | WasmMessagePayload | undefined;
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
  receive(frame: Uint8Array): Uint8Array;
  close(): Uint8Array;
};

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
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

export async function receive(
  options: WasmReceiveOptions,
  loadOptions: WasmLoadOptions = {},
): Promise<WasmMessagePayload | undefined> {
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
    receive(options) {
      const decoded = module.receive(options.frame);
      if (decoded.byteLength === 0) {
        return undefined;
      }
      const { consumedBytes, payload } = decodePayload(decoded);
      return { channel: options.channel ?? "", payload, consumedBytes };
    },
    close() {
      return { frame: module.close() };
    },
  };
}

async function loadGeneratedModule(source?: WasmSource): Promise<GeneratedWasmModule> {
  try {
    const module = await import(defaultGlueUrl().href) as GeneratedWasmModule;
    await module.default({ module_or_path: await wasmInitInput(source) });
    return module;
  } catch (cause) {
    throw wasmError("Failed to load liminal protocol WASM module", cause);
  }
}

async function wasmInitInput(source?: WasmSource): Promise<WasmSource> {
  const resolved = source ?? defaultWasmUrl();
  if (resolved instanceof WebAssembly.Module) {
    return resolved;
  }
  return readWasmBytes(resolved);
}

async function readWasmBytes(source: URL | string | ArrayBuffer | Uint8Array): Promise<ArrayBuffer> {
  if (source instanceof ArrayBuffer) {
    return source;
  }
  if (source instanceof Uint8Array) {
    return copyBytes(source);
  }
  const url = typeof source === "string" ? new URL(source, import.meta.url) : source;
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

function decodePayload(decoded: Uint8Array): { readonly consumedBytes: number; readonly payload: unknown } {
  if (decoded.byteLength < 4) {
    throw wasmError("Decoded protocol payload did not include a length prefix");
  }
  const view = new DataView(decoded.buffer, decoded.byteOffset, decoded.byteLength);
  const consumedBytes = view.getUint32(0);
  const payloadBytes = decoded.slice(4);
  if (payloadBytes.byteLength === 0) {
    return { consumedBytes, payload: undefined };
  }
  const text = textDecoder.decode(payloadBytes);
  try {
    return { consumedBytes, payload: JSON.parse(text) };
  } catch {
    return { consumedBytes, payload: text };
  }
}

function emptySchemaId(): Uint8Array {
  return new Uint8Array(32);
}

function copyBytes(bytes: Uint8Array): ArrayBuffer {
  const copy = new Uint8Array(bytes.byteLength);
  copy.set(bytes);
  return copy.buffer;
}

function defaultGlueUrl(): URL {
  return new URL("../wasm/liminal_protocol_wasm.js", import.meta.url);
}

function defaultWasmUrl(): URL {
  return new URL("../wasm/liminal_protocol_wasm_bg.wasm", import.meta.url);
}

function assertBindings(value: unknown): asserts value is WasmProtocolBindings {
  if (!isRecord(value)) {
    throw wasmError("Loaded WASM bridge did not expose protocol bindings");
  }
  const missing = ["connect", "send", "receive", "close"].filter(
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
