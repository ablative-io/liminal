declare module "../wasm/liminal_protocol_wasm.js" {
  export function close(): Uint8Array;
  export function connect(authToken: Uint8Array): Uint8Array;
  export function receive(bytes: Uint8Array): Uint8Array;
  export function send(
    streamId: number,
    channel: string,
    schemaId: Uint8Array,
    payload: Uint8Array,
  ): Uint8Array;
  export function subscribe(
    streamId: number,
    channel: string,
    acceptedSchemas: Uint8Array,
  ): Uint8Array;
  export function unsubscribe(streamId: number, subscriptionId: bigint): Uint8Array;
  const init: (input?: unknown) => Promise<unknown>;
  export default init;
}
