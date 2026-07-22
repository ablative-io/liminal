/**
 * Bundle entry for the self-contained browser artifact (dist/browser/liminal.js).
 *
 * This file is build-time input for scripts/build-browser.mjs only; it is not
 * shipped as-is. esbuild's `binary` loader inlines the protocol WASM binary as
 * base64 inside the bundle, and `setDefaultWasmSource` registers those bytes
 * synchronously (no instantiation at import time) so the generated
 * wasm-bindgen glue never falls back to fetching a sibling `.wasm` asset.
 *
 * The exported surface is exactly the package root surface: `export *` from
 * the compiled index, nothing more, nothing less.
 */
import embeddedWasm from "../wasm/liminal_protocol_wasm_bg.wasm";
import { setDefaultWasmSource } from "../dist/esm/wasm.js";

setDefaultWasmSource(embeddedWasm);

export * from "../dist/esm/index.js";
