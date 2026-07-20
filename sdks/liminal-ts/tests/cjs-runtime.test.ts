import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const packageRoot = fileURLToPath(new URL("../..", import.meta.url));
const disableRequireEsmFlag = "--no-experimental-require-module";

test("shipped CommonJS bridge loads ESM glue with explicit wasm bytes", () => {
  const flagProbe = spawnSync(
    process.execPath,
    [disableRequireEsmFlag, "-e", ""],
    { encoding: "utf8" },
  );
  const supportedFlags = flagProbe.status === 0 ? [disableRequireEsmFlag] : [];
  const mode = supportedFlags.length === 1
    ? disableRequireEsmFlag
    : "native CommonJS behavior (flag unavailable on this Node)";
  const cjsBridge = path.join(packageRoot, "dist/cjs/wasm.cjs");
  const wasmBytes = path.join(packageRoot, "dist/wasm/liminal_protocol_wasm_bg.wasm");
  const script = String.raw`
const { readFileSync } = require("node:fs");
const { connect } = require(process.argv[1]);

(async () => {
  const source = new Uint8Array(readFileSync(process.argv[2]));
  const connected = await connect({ authToken: new Uint8Array() }, { source });
  if (!(connected.frame instanceof Uint8Array) || connected.frame.byteLength === 0) {
    throw new Error("CommonJS bridge did not return a non-empty Connect frame");
  }
  console.log("CJS explicit WasmSource loaded; frame bytes: " + connected.frame.byteLength);
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
`;
  const result = spawnSync(
    process.execPath,
    [...supportedFlags, "-e", script, cjsBridge, wasmBytes],
    { cwd: packageRoot, encoding: "utf8" },
  );

  assert.equal(
    result.status,
    0,
    `CJS explicit-WasmSource smoke failed under ${mode}\nstdout:\n${result.stdout}\nstderr:\n${result.stderr}`,
  );
  assert.match(result.stdout, /CJS explicit WasmSource loaded; frame bytes: \d+/);
});
