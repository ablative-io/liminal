const fs = require("fs");
const path = require("path");

const commonJsGlueRequire =
  'Promise.resolve().then(() => require("../wasm/liminal_protocol_wasm.js"))';
const nativeGlueImport = 'import("../wasm/liminal_protocol_wasm.js")';
const expectedGlueImportTransforms = 1;
let glueImportTransforms = 0;

const outputs = [
  ["dist/esm", ".mjs", (content) => content],
  ["dist/cjs", ".cjs", rewriteCommonJsSpecifiers],
];
const commonJsOutputExists = fs.existsSync("dist/cjs");

for (const [directory, extension, transform] of outputs) {
  if (!fs.existsSync(directory)) {
    continue;
  }
  for (const entry of fs.readdirSync(directory)) {
    if (!entry.endsWith(".js")) {
      continue;
    }
    const source = path.join(directory, entry);
    const target = path.join(directory, entry.replace(/\.js$/, extension));
    fs.writeFileSync(target, transform(fs.readFileSync(source, "utf8")));
  }
}

if (commonJsOutputExists && glueImportTransforms !== expectedGlueImportTransforms) {
  throw new Error(
    "Expected to transform exactly " +
      `${expectedGlueImportTransforms} TypeScript-emitted CommonJS WASM glue import, ` +
      `but transformed ${glueImportTransforms}`,
  );
}

copyWasmPackageTo(path.join("dist", "wasm"));
if (fs.existsSync("dist-test")) {
  copyWasmPackageTo(path.join("dist-test", "wasm"));
}

function rewriteCommonJsSpecifiers(content) {
  const matches = content.split(commonJsGlueRequire).length - 1;
  glueImportTransforms += matches;
  return content
    .replaceAll(commonJsGlueRequire, nativeGlueImport)
    .replace(/require\("(\.\/.+?)\.js"\)/g, 'require("$1.cjs")');
}

function copyWasmPackageTo(targetDirectory) {
  const sourceDirectory = "wasm";
  if (!fs.existsSync(sourceDirectory)) {
    return;
  }
  fs.rmSync(targetDirectory, { recursive: true, force: true });
  fs.mkdirSync(targetDirectory, { recursive: true });
  for (const entry of fs.readdirSync(sourceDirectory)) {
    if (entry === ".gitignore") {
      continue;
    }
    fs.copyFileSync(path.join(sourceDirectory, entry), path.join(targetDirectory, entry));
  }
}
