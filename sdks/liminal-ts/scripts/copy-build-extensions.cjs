const fs = require("fs");
const path = require("path");

const outputs = [
  ["dist/esm", ".mjs", (content) => content],
  ["dist/cjs", ".cjs", rewriteCommonJsSpecifiers],
];

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

copyWasmPackageTo(path.join("dist", "wasm"));
if (fs.existsSync("dist-test")) {
  copyWasmPackageTo(path.join("dist-test", "wasm"));
}

function rewriteCommonJsSpecifiers(content) {
  return content.replace(/require\("(\.\/.+?)\.js"\)/g, 'require("$1.cjs")');
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
