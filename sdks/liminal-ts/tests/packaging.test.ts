import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

type PackFile = {
  path: string;
};

type PackReport = {
  files: PackFile[];
};

const packageRoot = fileURLToPath(new URL("../..", import.meta.url));

test("published package contains the default WASM glue import target", () => {
  const npm = process.platform === "win32" ? "npm.cmd" : "npm";
  const output = execFileSync(
    npm,
    ["pack", "--dry-run", "--json", "--ignore-scripts"],
    { cwd: packageRoot, encoding: "utf8" },
  );
  const reports = JSON.parse(output) as PackReport[];
  assert.equal(reports.length, 1, "npm pack should describe exactly one tarball");
  const report = reports[0];
  assert.ok(report);

  const shippedPaths = new Set(report.files.map((file) => file.path));
  const importers = report.files
    .filter((file) => file.path.endsWith(".mjs"))
    .map((file) => ({
      file,
      source: readFileSync(path.join(packageRoot, file.path), "utf8"),
    }))
    .filter(({ source }) => source.includes("liminal_protocol_wasm.js"));

  assert.equal(
    importers.length,
    1,
    "exactly one shipped ESM module should import the default WASM glue",
  );
  const importer = importers[0];
  assert.ok(importer);

  const glueSpecifier = importer.source.match(
    /(["'])([^"'\n]*liminal_protocol_wasm\.js)\1/,
  )?.[2];
  assert.ok(glueSpecifier, "the shipped ESM module should name the WASM glue file");
  assert.ok(
    glueSpecifier.startsWith("./") || glueSpecifier.startsWith("../"),
    `default WASM glue specifier must be relative, got ${glueSpecifier}`,
  );

  const resolvedTarget = path.posix.normalize(
    path.posix.join(path.posix.dirname(importer.file.path), glueSpecifier),
  );
  assert.ok(
    shippedPaths.has(resolvedTarget),
    `default WASM glue import ${glueSpecifier} from ${importer.file.path} resolves to ${resolvedTarget}, which npm pack does not ship`,
  );

  const literalImport = importer.source.match(
    /import\(\s*(["'])([^"'\n]*liminal_protocol_wasm\.js)\1\s*\)/,
  );
  assert.ok(
    literalImport,
    "the default WASM glue import must use an inline literal specifier",
  );
  assert.equal(literalImport[2], glueSpecifier);
});
