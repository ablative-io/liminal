import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";

type PackFile = {
  path: string;
};

type PackReport = {
  files: PackFile[];
};

type RuntimeCondition = "import" | "require";

type PackageManifest = {
  exports: {
    ".": Record<RuntimeCondition, string>;
    "./browser": {
      types: string;
      default: string;
    };
  };
};

type GlueImport = {
  kind: RuntimeCondition;
  specifier: string;
};

const packageRoot = fileURLToPath(new URL("../..", import.meta.url));
const runtimeFilePattern = /\.(?:js|mjs|cjs)$/;
const glueImportPattern =
  /\b(import|require)\(\s*(["'])([^"'\n]*liminal_protocol_wasm\.js)\2\s*\)/g;
const relativeDependencyPattern =
  /(?:\bfrom\s+|\bimport\s*\(\s*|\brequire\s*\(\s*)(["'])(\.[^"'\n]+)\1/g;

test("published runtime graphs contain every default WASM glue import target", () => {
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
    // The self-contained browser artifact INLINES the WASM glue (esbuild's
    // module-path comments still name it); it has no external glue import by
    // design and is covered by its own packaging and smoke tests below.
    .filter((file) => !file.path.startsWith("dist/browser/"))
    .filter((file) => runtimeFilePattern.test(file.path))
    .map((file) => ({
      file,
      source: readFileSync(path.join(packageRoot, file.path), "utf8"),
    }))
    .filter(({ source }) => source.includes("liminal_protocol_wasm.js"))
    .map((importer) => ({
      ...importer,
      glueImports: extractGlueImports(importer.source),
    }));

  assert.ok(importers.length > 0, "npm pack should ship at least one WASM glue importer");
  for (const importer of importers) {
    assert.ok(
      importer.glueImports.length > 0,
      `${importer.file.path} must use a literal import or require for the WASM glue`,
    );
    for (const glueImport of importer.glueImports) {
      assert.ok(
        glueImport.specifier.startsWith("./") || glueImport.specifier.startsWith("../"),
        `default WASM glue specifier in ${importer.file.path} must be relative, got ${glueImport.specifier}`,
      );
      const resolvedTarget = path.posix.normalize(
        path.posix.join(path.posix.dirname(importer.file.path), glueImport.specifier),
      );
      assert.ok(
        shippedPaths.has(resolvedTarget),
        `default WASM glue ${glueImport.kind} ${glueImport.specifier} from ${importer.file.path} resolves to ${resolvedTarget}, which npm pack does not ship`,
      );
    }

    if (importer.file.path.endsWith(".cjs")) {
      assert.ok(
        importer.glueImports.some(({ kind }) => kind === "import"),
        `${importer.file.path} must preserve a native dynamic import for ESM WASM glue`,
      );
      assert.ok(
        importer.glueImports.every(({ kind }) => kind !== "require"),
        `${importer.file.path} must not require ESM WASM glue`,
      );
    }
  }

  const manifest = JSON.parse(
    readFileSync(path.join(packageRoot, "package.json"), "utf8"),
  ) as PackageManifest;
  for (const condition of ["import", "require"] as const) {
    const exportTarget = packagePath(manifest.exports["."][condition]);
    assert.ok(
      shippedPaths.has(exportTarget),
      `package ${condition} export target ${exportTarget} must be shipped`,
    );
    const graph = collectRuntimeGraph(exportTarget, shippedPaths);
    assert.ok(
      importers.some(({ file }) => graph.has(file.path)),
      `package ${condition} export graph from ${exportTarget} must reach a WASM glue importer`,
    );
  }
});

test("npm pack ships the self-contained browser artifact wired to its export", async () => {
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

  const manifest = JSON.parse(
    readFileSync(path.join(packageRoot, "package.json"), "utf8"),
  ) as PackageManifest;
  const browserExport = manifest.exports["./browser"];
  assert.ok(browserExport, "package must expose a ./browser export");
  const artifactTarget = packagePath(browserExport.default);
  const typesTarget = packagePath(browserExport.types);
  assert.equal(
    artifactTarget,
    "dist/browser/liminal.js",
    "./browser must resolve to the single-file artifact",
  );
  assert.ok(
    shippedPaths.has(artifactTarget),
    `./browser export target ${artifactTarget} must be shipped by npm pack`,
  );
  assert.ok(
    shippedPaths.has(typesTarget),
    `./browser types target ${typesTarget} must be shipped by npm pack`,
  );

  const artifactPath = path.join(packageRoot, artifactTarget);
  const artifactText = readFileSync(artifactPath, "utf8");
  const staticImport = /^\s*(?:import\b|export\s+[^;\n]*?\bfrom\b)/m.exec(artifactText);
  assert.equal(
    staticImport,
    null,
    `shipped artifact must have no static module dependencies, found ${JSON.stringify(staticImport?.[0])}`,
  );
  assert.ok(
    !/\bimport\s*\(/.test(artifactText),
    "shipped artifact must have no dynamic import() calls",
  );

  const artifact = (await import(pathToFileURL(artifactPath).href)) as Record<string, unknown>;
  assert.equal(
    typeof artifact["LiminalFeedSource"],
    "function",
    "shipped artifact must parse as an ES module exporting LiminalFeedSource",
  );
  assert.equal(
    typeof artifact["createChannel"],
    "function",
    "shipped artifact must parse as an ES module exporting createChannel",
  );
});

function extractGlueImports(source: string): GlueImport[] {
  const imports: GlueImport[] = [];
  for (const match of source.matchAll(glueImportPattern)) {
    const kind = match[1];
    const specifier = match[3];
    assert.ok(kind === "import" || kind === "require");
    assert.ok(specifier);
    imports.push({ kind, specifier });
  }
  return imports;
}

function packagePath(specifier: string): string {
  assert.ok(specifier.startsWith("./"), `package export must be relative, got ${specifier}`);
  return path.posix.normalize(specifier.slice(2));
}

function collectRuntimeGraph(entry: string, shippedPaths: ReadonlySet<string>): Set<string> {
  const graph = new Set<string>();
  const pending = [entry];
  while (pending.length > 0) {
    const current = pending.pop();
    assert.ok(current);
    if (graph.has(current)) continue;
    graph.add(current);
    if (!runtimeFilePattern.test(current)) continue;

    const source = readFileSync(path.join(packageRoot, current), "utf8");
    for (const match of source.matchAll(relativeDependencyPattern)) {
      const specifier = match[2];
      assert.ok(specifier);
      const dependency = path.posix.normalize(
        path.posix.join(path.posix.dirname(current), specifier),
      );
      if (shippedPaths.has(dependency) && !graph.has(dependency)) {
        pending.push(dependency);
      }
    }
  }
  return graph;
}
