/**
 * CLI entry point for the liminal TypeScript type generator (story S8 / C19).
 *
 * Usage: `node codegen-cli.js <definitions.json> [output.ts]`
 *
 * Kept separate from `codegen.ts` so the programmatic generator stays free of
 * the ESM-only `import.meta` used for entry detection, letting the core module
 * compile under both the ESM and CommonJS build targets.
 */

import { generateFromFile } from "./codegen.js";

async function main(argv: readonly string[]): Promise<void> {
  const [inputPath, outputPath] = argv;
  if (inputPath === undefined) {
    process.stderr.write("usage: codegen <definitions.json> [output.ts]\n");
    process.exitCode = 1;
    return;
  }
  const source = await generateFromFile(inputPath, outputPath);
  if (outputPath === undefined) {
    process.stdout.write(source);
  }
}

void main(process.argv.slice(2));
