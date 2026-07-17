import { execFile } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createConnection, createServer, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { spawn, type ChildProcess } from "node:child_process";

const execFileAsync = promisify(execFile);
const sdkRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const repositoryRoot = resolve(sdkRoot, "../..");
const targetDirectory =
  process.env.CARGO_TARGET_DIR ?? resolve(repositoryRoot, "target");

export interface RunningLiminalServer {
  readonly tcpPort: number;
  readonly websocketPort: number;
  readonly wasmPath: string;
  close(): Promise<void>;
}

export async function startLiminalServer(
  channel: string,
  authToken: string,
): Promise<RunningLiminalServer> {
  const [tcpPort, healthPort, websocketPort] = await Promise.all([
    freePort(),
    freePort(),
    freePort(),
  ]);
  const temporary = await mkdtemp(join(tmpdir(), "liminal-ts-splice-"));
  const configPath = join(temporary, "server.toml");
  await writeFile(
    configPath,
    `listen_address = "127.0.0.1:${tcpPort}"
health_listen_address = "127.0.0.1:${healthPort}"
drain_timeout_ms = 1000
routing_rules = []

[[channels]]
name = "${channel}"
durable = false

[auth]
token = "${authToken}"

[websocket]
listen_address = "127.0.0.1:${websocketPort}"
path = "/liminal"
allowed_origins = ["null"]
`,
  );

  await execFileAsync("cargo", ["build", "-p", "liminal-server"], {
    cwd: repositoryRoot,
    env: { ...process.env, CARGO_TARGET_DIR: targetDirectory },
    maxBuffer: 8 * 1024 * 1024,
  });
  const binary = join(targetDirectory, "debug", "liminal-server");
  const child = spawn(binary, ["--config", configPath], {
    cwd: repositoryRoot,
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  child.stdout?.on("data", (chunk: Buffer) => (output += chunk.toString()));
  child.stderr?.on("data", (chunk: Buffer) => (output += chunk.toString()));

  try {
    await waitForPort(websocketPort, 30_000);
  } catch (cause) {
    await stopChild(child);
    await rm(temporary, { recursive: true, force: true });
    throw new Error(`real liminal server did not start: ${output}`, { cause });
  }

  return {
    tcpPort,
    websocketPort,
    wasmPath: join(sdkRoot, "wasm", "liminal_protocol_wasm_bg.wasm"),
    async close(): Promise<void> {
      await stopChild(child);
      await rm(temporary, { recursive: true, force: true });
    },
  };
}

export class ProtocolTcpClient {
  private readonly frames: Uint8Array[] = [];
  private readonly waiters: Array<{
    readonly resolve: (frame: Uint8Array) => void;
    readonly reject: (error: Error) => void;
  }> = [];
  private buffered = Buffer.alloc(0);

  private constructor(private readonly socket: Socket) {
    socket.on("data", (chunk) => this.accept(chunk));
    socket.on("error", (error) => this.rejectAll(error));
    socket.on("close", () => this.rejectAll(new Error("TCP publisher connection closed")));
  }

  static async open(port: number): Promise<ProtocolTcpClient> {
    const socket = createConnection({ host: "127.0.0.1", port });
    await withTimeout(
      new Promise<void>((resolve, reject) => {
        socket.once("connect", resolve);
        socket.once("error", reject);
      }),
      10_000,
      "TCP publisher connect",
    );
    return new ProtocolTcpClient(socket);
  }

  async exchange(frame: Uint8Array): Promise<Uint8Array> {
    const response = this.nextFrame();
    await new Promise<void>((resolve, reject) => {
      this.socket.write(frame, (error) => (error === null || error === undefined ? resolve() : reject(error)));
    });
    return withTimeout(response, 10_000, "liminal protocol response");
  }

  close(): void {
    this.socket.destroy();
  }

  private nextFrame(): Promise<Uint8Array> {
    const queued = this.frames.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    return new Promise((resolve, reject) => this.waiters.push({ resolve, reject }));
  }

  private accept(chunk: Buffer): void {
    this.buffered = Buffer.concat([this.buffered, chunk]);
    while (this.buffered.byteLength >= 10) {
      const payloadLength = this.buffered.readUInt32BE(6);
      const frameLength = 10 + payloadLength;
      if (this.buffered.byteLength < frameLength) return;
      const frame = new Uint8Array(this.buffered.subarray(0, frameLength));
      this.buffered = this.buffered.subarray(frameLength);
      const waiter = this.waiters.shift();
      if (waiter === undefined) this.frames.push(frame);
      else waiter.resolve(frame);
    }
  }

  private rejectAll(error: Error): void {
    this.waiters.splice(0).forEach(({ reject }) => reject(error));
  }
}

export function withTimeout<T>(promise: Promise<T>, milliseconds: number, label: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error(`${label} timed out`)), milliseconds);
    promise.then(
      (value) => {
        clearTimeout(timeout);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timeout);
        reject(error);
      },
    );
  });
}

async function freePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("failed to reserve test port");
  await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  return address.port;
}

async function waitForPort(port: number, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const socket = await ProtocolTcpClient.open(port);
      socket.close();
      return;
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
  }
  throw new Error(`port ${port} did not become ready`);
}

async function stopChild(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null) return;
  child.kill("SIGTERM");
  try {
    await withTimeout(
      new Promise<void>((resolve) => child.once("exit", () => resolve())),
      5_000,
      "liminal server shutdown",
    );
  } catch {
    child.kill("SIGKILL");
  }
}
