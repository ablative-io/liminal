import { LiminalFeedSourceError } from "./feed-source-error.js";

interface Cut {
  readonly generation: bigint;
  readonly seq: bigint;
}

interface CachedSnapshot {
  readonly bytes: string;
  readonly cut: Cut;
}

interface SnapshotWaiter {
  readonly resolve: (bytes: string) => void;
  readonly reject: (error: LiminalFeedSourceError) => void;
}

/**
 * The feed seam has no authority RPC, so its authoritative pull is the newest
 * in-stream baseline. A repeated pull never returns the same cut twice: it
 * remains pending until a strictly newer generation snapshot arrives.
 *
 * The demo publisher emits such a baseline after `SNAPSHOT_PERIOD` deltas.
 * Therefore any cache spin ends at that next generation bump and is bounded by
 * the publisher's `SNAPSHOT_PERIOD` (currently twenty deltas).
 */
export class FeedSnapshotCache {
  private latest: CachedSnapshot | undefined;
  private lastServedCut: Cut | undefined;
  private readonly waiters: SnapshotWaiter[] = [];

  observe(envelopeBytes: string): void {
    const snapshot = parseSnapshot(envelopeBytes);
    if (snapshot === undefined) return;
    if (this.latest === undefined || compareCuts(snapshot.cut, this.latest.cut) >= 0) {
      this.latest = snapshot;
    }
    this.resolveEligibleWaiters();
  }

  request(): Promise<string> {
    const eligible = this.eligibleSnapshot();
    if (eligible !== undefined) {
      this.lastServedCut = eligible.cut;
      return Promise.resolve(eligible.bytes);
    }
    return new Promise<string>((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
  }

  rejectPending(error: LiminalFeedSourceError): void {
    const pending = this.waiters.splice(0);
    pending.forEach(({ reject }) => reject(error));
  }

  private eligibleSnapshot(): CachedSnapshot | undefined {
    if (this.latest === undefined) return undefined;
    if (this.lastServedCut === undefined || compareCuts(this.latest.cut, this.lastServedCut) > 0) {
      return this.latest;
    }
    return undefined;
  }

  private resolveEligibleWaiters(): void {
    const eligible = this.eligibleSnapshot();
    if (eligible === undefined || this.waiters.length === 0) return;
    this.lastServedCut = eligible.cut;
    const pending = this.waiters.splice(0);
    pending.forEach(({ resolve }) => resolve(eligible.bytes));
  }
}

function parseSnapshot(bytes: string): CachedSnapshot | undefined {
  // The exact publisher form is RFC-8785 JCS, whose six keys have this order.
  // Reading the integer lexemes as bigint avoids a JavaScript 2^53 cut ceiling.
  const generation = /"generation":([1-9][0-9]*),"kind":"snapshot"/.exec(bytes)?.[1];
  const seq = /,"seq":(0|[1-9][0-9]*)}$/.exec(bytes)?.[1];
  if (generation === undefined || seq === undefined) return undefined;
  return {
    bytes,
    cut: { generation: BigInt(generation), seq: BigInt(seq) },
  };
}

function compareCuts(left: Cut, right: Cut): number {
  if (left.generation !== right.generation) return left.generation > right.generation ? 1 : -1;
  if (left.seq === right.seq) return 0;
  return left.seq > right.seq ? 1 : -1;
}
