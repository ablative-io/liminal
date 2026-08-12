# Per-record round-trip latency (board #68)

**Headline: this is a durability measurement, not a transport measurement.**
At `339e81a`, the durable append-and-flush is **99.8%** of a per-record round
trip. The median round trip is **~99 ms on a localhost socket** and **~88 ms
in-process** — and removing the durable write from the identical path drops the
same round trip to **153 µs** and **31 µs** respectively.

Numbers without their setup are refused downstream, so the setup is stated
first, and both raw runs are committed:

- [`gate-logs/p0-69/rtt-probe.log`](../../gate-logs/p0-69/rtt-probe.log) — the release run, the headline
- [`gate-logs/p0-69/rtt-probe-debug.log`](../../gate-logs/p0-69/rtt-probe-debug.log) — a debug comparison run, kept as evidence of run-to-run variance (read its provenance note)

---

## What was measured

One clock, started immediately before the client hands a `RecordAdmission` to
the transport, stopped the instant the correlated `RecordCommitted` returns. The
interval spans the SDK's outbound recording and encode, the write, the server's
read, the participant gate, the semantic apply, **the durable append and
flush**, the response encode, the write back, and the SDK's inbound decode and
correlation. It is a per-record commit latency, not a ping.

Harness:
`crates/liminal-server/tests/participant_record_rtt_probe.rs@339e81a`.

## Setup

| | |
|---|---|
| rev measured | `339e81a504709321f248a7c102677178b4391ca1` |
| branch | `p0-69-golden-trace`, off `origin/main` `339e81a` |
| host | Darwin 25.3.0 arm64, Apple M1 Max, 10 logical cores, 32 GiB |
| toolchain | rustc 1.97.1 (8bab26f4f 2026-07-14) |
| build profile | **release** (optimized, `debug_assertions` off) |
| wall clock | 2026-08-12T17:45:18Z → 17:50:30Z UTC |
| load average | 6.95 at start, 10.15 at end (1-minute), on 10 cores |
| clients | 1, steady state, no concurrency, one record outstanding at a time |
| warmup / measured | 100 / 1000 per profile |
| payload | 64 bytes |
| durability | on-disk store (`persistence_path` set); ack follows append **and flush** |
| flush site | `crates/liminal-server/src/server/participant/production/log.rs:228-250@339e81a` |

The flush site is the load-bearing fact. `OperationLog::append` writes the
transition-input row at the exact optimistic head and then calls
`store.flush().await`, under the in-tree comment *"The flush is the durability
barrier the caller's pending shell commit waits behind: nothing is published
until these bytes are durable."* Both profiles pay it.

**The box was busy, and it matters.** This is a laptop under shared load with a
concurrent build, not an isolated bench. Load average sat around 7–11 on 10
cores through the headline run. Read §"How much to trust these" before quoting
any figure from here.

## The two profiles, and the control

Both profiles are driven through the same client stack —
`RemoteParticipantHandle` from `liminal-sdk` — so the only difference between
them is the transport.

- **tcp** — a real `ServerListener` on `127.0.0.1:0` over a real socket.
- **loopback** — an `EmbeddedServer` in-process over the loopback duplex.

Each profile additionally measures a **control** exchange, interleaved
one-for-one with the records: a `ParticipantAck` the server answers `AckNoOp`.
A lone participant is excluded from its own records, so it holds no delivery
obligation, so an ack over an empty debt is a no-op that **writes no durable
row**. That exchange crosses the identical client stack, transport, participant
gate and semantic dispatch, and differs in exactly one term — no append, no
flush. The probe refuses to record a control sample unless the answer really was
`AckNoOp`, so the control cannot silently stop being a control.

Interleaving is deliberate: any drift in the box's load over the run lands on
both series equally instead of on one.

## Results — release run, n = 1000 per row

All figures in microseconds.

| profile | min | **median** | mean | **p99** | max |
|---------|-----|-----------|------|--------|-----|
| **tcp** — record commit | 62 026.0 | **99 029.5** | 174 288.9 | **527 988.9** | 707 833.2 |
| **loopback** — record commit | 45 441.5 | **87 586.5** | 88 394.5 | **168 222.0** | 246 279.0 |
| tcp — control (`AckNoOp`, no durable write) | 65.1 | **153.0** | 178.3 | 603.0 | 1 467.9 |
| loopback — control (`AckNoOp`, no durable write) | 12.6 | **31.4** | 286.0 | 6 648.3 | 20 128.8 |

In milliseconds, the two figures anyone actually wants:

| profile | median | p99 |
|---------|--------|-----|
| tcp | **99.0 ms** | 528.0 ms |
| loopback | **87.6 ms** | 168.2 ms |

## What the control says

| derived quantity | value |
|---|---|
| durability cost, median (record − control) | tcp **+98 876 µs**, loopback **+87 555 µs** |
| durability share of the median | tcp **99.8%**, loopback **99.96%** |
| transport delta, median, record path | tcp − loopback = **+11 443 µs** |
| transport delta, median, control path | tcp − loopback = **+122 µs** |

Three readings, in decreasing order of confidence:

1. **The per-record figure is a storage-flush figure.** Remove the durable
   write and the same round trip on the same transport falls from 99 ms to
   153 µs — roughly 650×. Any conversation about per-record latency that does
   not begin with the storage layer is discussing 0.2% of the number. If this
   number needs to come down, batching or relaxing the per-record flush is the
   only lever with room in it; the transport has none.

2. **The transport difference is real, small, and invisible at the record
   path.** On the control path, where the flush no longer masks anything,
   loopback is genuinely faster — 31.4 µs against 153.0 µs, a ~4.9× difference
   — and that ratio held across both banked runs. But ~120 µs against an ~88 ms
   flush is nothing. **Choose the in-process transport for its deployment
   properties, not for per-record latency.**

3. **The +11 443 µs record-path delta is NOT a transport measurement.** It is
   ~94× the same delta measured on the control path in the same run, and it
   changes sign between runs (the debug run put loopback *slower* by 390 ms).
   Read it as noise in the durability term, not as a cost of TCP.

## How much to trust these

The two banked runs disagree, and the pattern of their disagreement is the most
useful thing here.

| quantity | release run | debug run | stable? |
|---|---|---|---|
| tcp control median | 153.0 µs | 208.9 µs | yes, same order |
| loopback control median | 31.4 µs | 127.5 µs | same order |
| tcp record median | 99.0 ms | 86.6 ms | ±13% |
| loopback record median | 87.6 ms | **476.6 ms** | **no** |

The debug run's loopback record median of 476.6 ms is a contention artifact —
5.5× the tcp median in the *same* run, on a box with a concurrent build hitting
the same disk — and its control series did not move correspondingly. It is
retained (with a provenance note in the log) as evidence that **the durability
term absorbs disk contention and the transport term does not.**

So:

- **Trust the order of magnitude**: per-record commit is tens to hundreds of
  milliseconds on this hardware under this durability configuration, and the
  no-durable-write control is tens to hundreds of microseconds.
- **Trust the ratio**: durability is ≳99.8% of the round trip in every run.
- **Do not quote the medians as a benchmark.** They are single runs on a
  contended laptop. The p99 and max columns are contaminated by scheduler and
  disk noise and are **not** tail-latency SLOs.
- An ~88–99 ms median flush on an M1 Max NVMe is high, and the debug build gave
  a statistically indistinguishable tcp median (86.6 ms vs 99.0 ms) —
  optimization moves nothing, which is consistent with the time being spent
  waiting on the device rather than on CPU. Whether that is inherent to the
  haematite flush, to its configuration here, or to the concurrent build
  competing for the disk, **this probe does not determine**, and it must not be
  reported as though it did.

## Reproducing

```sh
cargo test --release -p liminal-server --test participant_record_rtt_probe \
  -- --ignored --nocapture --test-threads=1
```

The full probe is `#[ignore]` because it is a measurement rather than a pin and
commits 2,200 durable records (~5 minutes). A short-run smoke variant of the
same code path, `the_rtt_probe_harness_runs`, executes in the ordinary battery,
so the probe cannot rot unnoticed.

## What this does not measure

- **Throughput.** One client, one outstanding record at a time. Nothing here
  says what the server does with pipelining, batching, or concurrent
  participants — and a flush-dominated path is exactly where batching would
  change the answer most.
- **The WebSocket transport.** Not measured.
- **Payload scaling.** One 64-byte payload only.
- **Delivery latency.** The clock stops at the sender's `RecordCommitted`; it
  says nothing about when an observer receives the `ParticipantDelivery`.
- **Any durability configuration other than this one.** A store that does not
  flush per append would produce a completely different number, and this probe
  says nothing about it.
- **A quiet machine.** See above, twice.
