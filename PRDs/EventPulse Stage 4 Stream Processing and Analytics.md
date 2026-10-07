# EventPulse Stage 4: Stream Processing and Analytics

*Duration: about 4 weeks. Goal: compute correct real-time aggregates over event-time windows, keep that computation correct through crashes, and serve analytical queries over hundreds of millions of rows in milliseconds.*

## 1. Objective and Motivating Pain

Stage 3 made storage correct under duplicates, replays, crashes, and reordering. It did not compute anything over time. Aggregating in Postgres is slow at scale and, as soon as you want windows, depends on arrival order. Lateness has no policy, a stateful processor has no recovery story, and the analytical sink (ClickHouse) deduplicates very differently from a Postgres unique key.

By the end of this stage you must be able to:

1. Explain event time vs processing time, tumbling and sliding windows, watermarks, allowed lateness, and checkpointed state, and implement all of them yourself.
2. Show that a processor crashed mid-window recovers to exactly the result an uninterrupted run would have produced.
3. Show a dashboard query answering in milliseconds over a very large table, and prove why ClickHouse beats Postgres for this workload (and where it does not).
4. Explain the three ways to get aggregates (processor, ClickHouse materialized views, Flink or Kafka Streams) and choose between them with data.
5. Extend your delivery guarantees to the analytics layer.

**Predictions to write down first:** how many times faster will ClickHouse be than Postgres on your dashboard queries at 100M rows? How large will processor state grow per tenant and window? What happens to a materialized view when a duplicate raw event arrives?

## 2. Prerequisites and Setup

| Item | Requirement |
| --- | --- |
| Stages 0-3 | Complete, including the delivery guarantees document and crash-matrix tooling |
| ClickHouse | Single node in docker-compose with explicit CPU and memory limits (replication and sharding are not needed yet) |
| Client | `@clickhouse/client` for Node |
| Disk | Plan for 30-60 GB free for the large-table experiments |
| Data generation | Use ClickHouse itself to create hundreds of millions of synthetic rows (`INSERT ... SELECT ... FROM numbers(N)`), separate from the real pipeline which you test at lower, realistic rates |
| Tools | `clickhouse-client`, `clickhouse-benchmark`, `EXPLAIN indexes = 1`, system tables (`system.parts`, `system.merges`, `system.query_log`, `system.part_log`) |
| Optional comparison | Flink (JobManager and TaskManager containers) or Kafka Streams (needs a JVM); time-box to about one week |

## 3. Learning Plan

### Stream processing concepts

- **Event time vs processing time.** Windows over event time give correct results regardless of arrival order; processing-time windows are simpler and wrong under delay.
- **Window types:** tumbling (fixed, non-overlapping), sliding or hopping (fixed size, smaller slide, overlapping), session (gap-based). You will build the first two.
- **Watermarks:** a statement that event time has progressed to T, so events with timestamps at or below T are not expected anymore. The common heuristic is *bounded out-of-orderness*: watermark = maximum event time seen minus an allowed delay.
- **Allowed lateness and late-data policies:** a window fires when the watermark passes its end, state is kept for the lateness period so late events can update it, and events beyond that are dropped, sent to a side output, or trigger corrections.
- **Triggers and early results:** emitting partial results for open windows (throttled) for dashboards, then a final result.
- **Keyed state and its cost:** state size is the number of live windows times keys times aggregate size. Unbounded key cardinality means unbounded state.
- **Checkpointing:** periodically snapshot state together with the source positions so recovery restores both consistently. Know the trade-off between interval, overhead, and replay work after a crash.
- **Output correctness:** exactly-once state does not give exactly-once output. Either use transactional sinks or make output idempotent (deterministic keys with replace semantics).
- **Idle partitions:** a partition with no data holds the watermark back (the minimum across partitions); real systems add idleness detection.

### ClickHouse concepts

- **Columnar storage and compression.** A query reads only the columns it needs, and similar values compress heavily.
- **MergeTree family.** Data is written as immutable *parts* that background merges combine. The `ORDER BY` key defines the sparse primary index (one entry per granule of about 8192 rows). Choosing it is the main performance decision.
- **Partitioning.** `PARTITION BY` is for data lifecycle (dropping old data) and coarse pruning, not primary performance. Too many partitions hurts.
- **Insert behavior.** Each insert creates a part. Many tiny inserts create too many parts and slow merges ("too many parts" errors). Insert in large batches, or use asynchronous inserts and measure them.
- **Materialized views.** Triggered by inserts to the source table, operating on each inserted block. Combined with `AggregatingMergeTree` and `-State` / `-Merge` functions they maintain rolling aggregates incrementally.
- **Deduplication.** `ReplacingMergeTree` removes duplicates only during merges, eventually, so queries may need `FINAL` or `argMax` patterns. Block-level insert deduplication exists for retried identical batches; check the documentation for its defaults and settings on your table engine.
- **Other tools:** `LowCardinality(String)`, data-skipping indexes, projections, TTL.
- **Reading query cost:** `system.query_log` fields `read_rows`, `read_bytes`, `query_duration_ms`, `memory_usage`, and `EXPLAIN indexes = 1`.

### Reading

DDIA Chapter 11 (stream processing: time, windows, fault tolerance, joins) and the column-oriented storage part of Chapter 3. Akidau et al., *The Dataflow Model* paper, and the *Streaming 101* and *Streaming 102* articles. ClickHouse documentation on MergeTree, primary keys and sparse indexes, and materialized views. Flink documentation on watermarks and checkpoints, even if you do not run Flink. In `notebook/streaming-notes.md`, explain a watermark and a checkpoint in your own words with a worked example.

## 4. Requirements

| ID | Requirement |
| --- | --- |
| S4-F1 | A new consumer group `sink-clickhouse` batch-inserts raw events into `events_raw` in ClickHouse |
| S4-F2 | A processor computes per (tenant, type) aggregates: count, sum of a numeric payload field, min, max, over 1-minute and 1-hour tumbling windows and a 5-minute window sliding by 1 minute, all in event time |
| S4-F3 | Watermark policy: bounded out-of-orderness (configurable, default 10 s), allowed lateness (configurable, default 2 min), side output for later events |
| S4-F4 | Open windows emit throttled early updates; closed windows emit a final result; late updates re-emit with a higher version |
| S4-F5 | Processor state is checkpointed with source offsets atomically and restored on restart |
| S4-F6 | Aggregate output is idempotent: each result has a deterministic key and version, and replays do not double count |
| S4-F7 | A parallel implementation of the same aggregates using ClickHouse materialized views, for comparison |
| S4-F8 | Query API: tenant-scoped aggregates over a time range and raw event lookup, with an end-to-end freshness metric |
| S4-F9 | After comparison, retire the Postgres events sink; Postgres keeps only metadata and the outbox |
| S4-F10 | Delivery guarantees document extended with analytics guarantees |

### Performance targets (hypotheses to replace with measurements)

| Area | Target |
| --- | --- |
| Dashboard queries | under 200 ms at 100M+ rows, with a measured p95 |
| Freshness | 99% of events visible in aggregates within 5 s of ingest |
| Recovery | processor restored from checkpoint within a bounded time you measure |

## 5. Design

```
Kafka events.raw
  |-- group sink-clickhouse --> batch insert --> ClickHouse events_raw
  |                                                 |
  |                                  materialized view (Path A) --> agg tables
  |
  |-- group processor (one consumer per partition)
        keyed state: (tenant, type, windowSize, windowStart) -> aggregate
        per-partition watermark, global = min over partitions
        timers: close windows when watermark >= end; evict after end + lateness
        checkpoint every T s: state + offsets in ONE Postgres transaction
        emit: agg messages (deterministic key, version) --> aggregates.out
  
aggregates.out --> sink --> ClickHouse agg_* (ReplacingMergeTree by version)

Query API --> ClickHouse (tenant-scoped)
```

### Two aggregation paths, deliberately

- **Path A: materialized views.** Incremental and simple, handles late data automatically (it is additive), but it double counts duplicates inserted into the raw table and has no notion of watermarks or finality.
- **Path B: your processor.** Explicit lateness policy and finality, idempotent emission, but you own state, checkpoints, and recovery.

The point of building both is the comparison, and then choosing one or a hybrid with evidence.

### Window semantics (write these into the design doc exactly)

- A window `[start, start + size)` is **fired** when the global watermark is at or past `start + size`.
- Its state is **kept** until the watermark reaches `start + size + allowedLateness`. Events arriving in that period update the window and re-emit it with an incremented version.
- Events whose window state has been evicted are **late beyond the policy**: send them to `events.late` and a `late_events` table for inspection and offline correction.
- Window boundaries use UTC only.

### Deduplication at ClickHouse (decide with data)

1. **Dedup before the sink:** a dedup step with state keyed by event id and TTL equal to the dedup window.
2. **Dedup in the table:** `ReplacingMergeTree` with the event id in the ordering key plus query-time handling (`FINAL` or `argMax`).
3. **Block-level insert dedup:** retried identical batches are dropped. This protects against sink retries but not against a re-sent event inside a different batch.
4. **Accept duplicates in raw, protect aggregates:** the processor dedups, and raw is for audit and ad hoc analysis.

Record the chosen option as an ADR with its measured cost.

### ClickHouse schemas (starting point)

```sql
CREATE TABLE events_raw (
  tenant_id      LowCardinality(String),
  event_id       UUID,
  type           LowCardinality(String),
  occurred_at    DateTime64(3, 'UTC'),
  received_at    DateTime64(3, 'UTC'),
  schema_version UInt16,
  value          Float64,
  payload        String CODEC(ZSTD)
) ENGINE = MergeTree
PARTITION BY toYYYYMMDD(occurred_at)
ORDER BY (tenant_id, type, occurred_at, event_id);

CREATE TABLE agg_1m (
  tenant_id    LowCardinality(String),
  type         LowCardinality(String),
  window_start DateTime('UTC'),
  cnt          UInt64,
  sum_value    Float64,
  min_value    Float64,
  max_value    Float64,
  version      UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (tenant_id, type, window_start);

-- Path A
CREATE TABLE agg_1m_mv_target (
  tenant_id LowCardinality(String), type LowCardinality(String),
  window_start DateTime('UTC'),
  cnt AggregateFunction(count), sum_value AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree
ORDER BY (tenant_id, type, window_start);

CREATE MATERIALIZED VIEW agg_1m_mv TO agg_1m_mv_target AS
SELECT tenant_id, type, toStartOfMinute(occurred_at) AS window_start,
       countState() AS cnt, sumState(value) AS sum_value
FROM events_raw GROUP BY tenant_id, type, window_start;
```

The partition key (day by `occurred_at`), the ordering key, and column types are all hypotheses to test, not decisions.

### Processor state and checkpoint tables (Postgres, small state first)

```sql
CREATE TABLE processor_checkpoint (
  processor_id text, partition int, kafka_offset bigint,
  watermark_ms bigint, updated_at timestamptz,
  PRIMARY KEY (processor_id, partition)
);
CREATE TABLE processor_state (
  processor_id text, partition int, state bytea, checkpoint_id bigint,
  PRIMARY KEY (processor_id, partition)
);
```

Offsets and state for a partition are written in one transaction, and on startup the processor seeks to the stored offset. Kafka consumer-group commits are kept only for lag monitoring. Compare this with a compacted changelog topic and local state (Kafka Streams style) as a later improvement.

### Repo additions

```
services/processor/ (windows, watermark, state, checkpoint, emitter)
services/sink-clickhouse/ (raw and aggregate sinks)
services/query-api/ (ClickHouse queries, tenant scoping)
clickhouse/ (DDL, benchmark queries, data generator)
tests/stream/ (golden, property, recovery)
docs/adr/ (clickhouse schema, dedup option, path A vs B)
```

## 6. Step-by-Step Build Plan

1. **ClickHouse and large data first.** Create the schemas, generate 1M, 10M, 100M, and 300M+ synthetic rows, and write your benchmark query set. This delivers an early result and informs every later decision.
2. **Postgres vs ClickHouse benchmark** on identical data and queries, up to the size Postgres can reasonably hold.
3. **Raw sink** with batching on count or time and retries that are safe given your dedup choice.
4. **Path A materialized views** and their queries.
5. **Windowing library** with unit tests: window assignment, watermark computation, firing and eviction.
6. **Processor v1:** tumbling windows, in-memory state, idempotent emission.
7. **Late-data policy** and side output.
8. **Sliding windows.**
9. **Checkpoint and recovery.**
10. **Aggregate sink** with version-based replacement.
11. **Query API and freshness metric.**
12. **Golden and property tests**, then the break-it catalog.
13. **Optional:** implement the same job in Flink or Kafka Streams and compare.
14. **Retire the Postgres events sink** after the comparison is documented.
15. **Write the artifacts** (section 10).

### Window assignment and watermark sketch

```ts
const windowStart = (ts: number, size: number) => ts - (ts % size);

onEvent(e, partition) {
  const ts = e.occurredAtMs;
  maxTs.set(partition, Math.max(maxTs.get(partition) ?? 0, ts));
  const watermark = Math.min(...activePartitionMaxTs()) - outOfOrdernessMs;

  for (const size of WINDOW_SIZES) {
    const start = windowStart(ts, size);
    if (start + size + allowedLatenessMs <= watermark) {
      sideOutputLate(e);                      // state already evicted
    } else {
      merge(state, key(e.tenant, e.type, size, start), e);
      markDirty(key);                         // for throttled early emits / re-emits
    }
  }
  advance(watermark);                          // fire windows with end <= watermark,
}                                              // evict windows with end + lateness <= watermark
```

Sliding windows assign each event to several overlapping windows; think about the state multiplier (size divided by slide) and what that does to memory.

### Emission and output key

Each emitted record carries `(tenant, type, window_size, window_start)` as its key and a `version` that increases on every update. The sink writes into a `ReplacingMergeTree(version)` table, so re-emissions after a crash or a late update overwrite rather than add.

## 7. Test Plan

| Layer | Tests | Pass condition |
| --- | --- | --- |
| Unit | Window assignment at boundaries (including exactly on the edge and across hour and day), watermark math, fire and evict conditions, idle partition handling | Spec behavior |
| Golden test | Feed a recorded event log through the processor; compare final aggregates with the same aggregation computed by SQL over `events_raw` | Exact match |
| Property-based | For any permutation of events whose disorder is within the allowed bound, final aggregates equal the reference | No counterexample |
| Late data | Events inside lateness, just beyond, far beyond | Correct update, re-emission with higher version, or side output |
| Duplicates | Replay events through each dedup option | Aggregates unchanged for the chosen option; documented behavior for others |
| Recovery | Kill the processor at random points (including between state write and emit), restart, finish the stream | Final aggregates equal an uninterrupted run |
| Checkpoint atomicity | Kill during checkpoint write | Either the old or the new checkpoint is fully visible, never a mix |
| Sink | Retried identical batches, interleaved versions, out-of-order versions | Highest version wins after merge and via `FINAL` |
| Query | Tenant scoping tests, time range edge cases | No cross-tenant data, correct bounds |
| Freshness | Inject a marker event and poll the aggregate | Measured latency recorded per run |

The recovery-equivalence test (crash and compare with an uninterrupted run) is the central proof of this stage.

## 8. Break-It Experiment Catalog

Write the hypothesis first, run the audit and golden test after each experiment, and record every number.

### W1 Postgres vs ClickHouse at scale

- **Hypothesis:** ClickHouse is slower or similar on single-row lookups and far faster on scans and aggregations as rows grow; Postgres degrades roughly linearly with rows scanned.
- **How:** run the same queries at 1M, 10M, 100M, 300M+ rows: per-tenant count per minute over the last hour, top types over a day, wide time-range aggregate, point lookup by event id.
- **Observe:** latency, rows and bytes read, memory, disk size and compression ratio.
- **Explains:** why an analytical column store is the right tool for the dashboard, and the cases where it is not (point lookups, frequent updates).

### W2 Crash the processor mid-window

- **Hypothesis:** with atomic state-plus-offset checkpoints, restart yields aggregates identical to an uninterrupted run; crashing between emit and checkpoint produces duplicate emissions that version-based replacement absorbs.
- **How:** inject crashes at every step (after update, before checkpoint, during checkpoint, after checkpoint before emit, after emit before checkpoint).
- **Observe:** recovery time, replayed event count, duplicate emissions, final equality.
- **Explains:** the checkpoint protocol; then sweep the checkpoint interval and plot overhead vs replay work.

### W3 Late events

- **Hypothesis:** events inside the lateness period update windows; beyond it they are diverted; Path A silently includes them all, so the two paths disagree on old windows.
- **How:** inject events delayed 5 s, 1 min, 5 min, 1 h.
- **Observe:** re-emission counts, side output volume, differences between Path A and Path B for closed windows.
- **Explains:** finality as a product decision, and the cost of corrections.

### W4 Idle and skewed partitions

- **Hypothesis:** an idle partition freezes the global watermark so windows never close and state grows; one very busy partition dominates processing time.
- **How:** stop traffic to one partition; send 80% of traffic to one tenant.
- **Observe:** watermark lag, open window count, memory, partition lag skew.
- **Explains:** idleness detection and the hot-partition problem to revisit in Stage 6.

### W5 Cardinality explosion

- **Hypothesis:** millions of distinct tenants, types, or key values blow up processor memory and ClickHouse aggregation memory, and can make a naive ordering key worthless.
- **How:** generate high-cardinality keys; try `LowCardinality` on a truly high-cardinality column.
- **Observe:** processor RSS and state size, ClickHouse memory use, query latency, merge pressure.
- **Explains:** state bounding, key design, and per-tenant limits.

### W6 Tiny inserts and too many parts

- **Hypothesis:** inserting small batches at high frequency creates many parts, slows queries, and eventually refuses inserts.
- **How:** sweep insert batch size (1, 100, 1k, 10k, 100k rows) and flush interval; try asynchronous inserts.
- **Observe:** `system.parts` count, merge backlog, insert latency, query latency, errors.
- **Explains:** why the sink batches by size and time and the trade-off with freshness.

### W7 Bad ordering key

- **Hypothesis:** a query that filters on a column not in the leading ordering key scans most of the table.
- **How:** run a time-range query without tenant, compare `read_rows`; then fix with a different key, projection, or skipping index.
- **Observe:** `EXPLAIN indexes = 1`, `read_rows`, latency before and after.
- **Explains:** the ordering key as an access-pattern decision; it must be designed from queries.

### W8 Duplicates in ClickHouse

- **Hypothesis:** re-inserting events doubles counts in Path A; `ReplacingMergeTree` hides duplicates only after merges; `FINAL` is correct but slower; identical retried blocks may be dropped by block-level dedup.
- **How:** replay raw batches; re-insert in different batch shapes; query before and after merges.
- **Observe:** counts for each path and table type, `FINAL` cost, behavior with and without dedup settings.
- **Explains:** the dedup option you choose and its measured price.

### W9 ClickHouse slow or down

- **Hypothesis:** while ClickHouse is slow or restarting, ingest is unaffected, the sink lags, and recovery drains the backlog; inserts are atomic per block so no half-written batches appear.
- **How:** throttle ClickHouse resources; kill it mid-insert; restart.
- **Observe:** lag growth, duplicates after retry, drain time, data consistency.
- **Explains:** backpressure and failure isolation across the analytics boundary.

### W10 Partition granularity

- **Hypothesis:** very fine partitions create too many parts and slow queries; very coarse ones make drops and late-data handling clumsy.
- **How:** compare hourly, daily, monthly partitions with a late-data workload.
- **Observe:** part counts, query latency, merge activity.
- **Explains:** partitioning for lifecycle, not performance.

### W11 Time pitfalls

- **Hypothesis:** non-UTC boundaries, daylight saving shifts, and mixed time zones produce misaligned windows.
- **How:** feed events around midnight, month boundaries, and DST changes with time zone offsets.
- **Observe:** window assignments and daily rollups.
- **Explains:** why all processing is in UTC and display conversion is a presentation concern.

### W12 Backfill and reprocessing

- **Hypothesis:** resetting offsets and recomputing yields identical aggregates with idempotent replacement and wrong (doubled) ones with additive writes.
- **How:** replay a day of data into both designs.
- **Observe:** equality, time to reprocess, impact on live traffic.
- **Explains:** reprocessing as a first-class, safe operation.

### W13 Custom vs Flink or Kafka Streams (optional, time-boxed)

- **Hypothesis:** a mature engine gives better throughput and recovery with less code, at the price of operational complexity and a new runtime.
- **How:** implement the 1-minute tumbling aggregate with late data handling, run the same inputs and crash tests.
- **Observe:** lines of code, throughput, recovery time, state backend behavior, operational effort.
- **Explains:** the build-vs-adopt trade-off, which you can discuss credibly because you built the core yourself first.

## 9. Metrics and Diagnostics

**Processor:** events per second, watermark lag (wall clock minus watermark), open windows, state entries and bytes, checkpoint duration, size, and interval, late events by lateness bucket, emissions and re-emissions, recovery time.

**ClickHouse:** insert rows per second, parts per partition, merges running, query duration percentiles, `read_rows`, `read_bytes`, memory usage, disk size, compression ratio.

**End to end:** freshness (ingest time to queryable aggregate) p50 and p99, sink lag, query API latency.

Useful queries:

```sql
-- parts per table and partition
SELECT table, partition, count() AS parts, sum(rows) AS rows
FROM system.parts WHERE active GROUP BY table, partition ORDER BY parts DESC LIMIT 20;

-- query cost
SELECT query_duration_ms, read_rows, read_bytes, memory_usage, query
FROM system.query_log WHERE type = 'QueryFinish' ORDER BY event_time DESC LIMIT 20;

-- compression
SELECT table, formatReadableSize(sum(data_compressed_bytes)) AS compressed,
       formatReadableSize(sum(data_uncompressed_bytes)) AS raw
FROM system.parts WHERE active GROUP BY table;
```

## 10. Artifacts

1. **Dashboard query report:** Postgres vs ClickHouse across data sizes with charts, `read_rows` evidence, and an explanation of why ClickHouse wins and where it loses.
2. **Processor design doc:** window semantics, watermark and lateness policy, checkpoint protocol, idempotent emission, state size model.
3. **ADRs:** ClickHouse ordering and partition keys, dedup option, Path A vs Path B vs hybrid, custom processor vs Flink or Kafka Streams.
4. **Recovery report:** the W2 crash matrix with the equality proof and checkpoint interval trade-off chart.
5. **Delivery guarantees update:** analytics guarantees, for example: aggregates are eventually consistent; a window is final after watermark plus lateness; events beyond lateness are quarantined, not lost; replays and restarts do not change results; and the stated freshness target with measured data.
6. **Optional blog post:** "Why my dashboard went from seconds to milliseconds" or "Watermarks explained by breaking one."

## 11. Common Pitfalls

- Using processing time for windows because it is easier.
- Forgetting that the watermark is the minimum across partitions, so one idle partition stalls everything.
- Evicting state too early (losing lateness) or never (leaking memory).
- Writing state and offsets separately, so a crash can pair new state with old offsets and double count.
- Additive output sinks, which turn every replay into double counting.
- Choosing the ClickHouse ordering key before knowing the queries.
- Many tiny inserts, then blaming ClickHouse.
- Querying `ReplacingMergeTree` without handling unmerged duplicates and calling the numbers correct.
- Trusting a materialized view to dedup.
- Benchmarking only the happy size; the 1M-row result tells you little about 300M.
- Comparing against an untuned Postgres or an unindexed table in a way that is not a fair baseline. State what you tuned on both sides.

## 12. Exit Criteria

- [ ] ClickHouse benchmarks recorded for multiple data sizes, with dashboard queries answering in milliseconds at 100M+ rows.
- [ ] A written explanation, backed by `read_rows` and compression data, of why ClickHouse and not Postgres.
- [ ] Tumbling and sliding windows with event-time semantics, watermarks, allowed lateness, and a side output implemented and tested.
- [ ] Golden and property tests pass.
- [ ] Crash and recovery equivalence demonstrated across all crash points.
- [ ] Idempotent aggregate emission with versioned replacement, verified under replay and backfill.
- [ ] Path A vs Path B comparison done and a decision recorded.
- [ ] Dedup option chosen with measured cost.
- [ ] Experiments W1 through W12 have notebook entries (W13 optional).
- [ ] Delivery guarantees document extended; Postgres events sink retired.
- [ ] Repo tagged `stage-4-complete`.

**Self-test questions (answer aloud, without notes):**

1. Why does a window defined on processing time give wrong answers, and what does event time cost you instead?
2. Define a watermark. What happens to your results if you set the allowed delay too low? Too high?
3. Why must state and offsets be checkpointed together? Walk through what goes wrong otherwise.
4. What does it mean for output to be idempotent, and how did you achieve it in the aggregate sink?
5. Why is a ClickHouse primary key not a uniqueness constraint? What does it do?
6. When would you choose a materialized view over a stream processor, and when the reverse?

## 13. Handoff to Stage 5

Stage 4 leaves a correct, fast, observable analytics pipeline for well-behaved tenants. Open problems:

- Nothing protects the system from a tenant that floods it, or from a misbehaving one that sends enormous key cardinality.
- Aggregates exist, but nothing acts on them yet: no rules, thresholds, or notifications.
- Configuration and rule lookups will hit Postgres on every evaluation unless cached, and cache stampedes and Redis failures are new failure modes.
- One tenant can still saturate a partition and degrade the processor for others.

Stage 5 adds a distributed rate limiter, a rule engine that consumes your aggregates and triggers idempotent notifications, and a caching layer, then breaks them with a noisy neighbor, a Redis failover, and a thundering herd. Before starting, write down which tenant behaviors from your Stage 4 experiments (cardinality explosion, skew, bursts) a rate limiter could and could not have prevented.

When you are ready, say **Stage 5**.
