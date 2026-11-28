# Stage 4 Server Build Plan: Stream Processing and Analytics

*Source: `PRDs/EventPulse Stage 4 Stream Processing and Analytics.md`. Scope: build the application server for Stage 4 so that all ten functional requirements, S4-F1 to S4-F10, are delivered. That means the ClickHouse raw sink, the Path A materialized views, the event-time stream processor (windows, watermarks, lateness, emission, checkpoints), the aggregate sink, the query API, retiring the Postgres events sink, and the extended delivery guarantees. The break-it experiments W1 to W13 are out of scope. This plan only makes sure the server exposes the knobs, crash points and metrics those experiments will need.*

---

## 0. How to use this plan

- There are **10 steps, one per requirement**. Each step owns its requirement completely: infra, DDL, migrations, code, config, metrics, tests and docs. When a step is done, its requirement is done and nothing is left for later steps. A later step may *reuse* or *extend* something built earlier, and when it does, that is listed in its own "Extends" line.
- The steps are **ordered by dependency, not by requirement number**. The order follows section 6 of the PRD: ClickHouse and the raw sink come first, then Path A, then the processor from the inside out (windows, time, emission, output, checkpoint), then the query API, then the retirement, then the guarantees.
- Every step ends with a **"Done when"** checklist. Don't start the next step until every box is ticked.
- Commit at the end of each step, using the same style as Stage 3 (for example `S4-F1: sink-clickhouse batch inserts into events_raw`).
- Never write a guarantee before the test that proves it passes. Step 10 writes the analytics guarantees last, and only from passing tests.

| Step | Requirement | One-line summary |
| --- | --- | --- |
| 1 | S4-F1 | ClickHouse in compose, DDL and migration runner, `sink-clickhouse` raw sink (group `sink-clickhouse`) with batching by count or time, safe retries, a dedup token per batch, DLQ for poison records |
| 2 | S4-F7 | Path A: `AggregatingMergeTree` targets and materialized views for 1m and 1h, the 5m/1m sliding window computed from 1m states, Path A queries, and `tools/agg-compare` |
| 3 | S4-F2 | `packages/windowing` (UTC window assignment, tumbling and sliding, aggregate merge) and `services/processor` v1: keyed state per partition for count, sum, min and max over 1m, 1h and 5m/1m, golden test against SQL |
| 4 | S4-F3 | Watermarks (bounded out-of-orderness, per partition, global minimum, idleness detection), firing and eviction timers, allowed lateness, side output to `events.late` and `late_events` |
| 5 | S4-F4 | Emission: throttled early updates for open windows, final result on fire, late re-emission with a higher deterministic version, pending emissions kept in state |
| 6 | S4-F6 | Idempotent output: deterministic key and version, `aggregates.out` Avro contract, processor-side event dedup, aggregate sink into `ReplacingMergeTree(version)` tables, replay and backfill tests |
| 7 | S4-F5 | Checkpoints: state, offsets and watermark in ONE Postgres transaction, fenced writes, restore and seek on assignment, crash matrix and recovery-equivalence test |
| 8 | S4-F8 | `services/query-api`: tenant-scoped aggregates (Path B by default, Path A on request) and raw event lookup, ClickHouse row policy, end-to-end freshness metric (prober plus passive) |
| 9 | S4-F9 | Retire the Postgres events sink: move audit, tests and tools to ClickHouse, archive the `events` table, remove `consumer-sink` from the pipeline; Postgres keeps metadata and the outbox only |
| 10 | S4-F10 | `docs/delivery-guarantees.md` extended with analytics claims G11 to G18, each mapped to a test; crash matrix regenerated; Path A vs Path B decision ADR |

### Target repo layout after Stage 4

```
infra/compose/docker-compose.yml               # + clickhouse (CPU/mem limits), toxiproxy proxy "ch" (step 1)
infra/compose/clickhouse/                      # config.d / users.d overrides, query_api user + row policy (steps 1, 8)
infra/compose/docker-compose.kafka.yml         # + events.late (step 4), aggregates.out (step 6)
clickhouse/migrations/0001..00NN_*.sql         # ClickHouse DDL, applied by tools/ch-migrate (steps 1, 2, 4, 6, 8)
clickhouse/queries/                            # Path A / Path B / reference SQL used by API, compare tool, tests (2, 3, 6, 8)
clickhouse/generator/                          # INSERT ... SELECT FROM numbers(N) synthetic data (step 8)
db/migrations/0009_processor_checkpoint.sql    # processor_checkpoint + processor_state (step 7)
db/migrations/0010_retire_events.sql           # rename events -> events_stage3_archive etc. (step 9)
schemas/aggregates/v1.avsc                     # aggregates.out-value (step 6)
schemas/events-late/v1.avsc                    # events.late-value (step 4)
packages/clickhouse/                           # @clickhouse/client wrapper, health, insert with dedup token (step 1)
packages/windowing/                            # pure: windows, aggregates, value extraction, watermark, timers (3, 4, 5)
packages/fault-injector/src/points.ts          # + raw-sink:*, processor:*, agg-sink:* crash points (1, 6, 7)
packages/tenant-auth/                          # API key -> tenant, extracted from ingest-api (step 8)
services/sink-clickhouse/                      # raw sink (1), late sink (4), aggregate sink (6)
services/processor/                            # windows, watermark, state, checkpoint, emitter (3 to 7)
services/query-api/                            # tenant-scoped aggregates + raw lookup + freshness (step 8)
tools/ch-migrate/                              # ClickHouse migration runner (step 1)
tools/agg-compare/                             # Path A vs Path B vs SQL reference diff per window (2, 6)
tools/audit/                                   # reconciles against ClickHouse instead of Postgres (step 9)
tests/stream/                                  # golden, property, late, dedup, sink, recovery (3 to 8)
tests/correctness/                             # Stage 3 suites moved to a ClickHouse storage reader (step 9)
docs/adr/0009..0013-*.md                       # 0009 CH schema, 0010 dedup, 0011 watermark, 0012 checkpoint store, 0013 path A vs B
docs/design/processor.md                       # processor design doc, one section per step (3 to 7)
docs/delivery-guarantees.md                    # analytics claims (step 10)
docs/runbooks/                                 # + clickhouse-sink-lag, processor-recovery, backfill (1, 7, 6)
```

Notes:

- The PRD's `services/sink-clickhouse/ (raw and aggregate sinks)` is kept as **one service with three roles** (`raw`, `late`, `agg`). Each role has its own consumer group and can be turned on separately with `SINK_ROLES=raw,late,agg`. This lets you run one process in the lab or three in an experiment.
- New services follow the `outbox-relay` shape (plain `node:http`, `/healthz`, `/readyz`, `/metrics`, `/debug/stats`, `/debug/faults`), except `query-api`, which is a NestJS service like `ingest-api` because it serves a public HTTP API with guards and DTOs.
- All new topics, groups and header names go in `packages/kafka-config/src/topics.ts`, the one place for shared names.
- New crash points go in `packages/fault-injector/src/points.ts`. The crash-matrix runner builds scenarios from that list, so every point must come with a scenario (this rule is already enforced).

### Cross-cutting decisions made up front (each one is recorded in the step that owns it)

| Decision | Choice | Why | Owner step |
| --- | --- | --- | --- |
| Numeric payload field | `AGG_VALUE_FIELD` (default `value`, a dotted path into the payload). A finite number gives `value` and `has_value = 1`. Anything else gives `value = 0` and `has_value = 0`. `cnt` counts every event, while `sum`, `min` and `max` cover only `has_value = 1`, and `value_cnt` is emitted too | Without this, missing values become a silent 0 that distorts `min`. The same function (`packages/windowing/src/value.ts`) is used by the raw sink and the processor, so the golden test compares like with like | 1, 3 |
| Time | Epoch milliseconds in UTC everywhere. ClickHouse columns are `DateTime64(3,'UTC')` or `DateTime('UTC')`. No local time zone appears in any computation | PRD: "Window boundaries use UTC only" | 3 |
| Processor state partitioning | State is owned **per source partition**. The output key includes the source partition: `(tenant, type, window_size, window_start, src_partition)`. Queries sum over partitions after `FINAL` | Each partition checkpoints on its own, so each partition's contribution must be a sub-aggregate that can be replaced independently. This works whether `KAFKA_KEY_MODE` is `tenant` or `event`. With `tenant` keying there is exactly one partition per tenant, so the sum is over a single row | 3, 6 |
| Version | `version = 2 * contributions + (is_final ? 1 : 0)`, where `contributions` is the number of accepted (non-duplicate) events merged into the window | It is deterministic: the same events always produce the same version and the same content. So a replay after a crash re-emits *identical* rows, and equal versions can never carry different content. It is monotonic: every update and the final flag both raise it | 5 |
| Dedup | Raw: `ReplacingMergeTree` keyed on the ordering key that contains `event_id`, plus a block insert dedup token per batch (`topic:partition:firstOffset-lastOffset`). Aggregates: Path B dedups by `(tenant_id, event_id)` in processor state. Path A double counts, which is documented | This is PRD option 2 combined with option 3 for raw, and option 4 for aggregates | 1 (raw), 6 (aggregates) |
| Checkpoint store | Postgres (PRD tables), with `checkpoint_id` stored in both rows and fenced by the previous id | PRD "small state first". A compacted changelog is a later improvement | 7 |

---

## Step 1: S4-F1 Raw sink `sink-clickhouse` into `events_raw`

**Requirement:** a new consumer group `sink-clickhouse` batch-inserts raw events into `events_raw` in ClickHouse.

### 1.1 Infrastructure

- `infra/compose/docker-compose.yml`: add a `clickhouse` service (`clickhouse/clickhouse-server`, pinned version) with explicit `cpus` and `mem_limit` from `.env` (`CH_CPUS=4`, `CH_MEM=8g`), `ulimits nofile 262144`, a named volume `chdata`, port 8123 (HTTP) and 9000 (native), and a healthcheck (`clickhouse-client -q 'SELECT 1'`).
- `infra/compose/clickhouse/config.d/eventpulse.xml`: turn on `query_log`, `part_log` and `metric_log` so W-experiments can read them. Set `max_server_memory_usage_to_ram_ratio`, and add a `prometheus` endpoint on `:9363` (ClickHouse's built-in exporter) that is scraped by `prometheus-config.yml`.
- `infra/compose/clickhouse/users.d/eventpulse.xml`: user `eventpulse_writer` (inserts, DDL for the migrator) and user `eventpulse_reader` (read-only, used by tools). The `query_api` user is added in step 8.
- Toxiproxy: add proxy `ch` (`:18123 -> clickhouse:8123`) to `make toxiproxy`, so W9 can slow ClickHouse down.
- `.env.example`: `CLICKHOUSE_URL`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DB=eventpulse`.
- Makefile: `ch-up`, `ch-client` (opens `clickhouse-client`), `ch-migrate`, `ch-reset` (destructive, asks first), `sink-ch`. `scripts/up.sh` starts ClickHouse, runs `ch-migrate` and starts `sink-clickhouse`.

### 1.2 ClickHouse migrations (`tools/ch-migrate`, `clickhouse/migrations/`)

- A small runner, because ClickHouse has no transactional DDL: a `schema_migrations` table (`ReplacingMergeTree`, version, name, checksum, applied_at). Files `NNNN_name.sql` with `-- Up` statements split on `;`, each made idempotent with `IF NOT EXISTS`. The runner refuses to run if the checksum of an applied file has changed.
- `0001_events_raw.sql`:

```sql
CREATE TABLE IF NOT EXISTS events_raw (
  tenant_id      LowCardinality(String),
  event_id       UUID,
  type           LowCardinality(String),
  occurred_at    DateTime64(3, 'UTC'),
  received_at    DateTime64(3, 'UTC'),
  inserted_at    DateTime64(3, 'UTC') DEFAULT now64(3),
  schema_version UInt16,
  value          Float64,
  has_value      UInt8,
  source         LowCardinality(Nullable(String)),   -- Avro v2 field
  payload        String CODEC(ZSTD(3)),
  kafka_partition UInt16,
  kafka_offset    UInt64
) ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMMDD(occurred_at)
ORDER BY (tenant_id, type, occurred_at, event_id)
SETTINGS non_replicated_deduplication_window = 1000;
```

  - The engine is `ReplacingMergeTree` (dedup option 2) instead of the PRD's plain `MergeTree`. Duplicates of one event share `(tenant, type, occurred_at, event_id)`, so merges collapse them. `inserted_at` as the version keeps the most recent copy, which is harmless because the copies are identical.
  - `non_replicated_deduplication_window` turns on block-level insert dedup on a non-replicated table (it is off by default there). Check the server version's documentation and record the setting you used in the ADR.
  - `kafka_partition` and `kafka_offset` are for audit and backfill debugging.
  - A `-- variants` comment block lists the PRD's plain-`MergeTree` form, so W8 can create it side by side (`events_raw_plain`) without editing the main DDL.

### 1.3 `packages/clickhouse`

- `createClient(cfg)` wraps `@clickhouse/client` with keep-alive, `request_timeout`, `compression.request = true`, and the `clickhouse_settings` defaults.
- `insertBatch(table, rows, { dedupToken, format: 'JSONEachRow' })` sets `insert_deduplication_token` and `insert_deduplicate=1`, and returns `{ rows, durationMs, queryId }`.
- `ping()`, `isRetryable(err)`: network errors, `TIMEOUT_EXCEEDED`, `TOO_MANY_PARTS` (code 252), `MEMORY_LIMIT_EXCEEDED`, and 5xx count as retryable. Type and parse errors (`CANNOT_PARSE_*`, `TYPE_MISMATCH`) count as permanent.
- `query<T>(sql, params)` always uses ClickHouse query parameters (`{tenant:String}`). String interpolation of values is never allowed. A lint rule or a unit test greps for `${` inside SQL template strings in `packages/clickhouse` callers.

### 1.4 `services/sink-clickhouse`, role `raw`

- **Consumer:** group `GROUP_SINK_CLICKHOUSE = 'sink-clickhouse'` on `events.raw`, `autoCommit: false`, at-least-once.
- **Decode:** use `packages/schema` (Avro by registry id, plus legacy JSON for the content type). The row mapping (`toRawRow`) uses `extractValue(payload, AGG_VALUE_FIELD)` from `packages/windowing/src/value.ts` (created here as a small first module of the package, and grown in step 3).
- **Batcher:** flush when `CH_BATCH_MAX_ROWS` (default 10 000) or `CH_BATCH_MAX_BYTES` (default 16 MiB) is reached, or when `CH_FLUSH_INTERVAL_MS` (default 1000) passes since the first buffered row, whichever comes first. Batches are built **per partition**: one batch never mixes partitions, so the dedup token `events.raw:<p>:<first>-<last>` is deterministic for a retry of the same offsets.
- **Commit protocol:** insert the batch, wait for the ClickHouse ack, then commit Kafka offset `last + 1` for that partition. A crash before the commit means the same offsets are redelivered and the same batch is rebuilt. If the rebuilt batch has the same offsets, it is dropped by the token. If the batch boundaries differ (for example after a rebalance), `ReplacingMergeTree` collapses the duplicates at merge time.
  - To keep batch boundaries stable across restarts, a batch starts at the committed offset and is cut at `CH_BATCH_MAX_ROWS` offsets. The time trigger only decides *when* to flush. Write down that a flush triggered by time can still give a different boundary, and that this case falls back to the merge-time dedup.
- **Retries:** for retryable ClickHouse errors (the whole batch fails), pause the partition and retry with `backoffMs` from `packages/retry` (exponential with jitter, capped). Retry the *same* batch with the *same* token, without limit, while lag grows (W9 hypothesis). Readiness turns false after `CH_UNHEALTHY_AFTER_MS`.
- **Poison:** a record that fails to decode, or a permanent error isolated by bisecting the batch (`CANNOT_PARSE` on a batch leads to splitting it in halves down to one row), goes to `events.dlq` with a `dlq_meta` row (`source_group = sink-clickhouse`, reason, error class). This uses the existing DLQ publisher pattern from `consumer-sink/src/sink/dlq-publisher.ts`, moved into `packages/retry` or a new `packages/dlq` so both sinks share it.
- **Backpressure:** the in-memory buffer is bounded by `CH_MAX_INFLIGHT_BATCHES` per partition. When it is full, the partition is paused.
- **Shutdown:** stop fetching, flush the buffered batches, commit, disconnect.
- **Crash points** (add to `points.ts` plus matrix scenarios): `raw-sink:after-fetch`, `raw-sink:after-insert-before-commit`, `raw-sink:after-commit`. `/debug/faults` can also inject `ch-insert-error` (retryable or permanent) and `ch-insert-delay-ms`.

### 1.5 Metrics (`/metrics`, prefix `ep_ch_sink_`)

`rows_inserted_total{role}`, `batches_total{role,outcome}`, `batch_rows` (histogram), `batch_bytes` (histogram), `insert_duration_seconds` (histogram), `flush_reason_total{reason=rows|bytes|time|shutdown}`, `retries_total{error}`, `dlq_total{reason}`, `dedup_token_hits_total` (when ClickHouse reports 0 written rows for a token, read from `system.query_log` or from the response summary `written_rows`), `consumer_lag{partition}`, `buffered_rows`. `/debug/stats` shows per-partition buffer state, the last token, and the last error.

### 1.6 Docs

- ADR `0009-clickhouse-raw-schema.md`: the ordering key `(tenant_id, type, occurred_at, event_id)` derived from the query list in step 8, partitioning by day for lifecycle, `LowCardinality` columns, and ZSTD on payload. It states that these are hypotheses that W1, W7 and W10 will confirm.
- ADR `0010-clickhouse-dedup.md`: the four PRD options, the raw choice (2 + 3), what each one protects against (retry of the same batch vs a re-sent event in a different batch), and the aggregate choice (4), which is implemented in step 6. It leaves a "measured cost" table empty for W8 to fill in.
- Runbook `clickhouse-sink-lag.md`: what to check (`system.parts`, `system.merges`, insert errors, lag) and what not to do (lower the batch size under pressure).

### 1.7 Tests

| Layer | Test | Pass condition |
| --- | --- | --- |
| Unit | `toRawRow` for v1/v2 Avro and legacy JSON, `extractValue` (number, string number, NaN, Infinity, missing, nested path), batcher triggers (rows, bytes, time, shutdown), token format, `isRetryable` table | Spec behavior |
| Unit | Bisect isolates exactly the bad row in a batch of N | One DLQ record, N-1 inserted |
| E2E (`services/sink-clickhouse/test`) | Produce 10k events, all appear in `events_raw` with correct columns and `received_at` from the header | Exact count, field equality |
| E2E | Crash at each `raw-sink:*` point, restart | `SELECT count() FROM events_raw FINAL` equals the produced count, with no missing or extra keys |
| E2E | Retry the same batch twice (fault: fail after the ClickHouse ack, before the commit) | Second insert writes 0 rows (block dedup) |
| E2E | Re-send the same events in a different batch shape | Raw count without `FINAL` > N. With `FINAL`, or after `OPTIMIZE ... FINAL`, it equals N. This is documented behavior |
| E2E | ClickHouse stopped for 30 s via Toxiproxy | Lag grows, ingest unaffected, drain after restart, no loss |
| E2E | Poison record | Goes to DLQ with `dlq_meta`, partition keeps flowing |

### Done when

- [ ] `make up` starts ClickHouse with limits, runs ClickHouse migrations, and runs `sink-clickhouse` (role raw).
- [ ] Events from the ingest API reach `events_raw` within `CH_FLUSH_INTERVAL_MS` plus the insert time.
- [ ] Crash and retry tests pass. No events are lost. Duplicates are collapsed under `FINAL`.
- [ ] Metrics visible in Prometheus. ADRs 0009 and 0010 are written. The runbook exists.

---

## Step 2: S4-F7 Path A, materialized views

**Requirement:** a parallel implementation of the same aggregates (count, sum, min, max per tenant and type over 1m, 1h and 5m sliding by 1m, in event time) using ClickHouse materialized views, for comparison.

**Extends:** `events_raw` (step 1).

### 2.1 DDL (`clickhouse/migrations/0002_path_a.sql`)

```sql
CREATE TABLE IF NOT EXISTS agg_1m_mv_target (
  tenant_id LowCardinality(String), type LowCardinality(String),
  window_start DateTime('UTC'),
  cnt        AggregateFunction(count),
  value_cnt  AggregateFunction(countIf, UInt8),
  sum_value  AggregateFunction(sumIf, Float64, UInt8),
  min_value  AggregateFunction(minIf, Float64, UInt8),
  max_value  AggregateFunction(maxIf, Float64, UInt8),
  max_received_at AggregateFunction(max, DateTime64(3, 'UTC'))
) ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(window_start)
ORDER BY (tenant_id, type, window_start);

CREATE MATERIALIZED VIEW IF NOT EXISTS agg_1m_mv TO agg_1m_mv_target AS
SELECT tenant_id, type, toStartOfMinute(occurred_at) AS window_start,
       countState() AS cnt,
       countIfState(has_value = 1) AS value_cnt,
       sumIfState(value, has_value = 1) AS sum_value,
       minIfState(value, has_value = 1) AS min_value,
       maxIfState(value, has_value = 1) AS max_value,
       maxState(received_at) AS max_received_at
FROM events_raw GROUP BY tenant_id, type, window_start;
```

- The same pair is created for 1h: `agg_1h_mv_target` and `agg_1h_mv` with `toStartOfHour`. A second option, an MV chained from the 1m target (`-MergeState`), is documented as an alternative, but the direct form is used so that both tables read the same raw block.
- **Sliding 5m/1m:** it is not stored. It is computed at query time from 1m states, because states merge: for each slide start `s` (a multiple of 1m), merge the 1m rows with `window_start` in `[s, s + 5m)`. This is in `clickhouse/queries/path_a/sliding_5m.sql`, using `arrayJoin` to fan each 1m row out to the 5 windows that contain it, then `-Merge` grouped by the slide start. The reason is written down: the state multiplier (5x) is paid only at read time, over the small 1m table.
- **Backfill caveat:** an MV only sees inserts that happen after it is created. The migration therefore ends with a commented, manual `INSERT INTO agg_1m_mv_target SELECT ... FROM events_raw WHERE occurred_at < <mv_created_at>`, and `make path-a-backfill` runs it deliberately.

### 2.2 Queries (`clickhouse/queries/path_a/`)

- `range.sql`: per (tenant, type, window_start) for `[from, to)`, using `countMerge`, `sumIfMerge` and so on, grouped by the key. `min` and `max` are `NULL` when `value_cnt = 0`.
- `sliding_5m.sql`, as above.
- `top_types.sql`: top N types by count for a tenant over a range.
- `clickhouse/queries/reference/*.sql`: the **SQL reference** computed directly over `events_raw FINAL` (with `FINAL`, so duplicates in raw are excluded). This is the oracle for the golden tests in steps 3 to 7, and it uses the same window functions: `toStartOfMinute`, `toStartOfHour`, and the sliding fan-out.

### 2.3 `tools/agg-compare`

- A CLI: `agg-compare --tenant t --from --to --window 1m|1h|5m --left reference|path-a|path-b --right ...`. It prints per-window differences (missing, extra, count diff, sum diff with tolerance, min/max diff) and exits non-zero on any difference. In this step `path-b` reports "not available". Step 6 wires it.
- `make agg-compare ARGS=...`.

### 2.4 Metrics and docs

- `sink-clickhouse` exposes nothing new. Path A cost is visible in `system.query_log`. Add a `clickhouse/queries/diagnostics.sql` with the PRD section 9 queries (parts per partition, query cost, compression).
- `docs/design/path-a.md`: what Path A does (additive, includes all late data, no finality) and what it does not do (double counts duplicates inserted into raw *before* merges, because the MV sees every inserted block, not merged parts).

### 2.5 Tests (`tests/stream/test/path-a.e2e-spec.ts`)

| Test | Pass condition |
| --- | --- |
| Insert a fixed set of events directly into `events_raw`, then compare Path A 1m/1h/sliding with the reference SQL | Exact match (fixture values are multiples of 0.25 so sums are exact) |
| Window boundaries: events at `hh:mm:59.999`, `hh:mm+1:00.000`, across hour and across UTC midnight | Assigned to the right window in both |
| Late event (occurred 1 h ago) inserted now | Path A window updated (it is additive). Documented |
| Duplicate event inserted in a different batch | Path A count is +1 (double counted), reference with `FINAL` unchanged. This **asserts the documented weakness**, so the test fails if behavior changes silently |
| Events with `has_value = 0` | Counted in `cnt`, excluded from sum/min/max |
| `agg-compare` exit codes | 0 on equal data, 1 on an injected difference |

### Done when

- [ ] Path A tables and MVs exist through migrations. 1m and 1h fill up as the raw sink inserts.
- [ ] The 5m/1m sliding query returns correct results from 1m states.
- [ ] Reference SQL is committed and used by tests. `agg-compare` works for reference vs path-a.
- [ ] Path A behavior with late data and duplicates is asserted by tests and documented.

---

## Step 3: S4-F2 Windowing library and processor v1 (aggregates in event time)

**Requirement:** a processor computes per (tenant, type) aggregates (count, sum of a numeric payload field, min, max) over 1-minute and 1-hour tumbling windows and a 5-minute window sliding by 1 minute, all in event time.

This step builds the **window assignment and keyed state**. Watermarks and firing come in step 4, emission in step 5. To be testable on its own, v1 exposes its state (`/debug/state`, plus `snapshot()` in the engine), and the golden test compares the state after the input is drained.

### 3.1 `packages/windowing` (pure TypeScript, no I/O, fully unit tested)

- `value.ts` (started in step 1): `extractValue(payload, path)`.
- `windows.ts`:
  - `type WindowSpec = { name: '1m' | '1h' | '5m_s1m'; sizeMs: number; slideMs: number }`. Tumbling has `slideMs === sizeMs`.
  - `WINDOW_SPECS` is configurable through `PROCESSOR_WINDOWS` (default `1m:60000:60000,1h:3600000:3600000,5m_s1m:300000:60000`).
  - `windowStart(ts, size)`: `ts - mod(ts, size)` with a **floor modulo** (`((ts % size) + size) % size`), so negative or pre-epoch timestamps are still correct. This fixes the PRD sketch, which is only correct for positive `ts`.
  - `assignWindows(ts, spec)`: tumbling gives one start. Sliding gives every `start` with `start <= ts < start + size`, for `start` a multiple of the slide: `last = windowStart(ts, slide)`, then `last, last - slide, ...` while `start > ts - size`. For 5m/1m that is exactly 5 windows. A unit test asserts the multiplier `size / slide`.
  - Every window is `[start, start + size)`, half-open, in UTC epoch ms.
- `aggregate.ts`: `Agg = { cnt, valueCnt, sum, min, max, maxReceivedAt, contributions }`, with `empty()`, `add(agg, value, hasValue, receivedAt)`, and `merge(a, b)` (associative and commutative, which is property tested). `min` and `max` start at `+Infinity` and `-Infinity` and are serialized as `null` when `valueCnt = 0`.
- `key.ts`: `WindowKey = { tenantId, type, window: name, windowStart }`, plus `keyString(k, srcPartition)` = `tenant|type|window|start|p`, which is deterministic and used as the Kafka key in step 6. Tenant ids and types are escaped (they could contain `|`).
- `state.ts`: `PartitionState` = `Map<keyString, WindowEntry>` plus indexes ordered by window end (a min-heap or sorted buckets by end time, used for firing and eviction in step 4). `stats()` gives entry count and estimated bytes.

### 3.2 `services/processor` v1

- **Process shape:** plain Node (like `outbox-relay`), `src/main.ts`, `config.ts`, `metrics.ts`, `http.ts`.
- **Consumer:** group `processor` (`GROUP_PROCESSOR`) on `events.raw`. One consumer client. Each partition is processed by its own **lane** (sequential within a partition, concurrent across partitions, using `partitionsConsumedConcurrently = number of partitions`). This is the PRD's "one consumer per partition". Scaling out across instances is supported by the group (each instance owns a subset of partitions) and by the shared watermark in step 4.
- **Decoding:** `packages/schema`, the same decoder as the raw sink. Invalid records go to the DLQ (`source_group = processor`). The processor never blocks on poison.
- **Event handling:** `onEvent(e, partition)`. For each window spec, for each assigned window, `state[partition].upsert(key).add(...)`.
- **Offsets:** v1 commits Kafka offsets normally. Step 7 replaces this with checkpointed offsets.
- **Config:** `PROCESSOR_ID` (default `processor-1`, used as the checkpoint namespace in step 7), `PROCESSOR_WINDOWS`, `AGG_VALUE_FIELD`, and `PROCESSOR_MAX_KEYS_PER_PARTITION` (a safety bound. When it is exceeded, new keys are rejected, sent to the side output with reason `state_limit`, and counted. This is the hook for W5).
- **HTTP:** `/healthz`, `/readyz`, `/metrics`, `/debug/stats` (per-partition entries, bytes, offset), `/debug/state?tenant=&type=&window=` (a read-only snapshot; it is off unless `DEBUG_STATE=true`).

### 3.3 Metrics (prefix `ep_processor_`)

`events_total{partition}`, `events_per_second` (derived), `window_assignments_total{window}`, `state_entries{partition,window}`, `state_bytes_estimate{partition}`, `process_batch_duration_seconds`, `decode_errors_total`, `state_limit_rejections_total`.

### 3.4 Docs

`docs/design/processor.md`, sections "Window semantics" and "State model". The window semantics are written exactly as the PRD gives them: half-open, UTC, fire and keep rules (finished in step 4). The state size model is `live windows x keys x entry size`. It explains the sliding multiplier: with 5m/1m, one event creates or updates 5 entries, so roughly `keys x (1 + 1 + 5)` live entries per open period, plus the lateness horizon. Give a worked example with your tenant and type counts.

### 3.5 Tests

| Layer | Test | Pass condition |
| --- | --- | --- |
| Unit | `windowStart` exactly on the edge (`ts = start`), `start - 1`, `start + size - 1`, across an hour, across a UTC day, a negative ts, a leap day | Spec behavior |
| Unit | `assignWindows` for sliding: 5 windows, all containing ts, none missing (checked against brute force over a range) | Equal to brute force |
| Property (fast-check) | `merge` is associative and commutative. `fold(add)` in any order gives the same Agg | No counterexample |
| Golden (`tests/stream/test/golden.spec.ts`, in process) | Feed `tests/stream/fixtures/golden-1.ndjson` (generated by `tests/stream/src/gen-fixture.ts` with a fixed seed: several tenants and types, values that are multiples of 0.25, events across hour and midnight) into the engine, take the snapshot, sum over partitions | Exactly equal to the expected output computed by an independent naive implementation in the test |
| Golden e2e (`golden.e2e-spec.ts`) | Produce the fixture to `events.raw`, wait until the processor and raw sink are caught up, compare `/debug/state` with the reference SQL over `events_raw FINAL` | Exact match for 1m, 1h, 5m_s1m |
| Unit | `PROCESSOR_MAX_KEYS_PER_PARTITION` exceeded | Rejected events counted, existing keys still updated |

### Done when

- [ ] `packages/windowing` exists with windows, aggregates, keys and state, and every unit and property test passes.
- [ ] The processor consumes `events.raw` and holds correct per-partition state for all three window specs.
- [ ] The golden test (in process and e2e) matches the reference SQL exactly.
- [ ] The design doc sections are written.

---

## Step 4: S4-F3 Watermark policy, allowed lateness, side output

**Requirement:** a watermark policy of bounded out-of-orderness (configurable, default 10 s), allowed lateness (configurable, default 2 min), and a side output for later events.

**Extends:** `packages/windowing`, `services/processor`, `services/sink-clickhouse` (new role `late`).

### 4.1 `packages/windowing/src/watermark.ts`

- `PartitionClock { maxEventTs, lastEventWallMs, idle, caughtUp }`.
- Partition watermark = `maxEventTs - OUT_OF_ORDERNESS_MS`. It is −∞ until the partition has had an event.
- **Global watermark** = the minimum over all **active** partitions of `events.raw` (all of them, not only this instance's):
  - Partitions owned by this instance are read live.
  - Partitions owned by other instances are read from `processor_checkpoint.watermark_ms` (written in step 7; until step 7 lands, single instance only, and startup warns if the group has more than one member). They are refreshed every `WATERMARK_SHARE_INTERVAL_MS`.
- **Monotonic:** the global watermark never goes back. The emitted value is `max(previous, computed)`.
- **Idleness detection:** a partition is idle when it has had no event for `IDLE_TIMEOUT_MS` (default 30 s, wall clock) **and** it is caught up (consumer lag 0, so the silence is not just slowness). Idle partitions are left out of the minimum. If every partition is idle, the watermark stays where it is. An optional `IDLE_ADVANCE=processing-time` mode advances it on wall clock minus out-of-orderness, and the default is off, with the trade-off documented. A partition that becomes active again rejoins the minimum. It cannot pull the global watermark back, so its old events may now be late, and they are counted as `late_after_idle`.
- `WATERMARK_IDLE_DETECTION=false` turns detection off, so W4 can show the freeze.

### 4.2 Timers: firing and eviction (`packages/windowing/src/timers.ts`)

- Windows are indexed by `end = start + size` and by `evictAt = end + ALLOWED_LATENESS_MS`.
- `advance(watermark)`:
  1. **Fire** every window with `end <= watermark` that is not fired yet. Mark it `fired = true` and hand it to the emitter (step 5) as a *final* emission.
  2. **Evict** every window with `evictAt <= watermark`. Remove it from state and record its key in a small `evictedHorizon` per (window spec) as `maxEvictedEnd`, used for the late check.
- Fire and evict are driven after each batch per lane, and also on a `TIMER_TICK_MS` tick, so idle detection can close windows without new events.

### 4.3 Late classification per event, per window spec

For each window the event belongs to:

- `end > watermark`: **on time**, normal update.
- `end <= watermark < end + lateness`: **late within policy**. The window is fired but kept, so update it, mark it dirty for re-emission (step 5), and count `late_accepted`.
- `end + lateness <= watermark`: **late beyond policy**. The state is evicted (or never existed), so the event does **not** update this window and goes to the side output.

An event can be accepted for its 1h window and rejected for its 1m window. The side output record therefore says which windows rejected it (`rejected_windows: ['1m','5m_s1m']`), and the event is side-output **once** per event, not once per window. A unit test covers this split.

### 4.4 Side output

- Topic `events.late` (new in `kafka-init`, same partition count as `events.raw`, retention 14 d), with an Avro schema `schemas/events-late/v1.avsc`: the original envelope fields, plus `watermark_ms`, `lateness_ms` (watermark − occurred_at), `rejected_windows`, `reason` (`beyond_lateness` | `state_limit` | `late_after_idle`), `processor_id`, `src_partition` and `src_offset`.
- The produce is acknowledged before the source offset is checkpointed, which keeps it at least once (the full protocol is in step 7). The record key is `tenant|event_id`, so a replay produces the same key and downstream can dedup.
- `sink-clickhouse` role `late`: group `sink-clickhouse-late`, inserting into `late_events` (ClickHouse migration `0003_late_events.sql`, `ReplacingMergeTree` ordered by `(tenant_id, occurred_at, event_id)`, with a TTL of 30 days on `received_at`). It reuses the raw sink's batcher, dedup token and retry code.
- Lateness buckets for metrics: `<5s`, `<1m`, `<5m`, `<1h`, `>=1h`, matching the W3 injections.

### 4.5 Config

`OUT_OF_ORDERNESS_MS=10000`, `ALLOWED_LATENESS_MS=120000`, `IDLE_TIMEOUT_MS=30000`, `WATERMARK_IDLE_DETECTION=true`, `IDLE_ADVANCE=off`, `TIMER_TICK_MS=1000`, `WATERMARK_SHARE_INTERVAL_MS=2000`. They are validated at startup: no negative values, and lateness must be at least 0.

### 4.6 Metrics

`ep_processor_watermark_ms{scope=global|partition}`, `watermark_lag_seconds` (wall clock − global watermark), `partition_idle{partition}`, `open_windows{window}`, `fired_windows_total{window}`, `evicted_windows_total{window}`, `late_events_total{bucket,outcome=accepted|side_output}`, `side_output_total{reason}`.

### 4.7 Docs

- `docs/design/processor.md`, section "Watermark and lateness policy": the definition, the formula, the global minimum, idleness detection and its risk, monotonicity, a worked example timeline (events at t=0..70 s with one 15 s-late and one 3 min-late event, and what fires and evicts when), and the cross-partition caveat. Because the global watermark depends on how partitions interleave, whether an event *right at* the eviction boundary is side-output can differ between runs. Final aggregates are deterministic for every event whose lateness is within policy, which is what the property test in 4.8 checks.
- ADR `0011-watermark-and-lateness-defaults.md`: why 10 s and 2 min, measured against the Stage 3 event-time skew metrics (`event-time-metrics.ts` in ingest-api already records lateness).

### 4.8 Tests

| Layer | Test | Pass condition |
| --- | --- | --- |
| Unit | Watermark: minimum over partitions, −∞ before the first event, monotonic, idle exclusion only when caught up, all-idle hold, reactivation | Spec |
| Unit | Fire at `end == watermark` (inclusive per the PRD), not at `end - 1`. Evict at `end + lateness == watermark` | Spec |
| Unit | Classification table for every (window spec, lateness) combination, including the 1h-accepted, 1m-rejected split | Spec |
| Property (`tests/stream/test/order-property.spec.ts`) | For any permutation of a generated stream where every event's disorder is within `OUT_OF_ORDERNESS_MS`, the final aggregates (after a terminal watermark flush) equal the reference, and the side output is empty | No counterexample (fast-check, 200+ runs) |
| Property | With disorder up to `OUT_OF_ORDERNESS + LATENESS`, the final aggregates still equal the reference and nothing is side-output | No counterexample |
| Late data e2e (`late.e2e-spec.ts`) | Events delayed 5 s, 1 min (inside lateness), 3 min and 1 h (beyond) after the watermark | The first two update their windows. The others are in `events.late` and `late_events` with the correct `lateness_ms` and `rejected_windows` |
| Idle e2e | Stop producing to partition 0 while traffic continues on the others | With detection on, windows still fire after `IDLE_TIMEOUT_MS`. With detection off, the watermark freezes (asserted, documented) |
| Unit | Single instance with more than one group member | Startup warning (until step 7) |

### Done when

- [ ] Watermark, fire, evict and late classification are implemented with the configured defaults.
- [ ] Late events beyond the policy reach `events.late` and `late_events`. None are lost and none are counted into evicted windows.
- [ ] Idle detection works and can be switched off.
- [ ] Property tests pass. The design doc and ADR 0011 are written.

---

## Step 5: S4-F4 Emission (early, final, late re-emission)

**Requirement:** open windows emit throttled early updates, closed windows emit a final result, and late updates re-emit with a higher version.

**Extends:** `packages/windowing` (new `emitter.ts`), `services/processor`.

### 5.1 Emission model (`packages/windowing/src/emitter.ts`)

- Each `WindowEntry` keeps `{ agg, fired, lastEmittedVersion, dirty }`.
- `version(entry) = 2 * agg.contributions + (fired ? 1 : 0)`. This is the deterministic, monotonic rule decided up front. Consequences:
  - early updates: versions 2, 4, 6, ... (even)
  - final on fire: odd, `2n + 1`, always greater than any early version at the same count
  - late update: count goes to `n + 1`, so `2(n + 1) + 1`, greater than the previous final
  - a replay of the same events gives the same version with the same content
- **Triggers:**
  - *Early:* every `EARLY_EMIT_INTERVAL_MS` (default 1000, processing time), emit every dirty, unfired entry whose version is greater than `lastEmittedVersion`. This is throttled by coalescing: many events in one interval give one emission per key. `EARLY_EMIT=false` turns it off.
  - *Final:* when a window fires (step 4), queue the final emission immediately, so the next flush includes it whatever the throttle says.
  - *Late re-emit:* a fired window that changes through a late-within-policy event becomes dirty and is emitted on the next tick as `kind = late_update`, `is_final = true`.
- **Output record** (Avro `aggregates.out-value` comes in step 6. This step defines the TypeScript type): `tenant_id, type, window (1m|1h|5m_s1m), window_start, window_end, src_partition, cnt, value_cnt, sum_value, min_value|null, max_value|null, max_received_at, version, is_final, kind (early|final|late_update), watermark_ms, emitted_at, processor_id`.
- **Pending emissions are part of state:** `pending = Map<keyString, OutputRecord>`. An entry that is evicted while it still has an unacknowledged emission keeps its pending record until the producer acknowledges it. This is what makes "checkpoint, then crash before emit" safe in step 7. Eviction only removes the *window entry*, never a pending record.

### 5.2 Processor wiring

- An emit loop per lane: take `pending` plus the dirty entries due now, build records, produce them to `aggregates.out` (the topic is created here. The contract and the sink are in step 6), wait for acks, then clear `pending` and set `lastEmittedVersion`.
- Producer: `acks=all`, `enable.idempotence=true`, `linger.ms=20`, key = `keyString`, so all versions of one window land on one output partition in order.
- Backpressure: if the number of pending records goes over `MAX_PENDING_EMITS`, pause the source partition's fetch.
- End-of-input flush (for tests and backfill): `POST /debug/flush?watermark=+inf` fires everything and emits finals. It is test-only and behind `DEBUG_FLUSH=true`.

### 5.3 Metrics

`ep_processor_emits_total{kind,window}`, `reemits_total{window}` (late updates), `emit_batch_size`, `emit_latency_seconds` (from dirty to acked), `pending_emits{partition}`, `early_emit_coalesced_total` (events that did not cause their own emission).

### 5.4 Docs

`docs/design/processor.md`, section "Emission": triggers, the version formula with a worked table, why the version is deterministic (equal versions never disagree), and what a dashboard sees (early rows are not final, `is_final` and the watermark tell the reader how settled the number is).

### 5.5 Tests

| Layer | Test | Pass condition |
| --- | --- | --- |
| Unit | Version formula: early < final < late update. Monotonic per key over any event sequence (property) | No counterexample |
| Unit | Throttle: 1000 events for one key in one interval give 1 early emission. Fire within the interval still emits the final immediately | Spec |
| Unit | Late within policy after a final gives `late_update` with a higher version. Late beyond policy gives no emission (side output) | Spec |
| Unit | An evicted entry with a pending emission keeps its pending record until the ack | Spec |
| E2E (`emission.e2e-spec.ts`) | Stream for 3 minutes with early emits on, consume `aggregates.out` | For each key: versions strictly increase in offset order, exactly one `final` per window (absent late data), the last record per key equals the reference |
| E2E | `EARLY_EMIT=false` | Only `final` and `late_update` records |
| E2E | Determinism: run the same fixture twice into a fresh state | The set of (key, version, content) for finals and late updates is identical. Early emissions may differ, and that is documented |

### Done when

- [ ] Early, final and late-update emissions are produced to `aggregates.out` with the deterministic version.
- [ ] Throttling works and can be configured or turned off.
- [ ] Pending emissions are kept in state and survive eviction until acked.
- [ ] Tests pass. The design doc section is written.

---

## Step 6: S4-F6 Idempotent aggregate output and the aggregate sink

**Requirement:** aggregate output is idempotent: each result has a deterministic key and version, and replays do not double count.

**Extends:** emitter (step 5), `sink-clickhouse` (new role `agg`), `tools/agg-compare` (path-b source).

### 6.1 Output contract

- `schemas/aggregates/v1.avsc` is registered as subject `aggregates.out-value` with `FULL_TRANSITIVE` in `schemas/subjects.json`. It is checked by `tools/schema-check` in CI (the existing Stage 3 mechanism). The processor serializes with `packages/schema`.
- The Kafka key is the deterministic `keyString` = `tenant|type|window|window_start|src_partition`.
- Topic `aggregates.out` in `kafka-init`: partition count equal to `events.raw`, `cleanup.policy=compact,delete` with retention 7 d. Compaction is allowed because only the latest version per key matters, and this makes a rebuild of the agg tables from the topic cheap.

### 6.2 Processor-side dedup (dedup option 4: protect the aggregates)

- Duplicates can sit *in the topic* (DLQ replays, producer retries before idempotence, a client re-send with a new idempotency outcome). Replay from a checkpoint is already handled by restoring state (step 7). A duplicate *message* is a different problem.
- `seen: Map<tenantId|eventId, occurredAtMs>` per partition. A duplicate always lands in the same partition in both key modes (tenant key, or event_id key), so per-partition dedup is complete.
- Duplicate: skip the merge and count `duplicates_total`. It is not a contribution, so it does not change the version.
- **Bounded TTL that matches the lateness horizon:** an id can be dropped once `occurredAt + maxWindowSize + ALLOWED_LATENESS_MS <= watermark`. At that point every window it could touch has been evicted, so a duplicate after that is late beyond the policy anyway and goes to the side output (reason `beyond_lateness`, flagged `possible_duplicate`). Dedup memory is therefore bounded by the same horizon as window state. Write this argument in the design doc.
- `seen` is part of the checkpointed state (step 7). Metrics: `ep_processor_dedup_entries`, `duplicates_total`.
- `PROCESSOR_DEDUP=false` turns it off, so W8 and W12 can show double counting.

### 6.3 Aggregate tables (`clickhouse/migrations/0004_path_b.sql`)

```sql
CREATE TABLE IF NOT EXISTS agg_1m (
  tenant_id LowCardinality(String), type LowCardinality(String),
  window_start DateTime('UTC'), src_partition UInt16,
  cnt UInt64, value_cnt UInt64, sum_value Float64,
  min_value Nullable(Float64), max_value Nullable(Float64),
  max_received_at DateTime64(3,'UTC'),
  is_final UInt8, version UInt64, watermark_ms Int64,
  emitted_at DateTime64(3,'UTC'), inserted_at DateTime64(3,'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(window_start)
ORDER BY (tenant_id, type, window_start, src_partition);
```

The same structure is used for `agg_1h` and `agg_5m_s1m`. The table is chosen by the record's `window`.

### 6.4 `sink-clickhouse`, role `agg`

- Group `sink-clickhouse-agg` on `aggregates.out`. It reuses the batcher and the dedup token (`aggregates.out:<p>:<first>-<last>`). It routes rows to three tables, one insert per table per batch, with the token suffixed by the table name.
- Writes are **replace, not add**. Duplicate emissions, out-of-order versions and replays are all absorbed by `ReplacingMergeTree(version)`.
- **Query-time correctness:** reads always use either `FINAL` or `argMax(col, version)` grouped by the key, then `sum` (and `min`/`max`) over `src_partition`. Both forms are in `clickhouse/queries/path_b/` and are used by step 8. Querying without them is forbidden, and a test checks that every `path_b` query file contains `FINAL` or `argMax`.
- Crash points: `agg-sink:after-insert-before-commit`, `agg-sink:after-commit`.
- Metrics: the shared `ep_ch_sink_*` with `role="agg"`, plus `agg_rows_by_kind_total{kind}` and `agg_visibility_lag_seconds` (insert time − `max_received_at`, the passive freshness input for step 8).

### 6.5 Backfill and reprocessing (safe by design, W12 needs only a command)

- `tools/processor-reset` (or `make processor-backfill FROM=<iso> TO=<iso>`): stop the processor, delete checkpoint rows for the processor id (they exist after step 7) or write a seek position for the timestamp (`offsetsForTimes`), and restart. The re-emitted rows carry the same keys and versions, and the replacement makes the result identical.
- Runbook `docs/runbooks/backfill.md`, with the warning that Path A must **not** be backfilled by re-inserting into raw (it would double count).

### 6.6 `tools/agg-compare`

Wire `path-b` (Path B queries with `FINAL`). `make agg-compare-all` compares reference vs path-b for every window spec over a range.

### 6.7 Docs

`docs/design/processor.md`, section "Idempotent emission": key, version, replace semantics, dedup TTL argument, and why exactly-once state alone does not give exactly-once output. Update ADR 0010 with the aggregate part.

### 6.8 Tests

| Layer | Test | Pass condition |
| --- | --- | --- |
| Unit | Dedup: duplicate inside the horizon is skipped, duplicate after the horizon goes to side output with `possible_duplicate`, the TTL purge bound | Spec |
| Sink e2e (`agg-sink.e2e-spec.ts`) | Retried identical batches | Second insert writes 0 rows |
| Sink e2e | Interleaved versions (v2, v4, v3 final... for many keys) and out-of-order versions | Highest version wins via `FINAL` before any merge, and via plain select after `OPTIMIZE TABLE ... FINAL` |
| Duplicates e2e (`duplicates.e2e-spec.ts`) | Re-produce 10% of the events to `events.raw` | Path B aggregates equal the reference. Path A over-counts by exactly the duplicate count. Raw `FINAL` count unchanged |
| Replay e2e (`replay.e2e-spec.ts`) | Reset the processor group to an earlier offset (or the backfill command) and reprocess | `agg-compare reference path-b` is equal before and after, with no doubling |
| Backfill e2e | Backfill one window range while live traffic continues | Equal, and live lag recovers |
| Static | Every `clickhouse/queries/path_b/*.sql` contains `FINAL` or `argMax` | Pass |

### Done when

- [ ] `aggregates.out` has a registered Avro contract and a deterministic key.
- [ ] Processor dedup is in place and bounded. The agg sink writes the three `ReplacingMergeTree(version)` tables.
- [ ] Duplicate, replay and backfill tests show no double counting in Path B.
- [ ] `agg-compare` reference vs path-b passes. The docs and runbook are written.

---

## Step 7: S4-F5 Checkpointing and recovery

**Requirement:** processor state is checkpointed with source offsets atomically and restored on restart.

**Extends:** processor offsets (replaces the step 3 Kafka commits), watermark sharing (step 4), `seen` and `pending` (steps 5 and 6).

### 7.1 Migration `db/migrations/0009_processor_checkpoint.sql`

```sql
CREATE TABLE processor_checkpoint (
  processor_id  text   NOT NULL,
  partition     int    NOT NULL,
  checkpoint_id bigint NOT NULL,
  kafka_offset  bigint NOT NULL,        -- next offset to read
  watermark_ms  bigint NOT NULL,        -- this partition's watermark, also read by other instances
  max_event_ts  bigint,
  owner         text   NOT NULL,        -- instance id that wrote it
  updated_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (processor_id, partition)
);
CREATE TABLE processor_state (
  processor_id  text   NOT NULL,
  partition     int    NOT NULL,
  checkpoint_id bigint NOT NULL,
  format        smallint NOT NULL,      -- serialization format version
  state         bytea  NOT NULL,
  size_bytes    int    NOT NULL,
  PRIMARY KEY (processor_id, partition)
);
```

These are the PRD tables, plus `checkpoint_id` and `owner` on the checkpoint row and `format` and `size_bytes` on the state row.

### 7.2 Serialization (`services/processor/src/checkpoint/codec.ts`)

- Format 1: a header `{format, processorId, partition, checkpointId, windowsConfigHash}` followed by gzip of a compact JSON with the window entries, the partition clock, `seen`, `pending` and `lastEmittedVersion`.
- `windowsConfigHash` covers the window specs, lateness and the value field. When restoring with a different hash, the processor refuses by default (`ON_CONFIG_CHANGE=refuse|reset`). Changing the window config is a backfill, not a silent restart.
- A round-trip unit test runs over generated states (property test: `decode(encode(s))` deep-equals `s`, with ±Infinity and null handled).

### 7.3 Checkpoint protocol (write it into the design doc exactly)

The steps per partition, every `CHECKPOINT_INTERVAL_MS` (default 5000) or every `CHECKPOINT_EVERY_EVENTS`:

1. **Quiesce the lane** at a batch boundary. The state now reflects exactly offsets `< nextOffset`.
2. **Serialize** the state, including `pending` (emissions that are not acked yet) and `seen`.
3. **One Postgres transaction:**
   ```sql
   BEGIN;
   UPDATE processor_state SET state=$s, checkpoint_id=$new, format=1, size_bytes=$n
     WHERE processor_id=$id AND partition=$p AND checkpoint_id=$prev;      -- or INSERT for the first
   -- crash point processor:during-checkpoint (state row written, checkpoint row not, tx open)
   UPDATE processor_checkpoint SET checkpoint_id=$new, kafka_offset=$next, watermark_ms=$wm,
     max_event_ts=$max, owner=$me, updated_at=now()
     WHERE processor_id=$id AND partition=$p AND checkpoint_id=$prev;
   COMMIT;
   ```
   - **Fencing:** both updates are conditional on `$prev`. If 0 rows are updated, another instance has written since then (a zombie after a rebalance), so roll back, drop the partition's state, and stop processing that partition. Metric: `checkpoint_fenced_total`.
4. After the commit, commit the Kafka group offset `nextOffset` **for lag monitoring only** (async, errors ignored). The PRD says "Kafka consumer-group commits are kept only for lag monitoring."

**Emission ordering:** emission (step 5) is independent of checkpointing and can happen before or after it. Both orders are safe:

- crash **after emit, before checkpoint**: restore the old state, replay the events, and re-emit the *same* keys and versions (deterministic). The sink absorbs them.
- crash **after checkpoint, before emit**: `pending` and the dirty flags are in the checkpoint, so they are emitted after the restart.
- crash **during checkpoint**: the transaction rolls back, and the old checkpoint is fully visible.

The side output (step 4) follows the same rule. Side records are put into `pending` (kind `late`), so they are produced at least once with the same key.

### 7.4 Restore and assignment

- Rebalance callback **on assign**: for each partition, load the checkpoint and state rows in one `SELECT ... JOIN ... WHERE s.checkpoint_id = c.checkpoint_id`. A mismatch should be impossible because they are written in one transaction, but it is treated as corruption: fail loudly and alert. Then decode, rebuild the indexes, and **seek** to `kafka_offset` (assign with explicit offsets in the rebalance callback). No row means a fresh start from `PROCESSOR_START=earliest|latest`.
- **On revoke:** try one final checkpoint within `REVOKE_CHECKPOINT_TIMEOUT_MS`, then drop the partition state. If the checkpoint fails, nothing is lost: the new owner replays from the last checkpoint.
- **Readiness:** `/readyz` is false until every assigned partition is restored and seeked. Metrics record `recovery_duration_seconds{partition}` and `replayed_events_total` (events between the checkpoint offset and the offset at the time of the crash, measured with `restore offset` and the high-water mark at the restart). Recovery is "bounded" when it finishes within `RECOVERY_SLO_MS`, which is asserted in the test.
- Multi-instance watermark: other instances' `watermark_ms` are read from `processor_checkpoint` (this completes step 4's single-instance restriction, so the startup warning is removed).

### 7.5 Crash points (add to `points.ts`, with matrix scenarios)

| Point | State when the process dies |
| --- | --- |
| `processor:after-update` | batch merged into memory, nothing emitted, nothing checkpointed |
| `processor:before-checkpoint` | lane quiesced, state serialized, transaction not started |
| `processor:during-checkpoint` | state row updated inside the open transaction, checkpoint row not |
| `processor:after-checkpoint-before-emit` | checkpoint committed, pending emissions not produced |
| `processor:after-emit-before-checkpoint` | emissions acked, newer state not checkpointed |
| `processor:after-side-output-before-checkpoint` | late record produced, checkpoint not written |

### 7.6 Metrics

`ep_processor_checkpoint_duration_seconds`, `checkpoint_size_bytes{partition}`, `checkpoint_interval_seconds` (actual), `checkpoints_total{outcome=ok|fenced|error}`, `recovery_duration_seconds`, `replayed_events_total`, `restore_total{outcome}`.

### 7.7 Docs

- `docs/design/processor.md`, section "Checkpoint protocol": the steps above, a sequence diagram, the reasoning for each crash point, the interval trade-off (overhead vs replay work), and the fencing rule.
- ADR `0012-checkpoint-store-postgres.md`: Postgres bytea vs a compacted changelog topic plus local state (Kafka Streams style), with a size limit to revisit.
- Runbook `processor-recovery.md`: how to inspect checkpoints, force a reset, and handle a config-hash refusal.

### 7.8 Tests

| Layer | Test | Pass condition |
| --- | --- | --- |
| Unit | Codec round trip (property), config-hash refusal, format version check | Spec |
| Integration (Postgres) | Fenced write with a stale `$prev` | 0 rows, rollback, partition dropped |
| Checkpoint atomicity (`checkpoint-atomicity.e2e-spec.ts`) | Kill at `processor:during-checkpoint` (and with `pg_terminate_backend` mid-transaction) | After the restart, the checkpoint and state rows have the same `checkpoint_id`, equal to either the old one or the new one, never a mix |
| **Recovery equivalence (`recovery.e2e-spec.ts`), the central proof** | For each `processor:*` crash point and for N random kill times (`SIGKILL` at a random offset): run the fixture to completion with crashes and restarts, flush, and compare `agg_*` (`FINAL`) with a run without crashes | Equal for every window spec (`agg-compare` exit 0). Duplicate emissions counted and > 0 for `after-emit-before-checkpoint` (it proves replacement absorbed them) |
| Recovery time | Measure restore plus catch-up for a fixed state size | Under `RECOVERY_SLO_MS`, recorded in the test report JSON |
| Rebalance e2e | Start a second processor instance mid-stream, then stop it | Final aggregates equal the reference. The watermark keeps advancing across instances |
| Crash-matrix runner | `tests/correctness` matrix includes every new point | `docs/generated/crash-matrix.md` regenerated with processor rows |

### Done when

- [ ] State, offset and watermark are written in one fenced transaction. Restore and seek work on assignment.
- [ ] The recovery-equivalence test passes at every crash point and at random kill times.
- [ ] The checkpoint atomicity test passes. Recovery time is measured and bounded.
- [ ] Multi-instance watermark sharing works. The design doc section, ADR 0012 and the runbook are written.

---

## Step 8: S4-F8 Query API and end-to-end freshness

**Requirement:** a query API with tenant-scoped aggregates over a time range and raw event lookup, plus an end-to-end freshness metric.

### 8.1 Service `services/query-api` (NestJS, port 3003)

- **Auth:** extract the API-key guard from `ingest-api` into `packages/tenant-auth` (API key to tenant via Postgres `tenants`, cached with a TTL) and use it in both services. The tenant comes **only** from the key. No endpoint accepts a `tenant_id` parameter.
- **Endpoints** (documented in `docs/api-contract.md`):

| Method and path | Params | Returns |
| --- | --- | --- |
| `GET /v1/aggregates` | `window=1m\|1h\|5m_s1m`, `from`, `to` (ISO UTC, half-open `[from, to)`), optional `type` (repeatable), `source=processor\|mv` (default `processor`), `final_only=false` | Rows `{type, window_start, window_end, cnt, value_cnt, sum, min, max, avg, is_final}` plus `meta {source, watermark_ms, read_rows, elapsed_ms}` |
| `GET /v1/aggregates/top-types` | `from`, `to`, `limit<=100`, `source` | Types ranked by count |
| `GET /v1/events/:event_id` | none | One raw event (`FINAL`), or 404. It is found within the tenant only, and another tenant's id is a 404, not a 403 |
| `GET /v1/events` | `from`, `to`, `type?`, `limit<=1000`, `cursor` | Raw events ordered by `(occurred_at, event_id)`, keyset pagination with an opaque cursor |
| `GET /v1/freshness` | none | Latest probe results (p50/p99 over the last N) |

- **Validation:** `from < to`. The maximum range per window is `1m` up to 2 days, `1h` up to 90 days, and raw up to 1 day (configurable). Timestamps are aligned down to the window size for aggregates, and the response states the aligned range. Inputs that are not UTC (offsets) are parsed and converted to UTC, and the response is always `Z`. Everything else is a 400 with the existing error format.
- **Query layer:** `QueryRepository` with one method per endpoint, each using a committed SQL file from `clickhouse/queries/path_b|path_a|raw/` with query parameters (`{tenant:String}`, `{from:DateTime64(3)}`, ...). `tenant_id = {tenant:String}` is the **first** predicate, so the ordering key prunes on it. Settings per query: `max_execution_time`, `max_result_rows`, and `readonly=1`.
- **Defense in depth:** the ClickHouse user `query_api` (in `users.d`, read-only) plus migration `0005_row_policies.sql`:
  `CREATE ROW POLICY tenant_scope ON events_raw, agg_1m, agg_1h, agg_5m_s1m, agg_1m_mv_target, agg_1h_mv_target, late_events USING tenant_id = getSetting('SQL_tenant_id') TO query_api;`
  Each query also sends the setting `SQL_tenant_id=<tenant>`. Even an SQL bug that drops the `WHERE` cannot read another tenant's rows.
- **Response metadata:** `read_rows`, `read_bytes` and `elapsed` come from the ClickHouse response summary. `watermark_ms` is the minimum watermark across partitions from the latest rows, so the client knows how final the data is.

### 8.2 Freshness metric

- **Definition** (written in the API contract): freshness = the time from the API's `received_at` to the moment the event is counted in a queryable aggregate (Path B `agg_1m` via `FINAL`).
- **Active prober** (`services/query-api/src/freshness/prober.ts`, can be turned on with `FRESHNESS_PROBE=true`): every `PROBE_INTERVAL_MS` (default 5000) it sends a marker event via `ingest-api` as the reserved tenant `__probe` (seeded by `scripts/seed-tenants.sh`) with type `freshness_marker` and a monotonic `value`. Then it polls `agg_1m` (through the same repository, as tenant `__probe`) until `cnt` for the marker's minute includes it (expected count tracked locally), and records `ep_freshness_seconds{path="processor"}`. The same loop also checks Path A (`path="mv"`) and raw (`path="raw"`), so the three paths can be compared. Timeout `PROBE_TIMEOUT_MS=30000` counts `probe_timeouts_total`.
- **Passive** (from step 6): `ep_ch_sink_agg_visibility_lag_seconds`, a histogram over every inserted row, is exposed as a second signal.
- The `__probe` tenant is excluded from tenant-facing metrics and from the audit.
- A Grafana panel JSON for freshness p50/p99 is added to `infra/observability/dashboards/stage-4/pipeline.json`, together with the processor, sink and ClickHouse panels from section 9 of the PRD.

### 8.3 Dashboard query performance (verifying the target, not experimenting)

- `clickhouse/generator/generate.sql`: `INSERT INTO events_raw SELECT ... FROM numbers(N)`, with realistic tenant and type distributions, `occurred_at` spread over 30 days, and a parameter `N`. `make ch-generate N=100000000`.
- `clickhouse/bench/dashboard/*.sql` is the API's own queries with fixed parameters. `make ch-bench` runs `clickhouse-benchmark` and writes p50/p95/p99 to `docs/generated/query-bench.json`. Full W1 work (Postgres comparison, multiple sizes) stays in the experiment.

### 8.4 Metrics

`ep_query_api_requests_total{route,status}`, `request_duration_seconds{route}`, `clickhouse_read_rows{route}` (histogram), `clickhouse_elapsed_seconds{route}`, `ep_freshness_seconds{path}`, `probe_timeouts_total{path}`.

### 8.5 Tests

| Layer | Test | Pass condition |
| --- | --- | --- |
| Unit | Range validation: inverted, too wide, misaligned, non-UTC offsets, missing params | Spec 400s, alignment correct |
| Unit | Cursor encode and decode, opaque, rejects tampering | Spec |
| E2E tenant scoping (`query-scoping.e2e-spec.ts`) | Two tenants with identical types and event ids. Tenant A queries every endpoint | Never sees B's data. B's event id gives 404 |
| E2E row policy | As `query_api` user, run `SELECT count() FROM events_raw` with the setting for A and no `WHERE` | Returns only A's count. Without the setting: an error, or 0 rows |
| E2E time range edges | Events exactly at `from` (included), at `to` (excluded), at the window edge | Correct bounds |
| E2E correctness | `/v1/aggregates` (processor and mv) vs reference SQL for the golden fixture | Equal (mv also equal because the fixture has no duplicates) |
| E2E freshness (`freshness.e2e-spec.ts`) | Run the prober for 60 s on a running pipeline | Samples recorded. p99 reported in the test report JSON (the target 5 s is checked and reported, not asserted as a hard gate) |
| Perf smoke | `make ch-bench` at 100M rows | p95 recorded. Under 200 ms is the target, reported |

### Done when

- [ ] The query API serves tenant-scoped aggregates (both paths) and raw lookup and pagination, with validation.
- [ ] The row policy is in place, and the scoping tests pass.
- [ ] The freshness prober and passive metric are visible in Prometheus and Grafana.
- [ ] Dashboard query p95 at 100M+ rows is measured and recorded. The API contract is updated.

---

## Step 9: S4-F9 Retire the Postgres events sink

**Requirement:** after the comparison, retire the Postgres events sink. Postgres keeps only metadata and the outbox.

**Precondition:** the Postgres vs ClickHouse comparison (artifact 1, from W1) is written. This step does not run the comparison. It only checks that `docs/reports/dashboard-query-report.md` exists before the migration runs (`scripts/retire-pg-sink.sh` refuses without it, and `--force` overrides).

### 9.1 Inventory first (write it into the step's commit message and `docs/design/storage-after-stage-4.md`)

Everything that reads or writes Postgres `events`: `consumer-sink` (batch writer, retry tiers, e2e tests), `tests/correctness` (`harness.ts`, durability, ordering, event-time, order-property, schema, retry suites), `tools/audit` (reconciliation), `tools/dlq-replay` (precheck, if it looks up stored events), `scripts/audit.sh`, `scripts/reset.sh`, and the Stage 1 and 2 loadtest scripts. The loadtest scripts stay as historical experiment code and get a header note.

### 9.2 Port the readers to ClickHouse

- `tests/correctness/src/storage.ts`: a `StorageReader` interface (`countByRun`, `keysByRun`, `get(tenant, eventId)`, `occurredAt`/`receivedAt` lookups). `ClickHouseStorageReader` reads `events_raw FINAL`. The Postgres implementation is deleted after the port.
- Stage 3 suites run against ClickHouse: durability (no loss), replay (no duplicates under `FINAL`), event-time (late and out-of-order stored with both timestamps), and ordering (where the claim still applies, it is restated as "storage is order-insensitive"). Suites whose claim was about the Postgres sink's commit protocol (`consumer:*` crash points) are re-expressed with the `raw-sink:*` points from step 1.
- `tools/audit`: the snapshot reads ClickHouse (`count() FINAL`, keys per run id from `payload` via `JSONExtractString`), and it also reconciles aggregates (`agg-compare reference path-b` for the run's range). `make audit` keeps working.
- `tools/dlq-replay`: any check of "already stored" goes to `events_raw` with `FINAL`. The replay target (`events.raw`) is unchanged.

### 9.3 Retire the write path

- **Retry tiers:** `events.retry.{1,2,3}` existed for Postgres per-record errors. The ClickHouse raw sink retries whole batches and sends poison to the DLQ (step 1), so it does not use them. Before stopping `consumer-sink`, drain the retry topics (wait for `sink-postgres` and retry-group lag 0, which the script checks). Then delete the consumer groups. Keep the topics for one more stage, documented in the runbook, and drop them from `kafka-init` in Stage 5.
- Remove `consumer-sink` from `scripts/up.sh`, `scripts/down.sh` and the Makefile (`SINK` target), and remove its crash points from `points.ts` (the `consumer:*` names map to `raw-sink:*` through a `LEGACY_CRASH_AT` style alias for old scripts). Delete `services/consumer-sink`. Its shared pieces (DLQ publisher, parse, lag) were moved to packages in steps 1 and 3. Tag `stage-3-complete` already preserves the old code.
- Migration `db/migrations/0010_retire_events.sql`:
  - `ALTER TABLE events RENAME TO events_stage3_archive;` (no data loss. Dropping it is a separate manual `make pg-drop-archive`, which asks first).
  - Drop `tenant_counters` and `DELETE FROM sink_offsets WHERE consumer_group = 'sink-postgres'` (verify what `tenant_counters` feeds before dropping it. If anything still reads it, port it first).
  - The down migration renames it back.
- After this step, Postgres holds: `tenants`, `tenant_config`, `outbox`, `dlq_meta`, `dlq_replays`, `processor_checkpoint`, `processor_state`, `schema_migrations`, and the archive table until it is dropped. Add a test (`storage-inventory.spec.ts`) that lists the Postgres tables and fails on any table not in this allow-list.

### 9.4 Docs

- `docs/design/storage-after-stage-4.md`: what lives where and why.
- `README.md` and `infra/README.md`: the pipeline diagram updated (the Postgres sink is gone).
- Runbook `docs/runbooks/dlq-triage-and-replay.md`: updated for ClickHouse lookups.

### 9.5 Tests

| Test | Pass condition |
| --- | --- |
| Full `make test` and `make e2e` after the port | Green, with no references to Postgres `events` outside the archive migration (a grep check in CI) |
| Stage 3 durability, replay and event-time suites on ClickHouse | Pass |
| Storage inventory allow-list | Pass |
| `make audit` | Reconciles ingest and ClickHouse raw and aggregates for a run |
| Retire script on a running pipeline | Refuses with retry lag > 0. Succeeds after the drain. No event lost (audit) |

### Done when

- [ ] Every reader is ported to ClickHouse. `consumer-sink` is removed from the pipeline and the repo.
- [ ] The Postgres `events` table is archived. Postgres holds only the allow-listed metadata, outbox and checkpoint tables.
- [ ] All Stage 3 guarantees still have passing tests, now against ClickHouse.
- [ ] Docs and runbooks are updated.

---

## Step 10: S4-F10 Delivery guarantees extended with analytics guarantees

**Requirement:** the delivery guarantees document extended with analytics guarantees.

### 10.1 Restate the changed Stage 3 claims

- **G1 Durability:** storage is now ClickHouse `events_raw`. A 202 means the event is in Kafka with acks=all, and it reaches `events_raw` at least once.
- **G2 Storage idempotency:** restated. Raw duplicates are collapsed **eventually** (on merge) and are invisible under `FINAL` or `argMax`, and identical retried batches are dropped at insert. It is not "unique at insert" any more, and this is said plainly.
- **G4 / G5:** confirmed on ClickHouse. The tests are re-pointed by step 9.

### 10.2 New claims (each mapped to its test in the same style as G1 to G10)

| Claim | Statement | Proving test(s) |
| --- | --- | --- |
| G11 Event-time correctness | For any input whose disorder is within `OUT_OF_ORDERNESS + ALLOWED_LATENESS`, the final Path B aggregates equal the SQL reference over raw, whatever the arrival order | `order-property.spec.ts`, `golden.e2e-spec.ts` |
| G12 Finality | A window is final when the global watermark reaches `end + ALLOWED_LATENESS`. Before that, `is_final` rows may still be updated by late data with a higher version. Early rows (`is_final=0`) are provisional | `emission.e2e-spec.ts`, `late.e2e-spec.ts` |
| G13 Late data is quarantined, not lost | Events beyond the policy are in `events.late` and `late_events` with lateness and the rejected windows | `late.e2e-spec.ts` |
| G14 Restart equivalence | Crashes at any processor crash point, or at random times, leave final aggregates equal to a run without crashes | `recovery.e2e-spec.ts` |
| G15 Checkpoint atomicity | State and offsets are visible together: old or new, never a mix. Zombie writers are fenced | `checkpoint-atomicity.e2e-spec.ts` |
| G16 Idempotent output | Replays, backfills, duplicate emissions and duplicate input events do not change Path B aggregates. The highest version wins | `duplicates.e2e-spec.ts`, `replay.e2e-spec.ts`, `agg-sink.e2e-spec.ts` |
| G17 Tenant isolation in analytics | No query API response contains another tenant's data, enforced in SQL and in the ClickHouse row policy | `query-scoping.e2e-spec.ts` |
| G18 Freshness (measured, not promised) | Target: 99% of events visible in Path B aggregates within 5 s of ingest. The measured p50/p99 from the latest run are embedded from `docs/generated/test-reports/freshness.json` | `freshness.e2e-spec.ts` |

### 10.3 "Not guaranteed" and "Where exactly-once stops", extended

- Path A double counts duplicates inserted into raw and has no finality. It is a comparison path, not a source of truth (unless the ADR in 10.4 decides otherwise).
- Aggregates are **eventually consistent**: the dashboard may show early values, and the freshness is the target above, not a hard bound.
- Side-output membership of an event exactly at the eviction boundary can depend on how partitions interleave (the step 4 caveat).
- Raw queries without `FINAL` can see duplicates.
- Exactly-once stops at: the processor state (exactly once through checkpoints and dedup) → output (at least once, made idempotent by key and version) → ClickHouse (eventually unique under replacement, exact under `FINAL`).

### 10.4 ADR `0013-path-a-vs-path-b.md`

Decide which path is authoritative for the dashboard: Path B, Path A, or a hybrid such as Path A for early or approximate values and Path B for final. Use the data produced by the tests in steps 2 and 6 (duplicate behavior, late-data behavior, freshness per path from step 8's prober, and the code and operational cost). Experiment numbers (W3, W8) are appended when they exist. The query API default `source` follows the decision.

### 10.5 Tooling and CI

- `tools/guarantee-check`: register the new suites (`tests/stream` reports in `docs/generated/test-reports/stream-*.json`) so that every claim's test must exist, pass, and be stamped against the current source hash (the existing Stage 3 mechanism).
- Makefile: `make stream-test` (unit plus property), `make stream-e2e` (golden, late, emission, duplicates, replay, recovery, atomicity, scoping, freshness), both with the `json_report` and `stamp` macros.
- Regenerate `docs/generated/crash-matrix.md` with the `raw-sink:*`, `processor:*` and `agg-sink:*` rows.
- `.github/workflows/ci.yml`: unit and property tests for `packages/windowing` and `services/*`, `schema-check` for the new subjects, `guarantee-check`, and the "no Postgres events references" grep.

### 10.6 Tests

| Test | Pass condition |
| --- | --- |
| `guarantee-check verify` | Every claim G1 to G18 maps to an existing, passing, freshly stamped test |
| Remove one mapped test locally | `guarantee-check` fails (a negative check on the checker itself, already covered by `check.spec.ts`. Add a case for the new suites) |
| Crash matrix generation | Every crash point in `points.ts` has a row and a passing scenario |

### Done when

- [ ] `docs/delivery-guarantees.md` has the restated G1/G2 and the new G11 to G18, each mapped to a passing test, plus the extended "Not guaranteed" and "exactly-once" sections.
- [ ] The freshness numbers are embedded from a real run.
- [ ] ADR 0013 is written, and the query API default follows it.
- [ ] CI runs the stream suites, schema-check, guarantee-check and the grep checks. The crash matrix is regenerated.
- [ ] Tag `stage-4-complete` (after the experiments and notebook entries, which are outside this plan).

---

## Appendix A: requirement to step coverage

| PRD item | Where it is covered |
| --- | --- |
| S4-F1 raw sink, batching, retries safe for the dedup choice | Step 1 |
| S4-F2 count/sum/min/max, 1m, 1h, 5m/1m, event time | Step 3 (plus emission in 5 and output in 6) |
| S4-F3 out-of-orderness 10 s, lateness 2 min, side output | Step 4 |
| S4-F4 early, final, versioned late re-emission | Step 5 |
| S4-F5 atomic checkpoint plus offsets, restore | Step 7 |
| S4-F6 deterministic key and version, no double count on replay | Step 6 |
| S4-F7 materialized views | Step 2 |
| S4-F8 query API, tenant scoping, freshness | Step 8 |
| S4-F9 retire the Postgres events sink | Step 9 |
| S4-F10 guarantees document | Step 10 |
| Test plan (section 7): unit, golden, property, late, duplicates, recovery, atomicity, sink, query, freshness | Steps 3, 4, 3/4, 4, 6, 7, 7, 6, 8, 8 |
| Window semantics written exactly into the design doc | Steps 3 and 4 (`docs/design/processor.md`) |
| Dedup decision as an ADR | Steps 1 and 6 (ADR 0010) |
| ADRs: ClickHouse keys, dedup, Path A vs B | 0009 (step 1), 0010 (steps 1 and 6), 0013 (step 10). Flink/Kafka Streams ADR belongs to W13 (out of scope) |
| Section 9 metrics (processor, ClickHouse, end to end) | Steps 1, 3 to 8 (Grafana board in step 8) |
| Performance targets: dashboard <200 ms p95 at 100M, freshness 99% <5 s, bounded recovery | Step 8 (bench, prober), step 7 (recovery time) |

## Appendix B: knobs exposed for the experiments (built here, used later)

| Experiment | Knobs and hooks this plan provides |
| --- | --- |
| W1 | `ch-generate`, `ch-bench`, reference SQL, `query_log` on |
| W2 | `processor:*` crash points, `CHECKPOINT_INTERVAL_MS`, recovery metrics |
| W3 | `OUT_OF_ORDERNESS_MS`, `ALLOWED_LATENESS_MS`, lateness buckets, `agg-compare path-a path-b` |
| W4 | `WATERMARK_IDLE_DETECTION`, `IDLE_TIMEOUT_MS`, per-partition watermark metrics |
| W5 | `PROCESSOR_MAX_KEYS_PER_PARTITION`, state size metrics, DDL variants |
| W6 | `CH_BATCH_MAX_ROWS`, `CH_FLUSH_INTERVAL_MS`, async insert setting passthrough (`CH_ASYNC_INSERT=1`) |
| W7, W10 | DDL variants in migrations comments, `EXPLAIN indexes = 1` in bench |
| W8 | `events_raw_plain` variant, `PROCESSOR_DEDUP`, `non_replicated_deduplication_window` |
| W9 | Toxiproxy `ch` proxy, `ch-insert-error`/`delay` faults, sink lag metrics |
| W11 | UTC-only windowing with boundary unit tests |
| W12 | `processor-backfill`, compacted `aggregates.out` |
