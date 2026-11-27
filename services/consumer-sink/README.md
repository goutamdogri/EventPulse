# consumer-sink

Stage 2 service (S2-F2). It is a member of consumer group **`sink-postgres`**: it reads `events.raw` and writes events to the Postgres `events` table in batches.

```
events.raw (6 partitions) --> eachBatch (one partition, up to CONSUMER_MAX_BATCH_SIZE msgs)
                                 | parse every record (src/sink/parse.ts)
                                 | chunks of SINK_MAX_ROWS -> BEGIN; INSERT ... SELECT FROM unnest(...)
                                 |                            ON CONFLICT (tenant_id, event_id) DO NOTHING;
                                 |                            upsert sink_offsets; COMMIT   (Stage 3, S3-F2)
                                 | after EACH chunk's COMMIT: resolveOffset(last of chunk) + commit offsets to Kafka
                                 v
                              Postgres events (received_at from the Kafka header, stored_at = now())
```

## Run

```bash
make kafka-up               # 3 brokers + topics
make migrate                # applies db/migrations (0004 adds events.stored_at, 0005 the (tenant_id, event_id) key)
make build
make consumer-start         # reads services/consumer-sink/.env (copy .env.example)
```

Run more instances by giving each its own `METRICS_PORT`. With 6 partitions, up to 6 members get work and any extra members stay idle.

HTTP endpoints (default port 8001):
- `GET /healthz`: liveness.
- `GET /readyz`: 200 once the member has joined the group.
- `GET /metrics`: Prometheus (scraped as job `eventpulse-consumer-sink`, ports 8001-8003).
- `GET /debug/stats`: assigned partitions, group lag per partition, consumed / inserted / **duplicates** / dlq, errors, retries, chunk failures, rebalances, pauses, p99 timings, freshness, pool, effective config.

## Metrics (S2-F6)

Every series is also a field of `/debug/stats`. Dashboard: `infra/observability/dashboards/stage-2/pipeline.json` ("EventPulse Stage 2 - Pipeline"), imported through the Grafana UI.

| Series | Meaning |
| --- | --- |
| `ep_consumer_lag{partition}` | group lag = log end - committed offset, polled every `LAG_POLL_MS` (2 s) by an admin client; the same value on every member, so use `max by (partition)` |
| `ep_consumer_last_committed_offset{partition}` | from the same poll (-1 = never committed) |
| `ep_consumer_messages_consumed_total`, `ep_consumer_rows_inserted_total`, `ep_consumer_duplicates_total`, `ep_consumer_dlq_total{error_class}` | added together when a chunk **completes**, so `consumed = inserted + duplicates + dlq` exactly |
| `ep_consumer_chunk_failures_total{reason}` | chunk attempts that failed and will be retried (`poison_blocked`, `db_unavailable`, `dlq_publish`, `commit`, `other`): a retry loop shows here |
| `ep_consumer_errors_total{class}`, `ep_consumer_retries_total{class}` | bad records (permanent classes) / failed write attempts (transient, systemic); retries |
| `ep_event_freshness_seconds` | end-to-end visibility: DB commit time - `received_at` at the API, per **newly inserted** row (`RETURNING`), never for duplicates |
| `ep_consumer_batch_size_messages{kind=batch\|chunk}`, `ep_consumer_batch_processing_seconds`, `ep_consumer_db_write_seconds`, `ep_consumer_commit_seconds` | sizes and timings |
| `ep_consumer_commit_errors_total`, `ep_consumer_rebalances_total`, `ep_consumer_assigned_partitions`, `ep_consumer_paused` | group health |
| `ep_pg_pool_total` / `_idle` / `_waiting`, `ep_pg_pool_connect_wait_seconds` | Postgres pool |

`/debug/stats` also has `local_lag` (per partition, from the last batch, this member only) and reads only cached values: no Kafka call happens inside the HTTP handler.

## Delivery semantics: at-least-once (S2-F3)

Offsets are committed **only after the database transaction commits**, one chunk at a time. Exactly one code path resolves and commits offsets: [`handleBatch`](src/consumer/batch-handler.ts).

```
for each chunk (SINK_MAX_ROWS messages) of a batch:
    stop if shutting down (isRunning) or the partition was revoked (isStale)
    parse                                     <- crash points consumer:before-parse / consumer:after-parse
    BEGIN                                     <- consumer:before-insert
    INSERT ... ON CONFLICT (tenant_id, event_id) DO NOTHING RETURNING tenant_id, event_id
    upsert sink_offsets (GREATEST)            <- consumer:after-insert-before-commit
    COMMIT                                    <- consumer:after-db-commit
    (only if the chunk had duplicates: compare their content_hash with the stored rows -> conflicts)
    stop (without resolving) if the partition was revoked during the write
    resolveOffset(last offset of chunk)       (the client stores last + 1 = next offset to read)
    commit offsets to Kafka, heartbeat        <- consumer:after-offset-commit
```

### Stage 3 (S3-F2): idempotent on `(tenant_id, event_id)`

- The primary key is `(tenant_id, event_id)` (migration 0005): two tenants can use the same id.
- `DO NOTHING`, never `DO UPDATE`: the first committed copy wins and a duplicate changes nothing (not even `stored_at`).
- A duplicate whose `content_hash` differs from the stored row (same key, other content: a producer bug) is a
  **conflicting duplicate**: counted in `ep_sink_conflicting_duplicates_total` / `conflicting_duplicates`, never written.
- `sink_offsets` keeps the highest stored offset per (group, topic, partition), written in the same transaction
  (used by Stage 3 step 3 to tell reprocessed from redelivered duplicates; see ADR 0002).
- Proof: `tests/correctness` (log replay three times, replay from mid-partition, crash matrix at every consumer point;
  generated table in `docs/generated/crash-matrix.md`).

### Stage 3 (S3-F3): duplicates are counted by cause, never silently swallowed

Every duplicate the key absorbs is counted with its cause, from the record's offset:

| Cause | Meaning | Typical source |
| --- | --- | --- |
| `reprocessed` | the offset was already stored once for this partition (`sink_offsets`) | crash between DB commit and offset commit, rebalance, offset reset |
| `redelivered` | a new offset carrying a key that is already stored | a client retry that got past the API's idempotency (window expired, Redis lost, `fail_open`, API crash), a DLQ replay |
| `unknown` | `sink_offsets` could not be read for that chunk | Postgres trouble; counted, never dropped |

- Metrics: `ep_sink_duplicates_total{cause}`, `ep_sink_reprocessed_messages_total{partition}`, `ep_sink_rows_attempted_total`,
  `ep_sink_rows_inserted_total`, `ep_sink_conflicting_duplicates_total` (the Stage 2 `ep_consumer_*` series stay for the
  Stage 2 dashboards). `/debug/stats`: `duplicates_by_cause`, `reprocessed_messages`, `reprocessed_by_partition`.
- One structured log line per chunk that had duplicates: `{"msg":"duplicates","partition":..,"duplicates":..,"reprocessed":..,"redelivered":..,"conflicts":..}`.
- Dashboard: `infra/observability/dashboards/stage-3/correctness.json` (duplicates by layer: API replayed vs sink by cause).
- Reconciliation of a run: `make audit` (checks G, J "unexplained difference", K). Proof: `tests/correctness/test/duplicates.e2e-spec.ts`.

| Crash point | Rows in DB | Offset committed | Result after restart |
| --- | --- | --- | --- |
| before parse | no | no | chunk reprocessed: no loss, no duplicate rows |
| after parse | no | no | chunk reprocessed: no loss, no duplicate rows |
| after DB commit, before offset commit | yes | no | chunk reprocessed: `ON CONFLICT` absorbs it, **duplicates counted** (at most one chunk per partition) |
| after offset commit | yes | yes | nothing reprocessed |

Other failures:

| Failure | What happens |
| --- | --- |
| DB write fails (Postgres down, timeout) | nothing resolved, the client seeks back, the chunk is retried: no loss, no offset moves while the DB is down |
| Kafka offset commit fails after the DB commit | retried 3 times with backoff (rows are stored, so retrying is safe; each failure counts in `commit_errors`). If it still fails: `OffsetCommitError`, and the offset is committed with a later chunk or the chunk is reprocessed by the next owner: duplicates, never loss |
| Partition revoked mid-batch (rebalance) | chunks already committed stay committed; the rest is read by the new owner |
| SIGTERM | the current chunk finishes (DB commit, then offset commit), then the member leaves the group: no loss, no duplicates |

Measured with real SIGKILLs (`test/commit-protocol.e2e-spec.ts`, 5,000 events, chunks of 500, crash on the 4th batch):

| Crash point | rows at crash | committed at crash | duplicates after restart | missing |
| --- | --- | --- | --- | --- |
| after_db_commit | 1002 | 803 | 199 | 0 |
| before_parse | 847 | 847 | 0 | 0 |
| after_parse | 808 | 808 | 0 | 0 |

### Debug hooks (never set in normal runs)

- `FAULTS=<point>[@N|@bN]` (Stage 3, `packages/fault-injector`): SIGKILL this process at a named point on its Nth hit (`@N`) or in its Nth batch (`@bN`). `FAULT_ERRORS=<point>=<CODE>[xN|x*][:marker=TEXT]` makes that point throw an error with that code. `GET/POST/DELETE /debug/faults` show, arm and disarm them at runtime. All refused unless `DEBUG_FAULTS=true`.
- `CRASH_AT=before_parse|after_parse|after_db_commit` with `CRASH_AFTER_BATCHES=N` (Stage 2 form, still accepted): same as `FAULTS=consumer:<point>@bN`.
- `SINK_PROCESSING_DELAY_MS`: artificial work per chunk, before the DB write.
- `SINK_PROCESSING_DELAY_CHUNKS` / `SINK_PROCESSING_DELAY_SKIP_CHUNKS` (K8): delay only that many chunks (0 = every chunk), after skipping the first N.
- `CONSUMER_TOPIC`: read another topic (tests use a throw-away topic per run).

## Poison messages and the DLQ (S2-F4)

A record that fails **on its own** goes to `events.dlq`, and the offset moves past it. A database that is **down** never sends anything to the DLQ. Before the fix, one bad record blocked its partition forever: see [the before/after note](../../notebook/stage-2/s2-f4-poison-before-fix.md).

| Class | Examples | What the sink does |
| --- | --- | --- |
| `PERMANENT_PARSE` | invalid JSON, bad UUID, bad timestamp, type > 100 chars, missing tenant / `received_at` header | dead-letter at once (`dlq.attempts=1`) |
| `PERMANENT_DB` | Postgres rejects this row: `22xxx` (e.g. `22P05` `\u0000` in jsonb, `22001` too long), `23xxx` | bisect the chunk to isolate the row, store the good rows, retry the bad row `SINK_MAX_RETRIES` times with backoff, then dead-letter it |
| `UNKNOWN` | an error with no recognized code | same as `PERMANENT_DB` |
| `TRANSIENT_DB` | DB unreachable / overloaded / restarting: connection errors, `08xxx`, `53xxx`, `57P0x`, `57014`, `40001`, `40P01` | **never DLQ.** Retry with backoff for `SINK_TRANSIENT_RETRY_WINDOW_MS`, then pause the partition for `SINK_PAUSE_MS` and read the chunk again. No hot loop, no loss |
| `SYSTEMIC_DB` | every row would fail: `42xxx` (missing column...), `28xxx`, `3D000`, `0A`, `XX` | treated like `TRANSIENT_DB`, so a schema mistake doesn't dead-letter the whole topic. Needs a human |

Order per chunk: write the good rows (DB commit) → publish dead letters and wait for the ack (`acks=all`) → only then commit the offset. If the DLQ publish fails, nothing is committed and the chunk is retried, so a DLQ record can be duplicated (noted for Stage 3).

DLQ record: **value = the original bytes, key = the original key**, original headers kept, plus `dlq.original_topic`, `dlq.original_partition`, `dlq.original_offset`, `dlq.consumer_group`, `dlq.error_class`, `dlq.error_code`, `dlq.error_message` (≤ 1 KB), `dlq.attempts`, `dlq.failed_at`.

Tools:

```bash
scripts/produce-poison.sh --kind invalid-json|bad-uuid|oversized-field|constraint [--key KEY] [--topic T]
scripts/dlq-peek.sh [N] [--topic events.dlq]
```

`SINK_DLQ_ENABLED=false` brings back the blocking behavior on demand (used by a test, and for K6).

## Other behavior

- **Duplicates are counted, not hidden.** `attempted - inserted` = records absorbed by `ON CONFLICT` (reprocessing or client retries).
- **Backpressure:** a partition gets no new batch until its current one returns. `CONSUMER_MAX_BYTES_PER_PARTITION` / `CONSUMER_MAX_BYTES` bound the prefetch.
- **The client's first batches are small** (often 1 message). They grow to `CONSUMER_MAX_BATCH_SIZE` as the client's cache warms up.

## Tests

```bash
pnpm test        # unit: parser, writer, commit protocol, crash points, poison handling (fakes), classifier, config
pnpm test:e2e    # real Kafka + Postgres (+ Toxiproxy for the DB-down test); builds first.
                 # Crash tests run consumer-sink as a child process on a throw-away topic and group.
```
