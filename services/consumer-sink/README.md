# consumer-sink

Stage 2 service (S2-F2). It is a member of consumer group **`sink-postgres`**: it reads `events.raw` and writes events to the Postgres `events` table in batches.

```
events.raw (6 partitions) --> eachBatch (one partition, up to CONSUMER_MAX_BATCH_SIZE msgs)
                                 | parse every record (src/sink/parse.ts)
                                 | chunks of SINK_MAX_ROWS -> BEGIN; INSERT ... SELECT FROM unnest(...)
                                 |                            ON CONFLICT (event_id) DO NOTHING; COMMIT
                                 | after EACH chunk's COMMIT: resolveOffset(last of chunk) + commit offsets to Kafka
                                 v
                              Postgres events (received_at from the Kafka header, stored_at = now())
```

## Run

```bash
make stage2-kafka-up        # 3 brokers + topics
make stage2-migrate         # applies db/migrations (0004 adds events.stored_at)
make stage2-build
make stage2-consumer-start  # reads services/consumer-sink/.env (copy .env.example)
```

Run more instances by giving each its own `METRICS_PORT`. With 6 partitions, up to 6 members get work and any extra members stay idle.

HTTP endpoints (default port 8001):
- `GET /healthz`: liveness.
- `GET /readyz`: 200 once the member has joined the group.
- `GET /metrics`: Prometheus; process metrics only for now, sink series come in S2-F6.
- `GET /debug/stats`: assigned partitions, consumed / attempted / inserted / **duplicates**, batches, rebalances, pool, effective config.

## Delivery semantics: at-least-once (S2-F3)

Offsets are committed **only after the database transaction commits**, one chunk at a time. Exactly one code path resolves and commits offsets: [`handleBatch`](src/consumer/batch-handler.ts).

```
for each chunk (SINK_MAX_ROWS messages) of a batch:
    stop if shutting down (isRunning) or the partition was revoked (isStale)
    parse                                     <- crash points before_parse / after_parse
    BEGIN; INSERT ... ON CONFLICT DO NOTHING; COMMIT
                                              <- crash point after_db_commit
    stop (without resolving) if the partition was revoked during the write
    resolveOffset(last offset of chunk)       (the client stores last + 1 = next offset to read)
    commit offsets to Kafka, heartbeat
```

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

- `CRASH_AT=before_parse|after_parse|after_db_commit` with `CRASH_AFTER_BATCHES=N`: SIGKILL this process at that point on its Nth batch. Refused unless `DEBUG_FAULTS=true`.
- `SINK_PROCESSING_DELAY_MS`: artificial work per chunk, before the DB write.
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
