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
| Kafka offset commit fails after the DB commit | `OffsetCommitError` (counted as `commit_errors`). Rows are stored; the offset is committed with a later chunk, or the next owner reprocesses the chunk: duplicates, never loss |
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

## Other behavior

- **Duplicates are counted, not hidden.** `attempted - inserted` = records absorbed by `ON CONFLICT` (reprocessing or client retries).
- **Poison records block their partition.** Parsing is per chunk: earlier chunks are committed, then the poison chunk throws and is retried forever, so lag grows on that partition only. This is on purpose for now: S2-F4 first observes it, then adds bounded retry and `events.dlq`.
- **Backpressure:** a partition gets no new batch until its current one returns. `CONSUMER_MAX_BYTES_PER_PARTITION` / `CONSUMER_MAX_BYTES` bound the prefetch.
- **The client's first batches are small** (often 1 message). They grow to `CONSUMER_MAX_BATCH_SIZE` as the client's cache warms up.

## Tests

```bash
pnpm test        # unit: parser, writer, commit protocol + crash points (fakes), config
pnpm test:e2e    # real Kafka + Postgres (+ Toxiproxy for the DB-down test); builds first.
                 # Crash tests run consumer-sink as a child process on a throw-away topic and group.
```
