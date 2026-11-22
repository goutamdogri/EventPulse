# consumer-sink

Stage 2 service (S2-F2). It is a member of consumer group **`sink-postgres`**: it reads `events.raw` and writes events to the Postgres `events` table in batches.

```
events.raw (6 partitions) --> eachBatch (one partition, up to CONSUMER_MAX_BATCH_SIZE msgs)
                                 | parse every record (src/sink/parse.ts)
                                 | chunks of SINK_MAX_ROWS -> BEGIN; INSERT ... SELECT FROM unnest(...)
                                 |                            ON CONFLICT (event_id) DO NOTHING; COMMIT
                                 | after the last COMMIT: resolveOffset(last) + commit offsets to Kafka
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

## Behavior in S2-F2

- **Offsets:** `autoCommit` is off. Offsets are committed only after the batch's DB transactions committed. S2-F3 refines this into a per-chunk protocol with rebalance checks and crash hooks.
- **Duplicates are counted, not hidden.** `attempted - inserted` is the number of records absorbed by `ON CONFLICT`. These come from reprocessing or from client retries.
- **Poison records block their partition.** If any record in a batch can't be parsed, the batch throws and the client seeks back and retries forever, so lag grows on that partition only. This is on purpose for now: S2-F4 first observes it, then adds bounded retry and `events.dlq`.
- **Backpressure:** a partition gets no new batch until its current one returns. `CONSUMER_MAX_BYTES_PER_PARTITION` / `CONSUMER_MAX_BYTES` bound the prefetch.
- **Shutdown** (SIGTERM/SIGINT): leave the group (the current batch finishes and commits), then close the pool.

## Tests

```bash
pnpm test        # unit: parser, writer, batch handler, config
pnpm test:e2e    # real Kafka + Postgres; each run uses its own throw-away consumer group
```
