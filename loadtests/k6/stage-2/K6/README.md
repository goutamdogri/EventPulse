# K6 Poison message

Why this experiment and what I expect: see [notebook/stage-2/experiments/K6/Explanation.md](../../../../notebook/stage-2/experiments/K6/Explanation.md)

## 1. Run the server

`run.sh` starts and stops the ingest-api and one consumer-sink per run itself, so there is nothing to start by hand. It needs the stack up and both services built (the consumer-sink must have the S2-F4 change: `run.sh` checks for `SINK_DLQ_ENABLED` in its `dist`).

```bash
docker compose -f infra/compose/docker-compose.yml up -d                        # Prometheus, Grafana, node_exporter, Postgres
docker compose -f infra/compose/docker-compose.kafka.yml up -d                  # 3 brokers + kafka-exporter
pnpm --filter @eventpulse/ingest-api build && pnpm --filter @eventpulse/consumer-sink build
```

Per run the script starts the API (same for every cell) and the consumer-sink like this:

```bash
cd services/ingest-api && env KAFKA_BROKERS=localhost:9092,localhost:9094,localhost:9096 KAFKA_TOPIC=k6-events \
  KAFKA_ACKS=-1 KAFKA_IDEMPOTENT=true KAFKA_COMPRESSION=none KAFKA_LINGER_MS=0 BATCH_MAX_MESSAGES=500 BATCH_MAX_WAIT_MS=5 \
  BATCH_MAX_BYTES=900000 BATCH_MAX_IN_FLIGHT=1 KAFKA_KEY_MODE=event  taskset -c 0,6 node --env-file=.env dist/main.js

cd services/consumer-sink && env KAFKA_BROKERS=... CONSUMER_TOPIC=k6-events CONSUMER_GROUP_ID=k6-<testid>-<pid> METRICS_PORT=8001 \
  CONSUMER_FROM_BEGINNING=true CONSUMER_ASSIGNOR=roundRobin CONSUMER_PARTITIONS_CONCURRENTLY=1 CONSUMER_MAX_BATCH_SIZE=1000 SINK_MAX_ROWS=1000 PG_POOL_MAX=5 \
  SINK_DLQ_TOPIC=k6-dlq SINK_DLQ_ENABLED=<false|true>  taskset -c 1,7 node --env-file=.env dist/main.js
```

The retry settings (`SINK_MAX_RETRIES`, `SINK_RETRY_BASE_MS`, `SINK_RETRY_MAX_MS`) are left at the sink's defaults (3, 100 ms, 5000 ms).

CPU pinning: API 0,6 | consumer-sink 1,7 | brokers 2,8,3,9 | k6 4,10 | Prometheus, Grafana, node_exporter, exporters and Postgres 5,11. The container cpusets are changed with `docker update` and put back on exit.

## 2. Run configuration

The two variables are made into 8 cells, `<dlq>-<kind>`. The API and the load are identical in every cell.

| Variable | Levels | How |
|---|---|---|
| dlq | `off` | `SINK_DLQ_ENABLED=false`: any error of a chunk throws, the offset does not move |
| | `on` | `SINK_DLQ_ENABLED=true`: classify, bounded retries with backoff, publish to the DLQ, move on |
| poison kind | `invalid_json` | value is not JSON (`scripts/produce-poison.sh --kind invalid-json`) |
| | `bad_uuid` | `event_id` is not a UUID |
| | `oversized` | `type` has 200 characters (the API accepts 100) |
| | `constraint` | passes the parser, Postgres refuses it (a `\u0000` inside the jsonb payload, SQLSTATE 22P05) |

One run: fresh topics `k6-events` (6 partitions, RF 3, min ISR 2) and `k6-dlq` (1 partition), one consumer, k6 sends 2000 req/s for 60 s, one poison record is written into `k6-events` at t = 10 s with key `poison-observe-1` (partition 5), 30 s of observation after the load, audit. 8 cells x 3 repeats = 24 runs, about 60 min. Repeat 2 runs the cells in reverse order.

| Env knob | Default | Meaning |
|---|---|---|
| `RATE` | 2000 | requests per second |
| `DURATION_S` | 60 | seconds of load |
| `POISON_AT_S` | 10 | seconds after the load started when the poison is written |
| `OBSERVE_S` | 30 | seconds watched after the load |
| `CELLS` | all 8 | space separated list of cells |
| `REPEATS` | 3 | repeats per cell |
| `POISON_KEY` / `POISON_PARTITION` | `poison-observe-1` / 5 | key of the bad record and the partition it hashes to (checked against the DLQ header in the `on` cells) |
| `PARTITIONS` | 6 | partitions of `k6-events` |
| `RESULTS_NAME` | `k6-poison-message` | folder under `results/` |
| `DRY_RUN` | 0 | 1 = print the plan only |

What is recorded per run, in `summary.tsv` (shared `analyze.py`): `acked` (202 answers), `missing` (acked minus distinct rows in Postgres), `stuck_lag` (lag of the poison partition at the last sample), `other_lag_max` (largest lag of the other partitions at the last sample), `stall_s` (longest time after the poison during which the committed offset of the poison partition did not move while it had lag, from samples about 1 s apart), `dlq_records` (records in `k6-dlq`), `dlq_hdr` (`error_class/error_code/attempts` of the dead letter), `retries` (sum of `ep_consumer_retries_total`), `blocked_attempts` (failed chunk attempts with reason `poison_blocked`), `drained` (group lag was 0 within 5 s at the end), `status`. The lag samples of every run are in `captures/<testid>.lag.tsv` (epoch, partition, committed offset, end offset, lag).

## 3. Instrumentation needed

Added by K6 (no change to the services; the code already had everything):

- `ep_k6_phase{testid,cell}` gauge (ADDED BY K6): written by `run.sh` to `infra/compose/textfile/ep_k6.prom` for node_exporter's textfile collector, removed on exit. Same mechanism as `ep_k4_phase` and `ep_k5_phase`.

Used and already there:

- consumer-sink (job `eventpulse-consumer-sink`): `ep_consumer_lag`, `ep_consumer_last_committed_offset` (both with a `partition` label), `ep_consumer_messages_consumed_total`, `ep_consumer_rows_inserted_total`, `ep_consumer_dlq_total{error_class}`, `ep_consumer_retries_total{class}`, `ep_consumer_errors_total{class}`, `ep_consumer_chunk_failures_total{reason}`, `ep_consumer_batch_processing_seconds_bucket`, `ep_event_freshness_seconds_bucket`, `ep_pg_pool_waiting`, `process_cpu_user_seconds_total`, `process_cpu_system_seconds_total`, `nodejs_eventloop_lag_p99_seconds`.
- ingest-api (job `eventpulse-nodejs`): `ep_kafka_messages_acked_total`, `nodejs_eventloop_lag_p99_seconds`.
- k6: `k6_resp_202_total`, `k6_dropped_iterations_total`, `k6_http_req_failed_rate`, `k6_http_req_duration_seconds`.
- node_exporter (job `node`): `node_cpu_seconds_total`.

## 4. Grafana panels

`[w]` = `$__rate_interval`; job filters omitted.

| # | Panel | Query | What it shows |
|---|---|---|---|
| 1 | Lag per partition | `ep_consumer_lag` | which partition is blocked |
| 2 | Committed offset per partition | `ep_consumer_last_committed_offset` | where it stopped |
| 3 | Run phase | `max by (testid)(ep_k6_phase)` | when the poison went in |
| 4 | Events in vs rows written | `sum(rate(k6_resp_202_total[w]))`, `sum(rate(ep_consumer_rows_inserted_total[w]))` | the loss as a gap |
| 5 | Messages consumed per second | `sum(rate(ep_consumer_messages_consumed_total[w]))` | counter inflation by re-reads |
| 6 | Failed chunk attempts by reason | `sum by (reason)(rate(ep_consumer_chunk_failures_total[w]))` | the retry loop |
| 7 | Retries and errors by class | `sum by (class)(ep_consumer_retries_total)`, `sum by (class)(ep_consumer_errors_total)` | classification and bounded retries |
| 8 | Records published to the DLQ | `sum by (error_class)(ep_consumer_dlq_total)` | one per poison record |
| 9 | Chunk processing p99 | `histogram_quantile(0.99, sum by (le)(rate(ep_consumer_batch_processing_seconds_bucket[w])))` | cost of the retries with backoff |
| 10 | Freshness p99 | `histogram_quantile(0.99, sum by (le)(rate(ep_event_freshness_seconds_bucket[w])))` | age of rows when visible |
| 11 | Consumer process CPU | `rate(process_cpu_user_seconds_total[w])`, `rate(process_cpu_system_seconds_total[w])` | cost of the hot loop |
| 12 | Host CPU of pinned groups | `100 * (1 - avg(rate(node_cpu_seconds_total{mode="idle",cpu=~"$cpu_x"}[w])))` | busy share per group |
| 13 | Postgres connection wait | `ep_pg_pool_waiting` | the database side |
| 14 | k6 dropped iterations | `rate(k6_dropped_iterations_total[w])` | validity, must be 0 |
| 15 | k6 failed requests and p99 | `k6_http_req_failed_rate`, `k6_http_req_duration_seconds` | validity |
| 16 | Event loop lag p99 | `nodejs_eventloop_lag_p99_seconds` | validity of API and consumer |
| 17 | API acknowledged messages | `rate(ep_kafka_messages_acked_total[w])` | must match k6's 202 rate |

Verify in Grafana Explore (after one run): `count by (cell)(k6_resp_202_total)` and `ep_k6_phase`.

## 5. Run it

```bash
cd loadtests/k6/stage-2/K6
DRY_RUN=1 ./run.sh                                                                   # (a) the plan, runs nothing
CELLS="on-invalid_json" REPEATS=1 RESULTS_NAME=k6-trial ./run.sh                     # (b) one cell, one repeat (a trial, never quoted)
./run.sh                                                                             # (c) the full matrix, about 60 min
CELLS="off-constraint on-constraint" REPEATS=1 RESULTS_NAME=k6-one ./run.sh          # (d) two cells by hand
```

The k6 part of every run goes through the shared `loadtests/k6/runner.sh`. To look at a poisoned partition by hand while `run.sh` is paused, `scripts/lag.sh` and `scripts/dlq-peek.sh --topic k6-dlq` work on the same topics.

Watch in another terminal:

```bash
tail -f loadtests/k6/stage-2/K6/results/<name>/captures/*.sink.out         # the consumer's errors, retries and DLQ lines
tail -f loadtests/k6/stage-2/K6/results/<name>/captures/*.lag.tsv          # committed offset and lag per partition
```

Results: `loadtests/k6/stage-2/K6/results/<name>/` with `summary.tsv`, `runs.tsv` (epochs of the poison and of the measured window of every run), `env.txt`, `captures/` (API and consumer logs, lag samples, the poison write output, the dead letter as read from `k6-dlq`) and the k6 lap JSONs and manifests of the runner.

## 6. What this script changes on my machine

- `docker update --cpuset-cpus` on the brokers, Postgres and the monitoring containers; restored on exit (`KEEP_PINNING=1` leaves them pinned).
- Kafka: deletes and recreates topics `k6-events` and `k6-dlq` before every run; writes one bad record into `k6-events` per run (through `docker exec` into `ep-kafka1`); deletes the consumer groups `k6-*` after each run.
- Postgres: deletes the rows of each run (run id prefix `a6...`) from `events` after the audit.
- Starts and stops one ingest-api and one consumer-sink per run (only processes the script started).
- Writes `infra/compose/textfile/ep_k6.prom` (removed on exit).
- No `sudo`, no `sysctl`, no `tc`.

## 7. Analyze in Grafana

Import [K6 - Poison message.json](../../../../infra/observability/dashboards/stage-2/K6%20-%20Poison%20message.json) (Dashboards, New, Import, upload the file, pick the Prometheus datasource). Set the time range from the epochs in `runs.tsv` (the `poison` row and the `measured` window of the run). Read the rows in order: 1 (which partition stops, is anything lost), 2 (the retry loop, the retries and the dead letter), 3 (what the loop costs), 4 (is the run valid). The audit numbers are in `summary.tsv`, not in Grafana.
