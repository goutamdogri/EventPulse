# K5 Kill a consumer at every step

Why this experiment and what I expect: see [notebook/stage-2/experiments/K5/Explanation.md](../../../../notebook/stage-2/experiments/K5/Explanation.md)

## 1. Run the server

`run.sh` starts and stops the ingest-api and the consumer-sink itself (the consumer twice per run: the one that gets killed and its replacement), so there is nothing to start by hand. It needs the stack up and both services built, and the consumer-sink must be built after the K5 change (it adds `SINK_DEDUP`; `run.sh` checks for it).

```bash
docker compose -f infra/compose/docker-compose.yml up -d                        # Prometheus, Grafana, node_exporter, Postgres
docker compose -f infra/compose/docker-compose.kafka.yml up -d                  # 3 brokers + kafka-exporter
pnpm --filter @eventpulse/ingest-api build && pnpm --filter @eventpulse/consumer-sink build
```

Per run the script starts the API (same for every cell) and the consumer-sink like this:

```bash
cd services/ingest-api && env KAFKA_BROKERS=localhost:9092,localhost:9094,localhost:9096 KAFKA_TOPIC=k5-events \
  KAFKA_ACKS=-1 KAFKA_IDEMPOTENT=true KAFKA_COMPRESSION=none KAFKA_LINGER_MS=0 BATCH_MAX_MESSAGES=500 BATCH_MAX_WAIT_MS=5 \
  BATCH_MAX_BYTES=900000 BATCH_MAX_IN_FLIGHT=1 KAFKA_KEY_MODE=event  taskset -c 0,6 node --env-file=.env dist/main.js

cd services/consumer-sink && env KAFKA_BROKERS=... CONSUMER_TOPIC=k5-events CONSUMER_GROUP_ID=k5-<testid>-<pid> METRICS_PORT=8001 \
  CONSUMER_FROM_BEGINNING=true CONSUMER_PARTITIONS_CONCURRENTLY=1 CONSUMER_MAX_BATCH_SIZE=1000 SINK_MAX_ROWS=1000 PG_POOL_MAX=5 \
  CONSUMER_SESSION_TIMEOUT_MS=10000 CONSUMER_HEARTBEAT_INTERVAL_MS=1000 \
  DEBUG_FAULTS=true CRASH_AT=<point> CRASH_AFTER_BATCHES=5 [SINK_DEDUP=false SINK_NODEDUP_TABLE=events_nodedup] \
  taskset -c 1,7 node --env-file=.env dist/main.js
```

Consumer #2 (the replacement) is started the same way with `CRASH_AT=none` and the same group id.

CPU pinning: API 0,6 | consumer-sink 1,7 | brokers 2,8,3,9 | k6 4,10 | Prometheus, Grafana, node_exporter, exporters and Postgres 5,11. The container cpusets are changed with `docker update` and put back on exit.

## 2. Run configuration

The two variables are made into 8 cells. Both are consumer-sink settings; the API and the load are identical in every cell.

| Variable | Levels | How |
|---|---|---|
| dedup | `on` | normal sink: `INSERT ... ON CONFLICT (event_id) DO NOTHING` into `events` (primary key `event_id`) |
| | `off` | `SINK_DEDUP=false`: plain `INSERT` into `events_nodedup`, a copy of `events` with no primary key (debug only, needs `DEBUG_FAULTS=true`) |
| crash point | `none` (control) | nobody is killed |
| | `before_parse` | `SIGKILL` at the start of a chunk, before anything is parsed |
| | `after_parse` | `SIGKILL` after parsing, before the DB write |
| | `after_db_commit` | `SIGKILL` after the DB transaction committed, before the offset commit |

Cell names are `<dedup>-<crash>`, for example `off-after_db_commit`. One run: fresh topic `k5-events` (6 partitions, RF 3, min ISR 2), consumer #1 armed to kill itself on its 5th batch, k6 sends 2000 req/s for 30 s, consumer #2 started as soon as #1 is gone, wait for lag 0, audit in Postgres. 8 cells x 3 repeats = 24 runs, about 52 min. Repeat 2 runs the cells in reverse order.

| Env knob | Default | Meaning |
|---|---|---|
| `RATE` | 2000 | requests per second |
| `DURATION_S` | 30 | seconds of load |
| `CRASH_BATCH` | 5 | the sink kills itself while handling its Nth batch (`CRASH_AFTER_BATCHES` of the sink) |
| `CELLS` | all 8 | space separated list of cells |
| `REPEATS` | 3 | repeats per cell |
| `SESSION_TIMEOUT_MS` / `HEARTBEAT_MS` | 10000 / 1000 | consumer group timing (the sink default is 45000 / 3000) |
| `RESULTS_NAME` | `k5-kill-consumer` | folder under `results/` |
| `PARTITIONS` | 6 | partitions of `k5-events` |
| `DRY_RUN` | 0 | 1 = print the plan only |

What is recorded per run, in `summary.tsv` (shared `analyze.py`): `acked` (202 answers = events that must be stored), `topic` (records in the topic), `rows_total` (rows of the run in the table), the distinct count (audit column), `missing` (acked minus distinct), `dup_rows` (rows_total minus distinct, rows stored twice), `absorbed` (ON CONFLICT hits counted by consumer #2), `crash_chunk` (rows in the chunk in flight at the kill, from the sink's log line), `reread` (records handed to a handler more than once: consumed by #1, plus the chunk in flight, plus consumed by #2, minus records in the topic), `assign_s` and `resume_s` (seconds from the kill until consumer #2 was assigned a partition / processed its first chunk).

## 3. Instrumentation needed

Added by K5 (in the code, not metrics unless stated):

- `SINK_DEDUP` and `SINK_NODEDUP_TABLE` in `services/consumer-sink/src/config.ts` and `src/sink/batch-writer.ts` (ADDED BY K5). `SINK_DEDUP=false` needs `DEBUG_FAULTS=true`.
- The `[fault] ... rows=N` field in the crash log line, `src/consumer/fault-injector.ts` (ADDED BY K5): the size of the chunk in flight at the kill.
- `ep_k5_phase{testid,cell}` gauge (ADDED BY K5): written by `run.sh` to `infra/compose/textfile/ep_k5.prom` for node_exporter's textfile collector, removed on exit. Same mechanism as `ep_k4_phase`; the textfile file is already picked up by the existing node_exporter setup.

Used and already there:

- consumer-sink (job `eventpulse-consumer-sink`, own registry): `ep_consumer_messages_consumed_total`, `ep_consumer_rows_inserted_total`, `ep_consumer_duplicates_total`, `ep_consumer_lag`, `ep_consumer_last_committed_offset`, `ep_consumer_assigned_partitions`, `ep_consumer_rebalances_total`, `ep_consumer_batch_size_messages_{sum,count}`, `ep_consumer_db_write_seconds_bucket`, `ep_event_freshness_seconds_bucket`, `ep_pg_pool_waiting`, `process_cpu_user_seconds_total`, `process_cpu_system_seconds_total`, `nodejs_eventloop_lag_p99_seconds`.
- ingest-api (job `eventpulse-nodejs`): `ep_kafka_messages_acked_total`, `nodejs_eventloop_lag_p99_seconds`.
- k6: `k6_resp_202_total`, `k6_dropped_iterations_total`, `k6_http_req_failed_rate`, `k6_http_req_duration_seconds`.
- node_exporter (job `node`): `node_cpu_seconds_total`. Postgres exporter (job `postgres`): `pg_stat_database_tup_inserted`.

Note: a SIGKILL removes the dying process's counters, and consumer #2 starts its counters at 0 on the same port. `run.sh` therefore polls consumer #1's consumed counter every 0.1 s and keeps the last value.

## 4. Grafana panels

`[w]` = `$__rate_interval`; job filters omitted.

| # | Panel | Query | What it shows |
|---|---|---|---|
| 1 | Events in vs rows written | `sum(rate(k6_resp_202_total[w]))`, `rate(ep_consumer_rows_inserted_total[w])`, `rate(ep_consumer_messages_consumed_total[w])` | input rate, new rows, messages consumed |
| 2 | Consumer lag | `sum(ep_consumer_lag)` | the gap and the catch-up |
| 3 | Run phase | `max by (testid, cell) (ep_k5_phase)` | where the kill and resume are |
| 4 | Duplicates absorbed | `sum(ep_consumer_duplicates_total)` | ON CONFLICT hits of consumer #2 |
| 5 | Committed offset per partition | `ep_consumer_last_committed_offset` | offsets stop at the kill, continue after |
| 6 | Lag per partition | `ep_consumer_lag` | same gap split by partition |
| 7 | Partitions owned and group joins | `sum(ep_consumer_assigned_partitions)`, `sum(ep_consumer_rebalances_total)` | when consumer #2 got partitions |
| 8 | Messages per chunk | `rate(ep_consumer_batch_size_messages_sum{kind="chunk"}[w]) / rate(..._count[w])` | chunk size, scale of the reprocessed chunk |
| 9 | Freshness p99 | `histogram_quantile(0.99, sum by (le)(rate(ep_event_freshness_seconds_bucket[w])))` | age of rows when they become visible |
| 10 | Rows inserted in Postgres | `sum by (datname)(rate(pg_stat_database_tup_inserted[w]))` | the database's own view |
| 11 | Consumer process CPU | `rate(process_cpu_user_seconds_total[w])`, `..._system_...` | cost of the catch-up |
| 12 | Host CPU of pinned groups | `100 * (1 - avg(rate(node_cpu_seconds_total{mode="idle",cpu=~"$cpu_x"}[w])))` | busy share per group |
| 13 | DB write p99 and pool wait | `histogram_quantile(0.99, ... ep_consumer_db_write_seconds_bucket ...)`, `ep_pg_pool_waiting` | database side of the consumer |
| 14 | k6 dropped iterations | `rate(k6_dropped_iterations_total[w])` | validity, must be 0 |
| 15 | k6 failed requests and p99 | `k6_http_req_failed_rate`, `k6_http_req_duration_seconds` | validity |
| 16 | k6 CPUs busy | same as 12 with `$cpu_k6` | generator headroom |
| 17 | Event loop lag p99 | `nodejs_eventloop_lag_p99_seconds` | validity of API and consumer |
| 18 | API acknowledged messages | `rate(ep_kafka_messages_acked_total[w])` | must match k6's 202 rate |

Verify in Grafana Explore (after one run): `count by (cell)(k6_resp_202_total)` and `ep_k5_phase`.

## 5. Run it

```bash
cd loadtests/k6/stage-2/K5
DRY_RUN=1 ./run.sh                                                              # (a) the plan, runs nothing
CELLS="on-after_db_commit" REPEATS=1 RESULTS_NAME=k5-trial ./run.sh             # (b) one cell, one repeat (a trial, never quoted)
./run.sh                                                                        # (c) the full matrix, about 52 min
CELLS="off-after_db_commit" REPEATS=1 RESULTS_NAME=k5-one ./run.sh              # (d) one cell by hand
```

The k6 part of every run goes through the shared `loadtests/k6/runner.sh` (it needs the API running, so a manual k6 run is only useful while `run.sh` is paused; use `CELLS=...` instead).

Watch in another terminal:

```bash
tail -f loadtests/k6/stage-2/K5/results/<name>/captures/*.sink1.out       # the [fault] line when it is killed
tail -f loadtests/k6/stage-2/K5/results/<name>/captures/*.sink2.out       # the replacement joining the group
```

Results: `loadtests/k6/stage-2/K5/results/<name>/` with `summary.tsv`, `runs.tsv` (epochs of the kill, the resume and the measured window of every run), `env.txt`, `captures/` (API and consumer logs) and the k6 lap JSONs and manifests of the runner.

## 6. What this script changes on my machine

- `SIGKILL` of the consumer-sink process the script itself started (that is the experiment). Nothing else is killed.
- `docker update --cpuset-cpus` on the brokers, Postgres and the monitoring containers; restored on exit (`KEEP_PINNING=1` leaves them pinned).
- Postgres: creates `events_nodedup` (+ an index on `event_id`) if it is missing and drops it on exit; deletes the rows of each run (by run id prefix `a5...`) from `events` and `events_nodedup` after the audit.
- Kafka: deletes and recreates topic `k5-events` before every run; deletes the consumer groups `k5-*` after each run.
- Writes `infra/compose/textfile/ep_k5.prom` (removed on exit).
- No `sudo`, no `sysctl`, no `tc`.

## 7. Analyze in Grafana

Import [K5 - Kill a consumer at every step.json](../../../../infra/observability/dashboards/stage-2/K5%20-%20Kill%20a%20consumer%20at%20every%20step.json) (Dashboards, New, Import, upload the file, pick the Prometheus datasource). Set the time range from the epochs in `runs.tsv` (measured start and end of the run). Read the rows in order: 1 (did events survive, how long was the gap), 2 (what was re-delivered), 3 (cost of the catch-up), 4 (is the run valid). The numbers of the audit are in `summary.tsv`, not in Grafana.
