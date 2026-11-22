# K7 Slow Postgres

Why this experiment and what I expect: see [notebook/stage-2/experiments/K7/Explanation.md](../../../../notebook/stage-2/experiments/K7/Explanation.md)

## 1. Run the server

`run.sh` starts and stops both services itself, once per run, so every run starts with an empty topic, a new consumer group and new connections. The commands it uses:

```bash
# ingest-api (same settings as K5, written out in run.sh as API_ENV)
cd services/ingest-api && env KAFKA_BROKERS=localhost:9092,localhost:9094,localhost:9096 KAFKA_TOPIC=k7-events \
  KAFKA_ACKS=-1 KAFKA_IDEMPOTENT=true KAFKA_COMPRESSION=none KAFKA_LINGER_MS=0 BATCH_MAX_MESSAGES=500 BATCH_MAX_WAIT_MS=5 \
  BATCH_MAX_BYTES=900000 BATCH_MAX_IN_FLIGHT=1 KAFKA_KEY_MODE=event \
  taskset -c 0,6 node --env-file=.env dist/main.js

# consumer-sink: its DATABASE_URL is the one from its .env with host:port replaced by localhost:15434 (Toxiproxy)
cd services/consumer-sink && env KAFKA_BROKERS=... CONSUMER_TOPIC=k7-events CONSUMER_GROUP_ID=k7-<testid>-<pid> METRICS_PORT=8001 \
  DATABASE_URL=postgres://...@localhost:15434/eventpulse CONSUMER_FROM_BEGINNING=true CONSUMER_ASSIGNOR=roundRobin \
  CONSUMER_PARTITIONS_CONCURRENTLY=1 CONSUMER_MAX_BATCH_SIZE=1000 SINK_MAX_ROWS=1000 PG_POOL_MAX=5 LAG_POLL_MS=1000 \
  taskset -c 1,7 node --env-file=.env dist/main.js
```

- Before the first run, build both services (`pnpm build` in `services/ingest-api` and in `services/consumer-sink`) and start the stack:

```bash
docker compose -f infra/compose/docker-compose.yml up -d                        # Prometheus, Grafana, node_exporter, Postgres, Toxiproxy
docker compose -f infra/compose/docker-compose.kafka.yml up -d                  # 3 brokers + kafka-exporter
```

- Nothing may listen on :8000 or :8001. `run.sh` checks this and stops with a message.
- Toxiproxy (container `ep-toxiproxy`, control API :8474, proxied port :15434) is part of the compose file. `run.sh` creates the proxy `pg` (`:15434` to `postgres:5432`) if it is missing.
- CPU pinning: server `0,6`, consumer-sink `1,7`, brokers `2,8,3,9`, k6 `4,10`, monitoring + Postgres + Toxiproxy `5,11`. The container cpusets are set with `docker update` and restored at the end.

## 2. Run configuration

- Variable: the latency the Toxiproxy toxic adds to every answer Postgres sends to the sink: `s0` (no toxic, control), `s50`, `s200`, `s500` (ms).
- One run = one steady load of 2,000 req/s for 300 s through three windows: healthy 60 s, slow 120 s, after 120 s (toxic removed, load still on). Then the script waits until the group's lag is 0 and audits the rows.
- 4 cells x 3 repeats = 12 measured runs. Repeat 2 runs the cells in reverse order. About 9 min per run, about 1 h 50 min in total.
- Separate probe (not a measured run): `CALIBRATE=1` fills the topic with 60 s of load while no sink runs, then times one drain with healthy Postgres.

| Env knob | Default | Meaning |
|---|---|---|
| `RATE` | 2000 | req/s offered the whole run |
| `PRE_S`, `SLOW_S`, `POST_S` | 60, 120, 120 | length of the healthy, slow and after windows (load duration is their sum) |
| `CELLS` | `s0 s50 s200 s500` | cells to run |
| `REPEATS` | 3 | repeats of the whole matrix |
| `RESULTS_NAME` | `k7-slow-postgres` | folder under `results/` |
| `PARTITIONS` | 6 | partitions of `k7-events` |
| `DRAIN_LAG` | 2000 | lag (records) at or below which the sink counts as caught up |
| `DRAIN_TIMEOUT_S` | 900 | longest wait for lag 0 after the load |
| `CALIBRATE` | 0 | 1 = run the calibration probe instead of the matrix |
| `DRY_RUN` | 0 | 1 = print the plan only |
| `SERVER_CPUS`, `CONSUMER_CPUS`, `KAFKA_CPUS`, `K6_CPUS`, `MON_CPUS` | see above | pinning |
| `RETENTION_MS` | 86400000 | retention of `k7-events` (1 day: nothing is deleted during a run) |
| `API_KEY` | first key of `services/ingest-api/.env` | key k6 uses |

## 3. Instrumentation needed

Already in the code and used by the panels:

- Server (prom-client, job `eventpulse-nodejs`): `ep_kafka_messages_acked_total`, `ep_http_503_total`, `ep_kafka_send_errors_total`, `nodejs_eventloop_lag_p99_seconds`.
- Consumer-sink (job `eventpulse-consumer-sink`): `ep_consumer_lag`, `ep_consumer_last_committed_offset`, `ep_consumer_rows_inserted_total`, `ep_consumer_db_write_seconds_*`, `ep_consumer_batch_size_messages_*`, `ep_consumer_batch_processing_seconds_bucket`, `ep_event_freshness_seconds_bucket`, `ep_consumer_chunk_failures_total`, `ep_consumer_retries_total`, `ep_consumer_dlq_total`, `ep_consumer_rebalances_total`, `ep_pg_pool_waiting`, `process_cpu_*_seconds_total`, `nodejs_eventloop_lag_p99_seconds`.
- kafka_exporter (job `kafka`): `kafka_topic_partition_current_offset`.
- k6: `k6_http_req_duration_seconds`, `k6_resp_202_total`, `k6_iterations_total`, `k6_dropped_iterations_total`, `k6_http_req_failed_rate`.
- node_exporter (job `node`): `node_cpu_seconds_total`.

Added by K7:

- `ep_kafka_topic_disk_bytes{broker,topic}` and `ep_kafka_logdir_disk_bytes{broker}`, **ADDED BY K7** in `infra/scripts/kafka-disk-exporter.sh`. It runs `du -sb` inside each broker container every 2 s and writes `infra/compose/textfile/ep_kafka_disk.prom` for node_exporter's textfile collector. `run.sh` starts and stops it.
- `ep_k7_phase{testid,cell}` and `ep_k7_slow_ms{testid,cell}`, **ADDED BY K7** in `run.sh` (textfile `ep_k7.prom`, removed on exit).
- k6 trends `lat_healthy`, `lat_slow`, `lat_after`, **ADDED BY K7** in `script.js`: request time of the requests sent in each window. They go into `summary.tsv` as `p99_healthy_ms`, `p99_slow_ms`, `p99_after_ms` through `hook.py`. The window is decided by the time since the scenario started, so it can differ by about a second from the moment `run.sh` flips the toxic.

## 4. Grafana panels

Row order in `infra/observability/dashboards/stage-2/K7 - Slow Postgres.json`. `[w]` = `$__rate_interval`. Job filters are left out.

| # | Panel | Query | What it shows |
|---|---|---|---|
| 1 | Ingest request time p50 and p99 | `histogram_quantile(0.99, sum(rate(k6_http_req_duration_seconds[w])))` | what the client sees |
| 2 | Offered, accepted and stored per second | `sum(rate(k6_resp_202_total[w]))`, `sum(rate(ep_consumer_rows_inserted_total[w]))`, `sum(rate(k6_iterations_total[w]))` | arrivals against what reaches Postgres |
| 3 | Consumer lag | `sum(ep_consumer_lag)` | records not yet in Postgres |
| 4 | Run phase and injected latency | `max by (testid)(ep_k7_phase)`, `ep_k7_slow_ms` | which window the run is in |
| 5 | Lag per partition | `ep_consumer_lag` | whether partitions behave alike |
| 6 | Postgres write time | `rate(ep_consumer_db_write_seconds_sum[w]) / rate(..._count[w])`, p99 from `_bucket` | time of one BEGIN..COMMIT |
| 7 | Rows per chunk | `rate(ep_consumer_batch_size_messages_sum{kind="chunk"}[w]) / rate(..._count[w])` | chunk size the sink reaches |
| 8 | Chunk processing time p99 | `histogram_quantile(0.99, ... ep_consumer_batch_processing_seconds_bucket)` | whole chunk |
| 9 | Freshness p99 | `histogram_quantile(0.99, ... ep_event_freshness_seconds_bucket)` | age of events when they become visible |
| 10 | Kafka end offset and committed offset | `sum(kafka_topic_partition_current_offset{topic="k7-events"})`, `sum(ep_consumer_last_committed_offset)` | lag from Kafka's side |
| 11 | Topic size on the brokers' disks | `ep_kafka_topic_disk_bytes{topic="k7-events"}` | bytes per broker |
| 12 | Log growth and unread backlog | `sum(deriv(ep_kafka_topic_disk_bytes[30s]))`, lag x bytes per record / 3 | disk growth rate, size of the waiting backlog |
| 13 | Consumer process CPU | `rate(process_cpu_user_seconds_total[w])`, `..._system_...` | CPU of the sink |
| 14 | Host CPU busy of the pinned groups | `100 * (1 - avg(rate(node_cpu_seconds_total{mode="idle",cpu=~"$cpu_<group>"}[w])))` | generator and service headroom |
| 15 | Postgres connections waiting | `ep_pg_pool_waiting` | pool contention in the sink |
| 16 | k6 dropped iterations | `sum(rate(k6_dropped_iterations_total[w]))` | must be 0 |
| 17 | k6 failed requests | `max(k6_http_req_failed_rate)` | must be 0 |
| 18 | API acknowledged messages | `sum(rate(ep_kafka_messages_acked_total[w]))` | what Kafka acknowledged |
| 19 | API 503s and Kafka send errors | `ep_http_503_total`, `ep_kafka_send_errors_total` | must be 0 |
| 20 | Chunk failures, retries, DLQ | `increase(ep_consumer_chunk_failures_total[w])` and others | timeouts or bad records |
| 21 | Rebalances | `sum(ep_consumer_rebalances_total)` | consumer kicked out of the group |
| 22 | Event loop lag p99 | `nodejs_eventloop_lag_p99_seconds` | API and sink |

Verify queries to paste into Grafana Explore after a run:

```promql
count by (testid)(k6_http_reqs_total)
ep_kafka_topic_disk_bytes{topic="k7-events"}
```

## 5. Run it

Run from `loadtests/k6/stage-2/K7`.

(a) Dry run, prints the plan and runs nothing:

```bash
DRY_RUN=1 ./run.sh
```

(b) Calibration probe (separate from the measured runs, about 3 min):

```bash
RESULTS_NAME=k7-calibration CALIBRATE=1 ./run.sh
```

(c) One-cell trial (plumbing only, never quote its numbers):

```bash
CELLS="s200" REPEATS=1 RESULTS_NAME=k7-trial ./run.sh
```

(d) Full matrix:

```bash
./run.sh
```

(e) Watch from another terminal:

```bash
watch -n 2 'curl -s localhost:8001/metrics | grep -E "^ep_consumer_lag" | head; cat infra/compose/textfile/ep_k7.prom | grep -v "^#"'
curl -s localhost:8474/proxies/pg/toxics
```

Results go to `results/<RESULTS_NAME>/`: `summary.tsv` (one row per run), `runs.tsv` (epoch seconds of each window: `slow_on`, `slow_off`, `measured`, `drained`), `env.txt`, `captures/<testid>.lag.tsv` (every second: epoch, total lag, rows inserted, topic bytes on all brokers), the server and sink logs, and the k6 laps. Print the table again with:

```bash
python3 loadtests/k6/analyze.py table loadtests/k6/stage-2/K7/results/k7-slow-postgres/summary.tsv
```

Summary columns added by this experiment: `dbw_healthy_ms` and `dbw_slow_ms` (average write time in the healthy window and in the slow window), `lag_healthy_max`, `lag_end_slow` (lag when the toxic is removed), `lag_peak`, `growth_rps` (least-squares slope of the lag in the slow window, first 10 s skipped), `drain_s` (seconds from removing the toxic until the lag is at or below `DRAIN_LAG`), `drain_in_load` (whether that happened while the load was still on), `disk_mb_at_on` and `disk_mb_peak` (topic bytes on all brokers).

A run is marked `TOXIC_NOT_EFFECTIVE` if the average write time in the slow window is below 90 % of 3 x the added latency, and `DRAIN_TIMEOUT` if the lag did not reach 0 within `DRAIN_TIMEOUT_S`.

## 6. What this script changes on my machine

No `sudo` and no `sysctl`.

- `docker update --cpuset-cpus` on the broker, Postgres and monitoring containers (restored on exit unless `KEEP_PINNING=1`).
- Toxiproxy: creates the proxy `pg` if it does not exist, adds the toxic `lat` and deletes it again; the toxic is also deleted on exit and at the start of every run. The proxy itself is left in place.
- Topic `k7-events` is deleted and recreated before every run. Consumer groups `k7-*` are deleted after every run.
- Rows with the run's event-id prefix (`a7....`) are deleted from `events` before and after each run.
- `SIGTERM` to the ingest-api and consumer-sink that the script started.
- Background process `infra/scripts/kafka-disk-exporter.sh` (`docker exec ... du`, read-only) and the textfiles `ep_k7.prom` and `ep_kafka_disk.prom` in `infra/compose/textfile/` (removed on exit).

## 7. Analyze in Grafana

Import [K7 - Slow Postgres.json](../../../../infra/observability/dashboards/stage-2/K7%20-%20Slow%20Postgres.json) through Dashboards, New, Import and pick the Prometheus datasource. Set the time range to a run using the epoch seconds in `runs.tsv` (`slow_on`, `slow_off`, `drained`). Read the rows in order: 1 (does ingestion notice, how big is the lag), 2 (why: write time, chunk size, disk), 3 (resources), 4 (is the run valid: dropped iterations, failures, rebalances).
