# K9 Hot key and skew

Why this experiment and what I expect: see notebook/stage-2/experiments/K9/Explanation.md

## 1. Run the server

`run.sh` starts everything it needs itself, so there is nothing to start by hand except the stack:

- Docker stack up: 3 Kafka brokers (`ep-kafka1..3`), Postgres (`ep-postgres`), Prometheus, Grafana, node_exporter, kafka-exporter.
- Nothing listening on :8000 (ingest-api) or :8001 to :8003 (consumer-sinks). The preflight stops with a message if something is.
- Both services built: `pnpm --filter @eventpulse/consumer-sink build` and `pnpm --filter @eventpulse/ingest-api build`. The preflight checks `dist/` for `SINK_PROCESSING_DELAY_MS` and `KAFKA_KEY_MODE`.
- The ingest-api is started by `run.sh` with `node --env-file=.env dist/main.js` under `taskset`. The settings that matter are written in `run.sh` (acks=all, idempotent, no compression, `KAFKA_KEY_MODE`, `API_KEYS`), so a run does not depend on my shell or on `services/ingest-api/.env`. Variables already set win over `--env-file`, which is why this works.
- Pinning (CPU ids): ingest-api 0,6 | consumer-sinks 1, 7 and 1 (the 3rd shares cpu 1 with the 1st) | Kafka brokers 2,8,3,9 | k6 4,10 | Prometheus, Grafana, exporters and Postgres 5,11. The container cpusets are moved with `docker update` and put back when the script ends.

## 2. Run configuration

The variable is the Kafka key: `tenant` (key = `tenant_id`) or `event` (key = `event_id`), set with `KAFKA_KEY_MODE` when the API is started.

- Traffic: `TENANTS`=8 API keys. Key 0 is tenant `k9-hot` and gets `HOT_SHARE`=80 % of the requests; the other 7 (`k9-t1` to `k9-t7`) share the rest evenly. The ingest-api takes the tenant from the API key, so this is done in k6 by choosing the key.
- Topic `k9-events`: 6 partitions, RF 3, min ISR 2, deleted and recreated before every run.
- Consumers: 3 consumer-sinks in one new group per run (roundRobin, so 2 partitions each). `CONSUMER_PARTITIONS_CONCURRENTLY=2` makes a consumer work on its 2 partitions at the same time. Chunks of up to `BATCH`=100 rows, and every chunk gets `DELAY_MS`=60 ms of artificial work (`SINK_PROCESSING_DELAY_MS`). That is the speed limit of one partition. Plain INSERT into `events_nodedup` (no primary key).
- Matrix: 4 rates x 2 key modes x 3 repeats, 60 s each. Repeat 2 runs the cells in reverse order. After each load the script waits for lag 0 and then audits Postgres. 24 runs, about 65 minutes in total.

| Variable | Default | Meaning |
|---|---|---|
| `RATES` | `1000 1500 2500 4000` | req/s of each cell |
| `MODES` | `tenant event` | key modes compared |
| `DURATION_S` | `60` | length of the load |
| `HOT_SHARE` | `80` | % of requests of the hot tenant |
| `TENANTS` | `8` | tenants (1 hot + the rest) |
| `DELAY_MS` | `60` | artificial work per chunk in the sink |
| `BATCH` | `100` | rows per chunk |
| `REPEATS` | `3` | repeats per cell |
| `CELLS` | all | e.g. `"tenant-r1500 event-r1500"` |
| `RESULTS_NAME` | `k9-hot-key` | results folder |
| `PARTITIONS`, `TOPIC` | `6`, `k9-events` | topic |
| `DRAIN_TIMEOUT_S` | `420` | longest wait for lag 0 after the load |
| `DRY_RUN` | `0` | `1` prints the plan and runs nothing |

Per-run files in `captures/`: `<testid>.partition-bytes.txt` (bytes of each partition, from `kafka-log-dirs`, taken right after the load), `<testid>.lag-at-load-end.txt` (lag of each partition right after the load), `<testid>.rows.txt` (rows of the run in Postgres sampled every 0.3 s), the stdout of the API and of each sink.

`summary.tsv` columns added by K9 (besides the shared ones): `key_mode`, `acked`, `missing` (acked minus distinct rows), `hot_sent_pct` (measured share of the hot tenant), `hot_rows`, `hot_part` and `hot_bytes_pct` (the partition with most bytes and its share), `parts_used` (partitions with over 5 % of the bytes), `consumed_rps` (rows stored during the load divided by its length), `lag_max`, `lag_part`, `lag_rest` (lag of the most behind partition, which one, and the others added up, at load end), `drain_s` (seconds after the load until lag 0), `overtaken` and `pairs` (events stored before an older event of the same tenant, and the number of consecutive pairs checked), `hot_overtaken`, `hot_pairs` (same for the hot tenant only).

How `overtaken` is counted: for each tenant the events are sorted by `received_at` (the API's clock when it got the request). `stored_at` is when the sink's INSERT started. An event whose `stored_at` is earlier than that of the event just before it in `received_at` order was stored first although it is younger. Events inserted by the same INSERT share one `stored_at`, so they never count.

## 3. Instrumentation needed

No code was changed for K9. Everything below exists already.

- prom-client defaults used: `process_cpu_seconds_total`, `nodejs_eventloop_lag_seconds`.
- Custom server metrics used: `ep_consumer_lag{partition}`, `ep_consumer_messages_consumed_total`, `ep_consumer_rebalances_total`, `ep_consumer_assigned_partitions`, `ep_consumer_db_write_seconds_bucket`, `ep_event_freshness_seconds_bucket`, `ep_kafka_messages_acked_total`.
- Kafka exporter (job `kafka`): `kafka_topic_partition_current_offset`, `kafka_consumergroup_current_offset`, `kafka_consumergroup_lag`, all with `topic` and `partition` labels.
- Postgres exporter (job `postgres`): `pg_stat_database_xact_commit`.
- k6 metrics used: `k6_resp_202_total`, `k6_resp_503_total`, `k6_dropped_iterations_total`, and two counters of `script.js`: `k6_k9_hot_sent_total` and `k6_k9_other_sent_total` (these exist in Prometheus only after a k6 run).
- node_exporter metrics used: `node_cpu_seconds_total`.
- Bytes per partition: there is no metric for it. It is read from `kafka-log-dirs.sh` in `run.sh` after each load and written to `captures/`. In Grafana the events per partition panel stands in for it (the payload has a fixed size).

## 4. Grafana panels

`[w]` = `$__rate_interval`. Job filters are left out except where they decide the metric.

| # | Panel | Query | What it shows |
|---|---|---|---|
| 1 | Accepted by the API per cell | `sum by (cell)(rate(k6_resp_202_total[w]))` | what the API accepted |
| 2 | Consumed by the whole group | `sum(rate(ep_consumer_messages_consumed_total[w]))` and `sum(rate(ep_kafka_messages_acked_total[w]))` | the throughput that counts, next to what the API put into Kafka |
| 3 | Lag per partition | `max by (partition)(ep_consumer_lag)` | which partition falls behind |
| 4 | Event freshness p99 | `histogram_quantile(0.99, sum by (le)(rate(ep_event_freshness_seconds_bucket[w])))` | API receive to stored |
| 5 | Share on the busiest partition | `100 * max(rate(kafka_topic_partition_current_offset{topic="k9-events"}[w])) / sum(...)` | how skewed the written load is |
| 6 | Events written per partition | `sum by (partition)(rate(kafka_topic_partition_current_offset{topic="k9-events"}[w]))` | the skew itself |
| 7 | Events consumed per partition | `sum by (partition)(rate(kafka_consumergroup_current_offset{topic="k9-events"}[w]))` | the per-partition ceiling |
| 8 | Lag per partition (kafka exporter) | `sum by (partition)(kafka_consumergroup_lag{topic="k9-events"})` | a second measurement of the lag |
| 9 | Consumed per consumer | `sum by (instance)(rate(ep_consumer_messages_consumed_total[w]))` | busy and idle consumers |
| 10 | Hot tenant share | `100 * rate(k6_k9_hot_sent_total) / (hot + other)` | the generator sends what was planned |
| 11 | Sink process CPU | `sum by (instance)(rate(process_cpu_seconds_total{job="eventpulse-consumer-sink"}[w]))` | CPU per consumer |
| 12 | Sink CPUs busy | `100 * (1 - avg(rate(node_cpu_seconds_total{mode="idle",cpu=~"$cpu_sinks"}[w])))` | headroom of the sink CPUs (keep under 70 %) |
| 13 | Postgres commits | `sum(rate(pg_stat_database_xact_commit{datname="eventpulse"}[w]))` | INSERT chunks per second |
| 14 | Sink DB write time | `histogram_quantile(0.99, ... ep_consumer_db_write_seconds_bucket ...)` | part of the per-partition ceiling |
| 15 | Broker and API CPUs busy | `node_cpu_seconds_total` idle on `$cpu_brokers` and `$cpu_server` | not the limit |
| 16 | k6 dropped iterations | `sum by (cell)(increase(k6_dropped_iterations_total[w]))` | must be 0 |
| 17 | k6 503s | `sum by (cell)(rate(k6_resp_503_total[w]))` | must be 0 |
| 18 | k6 CPUs busy | idle on `$cpu_k6` | generator headroom (under 70 %) |
| 19 | API event loop lag | `max(nodejs_eventloop_lag_seconds{job="eventpulse-nodejs"})` | the API is not the problem |
| 20 | Partitions owned | `sum(ep_consumer_assigned_partitions)` | must stay 6 |
| 21 | Rebalances | `sum by (instance)(increase(ep_consumer_rebalances_total[w]))` | must not move during the load |

To verify the metrics exist, paste in Grafana Explore:

```
count by (partition)(kafka_topic_partition_current_offset{topic="k9-events"})
count by (cell)(k6_k9_hot_sent_total)
```

## 5. Run it

(a) Dry run (prints the plan, runs nothing):

```bash
cd loadtests/k6/stage-2/K9 && DRY_RUN=1 ./run.sh
```

(b) Small trial, one cell pair, one repeat (never quote its numbers):

```bash
cd loadtests/k6/stage-2/K9 && CELLS="tenant-r2500 event-r2500" REPEATS=1 RESULTS_NAME=k9-trial ./run.sh
```

(c) The full matrix, one command:

```bash
cd loadtests/k6/stage-2/K9 && ./run.sh
```

(d) One case by hand: not supported on purpose. A run needs the topic, the API with its keys, the group and the audit, which `run.sh` does together. Use `CELLS=` with `REPEATS=1`.

While it runs, in another terminal:

```bash
tail -f loadtests/k6/stage-2/K9/results/k9-hot-key/captures/*.server.out
```

```bash
docker exec ep-kafka1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka1:19092 --list | grep k9-
```

Results go to `loadtests/k6/stage-2/K9/results/<RESULTS_NAME>/`: `summary.tsv`, `runs.tsv` (epoch seconds of each run, to zoom Grafana), `env.txt`, `captures/`, and the k6 lap JSONs under a timestamped folder.

## 6. What this script changes on my machine

- No `sudo`, no `sysctl`, no `tc`.
- `docker update --cpuset-cpus` on the Kafka containers, Postgres and the monitoring containers (restored when the script ends, also on Ctrl+C; `KEEP_PINNING=1` leaves it).
- Starts the ingest-api and 3 consumer-sinks itself and stops them with SIGTERM (SIGKILL after 10 s). It only touches processes it started.
- Deletes and recreates the Kafka topic `k9-events` before every run, and deletes the consumer groups `k9-*` it created.
- In Postgres: creates `events_nodedup` (same columns as `events`, no primary key) and an index on `event_id` if the table is missing, and drops them on exit only if it created them. The rows of each run (by run id prefix) are deleted after its audit. The table `events` is not touched.
- Writes only inside the experiment folder.

## 7. Analyze in Grafana

Dashboard: [K9 - Hot key and skew.json](../../../../infra/observability/dashboards/stage-2/K9%20-%20Hot%20key%20and%20skew.json). Import it in Grafana (Dashboards, New, Import, upload the file) and pick the Prometheus datasource. The four cpu variables hold the pinned CPU ids.

Read the rows in order: 1 the verdict (accepted vs consumed, lag per partition), 2 the mechanism (written vs consumed per partition, per consumer), 3 resources (is the ceiling the artificial delay or a real CPU / Postgres limit), 4 validity (dropped iterations 0, no 503, owned partitions 6, no rebalance). Zoom to each run with the epochs of `runs.tsv`. The table of all runs: `python3 loadtests/k6/analyze.py table results/<name>/summary.tsv`.
