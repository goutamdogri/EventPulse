# K4 Kill a broker under load

Why this experiment and what I expect: see [notebook/stage-2/experiments/K4/Explanation.md](../../../../notebook/stage-2/experiments/K4/Explanation.md)

## 1. Run the server

`run.sh` starts and stops the ingest-api itself (and the consumer-sink for the audit), so there is nothing to start by hand. It needs the stack up and both services built.

```bash
docker compose -f infra/compose/docker-compose.yml up -d                        # Prometheus, Grafana, node_exporter, Postgres
docker compose -f infra/compose/docker-compose.kafka.yml up -d                  # 3 brokers + kafka-exporter
pnpm --filter @eventpulse/ingest-api build && pnpm --filter @eventpulse/consumer-sink build
```

Per run the script starts the API like this (settings that are not the variable are written out in `run.sh`, so my shell and `.env` do not matter):

```bash
cd services/ingest-api && env KAFKA_BROKERS=localhost:9092,localhost:9094,localhost:9096 KAFKA_TOPIC=k4-events \
  KAFKA_COMPRESSION=none KAFKA_LINGER_MS=0 BATCH_MAX_MESSAGES=500 BATCH_MAX_WAIT_MS=5 BATCH_MAX_BYTES=900000 \
  BATCH_MAX_IN_FLIGHT=1 KAFKA_KEY_MODE=event  <variant settings>  taskset -c 0,6 node --env-file=.env dist/main.js
```

CPU pinning (the same plan as K2): API 0,6 | brokers 1,7,2,8 | k6 3,9,4,10 | Prometheus, Grafana, node_exporter, kafka-exporter and Postgres 5,11. The container cpusets are changed with `docker update` and put back on exit. The audit consumers run on 0,6 after the API is stopped.

## 2. Run configuration

One steady rate; the cluster is damaged on a timetable. The variable is the producer's durability setting.

| Variant | API settings | Health check min ISR |
|---|---|---|
| `all` | `KAFKA_ACKS=-1 KAFKA_IDEMPOTENT=true` | 2 (default for acks=all) |
| `acks1` | `KAFKA_ACKS=1 KAFKA_IDEMPOTENT=false` | 1 (default for acks 0/1) |

Timetable of one run (seconds after the load starts):

| t | What happens |
|---|---|
| 0 | steady load, `RATE` req/s |
| 30 | `docker kill` broker A (a broker that is not the active KRaft controller) |
| 90 | `docker kill` broker B (the other non-controller broker, so 2 of 3 are dead) |
| 135 | `docker start` A and B, load continues |
| 210 | load ends; wait until all brokers are healthy and every partition has a full ISR; audit |

Topic `k4-events`: 6 partitions, RF 3, `min.insync.replicas=2`, deleted and created again before every run. Every event id starts with the run id of the run (`a4<variant no><repeat>00`), so the audit finds exactly the rows of that run.

| Env knob | Default | Meaning |
|---|---|---|
| `RATE` | 2000 | requests per second |
| `STEADY_S` / `HOLD1_S` / `HOLD2_S` / `RECOVER_S` | 30 / 60 / 45 / 75 | lengths of the four segments |
| `VARIANTS` | `all acks1` | variants, alternated (repeat 2 runs them in reverse) |
| `REPEATS` | 3 | repeats of every variant |
| `KILL1`, `KILL2` | auto | container names; auto = the two brokers that are not the active controller |
| `PARTITIONS` | 6 | partitions of `k4-events` |
| `SINK_N` | 3 | consumer-sink processes of the audit drain |
| `DRAIN_TIMEOUT_S`, `ISR_TIMEOUT_S` | 400, 180 | waits |
| `RESULTS_NAME` | `k4-kill` | results folder |

Total: 2 variants x 3 repeats x about 6.5 min = about 39 min (`DRY_RUN=1` prints the number).

## 3. Instrumentation needed

Prom-client defaults used: `process_cpu_seconds_total`, `nodejs_eventloop_lag_seconds`.

Custom server metrics used (already in `services/ingest-api`): `ep_http_503_total{reason}`, `ep_kafka_send_errors_total{type}`, `ep_kafka_messages_acked_total`, `ep_kafka_messages_failed_total`, `ep_kafka_health_ok`, `ep_kafka_breaker_state`, `ep_kafka_producer_connected`, `ep_kafka_broker_rtt_seconds`, `ep_kafka_client_queue_messages`, `ep_batcher_queue_depth`; from consumer-sink: `ep_consumer_rows_inserted_total`.

k6 metrics used: `k6_http_reqs_total` (labels `status`, `seg`, `acks`), `k6_http_req_duration_seconds` (native histogram), `k6_dropped_iterations_total`, `k6_vus`, `k6_vus_max`. In the k6 summary export: `resp_202`, `resp_503`, `resp_other` with the sub-metrics `{seg:steady|one_down|two_down|recovery}`.

node_exporter metrics used: `node_cpu_seconds_total` and the textfile gauge `ep_k4_phase`.

Kafka metrics used, from the kafka-exporter: `kafka_brokers`, `kafka_topic_partition_in_sync_replica`, `kafka_topic_partition_leader`, `kafka_topic_partition_under_replicated_partition`, `kafka_topic_partition_current_offset`.

ADDED BY K4:

| What | Where |
|---|---|
| `kafka-exporter` container (`danielqsj/kafka-exporter:v1.9.0`, port 9308, metadata refresh 2 s) | `infra/compose/docker-compose.kafka.yml` |
| Prometheus scrape job `kafka` (1 s) | `infra/compose/prometheus-config.yml` |
| `ep_k4_phase` (written by `run.sh` to the node_exporter textfile dir, removed on exit) | `loadtests/k6/stage-2/K4/run.sh` |
| k6 script with `seg` and `acks` tags | `loadtests/k6/stage-2/K4/script.js` |

No change to the ingest-api or consumer-sink code.

## 4. Grafana panels

`[w]` = `$__rate_interval`. Job filters are left out except where they matter.

| # | Panel | Query | What it shows |
|---|---|---|---|
| 1 | Answers per second by status | `sum(rate(k6_http_reqs_total{status="202"}[w]))`, same for `"503"`, and `status!~"202\|503"` | what the client got |
| 2 | Share not accepted (%) | `100 * sum by (acks)(rate(k6_http_reqs_total{status!="202"}[w])) / sum by (acks)(rate(k6_http_reqs_total[w]))` | error rate per variant |
| 3 | Latency p50 / p99 | `histogram_quantile(0.99, sum by (acks)(rate(k6_http_req_duration_seconds[w])))` | client latency |
| 4 | Run phase | `ep_k4_phase` | the kills on the time axis |
| 5 | Brokers seen | `kafka_brokers` | 3 -> 2 -> 1 -> 3 |
| 6 | ISR per partition | `kafka_topic_partition_in_sync_replica{topic="k4-events"}` | ISR shrink and growth |
| 7 | Leader per partition | `kafka_topic_partition_leader{topic="k4-events"}` | leader changes |
| 8 | Partitions below min ISR | `count(kafka_topic_partition_in_sync_replica{topic="k4-events"} < 2)` and the under-replicated sum | where acks=all cannot write |
| 9 | 503 and send errors | `sum by (reason)(rate(ep_http_503_total[w]))`, `sum by (type)(rate(ep_kafka_send_errors_total[w]))` | why the API said no |
| 10 | Health check and breaker | `ep_kafka_health_ok`, `ep_kafka_breaker_state`, `ep_kafka_producer_connected` | API behaviour |
| 11 | Acked vs failed (server) | `rate(ep_kafka_messages_acked_total[w])`, `rate(ep_kafka_messages_failed_total[w])` | producer side |
| 12 | Produce RTT per broker p99 | `ep_kafka_broker_rtt_seconds{quantile="0.99"}` | which brokers answer |
| 13 | Offset growth per partition | `rate(kafka_topic_partition_current_offset{topic="k4-events"}[w])` | which partitions receive records |
| 14 | Broker cores busy | `100 * (1 - avg(rate(node_cpu_seconds_total{mode="idle",cpu=~"$cpu_brokers"}[w])))` | cost on survivors |
| 15 | API CPU and event-loop lag | `rate(process_cpu_seconds_total[w])`, `nodejs_eventloop_lag_seconds` | API cost |
| 16 | Producer and batcher queue | `ep_kafka_client_queue_messages`, `ep_batcher_queue_depth` | backlog while Kafka refuses |
| 17 | k6 VUs | `k6_vus`, `k6_vus_max` | requests piling up |
| 18 | Dropped iterations | `sum(rate(k6_dropped_iterations_total[w]))` | validity (should be 0 in steady) |
| 19 | Generator CPU | `cpu=~"$cpu_k6"` | validity (under about 70%) |
| 20 | API and monitoring cores | `cpu=~"$cpu_server"`, `cpu=~"$cpu_monitoring"` | validity |
| 21 | Audit drain rows/s | `sum(rate(ep_consumer_rows_inserted_total[w]))` | audit finished |

Verify pair for Grafana Explore (after the stack is up and one run happened):

```promql
count(kafka_topic_partition_leader{topic="k4-events"})
count by (acks, seg)(k6_http_reqs_total)
```

## 5. Run it

(a) dry run, prints the plan and runs nothing:

```bash
DRY_RUN=1 loadtests/k6/stage-2/K4/run.sh
```

(b) trial, one variant, one repeat (plumbing only, never quote it):

```bash
VARIANTS="all" REPEATS=1 RESULTS_NAME=k4-trial loadtests/k6/stage-2/K4/run.sh
```

(c) the full experiment:

```bash
loadtests/k6/stage-2/K4/run.sh
```

(d) one case by hand through the shared runner (no kills, no audit; the API must already run with a topic and `KAFKA_TOPIC`):

```bash
cd loadtests/k6 && env RESULTS_NAME=k4-hand LABEL=k4-hand SCRIPT=stage-2/K4/script.js RUN_ID=a4ff0000 ACKS=all \
  RATE=2000 DURATION=30s API_KEY=<key> ROUTE=/healthz ./runner.sh
```

Watch during a run, in another terminal:

```bash
docker ps --format '{{.Names}} {{.Status}}' | grep ep-kafka
docker exec ep-kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:19092 --describe --topic k4-events
```

Results go to `loadtests/k6/stage-2/K4/results/<name>/`: `summary.tsv` (one row per run, including the audit column and the 202 / 503 counts per segment), `runs.tsv` (start and end epochs of every run, and one row per kill, restart and full-ISR moment), `env.txt`, `captures/` (the topic's leaders and ISR and the KRaft controller at 6 moments of every run, the API log, the metric snapshots), and the k6 lap JSONs.

## 6. What this script changes on my machine

- `docker kill` of two broker containers in every run, `docker start` of both at t=135 s, and again in the EXIT trap (so Ctrl+C never leaves a broker dead).
- `docker update --cpuset-cpus` on the brokers, Postgres and the monitoring containers (restored on exit; `KEEP_PINNING=1` keeps the new pinning).
- Deletes and creates the topic `k4-events`; deletes the audit consumer groups it creates.
- Deletes the rows of the run from the `events` table of Postgres (before and after the audit), found by the run id prefix of the event id.
- Writes `infra/compose/textfile/ep_k4.prom` (removed on exit).
- Starts and stops `node` processes (ingest-api, consumer-sink) and k6.
- No `sudo`, no `sysctl`, no `tc`.

## 7. Analyze in Grafana

Dashboard: [K4 - Kill a broker under load.json](../../../../infra/observability/dashboards/stage-2/K4%20-%20Kill%20a%20broker%20under%20load.json). Import it in Grafana (Dashboards, New, Import), pick the Prometheus datasource, and set the time range to a run from `runs.tsv` (epoch seconds; `date -d @<epoch>`).

Reading order: row 1 (what the client saw), then row 2 with the phase panel next to it (leader changes, ISR, the API's health check and breaker), row 3 (cost on the survivors), row 4 (is the run valid), and last the audit column of `summary.tsv`.
