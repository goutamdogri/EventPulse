# K12 Message size and batch limits

Why this experiment and what I expect: see `notebook/stage-2/experiments/K12/Explanation.md`.

## 1. Run the server

`run.sh` starts and stops the ingest-api itself (a fresh start for every cell), so there is nothing to start by hand. Nothing may listen on port 8000 when I start; the preflight checks this. There is no consumer-sink in this experiment: I only look at what reaches the topic.

The command it runs for every cell (the settings of the variant are in the table in section 2):

```
cd services/ingest-api && env KAFKA_BROKERS=localhost:9092,localhost:9094,localhost:9096 KAFKA_TOPIC=k12-events \
  KAFKA_ACKS=-1 KAFKA_IDEMPOTENT=true KAFKA_COMPRESSION=none KAFKA_LINGER_MS=0 BATCH_MAX_MESSAGES=500 BATCH_MAX_WAIT_MS=5 BATCH_MAX_IN_FLIGHT=1 KAFKA_KEY_MODE=event \
  <variant settings> taskset -c 0,6 node --env-file=.env dist/main.js
```

`dist/main.js` must be built (`pnpm build` in `services/ingest-api`). The settings come from `run.sh`, not from my shell and not from `.env` (except the API key and the database URL).

CPU pinning (same plan as K11, the consumer CPUs 1,7 stay unused):

| Group | CPUs |
|---|---|
| ingest-api | 0,6 |
| Kafka brokers (containers, moved with `docker update`) | 2,8,3,9 |
| k6 (`K6_CPUS`) | 4,10 |
| Prometheus, Grafana, node_exporter, kafka-exporter, Postgres | 5,11 |

## 2. Run configuration

The variable is the size of the event. The second variable is which layer of configuration is set to catch it (the variant). One cell = one variant x one size: the API is restarted, k6 sends 5 req/s for 10 s (50 requests) with that size, and the log end offset of the topic is read before and after.

| Variant | API settings | Topic `max.message.bytes` | Layer that should be the lowest limit |
|---|---|---|---|
| `edge` | `BODY_LIMIT=64kb`, `BATCH_MAX_BYTES=900000` | broker default (1048588) | the edge (body-parser) |
| `batcher` | `BODY_LIMIT=4mb`, `BATCH_MAX_BYTES=900000` | broker default | the batcher byte cap |
| `broker` | `BODY_LIMIT=4mb`, `BATCH_MAX_BYTES=900000` | 200000 | the broker (topic override of `message.max.bytes`) |
| `client` | `BODY_LIMIT=4mb`, `BATCH_MAX_BYTES=1000000` | broker default | the Kafka client (`message.max.bytes` of librdkafka, 1000000) |
| `acks0` | like `broker` + `KAFKA_ACKS=0`, `KAFKA_IDEMPOTENT=false` | 200000 | the broker, but the producer does not wait for an answer |

Size levels. `body:N` = bytes of the HTTP body (what the edge judges). `wire:N` = bytes the batcher counts for the event: body + 36 (key) + 96 (headers) + 15 + length of the tenant name. The tenant name is read from `services/ingest-api/.env` and written to `env.txt`.

| Variant | Levels |
|---|---|
| `edge`, `batcher`, `broker` | body 1000, 32768, 65000, 66000, 180000, 262144, 600000, 880000, 950000, 2000000, 5000000 |
| `broker` (extra cell) | `mix`: 1000 req/s of small events (about 1,000 bytes), every 100th request is 400000 bytes |
| `client` | body 880000, wire 990000, wire 999900, wire 1000000, wire 1000100 |
| `acks0` | body 65000, 180000, 262144, 600000 |

43 cells per repeat x 3 repeats = 129 cells. Repeat 2 runs the variants in reverse order. The topic is deleted and recreated before every variant run, with `max.message.bytes` only where the table says so. Total about 63 minutes.

| Env knob | Default | Meaning |
|---|---|---|
| `RATE` | 5 | requests per second of one cell (open model) |
| `DURATION_S` | 10 | seconds per cell |
| `VARIANTS` | `edge batcher broker client acks0` | which variants to run |
| `REPEATS` | 3 | repeats per variant |
| `TOPIC_MAX_BYTES` | 200000 | `max.message.bytes` of the topic in `broker` and `acks0` |
| `MIX_RATE`, `MIX_BIG_BYTES`, `MIX_BIG_EVERY` | 1000, 400000, 100 | the mix cell |
| `RESULTS_NAME` | `k12-message-size` | folder name under `results/` |
| `TOPIC`, `PARTITIONS`, `SERVER_CPUS`, `KAFKA_CPUS`, `K6_CPUS`, `MON_CPUS`, `KEEP_PINNING`, `DRY_RUN` | see `run.sh` | |

## 3. Instrumentation needed

No code was added to the services for K12. The settings it needs (`BODY_LIMIT`, `BATCH_MAX_BYTES`, `KAFKA_ACKS`, `KAFKA_IDEMPOTENT`) already exist in `services/ingest-api/src/config.ts`.

- prom-client defaults used: `process_cpu_seconds_total`, `process_resident_memory_bytes`.
- Custom server metrics used: `http_requests_total` (labels `route`, `status`), `ep_batcher_rejected_total` (label `reason`), `ep_batcher_batch_size_bytes` (histogram), `ep_kafka_send_errors_total` (label `type`), `ep_kafka_messages_acked_total`, `ep_kafka_messages_failed_total`, `eventpulse_event_loop_delay_seconds`.
- kafka-exporter metrics used: `kafka_topic_partition_current_offset` (log end offset), `kafka_topic_partition_under_replicated_partition`.
- k6 metrics used: `k6_http_reqs_total` (label `status`), `k6_http_req_duration_seconds` (native histogram), `k6_dropped_iterations_total`. In the k6 summary export: `resp_202`, `resp_503`, `resp_other`.
- `ADDED BY K12` (k6 counters in `script.js`, in Prometheus only after the first k6 run): `k6_resp_status0_total`, `k6_small_fail_total`, `k6_big_fail_total`. In the summary export only: `resp_413`, `resp_500`, `small_202`, `big_202`, gauges `body_bytes`, `wire_bytes`.
- `ADDED BY K12`: gauge `ep_k12_size_bytes{testid,variant}` (the size of the running cell, -1 between cells), written by `run.sh` to `infra/compose/textfile/ep_k12.prom` and removed on exit. It exists in Prometheus only after the first run.
- node_exporter metrics used: `node_cpu_seconds_total`.

## 4. Grafana panels

`[w]` = `$__rate_interval`, job filters omitted.

| # | Panel | Query | What it shows |
|---|---|---|---|
| 1 | Answers seen by k6, by status | `sum by (status)(rate(k6_http_reqs_total{url=~".*/v1/events"}[w]))` | 202 / 413 / 500 / 0 per size |
| 2 | Answers counted by the API | `sum by (status, route)(rate(http_requests_total{method="POST"}[w]))` | the same, from the API side |
| 3 | Size of the cell that is running | `max by (variant)(ep_k12_size_bytes)` | read the panels against the size |
| 4 | k6 latency p50 / p99 | `histogram_quantile(0.99, sum(rate(k6_http_req_duration_seconds[w])))` | fast refusal vs slow failure |
| 5 | k6 requests with status 0 | `sum(rate(k6_resp_status0_total[w]))` | no answer at all |
| 6 | Records added to the topic vs acknowledged | `sum(rate(kafka_topic_partition_current_offset{topic="k12-events"}[w]))`, `sum(rate(ep_kafka_messages_acked_total[w]))` | did Kafka see it |
| 7 | Refused by the batcher | `sum(rate(ep_batcher_rejected_total{reason="too_large"}[w]))` | second layer |
| 8 | Kafka send errors by type | `sum by (type)(rate(ep_kafka_send_errors_total[w]))` | client and broker layer |
| 9 | Messages in failed batches | `sum(rate(ep_kafka_messages_failed_total[w]))` | blast radius of a bad message |
| 10 | Batch size in bytes p50 / p99 | `histogram_quantile(0.99, sum by (le)(rate(ep_batcher_batch_size_bytes_bucket[w])))` | batch against the cap |
| 11 | Small and big events that failed | `sum(rate(k6_small_fail_total[w]))`, `sum(rate(k6_big_fail_total[w]))` | the mix cell |
| 12 | ingest-api process CPU | `rate(process_cpu_seconds_total[w])` | resource view |
| 13 | ingest-api memory | `process_resident_memory_bytes` | big bodies in memory |
| 14 | Host CPU busy by pinned group | `node_cpu_seconds_total` idle, `cpu=~"$cpu_server"`, `$cpu_brokers`, `$cpu_monitoring` | host view |
| 15 | k6 dropped iterations | `rate(k6_dropped_iterations_total[w])` | validity (must be 0) |
| 16 | Generator CPU busy | same as 14 with `$cpu_k6` | generator headroom |
| 17 | Event-loop delay p99 | `eventpulse_event_loop_delay_seconds{quantile="0.99"}` | API stalls |
| 18 | Under-replicated partitions | `kafka_topic_partition_under_replicated_partition{topic="k12-events"}` | cluster stayed healthy |

Verify in Grafana Explore: `count by (variant)(ep_k12_size_bytes)` and `sum by (status)(k6_http_reqs_total)`.

## 5. Run it

(a) Dry run, prints the plan and changes nothing:

```bash
cd loadtests/k6/stage-2/K12 && DRY_RUN=1 ./run.sh
```

(b) Trial, the `batcher` variant only, one repeat (plumbing check, about 5 minutes, never quoted):

```bash
cd loadtests/k6/stage-2/K12 && VARIANTS="batcher" REPEATS=1 RESULTS_NAME=k12-trial ./run.sh
```

(c) Full experiment:

```bash
cd loadtests/k6/stage-2/K12 && ./run.sh
```

(d) Watch from another terminal:

```bash
tail -f loadtests/k6/stage-2/K12/results/k12-message-size/runs.tsv
```

Results go to `results/<RESULTS_NAME>/`: `summary.tsv` (one row per cell), `runs.tsv` (epochs of every cell, for zooming Grafana), `env.txt`, `captures/` and the k6 lap JSONs.

Extra columns of `summary.tsv`: `body_bytes`, `wire_bytes`, `n413`, `n500`, `status0`, `kafka_delta` (records the topic gained in the cell, which is also the `rows` audit column), `api_acked`, `api_failed`, `batcher_too_large`, `send_errors` (type=count), `api_http` (status=count at the API), `small_ok`, `small_fail`, `big_ok`, `big_fail` (the mix cell), `status`.

Files in `captures/` per cell: `<testid>.server.out` (API output, with its Kafka errors), `<testid>.api-counters.txt` (the API counters read before it stopped), `<testid>.sample.txt` (one extra request made with curl after the cell, with the HTTP status and the first 400 bytes of the answer; not made for the mix cell).

## 6. What this script changes on my machine

- Deletes and recreates the topic `k12-events` before every variant run (`max.message.bytes=200000` in `broker` and `acks0`, on this topic only), and deletes it at the end. The brokers' own settings are not touched.
- Starts the ingest-api and stops it with `kill` (SIGTERM) of its own pid, before and after every cell.
- `docker update --cpuset-cpus` on the brokers, Postgres and the monitoring containers; the old values are restored on exit.
- Writes `infra/compose/textfile/ep_k12.prom` and removes it on exit.
- Sends one extra request per cell with `curl` (up to 5 MB) to the API.
- No sudo, no sysctl, no tc, no change to the broker configuration, nothing written to Postgres.

## 7. Analyze in Grafana

Dashboard: [K12 - Message size and batch limits.json](../../../../infra/observability/dashboards/stage-2/K12%20-%20Message%20size%20and%20batch%20limits.json). Import it through Dashboards > New > Import and pick the Prometheus datasource. Zoom to a variant with the epochs in `runs.tsv`. Read the rows in order: 1 verdict (which answer at which size), 2 mechanism (who caught it, and whether the topic grew), 3 resources, 4 validity (dropped iterations must be 0 and no under-replicated partitions before I trust a cell). k6 series are not marked stale after a run, so the last values can stay on screen for minutes.
