# K10 Kafka unavailable and API backpressure

Why this experiment and what I expect: see `notebook/stage-2/experiments/K10/Explanation.md`.

## 1. Run the server

`run.sh` starts and stops the ingest-api itself for every run, so there is nothing to start by hand. Nothing may listen on port 8000 when I start (the preflight checks this).

The command it runs inside `services/ingest-api` (same style as K4):

```
env KAFKA_BROKERS=localhost:9092,localhost:9094,localhost:9096 KAFKA_TOPIC=k10-events <common settings> <variant settings> \
  taskset -c 0,6 node --env-file=.env dist/main.js
```

`dist/main.js` must be built (`pnpm build` in `services/ingest-api`). The API reads the values written in `run.sh`, not my shell and not `.env` (except the API key and the database URL).

CPU pinning (the same plan as K2 and K4):

| Group | CPUs |
|---|---|
| ingest-api | 0,6 |
| Kafka brokers (containers, moved with `docker update`) | 1,7,2,8 |
| k6 (`K6_CPUS`) | 3,9,4,10 |
| Prometheus, Grafana, node_exporter, kafka-exporter | 5,11 |

The API's Node heap is capped with `NODE_OPTIONS=--max-old-space-size=512` in both variants, so a runaway queue ends in a crash within minutes and not in the machine's swap.

## 2. Run configuration

One steady rate. The cluster is damaged on a timetable (seconds after the load starts):

| t | What happens | k6 segment |
|---|---|---|
| 0 - 30 | load against a healthy cluster | `steady` |
| 30 | `docker kill` of all 3 brokers | `down` |
| 150 | `docker start` of all 3 brokers, the API is NOT restarted | `recovery` |
| 240 | load ends, then wait for healthy brokers and a full ISR | |

Variants (only the queue settings differ):

| Variant | API settings |
|---|---|
| `unbounded` | `BATCH_MAX_QUEUE=0` (`config.ts` then also drops the byte cap, the send timeout and the circuit breaker) |
| `bounded` | `BATCH_MAX_QUEUE=10000 BATCH_MAX_QUEUE_BYTES=67108864 KAFKA_SEND_TIMEOUT_MS=5000 KAFKA_BREAKER_FAILURES=3 KAFKA_BREAKER_COOLDOWN_MS=5000` |

Common API settings: `KAFKA_ACKS=-1 KAFKA_IDEMPOTENT=true KAFKA_COMPRESSION=none KAFKA_LINGER_MS=0 BATCH_MAX_MESSAGES=500 BATCH_MAX_WAIT_MS=5 BATCH_MAX_BYTES=900000 BATCH_MAX_IN_FLIGHT=1 KAFKA_KEY_MODE=event`. Topic `k10-events`: 6 partitions, replication factor 3, `min.insync.replicas=2`, deleted and recreated before every run.

2 variants x 3 repeats = 6 measured runs of 240 s. Repeat 2 runs the variants in the opposite order. The API is restarted before every run. Total about 39 minutes.

| Env knob | Default | Meaning |
|---|---|---|
| `RATE` | 2000 | requests per second (open model) |
| `STEADY_S` | 30 | seconds of load before the kill |
| `DOWN_S` | 120 | seconds with all brokers dead |
| `RECOVER_S` | 90 | seconds of load after the brokers start again |
| `HEAP_MB` | 512 | Node old-space cap of the API |
| `CLIENT_TIMEOUT` | 2s | how long k6 waits for one answer (an unanswered request keeps one VU busy, 2000 x 2 s = 4,000 VUs) |
| `VARIANTS` | `unbounded bounded` | which variants to run |
| `REPEATS` | 3 | repeats per variant |
| `RESULTS_NAME` | `k10-kafka-down` | folder name under `results/` |
| `PARTITIONS`, `TOPIC`, `ISR_TIMEOUT_S`, `SERVER_CPUS`, `KAFKA_CPUS`, `K6_CPUS`, `MON_CPUS`, `KEEP_PINNING`, `DRY_RUN` | see `run.sh` | |

## 3. Instrumentation needed

No code was added for K10: the queue, the bound and the breaker already exist in `services/ingest-api` (`BATCH_MAX_QUEUE=0` is the unbounded mode).

- prom-client defaults used: `process_resident_memory_bytes`, `process_cpu_seconds_total`, `nodejs_heap_size_used_bytes`, `nodejs_heap_size_total_bytes`.
- Custom server metrics used: `ep_batcher_queue_depth`, `ep_batcher_queue_bytes`, `ep_batcher_queue_wait_seconds` (histogram), `ep_batcher_rejected_total{reason}`, `ep_http_503_total{reason}`, `ep_kafka_breaker_state`, `ep_kafka_producer_connected`, `ep_kafka_health_ok`, `ep_kafka_messages_acked_total`, `ep_kafka_messages_failed_total`, `ep_kafka_send_errors_total`, `http_requests_in_flight`, `http_connections_open`, `eventpulse_event_loop_delay_seconds`.
- k6 metrics used: `k6_http_reqs_total` (labels `status`, `seg`, `variant`), `k6_http_req_duration_seconds` (native histogram), `k6_dropped_iterations_total`, `k6_vus`, `k6_vus_max`. In the k6 summary export: `resp_202`, `resp_503`, `resp_other` with the sub-metrics `{seg:steady|down|recovery}`.
- node_exporter metrics used: `node_cpu_seconds_total`.
- Phase gauge `ep_k10_phase{testid,variant}` (0 steady, 1 all brokers down, 2 brokers started again, -1 no run): `ADDED BY K10`, written by `run.sh` to `infra/compose/textfile/ep_k10.prom` and removed on exit. It exists in Prometheus only after the first run.
- `run.sh` also samples the API's RSS once per second from `/proc` into `captures/<testid>.rss.txt` (lines: epoch, MB, alive or dead). This gives the peak memory and the moment of death even when the process is gone.

## 4. Grafana panels

`[w]` = `$__rate_interval`, job filters omitted.

| # | Panel | Query | What it shows |
|---|---|---|---|
| 1 | Answers per second by kind | `sum by (variant)(rate(k6_http_reqs_total{status="202"}[w]))` and the same for `503` and `status!~"202\|503"` | what the client gets |
| 2 | Latency p50 | `histogram_quantile(0.5, sum by (variant)(rate(k6_http_req_duration_seconds[w])))` | client wait |
| 3 | Latency p99 | same with 0.99 | client wait, tail |
| 4 | Run phase | `max(ep_k10_phase)` | where in the timetable |
| 5 | API RSS | `process_resident_memory_bytes` | memory |
| 6 | Heap used and total | `nodejs_heap_size_used_bytes`, `nodejs_heap_size_total_bytes` | V8 heap against the cap |
| 7 | Queue depth | `ep_batcher_queue_depth` | messages waiting |
| 8 | Queue bytes | `ep_batcher_queue_bytes` | bytes waiting |
| 9 | Queue wait p99 | `histogram_quantile(0.99, sum by (le)(rate(ep_batcher_queue_wait_seconds_bucket[w])))` | wait in the queue |
| 10 | Rejections by reason | `sum by (reason)(rate(ep_batcher_rejected_total[w]))` | load shedding |
| 11 | 503 by reason (server) | `sum by (reason)(rate(ep_http_503_total[w]))` | what the API counted |
| 12 | Breaker, connection, health | `ep_kafka_breaker_state`, `ep_kafka_producer_connected`, `ep_kafka_health_ok` | recovery order |
| 13 | Acks and send errors | `rate(ep_kafka_messages_acked_total[w])`, `rate(ep_kafka_messages_failed_total[w])`, `rate(ep_kafka_send_errors_total[w])` | Kafka side |
| 14 | Requests and connections open | `http_requests_in_flight{route!~"/metrics"}`, `http_connections_open` | piled-up requests |
| 15 | API process CPU | `rate(process_cpu_seconds_total[w])` | GC and work |
| 16 | Server CPUs busy | `node_cpu_seconds_total` idle, `cpu=~"$cpu_server"` | host view |
| 17 | Broker CPUs busy | same with `$cpu_brokers` | broker view |
| 18 | k6 dropped iterations | `rate(k6_dropped_iterations_total[w])` | validity (must be 0) |
| 19 | k6 VUs | `k6_vus`, `k6_vus_max` | VU headroom |
| 20 | k6 CPUs busy | same with `$cpu_k6` | generator headroom |
| 21 | Event-loop delay | `eventpulse_event_loop_delay_seconds{quantile=~"0.99\|max"}` | stalls |

Verify in Grafana Explore: `count by (variant)(k6_http_reqs_total)` and `ep_k10_phase`.

## 5. Run it

(a) Dry run, prints the plan and changes nothing:

```bash
cd loadtests/k6/stage-2/K10 && DRY_RUN=1 ./run.sh
```

(b) Trial, one variant, one repeat (plumbing check only, never quoted):

```bash
cd loadtests/k6/stage-2/K10 && VARIANTS="bounded" REPEATS=1 RESULTS_NAME=k10-trial ./run.sh
```

(c) Full experiment:

```bash
cd loadtests/k6/stage-2/K10 && ./run.sh
```

(d) Watch from another terminal: `docker ps --format '{{.Names}} {{.Status}}' | grep ep-kafka` and `tail -f results/k10-kafka-down/captures/*.rss.txt`.

Results go to `results/<RESULTS_NAME>/`: `summary.tsv` (one row per run), `runs.tsv` (epochs of every kill, restart and run, for zooming Grafana), `env.txt`, `captures/` (server log, RSS samples, counters before and after), and the k6 lap JSONs.

Extra columns of `summary.tsv`: `ok_by_seg`, `fail_by_seg`, `other_by_seg` (steady/down/recovery), `api_alive`, `peak_rss_mb`, `died_at_s`, `acks_after_restart`, `connected`, `srv_503`, `rejected`.

## 6. What this script changes on my machine

- `docker kill` of all 3 broker containers once per run, and `docker start` of them at t=150 s. The EXIT trap starts any broker that is still down, so Ctrl+C does not leave the cluster dead.
- `docker update --cpuset-cpus` on the brokers and the monitoring containers; the old values are restored on exit.
- Deletes and recreates the topic `k10-events` before every run.
- Starts and stops the ingest-api (`kill` of its own pid).
- Writes `infra/compose/textfile/ep_k10.prom` and removes it on exit.
- No sudo, no sysctl, no tc.

## 7. Analyze in Grafana

Dashboard: [K10 - Kafka down and backpressure.json](../../../../infra/observability/dashboards/stage-2/K10%20-%20Kafka%20down%20and%20backpressure.json). Import it through Dashboards > New > Import and pick the Prometheus datasource. Zoom to a run with the epochs in `runs.tsv`. Read the rows in order: 1 verdict, 2 mechanism, 3 resources, 4 validity (dropped iterations must be 0 before I trust the rest).
