# K1 Throughput versus the Stage 1 baseline

Why this experiment and what I expect: see `notebook/stage-2/experiments/K1/Explanation.md`

## 1. Run the server

`run.sh` starts and stops `ingest-api` by itself (`MANAGE_SERVER=1`, default). It restarts the server before every
run, so each run starts with an empty batcher and a fresh event-loop histogram. What it runs (inside `services/ingest-api`):

```bash
KAFKA_BROKERS=localhost:9092,localhost:9094,localhost:9096 KAFKA_KEY_MODE=event \
  taskset -c 0,6 node --env-file=.env dist/main.js
```

Only the two `KAFKA_*` values above are set by `run.sh`. Every other knob (`KAFKA_ACKS`, `KAFKA_LINGER_MS`,
`BATCH_MAX_*` ...) stays at the API's default unless I export it before running. Environment variables win over
`.env`. `KAFKA_KEY_MODE=event` makes the record key the event id, so records spread over all 6 partitions. With the
API default (`tenant`) all events of my single API key would go to one partition.

Before the first run:

```bash
cd services/ingest-api && pnpm build                                  # dist/main.js must exist
docker compose -f infra/compose/docker-compose.yml up -d              # prometheus, grafana, node_exporter, postgres
docker compose -f infra/compose/docker-compose.kafka.yml up -d        # 3 brokers + topics
```

The consumer-sink must be stopped (preflight refuses to run if something listens on 8001-8003).

CPU plan (12 logical CPUs = 6 cores, SMT pairs 0/6 1/7 2/8 3/9 4/10 5/11). Server and k6 are the same as in B1.

| Component                  | Logical CPUs | How it is pinned                                                                  |
| -------------------------- | ------------ | --------------------------------------------------------------------------------- |
| ingest-api (Node)          | 0,6          | `taskset` in run.sh                                                               |
| Kafka brokers (3, 1 CPU quota each) | 1,7,2,8 | `docker update --cpuset-cpus` (compose pins them to 3,9,4,10, which k6 uses)     |
| k6                         | 3,9,4,10     | `K6_CPUS` read by `runner.sh`                                                     |
| Monitoring + idle Postgres | 5,11         | `docker update --cpuset-cpus`                                                     |
| perf-test container        | 3,9,4,10     | `docker run --cpuset-cpus` (ceiling mode only, k6 is idle then)                   |

The old cpusets are restored when the script ends (`KEEP_PINNING=1` keeps the new ones).

Facts recorded in `env.txt` for every pass: kernel, node, k6, broker image and limits, topic description, API knobs,
pinning, git commit and dirty count.

## 2. Run configuration

Variable: offered rate (open model, `constant-arrival-rate`). One server config. Payload about 500 B (`PAD=300`),
the same request as B1.

| Mode      | Command            | Rates (req/s)                                                                | Duration | Repeats | Probes                        | Rough time |
| --------- | ------------------ | ---------------------------------------------------------------------------- | -------- | ------- | ----------------------------- | ---------- |
| `coarse`  | `./run.sh coarse`  | 1000 2500 5000 7500 10000 15000 20000 30000                                  | 30 s     | 1       | none                          | about 12 min |
| `fine`    | `./run.sh fine`    | 9 even steps around the knee, read from `results/k1-coarse/suggestion.env`   | 60 s     | 3       | 2 (about 70% of the knee, knee) | about 55 min |
| `ceiling` | `./run.sh ceiling` | `kafka-producer-perf-test.sh`: 2 profiles (`big`, `small` batch.size) x 3 repeats, 2,000,000 records of 500 B | until done | 3 | none | about 14 min |

Before every run: stop server, delete and recreate the topic (6 partitions, RF 3, `min.insync.replicas=2`,
retention 24 h, like `events.raw`), start server, send 100 warm-up requests, wait 10 s. In ceiling mode the topic is
`k1-perf` and no server runs.

Audit: records added to the topic during the run (sum of end offsets after minus before) must equal the 202 answers k6
counted. The warm-up records are in the "before" number.

| Env knob | Default | Meaning |
| --- | --- | --- |
| `RATES` | per mode (above) | offered rates; the fine pass reads `suggestion.env` when it is not set |
| `REPEATS` | 1 coarse, 3 fine and ceiling | repeats; repeat is the outer loop |
| `DURATION` | 30s coarse, 60s fine | length of each measured run |
| `PROBE_DURATION` | 30s | length of a probe run |
| `RESULTS_NAME` | `k1-coarse` / `k1-fine` / `k1-ceiling` | folder under `results/` |
| `MANAGE_SERVER` | 1 | 0 = use a server I started myself |
| `SERVER_CPUS` / `KAFKA_CPUS` / `K6_CPUS` / `MON_CPUS` | 0,6 / 1,7,2,8 / 3,9,4,10 / 5,11 | pinning |
| `KAFKA_BROKERS`, `KAFKA_KEY_MODE` | the three localhost ports, `event` | passed to the API |
| `KAFKA_ACKS KAFKA_IDEMPOTENT KAFKA_COMPRESSION KAFKA_LINGER_MS BATCH_MAX_MESSAGES BATCH_MAX_WAIT_MS BATCH_MAX_BYTES BATCH_MAX_IN_FLIGHT BATCH_MAX_QUEUE` | not set (API defaults) | passed to the API only when exported |
| `TOPIC`, `PARTITIONS`, `RETENTION_MS` | `events.raw`, 6, 86400000 | topic that is recreated before each run |
| `RECORD_SIZE`, `NUM_RECORDS`, `WARM_RECORDS`, `LINGER_MS`, `PROFILES` | 500, 2000000, 300000, 5, `big:262144 small:32768` | ceiling mode |
| `K6 PAD / PRE_VUS / MAX_VUS / REQ_TIMEOUT` | 300 / 500 / 3000 / 10s | read by `script.js` |
| `DRY_RUN` | 0 | 1 = print the plan only |
| `KEEP_PINNING` | 0 | 1 = leave the new cpusets in place |

## 3. Instrumentation needed

Nothing was added to the code for K1. All of these already exist.

- **prom-client defaults used:** `process_cpu_user_seconds_total`, `process_cpu_system_seconds_total`, `nodejs_eventloop_lag_p99_seconds`, `nodejs_eventloop_lag_mean_seconds`, `nodejs_heap_size_used_bytes`.
- **Custom server metrics used** (`services/ingest-api/src/observability/`): `http_request_duration_seconds` (route `/v1/events`), `ep_batcher_batch_size_messages`, `ep_batcher_flush_total`, `ep_batcher_queue_wait_seconds`, `ep_batcher_queue_depth`, `ep_batcher_in_flight_batches`, `ep_batcher_rejected_total`, `ep_kafka_send_seconds`, `ep_kafka_broker_rtt_seconds`, `ep_http_503_total`, `ep_kafka_health_ok`, `ep_kafka_producer_connected`, `ep_kafka_breaker_state`.
- **k6 metrics used:** `k6_http_req_duration_seconds` (native histogram), `k6_iterations_total`, `k6_dropped_iterations_total`, `k6_vus`, `k6_vus_max`, and the three counters of `script.js`: `k6_k1_resp_202_total`, `k6_k1_resp_503_total`, `k6_k1_resp_other_total` (they appear in Prometheus after the first k6 run).
- **node_exporter metrics used:** `node_cpu_seconds_total` (per cpu, mode idle), `node_disk_written_bytes_total` (device `nvme0n1`).

There is no broker or container exporter. Broker CPU in the measured runs is the busy share of the CPUs the brokers are pinned to. Per-container CPU appears only in the probe runs (`docker stats` in `captures/`).

## 4. Grafana panels

Dashboard variables: `ds` (datasource) and four CPU variables `cpu_server` (0|6), `cpu_k6` (3|9|4|10), `cpu_kafka` (1|7|2|8), `cpu_monitoring` (5|11). `[w]` = `$__rate_interval`; job filters omitted (`eventpulse-nodejs` for server metrics, `node` for node_exporter).

| #  | Panel | Query | What it shows |
| -- | ----- | ----- | ------------- |
| 1  | Offered vs accepted rate (k6) | `sum(rate(k6_iterations_total[w])) + sum(rate(k6_dropped_iterations_total[w]))`, `sum(rate(k6_k1_resp_202_total[w]))`, 503 and other | rate k6 tried vs 202s |
| 2  | Latency seen by k6 | `histogram_quantile(0.99, sum(rate(k6_http_req_duration_seconds[w])))` (also p50, p99.9) | the client's view; the knee |
| 3  | Latency seen by the server | `histogram_quantile(0.99, sum by (le)(rate(http_request_duration_seconds_bucket{route="/v1/events"}[w])))` | the server's view |
| 4  | Average batch size | `sum(rate(ep_batcher_batch_size_messages_sum[w])) / sum(rate(ep_batcher_batch_size_messages_count[w]))` | messages per Kafka send |
| 5  | Flushes per second by reason | `sum by (reason)(rate(ep_batcher_flush_total[w]))` | time vs size vs drain |
| 6  | Time inside Kafka send | `histogram_quantile(0.99, sum by (le)(rate(ep_kafka_send_seconds_bucket[w])))` | broker ack time |
| 7  | Time in the batcher queue | `histogram_quantile(0.99, sum by (le)(rate(ep_batcher_queue_wait_seconds_bucket[w])))` | wait before a send |
| 8  | Queue depth and in-flight batches | `max(ep_batcher_queue_depth)`, `max(ep_batcher_in_flight_batches)` | back-pressure |
| 9  | Broker round trip per broker | `max by (broker)(ep_kafka_broker_rtt_seconds{quantile="0.99"})` | which broker is slow |
| 10 | Node process CPU | `rate(process_cpu_user_seconds_total[w])`, `rate(process_cpu_system_seconds_total[w])` | process CPU, max 2.0 |
| 11 | Server cores busy | `1 - rate(node_cpu_seconds_total{mode="idle",cpu=~"$cpu_server"}[w])` | CPU 0 and 6 |
| 12 | Broker cores busy | same with `$cpu_kafka` | broker CPU |
| 13 | Disk bytes written | `rate(node_disk_written_bytes_total{device="nvme0n1"}[w])` | disk write rate |
| 14 | Event loop lag | `max(nodejs_eventloop_lag_p99_seconds)` | main thread saturation |
| 15 | Server heap used | `max(nodejs_heap_size_used_bytes)` | memory |
| 16 | k6 dropped iterations | `sum(rate(k6_dropped_iterations_total[w]))` | must be 0 |
| 17 | Generator cores busy | same idle query with `$cpu_k6` | keep under 70% |
| 18 | k6 VUs | `max(k6_vus)`, `max(k6_vus_max)` | VU cap |
| 19 | Monitoring cores busy | same idle query with `$cpu_monitoring` | disturbance |
| 20 | Shedding | `sum by (reason)(rate(ep_http_503_total[w]))`, `sum by (reason)(rate(ep_batcher_rejected_total[w]))` | 503s and rejects |
| 21 | Kafka client health | `min(ep_kafka_health_ok)`, `min(ep_kafka_producer_connected)`, `max(ep_kafka_breaker_state)` | producer state |

Verify pair for Grafana Explore after the first run:

```promql
count by (testid)(k6_k1_resp_202_total)
count(ep_kafka_messages_acked_total)
```

## 5. Run it

```bash
cd loadtests/k6/stage-2/K1

DRY_RUN=1 ./run.sh coarse                                   # (a) dry run: prints the plan, runs nothing
RATES="5000" REPEATS=1 RESULTS_NAME=k1-trial ./run.sh coarse # (b) one-rate trial (plumbing check, never quoted)
./run.sh coarse                                             # (c) coarse pass, then:
./run.sh fine                                               #     fine pass (reads results/k1-coarse/suggestion.env)
./run.sh ceiling                                            #     raw Kafka ceiling (nothing else may run during it)
```

(d) A single case by hand through the shared runner (the server must already be running and pinned, and the topic
must exist):

```bash
cd loadtests/k6
env RESULTS_NAME=k1-manual LABEL=k1-manual SCRIPT=stage-2/K1/script.js RATE=8000 DURATION=30s \
    API_KEY=<key from services/ingest-api/.env> ROUTE=/healthz ./runner.sh
```

Watch during a run, in another terminal (cheap commands only; `docker stats` is a probe-run tool and would disturb a measured run):

```bash
tail -f loadtests/k6/stage-2/K1/results/k1-coarse/server.out
curl -s localhost:8000/debug/stats
```

Where results go, inside `loadtests/k6/stage-2/K1/results/<name>/`: `summary.tsv` (one row per run), `runs.tsv` (start and
end epoch of every run, to zoom Grafana), `env.txt`, `captures/` (offsets before and after, probe output, perf-test raw
output), `server.out`, the k6 lap JSONs in timestamp folders, and `ceiling.tsv` (ceiling mode).

```bash
python3 loadtests/k6/analyze.py table loadtests/k6/stage-2/K1/results/k1-fine/summary.tsv
```

## 6. What this script changes on my machine

- `docker update --cpuset-cpus` on the three broker containers (to `1,7,2,8`), the Postgres container and the monitoring containers (to `5,11`). The old values are saved first and restored in the EXIT trap (also after Ctrl+C), unless `KEEP_PINNING=1`.
- Deletes and recreates the topic `events.raw` before every run (this destroys the records currently in it) and the topic `k1-perf` before every ceiling run. `k1-perf` is deleted again at exit. `events.raw` is left empty and recreated with 6 partitions, RF 3, `min.insync.replicas=2`, retention 24 h; `events.dlq` is not touched.
- Starts and stops `node dist/main.js` (`kill` of its own pid only).
- Ceiling mode starts a throw-away container (`docker run --rm`) from the broker image.
- No `sudo`, no `sysctl`, no `tc`.

## 7. Analyze in Grafana

Dashboard: [`infra/observability/dashboards/stage-2/K1 - Throughput vs Stage 1.json`](../../../../infra/observability/dashboards/stage-2/K1%20-%20Throughput%20vs%20Stage%201.json).
Import it in Grafana through Dashboards > New > Import > Upload JSON file and pick the Prometheus datasource. Use the
epochs in `runs.tsv` to set the time range for a run. Read the rows in this order: 1 (where is the knee), 4 (is the
run valid: dropped iterations 0, generator under 70%, no shedding), 3 (which CPU is full), 2 (where the time inside the
API goes).
