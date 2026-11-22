# K2 Producer settings sweep

Why this experiment and what I expect: see `notebook/stage-2/experiments/K2/Explanation.md`

## 1. Run the server

`run.sh` starts and stops `ingest-api` by itself, and restarts it before every run so each run starts with an empty
batcher and fresh counters. It starts (inside `services/ingest-api`):

```bash
KAFKA_BROKERS=localhost:9092,localhost:9094,localhost:9096 KAFKA_KEY_MODE=event <baseline knobs> <config knobs> \
  taskset -c 0,6 node --env-file=.env dist/main.js
```

Unlike K1, `run.sh` writes out every producer/batcher knob for every config (the baseline below, then the config's
changes). Whatever is exported in my shell or written in `.env` for these knobs is ignored (the script unsets them).
`KAFKA_KEY_MODE=event` spreads records over all 6 partitions (same reason as K1).

Before the first run:

```bash
cd services/ingest-api && pnpm build                                  # dist/main.js must contain the K2 metric
docker compose -f infra/compose/docker-compose.yml up -d              # prometheus, grafana, node_exporter, postgres
docker compose -f infra/compose/docker-compose.kafka.yml up -d        # 3 brokers + topics
```

The consumer-sink must be stopped and nothing may listen on :8000 (preflight refuses to run otherwise).

CPU plan: the same as K1 (12 logical CPUs = 6 cores, SMT pairs 0/6 1/7 2/8 3/9 4/10 5/11).

| Component                     | Logical CPUs | How it is pinned                                  |
| ----------------------------- | ------------ | ------------------------------------------------- |
| ingest-api (Node)             | 0,6          | `taskset` in run.sh                               |
| Kafka brokers (3, 1 CPU each) | 1,7,2,8      | `docker update --cpuset-cpus`, restored on exit   |
| k6                            | 3,9,4,10     | `K6_CPUS` read by `runner.sh`                     |
| Monitoring + idle Postgres    | 5,11         | `docker update --cpuset-cpus`, restored on exit   |

## 2. Run configuration

Variable: producer settings, changed one group at a time around a baseline (one-factor-at-a-time, not a full grid).
One offered rate (open model, `constant-arrival-rate`), payload about 500 B with a random filler (`PAYLOAD_MODE=random`).

Baseline (`base`): `BATCH_MAX_WAIT_MS=5 BATCH_MAX_MESSAGES=500 BATCH_MAX_BYTES=900000 BATCH_MAX_IN_FLIGHT=1 KAFKA_LINGER_MS=0 KAFKA_ACKS=-1 KAFKA_IDEMPOTENT=true KAFKA_COMPRESSION=none`.

| Group | Config names | Changes against the baseline |
| ----- | ------------ | ---------------------------- |
| baseline | `base` | none (this is wait 5 ms, 500 messages) |
| A batcher grid | `w1_s50 w1_s500 w1_s5000 w5_s50 w5_s5000 w20_s50 w20_s500 w20_s5000` | `BATCH_MAX_WAIT_MS` 1/5/20 x `BATCH_MAX_MESSAGES` 50/500/5000; the `s5000` configs also set `BATCH_MAX_BYTES=1000000` (its limit), because the default 900,000 B caps a batch at about 1,800 messages of 500 B |
| B durability | `acks1 acks0 noidem` | acks=1 and idempotence off; acks=0 and idempotence off; acks=all with idempotence off (`KAFKA_IDEMPOTENT=true` is only allowed with acks=all) |
| C compression | `gzip snappy lz4 zstd` | `KAFKA_COMPRESSION=<codec>` |

16 configs x 2 repeats x (30 s + about 60 s restart, topic recreate, warm-up, settle) = about 48 min. Repeat is the outer
loop: repeat 1 runs the configs in the table order, repeat 2 in reverse order. There are no probe runs and no ceiling
mode in K2.

Before every run: stop server, delete and recreate the topic (6 partitions, RF 3, `min.insync.replicas=2`, retention
24 h), start server with the config, send 100 warm-up requests, wait 10 s. After k6, wait 6 s (the client statistics
that feed the wire-bytes counter arrive every 5 s).

Audit: records added to the topic during the run (end offsets after minus before) must equal the 202 answers k6
counted. Extra columns written to `summary.tsv` for every run (all from after-minus-before differences, so the warm-up
does not count):

| Column | Meaning |
| --- | --- |
| `avg_batch` | messages per Kafka send (`ep_batcher_batch_size_messages` sum / count) |
| `send_ms_avg` | average `ep_kafka_send_seconds` per batch, in ms |
| `srv_cpu_ms_per_req` | process CPU (user + system) per record, in ms |
| `wire_B_per_rec` | `ep_kafka_client_tx_bytes_total` difference / records: bytes on the wire per record, after compression |
| `log_B_per_rec` | bytes of the topic's `.log` files on broker 1 / all records in the topic: size on disk per record |

| Env knob | Default | Meaning |
| --- | --- | --- |
| `RATE` | 4000 | offered req/s for every run |
| `DURATION` | 30s | length of each run |
| `REPEATS` | 2 | repeats |
| `CONFIGS` | all 16 | space-separated config names to run |
| `PAYLOAD_MODE` | random | `random` (pool of 1024 random base64 strings) or `x` (K1 filler `xxxx...`) |
| `RESULTS_NAME` | `k2-sweep` | folder under `results/` |
| `SERVER_CPUS` / `KAFKA_CPUS` / `K6_CPUS` / `MON_CPUS` | 0,6 / 1,7,2,8 / 3,9,4,10 / 5,11 | pinning |
| `KAFKA_BROKERS`, `KAFKA_KEY_MODE` | the three localhost ports, `event` | passed to the API |
| `TOPIC`, `PARTITIONS`, `RETENTION_MS` | `events.raw`, 6, 86400000 | topic recreated before each run |
| `K6 PAD / PRE_VUS / MAX_VUS / REQ_TIMEOUT` | 300 / 500 / 3000 / 10s | read by `script.js` |
| `DRY_RUN` | 0 | 1 = print the plan only |
| `KEEP_PINNING` | 0 | 1 = leave the new cpusets in place |

## 3. Instrumentation needed

- **prom-client defaults used:** `process_cpu_user_seconds_total`, `process_cpu_system_seconds_total`, `nodejs_eventloop_lag_p99_seconds`.
- **Custom server metrics used** (`services/ingest-api/src/observability/kafka-metrics.ts`): `http_request_duration_seconds` (route `/v1/events`), `ep_batcher_batch_size_messages`, `ep_batcher_batch_size_bytes`, `ep_batcher_flush_total`, `ep_batcher_queue_wait_seconds`, `ep_kafka_send_seconds`, `ep_kafka_messages_acked_total`, `ep_kafka_broker_rtt_seconds`, `ep_http_503_total`, `ep_batcher_rejected_total`.
- **ADDED BY K2:** `ep_kafka_client_tx_bytes_total` (counter) in `services/ingest-api/src/observability/kafka-metrics.ts`, fed from the librdkafka statistics field `tx_bytes` in `onClientStats()` (the counter gets the difference between two statistics reports). It counts bytes the producer sent to the brokers, after compression. It updates every `KAFKA_STATS_INTERVAL_MS` (5 s). Unit test: `kafka-metrics.spec.ts`.
- **k6 metrics used:** `k6_http_req_duration_seconds` (native histogram), `k6_dropped_iterations_total`, and the counter `k6_k2_resp_202_total` from `script.js` (it appears in Prometheus after the first k6 run).
- **node_exporter metrics used:** `node_cpu_seconds_total` (per cpu, mode idle), `node_disk_written_bytes_total` (device `nvme0n1`).

There is no broker or container exporter. Broker CPU is the busy share of the CPUs the brokers are pinned to; broker-side bytes are the topic size in `log_B_per_rec`.

## 4. Grafana panels

Dashboard variables: `ds` and the CPU variables `cpu_server` (0|6), `cpu_k6` (3|9|4|10), `cpu_kafka` (1|7|2|8), `cpu_monitoring` (5|11). `[w]` = `$__rate_interval`; job filters omitted (`eventpulse-nodejs` for server metrics, `node` for node_exporter). Server series have no config label: read the steps over time against `runs.tsv`. k6 panels split by `testid` (`k2-<config>-n<repeat>`).

| #  | Panel | Query | What it shows |
| -- | ----- | ----- | ------------- |
| 1  | Accepted rate (202) per run | `sum by (testid)(rate(k6_k2_resp_202_total[w]))` | rate k6 got 202 for |
| 2  | Latency seen by k6 | `histogram_quantile(0.5 / 0.99, sum by (testid)(rate(k6_http_req_duration_seconds[w])))` | client latency per run |
| 3  | Latency seen by the server | `histogram_quantile(0.5 / 0.99, sum by (le)(rate(http_request_duration_seconds_bucket{route="/v1/events"}[w])))` | server latency |
| 4  | Average batch size (messages) | `sum(rate(ep_batcher_batch_size_messages_sum[w])) / sum(rate(ep_batcher_batch_size_messages_count[w]))` | messages per send |
| 5  | Flushes per second by reason | `sum by (reason)(rate(ep_batcher_flush_total[w]))` | time vs size vs bytes |
| 6  | Average batch size (bytes) | same with `ep_batcher_batch_size_bytes` | bytes per send before compression |
| 7  | Time in the batcher queue | `histogram_quantile(0.5 / 0.99, sum by (le)(rate(ep_batcher_queue_wait_seconds_bucket[w])))` | waiting for a batch |
| 8  | Time inside Kafka send | same with `ep_kafka_send_seconds_bucket` | ack time |
| 9  | Bytes sent to brokers per second | `rate(ep_kafka_client_tx_bytes_total[1m])` | wire bytes (ADDED BY K2) |
| 10 | Wire bytes per acknowledged message | `rate(ep_kafka_client_tx_bytes_total[1m]) / rate(ep_kafka_messages_acked_total[1m])` | compression ratio view |
| 11 | Broker round trip per broker | `max by (broker)(ep_kafka_broker_rtt_seconds{quantile="0.99"})` | slow broker |
| 12 | Node process CPU | `rate(process_cpu_user_seconds_total[w])`, `rate(process_cpu_system_seconds_total[w])` | process CPU, max 2.0 |
| 13 | Server CPU per accepted request | `(rate(process_cpu_user_seconds_total[1m]) + rate(process_cpu_system_seconds_total[1m])) / rate(ep_kafka_messages_acked_total[1m])` | cost of one request |
| 14 | Server cores busy | `1 - rate(node_cpu_seconds_total{mode="idle",cpu=~"$cpu_server"}[w])` | CPU 0 and 6 |
| 15 | Broker cores busy | same with `$cpu_kafka` | broker CPU |
| 16 | Disk bytes written | `rate(node_disk_written_bytes_total{device="nvme0n1"}[w])` | host disk writes |
| 17 | Event loop lag | `max(nodejs_eventloop_lag_p99_seconds)` | main thread saturation |
| 18 | k6 dropped iterations | `sum by (testid)(rate(k6_dropped_iterations_total[w]))` | must be 0 |
| 19 | Generator cores busy | same idle query with `$cpu_k6` | keep under 70% |
| 20 | Shedding | `sum by (reason)(rate(ep_http_503_total[w]))`, `sum by (reason)(rate(ep_batcher_rejected_total[w]))` | 503s and rejects |
| 21 | Monitoring cores busy | same idle query with `$cpu_monitoring` | disturbance |

Verify pair for Grafana Explore after the first run:

```promql
count by (testid)(k6_k2_resp_202_total)
rate(ep_kafka_client_tx_bytes_total[1m])
```

## 5. Run it

```bash
cd loadtests/k6/stage-2/K2
DRY_RUN=1 ./run.sh                                                              # (a) plan only
CONFIGS="base w20_s50 lz4" REPEATS=1 RESULTS_NAME=k2-trial ./run.sh             # (b) trial: never quote its numbers
./run.sh                                                                        # (c) the full sweep, about 48 min
RATE=3000 RESULTS_NAME=k2-r3000 ./run.sh                                        # (c') the same sweep at another rate
PAYLOAD_MODE=x CONFIGS="base gzip zstd" REPEATS=1 RESULTS_NAME=k2-xpad ./run.sh # (e) K1 filler: unrealistic best case
```

One case by hand (server already started with the knobs you want; `RATE`, `DURATION`, `PAYLOAD_MODE` and `API_KEY` reach k6):

```bash
cd loadtests/k6 && RESULTS_ROOT=$PWD/stage-2/K2/results RESULTS_NAME=k2-manual LABEL=k2-manual SCRIPT=stage-2/K2/script.js \
  RATE=4000 DURATION=30s API_KEY=<key> ROUTE=/healthz ./runner.sh
```

Watch from another terminal: `watch -n 5 'curl -s localhost:8000/debug/stats'` and `docker stats ep-kafka1 ep-kafka2 ep-kafka3`
(this costs CPU: do not run it during a measured sweep).

Results: `results/<RESULTS_NAME>/` holds `summary.tsv` (one row per run), `runs.tsv` (start and end epoch of every
run), `env.txt` (versions, every config's knobs, pinning, git commit), `captures/` (server counters before and after each
run), `server.out` and the k6 lap folders. Show the table again with `python3 loadtests/k6/analyze.py table results/<name>/summary.tsv`.

## 6. What this script changes on my machine

- `docker update --cpuset-cpus` on the three brokers, Postgres, Prometheus, Grafana and node_exporter; the old cpusets are restored when the script ends (`KEEP_PINNING=1` leaves them).
- Deletes and recreates the topic `events.raw` before every run, so its content is lost.
- Starts and stops `ingest-api` (kill of its own pid only).
- No `sudo`, no sysctl, no `tc`.

## 7. Analyze in Grafana

Import `infra/observability/dashboards/stage-2/K2 - Producer settings sweep.json` (Dashboards, New, Import; pick the
Prometheus datasource for `ds`). Read the rows in order: 1 the verdict (rate, latency), 2 the mechanism (batch size,
flush reasons, queue wait, send time, wire bytes), 3 resource view (where the CPU goes), 4 validity checks (dropped
iterations must be 0, generator under 70%, no 503). Zoom to the epochs in `runs.tsv`; `summary.tsv` has the per-run numbers.
