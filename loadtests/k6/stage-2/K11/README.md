# K11 Retention and lag

Why this experiment and what I expect: see `notebook/stage-2/experiments/K11/Explanation.md`.

## 1. Run the server

`run.sh` starts and stops the ingest-api and the consumer-sink itself, so there is nothing to start by hand. Nothing may listen on port 8000 (API) or 8001 (consumer metrics) when I start; the preflight checks this.

The commands it runs (same style as K5):

```
cd services/ingest-api    && env KAFKA_BROKERS=localhost:9092,localhost:9094,localhost:9096 KAFKA_TOPIC=k11-events <API settings> \
  taskset -c 0,6 node --env-file=.env dist/main.js
cd services/consumer-sink && env KAFKA_BROKERS=... CONSUMER_TOPIC=k11-events CONSUMER_GROUP_ID=k11-<testid>-<pid> METRICS_PORT=8001 <sink settings> \
  CONSUMER_FROM_BEGINNING=<true|false> taskset -c 1,7 node --env-file=.env dist/main.js
```

Both `dist/main.js` files must be built (`pnpm build` in each service). The settings come from `run.sh`, not from my shell and not from `.env` (except the API key and the database URL).

CPU pinning (same plan as K5):

| Group | CPUs |
|---|---|
| ingest-api | 0,6 |
| consumer-sink | 1,7 |
| Kafka brokers (containers, moved with `docker update`) | 2,8,3,9 |
| k6 (`K6_CPUS`) | 4,10 |
| Prometheus, Grafana, node_exporter, kafka-exporter, Postgres | 5,11 |

## 2. Run configuration

The variable is what the consumer does when its committed offset is no longer in the topic (the reset policy), plus a control with a short outage. The load is the same in every run.

| Variant | Outage | Consumer #2 reset policy (`CONSUMER_FROM_BEGINNING`) |
|---|---|---|
| `earliest` | `OUTAGE_S` = 660 s (longer than retention) | `true` (`auto.offset.reset=earliest`) |
| `latest` | `OUTAGE_S` = 660 s | `false` (`auto.offset.reset=latest`) |
| `control` | `CONTROL_OUTAGE_S` = 60 s (shorter than retention) | `true` |

Topic `k11-events`: 6 partitions, replication factor 3, `min.insync.replicas=2`, `retention.ms=180000` (3 min), `segment.ms=20000` (20 s). Set on this topic only, deleted and recreated before every run. The brokers' own settings are not touched.

Timetable of one run (seconds after the load started):

| t | What happens |
|---|---|
| 0 | consumer #1 is already running (new group, earliest). k6 starts: `main` stream, 1000 req/s for 60 s |
| 15 | consumer #1 is stopped with SIGTERM. Its committed offsets stay in Kafka. The load continues |
| 60 | `main` ends, the `trickle` stream starts: 20 req/s until the restart (it keeps the segments rolling, see Learnings) |
| 15 + outage | offsets are written down. For `earliest` and `latest` the script first waits until the log start offset is past the committed offset on every partition (at most `WAIT_DELETE_S` extra). Then consumer #2 starts with the same group id |
| after | wait until the group lag is 0, or until nothing new was consumed for `QUIET_S`. Then the audit in Postgres |

3 variants x 3 repeats = 9 measured runs. Repeat 2 runs the variants in reverse order. The API is restarted before every run. Total about 90 to 105 minutes (the `earliest` and `latest` runs take about 13 min each, the control about 4 min).

| Env knob | Default | Meaning |
|---|---|---|
| `RATE` | 1000 | requests per second of the main stream (open model) |
| `LOAD_S` | 60 | length of the main stream |
| `STOP_AT_S` | 15 | when consumer #1 is stopped |
| `OUTAGE_S` | 660 | outage of earliest and latest (planned; retention 180 s + one broker check up to 300 s + margin) |
| `CONTROL_OUTAGE_S` | 60 | outage of the control |
| `TRICKLE_RATE` | 20 | req/s of the trickle stream |
| `RETENTION_MS`, `SEGMENT_MS` | 180000, 20000 | topic settings |
| `WAIT_DELETE_S` | 420 | longest extra wait for the deletion after the planned outage |
| `QUIET_S` | 20 | consumer #2 counts as settled after this long without new records |
| `VARIANTS` | `earliest latest control` | which variants to run |
| `REPEATS` | 3 | repeats per variant |
| `RESULTS_NAME` | `k11-retention-lag` | folder name under `results/` |
| `PARTITIONS`, `TOPIC`, `SESSION_TIMEOUT_MS`, `SINK_PORT`, `SERVER_CPUS`, `CONSUMER_CPUS`, `KAFKA_CPUS`, `K6_CPUS`, `MON_CPUS`, `KEEP_PINNING`, `DRY_RUN` | see `run.sh` | |

## 3. Instrumentation needed

No code was added to the services for K11.

- prom-client defaults used: `process_cpu_seconds_total`.
- Custom server metrics used: `eventpulse_event_loop_delay_seconds` (API), `ep_consumer_messages_consumed_total`, `ep_consumer_rows_inserted_total`, `ep_pg_pool_waiting` (consumer-sink).
- kafka-exporter metrics used: `kafka_topic_partition_oldest_offset` (log start), `kafka_topic_partition_current_offset` (log end), `kafka_consumergroup_current_offset` (committed offset), `kafka_consumergroup_lag`, `kafka_consumergroup_members`, `kafka_topic_partition_under_replicated_partition`.
- k6 metrics used: `k6_resp_202_total`, `k6_http_reqs_total` (label `status`), `k6_http_req_duration_seconds` (native histogram), `k6_dropped_iterations_total`. In the k6 summary export: `resp_202`, `resp_503`, `resp_other`, also split by the tag `seg` (`main`, `trickle`).
- node_exporter metrics used: `node_cpu_seconds_total`.
- Phase gauge `ep_k11_phase{testid,variant}` (0 consumer running, 1 consumer stopped, 2 consumer restarted, -1 no run): `ADDED BY K11`, written by `run.sh` to `infra/compose/textfile/ep_k11.prom` and removed on exit. It exists in Prometheus only after the first run.
- The consumer does not count or log an offset reset by itself. The reset is seen through the offsets (log start, committed) and through the rows, which is also what `run.sh` records.

## 4. Grafana panels

`[w]` = `$__rate_interval`, job filters omitted.

| # | Panel | Query | What it shows |
|---|---|---|---|
| 1 | Acknowledged vs inserted per second | `sum(rate(k6_resp_202_total[w]))`, `sum(rate(ep_consumer_rows_inserted_total[w]))` | what the API promised and what reached Postgres |
| 2 | Group lag vs readable records | `sum(kafka_consumergroup_lag{topic="k11-events"})`, `sum(kafka_topic_partition_current_offset{...}) - sum(kafka_topic_partition_oldest_offset{...})` | lag as reported against what can still be read |
| 3 | API latency p50 / p99 | `histogram_quantile(0.99, sum(rate(k6_http_req_duration_seconds[w])))` | the API should not notice the consumer |
| 4 | Group members | `sum(kafka_consumergroup_members{consumergroup=~"k11-.*"})` | consumer present or not |
| 5 | Log start offset per partition | `kafka_topic_partition_oldest_offset{topic="k11-events"}` | when the broker deletes |
| 6 | Committed offset per partition | `max by (partition)(kafka_consumergroup_current_offset{topic="k11-events"})` | where the group would continue |
| 7 | Records deleted before anyone read them | `sum(clamp_min(oldest - on(partition) max by (partition)(committed), 0))` | the loss caused by retention |
| 8 | Consumer records per second | `sum(rate(ep_consumer_messages_consumed_total[w]))` | reading after the restart |
| 9 | Run phase | `max by (variant)(ep_k11_phase)` | where in the timetable |
| 10 | API and consumer CPU | `rate(process_cpu_seconds_total[w])` | resource view |
| 11 | Host CPU busy by pinned group | `node_cpu_seconds_total` idle, `cpu=~"$cpu_brokers"` etc. | host view |
| 12 | k6 dropped iterations | `rate(k6_dropped_iterations_total[w])` | validity (must be 0) |
| 13 | k6 requests not 202 | `rate(k6_http_reqs_total{status!="202"}[w])` | validity |
| 14 | Generator CPU busy | same as 11 with `$cpu_k6` | generator headroom |
| 15 | Event-loop delay | `eventpulse_event_loop_delay_seconds{quantile="0.99"}` | API stalls |
| 16 | Under-replicated partitions | `kafka_topic_partition_under_replicated_partition{topic="k11-events"}` | cluster stayed healthy |
| 17 | Postgres pool waiting | `ep_pg_pool_waiting` | consumer not limited by the pool |

Verify in Grafana Explore: `count by (variant)(ep_k11_phase)` and `kafka_topic_partition_oldest_offset{topic="k11-events"}`.

## 5. Run it

(a) Dry run, prints the plan and changes nothing:

```bash
cd loadtests/k6/stage-2/K11 && DRY_RUN=1 ./run.sh
```

(b) Trial, the control only, one repeat (plumbing check, about 4 minutes, never quoted):

```bash
cd loadtests/k6/stage-2/K11 && VARIANTS="control" REPEATS=1 RESULTS_NAME=k11-trial ./run.sh
```

(c) Full experiment:

```bash
cd loadtests/k6/stage-2/K11 && ./run.sh
```

(d) Watch from another terminal: `tail -f results/k11-retention-lag/runs.tsv` and `docker exec ep-kafka1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka1:19092 --topic k11-events --time -2` (the log start offset of every partition).

Results go to `results/<RESULTS_NAME>/`: `summary.tsv` (one row per run), `runs.tsv` (epochs of every outage and run, for zooming Grafana), `env.txt`, `captures/` (server and consumer output, and per run three offset files `*.offsets.at-stop.tsv`, `*.offsets.at-restart.tsv`, `*.offsets.after.tsv` with columns partition, log start, log end, committed), and the k6 lap JSONs.

Extra columns of `summary.tsv`: `acked`, `rows_total`, `missing` (acked minus distinct rows), `deleted_unread`, `retained_unread`, `consumed2`, `lag_after`, `oor_log_lines`, `settled` (`lag0` or `quiet`), `extra_wait_s`, `status`.

## 6. What this script changes on my machine

- Deletes and recreates the topic `k11-events` before every run, with short retention on that topic only.
- Starts the ingest-api and the consumer-sink and stops them with `kill` (SIGTERM) of their own pids. Stopping consumer #1 is part of the experiment.
- `docker update --cpuset-cpus` on the brokers, Postgres and the monitoring containers; the old values are restored on exit.
- Deletes the rows of each run from `events` (by run id prefix) and deletes the consumer groups `k11-*`.
- Writes `infra/compose/textfile/ep_k11.prom` and removes it on exit.
- No sudo, no sysctl, no tc, no change to the broker configuration.

## 7. Analyze in Grafana

Dashboard: [K11 - Retention and lag.json](../../../../infra/observability/dashboards/stage-2/K11%20-%20Retention%20and%20lag.json). Import it through Dashboards > New > Import and pick the Prometheus datasource. Zoom to a run with the epochs in `runs.tsv`. Read the rows in order: 1 verdict, 2 mechanism (log start against committed offset), 3 resources, 4 validity (dropped iterations must be 0 and no under-replicated partitions before I trust a run). k6 series are not marked stale after a run, so the last values can stay on screen for minutes.
