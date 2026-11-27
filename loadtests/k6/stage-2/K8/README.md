# K8 Rebalances and group stability

Why this experiment and what I expect: see notebook/stage-2/experiments/K8/Explanation.md

## 1. Run the server

`run.sh` starts everything it needs itself, so there is nothing to start by hand except the stack:

- Docker stack up: 3 Kafka brokers (`ep-kafka1..3`), Postgres (`ep-postgres`), Prometheus, Grafana, node_exporter, kafka-exporter.
- Nothing listening on :8000 (ingest-api) or :8001 to :8003 (consumer-sinks). The preflight stops with a message if something is.
- Both services built: `pnpm --filter @eventpulse/consumer-sink build` and `pnpm --filter @eventpulse/ingest-api build`. The consumer-sink build must include the K8 change (the preflight greps `dist/` for it).

What `run.sh` starts, per run: the ingest-api (`node dist/main.js` under `taskset`) and 1 to 3 consumer-sinks (`node dist/main.js` under `taskset`, ports 8001 to 8003, one new consumer group per run).

CPU pinning (the same plan as K5, one more consumer):

| What | CPUs |
|---|---|
| ingest-api | 0,6 |
| consumer-sink 1, 2, 3 | 1, 7, 1 (the third one shares CPU 1 with the first) |
| Kafka brokers | 2,8,3,9 |
| k6 | 4,10 |
| Prometheus, Grafana, exporters, Postgres | 5,11 |

The container cpusets are changed with `docker update` and put back when the script exits.

## 2. Run configuration

Variable: what happens to the group (4 scenarios) x how the group handles it (2 assignors). 8 cells, 3 repeats, 60 s of load at 2000 req/s each, about 52 min in total.

| Scenario | Consumers before the load | At `EVENT_AT_S` into the load |
|---|---|---|
| s1 scale out | 1 | consumers 2 and 3 start |
| s2 scale in | 3 | consumer 3 gets SIGTERM (it leaves the group) |
| s3 zombie | 2 | consumer 2 gets SIGSTOP (frozen, no heartbeats); `ZOMBIE_HOLD_S` later it gets SIGCONT |
| s4 slow | 2 | nothing from the script: consumer 2 has `SLOW_MS` of extra work in `SLOW_CHUNKS` chunks, after `SLOW_SKIP` normal chunks |

Assignor: `rr` = roundRobin (eager), `coop` = cooperativeSticky (incremental). Cell names are `<scenario>-<assignor>`, e.g. `s3-coop`.

Same in every cell: topic `k8-events` (6 partitions, RF 3, min ISR 2, recreated before every run); ingest-api with acks=all, idempotent; consumers read one partition at a time in chunks of up to 1000 rows; session timeout 6000 ms, heartbeat 1000 ms, rebalance timeout 8000 ms; `SINK_DEDUP=false` (plain INSERT into `events_nodedup`, so a row that is processed twice is stored twice).

Repeat 1 and 3 run the cells in the order above, repeat 2 runs them in reverse.

| Env knob | Default | Meaning |
|---|---|---|
| `CELLS` | all 8 | which cells to run |
| `REPEATS` | 3 | repeats of each cell |
| `RATE` | 2000 | req/s |
| `DURATION_S` | 60 | seconds of load |
| `EVENT_AT_S` | 20 | seconds into the load when the scenario acts |
| `ZOMBIE_HOLD_S` | 15 | s3: seconds the consumer stays frozen |
| `SLOW_MS` / `SLOW_CHUNKS` / `SLOW_SKIP` | 12000 / 2 / 40 | s4: extra work per slow chunk, how many chunks, normal chunks before them |
| `SESSION_TIMEOUT_MS` / `HEARTBEAT_MS` / `REBALANCE_TIMEOUT_MS` | 6000 / 1000 / 8000 | group timing |
| `PARTITIONS` | 6 | partitions of the topic |
| `RESULTS_NAME` | `k8-rebalances` | folder under `results/` |
| `DRY_RUN` | 0 | 1 prints the plan only |

## 3. Instrumentation needed

prom-client defaults used: `process_cpu_seconds_total`, `nodejs_eventloop_lag_seconds`.

Custom server metrics used (consumer-sink, job `eventpulse-consumer-sink`):
`ep_consumer_messages_consumed_total`, `ep_consumer_assigned_partitions`, `ep_consumer_rebalances_total`, `ep_consumer_commit_errors_total`, `ep_consumer_batch_processing_seconds`, `ep_event_freshness_seconds`, `ep_pg_pool_waiting`.
Custom server metrics used (ingest-api, job `eventpulse-nodejs`): `ep_kafka_messages_failed_total`.

ADDED BY K8 (all in `services/consumer-sink/src/observability/sink-stats.ts`, fed from `onRebalance` in `src/consumer/sink-consumer.ts`):
- `ep_consumer_revokes_total` (counter): revoke callbacks received.
- `ep_consumer_partitions_revoked_total` (counter): partitions taken away from this member.
- `ep_consumer_rebalance_gap_seconds` (histogram): time from a revoke callback to the next assign callback of this member.

Also ADDED BY K8 in the consumer-sink: `SINK_PROCESSING_DELAY_CHUNKS` and `SINK_PROCESSING_DELAY_SKIP_CHUNKS` (config.ts, batch-handler.ts, sink-consumer.ts), which limit the existing `SINK_PROCESSING_DELAY_MS` to a window of chunks. Default 0 = old behaviour (every chunk).

ADDED BY K8 in this folder: `ep_k8_phase` (gauge, textfile written by `run.sh` into `infra/compose/textfile/ep_k8.prom`, removed on exit).

k6 metrics used: `k6_http_reqs_total`, `k6_http_req_duration_seconds` (native histogram), `k6_dropped_iterations_total`, `k6_resp_202_total` (counted by `script.js` for the analyzer).

kafka-exporter metrics used: `kafka_consumergroup_members`, `kafka_consumergroup_lag_sum`.

node_exporter metrics used: `node_cpu_seconds_total`.

## 4. Grafana panels

`[w]` = `$__rate_interval`. Job filters omitted except where they matter. "Consumers" = `job="eventpulse-consumer-sink",instance=~".*:800[1-3]"`.

| # | Panel | Query | What it shows |
|---|---|---|---|
| 1.1 | Rows read per second by the whole group | `sum(rate(ep_consumer_messages_consumed_total[w]))` | pauses of the whole group |
| 1.2 | Group lag | `sum(kafka_consumergroup_lag_sum{consumergroup=~"k8-.*",topic="k8-events"})` | backlog during and after a pause |
| 1.3 | Ingest p99 latency | `histogram_quantile(0.99, sum(rate(k6_http_req_duration_seconds[w])))` | API unaffected by consumers |
| 1.4 | Accepted vs offered | `sum(rate(k6_http_reqs_total{status="202"}[w]))` and all answers | API shed nothing |
| 1.5 | Event freshness p99 | `histogram_quantile(0.99, sum by (le)(rate(ep_event_freshness_seconds_bucket[w])))` | how late rows become visible |
| 1.6 | Run phase | `max(ep_k8_phase)` | 0 steady, 1 scenario acting, 2 after |
| 2.1 | Group members | `max(kafka_consumergroup_members{consumergroup=~"k8-.*"})` | joins, leaves, session expiry |
| 2.2 | Partitions owned per consumer | `ep_consumer_assigned_partitions{Consumers}` | who owns what |
| 2.3 | Revoke callbacks per consumer | `ep_consumer_revokes_total{Consumers}` | rebalances seen by each member |
| 2.4 | Partitions revoked per consumer | `ep_consumer_partitions_revoked_total{Consumers}` | eager vs cooperative |
| 2.5 | Revoke to assign gap | `ep_consumer_rebalance_gap_seconds_sum{Consumers}` | time inside a member between callbacks |
| 2.6 | Rows read per second by consumer | `sum by (instance)(rate(ep_consumer_messages_consumed_total{Consumers}[w]))` | who is reading |
| 2.7 | Chunk processing p99 | `histogram_quantile(0.99, sum by (le)(rate(ep_consumer_batch_processing_seconds_bucket{Consumers}[w])))` | the slow chunk of s4 |
| 2.8 | Offset commit errors | `ep_consumer_commit_errors_total{Consumers}` | commits lost to a rebalance |
| 2.9 | Assign callbacks per consumer | `ep_consumer_rebalances_total{Consumers}` | group joins |
| 3.1 | Consumer CPU (pinned cores) | `1 - avg(rate(node_cpu_seconds_total{mode="idle",cpu=~"$cpu_consumers"}[w]))` | CPU on the consumer cores |
| 3.2 | Process CPU per consumer | `sum by (instance)(rate(process_cpu_seconds_total{Consumers}[w]))` | CPU per process |
| 3.3 | Broker cores busy | same with `$cpu_brokers` | broker load |
| 3.4 | Consumer event-loop lag | `nodejs_eventloop_lag_seconds{Consumers}` | starved or frozen process |
| 3.5 | Postgres pool waiting | `ep_pg_pool_waiting{Consumers}` | DB connection waits |
| 4.1 | k6 dropped iterations | `sum(rate(k6_dropped_iterations_total[w]))` | must be 0 |
| 4.2 | k6 non-202 answers | `sum(rate(k6_http_reqs_total{status!="202"}[w]))` | must be 0 |
| 4.3 | k6 cores busy | same CPU form with `$cpu_k6` | generator headroom |
| 4.4 | API cores busy | same with `$cpu_server` | API headroom |
| 4.5 | Monitoring and Postgres cores busy | same with `$cpu_monitoring` | shared cores |
| 4.6 | API failed Kafka publishes | `sum(rate(ep_kafka_messages_failed_total[w]))` | must be 0 |

Verify in Grafana Explore after the first run: `count by (instance)(ep_consumer_revokes_total)` and `max(ep_k8_phase)`.

## 5. Run it

All commands from `loadtests/k6/stage-2/K8/`.

(a) Dry run (prints the plan, runs nothing):

```bash
DRY_RUN=1 ./run.sh
```

(b) One-cell trial (plumbing only, never quote its numbers):

```bash
CELLS="s1-coop" REPEATS=1 RESULTS_NAME=k8-trial ./run.sh
```

(c) Full matrix, one command:

```bash
./run.sh
```

(d) One load by hand through the shared runner (no consumers, no scenario, only the k6 part):

```bash
cd ../.. && RUN_ID=a8010100 CELL=manual RATE=2000 DURATION=60s API_KEY=<key> BASE_URL=http://localhost:8000 ROUTE=/healthz SCRIPT=stage-2/K8/script.js RESULTS_NAME=k8-manual LABEL=k8-manual ./runner.sh
```

(e) Watch from another terminal while it runs:

```bash
watch -n 1 'docker exec ep-kafka1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka1:19092 --describe --group "$(docker exec ep-kafka1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka1:19092 --list | grep k8- | head -1)"'
```

Results go to `results/<RESULTS_NAME>/`: `summary.tsv` (one row per run), `runs.tsv` (start and end epoch of each run, the scenario event and the s3 resume), `env.txt`, `captures/` (server and consumer logs, `<testid>.rows.txt` = rows in Postgres sampled every 0.3 s, `<testid>.sinkN.metrics` = consumer counters copied before the process stopped) and the k6 lap JSONs in a timestamped subfolder.

Columns added to `summary.tsv` for K8: `scenario`, `assignor`, `acked` (202 answers), `rows_total`, `missing` (acked minus distinct rows), `dup_rows` (rows stored twice), `max_gap_s` (longest time the group read nothing from the first row to the end of the load), `gaps_gt2s`, `drain_s` (seconds from the end of the load to lag 0), `assigns`, `revokes`, `parts_revoked`, `cb_gap_s`, `commit_err` (sums over the consumers), `status`.

## 6. What this script changes on my machine

- Starts and stops the ingest-api and up to 3 consumer-sinks. It sends them SIGTERM, SIGSTOP, SIGCONT and, if a process does not exit within 10 s of SIGTERM, SIGKILL. Only processes the script started.
- `docker update --cpuset-cpus` on the Kafka, Postgres and monitoring containers (the old values are restored on exit, also on Ctrl+C).
- Postgres: creates `events_nodedup` and an index on it if missing (dropped on exit if the script created it); deletes each run's rows from it after the audit.
- Kafka: deletes and recreates topic `k8-events` before every run; deletes the `k8-*` consumer groups after every run.
- Writes `infra/compose/textfile/ep_k8.prom` (removed on exit).
- No sudo, no sysctl, no tc.
- If the script is interrupted, check for leftover node processes on :8000 to :8003 and run the cleanup by hand: `pkill -f 'node --env-file=.env dist/main.js'` only if nothing else of mine runs that command.

## 7. Analyze in Grafana

Dashboard: [K8 - Rebalances and group stability.json](../../../../infra/observability/dashboards/stage-2/K8%20-%20Rebalances%20and%20group%20stability.json)

Import: Grafana, Dashboards, New, Import, upload the JSON, pick the Prometheus datasource in the `ds` variable. Set the time range from the epochs in `runs.tsv`.

Reading order: row 4 first (is the run valid), then row 1 (did the group stop reading), then row 2 (which rebalance caused it and what it revoked), then row 3 (was CPU involved). Compare with the numbers in `summary.tsv` (`python3 ../../analyze.py table results/<RESULTS_NAME>/summary.tsv`).
