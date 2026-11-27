# K3 Partitions versus consumers

Why this experiment and what I expect: see `notebook/stage-2/experiments/K3/Explanation.md`

## 1. Run the servers

`run.sh` starts and stops everything it needs by itself:

- **ingest-api**, only during the prefill (one run per partition count), inside `services/ingest-api`:

```bash
KAFKA_BROKERS=localhost:9092,localhost:9094,localhost:9096 KAFKA_TOPIC=k3-p<P> KAFKA_KEY_MODE=event <K2 baseline knobs> \
  taskset -c 0,6 node --env-file=.env dist/main.js
```

- **consumer-sink**, N processes per drain, inside `services/consumer-sink` (consumer i gets `METRICS_PORT=8000+i`):

```bash
CONSUMER_TOPIC=k3-p<P> CONSUMER_GROUP_ID=<new for every run> METRICS_PORT=800<i> KAFKA_CLIENT_ID=k3-c<i> \
  SINK_PROCESSING_DELAY_MS=<0 or 200> CONSUMER_FROM_BEGINNING=true CONSUMER_ASSIGNOR=roundRobin \
  CONSUMER_PARTITIONS_CONCURRENTLY=1 CONSUMER_MAX_BATCH_SIZE=1000 SINK_MAX_ROWS=1000 PG_POOL_MAX=5 LAG_POLL_MS=1000 \
  taskset -c 0,6,3,9,4,10 node --env-file=.env dist/main.js
```

Every consumer and API knob that is not the variable is written out for every process, so what is exported in my shell
or written in `.env` does not matter (the script unsets them first). Nothing may listen on :8000 or :8001-8012 before
the start (preflight refuses to run otherwise).

Before the first run:

```bash
cd services/ingest-api && pnpm build
cd services/consumer-sink && pnpm build
docker compose -f infra/compose/docker-compose.yml up -d          # prometheus, grafana, node_exporter, postgres
docker compose -f infra/compose/docker-compose.kafka.yml up -d    # 3 brokers + topics
```

Prometheus must scrape the consumer ports 8001-8012 (`infra/compose/prometheus-config.yml`, job
`eventpulse-consumer-sink`, changed by K3; the Prometheus container has to be restarted to pick up an edited file).

CPU plan (12 logical CPUs = 6 cores, SMT pairs 0/6 1/7 2/8 3/9 4/10 5/11). The prefill uses the K2 plan and the drains
use the plan of `docker-compose`:

| Component | Prefill | Drain | How it is pinned |
| --------- | ------- | ----- | ---------------- |
| ingest-api | 0,6 | not running | `taskset` in run.sh |
| consumer-sink (all N share these) | not running | 0,6,3,9,4,10 | `taskset` in run.sh |
| Kafka brokers (3, 1 CPU quota each) | 1,7,2,8 | 3,9,4,10 | `docker update --cpuset-cpus` before the prefill, restored to the compose value before the drains |
| Postgres | 5,11 (idle) | 1,7,2,8 (quota 2 CPUs) | the same |
| k6 | 3,9,4,10 | not running | `K6_CPUS` read by `runner.sh` |
| Monitoring | 5,11 | 5,11 | compose |

Facts recorded in `env.txt`: kernel, node, k6, broker image and limits, the cpuset of every container, Postgres version
and settings, topic record counts, consumer knobs, git commit and dirty count.

## 2. Run configuration

Variable 1: partitions P of the topic (1, 3, 6, 12). Variable 2: consumers N in one group. Everything else is fixed. The
load is a **fixed backlog**, not a rate: about 300,000 events of about 500 B per topic, written once.

| Sweep | Command | Consumers N | `SINK_PROCESSING_DELAY_MS` | Cells | Rough time |
| ----- | ------- | ----------- | -------------------------- | ----- | ---------- |
| `pg` | `./run.sh pg` | 1 2 3 4 6 8 12 | 0 | 4 x 7 = 28 | about 90 min |
| `delay` | `./run.sh delay` | 1 3 6 12 | 200 | 4 x 4 = 16 | about 80 min |
| both | `./run.sh` | | | 44 | about 2.7 h |

Each cell is repeated 3 times. Repeat is the outer loop; repeat 1 runs the cells in table order, repeat 2 in reverse
order, repeat 3 in table order again.

Prefill (once per partition count, a topic that already holds at least `MIN_BACKLOG` records with the right partition
count is reused): delete and create topic `k3-p<P>` (RF 3, `min.insync.replicas=2`, retention 7 days), start ingest-api
with `KAFKA_TOPIC=k3-p<P>`, run `loadtests/k6/stage-2/ingest-kafka.js` through `runner.sh` (4,000 req/s for 75 s,
payload about 500 B, run id `a3a30001` / `a3a30003` / `a3a30006` / `a3a30012` for P = 1 / 3 / 6 / 12), wait 6 s, stop the API.
Every event id starts with the run id of its topic.

Drain (one cell): delete the rows of that topic from Postgres (a range on `event_id`, so only rows with that run id),
`VACUUM events`, start N consumers with a new group id (reading from the beginning), watch them (below) until the
messages consumed of the group reach the number of records in the topic, stop the consumers (SIGTERM), count the rows of
the topic in Postgres, delete the group.

What the watcher does (inside `run.sh`, a short Python block): every 0.5 s it reads `/metrics` of every consumer and
writes one line to `captures/<testid>.samples.tsv`. "Stable" is the first sample where every consumer answers, the group
owns all P partitions and the rebalance counter did not move for 1 s. All numbers come from the consumers' own counters,
so the watching puts no load on Postgres.

Audit: rows of the topic in Postgres must equal the records in the topic, and duplicates and DLQ must be 0.

Columns of `summary.tsv` (one row per drain; `analyze.py` is not used here because there is no k6 run to analyze):

| Column | Meaning |
| ------ | ------- |
| `rate_window` | rows inserted between "stable" and "drained", divided by that time (rows/s). The headline number |
| `rate_all` | all rows divided by the time since the first row, startup and rebalances included |
| `window_s` | length of the stable window in seconds |
| `t_stable_s`, `t_end_s` | seconds from the start of the consumers to "stable" and to "drained" |
| `pre_stable_pct` | share of the rows that were inserted before "stable" |
| `idle_consumers` | consumers that own 0 partitions at the end |
| `assigned_per_consumer` | partitions owned by each consumer at the end, in port order (`2,2,1,1`) |
| `rebalances`, `late_rebalances` | rebalances in the whole run, and those after "stable" |
| `audit`, `status` | `ok` or the problem (`MISMATCH(<rows>)`, `+dup/dlq`), `ok` / `TIMEOUT` / `FAILED` |

| Env knob | Default | Meaning |
| --- | --- | --- |
| `REPEATS` | 3 | repeats |
| `PARTS` | `1 3 6 12` | partition counts |
| `CONSUMERS_PG` / `CONSUMERS_DELAY` | `1 2 3 4 6 8 12` / `1 3 6 12` | consumer counts of each sweep |
| `DELAY_MS` | 200 | `SINK_PROCESSING_DELAY_MS` of the delay sweep |
| `PREFILL` | auto | `auto` reuse a big enough topic, `always` fill again, `never` fail if the topic is missing |
| `PREFILL_RATE` / `PREFILL_DURATION` / `MIN_BACKLOG` | 4000 / 75s / 250000 | size of the backlog |
| `DRAIN_TIMEOUT_S` | 400 | a cell not drained by then gets `TIMEOUT` |
| `CONSUMER_CPUS` / `SERVER_CPUS` / `KAFKA_CPUS` / `K6_CPUS` / `MON_CPUS` | see the CPU plan | pinning |
| `RESULTS_NAME` | `k3-sweep` | folder under `results/` |
| `PG_CONTAINER`, `PG_USER`, `PG_DB` | `ep-postgres`, `eventpulse`, `eventpulse` | used for `psql` through `docker exec` |
| `DRY_RUN` | 0 | 1 = print the plan only |
| `KEEP_PINNING` | 0 | 1 = leave the new cpusets in place |

## 3. Instrumentation needed

No code was changed for K3: everything the panels use already exists.

- **prom-client defaults used:** `process_cpu_user_seconds_total`, `process_cpu_system_seconds_total`, `nodejs_eventloop_lag_p99_seconds`.
- **Custom consumer-sink metrics used** (`services/consumer-sink/src/observability/sink-stats.ts`): `ep_consumer_rows_inserted_total`, `ep_consumer_messages_consumed_total`, `ep_consumer_lag{partition}`, `ep_consumer_assigned_partitions`, `ep_consumer_rebalances_total`, `ep_consumer_batch_size_messages{kind}`, `ep_consumer_batch_processing_seconds`, `ep_consumer_db_write_seconds`, `ep_consumer_duplicates_total`, `ep_consumer_dlq_total`, `ep_consumer_chunk_failures_total`, `ep_consumer_commit_errors_total`, `ep_consumer_paused`, `ep_pg_pool_total`, `ep_pg_pool_waiting`.
- **Other series used:** `up{job="eventpulse-consumer-sink"}`.
- **k6 metrics:** only the prefill uses k6; no K3 panel uses a k6 metric.
- **node_exporter metrics used:** `node_cpu_seconds_total` (per cpu, mode idle).

There is no broker or container exporter for Kafka. Consumer series are per `instance` (the metrics port): 8001 is consumer 1.
`ep_consumer_lag` is the same value on every member, so panels use `max by (partition)`.

## 4. Grafana panels

Dashboard variables: `ds` and the CPU variables `cpu_consumers` (0|6|3|9|4|10), `cpu_postgres` (1|7|2|8), `cpu_kafka` (3|9|4|10).
`[w]` = `$__rate_interval`; job filter `eventpulse-consumer-sink` for consumer series and `node` for node_exporter, omitted below. The
consumer series have no run label: read the steps over time against `runs.tsv`. The CPU variables match the drain part (the
prefill uses another plan).

| #  | Panel | Query | What it shows |
| -- | ----- | ----- | ------------- |
| 1  | Sink rows per second (whole group) | `sum(rate(ep_consumer_rows_inserted_total[w]))` | group throughput |
| 2  | Rows per second per consumer | `rate(ep_consumer_rows_inserted_total[w])` | which consumers work |
| 3  | Lag per partition | `max by (partition)(ep_consumer_lag)` | each partition draining |
| 4  | Total lag | `sum(max by (partition)(ep_consumer_lag))` | backlog left |
| 5  | Assigned partitions per consumer | `ep_consumer_assigned_partitions` | who owns what |
| 6  | Consumers with and without assignments | `count(ep_consumer_assigned_partitions > 0)`, `count(... == 0)` | idle consumers |
| 7  | Rebalances per consumer | `sum(increase(ep_consumer_rebalances_total[w]))` | rebalance bursts |
| 8  | Batch size | `sum(rate(..._sum{kind="batch"}[w])) / sum(rate(..._count{kind="batch"}[w]))` of `ep_consumer_batch_size_messages` | messages per batch |
| 9  | Chunk processing time | `histogram_quantile(0.5 / 0.99, sum by (le)(rate(ep_consumer_batch_processing_seconds_bucket[w])))` | time per chunk |
| 10 | Database write time | same with `ep_consumer_db_write_seconds_bucket` | insert time |
| 11 | Consumer cores busy | `1 - rate(node_cpu_seconds_total{mode="idle",cpu=~"$cpu_consumers"}[w])` | CPU of the shared consumer CPUs |
| 12 | Postgres cores busy | same with `$cpu_postgres` | database CPU |
| 13 | Node process CPU per consumer | `rate(process_cpu_user_seconds_total[w]) + rate(process_cpu_system_seconds_total[w])` | one thread, max 1.0 |
| 14 | Broker cores busy | same as 11 with `$cpu_kafka` | broker CPU |
| 15 | Postgres connections open | `sum(ep_pg_pool_total)`, `sum(ep_pg_pool_waiting)` | pool pressure |
| 16 | Duplicates and DLQ per second | `sum(rate(ep_consumer_duplicates_total[w]))`, `sum(rate(ep_consumer_dlq_total[w]))` | must be 0 |
| 17 | Failed chunks, commit errors, paused | `ep_consumer_chunk_failures_total`, `ep_consumer_commit_errors_total`, `ep_consumer_paused` | must be 0 |
| 18 | Event loop lag p99 per consumer | `nodejs_eventloop_lag_p99_seconds` | CPU saturation of a consumer |
| 19 | Scrape targets up | `sum(up{job="eventpulse-consumer-sink"})` | consumers visible to Prometheus |

Verify (paste in Grafana Explore while consumers run): `count(up{job="eventpulse-consumer-sink"} == 1)` and
`ep_consumer_assigned_partitions`.

## 5. Run it

```bash
cd loadtests/k6/stage-2/K3
DRY_RUN=1 ./run.sh                                                       # (a) the plan, runs nothing
PARTS="3" CONSUMERS_PG="1 3" REPEATS=1 RESULTS_NAME=k3-trial ./run.sh pg # (b) trial: one topic, 2 cells (never quote it)
./run.sh                                                                 # (c) the whole experiment, about 2.7 h
./run.sh pg                                                              # or one sweep at a time
```

Single pieces by hand: the prefill is `loadtests/k6/runner.sh` with `SCRIPT=stage-2/ingest-kafka.js` and `RUN_ID`, `RATE`,
`DURATION`, `API_KEY` (see `prefill_topic` in `run.sh`). A drain needs the consumer processes and the watcher, so it is
easier to run one cell with `PARTS=6 CONSUMERS_PG=4 REPEATS=1 RESULTS_NAME=<name> ./run.sh pg`.

While it runs, in another terminal:

```bash
tail -f loadtests/k6/stage-2/K3/results/k3-sweep/runs.tsv
column -t -s$'\t' loadtests/k6/stage-2/K3/results/k3-sweep/summary.tsv | cut -c1-200
```

Results go to `results/<RESULTS_NAME>/`: `summary.tsv`, `runs.tsv`, `env.txt`, `captures/` (`<testid>.samples.tsv` = the 0.5 s
watcher lines, `<testid>-c<i>.out` = consumer logs), `prefill/` (k6 summaries, `topics.tsv`), `server.out`, and the runner's
`<timestamp>/` folders for the prefill. The consumer logs contain the assignment lines.

## 6. What this script changes on my machine

- No `sudo`, no `sysctl`, no `tc`.
- `docker update --cpuset-cpus` on the three brokers, Postgres, and the monitoring containers for the prefill; restored to the old values before the drains and on exit (also on Ctrl+C). `KEEP_PINNING=1` leaves them.
- Creates, deletes and recreates the topics `k3-p1`, `k3-p3`, `k3-p6`, `k3-p12` (never `events.raw`).
- Deletes rows of Postgres table `events` whose `event_id` starts with `a3a30001`, `a3a30003`, `a3a30006` or `a3a30012` before every drain, and runs `VACUUM events`. Rows with any other id are untouched. The rows of the last drain stay in the table.
- Starts and stops `ingest-api` (prefill) and `consumer-sink` processes; stops them on exit. Deletes each consumer group after its drain.
- `infra/compose/prometheus-config.yml` (not by the script): consumer scrape ports 8004-8012 were added by hand; Prometheus was restarted once.

## 7. Analyze in Grafana

Dashboard: `infra/observability/dashboards/stage-2/K3 - Partitions versus consumers.json`. Import it through the Grafana UI
(Dashboards, New, Import, upload the file) and pick the Prometheus datasource. Zoom to the times in `runs.tsv` (epoch seconds).
Read the rows in order: 1 the verdict (rows per second, lag per partition), 2 the mechanism (assigned partitions, idle
consumers, rebalances), 3 resources (Postgres and consumer CPU), 4 validity (duplicates, DLQ, failures, event loop lag).
