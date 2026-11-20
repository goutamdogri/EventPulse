# B2 Pool exhaustion

I keep the request rate fixed and high, and change only the size of the pg connection pool in `ingest-api`
(`PG_POOL_MAX`: 5, 10, 25, 50, 90 and 120). B1 showed that with `max=10` the API collapsed at about 1,850 req/s.
Now I want to know if a bigger pool fixes that, where more connections stop helping, and what happens when the
pool is bigger than Postgres `max_connections` (100).

## 1. Run the server

`run.sh` starts and stops `ingest-api` by itself (`MANAGE_SERVER=1`, default), once per run, with a different
`PG_POOL_MAX` each time (it overrides the value in `services/ingest-api/.env`). It does **not** use the Stage 0
`server-runner.sh`. After starting, it asks `/debug/stats` for the pool max and stops if it is not the value it asked for.

What it runs (inside `services/ingest-api`):

```bash
PG_POOL_MAX=<level> taskset -c 0,6 node --env-file=.env dist/main.js
```

Before the first run (the B2 server change adds two metrics, so **rebuild**):

```bash
cd services/ingest-api && pnpm build          # dist/main.js must contain ep_db_errors_by_type_total
pnpm migrate:up                               # events table, if not done in B1
cd ../../infra/compose
docker compose up -d postgres-exporter        # new service for B2, see section 3
curl -X POST localhost:9090/-/reload          # Prometheus reads the new scrape job `postgres`
```

CPU plan (12 logical CPUs = 6 cores, SMT pairs 0/6 1/7 2/8 3/9 4/10 5/11), same as B1:

| Component                                    | Logical CPUs | How it is pinned                                          |
| -------------------------------------------- | ------------ | --------------------------------------------------------- |
| ingest-api (Node)                            | 0,6          | `taskset` in run.sh                                       |
| Postgres container                           | 1,7,2,8      | `docker update --cpuset-cpus` (container keeps `cpus: 2`) |
| k6                                           | 3,9,4,10     | `K6_CPUS` env var read by `runner.sh`                     |
| Monitoring (incl. the new postgres-exporter) | 5,11         | `docker update --cpuset-cpus`                             |

The old cpusets are restored when the script ends (`KEEP_PINNING=1` keeps the new ones).

I record in `env.txt`: versions, Postgres settings (`max_connections`, `superuser_reserved_connections`, `shared_buffers`,
`synchronous_commit`, `fsync`...), the number of **usable connection slots**, pinning. I do not tune anything.

## 2. What I am testing

**Hypothesis (from the stage doc):** with `max=10`, requests queue in the pool once concurrent DB work exceeds 10; latency rises by
queue wait, then requests fail on `connectionTimeoutMillis`. More connections help only up to a point, then hurt; the optimum is small
relative to the connection limit.

**Variable:** `PG_POOL_MAX` = 5, 10, 25, 50, 90, 120. **Fixed:** rate 2150 req/s (open model, `constant-arrival-rate`), 60 s per run,
`connectionTimeoutMillis` 2 s, statement timeout 5 s, k6 max 3000 VUs, 10 s request timeout. Nothing else is tuned.

| Item         | Value                                                                                                                                                          |
| ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Runs         | 6 pool sizes x 3 repeats = 18 measured runs of 60 s                                                                                                            |
| Order        | repeat is the outer loop; repeats 1 and 3 go 5 -> 120, repeat 2 goes 120 -> 5 (so order effects do not always hit one size)                                    |
| Between runs | stop server, wait for Postgres quiet, start server (empty pool), 100 warm-up requests, `TRUNCATE events`, reset `pg_stat_statements`, `CHECKPOINT`, 5 s settle |
| Probes       | 2 separate 30 s runs, pool 10 and pool 90, with `pg_stat_activity` by wait event, `iostat`, `vmstat`, `pidstat`. Never used for headline numbers               |
| Total        | about 34 min (shown by `DRY_RUN=1`)                                                                                                                            |

**What counts as collapsed (same definition as B1, written before the run):** more than 1% of answers that are not a 202, or p99 above 1 s,
or any acknowledged event missing from the table. `dropped_iterations > 0` is flagged, not hidden.

**Predictions (written before the run).** Facts from B1 at pool 10: knee about 1,850 req/s; the INSERT takes about 0.02 ms, but the commit waits
for the WAL fsync (about 2.5 ms, disk about 97% busy). So one request keeps a connection for about 10 / 1850 = 5.4 ms, mostly the commit.

- **Why B1 had zero 503:** k6 stops at 3000 VUs, so the longest possible wait is about 3000 / capacity. At 1,865 req/s that is 1.6 s, below the 2 s timeout
  (and B1's p50 was 1.57 s). A pool timeout can only appear if capacity < 3000 / 2 s = 1,500 req/s.
- **Pool 5:** capacity about 5 / 5.4 ms = 925 req/s (range 800 to 1,400). That is under 1,500, so I expect **503 from the pool wait timeout**,
  `waiting` pinned high, many dropped iterations.
- **Pool 10:** repeats B1: about 1,850 achieved, p50 about 1.6 s, no 503. This is my control.
- **Pool 25:** capacity about 25 / 5.4 ms = 4,600, above 2,150, so healthy: p99 about 10 to 40 ms, `waiting` about 0. Group commit (several commits sharing one fsync) should
  keep the hold time from growing. The limit moves to the next resource (Node CPU or the disk).
- **Pool 50 and 90:** about the same throughput as 25. Latency the same or slightly worse. Postgres memory grows, roughly 4 to 8 MB per backend: about +200 to +400 MB at 50
  and +350 to +700 MB at 90 (container limit is 2 GiB). Context switches and Postgres CPU rise. **I expect a mild degradation, not a cliff**, because Postgres has only 2 cores and the query is light.
- **Pool 120:** usable slots are about 100 - 3 reserved - 1 exporter - 1 my psql = 95. When all connections are busy and requests wait, `pg` tries to open another connection
  and gets `53300 too many clients`; those requests get a quick 503. I expect an error share above 1% with error type **`too_many_clients`, not `pool_timeout`**.
- **Shape of the answer:** throughput and p99 improve from 5 to 25, then flatten, degrade slightly at 90, fall apart at 120. Optimum about 25, small compared with `max_connections=100`.

All numbers are for one laptop with Postgres on 2 cores, loopback network. They are relative, not general.

## 3. Instrumentation needed

**prom-client defaults used:** `process_cpu_user_seconds_total`, `process_cpu_system_seconds_total`.

**Custom server metrics used:** `http_requests_total`, `http_request_duration_seconds`, `http_requests_in_flight`, `ep_pg_pool_total`, `ep_pg_pool_idle`, `ep_pg_pool_waiting`,
`ep_pg_pool_wait_seconds`, `ep_pg_query_duration_seconds`, `ep_events_inserted_total`, `ep_db_errors_total`, `eventpulse_event_loop_delay_seconds`.
**MISSING until the rebuild - see missing-metrics.txt:** `ep_db_errors_by_type_total{type}`, `ep_pg_pool_max`.

**k6 metrics used:** `k6_http_req_duration_seconds` (native histogram), `k6_http_req_failed_rate`, `k6_iterations_total`, `k6_dropped_iterations_total`, `k6_vus`, `k6_vus_max`.
**MISSING until the first run - see missing-metrics.txt:** `k6_b2_resp_202_total`, `k6_b2_resp_503_total`, `k6_b2_resp_other_total`. Every k6 series carries the tags `pool_max`, `offered_rate`, `phase`, `testid`.

**node_exporter metrics used:** `node_cpu_seconds_total`, `node_context_switches_total`, `node_disk_io_time_seconds_total`, `node_disk_write_time_seconds_total`,
`node_disk_writes_completed_total`, `node_netstat_TcpExt_ListenOverflows`.

**Postgres container metrics (pg-container-exporter.sh, started by run.sh):** `ep_pg_container_cpu_seconds_total`, `ep_pg_container_cpu_throttled_seconds_total`, `ep_pg_container_memory_bytes`.

**postgres-exporter metrics:** `pg_stat_activity_count` (connections by state),
`pg_settings_max_connections`, `pg_settings_superuser_reserved_connections`, `pg_stat_database_xact_commit`, `pg_up`.

**Infra added for B2 (additive):** `infra/compose/docker-compose.yml` service `postgres-exporter` (v0.20.1, cpuset 5,11, connects to `postgres:5432`),
`infra/compose/prometheus-config.yml` job `postgres` (1 s). It keeps 1 connection open, which I subtract from the usable slots.

## 4. Grafana panels

Query column: job filters omitted, `[w]` = `$__rate_interval`. Same order as the dashboard rows (25 panels).

| #   | Row | Panel                                   | Query idea                                                                                                            | What it shows                         |
| --- | --- | --------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | ------------------------------------- |
| 1   | 1   | Offered vs handled rate                 | `rate(k6_iterations_total[w]) + rate(k6_dropped_iterations_total[w])` vs `rate(http_requests_total{status="202"}[w])` | where handled leaves offered          |
| 2   | 1   | Latency seen by k6 p50 / p99            | `histogram_quantile(q, sum(rate(k6_http_req_duration_seconds[w])))`                                                   | the latency each pool size gives      |
| 3   | 1   | k6 p99 by pool size                     | same, `sum by (pool_max)`                                                                                             | the p99-vs-pool-size curve            |
| 4   | 1   | Errors per second by type               | `sum by (type)(rate(ep_db_errors_by_type_total[w]))`                                                                  | pool_timeout vs too_many_clients      |
| 5   | 1   | Responses by status                     | `sum by (status)(rate(http_requests_total[w]))`                                                                       | 202 turning into 503                  |
| 6   | 2   | Pool state vs configured max            | `ep_pg_pool_total / idle / waiting / max`                                                                             | is the pool full, who waits           |
| 7   | 2   | Pool wait vs query time                 | `histogram_quantile` on `ep_pg_pool_wait_seconds_bucket` and `ep_pg_query_duration_seconds_bucket`                    | queueing in Node vs time in Postgres  |
| 8   | 2   | In-flight vs Little's Law               | `http_requests_in_flight` vs `rate(http_request_duration_seconds_sum[w])`                                             | requests piling up                    |
| 9   | 2   | Postgres connections by state           | `sum by (state)(pg_stat_activity_count)`, `pg_settings_max_connections`, minus reserved                               | how many backends exist, usable slots |
| 10  | 2   | Pool-bound ceiling                      | `max(ep_pg_pool_max) / mean(ep_pg_query_duration_seconds)` vs 202/s                                                   | does throughput hit the pool line     |
| 11  | 2   | Postgres commits/s vs disk writes/s     | `rate(pg_stat_database_xact_commit[w])`, `rate(node_disk_writes_completed_total[w])`                                  | group commit                          |
| 12  | 3   | Postgres container CPU                  | `rate(ep_pg_container_cpu_seconds_total[w])` user, system, throttled                                                  | Postgres computing, hitting `cpus: 2` |
| 13  | 3   | Postgres container memory               | `ep_pg_container_memory_bytes`                                                                                        | cost of more connections              |
| 14  | 3   | Container memory per connection (rough) | memory / `sum(pg_stat_activity_count)`                                                                                | trend of memory per backend           |
| 15  | 3   | Context switches                        | `rate(node_context_switches_total[w])`                                                                                | scheduler cost                        |
| 16  | 3   | Host disk utilization                   | `rate(node_disk_io_time_seconds_total[w])`                                                                            | the shared fsync disk                 |
| 17  | 3   | Host disk write latency                 | write time / writes completed                                                                                         | fsync cost                            |
| 18  | 3   | Node process CPU                        | `rate(process_cpu_user_seconds_total[w])`, system                                                                     | is Node the next limit                |
| 19  | 3   | Event loop delay p99                    | `eventpulse_event_loop_delay_seconds{quantile="0.99"}`                                                                | JS thread blocked?                    |
| 20  | 4   | k6 dropped iterations                   | `rate(k6_dropped_iterations_total[w])`                                                                                | 0 expected for pool 25 and up         |
| 21  | 4   | k6 VUs vs max                           | `k6_vus`, `k6_vus_max`                                                                                                | VU cap (the 1,500 req/s argument)     |
| 22  | 4   | Generator CPU busy                      | idle share on CPUs 3,9,4,10                                                                                           | keep under 70%                        |
| 23  | 4   | Listen overflows and drops              | `rate(node_netstat_TcpExt_ListenOverflows[w])`                                                                        | accept queue not involved             |
| 24  | 4   | Exporter up and DB errors               | `pg_up`, `rate(ep_db_errors_total[w])`                                                                                | exporter alive at pool 120?           |
| 25  | 4   | Node heap and RSS                       | `process_resident_memory_bytes`, `nodejs_heap_size_used_bytes`                                                        | queued requests in memory             |

Verify the metrics exist (Grafana Explore, after the first run):

```promql
count by (pool_max)(k6_iterations_total)
ep_pg_pool_max
sum by (type)(ep_db_errors_by_type_total)
up{job="postgres"}
pg_stat_activity_count      # check that it has a `state` label (panel 9 assumes it)
```

## 5. Run it

All commands from `loadtests/k6/stage-1/B2`.

```bash
# (a) dry run: prints the plan and total minutes, runs nothing
DRY_RUN=1 ./run.sh

# (b) one-level trial (NOT a headline result): one pool size, one repeat, 30 s, no probes
LEVELS="25" REPEATS=1 DURATION=30s PROBES=0 RESULTS_NAME=b2-trial ./run.sh

# (c) the full experiment (about 34 min)
./run.sh

# (d) a single case by hand (server already running with the pool size I want, e.g. PG_POOL_MAX=25)
cd ../.. && LABEL=b2-hand SCRIPT=stage-1/B2/script.js RATE=2150 DURATION=60s POOL_MAX=25 \
  API_KEY=<key> ROUTE=/healthz RESULTS_NAME=b2-hand ./runner.sh
```

Watch in another terminal during a run:

```bash
watch -n1 'curl -s localhost:8000/debug/stats'
docker exec ep-postgres psql -U eventpulse -d eventpulse -Atc "select state, count(*) from pg_stat_activity group by 1"
```

(The second command opens one extra Postgres connection every second while it runs. It is fine to look, but **do not leave it open during a measured run**; it is one of the usable slots.)

Results go to `results/b2/` (`summary.tsv`, `runs.tsv`, `env.txt`, `captures/<testid>.txt`, `server.out`, one folder per k6 run with `manifest.md` and `*-lap.json`).
Every capture block starts with `=== exp=... testid=...`, so `grep -h '^=== ' results/b2/captures/*.txt` is an index.

## 6. What this script changes on my machine

| Command                                                       | What and how it is undone                                                                       |
| ------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `docker update --cpuset-cpus ...` on postgres and monitoring  | pinning; restored to the old cpusets in the EXIT trap (set `KEEP_PINNING=1` to keep)            |
| `kill <server pid>`                                           | stops only the `ingest-api` process that `run.sh` started (restart between runs and at the end) |
| `TRUNCATE events`, `CHECKPOINT`, `pg_stat_statements_reset()` | empties the table before every run. **This deletes all rows in `events`.**                      |
| `docker exec ... psql`                                        | read queries, plus the three statements above                                                   |
| `infra/scripts/pg-container-exporter.sh` in background        | writes `infra/compose/textfile/ep_pg_container.prom`; removed on exit                           |

No `sudo`, no `sysctl`, `max_connections` is not changed.

## 7. Analyze in Grafana

Import `infra/observability/dashboards/stage-1/B2 - Pool exhaustion.json` (Dashboards > Import, pick the Prometheus datasource for the `ds` variable).
Read the rows in order: verdict (what happened per pool size), mechanism (why: waiting, wait vs query time, connections), resources (what Postgres paid), validity (was the generator fine).
Use `runs.tsv` (epoch seconds) to zoom to each run, or filter by the `pool_max` tag.
