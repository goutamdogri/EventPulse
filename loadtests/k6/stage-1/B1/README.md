# B1 Ramp to collapse (the baseline)

This is the first Stage 1 experiment. I send `POST /v1/events` at a steadily rising rate and watch where
latency bends. The naive design waits for Postgres inside the request, so the bend should be caused by
the pool, by Postgres, or by Node's CPU. I have to find out which one and prove it with the dashboard.

All files here were generated before I ran anything. Nothing in this README is a result.

## 1. Run the server

`run.sh` starts and stops `ingest-api` by itself (`MANAGE_SERVER=1`, default). It does **not** use the Stage 0
`server-runner.sh`, because that script is hard-wired to `labs/stage-0-foundations`. I did not touch it.
It restarts the server before every run, so each run begins with an empty pool and a fresh event-loop histogram.

What it runs (inside `services/ingest-api`):

```bash
taskset -c 0,6 node --env-file=.env dist/main.js
```

Before the first run:

```bash
cd services/ingest-api && pnpm build          # dist/main.js must exist
docker compose -f infra/compose/docker-compose.yml up -d     # prometheus, grafana, node_exporter, postgres
pnpm migrate:up                                # events table
```

CPU plan (12 logical CPUs = 6 cores, SMT pairs 0/6 1/7 2/8 3/9 4/10 5/11). Every component gets whole cores,
so no two of them share a physical core:

| Component             | Logical CPUs | How it is pinned                                          |
| --------------------- | ------------ | --------------------------------------------------------- |
| ingest-api (Node)     | 0,6          | `taskset` in run.sh                                       |
| Postgres container    | 1,7,2,8      | `docker update --cpuset-cpus` (container keeps `cpus: 2`) |
| k6                    | 3,9,4,10     | `K6_CPUS` env var read by `runner.sh`                     |
| Monitoring containers | 5,11         | `docker update --cpuset-cpus`                             |

The old cpusets are restored when the script ends (set `KEEP_PINNING=1` to keep the new ones).

Facts I record in `env.txt` for every pass: Postgres version and settings (`synchronous_commit`, `fsync`,
`shared_buffers`, `max_connections`...), pool size, kernel, node, k6 version, pinning.
I do not tune anything (Stage 1 rule: defaults are the point).

## 2. What I am testing

**Hypothesis (from the stage doc):** latency stays flat, then bends sharply at a knee set by either pool size,
database commit capacity, or the CPU of the Node process. I predict which one first.

**Variable:** offered rate (open model, `constant-arrival-rate`). One server config, defaults only
(`PG_POOL_MAX=10`, connection timeout 2 s, statement timeout 5 s).

**Two passes, because I do not know the knee yet:**

| Pass   | Command           | Rates (req/s)                                                                          | Duration | Repeats                     | Probes                           | Rough time      |
| ------ | ----------------- | -------------------------------------------------------------------------------------- | -------- | --------------------------- | -------------------------------- | --------------- |
| coarse | `./run.sh coarse` | 100 250 500 1000 1500 2000 3000 4000 6000                                              | 30 s     | 1 (a trial, not a headline) | none                             | about 11 min    |
| fine   | `./run.sh fine`   | 9 even steps from 50% of the last good coarse rate upward (read from `suggestion.env`) | 60 s     | 3                           | 2 (70% of the knee and the knee) | about 50-60 min |

Between every run the script: stops the server, waits for Postgres to be quiet, starts the server, sends 100 warm-up
requests (opens the pool connections, warms V8), then `TRUNCATE events`, resets `pg_stat_statements` and runs a `CHECKPOINT`.
So table size, index size and the previous run's checkpoint cannot leak into the next run.
Repeats are the OUTER loop: every repeat sweeps all rates, so thermal drift hits every rate in the same way.

**What counts as collapsed (my definition, written before the run):** error share above 1% (anything that is not a 202),
or p99 above 1 s, or any acknowledged event missing from the table. `dropped_iterations > 0` is flagged, not hidden: it
can mean a generator problem (discard the step) or the server so slow that all VUs wait (that is part of the finding).

## 3. Instrumentation needed

**prom-client defaults used:** `process_cpu_user_seconds_total`, `process_cpu_system_seconds_total`, `process_resident_memory_bytes`,
`process_open_fds`, `nodejs_heap_size_used_bytes`, `nodejs_eventloop_lag_p99_seconds`.

**Custom server metrics used:** `http_requests_total`, `http_request_duration_seconds`, `http_requests_in_flight`, `http_connections_open`,
`ep_pg_pool_total`, `ep_pg_pool_idle`, `ep_pg_pool_waiting`, `ep_pg_pool_wait_seconds`, `ep_pg_query_duration_seconds`,
`ep_events_inserted_total`, `ep_events_duplicate_total`, `ep_db_errors_total`, `eventpulse_event_loop_delay_seconds`.

**k6 metrics used:** `k6_http_req_duration_seconds` (native histogram, no `_bucket`), `k6_http_req_failed_rate`,
`k6_iterations_total`, `k6_dropped_iterations_total`, `k6_vus`, `k6_vus_max`. Custom counters `b1_resp_202/503/other` (used by the audit).

**node_exporter metrics used:** `node_cpu_seconds_total`, `node_disk_io_time_seconds_total`, `node_disk_write_time_seconds_total`,
`node_disk_writes_completed_total`, `node_netstat_TcpExt_ListenOverflows`, `node_netstat_TcpExt_ListenDrops`.

**postgres exporter metrics used** `ep_pg_container_cpu_seconds_total`, `ep_pg_container_cpu_throttled_seconds_total`,
`ep_pg_container_memory_bytes`, `ep_pg_container_disk_bytes_total`.

## 4. Grafana panels

Query column: job filters omitted, `[w]` = `$__rate_interval`. Same order as the dashboard rows.

| #   | Panel                          | Query idea                                                                                                            | What it shows                               |
| --- | ------------------------------ | --------------------------------------------------------------------------------------------------------------------- | ------------------------------------------- |
| 1   | Offered vs handled rate        | `rate(k6_iterations_total[w]) + rate(k6_dropped_iterations_total[w])` vs `rate(http_requests_total{status="202"}[w])` | where handled leaves offered = the capacity |
| 2   | Error rate                     | `avg(k6_http_req_failed_rate)`, server 5xx share                                                                      | when requests start failing                 |
| 3   | k6 latency p50/p99/p99.9       | `histogram_quantile(0.99, sum(rate(k6_http_req_duration_seconds[w])))`                                                | the knee                                    |
| 4   | k6 p99 by step                 | same, `sum by (offered_rate)`                                                                                         | the p99-vs-rate curve for the report        |
| 5   | Server-side latency            | `histogram_quantile(.., sum by (le)(rate(http_request_duration_seconds_bucket[w])))`                                  | time inside Express                         |
| 6   | Responses by status            | `sum by (status)(rate(http_requests_total[w]))`                                                                       | 202 turning into 503                        |
| 7   | Pool state                     | `ep_pg_pool_total / idle / waiting`                                                                                   | is the pool exhausted                       |
| 8   | Pool wait vs query time        | `histogram_quantile` on `ep_pg_pool_wait_seconds_bucket` and `ep_pg_query_duration_seconds_bucket`                    | queueing in Node vs slowness in Postgres    |
| 9   | In-flight vs Little's Law      | `http_requests_in_flight` vs `rate x mean latency`                                                                    | requests piling up                          |
| 10  | Pool-bound ceiling             | `10 / mean(ep_pg_query_duration)` vs 202/s (edit the 10 if I change `POOL_MAX`)                                       | does throughput hit the pool line           |
| 11  | Inserts, duplicates, DB errors | `rate(ep_events_*_total[w])`, `rate(ep_db_errors_total[w])`                                                           | rows written vs acknowledged                |
| 12  | Node process CPU               | `rate(process_cpu_user_seconds_total[w])`, system                                                                     | event loop thread saturated?                |
| 13  | Event loop delay               | `eventpulse_event_loop_delay_seconds{quantile=...}`                                                                   | JS thread blocked?                          |
| 14  | Postgres container CPU         | `rate(ep_pg_container_cpu_seconds_total[w])`                                                                          | Postgres computing                          |
| 15  | Postgres CPU vs limit          | total cores and `rate(..._throttled_seconds_total[w])`                                                                | hitting `cpus: 2`                           |
| 16  | Postgres memory                | `ep_pg_container_memory_bytes`                                                                                        | cache growth                                |
| 17  | Postgres disk bytes/s          | `rate(ep_pg_container_disk_bytes_total[w])`                                                                           | WAL and checkpoint writes                   |
| 18  | Host CPU by pinned group       | `1 - avg(rate(node_cpu_seconds_total{mode="idle",cpu=~"0\|6"}[w]))` per group                                         | which component runs out of CPU first       |
| 19  | Host disk utilization          | `rate(node_disk_io_time_seconds_total[w])`                                                                            | disk busy share                             |
| 20  | Host disk write await          | `rate(node_disk_write_time_seconds_total[w]) / rate(node_disk_writes_completed_total[w])`                             | fsync cost                                  |
| 21  | k6 dropped iterations          | `rate(k6_dropped_iterations_total[w])`                                                                                | must be 0                                   |
| 22  | k6 VUs vs max                  | `k6_vus`, `k6_vus_max`                                                                                                | VU cap reached                              |
| 23  | Generator CPU busy             | k6 CPU group                                                                                                          | keep under 70%                              |
| 24  | fds and connections            | `process_open_fds`, `http_connections_open`                                                                           | limits not involved                         |
| 25  | Listen overflows and drops     | `rate(node_netstat_TcpExt_ListenOverflows[w])`                                                                        | accept queue not involved                   |
| 26  | Node heap and RSS              | `process_resident_memory_bytes`, `nodejs_heap_size_used_bytes`                                                        | queued requests in memory                   |

Verify the metrics exist (paste in Grafana Explore after the first run):

```promql
count by (offered_rate)(k6_http_reqs_total)
count(ep_pg_pool_waiting)
count(http_requests_total{route="/v1/events"})
count by (job)(ep_pg_pool_total)
rate(ep_pg_container_cpu_seconds_total[30s])
```

If the dashboard's `job="eventpulse-nodejs"` does not match how ingest-api is scraped (`count by (job)(ep_pg_pool_total)` tells me), I must tell the helper.

## 5. Run it

All commands from `loadtests/k6/stage-1/B1/`.

a) Dry run (prints the plan, runs nothing, needs no Docker):

```bash
DRY_RUN=1 ./run.sh coarse
```

b) One-rate trial (checks that everything is wired, about 2 minutes):

```bash
RATES="500" REPEATS=1 DURATION=20s RESULTS_NAME=b1-trial ./run.sh coarse
```

c) Full B1, in two commands:

```bash
./run.sh coarse        # finds the rough knee, writes results/b1-coarse/suggestion.env
./run.sh fine          # reads suggestion.env, 9 even steps x 3 repeats + 2 probes
```

If auto-detection of the Postgres container fails: `PG_CONTAINER=<name> ./run.sh coarse` (`docker ps` shows names).
If a coarse pass shows no collapse, extend: `RATES="6000 8000 10000" ./run.sh coarse` into a new `RESULTS_NAME`.

d) One single step by hand through `runner.sh` (server and Postgres already running and the table empty):

```bash
cd loadtests/k6
RESULTS_NAME=manual LABEL=b1-manual SCRIPT=stage-1/B1/script.js RATE=1000 DURATION=30s \
  API_KEY=<my key> ROUTE=/healthz ./runner.sh
```

e) Watch in another terminal during a run:

```bash
watch -n1 "docker exec <pg> psql -U eventpulse -d eventpulse -Atc \"select state, wait_event_type, wait_event, count(*) from pg_stat_activity where datname='eventpulse' group by 1,2,3\""
curl -s localhost:8000/debug/stats | python3 -m json.tool      # pool total/idle/waiting, in-flight
```

(These two cost a little CPU. Fine for watching while I wait; I must not leave them running during a headline run.)

Where results go (all under `loadtests/k6/stage-1/B1/results/<RESULTS_NAME>/`, next to this experiment):

| File                    | What                                                                                                                                                             |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `summary.tsv`           | one row per run: achieved rps, errors, p50/p95/p99, dropped, rows in table, audit, verdict                                                                       |
| `runs.tsv`              | testid, phase, start/end epoch, status (zoom Grafana with these times)                                                                                           |
| `env.txt`               | versions, Postgres settings, pinning                                                                                                                             |
| `captures/<testid>.txt` | pg_stat_database and checkpoint counters before/after (subtract pre from post); probe runs also hold wait events, locks, top statements, iostat, vmstat, pidstat |
| `<timestamp>/`          | runner.sh manifests and the k6 `*-lap.json` summaries                                                                                                            |
| `suggestion.env`        | (coarse only) rates for the fine pass                                                                                                                            |

**The audit** (done after every run): number of 202 answers k6 counted vs `select count(*) from events`.
Equal = good. More rows than 202s = events committed but the client never heard (timeouts / aborted requests; worth reporting).
Fewer rows than 202s = an acknowledged event is missing (`LOST`), which would be a serious finding.

## 6. What this script changes on my machine

| Command                                                                | What it does                                                      | Restored?                                                                                   |
| ---------------------------------------------------------------------- | ----------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `docker update --cpuset-cpus ... <postgres>` and monitoring containers | pins them to their own cores                                      | yes, back to the old cpuset (or all CPUs) when the script ends; `KEEP_PINNING=1` skips this |
| `TRUNCATE events`                                                      | **deletes all rows of the events table before every run**         | no (that is the point). Do not run it on data I want to keep                                |
| `SELECT pg_stat_statements_reset()`                                    | clears Postgres statement statistics                              | no                                                                                          |
| `CHECKPOINT`                                                           | forces Postgres to flush dirty pages                              | no, harmless                                                                                |
| start/stop `node ... dist/main.js`                                     | starts and `kill`s the ingest-api process it started (by pid)     | yes (EXIT trap)                                                                             |
| `infra/scripts/pg-container-exporter.sh`                               | writes `infra/compose/textfile/ep_pg_container.prom` every second | yes, file removed and process killed on exit                                                |
| warm-up `curl` POSTs                                                   | 100 events with type `warmup`, deleted by the truncate afterwards | yes                                                                                         |

No `sudo`, no `sysctl`, no `tc`. Probe runs use `iostat` (needs `sysstat`) and `pidstat`, both optional.

## 7. Analyze in Grafana

Import `infra/observability/dashboards/stage-1/B1 - Ramp to collapse.json` (pick the Prometheus datasource at import).
Reading order:

1. Row 1 (verdict): where does 202/s leave the offered line, where does p99 bend, when do errors start.
2. Row 2 (mechanism): pool waiting vs total, wait time vs query time, whether 202/s hits the pool-bound ceiling.
3. Row 3 (resources): which CPU group, container or disk saturates at the same moment.
4. Row 4 (validity): dropped iterations 0, generator CPU under 70%, no listen overflows. Discard steps that fail this.

Panels 7 to 11 and 12 to 20 together decide which resource limits the system;
