# B11 Table growth soak

Why this experiment and what I expect: see [notebook/stage-1/experiments/B11/Explanation.md](../../../../notebook/stage-1/experiments/B11/Explanation.md)

## 1. Run the server

`run.sh` starts and stops the ingest-api itself (`MANAGE_SERVER=1`, the default), once before every run, so every run begins with new pool connections. The command it uses (same as B6 and B7, the database URL comes from `.env` and goes straight to Postgres on `localhost:5434`):

```bash
cd services/ingest-api && API_KEYS=<epk_b11_0:t0,...> COUNTER_MODE=off PG_POOL_MAX=25 taskset -c 0,6 node --env-file=.env dist/main.js
```

- The build must exist: `pnpm build` in `services/ingest-api`. B11 adds no server code.
- Nothing else may answer on `localhost:8000` when I start (the preflight checks).
- Not set by `run.sh`, so the values of `.env` apply: `PG_CONN_TIMEOUT_MS`, `PG_STATEMENT_TIMEOUT_MS`, `RETRY_AFTER_S`. They are written to `env.txt`.
- Postgres is the compose container `ep-postgres` (postgres:15). Its settings are not changed: `shared_buffers` 128 MB, `checkpoint_timeout` 5 min, `max_wal_size` 1 GB, container limit 2 GB RAM and 2 CPUs. They are written to `env.txt`.
- CPU pinning: server 0,6 (taskset), Postgres 1,7,2,8 (compose cpuset), k6 3,9,4,10 (`K6_CPUS`), monitoring, the sampler and the two exporters 5,11.
- Disk: the soak writes table plus index to the Docker volume on `/`. Check `df -h /` first. The preflight stops if less than `MIN_FREE_GB + 5` GB are free.

## 2. Run configuration

Four runs in this order, one repeat each (a soak is too long to repeat three times):

| Run (`testid`) | Kind | Rate | Duration | Table before the run |
|---|---|---|---|---|
| `b11-cap_start` | probe | `PROBE_RATE` 6000 | `CAP_DURATION` 60 s | truncated (empty) |
| `b11-soak` | measured | `RATE` 3000 | `DURATION` 60 m | truncated (empty) |
| `b11-cap_end` | probe | 6000 | 60 s | the big table the soak left behind (not truncated) |
| `b11-fresh` | control | 3000 | `FRESH_DURATION` 180 s | truncated (empty) |

Every run: restart the server, warm-up (400 requests), `TRUNCATE events` (except `cap_end`), `CHECKPOINT`, 5 s settle, then k6 (constant-arrival-rate). About 69 minutes in total with settling. Only the soak is a measured run; the others are probes and a control and are reported separately.

During every run a sampler (pinned to 5,11) writes one line every `SAMPLE_S` seconds to `captures/<testid>.samples.tsv` with: estimated rows, table and index bytes, checkpoint counters, buffer counters, database hit/read counters, WAL bytes and full-page images, autovacuum count, events inserted and INSERT time as the server counts them, free disk. `hook.py` turns it into the summary columns: first-5-minutes vs last-5-minutes of rps, INSERT time, cache hit ratio, WAL bytes per row and full-page images per row, plus checkpoints, sizes and free disk.

A disk guard in `run.sh` checks the free space at every sample. Below `MIN_FREE_GB` it stops k6 and marks the run `ABORTED-DISK`.

| Knob | Default | Meaning |
|---|---|---|
| `PHASES` | `cap_start soak cap_end fresh` | which runs, in this order |
| `RATE` | 3000 | rate of soak and fresh (about 70% of 4300 req/s) |
| `DURATION` | 60m | soak length (`3h` for the long version) |
| `PROBE_RATE`, `CAP_DURATION` | 6000, 60s | the capacity probes |
| `FRESH_DURATION` | 180s | the fresh-table control |
| `SAMPLE_S` | 60 | sampler period |
| `MIN_FREE_GB`, `DISK_PATH` | 10, `/` | disk guard |
| `POOL`, `TENANTS` | 25, 100 | as B3 to B8 |
| `RESULTS_NAME` | b11 | folder under `results/` |
| `MANAGE_SERVER` | 1 | 0 = use a server I started myself |

The analyzer's collapse limits are switched off (the probes are above the capacity on purpose). Verdicts that can still show: `LOST-ROWS` and `dropped=N`.

## 3. Instrumentation needed

Nothing was added to the code for B11.

- prom-client defaults used: `process_cpu_user_seconds_total`, `process_cpu_system_seconds_total`, `nodejs_eventloop_lag_p99_seconds`.
- Custom server metrics used: `ep_events_inserted_total`, `ep_pg_query_duration_seconds_bucket`, `ep_pg_pool_wait_seconds_bucket`, `ep_pg_pool_waiting`, `http_requests_in_flight`, `http_connections_open`.
- Postgres exporter metrics used (job `postgres`): `pg_stat_user_tables_table_size_bytes`, `pg_stat_user_tables_index_size_bytes`, `pg_stat_user_tables_n_live_tup`, `pg_stat_user_tables_autovacuum_count`, `pg_stat_bgwriter_checkpoints_{timed,req}_total`, `pg_stat_bgwriter_buffers_{checkpoint,clean,backend}_total`, `pg_stat_database_blks_{hit,read}`, `pg_statio_user_tables_idx_blocks_{hit,read}`, `pg_settings_shared_buffers_bytes`. Postgres 15 has no `pg_stat_checkpointer`; the counters are in `pg_stat_bgwriter`.
- Textfile exporters started by `run.sh` (same as B6): `infra/scripts/pg-wal-exporter.sh` gives `ep_pg_wal_bytes_total` and `ep_pg_wal_fpi_total`; `infra/scripts/pg-container-exporter.sh` gives `ep_pg_container_cpu_seconds_total`, `ep_pg_container_memory_bytes`, `ep_pg_container_disk_bytes_total`.
- k6 metrics used: `k6_http_req_duration_seconds` (native histogram), `k6_http_reqs_total`, `k6_dropped_iterations_total`, `k6_vus`, `k6_vus_max`, and the three counters of `script.js` `k6_b11_resp_202_total`, `k6_b11_resp_503_total`, `k6_b11_resp_other_total` (ADDED BY B11, in `loadtests/k6/stage-1/B11/script.js`; they exist in Prometheus only after a k6 run).
- node_exporter metrics used (job `node`): `node_cpu_seconds_total`, `node_disk_io_time_seconds_total`, `node_disk_{read,written}_bytes_total`, `node_filesystem_avail_bytes`, `node_memory_Cached_bytes`, `node_memory_MemAvailable_bytes`.

## 4. Grafana panels

`[w]` = `$__rate_interval`. Job filters omitted. `T` = `{relname="events"}`.

| # | Panel | Query | What it shows |
|---|---|---|---|
| 1 | Accepted vs sent rate | `sum(rate(k6_b11_resp_202_total[w]))`; `sum(rate(k6_http_reqs_total[w])) + sum(rate(k6_dropped_iterations_total[w]))` | throughput over time; the plateau in the probes is the capacity |
| 2 | Latency seen by k6 | `histogram_quantile(0.5/0.99, sum(rate(k6_http_req_duration_seconds[w])))` | p50 and p99 over the hour |
| 3 | Inserted and errors | `sum(rate(ep_events_inserted_total[w]))`; 503 and other counters | server count vs client count |
| 4 | Size of events | `pg_stat_user_tables_table_size_bytes T`, `..._index_size_bytes T`, `pg_settings_shared_buffers_bytes` | growth against the cache size |
| 5 | Checkpoints per minute | `rate(pg_stat_bgwriter_checkpoints_timed_total[w])*60`, same for `_req_` | timer vs WAL-size checkpoints |
| 6 | Who writes dirty buffers | `rate(pg_stat_bgwriter_buffers_{checkpoint,clean,backend}_total[w])` | checkpointer vs bgwriter vs backends |
| 7 | Buffer cache hit ratio | `rate(blks_hit)/(rate(blks_hit)+rate(blks_read))` for the database; `idx_blocks_hit/(hit+read)` for events | cache misses |
| 8 | WAL per event, FPI per event | `rate(ep_pg_wal_bytes_total[w]) / sum(rate(ep_events_inserted_total[w]))`; `rate(ep_pg_wal_fpi_total[w]) / ...` | cost of each insert in WAL |
| 9 | INSERT p50 / p99 | `histogram_quantile(q, sum by (le)(rate(ep_pg_query_duration_seconds_bucket[w])))` | time on the connection |
| 10 | Pool wait p99, waiting | `ep_pg_pool_wait_seconds_bucket`, `ep_pg_pool_waiting` | queue for a connection |
| 11 | Autovacuum runs, live rows | `pg_stat_user_tables_autovacuum_count T`, `n_live_tup T` | vacuum steps |
| 12 | Disk busy and throughput | `rate(node_disk_io_time_seconds_total[w])`, read and written bytes | real disk load |
| 13 | Postgres container | `ep_pg_container_cpu_seconds_total`, `ep_pg_container_disk_bytes_total` | cores and disk of Postgres |
| 14 | Memory and page cache | `ep_pg_container_memory_bytes`, `node_memory_Cached_bytes`, `node_memory_MemAvailable_bytes` | cache against data size |
| 15 | CPU of pinned groups | `1 - avg(rate(node_cpu_seconds_total{mode="idle",cpu=~"$cpu_x"}[w]))` for server, postgres, k6 | where the CPU goes |
| 16 | Node process CPU | `process_cpu_{user,system}_seconds_total` | server cost per request |
| 17 | k6 dropped iterations | `sum(rate(k6_dropped_iterations_total[w]))` | must be 0 in soak and fresh |
| 18 | k6 VUs vs max | `k6_vus`, `k6_vus_max` | generator headroom |
| 19 | Free disk on `/` | `node_filesystem_avail_bytes{mountpoint="/"}` | disk guard |
| 20 | Event loop lag p99 | `nodejs_eventloop_lag_p99_seconds` | server thread busy? |
| 21 | In flight, open connections | `http_requests_in_flight`, `http_connections_open` | Little's law check |

Verify queries to paste into Grafana Explore after a run:

```promql
count by (phase)(k6_b11_resp_202_total)
pg_stat_user_tables_index_size_bytes{relname="events"}
```

## 5. Run it

(a) Dry run, prints the plan and runs nothing:

```bash
cd loadtests/k6/stage-1/B11 && DRY_RUN=1 ./run.sh
```

(b) Plumbing trial, not a result (soak shortened to 3 minutes, probes as default):

```bash
cd loadtests/k6/stage-1/B11 && DURATION=3m RESULTS_NAME=b11-trial ./run.sh
```

(c) The full experiment, one command, about 69 minutes:

```bash
cd loadtests/k6/stage-1/B11 && ./run.sh
```

The long version (check `df -h /` first, 3 hours writes about 20 GB of table and index):

```bash
cd loadtests/k6/stage-1/B11 && DURATION=3h RESULTS_NAME=b11-3h ./run.sh
```

(d) A single case by hand through the shared runner (server already running with the keys of `epk_b11_`):

```bash
cd loadtests/k6 && env RESULTS_NAME=b11-manual LABEL=b11-manual SCRIPT=stage-1/B11/script.js RATE=3000 DURATION=2m PHASE=soak ROUTE=/healthz ./runner.sh
```

(e) Watch during the soak, in another terminal:

```bash
watch -n 5 'df -h / | tail -1; tail -1 loadtests/k6/stage-1/B11/results/b11/captures/b11-soak.samples.tsv'
```

Results go to `results/<RESULTS_NAME>/` inside this folder: `summary.tsv` (one row per run), `runs.tsv` (start/end epochs and status), `env.txt`, `captures/` (`<testid>.samples.tsv`, `<testid>.txt`), `server.out`, and the k6 lap JSONs. Read the table with:

```bash
python3 loadtests/k6/analyze.py table loadtests/k6/stage-1/B11/results/b11/summary.tsv
```

## 6. What this script changes on my machine

- `TRUNCATE events` before every run except `cap_end`. The soak leaves about 10 million rows in `events` (several GB); the next experiment that truncates removes them. To free the space right away:

```bash
docker exec ep-postgres psql -U eventpulse -d eventpulse -c "TRUNCATE events"
```

- Starts and stops its own ingest-api (taskset 0,6) and two exporter scripts that write `infra/compose/textfile/ep_pg_wal.prom` and the container file (removed when they stop).
- Calls `pg_stat_statements_reset()` and `CHECKPOINT` before each run.
- On Ctrl+C or an error the EXIT trap stops the sampler, the server and the exporters. Postgres settings, sysctls and the proxy are not touched; there is no `sudo`. If the script is killed hard (`kill -9`) the trap does not run; stop the leftovers with `pkill -f 'dist/main.js'` and the exporters by hand.

## 7. Analyze in Grafana

Dashboard: [B11 - Table growth soak.json](../../../../infra/observability/dashboards/stage-1/B11%20-%20Table%20growth%20soak.json). Import it in Grafana (Dashboards, New, Import, upload the file) and pick the Prometheus datasource. Zoom to the times in `runs.tsv` (epoch seconds). Read the rows in this order: 1 the verdict (rate and p99 over the hour), 2 the mechanism (size, checkpoints, cache, WAL), 3 the resource view (disk, CPU, memory), 4 the validity checks (dropped iterations must be 0 in the soak and the fresh run, free disk).
