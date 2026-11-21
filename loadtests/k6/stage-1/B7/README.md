# B7 Payload size

Why this experiment and what I expect: see [notebook/stage-1/experiments/B7/Explanation.md](../../../../notebook/stage-1/experiments/B7/Explanation.md)

## 1. Run the server

`run.sh` starts and stops the ingest-api itself (`MANAGE_SERVER=1`, the default), once before every run, so every run begins with new pool connections. The command it uses (same as B3 to B6, plus a bigger body limit):

```bash
cd services/ingest-api && API_KEYS=<epk_b7_0:t0,...> COUNTER_MODE=off PG_POOL_MAX=25 BODY_LIMIT=1mb taskset -c 0,6 node --env-file=.env dist/main.js
```

- The build must exist: `pnpm --filter @eventpulse/ingest-api build` (or `pnpm build` in `services/ingest-api`).
- Nothing else may answer on `localhost:8000` when I start (the preflight checks).
- The server connects directly to Postgres (`localhost:5434`), not through Toxiproxy.
- `BODY_LIMIT` is an existing env var, the code is not changed. The measured runs use `1mb` for every size.
- Postgres must have `synchronous_commit=on` (the preflight checks; a leftover role setting from B6 must be reset with `loadtests/k6/stage-1/B6/run.sh RESTORE`).
- At least 20 GB free on the disk (preflight): 100 KB events write several GB per run.
- CPU pinning (from compose, not changed by the script): server 0,6 (taskset), Postgres 1,7,2,8, k6 3,9,4,10 (`K6_CPUS`), monitoring and toxiproxy 5,11.

## 2. Run configuration

- Variable: body size `SIZE` = 200, 500, 2048, 10240, 102400 bytes (the whole JSON body, the filler string is random hex).
- Phases: `at` (a rate just below the predicted capacity of the size) and `over` (a rate above it).
- 5 sizes x 2 phases x 3 repeats x 45 s = 30 measured runs, about 46 min with restarts and settling. Repeat is the outer loop; odd repeats go from small to big, even repeats from big to small.
- One body-limit probe at the end (curl with the default `BODY_LIMIT`).

| Env knob                 | Default                     | What it does                                          |
| ------------------------ | --------------------------- | ----------------------------------------------------- |
| `SIZES`                  | `200 500 2048 10240 102400` | body sizes in bytes                                   |
| `AT_RATES`               | `4000 4000 3900 3700 1500`  | `at` rate per size (same order as `SIZES`)            |
| `OVER_RATES`             | `8000 8000 8000 6000 3000`  | `over` rate per size                                  |
| `PHASES`                 | `at over`                   | which phases to run                                   |
| `REPEATS`                | `3`                         | repeats of every cell                                 |
| `DURATION`               | `45s`                       | duration of every k6 run                              |
| `POOL`                   | `25`                        | `PG_POOL_MAX` of the server                           |
| `TENANTS`                | `100`                       | tenants (api keys)                                    |
| `SERVER_BODY_LIMIT`      | `1mb`                       | `BODY_LIMIT` given to the server in the measured runs |
| `PROBE_413`              | `1`                         | `0` skips the body-limit probe                        |
| `DISK_MIN_GB`            | `20`                        | free disk needed at the start                         |
| `SERVER_CPUS`, `K6_CPUS` | `0,6`, `3,9,4,10`           | pinning                                               |
| `MANAGE_SERVER`          | `1`                         | `0` = attach to a server I started myself             |
| `RESULTS_NAME`           | `b7`                        | results folder name                                   |
| `DRY_RUN`                | `0`                         | `1` prints the plan and runs nothing                  |

## 3. Instrumentation needed

No server code was changed for B7.

- prom-client defaults used: `nodejs_eventloop_lag_p99_seconds`, `nodejs_gc_duration_seconds_sum`.
- Custom server metrics used (they already exist): `http_request_duration_seconds_bucket`, `ep_events_inserted_total`, `ep_pg_query_duration_seconds_bucket`, `ep_pg_pool_wait_seconds_bucket`, `ep_pg_pool_total`, `ep_pg_pool_idle`, `ep_pg_pool_waiting`.
- Exporter metrics used (they already exist): `ep_pg_wal_bytes_total`, `ep_pg_wal_records_total`, `ep_pg_wal_fpi_total` (infra/scripts/pg-wal-exporter.sh, started by `run.sh`); `ep_pg_container_cpu_seconds_total`, `ep_pg_container_disk_bytes_total` (infra/scripts/pg-container-exporter.sh, started by `run.sh`); `pg_stat_user_tables_table_size_bytes`, `pg_stat_user_tables_index_size_bytes`, `pg_stat_bgwriter_checkpoints_req_total`, `pg_stat_bgwriter_buffers_checkpoint_total` (postgres-exporter).
- k6 metrics used: `k6_http_reqs_total`, `k6_http_req_duration_seconds` (native histogram), `k6_http_req_failed_rate`, `k6_data_sent_total`, `k6_dropped_iterations_total`, `k6_vus`, `k6_vus_max`; custom counters defined in `script.js` (**ADDED BY B7**): `k6_b7_resp_202_total`, `k6_b7_resp_503_total`, `k6_b7_resp_other_total` (they exist in Prometheus only after a k6 run).
- node_exporter metrics used: `node_cpu_seconds_total`, `node_disk_written_bytes_total`, `node_disk_io_time_seconds_total`, `node_network_receive_bytes_total`.
- Not in Prometheus, taken by `run.sh` into `summary.tsv`: WAL bytes per row, full-page images per row, and heap, TOAST and index bytes per row (psql before and after each run).

## 4. Grafana panels

`[w]` = `$__rate_interval`. Job filters omitted.

| #   | Panel                           | Query                                                                                                         | What it shows                         |
| --- | ------------------------------- | ------------------------------------------------------------------------------------------------------------- | ------------------------------------- |
| 1   | Accepted (202) rps by body size | `sum by (size) (rate(k6_b7_resp_202_total[w]))`                                                               | events per second the server accepted |
| 2   | k6 offered rps                  | `sum by (size) (rate(k6_http_reqs_total[w]))`                                                                 | what k6 sent                          |
| 3   | Client latency p50 and p99      | `histogram_quantile(0.99, sum by (size) (rate(k6_http_req_duration_seconds[w])))`                             | latency seen by k6                    |
| 4   | Not accepted                    | `rate(k6_b7_resp_503_total[w])`, `rate(k6_b7_resp_other_total[w])`                                            | 503, 413, 500, timeouts               |
| 5   | Server request duration p99     | `histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket{route="/v1/events"}[w])))`   | latency inside the server             |
| 6   | WAL bytes per second            | `rate(ep_pg_wal_bytes_total[w])`                                                                              | WAL volume                            |
| 7   | WAL bytes per inserted event    | `rate(ep_pg_wal_bytes_total[w]) / rate(ep_events_inserted_total[w])`                                          | WAL cost of one event                 |
| 8   | WAL records and FPI per second  | `rate(ep_pg_wal_records_total[w])`, `rate(ep_pg_wal_fpi_total[w])`                                            | records and full-page images          |
| 9   | Table and index size            | `pg_stat_user_tables_table_size_bytes{relname="events"}`, `..._index_size_bytes`                              | growth of the table                   |
| 10  | Disk write rate                 | `rate(ep_pg_container_disk_bytes_total{direction="write"}[w])`, `sum(rate(node_disk_written_bytes_total[w]))` | bytes to disk                         |
| 11  | Bytes sent by k6                | `sum by (size) (rate(k6_data_sent_total[w]))`                                                                 | bytes on the wire                     |
| 12  | DB query p99 and pool wait p99  | `ep_pg_query_duration_seconds_bucket`, `ep_pg_pool_wait_seconds_bucket`                                       | INSERT time and pool queue            |
| 13  | Pool in use and waiting         | `ep_pg_pool_total - ep_pg_pool_idle`, `ep_pg_pool_waiting`                                                    | pool pressure                         |
| 14  | Server CPU user vs system       | `node_cpu_seconds_total{cpu=~"$cpu_server"}` by mode                                                          | Node CPU                              |
| 15  | Postgres container CPU          | `rate(ep_pg_container_cpu_seconds_total[w])`                                                                  | Postgres CPU                          |
| 16  | Event loop lag and GC           | `nodejs_eventloop_lag_p99_seconds`, `rate(nodejs_gc_duration_seconds_sum[w])`                                 | Node overload                         |
| 17  | Host disk busy                  | `max(rate(node_disk_io_time_seconds_total[w]))`                                                               | disk saturation                       |
| 18  | Checkpoints                     | `rate(pg_stat_bgwriter_checkpoints_req_total[w])`                                                             | forced checkpoints                    |
| 19  | Host network                    | `sum(rate(node_network_receive_bytes_total[w]))`                                                              | bytes through interfaces              |
| 20  | k6 dropped iterations           | `sum by (size) (rate(k6_dropped_iterations_total[w]))`                                                        | generator or server limit             |
| 21  | k6 failed share                 | `avg by (size) (k6_http_req_failed_rate)`                                                                     | not-202 share                         |
| 22  | k6 CPU busy                     | `1 - avg(rate(node_cpu_seconds_total{cpu=~"$cpu_k6", mode="idle"}[w]))`                                       | generator headroom (keep under 70%)   |
| 23  | k6 VUs                          | `sum(k6_vus)`, `sum(k6_vus_max)`                                                                              | VU cap                                |

Verify in Grafana Explore (after one k6 run):

```promql
count by (size) (k6_b7_resp_202_total)
count(ep_pg_wal_bytes_total)
```

## 5. Run it

All commands from `loadtests/k6/stage-1/B7/`.

(a) Dry run (prints the plan, runs nothing):

```bash
DRY_RUN=1 ./run.sh
```

(b) Trial (plumbing check only, never quoted as a result; small and big size, one repeat, 30 s):

```bash
SIZES="500 102400" AT_RATES="4000 1500" OVER_RATES="8000 3000" REPEATS=1 DURATION=30s PROBE_413=0 RESULTS_NAME=b7-trial ./run.sh
```

(c) The full matrix (about 46 min):

```bash
./run.sh
```

(d) One case by hand through the shared runner (the server must already run with `BODY_LIMIT=1mb`):

```bash
cd loadtests/k6 && RESULTS_NAME=b7-manual LABEL=b7-manual SCRIPT=stage-1/B7/script.js SIZE=10240 RATE=3000 DURATION=30s ROUTE=/healthz KEY_PREFIX=epk_b7_ ./runner.sh
```

Watch during a run (another terminal):

```bash
grep -h '^=== ' loadtests/k6/stage-1/B7/results/b7/captures/*.txt | tail -5
python3 loadtests/k6/analyze.py table loadtests/k6/stage-1/B7/results/b7/summary.tsv
df -h /
```

Results go to `results/<RESULTS_NAME>/` inside this folder: `summary.tsv` (one row per run, with WAL, heap, TOAST and index bytes per row), `runs.tsv` (start and end epochs), `env.txt`, `captures/` (one file per run with the snapshots and the mid-run backend states, and `b7-probe-413.txt`), `server.out`, and the k6 lap JSONs in the timestamped folders.

## 6. What this script changes on my machine

- `TRUNCATE events` before every run and once at the end (the data of the earlier runs is gone; the end truncate frees the disk). Also run for each trial.
- `CHECKPOINT` and `pg_stat_statements_reset()` before every run.
- Starts and stops the ingest-api and two exporter scripts (`pg-container-exporter.sh`, `pg-wal-exporter.sh`) in the background; they are stopped on exit.
- Writes several GB of table data per big run (100 KB events) that is released by the truncate.
- Does not use sudo, does not change a sysctl, a Postgres setting, a container or the role. If the script is killed hard (`kill -9`) the trap does not run: run `docker exec ep-postgres psql -U eventpulse -d eventpulse -c "TRUNCATE events"` to free the disk and stop a leftover server (`pkill -f dist/main.js`).

## 7. Analyze in Grafana

Dashboard: [B7 - Payload size.json](../../../../infra/observability/dashboards/stage-1/B7%20-%20Payload%20size.json). Import it in Grafana (Dashboards, New, Import, upload the JSON) and pick the Prometheus datasource in the `ds` variable. Zoom to each run with the epochs in `runs.tsv`. Read the rows in order: 1 the verdict (accepted rps and latency per size), 2 the mechanism (WAL per event, table growth, disk writes, query time), 3 the resources (Node CPU, Postgres CPU, disk busy), 4 the validity checks (dropped iterations, k6 CPU) before trusting a run.
