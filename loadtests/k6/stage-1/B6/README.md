# B6 Durability knob

Why this experiment and what I expect: see [notebook/stage-1/experiments/B6/Explanation.md](../../../../notebook/stage-1/experiments/B6/Explanation.md)

## 1. Run the server

`run.sh` starts and stops the ingest-api itself (`MANAGE_SERVER=1`, the default), once before every run, so every run begins with new pool connections. The command it uses (same as B3 to B5):

```bash
cd services/ingest-api && API_KEYS=<epk_b6_0:t0,...> COUNTER_MODE=off PG_POOL_MAX=25 taskset -c 0,6 node --env-file=.env dist/main.js
```

- The build must exist: `pnpm --filter @eventpulse/ingest-api build` (or `pnpm build` in `services/ingest-api`).
- Nothing else may answer on `localhost:8000` when I start (the preflight checks).
- The server connects directly to Postgres (`localhost:5434`), not through Toxiproxy.
- CPU pinning (nothing is changed by the script, the containers come from docker compose): server 0,6 (taskset), Postgres container 1,7,2,8, k6 3,9,4,10, monitoring + Toxiproxy 5,11.
- To use my own server instead: `MANAGE_SERVER=0`. Then I have to start it with pool 25, tenants `epk_b6_0..99` and set `synchronous_commit` myself, so I do not recommend it.

## 2. Run configuration

The variable is `synchronous_commit` of the database role `eventpulse`: `on` or `off`. `run.sh` sets it with `ALTER ROLE eventpulse SET synchronous_commit = ...`, checks it from a fresh session, then starts the server.

| Part          | Content                                                                                                                             |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Measured runs | modes {on, off} x phases {at, over}, 3 repeats, 45 s each = 12 runs. Odd repeats run `on` then `off`, even repeats `off` then `on`. |
| Crash probes  | 3 per mode (6 runs), run after the measured runs: 4000 req/s for 30 s, Postgres is killed with SIGKILL 20 s after k6 starts         |
| Total         | about 29 min (printed by the dry run)                                                                                               |

| Env knob                                     | Default            | Meaning                                                                   |
| -------------------------------------------- | ------------------ | ------------------------------------------------------------------------- |
| `MODES`                                      | `on off`           | values of synchronous_commit to compare                                   |
| `PHASES`                                     | `at over`          | `at` = `AT_RATE`, `over` = `OVER_RATE`                                    |
| `AT_RATE` / `OVER_RATE`                      | 4000 / 8000        | offered req/s (the same for both modes)                                   |
| `REPEATS`                                    | 3                  | repeats of every measured cell                                            |
| `DURATION`                                   | 45s                | k6 duration of a measured run                                             |
| `MEASURED`                                   | 1                  | 0 = skip the measured runs (crash trial)                                  |
| `PROBES`                                     | 1                  | 1 = also run the crash probes                                             |
| `CRASH_REPS`                                 | 3                  | crash probes per mode                                                     |
| `CRASH_RATE` / `CRASH_DURATION` / `CRASH_AT` | 4000 / 30s / 20    | offered rate, k6 duration, seconds after k6 start when Postgres is killed |
| `POOL`                                       | 25                 | `PG_POOL_MAX` of the server                                               |
| `TENANTS`                                    | 100                | tenants (api keys `epk_b6_0..`)                                           |
| `SERVER_CPUS` / `K6_CPUS`                    | `0,6` / `3,9,4,10` | pinning of server and k6                                                  |
| `RESULTS_NAME`                               | `b6`               | folder name under `results/`                                              |
| `PG_CONTAINER`                               | auto               | name of the Postgres container (`ep-postgres`)                            |
| `DRY_RUN`                                    | 0                  | 1 = print the plan only                                                   |

Every measured run: restart the server (with the mode set before), warm up with 400 requests, TRUNCATE `events`, CHECKPOINT, snapshot of WAL counters, k6 through `runner.sh`, snapshot again, count rows, one row in `summary.tsv` through the shared `analyze.py`. In the middle of each `over` run: one `pg_stat_activity` by wait event and `pg_stat_wal` capture. Crash probe: the same preparation, then k6 in the background, `docker kill -s KILL` of the Postgres container, the server is stopped right after (so a restarted database cannot answer any more 202), k6 finishes, Postgres is started again, I wait until it answers (crash recovery), then count rows. `lost = 202s counted by k6 - rows found`.

## 3. Instrumentation needed

No server code was changed for B6.

**prom-client defaults used:** `process_cpu_seconds_total`.

**Custom server metrics used:** `http_requests_in_flight`, `ep_pg_pool_total`, `ep_pg_pool_idle`, `ep_pg_pool_waiting`, `ep_pg_query_duration_seconds_bucket`. For the summary: `ep_db_errors_by_type_total` (read from `/metrics` after each run).

**k6 metrics used:** `k6_http_reqs_total`, `k6_http_req_duration_seconds`, `k6_dropped_iterations_total`, `k6_vus`, `k6_vus_max`.
ADDED BY B6 in `loadtests/k6/stage-1/B6/script.js`: `k6_b6_resp_202_total`, `k6_b6_resp_503_total`, `k6_b6_resp_other_total` (counters `b6_resp_202`, `b6_resp_503`, `b6_resp_other`; the exact Prometheus names are confirmed after the first run).
Tags on every k6 series: `exp`, `sync`, `phase` (`at`, `over`, `probe_crash`), `offered_rate`, `pool_max`, `tenants`, plus `testid` from `runner.sh`.

**postgres-exporter (job `postgres`):** `pg_stat_database_xact_commit`, `pg_stat_activity_count`, `pg_up`.

**node_exporter (job `node`):** `node_cpu_seconds_total`, `node_disk_flush_requests_total`, `node_disk_io_time_seconds_total`.

**Textfile exporters started by `run.sh` (existing, from B5):** `infra/scripts/pg-wal-exporter.sh` (`ep_pg_wal_bytes_total`, ...) and `infra/scripts/pg-container-exporter.sh` (`ep_pg_container_cpu_seconds_total`). Both read the Postgres container, so after the first crash probe they may stop updating. The probes run last for that reason.

## 4. Grafana panels

`[w]` = `$__rate_interval`. Rows in dashboard order.

| #   | Panel                        | Query                                                                                 | What it shows                          |
| --- | ---------------------------- | ------------------------------------------------------------------------------------- | -------------------------------------- |
| 1.1 | Accepted req/s               | `sum by (sync,phase)(rate(k6_b6_resp_202_total[w]))`                                  | 202 per second per mode and phase      |
| 1.2 | p50                          | `histogram_quantile(0.5, sum by (sync,phase)(rate(k6_http_req_duration_seconds[w])))` | median latency                         |
| 1.3 | p99                          | the same with 0.99                                                                    | tail latency                           |
| 1.4 | Not accepted %               | `100*(1 - 202 rate / k6_http_reqs_total rate)`                                        | share without 202                      |
| 1.5 | pg_up                        | `min(pg_up{job="postgres"})`                                                          | when Postgres was killed and came back |
| 1.6 | 503 / other per second       | `rate(k6_b6_resp_503_total[w])`, `..._other_total`                                    | error classes                          |
| 2.1 | Query time                   | `histogram_quantile(q, sum by (le)(rate(ep_pg_query_duration_seconds_bucket[w])))`    | time of one INSERT incl. COMMIT        |
| 2.2 | Disk flushes per second      | `sum(rate(node_disk_flush_requests_total[w]))`                                        | fsyncs reaching the disk               |
| 2.3 | Flushes per accepted request | flushes rate / 202 rate                                                               | commits sharing a flush                |
| 2.4 | Pool in use / waiting        | `ep_pg_pool_total - ep_pg_pool_idle`, `ep_pg_pool_waiting`                            | pool pressure                          |
| 2.5 | Commits per second           | `rate(pg_stat_database_xact_commit[w])`                                               | commits at Postgres                    |
| 2.6 | WAL bytes per second         | `rate(ep_pg_wal_bytes_total[w])`                                                      | WAL volume                             |
| 3.1 | Postgres container CPU       | `sum(rate(ep_pg_container_cpu_seconds_total[w]))`                                     | Postgres cores used                    |
| 3.2 | Node CPU                     | `rate(process_cpu_seconds_total[w])`                                                  | Node cores used                        |
| 3.3 | Disk busy                    | `100*max(rate(node_disk_io_time_seconds_total[w]))`                                   | busiest disk %util                     |
| 3.4 | In flight                    | `sum(http_requests_in_flight)`                                                        | requests in the server                 |
| 3.5 | CPU by pinned group          | `100*(1-avg(rate(node_cpu_seconds_total{mode="idle",cpu=~"$cpu_<group>"}[w])))`       | busy % per group                       |
| 3.6 | Connections by state         | `sum by (state)(pg_stat_activity_count)`                                              | active vs idle                         |
| 4.1 | Dropped iterations           | `rate(k6_dropped_iterations_total[w])`                                                | generator or server limit              |
| 4.2 | VUs                          | `sum(k6_vus)`, `sum(k6_vus_max)`                                                      | generator headroom                     |
| 4.3 | k6 CPU group                 | as 3.5, k6 group                                                                      | keep under about 70%                   |
| 4.4 | Server CPU group             | as 3.5, server group                                                                  | Node thread saturation                 |

Verify after a run, in Grafana Explore: `count by (sync, phase)(k6_b6_resp_202_total)` and `max(pg_up{job="postgres"})`.

## 5. Run it

From `loadtests/k6/stage-1/B6/` (the stack from `infra/compose` must be up):

(a) Dry run, prints the plan and runs nothing:

```bash
DRY_RUN=1 ./run.sh
```

(b) Trial, one mode, one rate, one repeat, NOT a result:

```bash
MODES="off" PHASES="at" REPEATS=1 DURATION=30s PROBES=0 RESULTS_NAME=b6-trial ./run.sh
```

(b2) Crash trial, one probe:

```bash
MEASURED=0 MODES="off" CRASH_REPS=1 RESULTS_NAME=b6-trial-crash ./run.sh
```

(c) The full experiment:

```bash
./run.sh
```

(d) One case by hand: set the role setting and start the server as `run.sh` does (section 1), then run `runner.sh` from `loadtests/k6` with `LABEL=b6-by-hand SCRIPT=stage-1/B6/script.js RATE=4000 DURATION=45s PHASE=at SYNC=on RESULTS_ROOT=.../B6/results RESULTS_NAME=b6-by-hand ROUTE=/healthz ./runner.sh`.

(e) If the script is killed hard (kill -9) and its trap did not run:

```bash
./run.sh RESTORE
```

To watch during a run, in another terminal: `watch -n1 'docker exec ep-postgres psql -U eventpulse -d eventpulse -Atc "show synchronous_commit"'` (a new session reads the role setting) and `docker ps --filter name=ep-postgres`.

Results go to `results/<RESULTS_NAME>/`: `summary.tsv` (one row per measured run), `summary-crash.tsv` (one row per crash probe; separate because the analyzer needs the same columns in one file), `runs.tsv` (epoch start/end per run, to zoom Grafana), `env.txt`, `captures/` (one text file per run: snapshots, wait events, crash details), `server.out`, and the k6 lap JSONs with a `manifest.md` per timestamp folder. `results/` is in `.gitignore`.

## 6. What this script changes on my machine

| What                                                                                                                                      | How it is restored                                                                            |
| ----------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `ALTER ROLE eventpulse SET synchronous_commit = on/off` (database level, role setting)                                                    | `ALTER ROLE ... RESET synchronous_commit` in the EXIT trap and in `./run.sh RESTORE`          |
| `TRUNCATE events` before every run (the table is emptied; the rows left from earlier experiments are lost)                                | not restored (the table is test data)                                                         |
| `CHECKPOINT` before every run                                                                                                             | nothing to restore                                                                            |
| `docker kill -s KILL ep-postgres` in the crash probes, then `docker start` if docker's restart policy has not already restarted it        | the script starts it again and waits until it answers; the trap also makes sure it is running |
| starts and stops `node ... dist/main.js` (pid kept in the script)                                                                         | stopped by the trap                                                                           |
| starts `pg-container-exporter.sh` and `pg-wal-exporter.sh` in the background, writes node_exporter textfiles in `infra/compose/textfile/` | stopped by the trap                                                                           |

No `sudo`, no `sysctl`, no `tc`. Postgres-exporter and Toxiproxy are not touched, but postgres-exporter loses its connection for a few seconds during each kill.

## 7. Analyze in Grafana

Dashboard: [B6 - Durability knob.json](../../../../infra/observability/dashboards/stage-1/B6%20-%20Durability%20knob.json). Import it in Grafana through Dashboards, New, Import, upload the file, and pick the Prometheus datasource for `ds`. The `cpu_*` variables hold the pinned CPU ids and can be edited.

Order to read: row 1 (verdict) first, with the time range set to the epochs in `runs.tsv` of a run; then row 2 (what happened to the commit wait and the flushes), then row 3 (CPU), and always row 4 (is the run valid) before trusting a number. Zoom to one crash probe to see the `pg_up` dip and the 503/other jump.
