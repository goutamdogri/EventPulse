# B5 Index write amplification

Why this experiment and what I expect: see `notebook/stage-1/experiments/B5/Explanation.md`

## 1. Run the server

`run.sh` starts and restarts ingest-api itself (`MANAGE_SERVER=1`, the default), once per run, so the pool starts empty:

```bash
cd services/ingest-api && env API_KEYS=<epk_b5_0:t0,...,epk_b5_99:t99> COUNTER_MODE=off PG_POOL_MAX=25 taskset -c 0,6 node --env-file=.env dist/main.js
```

- `API_KEYS` is generated per run: key `i` is `epk_b5_<i>` and belongs to tenant `t<i>`, for `i = 0 .. 99`. k6 uses the same names (`KEY_PREFIX`). Environment variables win over `--env-file`, so `.env` is not edited.
- The server connects straight to Postgres (`DATABASE_URL` from `.env`, port 5434). No Toxiproxy in B5.
- `run.sh` refuses to start if something already answers on `BASE_URL` (port 8000). Set `MANAGE_SERVER=0` to attach to a server I started myself (then `API_KEYS` must hold the 100 keys).
- Before the first run: `pnpm build` in `services/ingest-api`. No migration is needed (the table `events` already exists).
- Start the stack if it is not running:

```bash
docker compose -f infra/compose/docker-compose.yml up -d
```

- CPU pinning: server `0,6` (taskset), Postgres `1,7,2,8` (and a 2-CPU quota), k6 `3,9,4,10`, monitoring `5,11`. The container cpusets come from `infra/compose/docker-compose.yml`; `run.sh` only prints them into `env.txt`.

## 2. Run configuration

- Variables: number of secondary indexes on `events` (levels 0, 1, 3, 5) and the kind of primary key `event_id` (random v4 or time-ordered v7). The primary key always exists.
- Index levels, cumulative:

| Level | Secondary indexes on `events`                                                             |
| ----- | ----------------------------------------------------------------------------------------- |
| 0     | none (primary key only)                                                                   |
| 1     | `events_tenant_time_idx (tenant_id, occurred_at)`                                         |
| 3     | level 1 + `b5_type_idx (type)` + `b5_tenant_type_time_idx (tenant_id, type, occurred_at)` |
| 5     | level 3 + `b5_payload_gin` (GIN on `payload`) + `b5_received_idx (received_at)`           |
| 1g    | probe only: level 1 + `b5_payload_gin`                                                    |

- Cells (`key:level`): `v4:0 v7:0 v4:1 v7:1 v4:3 v7:3 v4:5 v7:5`.
- Constant: 100 tenants picked uniformly, 10 event types picked uniformly, pool 25, payload about 500 B, `COUNTER_MODE=off`, Postgres settings unchanged.
- Two phases per cell, the same offered rate for every cell: `under` 1500 req/s and `over` 6000 req/s.
- 8 cells x 2 phases x 3 repeats x 45 s = 48 measured runs. Odd repeats go through the cells as listed, even repeats reversed.
- Probe runs (separate, never headline numbers), `over` rate, 45 s: `probe_gin` (v4, level 1g), `probe_pre_v4` and `probe_pre_v7` (level 1, table pre-loaded with 2,000,000 rows through normal INSERTs, then `VACUUM ANALYZE` and `CHECKPOINT`).
- Per run: restart the server, warm up (400 requests on tenant `t0`), `TRUNCATE events`, drop all indexes and build the ones of the cell, `CHECKPOINT`, snapshot, k6, snapshot, sizes of the table and every index, row audit (rows added to `events` against 202 responses).
- One cheap capture in the middle of every `over` run (wait events, `pg_stat_wal`, one psql call).
- Total: about 81 min (printed by the dry run).

| Env knob                   | Default                                   | Meaning                                            |
| -------------------------- | ----------------------------------------- | -------------------------------------------------- |
| `CELLS`                    | `v4:0 v7:0 v4:1 v7:1 v4:3 v7:3 v4:5 v7:5` | `key:level` cells                                  |
| `PHASES`                   | `under over`                              | which phases to run                                |
| `UNDER_RATE` / `OVER_RATE` | `1500` / `6000`                           | offered req/s of each phase                        |
| `REPEATS`                  | `3`                                       | repeats of every cell                              |
| `DURATION`                 | `45s`                                     | k6 length of a measured run                        |
| `TENANTS`                  | `100`                                     | tenants (api keys) sharing the load                |
| `POOL`                     | `25`                                      | `PG_POOL_MAX` of every run                         |
| `PROBES`                   | `1`                                       | 1 = also run the three probe runs                  |
| `PRELOAD_ROWS`             | `2000000`                                 | rows in the table for the pre-loaded probes        |
| `KEY_PREFIX`               | `epk_b5_`                                 | api key `i` is `KEY_PREFIX` + `i`                  |
| `RESULTS_NAME`             | `b5`                                      | results folder name                                |
| `MANAGE_SERVER`            | `1`                                       | 0 = attach to a server I started myself            |
| `SERVER_CPUS`, `K6_CPUS`   | `0,6`, `3,9,4,10`                         | pinning                                            |
| `BASE_RPS`                 | `4820`                                    | only for the `cap_pred_rps` column (see `hook.py`) |

## 3. Instrumentation needed

**Server, custom (already in the code):** `ep_pg_pool_wait_seconds`, `ep_pg_pool_waiting`, `ep_db_errors_by_type_total`, `http_requests_in_flight`, `eventpulse_event_loop_delay_seconds`. B5 changes nothing in the server code.

**prom-client defaults:** `process_cpu_seconds_total`.

**Postgres exporter (`ep-postgres-exporter`):** `pg_stat_activity_count` (labels `state`, `wait_event_type`, `wait_event`), `pg_stat_bgwriter_checkpoints_timed_total`, `pg_stat_bgwriter_checkpoints_req_total`, `pg_stat_bgwriter_buffers_checkpoint_total`, `pg_stat_user_tables_index_size_bytes`, `pg_stat_user_tables_table_size_bytes`, `pg_stat_database_blks_hit`, `pg_stat_database_blks_read`, `pg_stat_database_tup_inserted`. This exporter has no WAL volume metric: its `xlog_location` collector only works on Postgres before 10 (it logs "not available" on 15 and exports nothing), and `pg_wal_size_bytes` is the size of the folder, not the amount written. So:

**WAL exporter, ADDED BY B5** (`infra/scripts/pg-wal-exporter.sh`, started by `run.sh`, one psql session reading `pg_stat_wal` once a second, written for node_exporter's textfile directory): `ep_pg_wal_bytes_total`, `ep_pg_wal_records_total`, `ep_pg_wal_fpi_total`, `ep_pg_wal_buffers_full_total`.

**Container exporter** (`infra/scripts/pg-container-exporter.sh`, started by `run.sh`): `ep_pg_container_cpu_seconds_total`, `ep_pg_container_cpu_throttled_seconds_total`.

**k6:** `k6_http_req_duration_seconds`, `k6_http_reqs_total`, `k6_dropped_iterations_total`, and `k6_b5_resp_202_total` (**ADDED BY B5**, in `script.js`; `b5_resp_503` and `b5_resp_other` are counted too). k6 series carry the labels `exp`, `key`, `idx`, `tenants`, `preload`, `pool_max`, `offered_rate`, `phase`, `testid`.

**node_exporter:** `node_cpu_seconds_total`.

**Only in `summary.tsv` and `captures/`, not in Prometheus:** exact WAL bytes per row (difference of the WAL position before and after the run, divided by the rows added), full-page images per row, size of the primary key and of every secondary index (`pg_relation_size`), checkpoints and autovacuum runs during the run.

## 4. Grafana panels

Query column: job filters omitted, `[w]` = `$__rate_interval`.

| #   | Panel                                 | Query                                                                                                        | What it shows                |
| --- | ------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ---------------------------- |
| 1   | Accepted requests per second          | `sum by (key,idx,phase)(rate(k6_b5_resp_202_total[w]))`                                                      | capacity per cell            |
| 2   | Client latency p50 and p99            | `histogram_quantile(0.99, sum by (key,idx,phase)(rate(k6_http_req_duration_seconds[w])))`                    | latency                      |
| 3   | Requests not accepted (%)             | `100*(1 - sum(rate(k6_b5_resp_202_total[w])) / sum(rate(k6_http_reqs_total[w])))`                            | failures                     |
| 4   | WAL written per second                | `rate(ep_pg_wal_bytes_total[w])`                                                                             | WAL volume                   |
| 5   | WAL bytes per accepted row            | `sum(rate(ep_pg_wal_bytes_total[w])) / sum(rate(k6_b5_resp_202_total[w]))`                                   | write amplification          |
| 6   | Full-page images and WAL records      | `rate(ep_pg_wal_fpi_total[w])`, `sum(rate(ep_pg_wal_records_total[w])) / sum(rate(k6_b5_resp_202_total[w]))` | page images, records per row |
| 7   | Table and index size                  | `sum(pg_stat_user_tables_index_size_bytes{relname="events"})`, `..._table_size_bytes`                        | growth                       |
| 8   | Checkpoints                           | `rate(pg_stat_bgwriter_checkpoints_timed_total[w])`, `..._req_total`, `..._buffers_checkpoint_total`         | checkpoint behaviour         |
| 9   | Postgres sessions by wait event       | `sum by (wait_event_type,wait_event)(pg_stat_activity_count{wait_event!=""})`                                | who waits for what           |
| 10  | Buffer cache hit ratio                | `100*sum(rate(pg_stat_database_blks_hit[w])) / (hit + read)`                                                 | cache misses                 |
| 11  | Rows inserted per second              | `sum(rate(pg_stat_database_tup_inserted[w]))`                                                                | Postgres-side rate           |
| 12  | CPU busy by pinned group              | `100*(1-avg(rate(node_cpu_seconds_total{mode="idle",cpu=~"$cpu_x"}[w])))`                                    | CPU per group                |
| 13  | Postgres container CPU and throttling | `sum(rate(ep_pg_container_cpu_seconds_total[w]))`, `rate(ep_pg_container_cpu_throttled_seconds_total[w])`    | cores used, quota hit        |
| 14  | Postgres CPU per accepted row         | `1000*sum(rate(ep_pg_container_cpu_seconds_total[w])) / sum(rate(k6_b5_resp_202_total[w]))`                  | CPU cost of a row (ms)       |
| 15  | Node process CPU                      | `sum(rate(process_cpu_seconds_total[w]))`                                                                    | Node cores used              |
| 16  | k6 dropped iterations                 | `sum by (key,idx,phase)(rate(k6_dropped_iterations_total[w]))`                                               | generator validity           |
| 17  | Pool waiting and pool wait p99        | `max(ep_pg_pool_waiting)`, `histogram_quantile(0.99, ... ep_pg_pool_wait_seconds_bucket)`                    | queue in the API             |
| 18  | In flight and event-loop delay        | `sum(http_requests_in_flight)`, `max(eventpulse_event_loop_delay_seconds)`                                   | server health                |
| 19  | Database errors by type               | `sum by (type)(rate(ep_db_errors_by_type_total[w]))`                                                         | error causes (empty if none) |

Verify in Grafana Explore after a run: `count by (key, idx)(k6_http_reqs_total)` and `ep_pg_wal_bytes_total`.

## 5. Run it

Dry run (prints the plan, runs nothing):

```bash
cd loadtests/k6/stage-1/B5 && DRY_RUN=1 ./run.sh
```

Trial (plumbing check, NOT a result):

```bash
cd loadtests/k6/stage-1/B5 && CELLS="v4:3" PHASES="over" REPEATS=1 DURATION=30s PROBES=0 RESULTS_NAME=b5-trial ./run.sh
```

Full run (one command):

```bash
cd loadtests/k6/stage-1/B5 && ./run.sh
```

One case by hand: create the indexes of the level in Postgres (see `run.sh`, section "Index sets"), start the server with the three env vars of section 1, then:

```bash
cd loadtests/k6 && env LABEL=b5-manual SCRIPT=stage-1/B5/script.js RATE=6000 DURATION=45s KEY_KIND=v4 IDX=3 TENANTS=100 PHASE=over PRELOAD=0 POOL_MAX=25 RESULTS_NAME=b5-manual ROUTE=/healthz ./runner.sh
```

While it runs, in another terminal (read only):

```bash
docker exec -i ep-postgres psql -U eventpulse -d eventpulse -c "select wait_event_type, wait_event, count(*) from pg_stat_activity group by 1,2 order by 3 desc"
```

If `run.sh` was killed hard (`kill -9`) and the indexes were not restored, put the table back as migration 0002 made it:

```bash
cd loadtests/k6/stage-1/B5 && ./run.sh RESTORE
```

Results go to `loadtests/k6/stage-1/B5/results/<RESULTS_NAME>/`: `summary.tsv` (one row per run, table printed at the end), `runs.tsv` (start and end epoch per run), `env.txt`, `captures/<testid>.txt` (counters before and after, sizes, mid-run capture), `server.out`, the exporter logs, the k6 lap JSONs. In the capture files the first block of numbers is `wal_bytes|wal_records|wal_fpi|ckpt_timed|ckpt_req|buffers_ckpt|autovacuum|blks_read|blks_hit`: subtract pre from post.

## 6. What this script changes on my machine

- No `sudo`, no `sysctl`, no `tc`, no docker compose commands, no change to the Postgres configuration.
- Database (every run): `TRUNCATE events`, `DROP INDEX` of all indexes of the experiment including `events_tenant_time_idx`, then `CREATE INDEX` of the cell's set, `pg_stat_statements_reset()`, `CHECKPOINT`, plus the warm-up and test events. Pre-loaded probes also insert 2,000,000 rows and run `VACUUM (ANALYZE) events`.
- Restore: when `run.sh` exits (also on Ctrl+C) it drops every `b5_*` index and recreates `events_tenant_time_idx`. If it is killed hard, `./run.sh RESTORE` does the same.
- Starts and stops ingest-api (`kill` of the pid it started).
- Starts and stops two background scripts: `infra/scripts/pg-container-exporter.sh` and `infra/scripts/pg-wal-exporter.sh`. Each writes one file in `infra/compose/textfile/` and removes it on exit; the WAL one keeps one psql session open while it runs.
- Creates the results folder inside this folder. Nothing outside the database is changed.

## 7. Analyze in Grafana

Dashboard: [`B5 - Index write amplification.json`](../../../../infra/observability/dashboards/stage-1/B5%20-%20Index%20write%20amplification.json). Import: Grafana, Dashboards, New, Import, upload the file, pick the Prometheus datasource for `ds`. Set the time range to the epochs in `runs.tsv` of the run I want to look at. Order to read: row 1 (outcome per cell), row 2 (WAL, sizes, checkpoints, wait events), row 3 (CPU per group, Postgres CPU per row), row 4 (validity: dropped iterations must be 0 in `under` runs).
