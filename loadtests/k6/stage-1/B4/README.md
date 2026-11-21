# B4 Lock contention with a hot row

Why this experiment and what I expect: see `notebook/stage-1/experiments/B4/Explanation.md`

## 1. Run the server

`run.sh` starts and restarts ingest-api itself (`MANAGE_SERVER=1`, the default), once per run, so the pool starts empty and every run has its own tenant keys:

```bash
cd services/ingest-api && env API_KEYS=<epk_b4_0:t0,...> COUNTER_MODE=<hot|off> PG_POOL_MAX=<25|50> taskset -c 0,6 node --env-file=.env dist/main.js
```

- `API_KEYS` is generated per run: key `i` is `epk_b4_<i>` and belongs to tenant `t<i>`, for `i = 0 .. tenants-1`. k6 uses the same names (`KEY_PREFIX`). Environment variables win over `--env-file`, so `.env` is not edited.
- The server connects straight to Postgres (`DATABASE_URL` from `.env`, port 5434). No Toxiproxy in B4.
- `COUNTER_MODE=off` is the default of the server, so every other experiment behaves as before. `hot` runs `BEGIN; INSERT; UPDATE tenant_counters ...; COMMIT` on one pool connection.
- Before the first run: `pnpm build` in `services/ingest-api` (the build must contain `COUNTER_MODE`), and apply migration 0003 once:

```bash
cd services/ingest-api && pnpm migrate:up
```

- Start the stack if it is not running:

```bash
docker compose -f infra/compose/docker-compose.yml up -d
```

- CPU pinning: server `0,6` (taskset), Postgres `1,7,2,8`, k6 `3,9,4,10`, monitoring `5,11`. The container cpusets come from `infra/compose/docker-compose.yml`; `run.sh` only prints them into `env.txt`.

## 2. Run configuration

- Variable: number of tenants sharing the load, **1, 10, 100, 1000**, in `hot` mode (every insert also increments its tenant's counter row). Control: `off` mode (no counter, no transaction) at 1 and 1000 tenants.
- Cells (`mode:tenants`): `hot:1 hot:10 hot:100 hot:1000 off:1 off:1000`.
- Pool: fixed at 25. Same value as B3.
- Two phases per cell, the same offered rate for every cell: `under` 150 req/s, `over` 3000 req/s.
- 6 cells x 2 phases x 3 repeats x 45 s = 36 measured runs. Odd repeats go through the cells as listed, even repeats reversed.
- One cheap capture in the middle of every `over` run (wait events, blocked sessions, `pg_locks`, one psql call). Prometheus holds the same numbers every second.
- Probe run (separate, never a headline number): `probe_pool50` = `hot:1`, pool 50, 3000 req/s, 45 s.
- Per run: restart the server, seed one `tenant_counters` row per tenant, warm up (400 requests on tenant `t0`), check the counter moved (hot) or stayed 0 (off), truncate `events`, run k6, row audit (rows in `events` against the sum of the counters).
- Total: about 55 min (printed by the dry run).

| Env knob | Default | Meaning |
| --- | --- | --- |
| `CELLS` | `hot:1 hot:10 hot:100 hot:1000 off:1 off:1000` | `mode:tenants` cells |
| `PHASES` | `under over` | which phases to run |
| `POOL` | `25` | `PG_POOL_MAX` of the measured runs |
| `UNDER_RATE` / `OVER_RATE` | `150` / `3000` | offered req/s of each phase |
| `REPEATS` | `3` | repeats of every cell |
| `DURATION` | `45s` | k6 length of a measured run |
| `PROBES` / `PROBE_POOL` | `1` / `50` | run the pool probe, and its pool size |
| `KEY_PREFIX` | `epk_b4_` | api key `i` is `KEY_PREFIX` + `i` |
| `RESULTS_NAME` | `b4` | results folder name |
| `MANAGE_SERVER` | `1` | 0 = attach to a server I started myself (then `API_KEYS`/`COUNTER_MODE` must match the cell) |
| `SERVER_CPUS`, `K6_CPUS` | `0,6`, `3,9,4,10` | pinning |
| `HOLD_MS`, `CEIL_RPS` | `3.0`, `3800` | only for the `cap_pred_rps` column (see `hook.py`) |

## 3. Instrumentation needed

**Server, custom (already in the code):** `ep_pg_pool_wait_seconds`, `ep_pg_pool_waiting`, `ep_pg_query_duration_seconds` (now covers the whole transaction in hot mode), `ep_db_errors_by_type_total`, `http_requests_in_flight`, `eventpulse_event_loop_delay_seconds`.

**Server, added by B4:**

- `ep_pg_counter_update_seconds` (histogram, buckets 0.5 ms to 5 s, no labels) in `services/ingest-api/src/observability/metrics.service.ts`, observed in `services/ingest-api/src/events/events.service.ts`. **ADDED BY B4.**
- `COUNTER_MODE` (`off|hot`) in `services/ingest-api/src/config.ts` and the hot transaction in `events.service.ts`. **ADDED BY B4.**
- Table `tenant_counters` in `db/migrations/0003_tenant_counters.sql`. **ADDED BY B4.**

**Postgres exporter:** `pg_stat_activity_count` (labels `state`, `wait_event_type`, `wait_event`), `pg_locks_count`, `pg_stat_database_xact_commit`, `pg_stat_database_tup_updated`, `pg_stat_database_deadlocks`.

**Container exporter** (`infra/scripts/pg-container-exporter.sh`, started by `run.sh`): `ep_pg_container_cpu_seconds_total`.

**k6:** `k6_http_req_duration_seconds`, `k6_http_reqs_total`, `k6_dropped_iterations_total`, and `k6_b4_resp_202_total` (**ADDED BY B4**, in `script.js`; `b4_resp_503` and `b4_resp_other` are counted too). k6 series carry the labels `exp`, `tenants`, `counter`, `pool_max`, `offered_rate`, `phase`, `testid`.

**node_exporter:** `node_cpu_seconds_total`. **ingest-api default:** `process_cpu_seconds_total`.

Note: `ep_pg_counter_update_seconds_bucket` and `k6_b4_resp_202_total` were not in the Prometheus inventory when the dashboard was validated (declared pending). The first is confirmed on the server's `/metrics`; the second appears after the first k6 run.

## 4. Grafana panels

Query column: job filters omitted, `[w]` = `$__rate_interval`.

| # | Panel | Query | What it shows |
| --- | --- | --- | --- |
| 1 | Accepted requests per second | `sum by (counter,tenants,phase)(rate(k6_b4_resp_202_total[w]))` | capacity per cell |
| 2 | Latency p50 and p99 | `histogram_quantile(0.99, sum by (counter,tenants,phase)(rate(k6_http_req_duration_seconds[w])))` | client latency |
| 3 | Not accepted (%) | `100*(1 - sum(rate(k6_b4_resp_202_total[w])) / sum(rate(k6_http_reqs_total[w])))` | failures (pool timeouts) |
| 4 | Postgres sessions by wait event | `sum by (wait_event_type,wait_event)(pg_stat_activity_count{wait_event!=""})` | who waits for what |
| 5 | Sessions by state | `sum by (state)(pg_stat_activity_count)` | active vs idle in transaction |
| 6 | Counter UPDATE latency | `histogram_quantile(0.99, sum(rate(ep_pg_counter_update_seconds_bucket[w])))` | lock wait seen by the server |
| 7 | Pool waiting / wait p99 | `max(ep_pg_pool_waiting)`, `histogram_quantile(0.99, ... ep_pg_pool_wait_seconds_bucket)` | queue in the API |
| 8 | Commits and row updates per s | `rate(pg_stat_database_xact_commit[w])`, `rate(pg_stat_database_tup_updated[w])` | transaction rate |
| 9 | Locks by mode and deadlocks | `sum by (mode)(pg_locks_count)`, `rate(pg_stat_database_deadlocks[w])` | lock population |
| 10 | CPU busy by pinned group | `100*(1-avg(rate(node_cpu_seconds_total{mode="idle",cpu=~"$cpu_x"}[w])))` | CPU per group |
| 11 | Postgres container CPU | `sum(rate(ep_pg_container_cpu_seconds_total[w]))` | Postgres cores used |
| 12 | Server process CPU | `sum(rate(process_cpu_seconds_total[w]))` | Node cores used |
| 13 | k6 dropped iterations | `sum by (counter,tenants,phase)(rate(k6_dropped_iterations_total[w]))` | generator validity |
| 14 | In flight and event-loop delay | `sum(http_requests_in_flight)`, `max(eventpulse_event_loop_delay_seconds)` | server health |
| 15 | DB errors by type | `sum by (type)(rate(ep_db_errors_by_type_total[w]))` | error causes |

Verify in Grafana Explore after a run: `count by (counter, tenants)(k6_http_reqs_total)` and `count by (wait_event_type, wait_event)(pg_stat_activity_count{wait_event!=""})`.

## 5. Run it

Dry run (prints the plan, runs nothing):

```bash
cd loadtests/k6/stage-1/B4 && DRY_RUN=1 ./run.sh
```

Trial (plumbing check, NOT a result):

```bash
cd loadtests/k6/stage-1/B4 && CELLS="hot:1" PHASES="over" REPEATS=1 DURATION=30s PROBES=0 RESULTS_NAME=b4-trial ./run.sh
```

Full run (one command):

```bash
cd loadtests/k6/stage-1/B4 && ./run.sh
```

One case by hand: start the server with the three env vars of section 1, seed the counters (`INSERT INTO tenant_counters SELECT 't'||g, 0 FROM generate_series(0, N-1) g`), then:

```bash
cd loadtests/k6 && env LABEL=b4-manual SCRIPT=stage-1/B4/script.js RATE=3000 DURATION=45s TENANTS=1 COUNTER=hot POOL_MAX=25 PHASE=over RESULTS_NAME=b4-manual ROUTE=/healthz ./runner.sh
```

While it runs, in another terminal (read only):

```bash
docker exec -i ep-postgres psql -U eventpulse -d eventpulse -c "select wait_event_type, wait_event, count(*) from pg_stat_activity group by 1,2 order by 3 desc"
```

Results go to `loadtests/k6/stage-1/B4/results/<RESULTS_NAME>/`: `summary.tsv` (one row per run, table printed at the end), `runs.tsv` (start and end epoch per run), `env.txt`, `captures/<testid>.txt` (snapshots before and after, mid-run capture), `server.out`, the k6 lap JSONs.

## 6. What this script changes on my machine

- No `sudo`, no `sysctl`, no `tc`, no docker compose commands.
- Starts and stops ingest-api (`kill` of the pid it started) and `infra/scripts/pg-container-exporter.sh` (stopped on exit).
- Writes to the database: `TRUNCATE events`, `TRUNCATE tenant_counters` and re-seed, `pg_stat_statements_reset()`, `CHECKPOINT`, and the warm-up/test events. It deletes all rows in `events` at the start of every run.
- Creates the results folder inside this folder. Nothing is restored afterwards because nothing outside the database is changed.
- Migration 0003 (creates `tenant_counters`) is run by me by hand (`pnpm migrate:up`), not by `run.sh`.

## 7. Analyze in Grafana

Dashboard: [`B4 - Lock contention.json`](../../../../infra/observability/dashboards/stage-1/B4%20-%20Lock%20contention.json). Import: Grafana, Dashboards, New, Import, upload the file, pick the Prometheus datasource for `ds`. Set the time range to the epochs in `runs.tsv` of the run I want to look at. Order to read: row 1 (outcome per cell), row 2 (wait events, counter UPDATE latency, pool queue), row 3 (CPU), row 4 (validity: dropped iterations must be 0 in `under` runs).
