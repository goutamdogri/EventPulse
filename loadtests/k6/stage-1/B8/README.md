# B8 Database restart under load

Why this experiment and what I expect: see [notebook/stage-1/experiments/B8/Explanation.md](../../../../notebook/stage-1/experiments/B8/Explanation.md)

## 1. Run the server

`run.sh` starts and stops the ingest-api itself (`MANAGE_SERVER=1`, the default), once before every run, so every run begins with new pool connections. The command it uses (same as B3 to B7, but with the proxy URL):

```bash
cd services/ingest-api && DATABASE_URL=postgres://...@localhost:15434/eventpulse API_KEYS=<epk_b8_0:t0,...> COUNTER_MODE=off PG_POOL_MAX=25 taskset -c 0,6 node --env-file=.env dist/main.js
```

- The build must exist and must contain the B8 instrumentation: `pnpm build` in `services/ingest-api` (the preflight checks for `ep_db_errors_by_code_total` in `dist/`).
- Nothing else may answer on `localhost:8000` when I start (the preflight checks).
- The server connects THROUGH Toxiproxy in every variant (`localhost:15434` -> `postgres:5432`), not straight to `localhost:5434`. The proxy `pg` is created by `run.sh` if it is missing. The URL is the `DATABASE_URL` of `.env` with only host and port replaced.
- Not set by `run.sh`, so the values of `.env` apply: `PG_CONN_TIMEOUT_MS=2000` (pool waits at most 2 s for a free or new connection), `PG_STATEMENT_TIMEOUT_MS=5000`, `RETRY_AFTER_S=1`. There is no client-side query timeout in the server.
- Postgres is the compose container `ep-postgres` (postgres:15, `restart: unless-stopped`). Docker stops it with its image's stop signal (recorded in `env.txt`).
- CPU pinning (from compose, not changed by the script): server 0,6 (taskset), Postgres 1,7,2,8, k6 3,9,4,10 (`K6_CPUS`), monitoring and toxiproxy 5,11. The two probe loops run on 5,11.

## 2. Run configuration

- Variable: the fault `VARIANT` = `restart`, `pause_reset`, `pause_blackhole`, `none` (control).
  - `restart`: `docker restart -t 30 ep-postgres`; the run script waits until a query works again.
  - `pause_reset`: Toxiproxy proxy `pg` disabled for `PAUSE_S` seconds, then enabled.
  - `pause_blackhole`: Toxiproxy `timeout` toxic with `timeout=0`, on the downstream and the upstream direction, for `PAUSE_S` seconds, then removed.
  - `none`: no fault.
- Every run: restart server, warm-up (400 requests), `TRUNCATE events`, checkpoint, 5 s settle, then k6 at `RATE` req/s (constant-arrival-rate) for `DURATION`. The fault starts `FAULT_AT` s after k6.
- 4 variants x 3 repeats x 120 s = 12 runs, about 34 min with settling. Repeat is the outer loop; odd repeats run the variants in the listed order, even repeats in reverse.
- Two probe loops per run (0.25 s cadence, pinned to cores 5,11): `<testid>.db.tsv` (pg_isready inside the container, not through the proxy) and `<testid>.api.tsv` (GET `/readyz`, 1 s timeout). `hook.py` turns them into the timeline columns of `summary.tsv`.
- The analyzer's collapse limits are switched off (errors are expected in a fault run); the only verdict that can fire is `LOST-ROWS`.

| Env knob                 | Default                                         | What it does                                          |
| ------------------------ | ----------------------------------------------- | ----------------------------------------------------- |
| `VARIANTS`               | `restart pause_reset pause_blackhole none`      | which faults to run                                   |
| `RATE`                   | `2000`                                          | offered req/s                                         |
| `DURATION`               | `120s`                                          | duration of every k6 run                              |
| `FAULT_AT`               | `30`                                            | seconds after k6 start when the fault is injected     |
| `PAUSE_S`                | `10`                                            | length of both network pauses                         |
| `REPEATS`                | `3`                                             | repeats of every variant                              |
| `POOL`                   | `25`                                            | `PG_POOL_MAX` of the server                           |
| `TENANTS`                | `100`                                           | tenants (api keys)                                    |
| `PRE_VUS`, `MAX_VUS`     | `1000`, `8000` (defaults of `script.js`)        | k6 virtual users (a pause makes many wait at once)    |
| `SERVER_CPUS`, `K6_CPUS` | `0,6`, `3,9,4,10`                               | pinning                                               |
| `PROBE_CPUS`             | `5,11`                                          | pinning of the two probe loops                        |
| `TOXI_URL`, `TOXI_PORT`  | `http://localhost:8474`, `15434`                | Toxiproxy control API and proxied port                |
| `MANAGE_SERVER`          | `1`                                             | `0` = attach to a server I started myself             |
| `RESULTS_NAME`           | `b8`                                            | results folder name                                   |
| `DRY_RUN`                | `0`                                             | `1` prints the plan and runs nothing                  |

## 3. Instrumentation needed

- prom-client defaults used: `process_cpu_user_seconds_total`, `process_cpu_system_seconds_total`, `nodejs_eventloop_lag_p99_seconds`.
- Custom server metrics used (they already exist): `http_requests_total`, `http_requests_in_flight`, `http_connections_open`, `ep_pg_pool_total`, `ep_pg_pool_idle`, `ep_pg_pool_waiting`, `ep_pg_pool_max`, `ep_pg_pool_wait_seconds_bucket`, `ep_pg_query_duration_seconds_bucket`.
- Custom server metrics **ADDED BY B8**:
  - `ep_db_errors_by_code_total{code}` (counter) in `services/ingest-api/src/observability/metrics.service.ts`, incremented in `services/ingest-api/src/common/all-exceptions.filter.ts` next to the existing counters. Label values: the SQLSTATE or Node error code, or `pool_timeout`, `terminated`, `none`. The existing `ep_db_errors_total` and `ep_db_errors_by_type_total` are unchanged.
  - `ep_pg_pool_connects_total`, `ep_pg_pool_removes_total`, `ep_pg_pool_idle_errors_total` (counters) in `metrics.service.ts`, fed by the `connect`, `remove` and `error` events of the pg pool.
- Exporter metrics used (they already exist): `pg_up`, `pg_stat_activity_count`, `pg_stat_database_xact_commit` (postgres-exporter, job `postgres`).
- k6 metrics used: `k6_http_req_duration_seconds` (native histogram), `k6_dropped_iterations_total`, `k6_vus`, `k6_vus_max`; custom counters defined in `script.js` (**ADDED BY B8**): `k6_b8_resp_202_total`, `k6_b8_resp_503_total`, `k6_b8_resp_other_total` (they exist in Prometheus only after a k6 run).
- node_exporter metrics used: `node_cpu_seconds_total`.
- Not in Prometheus, taken by `run.sh` into the captures and `summary.tsv`: the probe files (`*.db.tsv`, `*.api.tsv`), the server counters at the end of a run (error codes, pool connects and removes), the pool state at the end, the Postgres log lines of the restart, and the row count.

## 4. Grafana panels

`[w]` = `$__rate_interval`. Job filters shortened: server `eventpulse-nodejs`, exporter `postgres`, node `node`.

| #  | Panel                                         | Query                                                                                                    | What it shows                                              |
| -- | --------------------------------------------- | -------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| 1  | Responses per second the client got (k6)      | `sum(rate(k6_b8_resp_202_total[w]))`, same for `_503_` and `_other_`                                     | what the producer sees over time                           |
| 2  | Events lost by a producer without retries (%) | `100*(503+other)/(202+503+other)` rates                                                                  | share of events not accepted                               |
| 3  | Client latency p50 / p99                      | `histogram_quantile(0.99, sum(rate(k6_http_req_duration_seconds[w])))`                                   | fast failures vs waiting requests                          |
| 4  | Responses per second the server counted       | `sum by (status)(rate(http_requests_total{route!~"/metrics"}[w]))`                                       | the server's view by status                                |
| 5  | Is Postgres up?                               | `pg_up{job="postgres"}`                                                                                  | the restart window                                         |
| 6  | 503s by raw error code                        | `sum by (code)(rate(ep_db_errors_by_code_total[w]))`                                                     | which error belongs to which part of the outage            |
| 7  | Pool: total, idle, waiting, max               | `ep_pg_pool_total`, `_idle`, `_waiting`, `_max`                                                          | dead or stuck connections in the pool                      |
| 8  | Pool connections opened / removed per second  | `rate(ep_pg_pool_connects_total[w])`, `rate(ep_pg_pool_removes_total[w])`, `rate(ep_pg_pool_idle_errors_total[w])` | whether the pool reconnects cleanly              |
| 9  | Postgres backends and commits                 | `sum(pg_stat_activity_count{datname="eventpulse"})`, `rate(pg_stat_database_xact_commit{datname="eventpulse"}[w])` | what Postgres sees                          |
| 10 | Pool wait and query time p99                  | `histogram_quantile(0.99, sum by (le)(rate(ep_pg_pool_wait_seconds_bucket[w])))`, same for the query    | where requests wait                                        |
| 11 | CPU of the pinned groups                      | `100*(1-avg(rate(node_cpu_seconds_total{mode="idle",cpu=~"$cpu_server"}[w])))`, same for k6 and postgres | resource view                                              |
| 12 | Server process CPU                            | `rate(process_cpu_user_seconds_total[w])`, `rate(process_cpu_system_seconds_total[w])`                   | resource view                                              |
| 13 | Requests in flight, open connections          | `http_requests_in_flight`, `http_connections_open`                                                       | waiting requests pile up (Little's law)                    |
| 14 | k6 dropped iterations                         | `sum(rate(k6_dropped_iterations_total[w]))`                                                              | validity: must be 0                                        |
| 15 | k6 VUs in use / max                           | `sum(k6_vus)`, `sum(k6_vus_max)`                                                                         | validity: generator headroom                               |
| 16 | k6 cores busy %                               | `100*(1-avg(rate(node_cpu_seconds_total{mode="idle",cpu=~"$cpu_k6"}[w])))`                               | validity: keep under 70                                    |
| 17 | Event loop lag p99                            | `nodejs_eventloop_lag_p99_seconds`                                                                       | validity: Node itself is fine                              |

Verify in Grafana Explore after a run: `count by (code)(ep_db_errors_by_code_total)` and `sum(k6_b8_resp_202_total)`.

## 5. Run it

(a) Dry run (prints the plan, runs nothing):

```bash
cd loadtests/k6/stage-1/B8 && DRY_RUN=1 ./run.sh
```

(b) One-variant trial (plumbing only, never quoted as a result):

```bash
VARIANTS="restart" REPEATS=1 DURATION=60s RESULTS_NAME=b8-trial ./run.sh
```

(c) Full matrix, one command:

```bash
./run.sh
```

(d) A single run by hand through the shared runner (the fault is then up to me, the probes and audit are not done):

```bash
cd loadtests/k6 && RESULTS_NAME=b8-manual LABEL=b8-manual SCRIPT=stage-1/B8/script.js RATE=2000 DURATION=120s VARIANT=restart ROUTE=/healthz ./runner.sh
```

(e) If a run was killed hard (the EXIT trap did not run): put Postgres, the proxy and the toxics back:

```bash
./run.sh RESTORE
```

Watch during a run, in another terminal:

```bash
watch -n1 'curl -s localhost:8000/debug/stats'
```

Results go to `results/<RESULTS_NAME>/`: `summary.tsv` (one row per run, printed as a table at the end), `runs.tsv` (testid, variant, start/end epoch, status, fault start epoch), `env.txt`, `server.out`, `captures/<testid>.txt` (tagged blocks), `captures/<testid>.db.tsv` and `.api.tsv` (probes), and the k6 laps in `<timestamp>/`.

## 6. What this script changes on my machine

No sudo, no sysctl, no tc. It changes:

- `TRUNCATE events` before every run (the table is emptied).
- `docker restart ep-postgres` in the `restart` variant.
- The Toxiproxy proxy `pg`: created if missing; disabled and enabled; toxics `bh_down` and `bh_up` added and removed.
- It starts and stops the ingest-api on port 8000 and the two probe loops.

On exit (also Ctrl+C) the EXIT trap stops the probes and the server, starts the Postgres container if it is stopped, removes the toxics and enables the proxy. `./run.sh RESTORE` does the same by hand. The table `events` is not restored (it holds the rows of the last run).

## 7. Analyze in Grafana

Dashboard: [B8 - Database restart under load.json](../../../../infra/observability/dashboards/stage-1/B8%20-%20Database%20restart%20under%20load.json). Import it through the Grafana UI (Dashboards, New, Import, upload the JSON) and pick the Prometheus datasource. Zoom to the times in `runs.tsv` (epoch seconds); `fault_start_epoch` marks the moment of the fault.

Order to read it:

1. Row 1 (verdict): when did 202s drop and when did they come back, and how large is the not-accepted share.
2. Row 2 (mechanism): the error codes, the pool gauges and the connects and removes, to see what happened to the pool.
3. Row 4 (validity) before trusting a number: dropped iterations must be 0, k6 cores under 70 %.
4. Row 3 (resources) last.
5. Then `summary.tsv` for the timeline columns (`db_down_s`, `db_up_s`, `api_down_s`, `api_up_s`, `api_stable_s`, `lag_s`, `api_end`) and the row audit.
