# B10 Missing timeouts

Why this experiment and what I expect: see `notebook/stage-1/experiments/B10/Explanation.md`

## 1. Run the server

`run.sh` starts and restarts ingest-api itself (`MANAGE_SERVER=1`, the default), once per run, so the pool starts empty and each run gets its own timeout settings:

```bash
cd services/ingest-api && env DATABASE_URL=<proxy url> PG_POOL_MAX=25 PG_CONN_TIMEOUT_MS=<0|2000> PG_STATEMENT_TIMEOUT_MS=<0|5000> taskset -c 0,6 node --env-file=.env dist/main.js
```

- The proxy URL is `DATABASE_URL` from `services/ingest-api/.env` with host and port replaced by `localhost:15434` (Toxiproxy), as in B3. Environment variables win over `--env-file`, so `.env` is not edited.
- `0` means "no deadline": `src/db/db.module.ts` turns `0` into `undefined` for `connectionTimeoutMillis` and `statement_timeout`.
- The `dist/` build must exist and contain the B10 change (`pnpm build` in `services/ingest-api`).
- Check by hand: `curl localhost:8000/debug/stats` shows `pool.max`, `pool.conn_timeout_ms` and `pool.statement_timeout_ms`. `run.sh` checks all three after every restart and stops if they differ from what it asked for.
- CPU pinning: server `0,6` (taskset), Postgres `1,7,2,8`, k6 `3,9,4,10`, monitoring and Toxiproxy `5,11` (container cpusets from `infra/compose/docker-compose.yml`; `run.sh` only prints them into `env.txt`).

Start the stack before the first run:

```bash
docker compose -f infra/compose/docker-compose.yml up -d
```

## 2. Run configuration

- Variable: the server's deadlines. **unset** = `PG_CONN_TIMEOUT_MS=0`, `PG_STATEMENT_TIMEOUT_MS=0`. **set** = `PG_CONN_TIMEOUT_MS=2000`, `PG_STATEMENT_TIMEOUT_MS=5000`.
- Pool fixed at 25. k6 request timeout 15 s (a request with no answer by then is "hung", status 0).
- Two faults:
  - `lat`: Toxiproxy `latency` toxic, 200 ms, downstream, for the whole run. 180 req/s for 60 s, filler 300 B (same payload as B3).
  - `stall`: at +15 s after k6 starts, a separate Postgres session runs `BEGIN; LOCK TABLE events IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(35); COMMIT;`. 100 req/s for 80 s, filler 20,000 B.
- After k6 ends, `run.sh` waits (up to 180 s) until the server has nothing queued or running and records that time as `drain_s`.
- 2 faults x 2 variants x 3 repeats = 12 measured runs. Variants alternate: odd repeats unset first, even repeats set first.
- One probe run (`b10-probe-stall-unset`): stall + unset, with `pg_stat_activity`, `pg_locks`, `vmstat` and `pidstat` taken in the middle of the stall. Never a headline number.
- Total: about 31 minutes (printed by the dry run).

| Env knob | Default | Meaning |
| --- | --- | --- |
| `VARIANTS` | `unset set` | which timeout variants |
| `FAULTS` | `lat stall` | which faults |
| `REPEATS` | `3` | repeats of every cell |
| `POOL` | `25` | `PG_POOL_MAX` |
| `SET_CONN_MS`, `SET_STMT_MS` | `2000`, `5000` | values of variant `set` |
| `REQ_TIMEOUT` | `15s` | k6 per-request timeout |
| `PRE_VUS` | `3000` | k6 VUs created before the run (hung requests each hold one for 15 s) |
| `LAT_MS`, `LAT_RATE`, `LAT_DURATION`, `PAD_LAT` | `200`, `180`, `60`, `300` | fault `lat` |
| `STALL_RATE`, `STALL_DURATION`, `STALL_AT`, `STALL_S`, `PAD_STALL` | `100`, `80`, `15`, `35`, `20000` | fault `stall` (seconds, bytes) |
| `DRAIN_MAX_S` | `180` | longest wait for the server to drain |
| `PROBES` | `1` | `0` skips the probe run |
| `RESULTS_NAME` | `b10` | folder under `results/` |
| `TOXI_URL`, `TOXI_PORT`, `TOXI_UPSTREAM` | `http://localhost:8474`, `15434`, `postgres:5432` | Toxiproxy, as in B3 |
| `MANAGE_SERVER`, `SERVER_CPUS`, `K6_CPUS`, `PG_CONTAINER` | `1`, `0,6`, `3,9,4,10`, auto | as in B3 |

## 3. Instrumentation needed

- prom-client defaults used: `process_cpu_user_seconds_total`, `process_cpu_system_seconds_total`, `process_resident_memory_bytes`, `nodejs_heap_size_used_bytes`, `nodejs_external_memory_bytes`, `nodejs_eventloop_lag_p99_seconds`.
- Custom server metrics used (existing): `http_requests_in_flight`, `http_requests_total`, `ep_pg_pool_waiting`, `ep_pg_pool_total`, `ep_pg_pool_idle`, `ep_pg_pool_max`, `ep_pg_pool_wait_seconds`, `ep_pg_query_duration_seconds`, `ep_db_errors_by_type_total`, `ep_events_inserted_total`.
- k6 metrics used: `k6_http_reqs_total`, `k6_http_req_duration_seconds`, `k6_dropped_iterations_total`, `k6_vus`, `k6_vus_max`, and the counters `k6_b10_resp_202_total`, `k6_b10_resp_503_total`, `k6_b10_resp_other_total`, `k6_b10_resp_hung_total` (**ADDED BY B10** in `loadtests/k6/stage-1/B10/script.js`; they exist in Prometheus only after the first k6 run).
- node_exporter: `node_cpu_seconds_total`.
- Other: `ep_pg_container_cpu_seconds_total` (from `infra/scripts/pg-container-exporter.sh`, started by `run.sh`).
- **ADDED BY B10** in `services/ingest-api/src/observability/metrics.service.ts`: `/debug/stats` now also reports `pool.conn_timeout_ms` and `pool.statement_timeout_ms` (the values the pool really runs with, 0 = none). No Prometheus metric was added.
- Reused infrastructure: the `toxiproxy` service from B3.

## 4. Grafana panels

`[w]` = `$__rate_interval`. Job filters omitted. `K` = `phase!~"probe.*"`.

| # | Panel | Query | What it shows |
| --- | --- | --- | --- |
| 1 | Answers per second by class (k6) | `sum by (variant, fault)(rate(k6_b10_resp_{202,503,hung}_total{K}[w]))` | accepted, fast refusals and hung requests |
| 2 | Client latency p50 and p99 | `histogram_quantile(q, sum by (variant, fault)(rate(k6_http_req_duration_seconds{K}[w])))` | what the client waited |
| 3 | Offered vs accepted | `sum by (variant, fault)(rate(k6_http_reqs_total{K}[w]))` and the 202 counter | gap = failed or hung |
| 4 | Requests waiting for a pool connection | `max(ep_pg_pool_waiting)` | queue inside Node |
| 5 | Pool connections | `max(ep_pg_pool_total)`, `_idle`, `_max` | all busy during the stall |
| 6 | HTTP requests in flight | `sum(http_requests_in_flight)` | work held by the server |
| 7 | Server memory | `process_resident_memory_bytes`, `nodejs_heap_size_used_bytes`, `nodejs_external_memory_bytes` | growth from queued requests |
| 8 | Pool wait time p99 | `histogram_quantile(0.99, sum by (le)(rate(ep_pg_pool_wait_seconds_bucket[w])))` | waiting for a connection |
| 9 | Database errors by type | `sum by (type)(rate(ep_db_errors_by_type_total[w]))` | pool_timeout, statement_timeout |
| 10 | Server answers by status | `sum by (status)(rate(http_requests_total{route!~"/metrics"}[w]))` | server's own view |
| 11 | Events inserted per second | `sum(rate(ep_events_inserted_total[w]))` | rows committed |
| 12 | Statement time p99 | `histogram_quantile(0.99, sum by (le)(rate(ep_pg_query_duration_seconds_bucket[w])))` | INSERT time |
| 13 | Server process CPU | `rate(process_cpu_{user,system}_seconds_total[w])` | cores used by node |
| 14 | Pinned CPUs busy: server, postgres | `1 - avg(rate(node_cpu_seconds_total{mode="idle", cpu=~"$cpu_server"}[w]))` | host CPU |
| 15 | Postgres container CPU | `sum(rate(ep_pg_container_cpu_seconds_total[w]))` | cores |
| 16 | k6 dropped iterations | `sum by (variant, fault)(rate(k6_dropped_iterations_total{K}[w]))` | must be 0 |
| 17 | k6 VUs | `max(k6_vus)`, `max(k6_vus_max)` | VU headroom |
| 18 | k6 pinned CPUs busy | `1 - avg(rate(node_cpu_seconds_total{mode="idle", cpu=~"$cpu_k6"}[w]))` | generator headroom |
| 19 | Event loop lag p99 | `max(nodejs_eventloop_lag_p99_seconds)` | Node itself slow |

Verify (paste into Grafana Explore after the first k6 run):

```promql
count by (variant, fault)(k6_b10_resp_202_total)
max(ep_pg_pool_waiting{job="eventpulse-nodejs"})
```

## 5. Run it

Dry run (prints the plan, runs nothing):

```bash
cd loadtests/k6/stage-1/B10 && DRY_RUN=1 ./run.sh
```

One-run trial (stall fault, both variants, one repeat, no probe; a trial is not a result):

```bash
cd loadtests/k6/stage-1/B10 && FAULTS="stall" REPEATS=1 PROBES=0 RESULTS_NAME=b10-trial ./run.sh
```

Full matrix:

```bash
cd loadtests/k6/stage-1/B10 && ./run.sh
```

Watch in another terminal during a run:

```bash
watch -n1 'curl -s localhost:8000/debug/stats | jq "{pool, in_flight, rss: (.memory.rss/1e6)}"'
```

Results go to `loadtests/k6/stage-1/B10/results/<RESULTS_NAME>/`: `summary.tsv` (one row per run), `runs.tsv` (start/end epochs and drain seconds), `faults.tsv` (when the lock was taken and released), `env.txt`, `captures/<testid>.txt`, the k6 lap JSONs under a timestamp folder, `server.out`.

Extra columns in `summary.tsv` (from `hook.py`): `hung_pct`, `stale_rows`, `peak_waiting`, `peak_inflight`, `rss_growth_mb`, `drain_s`. `stale_rows` is rows in the table minus 202 answers k6 saw; there is deliberately no acknowledged-vs-stored audit.

## 6. What this script changes on my machine

- No `sudo`, no sysctl, no `tc`.
- Starts and kills the ingest-api node process (once per run) and the `pg-container-exporter.sh` helper; both are stopped in the EXIT trap.
- Adds a Toxiproxy proxy `pg` (if missing) and, in `lat` runs, a latency toxic named `lat`; the toxic is deleted after every run and in the EXIT trap.
- In `stall` runs a psql session in the Postgres container holds an `ACCESS EXCLUSIVE` lock on `events` for 35 s. It ends by itself; the EXIT trap also calls `pg_terminate_backend` on any session still running `LOCK TABLE events`.
- `TRUNCATE events`, `CHECKPOINT` and `pg_stat_statements_reset()` before every run (the table is emptied).

## 7. Analyze in Grafana

Dashboard: [`B10 - Missing timeouts.json`](../../../../infra/observability/dashboards/stage-1/B10%20-%20Missing%20timeouts.json). Import it in Grafana (Dashboards, New, Import, upload the file) and pick the Prometheus datasource. Zoom to a run with the epochs in `runs.tsv` (the lock window of a stall run is in `faults.tsv`). Read row 1 (hang or fail), row 2 (queue, connections, memory), row 3 (resources), and row 4 last to decide whether the run is valid.
