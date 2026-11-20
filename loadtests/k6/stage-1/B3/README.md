# B3 Slow database propagates to the API

Why this experiment and what I expect: see `notebook/stage-1/experiments/B3/Explanation.md`

## 1. Run the server

`run.sh` starts and restarts ingest-api itself (`MANAGE_SERVER=1`, the default), once per run, so the pool starts empty:

```bash
cd services/ingest-api && env DATABASE_URL=<proxy url> PG_POOL_MAX=25 taskset -c 0,6 node --env-file=.env dist/main.js
```

- The proxy URL is `DATABASE_URL` from `services/ingest-api/.env` with the host and port replaced by `localhost:15434` (Toxiproxy). Environment variables win over `--env-file`, so `.env` is not edited.
- The `dist/` build must exist (`pnpm build` in `services/ingest-api`). B3 changes no server code.
- Check by hand: `curl localhost:8000/debug/stats` shows `pool.max = 25`. `run.sh` checks this itself and also sends one request after the latency is added and fails if it was not slower.
- CPU pinning: server `0,6` (taskset), Postgres `1,7,2,8`, k6 `3,9,4,10`, monitoring and Toxiproxy `5,11`. The container cpusets come from `infra/compose/docker-compose.yml`. `run.sh` does not change them, it only prints them into `env.txt`.

Start the stack fresh before the first run:

```bash
docker compose -f infra/compose/docker-compose.yml up -d
```

## 2. Run configuration

- Variable: latency added to every answer of the database, **0, 20, 50, 200 ms** (a Toxiproxy `latency` toxic, stream `downstream`, jitter 0). Level 0 goes through the proxy without a toxic.
- Pool: fixed at 25.
- Two phases per level: `under` (about half of the predicted capacity) and `over` (about 1.2 times it).
- 4 levels x 2 phases x 3 repeats x 45 s = 24 measured runs. Odd repeats go 0 to 200 ms, even repeats 200 to 0.
- Probe runs (separate, not for headline numbers): `reca` (200 ms, 300 req/s) and `recb` (50 ms, 600 req/s): slow for 30 s, then the toxic is deleted and k6 keeps running (90 s in total); `ctl`: server connected straight to Postgres, no proxy, same rate and duration as the level-0 `under` runs (2000 req/s, 45 s by default).
- Total: about 42 minutes (printed by the dry run).

| Env knob                                                           | Default                                                         | Meaning                                                                  |
| ------------------------------------------------------------------ | --------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `LEVELS_MS`                                                        | `0 20 50 200`                                                   | added latency levels                                                     |
| `PHASES`                                                           | `under over`                                                    | which phases to run                                                      |
| `POOL`                                                             | `25`                                                            | `PG_POOL_MAX`                                                            |
| `UNDER_RATES` / `OVER_RATES`                                       | `0:2000 20:500 50:225 200:60` / `0:5000 20:1200 50:550 200:150` | offered req/s as `level:rate` pairs                                      |
| `REPEATS`                                                          | `3`                                                             | repeats of every cell                                                    |
| `DURATION`                                                         | `45s`                                                           | k6 length of a measured run                                              |
| `PROBES`                                                           | `1`                                                             | `0` skips the probe runs                                                 |
| `SLOW_S`, `REC_TOTAL_S`                                            | `30`, `90`                                                      | recovery probes: seconds slow, total seconds                             |
| `RECA_LEVEL`/`RECA_RATE`, `RECB_LEVEL`/`RECB_RATE`                 | `200`/`300`, `50`/`600`                                         | recovery probe levels and rates                                          |
| `CTL_RATE`, `CTL_DURATION`                                         | level-0 `under` rate, `DURATION`                                | control probe                                                            |
| `RESULTS_NAME`                                                     | `b3`                                                            | folder under `results/`                                                  |
| `TOXI_URL`, `TOXI_PORT`, `TOXI_UPSTREAM`                           | `http://localhost:8474`, `15434`, `postgres:5432`               | Toxiproxy API, proxied port, upstream as seen inside the compose network |
| `MANAGE_SERVER`, `SERVER_CPUS`, `K6_CPUS`, `PG_CONTAINER`, `H0_MS` | `1`, `0,6`, `3,9,4,10`, auto, `5.0`                             | as in B2; `H0_MS` only feeds the `cap_pred_rps` column                   |

Level 0 (no added latency) runs at 2000 req/s in `under` and 5000 req/s in `over`. These two rates were chosen by me after the trials (trial b3-trial0 accepted 3,995 of 4,000 req/s without errors).

## 3. Instrumentation needed

No server code was changed for B3.

- prom-client defaults used: `process_cpu_seconds_total`, `nodejs_eventloop_lag_seconds`.
- Custom server metrics used (existing): `http_requests_in_flight`, `http_request_duration_seconds`, `ep_pg_pool_total`, `ep_pg_pool_idle`, `ep_pg_pool_waiting`, `ep_pg_pool_wait_seconds`, `ep_pg_query_duration_seconds`, `ep_db_errors_by_type_total`.
- k6 metrics used: `k6_http_reqs_total`, `k6_http_req_duration_seconds`, `k6_http_req_failed_rate`, `k6_dropped_iterations_total`, and the counters `k6_b3_resp_202_total` / `k6_b3_resp_503_total` (**ADDED BY B3** in `loadtests/k6/stage-1/B3/script.js`; they exist in Prometheus only after the first k6 run). k6 scenario tags `added_ms`, `phase`, `pool_max`, `offered_rate`, `exp` (set in `script.js`). They are scenario tags on purpose: `runner.sh` passes `--tag testid=...`, and a command-line tag replaces the script's global `options.tags` (tested with k6 v2.4.0), so global tags never reached Prometheus.
- postgres-exporter metrics: `pg_stat_activity_count`, `pg_stat_database_xact_commit`.
- Other: `ep_pg_container_cpu_seconds_total` (from `infra/scripts/pg-container-exporter.sh`, started by `run.sh`), `node_cpu_seconds_total`, `node_disk_io_time_seconds_total`.
- Infrastructure ADDED BY B3: the `toxiproxy` service in `infra/compose/docker-compose.yml` (image `ghcr.io/shopify/toxiproxy:2.13.0`, ports 8474 and 15434, cpuset `5,11`).
- Analysis: the shared `loadtests/k6/scripts/analyze.py` (docs at the top of that file) with the B3 hook `hook.py` (columns `cap_pred_rps`, `little_L`). B1 and B2 still use their own `analyze.py`.

## 4. Grafana panels

`[w]` = `$__rate_interval`. Job filters omitted.

| #   | Panel                           | Query                                                                                                                    | What it shows                 |
| --- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------ | ----------------------------- |
| 1   | Offered vs accepted vs rejected | `sum by (added_ms, phase)(rate(k6_http_reqs_total[w]))`, same for `k6_b3_resp_202_total` and `_503_total`                | capacity per level            |
| 2   | API latency p50 and p99         | `histogram_quantile(0.5, sum by (added_ms, phase)(rate(k6_http_req_duration_seconds[w])))`                               | latency against added latency |
| 3   | 503 by type                     | `sum by (type)(rate(ep_db_errors_by_type_total[w]))`                                                                     | pool timeouts                 |
| 4   | In flight vs Little's Law       | `sum(http_requests_in_flight)` and `sum(rate(http_request_duration_seconds_sum{route!~"/metrics"}[w]))`                  | requests in the system        |
| 5   | Pool in use, total, waiting     | `max(ep_pg_pool_total) - max(ep_pg_pool_idle)`, `max(ep_pg_pool_waiting)`                                                | pool state                    |
| 6   | Query time and pool wait        | `histogram_quantile(0.5, sum by (le)(rate(ep_pg_query_duration_seconds_bucket[w])))`, pool wait p99                      | hold time                     |
| 7   | Postgres connections by state   | `sum by (state)(pg_stat_activity_count)`                                                                                 | idle vs active                |
| 8   | Postgres container CPU          | `sum(rate(ep_pg_container_cpu_seconds_total[w]))`                                                                        | database load                 |
| 9   | Commits per second              | `sum(rate(pg_stat_database_xact_commit{datname="eventpulse"}[w]))`                                                       | work reaching the database    |
| 10  | k6 dropped iterations           | `sum by (added_ms, phase)(rate(k6_dropped_iterations_total[w]))`                                                         | generator validity            |
| 11  | CPU busy by pinned group        | `100*(1-avg(rate(node_cpu_seconds_total{mode="idle",cpu=~"$cpu_server"}[w])))` and the same for postgres, k6, monitoring | headroom, proxy cores         |
| 12  | Event loop lag, disk busy       | `nodejs_eventloop_lag_seconds`, `rate(node_disk_io_time_seconds_total[w])`                                               | other limits                  |
| 13  | k6 failed ratio                 | `avg by (added_ms, phase)(k6_http_req_failed_rate)`                                                                      | non-202 share                 |

Verify in Grafana Explore after a k6 run: `count by (added_ms, phase)(k6_http_reqs_total)` and `count by (type)(ep_db_errors_by_type_total)`.

## 5. Run it

(a) Dry run (prints the plan, runs nothing):

```bash
cd loadtests/k6/stage-1/B3 && DRY_RUN=1 ./run.sh
```

(b) Trial (plumbing check only, never quoted as a result):

```bash
LEVELS_MS="50" PHASES="over" REPEATS=1 DURATION=30s PROBES=0 RESULTS_NAME=b3-trial ./run.sh
LEVELS_MS="0" PHASES="over" REPEATS=1 DURATION=30s PROBES=0 RESULTS_NAME=b3-trial0 OVER_RATES="0:5000" ./run.sh
```

The second trial is the one that tells me what the `over` rate for level 0 should be.

(c) Full run:

```bash
./run.sh
```

(d) One case by hand: add the toxic with `curl -X POST localhost:8474/proxies/pg/toxics -d '{"name":"lat","type":"latency","stream":"downstream","attributes":{"latency":50}}'`, start the server with the proxy URL, then call `loadtests/k6/runner.sh` with `SCRIPT=stage-1/B3/script.js RATE=... DURATION=... ADDED_MS=50 API_KEY=...`. Remove it with `curl -X DELETE localhost:8474/proxies/pg/toxics/lat`.

(e) While it runs, in another terminal: `watch -n1 'curl -s localhost:8000/debug/stats'` and `docker stats ep-toxiproxy ep-postgres`.

Results go to `loadtests/k6/stage-1/B3/results/<RESULTS_NAME>/`: `summary.tsv`, `runs.tsv` (epochs per run), `removal.tsv` (when the toxic was deleted in the recovery probes), `env.txt`, `captures/`, and the k6 lap JSONs in timestamped folders. Read the table with `python3 loadtests/k6/scripts/analyze.py table results/b3/summary.tsv`.

## 6. What this script changes on my machine

- Starts, restarts and stops ingest-api on port 8000 (`kill` of the node process it started).
- Creates the Toxiproxy proxy `pg` and adds / deletes a toxic named `lat` through the Toxiproxy HTTP API. The EXIT trap deletes the toxic, so Ctrl+C leaves no latency behind. The proxy itself stays (harmless, other experiments do not use port 15434).
- `TRUNCATE events` and `CHECKPOINT` in Postgres before every run (the table is emptied).
- Starts `infra/scripts/pg-container-exporter.sh` in the background and stops it at the end.
- No `sudo`, no `sysctl`, no `tc`, no `docker update` (the cpusets come from compose).

## 7. Analyze in Grafana

Dashboard: [`infra/observability/dashboards/stage-1/B3 - Slow database.json`](../../../../infra/observability/dashboards/stage-1/B3%20-%20Slow%20database.json). Import it through Grafana (Dashboards, New, Import), pick the Prometheus datasource in the `ds` variable. Zoom to the epochs in `runs.tsv`. Read the rows in order: 1 verdict, 2 mechanism, 3 database, 4 validity (dropped iterations must be 0 before I trust a run).
