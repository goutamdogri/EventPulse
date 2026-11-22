# B9 Retry storm

Why this experiment and what I expect: see `notebook/stage-1/experiments/B9/Explanation.md`

## 1. Run the server

`run.sh` starts and restarts ingest-api itself (`MANAGE_SERVER=1`, the default), once per run, so the pool starts empty:

```bash
cd services/ingest-api && env DATABASE_URL=<proxy url> PG_POOL_MAX=5 PG_CONN_TIMEOUT_MS=2000 PG_STATEMENT_TIMEOUT_MS=5000 \
  taskset -c 0,6 node --env-file=.env dist/main.js
```

- The proxy URL is `DATABASE_URL` from `services/ingest-api/.env` with host and port replaced by `localhost:15434` (Toxiproxy). Environment variables win over `--env-file`, so `.env` is not edited and cannot change the two timeouts silently.
- The `dist/` build must exist (`pnpm build` in `services/ingest-api`). B9 changes no server code.
- Check by hand: `curl localhost:8000/debug/stats` shows `pool.max = 5`. `run.sh` checks this itself, and also sends one request with the toxic on (before k6 starts) and fails if it was not slower.
- CPU pinning: server `0,6` (taskset), Postgres `1,7,2,8`, k6 `3,9,4,10`, monitoring and Toxiproxy `5,11`. The container cpusets come from `infra/compose/docker-compose.yml`; `run.sh` only prints them into `env.txt`.

Start the stack before the first run:

```bash
docker compose -f infra/compose/docker-compose.yml up -d
```

## 2. Run configuration

- Variable: the client retry policy (`VARIANT` tag in k6):
  - `none`: no retries (control)
  - `immediate`: up to 3 retries, no waiting
  - `backoff`: 3 retries, wait 100, 200, 400 ms, each with full jitter (random between 0 and the wait)
  - `backoff_nojitter`: the same waits without the random part
- Every run is one k6 run at a constant 600 new events/s (open model, `constant-arrival-rate`), in three parts: 30 s healthy, 20 s with a 50 ms latency toxic on every database answer (the trigger), then 120 s with the toxic deleted. Total 170 s of k6 per run.
- A retry belongs to the iteration that started it, so RATE counts new events only. Offered load = attempts per second (`k6_b9_attempts_total`).
- Retries use the same `event_id` (the server does `ON CONFLICT DO NOTHING`). A non-202 answer or a client timeout is retried; a 4xx stops the loop.
- Per attempt client timeout 1 s, server pool wait 2 s, statement timeout 5 s.
- Pool fixed at 5 (not 25 like B3) so that the original rate stays small enough for k6 to hold the virtual users of retrying iterations.
- 4 variants x 3 repeats x (170 s + about 60 s overhead). The variant order rotates by one every repeat.
- Probe run (separate, not for headline numbers): variant `immediate`, same timeline, then after 170 s the generator drops to 60 new events/s for 60 s.
- Total: about 50 minutes (printed by the dry run).

| Env knob                                      | Default                                      | Meaning                                                      |
| --------------------------------------------- | -------------------------------------------- | ------------------------------------------------------------ |
| `VARIANTS`                                    | `none immediate backoff backoff_nojitter`    | retry policies to run                                        |
| `POOL`                                        | `5`                                          | `PG_POOL_MAX`                                                |
| `BASE_RATE`                                   | `600`                                        | original rate, new events/s                                  |
| `SLOW_MS`                                     | `50`                                         | latency added by the toxic                                   |
| `PRE_S`, `SLOW_S`, `POST_S`                   | `30`, `20`, `120`                            | healthy, slow, after (seconds)                               |
| `REPEATS`                                     | `3`                                          | repeats of every variant                                     |
| `PROBES`                                      | `1`                                          | `0` skips the relief probe                                   |
| `RELIEF_RATE`, `RELIEF_TAIL_S`                | `BASE_RATE/10`, `60`                         | probe: rate and length after the relief                      |
| `REQ_TIMEOUT`, `MAX_RETRIES`                  | `1s`, `3`                                    | per-attempt client timeout, retries per event                |
| `PRE_VUS`, `MAX_VUS`                          | `1000`, `8000`                               | k6 virtual users                                             |
| `PG_CONN_TIMEOUT_MS`, `PG_STATEMENT_TIMEOUT_MS` | `2000`, `5000`                             | server timeouts, set by `run.sh`                             |
| `RESULTS_NAME`                                | `b9`                                         | folder under `results/`                                      |
| `TOXI_URL`, `TOXI_PORT`, `TOXI_UPSTREAM`      | `http://localhost:8474`, `15434`, `postgres:5432` | Toxiproxy API, proxied port, upstream inside compose     |
| `MANAGE_SERVER`, `SERVER_CPUS`, `K6_CPUS`, `PG_CONTAINER` | `1`, `0,6`, `3,9,4,10`, auto    | as in B3                                                     |

## 3. Instrumentation needed

No server code was changed for B9.

- prom-client defaults used: `process_cpu_seconds_total`, `process_resident_memory_bytes`, `nodejs_eventloop_lag_seconds`.
- Custom server metrics used (existing): `http_requests_in_flight`, `ep_pg_pool_total`, `ep_pg_pool_idle`, `ep_pg_pool_waiting`, `ep_pg_pool_wait_seconds`, `ep_db_errors_by_type_total`.
- k6 metrics used: existing `k6_iterations_total`, `k6_http_req_duration_seconds`, `k6_dropped_iterations_total`, `k6_vus`; **ADDED BY B9** in `loadtests/k6/stage-1/B9/script.js` (they exist in Prometheus only after the first k6 run): `k6_b9_attempts_total`, `k6_b9_retries_total`, `k6_b9_unique_ok_total`, `k6_b9_giveups_total`, `k6_b9_resp_202_total`, `k6_b9_resp_503_total`, `k6_b9_resp_other_total`. Scenario tags `exp`, `variant`, `base_rate`, `phase` (scenario tags on purpose, see B3).
- postgres-exporter: `pg_stat_database_xact_commit`.
- node_exporter: `node_cpu_seconds_total`.
- Infrastructure: the Toxiproxy service added by B3 (`infra/compose/docker-compose.yml`).
- Analysis: shared `loadtests/k6/analyze.py` with the B9 hook `hook.py`. The hook asks Prometheus for the time series of the run (needs Prometheus up when `run.sh` calls it) and adds `amplification`, `peak_offered_x`, `recovery_s`, `tail_ok_pct`, `stale_rows` (definitions at the top of `hook.py`).

## 4. Grafana panels

`[w]` = `$__rate_interval`. Job filters omitted.

| #   | Panel                                      | Query                                                                                              | What it shows                              |
| --- | ------------------------------------------ | -------------------------------------------------------------------------------------------------- | ------------------------------------------ |
| 1   | Offered attempts/s vs new events/s         | `sum by (variant)(rate(k6_b9_attempts_total[w]))`, same for `k6_iterations_total`                  | amplification by retries                   |
| 2   | Goodput                                    | `sum by (variant)(rate(k6_b9_unique_ok_total[w]))`                                                 | events accepted per second                 |
| 3   | Share of attempts answered 202             | `sum by (variant)(rate(k6_b9_resp_202_total[w])) / sum by (variant)(rate(k6_b9_attempts_total[w]))` | recovery signal                          |
| 4   | Retries and give-ups                       | `sum by (variant)(rate(k6_b9_retries_total[w]))`, `..._giveups_total`                              | retry volume                               |
| 5   | Answers by kind                            | `sum(rate(k6_b9_resp_202_total[w]))`, `_503_`, `_other_`                                           | accepted, rejected, client timeout         |
| 6   | Commits vs 202s                            | `sum(rate(pg_stat_database_xact_commit{datname="eventpulse"}[w]))` and the 202 rate                | wasted work                                |
| 7   | Pool in use and waiting                    | `max(ep_pg_pool_total) - max(ep_pg_pool_idle)`, `max(ep_pg_pool_waiting)`                          | queue in front of the database             |
| 8   | Pool wait p50 / p99                        | `histogram_quantile(0.99, sum by (le)(rate(ep_pg_pool_wait_seconds_bucket[w])))`                   | how stale a served request is              |
| 9   | Server errors by type                      | `sum by (type)(rate(ep_db_errors_by_type_total[w]))`                                               | pool and statement timeouts                |
| 10  | In flight and k6 VUs                       | `sum(http_requests_in_flight{route!~"/metrics"})`, `max(k6_vus)`                                   | queue size, generator load                 |
| 11  | Attempt latency p50 / p99                  | `histogram_quantile(0.99, sum(rate(k6_http_req_duration_seconds[w])))`                             | client view                                |
| 12  | CPU busy by pinned group                   | `100*(1-avg(rate(node_cpu_seconds_total{mode="idle",cpu=~"$cpu_server"}[w])))`, same for postgres, k6 | where the cost lands                    |
| 13  | Node process CPU and memory                | `rate(process_cpu_seconds_total[w])`, `process_resident_memory_bytes`                              | server load, queued requests               |
| 14  | k6 dropped iterations                      | `sum by (variant)(rate(k6_dropped_iterations_total[w]))`                                           | generator validity (must be 0)             |
| 15  | k6 CPU headroom                            | same CPU query for `$cpu_k6`                                                                       | keep under about 70 %                      |
| 16  | Event loop lag                             | `max(nodejs_eventloop_lag_seconds)`                                                                | other limit                                |

Verify in Grafana Explore after a k6 run: `count by (variant)(k6_b9_attempts_total)` and `count by (type)(ep_db_errors_by_type_total)`.

## 5. Run it

(a) Dry run (prints the plan, runs nothing):

```bash
cd loadtests/k6/stage-1/B9 && DRY_RUN=1 ./run.sh
```

(b) Trial (plumbing check only, never quoted as a result):

```bash
VARIANTS="immediate" REPEATS=1 PROBES=0 RESULTS_NAME=b9-trial ./run.sh
```

(c) Full run:

```bash
./run.sh
```

(d) One variant by hand: start the server as in section 1, then `cd loadtests/k6 && env LABEL=b9-hand SCRIPT=stage-1/B9/script.js VARIANT=immediate RATE=600 DURATION=170s API_KEY=<key> ROUTE=/healthz RESULTS_NAME=b9-hand ./runner.sh`, and add the toxic yourself: `curl -X POST localhost:8474/proxies/pg/toxics -d '{"name":"lat","type":"latency","stream":"downstream","attributes":{"latency":50}}'`. Remove it with `curl -X DELETE localhost:8474/proxies/pg/toxics/lat`.

(e) While it runs, in another terminal: `watch -n1 'curl -s localhost:8000/debug/stats'`.

Results go to `loadtests/k6/stage-1/B9/results/<RESULTS_NAME>/`: `summary.tsv`, `runs.tsv` (epochs per run), `events.tsv` (epochs when traffic started, the toxic was added and removed, k6 ended), `env.txt`, `captures/`, the k6 lap JSONs and manifests.

## 6. What this script changes on my machine

- Starts, restarts and stops ingest-api on port 8000 (`kill` of the node process it started).
- Creates the Toxiproxy proxy `pg` and adds / deletes a toxic named `lat` through the Toxiproxy HTTP API. The EXIT trap deletes the toxic, so Ctrl+C leaves no latency behind. The proxy itself stays.
- `TRUNCATE events` and `CHECKPOINT` in Postgres before every run (the table is emptied).
- Starts `infra/scripts/pg-container-exporter.sh` in the background and stops it at the end.
- No `sudo`, no `sysctl`, no `tc`, no `docker update`.

## 7. Analyze in Grafana

Dashboard: [`infra/observability/dashboards/stage-1/B9 - Retry storm.json`](../../../../infra/observability/dashboards/stage-1/B9%20-%20Retry%20storm.json). Import it through Grafana (Dashboards, New, Import) and pick the Prometheus datasource. Zoom to the epochs in `runs.tsv` and `events.tsv`. Read the rows in order: 1 (offered vs goodput), 2 (queues, timeouts, wasted work), 3 (resources), 4 (validity; dropped iterations must be 0).
