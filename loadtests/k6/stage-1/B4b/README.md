# B4b Why 10 hot tenants reach only half of the expected rate

Why this experiment and what I expect: see `notebook/stage-1/experiments/B4b/Explanation.md`

This is a follow-up of B4. It reuses B4's server changes, B4's `script.js` and B4's `hook.py`; it has only its own `run.sh`.

## 1. Run the server

`run.sh` starts and restarts ingest-api itself (`MANAGE_SERVER=1`, the default), once per run, with the cell's pool size and API keys:

```bash
cd services/ingest-api && env API_KEYS=<epk_b4_0:t0,...> COUNTER_MODE=hot PG_POOL_MAX=<10|25|60> taskset -c 0,6 node --env-file=.env dist/main.js
```

- Same server code as B4 (`COUNTER_MODE`, migration 0003 applied, `pnpm build` done). Nothing new in the server.
- Direct connection to Postgres (`DATABASE_URL` from `.env`, port 5434). No Toxiproxy.
- `synchronous_commit` is set for the database role before each run (section 6) and the server is restarted after it, so its connections get the value.
- CPU pinning: server `0,6`, Postgres `1,7,2,8`, k6 `3,9,4,10`, monitoring `5,11` (as in B4). Pool 60 plus the exporter and checks stays below `max_connections=100`.

## 2. Run configuration

- Cells (`tenants:pool:synchronous_commit`): `10:25:on 10:10:on 10:60:on 10:25:off 10:60:off 1:25:on 1:25:off`.
- Always `COUNTER_MODE=hot`, one phase (`over`, 3000 req/s offered), 45 s, 3 repeats. The order alternates by repeat (odd: as listed, even: reversed).
- 7 cells x 3 repeats = 21 measured runs, about 31 min (printed by the dry run). No probe runs.
- Per run: set `synchronous_commit`, check a new session sees it, restart the server, seed one counter row per tenant, warm up on tenant `t0`, check the counter moved, truncate `events`, run k6, one mid-run capture, row audit (rows in `events` against the sum of the counters).

| Env knob | Default | Meaning |
| --- | --- | --- |
| `CELLS` | `10:25:on 10:10:on 10:60:on 10:25:off 10:60:off 1:25:on 1:25:off` | `tenants:pool:sync` cells |
| `PHASES` | `over` | `under` can be added |
| `UNDER_RATE` / `OVER_RATE` | `150` / `3000` | offered req/s |
| `REPEATS` | `3` | repeats of every cell |
| `DURATION` | `45s` | k6 length of a run |
| `KEY_PREFIX` | `epk_b4_` | api key `i` is `KEY_PREFIX` + `i` |
| `RESULTS_NAME` | `b4b` | results folder name |
| `MANAGE_SERVER` | `1` | 0 = attach to a server I started myself |

## 3. Instrumentation needed

Nothing is added by B4b. All metrics come from B4 (see B4's README, section 3): `ep_pg_counter_update_seconds` (**ADDED BY B4**), `ep_pg_pool_wait_seconds`, `ep_pg_pool_waiting`, `ep_db_errors_by_type_total`, `pg_stat_activity_count` (labels `state`, `wait_event_type`, `wait_event`), `pg_stat_database_xact_commit`, `pg_stat_database_tup_updated`, `pg_stat_database_deadlocks`, `ep_pg_container_cpu_seconds_total`, `node_cpu_seconds_total`, and the k6 series (`k6_http_reqs_total`, `k6_http_req_duration_seconds`, `k6_dropped_iterations_total`, `k6_b4_resp_202_total`). The k6 series carry `tenants`, `counter`, `pool_max`, `phase` and `testid`; they do not carry the `synchronous_commit` value, so I tell the runs apart by `testid` (`b4b-over-t<tenants>-p<pool>-s<on|off>-n<repeat>`).

## 4. Grafana panels

No new dashboard. I use B4's dashboard (`infra/observability/dashboards/stage-1/B4 - Lock contention.json`) with the time range of one run from `runs.tsv`. The panels that matter here: accepted requests per second, "Postgres sessions by wait event", "Sessions by state", "Counter UPDATE latency", "Commits and counter-row updates per second", "CPU busy by pinned group".

Verify in Grafana Explore: `count by (testid)(k6_http_reqs_total{testid=~"b4b.*"})`.

## 5. Run it

Dry run (prints the plan, runs nothing):

```bash
cd loadtests/k6/stage-1/B4b && DRY_RUN=1 ./run.sh
```

Trial (plumbing check, NOT a result):

```bash
cd loadtests/k6/stage-1/B4b && CELLS="10:25:off" REPEATS=1 DURATION=30s RESULTS_NAME=b4b-trial ./run.sh
```

Full run (one command):

```bash
cd loadtests/k6/stage-1/B4b && ./run.sh
```

Check by hand that the role override is gone after a run (it should print `on`, the server default):

```bash
docker exec -i ep-postgres psql -U eventpulse -d eventpulse -At -c "show synchronous_commit"
```

Results go to `loadtests/k6/stage-1/B4b/results/<RESULTS_NAME>/`: `summary.tsv` (one row per run, table at the end), `runs.tsv` (epochs per run, with tenants, pool and sync), `env.txt`, `captures/<testid>.txt`, `server.out`, the k6 lap JSONs.

## 6. What this script changes on my machine

- No `sudo`, no `sysctl`, no `tc`, no docker compose commands. It does not touch `postgresql.conf` or the container.
- **Database role setting:** `ALTER ROLE eventpulse SET synchronous_commit = on|off` before every run. It applies to new sessions only. The EXIT trap runs `ALTER ROLE eventpulse RESET synchronous_commit`, so the role is back to the server default after the run and after Ctrl+C. If the script is killed hard (`kill -9`), run `ALTER ROLE eventpulse RESET synchronous_commit` by hand (command above).
- `synchronous_commit=off` can lose the last transactions if Postgres crashes. Here it only matters for the test data.
- Starts and stops ingest-api (`kill` of the pid it started) and `infra/scripts/pg-container-exporter.sh`.
- Database: `TRUNCATE events`, `TRUNCATE tenant_counters` and re-seed, `pg_stat_statements_reset()`, `CHECKPOINT`, warm-up events. It deletes all rows in `events` at the start of every run.

## 7. Analyze in Grafana

Import B4's dashboard if it is not imported yet (Grafana, Dashboards, New, Import, pick the Prometheus datasource for `ds`). Set the time range to the epochs of one run from `runs.tsv`. Order to read: accepted rate, wait events and sessions by state, counter UPDATE latency, commits per second, CPU.
