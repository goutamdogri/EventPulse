# B2 Pool exhaustion: learnings

This experiment is about one question: if the API has a limited number of database connections (the pool), what happens when more requests
want the database than there are connections, and is a bigger pool always better? I do not know the answer yet. This file holds the concepts
and the commands. No results.

The cycle I expect, step by step:

1. A request arrives and the handler calls `pool.connect()` (inside `pool.query`).
2. If an idle connection exists, it is used. If not, and `totalCount < max`, the pool opens a new connection (Postgres forks a new backend process).
3. If `totalCount == max`, the request waits in the pool queue (`pool.waitingCount` goes up).
4. The INSERT runs, then the commit waits for the WAL flush (fsync). Only then is the connection given back and the next waiter gets it.
5. If a waiter waits longer than `connectionTimeoutMillis` (2 s), the pool throws "timeout exceeded when trying to connect" and the API answers 503.
6. If the pool is bigger than Postgres can accept, step 2 fails with `53300 too many clients already`, also a 503, but a different error.

Our hypothesis: with a small pool, requests queue and then time out; more connections help only up to a point and then hurt; the best pool is small
compared with `max_connections`.

See also: `notebook/stage-1/learning/nodeAndNest/1. node-pg-pool.md`, `Postgres/1. postgres-process-per-connection.md`, `Postgres/2. postgres-write-path.md`,
`Postgres/7. postgres-wait-events.md`, `notebook/stage-1/experiments/B1/Learnings.md` and `Explanation.md`.

## Concepts

### Connection pool (node-postgres `pg.Pool`)

A pool keeps up to `max` open connections and lends them out. Settings that matter here: `max` (`PG_POOL_MAX`), `connectionTimeoutMillis` (2000 ms: how long
`connect()` may wait for a connection), `idleTimeoutMillis` (30000 ms: an idle connection is closed after this). Counters: `totalCount` (open), `idleCount`
(free), `waitingCount` (requests waiting). Why it matters: the pool size is a hard limit on how many INSERTs run at the same time from this one server.

### Why a request holds a connection longer than the INSERT

In B1 the INSERT itself was about 0.02 ms, but the commit waits for the WAL to be flushed to disk (fsync, about 2.5 ms in B1). The connection is busy for the
whole time. So the hold time is about 5.4 ms (10 connections / 1,850 req/s from B1), not 0.02 ms. The ceiling of a pool is `max / hold time`.

### Little's Law and the pool ceiling

Connections in use = arrival rate x hold time. Example: 2,150 req/s x 5.4 ms = 11.6 connections busy on average. A pool of 10 cannot supply 11.6, so a queue
grows. A pool of 25 can (utilization 11.6 / 25 = 46%). Ceiling = `max / hold time`: 5 / 0.0054 = 925 req/s, 10 / 0.0054 = 1,850, 25 / 0.0054 = 4,600.

### Why the pool timeout may not appear (the VU cap)

k6 has at most 3000 VUs (virtual users, each does one request at a time). If the server is slow, at most 3000 requests are in flight, so the longest queue wait is
about 3000 / capacity. The pool timeout (2 s) needs capacity < 3000 / 2 s = 1,500 req/s. B1 had capacity 1,865 and p50 1.57 s: no 503. So at pool 5 I expect
503 (capacity about 925), but at pool 10 I do not.

### Group commit

Postgres can flush the WAL once for many commits that arrive at nearly the same time. More concurrent connections means more commits per flush, so up to a point
a bigger pool gives more throughput on the same disk. This is why I expect pool 25 to beat pool 10 even though the disk was 97% busy in B1. (Not verified: I will read
the `pg_stat_database` commit counts and the disk flush counts in the captures.)

### Postgres process per connection

Each connection is a separate OS process (a backend) with its own memory (roughly 4 to 8 MB each is my estimate, not verified), and the scheduler has to switch between
them. With only 2 CPU cores, 90 active backends compete for them: more context switches, more cache misses, more contention on shared structures. This is the
cost of "more connections", and why more is not always better.

### max_connections and reserved slots

`max_connections=100`. `superuser_reserved_connections` (default 3) are kept for superusers, so normal users get about 97. postgres-exporter holds 1, and my own `psql`
checks hold 1 more. So usable for the pool: about 95. A pool of 120 cannot get there: the extra attempts fail with `53300 too many clients already`. Because each failing attempt
returns quickly, they become fast 503 and can look like "good latency" while hiding a high error rate. I must look at the error rate and its type, not only at latency.

### Error types I want to tell apart

| Type                | Where it comes from                                      | Meaning                                    |
| ------------------- | -------------------------------------------------------- | ------------------------------------------ |
| `pool_timeout`      | node-postgres: "timeout exceeded when trying to connect" | waited more than 2 s for a free connection |
| `too_many_clients`  | Postgres error code `53300`                              | Postgres is full (max_connections)         |
| `statement_timeout` | Postgres error code `57014`                              | a query ran more than 5 s                  |
| `other`             | anything else the filter treats as a DB error            | to look at manually                        |

### Open model and dropped iterations (from B1)

k6 `constant-arrival-rate` starts requests on schedule even if the server is slow. When all 3000 VUs are busy it cannot start more and counts `dropped_iterations`.
With a slow pool, dropping is part of the result, not a generator bug; with a healthy pool (25) it must be 0.

## Explanation of the commands

### Run the experiment

```bash
LEVELS="25" REPEATS=1 DURATION=30s PROBES=0 RESULTS_NAME=b2-trial ./run.sh
```

1. `LEVELS="25"`: the list of `PG_POOL_MAX` values to test (default `5 10 25 50 90 120`).
2. `REPEATS=1`: how many times the whole list is run. The default is 3, because one run can be an outlier (B1 had one).
3. `DURATION=30s`: length of one measured run (default 60 s).
4. `PROBES=0`: skip the 2 extra probe runs where heavy tools are allowed.
5. `RESULTS_NAME=b2-trial`: results go to `results/b2-trial/`, so a trial does not mix with the real run.
6. `DRY_RUN=1` in front prints the plan and the expected minutes and runs nothing.

### Start the server with one pool size

```bash
( cd services/ingest-api && exec env PG_POOL_MAX=25 taskset -c 0,6 node --env-file=.env dist/main.js )
```

1. `( ... )`: a subshell, so the `cd` does not change my shell.
2. `exec`: replaces the subshell by node, so the saved pid is the node pid and `kill` stops node itself.
3. `env PG_POOL_MAX=25`: sets the variable for this process only. `--env-file=.env` does not overwrite a variable that is already set, so this wins over the `.env` value (not verified for every node version; the script checks `/debug/stats` and stops if the pool max is wrong).
4. `taskset -c 0,6`: pins the process to logical CPUs 0 and 6 (one physical core), so the server does not move around and does not share a core with Postgres or k6.
5. `node --env-file=.env dist/main.js`: runs the built app and loads `.env` (database URL, API keys).

### Check the pool size the server really uses

```bash
curl -s localhost:8000/debug/stats
```

1. `-s` is silent: no progress bar.
2. The JSON has `pool.max`, `pool.total`, `pool.idle`, `pool.waiting`. The script compares `pool.max` with the value it asked for.

### Pin containers to CPUs

```bash
docker update --cpuset-cpus 1,7,2,8 ep-postgres
```

1. `docker update` changes a running container, no restart needed.
2. `--cpuset-cpus 1,7,2,8` lets the container use only these logical CPUs (two physical cores, matching `cpus: 2`).
3. The old value is read first with `docker inspect -f '{{.HostConfig.CpusetCpus}}'` and put back at the end.

### Postgres queries the script runs

```bash
docker exec -i ep-postgres psql -U eventpulse -d eventpulse -X -At -c "select count(*) from events"
```

1. `docker exec -i`: run a command inside the container; `-i` keeps stdin open.
2. `psql -U ... -d ...`: user and database.
3. `-X`: do not read `~/.psqlrc` (so no surprise settings).
4. `-A -t` (`-At`): unaligned output, tuples only: just the value, easy to use in bash.
5. `-c "..."`: run this one statement.

Statements used: `TRUNCATE events` (empty the table), `CHECKPOINT` (write dirty pages now, so a checkpoint does not land inside the next run),
`SELECT pg_stat_statements_reset()` (clear the statement statistics), `show max_connections` (a setting).

### Probe-only commands (never in measured runs)

```sql
select application_name, state, wait_event_type, wait_event, count(*)
from pg_stat_activity where datname='eventpulse' group by 1,2,3,4 order by 5 desc;
```

1. `pg_stat_activity` has one row per backend (connection).
2. `application_name` is `ingest-api` for the pool (set in `db.module.ts`), so I can tell the pool from the exporter and psql.
3. `state` is `active`, `idle`, `idle in transaction`...; `wait_event_type` / `wait_event` say what an active backend waits for (`LWLock WALWrite`, `IO WALSync`, `Client ClientRead`).
4. `count(*)`: how many backends in each group. Reading it at one moment is a sample, not a measurement.

```bash
iostat -x 1 5      # disk: w/s, w_await, f/s (flushes), f_await, %util. The first report is the average since boot: ignore it.
vmstat 1 5         # r (runnable), b (blocked), cs (context switches/s), id/wa (idle, io wait)
pidstat -u -p PID 1 5   # CPU of one process; -u = CPU, -p = pid, "1 5" = every 1 s, 5 times
```

These cost CPU and disturb the run, so they run only in probe runs and I do not use probe latency numbers.

### PromQL used in the panels (non-trivial ones)

```promql
histogram_quantile(0.99, sum by (pool_max)(rate(k6_http_req_duration_seconds[$__rate_interval])))
```

1. k6 sends latency as a native histogram, so there is no `_bucket` series; `histogram_quantile` works on it directly.
2. `rate(...[$__rate_interval])`: per-second rate over the window Grafana chooses.
3. `sum by (pool_max)`: one curve per pool size (the k6 tag), so the p99-vs-pool-size picture appears.

```promql
sum by (type)(rate(ep_db_errors_by_type_total[$__rate_interval]))
```

One line per error type: it separates `pool_timeout` from `too_many_clients`.

## How to read the output

| `summary.tsv` column    | Meaning                                                                        |
| ----------------------- | ------------------------------------------------------------------------------ |
| `pool`                  | `PG_POOL_MAX` of the run                                                       |
| `achieved_rps`          | answers per second k6 got; compare with the offered 2150                       |
| `n202 / n503 / n_other` | accepted / 503 / anything else (4xx, 500, timeout)                             |
| `err_pct`               | share of answers that are not 202                                              |
| `dropped`               | iterations k6 could not start (VU cap reached)                                 |
| `audit`                 | `ok` when rows in the table = 202 answers; `LOST n` is the serious one         |
| `err_types`             | from the server, e.g. `pool_timeout=120;too_many_clients=4`                    |
| `verdict`               | `OK` or `COLLAPSED:...` with my B1 definition (err > 1%, p99 > 1 s, lost rows) |

## Pitfalls to watch

1. **Probe tools inside a measured run** pollute latency (Stage 0 lesson). Heavy tools only in the 2 probe runs.
2. **Usable slots are fewer than `max_connections`**: 3 reserved, 1 exporter, my psql. Count them before saying "90 fits".
3. **Pool timeout may be hidden by the VU cap** (see above). If pool 5 shows no `pool_timeout`, I must check capacity vs 1,500 req/s before blaming the pool.
4. **Fast errors look like fast latency.** At pool 120, 503 answers are quick. Always read error rate next to latency.
5. **Order effects**: repeat 2 runs the pool sizes in reverse on purpose.
6. **Pool grows during the first seconds** of each run (the server restarts empty); small against 60 s, but it is part of the measurement.
7. **One machine, loopback, 2 Postgres cores**: results are relative. A bigger Postgres machine would move the optimum.
8. **Stale series in Grafana**: filter by `testid` / `pool_max` or zoom with `runs.tsv`, since the server restarts for every run.
9. **`TRUNCATE events` deletes data**: do not point this at a database I care about.
