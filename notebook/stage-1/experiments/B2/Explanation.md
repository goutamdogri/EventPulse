# B2 Experiment explanations

## Hypothesis

With `max=10`, requests queue in the pool once concurrent DB work exceeds 10; latency rises by queue wait, then requests fail on `connectionTimeoutMillis`. More connections help only up to a point, then hurt; the optimum is small relative to the connection limit.

## Predictions

Facts from B1 at pool 10: knee about 1,850 req/s; the INSERT takes about 0.02 ms, but the commit waits for the WAL fsync (about 2.5 ms, disk about 97% busy). So one request keeps a connection for about 10 / 1850 = 5.4 ms, mostly the commit.

- **Why B1 had zero 503:** k6 stops at 3000 VUs, so the longest possible wait is about 3000 / capacity. At 1,865 req/s that is 1.6 s, below the 2 s timeout (and B1's p50 was 1.57 s). A pool timeout can only appear if capacity < 3000 / 2 s = 1,500 req/s.
- **Pool 5:** capacity about 5 / 5.4 ms = 925 req/s (range 800 to 1,400). That is under 1,500, so I expect **503 from the pool wait timeout**, `waiting` pinned high, many dropped iterations.
- **Pool 10:** repeats B1: about 1,850 achieved, p50 about 1.6 s, no 503. This is my control.
- **Pool 25:** capacity about 25 / 5.4 ms = 4,600, above 2,150, so healthy: p99 about 10 to 40 ms, `waiting` about 0. Group commit (several commits sharing one fsync) should keep the hold time from growing. The limit moves to the next resource (Node CPU or the disk).
- **Pool 50 and 90:** about the same throughput as 25. Latency the same or slightly worse. Postgres memory grows, roughly 4 to 8 MB per backend: about +200 to +400 MB at 50 and +350 to +700 MB at 90 (container limit is 2 GiB). Context switches and Postgres CPU rise. **I expect a mild degradation, not a cliff**, because Postgres has only 2 cores and the query is light.
- **Pool 120:** usable slots are about 100 - 3 reserved - 1 exporter - 1 my psql = 95. When all connections are busy and requests wait, `pg` tries to open another connection and gets `53300 too many clients`; those requests get a quick 503. I expect an error share above 1% with error type **`too_many_clients`, not `pool_timeout`**.
- **Shape of the answer:** throughput and p99 improve from 5 to 25, then flatten, degrade slightly at 90, fall apart at 120. Optimum about 25, small compared with `max_connections=100`.

## Setup

| Item            | Value                                                                                                                                                                                                                                                                                                                      |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Date            | 2026-11-19, matrix started 08:23:36 +05:30, last probe ended about 08:46                                                                                                                                                                                                                                                   |
| Code            | git `8eb91c1`, 15 dirty files (the B2 server additions `ep_db_errors_by_type_total` and `ep_pg_pool_max`, the compose/Prometheus additions, the B2 files)                                                                                                                                                                  |
| Machine         | Intel i5-11400H, 6 cores / 12 logical CPUs, kernel 7.0.0-43-generic                                                                                                                                                                                                                                                        |
| Server          | NestJS on Express + `pg.Pool`, Node v24.21.0, pinned to CPUs 0,6, restarted before every run with `PG_POOL_MAX=<level>`. Pool timeout 2000 ms, statement timeout 5000 ms, `idleTimeoutMillis` 30000                                                                                                                        |
| Database        | PostgreSQL 15.17 in Docker, `cpus: 2`, 2 GiB, cpuset 1,7,2,8. `max_connections=100`, `superuser_reserved_connections=3`, `shared_buffers=128MB`, `work_mem=4MB`, `synchronous_commit=on`, `fsync=on`, `wal_level=replica`. Nothing tuned. The role `eventpulse` is a **superuser** (checked after the run with `pg_roles`) |
| Generator       | k6 v2.4.0, open model (`constant-arrival-rate`) at 2150 req/s, 60 s per run, max 3000 VUs, 10 s request timeout, pinned to CPUs 3,9,4,10                                                                                                                                                                                   |
| Monitoring      | Prometheus, Grafana, node_exporter, postgres-exporter v0.20.1 (new for B2), pinned to 5,11                                                                                                                                                                                                                                 |
| Route / payload | `POST /v1/events`, about 500 B per event, unique `event_id` each                                                                                                                                                                                                                                                           |
| Matrix          | 6 pool sizes (5, 10, 25, 50, 90, 120) x 3 repeats x 60 s = 18 measured runs, plus 2 probe runs (pool 10 and pool 90, 30 s). Repeats 1 and 3 ran 5 -> 120, repeat 2 ran 120 -> 5. About 23 min of wall time                                                                                                                 |
| Dashboard       | `infra/observability/dashboards/stage-1/B2 - Pool exhaustion.json`. **I did not bring screenshots**, see "Where the numbers come from"                                                                                                                                                                                     |

## Method

1. `./run.sh` from `loadtests/k6/stage-1/B2` (one command). For every run: stop the server, wait for Postgres to be quiet, start the server with the pool size, check `/debug/stats` reports that pool max, 100 warm-up requests, `TRUNCATE events`, reset `pg_stat_statements`, `CHECKPOINT`, 5 s settle, then k6 through `runner.sh`.
2. After every run: count rows and compare with the 202 answers (audit), read the error types from the server counter `ep_db_errors_by_type_total`.
3. The two probe runs (pool 10 and 90) had `pg_stat_activity` by wait event, `iostat`, `vmstat`, `pidstat` attached at the middle. I do not quote their latency, only the diagnostic output.
4. **Discarded:** none. **Flagged:** every pool 5 and pool 10 run has `dropped_iterations > 0` (15,000 to 39,000). I keep them because the dropping is part of the finding (the server is the limit, k6 CPU was only 14% to 19% busy), but this is my own decision.
5. A first trial run at pool 25 (`results/b2-trial`) is not used anywhere in this document.

### Where the numbers come from

- **k6 and audit numbers** (`summary.tsv`, `runs.tsv`, `env.txt`, `captures/`) are read straight from the result files.
- **Server, Postgres, host numbers** (pool total/waiting, connections, memory, CPU, context switches, commits, disk, RSS) were **not read from Grafana panels**. I (with Claude) queried the Prometheus API after the run for the window `[start + 3 s, start + 63 s]` of each run from `runs.tsv`, one value per run, then took the median and range over the 3 repeats. The window is approximate (k6 starts a second or two after `start_epoch`), "max" values are the maximum of a 1 s sampling, and everything can be re-checked in Grafana Explore. The queries are in the appendix.

## Results and observations

### Outcome per pool size (median of 3 repeats, range in brackets; offered rate 2150 req/s in every run)

"Accepted/s" is my own number: 202 answers divided by the real test length (requests / achieved rps). It is needed because k6's `achieved_rps` also counts the 503 answers, which at pool 5 would hide the real capacity.

| Pool | Achieved by k6 (answers/s) |     Accepted (202/s) |         503 share |         p50 (ms) |         p99 (ms) |      Max (ms) |     Dropped iterations | Verdict (my B1 definition)                        |
| ---: | -------------------------: | -------------------: | ----------------: | ---------------: | ---------------: | ------------: | ---------------------: | ------------------------------------------------- |
|    5 |           1445 (1445-1446) |    **926** (919-935) | 35.9% (35.4-36.4) |             2003 |             2011 |          2075 | 39,380 (39,340-39,390) | COLLAPSED, all 503 are `pool_timeout`             |
|   10 |           1845 (1844-1858) | **1845** (1844-1858) |                0% | 1590 (1575-1593) | 1686 (1679-1700) |          1746 | 15,300 (14,490-15,440) | COLLAPSED (p99 > 1 s), no errors                  |
|   25 |                       2150 |                 2150 |                0% |              5.0 | 19.3 (12.9-20.8) |   95 (80-112) |                      0 | OK                                                |
|   50 |                       2150 |                 2150 |                0% |              5.0 | 15.1 (14.4-16.9) |  132 (94-188) |                      0 | OK                                                |
|   90 |                       2150 |                 2150 |                0% |              5.0 | 17.6 (13.8-27.1) | 246 (172-307) |                      0 | OK                                                |
|  120 |                       2150 |                 2150 |      0% (0-0.04%) |              5.0 | 19.8 (14.2-20.7) | 264 (110-328) |                      0 | OK. 55 `too_many_clients` errors in repeat 1 only |

Audit: all 18 measured runs and both probes have rows in the table equal to the 202 answers (`audit = ok`). That includes the 32,600 503 answers at pool 5: no acknowledged event was lost.

### The cost side (median of 3 repeats, Prometheus, window approximate)

| Pool | Pool total (max) | Postgres connections (max, 1 s sample) | Avg active backends | Mean hold time (query incl. commit, ms) | Postgres container memory (MB) | Postgres CPU (cores of 2) | Context switches (k/s, host) | Commits per disk write | Disk busy (nvme0n1) | Node CPU (cores) | Node RSS (MB) |
| ---: | ---------------: | -------------------------------------: | ------------------: | --------------------------------------: | -----------------------------: | ------------------------: | ---------------------------: | ---------------------: | ------------------: | ---------------: | ------------: |
|    5 |                5 |                                      6 |                 4.7 |                                    5.38 |                  265 (168-266) |                      0.17 |                           48 |       1.21 (1.02-1.24) |                 93% |             0.30 |           514 |
|   10 |               10 |                                     12 |                 9.1 |                                    5.42 |                  308 (283-318) |                      0.28 |                           59 |                   2.46 |                 94% |             0.35 |           518 |
|   25 |               25 |                                     26 |                 8.6 |                                    4.53 |                  348 (333-353) |                      0.30 |                           61 |                   2.79 |                 93% |             0.34 |           264 |
|   50 |               50 |                                     52 |                 9.0 |                                    4.59 |                  392 (392-407) |                      0.31 |                           62 |                   2.79 |                 93% |             0.34 |           254 |
|   90 |       90 (69-90) |                             91 (70-91) |                 8.8 |                                    4.58 |                  438 (426-462) |                      0.32 |                           62 |                   2.80 |                 93% |             0.35 |           283 |
|  120 |   **97 (91-99)** |                             93 (68-98) |                 8.9 |                                    4.57 |                  456 (430-464) |                      0.32 |                           62 |                   2.79 |                 94% |             0.35 |           286 |

Postgres CPU throttled seconds were 0 in every run. `pg_up` stayed 1 in every run (the exporter never lost its connection, even at pool 120). k6 CPUs were 14% to 19% busy in every run.

### Representative pair: the worst small pool vs a comfortable big one (repeat 1)

|                                  |              Pool 5 |           Pool 25 |
| -------------------------------- | ------------------: | ----------------: |
| Offered / 202 answers per second |          2150 / 919 |       2150 / 2150 |
| 503 answers (`pool_timeout`)     |      32,600 (36.4%) |                 0 |
| p50 / p99                        | 2,003 ms / 2,011 ms |  5.0 ms / 12.9 ms |
| Dropped iterations               |              39,380 |                 0 |
| Pool `waiting` (max)             |         about 3,000 |                 0 |
| Mean hold time                   |             5.44 ms |           4.49 ms |
| Rows vs 202s                     |     57,020 = 57,020 | 129,000 = 129,000 |

### Probe runs (diagnostic output only; latency not quoted)

| What                                                                                           | Pool 10                                                                                    | Pool 90                                                                                            |
| ---------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------- |
| `pg_stat_activity` grouped by state / wait event (one sample)                                  | 9 active `LWLock WALWrite`, 1 active `IO WALSync`, 1 idle `ClientRead`, 1 psql             | 7 active `LWLock WALWrite`, 1 active `IO WALSync`, **29 idle** `ClientRead` + 2 other idle, 1 psql |
| Backends total / `max_connections`                                                             | 12 / 100                                                                                   | 40 / 100                                                                                           |
| `pg_stat_statements` INSERT                                                                    | 20,085 calls, mean **0.019 ms**                                                            | 23,540 calls, mean **0.020 ms**                                                                    |
| iostat `nvme0n1` (4 real intervals, the first report is the average since boot and is ignored) | 741-780 w/s, `w_await` 1.23-1.26 ms, `f/s` 370-382, `f_await` 2.45-2.49 ms, `%util` 93-95% | 738-750 w/s, `w_await` 1.29-1.31 ms, `f/s` 368-373, `f_await` 2.56-2.60 ms, `%util` 96-97%         |
| vmstat (4 intervals)                                                                           | `cs` 58-62 k/s, `wa` 7-8%, `id` 79-80%                                                     | `cs` 61-64 k/s, `wa` 7-8%, `id` 77-79%                                                             |
| pidstat, Node process                                                                          | about 33.8% of one CPU                                                                     | about 33.6% of one CPU                                                                             |

### What I saw

1. **Pool 5 is capacity-limited by the pool, and the numbers match the prediction closely.** It accepted 926 req/s (919 to 935) and the rest, 36%, got 503. All 503 were `pool_timeout` (e.g. 32,600 = the whole error count of repeat 1). The median request lasted 2,003 ms, which is the 2 s timeout. `waiting` sat at about 2,995 at its peak (the VU cap minus the connections in use) and 39,000 iterations were dropped.
2. **Pool 10 reproduces B1.** 1,845 req/s (B1 knee 1,850), p50 about 1.59 s, p99 1.69 s, **zero** 503 and zero other errors, 15,300 dropped iterations, `waiting` about 2,990. It is exactly the VU-cap explanation: 3000 / 1845 = 1.63 s of queue wait, shorter than the 2 s timeout.
3. **From pool 25 upward nothing is wrong at 2150 req/s.** p50 5.0 ms in every run, p99 between 12.9 and 27.1 ms with no trend across pool sizes (the ranges overlap), 0 dropped iterations, `waiting` 0 in all runs but one (see point 8).
4. **The pool size I need is small.** At pool 25 and above the mean hold time is about 4.5 ms and k6 sends 2150 req/s, so about 2150 x 0.00453 = 9.7 connections are busy. The measured average of active backends is 8.6 to 9.0 (about 10% lower, see Limitations). At pool 5 the measured active count is 4.7 (of 5), at pool 10 it is 9.1 (of 10).
5. **The pool ceiling formula works.** Pool / hold time: 5 / 5.379 ms = 929 vs measured 926; 10 / 5.418 ms = 1,846 vs measured 1,845.
6. **The hold time is shorter when the pool is not saturated.** 4.5 to 4.6 ms for pools 25 to 120, but 5.4 ms for pools 5 and 10 (the two saturated ones). The INSERT itself is 0.019 to 0.020 ms in the probes, so the 4.5 to 5.4 ms is the commit wait.
7. **Group commit grows with concurrency, then stops.** Commits per disk write: 1.21 (pool 5), 2.46 (pool 10), 2.79 (pool 25), 2.79 (50), 2.80 (90), 2.79 (120). Host disk writes were about 750 to 790 per second and the disk was 93% to 94% busy for every pool size, including the healthy ones. Both probes show the same wait picture: all active backends are in `LWLock WALWrite` (9 of 10 at pool 10, 7 of 8 active at pool 90) and one is in `IO WALSync`.
8. **A bigger pool opened more connections than the work needs.** Pool 90 reached 90 total connections in 2 of 3 runs (69 in the third) and the probe at pool 90 had 40 backends, 29 of them idle. Pool 120 reached 91 to 99 total connections. The pool never filled up to 120. In one pool 25 run (repeat 2) `waiting` briefly peaked at 62 (average 1.28) although the run was healthy (p99 19.3 ms, max 95 ms).
9. **Pool 120: errors in 1 of 3 runs, and they are small.** Repeat 1 had 55 `too_many_clients` errors (0.043% of requests, p99 19.8 ms, verdict OK). Repeats 2 and 3 had none. The pool total peaked at 99, 91, 97 in the three runs. In repeat 1, 99 pool connections + 1 postgres-exporter connection = 100 = `max_connections`.
10. **Extra connections cost memory first.** Postgres container memory (cgroup, includes cache) grows with the pool: 265 (5), 308 (10), 348 (25), 392 (50), 438 (90), 456 (120) MB. From pool 25 to 90 that is +90 MB for 65 more connections, about 1.4 MB per connection; from 10 to 50, 2.1 MB per connection. CPU of Postgres grows only from 0.30 to 0.32 cores (of 2) between pool 25 and 90. Host context switches are flat at 60 to 62 k/s from pool 10 on.
11. **Max latency grows with the pool, p99 does not.** Median max latency: 95 (25), 132 (50), 246 (90), 264 (120) ms. With 3 repeats and ranges that overlap (50: 94 to 188, 90: 172 to 307, 120: 110 to 328) this is a hint, not a result.
12. **Node is not the limit and memory shows the queue.** Node CPU 0.30 to 0.35 cores in all runs; RSS about 514 to 518 MB at pool 5 and 10 (requests waiting in memory) vs 254 to 286 MB at 25 to 120.
13. **Order did not matter.** Repeat 2 ran the pool sizes in reverse and its results are inside the same ranges (e.g. pool 10: 1,845 / 1,844 / 1,858; pool 5: 1,445 / 1,446 / 1,445).

## Explanation

**Why pool 5 and pool 10 fail differently.** Both pools are saturated at 2150 req/s: the ceiling is pool / hold time, and the hold time is dominated by the commit wait (about 5.4 ms), not by the INSERT (0.02 ms). Pool 5 can accept 926 req/s, pool 10 about 1,845 (observation 5). The queue is not bounded by the server but by k6: it has 3000 VUs and each VU waits for its answer, so the queue cannot hold more than 3000 requests. A request at the back of that queue waits about 3000 / capacity: 3000 / 926 = 3.2 s for pool 5 (longer than the 2 s `connectionTimeoutMillis`, so the pool throws and the API answers 503) and 3000 / 1845 = 1.6 s for pool 10 (shorter, so every request is eventually served but very late). That is the same reason B1 had zero 503 (observations 1 and 2). It agrees with the observed median of exactly the timeout at pool 5 (2,003 ms) and 1.59 s at pool 10. Evidence: `summary.tsv`, `waiting` peaks of 2,995 and 2,990.

**Why pool 25 solves it, and why 90 is not better.** At 2150 req/s with a hold time of 4.5 ms, about 9.7 connections are busy on average (Little's Law, observation 4). A pool of 10 has a utilization of about 97%, and because the hold time becomes 5.4 ms when the pool is saturated, it is above 100% (2150 x 5.42 ms / 10 = 1.17), so the queue grows without bound. A pool of 25 has utilization about 39%, so the queue almost never forms (`waiting` 0). A pool of 90 has utilization about 11%, but the throughput and p50 are exactly the same, because the disk, not the pool, is what sets the speed: 93% to 94% busy for every pool size (observation 7). More connections just wait in `idle` (29 of 40 backends idle in the pool 90 probe).

**Why group commit matters here.** Postgres flushes the WAL once for several commits that arrive together. With 5 connections there are almost never two commits waiting at the same time (1.2 commits per disk write); with 10, 2.5; with 25 and above, about 2.8, and it stops growing. So going from 5 to 25 connections is "free" capacity because it lets the same disk serve more commits per flush, but beyond about 25 there is nothing more to batch. This is why the improvement from 5 to 25 is large and the improvement from 25 to 90 is zero.

**Why the cost of big pools is small in this test.** Memory is the visible cost, about 1.4 to 2 MB per connection (observation 10), not the 4 to 8 MB I predicted. CPU and context switches did not rise beyond pool 10, because 90 connections do not mean 90 busy processes: only about 9 are active at any moment and the rest sleep.

**Why pool 120 mostly did not fail.** The pool only opens a new connection when a request arrives and no idle connection is free (this is my reading of `pg-pool`; not verified in its source), so it grows with bursts, not with the number of requests: 91 to 99 connections in 60 s, never 120. The failure needs the total (pool + exporter) to reach 100. That happened in repeat 1 (99 + 1 = 100, 55 errors) and not in repeats 2 and 3 (91 + 1 and 97 + 1). So the failure at pool 120 depends on timing, not on the setting (inferred from three runs).

**What I got wrong about usable slots.** I counted 3 reserved connections as unusable. The role `eventpulse` is a superuser, and superusers may use the reserved slots, so the real wall for this app was 100 total including the exporter, not 95. The 55 errors fit the wall at 100 (99 + 1).

**Why the fast-503 trap did not show up.** I warned that quick 503 at pool 120 could look like good latency. In the data the error share was 0.04% in one run, so this did not matter here, and it would only matter if the pool really reached the wall.

## Status of each claim

| Claim                                                                                                   | Status                                                                                                                                                         |
| ------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Pool 5 accepts about 926 req/s and answers 36% with 503 `pool_timeout`                                  | Observed (3 of 3 runs)                                                                                                                                         |
| Pool 10 reproduces B1 (1,845 req/s, p50 1.59 s, 0 errors)                                               | Observed (3 of 3 runs)                                                                                                                                         |
| The queue is bounded by the 3000 VUs and that decides whether `pool_timeout` appears                    | Inferred (arithmetic 3000 / capacity matches p50 and the presence/absence of 503 in both pools; not tested with a different VU cap)                            |
| Pool 25 to 120 handle 2150 req/s with p99 about 13 to 27 ms and no errors (except repeat 1 of pool 120) | Observed                                                                                                                                                       |
| About 9.7 connections are busy at 2150 req/s                                                            | Inferred (Little's Law; measured active count 8.6 to 9.0 is about 10% lower)                                                                                   |
| Pool ceiling = pool / hold time                                                                         | Observed (929 vs 926; 1,846 vs 1,845)                                                                                                                          |
| Group commit batches 1.2 to 2.8 commits per disk write, saturating at pool about 25                     | Observed (Prometheus counters ratio over the window); that this is the reason for the throughput gain is Inferred                                              |
| The disk (about 93% busy) is the shared limit for every pool size                                       | Observed (utilization), Inferred (as the cause of the plateau)                                                                                                 |
| Hold time is longer (5.4 ms) when the pool is saturated than when it is not (4.5 ms)                    | Observed                                                                                                                                                       |
| Why the hold time is longer when saturated                                                              | Likely, not verified. My guess: when all connections commit at the same time, each waits for the flush in progress and then its own. I have not looked at this |
| Memory per extra connection about 1.4 to 2 MB                                                           | Inferred (cgroup memory includes cache; pool 5 repeat 1 shows 168 MB vs 265 MB in the other two, so noise is at least 50 to 100 MB)                            |
| Big pools raise max latency                                                                             | Likely, not verified (ranges overlap, 3 repeats)                                                                                                               |
| Pool 120 reaches the Postgres wall only sometimes                                                       | Observed (1 of 3 runs); the mechanism "pool total + exporter = 100" is Inferred                                                                                |
| Why the pool grows to 91 to 99 when only about 9 connections are needed                                 | Likely, not verified (pool grows on each burst with no idle connection and closes idle ones only after 30 s)                                                   |
| Why one pool 25 run had `waiting` up to 62                                                              | Not resolved                                                                                                                                                   |
| Where the optimum between 10 and 25 is                                                                  | Not resolved (I did not test 12, 15, 20)                                                                                                                       |

## Prediction vs observation

| Prediction                                                                             | Observed                                                                                                                        | Verdict                                                                                                  |
| -------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| Pool 5 capacity about 925 req/s (range 800 to 1,400)                                   | 926 accepted/s                                                                                                                  | Matches                                                                                                  |
| Pool 5: 503 from the pool wait timeout, `waiting` pinned, many dropped iterations      | 36% 503, all `pool_timeout`, `waiting` 2,995, 39,380 dropped                                                                    | Matches                                                                                                  |
| Pool 10: about 1,850, p50 about 1.6 s, no 503                                          | 1,845, 1.59 s, 0 errors                                                                                                         | Matches                                                                                                  |
| Pool 25: healthy, p99 about 10 to 40 ms, `waiting` about 0                             | p99 19.3 ms, `waiting` 0 (one burst of 62 in one run)                                                                           | Matches                                                                                                  |
| Pool 25 capacity about 4,600 (pool / 5.4 ms)                                           | Not measured (fixed rate). The hold time at pool 25 is 4.5 ms, so the formula would give 25 / 4.53 ms = 5,500, still not tested | Not resolved                                                                                             |
| Pool 50 and 90: same throughput as 25, latency same or slightly worse                  | Same throughput, p99 within noise, max latency drifting up                                                                      | Matches                                                                                                  |
| Memory +200 to +400 MB at 50 and +350 to +700 MB at 90 (4 to 8 MB per backend)         | +44 MB at 50 and +90 MB at 90 relative to pool 25 (+84 and +130 relative to pool 10), about 1.4 to 2 MB per connection          | **Wrong**, too high by about 3 to 5 times                                                                |
| Context switches and Postgres CPU rise with pool size                                  | Context switches flat from pool 10 (59 to 62 k/s); Postgres CPU +0.02 cores from 25 to 90                                       | Partly wrong (CPU rises a little; context switches do not)                                               |
| Mild degradation, not a cliff                                                          | No visible loss in throughput or p99                                                                                            | Matches, and even milder than predicted                                                                  |
| Pool 120: usable slots about 95; error share > 1%, type `too_many_clients`             | 0.043% in one run, 0 in two; the type was `too_many_clients`; the wall was 100, not 95 (superuser)                              | **Wrong** on the error share and on the slot count; the type was right                                   |
| Throughput and p99 improve 5 to 25, flatten, degrade slightly at 90, fall apart at 120 | Improve 5 to 25 (a lot), flat to 120, no degradation at 90, no collapse at 120                                                  | Partly wrong: first half right, "slight degradation at 90" and "fall apart at 120" not seen at this rate |
| Optimum about 25, small compared to `max_connections`                                  | Needed concurrency about 10; anything from about 15 to 25 works                                                                 | Partly (the optimum is in the right place but I cannot say 25 vs 15)                                     |

## Verdict on the hypothesis

**Confirmed with corrections.** The first half holds: with a pool that is too small, requests queue, latency rises by the queue wait, and they fail at `connectionTimeoutMillis`. The correction is that failure on the timeout only appeared at pool 5 (capacity 926 < 1,500 req/s); at pool 10 the same queue exists but k6's 3000 VUs cap it at 1.6 s, below the 2 s timeout, so it shows up as a 1.6 s latency and dropped iterations, not as errors. The second half ("more connections help only up to a point, then hurt") is only half supported: help up to about 10 to 25 connections is clear (group commit, no queue), but at this rate I found no throughput or p99 loss from 50, 90 or 120 connections. What I found is a cost (about 1.4 to 2 MB and one backend process per connection, slightly higher max latency) and an occasional hard failure at the Postgres limit (55 errors in one of three pool 120 runs). "The optimum is small relative to the connection limit" is supported: about 10 connections of work at 2150 req/s, against a limit of 100.

## Limitations

1. **Three repeats per pool size** and one repeat of each probe. Ranges are shown; the max-latency trend and the pool 120 failure rate (1 of 3) would need more runs.
2. **One fixed rate (2150 req/s).** I cannot see where a big pool starts to reduce throughput, because the disk was already the limit and every pool of 25 or more kept up with the offered rate. A rate sweep per pool size (for example 3,000 and 4,000 req/s) is what would test "then hurt".
3. **Prometheus numbers were queried after the run, not read from the dashboard**, with an approximate window (k6 starts 1 to 2 s after `start_epoch`), a 1 s sampling and `max` values from a subquery. Averages of active backends are therefore about 10% lower than the Little's Law estimate.
4. **Postgres memory is cgroup memory** (includes cache and shared buffers), so the 1.4 to 2 MB per connection is a trend, not the size of a backend. Pool 5 repeat 1 (168 MB) shows the baseline varies.
5. **Slots assumption was wrong** (I assumed 95 usable, the superuser can use 100 minus the exporter). My README `env.txt` line "usable slots = 95" is therefore wrong for this role.
6. **Single machine, loopback, 2 Postgres cores**, one NVMe disk shared by WAL and everything else. The disk was 93% to 97% busy in all runs, so the numbers are relative and a faster disk would move the plateau.
7. **The k6 VU cap (3000) changes what failure looks like** at pool 10 (latency instead of 503). With another cap the pool 10 result would be different.
8. I did not test 12, 15 or 20 connections, so "the optimum" is a range, not a number.

## Cost

- **What a too-small pool costs:** at pool 5, 36% of requests fail and 30% of the offered iterations are dropped; the user sees a 2 s wait before the failure. At pool 10 nothing fails but every request takes 1.6 s, and the server holds about 500 MB RSS for the queue (vs about 260 MB healthy).
- **What a too-big pool costs here:** about 1.4 to 2 MB of Postgres memory and one process per connection (about 90 MB more at pool 90 than at pool 25), 0.02 cores more Postgres CPU, and a hint of higher max latency. Throughput and p99 did not change.
- **What the failure at the limit costs:** a few requests (55 of 129,002 = 0.043%) that fail with 503 and the risk that the exporter, other services or an admin cannot connect when the app uses the whole limit.
- **What the fix costs:** nothing in code; it is a number. But the number has to be chosen from the work (rate x hold time) and not from the limit.

## Takeaways

1. **Size the pool from the work, not from `max_connections`:** needed connections = rate x hold time (2150 x 4.5 ms is about 10), then add headroom (2 to 2.5x gave a healthy run here). Check utilization (needed / pool) and keep it well below 100% (pool 10 at about 97% collapsed).
2. **The hold time is the commit wait, not the INSERT.** Always measure the hold time (query duration around the whole transaction) because the ceiling is pool / hold time and it matched to 0.1%.
3. **Under-sizing fails in two different ways** depending on the generator: errors (when the queue wait is longer than the timeout) or only latency and dropped iterations (when the number of waiting clients is capped). Read errors, latency and dropped iterations together.
4. **Postgres speeds up with concurrency only until group commit is saturated** (about 25 connections here); after that extra connections only add memory.
5. **Connections that exist are not busy connections.** The pool grew to 90 to 99 although about 9 were needed. Set the max for what I can afford, not what the pool may open.
6. **Check the role before counting slots** (superuser or not) and remember that exporters and my own tools use connections too.
7. **For Stage 1 next:** B3 can use pool 25 as the baseline (the knee moved from about 1,850 to above 2,150 req/s); a separate follow-up (B2b) should repeat pools 15, 25, 50, 90 at 3,000 and 4,000 req/s to test the "then hurt" half, and pool 120 or 100 for a longer time to see how often the wall is hit.

## Appendix: the Prometheus queries I used (per run window, evaluated at `start_epoch + 63 s`)

- Pool total / waiting: `max_over_time(max(ep_pg_pool_total{job="eventpulse-nodejs"})[60s:1s])`, same for `ep_pg_pool_waiting`
- Mean hold time: `1000 * sum(increase(ep_pg_query_duration_seconds_sum[60s])) / sum(increase(ep_pg_query_duration_seconds_count[60s]))`
- Connections / active: `max_over_time(sum(pg_stat_activity_count{job="postgres"})[60s:1s])` and `avg_over_time(sum(pg_stat_activity_count{job="postgres",state="active"})[60s:1s])`
- Postgres memory / CPU: `max_over_time(ep_pg_container_memory_bytes[60s]) / 1e6` and `sum(increase(ep_pg_container_cpu_seconds_total[60s])) / 60`
- Commits / disk writes / disk busy: `sum(increase(pg_stat_database_xact_commit[60s])) / 60`, `max(increase(node_disk_writes_completed_total[60s])) / 60`, `max(increase(node_disk_io_time_seconds_total[60s])) / 60`
- Context switches, Node CPU, RSS, k6 CPUs busy: `increase(node_context_switches_total[60s]) / 60`, `(increase(process_cpu_user_seconds_total[60s]) + increase(process_cpu_system_seconds_total[60s])) / 60`, `max_over_time(process_resident_memory_bytes[60s])`, `1 - avg(rate(node_cpu_seconds_total{mode="idle",cpu=~"3|9|4|10"}[60s]))`
