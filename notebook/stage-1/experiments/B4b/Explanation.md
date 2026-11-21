# B4b Experiment explanations

Status: COMPLETE. Results added 2026-11-21.

## Purpose

In B4, 10 tenants with a hot counter accepted 1504 req/s, about half of the 3000 I predicted from 10 rows x 333 req/s, and the data could not say why. I want to know if the missing half comes from the pool distributing connections badly, or from every commit taking longer when 10 commits happen at once. When I know this I can say if "rows x per-row rate" is a usable way to estimate the capacity of a hot-row design, and I can also say how much of the 3 ms lock hold in B4 is the disk flush.

## Hypothesis

Two explanations from B4:

- (a) **Pool drift:** with a pool of 25 connections and 10 queues, connections pile up on tenants with long queues, so fewer than 10 rows work at the same time.
- (b) **Slower parallel commits:** with 10 rows committing at once, each commit waits longer (WAL write lock), so the lock hold is about 6.6 ms and not 3 ms (1504 / 10 = 150 req/s per row, 1 / 150 = 6.6 ms).

My bet is (b), because in the B4 capture at 10 tenants I saw `WALWrite` and `WALSync` waits and 3 sessions idle in transaction, and I did not see a clear pile on a few tenants. I am only moderately sure. A bigger pool cures (a) but not (b). Turning `synchronous_commit` off removes the flush wait from the lock hold, which should cure (b) and also shrink the 1-tenant hold time.

## Predictions (written before the run)

Cells are `tenants:pool:sync`. All hot, 3000 req/s offered, pool and sync as named.

1. **Baselines repeat B4.** `10:25:on` about 1500 req/s (range 1400 to 1600). `1:25:on` about 330 req/s (range 310 to 350).
2. **A big pool does not help (supports b).** `10:60:on` stays within +-10% of `10:25:on`: 1350 to 1650 req/s. If (a) were true it would go up to about 2500 or more.
3. **A small pool hurts a bit.** `10:10:on` about 1000 req/s (range 900 to 1300). With 10 connections thrown at 10 tenants at random, the expected number of tenants with at least one connection is 10 x (1 - 0.9^10) = 6.5 rows. 6.5 rows x 150 req/s = 980 req/s.
4. **No flush, much shorter hold (supports b and shows the flush is the 3 ms).** `1:25:off` accepts at least 2000 req/s. The hold would be the UPDATE plus a commit without waiting for the disk, my guess is 0.3 to 0.5 ms, so one row can do 2000 to 3300 req/s. If it stays near 330 req/s, the hold time was never the flush and my B4 explanation is wrong.
5. **10 tenants without the flush use the full offered rate.** `10:25:off` and `10:60:off` about 3000 req/s (at least 2700), mean counter UPDATE under 1 ms. The limit becomes the offered rate (and the CPU), as it was for 100 and 1000 tenants in B4.
6. **Mean counter UPDATE in `10:25:on`** about 11 ms (B4: 10.8 to 11.0), and Little's Law gives about 1500 x 0.011 = 16 sessions inside the UPDATE.
7. **CPU stays moderate.** Server cores under about 45%. The Postgres container below 1 core for the `on` cells, and below about 1.2 cores for the `off` cells at 3000 req/s.
8. **Safety.** Deadlocks 0, and the row audit passes (events rows equal the counter sum) in every run, also with `synchronous_commit=off` because nothing crashes.

What I expect NOT to change: the `1:25:on` baseline, the deadlock count, and the error type (`pool_timeout` only) in the collapsed cells.

What would change my mind: if `10:60:on` goes up a lot, (a) is the main cause. If both big pool and sync off leave the 1500 unchanged, neither of my two explanations is right and I would look at the server (a request path of 4 statements per request) instead.

## Setup (planned)

| Item              | Value                                                                                                                                                     |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Server            | ingest-api, `COUNTER_MODE=hot`, `PG_POOL_MAX` 10, 25 or 60 per cell, `taskset -c 0,6`, direct connection to Postgres                                      |
| Database          | Postgres 15, container 2 cpus, cores `1,7,2,8`, `max_connections=100`; `synchronous_commit` set per role to `on` or `off` for each cell, reset afterwards |
| Sysctls           | none changed                                                                                                                                              |
| Route / generator | `POST /v1/events`; B4's `script.js`, `constant-arrival-rate` 3000 req/s, tenant uniform at random; k6 on cores `3,9,4,10`                                 |
| Matrix            | 7 cells x 3 repeats x 45 s = 21 runs, no probes, about 31 min                                                                                             |
| Dashboard         | B4's: `infra/observability/dashboards/stage-1/B4 - Lock contention.json`                                                                                  |
| Versions          | filled in after the run from env.txt                                                                                                                      |

## Method (planned)

1. Per run: set the role's `synchronous_commit`, check a new session sees it, restart the server with the cell's pool and keys, seed the counters, warm up, truncate `events`, run k6 for 45 s, one mid-run capture, row audit. Odd repeats go through the cells as listed, even repeats reversed.
2. Every cell is measured at one offered rate, 3000 req/s. Each headline number is the median of 3 repeats with the range.
3. For every run I read from Prometheus over the window `start_epoch + 8 s` to `start_epoch + 46 s` (the same as B4): CPU of each pinned group, Postgres container cores, commits per second, mean counter UPDATE, and the average number of sessions per wait event and state.
4. Discard a run, decided now: k6 CPU group above about 70%; the row audit fails; the self-check for `synchronous_commit` or the pool size fails (the script stops by itself); Postgres container CPU above 1.8 cores (then the CPU, not the lock, is the limit, and I say so instead of drawing a conclusion about locks).
5. Dropped iterations are not a discard reason in cells where the system is overloaded by design (like `1:25:on`). They are noted.

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->

## What actually happened to the plan

- All 21 measured runs ran and each produced a k6 lap JSON and a row in `summary.tsv` (7 cells x 3 repeats). No run was discarded. The `b4b-trial` folder is not used anywhere.
- Dropped iterations: 107 to 116 per run in `1:25:off`, 29 to 39 in two `10:60:off` runs (0 in the other), 0 in `10:25:off`. The `on` cells with a collapse had 56,000 to 69,000 (VUs waiting on slow answers). The small numbers in the `off` cells come with 0 errors and an accepted rate at the offered rate; I did not look for the cause (flagged, not discarded).
- Row audit: rows in `events` equal the sum of the counters in all 21 runs (also with `synchronous_commit=off`). Deadlocks: 0 (`sum(increase(pg_stat_database_deadlocks{datname="eventpulse"}[<whole matrix>]))`), no "deadlock" in `server.out`.
- After the run `show synchronous_commit` in a new session reads `on`: the role override was removed.
- The pre-run half is unchanged (`git diff` against the pre-run commit `31b0887` shows nothing above the marker except the Status line).

## Environment (from env.txt)

| Item               | Value                                                                                                                      |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------- |
| Kernel / node / k6 | 7.0.0-43-generic / v24.21.0 / k6 v2.4.0                                                                                    |
| Commit             | `31b0887`, 0 dirty files                                                                                                   |
| Postgres           | 15.17, 2 cpus, cores `1,7,2,8`, `max_connections=100`, default `synchronous_commit=on`, `fsync=on`, `shared_buffers=128MB` |
| Pinning used       | server `0,6`, k6 `3,9,4,10`, monitoring `5,11`                                                                             |
| Run date           | 2026-11-21, start 06:25 +05:30                                                                                             |

## Results and observations

Sources: `results/b4b/summary.tsv` (k6 lap JSON via `analyze.py`) and Prometheus instant queries over the window `start_epoch + 8 s` to `start_epoch + 46 s` (38 s, the same as B4). Medians of 3 repeats, ranges in brackets. Queries: CPU `100*(1-avg(avg_over_time(rate(node_cpu_seconds_total{job="node",mode="idle",cpu=~"<group>"}[5s])[38s:5s])))`; commits `avg_over_time(rate(pg_stat_database_xact_commit{...}[5s])[38s:5s])`; mean counter UPDATE `sum(increase(ep_pg_counter_update_seconds_sum[38s])) / sum(increase(ep_pg_counter_update_seconds_count[38s]))`; sessions `avg_over_time(sum(pg_stat_activity_count{job="postgres",datname="eventpulse",<filter>})[38s:1s])` with the filter `wait_event_type="Lock"`, `wait_event=~"WALWrite|WALSync"` or `state="idle in transaction"`.

### Accepted rate at 3000 req/s offered

| Cell (tenants:pool:sync) | accepted req/s      | error %      | p50 ms | p99 ms       | dropped per run        |
| ------------------------ | ------------------- | ------------ | ------ | ------------ | ---------------------- |
| 10:25:on (baseline)      | 1514 (1496 to 1524) | 0.65 to 0.95 | 1951   | 2041 to 2050 | about 63,000 to 64,100 |
| 10:10:on                 | 1136 (1136 to 1141) | 21.6 to 22.0 | 2003   | 2023         | about 66,600           |
| 10:60:on                 | 1676 (1664 to 1684) | 0 to 0.08    | 1750   | 1983 to 2015 | about 56,300 to 57,100 |
| 10:25:off                | 3000 (3000 to 3000) | 0            | 0.5    | 82 to 86     | 0                      |
| 10:60:off                | 2999 (2999 to 3000) | 0            | 0.5    | 95 to 119    | 0, 29, 39              |
| 1:25:on (baseline)       | 330 (330 to 332)    | 76.4 to 76.5 | 2000.5 | 2218 to 2224 | about 68,800 to 68,900 |
| 1:25:off                 | 2997 (2997 to 2998) | 0            | 0.5    | 156 to 160   | 107 to 116             |

### Resources and waits in the same windows

| Cell      | server CPU % | Postgres cores (container) | commits/s | mean counter UPDATE ms | avg sessions waiting on `Lock` | avg sessions in `WALWrite`/`WALSync` | avg sessions idle in transaction |
| --------- | ------------ | -------------------------- | --------- | ---------------------- | ------------------------------ | ------------------------------------ | -------------------------------- |
| 10:25:on  | 22           | 0.53                       | 1560      | 10.9                   | 15.7                           | 7.2                                  | 1.3                              |
| 10:10:on  | 21           | 0.35                       | 1177      | 3.3                    | 3.5                            | 5.5                                  | 0.7                              |
| 10:60:on  | 27           | 0.79                       | 1703      | 29.7                   | 48.7                           | 8.4                                  | 1.1                              |
| 10:25:off | 36           | 0.60                       | 3012      | 0.13                   | 1.5                            | none seen                            | 0.6                              |
| 10:60:off | 34           | 0.61                       | 2977      | 0.13                   | 1.0                            | none seen                            | 0.6                              |
| 1:25:on   | 19           | 0.28                       | 357       | 72.7                   | 22.3                           | 1.0                                  | 0.2                              |
| 1:25:off  | 36           | 0.64                       | 3027      | 0.33                   | 5.3                            | none seen                            | 0.9                              |

k6 cores were at 13 to 21% in every cell, the Postgres cores group at 21 to 42%.

### What I saw

1. Switching `synchronous_commit` off removed the cap completely. One hot tenant went from 330 to 2997 req/s (9 times), and 10 tenants from 1514 to 3000. The offered rate (3000) was reached in both, so these are lower bounds on capacity, not the capacity.
2. One row with sync on had exactly 1.0 sessions in `WALWrite`/`WALSync` on average and 22 waiting on `Lock`. With sync off, the same cell had a mean counter UPDATE of 0.33 ms instead of 72.7 ms.
3. At 10 tenants with sync on, 7.2 sessions on average were waiting in `WALWrite`/`WALSync` at the same time, against 1.0 with 1 tenant. Little's Law on that stage: 7.2 / 1560 commits per s = 4.6 ms per commit, against 1.0 / 357 = 2.8 ms with 1 tenant.
4. The 25-connection baseline at 10 tenants repeated B4: 1514 req/s (B4: 1504), mean UPDATE 10.9 ms (B4: 10.8 to 11.0).
5. The pool changed the rate only a little: 10 connections gave 1136, 25 gave 1514, 60 gave 1676 req/s (+11% over 25). The sessions waiting on `Lock` grew with the pool (3.5, 15.7, 48.7) and so did the mean UPDATE (3.3, 10.9, 29.7 ms).
6. CPU was low in every `on` cell: server 19 to 27%, Postgres container 0.28 to 0.79 cores. The `off` cells used more (server 34 to 36%, 0.60 to 0.64 cores) because they do 3000 req/s of work.

## Explanation

- **The flush is almost the whole lock hold.** Without the flush wait one row can do at least 2997 transactions per second, so the hold is at most 1 / 2997 = 0.33 ms. With the flush it was 3.05 ms (1 / 330). So at least 2.7 ms, about 89% or more, of the hold time is waiting for the WAL flush. That is Observed as a bound (the offered rate stopped the measurement, so the true no-flush hold could be shorter). It also answers what I only guessed in B4: the 3 ms was the disk flush.
- **Why 10 tenants gave about half of what 10 rows should.** (b) explains most of it. With 10 commits arriving in parallel, a commit that arrives while a flush is running has to wait for that one to finish and then for the next one, so each commit waits longer. The sampled sessions in the WAL wait stage fit this: 4.6 ms per commit at 10 tenants against 2.8 ms at 1 tenant (observation 3). With the lock held 6.6 ms (1514 / 10 = 151 req/s per row, 1 / 151 = 6.6 ms) the capacity of 10 rows is 1514, not 3300. The 4.6 ms comes from sampled session counts at 1 Hz and Little's Law, so it is Inferred, not a direct timing. The cause "a commit arriving in a flush waits for two flushes" is Likely, not verified: I did not measure flush counts (`pg_stat_wal`) or flush times.
- **Pool drift (a) is a small part.** A pool of 60 raised the rate by 11% (1514 to 1676), not to about 3000. The pool of 10 gave 1136 against my 980 from the random occupancy maths (10 x (1 - 0.9^10) = 6.5 rows x 150 req/s). So the number of connections matters a little, as the drift idea says, but it does not explain the gap.
- **A bigger pool lengthens the queue.** 60 connections: mean UPDATE 29.7 ms, 48.7 sessions waiting on a lock. The extra connections only wait (as in the B4 pool 50 probe).
- **Low CPU again.** In every `on` cell the Postgres container used 0.79 cores or less; the limit is the commit path, not the CPU (Observed).

## Status of each claim

| Claim                                                                                      | Status                                                                  |
| ------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------- |
| Without the flush wait one hot row sustains at least about 3000 transactions per second    | Observed (3 repeats, 2997 to 2998; lower bound, capped by offered rate) |
| At least about 89% of the 3.05 ms lock hold is the WAL flush wait                          | Observed as a bound (hold <= 0.33 ms with sync off)                     |
| 10 tenants reach 1514 req/s because each commit waits longer when commits are parallel     | Inferred (sampled sessions + Little's Law: 4.6 ms against 2.8 ms)       |
| The longer wait is "a commit that arrives during a flush waits for the next flush as well" | Likely, not verified                                                    |
| Pool drift explains a small part: pool 60 gave +11%                                        | Observed for the +11%, Inferred for the cause                           |
| A bigger pool makes the lock wait longer and not shorter                                   | Observed (mean UPDATE 3.3, 10.9, 29.7 ms for pools 10, 25, 60)          |
| Capacity of 10 or 1 tenants with sync off                                                  | Not resolved (offered rate 3000 was reached; ceiling not found)         |
| No deadlocks, audit ok                                                                     | Observed (0 deadlocks, 21 of 21 audits)                                 |

## Prediction vs observation

1. _"Baselines repeat B4. `10:25:on` about 1500 req/s (range 1400 to 1600). `1:25:on` about 330 req/s (range 310 to 350)."_ **Matches.** 1514 (1496 to 1524) and 330 (330 to 332).
2. _"A big pool does not help (supports b). `10:60:on` stays within +-10% of `10:25:on`: 1350 to 1650 req/s. If (a) were true it would go up to about 2500 or more."_ **Slightly wrong on the number, right on the conclusion.** 1676 (1664 to 1684) is just above my range (+11%, not +-10%). It did not go near 2500, so (a) is not the main cause.
3. _"A small pool hurts a bit. `10:10:on` about 1000 req/s (range 900 to 1300)."_ **Matches.** 1136 (1136 to 1141); the 980 from the occupancy maths was a bit low.
4. _"`1:25:off` accepts at least 2000 req/s ... If it stays near 330 req/s, ... my B4 explanation is wrong."_ **Matches.** 2997 req/s.
5. _"`10:25:off` and `10:60:off` about 3000 req/s (at least 2700), mean counter UPDATE under 1 ms."_ **Matches.** 3000 and 2999; mean UPDATE 0.13 ms in both.
6. _"Mean counter UPDATE in `10:25:on` about 11 ms ... about 1500 x 0.011 = 16 sessions inside the UPDATE."_ **Matches.** 10.9 ms; 15.7 sessions sampled on `Lock`.
7. _"CPU stays moderate. Server cores under about 45%. The Postgres container below 1 core for the `on` cells, and below about 1.2 cores for the `off` cells."_ **Matches.** Server 19 to 36%; Postgres 0.28 to 0.79 cores (on), 0.60 to 0.64 (off).
8. _"Safety. Deadlocks 0, and the row audit passes ... in every run."_ **Matches.** 0 and 21 of 21.

## Verdict on the hypothesis

Confirmed with a small correction. My bet (b) was right: the missing half at 10 tenants comes from commits taking longer when several happen in parallel, and the flush is at least about 89% of the lock hold time, since removing the flush wait lifted one hot row from 330 to at least 2997 req/s. Pool drift (a) is real but small (+11% with a pool of 60). The correction is that my range for the big pool was a little too narrow, and that the longer commit wait at 10 tenants (4.6 ms against 2.8 ms) is inferred from samples and not timed directly. The purpose was met: "rows x per-row rate" fails because the per-row rate drops when rows commit in parallel (151 req/s per row at 10 tenants against 330 at 1), and I can now say that the commit flush, not the lock itself, is what sets the ceiling of the hot row.

## Limitations

- 3 repeats per cell; one offered rate (3000 req/s). In the `off` cells the offered rate was reached, so I never found their capacity.
- Wait-stage numbers are 1 Hz sampled session counts averaged over 38 s; Little's Law on them gives rough times, not measured ones. I did not collect flush counts or flush times (`pg_stat_wal`).
- Pool 60 raises Postgres load (0.79 cores), so the +11% may be partly limited by Postgres; I have no run that separates this.
- The `status` column of `runs.tsv` was lost by a bug in my script (fixed afterwards), so the status of each run is inferred from the existence of its lap JSON and summary row.
- One machine, one disk, Postgres in a 2-core container, k6 and the server on the same host: relative numbers only. The flush time is specific to this laptop's SSD.

## Cost

- **What `synchronous_commit=off` costs:** on a Postgres crash the last acknowledged transactions can be lost (the database stays consistent). In return the hot-row cap disappeared in these runs. For ingest, where 202 means "committed to Postgres", this changes what the 202 promises, so it is a product decision and not only a tuning.
- **What the failure cost (sync on):** 76% of requests failed at 1 tenant and 50% of the offered rate was lost at 10 tenants, with at most 0.79 Postgres cores in use.
- **What the pool does not cost or buy:** 60 connections instead of 25 gave +11% and made waits 2.7 times longer (29.7 against 10.9 ms).

## Takeaways

1. The ceiling of a hot row is `1 / (time the lock is held)`, and with synchronous commits the lock is held across the disk flush. The flush, not the CPU or the statement, is the number to know for a hot row.
2. Do not estimate capacity as rows x per-row rate: the per-row rate falls when rows commit at the same time (330 down to 151 req/s per row here).
3. A bigger pool is not a fix for lock waits: it adds waiters.
4. If a counter must stay synchronous, keep it out of the lock path of the commit (count downstream from the events) instead of loosening durability.
5. Before the next experiment that depends on the flush time, collect `pg_stat_wal` (flush count and time) so the claim can be measured and not inferred.
