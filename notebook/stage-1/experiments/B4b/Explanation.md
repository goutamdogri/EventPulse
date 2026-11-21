# B4b Experiment explanations

Status: PRE-RUN. Written 2026-11-21 before any measured run. The sections after the marker are appended after the experiment.

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

| Item | Value |
| --- | --- |
| Server | ingest-api, `COUNTER_MODE=hot`, `PG_POOL_MAX` 10, 25 or 60 per cell, `taskset -c 0,6`, direct connection to Postgres |
| Database | Postgres 15, container 2 cpus, cores `1,7,2,8`, `max_connections=100`; `synchronous_commit` set per role to `on` or `off` for each cell, reset afterwards |
| Sysctls | none changed |
| Route / generator | `POST /v1/events`; B4's `script.js`, `constant-arrival-rate` 3000 req/s, tenant uniform at random; k6 on cores `3,9,4,10` |
| Matrix | 7 cells x 3 repeats x 45 s = 21 runs, no probes, about 31 min |
| Dashboard | B4's: `infra/observability/dashboards/stage-1/B4 - Lock contention.json` |
| Versions | filled in after the run from env.txt |

## Method (planned)

1. Per run: set the role's `synchronous_commit`, check a new session sees it, restart the server with the cell's pool and keys, seed the counters, warm up, truncate `events`, run k6 for 45 s, one mid-run capture, row audit. Odd repeats go through the cells as listed, even repeats reversed.
2. Every cell is measured at one offered rate, 3000 req/s. Each headline number is the median of 3 repeats with the range.
3. For every run I read from Prometheus over the window `start_epoch + 8 s` to `start_epoch + 46 s` (the same as B4): CPU of each pinned group, Postgres container cores, commits per second, mean counter UPDATE, and the average number of sessions per wait event and state.
4. Discard a run, decided now: k6 CPU group above about 70%; the row audit fails; the self-check for `synchronous_commit` or the pool size fails (the script stops by itself); Postgres container CPU above 1.8 cores (then the CPU, not the lock, is the limit, and I say so instead of drawing a conclusion about locks).
5. Dropped iterations are not a discard reason in cells where the system is overloaded by design (like `1:25:on`). They are noted.

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->
