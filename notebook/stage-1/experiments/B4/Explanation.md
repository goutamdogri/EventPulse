# B4 Experiment explanations

Status: COMPLETE. Results added 2026-11-21.

## Purpose

I want to know if a synchronous per-tenant counter, incremented in the same transaction as every insert, makes all the writers of one tenant wait for each other on a single row lock. In Stage 1 a 202 means "committed to Postgres", so anything inside that transaction is on the critical path of every request. When I know the answer I can decide whether counting per tenant belongs in the write path at all, or whether later stages should count downstream from the events.

## Hypothesis

With `COUNTER_MODE=hot` every request runs `BEGIN; INSERT; UPDATE tenant_counters SET n = n + 1 WHERE tenant_id = $1; COMMIT`. The `UPDATE` locks the tenant's counter row until the `COMMIT` is flushed to disk. All requests of one tenant therefore go through that lock one at a time, so the throughput of one tenant is capped at 1 / lock hold time, however many connections or CPUs there are. Many tenants have many independent rows, so the cap grows with the tenant count until another limit (pool, server CPU) is reached. While the writers wait, Postgres and the server do almost no work, so CPU stays low while latency is high.

## Predictions (written before the run)

Numbers I use: pool 25, h0 (hold time of one autocommit insert) about 5 ms from B2/B3, no-counter capacity about 4300 req/s from B3 level 0 (about 4300 accepted at 5000 offered).

1. **One hot tenant is capped at about 300 req/s.** The lock is held from the `UPDATE` until the end of `COMMIT`. I guess that is one round trip plus the WAL flush, about 3 ms (guess, range 2 to 5 ms). 1 / 3 ms = 333 per second. Range 200 to 500 req/s. At 3000 req/s offered this is 10 times too much, so the over run with 1 tenant collapses: pool timeouts (503) after 2 s, p50 in seconds.
2. **The wait is a lock wait, not work.** In the 1-tenant over run about 20 or more of the 25 sessions show `Lock / transactionid` (24 waiters + 1 holder, minus those between statements). Little's Law: 25 connections / 333 per s = 75 ms per request inside the database, if the pool were the only queue.
3. **Low CPU.** In the 1-tenant over run Postgres uses well under 1 of its 2 cores and the server cores stay under about 30%. 333 transactions per second of about 0.3 ms work each is about 0.1 core. I expect the CPU panel to look idle while the latency panel looks collapsed.
4. **Capacity grows with tenants.** Capacity is about min(N x 333, ceiling). The ceiling is pool / hold time of the 4-statement transaction: 25 / (5 ms + 2 extra round trips of about 0.3 ms) = 25 / 5.6 ms, about 4400, but the server CPU limit in B3 was about 4300, so I use about 3800 for hot mode (extra statements cost server CPU too, guess). So: 10 tenants about 3000 (not 3330: uneven spread of random tenants and connections held by waiters), 100 tenants about 3000 (offered rate, near the ceiling), 1000 tenants about 3000.
5. **1000 tenants is close to the control.** With 3000 req/s offered, `hot:1000` and `off:1000` both accept about 3000 (below both ceilings). The cost of the counter shows up as latency, not as capacity: 3 extra round trips, so p50 up by about 1 to 2 ms.
6. **Under phase (150 req/s) is the same for every tenant count.** 150 is below 333, so no queue forms. p50 about 5 to 7 ms for hot at every tenant count, about 5 ms for off. If the hot 1-tenant under run is much slower than hot 1000, my lock-hold guess is wrong.
7. **No deadlocks.** Every transaction locks one counter row only, so `pg_stat_database_deadlocks` stays 0.
8. **Control `off` does not depend on tenants.** `off:1` and `off:1000` accept the same rate within noise (about 3000 at 3000 offered).
9. **More hardware does not fix it (probe).** `probe_pool50` (hot:1, pool 50): accepted rate stays about 300 (within noise of the pool 25 run), and the lock waits and pool wait times are longer, not shorter, because there are more waiters.

What I expect NOT to change: Postgres CPU in the 1-tenant run (stays low), the under-phase latency between tenant counts, and the accepted rate of `off` between 1 and 1000 tenants.

## Setup (planned)

| Item              | Value                                                                                                                                                             |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Server            | ingest-api (NestJS), `node --env-file=.env dist/main.js` under `taskset -c 0,6`, `PG_POOL_MAX=25`, `COUNTER_MODE=hot` or `off`, 1 to 1000 api keys                |
| Database          | Postgres 15, container 2 cpus, cores `1,7,2,8`, `synchronous_commit=on`; server connects directly (no Toxiproxy)                                                  |
| Sysctls           | none changed                                                                                                                                                      |
| Route / generator | `POST /v1/events`; k6 `constant-arrival-rate`, tenant chosen uniformly at random per request; k6 on cores `3,9,4,10`                                              |
| Matrix            | cells `hot:1 hot:10 hot:100 hot:1000 off:1 off:1000` x phases (under 150, over 3000 req/s) x 3 repeats x 45 s = 36 runs, plus 1 probe run (pool 50); about 55 min |
| Dashboard         | `infra/observability/dashboards/stage-1/B4 - Lock contention.json`                                                                                                |
| Versions          | filled in after the run from env.txt                                                                                                                              |

## Method (planned)

1. Per run: restart the server with the cell's `API_KEYS`, `COUNTER_MODE` and pool; seed one counter row per tenant; warm up 400 requests on tenant `t0`; check the counter moved (hot) or not (off); truncate `events` and reset the counters; run k6; compare the rows in `events` with the sum of the counters.
2. Measured runs are the 36 runs of the matrix. The pool 50 run is a separate probe and is never used for headline numbers. Odd repeats go through the cells in the listed order, even repeats in reverse. In the middle of every over run one cheap capture of wait events, blocked sessions and `pg_locks` is taken.
3. Discard a run, decided now: `dropped_iterations` > 0 in an under run; k6 CPU group above about 70%; the row audit fails in hot mode (rows in `events` different from the counter sum); the server did not reach the planned pool or mode. Dropped iterations in an over run of `hot:1` or `hot:10` are kept, because there all VUs wait on slow answers, and that is part of the finding. I will say so in the write-up.
4. Each headline number is the median of the 3 repeats with the range.

## What actually happened to the plan

- All 36 measured runs and the probe ran (`results/b4/`, 37 lap folders, `runs.tsv` has 37 rows, all `status=ok`). Nothing was discarded or aborted. The `b4-trial` folder is not used anywhere.
- No under run had dropped iterations (all 0) and the k6 cores never went above 32% busy, so none of my discard criteria fired.
- Over runs with dropped iterations (kept, as decided before the run): `hot:1` about 68,900 per run, `hot:10` about 63,500 to 63,900 per run (VUs all waiting on slow answers). `hot:100` had 48, 0, 14 and `hot:1000` had 73, 194, 161. Those small numbers are not a finding about locks: the answers were 202 at the offered rate and the `off` cells had 0. I flag them as a small amount of k6 scheduling loss at 3000 req/s, cause not checked.
- The row audit passed in every hot run: rows in `events` equal the sum of `tenant_counters` (checked on all 25 hot runs including the probe). Deadlocks: 0 over the whole matrix (`sum(increase(pg_stat_database_deadlocks{datname="eventpulse"}[<whole matrix>]))` at the end of the last run), no deadlock error type in `err_types`, no "deadlock" line in `server.out`.
- I did not look at the Grafana dashboard itself. All Prometheus numbers below are from instant queries I ran on the API, so I did not check that the dashboard panels draw correctly.
- The pre-run half is unchanged (compared with commit `99ed856`; only the Status line changed).

## Environment (from env.txt)

| Item               | Value                                                                                                                                                |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| Kernel / node / k6 | 7.0.0-43-generic / v24.21.0 / k6 v2.4.0                                                                                                              |
| CPU                | 11th Gen Intel i5-11400H, 6 cores, 2 threads each (12 cpus)                                                                                          |
| Postgres           | 15.17, container 2 cpus, 2 GB, cores `1,7,2,8`, `synchronous_commit=on`, `fsync=on`, `shared_buffers=128MB`, `commit_delay=0`, `deadlock_timeout=1s` |
| Pinning used       | server `0,6`, k6 `3,9,4,10`, monitoring `5,11`                                                                                                       |
| Server             | `PG_POOL_MAX=25` (50 for the probe), `PG_CONN_TIMEOUT_MS=2000`, `PG_STATEMENT_TIMEOUT_MS=5000`, direct connection (no Toxiproxy)                     |
| Code state         | 99ed856 (the pre-run commit), 0 dirty files                                                                                                          |
| Run date           | 2026-11-21, start 04:51 +05:30                                                                                                                       |

## Results and observations

Source of every number below: `results/b4/summary.tsv` (k6 lap JSON via `analyze.py`), `captures/<testid>.txt` for wait events, and Prometheus instant queries for the window `start_epoch + 8 s` to `start_epoch + 46 s` of each run (38 s, ends before the run ends). CPU: `100*(1-avg(avg_over_time(rate(node_cpu_seconds_total{job="node",mode="idle",cpu=~"<group>"}[5s])[38s:5s])))`. Mean counter UPDATE: `sum(increase(ep_pg_counter_update_seconds_sum[38s])) / sum(increase(ep_pg_counter_update_seconds_count[38s]))`. Postgres cores: `avg_over_time(sum(rate(ep_pg_container_cpu_seconds_total[5s]))[38s:5s])`.

### Over phase (3000 req/s offered), median of 3 repeats, range in brackets

| Cell                          | accepted req/s      | error %      | p50 ms           | p99 ms           | dropped per run        |
| ----------------------------- | ------------------- | ------------ | ---------------- | ---------------- | ---------------------- |
| hot, 1 tenant                 | 328 (325 to 331)    | 76.5 to 76.9 | 2000.5           | 2221 to 2227     | about 68,900           |
| hot, 10 tenants               | 1504 (1498 to 1513) | 0.5 to 1.0   | 1954             | 2042 to 2053     | about 63,500 to 63,900 |
| hot, 100 tenants              | 2999 (2999 to 3000) | 0            | 5.3 (5.2 to 5.5) | 149 (126 to 152) | 48, 0, 14              |
| hot, 1000 tenants             | 2996 (2995 to 2998) | 0            | 5.1 (5.1 to 5.2) | 173 (161 to 176) | 73, 194, 161           |
| off, 1 tenant                 | 3000 (3000 to 3000) | 0            | 5.0              | 48 (38 to 52)    | 0                      |
| off, 1000 tenants             | 3000 (3000 to 3000) | 0            | 5.0              | 55 (30 to 68)    | 0                      |
| probe: hot, 1 tenant, pool 50 | 321 (one run)       | 76.95        | 2000.5           | 2479             | 69,386                 |

### Under phase (150 req/s offered)

All 12 `hot` and 6 `off` under runs accepted 150 req/s with 0 errors and 0 dropped. p50 was 3.3 to 3.4 ms in every cell (hot and off, 1 to 1000 tenants). p99 was 5.0 to 9.7 ms; the highest is hot:1 repeat 2 with 9.7.

### Resources and waits, over phase (repeat 1 unless said; the other repeats are within a few points)

| Cell          | server CPU % | Postgres cores group % | k6 CPU % | Postgres container cores | commits/s | counter row updates/s | mean counter UPDATE ms (3 repeats) |
| ------------- | ------------ | ---------------------- | -------- | ------------------------ | --------- | --------------------- | ---------------------------------- |
| hot:1         | 19           | 31                     | 13       | 0.27                     | 361       | 331                   | 71.9, 72.6, 73.7                   |
| hot:10        | 23           | 37                     | 15       | 0.52                     | 1504      | 1474                  | 11.0, 10.9, 10.8                   |
| hot:100       | 35           | 43                     | 24       | 0.78                     | 3017      | 2987                  | 0.50, 0.62, 0.49                   |
| hot:1000      | 34           | 43                     | 23       | 0.76                     | 3057      | 3027                  | 0.16, 0.23, 0.19                   |
| off:1         | 25           | 36                     | 21       | 0.40                     | 2995      | 0                     | not applicable                     |
| off:1000      | 25           | 35                     | 20       | 0.41                     | 3030      | 0                     | not applicable                     |
| probe pool 50 | 20           | 34                     | 13       | 0.43                     | 348       | 319                   | 151.9                              |

### Mid-run captures (`captures/<testid>.txt`, one sample per run, repeat 1)

- **hot:1:** 24 blocked sessions with only 2 distinct blockers. Wait events at that instant: 15 `LWLock/LockManager`, 6 `Lock/transactionid`, 1 `Lock/tuple`, 3 running with no wait, 1 `Client/ClientRead`. `pg_locks`: 23 `transactionid ShareLock` not granted, 1 `tuple ExclusiveLock` not granted.
- **hot:10:** 15 blocked sessions, 7 distinct blockers. 8 `Lock/transactionid`, 2 `Lock/tuple`; also 6 idle plus 3 idle-in-transaction plus 3 active on `Client/ClientRead` (the database waiting for the API), and 3 on `WALWrite` or `WALSync`.
- **hot:100 and hot:1000:** 0 blocked sessions. 12 `LWLock/WALWrite`, 1 `IO/WALSync`, 13 idle on `Client/ClientRead`.
- **probe pool 50:** 49 blocked sessions, 2 blockers; 40 `Lock/transactionid`, 7 `LWLock/LockManager`.

### What I saw

1. With 1 hot tenant the accepted rate is 328 req/s (325 to 331), 11 times below the offered 3000. 1 / 328 = 3.05 ms per transaction. 76.5% of the requests failed with `pool_timeout` (about 50,500 per run) and the p50 sits at 2000.5 ms, which is the pool connection timeout, not a database time.
2. In that run Postgres used 0.27 cores of 2 (container) and the server cores were 19% busy. Throughput collapsed while neither was busy.
3. The mean counter UPDATE took 72 ms at 1 tenant. A request waits behind about 24 others at 3 ms each: 24 x 3.05 ms = 73 ms. It matches.
4. The pool 50 probe accepted 321 req/s (the pool 25 runs: 325 to 331), and the mean UPDATE doubled to 152 ms (49 waiters x 3.1 ms = 152 ms).
5. 10 tenants accepted 1504 req/s, about half of the offered rate and half of my prediction. That is 150 req/s per tenant if all 10 rows are busy, or 6.6 ms per transaction. It collapsed less than 1 tenant (error 0.5 to 1.0%) but p50 is still about 1950 ms.
6. 100 and 1000 tenants accepted the full 3000 req/s. Their p50 over was 5.3 and 5.1 ms against 5.0 ms for `off`. Their mean counter UPDATE was 0.5 and 0.2 ms.
7. The `off` cells do not depend on tenant count: both accepted 3000 req/s, p50 5.0 ms.
8. Under phase: no difference between hot and off at all (p50 3.3 to 3.4 ms).
9. p99 over is higher with the counter on: 149 ms (100 tenants) and 173 ms (1000 tenants) against 48 to 55 ms for `off`, even though nothing is waiting for a lock there.
10. Mid-run wait events show the waiters are not all labelled `Lock/transactionid`: at 1 tenant 15 of the 24 blocked sessions were on `LWLock/LockManager`.

## Explanation

- **One hot row serialises.** Every request of the tenant has to wait for the previous commit. With the lock held about 3 ms (1 / 328), the row can do about 330 transactions per second. The offered 3000 is 9 times more, so the queue grows until the pool is full of waiters and requests time out after 2 s. That is why the p50 is exactly the timeout (observation 1). The 72 ms mean UPDATE wait is the queue length (24 other sessions) times the hold time (observation 3). This is Observed for the rates and waits, Inferred for the 3 ms being the commit flush: I did not measure the flush time directly, but 3.05 ms is what the numbers give.
- **Low CPU is the same fact seen from the side.** A session in a lock wait does no work. At 328 transactions per second the database used 0.27 cores (observation 2). More CPU would only give more idle cores.
- **More pool does not fix it.** Doubling the pool kept the rate at about 321 and doubled the wait, because the extra connections are extra waiters (observation 4). One probe run, so this is Observed once, not a repeat.
- **Many tenants remove the queue.** At 100 and 1000 tenants there were 0 blocked sessions in the capture, the counter UPDATE took 0.2 to 0.5 ms and the database waits were on the WAL (`WALWrite`), which every commit needs (observation 6, mid-run captures). The counter still costs something: p99 rose from about 50 ms to about 150 to 175 ms, and Postgres used 0.76 to 0.86 cores against 0.40 (the transaction has 4 statements instead of 1 and 2 more round trips). I did not isolate why the p99 rose; the extra statements and the WAL queue are the obvious candidates (Likely, not verified).
- **10 tenants gave half of what I predicted.** I do not know exactly why. What the data shows: 7 distinct blockers in the capture, so not all 10 rows had a queue at that instant; the mean UPDATE was 11 ms; the capture had 9 sessions waiting on the API or on idle-in-transaction, not on a lock; the commits needed the WAL (`WALWrite`, `WALSync`). Two explanations fit and I cannot tell them apart with this data: (a) pool connections drift to the slow tenants, so fewer than 10 rows are working at the same time; (b) the commit flush gets slower when several tenants commit at once, so the hold time is longer than 3 ms (6.6 ms if all 10 rows are busy). Little's Law at 10 tenants: 1504 x 10.9 ms = 16.4 sessions in the UPDATE out of 25, which is consistent with both. Not resolved.
- **Hot-row waits also load the lock manager.** At 1 tenant the capture shows 15 sessions on `LWLock/LockManager` and 1 on a `tuple` lock. A row with many waiters is queued through a tuple lock, so waiting is not free inside Postgres. One sample, so Inferred.

## Status of each claim

| Claim                                                                                    | Status                                                                |
| ---------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| 1 hot tenant caps at about 328 req/s, collapses with pool timeouts at 3000 req/s offered | Observed (3 repeats, 325 to 331)                                      |
| Postgres and server are mostly idle while this happens                                   | Observed (0.27 cores, server 19 to 22%)                               |
| The wait is lock wait: mean UPDATE 72 ms = 24 waiters x 3 ms                             | Observed (3 repeats) for the number, Inferred for the arithmetic link |
| The 3 ms lock hold is the commit flush                                                   | Likely, not verified (not measured directly)                          |
| A bigger pool does not help                                                              | Observed once (1 probe run)                                           |
| 100 and 1000 tenants carry the full 3000 req/s with no lock waits                        | Observed (3 repeats each, 0 blocked in captures)                      |
| Counter costs about +0.1 to 0.2 ms at p50 and about 100 ms at p99 over                   | Observed for the numbers, cause Likely, not verified                  |
| Why 10 tenants only reach 1500 req/s                                                     | Not resolved                                                          |
| Capacity ceiling of hot mode near 3800                                                   | Not resolved (never reached: 3000 offered was the maximum)            |
| No deadlocks                                                                             | Observed (0 over the whole matrix)                                    |

## Prediction vs observation

1. _"One hot tenant is capped at about 300 req/s. ... Range 200 to 500 req/s. At 3000 req/s offered ... the over run with 1 tenant collapses: pool timeouts (503) after 2 s, p50 in seconds."_ **Matches.** 328 req/s (325 to 331), lock hold 3.05 ms against my guess of 3 ms, `pool_timeout` about 50,500 per run, p50 2000.5 ms.
2. _"The wait is a lock wait, not work. In the 1-tenant over run about 20 or more of the 25 sessions show Lock / transactionid ... 25 connections / 333 per s = 75 ms per request inside the database."_ **Partly.** 24 of 25 sessions were blocked and the mean UPDATE was 72 ms (75 predicted), but in the capture only 6 were labelled `Lock/transactionid` and 15 were `LWLock/LockManager`. The blocked count is right, the label was not what I expected.
3. _"Low CPU. ... Postgres uses well under 1 of its 2 cores and the server cores stay under about 30%."_ **Matches.** 0.27 cores, server 19% (repeats 19 to 22%).
4. _"Capacity grows with tenants ... 10 tenants about 3000 ..., 100 tenants about 3000, 1000 tenants about 3000."_ **Partly wrong.** 100 and 1000 matched (2999 and 2996). 10 tenants gave 1504, half of my 3000. My formula (10 x 333) and my guess about uneven spread were not enough. The ceiling of about 3800 was never tested, because 3000 was the highest rate.
5. _"1000 tenants is close to the control ... p50 up by about 1 to 2 ms."_ **Partly.** Accepted 2996 against 3000 for `off`, close. The p50 rose by only 0.1 ms over (5.1 against 5.0), and by 0.1 ms under (3.4 against 3.3), not 1 to 2 ms. The real cost showed up in p99 (173 against 55 ms) and Postgres CPU (0.76 against 0.41 cores), which I did not predict.
6. _"Under phase (150 req/s) is the same for every tenant count. ... p50 about 5 to 7 ms for hot at every tenant count, about 5 ms for off."_ **Partly wrong.** The same-for-every-cell part matches (3.3 to 3.4 ms everywhere, hot and off). The absolute number was wrong: 3.3 ms, not 5 to 7. My h0 of 5 ms came from B3, which went through Toxiproxy; B4 connects directly.
7. _"No deadlocks."_ **Matches.** 0 over the whole matrix.
8. _"Control `off` does not depend on tenants ... about 3000 at 3000 offered."_ **Matches.** 3000 and 3000, p50 5.0 ms for both.
9. _"More hardware does not fix it (probe) ... accepted rate stays about 300 ... lock waits and pool wait times are longer."_ **Matches.** 321 req/s (one run) against 325 to 331; mean UPDATE 152 ms against 72 ms.

## Verdict on the hypothesis

Confirmed with corrections. A synchronous per-tenant counter makes all writers of one tenant queue on one row lock: 1 tenant can do only about 328 req/s, the queue turns into pool timeouts, and it happens at 0.27 Postgres cores, and a bigger pool only lengthens the wait. The correction is that capacity does not simply grow as tenants x 330: 10 tenants reached only 1504 req/s, and I could not say why, and the counter also costs p99 latency and Postgres CPU even with 1000 tenants and no lock wait. The purpose was met: I can decide that a synchronous per-tenant counter does not belong in the write path of ingest, because its ceiling is set by the busiest tenant and not by the hardware, and that later stages should count downstream from the events.

## Limitations

- 3 repeats per cell, 1 run for the pool 50 probe, 1 mid-run capture sample per run (the wait-event labels are one instant, not an average).
- Only two offered rates (150 and 3000). The knee of `hot:10` and the ceiling of hot mode were not bracketed.
- The load is spread evenly over tenants. Real tenants are uneven, so a single busy tenant would hit the 328 req/s cap even in a system with 1000 tenants.
- One machine, Postgres on a 2-core container, k6 and the server on the same host: relative numbers only. The disk and the commit flush time are specific to this laptop (not measured directly).
- The k6 `dropped_iterations` of 0 to 194 in `hot:100`/`hot:1000` over runs is small but not zero and its cause was not checked.
- Window for Prometheus numbers is 38 s inside each run and averages ramp and steady state together.

## Cost

- **What the counter costs:** with 1000 tenants at 3000 req/s: Postgres CPU 0.76 to 0.86 cores against 0.40, p99 173 ms against 55 ms, p50 +0.1 ms. About 3 extra round trips and 1 extra row write per request.
- **What the failure costs:** at 1 hot tenant, 76.5% of the requests fail and the throughput is 11% of the offered rate, while 90% of the machine is idle.
- **What the fix costs (not measured):** counting downstream means the count is not exact at the moment of the request and needs another component. I did not measure that.

## Takeaways

1. A write that touches one shared row per tenant has a ceiling of 1 / (lock hold time) per tenant, about 330 req/s here. No pool size or CPU changes it.
2. A request that fails with a pool timeout and low CPU is a sign to look at lock waits first (`pg_stat_activity` wait events and `pg_blocking_pids`), not at CPU.
3. Keep the lock hold short and the row cold: nothing synchronous per tenant in the write path. Count downstream from the events, in a later stage.
4. Even when no one waits, the extra statements cost p99 and CPU, so measure the cost at the "no contention" level too.
5. Predicting capacity as (rows x per-row rate) failed at 10 tenants. Before I use that formula I need to measure the commit flush and the pool drift (open for a follow-up).
