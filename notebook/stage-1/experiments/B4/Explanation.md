# B4 Experiment explanations

Status: PRE-RUN. Written 2026-11-21 before any measured run. The sections after the marker are appended after the experiment.

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

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->
