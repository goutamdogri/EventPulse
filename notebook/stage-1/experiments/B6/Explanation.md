# B6 Experiment explanations

Status: PRE-RUN. Written 2026-11-21 before any measured run. The sections after the marker are appended after the experiment.

## Purpose

I want to know what `synchronous_commit=off` buys and what it costs in the ingest API: how much more throughput and how much lower latency I get, and how many events the API said 202 to are missing after Postgres is killed hard. The 202 is the promise of this stage, and B1 to B5 showed that the commit flush is most of the time a request holds a connection, so this is the first time I can put a number on the price of that promise. Once I know it I can say in numbers what durability costs, and I can recognise the same trade-off when Kafka `acks` appear in Stage 2. This is a diagnostic experiment, not a setting I plan to keep.

## Hypothesis

`synchronous_commit=off` raises throughput significantly, but a hard crash loses the most recent commits. The mechanism: with `on` every COMMIT waits for a WAL flush to disk (about 5 ms of the 5.8 ms a request holds its pool connection, B1 and B3). With `off` the commit returns at once and the WAL writer flushes every 200 ms, so each connection is free sooner and the pool allows more requests per second. The commits that are only in Postgres' WAL buffers when the process is killed are gone, although they were already answered with 202.

## Predictions (written before the run)

Constants from earlier experiments: pool 25, hold time with `on` about 5.8 ms (B3), Node CPU about 0.16 to 0.175 ms per request (B1), Postgres CPU about 0.164 ms per row (B5), Postgres limited to 2 cores, about 700 B of WAL per row (B5 level 0).

1. **Capacity with `on` (`over`, 8000 req/s offered).** 25 / 0.0058 s = 4,310 req/s. I expect 4,100 to 4,500 accepted, the rest 503 (pool timeout) or dropped iterations. This is what B3 measured at level 0.
2. **Capacity with `off` (`over`).** The commit wait is gone, so the hold time falls to roughly the INSERT itself, 1 to 1.5 ms. The pool bound would be 25 / 0.0013 s = 19,000 req/s, so the pool stops being the limit. The next limit is the single Node thread: 1 / 0.00017 s = 5,900 req/s (range 4,800 to 6,200 from 0.16 to 0.21 ms per request). Postgres CPU would allow 2 / 0.000164 = 12,000 req/s, so Node comes first. I predict **about 5,500 accepted req/s (range 4,800 to 6,200)**, which is a gain of about **+28% (range +10% to +45%)**. It is much smaller than the pool arithmetic alone (19,000) suggests, because the bottleneck moves to Node.
3. **`at` (4000 req/s).** Both modes accept 4,000 (on is at 4000 / 4310 = 93% of its capacity, off at 4000 / 5500 = 73%). p50 falls from about 5 ms with `on` to about 2 ms with `off`. p99 with `on` is high because it is at 93% utilisation, about 12 to 20 ms, and with `off` about 4 to 8 ms (about 3x lower). Errors 0 in both.
4. **Disk flushes.** With `on` about 380 flushes per second (what B1 measured at 1,050 to 1,500 req/s; I expect about the same at 4,000 because the disk was already 97% busy). With `off` under 50 per second (the WAL writer wakes 5 times per second, plus checkpoints and other containers). So the flushes per accepted request fall from about 0.1 to about 0.01 or less.
5. **Pool.** With `on` at 4000 req/s the pool is nearly full (in use about 4000 x 0.0058 = 23 of 25). With `off` about 4000 x 0.0013 = 5 of 25.
6. **WAL volume per row does not change** (about 700 B for both modes). `off` changes when the WAL is flushed, not how much is written.
7. **Rows lost after SIGKILL with `off` (4000 req/s).** Data the kernel already has survives a process kill, so only WAL still in Postgres' buffers is lost. The WAL writer wakes every 200 ms, so the unwritten commits are the ones of the last 0 to 200 ms: on average 100 ms x 4000 req/s = **about 400 rows (range 0 to 800)**. The documented worst case is 3 x 200 ms = 600 ms = 2,400 rows; I do not expect to reach it. Another effect pulls the number down: Postgres writes WAL earlier when more than `wal_writer_flush_after` (1 MB, about 1,400 rows of 700 B) is waiting, which at 4000 req/s x 0.7 KB = 2.8 MB/s happens about every 0.35 s, so it does not bind before the 200 ms timer. I do not know this for sure.
8. **Rows lost after SIGKILL with `on`: 0 acknowledged rows** (the count of rows can exceed the 202 count by a few, for requests that committed but whose answer never arrived).
9. **What I expect NOT to change:** WAL bytes per row, the number of commits per accepted request (1), and data integrity after recovery (no error, the table is simply shorter).
10. **Lost rows scale with the rate, not with the run time.** The loss is a window of time, so at 4000 req/s I expect a few hundred rows whether the run is 30 s or 5 min before the kill. I will not test this in B6; it is a note for later.

## Setup (planned)

| Item              | Value                                                                                                                                                                  |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Server            | ingest-api (NestJS), `node --env-file=.env dist/main.js`, `PG_POOL_MAX=25`, `taskset -c 0,6`, restarted before every run. Connects directly to `localhost:5434`. No server code change for B6. |
| Postgres          | `postgres:15` container `ep-postgres`, 2 CPUs (cpuset 1,7,2,8), 2 GB, `synchronous_commit` set per role by `run.sh` (default `on`), `wal_writer_delay` 200 ms, `fsync` on |
| Sysctls           | none changed                                                                                                                                                           |
| Route / generator | `POST /v1/events`, k6 `constant-arrival-rate`, 500 pre-allocated VUs (max 3000), pinned to 3,9,4,10, 100 tenants, about 500 B per event                               |
| Matrix            | modes {on, off} x phases {at 4000, over 8000 req/s} x 3 repeats x 45 s = 12 measured runs (order alternated per repeat); 6 crash probes (3 per mode, 4000 req/s, 30 s, kill at 20 s); about 29 min in total |
| Dashboard         | [B6 - Durability knob.json](../../../../infra/observability/dashboards/stage-1/B6%20-%20Durability%20knob.json)                                                        |
| Versions          | filled in after the run from env.txt                                                                                                                                   |

## Method (planned)

1. For every run: set the role setting, start a fresh server, warm up, TRUNCATE `events`, CHECKPOINT, take WAL counters, run k6 through `runner.sh`, count rows, add a row to `summary.tsv`. The measured runs (`at`, `over`) give the headline numbers for rps, latency, flushes and CPU. Repeat is the outer loop, and the order of the modes is reversed in even repeats.
2. The crash probes are separate runs after all measured runs, never used for the headline throughput numbers. In each one: k6 at 4000 req/s, `docker kill -s KILL ep-postgres` 20 s after k6 starts, the server is stopped at once, Postgres is started again, I wait until it answers, and I count rows. lost = 202 count of k6 minus rows found.
3. In the middle of each `over` run, one `pg_stat_activity` by wait event and `pg_stat_wal` capture, not in the `at` runs and not in probes.
4. **Discard criteria, decided now.** A measured `at` run is discarded if dropped iterations are above 0 or errors above 1%. Any measured run is discarded if the k6 CPU group goes above about 70% (the generator would be the limit), or if `run.sh` reports `FAILED`. An `over` run may have dropped iterations (that is how saturation looks) but not a k6 CPU above 70%. A crash probe is discarded if Postgres did not come back, if the kill did not happen while k6 was running at the full rate (k6 failed before the kill), or if the container was restarted by something other than the script. A discarded run is re-run, not replaced by hand.
5. Headline numbers are median and range of 3 repeats. The loss per mode is reported as the three values, not only a mean.

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->
