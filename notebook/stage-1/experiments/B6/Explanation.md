# B6 Experiment explanations

Status: COMPLETE. Results added 2026-11-21.

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

| Item              | Value                                                                                                                                                                                                       |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Server            | ingest-api (NestJS), `node --env-file=.env dist/main.js`, `PG_POOL_MAX=25`, `taskset -c 0,6`, restarted before every run. Connects directly to `localhost:5434`. No server code change for B6.              |
| Postgres          | `postgres:15` container `ep-postgres`, 2 CPUs (cpuset 1,7,2,8), 2 GB, `synchronous_commit` set per role by `run.sh` (default `on`), `wal_writer_delay` 200 ms, `fsync` on                                   |
| Sysctls           | none changed                                                                                                                                                                                                |
| Route / generator | `POST /v1/events`, k6 `constant-arrival-rate`, 500 pre-allocated VUs (max 3000), pinned to 3,9,4,10, 100 tenants, about 500 B per event                                                                     |
| Matrix            | modes {on, off} x phases {at 4000, over 8000 req/s} x 3 repeats x 45 s = 12 measured runs (order alternated per repeat); 6 crash probes (3 per mode, 4000 req/s, 30 s, kill at 20 s); about 29 min in total |
| Dashboard         | [B6 - Durability knob.json](../../../../infra/observability/dashboards/stage-1/B6%20-%20Durability%20knob.json)                                                                                             |
| Versions          | filled in after the run from env.txt                                                                                                                                                                        |

## Method (planned)

1. For every run: set the role setting, start a fresh server, warm up, TRUNCATE `events`, CHECKPOINT, take WAL counters, run k6 through `runner.sh`, count rows, add a row to `summary.tsv`. The measured runs (`at`, `over`) give the headline numbers for rps, latency, flushes and CPU. Repeat is the outer loop, and the order of the modes is reversed in even repeats.
2. The crash probes are separate runs after all measured runs, never used for the headline throughput numbers. In each one: k6 at 4000 req/s, `docker kill -s KILL ep-postgres` 20 s after k6 starts, the server is stopped at once, Postgres is started again, I wait until it answers, and I count rows. lost = 202 count of k6 minus rows found.
3. In the middle of each `over` run, one `pg_stat_activity` by wait event and `pg_stat_wal` capture, not in the `at` runs and not in probes.
4. **Discard criteria, decided now.** A measured `at` run is discarded if dropped iterations are above 0 or errors above 1%. Any measured run is discarded if the k6 CPU group goes above about 70% (the generator would be the limit), or if `run.sh` reports `FAILED`. An `over` run may have dropped iterations (that is how saturation looks) but not a k6 CPU above 70%. A crash probe is discarded if Postgres did not come back, if the kill did not happen while k6 was running at the full rate (k6 failed before the kill), or if the container was restarted by something other than the script. A discarded run is re-run, not replaced by hand.
5. Headline numbers are median and range of 3 repeats. The loss per mode is reported as the three values, not only a mean.

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->

## What actually happened to the plan

- All 18 runs (12 measured, 6 crash probes) finished with status `ok` in `runs.tsv`. No run was aborted and no setting was changed during the run.
- **I broke one of my own discard rules.** The method said an `at` run is discarded if dropped iterations are above 0. Five of the six `at` runs have dropped iterations: on-n1 323, on-n2 995, on-n3 124, off-n1 87, off-n2 56. Only `at-off-n3` has 0. Out of about 180,000 requests per run that is 0.03% to 0.55%, and the k6 CPU group was only 22% to 32% busy (so not the generator CPU), but I wrote the rule before the run so I do not get to relax it. I report all numbers and mark these runs as **contaminated**. The `at` throughput and p50 numbers are stable across repeats, but the `at` p99 numbers should be read with this in mind (see Limitations). The `over` runs always have dropped iterations (that is expected there). The crash probes have a few dropped iterations too (4 to 115).
- The verdict column of `summary-crash.tsv` says `COLLAPSED` for every probe. That is an artefact: after the kill about a third of the requests fail by design (accepted rps about 2,620 = 4,000 x 20 s / 30 s). It is not a finding.

## Environment (from env.txt)

| Item         | Value                                                                                                                                                                                                                                                                                               |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Commit       | 5ac90c8 (0 dirty files)                                                                                                                                                                                                                                                                             |
| Kernel / CPU | Linux 7.0.0-44-generic, i5-11400H, 6 cores / 12 threads                                                                                                                                                                                                                                             |
| Tools        | node v24.21.0, k6 v2.4.0                                                                                                                                                                                                                                                                            |
| Postgres     | 15.17 in `postgres:15`, 2 CPUs (cpuset 1,7,2,8), 2 GB, `shared_buffers` 128 MB, `fsync` on, `wal_sync_method` fdatasync, `wal_writer_delay` 200 ms, `wal_writer_flush_after` 1 MB, `wal_buffers` 4 MB, `max_wal_size` 1 GB, `synchronous_commit` default `on` (the role setting is changed per run) |
| Pinning      | server 0,6 (taskset), k6 3,9,4,10, Postgres 1,7,2,8, monitoring 5,11                                                                                                                                                                                                                                |
| Settings     | pool 25, 100 tenants, 3 repeats, 45 s, at 4000 and over 8000 req/s, crash probes 4000 req/s for 30 s with the kill at 20 s                                                                                                                                                                          |
| Sysctls      | none changed                                                                                                                                                                                                                                                                                        |

## Results and observations

All k6 numbers are from the lap JSONs through `analyze.py` (`results/b6/summary.tsv` and `summary-crash.tsv`). Prometheus numbers are instant queries at start + 43 s with a 35 s range (so the window is start + 8 s to start + 43 s of each run, epochs in `runs.tsv`). Queries: Node cores `rate(process_cpu_seconds_total{job="eventpulse-nodejs"}[35s])`; Postgres cores `sum(rate(ep_pg_container_cpu_seconds_total[35s]))`; flushes `sum(rate(node_disk_flush_requests_total{job="node"}[35s]))`; pool in use `avg_over_time((max(ep_pg_pool_total)-max(ep_pg_pool_idle))[35s:1s])`; query p50 `histogram_quantile(0.5, sum by (le)(rate(ep_pg_query_duration_seconds_bucket[35s])))`; CPU groups `100*(1-avg(rate(node_cpu_seconds_total{mode="idle",cpu=~"<ids>"}[35s])))`.

### Run outcome per phase (3 repeats)

| Phase                             | Mode | Accepted req/s (each repeat) | Median | p50 ms              | p99 ms              | Dropped iterations    |
| --------------------------------- | ---- | ---------------------------- | ------ | ------------------- | ------------------- | --------------------- |
| at 4000 (contaminated, see above) | on   | 3992, 3974, 3997             | 3992   | 5.2, 5.2, 5.0       | 130.7, 198.2, 94.4  | 323, 995, 124         |
| at 4000 (contaminated except n3)  | off  | 3998, 3999, 4000             | 3999   | 0.3, 0.3, 0.2       | 27.6, 45.0, 31.6    | 87, 56, 0             |
| over 8000                         | on   | 4316, 4363, 4359             | 4359   | 682.9, 677.3, 677.5 | 767.8, 768.0, 769.2 | about 161,000 each    |
| over 8000                         | off  | 7351, 7151, 7780             | 7351   | 309.0, 386.2, 56.7  | 451.9, 496.5, 249.8 | 27,487, 36,652, 9,771 |

Errors (not 202) were 0.00% in all 12 measured runs. The over-phase p50 and p99 are mostly queueing in the k6 and server queues, not database time.

Gain in `over` (accepted req/s, off / on) per repeat: 7351/4316 = +70%, 7151/4363 = +64%, 7780/4359 = +78%. Medians: 7351 / 4359 = +69%.

### Key numbers, representative runs (repeat 1)

| Number                                  | at on | at off | over on | over off | Source                  |
| --------------------------------------- | ----- | ------ | ------- | -------- | ----------------------- |
| Accepted req/s                          | 3,992 | 3,998  | 4,316   | 7,351    | lap JSON                |
| Query p50 in Node (ms)                  | 4.27  | 0.26   | 6.84    | 3.47     | Prometheus              |
| Pool in use (of 25)                     | 18.6  | 0.94   | 25.0    | 25.0     | Prometheus              |
| Pool waiting, max                       | 388   | 0      | 2,975   | 2,928    | Prometheus              |
| Disk flushes per second (host)          | 373   | 6.1    | 354     | 6.5      | Prometheus              |
| WAL syncs per 1000 rows (`pg_stat_wal`) | 92.5  | 1.3    | 81.7    | 0.8      | capture, post minus pre |
| WAL bytes per row                       | 732   | 733    | 732     | 733      | capture                 |
| Node process CPU (cores)                | 0.56  | 0.59   | 0.67    | 1.13     | Prometheus              |
| Postgres container CPU (cores of 2)     | 0.57  | 0.39   | 0.61    | 0.91     | Prometheus              |
| Disk busy % (busiest device)            | 95.3  | 5.8    | 90.7    | 5.5      | Prometheus              |
| k6 CPU group busy %                     | 28.7  | 26.2   | 29.7    | 48.9     | Prometheus              |

The other repeats look the same (for example flushes 354 to 378 per second in every `on` run and 5.8 to 7.3 in every `off` run; k6 CPU group 22% to 32% in `at` runs and 48% to 50% in `over off` runs, always under my 70% limit).

WAL writes and syncs (from `pg_stat_wal`, repeat 1, whole run): `at-on` 16,621 syncs and 16,644 writes for 178,964 commits. `at-off` **236 syncs but 15,651 writes** for 177,011 commits. `over-off` 261 syncs and 28,625 writes for 329,181 commits.

### Crash probes (`summary-crash.tsv`, 4000 req/s, kill at about 20 s)

| Probe                  | Mode | Result                                          |
| ---------------------- | ---- | ----------------------------------------------- |
| crash-on-n1 / n2 / n3  | on   | 0 lost; rows exceed the 202 count by 22, 15, 13 |
| crash-off-n1 / n2 / n3 | off  | **lost 10, 4, 7** acknowledged rows             |

Postgres answered a query within 1 s of the restart request in all 6 probes. The log shows `not properly shut down; automatic recovery in progress`, then `redo done` after 0.12 to 0.13 s of redo. No error and no corruption.

### What I saw

1. At 8000 req/s offered, `on` accepts about 4,360 req/s (4,316 to 4,363) and `off` accepts about 7,350 (7,151 to 7,780). The gain is +69% at the median.
2. At 4000 req/s both modes accept the load, but the median latency is 5.0 to 5.2 ms with `on` and 0.2 to 0.3 ms with `off`. About 20x lower.
3. The query time seen by Node falls from 4.3 ms to 0.26 ms at 4000 req/s, and the pool in use falls from 18.6 connections to 0.94.
4. Host disk flushes fall from 354 to 378 per second to 5.8 to 7.3 per second, and disk busy from 91% to 96% down to 5% to 7%. In `pg_stat_wal`, syncs per 1000 rows go from about 82 to 94 down to 0.8 to 1.3.
5. WAL per row is 730 to 734 B in every run, for both modes.
6. With `off` and over 8000 req/s the pool is full again (25 in use, up to 2,950 waiting) and Node's process CPU is 1.05 to 1.13 cores. Postgres uses 0.89 to 0.91 of its 2 cores. The disk is idle (5% to 7% busy).
7. In the `off` crash probes I lost only 4 to 10 acknowledged rows out of about 80,000. With `on` I lost none.
8. In the `on` crash probes the table holds 13 to 22 rows more than k6 counted as 202.
9. Postgres writes WAL to the kernel about once per 11 commits (15,651 writes for 177,011 commits) while it syncs only 236 times in the same run.
10. The `at` p99 is high and uneven in both modes: 94 to 198 ms with `on`, 28 to 45 ms with `off`, in runs that also had dropped iterations.

## Explanation

**Why `off` raises the ceiling (observations 1 to 4).** With `on`, a request holds its pool connection about 4.7 ms at 4000 req/s (18.6 in use / 3,992 req/s, Little's Law). Almost all of it is the COMMIT waiting for `fdatasync`: the disk flushes 354 to 378 times per second and is 91% to 96% busy, and about 11 commits share each flush (1000 / 92.5). With 25 connections and about 5.8 ms of hold time the pool can finish about 25 / 0.0058 = 4,300 req/s, which is what I measured (4,316 to 4,363). With `off` the COMMIT does not wait. The hold time falls to 0.24 ms (0.94 in use / 3,998 req/s), the flushes happen only about 5 to 7 times per second (the WAL writer's 200 ms timer, 1000 / 200 = 5), and the disk goes idle. Evidence: observations 1 to 4.

**Why the gain stops at about 7,350 and not at the pool limit (observation 6).** At 0.24 ms hold time the pool could do 25 / 0.00024 = 100,000 req/s, so the pool is not the limit any more. In the `over off` runs the process uses about 1.1 cores of CPU, which is 0.15 ms per request (1.13 / 7,351 = 0.154 ms; the `on` runs show 0.14 to 0.155 ms too), and the server CPU group is 61% to 68% busy. Postgres uses 0.9 of its 2 cores and the disk is idle, so neither is the limit. My reading is that the Node server is the limit (Inferred; I did not measure event-loop delay or per-thread CPU). The full pool and the 3.5 ms query time in `over off` fit this: when Node is behind, responses from Postgres wait in Node's event loop before the pool connection is released, so the measured query time and the hold time grow even though Postgres is quick (25 / 0.00347 = 7,200 req/s, 2% from the 7,351 measured). This is a reading of the numbers, not a test.

**Why latency drops 20x at the same load (observation 2).** The median request used to wait about 4 ms for the disk flush, and now it does not. The `on` mode is also at 4000 / 4,360 = 92% of its capacity, so even its p50 sits near the knee, while `off` is at 4000 / 7,350 = 54%. Evidence: observations 2 and 3.

**Why the loss is so small (observations 7 and 9).** The WAL buffers are not flushed often (236 syncs per run), but they are written to the kernel almost continuously: 15,651 writes for 177,011 commits is one write per 11.3 commits. One WAL page is 8192 B and one row is about 733 B of WAL, so a page holds about 8192 / 733 = 11.2 rows. So Postgres writes about one page's worth of WAL each time a page fills. After a SIGKILL the kernel still holds everything that was written, and crash recovery replays it. What is lost is the part of the last, unfinished page (up to about 11 rows) plus whatever was in flight. The measured loss of 4, 7 and 10 rows is in that range (this match is arithmetic, not a test: I did not inspect Postgres' source or the WAL on disk). So the "up to 3 x 200 ms" in the documentation is a limit for the time until data is flushed to disk; for a process kill the exposure is only the data not yet handed to the kernel. A power cut would lose the data between "written" and "synced", which is up to 200 ms of commits (about 800 rows at 4000 req/s). I did not test that (Likely, not verified).

**Why `on` shows more rows than 202s (observation 8).** The kill happens while requests are in flight. With `on` the pool holds about 18.6 requests at 4000 req/s, and those whose COMMIT had finished but whose HTTP answer had not reached k6 are in the table without a 202 in the count: 13, 15 and 22 (mean 17, close to the 18.6 in flight). With `off` only about 1 request is in flight (0.94), so the true loss is probably 1 or so more than the count I show (lost 4 to 10 is acknowledged minus rows, so the real number of lost acknowledged rows could be about 5 to 11). Inferred, from Little's Law.

**Why `at` p99 is high and uneven (observation 10).** With `on` the system is at 92% of capacity, and the pool waiting reached 388 to 606 at the peak, so queueing explains the tail (this is the idea in my prediction 3, but the size is much bigger than I predicted). With `off` there was no pool waiting at all in two of the three runs (max 0, 0, 27), so queueing in the pool does not explain a p99 of 28 to 45 ms. Dropped iterations (56 and 87) in those two runs say that k6 itself fell behind a little at the same time. I do not know the cause (Not resolved). It could be Node garbage collection, k6 pauses or something on the machine.

## Status of each claim

| Claim                                                             | Status                                                                                    |
| ----------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `off` accepts about 7,350 req/s in `over`; `on` about 4,360       | Observed (3 repeats; `off` spread 7,151 to 7,780)                                         |
| The commit flush is almost the entire hold time with `on`         | Observed (query p50 4.27 ms to 0.26 ms; flushes 373 to 6.1 per second)                    |
| Several commits share one flush with `on` (about 11)              | Observed (1000 / 92.5 syncs per 1000 rows), consistent with group commit                  |
| WAL bytes per row are the same in both modes (730 to 734 B)       | Observed                                                                                  |
| `off` p50 about 20x lower at 4000 req/s                           | Observed                                                                                  |
| `off` p99 lower than `on` at 4000 req/s                           | Observed in direction, but contaminated runs and high spread, so the size is Not resolved |
| Node is what limits `off` at about 7,350                          | Inferred (CPU numbers only)                                                               |
| A full pool in `over off` comes from Node's event loop delay      | Likely, not verified                                                                      |
| With `off`, a process kill loses up to about one WAL page of rows | Likely, not verified (observed 4 to 10 rows; the page arithmetic is mine)                 |
| A power cut would lose up to about 200 ms of commits with `off`   | Likely, not verified (not tested)                                                         |
| With `on`, no acknowledged row is lost                            | Observed in 3 of 3 probes                                                                 |
| Cause of the `off` `at` p99 of 28 to 45 ms                        | Not resolved                                                                              |

## Prediction vs observation

1. _Capacity with `on`, 4,100 to 4,500 accepted req/s._ **Matches.** 4,316 / 4,363 / 4,359.
2. _Capacity with `off` about 5,500 (range 4,800 to 6,200), gain about +28% (+10% to +45%), Node limit._ **Wrong.** 7,351 median (7,151 to 7,780), gain +69% (+64% to +78%). The limit probably is Node, but at about 1.1 cores of process CPU and 0.15 ms per request. I took 0.17 ms as the cost of one thread and 1 / 0.00017 = 5,900 as the ceiling; the process uses more than one core in total, and my ceiling calculation was too low.
3. _At 4000 req/s both modes accept 4,000; p50 about 5 ms (on) and about 2 ms (off); p99 12 to 20 ms (on) and 4 to 8 ms (off); no errors._ **Partly.** Accepted and errors match (3,992 to 4,000, 0%). p50 on matches (5.0 to 5.2 ms). p50 off is **wrong**, 0.2 to 0.3 ms instead of 2 ms. p99 is **wrong** for both: 94 to 198 ms (on) and 28 to 45 ms (off), with the contamination described above.
4. _Disk flushes from about 380 per second (on) to under 50 per second (off); flushes per request from about 0.1 to 0.01 or less._ **Matches.** 354 to 378 per second, and 5.8 to 7.3 per second; flushes per request 0.093 and 0.0015.
5. _Pool in use about 23 of 25 with on and about 5 with off at 4000 req/s._ **Partly.** `on` was 18.1 to 18.9 (I used a hold time of 5.8 ms, but at 4000 req/s it was 4.7 ms). `off` was 0.7 to 1.3, **wrong** by 4x to 7x: the hold time was 0.24 ms, not 1.3 ms. I had written in B1 that Postgres' own INSERT takes 0.02 ms, and I still guessed 1 to 1.5 ms.
6. _WAL per row does not change (about 700 B)._ **Matches.** 730 to 734 B in both modes (4% above my number).
7. _Rows lost after SIGKILL with `off`: about 400 (range 0 to 800)._ **Wrong.** 10, 4 and 7. I assumed the WAL stays in Postgres' buffers until the WAL writer runs every 200 ms; but Postgres writes it to the kernel about once per page, and a process kill does not touch the kernel's copy.
8. _Rows lost with `on`: 0 acknowledged rows._ **Matches.** 0 in 3 of 3; rows exceeded the 202s by 13 to 22.
9. _Not changing: WAL per row, one commit per accepted request, integrity after recovery._ **Matches** for WAL per row and integrity (recovery clean, `redo done` in 0.12 to 0.13 s). Commits per row is 1.00 for `on` and 0.98 to 0.99 for `off` (probably the stats update delay in `pg_stat_database`, not verified).
10. _Lost rows scale with the rate, not the run time._ **Not tested.**

## Verdict on the hypothesis

Confirmed with corrections. `synchronous_commit=off` did raise throughput a lot and cut latency, bigger than I predicted (+69% accepted rps at 8000 req/s offered, and a median of 0.3 ms instead of 5 ms at 4000 req/s), and it did allow rows to be lost on a hard kill, but far fewer than I predicted (4 to 10 acknowledged rows against 0 for `on`), because a process kill leaves the kernel's copy of the WAL intact. The purpose was met in part: I can now say what the flush costs in the request path (about 4.4 ms of a 4.7 ms hold time, and the disk busy 91% to 96% of the time), and what skipping it exposed in this test (a handful of rows, a lower bound), but not what a power cut would cost, which I could not test here.

## Limitations

- Three repeats per cell. The `off` over result spread is 9% (7,151 to 7,780) and the p50 in those runs varies a lot (56.7 to 386 ms), so only the accepted rate is a stable number there.
- The `at` runs had dropped iterations (five of six), against my own discard rule. I think they are good for throughput and p50, weak for p99. A clean re-run of `at` (maybe with a lower rate for `on`) would fix this.
- Three crash probes per mode, one kill moment (20 s), one rate. The loss figure (4 to 10) has no spread beyond 3 samples, and the count of rows lost is not separated from rows still in flight (about 1 for `off`).
- A process kill is not a power cut or a kernel crash, so the loss is a lower bound. The loss during a real crash is not measured.
- The Node limit is inferred from CPU numbers; I did not measure event-loop delay, or CPU per thread, or a profile.
- Everything ran on one machine over loopback, with k6, Node, Postgres and the disk all sharing one i5, so only the relative numbers carry over. Postgres is capped at 2 cores and 2 GB.
- The table was empty (TRUNCATE) at the start of every run, so there were no effects from a large table.
- I did not check the dashboard in Grafana for this write-up.

## Cost

- **The speed-up:** about +69% of accepted throughput and about 20x lower median latency, for one setting.
- **What it costs:** a window in which acknowledged writes may not be on disk. In this test a process kill lost 4 to 10 of about 80,000 acknowledged rows; a power cut could lose up to about 200 ms of commits (about 800 rows at 4000 req/s, not tested). The client was told 202 and has no way to know.
- **What `on` costs:** a disk that is 91% to 96% busy at 4,000 req/s and a hard ceiling at about 4,360 req/s with 25 connections; the cost is paid by every request in latency.
- **What it does not fix:** `off` moves the limit to the Node process at about 7,350 req/s; it does not remove it.

## Takeaways

1. A 202 is a promise, and the setting `synchronous_commit` decides how long the service waits before making it. In Stage 2 I should ask the same question of Kafka `acks`: what has been written, where (memory, kernel, disk, replicas) when the producer is told "ok".
2. When the commit flush leaves the request path, the bottleneck moves to the next resource. Here it moved from the disk to the Node process; I should predict the next limit with measured CPU per request, not a one-thread guess.
3. The loss depends on the kind of failure. A killed process and a dead machine are different failures with different amounts lost; the test must name which one it simulates.
4. Measure my own guesses against earlier results: B1 already said the INSERT itself takes 0.02 ms and I still predicted 1 to 1.5 ms for the hold time without the flush.
5. Keep the discard rules honest. Five `at` runs broke my own rule, so I report them as contaminated rather than change the rule after seeing the data. Next time set the `at` rate lower for the slower mode or allow a small dropped-iterations limit in advance.
6. For the next experiments: B7 and B8 should use `synchronous_commit=on` (it is the default and I restored it), and the numbers from B6 are a diagnostic, not a new baseline.
