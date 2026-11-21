# B7 Experiment explanations

Status: COMPLETE. Results added 2026-11-21.

## Purpose

I want to know how the size of one event changes what the ingest API can handle: how many events per second it accepts, how many WAL bytes and how much disk one event costs, and where the cost shows up first (Node, Postgres CPU, WAL, disk). Every later stage moves these same bytes (Kafka, replicas, ClickHouse), so the bytes per event are the unit everything else is priced in. When I know the answer I can say what a "typical" event costs and at which size the design has to change (for example storing big payloads somewhere else).

## Hypothesis

Throughput falls as the payload grows. Four things grow with the bytes: Node reads and parses the JSON body and serializes it again for the database, the body crosses the socket twice (client to server, server to Postgres), Postgres writes every byte to the WAL, and above about 2 KB per row Postgres stores the jsonb value in the TOAST table in about 2 KB chunks, each chunk being an extra insert plus an index entry. I expect the fixed cost of a request (the commit wait, the pool, the HTTP parsing) to dominate until the payload is around 10 KB, and the per-byte costs to dominate at 100 KB.

## Predictions (written before the run)

Constants from earlier experiments: pool 25, hold time with `synchronous_commit=on` about 5.8 ms at 500 B (B3), so 25 / 0.0058 s = 4,310 req/s; Node CPU about 0.17 ms per request at 500 B (B1, B6); WAL about 732 bytes per row at 500 B (B6).

1. **Capacity (`over` runs), accepted req/s.** 200 B: 4,400. 500 B: 4,300 (this repeats B6 `on`). 2 KB: 4,200. 10 KB: 4,000. 100 KB: 1,700. Maths for 10 KB: Node needs about 0.05 ms more for parsing and copying 10 KB, so hold time grows from 5.8 ms to about 6.2 ms and 25 / 0.0062 = 4,000. Maths for 100 KB: parsing the body, validating it and serializing it again costs about 0.4 ms more in Node, so 0.17 + 0.4 = 0.57 ms per request and one Node thread gives 1 / 0.00057 = 1,750 req/s. Hold time at 100 KB is about 9 to 10 ms, which would allow 25 / 0.0095 = 2,600 through the pool, so I expect Node CPU to be the limit and not the pool. I am least sure about this one (0.4 ms is a guess).
2. **The knee.** Capacity almost flat from 200 B to 10 KB (within about 10%), then a drop of more than half at 100 KB. If the drop already shows at 10 KB, my per-byte cost is too small.
3. **WAL bytes per event.** 200 B: about 430. 500 B: about 730 (B6 measured 732). 2 KB: about 2,300 to 2,600 (the 2 KB body is right at the TOAST threshold, so it may add TOAST records). 10 KB: about 11,000 (10 KB plus 6 TOAST chunks with about 90 B of index entry each). 100 KB: about 115,000 (about 52 chunks of 2 KB, each its own record, plus their index entries). WAL per second at the 100 KB capacity: 1,700 x 115 KB = about 195 MB/s.
4. **TOAST compression does not help.** The filler is random hex, and Postgres' pglz needs 25% savings to keep a compressed copy. Random hex should not reach that, so the stored size is about the body size and WAL per event stays about the body size.
5. **Disk per event (heap + TOAST + indexes).** Indexes: about the same for every size (roughly 90 to 100 B per event, they hold the uuid, tenant and time, not the payload). 200 B to 500 B: the heap holds the row (about 250 to 550 B), TOAST empty. 10 KB: TOAST about 11 KB per event (6 chunks, 3 chunks fit in an 8 KB page, so rounding adds a little). 100 KB: TOAST about 107 KB per event (52 chunks, 4 per page, 13 pages x 8 KB = 106.5 KB). At 1,700 events/s for 45 s that is about 76,000 events x 107 KB = 8 GB written to one table in one run.
6. **Latency in the `at` runs.** p50 about the same for 200 B to 10 KB (5 to 6 ms, the commit wait dominates); at 100 KB (1,500 req/s offered, 88% of 1,700) p50 about 10 ms.
7. **Pool.** At 10 KB the pool is nearly full at 3,700 req/s (3,700 x 0.0062 = 23 of 25). At 100 KB at 1,500 req/s I expect it about 1,500 x 0.0095 = 14 of 25, so not the limit.
8. **What I expect NOT to change:** the number of commits per event (1), the index bytes per event, and the 202 vs row count (every 202 is one row, audit ok).
9. **Body limit probe.** With the default `BODY_LIMIT=64kb`, a 60,000 B body is accepted (202) and a 100,000 B body is rejected with 413. This is the reason the measured runs raise the limit to 1 MB.

## Setup (planned)

| Item              | Value                                                                                                                                                                      |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Server            | ingest-api `node --env-file=.env dist/main.js` started by `run.sh` before every run, pool 25, `COUNTER_MODE=off`, `BODY_LIMIT=1mb`, `taskset -c 0,6`                       |
| Postgres          | the `ep-postgres` container, directly (no Toxiproxy), `synchronous_commit=on`, `max_wal_size` 1 GB, `shared_buffers` 128 MB, TOAST compression pglz                        |
| Sysctls           | none changed                                                                                                                                                               |
| Route / generator | `POST /v1/events`, k6 `constant-arrival-rate`, 100 tenants, random hex filler, body size set by `SIZE`, k6 pinned to CPUs 3,9,4,10                                         |
| Matrix            | sizes 200 B, 500 B, 2 KB, 10 KB, 100 KB x phase `at` (4000, 4000, 3900, 3700, 1500 req/s) and `over` (8000, 8000, 8000, 6000, 3000 req/s) x 3 repeats x 45 s, about 46 min |
| Probe             | one body-limit probe at the end (default `BODY_LIMIT`, 60 KB and 100 KB bodies, curl)                                                                                      |
| Dashboard         | [B7 - Payload size.json](../../../../infra/observability/dashboards/stage-1/B7%20-%20Payload%20size.json)                                                                  |
| Versions          | filled in after the run from env.txt                                                                                                                                       |

## Method (planned)

1. Every run: stop the server, wait until Postgres is quiet, start a fresh server, 400 warm-up requests, `TRUNCATE events`, `CHECKPOINT`, 5 s settle. Then WAL position snapshot, k6, wait, snapshot again, and the sizes of the heap, the TOAST table and the two indexes (the table was empty at the start, so these are the growth of the run).
2. Repeat is the outer loop. Odd repeats go from 200 B to 100 KB, even repeats from 100 KB to 200 B, so slow drift (disk fill, temperature) does not favor one size. Inside a size, `at` runs before `over`.
3. Headline numbers are the median and the range of 3 repeats per cell. The body-limit probe is separate and not a headline number.
4. A run is discarded (and repeated) when: an `at` run has dropped iterations above 1% of the offered requests, k6's pinned CPUs are above about 70% busy, the row audit (202 count vs rows) is not ok, or the disk has less than 20 GB free at the start. A big `over` run with dropped iterations is expected and not discarded.
5. The "knee" per size is read from the `over` runs (accepted rps) and cross-checked with the `at` runs (latency and failures at 88 to 93% of the predicted capacity).

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->

## What actually happened to the plan

- All 30 measured runs (5 sizes x `at`/`over` x 3 repeats) and the body-limit probe ran as planned. No run was aborted or repeated. All 30 rows have audit `ok` (every 202 became one row). Settings, rates and order are as in the Setup table.
- Three `at` cells were not "just under capacity" any more, because the capacity I predicted was too high: 10 KB `at` (3,700 req/s offered, accepted 3,294 median, dropped iterations 4.6% to 10% of the offered requests) and 100 KB `at` (1,500 offered, about 480 accepted, 66% dropped). My discard rule (dropped iterations above 1% in an `at` run) would discard them, so I use them only to show where the capacity is, not as "latency below capacity". The 200 B, 500 B and 2 KB `at` runs are valid (dropped below 0.5% of offered).
- At 100 KB k6 needs about 28 s to start 3,000 VUs (each builds its own 100 KB string), so the 45 s of load starts about 28 s after the start epoch in `runs.tsv`. For those runs I took Prometheus numbers from start+32 s to start+77 s (the 45 s of load, located with `rate(ep_events_inserted_total[10s])`). For the other sizes I used start+2 s to start+47 s.
- The generator check passed in the runs I use: k6's pinned CPUs were 33% (500 B), 60% to 67% (10 KB) and 54% to 57% (100 KB) busy, all under the 70% limit I set.
- The `Explanation.md` pre-run half is unchanged (checked with `git diff` against the pre-run commit; only the README was reformatted after it).

## Environment (from env.txt)

| Item              | Value                                                                                                                                                                                                                   |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Date, commit      | 2026-11-21 21:24 (+05:30), git 84ae32a (0 dirty files)                                                                                                                                                                  |
| Kernel, Node, k6  | 7.0.0-44-generic, v24.21.0, k6 v2.4.0                                                                                                                                                                                   |
| CPU               | 11th Gen Intel i5-11400H, 12 logical CPUs                                                                                                                                                                               |
| Postgres          | 15.17 in `ep-postgres`, cpuset 1,7,2,8, CPU quota 2 cores (`nanocpus=2000000000`), memory 2 GB                                                                                                                          |
| Postgres settings | `synchronous_commit=on`, `fsync=on`, `wal_sync_method=fdatasync`, `wal_compression=off`, `shared_buffers=128MB`, `max_wal_size=1GB`, `checkpoint_timeout=5min`, `default_toast_compression=pglz`, `full_page_writes=on` |
| Pinning           | server 0,6; k6 3,9,4,10; monitoring 5,11                                                                                                                                                                                |
| Server            | pool 25, 100 tenants, `COUNTER_MODE=off`, `BODY_LIMIT=1mb`                                                                                                                                                              |

The Postgres container is limited to 2 CPU cores. My pre-run text said "4 CPUs", the cpuset has 4 CPUs but the quota is 2 cores. This matters for the results below.

## Results and observations

### Per size, median of 3 repeats (range in brackets)

Source: `results/b7/summary.tsv` (k6 lap JSONs, WAL position difference and relation sizes from `captures/`).

| Size   | `over` accepted rps    | `at` offered / accepted        | `at` p50 / p99 (ms) | WAL B/event | heap B/event | TOAST B/event | index B/event | Failed (max in a run) |
| ------ | ---------------------- | ------------------------------ | ------------------- | ----------- | ------------ | ------------- | ------------- | --------------------- |
| 200 B  | 4,304 [4,286 to 4,319] | 4,000 / 3,997                  | 5.1 / 106           | 450         | 155          | 0             | 97            | 0%                    |
| 500 B  | 4,271 [4,268 to 4,337] | 4,000 / 3,995                  | 5.2 / 119           | 755         | 456          | 0             | 97            | 0%                    |
| 2 KB   | 4,178 [4,108 to 4,223] | 3,900 / 3,890                  | 5.3 / 104           | 2,312       | 2,050        | 0             | 96            | 0%                    |
| 10 KB  | 3,438 [3,401 to 3,478] | 3,700 / 3,294 [3,280 to 3,524] | 126 / 722           | 11,436      | 111          | 12,077        | 92            | 0.95% (pool timeouts) |
| 100 KB | 458 [442 to 468]       | 1,500 / 477 [427 to 486]       | 1,341 / 1,863       | 111,138     | 110          | 106,477       | 83            | 8.0% (pool timeouts)  |

In the `over` runs p50 is 620 to 690 ms for 200 B to 10 KB (queueing at an offered rate above capacity) and 1,462 ms at 100 KB.

### Key numbers for one representative `over` run per size

Prometheus queries evaluated at the end of the 45 s window, `increase(...[45s])/45` or `rate(...[45s])`: server CPU `node_cpu_seconds_total{cpu=~"0|6"}` (user + system), Postgres `ep_pg_container_cpu_seconds_total`, throttling `rate(ep_pg_container_cpu_throttled_seconds_total[45s])`, WAL `rate(ep_pg_wal_bytes_total[45s])`, pool in use `ep_pg_pool_total - ep_pg_pool_idle` (5 s steps), INSERT time `histogram_quantile(0.5, ... ep_pg_query_duration_seconds_bucket)`. Runs: b7-over-500-n1, b7-over-10240-n1, b7-over-102400-n1 (window start+32 s).

| Number                                  | 500 B       | 10 KB       | 100 KB                               |
| --------------------------------------- | ----------- | ----------- | ------------------------------------ |
| Accepted rps (lap JSON)                 | 4,337       | 3,401       | 468                                  |
| Server CPU, user + system (cores)       | 0.75        | 0.80        | 0.45                                 |
| Node CPU per request (ms) = cores / rps | 0.17        | 0.24        | about 1.0 (0.45 / 447 inserts per s) |
| Postgres CPU (cores, quota 2)           | 0.61        | 1.57        | 1.66                                 |
| Postgres CPU per row (ms)               | 0.14        | 0.46        | 3.7 (1.66 / 447.7)                   |
| Postgres CPU throttled (s per s)        | 0.00        | 0.01        | 0.99                                 |
| WAL (MB/s)                              | 3.3         | 38          | 48                                   |
| Host disk write (MB/s)                  | 8.3         | 113         | 148                                  |
| Pool in use (of 25)                     | 25.0        | 25.0        | 22.2                                 |
| INSERT p50 / p99 (s)                    | 0.01 / 0.02 | 0.01 / 0.02 | 0.05 / 0.20                          |
| k6 pinned CPUs busy                     | 0.33        | 0.60        | 0.54                                 |

For 100 KB the other two repeats gave the same picture: Postgres 1.61 and 1.64 cores, pool in use 22.2, server CPU 0.57 and 0.55 cores, k6 busy 57% and 56%.

### What I saw

1. **Capacity is flat up to 2 KB and then falls.** 4,304 (200 B), 4,271 (500 B), 4,178 (2 KB), 3,438 (10 KB), 458 (100 KB). From 200 B to 2 KB the loss is 3%, to 10 KB 20%, to 100 KB 89%.
2. **500 B repeats B6.** 4,271 median against B6 `on` 4,316 (first repeat), a 1% difference.
3. **No TOAST at 2 KB, TOAST at 10 KB.** TOAST bytes per event are 0 at 2 KB (heap 2,050 B per event) and 12,077 at 10 KB (heap only 111 B, the pointer row).
4. **WAL per event follows the body size.** 450, 755, 2,312, 11,436, 111,138 bytes. Above 2 KB it is the body size plus 12% (10 KB) or 9% (100 KB).
5. **TOAST did not compress the random hex.** TOAST bytes per event are 106,477 for a 102,400 B body and 12,077 for a 10,240 B body (more than the body, not less).
6. **Index bytes per event do not grow with payload**: 97, 97, 96, 92, 83.
7. **Postgres CPU per row grows much faster than the capacity falls**: 0.14 ms (500 B), 0.46 ms (10 KB), 3.7 ms (100 KB).
8. **At 100 KB the Postgres container was throttled 0.99 s per second** and used 1.6 to 1.66 cores of its 2-core quota, while Node used about 0.5 cores and the disk was 25% to 27% busy.
9. **At 10 KB nothing was throttled** (0.01 s per second, 1.57 cores) but the capacity was already 20% lower. A mid-run snapshot of `pg_stat_activity` in b7-over-10240-n1 showed 15 backends waiting on the `WALWrite` lock and 1 in `WALSync`. At 500 B a snapshot (b7-over-500-n1) showed 23 backends waiting on `WALWrite`. One snapshot per run is a weak signal, at 2 KB the snapshot looked quiet (27 idle in `ClientRead`) while the capacity was the same as at 500 B.
10. **The 413 probe:** with the default `BODY_LIMIT=64kb` a 60,000 B body returned 202 and a 100,000 B body returned 413 (`captures/b7-probe-413.txt`).
11. **Failures at 10 KB and 100 KB are pool timeouts**, not 413 or crashes (`err_types=pool_timeout=...`, 0.95% at most for 10 KB, 7% to 8% for 100 KB `over`).
12. **WAL rate:** 38 MB/s at 10 KB and 48 MB/s at 100 KB. Host disk write 113 and 148 MB/s, about 3 times the WAL rate (WAL plus checkpoint page writes plus the filesystem journal; the split is not measured).

## Explanation

- **Flat until 2 KB.** Up to 2 KB the cost of a request is the commit wait (about 5 ms of pool hold time) and the fixed Node cost. 25 connections / 5.8 ms = 4,310 req/s, and the measurements are 4,178 to 4,304. 1.5 KB more bytes cost Node about 0.03 ms (0.173 ms at 500 B, 0.197 ms at 2 KB, both from the table: 0.75 cores / 4,337 and 0.83 / 4,223), too little to move the pool bound.
- **Why 2 KB did not use TOAST.** The row stayed in the heap with 2,050 B per event, so Postgres did not move the jsonb out of line. My pre-run text said it was right at the threshold and might. It did not, so the 2 KB WAL per event is the heap tuple and its index entries only. (Inferred: the row is 2,050 B on average only because of per-page overhead, the exact tuple size is not measured.)
- **10 KB: where the knee is.** At 10 KB every event adds 6 TOAST chunk inserts and 6 TOAST index entries, so Postgres writes 11.4 KB of WAL instead of 755 B and uses 0.46 ms of CPU per row instead of 0.14 ms. The container used 1.57 of 2 cores without throttling, and many backends were queued on the WAL write lock, so the limit at 10 KB looks like WAL writing (the lock is taken for each WAL insert/flush, 15x more WAL bytes per commit). The pool still shows 25 in use because every connection is held longer (INSERT p50 is still 0.01 s, so the extra time is mainly waiting for the WAL, not the INSERT statement). This explanation is inferred from CPU, the lock-wait snapshots and WAL volume, no test separates WAL writing from the other costs.
- **100 KB: Postgres CPU, not Node.** Postgres used 3.7 ms of CPU per 100 KB row (1.66 cores / 447.7 inserts per s), parsing the JSON text into jsonb, trying to compress it with pglz (it fails on random hex and the work is wasted), and cutting it into 52 chunks with an index entry each. The container's quota is 2 cores and it was throttled for 0.99 s of every second, so the capacity is the quota divided by the CPU per row: 2 / 0.0037 = 540 rows/s, against 458 to 468 accepted (the rest of the quota goes to WAL writing and system time). INSERT p50 went from 0.01 s to 0.05 s, so the pool of 25 holds connections about 5 times longer (22 in use). The pool, the disk (26% busy) and Node (0.5 cores) were not the limit.
- **Why my Node CPU prediction did not matter.** I predicted 0.57 ms of Node time per 100 KB request. The measured server CPU is 0.45 to 0.57 cores for about 440 requests per second, which is about 1.0 to 1.3 ms per request, more than I predicted, but Node would only be the limit at about 1 / 0.001 = 1,000 req/s, and Postgres hit its limit before that.
- **Why the error rate grows.** With the INSERT taking 5 times longer the pool queue grows past the 2 s connection timeout, so requests fail with `pool_timeout` (503) instead of waiting, as in B3.

## Status of each claim

| Claim                                                                                  | Status                                                                                |
| -------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| Capacity is flat from 200 B to 2 KB (within 3%) and 20% lower at 10 KB                 | Observed (3 repeats)                                                                  |
| Capacity at 100 KB is about 460 req/s (89% below 500 B)                                | Observed (3 repeats, range 442 to 468)                                                |
| WAL bytes per event are the body size plus 9% to 12% above 2 KB                        | Observed                                                                              |
| TOAST does not compress the random hex payload                                         | Observed (106,477 B TOAST for a 102,400 B body)                                       |
| No TOAST at a 2 KB body                                                                | Observed                                                                              |
| The limit at 100 KB is the Postgres container's 2-core CPU quota                       | Inferred (throttled 0.99 s/s, 1.66 of 2 cores; no test with a larger quota)           |
| The limit at 10 KB is WAL write contention                                             | Likely, not verified (lock waits in one snapshot per run, no throttling)              |
| The extra Postgres CPU at 100 KB is jsonb parsing, the pglz attempt and TOAST chunking | Likely, not verified (no profile)                                                     |
| Node is not the limit at any size                                                      | Inferred (server CPU 0.45 to 0.86 cores of one thread, event loop lag 0.01 to 0.02 s) |
| A real network would make big bodies more expensive                                    | Not resolved (everything ran on one machine)                                          |
| Commits per event stay at 1                                                            | Not resolved (not measured in B7)                                                     |

## Prediction vs observation

1. **Capacity (`over`): 200 B 4,400; 500 B 4,300; 2 KB 4,200; 10 KB 4,000; 100 KB 1,700.** Partly wrong. 200 B, 500 B and 2 KB match (4,304, 4,271, 4,178, within 2%). 10 KB was 14% too high (3,438). 100 KB was wrong by 3.7 times (458). The binding resource at 100 KB was predicted to be Node CPU and was Postgres CPU.
2. **The knee: almost flat from 200 B to 10 KB (within about 10%), then a drop of more than half at 100 KB.** Partly wrong. The drop at 10 KB was 20%, and at 100 KB it was 89%. The knee is between 2 KB and 10 KB.
3. **WAL bytes per event: about 430, 730, 2,300 to 2,600, 11,000, 115,000; about 195 MB/s at 100 KB capacity.** Matches for the per-event numbers (450, 755, 2,312, 11,436, 111,138, within 4%). The 195 MB/s was wrong: measured 48 MB/s, because the capacity was lower.
4. **TOAST compression does not help.** Matches (TOAST bytes per event 106,477 for 102,400 B).
5. **Disk per event: indexes about 90 to 100 B for every size; TOAST about 11 KB at 10 KB and about 107 KB at 100 KB; about 8 GB in one 100 KB run.** Matches for indexes (83 to 97 B) and 100 KB TOAST (106,477 B). TOAST at 10 KB was 12,077 B (10% above my estimate). The 8 GB figure was wrong because it used 1,700 rps; at about 460 rps for 45 s the table is about 2.2 GB (458 x 45 x 106 KB), arithmetic from the table, not measured.
6. **Latency in `at` runs: p50 5 to 6 ms up to 10 KB, about 10 ms at 100 KB.** Matches up to 2 KB (5.1 to 5.3 ms). Wrong at 10 KB (126 ms) and at 100 KB (1,341 ms), because those runs offered more than the capacity.
7. **Pool: at 10 KB about 23 of 25 in use at 3,700 req/s, at 100 KB about 14 of 25 at 1,500 req/s.** Wrong in the part about 100 KB (22 of 25 in use with INSERT p50 five times longer than I predicted). The 10 KB number is consistent (25 in use in the `over` run, 20.5 averaged over the `at` run b7-at-10240-n1 window).
8. **Not to change: commits per event (1), index bytes per event, 202 equal to rows.** Index bytes: matches (97 to 83). Audit: matches (30 of 30 `ok`). Commits per event: not measured.
9. **Body limit probe: 60,000 B gets 202 and 100,000 B gets 413 at `BODY_LIMIT=64kb`.** Matches.

## Verdict on the hypothesis

Confirmed with corrections. Throughput does fall as the payload grows, and bytes per event drive the WAL and disk cost almost one to one above 2 KB. But the shape and the cause were different from what I wrote: the fall starts between 2 KB and 10 KB (not at 100 KB), and at 100 KB it is Postgres CPU (a 2-core quota fully used, throttled) that limits the system, not Node's JSON handling. TOAST for large jsonb is a real cost in CPU and WAL, but compression did not help. The purpose was met for the question "what does a bigger event cost": I can now say that one event costs 0.14 ms of Postgres CPU and 755 B of WAL at 500 B, and about 0.46 ms and 11 KB at 10 KB, and about 3.7 ms and 111 KB at 100 KB, and that the API stops being limited by the commit wait somewhere between 2 KB and 10 KB.

## Limitations

- Three repeats per cell. The 100 KB range is narrow (442 to 468 over, 427 to 486 at), the 10 KB `at` range is wider (3,280 to 3,524).
- Sizes are only 5 points. The knee is between 2 KB and 10 KB and is not bracketed; I did not test 4 KB or 6 KB.
- The Postgres container has a 2-core quota, so the 100 KB capacity depends on that quota. On a bigger limit the 100 KB number would be higher (not tested).
- The filler is one random hex string. Real payloads compress better with pglz, so storage and WAL per event for real data would be lower than here (not measured). Parsing cost of many small fields was not tested.
- Everything ran on one machine over localhost and docker's bridge, so network cost is understated, and all numbers are relative to this machine.
- One mid-run backend snapshot per run is a weak signal for lock waits.
- The 100 KB `at` runs and the 10 KB `at` runs are overloaded (see "What actually happened to the plan"), they do not show latency below capacity.
- The body sizes are about the target (type and user id change the length by a few bytes).

## Cost

- **What a bigger event costs:** Postgres CPU per row 0.14 ms (500 B), 0.46 ms (10 KB), 3.7 ms (100 KB); WAL per event 755 B, 11.4 KB, 111 KB; table growth per event (heap + TOAST + indexes) about 0.55 KB, 12.3 KB, 106.7 KB.
- **What the failure costs:** at 10 KB and 100 KB above capacity the p50 of the accepted requests is 640 ms and 1.5 s and up to 8% of requests get a 503 (`pool_timeout`).
- **What the fix costs:** raising `BODY_LIMIT` to 1 MB is free but only moves the limit from a 413 to the capacity of Postgres. Storing big payloads outside Postgres (object storage, a reference in the row) would keep rows near 0.5 KB; its cost (a second write and a second failure mode) was not tested here.

## Takeaways

1. Price an event by bytes: at 500 B the commit wait limits the API, above about 2 KB WAL and TOAST take over. Plan capacity per event size, not per request.
2. A 2 KB body still fits in the heap; the TOAST threshold is a real step in cost, and my events should stay under about 2 KB, or the big part should live elsewhere.
3. Check the container quota before predicting a limit: the 2-core Postgres quota decided the 100 KB result, and my Node-based prediction was wrong because I did not look at it.
4. A prediction from "CPU per request in Node" is not enough for big bodies; measure the CPU per row on the database side as well (Postgres used 26 times more CPU per row for 200 times more bytes).
5. Set `BODY_LIMIT` on purpose. The default of 64 kb rejects with 413 at 100 KB, and anything above it is only limited by the database.
6. In Stage 2, Kafka messages and ClickHouse inserts will carry the same bytes: keep event size small, and measure per-event bytes at every hop.
