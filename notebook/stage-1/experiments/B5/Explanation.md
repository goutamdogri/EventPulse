# B5 Experiment explanations

Status: COMPLETE. Results added 2026-11-21.

(I ran two one-cell plumbing trials, `b5-trial`, to check that the scripts work. I looked at their output once. They are not results and I did not use them for these predictions: the numbers below come from B3, from the cost model written in `hook.py`, and from arithmetic. One thing did change before the run: I noticed that the 4300 req/s of B3 was measured with `events_tenant_time_idx` already on the table, so the "no secondary index" base is 4300 x 1.12, not 4300. That is why the numbers below differ a little from the plan I approved in chat.)

## Purpose

I want to know what each index on the `events` table costs for every insert: how much capacity it takes away, how much WAL it adds per row, how much CPU, and how much disk it needs. I also want to know what a random primary key (UUIDv4) costs on top compared with a time-ordered one (UUIDv7). EventPulse is a write-heavy ingestion store, so this tells me how few indexes the hot table can afford. Once I know it, I can explain why the Stage 4 design moves the read-side indexing into ClickHouse instead of adding more Postgres indexes.

## Hypothesis

Each secondary index adds work to every INSERT: one more b-tree page to find and change, one more WAL record, and sometimes a full 8 KB page image in WAL. So insert throughput falls and WAL bytes per row grow roughly linearly with the number of indexes. A GIN index on a `jsonb` payload is worse than a b-tree because one row adds several keys. A random UUID primary key should cost more than a time-ordered one, because new keys land on random pages (more cache misses, more page splits, more full-page images), but only when the index is big enough not to fit in memory or when many different pages get touched after a checkpoint.

## Predictions (written before the run)

Cost model used for the numbers: each b-tree index adds 12 % to the CPU cost of a row, a GIN index counts as 3 b-trees. Capacity = base / (1 + 0.12 k).

1. **Max accepted rps in the `over` phase (6000 req/s offered), pool 25.**
   - B3 measured 4300 req/s with one secondary index, so the base with the primary key only is 4300 x 1.12 = 4820.
   - Index level 0 (k = 0): 4820. Level 1 (k = 1): 4820 / 1.12 = 4300. Level 3 (k = 3): 4820 / 1.36 = 3545. Level 5 (4 b-trees + GIN = 4 + 3 = 7): 4820 / 1.84 = 2620.
   - Probe idx1 + GIN (k = 1 + 3 = 4): 4820 / 1.48 = 3257.
   - v4 and v7 should be within noise of each other at every level on a fresh table: the 45 s table is only about 150k rows and fits in memory.
   - If Node (2 cores) turns out to be the limit at the low levels, accepted rps will be flat near the Node ceiling for levels 0 to 1, and the CPU panels will show the server cores full while Postgres is not. I do not expect it, but I cannot rule it out.
2. **`under` phase (1500 req/s).** Every cell accepts 1500 req/s (the lowest predicted capacity, 2620, is well above it). Latency p50 grows a little with the level. Postgres CPU per row grows about 12 % per b-tree and about 36 % for the GIN.
3. **WAL bytes per row** (from the WAL position difference of a run divided by its rows). Heap tuple about 600 B (jsonb payload about 500 B + tuple header), primary key entry about 70 B, commit record and full-page-image share about 30 B: 700 B at level 0. Each b-tree adds about 80 to 100 B, the GIN about 200 B: level 1 about 790 B, level 3 about 970 B, level 5 about 1260 B, idx1 + GIN about 990 B.
4. **Index size per row.** A uuid b-tree entry is 8 B header + 16 B key + 4 B line pointer = 28 B. At 90 % fill (v7, right-hand inserts) that is 28 / 0.9 = 31 B per row. For random inserts the average page fill is about ln 2 = 69 %: 28 / 0.69 = 41 B per row. So `pk_B_per_row` about 31 (v7) and 41 (v4). Secondary indexes per row: level 1 about 33 B, level 3 about 86 B (the `type` index is small, about 13 B, because it has only 10 distinct values and b-tree deduplication stores them as lists of row ids), level 5 about 175 B (GIN about 60 B, but this one I am least sure about).
5. **Checkpoints.** A run writes about 150 MB of WAL (4820 req/s x 0.7 KB x 45 s = 152 MB; 2620 x 1.26 KB x 45 s = 149 MB). A requested checkpoint starts at about (max_wal_size 1 GB / 16 MB - 1) / (2 + 0.9) = 21.7 segments = 347 MB. A run lasts about 50 s after the manual CHECKPOINT, far below the 5 min timer. So I predict 0 timed and 0 requested checkpoints inside every normal run: checkpoints will not be the story at 45 s.
6. **Full-page images on a fresh table.** About 570 to 750 index pages are created per run (150k rows x 31 to 41 B / 8192 B). Each is logged in full once, so about 0.004 to 0.005 full-page images per row, and v4 about 30 % more than v7 (41 / 31 = 1.3).
7. **Pre-loaded probe (2M rows, idx1, over).** The primary key is about 2M x 41 B = 82 MB (v4) or 62 MB (v7), the other index about 62 MB, together about the 128 MB `shared_buffers`, with 2 GB of container memory behind it as OS cache. So I expect the cache hit ratio to fall only a little and the rps of v4 to be within about 10 % of v7. The big difference should be in WAL: random inserts touch about 82 MB / 8 KB = 10k different leaf pages, and each gets a full-page image the first time after the checkpoint (180k inserts over 10k pages, so nearly all pages are hit). That is 10k x 8 KB = 80 MB extra, about 80 MB / 180k rows = 440 B more WAL per row for v4 than for v7.
8. **What I expect NOT to change:** accepted rps between v4 and v7 on the fresh table; Node CPU per request across levels (the server does not know about the indexes); error rate (0 in every run, B3 and B4 had 0 at these rates).

## Setup (planned)

| Item              | Value                                                                                                                                                                                        |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Server            | ingest-api `node dist/main.js` started by `run.sh` per run, pinned to cores 0,6, pool 25, `COUNTER_MODE=off`, 100 tenants, direct to Postgres :5434                                          |
| Postgres          | `postgres:15` container, 2 CPUs quota on cores 1,7,2,8, 2 GB, default settings (shared_buffers 128MB, max_wal_size 1GB, checkpoint_timeout 5min)                                             |
| Sysctls           | none changed                                                                                                                                                                                 |
| Route / generator | `POST /v1/events`, k6 `constant-arrival-rate`, pinned to cores 3,9,4,10, event_id v4 or v7, 10 event types                                                                                   |
| Matrix            | key {v4, v7} x index level {0, 1, 3, 5} = 8 cells; phases `under` 1500 and `over` 6000 req/s; 3 repeats; 45 s; 48 measured runs; 3 probes (idx1 + GIN; 2M-row table v4 and v7); about 81 min |
| Dashboard         | [`B5 - Index write amplification.json`](../../../../infra/observability/dashboards/stage-1/B5%20-%20Index%20write%20amplification.json)                                                      |
| Versions          | filled in after the run from env.txt                                                                                                                                                         |

## Method (planned)

1. For every repeat, cell and phase: restart the server, warm up, truncate `events`, build the index set of the cell, CHECKPOINT, snapshot the counters, run k6 through `runner.sh`, snapshot again, record index sizes, audit that rows in the table equal rows added. The repeat is the outer loop; odd repeats go through the cells forward, even repeats backward.
2. The 48 runs are measured. The three probe runs are separate and never used for a headline number. The two trial runs are plumbing checks and are never quoted.
3. Headline numbers are the median of 3 repeats with the min to max range. WAL bytes per row and fpi per row come from the counters in `summary.tsv`; Grafana is for the shape over time.
4. Criteria for discarding or flagging a run, decided now:
   - `under` run with dropped iterations > 0, or a failed row audit, or error rate > 1 %: discarded and repeated.
   - `over` run: dropped iterations are expected (that is how saturation looks); but k6 cores above about 70 % busy, or errors above 1 %, makes the run invalid as a capacity number.
   - A run during which autovacuum ran on `events` (autovac column > 0) is flagged and reported separately. It is not dropped unless its numbers differ from its sibling repeats by more than 10 %.
   - A repeat that differs from the median by more than 10 % is flagged.

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->

## What actually happened to the plan

- All 51 runs finished (`status ok` in `runs.tsv`): 48 measured runs and 3 probes. No run was aborted or discarded, no setting was changed during the run, and every row audit was `ok` (rows added to `events` equal the 202 responses). The whole thing took about 49 min (11:58 to 12:47), not the 81 min of the dry run, because the per-run overhead I assumed (45 s) was too high; this changes nothing in the data.
- **Dropped iterations.** All 24 `under` runs had 0 dropped iterations. All 27 `over` and probe runs had many (72,088 to 77,753 in 22 of them, the 19 normal `over` runs and the 3 probes; 34,887 to 65,529 in the 5 fast runs of repeat 2). This is what I wrote in the method: in an `over` run dropped iterations are expected, and a run is only invalid if the k6 cores are above about 70 % busy or errors are above 1 %. k6 cores were 31 to 39 % busy in the `over` runs and errors were 0 in all runs, so by my own rule no run is invalid. The reason for the drops is explained in the Explanation section.
- **Verdict label.** The analyzer marked one run `COLLAPSED:p99>1s`: `b5-over-v4i5-n2`, p99 1017 ms. Its accepted rps (4248) and error rate (0) are normal, so I treat it as a latency blip, not a collapse (see Limitations).
- **Autovacuum.** Autovacuum ran on `events` during 37 of the 51 runs (36 of the 48 measured runs). I planned to flag such runs and drop them only if they differed from their siblings by more than 10 %. They did not (see Limitations), so I kept all of them, but "autovacuum happened" is the normal case here, not the exception.
- **Repeat 2 of the `over` phase had five fast runs** (accepted 4543 to 5149 instead of about 4300). I did not discard them because no pre-registered criterion applies, but they are flagged and the tables use medians (see Limitations).
- The `NOTICE: index ... does not exist, skipping` lines printed at every reset come from `DROP INDEX IF EXISTS` and are harmless.
- The plan I wrote before the run said the base capacity was 4300 req/s; I corrected it to 4820 before the run (see the note at the top of this file). Nothing else changed.

## Environment (from env.txt)

| Item              | Value                                                                                                                                                                                                                        |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Commit            | `bc669bf` (the pre-run commit), 0 dirty files                                                                                                                                                                                |
| Kernel            | `7.0.0-43-generic`                                                                                                                                                                                                           |
| Node / k6         | v24.21.0 / k6 v2.4.0                                                                                                                                                                                                         |
| Postgres          | 15.17 (`postgres:15` container, 2 CPU quota, cpuset 1,7,2,8, 2 GB)                                                                                                                                                           |
| Postgres settings | `shared_buffers` 128MB, `synchronous_commit` on, `fsync` on, `wal_level` replica, `full_page_writes` on, `wal_compression` off, `max_wal_size` 1GB, `checkpoint_timeout` 5min, `autovacuum` on, `gin_pending_list_limit` 4MB |
| Pinning           | server 0,6; k6 3,9,4,10; Postgres 1,7,2,8; monitoring 5,11                                                                                                                                                                   |
| CPU               | 11th Gen Intel i5-11400H, 6 cores, 2 threads per core, 12 CPUs                                                                                                                                                               |
| Settings          | pool 25, 100 tenants, 3 repeats, 45 s, under 1500 and over 6000 req/s                                                                                                                                                        |

## Results and observations

How the numbers were read. Run-level numbers (accepted rps, p50, p99, dropped iterations, WAL, sizes) come from `summary.tsv` (the lap JSON of k6, and the counters before and after each run). Every "median [min to max]" is over the 3 repeats of a cell. CPU, queue and in-flight numbers come from Prometheus: for each run, the average over its middle part, from `start_epoch + 12 s` to `start_epoch + 42 s` (epochs in `runs.tsv`), as `avg_over_time((EXPR)[30s:5s])` with `EXPR` using a 15 s rate window. The expressions were:

- server CPU: `100*(1-avg(rate(node_cpu_seconds_total{job="node",mode="idle",cpu=~"0|6"}[15s])))`
- Postgres cores: `100*(1-avg(rate(node_cpu_seconds_total{job="node",mode="idle",cpu=~"1|2|7|8"}[15s])))`
- k6 cores: same with `cpu=~"3|4|9|10"`
- Postgres container CPU: `sum(rate(ep_pg_container_cpu_seconds_total[15s]))`, and throttling `rate(ep_pg_container_cpu_throttled_seconds_total[15s])`
- Node process: `sum(rate(process_cpu_seconds_total{job="eventpulse-nodejs"}[15s]))`
- queue: `max(ep_pg_pool_waiting{job="eventpulse-nodejs"})` and `sum(http_requests_in_flight{job="eventpulse-nodejs"})`

I did not look at the dashboard in Grafana for this write-up; no screenshot is linked.

### Run outcome per cell (`over`, 6000 req/s offered), median [min to max] of 3 repeats

| key | idx | accepted rps        | predicted rps | dropped iterations (median) | p50 ms | p99 ms |
| --- | --- | ------------------- | ------------- | --------------------------- | ------ | ------ |
| v4  | 0   | 4247 [4233 to 4306] | 4820          | 75,987                      | 682    | 777    |
| v7  | 0   | 4293 [4276 to 4596] | 4820          | 74,170                      | 685    | 775    |
| v4  | 1   | 4314 [4239 to 5084] | 4304          | 73,000                      | 680    | 777    |
| v7  | 1   | 4326 [4302 to 4543] | 4304          | 72,104                      | 679    | 756    |
| v4  | 3   | 4291 [4211 to 5149] | 3544          | 74,177                      | 687    | 781    |
| v7  | 3   | 4323 [4230 to 4732] | 3544          | 72,522                      | 674    | 743    |
| v4  | 5   | 4274 [4248 to 4312] | 2620          | 74,904                      | 685    | 769    |
| v7  | 5   | 4311 [4302 to 4338] | 2620          | 73,216                      | 681    | 750    |

`under` (1500 req/s): accepted 1500 in all 24 runs, 0 dropped, 0 % errors, p50 3.7 to 5.5 ms (5.0 ms in most), p99 9.6 to 14 ms, no pattern with the level.

### Cost per row (from `summary.tsv`, medians of 3 repeats; `over` and `under` phases agree within a few percent)

| idx | WAL B/row v4 / v7 (`over`) | predicted | primary key B/row v4 / v7 (`over`) | secondary index B/row (`over`) | full-page images per row |
| --- | -------------------------- | --------- | ---------------------------------- | ------------------------------ | ------------------------ |
| 0   | 636 / 621                  | 700       | 41.9 / 31.9                        | 0                              | 0.000                    |
| 1   | 733 / 715                  | 790       | 43.1 / 31.9                        | 53.8                           | 0.000                    |
| 3   | 920 / 903                  | 970       | 43.3 / 31.9                        | 121.4                          | 0.000                    |
| 5   | 1335 / 1323                | 1260      | 44.5 / 31.9                        | 210.7                          | 0.011 to 0.013           |

In the `under` phase the same numbers were: WAL 634/621, 727/714, 913/899, 1300/1312 B/row; primary key 38 to 40 B/row (v4) and 31.7 (v7); secondary index 48.3 (idx 1), 117.7 to 119.7 (idx 3), 232.7 to 234.1 (idx 5). Full-page images per row in `under`: 0.000 for idx 0, 1 and 3, and 0.007 to 0.013 for idx 5.

### Where the CPU went (Prometheus, method above; median of the repeats)

| phase                | server cores 0,6 busy | Postgres cores busy | Postgres container cores used | throttled | k6 cores busy | Node process cores | pool waiting (max) | in flight  |
| -------------------- | --------------------- | ------------------- | ----------------------------- | --------- | ------------- | ------------------ | ------------------ | ---------- |
| `over`, all 8 cells  | 41 to 46 %            | 40 to 44 %          | 0.56 to 0.72 of 2             | 0         | 31 to 39 %    | 0.69 to 0.77       | about 2960         | about 2990 |
| `under`, all 8 cells | 17.5 to 23 %          | 33 to 38 %          | 0.23 to 0.27                  | 0         | 12 to 18 %    | 0.27 to 0.29       | 0                  | 7 to 8     |

Postgres CPU per row in the `under` phase (container cores / 1500 req/s, in ms, median of 3 repeats, v4 and v7 together): idx 0 0.164, idx 1 0.168, idx 3 0.169, idx 5 0.177. That is +2.8 %, +3.3 % and +7.8 % over idx 0. The difference between v4 and v7 inside one level is about as large as the step between levels (idx 0: 0.156 v4, 0.173 v7).

### Per CPU and event loop (added later, same windows, same method)

- Per logical CPU (`100*(1-rate(node_cpu_seconds_total{job="node",mode="idle",cpu=~"0|6"}[15s]))`, median of runs): `over` CPU 0: 43 %, CPU 6: 43 % (ranges 38 to 51 % and 40 to 54 %); `under` 18 % and 19 %; probes 39 % and 42 %. By mode, CPU 0 + CPU 6 added together in `over`: user 67 %, system 10 %, softirq 4 % of one CPU. The two fastest runs of repeat 2 (`v4i3-n2`, `v4i1-n2`) were 44 to 54 % on these CPUs.
- Event-loop lag of the server (`1000*nodejs_eventloop_lag_mean_seconds`, `..._p99_seconds`, `..._max_seconds`, job `eventpulse-nodejs`): `over` mean 10.5 ms, p99 18.7 ms, max 20.8 ms; `under` mean 10.1 ms, p99 10.9 ms, max 11.4 ms.
- Run-queue wait of CPUs 0 and 6 (`sum(rate(node_schedstat_waiting_seconds_total{job="node",cpu=~"0|6"}[15s]))`): `over` 0.24 s/s, `under` 0.04 s/s; time actually running: `over` 0.82 s/s, `under` 0.34 s/s.
- CPUs 0 and 6 are the two hardware threads of one physical core (`thread_siblings_list` 0,6); Postgres has 1,7 and 2,8 (two cores), k6 has 3,9 and 4,10.

### Capture in the middle of an `over` run

In the one mid-run capture I read (`captures/b5-over-v4i5-n1.txt`, the other captures were not read): 27 sessions `idle / Client / ClientRead` and 1 `active`. `pg_stat_wal` there: `wal_buffers_full` 0.

### Checkpoints, cache, probes

- Checkpoints: `0t/0r` (no timed and no requested checkpoint) in all 51 runs. Cache hit ratio 99.1 to 99.6 % in every run.
- `probe_gin` (v4, idx 1 + GIN): accepted 4329, WAL 1083 B/row, full-page images 0.011/row, secondary index 119 B/row.
- `probe_pre_v4` / `probe_pre_v7` (2,000,000 rows before the run, idx 1): accepted 4248 / 4333; WAL 1026 / 721 B/row; full-page images 0.052 / 0.001 per row; primary key 38.9 / 31.6 B/row; cache hit 99.32 / 99.08 %. One run each.

### What I saw

1. The accepted rps of the `over` phase is about 4300 at every index level: medians 4247 to 4326 across the 8 cells. Between idx 0 and idx 5 there is no drop. B3 had measured the same 4300.
2. Dropped iterations are the arithmetic of saturation: (6000 - 4300) x 45 s = 76,500 expected, 72,088 to 77,753 observed in the 19 normal `over` runs (the 5 fast runs of repeat 2 had fewer). k6 had all its 3000 VUs busy (about 2990 requests in flight) and the pool queue held about 2960 requests.
3. During `over` runs Postgres was not busy: 0.56 to 0.72 of its 2 cores, never throttled, and in the capture 27 of 28 sessions waited for the client.
4. The server cores were 41 to 46 % busy (two logical CPUs, so about 0.9 of one CPU) and the Node process used 0.69 to 0.77 cores. Per CPU, CPU 0 and CPU 6 were each about 43 % busy (38 to 54 % over runs), with no CPU near 100 %. The generator was fine (31 to 39 %).
5. WAL per row goes up with every index: idx 0 to idx 5 is 636 to 1335 B for v4 (x2.10) and 621 to 1323 B for v7 (x2.13). A b-tree adds about 90 to 95 B per row (idx 0 to idx 1: +97 v4, +94 v7); the two b-trees from idx 1 to idx 3 add about 187 B (v4) and 188 B (v7).
6. The GIN index adds about 360 B per row (probe idx 1 + GIN 1083, against idx 1 at 715 to 733 B). Full-page images appear only in cells with the GIN (0.007 to 0.013 per row) and are 0.000 in the cells with only b-trees.
7. On the fresh table, v4 vs v7 differ by 15 to 16 B/row of WAL (636 vs 621, 733 vs 715, ...) and by nothing in rps. On the pre-loaded table the difference is large in WAL (1026 vs 721 B/row, +305) and in full-page images (0.052 vs 0.001 per row), but small in rps (4248 vs 4333, -2 %) and cache hit ratio (99.32 vs 99.08).
8. The random primary key is bigger: 38 to 44.5 B/row against 31.6 to 31.9 B/row for v7.
9. The extra Postgres CPU per row of five indexes (one of them a GIN) is only +7.8 % (0.164 to 0.177 ms).
10. No checkpoints during any run, no errors, every audit `ok`.
11. In repeat 2, five `over` runs were faster (4543 to 5149 accepted, 35k to 65k dropped, in flight down to 911 to 1063 in the two fastest): `v7i3-n2`, `v4i3-n2`, `v7i1-n2`, `v4i1-n2`, `v7i0-n2`, all between 12:19 and 12:27. The runs just before (12:15 and 12:17) and just after (12:28) were normal (4248 to 4338). In the two fastest the Node process used 0.80 and 0.85 cores, against about 0.7 in normal runs.

## Explanation

**Why the capacity did not fall with the indexes.** The indexes did not make Postgres the bottleneck. In the `over` runs the Postgres container used 0.56 to 0.72 of its 2 cores with no throttling, and the capture shows 27 of 28 sessions idle in `ClientRead`. My first version of this write-up then said that the Node main thread was most likely the limit. I changed that on 2026-11-21 after looking at the per-CPU and event-loop data (below), which do not support it: CPU 0 and CPU 6 (the server's cores) were about 43 % busy each, the Node process used 0.69 to 0.77 cores in total, and the event-loop lag in `over` was only a few ms above its floor (mean 10.5 ms, p99 18.7 ms, against 10.1 and 10.9 ms in `under`). A single JavaScript thread that limits the throughput would be near 100 % of a CPU and would make the loop lag grow, and neither happened. So I cannot see any CPU that is full: server, Postgres and k6 were all partly idle.

What the numbers do show is a queue in the application: about 2960 requests waiting for one of the 25 pool connections. With a fixed pool, throughput is at most pool size / time a connection is held: 25 / 4300 req/s = 5.8 ms per request. That is close to the 5.0 ms median latency of an `under` request, and B3 had the same pool and the same 4300 req/s. So my current best guess is "25 connections x about 5 to 6 ms per request (round trip + commit)", not CPU. This is an inference. I did not measure the time a connection is held, and the one capture (27 of 28 sessions idle in `ClientRead`) does not obviously fit it. Two more facts I noticed: CPUs 0 and 6 are the two hardware threads of one physical core, so the server has one core to share, and the threads on those CPUs waited in the run queue 0.24 s per second in `over` (0.04 in `under`), a sign of some contention. In five fast runs of repeat 2 the server did more work (0.80 to 0.85 cores); I do not know why.

**Why the dropped iterations appear.** In an open model k6 starts 6000 requests every second. The server finishes about 4300, so about 1700 per second are not served. k6 has 3000 virtual users; they are all busy waiting (2990 in flight) and every new arrival that finds no free VU is counted as dropped: 1700 x 45 = 76,500, observed 72k to 78k. The drops are the saturation itself, not a generator fault (k6 cores 31 to 39 %).

**Why WAL grows linearly with b-trees.** Every b-tree index adds one entry and one WAL record per row: about 90 to 95 B. Two indexes added about 188 B. That is the same step as predicted (90 B each), so the per-b-tree model of the prediction was right. The base was lower than predicted by 8 to 10 % (about 625 instead of 700 B); the heap row plus primary key is smaller than I guessed.

**Why the GIN is the expensive one.** One event adds several entries to the GIN (the keys and the values of the payload), and each goes into the pending list with its own record; the WAL cost of 360 B per row is about four b-trees, more than the "about 200 B" I predicted. Full-page images only show up in the cells with the GIN (0.007 to 0.013 per row, 0.011 in the GIN probe). My guess (inferred) is that the GIN touches pages that already existed at the checkpoint (the pending list and the small set of hot entries), while b-tree inserts on a fresh table mostly write pages that were created after the checkpoint, which do not need a full image.

**Why the secondary indexes are bigger than predicted.** I predicted 33 B/row for `(tenant_id, occurred_at)` and measured 48 to 54 B. My arithmetic assumed pages that stay about 90 % full. Inference: with 100 tenants the insert point is the end of each tenant's range, which is in the middle of the index, not at its right end. Postgres only gives the 90 % fill split to the right-most page; every other page splits 50/50, so pages sit at roughly 55 to 60 % full: 28 B / 0.58 = 48 B. I have not checked this against the page statistics.

**Why the primary key sizes match.** v7 entries are 31.6 to 31.9 B/row, exactly the 28 / 0.9 = 31 B I computed for a tight tree. v4 gave 38 to 44.5 B/row against 41 B predicted (the exact value depends on how many rows are in the table: 38 B in the `under` runs with 67k rows, 42 to 44 in the `over` runs with 190k rows, 39 B on the 2M-row table).

**Why almost no difference between v4 and v7 on the fresh table.** WAL +15 B/row and the same rps. The table is small and new pages are created, not modified, so the random inserts do not touch many old pages. On the 2M-row table the primary key has about 5,000 to 10,000 leaf pages that random inserts hit, and each one is logged in full the first time after the checkpoint: 0.052 full-page images per row against 0.001. That is the full-page image effect I predicted; the WAL difference (+305 B/row) is smaller than the 440 B I computed. My guess (inferred) is that a full-page image of a partly filled page is shorter than 8 KB, because Postgres leaves out the empty gap of the page. 0.052 x 8192 B = 426 B per row would be the full size.

**Why the CPU cost per row is small.** Five indexes (four b-trees and a GIN) cost +7.8 % of Postgres CPU per row, against the +84 % I predicted. Most of the CPU of one INSERT seems to be fixed work (protocol, parse, heap, commit), and an index insert into cached pages is cheap. This is an inference; I measured only the sum. The consequence is that even if Postgres had been the limit, five indexes would have cost less than 8 % of the capacity.

**Why the latency stayed flat.** The p50 of the `under` runs is 5.0 ms at every level; per row, the extra work is a few microseconds of CPU (0.164 to 0.177 ms) against a request that lasts 5 ms, so it is not visible in a latency that is dominated by something else (the round trip and the commit; I did not measure which part).

## Status of each claim

| Claim                                                                                                                        | Status                                                                                                       |
| ---------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| Accepted rps of the `over` phase is about 4300 at every level, v4 and v7                                                     | Observed (`summary.tsv`, 24 runs)                                                                            |
| Dropped iterations in `over` equal the unserved rate: (6000 - 4300) x 45 = 76,500                                            | Observed (72k to 78k in the 19 normal `over` runs)                                                           |
| Postgres was not CPU-bound in `over` (0.56 to 0.72 of 2 cores, no throttling)                                                | Observed (Prometheus, method above)                                                                          |
| Sessions were mostly waiting for the client                                                                                  | Observed, from one capture only (`b5-over-v4i5-n1`)                                                          |
| No CPU was saturated in `over`: CPU 0 and CPU 6 about 43 % each, Postgres 0.56 to 0.72 of 2 cores, k6 31 to 39 %             | Observed                                                                                                     |
| The Node event loop was not saturated in `over` (lag mean 10.5 ms, p99 18.7 ms; the 10 ms floor is the histogram resolution) | Observed; the floor is Inferred                                                                              |
| The ceiling of about 4300 req/s is pool size 25 / about 5.8 ms per request                                                   | Likely, not verified (fits the arithmetic and B3; the idle sessions in the one capture do not obviously fit) |
| The Node main thread is the ceiling                                                                                          | Not supported by the event-loop and per-CPU data, not excluded                                               |
| The effect of 5 indexes on capacity is below about 8 % when Postgres is the limit                                            | Inferred (from the CPU per row in `under`)                                                                   |
| WAL per row: about +90 B per b-tree, about +360 B for the GIN                                                                | Observed                                                                                                     |
| Full-page images only appear with the GIN on the fresh table                                                                 | Observed; the reason is Likely, not verified                                                                 |
| Secondary index bytes per row are larger than predicted because of 50/50 splits                                              | Inferred, not checked with page statistics                                                                   |
| Random key costs +305 B/row of WAL and 0.051 more full-page images per row on a 2M-row table                                 | Observed, but one run per key                                                                                |
| A random key does not change rps in a 2M-row table (4248 vs 4333)                                                            | Observed, single run each, inside the repeat noise                                                           |
| The five fast runs in repeat 2 were caused by a faster server CPU for a while                                                | Not resolved                                                                                                 |
| The p99 of 1017 ms in `v4i5-n2` was a GIN pending-list merge                                                                 | Not resolved (no other idx 5 run shows it)                                                                   |
| Autovacuum did not change the results                                                                                        | Observed for WAL per row (within a cell, repeats are within 3 %), Inferred for the rest                      |

## Prediction vs observation

1. _"Max accepted rps in the `over` phase (6000 req/s offered), pool 25. B3 measured 4300 req/s with one secondary index, so the base with the primary key only is 4300 x 1.12 = 4820. Index level 0 (k = 0): 4820. Level 1 (k = 1): 4820 / 1.12 = 4300. Level 3 (k = 3): 4820 / 1.36 = 3545. Level 5 (4 b-trees + GIN = 4 + 3 = 7): 4820 / 1.84 = 2620. Probe idx1 + GIN (k = 1 + 3 = 4): 4820 / 1.48 = 3257. v4 and v7 should be within noise of each other at every level on a fresh table: the 45 s table is only about 150k rows and fits in memory. If Node (2 cores) turns out to be the limit at the low levels, accepted rps will be flat near the Node ceiling for levels 0 to 1, and the CPU panels will show the server cores full while Postgres is not. I do not expect it, but I cannot rule it out."_ **Wrong for the main prediction.** Observed medians 4247 to 4326 at every level (idx 0 predicted 4820, idx 5 predicted 2620; the probe 4329 against 3257). The alternative I wrote down (Node as the limit, with the server cores full while Postgres is not) does not fit either: the server CPUs were about 43 % busy each and the event loop was not saturated, so the limit is something else (see the Explanation section). v4 vs v7: matches (the cells differ by at most 80 req/s in their medians).
2. _"`under` phase (1500 req/s). Every cell accepts 1500 req/s (the lowest predicted capacity, 2620, is well above it). Latency p50 grows a little with the level. Postgres CPU per row grows about 12 % per b-tree and about 36 % for the GIN."_ **Partly.** All 24 runs accepted 1500 req/s (matches). The p50 did not grow (3.7 to 5.5 ms, no pattern; wrong). CPU per row grew only +7.8 % at idx 5 against about +84 % (4 x 12 % + 36 %) predicted (wrong).
3. _"WAL bytes per row (from the WAL position difference of a run divided by its rows). Heap tuple about 600 B (jsonb payload about 500 B + tuple header), primary key entry about 70 B, commit record and full-page-image share about 30 B: 700 B at level 0. Each b-tree adds about 80 to 100 B, the GIN about 200 B: level 1 about 790 B, level 3 about 970 B, level 5 about 1260 B, idx1 + GIN about 990 B."_ **Partly.** Level 0 to 3 were 6 to 10 % lower than predicted (621 to 636, 715 to 733, 903 to 920 B); the per-b-tree step of 90 to 95 B matches. Level 5 was 5 to 6 % higher (1323 to 1335 B) and the idx 1 + GIN probe was 9 % higher (1083 B), because the GIN costs about 360 B, not about 200 B.
4. _"Index size per row. A uuid b-tree entry is 8 B header + 16 B key + 4 B line pointer = 28 B. At 90 % fill (v7, right-hand inserts) that is 28 / 0.9 = 31 B per row. For random inserts the average page fill is about ln 2 = 69 %: 28 / 0.69 = 41 B per row. So `pk_B_per_row` about 31 (v7) and 41 (v4). Secondary indexes per row: level 1 about 33 B, level 3 about 86 B (the `type` index is small, about 13 B, because it has only 10 distinct values and b-tree deduplication stores them as lists of row ids), level 5 about 175 B (GIN about 60 B, but this one I am least sure about)."_ **Partly.** Primary key: matches (31.6 to 31.9 for v7; 38 to 44.5 for v4). Secondary: wrong, all too low: 48 to 54 B (idx 1), 118 to 122 B (idx 3), 211 to 234 B (idx 5).
5. _"Checkpoints. A run writes about 150 MB of WAL (4820 req/s x 0.7 KB x 45 s = 152 MB; 2620 x 1.26 KB x 45 s = 149 MB). A requested checkpoint starts at about (max_wal_size 1 GB / 16 MB - 1) / (2 + 0.9) = 21.7 segments = 347 MB. A run lasts about 50 s after the manual CHECKPOINT, far below the 5 min timer. So I predict 0 timed and 0 requested checkpoints inside every normal run: checkpoints will not be the story at 45 s."_ **Matches.** `0t/0r` in all 51 runs. (The WAL per run was about 4300 x 0.64 to 1.33 KB x 45 s = 124 to 257 MB, still below 347 MB.)
6. _"Full-page images on a fresh table. About 570 to 750 index pages are created per run (150k rows x 31 to 41 B / 8192 B). Each is logged in full once, so about 0.004 to 0.005 full-page images per row, and v4 about 30 % more than v7 (41 / 31 = 1.3)."_ **Wrong.** 0.000 per row in all b-tree-only cells, and 0.007 to 0.013 in the cells with the GIN, with v4 and v7 equal. New pages were evidently not logged in full.
7. _"Pre-loaded probe (2M rows, idx1, over). The primary key is about 2M x 41 B = 82 MB (v4) or 62 MB (v7), the other index about 62 MB, together about the 128 MB `shared_buffers`, with 2 GB of container memory behind it as OS cache. So I expect the cache hit ratio to fall only a little and the rps of v4 to be within about 10 % of v7. The big difference should be in WAL: random inserts touch about 82 MB / 8 KB = 10k different leaf pages, and each gets a full-page image the first time after the checkpoint (180k inserts over 10k pages, so nearly all pages are hit). That is 10k x 8 KB = 80 MB extra, about 80 MB / 180k rows = 440 B more WAL per row for v4 than for v7."_ **Partly.** Cache hit fell little (99.32 v4 vs 99.08 v7, v7 even lower) and rps was within 2 % (4248 vs 4333): matches. The WAL difference was +305 B/row (1026 vs 721), against +440: right direction and 70 % of the size (one run each). The full-page images were 0.052 per row for v4, which is 426 B/row at full size.
8. _"What I expect NOT to change: accepted rps between v4 and v7 on the fresh table; Node CPU per request across levels (the server does not know about the indexes); error rate (0 in every run, B3 and B4 had 0 at these rates)."_ **Matches.** Node process cores per request were flat in `under` (0.27 to 0.29 at all levels); errors 0 in all 51 runs; rps v4 vs v7 within noise.

## Verdict on the hypothesis

Confirmed with corrections for the cost per row, not confirmed for throughput. Each added index does make every insert more expensive: WAL per row doubles from 636 to 1335 B (v4) with five indexes, a b-tree adds about 90 B and a GIN about 360 B, the indexes grow to about 250 to 280 B per row on top of the heap, and Postgres CPU per row is +7.8 %. But the hypothesis said each index lowers insert throughput, and I could not measure that: the capacity of this setup, about 4300 req/s, is set by something in front of Postgres that is not CPU (it fits a pool of 25 connections x about 5.8 ms per request, but I have not shown it), so the cost of the indexes is hidden under it. The random primary key costs +15 B/row of WAL on a fresh table and about +300 B/row on a 2M-row table (one run). The purpose was met in part: I can now say what an index costs in WAL, disk and CPU, and that the cost of the five indexes in CPU is small at this scale; I cannot yet say how much of the ingest capacity the indexes cost, because the experiment did not reach the point where Postgres is the limit.

## Limitations

- Three repeats per cell, one run per probe: the v4 vs v7 result on the 2M-row table and the GIN cost rest on single runs.
- Repeat 2 had five `over` runs that were 5 to 20 % faster (accepted up to 5149); the cause is not known. I used medians; the minimum to maximum ranges in the table are wide for that reason. The `over` accepted rps cannot resolve differences smaller than about 5 %.
- Autovacuum ran in 37 of 51 runs. Within one cell the WAL per row of the repeats stays within about 3 % whether or not it ran, so I did not exclude those runs, but I did not test the effect of autovacuum separately.
- The table is fresh and small (about 70k to 200k rows) except in the two probes; the real ingest store will be much bigger. The 2M-row table is still smaller than the working set of a real store.
- The payload is synthetic (`user` random up to 1e6, `data` a constant string of 300 characters); the GIN cost depends on the real payload.
- The `type` index uses 10 uniformly random types; the real distribution would change its size.
- CPU numbers are 30 s averages from Prometheus. node_exporter gives CPU per logical CPU, not per thread of the process; I used it together with the event-loop lag, and neither shows what the limit is. I did not measure how long a pool connection is held.
- Only one mid-run capture was read. One machine (6 cores, shared by the server, Postgres, k6 and monitoring), loopback only, so the numbers are relative.

## Cost

- **WAL.** At 1500 req/s, idx 0 writes about 1500 x 634 B = 0.95 MB/s, which is 82 GB per day (x 86,400 s). With five indexes (1300 B/row) it is 1.95 MB/s, 168 GB per day: twice as much to flush, ship and archive for the same events.
- **Disk.** Indexes grow from the primary key alone (32 to 44 B/row) to about 245 to 280 B/row with five indexes (primary key plus 211 to 234 B of secondary indexes). The GIN alone is about 70 to 75 B/row (idx 1 + GIN probe 119 B against 48 to 54 B for idx 1).
- **CPU.** Postgres CPU per row goes from 0.164 to 0.177 ms (+7.8 %) for five indexes.
- **Random key.** About 10 B/row more primary key (38 to 44 vs 32 B/row) on a fresh table, and about +305 B/row of WAL once the table is large (single run).
- **Failure.** There was no failure in this experiment: no errors, no collapse. The only cost of saturation is dropped iterations and a queue of about 2960 requests waiting for a connection (p50 680 ms).

## Takeaways

1. In this setup, adding an index is paid in WAL and disk long before it is paid in throughput. When I decide how many indexes the ingest table gets, I should look at WAL bytes per row and index bytes per row first.
2. A b-tree index costs about 90 B/row of WAL (about 14 % of the 636 B base) and about 30 to 55 B/row of disk; a GIN on `jsonb` costs about 360 B/row of WAL (about 57 % of the base), roughly four b-trees. Putting the GIN on `payload` is the first index to refuse on a hot table.
3. Never take a capacity number for an experiment without checking which part is saturated. Here nothing I measured was CPU-saturated (Postgres at about 30 % of its quota, the server CPUs at about 43 % each, low event-loop lag) while the rps was flat, so the `over` phase measured a limit that is not the indexes.
4. Dropped iterations in an open-model run at saturation are the unserved rate, not a faulty generator. I check this with the arithmetic (offered - accepted) x duration, the k6 CPU and the in-flight count.
5. A random primary key looks free on a small fresh table and costs about +300 B/row of WAL on a 2M-row table (one run). Any later decision about UUIDv4 should be tested on a table of realistic size.
6. For the follow-up: find the real limit before testing capacity against indexes again. The cheapest test is to repeat one `over` cell (for example v7, idx 5) with a pool of 50 and of 100: if the accepted rps grows with the pool, the limit is pool size x time per request. Only a setup where Postgres is the limit can show the cost of the indexes in capacity, and with +7.8 % CPU for five indexes that effect would be below 8 % anyway.
