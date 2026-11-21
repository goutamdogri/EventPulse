# B5 Experiment explanations

Status: PRE-RUN. Written 2026-11-21 before any measured run. The sections after the marker are appended after the experiment.

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

| Item              | Value                                                                                                                                 |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| Server            | ingest-api `node dist/main.js` started by `run.sh` per run, pinned to cores 0,6, pool 25, `COUNTER_MODE=off`, 100 tenants, direct to Postgres :5434 |
| Postgres          | `postgres:15` container, 2 CPUs quota on cores 1,7,2,8, 2 GB, default settings (shared_buffers 128MB, max_wal_size 1GB, checkpoint_timeout 5min) |
| Sysctls           | none changed                                                                                                                          |
| Route / generator | `POST /v1/events`, k6 `constant-arrival-rate`, pinned to cores 3,9,4,10, event_id v4 or v7, 10 event types                            |
| Matrix            | key {v4, v7} x index level {0, 1, 3, 5} = 8 cells; phases `under` 1500 and `over` 6000 req/s; 3 repeats; 45 s; 48 measured runs; 3 probes (idx1 + GIN; 2M-row table v4 and v7); about 81 min |
| Dashboard         | [`B5 - Index write amplification.json`](../../../../infra/observability/dashboards/stage-1/B5%20-%20Index%20write%20amplification.json) |
| Versions          | filled in after the run from env.txt                                                                                                  |

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
