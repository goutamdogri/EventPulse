# B11 Experiment explanations

Status: PRE-RUN. Written 2026-11-22 before any measured run. The sections after the marker are appended after the experiment.

## Purpose

I want to know if the ingest path gets slower only because time passes and the table grows, at a load that is comfortable (70% of the knee). Every earlier Stage 1 experiment ran for one or two minutes on an almost empty table, so none of them could show a slow decay. When I know the answer I can say how long later experiments have to run before their numbers are trustworthy, and whether the capacity I measured in B1 to B7 (about 4300 req/s) is a number for a new table only.

## Hypothesis

Over a long run, throughput and p99 get worse. The mechanism I think is behind it: the table and its indexes grow past the memory Postgres uses as cache, and the random uuid primary key makes every insert touch a cold index page; after every checkpoint each first touch of a page writes a whole 8 KB page into the WAL (full-page image), so WAL and disk writes per event grow, checkpoints come more often and the commit flush has to share the disk with them. The pool hold time rises and with it the p99 and, if it goes far enough, the capacity (pool 25 divided by hold time).

## Predictions (written before the run)

Constants from earlier experiments: pool 25, hold time about 5.8 ms (B3, B6, B7), capacity 25 / 0.0058 s = 4,310 req/s; WAL about 730 bytes per row at 500 B events (B6); indexes about 90 to 100 B per event and heap about 250 to 550 B per event (B7); the 2M-row random primary key cost about +300 B/row of WAL in B5 (one run). Soak rate is 70% of the knee: 0.7 x 4,300 = 3,000 req/s.

1. **Capacity on the empty table (`cap_start`, 6000 offered).** About 4,100 to 4,500 accepted req/s, the same as B6 `on` (4,316 / 4,363 / 4,359). The rest is 503 or dropped iterations.
2. **Growth.** 3,000 req/s x 3,600 s = 10.8 million rows in the hour. At 350 to 650 B per row (heap plus indexes) that is 3.8 to 7.0 GB, my point estimate is about 6 GB (heap about 5 GB, indexes about 1 GB). The uuid index has about 10.8M x 45 B / 8 KB = 59,000 pages = about 480 MB. It passes `shared_buffers` (128 MB) after about 128 MB / 45 B = 2.8 million rows, about 16 minutes. The table passes the 2 GB container limit after about 2 GB / 550 B = 3.6 million rows, about 20 minutes.
3. **WAL per row and checkpoints.** At the start 730 B x 3,000 = 2.2 MB/s. A checkpoint is forced when about 350 MB of WAL is written (1 GB / 2.9), so the first checkpoints come about every 350 / 2.2 = 160 s, earlier than the 5 minute timer: **requested checkpoints dominate from the start**, about 22 per hour at first. Then full-page images grow with the index. Model: the uuid index has P pages and a checkpoint interval has 3,000 x T inserts spread uniformly, so the pages touched are P x (1 - exp(-3000 T / P)) and each costs 8 KB of WAL. Solving "WAL in the interval = 350 MB": at 2M rows (P about 11,000) about +180 B/row (B5 measured +300); at 5M rows (P about 27,000) the interval shrinks to about 60 s and WAL is about 1.9 KB per row; at 10M rows (P about 55,000) the interval is about 20 s and WAL about 5 to 6 KB per row. So I expect **WAL per row to grow from about 730 B to several KB (3x to 8x) and checkpoints per hour to go from about 22 to over 100**. This is the part I am least sure about (uniform spread, no WAL compression, no effect of the checkpoint spreading).
4. **Buffer cache hit ratio.** About 99.9% at the start. At the end the uuid leaf page is missed with probability about 1 - 128 / 480 = 73% and an insert makes about 9 page accesses, so about 8% misses: **hit ratio falls to about 90 to 96%**. These are shared-buffer misses; most should still be OS page cache hits, so disk read bytes stay small until the data is clearly bigger than the container memory.
5. **Throughput of the soak.** Accepted rate stays within 2% of 3,000 for the whole hour (offered 3,000 is 70% of the capacity). I give this about 65% confidence. If the hold time grows by more than 30% the capacity falls below 3,000 and I would see 503 and dropped iterations near the end.
6. **Latency of the soak.** p50 from about 5 to 6 ms to 6 to 9 ms. p99 starts at about 8 to 12 ms and ends **2 to 5 times higher, 20 to 60 ms**, with a visible bump with every checkpoint.
7. **Capacity after the soak (`cap_end`).** Accepted rate **10 to 30% below `cap_start`**: about 3,000 to 3,900 req/s. Because the soak rate itself is 3,000, the low end of this range is right where the soak would start to fail.
8. **Fresh-table control.** At 3,000 req/s on a truncated table p99 is within 20% of the first minutes of the soak, which says the decay in 6 is caused by the table size and not by the machine.
9. **What I expect NOT to change:** the number of commits per event (1), Node CPU about 0.5 core and event loop lag (the server does not care about table size), the pool size, and the row audit (every 202 is one row, `rows == n_202`).
10. **Disk.** The soak needs about 6 GB for table and indexes plus WAL (kept at about 1 to 2 GB by recycling). Free space falls from about 51 GB to about 44 GB, so the 10 GB guard should not fire.

## Setup (planned)

| Item              | Value |
| ----------------- | ----- |
| Server            | ingest-api from `services/ingest-api/dist`, `node --env-file=.env dist/main.js` under `taskset -c 0,6`, pool 25, `COUNTER_MODE=off`, straight to Postgres on :5434, started by `run.sh` before every run |
| Postgres          | postgres:15 container `ep-postgres`, 2 CPUs (cpuset 1,7,2,8), 2 GB memory, `shared_buffers` 128 MB, `checkpoint_timeout` 5 min, `max_wal_size` 1 GB, `synchronous_commit` on; settings not changed |
| Sysctls           | none changed, no sudo |
| Route / generator | `POST /v1/events`, 500 B events, 100 tenants, k6 `constant-arrival-rate`, pinned to 3,9,4,10 |
| Matrix            | `cap_start` (6000 req/s, 60 s), `soak` (3000 req/s, 60 min), `cap_end` (6000 req/s, 60 s, big table), `fresh` (3000 req/s, 180 s); 1 repeat each; about 69 min in total |
| Sampling          | one sample per minute into `captures/<testid>.samples.tsv`, plus Prometheus at its scrape interval and k6 remote write every second |
| Dashboard         | [B11 - Table growth soak.json](../../../../infra/observability/dashboards/stage-1/B11%20-%20Table%20growth%20soak.json) |
| Versions          | filled in after the run from env.txt |

## Method (planned)

1. `run.sh` runs the four runs in order. Only `soak` is a measured run. `cap_start` and `cap_end` are probes (offered above the capacity on purpose; I use only their accepted rate), and `fresh` is a control. Each run starts with a new server, a warm-up, a truncated table (except `cap_end`) and a checkpoint.
2. I read the soak as first 5 minutes against last 5 minutes (rps, INSERT time, hit ratio, WAL bytes per row, full-page images per row), as p99 over time in Grafana, and as the checkpoint counts. The capacity is the accepted rate of `cap_end` against `cap_start`. The control is read against the first minutes of the soak.
3. A single repeat is a real limit: I cannot give a range, only one trajectory. The probes and the control are there to test whether the trajectory is the table or the machine.
4. Criteria for discarding, decided now: the soak is discarded if k6 drops iterations in the first 10 minutes (generator problem, not capacity), if the k6 cores are busier than about 70%, if the run ends `ABORTED-DISK` or `FAILED`, or if Prometheus loses data for more than a few minutes. Dropped iterations after the first 10 minutes are not discarded: they are the capacity falling under 3,000 and count as a finding. The row audit must be `ok` (rows equal to 202 answers) or the run is not trusted.
5. A trial run (`RESULTS_NAME=b11-trial`) is only to check the plumbing and is never quoted.

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->
