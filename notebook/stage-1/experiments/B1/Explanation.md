# B1 Experiment explanations

## Hypothesis

Latency stays flat, then bends sharply at a knee set by either pool size, database commit capacity, or the CPU of the Node process. I predict which one first.

## Predictions

These were my first numbers from simple arithmetic, with assumptions.

- **A, Node CPU:** about 0.20-0.30 ms of CPU per request. Node runs JS on one thread, so the ceiling is 1 / 0.00025 s = 4,000 req/s (range 3,300 to 5,000).
- **B, the pool:** one INSERT holds a connection for about 1-2 ms. Ceiling = pool size / query time = 10 / 0.002 = 5,000 req/s (10,000 if the commit is fast). Pool utilization = lambda x s / c, so at 4,000 req/s and s = 2 ms it is 80%.
- **C, Postgres CPU:** about 0.1 ms of CPU per insert, so 2 cores give 2 / 0.0001 = 20,000 inserts/s. Postgres CPU should not be the first limit.
- **Prediction:** Node CPU hits first at roughly 3,000-5,000 req/s, the pool is a close second. Expected shape: p50 flat at a few ms, p99 bending at the knee, then 503 (pool wait timeout 2 s) or client timeouts.

## Setup

| Item               | Value                                                                                                                                                                                                                                                            |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Date               | 2026-11-14, fine pass started 17:28:44 +05:30 (Grafana shows UTC, 11:58 to 12:33)                                                                                                                                                                                |
| Code               | git `56865d1`, 10 dirty files.                                                                                                                                                                                                                                   |
| Machine            | Intel i5-11400H, 6 cores / 12 logical CPUs, kernel 7.0.0-43-generic                                                                                                                                                                                              |
| Server             | NestJS on Express + `pg.Pool`, Node v24.21.0, pool max 10 (read from `/debug/stats`), pinned to CPUs 0,6, restarted before every run                                                                                                                             |
| Database           | PostgreSQL 15.17 (not 16 as in the stage doc) in Docker, `cpus: 2`, 2 GiB, cpuset 1,7,2,8. `synchronous_commit=on`, `fsync=on`, `shared_buffers=128MB`, `max_connections=100`, `wal_level=replica`, `checkpoint_timeout=5min`, `max_wal_size=1GB`. Nothing tuned |
| Generator          | k6 v2.4.0, open model (`constant-arrival-rate`), pinned to CPUs 3,9,4,10, max 3000 VUs, 10 s request timeout                                                                                                                                                     |
| Monitoring         | Prometheus, Grafana, node_exporter, grafana-renderer pinned to 5,11                                                                                                                                                                                              |
| Route / payload    | `POST /v1/events`, about 500 B per event, unique `event_id` each                                                                                                                                                                                                 |
| Matrix (fine pass) | 9 rates (750, 950, ... 2350, even steps of 200) x 3 repeats x 60 s = 27 measured runs, plus 2 probe runs (1050 and 1500, 30 s). Total time 2,014 s = 33.6 min                                                                                                    |
| Dashboard          | `infra/observability/dashboards/stage-1/B1 - Ramp to collapse.json`, screenshot `EventPulse B1 - Ramp to collapse (baseline).png`                                                                                                                                |

## Method

1. Coarse pass first (`./run.sh coarse`). I only have its output `suggestion.env`, which said the last coarse rate before the first collapse was 1,500 req/s, so it chose the fine rates 750 to 2,350.
2. Fine pass (`./run.sh fine`). Repeat is the outer loop, so each repeat sweeps all 9 rates.
3. Before every run: stop the server, wait for Postgres to be quiet, start the server, send 100 warm-up requests, `TRUNCATE events`, reset `pg_stat_statements`, `CHECKPOINT`, wait 5 s. So every run starts from an empty table and a fresh pool.
4. After every run: count rows with `select count(*) from events` and compare with the number of 202 answers k6 counted (the audit).
5. Probe runs (rate 1050 and 1500) had `pg_stat_activity`, `pg_stat_statements`, `iostat -x 1 5`, `vmstat 1 5` and `pidstat` attached at the middle. I do not quote their latency, only their diagnostic output.
6. **Discarded:** none. **Flagged:** run `b1-m-r1550-n1` (see "What I saw", point 6) and every run at 1,950 or more, because they have `dropped_iterations > 0`. I kept the collapsed runs because the dropping is part of the finding (point 4 below), but I note it here as my own decision.

## Results and observations

Dashboard screenshot: `EventPulse B1 - Ramp to collapse (baseline).png`.

### Outcome per rate (median of 3 repeats, range in brackets; from `summary.tsv`)

| Offered (req/s) | Achieved (req/s) |           p50 (ms) |        p95 (ms) |               p99 (ms) |  Dropped iterations | Errors | Verdict               |
| --------------: | ---------------: | -----------------: | --------------: | ---------------------: | ------------------: | -----: | --------------------- |
|             750 |              750 |      4.9 (4.8-4.9) |             5.8 |          7.8 (7.4-8.0) |                   0 |      0 | OK                    |
|             950 |              950 |      4.5 (4.3-4.6) |             5.8 |         9.6 (9.1-10.3) |                   0 |      0 | OK                    |
|            1150 |             1150 |      4.7 (4.6-4.8) |             5.8 |         9.5 (8.8-10.5) |                   0 |      0 | OK                    |
|            1350 |             1350 |                4.9 |             5.9 |       11.9 (10.8-13.5) |                   0 |      0 | OK                    |
|            1550 | 1550 (1535-1550) |      5.1 (5.1-5.3) | 7.5 (6.5-701.4) |      38.7 (29.9-770.1) |           0 (0-866) |      0 | OK, one outlier run   |
|            1750 |             1750 |      5.1 (5.0-5.1) | 10.9 (7.7-11.4) |       37.8 (23.4-46.2) |                   0 |      0 | OK                    |
|            1950 | 1862 (1855-1873) |    705.9 (660-735) |          1280.6 | 1367.9 (1195.6-1486.3) | 2,729 (2,347-2,970) |      0 | COLLAPSED (p99 > 1 s) |
|            2150 | 1865 (1855-1874) | 1566.9 (1566-1570) |          1655.9 |     1698.3 (1668-1726) |              14,204 |      0 | COLLAPSED             |
|            2350 | 1852 (1845-1853) | 1596.3 (1589-1598) |          1682.2 |     1787.3 (1711-1803) |              26,895 |      0 | COLLAPSED             |

Audit: all 27 measured runs and both probes have rows in the table equal to the 202 answers (`audit = ok`). That is 2,378,432 acknowledged events across the 27 measured runs and none missing. There were zero 503 and zero other errors in every run (`n503 = 0`, `n_other = 0`, `err_pct = 0`).

### Representative pair: a comfortable step vs a collapsed step (repeat 2)

|                                                         |                   Rate 1350 |                                                 Rate 2150 |
| ------------------------------------------------------- | --------------------------: | --------------------------------------------------------: |
| Achieved req/s                                          |                       1,350 |                                                     1,865 |
| p50 / p99                                               |            4.9 ms / 13.5 ms |                                       1,570 ms / 1,698 ms |
| Dropped iterations                                      |                           0 |                                                    14,204 |
| Pool state at the end of the run (`/debug/stats`, post) | max 10, total 10, waiting 0 | max 10, total 10, waiting 0 (the run had already drained) |
| RSS / heapUsed after the run                            |              251 MB / 61 MB |                                           529 MB / 227 MB |
| Rows vs 202s                                            |             81,002 = 81,002 |                                         114,798 = 114,798 |

### Probe runs (diagnostic output only; latency not quoted)

| What                                                                                     | Rate 1050                                                                                                                                  | Rate 1500                                                                                                                |
| ---------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------ |
| Active backend wait events (one sample)                                                  | `LWLock WALWrite` x3, `IO WALSync` x1, one active with no wait, 6 idle `ClientRead`                                                        | `LWLock WALWrite` x5, `IO WALSync` x1, one active with no wait, 4 idle `ClientRead`                                      |
| Blocked / blocking queries                                                               | none                                                                                                                                       | none                                                                                                                     |
| `pg_stat_statements` INSERT                                                              | 11,509 calls, mean **0.021 ms**                                                                                                            | 16,401 calls, mean **0.018 ms**                                                                                          |
| iostat `nvme0n1` (4 real intervals; the first report is the average since boot, ignored) | 739-803 w/s, about 3.9-4.6 MB/s written, `w_await` 1.22-1.27 ms, `f/s` (flushes) 395-400, `f_await` about 2.44-2.49 ms, `%util` 96.7-98.1% | 739-844 w/s, about 4.0-4.5 MB/s, `w_await` 1.17-1.32 ms, `f/s` 369-380, `f_await` about 2.57-2.63 ms, `%util` 96.7-97.6% |
| vmstat (4 intervals)                                                                     | `r` 0-1, `b` 1, `wa` 8%, `id` 83-86%, `cs` 45-59 k/s                                                                                       | `r` 0-4, `b` 1, `wa` 8%, `id` 81-83%, `cs` 52-57 k/s                                                                     |
| pidstat, Node process                                                                    | 18.4% of one CPU (15.0 usr + 3.4 sys), ran on CPU 0 and 6                                                                                  | 24.2% of one CPU (19.4 usr + 4.8 sys), ran on CPU 0 and 6                                                                |

### Highest dashboard values (legend max / mean / last)

| Panel                                                                                                |                      Max |                     Mean |
| ---------------------------------------------------------------------------------------------------- | -----------------------: | -----------------------: |
| Offered rate (k6 started + dropped)                                                                  |             2.35 K req/s |                   1.47 K |
| Server 202/s                                                                                         |             1.94 K req/s |                   1.21 K |
| k6 latency p50 / p99 / p99.9                                                                         | 1.74 s / 1.98 s / 2.00 s | 385 ms / 422 ms / 426 ms |
| Pool total / waiting                                                                                 |              10 / 2.99 K |               9.87 / 594 |
| Pool wait p50 / p99                                                                                  |          1.74 s / 2.49 s |          399 ms / 587 ms |
| A query-time series (the legend row is cut off at the bottom of the panel, I read it as "query p50") |                  7.58 ms |                  5.21 ms |
| In-flight: measured / rate x mean latency                                                            |          3.00 K / 3.12 K |                601 / 651 |
| Pool-bound ceiling (10 / mean query time)                                                            |             2.64 K req/s |                   2.10 K |
| Node CPU user / system (cores)                                                                       |            0.519 / 0.077 |            0.200 / 0.041 |
| Event loop delay p99 / max                                                                           |        24.7 ms / 61.0 ms |        11.8 ms / 13.0 ms |
| Postgres container CPU, total (cores)                                                                |                    0.393 |                    0.223 |
| Postgres CPU throttled (s per s)                                                                     |                        0 |                        0 |
| Host CPU busy: server (0,6) / postgres (1,7,2,8) / k6                                                |    36.8% / 45.6% / 30.9% |    15.7% / 27.9% / 11.8% |
| Host disk `nvme0n1` util                                                                             |                    98.7% |                    77.4% |
| Host disk write await                                                                                |                  12.9 ms |                  1.90 ms |
| Postgres disk write                                                                                  |                13.9 MB/s |                4.10 MB/s |
| Postgres container memory                                                                            |                  261 MiB |                  213 MiB |
| Server open fds / open connections                                                                   |          3.03 K / 3.00 K |                941 / 908 |
| k6 dropped iterations/s                                                                              |                      648 |                      240 |
| k6 VUs / VUs max                                                                                     |                3 K / 3 K |          1.09 K / 2.01 K |
| Listen overflows / drops                                                                             |                        0 |                        0 |
| RSS / heap used                                                                                      |        505 MiB / 274 MiB |       267 MiB / 79.8 MiB |

### What I saw

1. **The capacity is about 1,860 req/s.** Every run offered 1,950 or more handled only 1,845 to 1,874 req/s (nine runs, median 1,855, mean 1,859, so about 1% spread), no matter whether 1,950, 2,150 or 2,350 were offered. Up to 1,750 every request was handled at the offered rate.
2. **The tail bends before the median.** p50 stays at 4.3 to 5.3 ms from 750 to 1,750. p99 grows slowly (7.8 ms at 750, 11.9 ms at 1,350), then jumps to 38.7 ms at 1,550 and 37.8 ms at 1,750, which is about 3x the 1,350 value, while p95 only starts to move at 1,750 (10.9 ms).
3. **Between 1,750 and 1,950 the system tips over.** At 1,950 p50 is 706 ms and p99 1,368 ms in all 3 repeats; at 2,150 and 2,350 p50 is about 1.57 and 1.60 s.
4. **There were no errors at all, even during the collapse.** Zero 503, zero timeouts. The collapse shows up only as latency and dropped iterations. In the collapsed runs the in-flight count is exactly 3,000 (measured max 3.00 K), the same as k6's VU limit (`vus max` 3 K), and open connections peaked at 3.00 K.
5. **Little's Law holds.** At the peak the measured in-flight is 3.00 K and rate x mean latency is 3.12 K (4% apart). Also 3,000 VUs / 1,865 req/s = 1.61 s, close to the observed p50 of 1.57 s at 2,150.
6. **One outlier:** `b1-m-r1550-n1` had p95 701 ms, p99 770 ms, 866 dropped iterations and achieved 1,535 instead of 1,550. Repeats 2 and 3 at the same rate had p99 38.7 and 29.9 ms and no drops. The same bump is visible as a short spike in the latency panels near the first sweep. I do not have the capture file of that run (only repeat 2 and the probes).
7. **The pool is where the queue is.** In the collapsed runs the pool had total = 10 (its maximum) and `waiting` went up to 2.99 K. At the end of the 1,350 and 2,150 runs the pool state was 10 / 10 idle / 0 waiting, because the run had drained.
8. **Postgres executes the INSERT in about 0.02 ms, but Node sees about 5 ms.** `pg_stat_statements` says mean 0.021 ms and 0.018 ms. The query-time legend row (partly cut off) says mean 5.21 ms and max 7.58 ms. Request p50 stays at 4.3-5.3 ms for the whole OK range, which matches that Node-side time.
9. **Active backends were waiting for the WAL.** In both probe samples nearly every active backend waited on `WALWrite` or `WALSync`, none was blocked by a lock, and 4-6 backends were idle waiting for Node (`ClientRead`).
10. **The disk is almost always busy, and its flush rate does not change between 1,050 and 1,500 req/s.** `%util` was 96.7-98.1% in both probes, about 370-400 flushes/s, `f_await` about 2.4-2.6 ms, only about 4 MB/s written. The host disk util panel shows about 100% flat tops for every run, so I cannot tell the rate steps apart in it.
11. **CPU was not the limit anywhere.** Node: 0.52 user + 0.08 system cores at the maximum (the Node thread is one core). Postgres container: 0.39 of its 2 cores, never throttled. k6 CPUs: 30.9% busy at most (below my 70% rule). Event loop delay p99 averaged 11.8 ms, max 61 ms.
12. **Memory climbs in the collapsed runs.** After the runs at 1,950 to 2,350 the server RSS was 453 to 529 MB and heapUsed 207 to 289 MB, against 267 MB and 82 MB after the 1,750 run (post captures).
13. **No listen overflows or drops, no 4xx/5xx.** The fds peaked at 3.03 K (about one per open connection); I did not put the fd limit itself on a panel, so I cannot say how close that is.

## Explanation

**Why the knee is where it is (about 1,860 req/s).** Each request needs one of 10 pool connections for the whole INSERT, including the COMMIT. From Node's side that costs about 5 ms (observation 8), but Postgres' own execution of the INSERT is 0.02 ms (observation 8), so almost all of the time is the COMMIT waiting for the WAL to reach disk (observation 9: `WALWrite` / `WALSync`). With 10 connections and about 5.4 ms each, the pool can complete at most 10 / 0.0054 s = 1,860 INSERTs per second. That matches the measured plateau of 1,855-1,860 (arithmetic: 10 / 1,860 = 5.4 ms, and the legend mean query time of 5.21 ms gives 10 / 0.00521 = 1,919 req/s, 3% off). Evidence: observations 1, 7, 8, 9.

**Why it is latency and not errors.** Once the offered rate is above 1,860, the extra requests queue inside Node in front of the pool (`waiting` up to 2.99 K). k6 had 3,000 VUs, so there can be at most 3,000 requests in flight. By Little's Law the wait is 3,000 / 1,860 = 1.6 s, which is below the pool timeout of 2 s, so no request ever timed out and I got no 503. That is why p50 settles at about 1.6 s and dropped iterations simply grow with the excess (2,150: shortfall about 285 req/s; 2,350: about 500 req/s). This means the 1.6 s plateau and the "no errors" are partly a property of my test (the VU limit), not of the server. Evidence: observations 3, 4, 5. (Inferred, not tested: with more than about 3,700 VUs the wait would pass 2 s and I should start to see 503.)

**Why 1,950 shows only about 0.7 s and not 1.6 s.** At 1,950 the overload is only about 90 req/s, so the queue grows slowly, and over 60 s it had not yet reached 3,000 in flight. That is consistent with the smaller p50 and the smaller dropped count (2,347-2,970). This is an inference from arithmetic (3,000 / 90 = 33 s to fill the VUs); I did not measure the queue growth over time.

**Why the tail bends before the median.** The utilization of the pool is rho = offered rate / capacity: 1,350/1,860 = 0.73, 1,550/1,860 = 0.83, 1,750/1,860 = 0.94, 1,950/1,860 = 1.05. As rho goes to 1, waiting time rises like 1 / (1 - rho), so the unlucky requests (p99) feel it first while most requests still find a free connection. This matches the jump of p99 from 11.9 ms to 38.7 ms at 0.83 and the p95 move at 0.94. The numbers use one fixed capacity; capacity itself may depend on load (see next paragraph). Evidence: observation 2.

**Why CPU is low while the system is stuck.** Neither Node nor Postgres is computing. Node uses about 0.16-0.175 ms of CPU per request (pidstat: 18.4% of a CPU at 1,050 req/s = 0.175 ms; 24.2% at 1,500 = 0.161 ms), so on arithmetic alone one Node thread could do about 5,700-6,200 req/s. Postgres uses 0.39 of 2 cores. Time is spent waiting on the disk flush. Evidence: observations 9, 10, 11.

**Group commit and the disk.** The disk is already about 97% busy and flushing about 370-400 times per second at 1,050 req/s, and shows the same flush rate at 1,500 req/s (observation 10). If each flush served only one commit, 400 flushes/s would cap throughput at 400 req/s, but I handled 1,500 and 1,860. So several commits must share one flush (group commit). Arithmetic: 1,050 / 380 = 2.8 commits per flush, 1,500 / 380 = 3.9, and 1,860 / 380 = 4.9 at the plateau. The number of commits that can share a flush is limited by how many backends are committing at the same moment, and that is limited by the pool of 10. This is my explanation of why throughput is "pool size x commit speed", but I did not test it (see claim status).

## Status of each claim

| Claim                                                                                                            | Status                                                                 |
| ---------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| Capacity is about 1,860 req/s (1,845-1,874 over 9 collapsed runs)                                                | Observed                                                               |
| Knee lies between 1,750 and 1,950 req/s; 1,750 is fully handled in all 3 repeats                                 | Observed                                                               |
| p99 rises from about 12 ms to about 38 ms between 1,350 and 1,550 while p50 stays flat                           | Observed                                                               |
| No errors or 503 in any run; no rows lost; no committed-but-not-acked rows                                       | Observed                                                               |
| In-flight reached 3,000 = k6 VU limit at saturation                                                              | Observed                                                               |
| Little's Law matches (3.00 K vs 3.12 K)                                                                          | Observed                                                               |
| Requests wait in front of the pool (`waiting` up to 2.99 K, total = 10)                                          | Observed                                                               |
| INSERT executes in about 0.02 ms in Postgres but takes about 5 ms as seen from Node                              | Observed (the Node-side number comes from a partly cut-off legend row) |
| Active backends were waiting on `WALWrite`/`WALSync`, no lock waits                                              | Observed (two samples only)                                            |
| Node and Postgres CPU are not the limit                                                                          | Observed                                                               |
| The limit is "10 connections x commit latency"                                                                   | Inferred (it fits the arithmetic and the observations above)           |
| No 503/timeouts are due to the 3,000 VU cap and the 2 s pool timeout                                             | Inferred                                                               |
| Group commit is why 380 flushes/s can carry 1,860 commits/s                                                      | Likely, not verified                                                   |
| Raising the pool size would raise capacity, or `synchronous_commit=off` would raise it much more                 | Not tested (these are B2 and B6)                                       |
| What caused the one-off stall in `b1-m-r1550-n1`                                                                 | Not resolved                                                           |
| Why the Postgres CPU group (1,7,2,8) shows up to 45.6% host-CPU busy while the container used at most 0.39 cores | Not resolved (maybe kernel I/O threads on those CPUs; not checked)     |

## Prediction vs observation

| Prediction                                        | Observed                                                                                      | Verdict                                                                                                     |
| ------------------------------------------------- | --------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| Node CPU is the first limit, at 3,000-5,000 req/s | The knee is about 1,860 req/s. Node used at most 0.6 core and about 0.16-0.175 ms per request | **Wrong** (limit is lower, and it is not Node)                                                              |
| CPU per request 0.20-0.30 ms                      | 0.161-0.175 ms (two pidstat windows)                                                          | Wrong, cheaper than I assumed                                                                               |
| Pool ceiling 10 / 2 ms = 5,000 req/s              | Mean time holding a connection is about 5.2-5.4 ms, so the ceiling is about 1,860-1,920 req/s | **Partly right**: the pool is where the queue forms, but the service time was 2.5-2.7x higher than my guess |
| Pool at 80% busy at 4,000 req/s                   | At 1,750 req/s the pool is already about 94% busy by the same formula                         | Wrong rate, right idea                                                                                      |
| Postgres CPU not the first limit                  | 0.39 of 2 cores, never throttled                                                              | **Right**                                                                                                   |
| p50 flat, p99 bends at the knee                   | p50 4.3-5.3 ms to 1,750; p99 jumps at 1,550                                                   | **Right**                                                                                                   |
| Then 503 or client timeouts                       | No error in any run                                                                           | **Wrong** (VU cap hid them, see Explanation)                                                                |
| Little's Law: in-flight = rate x latency          | 3.00 K vs 3.12 K                                                                              | Right                                                                                                       |

## Verdict on the hypothesis

**Confirmed with corrections.** Latency stayed flat and then bent sharply, as hypothesized, and the limit is a mix of pool size and database commit capacity, not Node CPU. My prediction of which resource would hit first (Node CPU) was wrong. The honest statement is: with a pool of 10 and a commit that takes about 5 ms because of the WAL flush on this disk, the naive design handles about 1,860 events per second, and Node and Postgres are mostly idle at that point. I cannot yet say whether the pool or the disk is the more fundamental limit; that is what B2 (pool size sweep) and B6 (`synchronous_commit`) are for.

## Limitations

- **Single machine, loopback / Docker network.** Numbers show what limits what on this laptop, not absolute capacity. Laptop NVMe flush latency (about 2.5 ms) probably differs a lot from a server SSD.
- **3,000-VU cap shapes the collapsed runs.** The 1.6 s plateau and the missing 503s depend on it. I did not run with a higher `MAX_VUS`.
- **Only 3 repeats** and fine steps of 200 req/s, so the knee is bracketed (1,750 to 1,950), not located more precisely. The max-throughput number (1,860) is more precise than the knee position.
- **`pg_stat_database` deltas were lower than the exact row counts** (for example 56,638 inserts counted vs 57,001 rows at 950 req/s; 103,711 vs 105,001 at 1,750 req/s, always 227 to 1,290 lower in the repeat-2 captures). Probably the statistics are flushed with a delay (not verified). I used `count(*)` for the audit and did not use those deltas for rates.
- **No checkpoint fell inside the repeat-2 measured runs** (`checkpoints_timed` stayed 390 and `checkpoints_req` did not change between pre and post of each). I do not have that for the other repeats.
- **The event loop delay histogram has a 10 ms resolution.** Even an idle server shows p50 about 10.08 ms in every capture, so about 10 ms is the floor, not a real delay. The dashboard text "near 0 while healthy" was wrong; only values above 10 ms mean something.
- **Server-side latency histogram buckets jump from 1 s to 2.5 s,** so the "p99 2.49 s" and "pool wait p99 2.49 s" values are coarse estimates inside that bucket. k6 recorded a maximum p99.9 of 2.00 s and no 503 happened, so no wait really exceeded the 2 s pool limit. Trust the k6 tail numbers.
- **Probe runs** had heavy tools attached, so I did not use their latency (they were fine anyway: p50 4.9-5.1 ms).

## Cost

- **What one event costs now:** about 5.4 ms of one pool connection (mostly the commit), about 0.16-0.175 ms of Node CPU, about 0.02 ms of Postgres execution time, about 0.5 KB of request body, and roughly 2.7-3.8 KB of disk write per event (iostat showed about 4,000 KB/s at 1,500 and 1,050 req/s: 4,000/1,500 = 2.7, 4,000/1,050 = 3.8; includes WAL overhead; I do not have it at 1,860).
- **What the failure costs:** above 1,860 req/s every extra request waits. At 2,150 req/s the median user waits about 1.6 s, and the server holds 3,000 queued requests in memory (RSS up to 505 MiB vs about 250 MiB when healthy). With no client timeout or backoff, nothing tells the client to slow down.
- **What a fix would cost (not tested):** a bigger pool costs more connections and more Postgres memory (B2). `synchronous_commit=off` is cheap but can lose the last commits on a crash (B6). A queue in front of Postgres (Stage 2) removes the commit from the request path.

## Takeaways

1. On this machine and config the naive design handles about 1,860 events/s, and a safe operating point is about 70% of that, about 1,300 req/s (p99 about 12 ms at 1,350). I should always quote the operating point, not the peak.
2. Measure the service time instead of guessing it. My guess of 2 ms for the INSERT was off by about 2.7x, and that single number moved the prediction from 5,000 to 1,860.
3. Low CPU does not mean healthy. Node and Postgres were mostly idle while the system was stuck; the wait was on commit and on the pool. This is the answer to Stage 1 self-test question 3.
4. Look at what limits my own test: the VU cap decided the latency plateau and hid the 503s. For B2 and B10 I should raise `MAX_VUS` or set a lower request timeout on purpose.
5. The disk shows about 100% util even at low rates, so util alone cannot say when the disk is the limit. Use flushes per second, `f_await` and the wait events (`WALSync`) instead.
6. Next experiments that separate the pool from the commit: **B2** (pool size 5, 10, 25, 50: if capacity grows with the pool, group commit is the mechanism) and **B6** (`synchronous_commit=off`: if capacity jumps, the commit wait is the cost). Also B3 (slow database) now has a baseline.
7. This is the "before" number for Stage 2: the queue should move the ingest knee far above 1,860 req/s, and I can now check by how much.
