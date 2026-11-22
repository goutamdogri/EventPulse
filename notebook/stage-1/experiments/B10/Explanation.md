# B10 Experiment explanations

Status: PRE-RUN. Written 2026-11-22 before any measured run. The sections after the marker are appended after the experiment.

## Purpose

I want to know what ingest-api does when the database is slow or stuck and the server has no deadline on waiting for a pool connection or on a statement: does it fail fast, or does it hang? This matters now because Stage 1 says overload must produce fast 503s, never silent hangs (and B3 already showed that a slow database propagates to the API). When I know the answer I can explain why every external call needs a deadline, and I can decide which timeout values stay as the defaults.

## Hypothesis

Without a statement timeout and a pool wait timeout, a slow query ties up pool connections for as long as the database takes, and every other request waits in the pool queue with no limit. Requests then hang instead of failing, the queue and the memory of the Node process grow, and the server keeps working for clients that already gave up. With a pool wait deadline (2 s) and a statement deadline (5 s), the same fault should produce fast 503s, a bounded queue and bounded memory.

## Predictions (written before the run)

Pool capacity: 25 connections / (5 ms query + 200 ms added) = 122 req/s (h0 = 5 ms from B1/B2, same as B3).

**Fault `lat` (180 req/s for 60 s, 200 ms added, 15 s client timeout)**

1. `lat` + unset: the queue grows by 180 - 122 = 58 req/s. A request arriving at time t is served at about 1.475 t (it waits 58t/122 = 0.475 t seconds), so it gets an answer within 15 s only if t < 15 / 0.475 = 31.6 s. Requests arriving before 31.6 s: 180 x 31.6 = 5,700 get 202 (about 53%). Requests arriving later (about 5,100, 47%) are hung (status 0) because no deadline refuses them. Zero 503. The client latency p99 is 15 s (the timeout). Peak queue at the end of the load: 58 x 60 = 3,500 requests. After k6 stops the server still has about 3,500 requests to work through: 3,500 / 122 = about 29 s of drain, and about 5,100 rows are stored for clients that had already left (`stale_rows`). Memory growth: tens of MB (about 3,500 queued requests of a few KB each; I am not sure about the per-request size, so low confidence).
2. `lat` + set: the pool wait deadline caps the wait at 2 s. Accepted stays at about 122/s (68% of offered), 503 about 58/s (32%), hung 0. p99 about 2.2 s (2 s wait + 205 ms). The queue stays at most offered x wait = 180 x 2 = 360, so under 400. Drain after k6 under 3 s, `stale_rows` near 0, memory growth under 10 MB. No `statement_timeout` errors, because one INSERT takes about 205 ms, far from 5 s.

**Fault `stall` (100 req/s for 80 s, 20 KB filler, lock held 35 s from +15 s to +50 s)**

3. `stall` + unset: all 25 connections block on the lock and everything else queues: 100 x 35 = 3,500 requests in the queue at +50 s. Requests sent between +15 s and +35 s (20 s x 100 = 2,000) hit the 15 s client timeout while the lock is still held: hung about 2,000 / 8,000 = 25%. Requests sent between +35 s and +50 s wait up to 15 s and succeed late, so 202 is about 6,000 with latencies up to 15 s. p50 stays in milliseconds, p99 is 15 s. No 503. When the lock ends the queue drains in about 3,500 / 3,000 = 1 to 2 s (assuming about 3,000 INSERTs/s for 20 KB bodies, not measured), so `drain_s` after k6 is about 0. `stale_rows` about 2,000 (the INSERTs for the timed-out clients are still executed). Memory: 3,500 x 20 KB = 70 MB of bodies, probably 70 to 150 MB RSS growth because the body is copied when it is parsed.
4. `stall` + set: requests wait at most 2 s for a connection and then get 503; the 25 requests that hold a connection are cancelled by Postgres after 5 s (`statement_timeout`) and also answered with 503. About 100 x 35 = 3,500 requests get 503 (44%), hung 0, accepted about 4,500. Queue at most 100 x 2 = 200, memory growth under 20 MB (200 x 20 KB = 4 MB of bodies). `stale_rows` near 0. Normal service resumes within about 2 s after the lock ends. p99 between 2 s and 5 s.

**What I expect NOT to change**

5. Server CPU stays low in all stall runs (hung requests only wait), and Postgres CPU is near idle during the lock (waiting, not working). Event-loop lag stays small. In the probe run `pg_stat_activity` should show about 25 backends waiting with wait event type `Lock`.
6. Dropped iterations in k6 stay at 0: the most VUs needed is 180 x 15 = 2,700 (lat) and 100 x 15 = 1,500 (stall), below the limit of 4,000.

## Setup (planned)

| Item | Value |
| --- | --- |
| Server | `node --env-file=.env dist/main.js` under `taskset -c 0,6`, started and restarted by `run.sh` for every run; pool 25; variant unset = `PG_CONN_TIMEOUT_MS=0 PG_STATEMENT_TIMEOUT_MS=0`, set = `2000` / `5000` |
| Sysctls | none changed |
| Route / generator | `POST /v1/events`, k6 `constant-arrival-rate` (open model), k6 timeout 15 s, k6 pinned to 3,9,4,10; database behind Toxiproxy (as B3) |
| Faults | `lat`: 200 ms toxic for the whole run. `stall`: ACCESS EXCLUSIVE lock on `events` for 35 s at +15 s |
| Matrix | 2 faults x 2 variants x 3 repeats (lat 60 s, stall 80 s) + 1 probe run (stall, unset, heavy tools mid-stall), about 31 minutes |
| Dashboard | `infra/observability/dashboards/stage-1/B10 - Missing timeouts.json` |
| Versions | filled in after the run from env.txt |

## Method (planned)

1. For every repeat, for each fault, run both variants (unset and set), swapping the order each repeat. Each run restarts the server, warms up 400 requests, adds the fault, empties the table, runs k6, then waits until the server has nothing queued and records the drain time and the row count.
2. The probe run (stall, unset) is separate and never used for headline numbers. Heavy tools run only there.
3. From each run I read: answers by class (202, 503, hung), client p50 and p99, peak queue, peak in flight, RSS growth, drain time, stale rows, error types.
4. A run is discarded if k6 reports dropped iterations above 0, if the k6 CPUs are above about 70% busy, if the server's `/debug/stats` timeouts differ from the variant, or if the lock window in `faults.tsv` is not about 35 s long. These criteria are fixed now.

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->
