# K7 Experiment explanations

Status: PRE-RUN. Written 2026-11-22 before any measured run. The sections after the marker are appended after the experiment.

## Purpose

I want to know what a slow Postgres does to a system that has a queue between the API and the database. In Stage 1 a slow database made every request slow; in Stage 2 the API only needs Kafka, so the cost should move from request latency to consumer lag. Once I know how fast the lag grows, how fast it drains and what it costs on the brokers' disks, I can decide what to alert on (lag, not latency) and how much retention and drain headroom the sink needs.

## Hypothesis

Ingestion keeps its latency and its rate while Postgres is slow, because the API never touches the database. The consumer lag grows linearly with the shortfall (arrival rate minus what the sink can write), and after Postgres recovers the lag drains at (healthy sink capacity minus arrival rate), so recovery time depends on how far the sink can exceed the arrival rate. The mechanism: one write transaction is three round trips (BEGIN, INSERT, COMMIT), so an added latency L makes a chunk take about 3L longer, and the sink writes one chunk at a time. My own refinement, from reading the sink: chunks grow when records pile up (up to 1000 rows), so a small slowdown may be absorbed and the lag would level off instead of growing without end.

## Predictions (written before the run)

Settings: 2,000 req/s, 6 partitions, one sink writing one chunk at a time, chunk at most 1000 rows. Two numbers below are guesses I have not measured and the calibration probe will check them: a healthy write of about 1000 rows takes about 30 ms, and the healthy capacity C0 of the sink is about 1000 rows / 0.03 s, but certainly well above 2,000 rows/s (I take C0 = 5,000 rows/s as a working figure for the drain maths).

1. **Ingest does not notice.** In every cell the k6 request p99 in the slow window is within 10 % of the healthy window, accepted rate stays at 2,000/s, no 503, nothing dropped. The API answers after the Kafka acknowledgement and does not use the proxy.
2. **Write time seen by the sink** is about 3 L + 30 ms: s50 about 180 ms, s200 about 630 ms, s500 about 1.53 s (healthy about 30 ms).
3. **Sink capacity while slow** = 1000 rows / (3 L + 0.03 s):
   - s50: 1000 / 0.18 = about 5,500 rows/s, above 2,000. No lasting shortfall: the lag stays small (a few thousand records at most) and levels off, because the chunks get bigger. This is the cell where I expect the "linear growth" part of the hypothesis not to apply.
   - s200: 1000 / 0.63 = about 1,590 rows/s, shortfall 2,000 - 1,590 = about 410/s. Lag at the end of the slow window about 410 x 120 s = about 49,000 records.
   - s500: 1000 / 1.53 = about 650 rows/s, shortfall about 1,350/s. Lag at the end of the slow window about 1,350 x 120 s = about 162,000 records.
   - s0: no growth; lag stays at the healthy level.
4. **Growth is linear in the slow window for s200 and s500**: `growth_rps` about 400 and about 1,350 records/s. For s200 and s500 the rows per chunk reach the 1000 limit.
5. **Drain after the toxic is removed** (load still on): lag falls at C0 - 2,000 = about 3,000/s with my working C0. s200: 49,000 / 3,000 = about 16 s. s500: 162,000 / 3,000 = about 54 s. Both finish inside the 120 s after-window. If the calibration shows a different C0, the same formula `drain = lag_end / (C0 - 2000)` applies; I expect it to hold within 30 %.
6. **Freshness** at the end of the slow window is about lag / arrival rate: s200 49,000 / 2,000 = about 25 s, s500 162,000 / 2,000 = about 81 s.
7. **Disk on the brokers is the same in every cell.** The log grows with what the API writes (2,000 records/s x 300 s = 600,000 records), not with the lag. With a guess of about 400 bytes per record per copy: 600,000 x 400 B = about 240 MB per copy, about 720 MB over the three brokers. The difference between cells is within a few percent. No record is deleted (retention 1 day).
8. **No data lost.** In every run the rows in Postgres equal the 202 answers (missing = 0), no chunk failures, no DLQ records, no rebalances after the start.

What I expect NOT to change: ingest rate and latency, the API's CPU, the broker disk growth rate.

## Setup (planned)

| Item | Value |
|---|---|
| Services | real ingest-api (acks=all, idempotent, key mode `event`) and one consumer-sink; both started by `run.sh` per run |
| Consumer settings | `CONSUMER_PARTITIONS_CONCURRENTLY=1`, `CONSUMER_MAX_BATCH_SIZE=1000`, `SINK_MAX_ROWS=1000`, `PG_POOL_MAX=5`, new group per run |
| Topic | `k7-events`, 6 partitions, RF 3, min ISR 2, retention 1 day, recreated before each run |
| Slowness | Toxiproxy `latency` toxic, downstream, on proxy `pg` (`localhost:15434` to Postgres), added to every Postgres answer of the sink |
| Pinning | server 0,6; sink 1,7; brokers 2,8,3,9; k6 4,10; monitoring + Postgres + Toxiproxy 5,11 |
| Load | k6 `constant-arrival-rate`, 2,000 req/s for 300 s (healthy 60 s, slow 120 s, after 120 s) |
| Matrix | cells s0, s50, s200, s500 x 3 repeats (repeat 2 in reverse order) = 12 runs, about 9 min each, about 1 h 50 min; plus a separate calibration probe |
| Dashboard | [K7 - Slow Postgres.json](../../../../infra/observability/dashboards/stage-2/K7%20-%20Slow%20Postgres.json) |
| Scripts | [run.sh and script.js](../../../../loadtests/k6/stage-2/K7/README.md) |
| Versions | filled in after the run from env.txt |

## Method (planned)

1. Run the calibration probe once (`CALIBRATE=1`) to measure the healthy drain rate. It is a probe: its number is used only for the drain maths, never as a measured cell.
2. Run the 12 measured runs with one command. Each run: recreate the topic, start the API and the sink, start k6, wait 60 s, add the toxic (not for s0), wait 120 s, remove the toxic, let the load run 120 s more, wait for lag 0, check the rows against the 202 answers, compute the numbers from the one-second sampler file.
3. A run is valid if: dropped iterations = 0, no 503 and no failed requests, average write time in the slow window is at least 90 % of 3 x L (the toxic really acted), no rebalance after the start, missing = 0.
4. Discard rules, decided now: any run that breaks a validity rule above is not used for a headline number and is repeated; k6 generator CPU above about 70 % marks the run as contaminated; a trial run (`-trial` results name) is never quoted.
5. Report the median and range of the three repeats for each cell.

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->
