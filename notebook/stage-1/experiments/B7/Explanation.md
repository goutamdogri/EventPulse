# B7 Experiment explanations

Status: PRE-RUN. Written 2026-11-21 before any measured run. The sections after the marker are appended after the experiment.

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

| Item              | Value                                                                                                                                                  |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Server            | ingest-api `node --env-file=.env dist/main.js` started by `run.sh` before every run, pool 25, `COUNTER_MODE=off`, `BODY_LIMIT=1mb`, `taskset -c 0,6`   |
| Postgres          | the `ep-postgres` container, directly (no Toxiproxy), `synchronous_commit=on`, `max_wal_size` 1 GB, `shared_buffers` 128 MB, TOAST compression pglz     |
| Sysctls           | none changed                                                                                                                                           |
| Route / generator | `POST /v1/events`, k6 `constant-arrival-rate`, 100 tenants, random hex filler, body size set by `SIZE`, k6 pinned to CPUs 3,9,4,10                      |
| Matrix            | sizes 200 B, 500 B, 2 KB, 10 KB, 100 KB x phase `at` (4000, 4000, 3900, 3700, 1500 req/s) and `over` (8000, 8000, 8000, 6000, 3000 req/s) x 3 repeats x 45 s, about 46 min |
| Probe             | one body-limit probe at the end (default `BODY_LIMIT`, 60 KB and 100 KB bodies, curl)                                                                  |
| Dashboard         | [B7 - Payload size.json](../../../../infra/observability/dashboards/stage-1/B7%20-%20Payload%20size.json)                                                |
| Versions          | filled in after the run from env.txt                                                                                                                   |

## Method (planned)

1. Every run: stop the server, wait until Postgres is quiet, start a fresh server, 400 warm-up requests, `TRUNCATE events`, `CHECKPOINT`, 5 s settle. Then WAL position snapshot, k6, wait, snapshot again, and the sizes of the heap, the TOAST table and the two indexes (the table was empty at the start, so these are the growth of the run).
2. Repeat is the outer loop. Odd repeats go from 200 B to 100 KB, even repeats from 100 KB to 200 B, so slow drift (disk fill, temperature) does not favor one size. Inside a size, `at` runs before `over`.
3. Headline numbers are the median and the range of 3 repeats per cell. The body-limit probe is separate and not a headline number.
4. A run is discarded (and repeated) when: an `at` run has dropped iterations above 1% of the offered requests, k6's pinned CPUs are above about 70% busy, the row audit (202 count vs rows) is not ok, or the disk has less than 20 GB free at the start. A big `over` run with dropped iterations is expected and not discarded.
5. The "knee" per size is read from the `over` runs (accepted rps) and cross-checked with the `at` runs (latency and failures at 88 to 93% of the predicted capacity).

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->
