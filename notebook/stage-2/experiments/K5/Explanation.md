# K5 Experiment explanations

Status: PRE-RUN. Written 2026-11-22 before any measured run. The sections after the marker are appended after the experiment.

## Purpose

I want to know what really happens to events when the consumer dies at each of the steps of handling a chunk: before parsing, after parsing, and after the database commit but before the offset commit. This is the practical meaning of at-least-once delivery for EventPulse: whether an event can be lost, whether it can appear twice, and what the ON CONFLICT dedup changes. Once I know it I can write the table of delivery guarantees (crash point x dedup on/off -> lost / duplicated / absorbed) that goes into the Stage 3 delivery guarantees document, with measured numbers behind each cell.

## Hypothesis

A crash after the database commit and before the offset commit makes the next consumer read the same chunk again, so the rows are written a second time: with ON CONFLICT dedup they are absorbed (the table has each event once, the duplicates counter goes up), without dedup they become duplicate rows. A crash before the database commit (before parse or after parse) also makes the chunk be read again, but nothing was stored, so it is written once: no duplicates and no loss. In every case no event is lost, because the offset only moves after the database commit. The mechanism is the commit order: database first, offset second, so the offset is never ahead of the data.

## Predictions (written before the run)

Setup numbers: 2000 req/s x 30 s = 60,000 events per run, in 6 partitions (key = event id, so about 10,000 each, about 333 messages/s per partition). The consumer is killed on its 5th batch, so within the first second or two of the load.

1. **No loss in any cell.** `missing = acked - distinct rows = 0` in all 24 runs, including dedup off. The topic holds exactly the acked events (`topic = acked`, the producer is idempotent with acks=all).
2. **Control (no crash): 0 / 0 / 0.** `dup_rows = 0`, `absorbed = 0`, `reread = 0`, for dedup on and off.
3. **before_parse and after_parse: no duplicate rows, but a re-read.** `dup_rows = 0` and `absorbed = 0` for both dedup settings. `reread = crash_chunk` exactly (the chunk in flight is handed to a handler twice), so the re-read happens at every crash point and is not the same thing as a duplicate row.
4. **after_db_commit, dedup on:** `absorbed = crash_chunk` exactly and `rows_total = distinct = acked` (one row per event). **dedup off:** `dup_rows = crash_chunk` exactly, `rows_total = acked + crash_chunk`, `distinct = acked`. Same chunk, two different outcomes; only the dedup differs.
5. **Size of the crashed chunk:** the 5th batch is cut from what a partition received since its previous fetch. At about 333 messages/s per partition and a fetch wait of at most 0.5 s, a batch holds at most about 333 x 0.5 = 167 messages (the 1000 chunk limit is not reached). I expect `crash_chunk` between 10 and 170, around 100, and it will differ between repeats, so it is the number I read from the log, not a constant.
6. **Time to resume is about the session timeout, not the process start time.** The old member's last heartbeat was at most 1 s before the kill, so it is dropped at about 10 s - (up to 1 s) = 9 to 10 s after the kill. Consumer #2 starts and joins in 1 to 2 s, which is earlier, so it waits for the rebalance. First chunk processed at about 0.1 to 0.5 s after it gets its partitions. Prediction: `resume_s` about 10 s (range 8 to 13), the same for every crash point and for dedup on/off.
7. **Backlog and catch-up:** the gap of about 10 s at 2000 req/s makes a backlog of about 20,000 messages. I do not know the consumer's top speed here, so I only predict that lag reaches 0 within about 10 s after the resume and that the freshness p99 for the waiting rows is about the length of the gap (10 to 12 s).
8. **What I expect NOT to change:** the producer side (k6 achieved rate, API p99, 202 count). The API does not know the consumer died.

## Setup (planned)

| Item | Value |
|---|---|
| Server | ingest-api `node --env-file=.env dist/main.js` under `taskset -c 0,6`, acks=all + idempotent, started by `run.sh` |
| Consumer | consumer-sink under `taskset -c 1,7`, one process, 6 partitions, `CONSUMER_PARTITIONS_CONCURRENTLY=1`, chunk limit 1000, session timeout 10 s, heartbeat 1 s; `DEBUG_FAULTS=true`, `CRASH_AT=<point>`, `CRASH_AFTER_BATCHES=5`; dedup off = `SINK_DEDUP=false` writing to `events_nodedup` |
| Cluster | 3 brokers (brokers on 2,8,3,9), topic `k5-events` 6 partitions RF 3 min ISR 2, recreated before each run |
| Sysctls | none changed |
| Route / generator | `POST /v1/events`, k6 `constant-arrival-rate` 2000 req/s for 30 s, pinned to 4,10 via the shared `runner.sh` |
| Matrix | 2 dedup x 4 crash points (incl. control) = 8 cells x 3 repeats x 30 s load, about 52 min; order reversed on repeat 2 |
| Dashboard | [K5 - Kill a consumer at every step.json](../../../../infra/observability/dashboards/stage-2/K5%20-%20Kill%20a%20consumer%20at%20every%20step.json) |
| Versions | filled in after the run from env.txt |

## Method (planned)

1. For each run: recreate the topic, start the API, start consumer #1 with its crash point armed and a new group id, wait until it owns partitions, start k6. When consumer #1 dies, start consumer #2 with the same group id and no crash point and record the time until its first processed chunk. When k6 is done wait for group lag 0, then count rows (all and distinct) for the run's id prefix in the right table, count the records in the topic, and read the size of the crashed chunk from the log line `rows=N`.
2. Measured runs are all 24. There are no probes: nothing heavy runs next to a measured run (the only extra work is one `/metrics` poll every 0.1 s on the consumer).
3. A run is valid if: k6 dropped iterations = 0, k6 failed requests = 0, the crash happened (`status` column is `ok`, not `NO_CRASH`), consumer #2 resumed, the drain finished, and `topic = acked`.
4. Discard rules, fixed now: a run with dropped iterations > 0, `status` other than `ok`, or `topic != acked` is discarded and repeated; the audit of a run whose drain timed out is not used. I do not drop a run because its numbers look odd.
5. Headline numbers per cell are the median and the range over the 3 repeats; the exact crash_chunk of each run is used to check predictions 3 and 4 run by run (they are exact equalities, not averages).

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->
