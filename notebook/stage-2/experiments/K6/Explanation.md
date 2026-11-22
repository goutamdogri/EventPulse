# K6 Experiment explanations

Status: PRE-RUN. Written 2026-11-22 before any measured run. The sections after the marker are appended after the experiment.

## Purpose

I want to know what one bad record does to the consumer, and whether the dead-letter fix (S2-F4) really contains it. Every event reaches Kafka through the API, but a record can also reach the topic from somewhere else (a buggy tool, an old producer), and then the consumer has to deal with a record it can never process. Once I know how much one record costs with the DLQ off, and what it costs with the DLQ on, I can write the retry and replay policy of Stage 3 with numbers instead of guesses.

I already did one manual before/after run of this (`notebook/stage-2/s2-f4-poison-before-fix.md`, 200 req/s, one run, invalid JSON only). It showed the partition stuck at offset 2893 and a drain after the fix. K6 repeats it at 2000 req/s, for four kinds of bad record, three times each, with the numbers taken by the script. So these predictions are not blind: I know the shape of the "off" result already, and I do not know the numbers at this rate or the "on" behaviour for the database error.

## Hypothesis

With the DLQ off, one bad record makes the consumer throw on the same chunk again and again. The client goes back to the same offset, so the partition of the poison record stops while the other five partitions keep draining. The lag of that one partition grows with the arrival rate, and it never comes down by itself. With the DLQ on, the consumer classifies the failure. A record that cannot be parsed is dead-lettered at once, a row that Postgres refuses is retried a bounded number of times with backoff and then dead-lettered, and in both cases the offset moves past the record. The partition drains, the record is in `k6-dlq` with its original bytes and the error in headers, and no good event is lost.

## Predictions (written before the run)

Numbers for the setup: 2000 req/s x 60 s = about 120,000 events, in 6 partitions (key = event id), so about 20,000 per partition, about 333 per second per partition. The poison goes in at t = 10 s, so about 3,300 records of its partition are before it and about 50 s x 333 = 16,650 come after it while the load runs.

1. **DLQ off, all four kinds: the poison partition stops.** Its committed offset stays at the poison offset until the end of the run. `stuck_lag` at the end (30 s after the load) is about 16,650 (range 15,000 to 18,500: the hash spreads events a bit unevenly between partitions). `stall_s` is at least 75 (50 s of the load + 30 s of observation, the sampler starts at the poison, so about 80 minus the sampling step). It does not recover on its own.
2. **DLQ off: the loss equals that lag.** `missing = acked - rows` is about 16,650 (14% of 120,000, that is one partition out of 6 for 50 of 60 seconds). Those events got a 202 but are not in Postgres. The poison record itself is not part of `acked` (it did not come through the API).
3. **DLQ off: the other five partitions are fine.** `other_lag_max` stays under about 500 (about one batch of 333/s x 0.5 s fetch wait, plus the chunk size). This depends on the consumer being faster than 2000 events/s with one partition at a time; I do not know its top speed in this setup, so this is the prediction I am least sure about. If it is wrong, the other partitions would show lag too and the comparison is harder to read.
4. **DLQ off: it is a hot retry loop, not a backoff loop.** In the manual run the same batch came back about 28 times per second (1,274 `batch_errors` in 45 s). I expect the same order of magnitude here: `blocked_attempts` (failed chunk attempts with reason `poison_blocked`) of about 20 to 100 per second, so 1,500 to 8,000 in 80 s. The consumer CPU is higher than in the DLQ-on cells because it keeps re-reading and re-parsing the same chunk. The four kinds behave the same in the off cells: the DLQ-off code throws on any error of the chunk, whatever the class.
5. **DLQ on, parser errors (invalid_json, bad_uuid, oversized): dead-lettered at once.** `dlq_records = 1`, `retries = 0`, `stall_s` at most 2 (the sampling step is a bit over 1 s, the real stall is milliseconds), `stuck_lag = 0` and `drained = yes`, `missing = 0`. Headers: `PERMANENT_PARSE` with the codes `invalid_json`, `bad_event_id` and `bad_type`, `attempts = 1`, `original_partition = 5`.
6. **DLQ on, constraint (jsonb refuses \u0000, SQLSTATE 22P05): bounded retries, then dead letter.** The class is `PERMANENT_DB`, so the bad row is isolated by bisection and retried `SINK_MAX_RETRIES = 3` times with equal-jitter backoff (caps 100, 200, 400 ms: 50 to 100 + 100 to 200 + 200 to 400 = 0.35 to 0.7 s in total). I expect `retries = 3`, `attempts = 4`, `dlq_records = 1`, `stall_s` at most 2, `missing = 0`. The good rows around it are stored.
7. **What I expect NOT to change:** the producer side. k6 achieved rate (2000/s), 202 count (about 120,000), API p99 and failed requests are the same in all cells and there are no dropped iterations, because the API does not know the consumer has a problem.
8. **Rows audit for DLQ on:** rows in Postgres = 202 answers exactly (no loss, no duplicate), in all 12 runs.

## Setup (planned)

| Item | Value |
|---|---|
| Server | ingest-api `node --env-file=.env dist/main.js` under `taskset -c 0,6`, acks=all + idempotent, `KAFKA_KEY_MODE=event`, started by `run.sh` |
| Consumer | consumer-sink under `taskset -c 1,7`, one partition at a time, chunks up to 1000 rows, retry settings at the sink defaults (3 retries, backoff 100 ms to 5 s), `SINK_DLQ_ENABLED` is the variable, DLQ topic `k6-dlq` |
| Kafka | 3 brokers, topic `k6-events` 6 partitions RF 3 min ISR 2, recreated per run; `k6-dlq` 1 partition |
| Route / generator | `POST /v1/events`, 100 B events, k6 `constant-arrival-rate` 2000 req/s for 60 s, pinned to 4,10 |
| Poison | one record written straight into `k6-events` by `scripts/produce-poison.sh` at t = 10 s, key `poison-observe-1` (partition 5) |
| Matrix | 2 variants (DLQ off/on) x 4 kinds x 3 repeats = 24 runs, 60 s load + 30 s observation, about 60 min in total; repeat 2 runs the cells in reverse order |
| Dashboard | [K6 - Poison message.json](../../../../infra/observability/dashboards/stage-2/K6%20-%20Poison%20message.json) |
| Versions | filled in after the run from env.txt |

## Method (planned)

1. For each run: fresh topics, API and one consumer-sink started with the cell's setting, k6 load, one poison record at t = 10 s, a sampler that reads the group's committed offset and lag per partition about once a second, 30 s of observation after the load, then the audit (rows in Postgres against 202 answers, lag per partition, records and headers in `k6-dlq`). All 24 runs are measured. There are no probe runs.
2. Judged valid when: k6 has 0 dropped iterations, the API answered every request with 202, and for the DLQ-on cells the header `dlq.original_partition` is the partition I assumed (5), otherwise the run is marked `WRONG_PARTITION`.
3. Criteria for discarding a run, decided now: dropped iterations above 0, a k6 failure, the consumer not assigned a partition before the load, the poison write failing (`POISON_FAILED`), or a drain timeout in a DLQ-on cell. A DLQ-off run where `other_lag_max` is high is not discarded: it means the consumer cannot keep up at this rate and it is a finding of its own, but I would then also run the cells at 1000 req/s before drawing conclusions about the other partitions.
4. Stall time is measured from the 1 s samples of the Kafka tool, so its resolution is a bit over 1 s. Differences smaller than that are not claimed.

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->
