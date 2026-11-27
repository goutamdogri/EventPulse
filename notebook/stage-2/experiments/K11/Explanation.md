# K11 Experiment explanations

Status: PRE-RUN. Written 2026-11-22 before any measured run. The sections after the marker are appended after the experiment.

## Purpose

I want to know what happens to events when the consumer is down for longer than the topic's retention time, and what the consumer's reset policy (`earliest` or `latest`) does to the data after it comes back. This matters now because Stage 2 puts a retained log between the API and Postgres: the API says 202 when Kafka has the event, and from then on the event survives only as long as retention and a working consumer allow. Once I know the answer I can explain why the lag alert has to fire long before the retention time is reached, and I can choose a reset policy for the sink on purpose and not by default.

## Hypothesis

If the consumer is stopped for longer than retention, the broker deletes the records the consumer has not read yet. The consumer's committed offset then points before the log start, and the client applies its reset policy without any error reaching the application: `earliest` continues at the log start and recovers what is still in the topic, `latest` continues at the log end and skips that too. In both cases the rows in Postgres are fewer than the events the API acknowledged, and nothing at the API or in the consumer's error counters shows it. The mechanism: retention looks at the age of a record, not at whether it was read, and the group only stores a number.

## Predictions (written before the run)

Settings: main load 1,000 req/s for 60 s = 60,000 events, then a trickle of 20 req/s until the restart. Consumer #1 stopped at t=15 s. Outage 660 s (restart at t=675 s) for earliest and latest, 60 s (restart at t=75 s) for the control. Retention 180 s, segment.ms 20 s, broker retention check every 300 s.

Counts used below:

- Trickle in the long runs: 20 req/s x (675 - 60) s = 12,300 events. Acknowledged in total: 60,000 + 12,300 = 72,300. (Control: 60,000 + 20 x 15 = 60,300.)
- Read by consumer #1 before the stop: about 15 s x 1,000 req/s = 15,000 events (it should keep up with 1,000 req/s; not measured).
- Unread at the restart: 72,300 - 15,000 = about 57,300.
- The broker deletes at its check. The last check before t=675 s happens at a time c between about 375 s and 675 s (I do not know the phase of the 5 min cycle). At the check, records older than 180 s are deleted, that is up to the write time c - 180 s, between 195 s and 495 s. The whole main load (up to t=60 s) is therefore deleted, and the trickle up to c - 180 s: 20 x (c - 180 - 60) = between 2,700 and 8,700 events (plus or minus about 400 for the segment size of 20 s x 20 req/s).

Predictions:

1. **Control (outage shorter than retention):** nothing is deleted. Rows = acked = about 60,300, `missing` = 0, the lag returns to 0, `deleted_unread` = 0.
2. **Deleted before reading (earliest and latest):** `deleted_unread` = (60,000 - 15,000) + (2,700 to 8,700) = about 47,700 to 53,700 records, so 66 % to 74 % of the 72,300 acknowledged events are gone before anybody reads them. The log start offset of every partition is past the committed offset at the restart. I expect this to hold at t=675 s without extra waiting in most runs, because the oldest segment I need deleted is the one that holds the committed offset (written at about t=15 to 35 s, deletable at about t=215 s, deleted at the first check after that, at the latest about 515 s).
3. **earliest:** the consumer continues at the log start and reads what is left: `retained_unread` = 12,300 - (2,700 to 8,700) = about 3,600 to 9,600 records. Rows = about 15,000 + (3,600 to 9,600) = 18,600 to 24,600; `missing` = about 47,700 to 53,700, equal to `deleted_unread` (+/- 5 % for the timing of the check between my reading of the offsets and the restart). The group lag goes to 0 after the restart. The backlog is small (at most about 10,000 records), so I expect it to be read in under 10 s.
4. **latest:** the consumer jumps to the log end and reads nothing (the trickle ends at the restart). Rows = about 15,000 (plus a few records that arrive after the restart); `missing` = about 57,300 = `deleted_unread` + `retained_unread`. That is 79 % of the acknowledged events, 3,600 to 9,600 more than `earliest`. I am not sure about the lag after the restart: I expect it to stay at about 57,300 because nothing is committed after the jump (about 50 % confident).
5. **Silent:** the API answers 202 to every request (more than 99.9 %) in all runs, its latency does not change when the consumer stops or comes back, and the consumer's error, DLQ and commit-error counters stay at 0 in every variant. I expect no crash of the consumer. Whether the consumer prints a line about the out-of-range offset in its own output I do not know (the `oor_log_lines` column answers it).
6. **Lag as seen by the alert:** at the restart the lag (end minus committed) is about 57,300 while the readable records (end minus log start) are only 3,600 to 9,600. The lag looks about 6 to 16 times bigger than what I could still recover. A lag alert that waits for the lag to reach a "big" value would fire late; the number that matters is how old the oldest unread record is compared with retention (here: 15 s after the write the consumer stopped, so the oldest unread record is 660 s old against 180 s of retention).
7. **Deletion is stepwise:** the log start offset does not move smoothly. It stays flat until the first broker check after the first expired segment (somewhere between t=215 s and t=515 s) and then jumps. Between the time a record passes 180 s of age and the time it is deleted there are up to 300 s.
8. **Generator and cluster:** dropped iterations 0, k6 CPUs below 70 %, no under-replicated partitions (no broker is touched).
9. **What I expect NOT to change:** the API, the cluster, and the first 15 s (all variants start the same way: both policies read the first 15,000 events normally).

## Setup (planned)

| Item | Value |
| --- | --- |
| Server | ingest-api (`dist/main.js`, started by `run.sh`), `taskset -c 0,6`, acks=-1, idempotent, `KAFKA_KEY_MODE=event` |
| Consumer | consumer-sink (`dist/main.js`, started by `run.sh`), `taskset -c 1,7`, 6 partitions with 1 consumer, group id new per run, same for #1 and #2 |
| Kafka | 3 brokers in containers on CPUs 2,8,3,9; topic `k11-events`: 6 partitions, RF 3, min ISR 2, `retention.ms=180000`, `segment.ms=20000`; broker `log.retention.check.interval.ms` read into `env.txt` |
| Route / generator | `POST /v1/events`, k6 `constant-arrival-rate`: `main` 1,000 req/s for 60 s, `trickle` 20 req/s until the restart, pinned to CPUs 4,10 |
| Variable | reset policy: earliest / latest (outage 660 s) and a control (earliest, outage 60 s) |
| Matrix | 3 variants x 3 repeats = 9 runs; the long runs take about 13 min, the control about 4 min; about 90 to 105 min in total |
| Sysctls | none changed |
| Dashboard | [K11 - Retention and lag.json](../../../../infra/observability/dashboards/stage-2/K11%20-%20Retention%20and%20lag.json) |
| Versions | filled in after the run from env.txt |

## Method (planned)

1. For each run: recreate the topic with short retention, start the API and consumer #1 (earliest, new group), start k6. At t=15 s stop consumer #1 and write down the committed offsets. Keep the load going (main stream, then the trickle).
2. At t=15 s + outage: for earliest and latest wait until the log start of every partition is past the committed offset (at most 420 s extra), then write down log start, log end and committed offset per partition. Start consumer #2 with the same group id and the reset policy of the variant. Wait until the lag is 0 or nothing new was consumed for 20 s.
3. Audit: acknowledged events (k6 202 answers) against distinct rows in Postgres with the run's id prefix. Compare `missing` with `deleted_unread` and `retained_unread` from the offsets.
4. Measured runs are all 9 runs. The 5 minute broker check is the main source of variation between runs, so I report the range and the offsets of every run, not only the median.
5. Criteria for discarding a run, decided now: dropped iterations > 0; any answer that is not 202 (the audit would not be clean); k6 CPUs above about 70 %; any under-replicated partition; status `NOT_DELETED` (an earliest or latest run in which the broker did not delete in the extra wait time is not a longer-than-retention run); status `NO_COMMIT` (a partition had no committed offset when consumer #1 stopped); consumer #2 died. A discarded run is repeated and listed in Explanation.md after the run.

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->
