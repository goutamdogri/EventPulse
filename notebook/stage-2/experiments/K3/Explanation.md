# K3 Experiment explanations

Status: PRE-RUN. Written 2026-11-22 before any measured run. The sections after the marker are appended after the experiment.

## Purpose

I want to know how the speed of the consumer group depends on the number of partitions and the number of consumers, and
whether consumers beyond the partition count really do nothing. This matters for EventPulse because the sink (Kafka to
Postgres) is the slow half of the pipeline, and the first thing I would try to speed it up is "start more consumers".
After K3 I can say how many consumers are useful for a given partition count, what else (Postgres, CPU) limits the group
first, and how many partitions `events.raw` should have, which I cannot change cheaply later.

## Hypothesis

Throughput of the group grows with the number of consumers only up to the partition count. A partition has exactly one
owner in the group and one consumer works on one partition at a time, so with P partitions at most P consumers can work in
parallel, and extra consumers sit idle with an empty assignment. When N < P each consumer owns several partitions and
works through them one after another, so throughput follows N. In the real sink Postgres has only 2 CPUs of quota, so I
expect it to become the limit before the consumers do; the delayed sink should show the partition ceiling clearly.

## Predictions (written before the run)

Numbers marked (guess) are not derived from anything I measured; the rest follows from arithmetic. Backlog B is about 300,000 records per topic.

Delayed sink sweep (`SINK_PROCESSING_DELAY_MS=200`, consumers N = 1, 3, 6, 12):

1. One consumer handles one batch of up to 1,000 messages per 200 ms delay plus work. If the work is about 50 ms (guess), one consumer does about 1,000 / (0.2 + 0.05) = 4,000 rows/s, so draining 300,000 rows alone takes about 75 s.
2. P = 1: about 4,000 rows/s for every N (1, 3, 6, 12). N = 3, 6, 12 show 0, 2, 11 idle consumers.
3. P = 3: about 4,000 x N for N = 1 and 3 (4,000 and 12,000 rows/s), and still about 12,000 for N = 6 and 12 (3 and 9 idle).
4. P = 6: about 4,000, 12,000, 24,000 for N = 1, 3, 6, and still about 24,000 for N = 12 (6 idle), unless Postgres stops it first. N = 3 gives 2 partitions each.
5. P = 12: about 4,000, 12,000, 24,000 for N = 1, 3, 6 (12 and 6 partitions are shared out as 12/N each, which costs nothing because a consumer reads one partition at a time anyway), and for N = 12 up to 48,000 on paper. I expect N = 12 to fall clearly short of 48,000 because Postgres (2 CPUs) or the 6 shared consumer CPUs become the limit (guess: a ceiling around 25,000 to 35,000 rows/s).
6. The group is as slow as its most loaded consumer. Example: P = 6, N = 4 splits 2, 2, 1, 1; the 1-partition consumers finish after 50,000 / 4,000 = 12.5 s and the 2-partition ones after 100,000 / 4,000 = 25 s, so the whole group gives 300,000 / 25 = 12,000 rows/s, the same as N = 3 (2, 2, 2) and not 16,000. (The pg sweep has N = 4 but is probably limited by the database, so this shows best in the lag-per-partition panel.)

Real Postgres sweep (no delay, consumers N = 1, 2, 3, 4, 6, 8, 12):

7. One consumer: batches of 1,000 rows with an unnest insert (about 50 ms) plus parse and commit give about 10,000 to 20,000 rows/s (guess), so a drain of 300,000 rows takes 15 to 30 s.
8. The curve rises for N = 2 and 3 and then flattens because Postgres is limited to 2 CPUs (guess: a ceiling of 20,000 to 35,000 rows/s, reached with 2 to 4 consumers). After the knee, more consumers or more partitions add nothing.
9. P = 1 stays at the single-consumer rate for every N; the idle count is N - 1. P = 3 stays flat from N = 3 on at the latest.
10. Postgres CPU (cores 1,7,2,8, quota 2) is near its limit at the plateau and rows per second stops growing while the database write time p99 grows with N.

What I do not expect to change: the total rows (backlog) written, the duplicates and DLQ (both 0), the per-consumer share of partitions with `roundRobin` (counts differ by at most 1).

Startup and rebalances:

11. Every consumer gets at least one assignment, so the sum of `ep_consumer_rebalances_total` is at least N. The first assignment comes about 3 s after the consumers start (the initial rebalance delay of a new group) plus the time Node needs to boot. I expect a few more rebalances for N = 12 because 12 processes do not all join inside the initial delay.

Resources:

12. The six consumer CPUs (3 physical cores) stay below about 70% busy while N <= 6 in the pg sweep, because a consumer mostly waits for Postgres. In the delay sweep they are mostly idle (the delay is a sleep). For N = 12 in the pg sweep I am not sure and will check the event loop lag.

## Setup (planned)

| Item | Value |
| ---- | ----- |
| Server | `ingest-api` only for the prefill (`node dist/main.js`, K2 baseline settings, pinned to CPUs 0,6); `consumer-sink` N processes per drain (`node dist/main.js`, pinned to 0,6,3,9,4,10 together), roundRobin, 1 partition at a time, 1,000-message batches, 1,000-row chunks |
| Sysctls | none changed |
| Route / generator | prefill: `loadtests/k6/stage-2/ingest-kafka.js`, `constant-arrival-rate` 4,000 req/s for 75 s, pinned to 3,9,4,10 by `K6_CPUS`; no k6 during the drains |
| Matrix | P = 1, 3, 6, 12; pg sweep N = 1 2 3 4 6 8 12 (28 cells), delay sweep N = 1 3 6 12 with 200 ms (16 cells); 3 repeats; backlog about 300,000 records of about 500 B; no probe runs; about 2.7 h in total |
| Dashboard | `infra/observability/dashboards/stage-2/K3 - Partitions versus consumers.json` |
| Versions | filled in after the run from env.txt |

CPU plan of the drain: consumers 0,6,3,9,4,10 (all share them), Postgres 1,7,2,8 (quota 2 CPUs), brokers 3,9,4,10, monitoring 5,11.
Because the consumers and brokers share CPUs 3,9,4,10 this is a limitation of the setup; the brokers only serve fetches in the drains.

## Method (planned)

1. Prefill one topic per partition count through the real ingest-api, so every record is a valid event. Every event id starts with the run id of its topic.
2. For every cell: delete the topic's rows in Postgres, vacuum, start N consumers at once with a new group id, read the group from the beginning, and sample the consumers' metrics every 0.5 s until messages consumed equal the records in the topic.
3. The headline number is `rate_window`: rows inserted from the moment the group is stable (all consumers answer, the group owns all P partitions, no rebalance for 1 s) to the end of the drain, divided by that time. `rate_all` (from the first row) is kept next to it.
4. Repeats are alternated: repeat 1 in table order, repeat 2 in reverse order, repeat 3 in table order. Report the median and the range over the 3 repeats.
5. A run is discarded and repeated if any of these is true, decided now: the audit is not `ok` (rows in Postgres differ from the records in the topic, or duplicates or DLQ above 0); the status is `TIMEOUT` or `FAILED`; `window_s` is below 5 s (too short to read a rate); `late_rebalances` is above 0 (the group changed during the window); the event loop lag p99 of any consumer is above 100 ms (the generator of the work, a consumer, was CPU-saturated, which is then reported as a finding, not as the partition effect); the Postgres connections are exhausted (waiting above 0).
6. A cell with `pre_stable_pct` above 20 keeps its `rate_window` but its `rate_all` is not quoted.

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->
