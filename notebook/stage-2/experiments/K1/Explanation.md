# K1 Experiment explanations

Status: PRE-RUN. Written 2026-11-22 before any measured run. The sections after the marker are appended after the experiment.

## Purpose

I want to know where the ingest knee is now that the API writes to Kafka instead of waiting for Postgres, and what
limits it. The Stage 1 baseline (B1) was about 1,860 req/s with everything else the same. Once I know the new knee and
the raw ceiling of my three brokers, I can say how much of the gap between them is my own API code, and decide where
Stage 2 optimisation effort should go (the producer settings in K2, the batcher, or the Node process).

## Hypothesis

The stage document says: the knee moves up by roughly an order of magnitude on ingest, and the new limit is API CPU or
the load generator, not Kafka. My reasoning: the slow part in Stage 1 was the database commit inside the request. In
Stage 2 that wait is a batched append to Kafka that many requests share, so the per-request cost left is the Node
work (HTTP, validation, JSON, metrics), and that runs on one thread.

## Predictions (written before the run)

Baseline for comparison, from B1: capacity about 1,860 req/s (every offered rate from 1,950 up was handled at 1,845 to
1,874). An order of magnitude would be about 18,600 req/s.

1. **The knee will be lower than 10x.** I predict about 8,000 req/s (range 6,000 to 12,000), so 3x to 6x Stage 1.
   Maths: the Node main thread does HTTP parse, API key check, class-validator, building the envelope, JSON stringify
   and metrics for each request. I guess 100 to 150 microseconds per request. One thread saturates at
   1 / 0.000125 s = 8,000 req/s (6,700 at 150 us, 10,000 at 100 us). The SMT sibling (CPU 6) is shared with the Kafka
   client threads, so I do not expect it to double the capacity.
2. **The limit will be Node CPU.** At the knee the Node process CPU should be about 1.0 to 1.5 (main thread full, helper
   threads partly busy), the event loop lag p99 should rise to tens of ms, and the server CPUs (0 and 6) should be the
   busiest group. This agrees with the stage document's hypothesis (API CPU) on the cause, but not on the size.
3. **The generator is a risk, not a sure limit.** k6 has 4 logical CPUs (2 cores). I guess one k6 request costs about
   150 to 250 microseconds of generator CPU, which gives 4 / 0.0002 = 20,000 req/s at 100% on all 4 CPUs, about
   14,000 req/s at 70%. So I expect k6 to be at about 50% to 60% at 8,000 req/s, under my 70% limit, and to become the
   limit only above about 12,000 req/s.
4. **Latency below the knee will be higher than in Stage 1.** Stage 1 had p50 of about 3 to 5 ms. Now every request
   waits for its batch: up to 5 ms of batch window plus the acks=all round trip to the brokers, which I guess is 3 to
   5 ms. So p50 about 8 to 12 ms and p99 about 15 to 40 ms at 1,000 to 5,000 req/s. Throughput goes up, latency at low
   load goes up too.
5. **Batch size follows the rate.** Messages per batch is about rate x (wait + send time) = 8,000 x 0.008 s = 64
   messages at 8,000 req/s, so about 60 (range 30 to 100) at the knee and below the 500 cap. Batches will mostly be
   flushed by time, not by size.
6. **Kafka will not be the limit.** Raw ceiling with `acks=all`, replication factor 3, 500 B records, 3 brokers with 1
   CPU quota each: I predict about 100,000 records/s (range 60,000 to 200,000), about 50 MB/s from the producer.
   Maths: every record is written 3 times in the cluster, so 100,000 x 500 B x 3 = 150 MB/s of broker log writes.
   One broker CPU can handle roughly 50 MB/s of produce plus replication, so 3 CPUs give about 150 MB/s of cluster
   work. At the API knee (8,000 req/s x 500 B = 4 MB/s) the brokers should be at about 8% of their ceiling, and the
   broker cores should be well below 30% busy.
7. **The gap.** API capacity / raw ceiling = 8,000 / 100,000 = 8%, so the API will sit about 12x below the raw Kafka
   ceiling. That gap is my code's overhead.
8. **What I expect NOT to change:** the audit must show zero lost records (202 count equals records added to the
   topic), no 503 before the knee, and Kafka send time (p99) should stay flat across rates until near the knee.

## Setup (planned)

| Item              | Value                                                                                                                                                       |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Server            | `services/ingest-api` (NestJS on Express, `@confluentinc/kafka-javascript` client), `node --env-file=.env dist/main.js`, pinned to CPUs 0,6, restarted before every run. `KAFKA_KEY_MODE=event`, all other Kafka and batcher settings at the API defaults (`acks=-1`, idempotent, no compression, 5 ms / 500 messages / 900 KB batches, 1 batch in flight) |
| Brokers           | 3 x `apache/kafka:4.3.2` in KRaft mode, 1 CPU quota and 1 GiB each (512 MiB heap), pinned to CPUs 1,7,2,8 for K1, RF 3, `min.insync.replicas=2`            |
| Topic             | `events.raw`, 6 partitions, RF 3, deleted and recreated before every run                                                                                    |
| Sysctls           | none changed                                                                                                                                                |
| Route / generator | `POST /v1/events`, about 500 B per event, unique `event_id`; k6 `constant-arrival-rate`, pinned to CPUs 3,9,4,10, 500 pre-allocated and 3,000 max VUs        |
| Monitoring        | Prometheus, Grafana, node_exporter and the idle Postgres container pinned to CPUs 5,11. consumer-sink stopped                                              |
| Raw ceiling       | `kafka-producer-perf-test.sh` from the broker image in a throw-away container pinned to 3,9,4,10, topic `k1-perf` (6 partitions, RF 3, min ISR 2), `acks=all`, idempotence on, 500 B records, 2,000,000 records, two batch sizes (256 KiB and 32 KiB), 3 repeats |
| Matrix            | coarse: 8 rates (1,000 to 30,000) x 1 repeat x 30 s, about 12 min. Fine: 9 rates around the coarse knee x 3 repeats x 60 s + 2 probe runs of 30 s, about 55 min. Ceiling: 2 profiles x 3 repeats + 1 warm-up, about 14 min |
| Dashboard         | `infra/observability/dashboards/stage-2/K1 - Throughput vs Stage 1.json`                                                                                    |
| Versions          | filled in after the run from env.txt                                                                                                                        |

## Method (planned)

1. Run `./run.sh ceiling` first, with nothing else running. This gives the raw broker ceiling for the same record size and the same topic shape.
2. Run `./run.sh coarse` to find roughly where the knee is. It is one repeat per rate and is a search pass, not a headline result.
3. Run `./run.sh fine`: nine rates around the coarse knee, three repeats each, repeat as the outer loop so drift hits every rate equally. For every run the server is restarted, the topic is recreated, and 100 warm-up requests are sent and not counted.
4. Two probe runs (about 70% of the knee and the knee) run with the heavy tools (per-thread CPU of the Node process, `docker stats` of the brokers, iostat). They are reported separately and never used for headline numbers.
5. Judge every run with the audit (202 answers equal records added to the topic), dropped iterations, errors and the generator CPU. Report median and range over the three repeats.
6. Discard a run if: dropped iterations > 0 while the generator cores are above 70% busy (then it measured k6); the producer disconnected or the breaker opened; a probe tool ran inside the window; or the audit shows a mismatch I cannot explain. A run with dropped iterations while the generator is under 70% is kept, because then the server being too slow is the finding. Decided now so I cannot bend the rules after seeing data.
7. Compare the median capacity with the B1 value (about 1,860 req/s) and with the raw ceiling, and say which resource was at about 100% at the knee.

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->
