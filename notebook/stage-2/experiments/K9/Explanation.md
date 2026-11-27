# K9 Experiment explanations

Status: PRE-RUN. Written 2026-11-22 before any measured run. The sections after the marker are appended after the experiment.

## Purpose

I want to know how much throughput I lose when one tenant is much busier than the others and the Kafka key is `tenant_id`, and what I give up (per-tenant order) if I change the key to `event_id` to spread the load. EventPulse keys by tenant on purpose so that one tenant's events stay in order, so I need to see the cost of that choice with real numbers before Stage 6. When I have the answer I can say what skew the current design tolerates and what the key change buys and takes away.

## Hypothesis

With `KAFKA_KEY_MODE=tenant` and one tenant sending 80 % of the events, that tenant's partition gets about 80 % of the records. When that partition's input rate is above what its consumer can process, its lag grows while the other partitions idle, so the group's throughput stops at the ceiling of one partition divided by the hot share, even though the other consumers have capacity. With `KAFKA_KEY_MODE=event` the records spread evenly over the six partitions, the group follows the offered rate, and the lag stays near zero. The price is that events of one tenant are no longer stored in the order they arrived.

## Predictions (written before the run)

The consumer ceiling per partition: a chunk is up to 100 rows and takes 60 ms of artificial work plus the INSERT (I guess 5 to 15 ms, not measured). So ceiling C about 100 / (0.060 + 0.010) = 1,430 events/s per partition, range about 1,250 to 1,600 (the hard upper limit is 100 / 0.060 = 1,667).

1. **Skew in the logs.** Tenant mode: the busiest partition holds about 80 % of the bytes (80 to 85 % if another tenant hashes to the same partition: each other tenant is 20 % / 7 = 2.9 %). Event mode: each partition about 100 / 6 = 16.7 % (all between 15 and 18.5 %, random hashing of about 60,000 to 240,000 events per run).
2. **Ceiling.** In tenant mode the hot partition gets 0.8 x R. It stays below C for R = 1,000 (800) and R = 1,500 (1,200, close to the lower end of my range for C, so this cell is the one I am least sure about). It goes above C for R = 2,500 (2,000) and R = 4,000 (3,200). So the group's throughput ceiling in tenant mode is C / 0.8 = 1,430 / 0.8 = about 1,800 req/s (1,560 to 2,000).
3. **Consumed rate during the load, tenant mode.** At R = 2,500: hot partition at C plus the others at 0.2 x 2,500 = 500, so about 1,430 + 500 = 1,930 events/s, 77 % of the offered rate. At R = 4,000: 1,430 + 800 = 2,230 events/s, 56 %.
4. **Lag, tenant mode.** At R = 2,500 the hot partition lag grows at 2,000 - 1,430 = 570 events/s, so about 570 x 60 = 34,000 at the end of the load, and it drains in about 34,000 / 1,430 = 24 s (before the other partitions' leftover). At R = 4,000: 3,200 - 1,430 = 1,770/s, about 106,000 at load end, draining in about 106,000 / 1,430 = 74 s. The other five partitions stay under a few hundred. At R = 1,000 and 1,500 the lag stays close to 0 (R = 1,500 may show a small climb if C is below 1,200).
5. **Event mode.** Each partition gets R / 6: 167, 250, 417, 667 events/s, all well below C = 1,430. So the lag stays near 0, the consumed rate equals the offered rate in every cell, and the drain after the load takes only a few seconds. Event mode would need R = 6 x 1,430 = 8,600 req/s to reach the same ceiling, which is above what the API handles (about 7,500 req/s from K1), so I will not see it.
6. **What does not change.** The API's accepted rate, 503s and request latency are the same in both modes (the API only waits for Kafka, not for the sinks), and every acknowledged event ends up in Postgres in both modes (missing = 0). The measured hot share of the requests is 80 % (78 to 82 %).
7. **Ordering lost.** Tenant mode: events of a tenant are in one partition and read in order, so `overtaken` is about 0 (under 0.1 % of the pairs). Event mode: two consecutive events of the hot tenant are in different partitions with probability 5/6, and then the later one is stored first about half of the time (chunk start times of two partitions are independent), so about 5/6 x 0.5 = 0.4. I predict 30 to 45 % of the consecutive pairs of a tenant are overtaken in event mode, at every rate.

## Setup (planned)

| Item              | Value                                                                                                                                                              |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Server            | ingest-api started by `run.sh` (`node dist/main.js`), acks=all, idempotent, `KAFKA_KEY_MODE` = the variable, 8 API keys (one tenant each), pinned to cpu 0,6      |
| Consumers         | 3 consumer-sinks in one group (roundRobin, 2 partitions each), 2 partitions in parallel per consumer, chunks of 100 rows, 60 ms artificial work per chunk, cpu 1,7,1 |
| Topic             | `k9-events`, 6 partitions, RF 3, min ISR 2, recreated every run                                                                                                    |
| Sysctls           | none changed                                                                                                                                                       |
| Route / generator | `POST /v1/events`, k6 `constant-arrival-rate`, 60 s, 80 % of the requests with the key of tenant `k9-hot`, pinned to cpu 4,10                                      |
| Matrix            | key mode (tenant, event) x rate (1,000, 1,500, 2,500, 4,000 req/s) x 3 repeats, 60 s, repeat 2 in reverse order, no probe runs, about 65 minutes                   |
| Dashboard         | [K9 - Hot key and skew.json](../../../../infra/observability/dashboards/stage-2/K9%20-%20Hot%20key%20and%20skew.json)                                              |
| Versions          | filled in after the run from env.txt                                                                                                                               |

## Method (planned)

1. For every run: recreate the topic, start the API with the key mode of the cell, start 3 consumers and wait until they own the 6 partitions, run k6 for 60 s, capture the lag and the bytes of each partition right after the load, wait until the lag is 0, then audit Postgres (rows, distinct ids, rows of the hot tenant, overtaken events). All runs are measured; there are no probe runs because nothing heavy runs during the load.
2. Cells are alternated (tenant and event next to each other at each rate) and repeat 2 runs them in reverse order, so drift hits both modes the same way. I report the median and the range of the 3 repeats.
3. A run is discarded before I look at the result if any of these is true: k6 dropped iterations above 0; any 503 or other non-202 answer; the partitions owned by the group are not 6 at any time of the load, or a rebalance happens during the load; the drain times out; acknowledged and distinct rows differ (missing is not 0); the measured hot share is outside 78 to 82 %; the k6 CPUs are above 70 % busy. A run with the sink CPUs above 70 % busy is kept but not used to say anything about the key, because then the CPU, not the delay, is the ceiling.

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->
