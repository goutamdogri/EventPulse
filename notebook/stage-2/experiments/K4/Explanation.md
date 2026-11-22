# K4 Experiment explanations

Status: PRE-RUN. Written 2026-11-22 before any measured run. The sections after the marker are appended after the experiment.

## Purpose

I want to know what the Stage 2 durability settings (RF 3, min.insync.replicas 2, acks=all) really protect when a broker dies while events are arriving, and what they cost in availability. EventPulse tells a client "202" only when the event is safe, so I need to know whether that promise holds during a failure and how long the API refuses writes. Once I know this I can say what an acknowledged event is worth, and decide how the API should behave (503 or accept) while Kafka is damaged.

## Hypothesis

With RF=3, min ISR 2 and acks=all, killing one broker causes a short burst of errors and latency (the partitions it led have no leader until the controller notices and elects another), then the writes recover, and no acknowledged event is lost because every acknowledged event is on at least two brokers. Killing a second broker makes writes fail with 503 and does not lose acknowledged data silently, because the cluster refuses writes instead of accepting them on one copy. With acks=1 I expect the same burst but some acknowledged events to be lost, because the leader can answer before any follower has the record.

## Predictions (written before the run)

Setup numbers: 2000 req/s for 210 s = 420,000 requests per run. 6 partitions on 3 brokers, so about 2 partitions per leader (1/3 of the partitions per broker). Timetable: kill A at t=30, kill B at t=90, start both at t=135.

1. **acks=all, one broker down (t=30 to 90).** A burst of 503 lasting about 8 to 15 s. I think the time is set by the controller's failure detection (I believe 9 s default session timeout, plus the producer's metadata refresh), not by the election itself. 2000 req/s x 8 to 15 s = 16,000 to 30,000 refused requests, which is 4% to 7% of the run. Which kind of 503: fast ones from the breaker once the health check (every 2 s) sees a partition without a leader, and slow ones (about 5 s latency, the send timeout) before that. After the burst the 202 rate returns to 2000/s and stays there while one broker is gone, because the ISR is 2, which is the min ISR. ISR of the partitions that had a copy on the dead broker falls from 3 to 2.
2. **acks=all, two brokers down (t=90 to 135).** Close to 100% of the requests are not accepted for the whole 45 s: 45 s x 2000 = about 90,000 requests. The reason is the lost controller quorum (1 of 3 controllers alive), not only the ISR: no leader can be elected and the ISR cannot be changed. I expect the 503s to come from the send timeout (latency about 5 s) at least at first, and that k6 needs up to 2000 x 5 = 10,000 virtual users at once. Zero 202 events that are not stored later.
3. **acks=all, recovery (t=135 to 210).** The first broker takes some seconds to start and the second one is needed for the quorum. I expect 202 to return 15 to 45 s after the start, so another 30,000 to 90,000 refused requests, and the ISR of all 6 partitions to be back at 3 within about 60 s after the start. Total 503 for a run of this variant: roughly 140,000 to 210,000 of 420,000 (33% to 50%).
4. **acks=all audit:** every 202 is found in Postgres (LOST 0). A few events may be present although the client got a 503 (the batch was written, but the send timed out), so "committed-not-acked" can be above 0 (I expect tens to a few thousand, at the kills).
5. **acks=1, one broker down.** Fewer errors than acks=all: the partitions led by the dead broker are unavailable for the same 8 to 15 s, but the other 4 of 6 partitions keep accepting writes without waiting for followers. I cannot predict the share of 503 per request, because I do not know if one batch fails as a whole when one of its partitions fails; my range is 1/3 to 100% of the requests during those seconds. After the election the writes run at full rate, with an ISR of 2 on some partitions, which `acks=1` does not care about (the health check also asks for only 1).
6. **acks=1, two brokers down.** Not a clean 100% of 503: the partitions whose leader is the surviving broker should still accept writes (their ISR cannot be shrunk, but acks=1 does not wait for it). I expect between 30% and 70% of the requests to still get 202 as long as the batches are split per partition, and nearly 0% if a batch with one failing partition fails as a whole. So in this segment acks=1 is more available than acks=all.
7. **acks=1 audit:** LOST above 0 is possible but small. The leader of a partition appends records and the followers copy them a few milliseconds later on loopback. About 2000 req/s / 6 = 333 events/s per partition, so about 1 to 3 events per partition sit unreplicated at any moment. Killing 2 leaders, then 1 more leader, loses of the order of 0 to 20 acknowledged events per run, out of 420,000 (under 0.01%). So some runs may show LOST 0, and then the loss is real but not demonstrated by that run. I expect no run to lose more than a few hundred.
8. **What I expect NOT to change:** the steady segment (t=0 to 30) is all 202 for both variants, with identical p50 and p99; the broker CPU, API CPU and event-loop lag stay low; no dropped iterations before the first kill.

## Setup (planned)

| Item | Value |
|---|---|
| Server | `services/ingest-api` `dist/main.js`, started by `run.sh` under `taskset -c 0,6`; settings written out in `run.sh` (acks per variant; batcher 500 messages / 5 ms; compression none; key mode event; topic `k4-events`) |
| Cluster | 3 brokers in KRaft mode, apache/kafka 4.3.2, 1 CPU and 1 GiB each, cores 1,7,2,8 during the run; RF 3, min.insync.replicas 2, unclean leader election off |
| Route / generator | `POST /v1/events`, k6 `constant-arrival-rate` 2000 req/s for 210 s, pinned to cores 3,9,4,10; up to 12,000 VUs; request timeout 15 s |
| Kills | `docker kill` (SIGKILL) of the two brokers that are not the active controller, at t=30 s and t=90 s; both started at t=135 s |
| Matrix | 2 variants (acks=all, acks=1) x 3 repeats, 210 s each, alternated (repeat 2 reversed); about 39 min in total; no probe runs |
| Audit | after each run the topic is drained into Postgres by 3 consumer-sinks and the distinct event ids of the run are counted against the 202 answers |
| Observability | k6 to Prometheus; ingest-api metrics; kafka-exporter (leader, ISR, brokers); `ep_k4_phase` textfile |
| Dashboard | [K4 - Kill a broker under load.json](../../../../infra/observability/dashboards/stage-2/K4%20-%20Kill%20a%20broker%20under%20load.json) |
| Versions | filled in after the run from env.txt |

## Method (planned)

1. For every run: all brokers started and healthy, the topic created again with a full ISR, the rows of the run id deleted from Postgres, a fresh API process, 100 warm-up requests, 10 s settle. Then 210 s of load with the kills on the timetable. Then wait for a full ISR on all partitions, stop the API, drain the topic into Postgres and count.
2. Measured: the 6 runs. Per run I take: 202 / 503 / other per segment (k6), the leaders and the ISR at 6 moments, the API's 503 reasons and send error types, the audit result (the number of 202 against the distinct rows in Postgres), and the time until the ISR is full again.
3. Which broker was killed and which one was the controller is recorded per run.
4. A run is discarded if: dropped iterations above 0 in the steady segment (generator problem), the generator cores above about 70% busy in the steady segment, the first kill happens before the load has started, a broker fails to start again, or the audit drain times out. A discarded run is repeated, not averaged in. Dropped iterations during the two-down segment are not a reason to discard: they are part of what I measure.
5. Results are reported as the median and range of the 3 repeats per variant. A trial run (`RESULTS_NAME=k4-trial`) is plumbing only and is never quoted.

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->
