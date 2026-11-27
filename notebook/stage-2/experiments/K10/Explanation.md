# K10 Experiment explanations

Status: PRE-RUN. Written 2026-11-22 before any measured run. The sections after the marker are appended after the experiment.

## Purpose

I want to know what the ingest-api does when Kafka is completely unavailable while clients keep sending: does it keep accepting work until it dies, or does it refuse work early and stay alive. This matters now because Stage 2 puts Kafka between the API and the database, so a Kafka outage is a normal event the API has to survive. Once I know the answer I can decide whether an in-memory queue may ever be left without a limit, and I can explain why every boundary (queue, timeout, circuit breaker) needs its own bound.

## Hypothesis

If all 3 brokers are down during steady load, an unbounded in-memory queue in the API grows until the process runs out of heap and dies, and the clients only ever see timeouts. A bounded queue with a send timeout and a circuit breaker turns the same outage into fast 503 answers with flat memory, and the API goes back to 202 by itself when the brokers return, without a restart. The mechanism: the sender is stuck on a batch nobody acknowledges, so the queue drains at 0 per second while requests arrive at 2,000 per second. The only question is what happens to the surplus.

## Predictions (written before the run)

Settings: 2,000 req/s, 100-byte payloads, brokers killed at t=30 s, started at t=150 s, load ends at t=240 s, Node heap cap 512 MB, client timeout 2 s.

1. **Steady segment (both variants):** almost every answer is 202 (more than 99 %). Memory is flat. Queue depth is close to 0. The variants should look the same here, because the settings differ only in limits that are not reached.
2. **Unbounded, memory:** the queue grows by 2,000 messages per second once the sender is stuck. If one waiting request costs 2 to 8 KB (message, promise, closure, request and response objects; my estimate, not measured), memory grows by 4 to 16 MB/s. From a steady RSS of about 100 to 150 MB (guess) up to the 512 MB cap leaves roughly 360 to 410 MB. 400 MB / 16 MB/s = 25 s and 400 MB / 4 MB/s = 100 s. So I expect the process to die between 25 and 100 s after the kill, that is between t=55 s and t=130 s, inside the 120 s outage. I give this about 75 % confidence; the other 25 % is that a request costs less than 1 KB, the heap never fills (240,000 x 1 KB = 240 MB), and the process survives with a huge queue.
3. **Unbounded, answers:** nobody gets a 503 from the queue (it has no limit). Clients wait until their 2 s timeout, so k6 latency p50 and p99 during the outage are about 2 s and the answers are "other" (status 0). About 500 messages leave the queue as a 503 every 30 s when librdkafka gives up on a batch (default delivery timeout 30 s); that is under 1 % of the traffic and I expect it to be nearly invisible. After the process dies, requests are refused at once (connection refused), also counted as "other", and latency drops to near 0.
4. **Unbounded, recovery:** if the process died, it does not recover by itself: 0 acks after the restart of the brokers and `api_alive=no`. If it survived (prediction 2, 25 % case), it should reconnect and drain the backlog, but those requests are already abandoned by the clients.
5. **Bounded, memory:** flat. The queue cap is 10,000 messages and 64 MB. The health check runs every 2 s and the breaker needs 3 failed sends, so the breaker should open within about 2 to 15 s of the kill. Until then 2,000 req/s x 5 s = 10,000 messages could sit in the queue, so the queue should reach at most about the cap and then drop to 0 when the breaker opens and empties it. RSS should stay within about 50 MB of its steady value.
6. **Bounded, answers:** after the breaker opens almost every request is a fast 503 (about 2,000 per second, p50 under 50 ms). Over the roughly 110 s that the breaker is open this is about 220,000 503s. During the first few seconds some requests wait in the queue and hit the 2 s client timeout (a small number of "other", under about 5,000 per run = 2,000 x about 2.5 s). I expect no process death.
7. **Bounded, recovery:** after the brokers start, the producer reconnects, the health check passes and the breaker closes after its 5 s cooldown with one successful probe. 202 should come back without restarting the API. The time depends on the KRaft election and ISR catch-up (not estimated here, I have no earlier measurement); I expect it to take tens of seconds, not minutes, and I expect `connected=1` at the end.
8. **Generator:** unbounded needs about 2,000 x 2 s = 4,000 VUs while the API hangs; the limit is 6,000, so dropped iterations should be 0. I expect k6 CPUs below 70 %.
9. **What I expect NOT to change:** the Kafka cluster itself (same recovery for both variants), and the steady-state latency (the bounded queue costs one comparison per request).

## Setup (planned)

| Item | Value |
| --- | --- |
| Server | `services/ingest-api` `dist/main.js`, started by `run.sh` per run, `taskset -c 0,6`, `NODE_OPTIONS=--max-old-space-size=512`, acks=-1 idempotent, batch 500 / 5 ms, one batch in flight |
| Variants | `unbounded` (`BATCH_MAX_QUEUE=0`) and `bounded` (10,000 messages, 64 MB, 5 s send timeout, breaker 3 failures / 5 s cooldown) |
| Sysctls | none changed |
| Route / generator | `POST /v1/events`, k6 `constant-arrival-rate` 2,000 req/s, client timeout 2 s, up to 6,000 VUs, pinned to CPUs 3,9,4,10 |
| Cluster | 3 brokers in docker (pinned 1,7,2,8), topic `k10-events` with 6 partitions, RF 3, min ISR 2; all 3 brokers killed at t=30 s and started at t=150 s |
| Matrix | 2 variants x 3 repeats x 240 s, order reversed in repeat 2, about 39 minutes in total, no probe runs |
| Dashboard | `infra/observability/dashboards/stage-2/K10 - Kafka down and backpressure.json` |
| Versions | filled in after the run from env.txt |

## Method (planned)

1. For every run the script restarts all brokers, waits until they are healthy, recreates the topic, starts the API with the variant's settings, sends 100 warm-up requests and waits 10 s. Then the 240 s load starts and the kill and restart happen on the timetable above, with the API left running throughout.
2. While the load runs, a loop reads the API's RSS from `/proc` once per second, so peak memory and the moment of death are recorded even if the process dies. After the run the script records whether the API is alive, the acks counted after the restart, and whether the producer is connected. Cluster health (all brokers healthy, full ISR) is restored before the next run.
3. Every run is judged by k6 dropped iterations (must be 0), k6 CPUs (must stay under about 70 %), and the segment counts from the k6 summary. I read memory and the queue from Prometheus and from the RSS samples.
4. Criteria for discarding a run, decided now: dropped iterations above 0; k6 CPUs above 70 % in the steady segment; a broker that did not come back healthy before the run started; the API not ready at the start. A discarded run is repeated, not edited. A run where the unbounded API dies is a valid result, not a failure.

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->
