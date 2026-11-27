# K12 Experiment explanations

Status: PRE-RUN. Written 2026-11-22 before any measured run. The sections after the marker are appended after the experiment.

## Purpose

I want to know what an event that is too big for the system does at each layer on its way to Kafka: which layer catches it, what error the API client gets, whether the failure is loud or silent, and whether Kafka ever sees it. This matters now because Stage 2 puts a broker with its own limits behind the API, and the limits live in four different places (edge, batcher, Kafka client, broker). Once I know the answer I can explain why the size limit belongs at the edge, below every other limit, and decide the order the limits must have.

## Hypothesis

If an event is above a limit, it fails at the first layer whose limit it exceeds, and the failure looks different at every layer: a clean 413 at the edge and at the batcher, a Kafka error type (probably turned into a 500, because the API does not recognise it as "Kafka unavailable") at the client and at the broker. With the edge set below the others, no oversized event reaches Kafka. If the edge is opened, the next layer catches the event, and if the producer does not wait for an answer (`acks=0`), the broker's refusal can be silent. In a batch, one refused message takes its neighbours with it. The mechanism: each layer only checks its own number, and the API sends a whole batch as one call.

## Predictions (written before the run)

Settings: 5 req/s for 10 s = 50 requests per cell. T = length of the tenant name in `.env` (not known yet). Wire size = body + 36 + 96 + 15 + T = body + 147 + T. Edge limit `64kb` = 65,536 bytes. Raised edge `4mb` = 4,194,304 bytes. Batcher cap 900,000 (1,000,000 in `client`). Topic limit in `broker` and `acks0`: 200,000. Broker default 1,048,588.

1. **`edge` variant:** body 1,000, 32,768 and 65,000 (65,000 < 65,536): 50 x 202, topic grows by 50. Body 66,000 (> 65,536) and every size above it: 50 x 413 from the body-parser (answer type `entity.too.large`), topic grows by 0, `batcher_too_large` = 0, no Kafka send errors, API route label `unmatched`. The 413 answers are fast (under 20 ms for sizes up to 600,000). For the 2,000,000 and 5,000,000 bytes bodies I expect most answers to be 413, but I do not know whether k6 gets the 413 or a reset when the server answers while k6 is still sending (about 55 % sure that 413 is at least 90 % of them; the rest would be status 0).
2. **`batcher` variant:** body 1,000 to 880,000: 50 x 202 (880,000 + 147 + T < 900,000, so it passes the batcher; the record batch of about 880 KB is below the broker's 1,048,588). Body 950,000 (wire = 950,147 + T > 900,000) and 2,000,000: 50 x 413 with the message "Message of N bytes is larger than the batch byte limit (900000)", `batcher_too_large` = 50, topic grows by 0, no send errors. Body 5,000,000 (> 4,194,304): 413 at the edge, `batcher_too_large` = 0.
3. **`broker` variant (topic limit 200,000):** body 1,000 to 180,000 (180,000 + 147 + T < 200,000): 50 x 202. Body 262,144, 600,000 and 880,000: pass the edge, the batcher and the client (all above 262,144 but below 1,000,000), the broker refuses. Prediction: 50 x 500 (not 202, not 413), a send error type about "message size too large", `api_failed` = 50, topic grows by 0. I am about 75 % sure of the 500: the error code of this refusal is not in the API's list of "Kafka unavailable" codes, so I expect it to fall through to the unhandled branch. If it is a 503 instead, the API would tell clients to retry something that can never succeed. Body 950,000 and 2,000,000: 413 from the batcher; 5,000,000: 413 from the edge, as in prediction 2. The failing answers (500) are slower than the 413 answers, because they wait for the broker (tens of ms against under 20 ms).
4. **Mix cell (`broker`):** 1,000 req/s for 10 s, every 100th request 400,000 bytes (above the topic limit of 200,000). That is about 9,900 small and 100 big events. All 100 big events fail (500). Small events fail too when they are in the same batch as a big one. With maxWait 5 ms and one batch in flight, I estimate 3 to 10 events per batch at 1,000 req/s, so 2 to 9 small neighbours per big event: about 200 to 900 small events fail (2 % to 9 % of the small ones). I am not sure about the batch size. Not all of those neighbours are lost: the ones that were on other partitions than the big event may have been appended, so the topic should grow by more than the number of 202 answers, by roughly 5/6 of the failed neighbours (6 partitions): about 170 to 750 events reported as failed but stored. About 50 % confident on this last number.
5. **`client` variant (batcher cap 1,000,000 = client limit 1,000,000):** body 880,000 and wire 990,000: 50 x 202. Wire 1,000,100 (> 1,000,000, the batcher refuses with `>`): 50 x 413 from the batcher. Wire 1,000,000: passes the batcher (not strictly greater) but the client adds its own overhead per message, so I expect the client to refuse it: 50 x 500 with a "message size too large" error type, topic grows by 0 (about 70 % sure). Wire 999,900: 100 bytes under the limit; I expect the overhead of the client to be below 100 bytes, so 202 (about 55 % sure). The point where 202 turns into an error lies between 999,900 and 1,000,000.
6. **`acks0` variant:** body 65,000 and 180,000: 50 x 202, topic grows by 50. Body 262,144 and 600,000: the broker refuses the batch but the producer does not wait for an answer, so the API answers 202 and the topic grows by 0: silent loss. About 60 % sure. The counters of failed messages and send errors stay at 0 in these cells, which is the "silent" signature. If the client does notice (a connection reset, then an error), I would see 500 or 503 and the cell would look like `broker`.
7. **Loud everywhere else:** in all variants with `acks=-1`, the count of 202 answers equals the number of records the topic gained, in every cell except the mix cell. No oversized event gets 202 in these variants. I expect no 503 anywhere (the breaker should stay closed because a size error is not an "unavailable" error; about 70 % sure), and no crash of the API.
8. **Cost of the refusals:** the edge refuses without parsing, so the API CPU and the event-loop delay at 5,000,000 bytes (413 at the edge) are lower than at 880,000 bytes (the whole body is read, validated, copied and sent). I expect the CPU of the API to be below 25 % of one core in every cell except the mix cell, and the CPU of the brokers to be low everywhere.
9. **What I expect NOT to change:** cluster health (no under-replicated partitions), generator health (dropped iterations 0, k6 CPUs below 70 %), and the small events of every cell (1,000 bytes always get 202 with a normal API).

## Setup (planned)

| Item | Value |
| --- | --- |
| Server | ingest-api (`dist/main.js`, started by `run.sh`, restarted for every cell), `taskset -c 0,6`, acks=-1 and idempotent (except `acks0`), `KAFKA_KEY_MODE=event`, 6 partitions |
| Kafka | 3 brokers in containers on CPUs 2,8,3,9; topic `k12-events`: 6 partitions, RF 3, min ISR 2, `max.message.bytes=200000` in `broker` and `acks0`, broker default (1,048,588, read into `env.txt`) otherwise |
| Variants | `edge`, `batcher`, `broker`, `client`, `acks0` (settings in the README) |
| Route / generator | `POST /v1/events`, k6 `constant-arrival-rate`, 5 req/s for 10 s per cell (1,000 req/s in the mix cell), pinned to CPUs 4,10 |
| Matrix | 43 cells per repeat x 3 repeats = 129 cells, about 63 minutes in total |
| Sysctls | none changed |
| Dashboard | [K12 - Message size and batch limits.json](../../../../infra/observability/dashboards/stage-2/K12%20-%20Message%20size%20and%20batch%20limits.json) |
| Versions | filled in after the run from env.txt |

## Method (planned)

1. For each repeat and variant: recreate the topic (with the topic limit where the variant has one). For each size level: restart the API with the settings of the variant, read the log end offset of the topic (sum over the partitions), let k6 send the cell, wait 2 s, read the offset again, save the API counters, make one curl request of the same size to see the error text, stop the API.
2. The audit of a cell: 202 answers against records added to the topic (should be equal unless the cell is `acks0` or the mix cell), 413 answers against `batcher_too_large` and the API's status counts, and the send error types of the API.
3. All 129 cells are measured cells. The levels near the limits (65,000 / 66,000, 880,000 / 950,000, the wire sizes around 1,000,000) are the ones the conclusions are drawn from. I report the status counts of every repeat, not only a median, because the answer of a cell is a category and not a number.
4. Criteria for discarding a cell, decided now: dropped iterations > 0 (except where the answer itself is a missing answer, status 0); k6 CPUs above about 70 %; an under-replicated partition during the cell; any 503 in a cell where I predicted none, if the API log shows the cause was the topic recreation or the start-up (not the size); status `FAILED`; the API was not ready. A discarded cell is repeated and listed in Explanation.md after the run.

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->
