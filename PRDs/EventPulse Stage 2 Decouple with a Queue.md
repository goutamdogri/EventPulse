# EventPulse Stage 2: Decouple with a Queue

*Duration: about 3 weeks. Goal: put Kafka between the API and the database so ingestion depends only on durably appending to a log, then discover every new problem that creates (duplicates, lag, rebalances, poison messages, a different durability story).*

## 1. Objective and Motivating Pain

Stage 1 proved that the API's capacity and availability are bounded by Postgres. Stage 2 changes the architecture so the request path does the minimum possible work: authenticate, validate, append to a replicated log, answer 202.

By the end of this stage you must be able to:

1. Show the ingest knee moving relative to the Stage 1 baseline, and explain the mechanism (not just the number).
2. Explain partitions, keys, consumer groups, offsets, replication, and acks well enough to predict what happens in a failure before you cause it.
3. Demonstrate, with your own experiments, that this design produces duplicates and possibly loss under specific failures, and list exactly which ones.
4. Show consumer lag as the new first-class signal, and show that a slow database no longer slows ingestion.
5. Explain what a 202 means now and what it no longer promises.

**Predictions to write down before building:** by how much will the ingest knee move, and which resource becomes the new limit (API CPU, load generator, broker disk, network)? How many duplicates will a consumer crash produce? Check each against your results.

## 2. Prerequisites and Setup

| Item | Requirement |
| --- | --- |
| Stages 0-1 | Complete: harness, notebook habit, Stage 1 baseline report |
| Kafka | 3 brokers in KRaft mode (no ZooKeeper) via docker-compose |
| Client library | `kafkajs` is the simplest to learn and is what NestJS's Kafka transport uses. Check its current maintenance status before committing; a librdkafka-based client (for example Confluent's JavaScript client) is the higher-performance option and a good Stage 8 comparison |
| Tools | Kafka CLI scripts (`kafka-topics.sh`, `kafka-consumer-groups.sh`, `kafka-get-offsets.sh`, `kafka-producer-perf-test.sh`), `kcat` (optional), `docker stats`, `iostat` |
| Reuse | Stage 1 NestJS service (Express), Postgres, k6 harness, notebook template, row-count audit habit |

### Compose sketch (verify against the official docs for your image version)

```yaml
# One of three brokers; duplicate with node ids 2 and 3 and different ports.
kafka1:
  image: apache/kafka:3.8.0   # pin a version; confirm current stable
  environment:
    KAFKA_NODE_ID: 1
    KAFKA_PROCESS_ROLES: broker,controller
    KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka1:9092
    KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka1:9093,2@kafka2:9093,3@kafka3:9093
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
    KAFKA_DEFAULT_REPLICATION_FACTOR: 3
    KAFKA_MIN_INSYNC_REPLICAS: 2
    CLUSTER_ID: <generate one and use it on all three>
```

You will need host-reachable listeners for clients running outside Docker. Getting listener and advertised-listener settings right is itself a classic Kafka learning exercise; document what you had to change and why.

**Resource note:** three brokers, Postgres, the API, consumers, and a load generator can overwhelm one laptop. Give each container explicit CPU and memory limits, record them, and expect the machine, not Kafka, to be your bottleneck locally. Real per-node numbers come from cloud VMs in Stage 8.

## 3. Learning Plan

Learn each concept, then prove it with a command before moving on.

### Kafka core model

- **Topic, partition, log, segment.** A partition is an append-only log on disk. Order is guaranteed only within a partition. Throughput scales by partitions.
- **Offsets.** Position of a record within a partition. Consumers track and commit offsets; the broker does not delete records when they are read.
- **Replication.** Each partition has a leader and followers. The ISR (in-sync replicas) is the set of replicas caught up with the leader. `min.insync.replicas` and producer `acks` together decide when a write is acknowledged and when writes are refused.
- **Leader election.** On broker failure, a new leader is chosen from the ISR. Unclean leader election (off by default) trades data loss for availability.
- **Retention.** Data is kept by time or size, not by consumption. A consumer that falls behind retention loses data.

### Producer

- **Batching:** `linger.ms`, `batch.size`, `buffer.memory`. Larger batches raise throughput and add latency.
- **Acks:** `0` (fire and forget), `1` (leader only), `all` (all ISR). Know the loss window of each.
- **Idempotent producer:** `enable.idempotence` removes duplicates caused by producer retries within a session. It does not deduplicate across application-level retries or consumer reprocessing.
- **Compression:** gzip, snappy, lz4, zstd trade CPU for network and disk.
- **Retries and timeouts:** `retries`, `delivery.timeout.ms`, `max.in.flight.requests.per.connection` (interacts with ordering).
- **Key and partitioner:** the key decides the partition. Same key, same partition, ordered. Skewed keys create hot partitions.
- **Library note:** `kafkajs` has no `linger.ms`; it sends what you hand it. To batch across concurrent HTTP requests you must build a **micro-batcher** yourself (collect messages for a few milliseconds or until N messages, send once, resolve each waiting request when the batch is acknowledged). Building it teaches the mechanism and is part of this stage.

### Consumer

- **Consumer groups:** partitions are divided among consumers in a group. More consumers than partitions leaves the extras idle.
- **Rebalances:** triggered when members join, leave, or are considered dead. Consumption pauses during eager rebalances. Know `session.timeout.ms`, `heartbeat.interval.ms`, and `max.poll.interval.ms` (or the `kafkajs` equivalents).
- **Offset commits:** commit after processing gives at-least-once; commit before gives at-most-once. Auto-commit is convenient and dangerous.
- **Lag:** log end offset minus committed offset per partition. It is the main health signal of a consumer.
- **Backpressure:** pause fetching when downstream is slow instead of buffering without bound.

### Delivery semantics (state them in your own words)

- At-most-once, at-least-once, and what "exactly once" actually means (effectively once through idempotent processing, or Kafka transactions inside Kafka). Stage 3 builds correctness; Stage 2 must discover why it is needed.

### Reading

DDIA Chapter 5 (replication, leaders and followers, replication lag) and Chapter 11 (messaging, logs, consumer offsets). Read the Kafka documentation sections on design and on producer and consumer configuration. In `notebook/kafka-notes.md`, write one paragraph on each: why a log instead of a queue, and what the ISR protects.

## 4. Requirements

| ID | Requirement |
| --- | --- |
| S2-F1 | `POST /v1/events` validates, produces to `events.raw`, and returns 202 only after Kafka acknowledges the write |
| S2-F2 | A consumer group `sink-postgres` reads `events.raw` and writes batches to Postgres |
| S2-F3 | Offsets are committed only after the database transaction commits |
| S2-F4 | Poison messages must not block a partition forever (after you first observe that they do) |
| S2-F5 | If Kafka is unavailable the API returns 503 with `Retry-After`; memory use stays bounded |
| S2-F6 | Expose producer, batcher, and consumer metrics on `/debug/stats` |
| S2-F7 | A one-command audit compares requests sent, acknowledged, Kafka offsets, and distinct rows in Postgres |

### Topic configuration

| Setting | Initial value | Why |
| --- | --- | --- |
| Partitions | 6 (experiment with 1, 3, 6, 12) | Enough to explore parallelism locally |
| Replication factor | 3 | Survive one broker failure |
| `min.insync.replicas` | 2 | With `acks=all`, tolerate one broker down without losing acknowledged writes |
| Retention | 24 h default; shorten for the retention experiment | Observe expiry behavior |
| Key | `tenant_id` first, then compare with `event_id` | Understand ordering vs distribution |

### What 202 means now

In Stage 1, 202 meant committed to Postgres. In Stage 2 it means **durably appended to the replicated log under the configured acks policy**. The event may not be queryable yet. Document this in the API contract, and add the property "eventual visibility within N seconds" as a metric (end-to-end freshness) you will track from now on.

## 5. Design

```
k6 -> NestJS API -> micro-batcher -> Kafka (events.raw, 6 partitions, RF=3)
         |  validate, auth                    |
         v                                    v
     202 after ack              Consumer group sink-postgres
                                   | batch (N msgs or T ms)
                                   v
                             INSERT ... unnest(...) ON CONFLICT DO NOTHING
                                   | commit DB tx, then commit offsets
                                   v
                               Postgres
                                   |
                  poison -> events.dlq (minimal version)
```

### Message format

Key: `tenant_id`. Value: JSON event envelope (Protobuf or Avro arrives in Stage 3). Headers: `received_at`, `schema_version`, and a trace/request id.

### Repo additions

```
services/ingest-api/src/kafka/ (producer, micro-batcher)
services/consumer-sink/ (consumer, batch writer, dlq publisher)
libs/kafka-config/ (shared topic and client settings)
infra/docker-compose.kafka.yml
scripts/ (audit.sh, kill-broker.sh, kill-consumer.sh, lag.sh)
loadtests/k6/ingest-kafka.js
notebook/ stage-2-*.md
```

## 6. Step-by-Step Build Plan

1. **Cluster.** Bring up 3 brokers, create `events.raw` and `events.dlq`, verify with `kafka-topics.sh --describe` (leader and ISR for every partition).
2. **Raw Kafka baseline.** Run `kafka-producer-perf-test.sh` against the cluster with your event size. This is the ceiling of the broker setup, independent of your code.
3. **Producer path.** Add a producer service and replace the DB insert in the controller.
4. **Micro-batcher.** Bounded queue, flush on size or time, resolve or reject each waiting request per batch result.
5. **Consumer.** Batch consumption with manual offset management.
6. **Batch writer.** One multi-row insert per batch.
7. **Audit script.** Reconciliation across all stages of the pipeline.
8. **Compare with Stage 1:** same event shape, same load harness, same hardware.
9. **Run the break-it catalog** (section 8).
10. **Fix what you broke** (poison handling, bounded memory, timeouts) and re-run the relevant experiments.
11. **Write the report** (section 10).

### Micro-batcher sketch

```ts
class MicroBatcher {
  private queue: { msg: Message; resolve: () => void; reject: (e: Error) => void }[] = [];
  private timer?: NodeJS.Timeout;
  constructor(private producer: Producer, private opts: {
    maxMessages: number; maxWaitMs: number; maxQueue: number }) {}

  enqueue(msg: Message): Promise<void> {
    if (this.queue.length >= this.opts.maxQueue) {
      return Promise.reject(new OverloadedError());   // shed load -> 503
    }
    return new Promise((resolve, reject) => {
      this.queue.push({ msg, resolve, reject });
      if (this.queue.length >= this.opts.maxMessages) this.flush();
      else if (!this.timer) this.timer = setTimeout(() => this.flush(), this.opts.maxWaitMs);
    });
  }

  private async flush() {
    clearTimeout(this.timer); this.timer = undefined;
    const batch = this.queue.splice(0, this.opts.maxMessages);
    if (!batch.length) return;
    try {
      await this.producer.send({
        topic: 'events.raw', acks: -1,
        messages: batch.map((b) => b.msg),
      });
      batch.forEach((b) => b.resolve());
    } catch (e) {
      batch.forEach((b) => b.reject(e as Error));
    }
  }
}
```

Note the design decisions you must make deliberately: queue bound, behavior when full, behavior on partial failure, timeout per send, and whether to allow multiple in-flight batches.

### Consumer batch handler sketch

```ts
await consumer.run({
  autoCommit: false,
  eachBatchAutoResolve: false,
  eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary }) => {
    const rows = parse(batch.messages);                 // may throw on poison
    await writeBatch(pool, rows);                       // one DB transaction
    resolveOffset(batch.messages.at(-1)!.offset);
    await commitOffsetsIfNecessary();                   // commit AFTER the DB commit
    await heartbeat();
  },
});
```

```sql
INSERT INTO events (event_id, tenant_id, type, occurred_at, schema_version, payload)
SELECT * FROM unnest($1::uuid[], $2::text[], $3::text[], $4::timestamptz[], $5::int[], $6::jsonb[])
ON CONFLICT (event_id) DO NOTHING;
```

The `ON CONFLICT DO NOTHING` hides duplicates at the sink. Count reprocessed messages separately (messages consumed minus rows inserted) so duplicates stay visible in your measurements rather than being silently absorbed.

## 7. Test Plan

| Layer | Tests | Pass condition |
| --- | --- | --- |
| Unit | Micro-batcher: flush on size, flush on time, bounded queue rejects, error fan-out to all waiters, no message lost or sent twice on partial failure | Behavior matches spec |
| Unit | Batch parser and writer with valid, invalid, and mixed batches | Correct rows, clear errors |
| Integration | API to Kafka to Postgres end to end with real services | Row appears within a bounded delay |
| Integration | Kafka down at request time | 503, memory flat, recovery on restart |
| Integration | Restart consumer cleanly (SIGTERM) | No loss, minimal duplicates, rejoin works |
| Audit | After every run: sent, 202 count, sum of partition end offsets, rows in Postgres, duplicates observed | Must reconcile or the gap is a logged finding |
| Load | Same harness and event as Stage 1 | Comparable numbers |
| Soak | 30-60 minutes at about 70% of the new knee | No lag drift, no memory growth |

The audit is the core test of this stage. Write it first and run it after every experiment.

## 8. Break-It Experiment Catalog

Write the hypothesis before each run, change one variable at a time, and run the audit after each experiment.

### K1 Throughput versus the Stage 1 baseline

- **Hypothesis:** the knee moves up by roughly an order of magnitude on ingest, and the new limit is API CPU or the load generator, not Kafka.
- **How:** repeat the Stage 1 step test on the same hardware. Run `kafka-producer-perf-test.sh` separately to get the broker ceiling.
- **Observe:** p99 vs rate, Node CPU, broker CPU and disk, generator headroom, how far below the raw Kafka ceiling your API sits.
- **Explains:** where the time went; the gap between raw Kafka and your API is your code's overhead.

### K2 Producer settings sweep

- **Hypothesis:** bigger batches and compression raise throughput and p50 latency slightly; `acks=all` costs latency vs `acks=1`.
- **How:** vary batcher size and wait (1, 5, 20 ms; 50, 500, 5000 messages), `acks` (0, 1, all), compression (none, gzip, plus lz4 or zstd if your client supports it), idempotence on/off.
- **Observe:** rps, p99, average batch size, broker network bytes, CPU split.
- **Explains:** the throughput-latency-durability triangle with data of your own.

### K3 Partitions versus consumers

- **Hypothesis:** consumer throughput scales with consumers only up to the partition count; extra consumers sit idle.
- **How:** partitions 1, 3, 6, 12 against 1 to 12 consumers.
- **Observe:** sink rows/sec, lag per partition, which consumers have assignments.
- **Explains:** partition count as the unit of parallelism, and why it is a hard-to-change decision.

### K4 Kill a broker under load

- **Hypothesis:** with RF=3, `min.insync.replicas=2`, `acks=all`, killing one broker causes a brief burst of errors and latency, then recovery, with zero acknowledged events lost. Killing two brokers makes writes fail (503), not silently lose data.
- **How:** `docker kill` a broker that leads some partitions during a steady run; later kill a second one. Repeat with `acks=1`.
- **Observe:** error rate and duration, leader changes, ISR shrink, API behavior, audit result (acknowledged vs stored).
- **Explains:** exactly what the durability settings protect and what they cost in availability.

### K5 Kill a consumer at every step

- **Hypothesis:** a crash after the DB commit but before the offset commit causes reprocessing and duplicates (absorbed or not depending on dedup); a crash before the DB commit causes reprocessing without loss.
- **How:** inject crash points (before parse, after parse, after DB commit before offset commit) with a SIGKILL at a chosen step, using a debug flag.
- **Observe:** duplicate count (messages consumed minus unique rows), missing events, time to resume.
- **Explains:** at-least-once delivery in practice, and the table you will build into the Stage 3 delivery guarantees document.

### K6 Poison message

- **Hypothesis:** one malformed or constraint-violating message makes the consumer throw, retry forever, and block its partition while lag grows without bound.
- **How:** produce one bad record (invalid JSON, bad UUID, oversized field) into a busy partition.
- **Observe:** consumer crash loop or retry loop, lag on that partition only, healthy partitions continuing.
- **Fix and re-test:** bounded retries with backoff, then publish to `events.dlq` with error headers and advance the offset. Verify the partition drains and the DLQ record is inspectable. Stage 3 formalizes the retry and replay policy.

### K7 Slow Postgres

- **Hypothesis:** ingestion keeps its latency and rate; consumer lag grows linearly with the shortfall; recovery time depends on how far the sink can exceed the arrival rate.
- **How:** repeat the Stage 1 slow-database injection while running at a steady ingest rate.
- **Observe:** ingest p99 (should stay flat), lag growth, disk usage on brokers, time to drain after the DB recovers.
- **Explains:** the central benefit of the queue, and the new risk: unbounded lag.

### K8 Rebalances and group stability

- **Hypothesis:** adding or removing consumers, or processing longer than the poll or session limits, triggers rebalances that pause consumption and cause duplicates.
- **How:** start and stop consumers under load, set an aggressive session timeout, add an artificial processing delay beyond the limit.
- **Observe:** rebalance count and duration, consumption gaps, duplicates, whether the cooperative or sticky assignor helps.
- **Explains:** why heartbeats, batch sizes, and processing time must be designed together.

### K9 Hot key and skew

- **Hypothesis:** keying by `tenant_id` with one dominant tenant saturates one partition while others idle, capping throughput and growing that partition's lag.
- **How:** generate traffic where one tenant sends 80% of events; compare with keying by `event_id`.
- **Observe:** per-partition bytes and lag, throughput ceiling, what ordering you lost by changing the key.
- **Explains:** the ordering vs distribution trade-off and why Stage 6 handles hot partitions explicitly.

### K10 Kafka unavailable and API backpressure

- **Hypothesis:** if Kafka is down, an unbounded in-memory queue in the API grows until the process dies; a bounded queue turns the problem into fast 503s.
- **How:** stop all brokers during load, with the bounded queue disabled and then enabled.
- **Observe:** API memory over time, latency, error responses, recovery behavior and whether the API reconnects without a restart.
- **Explains:** load shedding and the need for backpressure at every boundary.

### K11 Retention and lag

- **Hypothesis:** if a consumer is down longer than retention, unread data is deleted and lost silently from the consumer's point of view.
- **How:** set topic retention to a few minutes, stop the consumer for longer, restart.
- **Observe:** offset out of range behavior, rows missing vs events acknowledged, the consumer's reset policy (`earliest`, `latest`) and what each does to your data.
- **Explains:** why lag alerts must fire long before retention expires.

### K12 Message size and batch limits

- **Hypothesis:** oversized events or batches exceed broker or client limits and fail in ways that depend on configuration layers.
- **How:** send events around and above `message.max.bytes` and the client's max request size.
- **Observe:** error types at the API, whether the failure is silent or loud, and that your edge validation rejects large events with 413 before Kafka sees them.
- **Explains:** why validation limits belong at the edge.

## 9. Metrics and Diagnostics

**API and batcher:** rps, status codes, latency percentiles, queue depth, batch size distribution, send latency, rejected-for-overload count, event-loop delay, memory.

**Kafka:** bytes in and out per broker, request latency, under-replicated partitions, ISR changes, leader elections, disk usage, partition sizes.

**Consumer:** lag per partition, messages per second, batch size, processing time per batch, rebalance count, commit latency, errors, DLQ count.

**Postgres:** insert rate, active connections, WAL, commit latency.

**Correctness:** sent, acknowledged, partition end offsets total, rows (distinct), duplicates, DLQ.

Useful commands:

```
kafka-topics.sh --bootstrap-server kafka1:9092 --describe --topic events.raw
kafka-consumer-groups.sh --bootstrap-server kafka1:9092 --describe --group sink-postgres
kafka-get-offsets.sh --bootstrap-server kafka1:9092 --topic events.raw
kafka-producer-perf-test.sh --topic perf --num-records 2000000 --record-size 500 \
  --throughput -1 --producer-props bootstrap.servers=kafka1:9092 acks=all
```

## 10. Artifacts

**1. `docs/benchmarks/stage-2-decoupling.md`**

1. Setup and topology, including every container resource limit.
2. Raw Kafka ceiling vs your API result vs the Stage 1 baseline, with the explained mechanism.
3. Producer settings sweep results.
4. Partition and consumer scaling results.
5. Failure catalog (K4 to K12) with data and explanation.
6. Lag behavior charts during slow-database and consumer-crash experiments.
7. Predictions you got wrong.

**2. `docs/failure-ledger-stage-2.md`:** a table of every failure you injected with columns: failure, duplicates produced, events lost, acknowledged-but-missing, recovery time, whether a human was needed. This ledger is the direct input to Stage 3.

**3. API contract update:** what 202 now means, what is not guaranteed yet, and the retry guidance for clients.

**Optional blog post:** "What happens when a Kafka broker dies mid-batch", built from K4 and K5.

## 11. Common Pitfalls

- Benchmarking on one laptop and concluding Kafka is slow when the machine is saturated. Always record CPU for every component and the generator.
- Auto-committing offsets and then wondering why events vanish after a crash.
- Hiding duplicates with `ON CONFLICT DO NOTHING` and never measuring how many reprocessed messages there were.
- Increasing partitions without considering keyed ordering and consumer limits.
- Unbounded queues anywhere (batcher, consumer prefetch, worker pools).
- Confusing producer retry duplicates with consumer reprocessing duplicates. They have different causes and different fixes.
- Forgetting listener and advertised-listener configuration, then debugging connectivity instead of Kafka.
- Treating `acks=all` as sufficient without `min.insync.replicas`.
- Reading lag as only a number rather than a rate of change: growing, stable, or draining.
- Testing failures only once; repeat key failure experiments to see variance.

## 12. Exit Criteria

- [ ] Pipeline runs with one command; audit reconciles on clean runs.
- [ ] Ingest knee measured against Stage 1 on the same hardware, with the mechanism explained and the new limiting resource identified.
- [ ] Raw Kafka ceiling recorded and compared with your API's throughput.
- [ ] Experiments K1 through K12 have notebook entries with hypotheses, data, and explanations.
- [ ] Failure ledger complete, including duplicates and any loss.
- [ ] Poison messages handled by bounded retry and a DLQ, with a test.
- [ ] API sheds load with 503 when Kafka is unavailable, with memory staying bounded.
- [ ] You can explain partitions, ISR, `min.insync.replicas`, `acks`, offset commit ordering, and rebalances without notes.
- [ ] Report and ledger committed, repo tagged `stage-2-complete`.

**Self-test questions (answer aloud, without notes):**

1. With RF=3, `min.insync.replicas=2`, and `acks=all`, what happens when one broker dies? When two die? Why is each outcome safe or unsafe?
2. A consumer commits an offset before writing to Postgres and crashes. What is lost? What if it commits after and crashes between the two steps?
3. Why do 12 consumers on 6 partitions not go faster than 6?
4. Your API returns 202 and the event is not yet in Postgres. What exactly did you promise the client?
5. How can lag be zero and data still be missing? How can lag be huge and the system still healthy?
6. Why does one poison message stall only one partition, and why is that still a serious problem?

## 13. Handoff to Stage 3

Stage 2 should leave you with a fast pipeline and a ledger of its flaws:

- Duplicates from consumer crashes, rebalances, and producer or client retries.
- Possible loss paths under weaker acks settings or unclean failures.
- No ordering or time semantics: events can arrive late and out of order, and nothing yet handles it.
- Poison handling that is minimal and not yet a policy.
- A message format with no schema governance, so a producer change can break every consumer.
- A durability promise that is correct for the log but not yet defined end to end.

Stage 3 turns these into explicit, tested guarantees: idempotency keys and a dedup layer, a transactional outbox, a real dead-letter and retry policy, and schema management with compatibility rules. Before starting it, write down, from your failure ledger, the exact sentence you wish you could put in your delivery guarantees document and cannot yet.

When you are ready, say **Stage 3**.
