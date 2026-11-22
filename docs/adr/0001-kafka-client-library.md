# ADR 0001: Kafka client library for Node services

- **Status:** Accepted
- **Date:** 2026-11-22
- **Author:** Goutam Dogri
- **Stage:** 2 (Decouple with a Queue), requirement S2-F1
- **Affects:** `packages/kafka-config`, `services/ingest-api/src/kafka/`, later `services/consumer-sink/`

## Context

Stage 2 puts Kafka between the ingest API and Postgres. Both the API (producer) and the new sink (consumer group `sink-postgres`) are Node.js/TypeScript services, so we need a Kafka client for Node. The Stage 2 PRD names `kafkajs` as the easiest option to learn, and asks to check its maintenance status before committing, with a librdkafka-based client as the higher-performance alternative.

What we need from the client in Stage 2:

- A producer with `acks=all`, idempotence, compression choice and bounded timeouts (S2-F1, S2-F5).
- A consumer group with manual offset commits and batch processing (S2-F2, S2-F3).
- An admin API for end offsets and committed offsets (lag, the audit in S2-F7).
- To keep working through Stage 8 (performance engineering) without a rewrite.

## Options considered

| | `kafkajs` | `@confluentinc/kafka-javascript` | `node-rdkafka` |
| --- | --- | --- | --- |
| Implementation | Pure JavaScript | librdkafka (C) + a KafkaJS-compatible API | librdkafka (C), callback/stream API |
| Maintenance | Last release 2.2.4 on 2023-02-27, no releases since | Maintained by Confluent; 1.11.0 is current (2026), regular releases | Superseded by the Confluent client (same code lineage) |
| API to learn | KafkaJS | KafkaJS-style (`kafkaJS: {...}` block) + any librdkafka property | Lower level, very different from tutorials |
| Producer batching | None: sends what you hand it | librdkafka batches (`linger.ms`, `batch.size`) | Same as Confluent |
| Install | Plain npm package | Native addon (prebuilt binary download, `allowBuilds` in pnpm) | Native addon |
| Performance | Single-threaded JS protocol code | librdkafka threads do I/O and batching off the event loop | Same as Confluent |
| Kafka features | Frozen in 2023 (e.g. no KIP-848 consumer protocol) | Follows librdkafka (new consumer protocol, current broker versions) | Same as Confluent |

## Decision

Use **`@confluentinc/kafka-javascript`** (pinned to `1.11.0`) through its **KafkaJS-compatible API** (`Confluent.KafkaJS`).

Only `packages/kafka-config/src/client.ts` creates the client, and only the services' `kafka/` folders call it. If the library ever has to change, those are the only files to touch.

## Reasons

1. **Maintenance.** `kafkajs` has had no release since February 2023. Building a multi-stage project on a frozen client means carrying its bugs and missing broker features for the whole project.
2. **We keep the learning value of the KafkaJS API.** The compatible API keeps the familiar producer/consumer/admin shape, so tutorials and the PRD sketches still apply with small changes.
3. **Same client from Stage 2 to Stage 8.** The PRD suggests a librdkafka client as a Stage 8 comparison. Starting with it means Stage 8 compares settings instead of rewriting the client layer.
4. **Real producer settings.** Every librdkafka property (`linger.ms`, `message.timeout.ms`, `batch.size`, ...) can be set next to the `kafkaJS` block, which K2 (producer settings sweep) needs.

## Consequences

**Differences from KafkaJS that our code must respect:**

- `acks`, `compression` and `timeout` are set **per producer**, not per `send()`. The PRD micro-batcher sketch passes `acks: -1` to `send()`; with this client that throws. Changing acks (K2, K4) means restarting the API with another `KAFKA_ACKS`.
- The client has **its own batching** (librdkafka `linger.ms`). Since S2-F1 asks us to build a micro-batcher to learn the mechanism, we set `linger.ms=0` by default (`KAFKA_LINGER_MS`), so our batcher alone decides the batch. One `send()` of N messages is still sent efficiently by librdkafka. K2 can compare "our batcher" vs "librdkafka linger" by changing env vars only.
- `send()` with many messages rejects if **any** message fails, while others may have been written. This matches the batcher's rule (one outcome per batch) and is recorded as a known source of duplicates on client retry.
- Retries are done by librdkafka, bounded by `message.timeout.ms` (`KAFKA_DELIVERY_TIMEOUT_MS`, default 30 s). Without that setting librdkafka's default is 300 s, far too long for a request that is waiting.
- Errors are checked by `error.code`, not by `error.name`/type (relevant for S2-F5's mapping to 503).
- Consumer differences for step 2: `fromBeginning` and `autoCommit` are set in the consumer config, not in `subscribe()`/`run()`; `eachBatch` batches are capped by `js.consumer.max.batch.size` (default 32); `heartbeat()` no longer has to be called manually.

**Operational costs:**

- Native addon: `pnpm-workspace.yaml` must allow its install script (`allowBuilds`). The prebuilt binary is downloaded from Confluent's GitHub releases; on an unsupported platform it falls back to a source build (needs a C/C++ toolchain).
- The client runs its own threads. Event-loop metrics no longer show all Kafka work, so the CPU of the Node process includes librdkafka threads. Keep that in mind when reading K1 CPU numbers.

## Measurement

How we will know whether this decision was right:

- K1: the gap between `kafka-producer-perf-test.sh` (broker ceiling) and the API's throughput is our code's overhead, not the client's.
- K2: changing `KAFKA_LINGER_MS`, `KAFKA_COMPRESSION`, `KAFKA_ACKS` and the batcher settings changes throughput and latency as expected, without code changes.
- Stage 8: compare against a Go client on the same cluster before deciding the client is a bottleneck.
