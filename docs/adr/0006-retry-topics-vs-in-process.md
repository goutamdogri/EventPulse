# ADR 0006: Retry tiers on Kafka topics (not_before + pause), in-process retry only for the whole chunk

- **Status:** Accepted
- **Date:** 2026-11-28
- **Author:** Goutam Dogri
- **Stage:** 3 (Correctness), requirement S3-F5
- **Affects:** `packages/retry`, `services/consumer-sink/src/consumer/batch-handler.ts`, `settle.ts`, `src/retry/`, `db/migrations/0006_dlq_meta.sql`, `infra/compose/docker-compose.kafka.yml`

## Context

Stage 2 retried a failing row inside the handler (`SINK_MAX_RETRIES` times with backoff, then the DLQ), and treated every transient database error as "the database is down" (pause the partition). Two problems:

1. **Head-of-line blocking.** While a row is being retried in the handler, nothing behind it in the partition moves. Long waits (minutes) inside a handler also exceed the poll interval and trigger rebalances (PRD pitfall: "retrying inside the handler with sleeps").
2. **One bucket for all transient errors.** A deadlock on one row and an unreachable database were handled the same way, so a record-level transient error could only block the partition or exhaust a short in-process budget.

S3-F5 asks for bounded retry with backoff and jitter, then the DLQ with full metadata, without blocking a partition indefinitely, and without dead-lettering what an outage merely delayed.

## Options considered

| | In-process retry (Stage 2) | Retry topics with `not_before` (this ADR) | Delayed re-publish to the SAME topic |
| --- | --- | --- | --- |
| Records behind a failing one | wait for every retry | keep flowing (the failing record is parked on a tier) | keep flowing |
| Long delays (minutes) | impossible without hitting the poll interval | a paused partition waits; nothing sleeps | records come back at once: hot loop |
| Retry state survives a crash | no (counter in memory) | yes (`retry.attempt` header) | only in headers |
| Ordering of one key | kept | **lost** for the retried record | lost |
| Extra moving parts | none | 3 topics, a second consumer group | none |

## Decision

- **One classifier** (`packages/retry`) with seven classes and four actions: `dlq` (PERMANENT_PARSE, PERMANENT_SCHEMA), `isolate_then_dlq` (PERMANENT_DB), `retry_tier` (TRANSIENT_RECORD: 40001 / 40P01 / 57014, UNKNOWN), `block` (TRANSIENT_SYSTEMIC: connection, 08/53, 57P0x, registry unavailable; SYSTEMIC_CONFIG: 42/28/3D000/0A/XX).
- **`block` never becomes retry traffic or a dead letter.** An outage pauses the partition with backoff (the Stage 2 rule, kept).
- **Retry tiers** `events.retry.1..3`, fixed delays 5 s / 60 s / 10 min, equal jitter `[d/2, d]`, `RETRY_MAX_ATTEMPTS` = 3. The record keeps its original bytes and headers, plus `retry.*` headers: attempt, `not_before`, the original topic/partition/offset, first failure time, and the last error.
- **The retry-tier consumer pauses, it does not sleep.** If the head record is not due, it commits what is done, pauses that partition until `not_before`, and lets the client seek back. Each tier has one delay, so a tier is in `not_before` order up to the jitter: only the head waits.
- **In-process retry stays, for the whole chunk only.** A chunk failing with TRANSIENT_RECORD is retried `RETRY_INPROCESS_ATTEMPTS` (2) times before it is bisected. A deadlock usually clears at once, and without this a slow database (57014 on every chunk) would push whole chunks onto the tiers.
- **Rate limit.** At most `RETRY_MAX_RATE_PER_S` (100) records/s go to the tiers. Above that the chunk is treated as blocking (pause), so a misclassified outage or a bad deploy cannot flood Kafka (C9).
- **DLQ with an index.** Every dead letter goes to `events.dlq` (acked), then `dlq_meta` (one row per source record, `ON CONFLICT DO NOTHING`), then the source offset is committed. A re-publish after a crash is a counted DLQ duplicate (`ep_dlq_duplicates_total`), never a second row. The DLQ record of a retried event points at where it was first consumed (`dlq.original_*`) and names the tier it last failed on (`dlq.failed_topic`).
- **Switch:** `RETRY_TOPICS_ENABLED=false` brings back the Stage 2 behavior (C14 and this comparison).

## Consequences

- **Ordering:** a retried event is stored after later events of the same key. Storage is order-insensitive (the `(tenant_id, event_id)` key, Stage 3 step 8), so this is acceptable. G4 will say "ordering within a partition, except for retried records".
- **`attempts` changed meaning.** It now counts retry-tier attempts (0 for a record dead-lettered straight from the main consumer). With tiers off it keeps the Stage 2 value (1 + in-process retries).
- **Class names changed** (`TRANSIENT_DB` was split into TRANSIENT_RECORD and TRANSIENT_SYSTEMIC; `SYSTEMIC_DB` is now SYSTEMIC_CONFIG). The `ep_consumer_errors_total{class}` labels follow.
- **The audit** (check J) now waits for the retry-tier group too, and counts dead letters by `dlq_meta` rows, not DLQ topic records.
- **Records stay longer in flight:** up to about 11 minutes through all tiers before reaching the DLQ.

## Proof

- `tests/correctness/test/retry.e2e-spec.ts`:
  - A transient failure is stored from tier 1 after its delay, while the 200 records behind it are stored at once.
  - A record that never recovers goes through tiers 1, 2 and 3, then to the DLQ with attempts 3 and its origin kept.
  - A permanent failure goes to the DLQ at once.
  - Postgres down for 15 s produces 0 retry and 0 DLQ records.
  - The rate limit pauses instead of flooding the tiers.
  - 1 poison record among 500 leaves 499 stored and 1 dead letter.
- Crash matrix rows `retry:*` and `dlq:*` (`docs/generated/crash-matrix.md`): no loss, nothing stored twice, one `dlq_meta` row per poison record even when the dead letter was published twice.

## Rejected alternatives

- **In-process retry only (Stage 2):** head-of-line blocking, and long delays are impossible.
- **Re-publishing to `events.raw`:** no delay, so it becomes a hot loop, and it mixes retries with new traffic.
- **Sleeping in the retry-tier handler until `not_before`:** blocks the consumer and trips rebalances. Pausing is free.
