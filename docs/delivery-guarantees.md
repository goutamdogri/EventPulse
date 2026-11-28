# EventPulse delivery guarantees (Stage 3)

This document says exactly what EventPulse promises about the events it accepts, under which conditions, how each promise is kept, and what it does **not** promise. It claims only what an automated test proves: every claim lists the tests that prove it (`[test: <id>]`, the id in the test's name). `tools/guarantee-check` fails the build when a claim has no test, a test id no longer exists, a referenced test failed or was skipped, the test reports are stale, or the crash matrix is stale, incomplete or shows any loss. See [How this is checked](#how-this-is-checked).

Scope: the ingest path (`POST /v1/events` -> `events.raw` -> `consumer-sink` -> Postgres `events`), the retry tiers and DLQ, the DLQ replay tool, and tenant config change events (`admin-api` -> outbox -> `config.changed`).

**In one sentence:** EventPulse delivers every acknowledged event **at least once** through Kafka and stores it **exactly once** in Postgres, because storage is idempotent on `(tenant_id, event_id)`. That is an exactly-once **effect** in Postgres, not exactly-once delivery ([where exactly-once stops](#where-exactly-once-stops)).

Reference: [API contract](api-contract.md) · [crash matrix](generated/crash-matrix.md) · [ADRs and runbooks](README.md)

## Claims

### G1 Durability
- Claim: after a 202, the event is not lost: it is stored in Postgres eventually, even if the API, the consumer and one Kafka broker are killed (SIGKILL) around it, and whatever point the API or consumer crashes at.
- Conditions: the defaults: `KAFKA_ACKS=-1` (all), topic replication factor 3 with `min.insync.replicas=2`, at most one broker lost at a time, Postgres durable (no disk loss). The consumer is restarted and runs until lag 0.
- Mechanism: the API answers 202 only after Kafka acknowledged the record on at least 2 replicas (S2-F1). The consumer commits its Kafka offset only after the Postgres transaction committed (S2-F3), so a crash anywhere re-reads uncommitted records instead of skipping them. A 503 is NOT an acknowledgement: the client retries (same `event_id`). The API's Kafka health check discards a client stuck on a dead broker's connection, so the API serves again within seconds of a broker kill (fixed in S3-F9, found by the test below).
- Proved by: [test: G1.durability] [test: G2.crash-matrix]
- Not guaranteed: an event that got a 503, 500 or no answer (retry it); events whose 202 was given with weaker settings (`KAFKA_ACKS=0/1`, see the API contract table); loss of 2 or more replicas before the data is copied further; recovery time (the crash matrix measures it, nothing bounds it).

### G2 Storage idempotency
- Claim: any number of deliveries of one `(tenant_id, event_id)` results in exactly one stored row; the stored row never changes once written.
- Conditions: the primary key `(tenant_id, event_id)` on `events` (migration 0005) in place; `SINK_DEDUP` not disabled (a debug-only switch for experiment C14).
- Mechanism: `INSERT ... ON CONFLICT (tenant_id, event_id) DO NOTHING` in the same transaction as the `sink_offsets` progress row (S3-F2). A row not inserted is a duplicate, counted by cause. Event ids are scoped by tenant.
- Proved by: [test: G2.same-chunk-twice] [test: G2.tenant-scope] [test: G2.conflict] [test: G2.crash-matrix] [test: G9.reprocessed]
- Not guaranteed: which copy wins if two copies of one key differ in content: the first committed wins, the others are counted as conflicts (`ep_sink_conflicting_duplicates_total`) and never overwrite it (a client bug: the API rejects it within its window, G8).

### G3 Replay safety
- Claim: replaying any range of `events.raw`, any number of times, or replaying dead letters with the DLQ tool, does not change stored data: the stored set and its content are byte-for-byte the same, and every replayed record is a counted duplicate.
- Conditions: G2's conditions; the records are still in the topic (retention 24 h for `events.raw`, 7 days for `events.dlq`).
- Mechanism: G2's key. The DLQ tool re-injects the original bytes and headers (original `received_at` included), marks a dead letter only after Kafka acknowledged it, and a re-run after a crash only replays what was not marked.
- Proved by: [test: G3.triple-replay] [test: G3.range-replay] [test: G3.replay-twice] [test: G3.replay-crash] [test: G2.crash-matrix]
- Not guaranteed: replaying data that retention already deleted; replaying with a newer consumer whose parsing changed meaning (schema evolution is governed separately, G10).

### G4 Ordering
- Claim: events with the same Kafka key (the tenant, `KAFKA_KEY_MODE=tenant`) are in one partition of `events.raw`, in the order the API acknowledged them. There is no order across keys. A record that went through a retry tier is stored after later events of the same key.
- Conditions: the client sends a key's next event after the previous one's 202 (concurrent requests have no defined order); the idempotent producer (on by default) so a broker retry cannot reorder a batch; the partition count of `events.raw` is not changed.
- Mechanism: key = tenant, one partition per key, idempotent producer with ordered in-flight requests. Storage does not depend on arrival order (G5), which is why the retry tiers may store late.
- Proved by: [test: G4.per-key-order] [test: G5.retry-late]
- Not guaranteed: global ordering; ordering of events sent concurrently; order of rows in Postgres (query by `occurred_at`); order with `KAFKA_KEY_MODE=event`.

### G5 Late and out-of-order data
- Claim: events are stored correctly whatever their lateness or arrival order (shuffled, minutes to a day late, retried after newer events, replayed from the DLQ months later), keeping `occurred_at` exactly (UTC, milliseconds) and the original server `received_at`.
- Conditions: `occurred_at` is an RFC 3339 time with a zone, within the clock-skew bounds checked at the API against `received_at`: at most `OCCURRED_AT_MAX_FUTURE_S` (300 s) ahead and `OCCURRED_AT_MAX_PAST_DAYS` (365 days) behind; otherwise 422 (`occurred_at_in_future` / `occurred_at_too_old`) or 400 (no zone).
- Mechanism: set semantics of G2 (no write depends on order: review in ADR 0008); `received_at` set by the API and carried inside the record; bounds only at the API, never at the sink (S3-F8).
- Proved by: [test: G5.property] [test: G5.property-pipeline] [test: G5.reorder-late] [test: G5.retry-late] [test: G5.replay-late] [test: G5.skew]
- Not guaranteed: sub-millisecond precision (extra digits are truncated, documented); what "late" means for queries and aggregates (the analytics layer's job, later stages); events outside the skew bounds.

### G6 Failure handling
- Claim: a failing record cannot block its partition indefinitely and is never silently dropped: it is retried within limits (retry tiers with `not_before`), then quarantined in `events.dlq` with full metadata and one `dlq_meta` row, and can be replayed safely. A dependency outage (Postgres, registry) pauses the partition instead of dead-lettering, and nothing is lost.
- Conditions: retry tiers enabled (default), the DLQ topic writable; the classifier puts the error in the right class (a misclassification is experiment C9).
- Mechanism: one classifier, four actions (dead-letter, isolate then dead-letter, retry tier, block); bisection isolates a bad row among good ones; tiers at 5 s / 60 s / 10 min with jitter and a rate limit; DLQ publish, then `dlq_meta`, then offset commit (S3-F5); the replay tool with dry run, pre-check and loop guard (S3-F6).
- Proved by: [test: G6.transient-recovers] [test: G6.never-recovers] [test: G6.permanent] [test: G6.poison-in-batch] [test: G6.outage] [test: G6.rate-limit] [test: G6.replay-dry-run] [test: G6.replay-execute] [test: G6.replay-filters] [test: G6.replay-loop-guard] [test: G6.dlq-reindex] [test: G2.crash-matrix]
- Not guaranteed: how long a record stays in flight (up to about 11 minutes through all tiers); that a dead letter is published only once (a crash republishes it, counted, one `dlq_meta` row); keeping dead letters beyond the DLQ topic's 7-day retention unless replayed or exported.

### G7 Config change events
- Claim: a tenant config change is published to `config.changed` at least once if and only if its transaction committed. A rejected or rolled-back change (412, 400, error, crash before commit) is never published. A consumer applying by version (`applyIfNewer`) ends on the latest config despite duplicates and reordering.
- Conditions: the change goes through `admin-api` (or another writer using `packages/outbox` `enqueue` in its transaction); one `outbox-relay` running (default `RELAY_MODE=single`); consumers dedup on `event_id` or apply by `version`.
- Mechanism: transactional outbox: the config row and the outbox row commit together (S3-F4); the relay publishes then marks (at-least-once), single active relay via a Postgres advisory lock, backs off while Kafka is down; full-config snapshots with a version.
- Proved by: [test: G7.commit-published] [test: G7.no-enqueue-on-reject] [test: G7.rollback-error] [test: G7.rollback-crash] [test: G7.relay-before-publish] [test: G7.relay-after-publish] [test: G7.relay-after-mark] [test: G7.single-leader] [test: G7.multi-mode] [test: G7.kafka-down] [test: G7.retention] [test: G7.latency] [test: G2.crash-matrix]
- Not guaranteed: exactly-once publishing (a relay crash republishes its last batch); ordering of `config.changed` with `RELAY_MODE=multi` (measured: 45 of 1,000 out of order); publish latency while Kafka is down.

### G8 API idempotency
- Claim: within `IDEM_TTL_S` (1 h), a request repeated with the same key (`Idempotency-Key`, or the `event_id`) gets the original result (202 with `Idempotent-Replayed: true`) and produces nothing new; the same key with a different payload gets 422; 50 concurrent identical requests produce exactly one record; keys are scoped by tenant.
- Conditions: Redis reachable; `IDEM_ENABLED=true` (default). With Redis down, `IDEM_FAILURE_POLICY=open` (default) accepts without API dedup (the sink still dedups, G2) and `closed` answers 503.
- Mechanism: an atomic Redis state machine per key (inflight, then done) with an owner token and a canonical payload hash (S3-F1).
- Proved by: [test: G8.same-key] [test: G8.key-reuse] [test: G8.concurrency] [test: G8.tenant-scope] [test: G8.redis-down-open] [test: G8.redis-down-closed]
- Not guaranteed: API dedup after `IDEM_TTL_S`, after Redis data loss, or while Redis is down with the open policy (G2 still holds in all three); a second copy when the API dies between produce and complete (the crash matrix shows it, G2 absorbs it).

### G9 Duplicate visibility
- Claim: every duplicate dropped is counted at the layer that dropped it (API: replayed / in progress / key reused; sink: by cause reprocessed, redelivered, unknown; conflicts), and the audit reconciles a run to an unexplained difference of 0.
- Conditions: the counters of the processes involved are complete for the run (a restarted process resets its counters; the audit then reports best effort).
- Mechanism: per-layer metrics (`ep_idem_requests_total`, `ep_sink_duplicates_total{cause}`, ...) and `make audit` check J: committed - new rows - new dead letters - redelivered - unknown = 0 (S3-F3).
- Proved by: [test: G9.replay-api] [test: G9.redelivered] [test: G9.reprocessed] [test: G9.conflict] [test: G9.metrics-present] [test: G9.audit-resend] [test: G9.audit-redelivered]
- Not guaranteed: an exact split between "reprocessed" and "redelivered" for a duplicate whose first copy came from another consumer group (it shows as unknown).

### G10 Schema compatibility
- Claim: every value on `events.raw` is Avro with a registered schema id, and no schema change that breaks a reader or a writer can be registered: the whole history of each subject is checked under `FULL_TRANSITIVE`, in CI. A record with an unknown schema id is dead-lettered, a registry outage never is.
- Conditions: schemas registered by the deploy step (`make schemas`), never by the services; the CI `schema-check` job passes.
- Mechanism: Confluent wire format, own registry client, producer looks its id up once, consumer prefetches writer schemas (S3-F7); `tools/schema-check` with compatible and incompatible fixtures.
- Proved by: [test: G10.history] [test: G10.fixtures] [test: G10.avro-e2e] [test: G10.unknown-id] [test: G10.registry-down-producer] [test: G10.registry-down-startup] [test: G10.registry-down-consumer]
- Not guaranteed: the meaning of fields across versions (compatibility is structural); payload content (the tenant payload travels as JSON text and is not schema-checked).

## Not guaranteed

Each of these is out of scope on purpose, with the reason:

- **Global ordering** across tenants or partitions: one partition per key is what scales (G4).
- **Exactly-once delivery** anywhere: duplicates travel and are absorbed or counted ([where exactly-once stops](#where-exactly-once-stops)).
- **API dedup beyond the Redis window, after Redis loss, or with `IDEM_FAILURE_POLICY=open` while Redis is down:** storage dedup still holds (G2); the API layer is an optimization that reduces duplicate traffic.
- **Visibility latency:** an accepted event is visible in Postgres *eventually*. The target (5 s, `ep_event_freshness_seconds`) is a measured objective, not a guarantee: a slow or down database grows the lag without losing data.
- **Ordering of `config.changed` with `RELAY_MODE=multi`** (G7), and exactly-once publishing of it.
- **Durability under weaker settings** (`KAFKA_ACKS=0/1`, `min.insync.replicas=1`) or with two replicas lost at once.
- **Retention:** nothing is kept in Kafka beyond the topics' retention (24 h `events.raw`, 7 days `events.dlq` and the retry tiers); Postgres is the system of record.
- **Anything without a test in this document.**

## Where exactly-once stops

- **Kafka's idempotent producer** (on by default) removes duplicates caused by the producer's *own* retries to a broker, within one producer session. It does not help when the API crashes after producing and the client retries (a new request), or when a new producer instance sends again.
- **Kafka transactions** would make read-process-write *inside Kafka* atomic (consume, produce, commit offsets together). They cannot include the Postgres write, so they would not make the sink exactly-once; EventPulse does not use them.
- **What EventPulse gives:** at-least-once delivery end to end (a 202 means acknowledged by Kafka on 2 replicas; offsets are committed only after the database commit), plus idempotent storage on `(tenant_id, event_id)`. Result: an **exactly-once effect in Postgres**, and nothing more. Downstream consumers of `events.raw`, `events.dlq` and `config.changed` see duplicates and must be idempotent themselves (dedup on `event_id`, or apply by version).

## Crash matrix

The generated table [generated/crash-matrix.md](generated/crash-matrix.md) (data: [generated/crash-matrix.json](generated/crash-matrix.json)) has one row per named crash point (`packages/fault-injector/src/points.ts`): consumer, API, retry tier, DLQ, replay tool, outbox relay and admin API. For each: the process is SIGKILLed at that exact point, restarted, and the pipeline is run to the end; the row records duplicates produced, records processed again, stored twice (must be 0), lost (must be 0) and recovery time. Its header carries the run date and commit. It is regenerated by every `make correctness` and is part of `[test: G2.crash-matrix]`; `guarantee-check` fails if it is older than the last change to the list of crash points, misses a point, or shows a loss or a double store.

## How this is checked

| Where | What runs | Result |
| --- | --- | --- |
| Locally, `make test-e2e` | consumer-sink and ingest-api e2e suites (Kafka, Postgres test database, Redis) | `docs/generated/test-reports/{consumer-sink-e2e,ingest-api-e2e}.json` |
| Locally, `make correctness` | the correctness suite: crash matrix, replays, retry and DLQ, DLQ replay tool, outbox, event time, durability, ordering | `docs/generated/test-reports/correctness.json`, `docs/generated/crash-matrix.{md,json}` |
| Locally or CI, `make guarantees` | `tools/guarantee-check` | fails on any problem listed at the top |
| CI, job `guarantees` | `make guarantees` on the committed files | blocks the merge |

Each stamped report records the commit and a hash of every source and test file it ran against. In CI the checker recomputes that hash from the checkout: if any source or test file changed after the suites ran (for example a test id was renamed), the report is stale and the job fails until the suites are run again and their reports committed. The infrastructure-heavy suites are not run in CI (they need the Kafka cluster, Redis, the registry and Postgres); the committed reports are the evidence, and the hash makes them impossible to reuse for other code.
