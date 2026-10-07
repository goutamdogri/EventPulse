# EventPulse Stage 3: Correctness

*Duration: 3-4 weeks. This is the stage most freshers never reach. Goal: turn the duplicates, losses, and ambiguities in your Stage 2 failure ledger into explicit guarantees, each backed by a test you can run.*

## 1. Objective and Motivating Pain

Stage 2 made the pipeline fast. Its failure ledger showed it is not yet correct: consumer crashes and rebalances cause duplicates, client retries create duplicate events, a poison message needed an ad hoc fix, events can arrive late and out of order with no policy, and nothing governs the message format.

By the end of this stage you must be able to:

1. State, in a written delivery guarantees document, exactly what EventPulse promises and what it does not, with a test for every promise.
2. Explain why "exactly once" is really at-least-once delivery plus idempotent processing, and demonstrate it by replaying the same events three times with no double counting.
3. Implement and break the main correctness patterns: idempotency keys, dedup layers, the transactional outbox, retry and dead-letter handling, and schema compatibility rules.
4. Measure the cost of correctness (latency, memory, throughput) so you can justify each mechanism.

**Predictions to write down first:** after a hard crash at each step of the consumer, how many duplicates will exist before dedup and how many after? Which failure point will surprise you? How much throughput will dedup cost?

## 2. Prerequisites and Setup

| Item | Requirement |
| --- | --- |
| Stages 0-2 | Complete, with the Stage 2 failure ledger and audit script |
| Redis | Single instance in docker-compose for now (clustering and failover come in Stage 5) |
| Schema registry | Confluent Schema Registry (or a compatible one) in docker-compose |
| Serialization | Avro for the first implementation because registry compatibility rules and tooling are the most mature; Protobuf is a Stage 8 comparison. Confirm current Node library support before committing (for example `@kafkajs/confluent-schema-registry` or a maintained alternative) |
| Testing | `fast-check` (property-based tests), a fault-injection helper, toxiproxy for Redis and registry faults |
| Postgres | Existing instance; you will add unique constraints, an outbox table, and a DLQ metadata table |

### Fault injection helper

Build a small `FaultInjector` early. It reads named crash points from an environment variable or a debug endpoint (for example `crash:consumer:after-db-commit`) and calls `process.kill(process.pid, 'SIGKILL')` when execution reaches that point. Every failure experiment in this stage depends on being able to crash a process at an exact step, repeatably.

## 3. Learning Plan

### Delivery semantics, precisely

- **At-most-once, at-least-once, exactly-once.** Real systems give at-least-once delivery. "Exactly once" means the *effect* happens once, achieved by idempotent processing or by atomic commits.
- **Why duplicates are inevitable:** any time a sender does not know whether an action succeeded (timeout, crash after acting but before acknowledging), it must retry or risk loss. This is the two generals problem showing up in practice.
- **Kafka transactions and exactly-once semantics (EOS):** guarantee atomic read-process-write *within Kafka* (consume from a topic, produce to another, commit offsets in one transaction). They do not extend to an external database. Know exactly where the boundary is.
- **Idempotent producer vs idempotent consumer:** the producer setting stops duplicates from broker-level retries inside a session. It does not stop an application retrying a request or a consumer reprocessing after a crash.

### Idempotency

- An operation is idempotent if repeating it has the same effect as doing it once.
- **Idempotency key design:** who generates it (client), its scope (tenant plus key), its lifetime (TTL), and what happens when the same key arrives with a different payload (a client bug that must be reported, not silently accepted).
- **Dedup stores:** Redis (`SET key value NX EX ttl`, fast, bounded window, can be lost) vs a Postgres unique constraint (durable, slower, unbounded). Know the failure window of each.
- **Natural vs synthetic keys:** `event_id` supplied by the producer vs a hash of content.

### Dual writes and the outbox pattern

- **The dual-write problem:** writing to a database and to Kafka as two separate steps can leave them inconsistent if the process dies between them.
- **Transactional outbox:** write the business change and an outbox row in one database transaction; a separate relay publishes outbox rows to Kafka and marks them published. Delivery is at-least-once, so consumers must be idempotent.
- **Relay options:** a polling publisher (`SELECT ... FOR UPDATE SKIP LOCKED`) vs change data capture (for example Debezium reading the WAL). Start with polling; understand CDC as the production-scale alternative.
- **Inbox pattern:** the consumer-side mirror: record processed message ids in the same transaction as the side effect.

### Retries and dead letters

- **Transient vs permanent failures.** Retrying a permanent failure forever is a bug; giving up on a transient one loses data.
- **Backoff with jitter** and **bounded attempts.**
- **Retry topics and delayed processing:** park a failed message on a retry topic with a `not_before` header so the main partition keeps moving.
- **Dead-letter queue (DLQ):** a quarantine with enough metadata to diagnose and replay (original topic, partition, offset, error class and message, attempts, timestamps).
- **Replay tooling:** a DLQ you cannot safely replay is a graveyard.

### Schema management

- **Why:** producers and consumers deploy independently; an uncontrolled format change breaks someone.
- **Registry and subjects:** each topic's value schema is a subject with numbered versions. Messages carry a schema id.
- **Compatibility modes:** BACKWARD (new schema can read old data; upgrade consumers first), FORWARD (old schema can read new data; upgrade producers first), FULL, and their TRANSITIVE variants. Know which changes each allows (adding a field with a default, removing an optional field, renaming, changing a type).
- **Policy:** compatibility is checked automatically in CI, not by convention.

### Time

- **Event time vs processing time**, out-of-order and late events. Stage 4 builds windows and watermarks; in this stage you only need storage and counting that stay correct regardless of arrival order.

### Reading

DDIA Chapter 8 (faults, unreliable networks, timeouts), Chapter 7 (transactions, isolation) at skim level, and Chapter 11 (stream processing, idempotence, exactly-once). Also read Pat Helland's paper "Idempotence Is Not a Medical Condition" and a public write-up of how a payments company designs idempotency keys. In `notebook/correctness-notes.md`, write one paragraph on where the "exactly once" boundary lies in your own system.

## 4. Requirements

| ID | Requirement |
| --- | --- |
| S3-F1 | Ingest is idempotent: the same `(tenant, Idempotency-Key or event_id)` accepted repeatedly produces one logical event; a reused key with a different payload returns a conflict error |
| S3-F2 | The sink is idempotent: replaying any range of the log leaves stored data unchanged |
| S3-F3 | Duplicates are counted and visible (metrics), never silently swallowed |
| S3-F4 | Config changes that must also publish an event use a transactional outbox (first use: tenant config change events, later used for cache invalidation) |
| S3-F5 | Failed messages follow bounded retry with backoff and jitter, then go to the DLQ with full diagnostic metadata |
| S3-F6 | A replay tool re-injects DLQ messages safely, with filters and a dry-run mode |
| S3-F7 | All events on `events.raw` use a registered Avro schema; incompatible changes are rejected in CI |
| S3-F8 | Late and out-of-order events are stored correctly, keeping both `occurred_at` and `received_at` |
| S3-F9 | A published delivery guarantees document, with each claim mapped to an automated test |

### Idempotency semantics

| Situation | Result |
| --- | --- |
| First time key seen | Process, return 202 |
| Same key, same payload, original still in progress | 409 with `Retry-After` (or short wait; decide and document) |
| Same key, same payload, original complete | Return the original result (202), no new event |
| Same key, different payload | 422 conflict |
| Key older than the dedup window | Treated as new at the API layer; the sink's unique constraint still protects storage |

## 5. Design

```
Client --(Idempotency-Key)--> API
   |  Layer 1: Redis  SET idem:{tenant}:{key} <state:hash> NX EX 86400
   |  validate against schema, serialize to Avro (id from registry)
   v
Kafka events.raw (key = tenant_id)
   v
Sink consumer:
   try: BEGIN
          INSERT INTO events ... ON CONFLICT (tenant_id, event_id) DO NOTHING RETURNING event_id
          -> inserted vs duplicates counted
        COMMIT, then commit offsets
   fail(transient): -> events.retry.{tier} (not_before header)
   fail(permanent) or attempts exhausted: -> events.dlq (with metadata)

Tenant config change (admin API):
   BEGIN; UPDATE config; INSERT INTO outbox; COMMIT
   Relay: poll outbox -> Kafka config.changed -> mark published
```

### Two dedup layers, deliberately

- **Layer 1 (API, Redis):** cheap, early, saves Kafka and storage work, but bounded by TTL and lost if Redis is lost. It is an optimization.
- **Layer 2 (sink, Postgres unique key):** the source of truth. Correctness never depends on Layer 1.

A key design decision to record as an ADR: where does correctness live, and why not rely on one layer alone. Also evaluate the alternative of storing consumer offsets in Postgres inside the same transaction as the insert, which gives exactly-once effects for that sink without relying on keys.

### Schema change

The primary key changes from `event_id` to `(tenant_id, event_id)` so tenants cannot collide or probe each other's ids.

```sql
ALTER TABLE events DROP CONSTRAINT events_pkey;
ALTER TABLE events ADD PRIMARY KEY (tenant_id, event_id);

CREATE TABLE outbox (
  id           bigserial PRIMARY KEY,
  aggregate_id text        NOT NULL,
  topic        text        NOT NULL,
  msg_key      text        NOT NULL,
  payload      bytea       NOT NULL,
  headers      jsonb       NOT NULL DEFAULT '{}',
  created_at   timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE dlq_meta (
  id bigserial PRIMARY KEY, source_topic text, source_partition int, source_offset bigint,
  error_class text, error_message text, attempts int,
  first_failed_at timestamptz, last_failed_at timestamptz, replayed_at timestamptz
);
```

### Repo additions

```
schemas/events/v1.avsc, v2.avsc (evolution tests)
libs/idempotency/ (middleware, redis store)
libs/outbox/ (writer, polling relay)
libs/retry/ (classifier, backoff, retry-topic router)
libs/fault-injector/
tools/dlq-replay/
tools/schema-check/ (CI compatibility script)
tests/correctness/ (replay, reorder, crash matrix, property tests)
docs/delivery-guarantees.md, docs/adr/
```

## 6. Step-by-Step Build Plan

1. **Baseline the ledger.** Re-run the Stage 2 audit and write the list of defects you will fix.
2. **Fault injector and crash points.** Add named crash points at every step of API and consumer paths before changing behavior.
3. **Sink idempotency.** Change the primary key, switch to `RETURNING`, add duplicate counters. Re-run the crash matrix.
4. **API idempotency.** Redis middleware with the state machine in section 4.
5. **Schema registry and Avro.** Define v1, register it, serialize at the producer, deserialize at the consumer, and add the CI compatibility check.
6. **Retry and DLQ.** Error classifier, retry tiers, DLQ writer, replay tool.
7. **Outbox and relay.** First consumer of the outbox: the `config.changed` event.
8. **Correctness test suite.** Property tests, replay, reorder, crash matrix.
9. **Run the break-it catalog.**
10. **Measure the correctness tax** (section 9).
11. **Write the delivery guarantees document** and ADRs.

### Sink transaction sketch

```sql
INSERT INTO events (tenant_id, event_id, type, occurred_at, schema_version, payload)
SELECT * FROM unnest($1::text[], $2::uuid[], $3::text[], $4::timestamptz[], $5::int[], $6::jsonb[])
ON CONFLICT (tenant_id, event_id) DO NOTHING
RETURNING event_id;
-- duplicates in this batch = batch size - rows returned
```

### API idempotency sketch

```ts
const k = `idem:${tenantId}:${key}`;
const hash = sha256(canonicalJson(body));
const created = await redis.set(k, `inflight:${hash}`, 'EX', 86400, 'NX');
if (!created) {
  const v = await redis.get(k);              // 'inflight:<h>' or 'done:<h>'
  if (!v) return retryOnce();                // expired between calls
  const [state, h] = v.split(':');
  if (h !== hash) throw new UnprocessableEntityException('idempotency key reused');
  if (state === 'inflight') throw new ConflictException();   // 409 + Retry-After
  return { event_id, duplicate: true };       // original result, nothing new produced
}
try { await produce(event); await redis.set(k, `done:${hash}`, 'EX', 86400); }
catch (e) { await redis.del(k); throw e; }   // let the client retry
```

Think hard about the gaps in this sketch and write them down: what if the process dies after `produce` but before `done`? What if Redis is unreachable (fail open or closed)? The experiments below will find them.

### Outbox relay sketch

```sql
SELECT id, topic, msg_key, payload, headers FROM outbox
WHERE published_at IS NULL ORDER BY id LIMIT 100
FOR UPDATE SKIP LOCKED;
-- publish to Kafka, then:
UPDATE outbox SET published_at = now() WHERE id = ANY($1);
```

Note the semantics you are choosing: publish-then-mark means a crash in between republishes (at-least-once). Note also that multiple relays with `SKIP LOCKED` can reorder messages for the same aggregate; decide whether you need a single relay, partitioned relays, or consumer-side ordering handling.

### Retry routing sketch

Classify errors into transient (connection refused, timeout, deadlock, serialization failure) and permanent (schema mismatch, validation failure, constraint violation caused by bad data). On a transient failure, publish to the next retry tier with `not_before = now + backoff(attempt) + jitter` and an incremented attempt header. On a permanent failure or when attempts are exhausted, publish to `events.dlq` and record `dlq_meta`. Retry consumers pause their partition until `not_before` instead of sleeping inside the handler.

## 7. Test Plan

| Layer | Tests | Pass condition |
| --- | --- | --- |
| Unit | Idempotency state machine (all rows of the semantics table), error classifier, backoff with jitter bounds, outbox writer | Spec behavior |
| Property-based (`fast-check`) | For any generated list of events containing duplicates, reordering, and delayed items, the stored unique set equals the expected set and counts are identical to a reference computation | No counterexample in thousands of cases |
| Replay | Run the same N events through the pipeline 3 times | Row count and any aggregate unchanged; duplicate counter equals N x 2 |
| Reorder and late | Shuffle events; delay a subset by minutes and hours | Same stored result; `occurred_at` preserved |
| Crash matrix | SIGKILL at each named crash point, restart, run the audit | Table of crash point vs duplicates, losses, final state |
| Concurrency | 50 parallel requests with the same key | Exactly one event produced |
| Schema evolution | Add optional field with default, remove optional field, rename field, change type | Registry accepts or rejects as the compatibility mode dictates; old and new consumers behave as predicted |
| DLQ | Permanent failure, transient failure that recovers, transient that never recovers | Goes to retry tiers, then DLQ, with correct metadata and replay works |
| Outbox | Kill relay between publish and mark; kill before publish; two relays | No loss; duplicates visible; ordering behavior documented |
| End-to-end audit | Extended Stage 2 audit including duplicates dropped, DLQ count, outbox lag | Reconciles: sent = stored + dlq + still in flight |

The crash-matrix output is the raw material for the delivery guarantees document, so make it a generated table, not a hand-written one.

## 8. Break-It Experiment Catalog

Write the hypothesis first. Run the audit after each experiment. Keep every result, including the ones that embarrass your design.

### C1 Triple replay

- **Hypothesis:** replaying the same 100,000 events three times leaves storage unchanged and increments the duplicate counter by exactly 200,000.
- **How:** re-send via the API with the same ids; separately reset consumer offsets to earlier positions.
- **Observe:** row counts, aggregate checks, duplicate metrics, time cost of dedup.
- **Explains:** idempotency at both entry and sink.

### C2 Out-of-order and late events

- **Hypothesis:** storage is order-insensitive; any ordering gives the same final data, but any computation that depends on arrival order will be wrong.
- **How:** shuffle a dataset; delay 10% by 1 minute, 1 hour, and 24 hours; compare against the expected result.
- **Observe:** stored data equality, `received_at - occurred_at` distribution, which queries would have been wrong if written naively.
- **Explains:** why Stage 4 needs event-time windows and watermarks.

### C3 Crash at every step

- **Hypothesis:** crash points before the database commit lose nothing and duplicate nothing visible; crash between database commit and offset commit duplicates processing but not storage.
- **How:** run the crash matrix across API (before produce, after produce before response), consumer (before parse, before insert, after insert before commit), and relay (after publish before mark).
- **Observe:** per crash point: duplicates produced, duplicates stored, events lost, recovery time.
- **Explains:** exactly where at-least-once duplicates arise, with proof.

### C4 Dedup window expiry

- **Hypothesis:** after the Redis TTL passes, a replayed key is accepted by the API again; the sink still prevents duplicate storage, but anything counting before the sink (for example a stream processor, Stage 4) could double count.
- **How:** use a short TTL, replay after expiry, watch each layer.
- **Observe:** where the second copy is stopped and what it cost on the way.
- **Explains:** why dedup windows must be sized against realistic retry and replay horizons, and why the sink is authoritative.

### C5 Redis loss and failure policy

- **Hypothesis:** flushing or killing Redis mid-run causes duplicates to flow to Kafka but not into storage; failing closed makes ingest unavailable while Redis is down, failing open keeps ingest available at the cost of duplicate load.
- **How:** `FLUSHALL`, kill Redis, add toxiproxy latency, for both policies.
- **Observe:** ingest availability, duplicate rates, latency, sink duplicate counter.
- **Explains:** an explicit, documented failure policy per dependency.

### C6 Key reuse with different payload

- **Hypothesis:** the second request is rejected with 422 and nothing is produced.
- **How:** send two different bodies with one key; also two requests that differ only by key order or whitespace (canonicalization matters).
- **Observe:** responses and what reached Kafka.
- **Explains:** why canonical hashing is part of the contract.

### C7 Concurrent identical requests

- **Hypothesis:** with `SET NX`, exactly one of 50 simultaneous identical requests produces an event; others get 409 or the stored result.
- **How:** fire parallel requests; also crash the winner mid-flight and observe what the losers experience.
- **Observe:** produced count, stuck `inflight` keys, recovery behavior after the TTL.
- **Explains:** the in-progress state and why abandoned `inflight` keys need their own shorter TTL.

### C8 Outbox failure modes

- **Hypothesis:** killing the relay between publish and mark causes duplicate publication; killing before publish delays but never loses; two relays with `SKIP LOCKED` can reorder messages for one aggregate.
- **How:** crash points in the relay, run two relays against a stream of ordered changes.
- **Observe:** duplicates published, oldest unpublished age, order violations at consumers.
- **Explains:** at-least-once publication, and why consumers still need idempotency.

### C9 Retry storm and misclassification

- **Hypothesis:** classifying a permanent error as transient creates an endless retry loop that wastes capacity; classifying a transient error as permanent sends recoverable events to the DLQ.
- **How:** inject each misclassification; simulate a long database outage and watch retry tiers.
- **Observe:** retry topic volume, amplification factor, DLQ growth, effect on healthy traffic.
- **Explains:** the cost of getting error taxonomy wrong; then replay the DLQ with your tool (dry run first) and verify idempotent recovery.

### C10 Schema compatibility

- **Hypothesis:** the registry rejects incompatible changes; a compatible change lets old and new consumers coexist; a renamed field without an alias breaks consumers.
- **How:** attempt each change type under BACKWARD and FULL modes; run old consumer against new data and new consumer against old data.
- **Observe:** registry responses, consumer errors, CI check output.
- **Explains:** the evolution rules you will write into policy.

### C11 Registry unavailable

- **Hypothesis:** producers with cached schema ids keep working for known schemas but cannot register new ones; consumers fail on unknown ids.
- **How:** stop the registry under load, then start a fresh consumer.
- **Observe:** errors, whether caches save you, restart behavior.
- **Explains:** a hidden dependency and its failure policy.

### C12 Poison inside a batch

- **Hypothesis:** one bad message fails the whole batch; naive retry reprocesses good messages repeatedly.
- **How:** put one bad message among 500 good ones.
- **Observe:** retry volume, time to isolate. Then implement bisect-and-retry or per-message fallback and measure.
- **Explains:** batch efficiency versus failure isolation.

### C13 Client clock skew

- **Hypothesis:** events with `occurred_at` far in the future or past distort time-based queries unless policy limits them.
- **How:** send timestamps years off and seconds in the future.
- **Observe:** stored values, partition placement later in ClickHouse.
- **Explains:** validation bounds and the role of `received_at`.

### C14 The correctness tax

- **Hypothesis:** each mechanism costs measurable throughput, latency, or memory.
- **How:** run the Stage 2 step test with each mechanism toggled on and off: Redis dedup, unique key, Avro serialization, registry lookups, outbox writes.
- **Observe:** knee movement per mechanism, Redis memory per million keys, extra latency at p99, unique index size.
- **Explains:** the price of each guarantee, so you can say why it is worth it.

## 9. Metrics and Diagnostics

- **Duplicates:** dropped at API, dropped at sink, reprocessed messages after restart.
- **Idempotency:** hits, conflicts, in-progress rejections, Redis latency and memory, fail-open and fail-closed counts.
- **Outbox:** unpublished count, age of oldest unpublished row, publish rate, republish count.
- **Retry and DLQ:** attempts by tier, retry lag, DLQ size and age, replay success rate.
- **Schema:** registry errors, schema ids in use, consumer deserialization failures.
- **Audit output:** sent, accepted, stored, duplicates dropped, DLQ, in flight, unexplained difference (must be zero).

## 10. Artifacts

**1. `docs/delivery-guarantees.md`** (the main artifact). For each claim use a fixed format: *claim, conditions it depends on, mechanism, test that proves it, what is explicitly not guaranteed.* Starting points to adapt (write only what your tests prove):

- **G1 Durability:** once the API returns 202 and Kafka replication conditions hold, the event is not lost.
- **G2 Storage idempotency:** any number of deliveries of one `(tenant, event_id)` results in one stored row.
- **G3 Replay safety:** replaying any range of the log does not change stored data.
- **G4 Ordering:** events with the same key are ordered within a partition; no global order is promised.
- **G5 Late data:** events are stored regardless of lateness; query semantics for lateness are defined in the analytics layer.
- **G6 Failure handling:** a failing message cannot block a partition indefinitely and is never silently dropped; it is retried within limits, then quarantined with metadata.
- **G7 Config events:** a config change is published at least once if and only if its database transaction committed.
- **Not guaranteed:** global ordering, deduplication beyond the Redis window at the API layer, visibility latency below the freshness target, anything not covered by a test.

**2. Crash-matrix table:** crash point vs outcome, generated by the test suite.

**3. ADRs:** dedup placement, Redis vs Postgres for idempotency, Avro vs Protobuf, polling outbox vs CDC, offset-in-database vs key-based dedup, retry topics vs in-process retry. Each with rejected alternatives and your measurements.

**4. Runbooks:** DLQ triage and replay; schema change procedure; outbox stuck procedure.

**5. Correctness tax report:** the C14 results as a table and chart.

**Optional blog post:** "Exactly-once is a lie (and what to build instead)" built from your crash matrix.

## 11. Common Pitfalls

- Writing the guarantees document before running the tests, then bending tests to match claims.
- Letting Redis dedup become the source of truth.
- Counting duplicates only at the sink, hiding retry storms upstream.
- Retrying inside the handler with sleeps, which blocks partitions and trips rebalances.
- Treating all errors the same instead of classifying transient and permanent.
- Registering schemas manually with no CI check, so compatibility depends on memory.
- Forgetting that the outbox relay is itself at-least-once and unordered unless designed otherwise.
- Assuming Kafka's exactly-once feature covers your database writes.
- Dedup keys that are not canonical (field order or whitespace changes the hash).
- Not bounding DLQ and retry volume, so a bad deploy floods Kafka and Postgres.
- Claiming a guarantee you tested only on the happy path.

## 12. Exit Criteria

- [ ] Replaying the same data 3 times leaves storage unchanged, proven by an automated test.
- [ ] Out-of-order and late-event tests pass.
- [ ] Crash matrix covers every named crash point and its results are recorded.
- [ ] Property-based tests run clean.
- [ ] Idempotency key semantics all implemented and tested, including conflicts and concurrency.
- [ ] Outbox with relay works and its failure modes are documented.
- [ ] Retry tiers, DLQ, and a replay tool (with dry run) work and are tested.
- [ ] Schema registry in use with a CI compatibility check, and evolution experiments recorded.
- [ ] Experiments C1 through C14 each have a notebook entry.
- [ ] Delivery guarantees document published, every claim linked to a test.
- [ ] Correctness tax measured.
- [ ] Repo tagged `stage-3-complete`.

**Self-test questions (answer aloud, without notes):**

1. Why can no system with timeouts avoid duplicates, and what do you do instead?
2. Where exactly does Kafka's exactly-once guarantee stop, and why does your Postgres sink still need its own idempotency?
3. Your Redis is flushed during an incident. What goes wrong and what does not, and why?
4. Describe the dual-write problem and how the outbox fixes it. What does the outbox not fix?
5. A permanent failure is classified as transient. Walk through what happens to the system.
6. A producer adds a required field with no default. What does the registry do and why?

## 13. Handoff to Stage 4

Stage 3 gives you a pipeline whose storage is correct under duplicates, replays, crashes, and reordering. What it does not do yet:

- Compute anything over time. Aggregates in Postgres are slow and arrival-order dependent as soon as you want windows.
- Handle lateness as a policy: when is a window final, and what happens to events after it closes?
- Keep stateful processing correct: your dedup protects stored rows, but a stream processor holding counts in memory needs its own idempotency and checkpointing.
- Serve analytical queries over hundreds of millions of rows quickly.

Stage 4 introduces event-time windows, watermarks, and checkpointed state, and a ClickHouse sink where deduplication works very differently from a Postgres unique constraint. Before starting, write down how you expect duplicate handling to change when the sink is an analytical column store with eventual merges.

When you are ready, say **Stage 4**.
