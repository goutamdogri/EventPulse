# Stage 2 Server Build Plan: Decouple with a Queue

*Source: `PRDs/EventPulse Stage 2 Decouple with a Queue.md`. Scope: build the application server (ingest API, Kafka, consumer sink, audit tooling) so that all seven functional requirements S2-F1 to S2-F7 are delivered. The break-it experiments (K1 to K12) are out of scope here. This plan only makes sure the server exposes the knobs and hooks those experiments will need.*

---

## 0. How to use this plan

- There are **7 steps, one per requirement**. Each step owns its requirement completely: code, config, tests, docs. When a step is done, its requirement is done and nothing is left for later steps.
- Do the steps **in order**. Each one builds on the previous ones (step 2 needs the topic from step 1, step 3 hardens the consumer from step 2, and so on).
- Every step ends with a **"Done when"** checklist. Don't start the next step until every box is ticked.
- Commit at the end of each step (e.g. `S2-F1: produce to events.raw, 202 after ack`).

| Step | Requirement | One-line summary |
| --- | --- | --- |
| 1 | S2-F1 | Kafka cluster + topics + shared config + producer + micro-batcher; `POST /v1/events` returns 202 only after Kafka acks |
| 2 | S2-F2 | New `consumer-sink` service: group `sink-postgres` reads `events.raw` and writes batches to Postgres |
| 3 | S2-F3 | Offset commit ordering: offsets committed only after the DB transaction commits (at-least-once, proven) |
| 4 | S2-F4 | Poison messages: show they block a partition, then bounded retry + `events.dlq` |
| 5 | S2-F5 | Kafka unavailable: 503 + `Retry-After`, bounded memory, recovery without restart |
| 6 | S2-F6 | Producer, batcher and consumer metrics on `/debug/stats` (and `/metrics`) |
| 7 | S2-F7 | One-command audit: sent vs acknowledged vs Kafka offsets vs distinct Postgres rows |

### Target repo layout after Stage 2

```
infra/compose/docker-compose.kafka.yml      # 3 KRaft brokers + topic init (step 1)
packages/kafka-config/                      # shared topic names, client factory, env parsing (step 1)
services/ingest-api/src/kafka/              # producer, micro-batcher, errors (steps 1, 5, 6)
services/consumer-sink/                     # consumer, parser, batch writer, DLQ publisher (steps 2-4, 6)
db/migrations/0004_events_stored_at.sql     # stored_at column for freshness (step 2)
scripts/                                    # audit.sh, lag.sh, produce-poison.sh, dlq-peek.sh, ... (steps 4, 7)
loadtests/k6/ingest-kafka.js                # minimal load script used by the audit (step 7)
docs/api-contract.md                        # what 202 means now (step 1)
```

Note: `pnpm-workspace.yaml` already includes `packages/*` and `services/*`, so `packages/kafka-config` and `services/consumer-sink` are picked up automatically. The folder-structure PRD uses `packages/kafka-config` (the Stage 2 PRD says `libs/`), so `packages/` is used here.

### Global conventions (apply in every step)

- ESM everywhere, relative imports end in `.js`, tests on Vitest (same as `ingest-api`).
- All new settings come from env vars, parsed once in a `loadConfig()` style function with defaults and validation (same pattern as `services/ingest-api/src/config.ts`). Add every new var to `.env.example`.
- No unbounded queue anywhere: batcher, consumer prefetch, retry loops, DLQ buffer.
- Every new metric gets a Prometheus series **and** a field in `/debug/stats` (step 6 completes this, but name metrics consistently from step 1: prefix `ep_`).
- Record container CPU/memory limits in the compose files. The Stage 2 report needs them.

---

## Step 1: S2-F1. Produce to `events.raw`, return 202 only after Kafka acknowledges

> **Requirement:** `POST /v1/events` validates, produces to `events.raw`, and returns 202 only after Kafka acknowledges the write.

### 1.1 What to build

**A. Kafka cluster (`infra/compose/docker-compose.kafka.yml`)**

- 3 brokers, `apache/kafka:<pinned stable>` image, KRaft combined mode (`broker,controller`), no ZooKeeper. Same `CLUSTER_ID` on all three (generate once with `kafka-storage.sh random-uuid` and keep it in `infra/compose/.env`).
- **Two client listeners per broker**, because the API and consumer run on the host (pinned with `taskset`) while tools may run inside Docker:
  - `INTERNAL://kafkaN:19092`, advertised as `kafkaN:19092` (container-to-container, CLI tools in containers).
  - `EXTERNAL://:9092` / `:9094` / `:9096`, advertised as `localhost:9092`, `localhost:9094`, `localhost:9096` (host clients).
  - `CONTROLLER://:9093` for the KRaft quorum.
  - `KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL`, a `LISTENER_SECURITY_PROTOCOL_MAP` covering all three.
  - Write down in `notebook/kafka-notes.md` what you had to change and why. The PRD calls this out as a learning exercise.
- Cluster defaults: `default.replication.factor=3`, `min.insync.replicas=2`, `offsets.topic.replication.factor=3`, `transaction.state.log.replication.factor=3`, `transaction.state.log.min.isr=2`, `auto.create.topics.enable=false` (so typos fail loudly instead of making 1-partition topics).
- Explicit `cpus`/`cpuset` + `mem_limit` per broker, plus a `KAFKA_HEAP_OPTS` that fits inside the limit. Pick cores that don't overlap the API/k6/Postgres cpusets already in `infra/compose/docker-compose.yml`.
- Named volumes per broker (so `docker kill` + restart keeps the log).
- Healthcheck per broker (e.g. `kafka-broker-api-versions.sh --bootstrap-server localhost:19092`).
- **`kafka-init` one-shot service** (depends on healthy brokers) that runs idempotent topic creation:
  - `events.raw`: partitions from `EVENTS_RAW_PARTITIONS` (default 6), RF 3, `min.insync.replicas=2`, `retention.ms=86400000` (24 h).
  - `events.dlq`: 3 partitions, RF 3, `min.insync.replicas=2`, longer retention (7 days).
  - Use `--if-not-exists`, then `--describe` both topics to the log.
- Add the Kafka brokers as Prometheus scrape targets later only if you add a JMX/kafka-exporter. That's optional and not required for F1.

**B. Shared package `packages/kafka-config`**

- `topics.ts`: `TOPIC_EVENTS_RAW = 'events.raw'`, `TOPIC_EVENTS_DLQ = 'events.dlq'`, `GROUP_SINK_POSTGRES = 'sink-postgres'`.
- `env.ts`: parse shared client settings: `KAFKA_BROKERS` (comma list, default `localhost:9092,localhost:9094,localhost:9096`), `KAFKA_CLIENT_ID`, connection/request timeouts, retry settings, log level.
- `client.ts`: `createKafka(cfg)` factory returning the client instance, so both services build the client the same way.
- `envelope.ts`: the message contract, used by both producer and consumer:
  - **Key:** `tenant_id` by default; `KAFKA_KEY_MODE=tenant|event` switches to `event_id` (K9 needs this knob).
  - **Value:** JSON `{ event_id, tenant_id, type, occurred_at, schema_version, payload }`.
  - **Headers:** `received_at` (ISO, set by the API at request time), `schema_version`, `request_id` (from `x-request-id` if present, else generated UUID).
  - `encodeEvent()` / `header names` constants. Decoding belongs in the consumer (step 2) because that's where poison handling lives.
- **Client library decision (do this first):** check the current maintenance status of `kafkajs` vs `@confluentinc/kafka-javascript` (which offers a KafkaJS-compatible API on librdkafka). Record the choice and reason as `docs/adr/000X-kafka-client.md`. Whatever you choose, only `packages/kafka-config` and the two `kafka/` folders import it, so swapping later (Stage 8) touches few files.

**C. Producer in ingest-api (`services/ingest-api/src/kafka/`)**

- `kafka.module.ts`: global Nest module that provides the producer and the batcher, connects on `onModuleInit`, disconnects on `onApplicationShutdown` **after** flushing the batcher (graceful shutdown must not drop queued events).
- `producer.service.ts`: wraps the client producer. Settings from env so K2 can sweep them without code changes:
  - `KAFKA_ACKS` (`-1` default = all, `1`, `0`)
  - `KAFKA_COMPRESSION` (`none` default, `gzip`, plus `lz4`/`zstd` if the client supports them)
  - `KAFKA_IDEMPOTENT` (`true` default; note it forces `acks=all` and limits in-flight requests)
  - `KAFKA_REQUEST_TIMEOUT_MS`, `KAFKA_RETRIES`
- `micro-batcher.ts`: a plain class, no Nest dependencies, so it is unit-testable with a fake producer. Based on the PRD sketch, with these decisions made explicitly:
  - Flush on `BATCH_MAX_MESSAGES` (default 500) **or** `BATCH_MAX_WAIT_MS` (default 5), whichever first.
  - Also flush on `BATCH_MAX_BYTES` (default ~900 KB) so a batch never exceeds the broker's `message.max.bytes` / client max request size. Track encoded byte size per message at enqueue.
  - Bounded queue `BATCH_MAX_QUEUE` (default 10 000). When full, reject immediately with `OverloadedError`. (Step 5 maps this to 503 and adds the Kafka-down behavior; step 1 only needs the bound to exist.)
  - `BATCH_MAX_IN_FLIGHT` (default 1, allow >1 for experiments). With >1 in flight, ordering across batches for the same key is not guaranteed. Document that.
  - **Partial failure rule:** one `send()` call = one outcome for every waiter in the batch. On success resolve all; on error reject all. The batcher never retries internally and never re-queues (the client's own retries plus the idempotent producer cover transient errors). Document that a rejected batch may still have been partly written, so a client retry can create duplicates. This is a known Stage 2 flaw for the ledger.
  - Every message is in exactly one batch: `splice` removes it from the queue before sending, so nothing is sent twice by the batcher and nothing is dropped.
  - `flush()` and `drain()` methods: `drain()` flushes everything and waits for in-flight sends, used on shutdown.
- `events.service.ts` / `events.controller.ts`: replace the Postgres insert with `batcher.enqueue(encodeEvent(tenantId, dto, headers))` and `await` it. Return 202 `{ event_id }` only after the promise resolves.
- Remove Postgres from the request path: `EventsModule` no longer injects `PG_POOL`; `DbModule` is no longer imported by the API (the Stage 1 path stays available via the Stage 1 git tag for comparison). Remove `DATABASE_URL` as a required var for the API.
- Keep the Stage 1 validation exactly as is (`CreateEventDto`, `ValidationPipe`, `BODY_LIMIT` 413 at the edge, `ApiKeyGuard`). The edge limits must stay below Kafka's limits so oversized events get 413 before Kafka sees them (K12).

**D. API contract (`docs/api-contract.md`)**

- 202 now means: durably appended to `events.raw` under the configured acks policy (with defaults: written to the leader and at least one more in-sync replica).
- What it no longer means: the event may not be queryable in Postgres yet. Visibility is eventual. State the target (e.g. "visible within N seconds under normal load") and say it's measured by the freshness metric from step 6.
- Responses: 202, 400, 401, 413, 503 (+ `Retry-After`), 500. Retry guidance: retry 503 and network errors with backoff and the **same `event_id`**; duplicates are absorbed by `event_id` at the sink.

### 1.2 What to cover

- Listener config works for host clients **and** in-container CLI tools.
- Topic has 6 partitions, RF 3, leader + 3 ISR for every partition.
- Key mode `tenant` and `event` both produce correct keys.
- Headers present on every record.
- 202 is never sent before the ack. 400/401/413 never reach Kafka.
- Graceful shutdown: SIGTERM with a non-empty queue flushes before exit.
- Batch byte cap respected even with max-size (64 KB) events.

### 1.3 Tests

| Type | Test | Pass condition |
| --- | --- | --- |
| Unit (batcher) | Flush on size: enqueue `maxMessages` items | exactly one `send` with all of them, timer cleared |
| Unit (batcher) | Flush on time: enqueue 1 item, advance fake timers by `maxWaitMs` | one `send` with 1 item |
| Unit (batcher) | Flush on bytes: items whose total exceeds `maxBytes` | split into batches each ≤ `maxBytes` |
| Unit (batcher) | Bounded queue: fill to `maxQueue`, enqueue one more | rejects with `OverloadedError`, queue length unchanged |
| Unit (batcher) | Error fan-out: fake `send` rejects | every waiter in that batch rejects with the same error; waiters in other batches unaffected |
| Unit (batcher) | No loss / no double send on failure: fail batch 1, succeed batch 2 | each message appears in exactly one `send` call; total sent count == total enqueued |
| Unit (batcher) | In-flight limit: `send` that hangs, `maxInFlight=1` | second batch not sent until first settles |
| Unit (batcher) | `drain()` | resolves only after queue empty and in-flight sends settled |
| Unit (envelope) | `encodeEvent` for both key modes | correct key, value JSON, all three headers |
| Unit (controller) | Fake batcher that resolves later | response not sent until the promise resolves; status 202 |
| E2E (real Kafka) | POST a valid event, then consume `events.raw` from the test | record found with right key, value, headers; offset increased by 1 |
| E2E | Invalid body / missing key / 65 KB body | 400 / 401 / 413, and topic end offsets unchanged |
| Manual | `kafka-topics.sh --describe --topic events.raw` | 6 partitions, RF 3, `min.insync.replicas=2`, ISR = 3 for all |
| Manual | `kafka-producer-perf-test.sh` with your event size and `acks=all` | runs; record the raw broker ceiling number (input for K1) |

Update `test/setup-env.ts` and `.env.test.example`: require `KAFKA_BROKERS` instead of `DATABASE_URL`. Replace `events-db-down.e2e-spec.ts` (step 5 adds the Kafka-down equivalent).

### 1.4 Done when

- [ ] `docker compose -f docker-compose.kafka.yml up -d` gives 3 healthy brokers and both topics, created by `kafka-init`.
- [ ] Client library ADR written.
- [ ] `POST /v1/events` returns 202 only after the ack, and the record is in `events.raw` with the right key and headers.
- [ ] Batcher unit tests (all rows above) pass.
- [ ] API no longer touches Postgres.
- [ ] `docs/api-contract.md` updated; listener notes written.

---

## Step 2: S2-F2. Consumer group `sink-postgres` writes batches to Postgres

> **Requirement:** A consumer group `sink-postgres` reads `events.raw` and writes batches to Postgres.

### 2.1 What to build

**A. New service `services/consumer-sink/`**

- Same tooling as `ingest-api` (TypeScript, ESM, Vitest, oxlint, prettier, `node --env-file`). Package name `@eventpulse/consumer-sink`.
- Build it as a NestJS app with a small HTTP server only for `/health`, `/metrics` and `/debug/stats` (step 6 fills the metrics). Consumer logic lives in plain classes so it can be unit-tested without Nest.
- `METRICS_PORT` env (default 8001) so several consumer instances can run on one host.
- Reuse `packages/kafka-config` for the client, topic and group names.
- Own Postgres pool (`pg`), with `PG_POOL_MAX`, `PG_CONN_TIMEOUT_MS`, `PG_STATEMENT_TIMEOUT_MS`, `application_name='consumer-sink'` and the idle-client `error` handler (copy the Stage 1 `DbModule` pattern).

**B. Consumer (`src/consumer/sink-consumer.ts`)**

- Group id `sink-postgres`, subscribes to `events.raw`.
- `fromBeginning` / reset policy from `CONSUMER_FROM_BEGINNING` (default `true` = `earliest`, so a new group doesn't skip data; K11 compares with `latest`).
- `autoCommit: false` from the very start. Never use auto-commit in this service.
- Group settings from env (K8 needs them): `CONSUMER_SESSION_TIMEOUT_MS`, `CONSUMER_HEARTBEAT_INTERVAL_MS`, `CONSUMER_REBALANCE_TIMEOUT_MS`, `CONSUMER_MAX_BYTES_PER_PARTITION`, `CONSUMER_MAX_BYTES`, `CONSUMER_MAX_WAIT_MS`, `CONSUMER_PARTITIONS_CONCURRENTLY` (default 1; keep ≤ `PG_POOL_MAX`), and the assignor choice if the client supports it.
- **Bounded prefetch / backpressure:** `maxBytesPerPartition` and `maxBytes` cap how much is fetched and held in memory. Processing happens inside `eachBatch`, so a partition isn't fetched again until its batch finishes. That is the backpressure. Also implement `pause()`/`resume()` of the topic when the DB is failing (wired up in step 4's retry logic).
- Graceful shutdown on SIGTERM/SIGINT: `consumer.disconnect()` (waits for the current batch), then close the pool, then exit 0.

**C. Parser (`src/sink/parse.ts`)**

- Decodes one Kafka message into a row: `{ event_id, tenant_id, type, occurred_at, schema_version, payload, received_at, kafka: { partition, offset } }`.
- Validates the same rules as the API DTO (UUID, type length 1-100, ISO 8601, `schema_version ≥ 1`, payload is an object) plus `tenant_id` present and `received_at` header parseable.
- Returns a result type `{ ok: true, row } | { ok: false, error }` per message, so one bad message never prevents parsing the others. (In this step a bad message simply throws out of the batch; step 4 turns that into DLQ handling. Keep the per-message result type now so step 4 is a small change.)

**D. Batch writer (`src/sink/batch-writer.ts`)**

- One multi-row insert per chunk, inside one transaction:

  ```sql
  INSERT INTO events (event_id, tenant_id, type, occurred_at, schema_version, payload, received_at)
  SELECT * FROM unnest($1::uuid[], $2::text[], $3::text[], $4::timestamptz[], $5::int[], $6::jsonb[], $7::timestamptz[])
  ON CONFLICT (event_id) DO NOTHING;
  ```

- Chunking: a fetched Kafka batch can be large, so split it into chunks of `SINK_MAX_ROWS` (default 1000). One transaction per chunk.
- Returns `{ attempted, inserted }` (`inserted = rowCount`). `attempted - inserted` = duplicates absorbed by `ON CONFLICT`. **Count it**, never hide it (step 6 exposes it).
- `received_at` now comes from the Kafka header (API receipt time), not `now()` in Postgres.

**E. Migration `db/migrations/0004_events_stored_at.sql`**

- Add `stored_at timestamptz NOT NULL DEFAULT now()` to `events`. Then `stored_at - received_at` is the end-to-end freshness of every row, queryable in SQL (the "eventual visibility within N seconds" property from the PRD). Down migration drops the column.
- Add a `migrate` script to consumer-sink (or keep running migrations from ingest-api, but since the API no longer needs the DB, move the migrate scripts to consumer-sink).

**F. Ops**

- `Makefile` targets: `stage2-consumer-start`, `stage2-consumer-start-pinned` (with `taskset`), `stage2-migrate`.

### 2.2 What to cover

- Several consumer instances in one group split the 6 partitions; extra instances beyond 6 get no assignment (log assignments on every group join).
- Consumer restart resumes from committed offsets, not from the start.
- Empty batches, single-message batches, batches larger than `SINK_MAX_ROWS`.
- Duplicate `event_id` inside the same chunk (`ON CONFLICT` handles it in one statement) and across chunks.
- DB pool sized for `CONSUMER_PARTITIONS_CONCURRENTLY`.

### 2.3 Tests

| Type | Test | Pass condition |
| --- | --- | --- |
| Unit (parser) | Valid message | correct row incl. `received_at` from header and partition/offset |
| Unit (parser) | Invalid JSON, bad UUID, oversized `type`, missing header, payload array, missing `tenant_id` | `{ ok: false }` with a clear error code/message, no throw |
| Unit (parser) | Mixed batch (valid + invalid) | valid rows returned, invalid listed separately |
| Unit (writer) | Chunking 2 500 rows with `SINK_MAX_ROWS=1000` | 3 transactions of 1000/1000/500 |
| Integration (writer, real Postgres) | Insert 100 rows | 100 rows, `inserted=100` |
| Integration (writer) | Insert the same 100 again | `inserted=0`, `attempted=100`, still 100 rows |
| Integration (writer) | Chunk with an in-chunk duplicate | one row, `attempted - inserted = 1` |
| E2E (API → Kafka → consumer → Postgres) | POST 1 000 events | all 1 000 rows present within a bounded delay (e.g. 10 s), `stored_at ≥ received_at` |
| E2E | Start 2 consumers, check assignments | partitions split between them, no partition owned twice |
| Manual | `kafka-consumer-groups.sh --describe --group sink-postgres` | members listed, lag goes to 0 after the load |

### 2.4 Done when

- [ ] `consumer-sink` runs with one command and joins group `sink-postgres`.
- [ ] Batches are written with one `unnest` insert per chunk, inside a transaction.
- [ ] Migration 0004 applied; `received_at` comes from the header.
- [ ] Duplicates (`attempted - inserted`) counted.
- [ ] All tests above pass, including the end-to-end test.

---

## Step 3: S2-F3. Commit offsets only after the DB transaction commits

> **Requirement:** Offsets are committed only after the database transaction commits.

### 3.1 What to build

**A. Commit protocol in `eachBatch` (`src/consumer/sink-consumer.ts`)**

Use `eachBatchAutoResolve: false`, `autoCommit: false`. For each fetched batch:

```
for each chunk of the batch (SINK_MAX_ROWS):
    if (!isRunning() || isStale()) break        // partition revoked by a rebalance: stop, don't commit
    rows = parse(chunk)                         // crash point: before_parse / after_parse
    await writer.write(rows)                    // BEGIN ... INSERT ... COMMIT
                                                // crash point: after_db_commit (before offset commit)
    resolveOffset(lastOffsetOf(chunk))          // only now is the chunk "done"
    await commitOffsetsIfNecessary()            // commit to Kafka, after the DB commit
    await heartbeat()
```

Rules this enforces:

- The offset is resolved only after `COMMIT` returned successfully. If the DB write throws, the offset is not resolved, nothing is committed, and the chunk is reprocessed (no loss).
- The committed offset is `last processed offset + 1` (the client does the +1 when committing resolved offsets; verify this for your library and write a test).
- Commits happen per chunk, so a crash loses at most one chunk of progress (bounded duplicates).
- If a rebalance revokes the partition mid-batch (`isStale()`), stop before the next chunk. Already-committed chunks stay committed; the new owner starts from there.
- If the offset commit itself fails (broker issue, rebalance in progress): log, count it, and **do not** treat the rows as lost. They are already in Postgres; the next owner will reprocess them and `ON CONFLICT` absorbs them. Let the error propagate so the client re-joins.
- Heartbeat between chunks so long batches don't trigger a session timeout.

**B. Graceful shutdown commit**

- On SIGTERM: stop taking new batches, let the current chunk finish (DB commit → offset commit), then disconnect. A clean shutdown must produce zero loss and zero (or near zero) reprocessing.

**C. Crash-point injection (debug only)**

- Env `CRASH_AT=none|before_parse|after_parse|after_db_commit` and `CRASH_AFTER_BATCHES=N` (crash on the Nth batch). At the chosen point, `process.kill(process.pid, 'SIGKILL')` (a real hard crash, no cleanup, no commit).
- Refuse to start with `CRASH_AT != none` unless `DEBUG_FAULTS=true`, so it never runs by accident.
- These hooks are how this step proves F3, and K5 reuses them later.

**D. Processing-delay hook**

- Env `SINK_PROCESSING_DELAY_MS` (default 0): adds a sleep per chunk before the DB write. Used here to make the "crash in the middle" test deterministic, and later by K8 (rebalance) experiments.

**E. Delivery semantics note**

- Add a short section to `services/consumer-sink/README.md`: this sink is **at-least-once**. Table of crash point → expected outcome:

  | Crash point | Rows in DB | Offset committed | Result after restart |
  | --- | --- | --- | --- |
  | before parse | no | no | chunk reprocessed, no loss, no duplicate rows |
  | after parse | no | no | chunk reprocessed, no loss, no duplicate rows |
  | after DB commit, before offset commit | yes | no | chunk reprocessed, `ON CONFLICT` absorbs it, duplicates counted |
  | after offset commit | yes | yes | nothing reprocessed |

### 3.2 What to cover

- No code path commits an offset before the matching DB commit (review every `resolveOffset` / `commitOffsets` call; there should be exactly one place).
- DB error → no commit → retry of the same offsets.
- Rebalance mid-batch → no commit for chunks not written.
- Commit failure after a successful DB write → duplicates on reprocess, not loss.
- Clean shutdown → no duplicates.

### 3.3 Tests

| Type | Test | Pass condition |
| --- | --- | --- |
| Unit (fake consumer + fake writer) | Writer resolves | `resolveOffset` called after `write` resolves, then commit; call order asserted |
| Unit | Writer rejects | `resolveOffset` and commit never called for that chunk |
| Unit | 3 chunks, writer fails on chunk 2 | offsets resolved/committed for chunk 1 only |
| Unit | `isStale()` becomes true after chunk 1 | chunk 2 not written, not committed |
| Unit | Commit throws after successful write | error propagates, counted as commit failure, no rows "un-done" |
| Unit | Committed offset value | last processed offset + 1 |
| Integration (real Kafka + Postgres) | `CRASH_AT=after_db_commit`, produce 5 000 events, restart without crash | all 5 000 unique rows, duplicates absorbed ≥ 1 and ≤ one chunk |
| Integration | `CRASH_AT=before_parse` and `after_parse` | all unique rows, zero absorbed duplicates for that chunk, no loss |
| Integration | DB unreachable (stop Postgres or point at toxiproxy and cut it) during load, then restore | no offsets advance while DB is down; after restore, all rows present, lag drains |
| Integration | SIGTERM under load, restart | zero missing rows, duplicates 0 (or one chunk at most; record what you see) |

### 3.4 Done when

- [ ] Exactly one code path resolves/commits offsets, and it runs after the DB commit.
- [ ] Crash hooks implemented and guarded by `DEBUG_FAULTS`.
- [ ] All crash-point integration tests pass: no loss in any case; duplicates only for `after_db_commit`, and bounded.
- [ ] Semantics table in the consumer README.

---

## Step 4: S2-F4. Poison messages must not block a partition forever

> **Requirement:** Poison messages must not block a partition forever (after you first observe that they do).

### 4.1 Part 1: observe the problem first (required by the PRD)

1. Add `scripts/produce-poison.sh`: writes one bad record straight into `events.raw` (bypassing the API) using the Kafka console producer inside a broker container (or `kcat`). Options: `--kind invalid-json|bad-uuid|oversized-field|constraint`, `--partition N` or `--key tenant_x` to target a busy partition.
2. With the step 3 consumer (bad message throws out of `eachBatch`), run a steady load, inject one poison record, and record in `notebook/stage-2-poison.md`: the consumer retries/crash-loops forever, lag grows on that one partition only, other partitions keep draining. Save `kafka-consumer-groups.sh --describe` output before/after. Commit this note **before** the fix.

### 4.2 What to build (the fix)

**A. Classify every failure (`src/sink/errors.ts`)**

| Class | Examples | Action |
| --- | --- | --- |
| `PERMANENT_PARSE` | invalid JSON, bad UUID, bad timestamp, missing tenant/header, oversized field caught by parser | send straight to DLQ, no retry |
| `PERMANENT_DB` | Postgres `22xxx` data exceptions (e.g. `22P02` invalid text, `22001` value too long), `23xxx` constraint violations other than the handled PK conflict | isolate the row, retry it `SINK_MAX_RETRIES` times, then DLQ |
| `TRANSIENT_DB` | connection refused/reset, `08xxx`, `53xxx`, `57P01`, pool timeout, statement timeout | **never** DLQ; retry with backoff and pause the partition until the DB is back |
| `UNKNOWN` | anything else | treat like `PERMANENT_DB` (bounded retries, then DLQ) |

Reuse the SQLSTATE / Node error-code logic from `ingest-api/src/common/all-exceptions.filter.ts` (move the shared bits into a small util).

The key design point: a down database must not dump every message into the DLQ. Only messages that fail on their own go there.

**B. Handling flow per chunk**

1. Parse every message. Parse failures → DLQ list. Good rows → insert list.
2. Insert the good rows as one batch.
   - Success → go to 4.
   - `TRANSIENT_DB` → back off (exponential, `SINK_RETRY_BASE_MS`, capped at `SINK_RETRY_MAX_MS`, with jitter), heartbeat while waiting, `pause()` the partition if waiting is long, retry the same chunk. No commit. This is the "DB is down" case and it waits as long as needed (bounded memory: it holds only the current chunk).
   - `PERMANENT_DB` / `UNKNOWN` → **isolate**: re-run the chunk row by row (or by bisection for big chunks) to find the bad row(s). Good rows get inserted; each bad row is retried up to `SINK_MAX_RETRIES` (default 3) with backoff, then goes to the DLQ list.
3. Publish the DLQ list to `events.dlq` with `acks=all` and wait for the ack.
4. Only after both the DB commit and the DLQ ack: `resolveOffset` + commit (same single commit path as step 3). If the DLQ publish fails, do not commit; the chunk is retried (possible duplicate DLQ records, which is accepted and noted for Stage 3).

**C. DLQ publisher (`src/sink/dlq-publisher.ts`)**

- Dedicated producer in the consumer service, `acks=all`, idempotent.
- Record value = the **original raw bytes** (not re-encoded, so you can inspect exactly what failed). Key = original key.
- Headers: all original headers, plus `dlq.original_topic`, `dlq.original_partition`, `dlq.original_offset`, `dlq.consumer_group`, `dlq.error_class`, `dlq.error_code` (SQLSTATE or parser code), `dlq.error_message` (truncated, e.g. 1 KB), `dlq.attempts`, `dlq.failed_at`.

**D. Inspection tooling**

- `scripts/dlq-peek.sh [N]`: prints the last N DLQ records with headers (console consumer with `print.headers=true`, or `kcat -C -f ...`).

**E. Config**

- `SINK_MAX_RETRIES`, `SINK_RETRY_BASE_MS`, `SINK_RETRY_MAX_MS`, `SINK_DLQ_ENABLED` (default `true`; set `false` to reproduce the blocking behavior from 4.1 on demand).

### 4.3 What to cover

- Each poison kind from the PRD: invalid JSON, bad UUID, oversized field, plus a DB-level constraint/data error that passes the parser.
- Poison in the middle of a busy batch: good rows before and after it still get stored.
- Several poison records in one chunk.
- DB down vs poison: DB down produces zero DLQ records.
- DLQ publish failure: no commit, retry, no loss.
- Retries are bounded (count and total time); the partition always moves forward.

### 4.4 Tests

| Type | Test | Pass condition |
| --- | --- | --- |
| Unit (classifier) | Table of error codes/messages | each maps to the expected class |
| Unit (handler, fakes) | Chunk with 1 invalid JSON among 99 valid | 99 inserted, 1 DLQ'd with `PERMANENT_PARSE` headers, offset committed past all 100 |
| Unit | Batch insert throws `22001` | row-by-row isolation, good rows inserted, bad row retried `SINK_MAX_RETRIES` times then DLQ'd |
| Unit | Insert throws `ECONNREFUSED` 5 times then succeeds | no DLQ, retried with growing backoff, committed only after success |
| Unit | DLQ publish rejects | offset not resolved, not committed |
| Unit | Backoff values | exponential, capped, with jitter in range |
| Integration (real stack) | Run load, `produce-poison.sh --kind invalid-json` into a busy partition | partition lag returns to ~0, DLQ has 1 record, `dlq-peek.sh` shows original bytes + error headers |
| Integration | Same for `bad-uuid`, `oversized-field`, `constraint` | same pass condition |
| Integration | Stop Postgres for 30 s under load, restart | DLQ count unchanged (0 new), all rows stored after recovery |
| Integration | `SINK_DLQ_ENABLED=false` + poison | reproduces the blocking (sanity check that the fix is what changed the behavior) |

### 4.5 Done when

- [ ] "Before" note committed showing a poison message blocking a partition.
- [ ] Bounded retry + DLQ implemented; transient DB errors never go to the DLQ.
- [ ] DLQ records carry original bytes and error headers; `dlq-peek.sh` works.
- [ ] All tests above pass, including the "partition drains after poison" integration test (this is the exit-criteria test).

---

## Step 5: S2-F5. Kafka unavailable → 503 + `Retry-After`, bounded memory

> **Requirement:** If Kafka is unavailable the API returns 503 with `Retry-After`; memory use stays bounded.

### 5.1 What to build

**A. Error types (`services/ingest-api/src/kafka/errors.ts`)**

- `OverloadedError`: batcher queue full (from step 1).
- `KafkaUnavailableError`: wraps client errors that mean "can't write right now": connection errors, request timeouts, retries exhausted, leader not available, `NOT_ENOUGH_REPLICAS` / `NOT_ENOUGH_REPLICAS_AFTER_APPEND` (this is what two brokers down with `min.insync.replicas=2` gives you), broker not available, and our own send timeout.
- `isKafkaUnavailable(err)` helper with a unit-tested mapping table.

**B. Exception filter**

- Extend `AllExceptionsFilter`: `OverloadedError` and `KafkaUnavailableError` → `503`, header `Retry-After: RETRY_AFTER_S`, body `{ statusCode: 503, error: 'Service Unavailable', message: 'Ingest temporarily unavailable', reason: 'overloaded' | 'kafka_unavailable' }`.
- Remove the Postgres-specific transient handling from the API's filter (the API has no DB any more). Move it to the shared util used by consumer-sink (step 4).
- Count 503s by reason (step 6 exposes the counter).

**C. Bounded memory and fast failure in the batcher**

- **Queue bound by count and bytes:** `BATCH_MAX_QUEUE` (messages) and `BATCH_MAX_QUEUE_BYTES` (e.g. 64 MB). Whichever is hit first → `OverloadedError` immediately, without allocating a promise or keeping the body.
- **Per-send timeout:** `KAFKA_SEND_TIMEOUT_MS` (e.g. 5 s). Wrap each `producer.send` in a timeout so a hung broker can't hold waiters forever. On timeout, reject every waiter with `KafkaUnavailableError`. (Document: the send may still complete later, so a client retry can duplicate. That's another ledger entry.)
- **Request wait cap:** a request never waits longer than `queue wait + send timeout`. Add an end-to-end test for that.
- **Fail fast when Kafka is known down (simple circuit breaker):** after `KAFKA_BREAKER_FAILURES` consecutive failed sends, mark the producer `unavailable` for `KAFKA_BREAKER_COOLDOWN_MS`. While unavailable, `enqueue` rejects immediately with `KafkaUnavailableError` instead of filling the queue. After the cooldown, let one batch through as a probe; success closes the breaker. This turns an outage into fast 503s instead of slow ones and keeps memory flat.
- **Unbounded mode for comparison (K10 needs it):** `BATCH_MAX_QUEUE=0` means "no bound" and also disables the breaker. Log a loud warning at startup. Default stays bounded.

**D. Startup and reconnect without restart**

- The API must start even if Kafka is down. `onModuleInit` tries to connect but does not crash the process; it schedules reconnect attempts with backoff.
- If the client disconnects at runtime (all brokers killed), reconnect in the background. Verify whether your client reconnects on its own on the next send or needs an explicit `connect()`, and handle the case it doesn't.
- `GET /health/ready`: 200 when the producer is connected and the breaker is closed, else 503. `GET /health/live` stays 200 while the process is healthy (don't let Kafka outages kill the process via liveness).
- Graceful shutdown with Kafka down: `drain()` has a deadline (`SHUTDOWN_DRAIN_TIMEOUT_MS`); waiters still queued at the deadline get 503, then exit.

### 5.2 What to cover

- All brokers down at request time → 503 fast (well under the send timeout once the breaker is open).
- All brokers down at startup → API starts, readiness 503, requests 503; brokers come up → 202 again with no API restart.
- Two of three brokers down → `NOT_ENOUGH_REPLICAS` → 503 (never 202, never silent loss).
- One broker down → brief errors at most, then 202 (client retries + leader election).
- Overload with Kafka healthy (queue full) → 503 `overloaded`.
- Memory stays flat under a sustained flood while Kafka is down.

### 5.3 Tests

| Type | Test | Pass condition |
| --- | --- | --- |
| Unit (errors) | Mapping table of client error types/codes | correct `isKafkaUnavailable` result |
| Unit (filter) | `OverloadedError`, `KafkaUnavailableError` | 503, `Retry-After` header, `reason` in body |
| Unit (batcher) | Queue full by bytes | `OverloadedError` |
| Unit (batcher) | `send` never resolves | all waiters reject with `KafkaUnavailableError` after `KAFKA_SEND_TIMEOUT_MS` (fake timers) |
| Unit (breaker) | N failures → open; enqueue during cooldown; probe success → closed; probe failure → open again | expected states and immediate rejections |
| Unit (batcher) | `BATCH_MAX_QUEUE=0` | no `OverloadedError` (unbounded mode works) |
| Unit (shutdown) | `drain()` with hung send | returns by the deadline, waiters rejected |
| E2E (real Kafka) | `docker stop` all brokers, send 1 000 requests | all 503 with `Retry-After`; p99 latency below send timeout after the breaker opens; `/health/ready` 503 |
| E2E | Start brokers again (API not restarted) | within a bounded time new requests get 202 and land in `events.raw` |
| E2E | Start API with brokers stopped | API boots; requests 503; brokers start; requests 202 |
| E2E | Stop 2 of 3 brokers | 503 (`NOT_ENOUGH_REPLICAS`), no 202 |
| E2E (memory) | Brokers down, flood at a fixed rate for 60 s, sample `process.memoryUsage().heapUsed` via `/debug/stats` every second | heap stays within a fixed band (no upward trend); queue depth never exceeds `BATCH_MAX_QUEUE` |

Keep these Kafka-down tests in their own e2e file (`events-kafka-down.e2e-spec.ts`) since they stop and start containers. Use a helper that runs `docker stop/start` and waits for broker health.

### 5.4 Done when

- [ ] Kafka down (fully, or below `min.insync.replicas`) always gives 503 + `Retry-After`, never a hang, never a 202.
- [ ] Memory flat under a flood with Kafka down (test passes).
- [ ] API starts without Kafka and recovers without a restart.
- [ ] Readiness/liveness endpoints behave as described.
- [ ] Unbounded mode available behind config, off by default.

---

## Step 6: S2-F6. Producer, batcher and consumer metrics on `/debug/stats`

> **Requirement:** Expose producer, batcher, and consumer metrics on `/debug/stats`.

Each process exposes **both** a Prometheus `/metrics` endpoint (for Grafana) and a JSON `/debug/stats` snapshot (for quick `curl` and for the audit script in step 7). Every metric below appears in both.

### 6.1 What to build

**A. ingest-api: batcher + producer metrics (`MetricsService` + `src/kafka/`)**

| Metric | Type | Notes |
| --- | --- | --- |
| `ep_batcher_queue_depth` | gauge | messages waiting |
| `ep_batcher_queue_bytes` | gauge | bytes waiting |
| `ep_batcher_in_flight_batches` | gauge | sends in progress |
| `ep_batcher_batch_size_messages` | histogram | messages per flushed batch |
| `ep_batcher_batch_size_bytes` | histogram | bytes per flushed batch |
| `ep_batcher_flush_total{reason}` | counter | `size`, `bytes`, `time`, `drain` |
| `ep_batcher_queue_wait_seconds` | histogram | enqueue → batch sent |
| `ep_batcher_rejected_total{reason}` | counter | `overloaded`, `breaker_open` |
| `ep_kafka_send_seconds` | histogram | `producer.send` duration (ack latency) |
| `ep_kafka_send_errors_total{type}` | counter | low-cardinality error type |
| `ep_kafka_messages_acked_total` | counter | **the API's count of acknowledged events**, used by the audit |
| `ep_kafka_messages_failed_total` | counter | messages in failed batches |
| `ep_kafka_breaker_state` | gauge | 0 closed, 1 open, 2 half-open |
| `ep_kafka_producer_connected` | gauge | 0/1 |
| `ep_kafka_producer_request_queue_size` / request latency | gauge/histogram | from the client's instrumentation events if available |
| `ep_http_503_total{reason}` | counter | from the filter |
| existing | — | rps, status codes, latency, event-loop delay, memory (keep Stage 1 ones; remove DB pool metrics from the API) |

**B. consumer-sink metrics**

| Metric | Type | Notes |
| --- | --- | --- |
| `ep_consumer_lag{partition}` | gauge | log end offset − committed offset, per partition |
| `ep_consumer_messages_consumed_total` | counter | every message handled (incl. reprocessed) |
| `ep_consumer_rows_inserted_total` | counter | `rowCount` sum |
| `ep_consumer_duplicates_total` | counter | `attempted − inserted` (reprocessing made visible) |
| `ep_consumer_batch_size_messages` | histogram | per Kafka batch and per chunk |
| `ep_consumer_batch_processing_seconds` | histogram | whole chunk: parse + write + commit |
| `ep_consumer_db_write_seconds` | histogram | the transaction |
| `ep_consumer_commit_seconds` | histogram | offset commit latency |
| `ep_consumer_commit_errors_total` | counter | |
| `ep_consumer_rebalances_total` | counter | group join events |
| `ep_consumer_assigned_partitions` | gauge | partitions this instance owns (and list them in `/debug/stats`) |
| `ep_consumer_errors_total{class}` | counter | classes from step 4 |
| `ep_consumer_retries_total{class}` | counter | |
| `ep_consumer_dlq_total{error_class}` | counter | |
| `ep_consumer_paused` | gauge | 1 while paused for DB backpressure |
| `ep_event_freshness_seconds` | histogram | `now − received_at` at DB commit, the **end-to-end visibility** metric from the PRD |
| `ep_consumer_last_committed_offset{partition}` | gauge | |
| `ep_consumer_crashes` | — | restarts are visible from `process_start_time_seconds`, no custom metric needed |
| pg pool | gauges/histograms | total/idle/waiting, connect wait (copy Stage 1 pool metrics here) |

**How to compute lag:** a background loop every `LAG_POLL_MS` (default 2 s) uses the admin client to fetch topic end offsets and the group's committed offsets, computes lag per partition, and updates the gauge. Lag is a group-level value, so every instance reports the same numbers; label with `partition` only and use `max` in Grafana. Also expose the per-batch `highWatermark − lastOffset` as a cheaper "local lag" field in `/debug/stats`.

**C. `/debug/stats` JSON shape**

- API:
  ```json
  { "uptime_s": 0, "memory": { "rss": 0, "heapUsed": 0 }, "event_loop_delay_ms": { "p50": 0, "p99": 0 },
    "http": { "requests": 0, "by_status": {}, "503_by_reason": {} },
    "batcher": { "queue_depth": 0, "queue_bytes": 0, "in_flight": 0, "flushes_by_reason": {}, "avg_batch_size": 0, "rejected": {} },
    "producer": { "connected": true, "breaker": "closed", "acked": 0, "failed": 0, "send_p50_ms": 0, "send_p99_ms": 0, "errors_by_type": {} },
    "config": { "acks": -1, "compression": "none", "idempotent": true, "batch_max_messages": 500, "batch_max_wait_ms": 5, "key_mode": "tenant" } }
  ```
- consumer-sink:
  ```json
  { "uptime_s": 0, "memory": {}, "group": "sink-postgres", "member_id": "", "assigned_partitions": [0, 3],
    "lag": { "total": 0, "by_partition": {} }, "consumed": 0, "inserted": 0, "duplicates": 0,
    "dlq": 0, "errors_by_class": {}, "retries": 0, "rebalances": 0, "paused": false,
    "batch": { "avg_size": 0, "processing_p99_ms": 0, "commit_p99_ms": 0 }, "freshness_p99_s": 0,
    "pg_pool": { "total": 0, "idle": 0, "waiting": 0 }, "config": {} }
  ```
- Including the effective `config` in the snapshot means every experiment result records the settings it ran with.

**D. Prometheus + Grafana**

- Add scrape jobs in `infra/compose/prometheus-config.yml` for `consumer-sink` (`host.docker.internal:8001`, plus more ports if you run several instances). The API job already exists.
- Optional but useful: add `danielqsj/kafka-exporter` (or a JMX exporter) to the Kafka compose for broker-side series (partition offsets, under-replicated partitions, consumer group lag from the broker's view).
- A basic `infra/observability/dashboards/stage-2/pipeline.json`: ingest rps + p99, batcher queue depth and batch size, send latency, 503 by reason, lag per partition, consumed vs inserted vs duplicates, freshness p99, DLQ count. (Per-experiment dashboards come later with the experiments.)

### 6.2 What to cover

- Label cardinality stays low (no event ids, tenant ids or raw error messages as labels).
- Metrics are updated on success **and** failure paths.
- `consumed − inserted − dlq` is explainable as duplicates (it should equal `duplicates_total`; add a test for that invariant).
- Snapshot is cheap (no Kafka admin call inside the HTTP handler; read cached lag values).

### 6.3 Tests

| Type | Test | Pass condition |
| --- | --- | --- |
| Unit (API) | Batcher with fake producer: 3 flushes (size, time, drain) | flush counters, batch-size histogram and queue gauges have the expected values |
| Unit (API) | Overload + breaker rejection | `rejected_total` by reason incremented |
| Unit (consumer) | Handler with fakes: 100 messages, 10 duplicates, 2 poison | consumed=100, inserted=88, duplicates=10, dlq=2 |
| Unit (consumer) | Lag calculator with given end/committed offsets | correct per-partition and total lag, including partitions with no committed offset |
| E2E (API) | `GET /debug/stats` and `GET /metrics` after some requests | all fields/series present; `acked` equals number of 202s |
| E2E (consumer) | After an end-to-end run | lag 0, `inserted` equals new rows, freshness histogram has samples |
| E2E (consumer) | Start a second consumer | `rebalances` increments, assigned partitions change in both snapshots |
| Manual | Prometheus targets page | both API and consumer targets UP |

### 6.4 Done when

- [ ] Every metric in both tables exists in `/metrics` and `/debug/stats`.
- [ ] Lag per partition and freshness are reported.
- [ ] Prometheus scrapes the consumer; the basic pipeline dashboard shows live data.
- [ ] Tests pass, including the `consumed = inserted + duplicates + dlq` invariant.

---

## Step 7: S2-F7. One-command audit

> **Requirement:** A one-command audit compares requests sent, acknowledged, Kafka offsets, and distinct rows in Postgres.

### 7.1 What to build

**A. Run-scoped audit with start/end snapshots (`scripts/audit.sh`)**

The topic and table keep data across runs, so the audit works on **deltas** between a start and an end snapshot instead of requiring a full reset.

- `scripts/audit.sh start [RUN_ID]` records into `loadtests/results/<run_id>/audit-start.json`:
  - `events.raw` end offsets per partition (`kafka-get-offsets.sh`, run inside a broker container) and log start offsets (so retention deletions are visible).
  - `events.dlq` end offsets.
  - `sink-postgres` committed offsets per partition (`kafka-consumer-groups.sh --describe`).
  - `SELECT count(*) FROM events` and `max(stored_at)`.
  - API `/debug/stats` counters (`acked`, `failed`, 503 by reason) and consumer `/debug/stats` counters (`consumed`, `inserted`, `duplicates`, `dlq`) from every running consumer instance.
- `scripts/audit.sh end [RUN_ID]`:
  1. Waits for the pipeline to drain: polls consumer lag until it is 0 (or `AUDIT_DRAIN_TIMEOUT_S`, default 120 s; if it times out, the audit reports "not drained" as a finding instead of a false mismatch).
  2. Takes the same snapshot again.
  3. Reads the k6 summary for the run (`requests sent`, count of 202 / 503 / other) from `loadtests/results/<run_id>/k6-summary.json`.
  4. Counts distinct rows for this run. Because k6 uses run-scoped event ids (see B), this is an exact query: `SELECT count(DISTINCT event_id) FROM events WHERE event_id::text LIKE '<run_prefix>%'`, plus the freshness p50/p99 of those rows from `stored_at - received_at`.
  5. Prints a table and writes `audit-result.json`.
- `scripts/audit.sh run -- <command>` = `start`, run the command (e.g. the k6 script), `end`. **This is the one command.**
- Implementation: a bash wrapper calling a small Node/TS script (`tools/audit/` or `scripts/audit.mjs`) for the JSON math and table printing. Bash alone gets painful for this.

**B. Run-scoped event ids in the load script (`loadtests/k6/ingest-kafka.js`)**

- Minimal k6 script (constant-arrival-rate, configurable `RATE`, `DURATION`, `TENANTS`, payload size) that reuses the Stage 1 event shape.
- `event_id` = UUID whose first 8 hex chars are the run id (e.g. `RUN_ID=a1b2c3d4`), rest random. This keeps the UUID valid and lets the audit count exactly this run's rows.
- `handleSummary` writes `k6-summary.json` with: total requests sent, count by status (202, 4xx, 503, other/network errors).
- Option to send a fixed fraction of retries with the **same** event id after a 503 (off by default), so later experiments can measure client-retry duplicates.

**C. Reconciliation rules (what "reconciles" means)**

| Check | Rule | If violated |
| --- | --- | --- |
| A | `sent ≥ acked_202` (k6) | harness bug |
| B | `acked_202 (k6) == Δ ep_kafka_messages_acked_total (API)` | lost responses / counting bug |
| C | `Δ Kafka end offsets (events.raw) ≥ acked_202` | **acknowledged but not in the log** → loss, critical |
| D | `Δ Kafka end offsets − acked_202` = records written but not acknowledged (failed/timed-out batches, retries) | reported, not a failure |
| E | `distinct rows for run == acked_202 − dlq_for_run` | below → **acknowledged but missing from Postgres** (loss, critical); above → rows from events that got a 503 but were actually written (expected, reported) |
| F | `Δ consumed − Δ inserted − Δ dlq == Δ duplicates` | metrics bug |
| G | `Δ duplicates` (reprocessed messages absorbed by `ON CONFLICT`) | reported as a number, never hidden |
| H | log start offset moved during the run (retention deleted unread data) | reported as a finding |
| I | consumer lag at end == 0 | otherwise "not drained" |

Output example:

```
RUN a1b2c3d4  (duration 120s, rate 2000/s)
sent                      240000
acknowledged (202)        239812   503: 188   other: 0
API acked counter         239812   [B OK]
kafka records (Δ offsets) 239850   [C OK]  written-not-acked: 38
rows (distinct, this run) 239850   [E OK]  acked-but-missing: 0   extra (503 but stored): 38
consumed / inserted       240210 / 239850   duplicates: 360   dlq: 0   [F OK]
lag at end                0        [I OK]
freshness p50 / p99       0.21s / 1.8s
RESULT: RECONCILED   (findings: 38 written-not-acked, 360 duplicates)
```

Exit code: 0 if reconciled, 1 if any critical check (C, E-below, B) fails, 2 if not drained. Any gap is printed as a "finding" line to copy into the notebook / failure ledger.

**D. Supporting scripts (`scripts/`)**

- `lag.sh`: `kafka-consumer-groups.sh --describe --group sink-postgres`, refreshed every N seconds (`watch`-style), shows per-partition lag and owner.
- `reset.sh` (optional clean slate): truncate `events`, delete and recreate `events.raw`/`events.dlq` via `kafka-init`, reset the group offsets. Must ask for confirmation, since it deletes data.
- `kill-broker.sh` / `kill-consumer.sh`: thin `docker kill` / `kill -9` helpers with a "which broker leads partition X" lookup. They're tiny and the PRD lists them in the repo additions, but the experiments that use them are out of scope here.
- `Makefile`:
  - `stage2-up`: Postgres + Kafka compose up, wait healthy, run migrations, start consumer and API. **"Pipeline runs with one command"** (exit criterion).
  - `stage2-down`
  - `stage2-audit RUN=... RATE=... DURATION=...` → `scripts/audit.sh run -- k6 run loadtests/k6/ingest-kafka.js`

### 7.2 What to cover

- Works with 1 or many consumer instances (sums counters from every instance listed in `CONSUMER_STATS_URLS`).
- Works when consumers restarted during the run (process counters reset on restart: prefer Kafka offsets and Postgres for the critical checks; treat consumer counters as best-effort and say so in the output when a restart is detected via uptime).
- Works without a reset between runs (deltas + run-scoped ids).
- Doesn't hang: every wait has a timeout.
- Read-only by default; only `reset.sh` writes.

### 7.3 Tests

| Type | Test | Pass condition |
| --- | --- | --- |
| Unit (audit math) | Fixture snapshots for a clean run | all checks OK, exit 0 |
| Unit | Fixture with acked > Kafka offsets | check C fails, exit 1, finding printed |
| Unit | Fixture with rows < acked | check E fails as "acknowledged but missing", exit 1 |
| Unit | Fixture with rows > acked (503-but-stored) | reported as extra, exit 0 |
| Unit | Fixture with lag > 0 after timeout | "not drained", exit 2 |
| Unit | Fixture with log start offset moved | retention finding printed |
| Integration | `make stage2-up`, then `make stage2-audit RATE=500 DURATION=30s` on a clean stack | `RECONCILED`, duplicates 0, exit 0 (this is the "audit reconciles on clean runs" exit criterion) |
| Integration | Same, but SIGKILL the consumer mid-run with `CRASH_AT=after_db_commit` and restart it | still reconciles on rows (no loss); duplicates > 0 reported |
| Integration | Same, but produce one poison record mid-run | reconciles with `dlq: 1` |
| Integration | Two audits back to back without reset | both reconcile (deltas work) |

### 7.4 Done when

- [ ] `make stage2-up` brings up the whole pipeline with one command.
- [ ] `make stage2-audit ...` runs load + audit and prints the reconciliation table with exit codes.
- [ ] Clean run reconciles; crash and poison runs show the expected duplicates/DLQ without false "loss".
- [ ] Audit math unit tests pass.

---

## Final checklist: Stage 2 server complete

| Requirement | Delivered in | Proof |
| --- | --- | --- |
| S2-F1 202 after Kafka ack | Step 1 | batcher unit tests + e2e record-in-topic test |
| S2-F2 `sink-postgres` batch writes | Step 2 | writer integration tests + API→Postgres e2e |
| S2-F3 offsets after DB commit | Step 3 | call-order unit tests + crash-point integration tests |
| S2-F4 poison doesn't block forever | Step 4 | "before" note + partition-drains-after-poison test + DLQ inspection |
| S2-F5 503 + Retry-After, bounded memory | Step 5 | Kafka-down e2e + memory-flat test + recovery test |
| S2-F6 metrics on `/debug/stats` | Step 6 | snapshot e2e tests + invariant test |
| S2-F7 one-command audit | Step 7 | `make stage2-audit` reconciles on a clean run |

Server knobs ready for the experiments later (no experiment work in this plan): `KAFKA_ACKS`, `KAFKA_COMPRESSION`, `KAFKA_IDEMPOTENT`, `BATCH_MAX_MESSAGES`, `BATCH_MAX_WAIT_MS`, `BATCH_MAX_BYTES`, `BATCH_MAX_QUEUE` (0 = unbounded), `BATCH_MAX_IN_FLIGHT`, `KAFKA_KEY_MODE`, `EVENTS_RAW_PARTITIONS`, `CONSUMER_*` group settings, `CONSUMER_FROM_BEGINNING`, `SINK_MAX_ROWS`, `SINK_PROCESSING_DELAY_MS`, `CRASH_AT` + `DEBUG_FAULTS`, `SINK_DLQ_ENABLED`, `SINK_MAX_RETRIES`.

Known flaws to leave in place on purpose (they go to the Stage 3 ledger, don't fix them now): duplicates from consumer crashes/rebalances/client retries; a rejected or timed-out batch may still have been written; DLQ publish and DB write are not atomic; no ordering or late-event handling; JSON envelope with no schema registry.
