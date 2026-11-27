# Stage 3 Server Build Plan: Correctness

*Source: `PRDs/EventPulse Stage 3 Correctness.md`. Scope: build the application server (ingest API, consumer sink, retry and DLQ path, outbox relay, schema registry integration, tooling and the correctness test suite) so that all nine functional requirements S3-F1 to S3-F9 are delivered. The break-it experiments (C1 to C14) are out of scope. This plan only makes sure the server exposes the knobs, crash points and metrics those experiments will need.*

---

## 0. How to use this plan

- There are **9 steps, one per requirement**. Each step owns its requirement completely: migrations, code, config, metrics, tests and docs. When a step is done, its requirement is done and nothing is left for later steps.
- The steps are **ordered by dependency, not by requirement number**. The order follows section 6 of the PRD (sink first, then API, then schema, then retry, then outbox), because the sink is where correctness lives and every later step leans on it.
- Every step ends with a **"Done when"** checklist. Don't start the next step until every box is ticked.
- Commit at the end of each step (for example `S3-F2: sink idempotent on (tenant_id, event_id)`).
- Never write a guarantee before the test that proves it passes (PRD pitfall: "writing the guarantees document before running the tests"). Step 9 writes `docs/delivery-guarantees.md` last, from passing tests only.

| Step | Requirement | One-line summary |
| --- | --- | --- |
| 1 | S3-F2 | Sink idempotent on `(tenant_id, event_id)`; shared fault injector; offset replay and consumer crash matrix prove storage never changes |
| 2 | S3-F1 | Redis idempotency layer at the API: `Idempotency-Key`/`event_id`, canonical hash, full state machine (202 / 409 / 202-replay / 422), fail-open or fail-closed policy |
| 3 | S3-F3 | Every duplicate counted at every layer (API, sink, reprocessing, DLQ), visible in `/metrics`, `/debug/stats`, Grafana and the extended audit |
| 4 | S3-F7 | Schema Registry + Avro on `events.raw`; producer serializes with a cached schema id, consumer deserializes by id; CI rejects incompatible schema changes |
| 5 | S3-F5 | Error classifier, retry tiers `events.retry.{1,2,3}` with `not_before`, bounded attempts with backoff and jitter, DLQ with `dlq_meta` |
| 6 | S3-F6 | `tools/dlq-replay`: triage, filters, dry run by default, safe and idempotent re-injection, replay bookkeeping |
| 7 | S3-F4 | Tenant config admin API writing config + outbox row in one transaction; polling relay publishing `config.changed` at least once |
| 8 | S3-F8 | Late and out-of-order events stored correctly with `occurred_at` and `received_at`; clock-skew bounds; property tests over shuffled, delayed, duplicated streams |
| 9 | S3-F9 | `docs/delivery-guarantees.md`: every claim mapped to an automated test, generated crash matrix, CI check that every claim's test exists and passes |

### Target repo layout after Stage 3

```
infra/compose/docker-compose.yml             # + redis, schema-registry, toxiproxy proxies for both (steps 2, 4)
infra/compose/docker-compose.kafka.yml       # + events.retry.1..3, config.changed topics (steps 5, 7)
db/migrations/0005_events_tenant_pk.sql      # PK (tenant_id, event_id), content_hash, sink_offsets (step 1)
db/migrations/0006_dlq_meta.sql              # dlq_meta (step 5)
db/migrations/0007_dlq_replays.sql           # replay bookkeeping (step 6)
db/migrations/0008_tenant_config_outbox.sql  # tenants, tenant_config, outbox (step 7)
db/migrations/0009_events_lateness.sql       # lateness index / check (step 8)
schemas/events/v1.avsc, v2.avsc               # events.raw-value (step 4)
schemas/config/v1.avsc                        # config.changed-value (step 7)
schemas/subjects.json                         # subject -> schema files + compatibility level (step 4)
packages/fault-injector/                      # named crash points + injected errors (step 1, extended in 2, 5, 6, 7)
packages/idempotency/                         # canonical hash, state machine, Redis store + Lua scripts (step 2)
packages/schema/                              # registry client wrapper, Avro serde, wire format (step 4)
packages/retry/                               # classifier, backoff, retry-topic router, headers (step 5)
packages/outbox/                              # writer (enqueue in a tx), polling relay (step 7)
services/ingest-api/src/idempotency/          # Nest interceptor using packages/idempotency (step 2)
services/consumer-sink/src/retry/             # retry-tier consumer, DLQ writer + dlq_meta (step 5)
services/admin-api/                           # tenant config admin API (step 7)
services/outbox-relay/                        # runnable relay process (step 7)
tools/schema-check/                           # CI compatibility check + register command (step 4)
tools/dlq-replay/                             # triage + replay CLI (step 6)
tools/audit/                                  # extended reconciliation (step 3)
tools/guarantee-check/                        # doc claim -> test traceability check (step 9)
tests/correctness/                            # replay, reorder, crash matrix, property tests (steps 1, 2, 5, 7, 8)
.github/workflows/ci.yml                      # unit + schema-check + guarantee-check (steps 4, 9)
docs/delivery-guarantees.md                   # the main artifact (step 9)
docs/generated/crash-matrix.md                # generated by the crash-matrix runner (steps 1, 2, 5, 7; published in 9)
docs/adr/0002..0007-*.md                      # one ADR per decision, written in the step that makes it
docs/runbooks/                                # dlq-triage-and-replay, schema-change, outbox-stuck
```

Notes:

- The PRD says `libs/`; this repo uses `packages/` (same decision as Stage 2), so `libs/idempotency` becomes `packages/idempotency` and so on.
- `pnpm-workspace.yaml` already covers `packages/*`, `services/*` and `tools/*`. **Add `tests/*`** (or make `tests/correctness` a single workspace entry) in step 1.
- `consumer-sink` still owns the `events` migrations. The `admin-api` owns `tenants`, `tenant_config` and `outbox`. Keep one migration sequence in `db/migrations/` (as today) so numbering never collides.

### Global conventions (apply in every step)

- Everything from the Stage 2 plan still applies: ESM, `.js` relative imports, Vitest, `loadConfig()`-style env parsing with defaults and validation, every new var in `.env.example`, no unbounded queue, metric prefix `ep_`, every metric also in `/debug/stats`, CPU/memory limits recorded in compose files.
- **Tests never touch dev data:** `eventpulse_test` database, throw-away topics, throw-away consumer groups, Redis **database 15** (or key prefix `test:{run}:`) for tests. Throw-away retry/DLQ topics are created per run too, and used only after `waitTopicReady()`.
- **Every correctness mechanism has an on/off switch** (`IDEM_ENABLED`, `SINK_DEDUP`, `SERIALIZATION=json|avro`, `RETRY_TOPICS_ENABLED`, `OUTBOX_RELAY_ENABLED`, ...). Defaults are "on". The switches exist so C14 can measure each mechanism; turning one off is debug-only, like `SINK_DEDUP=false` in Stage 2.
- **Every crash point is registered in one list** (`packages/fault-injector/src/points.ts`). The crash-matrix runner reads that list, so a new crash point without a matrix scenario fails the build (step 1 sets this up, steps 2, 5, 6, 7 add points).
- **The sink (Postgres) is the source of truth for correctness. Redis is never required for a correct result**, only for saving work. Any code that would make storage depend on Redis is a bug.
- **No sleeps in handlers.** Delays are done by pausing partitions, by `not_before` headers, or by timers outside the handler.
- **Errors are classified once, in `packages/retry`**, and every component (API, sink, retry consumer, relay, replay tool) uses the same classifier.

### Infrastructure added in this stage

| Component | Where | Notes |
| --- | --- | --- |
| Redis (single instance) | `infra/compose/docker-compose.yml`, `ep-redis` | **Redis 7**, pinned to an exact tag (`redis:7.x.y`, not `latest`). `appendonly no` (it is a cache by design), `maxmemory` set, **`maxmemory-policy noeviction`** so keys are never silently evicted (a full Redis fails writes, which the failure policy handles visibly). Exporter `oliver006/redis_exporter` for memory and ops. |
| Schema Registry | same compose, `ep-schema-registry` | `confluentinc/cp-schema-registry` pinned, `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS` pointing at the KRaft cluster, `_schemas` topic RF=3. Port 8081. |
| toxiproxy proxies | existing `ep-toxiproxy` | Add proxies `redis` (16379 -> redis:6379) and `registry` (18081 -> schema-registry:8081), created by a script `scripts/toxiproxy-setup.sh` that is idempotent. Services connect through the proxy when `*_VIA_TOXIPROXY=true`. |
| New topics | `kafka-init` | `events.retry.1`, `events.retry.2`, `events.retry.3` (same partition count as `events.raw`, RF=3, retention >= longest tier x 10), `config.changed` (RF=3, `cleanup.policy=compact`, key = tenant_id). |

Add `make stage3-up` / `make stage3-down` (extends `scripts/stage2-up.sh`: containers, toxiproxy setup, topics, migrations, schema registration, build, consumer, retry consumer, relay, admin API, ingest API) and `make stage3-correctness` (runs the full `tests/correctness` suite and regenerates the crash matrix).

---

## Step 1: S3-F2. The sink is idempotent

> **Requirement:** replaying any range of the log leaves stored data unchanged.

This is first because every other step (API dedup, retries, DLQ replay, outbox) relies on the sink absorbing duplicates. It also introduces the shared fault injector and the crash-matrix runner, since the proof of this requirement is a crash matrix.

### 1.1 What to build

**A. Migration `0005_events_tenant_pk.sql`**

```sql
-- Up Migration
ALTER TABLE events DROP CONSTRAINT events_pkey;
ALTER TABLE events ADD PRIMARY KEY (tenant_id, event_id);
ALTER TABLE events ADD COLUMN content_hash bytea;          -- sha256 of the canonical event, see 1.1.C

CREATE TABLE sink_offsets (                                 -- see 1.1.D
  consumer_group text   NOT NULL,
  topic          text   NOT NULL,
  partition      int    NOT NULL,
  max_offset     bigint NOT NULL,
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_group, topic, partition)
);
-- Down Migration: reverse in order.
```

- Before running it on the dev database, check that no `event_id` exists twice (it cannot today, but prove it with a query in the migration notes).
- Record in the migration comment that on a large table this would be `CREATE UNIQUE INDEX CONCURRENTLY` + `ADD CONSTRAINT ... PRIMARY KEY USING INDEX` to avoid a long lock. For the lab, the plain form is fine.
- `events_tenant_time_idx (tenant_id, occurred_at)` stays. The new PK index starts with `tenant_id`, so a lookup by tenant + id is one index probe.
- `content_hash` is nullable so rows from Stage 2 stay valid; new rows always fill it.
- Keep `events_nodedup` (K5, `SINK_DEDUP=false`) as is. It becomes the "dedup off" arm of C14.

**B. Batch writer (`services/consumer-sink/src/sink/batch-writer.ts`)**

- `ON CONFLICT (tenant_id, event_id) DO NOTHING RETURNING tenant_id, event_id, received_at`.
- Add `content_hash` to the `unnest(...)` columns.
- `duplicates = attempted - inserted` stays, but the writer now also returns **which keys were duplicates** (keys sent minus keys returned), so step 3 can split them by cause and so conflicts can be checked (C below).
- **Rule:** nothing else in the sink transaction may have a side effect for a duplicate row. If `COUNTER_MODE=hot` (B4 tenant counters) is still wired, it must increment only by the number of rows **returned**. Add a test for that or remove the counter path from the sink.
- **Within-batch duplicates:** a single `INSERT ... ON CONFLICT DO NOTHING` with the same key twice in one statement is allowed in Postgres (the second row is a conflict with the first). Add a test anyway, because `ON CONFLICT DO UPDATE` would fail with "cannot affect row a second time", and this rule must stay `DO NOTHING`.

**C. Conflicting duplicates (same key, different content)**

- `DO NOTHING` means "first committed copy wins". That is correct for true duplicates, but if a buggy producer reuses `(tenant, event_id)` with a different body, the stored result would depend on arrival order.
- When a chunk has duplicates, run one extra query for only those keys: `SELECT tenant_id, event_id, content_hash FROM events WHERE (tenant_id, event_id) IN (...)` and compare hashes. Count mismatches as **conflicting duplicates** (metric in step 3), log one line per mismatch (sampled, with tenant and event id, never the payload), and **never overwrite** the stored row.
- `content_hash` = sha256 of the canonical JSON of `{tenant_id, event_id, type, occurred_at, schema_version, payload}`. The canonicalization function lives in `packages/idempotency/src/canonical.ts` (created here, reused by step 2) so API and sink hash the same way.

**D. Durable per-partition progress (`sink_offsets`)**

- In the **same transaction** as the insert: `INSERT INTO sink_offsets ... ON CONFLICT DO UPDATE SET max_offset = GREATEST(sink_offsets.max_offset, EXCLUDED.max_offset)`.
- Purpose 1: step 3 uses it to count **reprocessed messages** after a crash or rebalance (`offset <= max_offset` means "this partition already stored this offset once").
- Purpose 2: it is the working prototype of the PRD alternative "store consumer offsets in Postgres inside the same transaction". Kafka offsets are still the ones used for resuming (the ADR explains why), but the data needed to switch is there.

**E. Shared fault injector (`packages/fault-injector`)**

Generalize `services/consumer-sink/src/consumer/fault-injector.ts` into a package and move the consumer to it.

- **Named points** in `src/points.ts`, format `<component>:<step>`, for example:
  - consumer: `consumer:before-parse`, `consumer:after-parse`, `consumer:before-insert`, `consumer:after-insert-before-commit` (inside the transaction, before `COMMIT`), `consumer:after-db-commit` (before offset commit), `consumer:after-offset-commit`.
  - Later steps add `api:*`, `retry:*`, `dlq:*`, `replay:*`, `relay:*`, `admin:*`.
- **Arming:** `FAULTS=consumer:after-db-commit@3` (crash on the 3rd hit), several points comma-separated. Requires `DEBUG_FAULTS=true` exactly like Stage 2. Also a debug endpoint `POST /debug/faults` (only mounted when `DEBUG_FAULTS=true`) to arm or disarm at runtime, so a test can start a service, warm it up and then arm.
- **Crash action:** sync write of `[fault] <point> hit=<n> pid=<pid> ctx=<json>` to stderr, then `process.kill(process.pid, 'SIGKILL')`.
- **Error action (for step 5):** `FAULT_ERRORS=consumer:before-insert=40001x2:marker=retry-me`, meaning "throw a synthetic error with SQLSTATE 40001, twice, only for rows whose payload contains `marker`". Lets tests produce transient and permanent failures for specific events without breaking the database.
- Backwards compatible: keep accepting Stage 2's `CRASH_AT` / `CRASH_AFTER_BATCHES` by mapping them to the new names, so K5 scripts still work.

**F. Crash-matrix runner (`tests/correctness/crash-matrix/`)**

- A runner that, for each registered crash point of a component: starts the pipeline on throw-away topics and a test database, sends a known set of N events, arms the point, waits for the SIGKILL, restarts the process without the fault, waits until the consumer group lag is 0, then runs the audit checks.
- It records per point: `duplicates produced` (messages in Kafka beyond N), `duplicates processed` (rows attempted beyond N), `duplicates stored` (rows beyond N, must be 0), `lost` (expected keys missing, must be 0), `recovery_ms` (restart to lag 0).
- Output: `docs/generated/crash-matrix.json` and a rendered `docs/generated/crash-matrix.md` (PRD: "generated table, not hand-written").
- A unit test fails if any point in `points.ts` has no scenario (the runner's scenario list is built from `points.ts`, so in practice this checks that each component has a start command).

**G. ADR `docs/adr/0002-offsets-in-db-vs-key-dedup.md`**

Decision: key-based dedup on `(tenant_id, event_id)` + Kafka-committed offsets. Rejected: offsets stored only in Postgres (exactly-once effect for this one sink, but every new sink needs its own scheme, and it does not stop duplicates that arrive with **different offsets**, such as client retries or DLQ replays). Record the measurements from 1.3 (insert cost with and without the PK, `sink_offsets` upsert cost).

### 1.2 What to cover

- Same event delivered twice in one batch, in two batches, in two partitions (key mode `event` puts one tenant's events on many partitions), after a crash, after a rebalance, after an offset reset.
- Same `event_id` under two tenants: **both stored** (this is the reason for the PK change).
- Same key with different content: first wins, conflict counted, row unchanged.
- Offset reset to any earlier position (including earliest) and re-consumption.
- Crash at every consumer point.

### 1.3 What to test

| Type | Test | Pass condition |
| --- | --- | --- |
| Unit | `insertSql()` uses `(tenant_id, event_id)`; `toColumns()` includes `content_hash`; duplicate keys computed correctly | Exact SQL and keys |
| Unit | Fault injector: arm by name, hit count, error injection with marker, legacy env mapping, disabled without `DEBUG_FAULTS` | Spec behavior |
| e2e (DB) | Write the same chunk twice | Second write: `inserted=0`, `duplicates=N`, table checksum unchanged |
| e2e (DB) | Same `event_id`, two tenants | 2 rows |
| e2e (DB) | Same key, different payload | 1 row, original payload, 1 conflict reported |
| e2e (DB) | Same key twice inside one chunk | 1 row, `duplicates=1` |
| e2e (DB) | `sink_offsets` advances only with `GREATEST`, and is rolled back with the insert on failure | Value correct after a forced rollback |
| e2e (pipeline) | **Offset replay:** produce N, consume, then reset the group to earliest (and separately to the middle), consume again | Row count and **table checksum** (`md5(string_agg(tenant_id||event_id||content_hash, ',' ORDER BY tenant_id, event_id))`) unchanged; `duplicates = replayed count` |
| e2e (pipeline) | **Triple replay through the log:** reset to earliest twice more | Checksum unchanged; sink duplicate counter = 2N |
| Crash matrix | Every `consumer:*` point | `duplicates stored = 0`, `lost = 0` for every point; table generated |

### 1.4 Done when

- [ ] Migration 0005 applied up and down cleanly on the test database.
- [ ] Sink uses `(tenant_id, event_id)` everywhere; `content_hash` filled for new rows; conflicts detected and counted.
- [ ] `sink_offsets` updated in the same transaction.
- [ ] `packages/fault-injector` used by the consumer; Stage 2 `CRASH_AT` still works.
- [ ] Offset-replay and triple-replay tests pass with unchanged checksums.
- [ ] Crash matrix for all consumer points generated in `docs/generated/`.
- [ ] ADR 0002 written with measurements.

---

## Step 2: S3-F1. Ingest is idempotent

> **Requirement:** the same `(tenant, Idempotency-Key or event_id)` accepted repeatedly produces one logical event; a reused key with a different payload returns a conflict error.

### 2.1 What to build

**A. Redis in compose** (see "Infrastructure"). API connects with `ioredis` (pick one and pin it): `enableOfflineQueue: false` (never queue commands while disconnected), `commandTimeout` ~50 ms (`REDIS_COMMAND_TIMEOUT_MS`), `maxRetriesPerRequest: 0`, reconnect with backoff. Connect through toxiproxy when `REDIS_VIA_TOXIPROXY=true`.

**B. Key design (write it into `docs/api-contract.md`)**

| Aspect | Decision |
| --- | --- |
| Who generates it | The client. `Idempotency-Key` header, 1 to 255 printable ASCII chars (validated, else 400). |
| Fallback | No header: the key is the body's `event_id` (natural key). Document that clients should send one or the other consistently. |
| Scope | `idem:{tenant_id}:{key}`. `tenant_id` comes from the API key, never the body, so tenants cannot read or block each other's keys. |
| Fingerprint | sha256 of the **canonical** JSON of the validated event (`event_id, type, occurred_at, schema_version, payload`), using `packages/idempotency/src/canonical.ts` from step 1: keys sorted recursively, no insignificant whitespace, numbers in shortest form, strings NFC-normalized (RFC 8785 JCS style). Two bodies that differ only by key order or whitespace get the same hash. |
| Lifetime | `done` state: `IDEM_TTL_S` (default 86400). `inflight` state: `IDEM_INFLIGHT_TTL_S` (default 30, must be > producer delivery timeout + response time) so an abandoned key frees itself quickly (C7). |
| Stored value | `inflight:<hash>:<owner_token>:<started_ms>` or `done:<hash>:<event_id>` (the "original result", enough to rebuild the 202 body). |

**C. The state machine (`packages/idempotency`)**

Implement each transition as **one Lua script** (`EVALSHA`) so check-and-set is atomic and cannot race:

- `acquire(key, hash, owner, inflightTtl)`: `SET NX` with inflight value; if the key exists, return its value.
- `complete(key, hash, owner, eventId, ttl)`: set `done:...` **only if** the current value is `inflight` with the same `owner` (a slow request whose inflight key expired and was re-acquired by someone else must not overwrite).
- `release(key, owner)`: delete only if the current value is `inflight` with the same `owner`.

Results, mapped exactly to the PRD table:

| Situation | Result |
| --- | --- |
| First time key seen | acquire OK -> produce -> complete -> **202** `{event_id}` |
| Same key, same hash, `inflight` | **409** `{"error":"idempotency_in_progress"}` + `Retry-After: 1`. Decision: return 409 rather than wait (keeps API threads free, the client already retries). Write it in the contract. |
| Same key, same hash, `done` | **202** with the original `{event_id}` and header `Idempotent-Replayed: true`. Nothing produced. |
| Same key, different hash (any state) | **422** `{"error":"idempotency_key_reused"}`. Nothing produced. |
| Key expired (older than the window) | Treated as new: produced again. The sink's PK stops duplicate storage (step 1). Counted in step 3. |

- Keep an in-memory `FakeIdempotencyStore` implementing the same interface for unit tests, and test the Lua scripts against real Redis.

**D. Nest integration (`services/ingest-api/src/idempotency/`)**

- An interceptor (or a step inside `EventsService.publish`) on `POST /v1/events`, order: auth guard -> DTO validation -> **idempotency acquire** -> produce (wait for Kafka ack) -> complete -> respond.
- **Produce fails with a definite failure** (queue full, breaker open): `release` and return the existing 503 + `Retry-After`. The client retry is then a first attempt.
- **Produce fails ambiguously** (delivery timeout: Kafka may or may not have it): `release` and 503. The retry may create a second copy in Kafka. That is accepted and documented; the sink absorbs it.
- **Process dies after produce, before complete** (the gap the PRD asks about): the `inflight` key expires after `IDEM_INFLIGHT_TTL_S`; meanwhile retries get 409; after it, the retry produces a second copy, stopped by the sink. Documented, and covered by a crash point.
- **`complete` fails** (Redis down right after produce): still return 202 (the event is durable). Count it. The key stays `inflight` until its short TTL.

**E. Redis failure policy (`IDEM_FAILURE_POLICY=open|closed`, default `open`)**

- `open`: if Redis errors or times out on `acquire`, skip dedup for this request, produce, return 202, count `ep_idem_fail_open_total`. Correct because the sink is authoritative; the cost is duplicate load on Kafka and the sink.
- `closed`: return 503 + `Retry-After` and count `ep_idem_fail_closed_total`.
- Wrap Redis calls in the existing `breaker.ts` pattern so a dead Redis does not add 50 ms to every request: when the breaker is open, apply the policy immediately.
- Write `docs/adr/0003-dedup-placement.md` (two layers, correctness only in the sink) and `docs/adr/0004-redis-vs-postgres-idempotency.md` (Redis: fast, bounded, losable; Postgres unique key: durable, unbounded, slower; why both).

**F. Crash points:** `api:before-produce`, `api:after-produce-before-complete`, `api:after-complete-before-response`. Add them to `points.ts` and to the crash matrix (API rows: send with a key, crash, restart, client retries with the same key, measure what reaches Kafka and the table).

**G. Switch:** `IDEM_ENABLED=true|false` (C14).

### 2.2 What to cover

All five rows of the semantics table; header and `event_id` fallback; canonicalization (key order, whitespace, unicode, numbers like `1.0` vs `1`); concurrency (many identical requests at once); an abandoned `inflight` key; Redis down, slow and flushed under both policies; key validation; tenant isolation (same key, two tenants, both accepted).

### 2.3 What to test

| Type | Test | Pass condition |
| --- | --- | --- |
| Unit | State machine with the fake store: every row of the semantics table | Exact status, body, headers; produce called 0 or 1 times as specified |
| Unit | Canonical JSON: key order, whitespace, nested objects, arrays (order matters), unicode NFC, number forms | Equal hash for equivalent bodies, different hash otherwise |
| Property (`fast-check`) | Random sequence of `(key, payload)` requests against the fake store vs a reference model (`Map`) | Same responses and same number of produce calls for every sequence |
| e2e (Redis) | Lua scripts: complete/release ignore a foreign owner; TTLs set correctly for both states | Spec behavior |
| e2e (Redis) | `SCRIPT FLUSH` (or restart Redis), then a request | Script reloaded automatically; request succeeds |
| e2e (API + Kafka) | Same key twice; same `event_id` twice without header; different payload same key | 1 message in Kafka; second response 202 replayed / 422 |
| e2e | **Concurrency:** 50 parallel identical requests | Exactly 1 Kafka message; other responses 409 or 202 replayed |
| e2e | Same key, two tenants | 2 messages |
| e2e | Inflight expiry: crash at `api:after-produce-before-complete`, retry before and after `IDEM_INFLIGHT_TTL_S` | 409 before; produced again after; 1 row stored |
| e2e (toxiproxy) | Redis down / latency above timeout, `open` and `closed` | `open`: 202 and fail-open counter; `closed`: 503 + `Retry-After`; latency bounded by timeout or breaker |
| e2e | `FLUSHALL` between two identical requests | Second produces again; 1 row stored |
| Crash matrix | All `api:*` points | No loss; duplicates stored = 0 |

### 2.4 Done when

- [ ] Redis in compose with `noeviction`, exporter and toxiproxy proxy.
- [ ] All five semantics rows implemented and tested; 409 vs wait decision documented.
- [ ] Canonical hashing shared by API and sink.
- [ ] Failure policy configurable and tested both ways; default and reason documented.
- [ ] 50-way concurrency test produces exactly one event.
- [ ] API crash points in the matrix.
- [ ] `docs/api-contract.md` updated (headers, statuses, key rules); ADRs 0003 and 0004 written.

---

## Step 3: S3-F3. Duplicates are counted and visible

> **Requirement:** duplicates are counted and visible (metrics), never silently swallowed.

Steps 1 and 2 already compute the numbers. This step makes **every** layer report them consistently, in metrics, `/debug/stats`, a dashboard and the audit, so that a retry storm upstream can never hide behind a clean sink (PRD pitfall).

### 3.1 What to build

**A. Metric catalogue (names fixed here, used by all later steps)**

| Layer | Metric | Labels |
| --- | --- | --- |
| API | `ep_idem_requests_total` | `result = new \| replayed \| in_progress \| payload_mismatch \| fail_open \| fail_closed \| disabled` |
| API | `ep_idem_complete_failures_total` | none |
| API | `ep_idem_redis_latency_seconds` (histogram) | `op = acquire \| complete \| release` |
| API | `ep_idem_inflight_expired_total` (seen when a retry finds no key after a 409 earlier; approximate, documented) | none |
| Sink | `ep_sink_rows_attempted_total`, `ep_sink_rows_inserted_total` | none |
| Sink | `ep_sink_duplicates_total` | `cause = reprocessed \| redelivered` (reprocessed = offset <= `sink_offsets.max_offset` for that partition, i.e. the sink already saw this offset; redelivered = a new offset carrying an already-stored key: client retry, expired window, Redis loss, replay) |
| Sink | `ep_sink_conflicting_duplicates_total` | none |
| Sink | `ep_sink_reprocessed_messages_total` | `partition` |
| Retry/DLQ | `ep_dlq_duplicates_total` (dead letters already in `dlq_meta`, step 5) | none |
| Relay | `ep_outbox_republished_total` (step 7) | none |

- Optional `tenant` label only behind `METRICS_TENANT_LABEL=true` (cardinality).
- All of these also appear in `/debug/stats` of their service under a `duplicates` section.

**B. Reprocessing detection.** On partition assignment, read `sink_offsets.max_offset` for each assigned partition (one query). While processing, messages with `offset <= max_offset` are counted as reprocessed. This is the proof for "duplicates after restart or rebalance".

**C. Grafana dashboard** `infra/observability/dashboards/stage-3/correctness.json`: duplicates by layer and cause (stacked), idempotency results, conflict counters, reprocessed messages per partition, Redis latency and memory, and placeholder rows for retry/DLQ/outbox filled by steps 5 and 7.

**D. Extended audit (`tools/audit`)**

- New snapshot fields: API idempotency counters, sink duplicate counters by cause, DLQ topic offsets and `dlq_meta` count (step 5), retry topic lag (step 5), outbox unpublished count and oldest age (step 7).
- New reconciliation (the PRD's "unexplained difference must be zero"):
  - `unique_accepted = 202_new` (from the load generator's own record of distinct `(tenant, event_id)` that got 202)
  - `stored + dlq_unique + in_flight (raw lag + retry lag) - unique_accepted = unexplained`, must be **0** once lag is 0.
  - `kafka_messages - unique_accepted = duplicates_in_kafka`, and it must equal `ep_sink_duplicates_total` + DLQ duplicates (every extra copy is accounted for).
- `scripts/audit.sh` and `make stage3-audit` (same interface as Stage 2, plus `IDEM_KEYS=reuse` to make the k6 script resend a share of requests with the same key).
- Update `loadtests/k6/stage-2/ingest-kafka.js` (or a stage-3 copy) to send `Idempotency-Key` and optionally resend a configurable fraction.

**E. Logs.** One structured log line per batch with duplicates (`{partition, attempted, inserted, duplicates, reprocessed, conflicts}`), never one per row.

### 3.2 What to cover

Every place a duplicate can be dropped or created: API (replay, in progress, mismatch, fail-open), Kafka (ambiguous produce, crash after produce), sink (reprocessing vs redelivery, conflicts), DLQ duplicates, outbox republish. Every counter must have a test that makes it move.

### 3.3 What to test

| Type | Test | Pass condition |
| --- | --- | --- |
| Unit | Duplicate cause split given `sink_offsets` and the batch's offsets | Correct counts |
| Unit | Audit reconciliation math (extend `checks.spec.ts`): balanced run, missing event, extra Kafka copy, DLQ'd event, in flight | `unexplained` correct in each case |
| e2e | Replay N events 3 times via the API with the same keys (keys within the window) | `ep_idem_requests_total{result="replayed"} = 2N`; Kafka has N; sink duplicates 0 |
| e2e | Same with `IDEM_ENABLED=false` (or after `FLUSHALL`) | Kafka has 3N; `ep_sink_duplicates_total{cause="redelivered"} = 2N`; rows N |
| e2e | Offset reset replay | `cause="reprocessed"` = replayed count |
| e2e | Conflict | `ep_sink_conflicting_duplicates_total = 1` |
| e2e | Every metric in the catalogue is present on `/metrics` and in `/debug/stats` after startup (value 0 is fine) | All present |
| Audit | `make stage3-audit` on a clean run and on a run with a crash | `unexplained = 0` in both |

### 3.4 Done when

- [ ] Metric catalogue implemented with the exact names; present in `/metrics` and `/debug/stats`.
- [ ] Reprocessed vs redelivered split working.
- [ ] Dashboard imported and showing all layers.
- [ ] Audit extended; reconciles to zero on clean and crash runs.
- [ ] Every counter has a test that moves it.

---

## Step 4: S3-F7. Registered Avro schema on `events.raw`, compatibility checked in CI

> **Requirement:** all events on `events.raw` use a registered Avro schema; incompatible changes are rejected in CI.

Done before retries and replay so that the retry, DLQ and replay code is written once against the final wire format.

### 4.1 What to build

**A. Library choice (verify before committing).** The repo uses `@confluentinc/kafka-javascript`, so first evaluate Confluent's own `@confluentinc/schemaregistry` (Avro serde + registry client, maintained alongside it). Fallback: `@kafkajs/confluent-schema-registry` or `avsc` + a small registry HTTP client. Record the choice and the check you did (last release date, Avro support, caching) in `docs/adr/0005-avro-vs-protobuf.md` together with the Avro vs Protobuf decision (Avro now because registry compatibility tooling is most mature; Protobuf is a Stage 8 comparison).

**B. Schemas (`schemas/`)**

- `schemas/events/v1.avsc`, record `com.eventpulse.Event`:
  - `event_id` string (`logicalType: uuid`), `tenant_id` string, `type` string,
  - `occurred_at` long (`timestamp-millis`, see step 8 for precision), `received_at` long (`timestamp-millis`),
  - `schema_version` int,
  - `payload_json` string. Avro has no "any JSON object" type, so the payload travels as canonical JSON text. Record this trade-off (the envelope is typed and evolvable; the tenant payload is opaque until a later stage introduces per-type schemas).
- `schemas/events/v2.avsc`: v1 + one optional field with a default (for example `source: ["null","string"], default null`). It exists to prove evolution end to end.
- `schemas/subjects.json`: `{ "events.raw-value": { "files": ["events/v1.avsc","events/v2.avsc"], "compatibility": "FULL_TRANSITIVE" } }`. Decision to record: **FULL_TRANSITIVE** for `events.raw` because producers (API) and consumers (sink, later processors) deploy independently and replays read old data with new code; this forces "add or remove only fields with defaults". Note what BACKWARD would allow instead.
- `schemas/fixtures/incompatible/`: rename without alias, type change (`int` -> `string`), required field added without default, field removed without default. Used by tests of the checker.

**C. `packages/schema`**

- `RegistryClient` wrapper with an **id cache** (`schemaId -> parsed schema`) and **subject cache** (`subject+schema -> id`), registry URL via toxiproxy when `REGISTRY_VIA_TOXIPROXY=true`, timeouts, and metrics `ep_schema_registry_requests_total{op,result}`, `ep_schema_registry_latency_seconds`.
- `serializeEvent(envelope)` -> Confluent wire format: magic byte `0x00` + 4-byte big-endian schema id + Avro binary.
- `deserializeEvent(buf)`: checks magic byte, looks up the writer schema by id (cache, then registry), decodes using the **reader schema the consumer was built with** (schema resolution), returns the envelope plus `{schemaId, writerVersion}`.
- Errors are typed: `NotAvroError` (bad magic byte, too short), `UnknownSchemaIdError` (registry says 404), `RegistryUnavailableError` (timeout, 5xx, connection), `AvroDecodeError`. Step 5's classifier maps the first, second and fourth to permanent and the third to transient.

**D. Producer side (ingest-api)**

- **No auto-registration at runtime** (`auto.register.schemas=false`). Schemas are registered by the deploy step (`tools/schema-check register`). At startup the API looks up the id for its compiled-in schema (`POST /subjects/{subject}` lookup) and caches it.
- Startup policy: registry unreachable at startup -> retry with backoff, readiness probe stays not-ready (the API does not take traffic it cannot serialize). Registry unreachable later -> the cached id keeps working (nothing to call). Document it as the "hidden dependency" policy (C11).
- DTO validation (class-validator) stays first; Avro serialization is a second, structural check. A serialization failure is a 400/422 with the field, never a 500.
- `SERIALIZATION=json|avro` switch (default `avro`) for C14 and for the transition.
- Header `content-type: application/vnd.eventpulse.event+avro` (or `+json`) on every message.

**E. Consumer side (consumer-sink)**

- `parse.ts` dispatches on the first byte (and the `content-type` header): `0x00` -> Avro; `{` -> legacy JSON (kept so that **replaying Stage 2 data still works**, which S3-F2 requires); anything else -> `NotAvroError`. `SINK_ACCEPT_JSON=true` by default during this stage; document when it can be turned off.
- Store `schema_version` as the writer's registry version (and the schema id in the log context).
- Unknown schema id + registry reachable = permanent (DLQ in step 5). Unknown id + registry unreachable = transient (blocks or retries, **never DLQ**), because a dead registry would otherwise dead-letter everything.
- Metrics: `ep_schema_deserialize_failures_total{reason}`, `ep_schema_ids_in_use` (gauge of distinct writer ids seen in the last N minutes).

**F. `tools/schema-check` (CI and deploy)**

- `check`: starts from an **empty ephemeral registry** (CI service container), sets each subject's compatibility level from `subjects.json`, registers every version **in order**, and fails if any version is rejected. This checks the whole history under the transitive rule, not only the last pair.
- `check --against <registry-url>`: checks the newest local version against a running registry (`POST /compatibility/subjects/{s}/versions?verbose=true`), printing the registry's reasons.
- `register --registry <url>`: idempotent registration for deploys (`make stage3-schemas`).
- Exit codes: 0 compatible, 1 incompatible (with messages), 2 infrastructure error.
- `.github/workflows/ci.yml`: job `schema-check` with a Kafka + registry service, runs `pnpm --filter @eventpulse/schema-check check`. Also a job for unit tests of all packages. (No CI exists yet; this creates it.)

**G. Runbook `docs/runbooks/schema-change.md`:** how to add a field (with default), which side deploys first under the chosen mode, how to run the check locally, how to register, how to roll back, what is forbidden (renames without aliases, type changes, required fields without defaults).

**H. Tests run on Avro from this step on**

Steps 1 to 3 were built and tested before Avro existed, so their e2e tests send JSON. After this step, production sends Avro, and the tests must too, or they keep passing on a path production no longer uses.

- **Shared test encoder.** Change the test helpers that write straight to Kafka (`services/consumer-sink/test/process-helpers.ts`, `services/ingest-api/test/kafka-helpers.ts`, the crash-matrix runner and the property-test feeders in `tests/correctness/`) to encode with `packages/schema` (`serializeEvent`) by default. Keep a `encoding: 'json'` option only for the tests that deliberately send legacy JSON (old JSON followed by Avro; replay of Stage 2 data).
- **Re-run steps 1 to 3 under Avro.** Run the step 1 to 3 e2e suites and the crash matrix again with `SERIALIZATION=avro`, and fix anything that breaks. From now on these suites run with Avro by default.
- **Subjects for throw-away test topics.** The registry's default naming (TopicNameStrategy) makes the subject `<topic>-value`, and every test run makes a new topic. Decision: **the test setup registers the schema under each test topic's subject** (helper `registerTestSubject(topic)` in `packages/schema/src/testing.ts`, called right after `waitTopicReady()`, with the same compatibility level as `subjects.json`). Production naming stays the default `events.raw-value`.
- **Which registry tests use.** Tests reuse the dev registry (it sits on the same Kafka cluster), isolated by name: test topics, and so test subjects, start with `test.<run>.`, the same rule as topics today. A global teardown soft-deletes (and then hard-deletes) the `test.<run>.*` subjects so the registry does not fill up. The isolation rule is "never write to a subject without the `test.` prefix from a test", enforced by the helper.
- **Unit tests are not affected.** Batch writer, idempotency state machine, classifier and router unit tests work on decoded rows and don't serialize anything.

### 4.2 What to cover

Producer serialization, consumer deserialization, schema resolution between v1 and v2 in both directions, legacy JSON records still readable, registry down at startup and during the run, unknown ids, malformed bytes, CI rejection of each incompatible change type.

### 4.3 What to test

| Type | Test | Pass condition |
| --- | --- | --- |
| Unit | Wire format encode/decode; magic byte and id parsing; too-short buffers | Exact bytes; typed errors |
| Unit | Round trip with v1; v2 writer read by v1 reader (new field ignored); v1 writer read by v2 reader (default filled) | Values equal / defaults applied |
| Unit | Cache: second lookup of an id does not call the registry | 1 HTTP call |
| Unit | `payload_json` is canonical and round-trips any JSON object (property test with `fast-check` arbitraries for JSON) | Deep-equal |
| e2e (registry) | `schema-check check` accepts the real history; rejects each fixture in `fixtures/incompatible/` under FULL_TRANSITIVE; also shows which ones BACKWARD would accept | Exit codes and messages as expected |
| e2e (pipeline) | API (avro) -> Kafka -> sink | Rows equal to inputs; `schema_version` stored |
| e2e | Old JSON records followed by Avro records in one topic | All stored |
| e2e | Message with an unregistered id | Classified permanent (DLQ once step 5 exists; until then, counted and blocking) |
| e2e (toxiproxy) | Registry down after API startup | API keeps returning 202 |
| e2e (toxiproxy) | Registry down, fresh consumer sees an uncached id | Partition blocked/retried, nothing dead-lettered; resumes when the registry is back |
| CI | Workflow runs on a branch with an incompatible schema change | Job fails |
| e2e (regression) | Step 1 to 3 e2e suites and the crash matrix re-run with the Avro test encoder | All green; crash matrix unchanged (0 lost, 0 duplicates stored) |
| e2e | `registerTestSubject` + teardown | Test subject exists during the run and is gone after; no non-`test.` subject touched |

### 4.4 Done when

- [ ] Registry in compose; schemas registered by `make stage3-schemas`.
- [ ] API produces Avro with a cached id, no runtime auto-registration.
- [ ] Sink reads Avro and legacy JSON; registry failures classified correctly.
- [ ] Test helpers encode Avro by default; step 1 to 3 suites and the crash matrix pass under Avro.
- [ ] Test subjects registered per test topic and cleaned up after the run.
- [ ] `tools/schema-check` + CI workflow reject every incompatible fixture.
- [ ] ADR 0005 and schema-change runbook written.

---

## Step 5: S3-F5. Bounded retry with backoff and jitter, then DLQ with full metadata

> **Requirement:** failed messages follow bounded retry with backoff and jitter, then go to the DLQ with full diagnostic metadata.

### 5.1 What to build

**A. `packages/retry`: one classifier for the whole system**

Move `services/consumer-sink/src/sink/errors.ts` here and extend it. Each error gets a class **and** an action:

| Class | Examples | Action |
| --- | --- | --- |
| `PERMANENT_PARSE` | `NotAvroError`, `AvroDecodeError`, invalid JSON, bad UUID | DLQ at once |
| `PERMANENT_SCHEMA` | `UnknownSchemaIdError` (registry reachable, id unknown) | DLQ at once |
| `PERMANENT_DB` | SQLSTATE 22xxx, 23xxx caused by this row's data | Isolate by bisection (existing), then DLQ (no retry tiers: retrying bad data never helps) |
| `TRANSIENT_RECORD` | 40001 serialization failure, 40P01 deadlock, 57014 statement timeout for one chunk | Retry tiers |
| `TRANSIENT_SYSTEMIC` | connection refused/reset, pool timeout, 08xxx, 53xxx, 57P0x, registry unavailable | **Do not route to retry topics.** Pause the partition with backoff (existing Stage 2 behavior) until the dependency is back. Sending every record of an outage to retry topics would be a retry storm (C9). |
| `SYSTEMIC_CONFIG` | 42xxx, 28xxx, 3D000, 0A, XX | Like `TRANSIENT_SYSTEMIC`, plus an alert-level log: needs a human |
| `UNKNOWN` | anything else | Retry tiers (bounded), then DLQ |

- The classifier is a pure function with a table-driven spec. Expose `classify(err) -> {class, action}` and `isRetriable`, `isBlocking`.
- Keep the Stage 2 rule written at the top: **a dependency that is down never sends records to the DLQ.**

**B. Backoff** `backoffMs(attempt, base, max, rand)` moves here unchanged (equal jitter) and gets a property test. Add `fullJitter` as an option (`RETRY_JITTER=equal|full`).

**C. Retry tiers**

- Topics `events.retry.1`, `.2`, `.3` with configured base delays `RETRY_TIER_DELAYS_MS=5000,60000,600000` and `RETRY_MAX_ATTEMPTS=3` (= number of tiers; attempt k goes to tier k).
- **Retry record:** same key and **original raw value bytes**, original headers, plus:
  `retry.attempt`, `retry.not_before` (epoch ms = now + backoff(tier) with jitter), `retry.original_topic`, `retry.original_partition`, `retry.original_offset`, `retry.first_failed_at`, `retry.last_error_class`, `retry.last_error_code`, `retry.last_error_message` (truncated 1 KB).
- **Router** (`packages/retry/src/router.ts`), pure: given the message, its headers and the classified error, returns `{kind: 'retry', topic, headers} | {kind: 'dlq', headers} | {kind: 'block'}`. Attempts exhausted -> DLQ.
- **Main consumer change (batch handler):** for `TRANSIENT_RECORD`/`UNKNOWN` failures of isolated rows, publish the row to `events.retry.1` (wait for ack), then commit the source offset. The main partition keeps moving (G6: "cannot block a partition indefinitely").
- **Retry consumer** (`services/consumer-sink/src/retry/retry-consumer.ts`, separate consumer group `sink-postgres-retry`). `SINK_ROLE=main|retry|both`, **default `both`** (one process runs the main and the retry consumer); `main` and `retry` let them run as separate processes later:
  - Subscribes to all retry tiers.
  - For each message: if `now < not_before`, **pause that partition**, `seek` back to this offset, and schedule `resume` at `not_before` (timer outside the handler, no sleep). Because each tier has a fixed delay, messages in a tier are close to `not_before` order, so head-of-line waiting is bounded by the jitter.
  - Otherwise process it with the **same** parse + write code as the main consumer (so dedup applies; a retried row that was meanwhile stored by a replay is just a duplicate).
  - On failure: router decides next tier or DLQ.
  - Commits offsets only after the outcome (stored, re-routed with ack, or dead-lettered with ack).
- **Bounding volume:** `RETRY_MAX_RATE_PER_S` token bucket on retry publishes. If it is exceeded, the main consumer switches to pausing the partition (treat as systemic) and logs it; this stops a bad deploy from flooding Kafka. Metric `ep_retry_rate_limited_total`.
- Ordering note (documented, used by step 9): a retried event is stored **after** later events of the same key. Storage is order-insensitive (step 8), so this is acceptable; G4 says "ordering within a partition, except for retried messages".

**D. DLQ writer + `dlq_meta`**

Migration `0006_dlq_meta.sql`:

```sql
CREATE TABLE dlq_meta (
  id               bigserial PRIMARY KEY,
  source_topic     text        NOT NULL,   -- events.raw (the original, even if it failed on a retry tier)
  source_partition int         NOT NULL,
  source_offset    bigint      NOT NULL,
  failed_topic     text        NOT NULL,   -- where it failed last (events.raw or events.retry.N)
  dlq_partition    int, dlq_offset bigint, -- where it sits in events.dlq (for replay)
  tenant_id        text, event_id uuid,    -- null if unparseable
  error_class      text NOT NULL, error_code text, error_message text,
  attempts         int  NOT NULL,
  first_failed_at  timestamptz NOT NULL, last_failed_at timestamptz NOT NULL,
  replayed_at      timestamptz, replay_count int NOT NULL DEFAULT 0, last_replay_id uuid,
  UNIQUE (source_topic, source_partition, source_offset)
);
CREATE INDEX dlq_meta_unreplayed_idx ON dlq_meta (first_failed_at) WHERE replayed_at IS NULL;
CREATE INDEX dlq_meta_class_idx ON dlq_meta (error_class, tenant_id);
```

- Order of writes for a dead letter: **publish to `events.dlq` and wait for the ack** (Kafka is the durable quarantine; value = original raw bytes, headers = existing `dlq.*` set + `dlq.first_failed_at`, `dlq.failed_topic`) -> `INSERT INTO dlq_meta ... ON CONFLICT (source_topic, source_partition, source_offset) DO NOTHING` (a re-published dead letter after a crash becomes a counted DLQ duplicate, `ep_dlq_duplicates_total`) -> commit the source offset.
- If the `dlq_meta` insert fails because the DB is down, the classifier already blocks the partition; the DLQ record exists and `tools/dlq-replay reindex` (step 6) can rebuild `dlq_meta` from the topic. Write that down.
- Crash points: `retry:after-retry-publish-before-commit`, `retry:before-resume`, `dlq:after-dlq-publish-before-meta`, `dlq:after-meta-before-commit`.

**E. Metrics:** `ep_retry_published_total{tier}`, `ep_retry_processed_total{tier,result}`, `ep_retry_lag_seconds{tier}` (processing time minus `not_before`), `ep_retry_paused_partitions{tier}`, `ep_dlq_published_total{error_class}`, `ep_dlq_size` (unreplayed rows in `dlq_meta`), `ep_dlq_oldest_unreplayed_age_seconds`. Add them to the step 3 dashboard and to the audit's `in_flight` (retry topic lag) and `dlq_unique` terms.

**F. Switch** `RETRY_TOPICS_ENABLED=true|false` (false = Stage 2 behavior: in-process retry then DLQ) for C14 and for the ADR comparison. Write `docs/adr/0006-retry-topics-vs-in-process.md`.

### 5.2 What to cover

Every class in the table; permanent failure; transient that recovers on tier 1, 2 or 3; transient that never recovers; systemic outage (no retry flood, no DLQ); poison inside a large batch (only the bad row isolated); partition progress while a record is in retry; `not_before` respected; crash at each new point; DLQ metadata completeness; DLQ duplicates.

### 5.3 What to test

| Type | Test | Pass condition |
| --- | --- | --- |
| Unit | Classifier table: every SQLSTATE family, Node codes, schema errors, unknown | Expected class and action |
| Property | `backoffMs`: for any attempt/base/max, result in `[cap/2, cap]`, cap non-decreasing in attempt, never > max | No counterexample |
| Unit | Router: attempt -> tier mapping, exhaustion -> DLQ, headers carried and incremented, original topic/offset preserved across tiers | Exact headers |
| Unit | `not_before` handling: pause/seek/resume called with the right values (fake clock) | Spec behavior |
| e2e | Permanent (bad Avro bytes) | 1 DLQ record, `attempts=0`/`1`, `dlq_meta` row with all fields, no retry records |
| e2e | Transient that recovers (`FAULT_ERRORS=...40001x1:marker`) | Stored after about tier-1 delay; 1 retry record; `stored_at - first_failed_at >= tier1 delay / 2` |
| e2e | Transient that never recovers | Passes tiers 1, 2, 3 (use short test delays), then DLQ with `attempts=3` |
| e2e | Healthy records after the failing one in the same partition | Stored without waiting for the retry |
| e2e (toxiproxy) | Postgres down for 30 s | Partition paused; **0** retry or DLQ records; everything stored after recovery |
| e2e | Retry rate limit exceeded | Switches to pause; counter moves |
| e2e | 1 poison among 500 good | 499 stored, 1 DLQ, retry volume = 0 for good rows |
| Crash matrix | All `retry:*` and `dlq:*` points | No loss; DLQ duplicates counted, `dlq_meta` has one row per source offset |

### 5.4 Done when

- [ ] `packages/retry` used by sink (and later by replay and relay).
- [ ] Retry tiers with `not_before`, pause-not-sleep, bounded attempts and rate limit.
- [ ] DLQ records + `dlq_meta` with full metadata and dedup on source offset.
- [ ] Outage never floods retry or DLQ.
- [ ] Metrics on dashboard and in audit; ADR 0006 written.

---

## Step 6: S3-F6. A safe DLQ replay tool

> **Requirement:** a replay tool re-injects DLQ messages safely, with filters and a dry-run mode.

### 6.1 What to build

**A. Migration `0007_dlq_replays.sql`**: `dlq_replays (replay_id uuid PK, started_at, finished_at, operator text, filters jsonb, dry_run bool, selected int, replayed int, skipped int, failed int, status text)`.

**B. `tools/dlq-replay` CLI** (TypeScript, `pnpm dlq-replay <cmd>`, also `make dlq-replay ARGS=...`)

| Command | What it does |
| --- | --- |
| `stats` | Counts of unreplayed dead letters by `error_class`, tenant, `failed_topic`, age buckets. The start of triage. |
| `list [filters]` | Table of matching `dlq_meta` rows (id, tenant, event id, class, code, message, attempts, age, replay count). |
| `show <id>` | One dead letter in full: metadata, headers, and the decoded value (Avro via `packages/schema`, JSON, or hex for unparseable bytes). |
| `replay [filters]` | Plan and (with `--execute`) re-inject. **Dry run is the default.** |
| `reindex` | Rebuild missing `dlq_meta` rows from the `events.dlq` topic (assign + seek, no consumer group commits). |

**Filters** (all combinable): `--id`, `--tenant`, `--event-id`, `--error-class`, `--error-code`, `--failed-topic`, `--source-partition`, `--source-offset-from/--to`, `--since/--until` (failed time), `--limit`, `--include-replayed`.

**Replay behavior**

1. Select rows from `dlq_meta` with the filters (unreplayed by default). Print the **plan**: count, breakdown by class and tenant, 5 samples, target topic, rate. In dry run, also run the **pre-check** (below) and report how many would fail again. Write a `dlq_replays` row with `dry_run=true`. Exit. Nothing is produced and `dlq_meta` is unchanged.
2. With `--execute`: ask for confirmation (`--yes` to skip in scripts), then for each row read the exact record from `events.dlq` at `(dlq_partition, dlq_offset)`.
3. **Pre-check** (`--check`, default on): run the same parse/deserialize step the sink uses. Records that would fail permanently again are skipped (`--include-failing` to force). This stops a replay from refilling the DLQ.
4. Produce to `source_topic` (`events.raw`) with the **original key, value and headers**, plus `replay.id`, `replay.count`, `replay.dlq_offset`. Idempotent producer, `acks=all`, rate limit `--rate` (default 100/s).
5. After the ack, `UPDATE dlq_meta SET replayed_at=now(), replay_count=replay_count+1, last_replay_id=$1`.
6. **Loop guard:** if `replay.count` would exceed `DLQ_MAX_REPLAYS` (default 3), skip and report. When a replayed record fails again, the sink copies `replay.count` into the new dead letter so the guard works across rounds.
7. Finish the `dlq_replays` row with counts and status.

**Why it is safe (write it in the runbook):** the sink dedups on `(tenant_id, event_id)`, so replaying a record that was in fact stored, or replaying twice, changes nothing. A crash mid-replay leaves some rows marked and some not; re-running the same command replays the rest, and any record produced but not yet marked becomes a counted sink duplicate.

- Crash points: `replay:after-produce-before-mark`.
- Metrics: the tool writes a Prometheus textfile (`infra/compose/textfile/dlq_replay.prom`) with `ep_dlq_replay_selected/replayed/skipped/failed` for the last run, so "replay success rate" is on the dashboard.

**C. Runbook `docs/runbooks/dlq-triage-and-replay.md`:** `stats` -> find the class -> `show` a few -> fix the cause (code, schema registration, data fix) -> `replay --error-class X` dry run -> `--execute` -> verify with the audit and `ep_sink_duplicates_total`.

### 6.2 What to cover

Every filter; dry run produces nothing and changes nothing; execute replays exactly the selection; replay twice is harmless; still-poison records skipped or, when forced, returned to the DLQ with an incremented count; loop guard; crash mid-replay; `reindex` restores missing metadata; replayed events end up stored exactly once.

### 6.3 What to test

| Type | Test | Pass condition |
| --- | --- | --- |
| Unit | Filter parsing and SQL builder (parameterized, no string concatenation of values) | Expected SQL + params |
| Unit | Header building for replay; loop guard decision | Exact headers; skip at limit |
| e2e | Create dead letters: 3 that are now fixable (fault cleared), 2 still poison | DLQ and `dlq_meta` populated |
| e2e | `replay` dry run | 0 messages produced, `dlq_meta` unchanged, plan says 3 OK / 2 would fail |
| e2e | `replay --execute --yes` | 3 events stored once; 3 rows marked; 2 skipped |
| e2e | Run the same replay again with `--include-replayed` | Sink duplicates +3, rows unchanged |
| e2e | `--include-failing` | 2 records back in DLQ with `replay.count=1`; after 3 rounds, skipped by guard |
| e2e | Filters: by tenant, class, time, offset range | Only matching rows selected |
| e2e | Crash at `replay:after-produce-before-mark`, re-run | All stored once; all marked |
| e2e | Delete `dlq_meta` rows, run `reindex` | Rows restored with the same source offsets |

### 6.4 Done when

- [ ] All commands implemented; dry run is the default.
- [ ] Pre-check, loop guard, rate limit, bookkeeping in `dlq_replays`.
- [ ] Replay is idempotent end to end, proven by tests.
- [ ] Runbook written.

---

## Step 7: S3-F4. Transactional outbox for tenant config changes

> **Requirement:** config changes that must also publish an event use a transactional outbox (first use: tenant config change events, later used for cache invalidation).

### 7.1 What to build

**A. Migration `0008_tenant_config_outbox.sql`**

```sql
CREATE TABLE tenants (tenant_id text PRIMARY KEY, name text NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE tenant_config (
  tenant_id  text PRIMARY KEY REFERENCES tenants,
  config     jsonb       NOT NULL,
  version    bigint      NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE outbox (
  id           bigserial PRIMARY KEY,
  aggregate_id text        NOT NULL,
  topic        text        NOT NULL,
  msg_key      text        NOT NULL,
  payload      bytea       NOT NULL,
  headers      jsonb       NOT NULL DEFAULT '{}',
  created_at   timestamptz NOT NULL DEFAULT now(),
  attempts     int         NOT NULL DEFAULT 0,
  published_at timestamptz
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
```

Seed `tenants` from the API keys the ingest API already knows (`scripts/seed-tenants.sh`). The `attempts` column is an addition to the PRD table so republishing is measurable.

**B. `services/admin-api` (new, small Nest service)**

A separate service, not a module in `ingest-api`, so the ingest hot path keeps zero dependency on Postgres (its failure modes stay as measured in Stage 2). Record this in the service README.

- Auth: `Authorization: Bearer <ADMIN_API_KEY>` (separate from tenant keys).
- `GET /v1/admin/tenants/:tenantId/config` -> `{config, version}` with `ETag: "<version>"`.
- `PUT /v1/admin/tenants/:tenantId/config` body `{config}`, optional `If-Match: "<version>"` (412 on mismatch, so two admins cannot silently overwrite each other). Validate `config` with a DTO (known keys and types, size limit).
- One transaction:

  ```
  BEGIN
    SELECT version FROM tenant_config WHERE tenant_id=$1 FOR UPDATE   (404 if tenant unknown)
    INSERT ... ON CONFLICT (tenant_id) DO UPDATE SET config=$2, version=tenant_config.version+1, updated_at=now()
    RETURNING version
    outbox.enqueue(tx, { aggregate_id: tenant_id, topic: 'config.changed', msg_key: tenant_id,
                         payload: <Avro config.changed>, headers: { event_id: <uuid>, version } })
  COMMIT
  ```

- `config.changed` payload (schema `schemas/config/v1.avsc`, subject `config.changed-value`, registered and checked by step 4's tool): `event_id` (uuid, for consumer dedup), `tenant_id`, `version`, `changed_at`, `config_json` (**full snapshot**, not a diff, so a consumer can apply "if version > mine" and never needs the previous event).
- Crash point: `admin:after-update-before-commit` (must publish nothing).
- No-op PUT (same config): decide and document (recommended: still bump the version and publish; simpler and harmless).

**C. `packages/outbox`**

- `enqueue(client, msg)`: insert using the **caller's transaction client**. It refuses to run outside a transaction (checks `txid_current_if_assigned()` is not null) so the dual-write bug cannot come back by accident.
- `PollingRelay`:
  1. `BEGIN; SELECT id, topic, msg_key, payload, headers FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT $batch FOR UPDATE SKIP LOCKED;`
  2. `UPDATE outbox SET attempts = attempts + 1 WHERE id = ANY($ids)` is done in a **separate short transaction before producing**, so a crash after publish still leaves `attempts > 1` as evidence of a republish. (Alternative: keep everything in one transaction and count republishes at the consumer; choose one, document it.)
  3. Produce all rows (idempotent producer, `acks=all`, headers include `outbox_id`), wait for every ack.
  4. `UPDATE outbox SET published_at = now() WHERE id = ANY($ids); COMMIT`.
  - Publish-then-mark = **at-least-once** (crash between 3 and 4 republishes).
  - Poll interval `RELAY_POLL_MS` (default 200) plus optional `LISTEN outbox_new` / `NOTIFY` from `enqueue` to cut latency without busy polling.
  - Kafka down: produce fails, transaction rolls back, rows stay unpublished, backoff with jitter (`packages/retry`), never drops.
- **Ordering decision:** with several relays, `SKIP LOCKED` lets relay B publish row 11 while relay A is still publishing row 10 of the same tenant. Choose **single active relay** using `pg_try_advisory_lock(<const>)`: extra relay instances stand by and take over when the leader's session dies. Keep `RELAY_MODE=multi` (no advisory lock) only for C8. Consumers are still protected by the `version` field.
- Retention: `RELAY_RETENTION` job deletes published rows older than 7 days in small batches, so the table stays bounded.

**D. `services/outbox-relay`**: a runnable process wrapping `PollingRelay` with `/healthz`, `/metrics`, `/debug/stats`, the fault injector (`relay:before-publish`, `relay:after-publish-before-mark`, `relay:after-mark-before-commit`) and graceful shutdown (finish the in-flight batch, release the lock). Switch `OUTBOX_RELAY_ENABLED`.

**E. Minimal consumer helper** `applyIfNewer(current, event)` in `packages/outbox` (or `packages/config-events`): applies a `config.changed` event only if `event.version > current.version`, dedups on `event_id`. Stage 4 cache invalidation will use it; here it is used by tests to prove consumers stay correct under duplicates and reordering.

**F. Metrics:** `ep_outbox_unpublished` (gauge, from a cheap `count(*)` on the partial index every few seconds), `ep_outbox_oldest_unpublished_age_seconds`, `ep_outbox_published_total`, `ep_outbox_republished_total` (rows published with `attempts > 1`), `ep_outbox_publish_latency_seconds` (created_at -> published_at), `ep_outbox_relay_is_leader`. Add to dashboard and audit (`outbox lag`).

**G. Docs:** `docs/adr/0007-polling-outbox-vs-cdc.md` (polling now: simple, no extra infra, costs DB polling and latency; CDC with Debezium reading the WAL as the production-scale alternative: no polling, preserves commit order, needs Kafka Connect and replication slots). `docs/runbooks/outbox-stuck.md` (symptoms: oldest age rising; checks: relay leader, Kafka health, lock holders in `pg_locks`, rows with high `attempts`; actions).

### 7.2 What to cover

The dual-write problem is gone: committed change -> published at least once; rolled back change -> never published. Crash at every relay and admin point. Kafka down. Two relays (single-leader mode and multi mode). Optimistic concurrency on the admin API. Consumer correctness under duplicates and reordering.

### 7.3 What to test

| Type | Test | Pass condition |
| --- | --- | --- |
| Unit | Outbox writer refuses to run outside a transaction | Throws |
| Unit | `applyIfNewer`: older, equal, newer versions; duplicate `event_id` | Only newer applied |
| Unit | Admin DTO validation; `If-Match` handling | 400/412 as specified |
| e2e | PUT config | Row in `tenant_config` with version+1; exactly 1 outbox row; relay publishes it; `config.changed` message decodes with the registered schema |
| e2e | Transaction rolled back (crash at `admin:after-update-before-commit`, or forced error after enqueue) | No config change, no outbox row, nothing published |
| e2e | Crash relay at `relay:before-publish` | Published after restart; 0 lost |
| e2e | Crash relay at `relay:after-publish-before-mark` | Published twice; `attempts=2`; republish counter 1; consumer applies once |
| e2e | Kafka down for 30 s while changes keep coming | Oldest age grows, nothing lost, all published in order after recovery |
| e2e | Two relays, single-leader mode, 1,000 ordered changes for one tenant | Messages in version order; standby takes over when the leader is killed |
| e2e | Two relays, `RELAY_MODE=multi` | Order violations measured and reported (not asserted to be zero); consumer final state still the latest version |
| Crash matrix | All `relay:*` and `admin:*` points | Rows: published count, duplicates, lost (must be 0) |

### 7.4 Done when

- [ ] Admin API writes config + outbox row in one transaction, with optimistic concurrency.
- [ ] Relay publishes at least once, single leader by default, bounded table, backoff on Kafka failure.
- [ ] `config.changed` schema registered and checked in CI.
- [ ] Every relay/admin crash point in the matrix; no loss.
- [ ] Metrics, ADR 0007 and outbox-stuck runbook written.

---

## Step 8: S3-F8. Late and out-of-order events are stored correctly

> **Requirement:** late and out-of-order events are stored correctly, keeping both `occurred_at` and `received_at`.

### 8.1 What to build

**A. Time fields, end to end (write the rules into `docs/api-contract.md`)**

- `occurred_at`: client-supplied event time. Stored exactly as received after normalization to UTC. **Precision decision:** JavaScript `Date` and Avro `timestamp-millis` keep milliseconds; if a client sends microseconds they would be silently truncated. Choose one and test it: (a) reject more than 3 fractional digits with 400, or (b) accept and truncate to milliseconds, documented. Recommended: (b), documented, and the test asserts the exact stored value.
- `received_at`: **server time** set by the API when the request arrives (already true since Stage 2; it is now an Avro field as well as a header). Never from the client. Retries, replays and DLQ replays **keep the original `received_at`** (it travels in the record), so a replayed event does not look "new".
- `stored_at`: sink commit time (exists). `stored_at - received_at` = pipeline delay; `received_at - occurred_at` = lateness.

**B. Clock-skew bounds at the API (C13 policy)**

- `OCCURRED_AT_MAX_FUTURE_S` (default 300): `occurred_at > received_at + 5 min` -> **422** `occurred_at_in_future`.
- `OCCURRED_AT_MAX_PAST_DAYS` (default 365): older -> **422** `occurred_at_too_old`. The bound is deliberately generous because G5 says late events are stored regardless of lateness; it only stops obviously broken clocks (year 1970, year 2099).
- Both bounds checked **only at the API**. The sink stores whatever it gets (a replay of old data a year later must still be stored), so `received_at` is compared, not `now()`.
- Metrics: `ep_event_lateness_seconds` histogram (`received_at - occurred_at`, buckets from 1 s to 30 days) at the API, `ep_event_rejected_total{reason="future|too_old"}`, `ep_sink_lateness_seconds` at the sink.

**C. Migration `0009_events_lateness.sql`**: no new columns are needed (`occurred_at`, `received_at`, `stored_at` exist). Add a `CHECK (received_at IS NOT NULL)` audit query in the migration notes, and an index decision: keep `events_tenant_time_idx (tenant_id, occurred_at)` for event-time queries; do **not** add a `received_at` index unless a query needs it (write the reason). If you decide nothing is needed, skip the migration and say so in the step notes.

**D. Order-insensitivity review of the sink.** Go through every write path and confirm nothing depends on arrival order:

- Insert is `DO NOTHING` keyed on `(tenant_id, event_id)` (step 1): set semantics.
- Conflicting duplicates: "first committed wins" is the only order-dependent case, and it only happens for a producer bug; it is counted (step 1/3) and the API already rejects it within the window (step 2). Document it as the one exception.
- No "latest event" or counter column updated by the sink. If one is ever added, it must use `GREATEST(occurred_at)`-style updates, never "last write wins".
- Retry tiers store late (step 5): still correct because of the points above.

### 8.2 What to cover

Shuffled order, events delayed by minutes, hours and a day (by `occurred_at` and by actually sending later), duplicates mixed in, retried records arriving after newer ones, clock-skew bounds, precision, `received_at` preservation across retry and replay.

### 8.3 What to test

| Type | Test | Pass condition |
| --- | --- | --- |
| Unit | Skew bounds at exactly the limits (inclusive/exclusive decided and tested), timezone offsets normalized to UTC | 202/422 as specified |
| Unit | Precision rule | Exact stored value |
| **Property (`fast-check`)** | Generate a set of events, add duplicates, shuffle, assign random lateness (0 s to 24 h), split into random batches; feed them to the real `BatchWriter` against the test DB (each case in its own schema or truncated table). Compare with a reference model (`Map` keyed by `(tenant, event_id)`, first-wins) | Stored set = expected set; per-tenant counts and `min/max(occurred_at)` equal the reference; duplicates counter = generated duplicates. **At least 1,000 runs** (keep each case <= 50 events so it fits in about a minute; run 10,000 with `PROPERTY_RUNS=10000` nightly) |
| Property | Same generator, but delivered through the full pipeline (API -> Kafka -> sink) with fewer runs (for example 20) | Same equality |
| e2e | **Reorder and late:** send dataset D in shuffled order with 10% of events having `occurred_at` 1 min, 1 h, 24 h in the past, some actually sent after a delay | Table equals D exactly; `occurred_at` byte-for-byte as expected; `received_at` >= send time |
| e2e | Event goes through retry tier, then a newer event of the same tenant is stored first | Both stored, correct `occurred_at`; `received_at` of the retried one is the original |
| e2e | DLQ replay of an old event | `received_at` unchanged from the original request |
| e2e | `occurred_at` 1 year + 1 day old, 10 min in future | 422 with the right reason, nothing produced |

### 8.4 Done when

- [ ] Time field rules (source, precision, preservation) implemented and documented.
- [ ] Skew bounds enforced at the API, not the sink; metrics present.
- [ ] Sink order-insensitivity reviewed and written down, with the one documented exception.
- [ ] Property test (>= 1,000 runs) and reorder/late e2e tests pass.

---

## Step 9: S3-F9. The delivery guarantees document, every claim mapped to a test

> **Requirement:** a published delivery guarantees document, with each claim mapped to an automated test.

Last, because it may only claim what steps 1 to 8 proved.

### 9.1 What to build

**A. `docs/delivery-guarantees.md`**, one section per claim, in the fixed format from the PRD:

```
### G2 Storage idempotency
- Claim: any number of deliveries of one (tenant_id, event_id) results in exactly one stored row.
- Conditions: Postgres primary key in place; SINK_DEDUP not disabled.
- Mechanism: PK (tenant_id, event_id) + INSERT ... ON CONFLICT DO NOTHING (step 1).
- Proved by: [test: G2.same-chunk-twice] [test: G2.triple-replay] [test: G2.crash-matrix]
- Not guaranteed: which copy wins if two copies differ in content (first committed wins; counted as a conflict).
```

Claims to write (only after their tests pass; adapt the wording to what the tests actually prove):

| Claim | Proved by (tests from earlier steps, plus the two new ones below) |
| --- | --- |
| **G1 Durability:** after a 202 (acks=all, min.insync.replicas=2, RF=3), the event is not lost | New `G1.durability` test: send N, get 202s, kill the API and the consumer (SIGKILL) and one broker, restart, all N stored. Plus the API crash-matrix rows. |
| **G2 Storage idempotency** | Step 1 tests, step 3 replay tests |
| **G3 Replay safety:** replaying any range of the log does not change stored data | Step 1 offset-replay and triple-replay checksum tests; step 6 replay-twice test |
| **G4 Ordering:** events with the same key are ordered within a partition; no global order; retried events may be stored after later ones | New `G4.per-key-order` test: one tenant, sequence numbers in payload, consume `events.raw` directly, offsets in sequence order. Step 5 test showing the retry exception. |
| **G5 Late data:** stored regardless of lateness (within the documented skew bounds); query semantics are the analytics layer's job | Step 8 property and e2e tests |
| **G6 Failure handling:** a failing message cannot block a partition indefinitely and is never silently dropped; retried within limits, then quarantined with metadata; a dependency outage pauses instead of dead-lettering | Step 5 tests (healthy records keep flowing, tiers then DLQ, outage pauses), step 6 replay tests |
| **G7 Config events:** a config change is published at least once if and only if its transaction committed | Step 7 tests (commit -> published, rollback -> nothing, relay crash matrix) |
| **API idempotency (G8, new):** within `IDEM_TTL_S`, the same key returns the original result and produces nothing; a different payload gets 422; 50 concurrent identical requests produce one event | Step 2 tests |
| **Duplicate visibility (G9, new):** every dropped duplicate is counted at the layer that dropped it; the audit reconciles to zero | Step 3 tests |
| **Not guaranteed** | Global ordering; API dedup beyond the Redis window or after Redis loss; dedup when `IDEM_FAILURE_POLICY=open` and Redis is down; visibility latency below the freshness target; ordering of `config.changed` in multi-relay mode; anything without a test |

Also a short section **"Where exactly-once stops"**: Kafka's idempotent producer stops broker-level retry duplicates within a producer session only; Kafka transactions would cover read-process-write inside Kafka but not the Postgres write; EventPulse gives at-least-once delivery + idempotent storage = exactly-once **effect** in Postgres, and nothing more.

**B. Stable test ids.** Every test referenced by the document has a stable id in its name, for example `it('[G2.triple-replay] replaying the log three times leaves the table unchanged', ...)`. Add the ids to the existing tests from steps 1 to 8 now.

**C. Crash matrix in the document.** `make stage3-correctness` runs every crash-matrix scenario (consumer, API, retry, DLQ, replay, relay, admin) and regenerates `docs/generated/crash-matrix.md`. The guarantees document embeds or links it, with the run date and commit hash in the generated header. Columns: crash point, duplicates produced, duplicates processed, duplicates stored, lost, recovery time, notes.

**D. `tools/guarantee-check`**

- Parses `docs/delivery-guarantees.md` for every `[test: <id>]`.
- Runs (or reads) the Vitest JSON report from `tests/correctness` and the services' e2e suites (`--reporter=json --outputFile=...`).
- Fails if: a claim has no test reference; a referenced id does not exist; a referenced test was skipped or failed; the crash matrix is older than the latest change to `packages/fault-injector/src/points.ts` or has a row with `lost > 0` or `duplicates stored > 0`.
- Runs in CI after the test jobs (`.github/workflows/ci.yml`, job `guarantees`). Decision: CI runs unit tests, `schema-check` and `guarantee-check`. The infrastructure-heavy e2e suites (Kafka, Redis, registry, Postgres) run locally with `make stage3-correctness`, which writes `docs/generated/test-report.json` (Vitest JSON, with the commit hash). That report is committed, and in CI `guarantee-check` reads it and fails if its commit is older than the last change to any source or test file.

**E. Index.** `docs/README.md` (or a section in the root README) linking the guarantees document, the generated crash matrix, ADRs 0002 to 0007, the three runbooks and the updated API contract.

### 9.2 What to cover

Every claim maps to at least one passing test; every guarantee from the PRD list (G1 to G7) is either claimed with a test or moved to "not guaranteed" with a reason; the document says what it does **not** promise; the crash matrix is generated, current and complete.

### 9.3 What to test

| Type | Test | Pass condition |
| --- | --- | --- |
| e2e | `G1.durability` (new) | All N acknowledged events stored after kills |
| e2e | `G4.per-key-order` (new) | Offsets follow sequence order per key |
| Unit | `guarantee-check` against fixture docs: missing test id, failed test, skipped test, stale matrix, matrix with loss | Each fails with a clear message; a good fixture passes |
| CI | Remove a test id from a test file on a branch | `guarantees` job fails |
| Full suite | `make stage3-correctness` from a clean `make stage3-up` | All suites green; crash matrix regenerated with 0 lost and 0 duplicates stored |

### 9.4 Done when

- [ ] `docs/delivery-guarantees.md` written in the fixed format; G1 to G7 plus any added claims, each with test ids; "not guaranteed" section; "where exactly-once stops" section.
- [ ] Crash matrix generated by the suite and linked.
- [ ] `tools/guarantee-check` passes locally and in CI.
- [ ] Docs index links guarantees, matrix, ADRs and runbooks.

---

## 10. Stage 3 server exit checklist

Maps the PRD exit criteria to the steps (experiment notebook entries C1 to C14 are out of scope for this plan).

| PRD exit criterion | Step |
| --- | --- |
| Replaying the same data 3 times leaves storage unchanged, proven by an automated test | 1, 3 |
| Out-of-order and late-event tests pass | 8 |
| Crash matrix covers every named crash point and its results are recorded | 1 (runner), 2, 5, 6, 7 (points), 9 (published) |
| Property-based tests run clean | 2, 5, 8 |
| Idempotency key semantics implemented and tested, including conflicts and concurrency | 2 |
| Outbox with relay works and its failure modes are documented | 7 |
| Retry tiers, DLQ and a replay tool (with dry run) work and are tested | 5, 6 |
| Schema registry in use with a CI compatibility check | 4 |
| Delivery guarantees document published, every claim linked to a test | 9 |
| Repo tagged `stage-3-complete` | after step 9 and the experiments |

### Knobs left for the experiments (built above, not used by this plan)

| Knob | Step | Used by |
| --- | --- | --- |
| `FAULTS=<point>@<n>`, `POST /debug/faults`, `FAULT_ERRORS=...` | 1, 5 | C3, C7, C8, C9 |
| `IDEM_ENABLED`, `IDEM_TTL_S`, `IDEM_INFLIGHT_TTL_S`, `IDEM_FAILURE_POLICY`, Redis via toxiproxy | 2 | C1, C4, C5, C6, C7, C14 |
| `SINK_DEDUP`, `events_nodedup` | 1 | C14 |
| `SERIALIZATION=json|avro`, registry via toxiproxy | 4 | C10, C11, C14 |
| `RETRY_TOPICS_ENABLED`, `RETRY_TIER_DELAYS_MS`, `RETRY_MAX_ATTEMPTS`, `RETRY_MAX_RATE_PER_S`, classifier override for misclassification | 5 | C9, C12, C14 |
| `RELAY_MODE=single|multi`, `OUTBOX_RELAY_ENABLED`, relay crash points | 7 | C8, C14 |
| `OCCURRED_AT_MAX_FUTURE_S`, `OCCURRED_AT_MAX_PAST_DAYS` | 8 | C2, C13 |
| Extended audit, correctness dashboard | 3 | all |

For C9, add one debug-only switch in step 5: `CLASSIFIER_OVERRIDE=<code>=<class>` (requires `DEBUG_FAULTS=true`) so a permanent error can be forced to look transient and the other way round.
