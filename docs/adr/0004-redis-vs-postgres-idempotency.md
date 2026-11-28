# ADR 0004: Redis for the API idempotency store, Postgres for the sink's key

- **Status:** Accepted
- **Date:** 2026-11-27
- **Author:** Goutam Dogri
- **Stage:** 3 (Correctness), requirement S3-F1
- **Affects:** `packages/idempotency` (`RedisStore`, Lua scripts), `infra/compose/docker-compose.yml` (`ep-redis`), `services/ingest-api/src/idempotency/`

## Context

Layer 1 of deduplication (ADR 0003) needs a store shared by every API instance that can do an atomic "set if absent" with an expiry, answer in well under a millisecond on the request path, and forget keys after the dedup window. Since Stage 2 the ingest API has had **no** dependency on Postgres (its failure modes were measured that way).

## Options considered

| | Redis (`SET NX EX` + Lua) | Postgres table with a unique key | In-process memory |
| --- | --- | --- | --- |
| Atomic check-and-set across instances | yes (one Lua script per transition) | yes (`INSERT ... ON CONFLICT`, row locks) | no (per instance) |
| Latency per request | sub-millisecond (measured below) | a round trip + WAL write + index update per request | ~0 |
| Expiry | built in (`EX`), per key, no cleanup job | needs a cleanup job and makes the table churn | needs its own eviction |
| Durability | none (no AOF, no RDB, by choice): lost on restart / flush | durable | lost on restart |
| New dependency for the API | Redis | Postgres back on the ingest hot path (Stage 1 problems) | none |
| Failure window | keys lost on Redis loss; bounded by TTL | unbounded history, but every ingest waits on the database | per instance only |

## Decision

- **Redis** (single instance, `redis:7.4.2-alpine`, `appendonly no`, `save ""`, `maxmemory 1gb`, `maxmemory-policy noeviction`, container limit 1280 MB) for layer 1. Durability is not needed because correctness lives in the sink (ADR 0003).
- **`noeviction`:** when Redis is full, writes fail visibly (and `IDEM_FAILURE_POLICY` applies) instead of keys being evicted silently, which would look like a working dedup that quietly lets duplicates through.
- Every transition is **one Lua script** (`acquire`: SET NX EX or return the current value; `replace`/`remove`: only if the value is exactly the caller's inflight value, the owner check). No read-then-write races between instances or between concurrent requests (50 parallel identical requests produce exactly one record: `test/idempotency.e2e-spec.ts`).
- Client settings: `enableOfflineQueue: false`, `maxRetriesPerRequest: 0`, `commandTimeout` 50 ms, background reconnect with backoff, plus a circuit breaker (`IDEM_BREAKER_FAILURES`, default 5). A dead Redis costs a request at most the command timeout, and nothing once the breaker is open.
- **Postgres** keeps the durable, unbounded key in the sink (`(tenant_id, event_id)`, ADR 0002).

## Reasons

1. Redis gives atomic set-if-absent with per-key expiry in one round trip, without a cleanup job.
2. A Postgres idempotency table would put the database back on the ingest path, undoing the Stage 2 decoupling (B-series and K-series measurements).
3. The durability Redis lacks is provided by the sink's key, so the lack of persistence is acceptable and keeps Redis simple.

## Consequences

- Redis loss (flush, restart, outage) re-opens the window: duplicates reach Kafka and are absorbed by the sink (test: "Redis flushed between two identical requests").
- **Window: `IDEM_TTL_S` = 1 h (default).** Client retries happen within seconds to minutes, so an hour covers them with a wide margin. Anything older (a replay a day later) passes layer 1 and is stopped by the sink.
- Memory grows with the number of keys inside the window. The 1 GB cap plus `noeviction` makes the limit explicit: `memory ≈ 276 B × events/s × IDEM_TTL_S`.
- Clustering and failover are Stage 5.

## Measurements

Local Docker Redis (test database 15), one client, 100,000 sequential requests with UUID keys (`bench-idem.mjs`, run from the scratchpad, not committed):

| Operation | p50 | p99 |
| --- | ---: | ---: |
| New request: `acquire` + `complete` (2 Lua round trips) | 0.098 ms | 0.176 ms |
| Replayed request: `acquire` only | 0.057 ms | 0.107 ms |

- **Memory: 276 bytes per completed key, about 264 MiB per million keys** (key `idem:<tenant>:<uuid>`, value `done:<64 hex hash>:<uuid>`, plus Redis overhead).
- **Sizing decision:** the first defaults (24 h window, 256 MB) held about 970,000 keys, which covers only about 11 events/s sustained; 1,000 events/s over 24 h would need about 22 GB. Decided: **1 h window and 1 GB** (`maxmemory 1gb`). That holds about **3.9 million keys, about 1,080 events/s sustained** over the window.
- Above that rate Redis fills up. With `noeviction` every new key then fails, and the API runs in `fail_open` (correct, but with no API-level dedup) until keys expire. Watch `redis_memory_used_bytes` against `redis_memory_max_bytes`, and `ep_idem_requests_total{result="fail_open"}`.
- If more rate is needed: more memory, a shorter window, or a smaller value (a binary or truncated hash instead of 64 hex characters). C4 and C14 measure these trade-offs.

Crash matrix (`docs/generated/crash-matrix.md`): 200 requests with a client that retries the same key. `api:before-produce` gives 0 duplicates produced (5 × 409 until the 2 s inflight TTL). `api:after-produce-before-complete` gives exactly 1 extra record in Kafka and 1 duplicate absorbed by the sink. `api:after-complete-before-response` gives 0 duplicates produced and 1 replayed 202. Every point: 0 lost, 0 stored twice.

## Rejected alternatives

- **Postgres table:** durable but puts a database write on every ingest request and needs cleanup.
- **In-process memory:** useless with more than one API instance and lost on every deploy.
- **Redis with `allkeys-lru` eviction:** silently drops keys under memory pressure; a full store should be loud.
