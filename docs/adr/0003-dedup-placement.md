# ADR 0003: Where deduplication lives: two layers, correctness only in the sink

- **Status:** Accepted
- **Date:** 2026-11-27
- **Author:** Goutam Dogri
- **Stage:** 3 (Correctness), requirements S3-F1 and S3-F2
- **Affects:** `packages/idempotency`, `services/ingest-api/src/events/events.service.ts`, `services/consumer-sink/src/sink/batch-writer.ts`

## Context

Duplicates enter EventPulse in several places: client retries after a timeout, the API dying between the Kafka ack and its response, ambiguous produce failures, consumer crashes between the database commit and the offset commit, rebalances, offset resets, DLQ replays. Every one of them must end as **one stored row**. S3-F1 also asks that a repeated request with the same idempotency key returns the original result and that a reused key with a different payload is rejected.

## Options considered

| | Sink only (Postgres key) | API only (Redis) | Both layers, sink authoritative |
| --- | --- | --- | --- |
| Stored data correct under every duplicate source | yes | **no**: consumer-side duplicates (crash, rebalance, replay) never pass the API; Redis loss or TTL expiry lets duplicates through | yes |
| Client sees the original result on retry, and a 422 on key reuse (S3-F1) | no (every retry is "new" to the API) | yes | yes |
| Duplicate load on Kafka and the sink | every client retry | only what passes the window | only what passes the window |
| Depends on Redis being up and intact | no | yes, for correctness | no (only for saving work) |

## Decision

**Two layers, deliberately:**

1. **Layer 1, API (Redis):** `idem:{tenant}:{Idempotency-Key or event_id}` with an `inflight` / `done` state machine (`packages/idempotency`). Cheap and early: a retried request is answered from Redis and never reaches Kafka. It is an **optimization and a client-facing contract** (202 replay, 409, 422). It is bounded by `IDEM_TTL_S` and can be lost.
2. **Layer 2, sink (Postgres primary key `(tenant_id, event_id)`):** the **source of truth** (ADR 0002). Correctness never depends on layer 1.

**Failure policy for layer 1: fail open by default** (`IDEM_FAILURE_POLICY=open`). When Redis is unreachable, the API accepts events without layer-1 dedup and counts them (`ep_idem_requests_total{result="fail_open"}`). This is safe *because* of layer 2; it only costs duplicate load. `closed` (503 while Redis is down) is available for setups that prefer rejecting load to absorbing duplicates.

## Reasons

1. Only the sink sees every duplicate source; only the API can answer the client with the original result. Neither layer alone meets S3-F1 + S3-F2.
2. Putting correctness in Redis would make an in-memory cache with a TTL and no persistence the source of truth (PRD pitfall: "letting Redis dedup become the source of truth").
3. Fail open keeps ingest available during a Redis outage at the price of duplicates that layer 2 already handles. Fail closed would make Redis a hard dependency of ingest for no gain in correctness.

## Consequences

- The gaps of layer 1 are documented in `docs/api-contract.md` and proven by the crash matrix (`docs/generated/crash-matrix.md`):
  - `api:after-produce-before-complete`: retries get 409 until the inflight TTL, then a second copy is produced; the sink stores one row and counts one duplicate.
  - `api:before-produce`: retries get 409 until the inflight TTL, then the event is produced once.
  - `api:after-complete-before-response`: the retry gets the replayed 202; nothing new is produced.
- Anything that counts **before** the sink (a Stage 4 stream processor) must not assume layer 1 removed all duplicates (C4).
- `IDEM_INFLIGHT_TTL_S` must exceed `KAFKA_SEND_TIMEOUT_MS` (checked at startup), otherwise a retry could produce a second copy while the first request still waits for its ack.

## Rejected alternatives

- **Sink only:** fails S3-F1 (no original result, no 422 for key reuse) and sends every retry through Kafka.
- **API only:** loses correctness on Redis loss, on TTL expiry and for every consumer-side duplicate.
- **Fail closed by default:** turns a cache outage into an ingest outage while storage would have stayed correct anyway.
