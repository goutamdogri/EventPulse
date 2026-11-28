# EventPulse Ingest API Contract

*Current as of Stage 3 (S3-F1: idempotent ingest, S3-F7: Avro on events.raw). Each stage that changes a guarantee updates this file.*

## `POST /v1/events`

### Request

- Header `Authorization: Bearer <api key>`. The tenant comes from the key, never from the body.
- Optional header `x-request-id`: up to 128 characters from `A-Z a-z 0-9 . _ : -`. It is copied to the Kafka record for tracing. Missing or invalid values are replaced by a generated UUID.
- Optional header `Idempotency-Key` (Stage 3): 1 to 255 printable ASCII characters, no spaces. Identifies one logical request; send the same key on every retry of it. Without the header, the body's `event_id` is the key. See "Idempotency" below.
- JSON body, at most `BODY_LIMIT` (default 64 KB):

| Field | Type | Rule |
| --- | --- | --- |
| `event_id` | string | UUID. The client generates it and reuses it on every retry of the same event. |
| `type` | string | 1 to 100 characters |
| `occurred_at` | string | ISO 8601 timestamp |
| `schema_version` | integer | ≥ 1 |
| `payload` | object | JSON object (not an array) |

### Responses

| Status | Meaning |
| --- | --- |
| **202** `{ "event_id": "..." }` | The event is **durably appended to the replicated log** (`events.raw`) under the configured acks policy. See below. With header `Idempotent-Replayed: true`: this key was already accepted; this is the **original** result and nothing new was written. |
| 400 | Body failed validation, is not valid JSON, `Idempotency-Key` is invalid, or the event does not fit the registered schema. Nothing was written. |
| 401 | Missing or unknown API key. Nothing was written. |
| 409 + `Retry-After` | `reason: "idempotency_in_progress"`: a request with the same key and the same payload is still being written. Nothing new was written. Retry after `Retry-After` seconds with the same key. |
| 413 | Body larger than `BODY_LIMIT` (rejected at the edge, Kafka never sees it), or an event larger than the batch byte cap. |
| 422 | `reason: "idempotency_key_reused"`: this key was already used with a **different** payload. Nothing was written. This is a client bug (a key reused for another request): do not retry, fix the key. |
| 500 | Unexpected error (a bug). The event **may or may not** have been written. |
| 503 + `Retry-After` | Kafka unavailable, or the ingest queue is full. Body: `{ statusCode: 503, error, message, reason }` with `reason` = `kafka_unavailable`, `overloaded`, `shutting_down`, `idempotency_unavailable` (Redis down and `IDEM_FAILURE_POLICY=closed`) or `schema_unavailable` (the API has not found its writer schema id yet: registry unreachable at startup, or the schema is not registered). The event **may or may not** have been written (a timed-out or failed batch can be partly in the log). |

### 503 in detail (S2-F5)

- `reason: "kafka_unavailable"`: Kafka cannot take the write now (all brokers down, fewer in-sync replicas than `min.insync.replicas`, send timeout, circuit breaker open). After `KAFKA_BREAKER_FAILURES` failed sends the API answers immediately instead of waiting for the send timeout, and probes Kafka again after `KAFKA_BREAKER_COOLDOWN_MS`. The API needs no restart when Kafka returns.
- `reason: "overloaded"`: the in-memory ingest queue is full (`BATCH_MAX_QUEUE` messages or `BATCH_MAX_QUEUE_BYTES`) while Kafka may be healthy.
- `reason: "shutting_down"`: this instance received SIGTERM and is draining its queue; retry (another instance, or this one after its restart).
- `Retry-After: RETRY_AFTER_S` seconds (default 1).
- A request never waits longer than the queue wait plus `KAFKA_SEND_TIMEOUT_MS` (default 5 s).
- Readiness `GET /readyz` is 200 only when the producer is connected, a background Kafka health check (every `KAFKA_HEALTH_INTERVAL_MS`, default 2 s: every partition has a leader and enough in-sync replicas for `acks=all`) passed recently, and the breaker is closed. It follows Kafka in both directions without needing any traffic. Liveness `GET /healthz` stays 200.

## Idempotency (S3-F1)

The same `(tenant, Idempotency-Key or event_id)` accepted repeatedly produces **one** logical event.

| Situation | Response |
| --- | --- |
| First time the key is seen | Written, **202** |
| Same key, same payload, original still in progress | **409** + `Retry-After` (no waiting on the server side: the client already retries) |
| Same key, same payload, original completed | **202** with the original `event_id` and `Idempotent-Replayed: true`, nothing written |
| Same key, different payload | **422** `idempotency_key_reused`, nothing written |
| Key older than the dedup window (`IDEM_TTL_S`, default 1 h) | Treated as new: written again. The sink's `(tenant_id, event_id)` key still keeps one stored row. |

- **Scope:** keys are per tenant (the tenant from the API key). Two tenants can use the same key.
- **"Same payload"** is decided by a sha256 of the **canonical** event (`event_id`, `type`, `occurred_at`, `schema_version`, `payload`): object key order, whitespace, number formatting (`1.0` = `1`) and Unicode normalization do not matter; array order does. `occurred_at` is compared as an instant (`+02:00` and `Z` forms of the same time are equal; sub-millisecond digits are ignored).
- **A header key and the `event_id` key are different keys.** Use one or the other consistently for an event.
- **In progress** lasts at most `IDEM_INFLIGHT_TTL_S` (default 30 s). If the API instance died while writing, the key frees itself after that.

### What idempotency does NOT promise (by design; the sink is the source of truth)

- **API dies after writing, before recording the key as done:** retries get 409 for up to `IDEM_INFLIGHT_TTL_S`, then the next retry writes a **second copy** to Kafka. The sink stores one row and counts the duplicate.
- **Ambiguous write failure** (503 after a send timeout: Kafka may have the event): the key is released, so the retry writes again; the sink stores one row.
- **Redis unavailable:** with `IDEM_FAILURE_POLICY=open` (default) events are accepted **without** API-level dedup (counted as `fail_open`); duplicates reach Kafka and the sink absorbs them. With `closed`, the API answers 503 `idempotency_unavailable` until Redis is back. A dead Redis costs a request at most `REDIS_COMMAND_TIMEOUT_MS` (50 ms); after `IDEM_BREAKER_FAILURES` failures, nothing.
- **Redis flushed or restarted:** the window starts again from empty. Correctness of stored data never depends on Redis.
- `/readyz` does not include Redis (with the default policy the API works without it).

Metrics: `ep_idem_requests_total{result}` (`new`, `replayed`, `in_progress`, `payload_mismatch`, `fail_open`, `fail_closed`, `disabled`), `ep_idem_complete_failures_total`, `ep_idem_redis_latency_seconds`, `ep_idem_redis_errors_total`; `/debug/stats` section `idempotency`.

## Wire format on `events.raw` (S3-F7)

The HTTP body is unchanged (JSON). What the API writes to Kafka changed:

- **Value:** Avro, Confluent wire format (`0x00` + 4-byte schema id + Avro binary), schema `schemas/events/vN.avsc`, subject `events.raw-value`, compatibility `FULL_TRANSITIVE`. Header `content-type: application/vnd.eventpulse.event+avro`. `SERIALIZATION=json` restores the Stage 2 JSON value (`application/vnd.eventpulse.event+json`).
- **Precision:** `occurred_at` and `received_at` travel as epoch milliseconds; digits below the millisecond are not kept.
- **Payload:** carried exactly as sent, as JSON text.
- **Readiness:** `/readyz` includes `schema`: 503 until the API has found its writer schema id. After that a registry outage does not affect the API.

## What 202 means now

**Stage 1:** 202 meant "committed to Postgres".

**Stage 2:** 202 means **"appended to `events.raw` and acknowledged by Kafka under the configured acks policy"**. With the defaults (`KAFKA_ACKS=-1`, topic `min.insync.replicas=2`, replication factor 3) the record is on the partition leader **and at least one other in-sync replica** before the API answers. One broker can die without losing an acknowledged event.

If the server runs with weaker settings for an experiment, 202 promises less:

| `KAFKA_ACKS` | 202 means | Lost if |
| --- | --- | --- |
| `-1` (all, default) | on ≥ `min.insync.replicas` (2) replicas | 2+ replicas fail before the data is copied further |
| `1` | on the leader only | the leader dies before followers copied it |
| `0` | handed to the client library, no broker ack | anything goes wrong |

### What 202 no longer promises

- **The event is not necessarily queryable yet.** It reaches Postgres later, when the `sink-postgres` consumer group processes it (S2-F2). Visibility is **eventual**.
- Target: visible in Postgres within **5 seconds** of the 202 under normal load. This is tracked as end-to-end freshness (`stored_at - received_at`, metric `ep_event_freshness_seconds`) from S2-F2/S2-F6 on. During a database outage or slow database, ingestion keeps accepting events and the delay grows (consumer lag).
- **No ordering guarantee across tenants.** Events of one tenant go to one partition (key = `tenant_id`), so they keep their append order in the log; this is not yet carried through to queries.
- **Deduplication has two layers.** The API dedups within the window (above). Whatever gets past it is absorbed at the sink: the key is `(tenant_id, event_id)` (`ON CONFLICT (tenant_id, event_id) DO NOTHING`), so ids are scoped to your tenant, the first stored copy wins, and a resend with **different content** under the same id that reaches the sink (window expired, Redis lost) is ignored and counted as a conflict.

## Retry guidance for clients

- Retry on **503**, **409**, **500**, timeouts and network errors, with exponential backoff and jitter. Honor `Retry-After` when present.
- **Always reuse the same `event_id` and the same `Idempotency-Key`** when retrying. A failed or timed-out request may still have been written to Kafka; the same key lets the API return the original result, and the same `event_id` lets the sink keep exactly one row.
- Do not retry 400, 401, 413 or 422; they will fail the same way.

## Tenant config admin API (S3-F4, `services/admin-api`, port 8020)

Separate service and credential (`Authorization: Bearer <ADMIN_API_KEY>`); tenant keys are not accepted. Full
reference: [services/admin-api/README.md](../services/admin-api/README.md).

- `GET /v1/admin/tenants/:tenantId/config`: `{tenant_id, config, version, updated_at}` + `ETag: "<version>"`.
- `PUT /v1/admin/tenants/:tenantId/config` with `{config}` and optional `If-Match: "<version>"`: 200 with the new
  version and the `event_id` of the `config.changed` event; 412 when `If-Match` is stale; 400 on an unknown key or a
  bad value; 404 unknown tenant; 503 + `Retry-After` when Postgres is down (the change did not happen).
- **What 200 means:** the change and its `config.changed` event committed together. The event is published to Kafka
  at least once (usually within milliseconds; later if Kafka is down, never lost). A non-2xx means the change did not
  happen and nothing is published.

### `config.changed` (topic, compacted, key = tenant id)

Avro `config.changed-value` (`schemas/config/v1.avsc`): `event_id`, `tenant_id`, `version`, `changed_at`,
`config_json` (the WHOLE config). Headers `event_id`, `version`, `outbox_id`, `content-type`. Consumers MUST:

- dedup on `event_id`: a message can be delivered more than once (relay crash between publish and mark);
- apply only if `version` is greater than the one they hold (`applyIfNewer` in `packages/outbox`): with the default
  single relay messages of one tenant arrive in version order, but after a failover the last batch can be repeated.

