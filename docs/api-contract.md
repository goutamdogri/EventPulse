# EventPulse Ingest API Contract

*Current as of Stage 2 (S2-F1). Each stage that changes a guarantee updates this file.*

## `POST /v1/events`

### Request

- Header `Authorization: Bearer <api key>`. The tenant comes from the key, never from the body.
- Optional header `x-request-id`: up to 128 characters from `A-Z a-z 0-9 . _ : -`. It is copied to the Kafka record for tracing. Missing or invalid values are replaced by a generated UUID.
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
| **202** `{ "event_id": "..." }` | The event is **durably appended to the replicated log** (`events.raw`) under the configured acks policy. See below. |
| 400 | Body failed validation or is not valid JSON. Nothing was written. |
| 401 | Missing or unknown API key. Nothing was written. |
| 413 | Body larger than `BODY_LIMIT` (rejected at the edge, Kafka never sees it), or an event larger than the batch byte cap. |
| 500 | Unexpected error (a bug). The event **may or may not** have been written. |
| 503 + `Retry-After` | Kafka unavailable, or the ingest queue is full. Body: `{ statusCode: 503, error, message, reason }` with `reason` = `kafka_unavailable`, `overloaded` or `shutting_down`. The event **may or may not** have been written (a timed-out or failed batch can be partly in the log). |

### 503 in detail (S2-F5)

- `reason: "kafka_unavailable"`: Kafka cannot take the write now (all brokers down, fewer in-sync replicas than `min.insync.replicas`, send timeout, circuit breaker open). After `KAFKA_BREAKER_FAILURES` failed sends the API answers immediately instead of waiting for the send timeout, and probes Kafka again after `KAFKA_BREAKER_COOLDOWN_MS`. The API needs no restart when Kafka returns.
- `reason: "overloaded"`: the in-memory ingest queue is full (`BATCH_MAX_QUEUE` messages or `BATCH_MAX_QUEUE_BYTES`) while Kafka may be healthy.
- `reason: "shutting_down"`: this instance received SIGTERM and is draining its queue; retry (another instance, or this one after its restart).
- `Retry-After: RETRY_AFTER_S` seconds (default 1).
- A request never waits longer than the queue wait plus `KAFKA_SEND_TIMEOUT_MS` (default 5 s).
- Readiness `GET /readyz` is 200 only when the producer is connected, a background Kafka health check (every `KAFKA_HEALTH_INTERVAL_MS`, default 2 s: every partition has a leader and enough in-sync replicas for `acks=all`) passed recently, and the breaker is closed. It follows Kafka in both directions without needing any traffic. Liveness `GET /healthz` stays 200.

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
- **No deduplication at ingest.** The same `event_id` sent twice is appended twice. Duplicates are absorbed at the sink (`ON CONFLICT (event_id) DO NOTHING`).

## Retry guidance for clients

- Retry on **503**, **500**, timeouts and network errors, with exponential backoff and jitter. Honor `Retry-After` when present.
- **Always reuse the same `event_id`** when retrying. A failed or timed-out request may still have been written to Kafka; the same `event_id` lets the sink keep exactly one row.
- Do not retry 400, 401 or 413; they will fail the same way.
