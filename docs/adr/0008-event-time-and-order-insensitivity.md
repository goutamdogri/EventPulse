# ADR 0008: Event time rules, skew bounds at the API only, and an order-insensitive sink

- **Status:** Accepted
- **Date:** 2026-11-28
- **Author:** Goutam Dogri
- **Stage:** 3 (Correctness), requirement S3-F8 (guarantee G5)
- **Affects:** `services/ingest-api/src/events/event-time.ts`, `events.service.ts`, `create-event.dto.ts`, `services/consumer-sink/src/sink/batch-writer.ts`, `docs/api-contract.md`

## Context

Events arrive late and out of order. Mobile clients buffer offline, retry tiers (S3-F5) store a record minutes after newer ones, and DLQ replays (S3-F6) store it days later. The requirement: such events are stored correctly, and both `occurred_at` and `received_at` are kept.

Before this change there were four gaps:

- The API accepted any ISO 8601 string: a time without a zone, a date only, microseconds.
- On the Avro path, sub-millisecond digits were silently dropped.
- A timestamp without a zone is read by JavaScript as the API host's local time.
- There was no bound against broken clocks (year 1970, 2099).

## Decisions

### 1. Time fields

| Field | Source | Rule |
| --- | --- | --- |
| `occurred_at` | client | RFC 3339 with a mandatory zone (else 400). Normalized to UTC. **Milliseconds, extra digits truncated** (option (b) of the plan: accept and truncate, documented). |
| `received_at` | API clock | Set when the request is handled. Travels in the Avro value and a header, so retry tiers and replays keep it. |
| `stored_at` | sink | Commit time. |

- **Why truncate rather than reject (a) or round:**
  - Rejecting more than 3 fractional digits would break clients whose libraries print microseconds by default (Python, Java `Instant`).
  - Rounding could move an event into the next second, day or year (`23:59:59.9999` would become `00:00:00.000` of the next day).
  - Truncation is deterministic and documented, and the tests assert the exact stored value.
- **Why a mandatory zone:** without one, the same request means a different instant on hosts in different time zones.
- **The normalized value is hashed (idempotency) and stored.** Two spellings of the same instant (`Z` and `+02:00`) are the same event content.

### 2. Clock-skew bounds: at the API only, against `received_at`

| Condition | Response |
| --- | --- |
| `occurred_at - received_at > OCCURRED_AT_MAX_FUTURE_S` (300 s) | 422 `occurred_at_in_future` |
| `received_at - occurred_at > OCCURRED_AT_MAX_PAST_DAYS` (365 days) | 422 `occurred_at_too_old` |

- **Inclusive limits:** exactly 300 s ahead and exactly 365 days old are accepted. The unit tests check both sides of each limit to the millisecond.
- The check runs before idempotency, so a rejected event consumes no key and can be corrected under the same `event_id`.
- **The past bound is generous** because G5 says late events are stored regardless of lateness. It only stops broken clocks.
- **The sink does not check the bounds.** A record replayed a year after its request, or one that waited on a retry tier, must still be stored. Comparing against `now()` in the sink would make correct data poison over time.
- **Metrics:**
  - `ep_event_lateness_seconds`: histogram at the API, 1 s to 30 days.
  - `ep_event_rejected_total{reason=future|too_old}`.
  - `ep_event_ahead_total`: accepted events ahead of the server clock.
  - `ep_sink_lateness_seconds`: the same lateness, for rows actually stored, at the sink.

### 3. Migration 0009: not needed (skipped)

- **`received_at` is already `NOT NULL`** (`0002_create_events.sql`), and the sink always writes it. The column default `now()` is never used by the sink. A check for rows where the default might have been used (`received_at` within a millisecond of `stored_at`, meaning no header):

  ```sql
  SELECT count(*) FROM events WHERE received_at IS NULL;                       -- 0 by the constraint
  SELECT count(*) FROM events WHERE abs(extract(epoch FROM stored_at - received_at)) < 0.001;  -- expected ~0
  ```

- **Indexes:**
  - Keep `events_tenant_time_idx (tenant_id, occurred_at)` for event-time queries.
  - **No index on `received_at`.** No query uses it today. Lateness and freshness are metrics, not queries, and every extra index costs write throughput on the hottest table (Stage 1 B-experiments).
  - Add one only when a query by ingestion time appears (for example "what arrived in the last hour", for reprocessing).

### 4. The sink is order-insensitive (review of every write path)

| Write | Order-sensitive? | Why |
| --- | --- | --- |
| `INSERT ... ON CONFLICT (tenant_id, event_id) DO NOTHING` (step 1) | no | Set semantics: the stored set is the union of what arrived, whatever the order. |
| `sink_offsets` upsert | no | `GREATEST(max_offset)`: a bisected or reprocessed smaller write never moves it back. |
| Retry tiers (step 5) store late | no | Same INSERT. A retried event stored after newer ones lands in the same set. |
| DLQ replay (step 6) | no | Same INSERT. A replay of a stored event is a counted duplicate. |
| `dlq_meta` insert | no | `ON CONFLICT (source_topic, source_partition, source_offset) DO NOTHING`. |
| Counters / "latest event" columns | none exist | `tenant_counters` belongs to a Stage 1 ingest experiment (B4), not the sink. **Rule:** any future "latest" column must use `GREATEST(occurred_at)`-style updates, never last write wins. |

**The one documented exception:** a conflicting duplicate (same key, different content) is first committed wins. It only happens for a client or producer bug (a reused `event_id`), and it is handled at three levels:

1. The API rejects it within the dedup window (422 `idempotency_key_reused`, step 2).
2. Past the window, the sink counts it (`ep_sink_conflicting_duplicates_total`, step 1/3) and never overwrites the stored copy.
3. The property test models it explicitly.

## Proof

- **Unit tests** (`services/ingest-api/src/events/event-time.spec.ts`):
  - Zone offsets normalized to UTC, including across a day boundary.
  - Truncation, never rounding: `.999999999Z` and `23:59:59.9999Z` at year end.
  - Rejections: no zone, date only, February 30, hour 24, an invalid offset.
  - Skew limits at exactly 300 s / 365 days and 1 ms beyond.
- **Property test, BatchWriter** (`tests/correctness/test/order-property.e2e-spec.ts`), **1,000 runs in about 8 s**; `PROPERTY_RUNS=10000` for a nightly run. Each run has:
  - up to 30 events over up to 3 tenants, with lateness 0 s to 24 h;
  - up to 20 duplicates, exact and conflicting;
  - shuffled delivery, split into random chunks;
  - fed to the real `BatchWriter` against the test database.

  It passes when, compared with a first-delivered-wins reference model, all of the following match:
  - the stored set;
  - the content (`occurred_at` and `received_at` to the ms);
  - per-tenant counts and min/max `occurred_at`;
  - the counted duplicates and the reported conflicts.
- **Property test through the pipeline:** 20 runs through API, Kafka and sink. Each run sends shuffled, late events written in different zone offsets, with resends, concurrently. Stored equals sent, and `received_at` falls within the request's server-time window.
- **Scenario e2e** (`tests/correctness/test/event-time.e2e-spec.ts`):
  - 300 events, shuffled, 10% 1 min / 1 h / 24 h late, 30 of them sent seconds later: the table equals the dataset exactly.
  - A record that waited on a retry tier while 50 newer events were stored first: stored after them, with the right `occurred_at` and its original `received_at`.
  - A DLQ replay of a half-year-old event: `received_at` is the original request's, earlier than the replay.
  - 366 days old and 10 min ahead are rejected with the right reasons, and nothing is produced. 364 days old and 4 min ahead are accepted. A rejected event's id is still usable.
  - The sink lateness histogram.
