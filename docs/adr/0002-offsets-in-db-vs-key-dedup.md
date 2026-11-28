# ADR 0002: Sink idempotency by key (tenant_id, event_id), offsets stay in Kafka

- **Status:** Accepted
- **Date:** 2026-11-27
- **Author:** Goutam Dogri
- **Stage:** 3 (Correctness), requirement S3-F2
- **Affects:** `db/migrations/0005_events_tenant_pk.sql`, `services/consumer-sink/src/sink/batch-writer.ts`, `tests/correctness/`

## Context

The sink is at-least-once by design (Stage 2, S2-F3): offsets are committed to Kafka only after the Postgres transaction commits, so a crash between the two makes the next owner process the same chunk again. S3-F2 asks that replaying **any** range of the log leaves stored data unchanged.

There are two ways to get an exactly-once **effect** in Postgres:

1. **Key-based dedup.** Every row has a natural key and the insert ignores a key it already has.
2. **Offsets in the database.** Store the consumed offset per partition in the same transaction as the rows, and on assignment seek to the offset in Postgres instead of the one in Kafka. A chunk is then either fully applied with its offset, or not at all.

Duplicates do not only come from reprocessing the same offsets. They also come with **new offsets**: a client that retries after a timeout, an API whose idempotency window expired or whose Redis was flushed (steps 2, C4, C5), a DLQ replay (step 6), a producer retry across sessions. Those copies have offsets the sink has never seen.

## Options considered

| | Key-based dedup on `(tenant_id, event_id)` | Offsets stored in Postgres | Both |
| --- | --- | --- | --- |
| Reprocessing the same offsets (crash, rebalance, offset reset) | absorbed | absorbed (never re-read) | absorbed |
| Same event at a new offset (client retry, replay, expired window) | absorbed | **stored twice** | absorbed |
| Cost per chunk | unique index probe per row | one upsert per partition + custom seek on assignment | both |
| Works for every future sink (ClickHouse in Stage 4, caches) | each sink needs its own key handling | each sink needs its own offset store and seek logic | |
| Consumer code | unchanged commit protocol | rebalance callback must read Postgres and seek; offset commits to Kafka become cosmetic (lag tooling still reads them) | |

## Decision

- **Correctness comes from the key.** The primary key becomes `(tenant_id, event_id)` and the insert is `ON CONFLICT (tenant_id, event_id) DO NOTHING RETURNING tenant_id, event_id`. `DO NOTHING`, never `DO UPDATE`: the first committed copy wins, a duplicate changes nothing (not even `stored_at`), and a key repeated inside one statement is allowed.
- **Offsets stay in Kafka** for resuming. The commit protocol of S2-F3 is unchanged.
- **The data for the alternative is recorded anyway:** `sink_offsets (consumer_group, topic, partition, max_offset)` is upserted with `GREATEST` in the same transaction as the rows. Step 3 uses it to split duplicates into *reprocessed* (offset already stored once) and *redelivered* (new offset, existing key). If the offsets-in-database approach is ever wanted, the table and the transactional write are already there; only the seek on assignment is missing.
- **The tenant is part of the key** so tenants cannot collide on, probe or block each other's ids.
- **Same key, different content** (a producer reusing an id) is detected by comparing a `content_hash` (sha256 of the canonical event, `packages/idempotency`) with the stored row, only for chunks that have duplicates. It is counted as a conflicting duplicate and never overwrites the stored row.

## Reasons

1. Offsets in Postgres only stop duplicates that arrive at the **same offsets**. The Stage 3 failure ledger has as many duplicates arriving at **new offsets** (client retries, expired or lost API dedup, replays). The key is needed in any case; once it is there, the offset store adds no extra guarantee for this sink.
2. The key keeps the S2-F3 commit protocol, its tests and the lag tooling as they are.
3. Keys carry over to later sinks and to the API idempotency layer (same `(tenant, key)` scope, same canonical hash).

## Consequences

- No index on `event_id` alone any more. Queries that look up ids without a tenant (the Stage 2 audit's `event_id BETWEEN` run range, the Stage 2 experiment scripts, e2e helpers counting by id) still work but scan the table instead of using an index. They are tooling, not the hot path, so no index is added for them: a third index would be paid on every insert. Revisit if the audit gets slow on the dev table (tools can add `tenant_id = ANY(...)` to use the primary key).
- One extra statement per chunk (`sink_offsets` upsert) inside the transaction, and one extra query for chunks that contain duplicates (conflict check, after the commit; its failure never fails the write).
- Rows written before migration 0005 have no `content_hash`, so a conflicting duplicate of an old row is not detected (it is still not stored twice).
- The down migration fails if two tenants already share an `event_id`, on purpose.

## Measurements

Test database (`eventpulse_test`), Postgres 15 in Docker, chunks of 1,000 rows (~200-byte payload), 40 chunks per case after 5 warm-up chunks (`bench-writer.mjs`, run from the scratchpad, not committed):

| Case | p50 | p90 |
| --- | ---: | ---: |
| INSERT of 1,000 new rows, no `sink_offsets` | 19.7 to 20.5 ms | 23.0 ms |
| Same + `sink_offsets` upsert in the transaction | 20.4 to 20.7 ms | 23.7 to 28.2 ms |
| 1,000 duplicate rows (all skipped) + conflict check query | 18.0 ms | 19.6 ms |

- The `sink_offsets` upsert costs well under 1 ms per 1,000-row chunk at p50, within run-to-run noise.
- A chunk made only of duplicates, conflict check included, is slightly cheaper than inserting new rows: replays do not cost more than first writes.
- `events_pkey (tenant_id, event_id)` is 27 MB for 357k rows (heap 89 MB). That is about 76 bytes per row, versus about 40 bytes for a uuid-only key. The tenant text is the difference.
- The cost of the key against no key at all (the C14 "dedup off" arm) is measured in experiment C14, not here.

Crash matrix (generated, `docs/generated/crash-matrix.md`): SIGKILL at each of the six consumer points over 5,000 events: 0 lost and 0 stored twice at every point; only `consumer:after-db-commit` processes records again (exactly the stored-but-uncommitted chunk, absorbed by the key and counted). Log replay (`tests/correctness/test/replay.e2e-spec.ts`): 3,000 events consumed three times leave the stored fingerprint (every column, `stored_at` included) unchanged, with 6,000 duplicates counted.

## Rejected alternatives

- **Offsets in Postgres only:** see reason 1.
- **Both mechanisms in full (offset store + custom seek + key):** more consumer code for no extra guarantee on this sink. The table is kept as a measurement aid only.
- **`ON CONFLICT ... DO UPDATE` (last write wins):** makes stored data depend on arrival order, changes `stored_at` on every replay, and fails when a key repeats inside one statement.
- **Key on `event_id` alone (Stage 2):** tenants could collide on or probe each other's ids.
