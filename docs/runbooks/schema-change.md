# Runbook: changing the events.raw schema

*Stage 3, S3-F7. Subject `events.raw-value`, compatibility `FULL_TRANSITIVE` (ADR 0005).*

## The rule

Under `FULL_TRANSITIVE` a new version must be readable **in both directions against every earlier version**:

| Change | Allowed? |
| --- | --- |
| Add a field **with a default** (e.g. `["null","string"]`, `default: null`) | yes |
| Remove a field that **has a default** | yes |
| Add a field without a default | **no** (old data has no value for it) |
| Remove a field without a default | **no** (old readers need it) — `BACKWARD` alone would wrongly allow this |
| Rename a field (no alias) | **no** |
| Change a field's type (`int` -> `string`) | **no** (only Avro promotions like `int` -> `long` resolve, and only one way) |
| Edit an already published `vN.avsc` | **never** — add `vN+1.avsc` |

## Procedure

1. Copy the newest file: `cp schemas/events/v2.avsc schemas/events/v3.avsc`, make the change in `v3.avsc`.
2. Append it to `files` of `events.raw-value` in `schemas/subjects.json` (order = registration order).
3. Check locally (dev registry, throw-away subjects, nothing real touched):
   ```bash
   make schema-check
   make schema-check-against
   ```
   `schema-check` registers the whole history in order under the level; `-against` tests the new version against what the running registry has.
4. Open the PR. CI (`schema-check` job) repeats step 3 on an empty registry and runs the policy fixtures. A red job = incompatible: fix the schema, do not change the level.
5. Deploy order (FULL makes both orders safe, this is the conservative one):
   1. `make schemas` (registers v3; idempotent).
   2. Deploy consumers (sink): their reader schema becomes v3; they still read v1/v2 records.
   3. Deploy producers (API): they look up v3's id at startup and write it.
6. Verify: `ep_schema_ids_in_use` on the sink rises to include the new id; `ep_schema_deserialize_failures_total` stays flat; `/debug/stats` of the API shows `schema.writer_schema_id`.

## Rollback

- Producer: redeploy the previous build; it writes the previous version again. Consumers already read both.
- Never delete a registered version that records in the log may use (replays need it). Soft-deleted ids stay resolvable by id, but don't rely on it.

## When something is wrong

| Symptom | Cause | Action |
| --- | --- | --- |
| API `/readyz` 503 `schema: unavailable`, log "schema not registered" | the build's writer schema was never registered | `make schemas` |
| API `/readyz` 503, registry unreachable | registry down at API startup | fix the registry; the API retries by itself |
| Sink `chunk_failures_by_reason.registry_unavailable` rising, lag growing | registry down and records with a new writer id | fix the registry; nothing is dead-lettered, the partition resumes by itself |
| Sink DLQ records with `dlq.error_code=unknown_schema_id` | a producer used an id this registry does not know (wrong registry / deleted subject) | find the producer; replay the DLQ (`tools/dlq-replay`, Stage 3 step 6) after registering |
| `incompatible_schema` dead letters | a writer schema the sink's reader cannot resolve (registered with compatibility NONE?) | check `GET /config/events.raw-value` is FULL_TRANSITIVE |
