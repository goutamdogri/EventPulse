# Fixes for Step 2 (S2-F2: consumer group `sink-postgres` writes batches to Postgres)

*Found in the review of 2026-11-22, fixed the same day. Each entry: the problem, why it mattered, the fix, how it was verified.*

---

## 2.1 The e2e tests wrote into the dev/benchmark database

**Problem.** `services/consumer-sink/.env.test` was created by copying `DATABASE_URL` from `ingest-api/.env.test`, which (since Stage 1) points at the **same** database as `.env`: `eventpulse`. Every consumer-sink e2e run wrote its test rows into the database used for benchmarks. At review time the `events` table had **863,079 rows, about 353,000 of them test data**:

| tenant_id | rows | written by |
| --- | --- | --- |
| `sink_e2e_proc` | 344,513 | crash / DB-down / poison process tests (5,000 events × many runs) |
| `sink_e2e` | 8,700 | `sink-consumer.e2e-spec.ts` |
| `writer_e2e` | a few hundred | `batch-writer.e2e-spec.ts` |

A second path made it worse: `sink-consumer.e2e-spec.ts` produced its records into the **real `events.raw` topic**, so the real `sink-postgres` group later stored them in the dev database a second time (and counted them as duplicates).

**Why it mattered.** Table and index size change insert cost (Stage 1's B11 measured exactly that). Stage 2 numbers (K1 knee vs Stage 1, K7 drain time) would have been measured on a table inflated by about 40% of junk, and every e2e run (about 25,000 rows) kept changing it.

**Fix.**
1. Created a separate database **`eventpulse_test`** on the same Postgres server and applied all migrations to it with a new script `pnpm migrate:test` (`dotenv -f .env.test`).
2. `services/consumer-sink/.env.test` now points at `eventpulse_test`. `.env.test.example` says it must be a separate database whose name contains `test`.
3. **Guard** in `test/setup-env.ts`: the e2e run refuses to start if the database name doesn't contain `test`, or if it's the same host and database as `.env`. This mistake now fails loudly instead of silently polluting data.
4. `sink-consumer.e2e-spec.ts` now creates its own throw-away topic (via `CONSUMER_TOPIC`) and deletes it afterwards. No consumer-sink test touches `events.raw` any more (the step 3 and 4 process tests already used throw-away topics). The API's e2e tests were fixed the same way, see [step-1.md](step-1.md) 1.1.
5. Side benefit: with a fresh topic the test starts the group from the beginning, so the old `sleep(1500)` (waiting for a "latest" start position to resolve, a race) is gone.

**Cleanup of existing rows.** Deleting the test rows already in `eventpulse.events` is destructive and was left for an explicit decision (see the end of this file).

**Verified.** `eventpulse_test` has migrations 0001–0004 and started empty. The full e2e suite runs green against it; a run against the dev database is refused by the guard.

---

## 2.2 No automated test through the real API

**Problem.** The plan's step 2 test table asks for *"E2E (API → Kafka → consumer → Postgres): POST 1 000 events, all rows within a bounded delay"*. The automated tests produced records directly with a Kafka producer. The real-API path was only checked once by hand with a scratch script.

**Fix.** New `test/pipeline.e2e-spec.ts` starts the **built ingest-api** (`../ingest-api/dist/main.js`) and the built consumer-sink as child processes, both on a throw-away topic, with the test database. It POSTs 1,000 events over HTTP and requires:
- all 1,000 answers are 202,
- all 1,000 rows are in Postgres **within 10 s of the first POST**,
- `stored_at >= received_at` for every row,
- `tenant_id` comes from the API key (`pipeline_e2e`), all the way through the envelope into the row,
- every offset ends up committed.

`pretest:e2e` of consumer-sink now builds ingest-api too.

**Verified.** Passes.

---

## 2.3 "Extra members sit idle" was not shown

**Problem.** The plan's "what to cover" says members beyond the partition count get no assignment. Only a 2-member split of 6 partitions was tested.

**Fix.** In `test/pipeline.e2e-spec.ts`: 3 members on a **2-partition** topic. The test requires assignments `0, 1, 1` (exactly one idle member), that delivery still works (500 rows stored), and that the idle member consumed **0** messages.

**Verified.** Passes.

---

## 2.4 SIGTERM exited with code 143 instead of 0

**Problem.** The plan says graceful shutdown ends with *"then exit 0"*. Nest's `enableShutdownHooks()` runs the hooks and then re-raises the signal, so the process ended with signal SIGTERM (exit status 143). Process supervisors and scripts can read that as an abnormal exit.

**Fix.** `services/consumer-sink/src/main.ts` no longer uses `enableShutdownHooks()`. It handles SIGTERM/SIGINT itself: `app.close()` runs the same hooks in the same order (leave the group while the current chunk finishes and commits, close the DLQ producer, close the pool), then `process.exit(0)`, or `exit(1)` if the shutdown itself fails.

**Verified.** The SIGTERM-under-load test in `test/commit-protocol.e2e-spec.ts` now requires exactly `{ code: 0, signal: null }` (it used to accept "code 0 or signal SIGTERM"), and still requires zero loss and zero duplicates.

---

## Cleanup of the test rows already in the dev database (done, approved)

On 2026-11-22, with explicit approval, the test rows were removed from `eventpulse.events` (nothing else was using the database at the time):

```sql
BEGIN; DELETE FROM events WHERE tenant_id IN ('sink_e2e_proc','sink_e2e','writer_e2e'); COMMIT;
VACUUM (FULL, ANALYZE) events;   -- rewrite the table so its size is real again, refresh planner stats
```

| | before | after |
| --- | --- | --- |
| rows | 863,079 | 508,648 |
| rows deleted | | 354,431 (`sink_e2e_proc` 344,513, `sink_e2e` 8,700, `writer_e2e` 1,218) |
| `pg_total_relation_size('events')` | 310 MB | 247 MB |

`VACUUM FULL` rather than plain `VACUUM` because plain vacuum only marks the space reusable: the table and index files would have stayed at 310 MB, and Stage 2 benchmarks compare against a table size. The `tenant_a` rows were kept: Stage 1 load tests and older API e2e runs share that tenant, so they can't be told apart.
