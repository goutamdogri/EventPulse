# Fixes for Step 4 (S2-F4: poison messages must not block a partition forever)

*Found in the review of 2026-11-22, fixed the same day. Each entry: the problem, why it mattered, the fix, how it was verified.*

---

## 4.1 `errors_by_class` counted failed sub-writes, not bad records

**Problem.** While isolating a bad row by bisection, every failed sub-write was counted: one bad row in a chunk of 8 failed the writes of 8, then 4, then 2, then 1 row, plus each of its 3 retries. So `errors_by_class.PERMANENT_DB` showed about 7 for **one** bad record. Someone reading `/debug/stats` (or the step 6 metric built on it) would believe several records were bad.

**Fix.** The counter now has one clear meaning per class (documented in `src/observability/sink-stats.ts`):
- `PERMANENT_PARSE`, `PERMANENT_DB`, `UNKNOWN`: **one per bad record**, counted once, when the record is dead-lettered (or when it blocks the partition with the DLQ off). Bisection steps are no longer counted. Retries stay visible in `retries_by_class`.
- `TRANSIENT_DB`, `SYSTEMIC_DB`: one per failed write attempt of a chunk. The DB was down; no record is at fault, so "attempts" is the meaningful unit.

**Verified.** Unit tests now assert it: the bisection test (1 bad row out of 8) expects `errors_by_class = { PERMANENT_DB: 1 }`, and the 3-poison test expects `{ PERMANENT_PARSE: 3 }`.

---

## 4.2 "Stop Postgres for 30 s" was replaced by a 20 s Toxiproxy cut

**Problem.** The plan's step 4 test table says: *"Stop Postgres for 30 s under load, restart: DLQ count unchanged (0 new), all rows stored after recovery"*. The test cut the network path through Toxiproxy for 20 s instead. That's similar but not the same failure: a real shutdown closes connections from the server side (`57P01 admin_shutdown`), then refuses new ones (`ECONNREFUSED`), then answers `57P03 cannot_connect_now` while starting up. A proxy cut only produces connection resets and timeouts.

**Fix.** Added the test the plan describes to `test/poison.e2e-spec.ts`: warm-up load (300 rows stored), then **`docker stop ep-postgres`**, 1,000 more events produced while it is down, wait **30 s**, `docker start ep-postgres` and wait for `pg_isready`. Requires:
- while it's down: `dlq = 0`, `errors_by_class.TRANSIENT_DB > 0`, `pauses > 0` (paused, not a hot loop);
- after the restart: all 1,000 rows stored, every offset committed, `dlq = 0`, DLQ topic empty.

`afterAll` always restarts the container, so a failing test can't leave Postgres stopped. The test-side pool ignores idle-client errors (otherwise stopping Postgres crashed the test process). The 20 s Toxiproxy test stays too, since it covers the network-cut variant.

Note: this test stops the shared `ep-postgres` container (dev and test databases live in it) for about 35 s. Don't run the e2e suite during a benchmark.

**Verified.** Passes; `ep-postgres` is healthy afterwards.

---

## 4.3 The "before" note was not committed before the fix

**Problem.** The plan's 4.1 says the observation note must be committed **before** the fix, so history proves the blocking behavior was seen on the unfixed code. Nothing from step 4 is committed yet. The note also gained an "after the fix" section.

**Fix.** Commit in this order:
1. `notebook/stage-2/s2-f4-poison-before-fix.md` (before section only), `scripts/produce-poison.sh`, `scripts/dlq-peek.sh`
2. the fix, the tests, and the note's "after the fix" section

This needs a commit decision, so it's left as the last step of this review.

---

## 4.4 Deliberate differences from the plan (kept, plan updated)

- **`SYSTEMIC_DB` error class added.** The plan's classifier had `PERMANENT_PARSE`, `PERMANENT_DB`, `TRANSIENT_DB` and `UNKNOWN`, with `UNKNOWN` treated like `PERMANENT_DB`. A schema mistake (`42703` undefined column), bad credentials (`28xxx`), a missing database (`3D000`) or an internal error (`XX`, `0A`) makes **every** row fail. Treated as `PERMANENT_DB`, each row would be isolated, retried and dead-lettered, emptying the whole topic into the DLQ. `SYSTEMIC_DB` is handled like `TRANSIENT_DB` instead (never DLQ, backoff and pause) until a human fixes it. Covered by unit test *"schema broken (42703 undefined column): never dead-lettered"*.
- **Before note location.** It lives at `notebook/stage-2/s2-f4-poison-before-fix.md` (your notebook's per-stage layout), not `notebook/stage-2-poison.md` as the plan says. The plan is updated to the real path.
- **A step 3 test assertion changed because of step 4.** The step 3 DB-unreachable test required `batch_errors > 0`. Step 4 retries inside the batch first (a 20 s window) instead of throwing, so it now checks `errors_by_class.TRANSIENT_DB > 0`. The claim the test exists for ("no offset moves and no row appears while the DB is down") is unchanged.
