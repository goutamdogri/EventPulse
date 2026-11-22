# Fixes for Step 3 (S2-F3: offsets committed only after the DB transaction commits)

*Found in the review of 2026-11-22, fixed the same day. Each entry: the problem, why it mattered, the fix, how it was verified.*

---

## 3.1 A failed offset commit on a partition's last chunk left lag stuck

**Problem.** When `commitOffsetsIfNecessary()` failed after a successful DB commit, the handler threw `OffsetCommitError` straight away. That's safe for data: the rows are in Postgres, and with this client the offset stays *stored* (resolved) in librdkafka and is committed by the next chunk's commit on that partition. But if that failing chunk was the **last** one the partition would see for a while, no later commit came:
- consumer lag stayed above 0 although every row was stored,
- a restart (or a rebalance) reprocessed the chunk, producing duplicates that `ON CONFLICT` absorbs,
- the S2-F7 audit (check I, "lag at end == 0") would report "not drained" for a pipeline that is fine.

Typical triggers are transient: `REBALANCE_IN_PROGRESS`, `COORDINATOR_LOAD_IN_PROGRESS`, a broker restart.

**Fix.** `commitWithRetry()` in `src/consumer/batch-handler.ts`: a failed commit is retried up to **3 more times** with the same exponential backoff as the DB retries (`SINK_RETRY_BASE_MS`, capped by `SINK_RETRY_MAX_MS`, with jitter). Retrying a commit is always safe because it only runs after the DB commit. It stops retrying at once if the consumer is shutting down or the partition was revoked (committing for a partition we no longer own can't succeed). Every failed attempt counts in `commit_errors`. If all 4 attempts fail, the old behavior applies (`OffsetCommitError`, duplicates possible, never loss), which is now much rarer.

**Verified.** New unit tests in `src/consumer/batch-handler.spec.ts`:
- fails on every attempt → `OffsetCommitError` after exactly 4 attempts, `commit_errors = 4`, rows stay stored;
- fails twice then succeeds → no error, backoff 100 ms then 200 ms, `commits = 1`, `commit_errors = 2`;
- partition revoked during the failure → no retry (1 attempt).

The consumer README's failure table was updated.

---

## 3.2 No explicit test that the committed offset = last processed offset + 1

**Problem.** The plan's step 3 test table asks for *"Committed offset value: last processed offset + 1"*, written as a test. It was only implied: tests checked "committed == log end offset" on 6-partition topics, which is the same thing but never stated or asserted directly.

**Fix.** New test in `test/commit-protocol.e2e-spec.ts`: produce **1,234** records to a **1-partition** topic (offsets 0..1233) and run the real consumer with chunks of 500. Requires all 1,234 rows stored and the group's committed offset to be **exactly 1234**. That is the last processed offset (1233) + 1 = the next offset to read: not 1233 (which would re-read the last record on every restart), and not beyond the log end.

**Verified.** Passes.

---

## 3.3 Finding raised during step 3 testing: an acknowledged record missing from the log

During the step 3 crash tests, one run found a topic holding 4,999 records although the producer had been told 5,000 were acknowledged. The consumer was correct (it stored and committed exactly what was in the log). This is about the **producer / ack guarantee**, so it's tracked, investigated and resolved in [step-1.md](step-1.md) 1.4.
