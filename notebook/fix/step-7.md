# Step 7 (S2-F7): problems found while building and verifying the audit

Date: 2026-11-22.

## 7.1 A not-drained run was reported as data loss (my bug in the first version of the audit)

**Problem.** In the first version, check E ("distinct rows == acknowledged - DLQ") was critical whenever rows were short, even when the consumer had not drained yet. Real run with the consumer stopped (500 acknowledged, 0 rows, lag 500, 3 s drain timeout): **NOT RECONCILED, exit 1, "ACKNOWLEDGED BUT MISSING FROM POSTGRES: 500"**. Nothing was lost; the rows were simply still in Kafka. The plan says the opposite: a drain timeout must be reported as "not drained", *not as a false mismatch*. The unit test I had written ("a critical failure wins over not drained") had encoded the wrong behavior for E.

**Fix.** `tools/audit/src/checks.ts`: when the group has not drained, rows short in E are a non-critical "not final: N acknowledged event(s) not stored YET" finding. The result is NOT DRAINED (exit 2), and `audit end <RUN_ID>` can be rerun later. Checks that don't depend on the consumer (C: acknowledged but not in the log; B; A) stay critical in every case. Unit tests: not drained + rows short → exit 2; C failure + not drained → still exit 1.

**Verified (real pipeline).** Consumer stopped: `RESULT: NOT DRAINED (exit 2)`, E "not final: 500". Consumer restarted, `scripts/audit.sh end 0d0d0d0d` on the same run: `RECONCILED (exit 0)`, 500 rows.

## 7.2 Observation (not a bug): a consumer crash stalls its partitions for up to the session timeout

In the crash run (consumer SIGKILLed after a DB commit, a new one started at once), group lag stayed at 14,471 for about 20 s and the run's freshness p50 was **30.5 s** (p99 44.7 s), against 8 ms / 79 ms in a clean run. The dead member keeps its partitions until the coordinator notices it is gone (`CONSUMER_SESSION_TIMEOUT_MS`, 45 s default); the new member waits for that rebalance. No data was lost (all 15,001 acknowledged events stored, 4 duplicates absorbed). This is input for K5 (kill a consumer) and K8 (rebalances, session timeout), and for the "eventual visibility within N seconds" target in `docs/api-contract.md`: a crash breaks a 5 s target unless the session timeout is lowered.

## Verification runs (all on the real pipeline, `make stage2-up`)

| Run | Result | Key numbers |
| --- | --- | --- |
| clean, 500 req/s × 30 s | RECONCILED, exit 0 | 14,999 sent = 202s = API acked = Kafka records = rows; 0 duplicates; freshness p50 8 ms / p99 79 ms |
| consumer crash (`CRASH_AT=after_db_commit`) mid-run | RECONCILED, exit 0 | 15,001 = rows (no loss); 4 duplicates reported; restart flagged, sums best-effort |
| poison record injected mid-run | RECONCILED, exit 0 | D: 1 written-not-acked (the poison); DLQ new 1 "not from this run"; consumer dlq 1 |
| two audits back to back, no reset | both RECONCILED, exit 0 | 6,001 and 6,000 rows |
| consumer stopped, 3 s drain timeout | NOT DRAINED, exit 2 | then `audit end` again after restart: RECONCILED |
