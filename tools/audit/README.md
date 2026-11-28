# audit (S2-F7, extended for S3-F3)

One command that reconciles a load run across the whole pipeline: **requests sent (k6) vs acknowledged (202) vs the API's acked counter vs new Kafka records vs distinct rows in Postgres**, plus duplicates, DLQ, retention and lag.

```bash
make up                                         # whole pipeline, one command
make audit RATE=500 DURATION=30s          # load + audit (= scripts/audit.sh run)
make audit RATE=500 RESEND=0.1            # Stage 3: resend 10% of accepted events with the same Idempotency-Key
scripts/audit.sh run -- <your command>           # audit around any command (gets RUN_ID, SUMMARY_PATH, API_KEY)
scripts/audit.sh start [RUN_ID] / end <RUN_ID>   # the two halves, e.g. to rerun "end" after a drain
make down
```

How it works:
- **Deltas, not resets.** `start` snapshots Kafka end/start offsets of `events.raw` and `events.dlq`, the committed offsets of `sink-postgres`, `count(*)` of `events`, and the `/debug/stats` of the API and every consumer (ports 8001–8003). `end` takes the same snapshot after waiting for the group to drain (`AUDIT_DRAIN_TIMEOUT_S`, 120 s). Runs can follow each other with no reset.
- **Run-scoped event ids.** `loadtests/k6/stage-2/ingest-kafka.js` makes every `event_id` start with the 8-hex run id, so the run's rows are counted exactly through the primary key (`event_id BETWEEN '<run>-0000-…' AND '<run>-ffff-…'`), and DLQ records of the run are recognized by their event id.
- **Read-only.** It never writes to Kafka or Postgres.

Checks (rules in `src/checks.ts`; unit tests in `src/checks.spec.ts`):

| | Rule | If violated |
| --- | --- | --- |
**Accepted** = distinct events that got a 202 (k6 `unique_202`). A resend with the same key gets the original result (replayed 202) and is not a second event. Stage 2 k6 summaries fall back to the 202 count.

| | Rule | If violated |
| --- | --- | --- |
| A | sent ≥ 202s (k6) | harness bug, **critical** |
| B | 202s == Δ API `acked` counter + Δ replayed 202s (API not restarted) | lost responses / other traffic / counting bug, **critical** |
| C | Δ Kafka end offsets ≥ accepted | **acknowledged but not in the log**, critical |
| D | Δ Kafka end offsets − accepted | extra copies in the log (resends past API dedup, failed/timed-out batches, injected records): reported |
| E | distinct run rows == accepted − DLQ'd run events | below, once drained: **acknowledged but missing**, critical. Below while not drained: "not final". Above: 503 but stored, reported |
| F | per consumer process: consumed − inserted − dlq == duplicates, and reprocessed + redelivered + unknown == duplicates | metrics bug |
| G | duplicates absorbed by the sink, by cause (reprocessed / redelivered / unknown) and conflicting content | reported, never hidden |
| H | log start offset moved | retention deleted data: reported |
| I | group lag at the end == 0 | NOT DRAINED |
| J | **unexplained difference** = Δ committed offsets − new rows − new DLQ records − redelivered − unknown == 0 | with complete counters and drained: **critical** (a record vanished or a counter lies). Restarted consumer: best-effort finding |
| K | API idempotency: new, replayed, 409, 422, fail_open, fail_closed | reported; any fail_open / fail_closed is a finding (Redis degraded) |

Exit code: **0** reconciled, **1** a critical check failed, **2** not drained. Files: `loadtests/results/<RUN_ID>/` (`audit-start.json`, `audit-end.json`, `k6-summary.json`, `audit-result.json`, `audit-report.txt`).

Limits to keep in mind:
- B, D and J assume this run is the only traffic during the audit window.
- J counts DLQ **records**: a dead letter published twice after a crash shows up as −1 until Stage 3 step 5 dedups dead letters (`dlq_meta`).
- Consumer counters reset when a consumer restarts; the audit notices (uptime) and marks the sums best-effort. C and E (Kafka offsets and Postgres) decide loss, never the consumer counters.

Config (env): `AUDIT_TOPIC`, `AUDIT_DLQ_TOPIC`, `AUDIT_GROUP`, `AUDIT_DATABASE_URL` (default: `services/consumer-sink/.env`), `API_STATS_URL`, `CONSUMER_STATS_URLS`, `AUDIT_DRAIN_TIMEOUT_S`, `AUDIT_RESULTS_DIR`, `KAFKA_BROKERS`; for k6: `RATE`, `DURATION`, `BASE_URL`, `API_KEY` (default: first key in `services/ingest-api/.env`), `PAYLOAD_BYTES`, `RETRY_503_FRACTION`, `IDEM_HEADER` (default true), `RESEND_FRACTION`.
