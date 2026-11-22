# audit (S2-F7)

One command that reconciles a load run across the whole pipeline: **requests sent (k6) vs acknowledged (202) vs the API's acked counter vs new Kafka records vs distinct rows in Postgres**, plus duplicates, DLQ, retention and lag.

```bash
make stage2-up                                   # whole pipeline, one command
make stage2-audit RATE=500 DURATION=30s          # load + audit (= scripts/audit.sh run)
scripts/audit.sh run -- <your command>           # audit around any command (gets RUN_ID, SUMMARY_PATH, API_KEY)
scripts/audit.sh start [RUN_ID] / end <RUN_ID>   # the two halves, e.g. to rerun "end" after a drain
make stage2-down
```

How it works:
- **Deltas, not resets.** `start` snapshots Kafka end/start offsets of `events.raw` and `events.dlq`, the committed offsets of `sink-postgres`, `count(*)` of `events`, and the `/debug/stats` of the API and every consumer (ports 8001–8003). `end` takes the same snapshot after waiting for the group to drain (`AUDIT_DRAIN_TIMEOUT_S`, 120 s). Runs can follow each other with no reset.
- **Run-scoped event ids.** `loadtests/k6/stage-2/ingest-kafka.js` makes every `event_id` start with the 8-hex run id, so the run's rows are counted exactly through the primary key (`event_id BETWEEN '<run>-0000-…' AND '<run>-ffff-…'`), and DLQ records of the run are recognized by their event id.
- **Read-only.** It never writes to Kafka or Postgres.

Checks (rules in `src/checks.ts`; unit tests in `src/checks.spec.ts`):

| | Rule | If violated |
| --- | --- | --- |
| A | sent ≥ 202s (k6) | harness bug, **critical** |
| B | 202s == Δ API `acked` counter (API not restarted) | lost responses / other traffic / counting bug, **critical** |
| C | Δ Kafka end offsets ≥ 202s | **acknowledged but not in the log**, critical |
| D | Δ Kafka end offsets − 202s | written but not acknowledged (failed/timed-out batches, retries, injected records): reported |
| E | distinct run rows == 202s − DLQ'd run events | below, once drained: **acknowledged but missing**, critical. Below while not drained: "not final". Above: 503 but stored, reported |
| F | per consumer process: consumed − inserted − dlq == duplicates | metrics bug |
| G | duplicates (`ON CONFLICT`) | reported, never hidden |
| H | log start offset moved | retention deleted data: reported |
| I | group lag at the end == 0 | NOT DRAINED |

Exit code: **0** reconciled, **1** a critical check failed, **2** not drained. Files: `loadtests/results/<RUN_ID>/` (`audit-start.json`, `audit-end.json`, `k6-summary.json`, `audit-result.json`, `audit-report.txt`).

Limits to keep in mind:
- B and D assume this run is the only traffic during the audit window.
- Consumer counters reset when a consumer restarts; the audit notices (uptime) and marks the sums best-effort. C and E (Kafka offsets and Postgres) decide loss, never the consumer counters.

Config (env): `AUDIT_TOPIC`, `AUDIT_DLQ_TOPIC`, `AUDIT_GROUP`, `AUDIT_DATABASE_URL` (default: `services/consumer-sink/.env`), `API_STATS_URL`, `CONSUMER_STATS_URLS`, `AUDIT_DRAIN_TIMEOUT_S`, `AUDIT_RESULTS_DIR`, `KAFKA_BROKERS`; for k6: `RATE`, `DURATION`, `BASE_URL`, `API_KEY` (default: first key in `services/ingest-api/.env`), `PAYLOAD_BYTES`, `RETRY_503_FRACTION`.
