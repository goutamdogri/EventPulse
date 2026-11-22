# Step 6 (S2-F6): problems found in earlier steps while building the metrics

Date: 2026-11-22. Building S2-F6 exposed places where the earlier code or setup could not support the requirement as written.

## 6.1 The consumer's `consumed` counter couldn't satisfy the plan's invariant, and inflated in retry loops

**Problem.** Since S2-F2, `consumed` was added when a chunk *started*, while `attempted`/`inserted` were added after the DB write and `dlq` after the DLQ publish. Every failed attempt (DB down, poison with the DLQ off, DLQ publish failure) added `consumed` again without the rest. Two effects:
- The plan's S2-F6 invariant `consumed = inserted + duplicates + dlq` could never hold after any failure.
- In the S2-F4 "before" observation, `consumed` reached 787,810 for 11,874 real events (the hot retry loop counted every retry as consumption).

**Fix.** `src/observability/sink-stats.ts` + `src/consumer/batch-handler.ts`: consumed, attempted, inserted and dlq are added **together when a chunk completes** (rows committed in Postgres and dead letters acknowledged), via `completeChunk()`. A failed attempt counts in the new `chunk_failures_by_reason` / `ep_consumer_chunk_failures_total{reason}` (`poison_blocked`, `db_unavailable`, `dlq_publish`, `commit`, `other`) instead, so retry loops stay visible without inflating consumption. A failed attempt's partial writes show up as duplicates when its retry completes.

**Verified.** Unit test: 100 messages, 10 duplicates, 2 poison → consumed 100, inserted 88, duplicates 10, dlq 2, and `consumed == inserted + duplicates + dlq`. Failed attempt → consumed 0, `chunk_failures.db_unavailable = 1`. Live run: 7,918 consumed = 7,918 inserted + 0 + 0. The S2-F4 unit test "DLQ publish rejects" was updated to the new rule.

## 6.2 Freshness would have been wrong for reprocessed rows

**Problem.** The writer only returned `rowCount`. Computing freshness (`commit time - received_at`) for every row in a chunk would also have counted rows that `ON CONFLICT` skipped. After a crash, those reprocessed duplicates would report a freshness of minutes for events that had been visible all along.

**Fix.** The insert now ends with `RETURNING received_at`, which lists only rows actually inserted. Freshness is observed for those rows only (`ep_event_freshness_seconds`). As a side effect, write latency and pool-wait time are measured per transaction (`ep_consumer_db_write_seconds`, `ep_pg_pool_connect_wait_seconds`).

**Verified.** Unit test (`freshness _count` = 88 = inserted rows, not 98); e2e (`_count` 600 for 600 new rows).

## 6.3 Prometheus didn't pick up the new scrape job (stale single-file bind mount)

**Problem.** `infra/compose/prometheus-config.yml` is bind-mounted as a single file. After adding the `eventpulse-consumer-sink` job, `POST /-/reload` returned 200 but the job didn't appear. Inside the container, `/etc/prometheus/prometheus.yml` still had the old content: a single-file bind mount follows the file's original inode, and that file had been replaced (new inode) at some point, so the container was reading a stale copy.

**Fix.** `docker restart ep-prometheus` remounts the current file; the target then showed UP. **For the future:** after editing the Prometheus config, check `docker exec ep-prometheus grep <job> /etc/prometheus/prometheus.yml` before trusting a reload, or restart the container. (Mounting the directory instead of the file avoids this; that change was not made.)

## 6.4 Labelled counters showed "No data" on a clean run

**Problem.** prom-client only emits a labelled series after its first increment. On a clean run, the 503, rejection, DLQ, error, retry and chunk-failure panels showed "No data" (7 of 43 dashboard queries), which looks like a broken panel rather than "zero".

**Fix.** Known label values are created at 0 at startup (503 and rejection reasons, flush reasons, error classes, DLQ classes, chunk-failure reasons). Live check afterwards: 43 of 43 queries return data.
