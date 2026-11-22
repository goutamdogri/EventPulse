# K8 Experiment explanations

Status: PRE-RUN. Written 2026-11-22 before any measured run. The sections after the marker are appended after the experiment.

## Purpose

I want to know how much a consumer group stops reading when its membership changes or a member is slow, how many rows get processed twice because of it, and whether the cooperative assignor reduces that compared with the eager one. In K5 I saw that a killed consumer holds its partitions until the session timeout; here I look at the other triggers (scale out, scale in, a frozen member, a slow batch). The answer lets me decide the group settings (session timeout, heartbeat, rebalance timeout, batch size, assignor) for the sink and explains why those settings cannot be chosen one at a time.

## Hypothesis

Adding or removing consumers, or processing longer than the poll or session limits, triggers rebalances that pause consumption and cause duplicates. My idea of the mechanism: a rebalance revokes partitions, and a revoked member may have written rows whose offset it can no longer commit, so the next owner reads them again. With the eager assignor every member stops during every rebalance; with cooperativeSticky only the partitions that move are affected, so I expect shorter group-wide pauses but more rebalance rounds.

## Predictions (written before the run)

Setup numbers used below: 2000 events/s into 6 partitions, so about 333 events/s per partition; session timeout 6 s, heartbeat 1 s, rebalance timeout 8 s; chunks of at most 1000 rows, one partition at a time per consumer; a normal chunk finishes in well under 1 s.

1. **Nothing is lost, the API does not notice.** In all 24 runs `missing` = 0 (every 202 answer ends up in Postgres), because offsets are committed after the database write. Ingest p99 stays flat at the scenario time, because the API only waits for Kafka. Not predicted to change: acked count, API errors.
2. **s1 scale out (1 to 3 consumers), eager (roundRobin).** The existing member learns about the rebalance through its next heartbeat (up to 1 s), finishes its chunk (under 0.5 s), then the join and sync rounds take a few hundred ms. Longest read gap about 1 s + 0.5 s + 0.3 s, so 1.5 to 3 s (central 2 s); if the two joins land in separate rounds, up to 4 s. Partitions revoked: 6 per round, so 6 to 12. Cooperative: the old member keeps reading its partitions in the first round and only gives up 4 of its 6 partitions in total (6 becomes 2 + 2 + 2), so partitions revoked is about 4, and the longest group read gap stays under 1 s with `gaps_gt2s` = 0. Cooperative needs more rounds, so its assign count is higher than the eager one.
3. **s2 scale in (3 to 2 consumers), graceful SIGTERM.** The leaving member finishes and commits its chunk before it leaves, so the rows are not read again: `dup_rows` = 0 in both assignors. Eager: the 2 survivors each give up their 2 partitions and rejoin, so partitions revoked = 4 and a read gap of 1 to 2 s (no session timeout wait, the leave is announced). Cooperative: the survivors only gain partitions, partitions revoked = 0, read gap under 1 s.
4. **s3 zombie (SIGSTOP, then SIGCONT after 15 s).** The coordinator removes the frozen member when its last heartbeat is 6 s old, so 5 to 6 s after the freeze (last heartbeat was up to 1 s before the freeze); the group member count drops then. Until then the frozen member's 3 partitions are not read: 3 partitions x 333 events/s x about 6 s = about 6000 events of extra lag, which the survivor clears quickly afterwards (it can read far above 2000/s). The assignor cannot help in that window, because the partitions only move after the session expires. After the expiry the eager survivor pauses for 1 to 2 s (the read gap, since the survivor was still reading its own 3 partitions before), the cooperative one does not. When the frozen member wakes up it finishes its in-flight chunk and its commit is rejected; the survivor already re-read from the last committed offset. So `dup_rows` is the size of that chunk: between about 100 and 1000 rows in both assignors, and `commit_err` of at least 1. The wake-up makes the member rejoin, which causes a second rebalance: partitions revoked about 9 in total for eager (3 at the expiry, 6 at the rejoin) and about 3 for cooperative (0 at the expiry, 3 handed back at the rejoin). These revoke totals are rough.
5. **s4 slow chunk (12 s of extra work, limit 8 s).** I do not know whether this client counts the processing time against the rebalance timeout, so there are two outcomes. (a) It does (my guess, about 60%, because the config comment says so): about 8 s into the stall the member is removed, its 3 partitions move to the other member, and when the 12 s chunk ends its write succeeds and its commit fails, so `dup_rows` is about one chunk (100 to 1000 rows) per slow chunk, `commit_err` is at least 1, and the member rejoins in a second rebalance. In this outcome the read gap is again small for cooperative and 1 to 2 s for eager at the moment of the removal, and the group reads at about half rate (1000/s) for the first 8 s. (b) It does not: no rebalance at all, `revokes` = 0, `dup_rows` = 0, but the slow member's partitions are not read for 12 s, giving about 3 x 333 x 12 = 12000 events of lag per slow chunk. Either way the assignor does not change what the slow member does to itself.
6. **Eager vs cooperative overall.** Cooperative has a smaller longest read gap and fewer partitions revoked in s1 and s2, and the same order of magnitude of duplicates in all four scenarios (duplicates depend on the chunk in flight, not on the assignor). It does not help in s3 and s4 until the member is actually removed.
7. **Design check with numbers.** The heartbeat (1 s) is one sixth of the session timeout (6 s), which leaves room for 5 lost heartbeats. A normal chunk (well under 1 s) is more than 8 times smaller than the rebalance timeout (8 s), so only the artificial 12 s chunk can break it. At the default 45 s session timeout a frozen member would hold its partitions for up to 45 s, about 7 times longer than here.

## Setup (planned)

| Item | Value |
| --- | --- |
| Server | ingest-api `node dist/main.js`, pinned to CPUs 0,6, acks=all, idempotent, linger 0, key by event id |
| Consumers | consumer-sink `node dist/main.js`, 1 to 3 of them, pinned to CPUs 1, 7, 1; one new group per run; `SINK_DEDUP=false` (table `events_nodedup`) |
| Group timing | session 6000 ms, heartbeat 1000 ms, rebalance timeout 8000 ms in every cell; assignor roundRobin or cooperativeSticky |
| Topic | `k8-events`, 6 partitions, RF 3, min ISR 2, recreated before every run |
| Route / generator | POST /v1/events, k6 `constant-arrival-rate` 2000/s, pinned to CPUs 4,10, via `loadtests/k6/runner.sh` |
| Other pinning | Kafka brokers 2,8,3,9; Prometheus, Grafana, exporters and Postgres 5,11 |
| Matrix | 4 scenarios x 2 assignors x 3 repeats x 60 s of load = 24 runs, scenario acts at t = 20 s, about 52 minutes; no separate probe runs |
| Dashboard | `infra/observability/dashboards/stage-2/K8 - Rebalances and group stability.json` |
| Versions | filled in after the run from env.txt |

## Method (planned)

1. Per run: recreate the topic, start the API and the starting consumers, wait until the 6 partitions are owned, start the load, apply the scenario at t = 20 s, wait for the group lag to reach 0, audit Postgres (rows, distinct ids, acked), copy the consumer counters before stopping them, delete the run's rows.
2. Every run is measured; there are no probe runs because nothing heavy is running next to the load. The consumption gap comes from sampling the run's row count in Postgres every 0.3 s; the revoke and assign counters come from the consumers' own metrics.
3. Repeat 2 runs the cells in reverse order, so drift (thermal, caches) hits all cells the same way.
4. A run is discarded (decided now) if: k6 dropped iterations > 0; any answer other than 202; the ingest-api, k6 or the Postgres and monitoring cores stay above about 70% busy; the status in `summary.tsv` is not `ok` (drain timeout, k6 failed); the group did not settle before the load; or the scenario did not happen (for example the consumer did not exit on SIGTERM).
5. A cell is reported with its median and range over the 3 repeats. Duplicates and gaps vary from run to run, so I will not quote a single run.
6. If a cell loses a repeat, it is re-run on its own and flagged as not alternated.

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->
