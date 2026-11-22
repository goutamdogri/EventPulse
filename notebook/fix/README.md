# Stage 2 fixes

On 2026-11-22, after steps 1–4 of `docs/plans/stage-2-server-build-plan.md` were built, the code and environment were reviewed against the plan and the Stage 2 PRD. Every problem found is documented in the file of the step that introduced it: the problem, why it mattered, the fix, and how the fix was verified.

| File | Step | Problems |
| --- | --- | --- |
| [step-1.md](step-1.md) | S2-F1 producer / API | API e2e tests wrote into the real `events.raw`; no automated SIGTERM-flush test; `CLUSTER_ID` not in `.env`; leftover `perf` topic; **acknowledged records missing from a just-created topic** (investigated) |
| [step-2.md](step-2.md) | S2-F2 consumer sink | **e2e tests wrote ~353k rows into the dev database**; no automated test through the real API; "extra members sit idle" not shown; SIGTERM exit code 143 instead of 0 |
| [step-3.md](step-3.md) | S2-F3 commit protocol | a failed commit on a partition's last chunk left lag stuck; no explicit "committed = last + 1" test |
| [step-4.md](step-4.md) | S2-F4 poison / DLQ | `errors_by_class` counted bisection sub-writes; real "stop Postgres 30 s" test missing; "before" note not committed before the fix; deliberate differences (`SYSTEMIC_DB`, note path) |
| [step-5.md](step-5.md) | S2-F5 Kafka down | step 1 producer could not start without Kafka or reconnect; batcher had no send timeout, byte bound, breaker or drain deadline; filter mapped Kafka errors to 500; **review:** request during SIGTERM drain got 500; readiness only changed with traffic (fixed with a background health check); unbounded mode wasn't unbounded |
| [step-6.md](step-6.md) | S2-F6 metrics | consumer `consumed` counted per attempt (invariant impossible, inflated in retry loops); freshness would include duplicates; Prometheus stale bind mount; labelled counters "No data" |

Rules that came out of this review (now in the plan's global conventions):
- Tests never touch dev data: a separate `eventpulse_test` database (guarded) and throw-away Kafka topics, never `events.raw` or the `sink-postgres` group.
- A topic created by a test is only used after `waitTopicReady()` (leaders + full ISR + settle).
