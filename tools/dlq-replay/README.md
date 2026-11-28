# tools/dlq-replay (S3-F6)

DLQ triage and safe replay. How to use it: [docs/runbooks/dlq-triage-and-replay.md](../../docs/runbooks/dlq-triage-and-replay.md).

```bash
pnpm dlq-replay stats|list|show <id>|replay|reindex [filters] [--json]
make dlq-replay ARGS="replay --error-class PERMANENT_DB"          # dry run; add --execute to replay
```

| File | What |
| --- | --- |
| `src/cli.ts` | commands, argument parsing, wiring |
| `src/filters.ts` | filters to a parameterized `WHERE` (values are never concatenated into SQL) |
| `src/decide.ts` | per-row decision (replay / skip by loop guard or pre-check / missing) and the replay headers |
| `src/replay.ts` | plan, dry run, execute (produce, wait for the ack, then mark), bookkeeping in `dlq_replays` |
| `src/precheck.ts` | the sink's own `parseMessage` + Avro serde, so the pre-check matches what the sink does |
| `src/reindex.ts` | rebuild a `dlq_meta` row from a DLQ record's `dlq.*` headers |
| `src/store.ts` | Postgres queries (`dlq_meta`, `dlq_replays`) |
| `src/textfile.ts` | `ep_dlq_replay_*` gauges of the last executed run (node-exporter textfile collector) |

Exact-offset reads and the topic scan use `scanTopic` / `readRecordsAt` from `packages/kafka-config`. They rely on
manual `assign` and never commit offsets.

Tests:

- `pnpm test`: unit tests (filters/SQL, loop guard, headers, orchestration with fakes, reindex rows, textfile).
- `tests/correctness/test/dlq-replay.e2e-spec.ts`: real sink and CLI.
- The `replay:after-produce-before-mark` row of the crash matrix.
