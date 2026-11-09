Here is the end-state layout first, then how it grows stage by stage, then the conventions that keep it clean.

**Principles**
- Top level is organized by *concern* (code, infra, docs, tests), not by stage.
- One folder per deployable service, one per shared library.
- Docs, tests, and experiment results are first-class, because they're your evidence.
- Stages are marked with git tags and notebook entries, not folders.

## End-state layout

```
eventpulse/
├── README.md                    # pitch, architecture diagram, results summary, quickstart
├── LICENSE
├── Makefile                     # one-command entry points (up, test, bench, chaos, tf-apply)
├── pnpm-workspace.yaml
├── package.json                 # root scripts and dev tooling only
├── mise.toml                    # pinned Node, Go, Terraform, k6 versions
├── .editorconfig  .gitignore  .gitattributes
├── .github/
│   └── workflows/               # ci.yml, deploy.yml, nightly-bench.yml
│
├── services/                    # deployable things, one folder each
│   ├── ingest-api/              # NestJS (Stage 1+)
│   ├── ingest-go/               # Go comparison (Stage 8)
│   ├── consumer-sink/           # Kafka -> Postgres (Stage 2), retired in Stage 4
│   ├── sink-clickhouse/         # Stage 4
│   ├── processor/               # windowing, state, checkpoints (Stage 4)
│   ├── query-api/               # Stage 4
│   ├── rule-engine/             # Stage 5
│   ├── notifier/                # Stage 5
│   ├── shard-migrator/          # Stage 6
│   ├── partition-maint/         # Stage 6
│   └── realtime-gateway/        # Stage 9
│
├── apps/
│   └── dashboard/               # React + Vite (Stage 9)
│
├── packages/                        # shared code, imported by services
│   ├── kafka-config/            # Stage 2
│   ├── fault-injector/          # Stage 3
│   ├── idempotency/  outbox/  retry/     # Stage 3
│   ├── ratelimit/  cache/                # Stage 5
│   ├── hashring/  shard-router/          # Stage 6
│   └── telemetry/                        # Stage 7
│
├── schemas/                     # Avro/Protobuf definitions + compatibility tests (Stage 3)
├── db/
│   └── migrations/              # numbered SQL migrations (Stage 1+)
├── clickhouse/                  # DDL, benchmark queries, data generator (Stage 4)
│
├── infra/                       # everything that runs things
│   ├── compose/                 # docker-compose files (Stage 1+)
│   ├── helm/                    # charts (Stage 7)
│   ├── terraform/               # modules/ and envs/ (Stage 7)
│   └── observability/           # dashboards JSON, alert rules, otel config (Stage 7)
│
├── loadtests/                   # k6 and wrk2 scripts, scenarios (Stage 0+)
├── bench/                       # experiment harness, plotting, result archives (Stage 8)
├── chaos/                       # fault experiments as code + abort checks (Stage 7)
├── tests/                       # cross-service tests: e2e, correctness, ring, migration
├── tools/                       # dlq-replay, schema-check, webhook-sim, demo scenarios
├── scripts/                     # audit.sh, kill-broker.sh, lag.sh and similar glue
├── labs/
│   └── stage-0-foundations/     # permanent measurement lab
│
├── notebook/                    # one markdown file per experiment
│   └── _template.md
└── docs/
    ├── architecture.md
    ├── delivery-guarantees.md
    ├── slo.md
    ├── adr/                     # 0001-use-kafka.md, 0002-... (numbered)
    ├── benchmarks/              # stage-0.md ... stage-8/
    ├── runbooks/
    ├── postmortems/
    ├── chaos-report.md
    └── cost-model/
```

## How it grows

| Stage | What appears |
|---|---|
| 0 | Root files (README, Makefile, pnpm workspace, mise.toml), `labs/stage-0-foundations/`, `loadtests/` (k6 + wrk2 + results), `notebook/` + template, `docs/benchmarks/stage-0.md`, `docs/adr/` (empty, start with ADR 0001 about the monorepo tool) |
| 1 | `services/ingest-api/` (NestJS), `db/migrations/`, `infra/compose/` (Postgres), `docs/benchmarks/stage-1-baseline.md` |
| 2 | `services/consumer-sink/`, `packages/kafka-config/`, Kafka in `infra/compose/`, `scripts/` (audit, kill, lag), `docs/failure-ledger-stage-2.md` |
| 3 | `packages/idempotency`, `outbox`, `retry`, `fault-injector`; `schemas/`; `tools/dlq-replay`, `tools/schema-check`; `tests/correctness/`; `docs/delivery-guarantees.md` |
| 4 | `services/processor`, `sink-clickhouse`, `query-api`; `clickhouse/`; `tests/stream/`; `consumer-sink` is retired (moved to `docs/` history or deleted) |
| 5 | `services/rule-engine`, `notifier`; `packages/ratelimit`, `cache`; `tools/webhook-sim`; `tests/isolation/` |
| 6 | `packages/hashring`, `shard-router`; `services/shard-migrator`, `partition-maint`; `tests/ring`, `tests/migration` |
| 7 | `infra/helm`, `infra/terraform`, `infra/observability`; `chaos/`; `.github/workflows/`; `packages/telemetry`; `docs/runbooks`, `postmortems`, `slo.md` |
| 8 | `services/ingest-go/`, `bench/`, `docs/benchmarks/stage-8/`, `docs/cost-model/` |
| 9 | `services/realtime-gateway/`, `apps/dashboard/`, `tests/e2e/`, `tools/demo/` |
| 10 | Depth project in its **own repo** (linked from README); in this repo only `docs/adr/depth-project.md` and the final write-ups |

One reconciliation: the stage guides sometimes say `deploy/` for Helm and Terraform. Use the single `infra/` folder from this layout instead and read `deploy/...` as `infra/...`. Consistency matters more than the name.

## Conventions to set on day one

- **Package scope:** name TypeScript packages `@eventpulse/ingest-api`, `@eventpulse/hashring`, and so on. Internal imports then look the same everywhere.
- **Each service is self-contained:** its own `src/`, `test/`, `Dockerfile`, `README.md`, and `package.json` (or `go.mod`). A service may import from `packages/`, never from another service.
- **Dependency direction:** `services` and `apps` depend on `packages`; `packages` never depend on services. Enforce this with a lint rule later.
- **Per-service README template:** what it does, how to run, config env vars, ports, health endpoints, metrics emitted.
- **Config:** environment variables only, with a committed `.env.example` per service and real secrets never in git.
- **Branching:** trunk-based. Short-lived branches (`stage-3/outbox`), merged into `main`, with a tag `stage-N-complete` at each exit.
- **Commits:** Conventional Commits (`feat(ingest-api): ...`, `fix(processor): ...`). It makes the history readable and enables changelogs.
- **ADR numbering:** `docs/adr/0007-clickhouse-ordering-key.md`, with the same template every time (context, options, decision, consequences, measurement).
- **Notebook naming:** `notebook/stage-2-K4-kill-broker.md`, matching the experiment IDs in the stage guides, so reports can link to them.
- **Results data:** keep raw benchmark output under `bench/results/` or `loadtests/results/`, but gitignore large files and commit a small manifest plus summaries. If you want raw data public, use Git LFS or release assets.
- **Per-path CI:** run only the checks for changed paths (the monorepo tool or path filters in GitHub Actions handle this).

## Mistakes to avoid

- Creating the whole tree on day one with empty placeholder folders. Add folders when the stage that needs them starts, so the commit history shows the project growing.
- Putting shared code in a `common/` or `utils/` dump. Make each lib small and single-purpose.
- Scattering Dockerfiles and compose files at random depths; keep compose in `infra/compose/` and Dockerfiles beside their service.
- Mixing experiment code into production services. Anything throwaway belongs in `labs/` or `bench/`.

I can write a small scaffold script that creates exactly the Stage 0 subset (root files, `labs/`, `loadtests/`, `notebook/`, docs skeleton), so you start with a clean repo.