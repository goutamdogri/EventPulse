#!/usr/bin/env bash
# S2-F7 one-command audit: requests sent vs acknowledged vs Kafka offsets vs distinct rows in Postgres.
#
#   scripts/audit.sh run                      start + k6 (RATE, DURATION env) + end   <- the one command
#   scripts/audit.sh run -- <command...>      start + your command + end (gets RUN_ID, SUMMARY_PATH, API_KEY)
#   scripts/audit.sh start [RUN_ID]           snapshot only (prints the run id)
#   scripts/audit.sh end <RUN_ID>             drain wait + snapshot + reconciliation
#
# Results: loadtests/results/<RUN_ID>/ (audit-start.json, audit-end.json, k6-summary.json, audit-result.json,
# audit-report.txt). Exit code: 0 reconciled, 1 critical check failed (possible loss), 2 not drained.
# Read-only: it never writes to Kafka or Postgres. Implementation: tools/audit (Node, no build step).
set -euo pipefail
cd "$(dirname "$0")/.."
exec node tools/audit/src/cli.ts "$@"
