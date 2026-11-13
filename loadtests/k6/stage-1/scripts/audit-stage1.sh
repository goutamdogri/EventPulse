#!/usr/bin/env bash
# usage: audit-stage1.sh path/to/k6-summary.json
set -Eeuo pipefail

SUMMARY="${1:?k6 summary json}"
SENT=$(jq '(.metrics.ep_accepted.count // .metrics.ep_accepted.values.count // 0)' "$SUMMARY")

ROWS=$(docker exec ep-postgres psql -U eventpulse -d eventpulse -tAc "select count(*) from events")
echo "202 responses: $SENT | rows in table: $ROWS | diff: $((SENT - ROWS))"

[[ "$SENT" -eq "$ROWS" ]] && echo "AUDIT OK" || { echo "AUDIT MISMATCH (a finding, not noise)"; exit 1; }
