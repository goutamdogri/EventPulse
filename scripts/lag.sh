#!/usr/bin/env bash
# Per-partition lag and owner of a consumer group, refreshed every N seconds (watch-style).
# Usage: scripts/lag.sh [--once] [--interval S] [--group sink-postgres]
set -euo pipefail
ONCE=0 INTERVAL=2 GROUP=sink-postgres CONTAINER="${KAFKA_CONTAINER:-ep-kafka1}"
while [ $# -gt 0 ]; do
  case "$1" in
    --once) ONCE=1; shift ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    --group) GROUP="$2"; shift 2 ;;
    -h|--help) sed -n '2,3p' "$0"; exit 0 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done
show() {
  docker exec "$CONTAINER" /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka1:19092 --describe --group "$GROUP" 2>/dev/null \
    | awk 'NR==1 || /^$/ {next} {printf "%-12s %-10s %-15s %-15s %-8s %s\n", $2, $3, $4, $5, $6, $7}'
  docker exec "$CONTAINER" /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka1:19092 --describe --group "$GROUP" 2>/dev/null \
    | awk 'NR>2 && $6 ~ /^[0-9]+$/ {s+=$6} END {print "total lag: " s+0}'
}
if [ "$ONCE" = 1 ]; then show; exit 0; fi
while true; do clear; date '+%T'; echo "group $GROUP"; show; sleep "$INTERVAL"; done
