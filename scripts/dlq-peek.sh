#!/usr/bin/env bash
# Print the last N records of the dead-letter topic with partition, offset, key, headers and value.
#
# Usage: scripts/dlq-peek.sh [N] [--topic TOPIC]      (default N=10, topic events.dlq)
# Reads the whole DLQ from the beginning (it is small by design) and keeps the last N lines.
set -euo pipefail

N=10 TOPIC="events.dlq" CONTAINER="${KAFKA_CONTAINER:-ep-kafka1}"
while [ $# -gt 0 ]; do
  case "$1" in
    --topic) TOPIC="$2"; shift 2 ;;
    -h|--help) sed -n '2,5p' "$0"; exit 0 ;;
    *) N="$1"; shift ;;
  esac
done

docker exec "$CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka1:19092 --topic "$TOPIC" --from-beginning --timeout-ms 5000 \
  --formatter-property print.timestamp=true --formatter-property print.partition=true \
  --formatter-property print.offset=true --formatter-property print.key=true \
  --formatter-property print.headers=true --formatter-property headers.separator=' | ' \
  2>/dev/null | tail -n "$N"
