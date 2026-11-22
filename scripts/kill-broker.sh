#!/usr/bin/env bash
# Kill a Kafka broker container (docker kill = abrupt, like a crash), e.g. the leader of a partition.
# Usage: scripts/kill-broker.sh --partition N [--topic events.raw]   (kills that partition's current leader)
#        scripts/kill-broker.sh --broker 1|2|3
# Bring it back with: docker start ep-kafka<N>
set -euo pipefail
TOPIC=events.raw PARTITION="" BROKER=""
while [ $# -gt 0 ]; do
  case "$1" in
    --partition) PARTITION="$2"; shift 2 ;;
    --topic) TOPIC="$2"; shift 2 ;;
    --broker) BROKER="$2"; shift 2 ;;
    -h|--help) sed -n '2,5p' "$0"; exit 0 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done
if [ -n "$PARTITION" ]; then
  for c in ep-kafka1 ep-kafka2 ep-kafka3; do
    out=$(docker exec "$c" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe --topic "$TOPIC" 2>/dev/null) && break
  done
  BROKER=$(printf '%s\n' "$out" | awk -v p="$PARTITION" '$3=="Partition:" && $4==p {for(i=1;i<=NF;i++) if($i=="Leader:") print $(i+1)}')
  [ -n "$BROKER" ] || { echo "partition $PARTITION of $TOPIC not found"; exit 1; }
  echo "leader of $TOPIC partition $PARTITION is broker $BROKER"
fi
[ -n "$BROKER" ] || { echo "give --partition N or --broker N"; exit 2; }
docker kill "ep-kafka$BROKER" >/dev/null && echo "killed ep-kafka$BROKER  (restart: docker start ep-kafka$BROKER)"
