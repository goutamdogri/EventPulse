#!/usr/bin/env bash
# kafka-disk-exporter.sh - how many bytes one topic occupies in each broker's log directory, for Prometheus
#
# Why: K7 asks "disk usage on brokers" while the consumer is far behind. kafka_exporter only knows offsets
# (message counts), not bytes, and node_exporter sees the whole disk, not one topic. A broker keeps every
# partition as a directory <topic>-<partition> inside its log dir, so I ask the broker container for the size
# of those directories with du and write the result in the node_exporter textfile format, the same way
# listen-queue-exporter.sh and pg-container-exporter.sh do.
#
# Usage:  KAFKA_TOPIC=k7-events ./infra/scripts/kafka-disk-exporter.sh &        (stop with kill / Ctrl+C)
# Env:    KAFKA_TOPIC    topic whose partition directories are summed (required)
#         BROKERS        container names, space separated (default: ep-kafka1 ep-kafka2 ep-kafka3)
#         LOG_DIR        log dir inside the container (default /var/lib/kafka/data, KAFKA_LOG_DIRS in the compose file)
#         INTERVAL       seconds between samples (default 5; du walks the segment files, so not faster)
#         TEXTFILE_DIR   default <repo>/infra/compose/textfile
# Output: <textfile dir>/ep_kafka_disk.prom
#         ep_kafka_topic_disk_bytes{broker,topic}   bytes of all <topic>-* directories of that broker (replicas included)
#         ep_kafka_logdir_disk_bytes{broker}        bytes of the whole log dir (other topics and metadata included)
# The file is removed on exit so Grafana shows a gap instead of a frozen value.
set -u
KAFKA_TOPIC="${KAFKA_TOPIC:?set KAFKA_TOPIC to the topic name}"
BROKERS="${BROKERS:-ep-kafka1 ep-kafka2 ep-kafka3}"
LOG_DIR="${LOG_DIR:-/var/lib/kafka/data}"
INTERVAL="${INTERVAL:-5}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEXTFILE_DIR="${TEXTFILE_DIR:-$HERE/../compose/textfile}"
OUT="$TEXTFILE_DIR/ep_kafka_disk.prom"; TMP="$OUT.tmp"

[ -d "$TEXTFILE_DIR" ] || { echo "textfile directory not found: $TEXTFILE_DIR" >&2; exit 1; }
command -v docker >/dev/null || { echo "docker not found" >&2; exit 1; }
trap 'rm -f "$OUT" "$TMP"' EXIT
trap 'exit 0' INT TERM

echo "kafka-disk-exporter: topic=$KAFKA_TOPIC brokers=[$BROKERS] interval=${INTERVAL}s -> $OUT" >&2
while true; do
  {
    echo "# HELP ep_kafka_topic_disk_bytes Bytes of the topic's partition directories in the broker log dir (du -sb)."
    echo "# TYPE ep_kafka_topic_disk_bytes gauge"
    for b in $BROKERS; do
      # sh -c inside the container so the glob <topic>-* is expanded there. 2>/dev/null: a directory may vanish while du runs.
      # awk adds up the per-directory lines; no match at all (topic not created yet) gives 0.
      t=$(docker exec "$b" sh -c "du -sb $LOG_DIR/${KAFKA_TOPIC}-* 2>/dev/null" | awk '{s+=$1} END{printf "%d", s+0}')
      echo "ep_kafka_topic_disk_bytes{broker=\"$b\",topic=\"$KAFKA_TOPIC\"} ${t:-0}"
    done
    echo "# HELP ep_kafka_logdir_disk_bytes Bytes of the whole broker log dir (du -sb)."
    echo "# TYPE ep_kafka_logdir_disk_bytes gauge"
    for b in $BROKERS; do
      a=$(docker exec "$b" sh -c "du -sb $LOG_DIR 2>/dev/null" | awk '{printf "%d", $1}')
      echo "ep_kafka_logdir_disk_bytes{broker=\"$b\"} ${a:-0}"
    done
  } >"$TMP" && mv "$TMP" "$OUT"      # atomic: node_exporter never reads a half-written file
  sleep "$INTERVAL"
done
