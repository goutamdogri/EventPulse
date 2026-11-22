#!/usr/bin/env bash
# CLEAN SLATE (destructive): TRUNCATE events, delete + recreate events.raw / events.dlq, delete the consumer group.
# Asks for confirmation. Refuses while ingest-api (:8000) or consumer-sink (:8001) is running.
set -euo pipefail
cd "$(dirname "$0")/.."
for port in 8000 8001; do
  if curl -sf "localhost:$port/healthz" >/dev/null 2>&1; then echo "something is running on :$port: stop the pipeline first (make stage2-down)"; exit 1; fi
done
set -a; . infra/compose/.env; set +a
echo "This DELETES all rows of ${POSTGRES_DB}.events, all records of events.raw and events.dlq, and group sink-postgres."
read -r -p 'Type "reset" to continue: ' answer
[ "$answer" = reset ] || { echo "aborted"; exit 1; }
K="docker exec ep-kafka1 /opt/kafka/bin"
docker exec ep-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "TRUNCATE events;" >/dev/null && echo "events truncated"
$K/kafka-consumer-groups.sh --bootstrap-server kafka1:19092 --delete --group sink-postgres >/dev/null 2>&1 && echo "group sink-postgres deleted" || echo "group sink-postgres did not exist"
$K/kafka-topics.sh --bootstrap-server kafka1:19092 --delete --topic events.raw >/dev/null 2>&1 || true
$K/kafka-topics.sh --bootstrap-server kafka1:19092 --delete --topic events.dlq >/dev/null 2>&1 || true
for i in $(seq 1 30); do $K/kafka-topics.sh --bootstrap-server kafka1:19092 --list | grep -qE '^events\.(raw|dlq)$' || break; sleep 1; done
docker start -a ep-kafka-init | tail -n 12   # recreates both topics and prints leader / ISR
