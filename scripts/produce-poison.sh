#!/usr/bin/env bash
# Write ONE bad record straight into a topic (bypassing the API), to test poison-message handling (S2-F4, K6).
#
# Usage: scripts/produce-poison.sh --kind KIND [--key KEY] [--topic TOPIC]
#   KIND  invalid-json     value is not JSON                              -> parser: invalid_json
#         bad-uuid         event_id is not a UUID                         -> parser: bad_event_id
#         oversized-field  type is 200 chars (API max is 100)             -> parser: bad_type
#         constraint       passes the parser, rejected by Postgres:
#                          payload contains \u0000 (jsonb refuses it)     -> SQLSTATE 22P05
#   KEY   record key; the key decides the partition (same partitioner as our producer: murmur2).
#         Use a key that the load is using to hit a busy partition, e.g. a tenant id. Default: poison-<random>.
#   TOPIC default events.raw
#
# Runs kafka-console-producer inside the ep-kafka1 container (INTERNAL listener), so nothing to install.
set -euo pipefail

KIND="" KEY="poison-$RANDOM" TOPIC="events.raw" CONTAINER="${KAFKA_CONTAINER:-ep-kafka1}"
while [ $# -gt 0 ]; do
  case "$1" in
    --kind) KIND="$2"; shift 2 ;;
    --key) KEY="$2"; shift 2 ;;
    --topic) TOPIC="$2"; shift 2 ;;
    -h|--help) sed -n '2,16p' "$0"; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

NOW="$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)"
ID="$(cat /proc/sys/kernel/random/uuid)"
case "$KIND" in
  invalid-json)    VALUE='{"event_id": "'"$ID"'", "type": broken json' ;;
  bad-uuid)        VALUE='{"event_id":"not-a-uuid","tenant_id":"poison","type":"poison.bad_uuid","occurred_at":"'"$NOW"'","schema_version":1,"payload":{}}' ;;
  oversized-field) VALUE='{"event_id":"'"$ID"'","tenant_id":"poison","type":"'"$(printf 'x%.0s' $(seq 1 200))"'","occurred_at":"'"$NOW"'","schema_version":1,"payload":{}}' ;;
  constraint)      VALUE='{"event_id":"'"$ID"'","tenant_id":"poison","type":"poison.constraint","occurred_at":"'"$NOW"'","schema_version":1,"payload":{"note":"nul \u0000 byte"}}' ;;
  *) echo "--kind must be invalid-json | bad-uuid | oversized-field | constraint" >&2; exit 2 ;;
esac

# Line format: <headers> TAB <key> | <value>. Headers k=v separated by commas (received_at contains ':').
LINE="$(printf 'received_at=%s,schema_version=1,request_id=poison-%s\t%s|%s' "$NOW" "$KIND" "$KEY" "$VALUE")"
printf '%s\n' "$LINE" | docker exec -i "$CONTAINER" /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka1:19092 --topic "$TOPIC" --request-required-acks all --sync \
  --reader-property parse.key=true --reader-property key.separator='|' \
  --reader-property parse.headers=true --reader-property headers.delimiter=$'\t' \
  --reader-property headers.separator=',' --reader-property headers.key.separator='=' >/dev/null

echo "produced poison kind=$KIND key=$KEY topic=$TOPIC event_id=$ID"
