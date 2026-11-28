#!/usr/bin/env bash
# One command for the whole pipeline (make up):
#   Postgres + Redis + observability containers, the 3 Kafka brokers + topics + Schema Registry, migrations, schema
#   registration (make schemas), tenants (scripts/seed-tenants.sh), build, consumer-sink, ingest-api, and (S3-F4)
#   admin-api + outbox-relay when their .env exists.
# The Node processes run in the background; pids and logs go to .run/. Stop them with make down (scripts/down.sh).
# Uses each service's .env (services/ingest-api/.env, services/consumer-sink/.env).
set -euo pipefail
cd "$(dirname "$0")/.."
RUN=.run; mkdir -p "$RUN"

running() { [ -f "$RUN/$1.pid" ] && kill -0 "$(cat "$RUN/$1.pid")" 2>/dev/null; }
if running ingest-api || running consumer-sink || running admin-api || running outbox-relay; then echo "already running (make down to stop)"; exit 1; fi

echo "[1/6] Postgres + observability"
docker compose -f infra/compose/docker-compose.yml up -d >/dev/null
echo "[2/6] Kafka (3 brokers) + topics"
docker compose -f infra/compose/docker-compose.kafka.yml up -d >/dev/null
for i in $(seq 1 120); do
  [ "$(docker inspect -f '{{.State.Health.Status}}' ep-kafka1 ep-kafka2 ep-kafka3 2>/dev/null | sort -u)" = healthy ] && break
  [ "$i" = 120 ] && { echo "brokers not healthy after 120 s"; exit 1; }; sleep 1
done
docker wait ep-kafka-init >/dev/null   # topic creation (idempotent) finished
for i in $(seq 1 60); do docker exec ep-postgres pg_isready -q 2>/dev/null && break; sleep 1; done
echo "[3/6] migrations + schemas"
pnpm --filter @eventpulse/consumer-sink migrate:up >/dev/null
for i in $(seq 1 120); do curl -sf localhost:8081/subjects >/dev/null && break; [ "$i" = 120 ] && { echo "schema registry not up after 120 s"; exit 1; }; sleep 1; done
pnpm --filter @eventpulse/schema build >/dev/null
node tools/schema-check/src/cli.ts register >/dev/null   # idempotent: the API only looks its schema id up
scripts/seed-tenants.sh >/dev/null                         # S3-F4: tenants from ingest-api's API_KEYS
echo "[4/6] build"
pnpm --filter @eventpulse/ingest-api build >/dev/null
pnpm --filter @eventpulse/consumer-sink build >/dev/null
pnpm --filter @eventpulse/admin-api --filter @eventpulse/outbox-relay build >/dev/null

start() { # name dir port
  (cd "services/$2" && exec node --env-file=.env dist/main.js) >"$RUN/$1.log" 2>&1 &
  echo $! >"$RUN/$1.pid"
  for i in $(seq 1 60); do curl -sf "localhost:$3/readyz" >/dev/null && return 0; sleep 1; done
  echo "$1 not ready after 60 s, see $RUN/$1.log"; return 1
}
echo "[5/6] consumer-sink (group sink-postgres, :8001)"
start consumer-sink consumer-sink 8001
echo "[6/6] ingest-api (:8000)"
start ingest-api ingest-api 8000
# S3-F4: tenant config admin API and the outbox relay (skipped without their .env, e.g. on an older checkout).
for svc in "admin-api 8020" "outbox-relay 8021"; do
  set -- $svc
  if [ -f "services/$1/.env" ]; then echo "[+] $1 (:$2)"; start "$1" "$1" "$2"; else echo "[skip] $1: no services/$1/.env (copy .env.example)"; fi
done
echo "pipeline up. logs: $RUN/*.log   lag: scripts/lag.sh   audit: make audit   stop: make down"
