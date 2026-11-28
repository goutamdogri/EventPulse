#!/usr/bin/env bash
# Seed the `tenants` table (S3-F4) from the API keys ingest-api knows (API_KEYS in services/ingest-api/.env, or the
# API_KEYS environment variable). Idempotent. Uses DATABASE_URL of services/admin-api/.env (else consumer-sink's).
set -euo pipefail
cd "$(dirname "$0")/.."
envfile=services/admin-api/.env
[ -f "$envfile" ] || envfile=services/consumer-sink/.env
pnpm --filter @eventpulse/admin-api build >/dev/null
node --env-file="$envfile" services/admin-api/dist/seed.js
