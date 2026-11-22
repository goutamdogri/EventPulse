stage1-reset:
	docker exec ep-postgres psql -U eventpulse -d  eventpulse -c "SELECT pg_stat_statements_reset(); TRUNCATE events;"
stage1-migrate: stage2-migrate
stage1-prod-start:
	pnpm --filter @eventpulse/ingest-api start:prod
stage1-prod-start-pinned:
	pnpm --filter @eventpulse/ingest-api start:pinned

# --- Stage 2 ---
# Migrations now live with the service that owns the table (consumer-sink).
stage2-migrate:
	pnpm --filter @eventpulse/consumer-sink migrate:up
stage2-kafka-up:
	docker compose -f infra/compose/docker-compose.kafka.yml up -d
stage2-kafka-down:
	docker compose -f infra/compose/docker-compose.kafka.yml down
stage2-build:
	pnpm --filter @eventpulse/ingest-api build && pnpm --filter @eventpulse/consumer-sink build
stage2-api-start:
	pnpm --filter @eventpulse/ingest-api start:prod
stage2-api-start-pinned:
	pnpm --filter @eventpulse/ingest-api start:pinned
stage2-consumer-start:
	pnpm --filter @eventpulse/consumer-sink start:prod
stage2-consumer-start-pinned:
	pnpm --filter @eventpulse/consumer-sink start:pinned

# One command for the whole pipeline (containers, topics, migrations, build, consumer, API) and its stop.
stage2-up:
	scripts/stage2-up.sh
stage2-down:
	scripts/stage2-down.sh
stage2-lag:
	scripts/lag.sh
# S2-F7 audit: load + reconciliation. make stage2-audit RATE=500 DURATION=30s [RUN=a1b2c3d4]
stage2-audit:
	RATE=$(or $(RATE),200) DURATION=$(or $(DURATION),30s) $(if $(RUN),RUN_ID=$(RUN)) scripts/audit.sh run
