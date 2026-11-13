stage1-reset:
	docker exec ep-postgres psql -U postgres -d eventpulse -c "SELECT pg_stat_statements_reset(); TRUNCATE events;"
stage1-migrate:
	pnpm --filter @eventpulse/ingest-api migrate:up
stage1-prod-start:
	pnpm --filter @eventpulse/ingest-api start:prod
stage1-prod-start-pinned:
	pnpm --filter @eventpulse/ingest-api start:pinned