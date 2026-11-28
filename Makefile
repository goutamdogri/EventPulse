# EventPulse: one entry point for the current pipeline (Stage 3: Correctness).
# `make` or `make help` lists every target.

SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE_CORE  := docker compose -f infra/compose/docker-compose.yml
COMPOSE_KAFKA := docker compose -f infra/compose/docker-compose.kafka.yml
SINK          := pnpm --filter @eventpulse/consumer-sink
API           := pnpm --filter @eventpulse/ingest-api
ADMIN         := pnpm --filter @eventpulse/admin-api
RELAY         := pnpm --filter @eventpulse/outbox-relay
CORRECTNESS   := pnpm --filter @eventpulse/correctness-tests
UNIT_PACKAGES := --filter "./packages/*" --filter "./services/*" --filter "./tools/*" --filter "./tests/*"
TOXI_URL      ?= http://localhost:8474

.PHONY: help
help: ## List targets
	@awk 'BEGIN {FS = ":.*## "} /^# ---/ {printf "\n%s\n", substr($$0, 7)} /^[a-z0-9-]+:.*## / {printf "  make %-22s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# --- Pipeline ---
.PHONY: up down lag audit reset
up: ## Whole pipeline: containers, topics, migrations, schemas, tenants, build, consumer-sink, ingest-api, admin-api, outbox-relay
	scripts/up.sh
down: ## Stop the four services gracefully (containers keep running)
	scripts/down.sh
lag: ## Consumer group lag per partition
	scripts/lag.sh
audit: ## Load + reconciliation: make audit RATE=500 DURATION=30s [RUN=a1b2c3d4] [RESEND=0.1]
	RATE=$(or $(RATE),200) DURATION=$(or $(DURATION),30s) $(if $(RUN),RUN_ID=$(RUN)) $(if $(RESEND),RESEND_FRACTION=$(RESEND)) scripts/audit.sh run
reset: ## DESTRUCTIVE clean slate: truncate events, recreate topics, delete the group (asks first)
	scripts/reset.sh

# --- Infrastructure ---
.PHONY: infra-up infra-down kafka-up kafka-down toxiproxy
infra-up: ## Postgres, Redis, Prometheus, Grafana, exporters, Toxiproxy (then make toxiproxy)
	$(COMPOSE_CORE) up -d
infra-down: ## Stop the core containers (data volumes are kept)
	$(COMPOSE_CORE) down
kafka-up: ## 3 KRaft brokers + topic init + exporter
	$(COMPOSE_KAFKA) up -d
kafka-down: ## Stop the brokers
	$(COMPOSE_KAFKA) down
define toxi_proxy
	@curl -sf $(TOXI_URL)/proxies/$(1) >/dev/null \
	  && echo 'toxiproxy: proxy "$(1)" exists' \
	  || { curl -sf -X POST $(TOXI_URL)/proxies -H 'Content-Type: application/json' \
	         -d '{"name":"$(1)","listen":"0.0.0.0:$(2)","upstream":"$(3)","enabled":true}' >/dev/null \
	       && echo 'toxiproxy: proxy "$(1)" created'; }
endef
toxiproxy: ## Create the Toxiproxy proxies if missing: "pg" (:15434), "redis" (:16379), "registry" (:18081)
	$(call toxi_proxy,pg,15434,postgres:5432)
	$(call toxi_proxy,redis,16379,redis:6379)
	$(call toxi_proxy,registry,18081,host.docker.internal:8081)

# --- Database (db/migrations, run through consumer-sink, which owns the events table) ---
.PHONY: migrate migrate-down migrate-test migrate-new
migrate: ## Apply pending migrations to the dev database (services/consumer-sink/.env)
	$(SINK) migrate:up
migrate-down: ## Roll back the LAST migration on the dev database
	$(SINK) migrate:down
migrate-test: ## Apply pending migrations to the test database (services/consumer-sink/.env.test)
	$(SINK) migrate:test
migrate-new: ## New migration file: make migrate-new NAME=add_something
	@test -n "$(NAME)" || { echo "usage: make migrate-new NAME=add_something"; exit 2; }
	$(SINK) migrate:new $(NAME)

# --- Schemas (S3-F7: schemas/, tools/schema-check, Schema Registry on :8081) ---
.PHONY: schemas schema-check schema-check-against schema-fixtures
schemas: ## Register every schema version in order (idempotent deploy step; services never register at runtime)
	node tools/schema-check/src/cli.ts register
schema-check: ## Whole history of every subject under its level, on throw-away subjects (what CI runs)
	node tools/schema-check/src/cli.ts check
schema-check-against: ## Newest local version vs the REAL subject on the running registry
	node tools/schema-check/src/cli.ts check --against
schema-fixtures: ## Incompatible fixtures must be rejected, compatible ones accepted (also shows BACKWARD's verdict)
	node tools/schema-check/src/cli.ts fixtures

# --- DLQ triage and replay (S3-F6: tools/dlq-replay, docs/runbooks/dlq-triage-and-replay.md) ---
.PHONY: dlq-stats dlq-replay
dlq-stats: ## Unreplayed dead letters by class, code, tenant, failed topic and age (start of triage)
	node tools/dlq-replay/src/cli.ts stats
dlq-replay: ## DLQ tool: make dlq-replay ARGS="replay --error-class PERMANENT_DB" (dry run unless ARGS has --execute)
	node tools/dlq-replay/src/cli.ts $(or $(ARGS),--help)

# --- Tenant config and outbox (S3-F4: services/admin-api, services/outbox-relay, docs/runbooks/outbox-stuck.md) ---
.PHONY: seed-tenants admin-start relay-start outbox
seed-tenants: ## Insert the tenants of ingest-api's API_KEYS into `tenants` (idempotent)
	scripts/seed-tenants.sh
admin-start: ## admin-api on :8020 (services/admin-api/.env), foreground
	$(ADMIN) start:prod
relay-start: ## outbox-relay on :8021 (services/outbox-relay/.env), foreground; a 2nd one: PORT=8022 stands by
	$(RELAY) start:prod
outbox: ## Outbox backlog, oldest unpublished age, relay leader (from the relay's /debug/stats)
	@curl -sf localhost:8021/debug/stats | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const j=JSON.parse(s);console.log(JSON.stringify({leader:j.is_leader,backlog:j.backlog,published:j.relay.published,republished:j.relay.republished,failed_cycles:j.relay.failedCycles,last_error:j.relay.lastError},null,2))})' || echo "outbox-relay not reachable on :8021"

# --- Build and run single services ---
.PHONY: build api-start api-start-pinned consumer-start consumer-start-pinned
build: ## Build the shared packages and the four services
	$(API) build && $(SINK) build && $(ADMIN) build && $(RELAY) build
api-start: ## ingest-api on :8000 (services/ingest-api/.env), foreground
	$(API) start:prod
api-start-pinned: ## Same, pinned with taskset to SERVER_CPUS (default 0,6)
	$(API) start:pinned
consumer-start: ## consumer-sink on :8001 (services/consumer-sink/.env), foreground
	$(SINK) start:prod
consumer-start-pinned: ## Same, pinned with taskset to CONSUMER_CPUS (default 0,6)
	$(SINK) start:pinned

# --- Tests ---
.PHONY: test test-e2e correctness crash-matrix test-all
test: ## Unit tests of every package, service, tool and the correctness suite (no infrastructure)
	pnpm --filter "./packages/*" build >/dev/null
	pnpm $(UNIT_PACKAGES) --workspace-concurrency=1 test
test-e2e: toxiproxy migrate-test ## Service e2e suites (Kafka + test database): consumer-sink, then ingest-api (admin-api/relay: make correctness)
	$(SINK) test:e2e
	$(API) test:e2e
correctness: toxiproxy migrate-test ## Correctness suite: replay, crash matrix (regenerates docs/generated/crash-matrix.*), retry, DLQ replay, outbox
	$(CORRECTNESS) test:e2e
crash-matrix: toxiproxy migrate-test ## Only the crash matrix; one point: make crash-matrix ONLY=consumer:after-db-commit
	$(if $(ONLY),CRASH_MATRIX_ONLY=$(ONLY)) $(CORRECTNESS) crash-matrix
test-all: test test-e2e correctness ## Everything above, in order
