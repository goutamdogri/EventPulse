# EventPulse: one entry point for the current pipeline (Stage 3: Correctness).
# `make` or `make help` lists every target.

SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE_CORE  := docker compose -f infra/compose/docker-compose.yml
COMPOSE_KAFKA := docker compose -f infra/compose/docker-compose.kafka.yml
SINK          := pnpm --filter @eventpulse/consumer-sink
API           := pnpm --filter @eventpulse/ingest-api
CORRECTNESS   := pnpm --filter @eventpulse/correctness-tests
UNIT_PACKAGES := --filter "./packages/*" --filter "./services/*" --filter "./tools/*" --filter "./tests/*"
TOXI_URL      ?= http://localhost:8474

.PHONY: help
help: ## List targets
	@awk 'BEGIN {FS = ":.*## "} /^# ---/ {printf "\n%s\n", substr($$0, 7)} /^[a-z0-9-]+:.*## / {printf "  make %-22s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# --- Pipeline ---
.PHONY: up down lag audit reset
up: ## Whole pipeline: containers, topics, migrations, build, consumer-sink, ingest-api (scripts/up.sh)
	scripts/up.sh
down: ## Stop ingest-api and consumer-sink gracefully (containers keep running)
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
toxiproxy: ## Create the Toxiproxy proxies if missing: "pg" (:15434 -> postgres), "redis" (:16379 -> redis)
	$(call toxi_proxy,pg,15434,postgres:5432)
	$(call toxi_proxy,redis,16379,redis:6379)

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

# --- Build and run single services ---
.PHONY: build api-start api-start-pinned consumer-start consumer-start-pinned
build: ## Build the shared packages, ingest-api and consumer-sink
	$(API) build && $(SINK) build
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
test-e2e: toxiproxy migrate-test ## Service e2e suites (Kafka + test database): consumer-sink, then ingest-api
	$(SINK) test:e2e
	$(API) test:e2e
correctness: migrate-test ## Correctness suite: log replay + crash matrix (regenerates docs/generated/crash-matrix.*)
	$(CORRECTNESS) test:e2e
crash-matrix: migrate-test ## Only the crash matrix; one point: make crash-matrix ONLY=consumer:after-db-commit
	$(if $(ONLY),CRASH_MATRIX_ONLY=$(ONLY)) $(CORRECTNESS) crash-matrix
test-all: test test-e2e correctness ## Everything above, in order
