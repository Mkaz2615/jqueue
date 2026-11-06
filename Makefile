# JQueue — developer entry points.
#
# Every command a contributor needs lives here, so nobody has to reconstruct a Maven
# invocation from memory or from CI logs. ./mvnw is the Maven wrapper: it downloads the
# pinned Maven version on first use, so only a JDK 21 is required.

SHELL        := /bin/bash
MVN          := ./mvnw -B
JAR          := target/jqueue.jar
CONFIG_PATH  ?= ./config/config.local.yaml
COMPOSE      := docker compose -f deployment/docker-compose.yml

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

.PHONY: deps
deps: ## Download every dependency (run this first after cloning)
	@$(MVN) -q dependency:go-offline
	@echo "==> dependencies are ready"

.PHONY: build
build: ## Build the executable jar into ./target
	@$(MVN) -q package -DskipTests
	@echo "==> $(JAR)"

.PHONY: run
run: build ## Run the service against the local config
	@JQUEUE_CONFIG_PATH=$(CONFIG_PATH) java -jar $(JAR)

.PHONY: clean
clean: ## Remove build artefacts and coverage output
	@$(MVN) -q clean
	@echo "==> cleaned"

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

.PHONY: test
test: ## Run unit tests (no external dependencies required)
	@$(MVN) test

.PHONY: test-integration
test-integration: ## Run integration tests against a live Redis (set REDIS_ADDR to override)
	@echo "==> integration tests require Redis at $${REDIS_ADDR:-127.0.0.1:6379}"
	@$(MVN) test -Pintegration

.PHONY: cover
cover: ## Run unit tests with coverage; report at target/site/jacoco/index.html
	@$(MVN) test
	@echo "==> target/site/jacoco/index.html"

.PHONY: validate
validate: test test-integration ## Full pre-push check
	@echo "==> validate passed"

# ---------------------------------------------------------------------------
# Local stack
# ---------------------------------------------------------------------------

.PHONY: env-up
env-up: ## Start Redis only, for running the service from source
	@$(COMPOSE) up -d redis
	@echo "==> redis is up on 127.0.0.1:6379"

.PHONY: env-down
env-down: ## Stop Redis
	@$(COMPOSE) stop redis

.PHONY: stack-up
stack-up: ## Start the full stack: service, Redis, Prometheus, Grafana
	@$(COMPOSE) up -d --build
	@echo ""
	@echo "  dashboard   http://localhost:9090/dashboard"
	@echo "  api         http://localhost:8080/api/v1"
	@echo "  metrics     http://localhost:9090/metrics"
	@echo "  prometheus  http://localhost:9091"
	@echo "  grafana     http://localhost:3000  (admin/admin)"
	@echo ""

.PHONY: stack-down
stack-down: ## Stop the full stack
	@$(COMPOSE) down

.PHONY: stack-clean
stack-clean: ## Stop the full stack and delete its volumes
	@$(COMPOSE) down -v

.PHONY: logs
logs: ## Tail the service logs
	@$(COMPOSE) logs -f jqueue

# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

.PHONY: seed
seed: ## Enqueue a spread of demo jobs against a running service
	@./scripts/seed.sh

.PHONY: load
load: ## Generate continuous load against a running service
	@./scripts/load.sh

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
