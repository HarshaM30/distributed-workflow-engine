UV ?= uv
COMPOSE ?= docker compose

.DEFAULT_GOAL := help
.PHONY: help install up down tracing-up migrate api worker maintenance lint fmt typecheck \
        test-unit test-integration test-chaos test bench

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-18s %s\n", $$1, $$2}'

install: ## Install runtime + dev dependencies
	$(UV) sync

up: ## Start Postgres + Redis and wait until healthy
	$(COMPOSE) up -d --wait postgres redis

down: ## Stop and remove containers (volumes are kept)
	$(COMPOSE) --profile app --profile tracing down

tracing-up: ## Start Jaeger (UI on :16686); run processes with OTEL_ENABLED=true to export spans
	$(COMPOSE) --profile tracing up -d jaeger

migrate: ## Apply database migrations
	$(UV) run alembic upgrade head

api: ## Run the HTTP API
	$(UV) run python -m workflow_engine.api

worker: ## Run one worker process
	$(UV) run python -m workflow_engine.worker

maintenance: ## Run the maintenance process (scheduler, reaper, sweeper; leader-elected)
	$(UV) run python -m workflow_engine.maintenance

lint: ## ruff check + format check + mypy --strict
	$(UV) run ruff check .
	$(UV) run ruff format --check .
	$(UV) run mypy

typecheck: ## mypy --strict on src/
	$(UV) run mypy

fmt: ## Format and auto-fix
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

test-unit: ## Unit tests (no services)
	$(UV) run pytest -m unit

test-integration: ## Integration tests (needs `make up`)
	$(UV) run pytest -m integration

test-chaos: ## Chaos tests (local only, slow)
	$(UV) run pytest -m chaos

test: test-unit test-integration ## Unit + integration

bench: ## Benchmarks -> benchmarks/results.md (Milestone 5)
	@echo "benchmarks are not implemented yet (Milestone 5)" >&2; exit 1
