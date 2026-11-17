UV ?= uv
COMPOSE ?= docker compose
KIND ?= kind
KIND_CLUSTER ?= workflow-engine
K8S_DIR := deploy/k8s
K8S_IMAGE := workflow-engine:k8s
KUBECTL := kubectl --context kind-$(KIND_CLUSTER)
KUBECTL_NS := $(KUBECTL) -n workflow-engine
N ?= 4

.DEFAULT_GOAL := help
.PHONY: help install up down tracing-up migrate api worker maintenance lint fmt typecheck \
        test-unit test-integration test-chaos test bench \
        k8s-up k8s-scale k8s-load k8s-chaos k8s-port-forward k8s-down

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

test-chaos: ## Chaos tests (Docker; local or manual CI only, slow)
	$(UV) run pytest -m chaos

test: test-unit test-integration ## Unit + integration

bench: ## Throughput + chaos runs -> benchmarks/results.md (Docker; ~15 min, own throwaway stack)
	$(UV) run python -m benchmarks.run_throughput
	$(UV) run python -m benchmarks.run_chaos --no-build
	$(UV) run python -m benchmarks.report

k8s-up: ## kind cluster + image + manifests, then wait until every component is ready
	@$(KIND) get clusters 2>/dev/null | grep -qx $(KIND_CLUSTER) \
		|| $(KIND) create cluster --config $(K8S_DIR)/kind-config.yaml
	docker build -t $(K8S_IMAGE) .
	$(KIND) load docker-image $(K8S_IMAGE) --name $(KIND_CLUSTER)
	@test -f $(K8S_DIR)/secrets.env || { cp $(K8S_DIR)/secrets.example.env $(K8S_DIR)/secrets.env; \
		echo "created $(K8S_DIR)/secrets.env from secrets.example.env (placeholder values)"; }
	@# A Job's pod template is immutable, so the previous migrate Job is replaced each time.
	$(KUBECTL_NS) delete job migrate --ignore-not-found
	@# Re-running after a code change reloads the same tag, so existing pods are restarted.
	@existing=$$($(KUBECTL_NS) get deploy worker -o name 2>/dev/null); \
		$(KUBECTL) apply -k $(K8S_DIR) && if [ -n "$$existing" ]; then \
		$(KUBECTL_NS) rollout restart deploy/api deploy/worker deploy/maintenance; fi
	$(KUBECTL_NS) rollout status statefulset/postgres --timeout=180s
	$(KUBECTL_NS) wait --for=condition=complete job/migrate --timeout=300s
	$(KUBECTL_NS) rollout status deploy/redis --timeout=120s
	$(KUBECTL_NS) rollout status deploy/api --timeout=180s
	$(KUBECTL_NS) rollout status deploy/worker --timeout=180s
	$(KUBECTL_NS) rollout status deploy/maintenance --timeout=180s
	@echo "API: http://127.0.0.1:8800 (kind port mapping)"

k8s-scale: ## Scale workers: make k8s-scale N=4
	$(KUBECTL_NS) scale deploy/worker --replicas=$(N)
	$(KUBECTL_NS) rollout status deploy/worker --timeout=180s

k8s-load: ## Throughput at the current worker count (sleep jobs through the API)
	$(UV) run python -m benchmarks.run_k8s load $(ARGS)

k8s-chaos: ## Force-delete random worker pods under load; check every job completes
	$(UV) run python -m benchmarks.run_k8s chaos --force $(ARGS)

k8s-port-forward: ## Port-forward the API Service to localhost:8000
	$(KUBECTL_NS) port-forward svc/api 8000:8000

k8s-down: ## Delete the kind cluster
	$(KIND) delete cluster --name $(KIND_CLUSTER)
