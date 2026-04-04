.PHONY: help setup up up-infra down logs test lint build push deploy clean status

# Load .env if it exists (for local training scripts etc.)
-include .env
export

AWS_REGION       ?= ap-northeast-1
AWS_ACCOUNT_ID   ?= $(shell aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "000000000000")
ECR_REGISTRY     ?= $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com
ENV              ?= dev
SERVICES         := ingestion preprocessing inference alert

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# --- Setup ---
setup: ## Install all workspace dependencies
	uv sync --all-packages

setup-dev: ## Install with dev dependencies
	uv sync --all-packages --dev

lock: ## Update uv.lock
	uv lock

# --- Local Development ---
up: ## Start local environment
	docker compose up -d 
	@echo ""
	@echo "  Ingestion:     http://localhost:8001"
	@echo "  Preprocessing: http://localhost:8002"
	@echo "  Inference:     http://localhost:8003"
	@echo "  Alert:         http://localhost:8004"
	@echo "  MLflow:        http://localhost:5001"
	@echo "  MinIO Console: http://localhost:9001"
	@echo "  Prometheus:    http://localhost:9090"
	@echo "  Grafana:       http://localhost:3000  (admin/admin)"

up-infra:
	docker compose up -d redpanda valkey postgres minio minio-init mlflow dapr-placement

down: ## Stop local environment
	docker compose down

logs: ## Tail all logs
	docker compose logs -f --tail=50

logs-%: ## Tail specific service (e.g. make logs-ingestion)
	docker compose logs -f --tail=100 $*

status: ## Health check
	@docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

# --- Run a service locally (outside Docker, with hot reload) ---
run-%: ## Run service locally (e.g. make run-ingestion)
	uv run --package $*-svc uvicorn $*.main:app --host 0.0.0.0 --port 8000 --reload

# --- Testing ---
test: ## Run all tests
	uv run pytest --tb=short -q

test-%: ## Test specific service (e.g. make test-ingestion)
	uv run pytest services/$*/tests/ -v

cov: ## Run tests with coverage
	uv run pytest --cov --cov-report=html

# --- Linting ---
lint: ## Lint all code
	uv run ruff check .
	uv run ruff format --check .

lint-fix: ## Auto-fix lint
	uv run ruff check --fix .
	uv run ruff format .

# --- Build & Push ---
build: ## Build all Docker images
	@for svc in $(SERVICES); do \
		echo "\n=== Building $$svc ==="; \
		docker build -f services/$$svc/Dockerfile -t ml-pipeline/$$svc:latest .; \
	done
	@echo "\n=== Building training (GPU) ==="
	docker build -f ml/training/Dockerfile.gpu -t ml-pipeline/training:latest .

build-%: ## Build specific service (e.g. make build-ingestion)
	docker build -f services/$*/Dockerfile -t ml-pipeline/$*:latest .

ecr-login: ## Login to ECR
	aws ecr get-login-password --region $(AWS_REGION) | \
		docker login --username AWS --password-stdin $(ECR_REGISTRY)

push: ecr-login ## Push all images to ECR
	@for svc in $(SERVICES); do \
		docker tag ml-pipeline/$$svc:latest $(ECR_REGISTRY)/ml-pipeline-$$svc:latest; \
		docker push $(ECR_REGISTRY)/ml-pipeline-$$svc:latest; \
	done

# --- Deploy ---
deploy: ## Deploy to EKS (ENV=dev|prod)
	helm upgrade --install ml-pipeline ./helm \
		-f helm/values-$(ENV).yaml \
		--namespace ml-pipeline --create-namespace \
		--wait --timeout 5m

deploy-dry: ## Dry-run deploy
	helm upgrade --install ml-pipeline ./helm \
		-f helm/values-$(ENV).yaml \
		--namespace ml-pipeline --dry-run --debug

# --- Terraform ---
tf-init: ## Terraform init
	cd terraform && terraform init -backend-config=envs/$(ENV)/backend.hcl

tf-plan: ## Terraform plan
	cd terraform && terraform plan -var-file=envs/$(ENV)/terraform.tfvars -out=tfplan

tf-apply: ## Terraform apply
	cd terraform && terraform apply tfplan

# --- Utilities ---
create-topics: ## Create Kafka topics
	docker compose exec redpanda rpk topic create raw-data --partitions 3
	docker compose exec redpanda rpk topic create preprocessed-data --partitions 3
	docker compose exec redpanda rpk topic create inference-results --partitions 3
	docker compose exec redpanda rpk topic create alerts --partitions 1
	docker compose exec redpanda rpk topic create model-updates --partitions 1
	@docker compose exec redpanda rpk topic list

clean: ## Remove all local volumes
	docker compose down -v
	@echo "All volumes removed."

# --- ML Training (local) ---
train-preprocess: ## Generate training data
	uv run python -m training.preprocess

train: ## Train the model
	uv run python -m training.train

train-evaluate: ## Evaluate the trained model
	uv run python -m training.evaluate

train-register: ## Register model to MLflow
	uv run python -m training.register

train-all: train-preprocess train train-evaluate train-register ## Run full training pipeline
	@echo "Training pipeline complete."

# --- Developer Tools ---
seed: ## Send sample data through the pipeline
	uv run python scripts/seed_data.py

seed-continuous: ## Continuous data stream (1/sec)
	uv run python scripts/seed_data.py --continuous

health: ## Check health of all services + Dapr
	uv run python scripts/health_check.py

watch: ## Real-time pipeline monitor
	uv run python scripts/watch_pipeline.py

demo: up ## Full demo: start + wait + seed + watch
	@echo "Waiting 15s for services to start..."
	@sleep 15
	uv run python scripts/seed_data.py --count 20
	@echo "\nStarting monitor..."
	uv run python scripts/watch_pipeline.py
