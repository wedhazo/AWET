VENV=.venv
PYTHON=$(VENV)/bin/python
PIP=$(VENV)/bin/pip

.PHONY: setup setup-ml lint typecheck test up down demo backfill train infer demo-real approve revoke llm-test

setup:
	python -m venv $(VENV)
	$(PIP) install -U pip
	$(PIP) install -e .[dev]

setup-ml: setup
	$(PIP) install -e .[ml]

lint:
	$(VENV)/bin/ruff check src tests execution

typecheck:
	$(VENV)/bin/mypy src

test:
	$(VENV)/bin/pytest

up:
	docker compose up -d

down:
	docker compose down

# SuperAGI - Autonomous AI Orchestration
superagi-up:
	@echo "🚀 Starting SuperAGI..."
	docker compose up -d superagi-backend superagi-celery superagi-gui superagi-proxy superagi-redis superagi-postgres
	@echo "✅ SuperAGI GUI available at http://localhost:3001"
	@echo "📝 Configure API keys in .env: OPENAI_API_KEY, ANTHROPIC_API_KEY"

superagi-patch-ollama:
	@echo "🔧 Applying Ollama compatibility patch to SuperAGI..."
	@bash superagi/patches/apply_ollama_patch.sh

superagi-down:
	@echo "🛑 Stopping SuperAGI..."
	docker compose stop superagi-backend superagi-celery superagi-gui superagi-proxy superagi-redis superagi-postgres

superagi-logs:
	docker compose logs -f superagi-backend superagi-celery

superagi-build:
	@echo "🔨 Building SuperAGI containers..."
	docker compose build superagi-backend superagi-celery superagi-gui

# Tool Gateway - HTTP endpoints for SuperAGI to call AWET scripts
tool-gateway:
	@echo "🔧 Starting AWET Tool Gateway on port 8200..."
	$(PYTHON) -m src.orchestration.tool_gateway

tool-gateway-bg:
	@echo "🔧 Starting AWET Tool Gateway in background..."
	nohup $(PYTHON) -m src.orchestration.tool_gateway > .tmp/tool_gateway.log 2>&1 &
	@echo "✅ Tool Gateway started (logs: .tmp/tool_gateway.log)"

# SuperAGI Tool Registration
superagi-register-tools:
	@echo "📦 Registering AWET tools with SuperAGI..."
	$(PYTHON) -m execution.register_superagi_tools --check-gateway

superagi-register-awet-agent:
	@echo "🤖 Registering AWET Orchestrator agent in SuperAGI..."
	$(PYTHON) -m tools.register_awet_agent

superagi-register-agents:
	@echo "🤖 Registering AWET SuperAGI agent team..."
	$(PYTHON) -m superagi.register_awet_agents

superagi-list-tools:
	@echo "📋 Available AWET tools:"
	$(PYTHON) -m execution.register_superagi_tools --list-tools

superagi-create-agent:
	@echo "🤖 Creating AWET Orchestrator agent in SuperAGI..."
	docker cp execution/create_superagi_agent.py superagi-backend:/tmp/create_agent.py
	docker exec superagi-backend python /tmp/create_agent.py
	@echo "✅ Open http://localhost:3001 → Agents → AWET Orchestrator → Run"

demo:
	docker compose up -d
	$(PYTHON) execution/demo.py

e2e-demo:
	$(PYTHON) scripts/run_e2e_demo.py

# SuperAGI end-to-end smoke test
# Full test requires LLM configured (~15 min with CPU Ollama)
# Set E2E_SKIP_FULL_RUN=1 to skip the full run
e2e-superagi:
	@echo "🧪 Running SuperAGI e2e smoke test (may take 15+ min with CPU Ollama)..."
	$(VENV)/bin/pytest tests/integration/test_e2e_superagi_demo.py -v -s -m "e2e and slow"

# Quick connectivity test - no LLM required (~10s)
e2e-superagi-quick:
	@echo "🧪 Running SuperAGI connectivity tests only (~10s)..."
	$(VENV)/bin/pytest tests/integration/test_e2e_superagi_demo.py -v -s -k "connection or exists"

# Skip full e2e run (for CI without GPU)
e2e-superagi-ci:
	@echo "🧪 Running SuperAGI tests (skipping full e2e run)..."
	E2E_SKIP_FULL_RUN=1 $(VENV)/bin/pytest tests/integration/test_e2e_superagi_demo.py -v -s -m e2e

llm-test:
	$(PYTHON) -m execution.test_llm

llm-plan:
	$(PYTHON) -m execution.generate_plan trading_pipeline

llm-explain:
	$(PYTHON) -m execution.explain_run --latest

# Approval gate controls (GAP 6.2)
approve:
	@mkdir -p .tmp && touch .tmp/APPROVE_EXECUTION
	@echo "✅ Execution APPROVED - .tmp/APPROVE_EXECUTION created"

revoke:
	@rm -f .tmp/APPROVE_EXECUTION
	@echo "🚫 Execution REVOKED - .tmp/APPROVE_EXECUTION removed"

# Real implementation targets

backfill-polygon:
	@echo "Starting Polygon historical backfill to Kafka..."
	docker compose up -d kafka schema-registry timescaledb
	sleep 5
	$(PYTHON) execution/backfill_polygon.py \
		--data-dir "/home/kironix/train/poligon/Minute Aggregates" \
		--symbols AAPL,MSFT,GOOG,AMZN,NVDA,META,TSLA \
		--batch-size 1000

backfill-polygon-dry:
	@echo "Dry run: validating Polygon data..."
	$(PYTHON) execution/backfill_polygon.py \
		--data-dir "/home/kironix/train/poligon/Minute Aggregates" \
		--symbols AAPL \
		--dry-run

backfill-reddit:
	@echo "Starting Reddit historical backfill to Kafka..."
	docker compose up -d kafka schema-registry
	sleep 3
	$(PYTHON) execution/backfill_reddit.py \
		--submissions-dir "/home/kironix/train/reddit/submissions" \
		--batch-size 500

backfill-reddit-dry:
	@echo "Dry run: validating Reddit data..."
	$(PYTHON) execution/backfill_reddit.py \
		--submissions-dir "/home/kironix/train/reddit/submissions" \
		--max-records 100 \
		--dry-run

backfill: backfill-polygon
	@echo "Backfill complete!"

# YFinance backfill (FREE - no API key required)
backfill-yfinance:
	@echo "📈 Backfilling daily data from Yahoo Finance (FREE)..."
	$(PYTHON) -m execution.yfinance_backfill --symbols AAPL,MSFT,GOOGL,AMZN,NVDA --days 30 --dry-run

backfill-yfinance-live:
	@echo "📈 Backfilling to Kafka from Yahoo Finance..."
	$(PYTHON) -m execution.yfinance_backfill --symbols AAPL,MSFT,GOOGL,AMZN,NVDA --days 30

# =============================================
# Live Daily Ingestion (FREE - Yahoo Finance)
# =============================================
live-ingest:
	@echo "📊 Fetching latest daily data from Yahoo Finance (FREE)..."
	$(PYTHON) -m execution.run_live_ingestion

live-ingest-dry:
	@echo "📊 [DRY RUN] Live ingestion preview..."
	$(PYTHON) -m execution.run_live_ingestion --dry-run

live-pipeline:
	@echo "🚀 Live ingestion + full pipeline trigger..."
	docker compose up -d kafka schema-registry timescaledb redis
	sleep 3
	$(PYTHON) -m execution.run_live_ingestion --trigger-pipeline

live-ingest-custom:
	@echo "📊 Live ingestion with custom symbols..."
	@read -p "Enter symbols (comma-separated, e.g., AAPL,TSLA): " symbols && \
	$(PYTHON) -m execution.run_live_ingestion --symbols $$symbols

train:
	@echo "Training TFT model..."
	mkdir -p models .tmp
	$(PYTHON) -m src.ml.train_tft --cache .tmp/tft_dataset.npz

train-from-db:
	@echo "Training TFT model from features_tft table..."
	mkdir -p models
	$(PYTHON) -m src.ml.train_tft --from-db

train-export:
	@echo "Exporting existing model to ONNX..."
	$(PYTHON) -m src.ml.train_tft --export-only

infer:
	@echo "Starting prediction agent with ONNX inference..."
	docker compose up -d
	PREDICTION_ENGINE=onnx $(PYTHON) -m src.agents.time_series_prediction

demo-real:
	@echo "Running full real pipeline: backfill -> features -> inference -> risk -> execution"
	docker compose up -d
	sleep 5
	@echo "Step 1: Backfilling historical data to Kafka..."
	$(PYTHON) execution/backfill_polygon.py \
		--data-dir "/home/kironix/train/poligon/Minute Aggregates" \
		--symbols AAPL,MSFT \
		--batch-size 500 || true
	@echo "Step 2: Running pipeline agents..."
	timeout 120 $(PYTHON) execution/demo.py || true
	@echo "Step 3: Checking DB tables..."
	docker exec timescaledb psql -U awet -c "SELECT 'audit_events' as tbl, count(*) FROM audit_events UNION ALL SELECT 'features_tft', count(*) FROM features_tft UNION ALL SELECT 'predictions_tft', count(*) FROM predictions_tft;"

clean-db:
	@echo "Truncating all tables..."
	docker exec timescaledb psql -U awet -c "TRUNCATE audit_events, features_tft, predictions_tft, paper_trades CASCADE;"

verify-db:
	$(PYTHON) execution/verify_db.py

# =============================================
# SuperAGI - Autonomous AI Orchestrator
# =============================================
superagi-up:
	@echo "🚀 Starting SuperAGI orchestrator..."
	docker compose up -d superagi-backend superagi-celery superagi-gui superagi-proxy superagi-redis superagi-postgres
	@echo "✅ SuperAGI GUI available at http://localhost:3001"

superagi-down:
	@echo "🛑 Stopping SuperAGI..."
	docker compose stop superagi-backend superagi-celery superagi-gui superagi-proxy superagi-redis superagi-postgres

superagi-logs:
	docker compose logs -f superagi-backend superagi-celery

superagi-build:
	@echo "🔨 Building SuperAGI containers..."
	docker compose build superagi-backend superagi-gui

# =============================================
# Cloud Deployment (Kubernetes)
# =============================================
deploy-preview-dev:
	@echo "📋 Preview dev deployment..."
	kubectl kustomize deploy/k8s/overlays/dev

deploy-preview-prod:
	@echo "📋 Preview prod deployment..."
	kubectl kustomize deploy/k8s/overlays/prod

deploy-dev:
	@echo "🚀 Deploying to dev environment..."
	kubectl apply -k deploy/k8s/overlays/dev
	@echo "⏳ Waiting for rollout..."
	kubectl rollout status deployment -n awet-dev -w --timeout=300s

deploy-prod:
	@echo "🚀 Deploying to production..."
	@read -p "⚠️  Are you sure you want to deploy to PRODUCTION? (yes/no): " confirm && [ "$$confirm" = "yes" ]
	kubectl apply -k deploy/k8s/overlays/prod
	@echo "⏳ Waiting for rollout..."
	kubectl rollout status deployment -n awet-prod -w --timeout=300s

deploy-status:
	@echo "📊 Deployment status:"
	kubectl get pods -n awet-prod -o wide 2>/dev/null || kubectl get pods -n awet-dev -o wide

docker-build:
	@echo "🐳 Building production Docker image..."
	docker build -f deploy/Dockerfile -t awet:latest --target production .

docker-build-dev:
	@echo "🐳 Building development Docker image..."
	docker build -f deploy/Dockerfile -t awet:dev --target development .
