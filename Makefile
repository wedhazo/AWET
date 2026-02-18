VENV=.venv
PYTHON=$(VENV)/bin/python
PIP=$(VENV)/bin/pip

.PHONY: setup setup-ml lint typecheck test up down demo backfill train infer demo-real approve revoke llm-test replay replay-last replay-test

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

schema-check:
	$(VENV)/bin/python scripts/check_schema_compatibility.py --schemas-dir schemas/avro src/schemas

# Deterministic Replay Engine
replay:
	$(PYTHON) scripts/replay_pipeline.py $(ARGS)

replay-last:
	@cid=$$(PGPASSWORD=awet psql -h localhost -p 5433 -U awet -d awet -A -t -c \
	  "SELECT correlation_id FROM audit_events WHERE source='synthetic_demo' ORDER BY created_at DESC LIMIT 1"); \
	if [ -z "$$cid" ]; then echo "No demo run in audit_events"; exit 1; fi; \
	echo "Replaying correlation_id=$$cid"; \
	$(PYTHON) scripts/replay_pipeline.py --correlation-id "$$cid" --json-report

replay-test:
	$(VENV)/bin/pytest tests/integration/test_replay_determinism.py -v

up:
	docker compose up -d

down:
	docker compose down

# List Docker containers by Compose project
stack-list:
	@$(PYTHON) scripts/list_compose_projects.py

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

# Single-command pipeline orchestrator
pipeline:
	$(PYTHON) scripts/run_pipeline.py

pipeline-full:
	$(PYTHON) scripts/run_pipeline.py --train --paper --source yfinance

# Production-grade E2E test
e2e:
	$(PYTHON) scripts/e2e_test.py

e2e-verbose:
	$(PYTHON) scripts/e2e_test.py --verbose

e2e-pytest:
	$(VENV)/bin/pytest tests/test_e2e.py -v -m e2e

# Ingest synthetic data (quick)
ingest:
	$(PYTHON) scripts/ingest_market_data.py --symbols AAPL --start 2024-01-01 --end 2024-01-03 --source synthetic

# Reddit ingestion - July 2025 (from local Pushshift dumps)
ingest-reddit-2025-07:
	@echo "📥 Ingesting Reddit data for July 2025..."
	$(PYTHON) scripts/ingest_reddit_month.py --month 2025-07 --write-db --update-features --max-items 50000
	@echo "✅ Verifying ingestion..."
	@psql -h localhost -p 5433 -U awet -d awet -t -c \
		"SELECT 'reddit_daily_mentions rows for 2025-07:', COUNT(*) FROM reddit_daily_mentions WHERE day >= '2025-07-01' AND day < '2025-08-01';"

ingest-reddit-2025-07-dry:
	@echo "🔍 Dry-run: Reddit ingestion for July 2025..."
	$(PYTHON) scripts/ingest_reddit_month.py --month 2025-07 --dry-run --max-items 10000

ingest-reddit-2025-07-full:
	@echo "📥 Full Reddit ingestion for July 2025 (no limit)..."
	$(PYTHON) scripts/ingest_reddit_month.py --month 2025-07 --write-db --update-features

# Reddit ingestion - July 2025 via API (requires credentials)
# Set: REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USERNAME, REDDIT_PASSWORD
ingest-reddit-2025-07-api:
	@echo "📥 Ingesting Reddit data for July 2025 via API..."
	$(PYTHON) scripts/ingest_reddit_month.py --month 2025-07 --use-api --write-db --update-features --max-items 50000
	@echo "✅ Verifying ingestion..."
	@psql -h localhost -p 5433 -U awet -d awet -t -c \
		"SELECT 'reddit_daily_mentions rows for 2025-07:', COUNT(*) FROM reddit_daily_mentions WHERE day >= '2025-07-01' AND day < '2025-08-01';"

ingest-reddit-2025-07-api-dry:
	@echo "🔍 Dry-run: Reddit API ingestion for July 2025..."
	$(PYTHON) scripts/ingest_reddit_month.py --month 2025-07 --use-api --dry-run --max-items 1000

# Model promotion
promote:
	$(PYTHON) scripts/promote_model.py

promote-dry:
	$(PYTHON) scripts/promote_model.py --dry-run --json

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

llm-gpu:
	$(PYTHON) scripts/verify_ollama_gpu.py

# =============================================
# LLM Tracing & Observability
# =============================================
# View last 20 LLM traces from database
llm-last:
	$(PYTHON) scripts/show_llm_traces.py -n 20

# Watch LLM traces in real-time (from logs)
llm-watch:
	@echo "Watching for LLM_TRACE log lines... (Ctrl+C to stop)"
	docker compose logs -f --tail=50 2>&1 | grep --line-buffered "LLM_TRACE"

# Watch LLM traces from database (poll every 2s)
llm-follow:
	$(PYTHON) scripts/show_llm_traces.py --follow

# Show LLM daily summary
llm-summary:
	$(PYTHON) scripts/show_llm_traces.py --summary

# Filter LLM traces by agent
llm-agent:
	@read -p "Agent name: " agent; $(PYTHON) scripts/show_llm_traces.py --agent "$$agent" -n 50

# ====================
# Trade Chain Tracing
# ====================

# Trace a trade back to its LLM prompts by correlation ID
trade-chain:
ifdef CORR
	$(PYTHON) scripts/show_trade_chain.py --correlation-id "$(CORR)"
else ifdef ORDER
	$(PYTHON) scripts/show_trade_chain.py --alpaca-order-id "$(ORDER)"
else ifdef TRADE
	$(PYTHON) scripts/show_trade_chain.py --trade-id "$(TRADE)"
else
	@echo "Usage: make trade-chain CORR=<correlation-id>"
	@echo "   or: make trade-chain ORDER=<alpaca-order-id>"
	@echo "   or: make trade-chain TRADE=<trade-id>"
endif

# Trace with JSON output (for scripting)
trade-chain-json:
ifdef CORR
	$(PYTHON) scripts/show_trade_chain.py --correlation-id "$(CORR)" --json
else ifdef ORDER
	$(PYTHON) scripts/show_trade_chain.py --alpaca-order-id "$(ORDER)" --json
else
	@echo "Usage: make trade-chain-json CORR=<correlation-id> or ORDER=<alpaca-order-id>"
endif

# Paper trading live loop
paper-buy:
	$(PYTHON) scripts/paper_trade_live.py --symbol $(SYMBOL) --qty $(QTY) --side buy

paper-sell:
	$(PYTHON) scripts/paper_trade_live.py --symbol $(SYMBOL) --qty $(QTY) --side sell

paper-loop:
	$(PYTHON) scripts/paper_trade_live.py --symbol $(SYMBOL) --qty $(QTY) --side buy --loop

# Paper trading smoke test
paper-smoke:
	$(PYTHON) scripts/paper_trade_smoke.py

# View recent execution events
trades:
	$(PYTHON) scripts/show_last_trades.py

trades-watch:
	$(PYTHON) scripts/show_last_trades.py --watch

# Order lifecycle reconciliation (legacy - uses reconcile_orders.py)
reconcile-legacy:
	$(PYTHON) scripts/reconcile_orders.py -v

reconcile-dry:
	$(PYTHON) scripts/reconcile_orders.py --dry-run -v

# Position/Portfolio reconciliation
positions:
	$(PYTHON) scripts/reconcile_positions.py

positions-watch:
	$(PYTHON) scripts/reconcile_positions.py --watch --interval 60

portfolio:
	$(PYTHON) scripts/reconcile_positions.py --portfolio

# Ops utilities
ops-watch:
	@echo "Tailing reconcile + execution logs (Ctrl+C to stop)..."
	@tail -f logs/*.log 2>/dev/null | grep -i "reconcile\|execution\|error" || true

ops-snapshot:
	$(PYTHON) scripts/ops_status.py

ops-install:
	@echo "Installing systemd unit files..."
	@sudo cp deploy/awet-ops.service /etc/systemd/system/awet-ops.service
	@sudo cp deploy/awet-ops.timer /etc/systemd/system/awet-ops.timer
	@sudo systemctl daemon-reload
	@echo "✅ Installed. Enable with: sudo systemctl enable --now awet-ops"

ops-start:
	@sudo systemctl start awet-ops

ops-stop:
	@sudo systemctl stop awet-ops

ops-logs:
	@sudo journalctl -u awet-ops -f

ops-status:
	@sudo systemctl status awet-ops --no-pager

# DB migrations
db-migrate:
	$(PYTHON) scripts/migrate_trades_brackets.py

# Exit Logic - Automatic Position Closing
exit-run:
	$(PYTHON) -m src.agents.exit_agent --once

exit-watch:
	$(PYTHON) -m src.agents.exit_agent --watch

exit-on:
	@sed -i 's/enable_exit_logic: false/enable_exit_logic: true/' config/app.yaml
	@echo "✅ Exit logic ENABLED"

exit-off:
	@sed -i 's/enable_exit_logic: true/enable_exit_logic: false/' config/app.yaml
	@echo "🚫 Exit logic DISABLED"

# =============================================
# Telegram Bot (with local Ollama)
# =============================================
telegram-bot-local:
	@echo "🤖 Starting AWET Telegram Bot with local Ollama..."
	@echo "   Model: llama3.2:1b"
	@echo "   Bot: @$$(grep TELEGRAM_BOT_TOKEN .env | cut -d'=' -f2 | cut -d':' -f1 || echo 'unknown')"
	@echo ""
	cd services/telegram-bot && \
	export $$(grep -v '^#' ../../.env | xargs) && \
	export KB_QUERY_BASE_URL=http://localhost:8000 && \
	export METRICS_PORT=9201 && \
	export STATE_FILE=/tmp/awet-telegram-bot-state.json && \
	python -m telegram_bot.main

telegram-bot-test:
	@export $$(grep -v '^#' .env | xargs) && python3 test_telegram_bot.py

# =============================================
# PnL Reporting (Performance Reporting v1)
# =============================================
pnl-today:
	$(PYTHON) scripts/daily_pnl_report.py

pnl-week:
	$(PYTHON) scripts/daily_pnl_report.py --days 7

pnl-watch:
	$(PYTHON) scripts/daily_pnl_report.py --watch

pnl-save:
	$(PYTHON) scripts/daily_pnl_report.py --days 7 --save

# =============================================
# Order Reconciliation + PnL
# =============================================
# Run single reconciliation cycle (local)
reconcile:
	$(PYTHON) scripts/reconcile_scheduler.py --once

# Continuous reconciliation during market hours (local)
reconcile-watch:
	$(PYTHON) scripts/reconcile_scheduler.py --watch

# Continuous reconciliation with market-closed override (local)
reconcile-watch-force:
	FORCE_RUN_WHEN_MARKET_CLOSED=1 $(PYTHON) scripts/reconcile_scheduler.py --watch

# Force end-of-day job (reconcile + positions + PnL)
eod:
	$(PYTHON) scripts/reconcile_scheduler.py --eod

# ---- Docker-based reconciliation (recommended for production) ----
# Start reconciler service in Docker
reconciler-up:
	docker compose up -d awet-reconciler
	@echo "✅ Reconciler started. View logs: docker logs -f awet-reconciler"

# Stop reconciler service
reconciler-down:
	docker compose stop awet-reconciler
	@echo "🛑 Reconciler stopped"

# View reconciler logs
reconciler-logs:
	docker logs -f awet-reconciler

# Run EOD job in Docker
eod-docker:
	docker compose run --rm awet-eod

# Approval gate controls (GAP 6.2)
approve:
	@mkdir -p .tmp && touch .tmp/APPROVE_EXECUTION
	@echo "✅ Execution APPROVED - .tmp/APPROVE_EXECUTION created"

revoke:
	@rm -f .tmp/APPROVE_EXECUTION
	@echo "🚫 Execution REVOKED - .tmp/APPROVE_EXECUTION removed"

# =============================================
# Paper Trading Controls
# =============================================
# Verify Alpaca credentials are configured
verify-alpaca:
	$(PYTHON) scripts/verify_alpaca_env.py

# Show current paper trading status
paper-status:
	@$(PYTHON) scripts/toggle_paper_mode.py

# Enable paper trading (set dry_run=false, create approval file, restart agents)
paper-on:
	@echo "🚀 Enabling paper trading..."
	@$(PYTHON) scripts/toggle_paper_mode.py --on
	@mkdir -p .tmp && touch .tmp/APPROVE_EXECUTION
	@echo "✅ Approval file created"
	@pkill -f "src.agents" 2>/dev/null || true
	@echo "🔄 Agents stopped (will restart on next demo/run)"
	@echo ""
	@echo "✅ Paper trading ENABLED. Run 'make demo' to test."

# Disable paper trading (set dry_run=true, remove approval file)
paper-off:
	@echo "🛑 Disabling paper trading..."
	@$(PYTHON) scripts/toggle_paper_mode.py --off
	@rm -f .tmp/APPROVE_EXECUTION
	@echo "🚫 Approval file removed"
	@pkill -f "src.agents" 2>/dev/null || true
	@echo "🔄 Agents stopped"
	@echo ""
	@echo "🛑 Paper trading DISABLED. All trades will be blocked."

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

# =============================================
# Training Data Pipeline
# =============================================
# Full pipeline: Load CSV → Build Features → Train Model
# SINGLE SOURCE OF TRUTH: config/universe.csv

# Universe file (auto-generated by make universe)
UNIVERSE_FILE := config/universe.csv

# Use universe.csv if exists (no hardcoded fallback)
TRAIN_SYMBOLS ?= $(shell if [ -f $(UNIVERSE_FILE) ]; then tail -n +2 $(UNIVERSE_FILE) | cut -d',' -f2 | head -500 | tr '\n' ',' | sed 's/,$$//'; fi)
TRAIN_DATA_DIR ?= /home/kironix/train/poligon

# Universe Builder - select top N symbols by liquidity
# Usage: make universe N=500 DAYS=180 MODE=daily
N ?= 500
DAYS ?= 180
MIN_COVERAGE ?= 0.90
MIN_VOLUME ?= 200000
MODE ?= daily

universe:
	@echo "🌍 Building universe: Top $(N) by liquidity over $(DAYS) days..."
	@echo "   Filters: coverage >= $(MIN_COVERAGE), volume >= $(MIN_VOLUME), mode=$(MODE)"
	$(PYTHON) scripts/build_universe.py --top $(N) --days $(DAYS) --min-coverage $(MIN_COVERAGE) --min-volume $(MIN_VOLUME) --mode $(MODE)

universe-dry:
	@echo "🔍 Preview universe selection..."
	$(PYTHON) scripts/build_universe.py --top $(N) --days $(DAYS) --min-coverage $(MIN_COVERAGE) --min-volume $(MIN_VOLUME) --mode $(MODE) --dry-run

universe-daily:
	@echo "🌍 Building universe from daily data..."
	$(PYTHON) scripts/build_universe.py --top $(N) --days $(DAYS) --mode daily

# Preset targets for common scales
universe-small:
	$(MAKE) universe N=50 DAYS=30

universe-medium:
	$(MAKE) universe N=200 DAYS=60

universe-large:
	$(MAKE) universe N=500 DAYS=60

universe-xlarge:
	$(MAKE) universe N=1000 DAYS=90

# Explicit requested presets
universe-50-daily:
	$(MAKE) universe N=50 DAYS=60 MODE=daily

universe-500-daily:
	$(MAKE) universe N=500 DAYS=180 MODE=daily

# Step 1: Load Polygon CSV files into TimescaleDB
# Now reads from universe.csv by default
load-market-data:
	@echo "📥 Loading market data (from universe.csv)..."
	$(PYTHON) scripts/load_market_data.py

load-market-data-symbols:
	@echo "📥 Loading specific symbols..."
	$(PYTHON) scripts/load_market_data.py --symbols $(TRAIN_SYMBOLS)

load-market-data-all:
	@echo "📥 Loading ALL symbols (this may take a while)..."
	$(PYTHON) scripts/load_market_data.py --all-symbols

load-market-data-dry:
	@echo "📋 Preview: checking data files..."
	$(PYTHON) scripts/load_market_data.py --dry-run

# Step 2: Build features from raw market data
build-features:
	@echo "🔧 Building TFT features from market data..."
	$(PYTHON) scripts/build_features.py

build-features-all:
	@echo "🔧 Building features for ALL symbols..."
	$(PYTHON) scripts/build_features.py --all-symbols

# Step 3: Train TFT baseline model
train-tft:
	@echo "🧠 Training TFT baseline model..."
	$(PYTHON) scripts/train_tft_baseline.py

train-tft-gpu:
	@echo "🧠 Training TFT with GPU acceleration..."
	$(PYTHON) scripts/train_tft_baseline.py --gpu

train-tft-dry:
	@echo "🔍 Dry run: validating training data..."
	$(PYTHON) scripts/train_tft_baseline.py --dry-run

# Combined: Full training pipeline (load → features → train)
# Supports DRY_RUN=1 to validate without training
DRY_RUN ?= 0
train-baseline:
	@echo ""
	@echo "═══════════════════════════════════════════════════════════"
	@echo "  FULL TRAINING PIPELINE: Data → Features → Model"
	@echo "═══════════════════════════════════════════════════════════"
ifeq ($(DRY_RUN),1)
	@echo "  MODE: DRY RUN (validation only)"
else
	@echo "  MODE: FULL TRAINING"
endif
	@echo "  Symbol source: universe.csv"
	@echo ""
	docker compose up -d timescaledb
	@sleep 2
ifeq ($(DRY_RUN),1)
	@echo ""
	@echo "🔍 Dry run: validating data and ticker count..."
	$(PYTHON) scripts/train_tft_baseline.py --dry-run
	@echo ""
	@echo "═══════════════════════════════════════════════════════════"
	@echo "  ✅ Dry run complete - see ticker count above"
	@echo "═══════════════════════════════════════════════════════════"
else
	@echo ""
	@echo "📥 Step 1/3: Loading market data..."
	$(PYTHON) scripts/load_market_data.py
	@echo ""
	@echo "🔧 Step 2/3: Building features..."
	$(PYTHON) scripts/build_features.py
	@echo ""
	@echo "🧠 Step 3/3: Training model..."
	$(PYTHON) scripts/train_tft_baseline.py
	@echo ""
	@echo "═══════════════════════════════════════════════════════════"
	@echo "  ✅ Training pipeline complete!"
	@echo "═══════════════════════════════════════════════════════════"
endif

# Validate training data without running pipeline
train-check:
	@echo "🔍 Validating training data..."
	$(PYTHON) scripts/load_market_data.py --dry-run
	$(PYTHON) scripts/train_tft_baseline.py --dry-run

# Legacy training targets (use src.ml.train_tft)
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

# =============================================
# System Diagnostics & Portals
# =============================================
# Comprehensive system check
doctor:
	@chmod +x scripts/awet_doctor.sh
	@./scripts/awet_doctor.sh

doctor-json:
	@chmod +x scripts/awet_doctor.sh
	@./scripts/awet_doctor.sh --json

doctor-quiet:
	@chmod +x scripts/awet_doctor.sh
	@./scripts/awet_doctor.sh --quiet

# Print all portal URLs
portals:
	@echo ""
	@echo "═══════════════════════════════════════════════════════════"
	@echo "  AWET Portal URLs"
	@echo "═══════════════════════════════════════════════════════════"
	@echo ""
	@echo "  📊 Grafana:         http://localhost:3000  (admin/admin)"
	@echo "  📈 Prometheus:      http://localhost:9090"
	@echo "  🚨 Alertmanager:    http://localhost:9093"
	@echo "  📬 Kafka UI:        http://localhost:8080"
	@echo "  📋 Schema Registry: http://localhost:8081"
	@echo "  🤖 SuperAGI:        http://localhost:3001"
	@echo "  🧠 Ollama:          http://localhost:11434"
	@echo "  🗄️  TimescaleDB:     postgresql://awet:awet@localhost:5433/awet"
	@echo ""
	@echo "  Agent Endpoints (when running):"
	@echo "    DataIngestion:    http://localhost:8001/health"
	@echo "    FeatureEng:       http://localhost:8002/health"
	@echo "    Prediction:       http://localhost:8003/health"
	@echo "    Risk:             http://localhost:8004/health"
	@echo "    Execution:        http://localhost:8005/health"
	@echo "    Watchtower:       http://localhost:8006/health"
	@echo ""

# Monitoring access guide (URLs only)
monitoring-access:
	@echo ""
	@echo "═══════════════════════════════════════════════════════════"
	@echo "  Monitoring Access URLs"
	@echo "═══════════════════════════════════════════════════════════"
	@echo ""
	@echo "  📊 Grafana:        http://localhost:3000  (admin/admin)"
	@echo "  📈 Prometheus:     http://localhost:9090"
	@echo "  🚨 Alertmanager:   http://localhost:9093"
	@echo "  📬 Kafka UI:       http://localhost:8080"
	@echo "  📊 Agent metrics:  http://localhost:8001/metrics .. http://localhost:8006/metrics (if agents running)"
	@echo ""

# Monitoring status checks
monitoring-status:
	@echo ""
	@echo "═══════════════════════════════════════════════════════════"
	@echo "  Monitoring Status"
	@echo "═══════════════════════════════════════════════════════════"
	@echo ""
	@docker compose ps
	@echo ""
	@echo "Health checks:"
	@printf "  Grafana:       "
	@curl -fsS http://localhost:3000/api/health >/dev/null && echo "PASS" || echo "FAIL"
	@printf "  Prometheus:    "
	@curl -fsS http://localhost:9090/-/healthy >/dev/null && echo "PASS" || echo "FAIL"
	@printf "  Alertmanager:  "
	@curl -fsS http://localhost:9093/-/healthy >/dev/null && echo "PASS" || echo "FAIL"
	@printf "  Kafka UI:      "
	@curl -fsS http://localhost:8080/ >/dev/null && echo "PASS" || echo "FAIL"
	@echo ""
	@echo "Checking Prometheus targets (http://localhost:9090/api/v1/targets)"
	@curl -fsS http://localhost:9090/api/v1/targets | $(PYTHON) - <<'PY'
	import json
	import sys

	try:
	    data = json.load(sys.stdin)
	except json.JSONDecodeError:
	    print("❌ Failed to parse Prometheus targets response")
	    sys.exit(1)

	targets = data.get("data", {}).get("activeTargets", [])
	if not targets:
	    print("⚠️  No targets returned")
	    sys.exit(0)

	up = []
	down = []
	for t in targets:
	    job = t.get("labels", {}).get("job", "unknown")
	    inst = t.get("labels", {}).get("instance", "")
	    health = (t.get("health") or "unknown").lower()
	    label = f"{job} ({inst})" if inst else job
	    if health == "up":
	        up.append(label)
	    else:
	        down.append(f"{label} [{health}]")

	print("Targets UP:")
	for item in up:
	    print(f"  ✅ {item}")

	print("Targets DOWN:")
	if down:
	    for item in down:
	        print(f"  ❌ {item}")
	else:
	    print("  ✅ none")
	PY

# Verify Reddit data alignment
REDDIT_COMMENTS_2025_07 ?= /home/kironix/train/reddit/comments/RC_2025-07.zst
REDDIT_SUBMISSIONS_2025_07 ?= /home/kironix/train/reddit/submissions/RS_2025-07.zst

ingest-reddit-july2025:
	@echo "📥 Ingesting Reddit dumps for 2025-07..."
	$(PYTHON) scripts/ingest_reddit_zst.py --month 2025-07 \
		--file $(REDDIT_COMMENTS_2025_07) \
		--file $(REDDIT_SUBMISSIONS_2025_07)

verify-reddit:
	$(PYTHON) scripts/verify_reddit_alignment.py --db-only

verify-reddit-all:
	$(PYTHON) scripts/verify_reddit_alignment.py --no-filter

# ============================================================================
# SSD Data Linking - Polygon & Reddit from external drive
# ============================================================================
ssd-ls:
	@./scripts/ssd_ls.sh

link-reddit:
ifndef MONTH
	$(error MONTH is required. Usage: make link-reddit MONTH=2025-07)
endif
	@./scripts/link_reddit_month.sh $(MONTH)

link-polygon:
ifndef MONTH
	$(error MONTH is required. Usage: make link-polygon MONTH=2025-01)
endif
	@./scripts/link_polygon_month.sh $(MONTH)

find-data-roots:
	@$(PYTHON) scripts/find_data_roots.py

