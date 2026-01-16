# AWET - Automated Trading Platform

> **Institutional-grade, audit-ready paper trading platform**

A 3-layer architecture separating Directives (what to do), Orchestration (decision making), and Execution (deterministic processing). See [AGENTS.md](AGENTS.md) for the full architectural specification.

## 🚀 Quick Start (Fresh Machine)

```bash
# 1. Clone and setup
git clone <repo-url> && cd Awet
make setup

# 2. Start the entire system with ONE command
./scripts/awet_start.sh

# 3. Check system health
.venv/bin/python scripts/awet_health_check.py

# 4. Run end-to-end demo
make demo
```

## 📅 Daily Operations

One-command scripts to start, stop, and health-check the entire AWET + SuperAGI system.

### Start Everything

```bash
# Start all services and wait until healthy
./scripts/awet_start.sh

# Start without waiting for health checks
./scripts/awet_start.sh --no-wait
```

This starts:
- AWET infrastructure (Kafka, TimescaleDB, Redis, Prometheus, Grafana)
- SuperAGI (backend, celery, GUI, proxy)
- Waits for all services to be healthy
- Prints a "SYSTEM READY" message with URLs

### Stop Everything

```bash
# Graceful shutdown
./scripts/awet_stop.sh

# Shutdown and remove volumes (DESTRUCTIVE - resets all data)
./scripts/awet_stop.sh --clean
```

### Health Check

```bash
# Human-readable output
.venv/bin/python scripts/awet_health_check.py

# JSON output (for scripts/monitoring)
.venv/bin/python scripts/awet_health_check.py --json
```

Checks:
- ✅ TimescaleDB connectivity
- ✅ Kafka availability
- ✅ SuperAGI API responding
- ✅ All 4 agents exist (Orchestrator, Night Trainer, Morning Deployer, Trade Watchdog)
- ✅ Ollama LLM service

Exit code 0 = healthy, non-zero = problems detected.

### Integration with Cron Scheduling

The agent scheduling (cron) assumes the system is already running. A typical workflow:

```bash
# After reboot: start the system
./scripts/awet_start.sh

# Cron jobs (already configured) will trigger agents automatically:
#   02:00 - Night Trainer
#   08:30 - Morning Deployer
#   */15 9-16 - Trade Watchdog

# Check health periodically
.venv/bin/python scripts/awet_health_check.py
```

You can add the startup script to `@reboot` in cron if you want auto-start after reboot:

```bash
# Add to crontab -e:
@reboot cd /home/kironix/Awet && ./scripts/awet_start.sh >> logs/startup.log 2>&1
```

## 📋 Prerequisites

- **Python 3.11+**
- **Docker + Docker Compose v2**
- **16GB+ RAM** (for Kafka, TimescaleDB, and ML models)
- **(Optional)** NVIDIA GPU for faster TFT training

## 🛠️ Full Setup Guide

### 1. Environment Setup

```bash
# Create virtual environment and install dependencies
make setup

# Copy environment template and configure API keys
cp .env.example .env
# Edit .env with your Polygon API key, etc.
```

### 2. Start Infrastructure

```bash
# Start all services (Kafka, TimescaleDB, Schema Registry, Prometheus, Grafana)
make up

# Verify all services are healthy
make health

# View service logs
docker compose logs -f kafka
docker compose logs -f timescaledb
```

### 3. Backfill Historical Data

```bash
# Backfill Polygon market data
python execution/backfill_polygon.py \
  --data-dir /path/to/polygon/data \
  --symbols AAPL,MSFT,NVDA \
  --start-date 2024-01-01

# Resume interrupted backfill
python execution/backfill_polygon.py \
  --data-dir /path/to/polygon/data \
  --resume

# Backfill Reddit sentiment data
python execution/backfill_reddit.py \
  --submissions-dir /path/to/reddit/submissions \
  --resume
```

### 4. Train TFT Model

```bash
# Train TFT model on backfilled data
python -m src.ml.train train \
  --symbol AAPL \
  --lookback 120 \
  --horizons 30,45,60 \
  --epochs 50

# Export to ONNX format
python -m src.ml.train export --model-id <model_id>

# Promote model to production (green)
python -m src.ml.train promote --model-id <model_id>

# List all registered models
python -m src.ml.train list
```

### 5. Run Pipeline

```bash
# Start all agents (streaming mode)
make agents

# Or run individual agents
python -m src.agents.data_ingestion
python -m src.agents.feature_engineering
python -m src.agents.prediction_agent
python -m src.agents.risk_agent
python -m src.agents.execution_agent
```

### 6. Live Daily Ingestion (FREE - No API Key)

For daily trading workflows using **Yahoo Finance** (free, no API key required):

```bash
# Install yfinance if not already installed
pip install yfinance

# Fetch latest daily bars (dry run - preview only)
make live-ingest-dry

# Fetch latest bars and publish to Kafka
make live-ingest

# Fetch latest bars AND trigger full pipeline
make live-pipeline

# Custom symbols
python -m execution.run_live_ingestion --symbols AAPL,TSLA,NVDA

# Verify data arrived
docker exec timescaledb psql -U awet -c \
  "SELECT symbol, ts, source FROM audit_events WHERE source='live_ingestion' ORDER BY ts DESC LIMIT 5;"
```

**When to use:**
- Daily after market close (e.g., 4:30 PM ET)
- On-demand data refresh before running predictions
- Testing pipeline end-to-end with real market data

**Limitations of yfinance:**
- Data delayed 15-20 minutes
- Intraday data limited to 60 days
- 1-minute data limited to 7 days

### 7. Execution Approval Gate


```bash
# APPROVE execution (enables paper trading)
make approve

# REVOKE execution (blocks all trades)
make revoke
```

## 📊 Observability

| Service | URL | Description |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | Dashboards (user: admin, pass: admin) |
| Prometheus | http://localhost:9090 | Metrics queries |
| SuperAGI | http://localhost:3001 | Autonomous orchestration GUI |

### Key Metrics

- `events_processed_total` — Throughput by agent
- `event_latency_seconds` — Processing latency histogram
- `kafka_consumer_lag_total` — Consumer group lag
- `events_failed_total` — Error rate

## 🧪 Testing

```bash
# Run all tests
make test

# Run unit tests only
pytest tests/unit/

# Run integration tests (requires infrastructure)
pytest tests/integration/

# Run with coverage
pytest --cov=src tests/

# Run SuperAGI e2e smoke test (requires SuperAGI running)
make e2e-superagi

# Quick SuperAGI connectivity check
make e2e-superagi-quick
```

### Pytest Markers

| Marker | Description |
|--------|-------------|
| `slow` | Tests that take a long time (e.g., LLM calls) |
| `e2e` | End-to-end tests requiring external services |

Skip slow tests: `pytest -m "not slow"`

## ✅ End-to-end AWET demo (CLI and SuperAGI)

### A. CLI demo

```bash
make setup  # first time only
make up
make e2e-demo
```

Expected output example:

```
E2E demo OK: 3 paper trades, 42 audit events.
```

DB tables touched:
- `paper_trades`
- `audit_events`

### B. SuperAGI demo

```bash
make up
make superagi-up
```

Then:
1. Open http://localhost:3001
2. Configure LLM model: **Settings** → **Models** → **Add Model** (e.g., OpenAI gpt-3.5-turbo)
3. Select **AWET Orchestrator**
4. Click **New Run** and paste the demo goal:

```
Run the AWET end-to-end demo:
1. Call awet_run_demo
2. Call awet_check_pipeline_health
3. Summarize the results in 3 bullet points
```

5. Watch the **Activity Feed** for tool calls (`awet_run_demo`, `awet_check_pipeline_health`).
6. Check the **APM** tab for runs, calls, and tokens.

### C. End-to-end SuperAGI smoke test

```bash
# Prerequisites
make up                    # Start AWET stack
make superagi-up           # Start SuperAGI

# The e2e test requires:
# 1. SuperAGI API key (from UI → Settings → API Keys)
# 2. An LLM model configured (Ollama is auto-configured via docker-compose)

# Quick connectivity test (no LLM required, ~10s)
export SUPERAGI_API_KEY="your-api-key"
make e2e-superagi-quick

# Full e2e test (requires LLM, ~10-15 min with CPU Ollama)
make e2e-superagi

# Or run with custom timeout (default: 600s)
E2E_TIMEOUT_SECONDS=900 make e2e-superagi
```

**LLM Configuration:**

The docker-compose automatically configures SuperAGI to use Ollama running in Docker:
- Model: `llama3.2:1b` at `http://ollama:11434/v1`
- The model is pulled automatically when Ollama starts

⚠️ **Performance Warning:** Ollama running on CPU takes ~10-12 minutes per LLM response.
For faster testing:
- Use GPU: Add `--gpus all` to the Ollama container in docker-compose.yml
- Use cloud LLM: Set `OPENAI_API_KEY` in `.env` and configure via SuperAGI UI

**What success looks like:**
- pytest exits with status 0
- Output shows `✅ Run created: run_id=X`, `✅ Run completed`, `✅ E2E smoke test PASSED`
- SuperAGI UI shows a completed run for "AWET Orchestrator"
- At least one AWET tool was invoked (awet_run_demo, awet_check_pipeline_health, etc.)

Notes:
- CLI proves the deterministic pipeline is healthy.
- SuperAGI proves the LLM orchestration is calling tools correctly.

## 🏗️ Architecture

```
Layer 1: Directives (directives/)
    ↓ Read by
Layer 2: Orchestration (SuperAGI / Copilot / GPT)
    ↓ Calls
Layer 3: Execution (src/agents/, execution/)
```

### Data Flow

```
market.raw → market.engineered → predictions.tft → risk.approved → execution.completed
                                                 → risk.rejected
                                                               → execution.blocked
```

### Key Directories

- `directives/` — SOPs in Markdown
- `execution/` — Deterministic scripts (short-lived)
- `src/agents/` — Long-running services
- `src/ml/` — TFT model training and inference
- `config/` — YAML configurations
- `db/` — TimescaleDB schema

## 🔧 Configuration

All configs in `config/` support environment variable overrides:

| File | Key Env Vars |
|------|--------------|
| `app.yaml` | `APPROVAL_FILE_PATH` |
| `kafka.yaml` | `KAFKA_BOOTSTRAP_SERVERS` |
| `limits.yaml` | `RISK_MAX_POSITION_PCT`, `RISK_MAX_DAILY_LOSS_PCT` |
| `llm.yaml` | `LLM_BASE_URL`, `LLM_MODEL` |

## 🤖 Using SuperAGI as Orchestrator

SuperAGI provides an autonomous agent UI for orchestrating the AWET pipeline. This section explains how to set up the "AWET Orchestrator" agent.

### Step 1: Start Infrastructure

```bash
# Start Kafka, TimescaleDB, and other services
make up

# Wait for services to be healthy (~30s)
make health
```

### Step 2: Start SuperAGI

```bash
# Build and start SuperAGI containers
make superagi-build && make superagi-up

# SuperAGI GUI will be available at http://localhost:3001
```

### Step 3: Start the Tool Gateway

The Tool Gateway exposes AWET scripts as HTTP endpoints that SuperAGI can call:

```bash
# Start Tool Gateway (runs on port 8200)
make tool-gateway

# Or run in background
make tool-gateway-bg
```

### Step 4: Register AWET Tools

Register all AWET tools with SuperAGI:

```bash
make superagi-register-tools

# To see available tools without registering
make superagi-list-tools
```

This registers the following tools:

| Tool | Description |
|------|-------------|
| `awet_read_directive` | Read a directive (SOP) from directives/ |
| `awet_run_backfill` | Backfill historical data (Polygon, Reddit, or yfinance) |
| `awet_train_model` | Train a new TFT model |
| `awet_promote_model` | Promote model to green (production) |
| `awet_run_demo` | Run end-to-end pipeline demo |
| `awet_check_pipeline_health` | Check health of all agents |
| `awet_check_kafka_lag` | Check Kafka consumer lag |
| `awet_query_audit_trail` | Query audit trail events |
| `awet_approve_execution` | Approve trade execution |
| `awet_revoke_execution` | Revoke trade execution |
| `awet_run_live_ingestion` | Run live daily ingestion (yfinance) |

### Step 5: Create the AWET Orchestrator Agent

Programmatic registration (recommended):

```bash
SUPERAGI_BASE_URL=http://localhost:3001/api \
SUPERAGI_API_KEY=YOUR_TOKEN_HERE \
make superagi-register-awet-agent
```

Then open http://localhost:3001 → Agents → **AWET Orchestrator** → Run.

### SuperAGI Agent Team (Optional)

You can also register a team of specialized SuperAGI agents:

- **AWET Orchestrator** — interactive, manual orchestration
- **AWET Night Trainer** — nightly backfill + training
- **AWET Morning Deployer** — morning health check + smoke test
- **AWET Trade Watchdog** — periodic health monitoring

Register them all at once:

```bash
export SUPERAGI_API_KEY=...
export SUPERAGI_BASE_URL=http://localhost:3001/api
make superagi-register-agents
```

Scheduling can be done via the SuperAGI UI or programmatically using cron.

### 📅 Cron-Based Agent Scheduling

For automated, headless agent scheduling (no SuperAGI UI required), use the scheduler script:

```bash
# Run the orchestrator agent
python scripts/schedule_agents.py orchestrator

# Run night trainer (typically at 2:00 AM)
python scripts/schedule_agents.py night_trainer

# Run morning deployer (typically at 8:30 AM)
python scripts/schedule_agents.py morning_deployer

# Run trade watchdog (every 15 min during market hours)
python scripts/schedule_agents.py trade_watchdog

# Dry run (preview what would happen)
python scripts/schedule_agents.py orchestrator --dry-run

# JSON output for machine parsing
python scripts/schedule_agents.py orchestrator --json
```

**Cron Examples** (add to `crontab -e`):

```bash
# Night Trainer - 2:00 AM on weekdays
0 2 * * 1-5 cd /home/kironix/Awet && /home/kironix/Awet/.venv/bin/python scripts/schedule_agents.py night_trainer >> /var/log/awet/night_trainer.log 2>&1

# Morning Deployer - 8:30 AM on weekdays
30 8 * * 1-5 cd /home/kironix/Awet && /home/kironix/Awet/.venv/bin/python scripts/schedule_agents.py morning_deployer >> /var/log/awet/morning_deployer.log 2>&1

# Trade Watchdog - Every 15 min during market hours (9am-4pm ET, Mon-Fri)
*/15 9-16 * * 1-5 cd /home/kironix/Awet && /home/kironix/Awet/.venv/bin/python scripts/schedule_agents.py trade_watchdog >> /var/log/awet/watchdog.log 2>&1

# Full pipeline - 6:00 AM on weekdays
0 6 * * 1-5 cd /home/kironix/Awet && /home/kironix/Awet/.venv/bin/python scripts/schedule_agents.py orchestrator >> /var/log/awet/orchestrator.log 2>&1
```

**Environment Variables:**

| Variable | Description | Default |
|----------|-------------|---------|
| `SUPERAGI_API_KEY` | SuperAGI API key (required) | - |
| `SUPERAGI_BASE_URL` | SuperAGI API endpoint | `http://localhost:8100` |
| `AWET_ORCHESTRATOR_AGENT_ID` | Override orchestrator agent ID | `3` |
| `AWET_NIGHT_TRAINER_AGENT_ID` | Override night trainer agent ID | `4` |
| `AWET_MORNING_DEPLOYER_AGENT_ID` | Override morning deployer agent ID | `5` |
| `AWET_TRADE_WATCHDOG_AGENT_ID` | Override trade watchdog agent ID | `6` |

### Manual UI Scheduling

1. Open **http://localhost:3001** in your browser
2. Go to **Agents** → **New Agent**
3. Configure the agent:

   | Field | Value |
   |-------|-------|
   | **Name** | `AWET Orchestrator` |
   | **Description** | `Orchestrates the AWET trading pipeline using deterministic tools` |
   | **Model** | Select your LLM (local Llama or OpenAI) |
   | **Tools** | Attach the **AWET Trading Pipeline** toolkit |

4. Set the agent **Goal**. Example goals:

   **Daily Pipeline Run:**
   ```
   Run the AWET trading pipeline:
   1. Check pipeline health
   2. Backfill latest data from Yahoo Finance
   3. Query the audit trail to verify data ingestion
   4. If no recent model, train a new TFT model
   5. Promote the trained model to green
   6. Approve execution
   7. Run the demo to verify the pipeline
   8. Show me the latest audit summary
   ```

   **Quick Health Check:**
   ```
   Check the health of the AWET pipeline and report any issues.
   Also check Kafka consumer lag.
   ```

   **Train and Deploy Model:**
   ```
   Train a new TFT model on AAPL,MSFT,NVDA with 50 epochs.
   Once trained, promote it to green and run a demo to verify.
   ```

5. Click **Create Agent** and then **Run**

### Step 6: Monitor in APM Dashboard

- View agent runs in the SuperAGI **Runs** tab
- Each tool call is logged with correlation_id
- Check Grafana at http://localhost:3000 for metrics:
  - `tool_gateway_requests_total` - Tool invocations
  - `tool_gateway_latency_seconds` - Execution time
  - `tool_gateway_errors_total` - Failures

### Quick Start (All Steps)

```bash
# 1. Start everything
make up
make superagi-build && make superagi-up

# 2. In a separate terminal, start the Tool Gateway
make tool-gateway

# 3. Register tools (wait for SuperAGI to be ready)
sleep 30
make superagi-register-tools

# 4. Open http://localhost:3001 and create your agent
```

### Available AWET Tools (Legacy Direct Mode)

The tools can also be used directly in SuperAGI containers (without the Tool Gateway) via mounted scripts:

```
awet_read_directive      - Read pipeline directives
awet_generate_plan       - Generate execution plan (LLM-powered)
awet_explain_run         - Explain pipeline run (LLM-powered)
```

## ⏰ Agent Scheduling

Run SuperAGI agents automatically via cron without needing the web UI.

### Prerequisites

1. Docker stack running: `make up`
2. SuperAGI running: `make superagi-up`
3. `SUPERAGI_API_KEY` set in environment or `.env`

### CLI Usage

```bash
# See available commands
python scripts/schedule_agents.py --help

# Dry-run (shows what would happen, no API call)
python scripts/schedule_agents.py night_trainer --dry-run

# Actually trigger each agent
python scripts/schedule_agents.py night_trainer
python scripts/schedule_agents.py morning_deployer
python scripts/schedule_agents.py trade_watchdog
python scripts/schedule_agents.py orchestrator
```

### Cron Setup

Add to your crontab (`crontab -e`):

```bash
# Environment - adjust path as needed
AWET_DIR=/home/kironix/Awet
SUPERAGI_API_KEY=your-api-key-here

# Night Trainer – 2:00 AM on weekdays
0 2 * * 1-5  cd $AWET_DIR && .venv/bin/python scripts/schedule_agents.py night_trainer >> logs/night_trainer.log 2>&1

# Morning Deployer – 8:30 AM on weekdays
30 8 * * 1-5 cd $AWET_DIR && .venv/bin/python scripts/schedule_agents.py morning_deployer >> logs/morning_deployer.log 2>&1

# Trade Watchdog – every 15 min during market hours (9:15 AM - 4:00 PM ET)
*/15 9-16 * * 1-5 cd $AWET_DIR && .venv/bin/python scripts/schedule_agents.py trade_watchdog >> logs/trade_watchdog.log 2>&1
```

### Agent Schedule Summary

| Agent | Schedule | Purpose |
|-------|----------|---------|
| **Night Trainer** | 2:00 AM weekdays | Backfill data, train models |
| **Morning Deployer** | 8:30 AM weekdays | Health check, promote models |
| **Trade Watchdog** | Every 15 min (market hours) | Monitor pipeline, alert on issues |
| **Orchestrator** | Manual / on-demand | Full pipeline run |

### Verify It Works

1. Trigger manually: `python scripts/schedule_agents.py night_trainer`
2. Open SuperAGI UI → **APM** tab
3. You should see a new run for "AWET Night Trainer"

## 🔒 Safety

- **Paper trading only** — No real broker integration
- **Approval gate** — Execution requires explicit approval file
- **Risk controls** — Position limits, daily loss limits, kill switch
- **Audit trail** — Every decision logged to TimescaleDB
- **Idempotency** — All operations safe to retry

## 📚 Additional Resources

- [AGENTS.md](AGENTS.md) — Full architectural specification
- [GAP_AUDIT_REPORT.md](GAP_AUDIT_REPORT.md) — Implementation status

## 📈 Monitoring & Grafana

Start the monitoring stack:

```bash
make up
```

Open dashboards:

- Grafana: http://localhost:3000 (default login: admin / admin)
- Prometheus: http://localhost:9090

Main dashboard:

- **AWET – Pipeline Health** (auto-provisioned from Grafana provisioning)

What the panels mean:

- **Events/sec by Topic**: Throughput per Kafka topic (e.g., market.raw, market.engineered, predictions, risk, execution).
- **End-to-End Latency (p50/p95/p99)**: Pipeline processing latency percentiles.
- **Consumer Lag by Group**: Kafka lag per consumer group (watch feature/prediction groups).
- **Risk Decisions/sec**: Approved vs rejected risk outcomes.
- **Last Prediction Timestamp**: When the prediction agent last emitted a prediction.
- **Model Version In Use**: Current model version label from the prediction agent.

Red flags:

- **Zero events/sec** while the pipeline is running.
- **High or rising consumer lag** (agents falling behind).
- **Spikes in errors/sec** or latency p99.
- **Risk rejections spike** (risk gate blocking most trades).

## ⚡ SuperAGI Latency Notes

See [PERF.md](PERF.md) for LLM latency checks, fast-run recipes, and tool timing logs.
- [directives/trading_pipeline.md](directives/trading_pipeline.md) — Trading pipeline SOP

## License

Proprietary — All rights reserved.
