# AWET Project Status Report

> **Generated:** January 17, 2026  
> **Purpose:** Full audit of implemented vs missing components

---

## Executive Summary

| Subsystem | Status | Completion |
|-----------|--------|------------|
| **Infrastructure** | ✅ Complete | 100% |
| **Data Ingestion** | ✅ Complete | 95% |
| **Feature Engineering** | ✅ Complete | 90% |
| **Model Training** | ✅ Complete | 90% |
| **Inference** | ⚠️ Partial | 75% |
| **Risk Management** | ✅ Complete | 90% |
| **Execution** | ✅ Complete | 95% |
| **Reconciliation** | ✅ Complete | 95% |
| **Monitoring** | ⚠️ Partial | 70% |
| **Orchestration** | ⚠️ Partial | 60% |
| **Documentation** | ⚠️ Partial | 75% |
| **Security/Auth** | ⚠️ Partial | 30% |

**Overall Project Completion: ~82%**

---

## Current Status Snapshot (Jan 17, 2026)

### Implemented and Working

- Universe builder and CSV source-of-truth: [scripts/build_universe.py](../scripts/build_universe.py)
- Market data loader (universe.csv by default): [scripts/load_market_data.py](../scripts/load_market_data.py)
- Feature builder (reddit_mentions column, universe default): [scripts/build_features.py](../scripts/build_features.py)
- Training baseline (dry-run + scale guardrails): [scripts/train_tft_baseline.py](../scripts/train_tft_baseline.py)
- Reddit verification + DB ingestion: [scripts/verify_reddit_alignment.py](../scripts/verify_reddit_alignment.py)
- Diagnostics: [scripts/awet_doctor.sh](../scripts/awet_doctor.sh)
- API key auth for agents: [src/agents/base_agent.py](../src/agents/base_agent.py)

### Implemented but Not Verified

- Prediction/inference pipeline (no rows in `predictions_tft`): [src/agents/time_series_prediction.py](../src/agents/time_series_prediction.py)
- SuperAGI integration in production flow: [superagi/](../superagi/)

### Missing or Partial

- Backtesting framework and strategy evaluation
- Production dashboards beyond current Grafana provisioning
- Model promotion automation (manual step)

---

## 1. Docker Services

### Core Infrastructure (8 services)

| Service | Container | Port | Status | Verify Command |
|---------|-----------|------|--------|----------------|
| **Kafka** | `kafka` | 9092 | ✅ Running | `docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list` |
| **Schema Registry** | `schema-registry` | 8081 | ✅ Running | `curl -s http://localhost:8081/subjects` |
| **TimescaleDB** | `timescaledb` | 5433 | ✅ Running | `psql postgresql://awet:awet@localhost:5433/awet -c "SELECT 1"` |
| **Redis** | `redis` | 6379 | ✅ Running | `redis-cli -p 6379 ping` |
| **Kafka UI** | `kafka-ui` | 8080 | ✅ Running | `curl -s http://localhost:8080/api/clusters` |
| **Prometheus** | `prometheus` | 9090 | ✅ Running | `curl -s http://localhost:9090/-/healthy` |
| **Grafana** | `grafana` | 3000 | ✅ Running | `curl -s http://localhost:3000/api/health` |
| **Alertmanager** | `alertmanager` | 9093 | ✅ Running | `curl -s http://localhost:9093/-/healthy` |

### SuperAGI Orchestration (6 services)

| Service | Container | Port | Status | Verify Command |
|---------|-----------|------|--------|----------------|
| **SuperAGI Backend** | `superagi-backend` | 8100 | ✅ Running | `curl -s http://localhost:8100/health` |
| **SuperAGI Celery** | `superagi-celery` | - | ✅ Running | `docker logs superagi-celery --tail 5` |
| **SuperAGI GUI** | `superagi-gui` | - | ✅ Running | (proxied via nginx) |
| **SuperAGI Proxy** | `superagi-proxy` | 3001 | ✅ Running | `curl -s http://localhost:3001` |
| **SuperAGI Redis** | `superagi-redis` | - | ✅ Running | internal only |
| **SuperAGI Postgres** | `superagi-postgres` | - | ✅ Running | internal only |

### Automated Jobs (2 services)

| Service | Container | Port | Status | How to Run |
|---------|-----------|------|--------|------------|
| **Reconciler** | `awet-reconciler` | - | ⚠️ On-demand | `make reconciler-up` |
| **EOD Job** | `awet-eod` | - | ⚠️ Manual | `make eod-docker` |

### External Services (not in compose)

| Service | Port | Status | Verify Command |
|---------|------|--------|----------------|
| **Ollama (LLM)** | 11434 | ✅ Running | `curl -s http://localhost:11434/api/tags` |

---

## 2. Python Agents (`src/agents/`)

| Agent | File | HTTP Port | Status | Run Command |
|-------|------|-----------|--------|-------------|
| **DataIngestionAgent** | `data_ingestion.py` | 8001 | ✅ Implemented | `python -m src.agents.data_ingestion` |
| **FeatureEngineeringAgent** | `feature_engineering.py` | 8002 | ✅ Implemented | `python -m src.agents.feature_engineering` |
| **TimeSeriesPredictionAgent** | `time_series_prediction.py` | 8003 | ✅ Implemented | `python -m src.agents.time_series_prediction` |
| **RiskAgent** | `risk_agent.py` | 8004 | ✅ Implemented | `python -m src.agents.risk_agent` |
| **ExecutionAgent** | `execution_agent.py` | 8005 | ✅ Implemented | `python -m src.agents.execution_agent` |
| **WatchtowerAgent** | `watchtower_agent.py` | 8006 | ✅ Implemented | `python -m src.agents.watchtower_agent` |
| **ExitAgent** | `exit_agent.py` | - | ✅ Implemented | `python -m src.agents.exit_agent --watch` |
| **BaseAgent** | `base_agent.py` | - | ✅ Implemented | (base class) |

**Agent Health Endpoints:**
- All agents expose `/health` and `/metrics` endpoints
- API key auth available via `AGENT_API_KEY` in [src/agents/base_agent.py](../src/agents/base_agent.py)

---

## 3. Scripts (`scripts/`)

### Data & Training

| Script | Status | Purpose | Run Command |
|--------|--------|---------|-------------|
| `load_market_data.py` | ✅ Done | Load Polygon CSVs → TimescaleDB (universe.csv default) | `python scripts/load_market_data.py` |
| `build_features.py` | ✅ Done | Compute TFT features (+ reddit_mentions) | `python scripts/build_features.py` |
| `train_tft_baseline.py` | ✅ Done | Train TFT model + ONNX export | `python scripts/train_tft_baseline.py --gpu` |
| `build_universe.py` | ✅ Done | Universe selection by liquidity | `python scripts/build_universe.py --top 500 --days 60 --mode daily` |

### Operations

| Script | Status | Purpose | Run Command |
|--------|--------|---------|-------------|
| `awet_start.sh` | ✅ Done | Start all services | `./scripts/awet_start.sh` |
| `awet_stop.sh` | ✅ Done | Stop all services | `./scripts/awet_stop.sh` |
| `awet_health_check.py` | ✅ Done | Check system health | `python scripts/awet_health_check.py` |
| `awet_doctor.sh` | ✅ Done | Comprehensive diagnostics | `./scripts/awet_doctor.sh` |
| `schedule_agents.py` | ✅ Done | Trigger agents manually | `python scripts/schedule_agents.py` |
| `run_e2e_demo.py` | ✅ Done | End-to-end demo | `python scripts/run_e2e_demo.py` |

### Trading & Reconciliation

| Script | Status | Purpose | Run Command |
|--------|--------|---------|-------------|
| `reconcile_orders.py` | ✅ Done | Reconcile Alpaca orders | `python scripts/reconcile_orders.py` |
| `reconcile_positions.py` | ✅ Done | Reconcile positions | `python scripts/reconcile_positions.py` |
| `reconcile_scheduler.py` | ✅ Done | Scheduled reconciliation | `python scripts/reconcile_scheduler.py --watch` |
| `daily_pnl_report.py` | ✅ Done | Generate PnL reports | `python scripts/daily_pnl_report.py` |
| `eod_pnl_job.py` | ✅ Done | End-of-day job | `python scripts/eod_pnl_job.py` |
| `show_last_trades.py` | ✅ Done | View recent trades | `python scripts/show_last_trades.py` |
| `show_trade_chain.py` | ✅ Done | Trace trade → LLM | `make trade-chain CORR=<id>` |
| `toggle_paper_mode.py` | ✅ Done | Enable/disable paper trading | `python scripts/toggle_paper_mode.py` |
| `verify_alpaca_env.py` | ✅ Done | Verify Alpaca credentials | `python scripts/verify_alpaca_env.py` |

### LLM & Observability

| Script | Status | Purpose | Run Command |
|--------|--------|---------|-------------|
| `show_llm_traces.py` | ✅ Done | View LLM call traces | `make llm-last` |
| `verify_ollama_gpu.py` | ✅ Done | Verify GPU for Ollama | `make llm-gpu` |
| `list_compose_projects.py` | ✅ Done | List Docker projects | `make stack-list` |

---

## 4. Execution Scripts (`execution/`)

| Script | Status | Purpose | Run Command |
|--------|--------|---------|-------------|
| `backfill_polygon.py` | ✅ Done | Backfill Polygon → Kafka | `make backfill-polygon` |
| `backfill_reddit.py` | ✅ Done | Backfill Reddit → Kafka | `make backfill-reddit` |
| `yfinance_backfill.py` | ✅ Done | FREE Yahoo Finance backfill | `make backfill-yfinance-live` |
| `run_live_ingestion.py` | ✅ Done | Daily live ingestion | `make live-ingest` |
| `demo.py` | ✅ Done | Demo mode | `make demo` |
| `verify_db.py` | ✅ Done | Verify DB connectivity | `python execution/verify_db.py` |
| `test_llm.py` | ✅ Done | Test LLM connectivity | `make llm-test` |
| `generate_plan.py` | ✅ Done | LLM plan generation | `make llm-plan` |
| `explain_run.py` | ✅ Done | LLM run explanation | `make llm-explain` |
| `register_superagi_tools.py` | ✅ Done | Register AWET tools | `make superagi-register-tools` |
| `create_superagi_agent.py` | ✅ Done | Create SuperAGI agent | `make superagi-create-agent` |

---

## 4.1 Data Sources & Paths (Local)

| Source | Path | Script(s) | Notes |
|--------|------|-----------|-------|
| Polygon CSV (minute/day) | `/home/kironix/train/poligon` | [scripts/load_market_data.py](../scripts/load_market_data.py) | 20 daily files and 20 minute files (July 2025) |
| Reddit dumps | `/home/kironix/train/reddit` | [scripts/verify_reddit_alignment.py](../scripts/verify_reddit_alignment.py), [execution/backfill_reddit.py](../execution/backfill_reddit.py) | 1 submissions + 1 comments file (July 2024) |
| Yahoo Finance | External (API) | [execution/yfinance_backfill.py](../execution/yfinance_backfill.py) | Free daily bars |

## 4.2 ML Pipeline Flow (DB Tables + Files)

1) Load data: [scripts/load_market_data.py](../scripts/load_market_data.py) → `market_raw_minute`, `market_raw_day`
2) Build features: [scripts/build_features.py](../scripts/build_features.py) → `features_tft` (includes `reddit_mentions`)
3) Train: [scripts/train_tft_baseline.py](../scripts/train_tft_baseline.py) → `models/tft/*` + `models/registry.json`
4) Predict: [src/agents/time_series_prediction.py](../src/agents/time_series_prediction.py) → `predictions_tft`

## 4.3 Universe Defaults (Why 7 Tickers)

- Universe source-of-truth: [config/universe.csv](../config/universe.csv)
- Default symbols derived from universe.csv in [Makefile](../Makefile)
- Current DB only contains 7 tickers (July 2025 data): AAPL, AMZN, GOOG, META, MSFT, NVDA, TSLA

---

## 4.4 Data Inventory (Local Files)

**Polygon (Minute CSVs)**
- Path: `/home/kironix/train/poligon/Minute Aggregates/July`
- Files: 20
- Sample file: `2025-07-01.csv.gz` (header: `ticker,volume,open,close,high,low,window_start,transactions`)

**Polygon (Day CSVs)**
- Path: `/home/kironix/train/poligon/Day Aggregates/July`
- Files: 20
- Sample file: `2025-07-01.csv.gz`

**Reddit dumps**
- Submissions: `/home/kironix/train/reddit/submissions/RS_2024-07.zst`
- Comments: `/home/kironix/train/reddit/comments/RC_2024-07.zst`
- Sample keys include: `id`, `subreddit`, `author`, `title`, `selftext`, `created_utc`, `score`

## 5. Database Tables (TimescaleDB)

| Table | Rows | Purpose | Status |
|-------|------|---------|--------|
| `market_raw_minute` | 100,508 | Minute OHLCV bars | ✅ Populated (7 tickers) |
| `market_raw_day` | 140 | Daily OHLCV bars | ✅ Populated (7 tickers) |
| `features_tft` | 99,759 | Engineered features (+reddit_mentions) | ✅ Populated |
| `reddit_posts` | 0 | Reddit posts/comments w/ tickers | ⚠️ Populates via verify-reddit --write-db |
| `reddit_daily_mentions` | 0 | Daily ticker mention counts | ⚠️ Populates via verify-reddit --write-db |
| `predictions_tft` | 0 | Model predictions | ⚠️ Empty (run inference) |

**DB Data Audit (current):**
- `market_raw_minute`: 100,508 rows, 7 tickers, range 2025-07-01 → 2025-07-29
- `market_raw_day`: 140 rows, 7 tickers, range 2025-07-01 → 2025-07-29
| `risk_decisions` | 0 | Risk gate logs | ⚠️ Empty (run pipeline) |
| `trades` | ~5 | Executed trades | ✅ Working |
| `paper_trades` | 0 | Legacy paper trades | ⚠️ Empty |
| `positions` | ~3 | Current positions | ✅ Working |
| `audit_events` | ~50 | Pipeline audit trail | ✅ Working |
| `llm_traces` | ~100 | LLM call traces | ✅ Working |
| `llm_daily_summary` | ~5 | LLM daily stats | ✅ Working |
| `daily_pnl_summary` | ~3 | PnL reports | ✅ Working |
| `models_registry` | 2 | Trained models | ✅ Working |
| `backfill_checkpoints` | ~20 | Backfill progress | ✅ Working |

---

## 6. Configuration Files

| File | Status | Purpose |
|------|--------|---------|
| `config/app.yaml` | ✅ Done | Main app config |
| `config/kafka.yaml` | ✅ Done | Kafka topics config |
| `config/logging.yaml` | ✅ Done | Logging config |
| `config/llm.yaml` | ✅ Done | LLM config |
| `config/limits.yaml` | ✅ Done | Trading limits |
| `config/market_data.yaml` | ✅ Done | Market data sources |
| `config/universe.csv` | ❌ Missing | Universe symbols | **TODO** |

---

## 7. Kafka Topics

| Topic | Status | Purpose |
|-------|--------|---------|
| `market.raw` | ✅ Created | Raw market data |
| `market.engineered` | ✅ Created | Feature data |
| `predictions.tft` | ✅ Created | Model predictions |
| `risk.approved` | ✅ Created | Approved trades |
| `risk.rejected` | ✅ Created | Rejected trades |
| `execution.completed` | ✅ Created | Filled trades |
| `social.raw` | ✅ Created | Reddit data |
| `audit` | ✅ Created | Audit trail |

---

## 8. Model Registry

| Model ID | Status | Val Loss | Created |
|----------|--------|----------|---------|
| `tft_20260115_081459_7f6cae3b` | 🟢 green | 0.00347 | 2026-01-15 |
| `20260117_035006_ab98e94b` | 🟡 candidate | 0.000009 | 2026-01-17 |

---

## 9. Missing / TODO Items

### High Priority (Production Blockers)

| Item | File | Description |
|------|------|-------------|
| **API Authentication** | `src/agents/base_agent.py` | No auth on agent endpoints |
| **Universe Builder** | `scripts/build_universe.py` | Hardcoded 7 symbols |
| **Doctor Script** | `scripts/awet_doctor.sh` | No comprehensive diagnostics |
| **Reddit Alignment** | `scripts/verify_reddit_alignment.py` | Untested |
| **Grafana Dashboards** | `grafana/provisioning/dashboards/` | Empty |
| **Alerting Rules** | `prometheus/alerts.yml` | Missing |

### Medium Priority

| Item | File | Description |
|------|------|-------------|
| **CI/CD** | `.github/workflows/` | No pipeline |
| **Secrets Management** | `.env` | Plain text secrets |
| **Portal Docs** | `docs/OPERATIONS.md` | Missing portal URLs section |
| **Inference Agent ONNX** | `src/agents/time_series_prediction.py` | Uses PyTorch, not ONNX |

### Low Priority

| Item | File | Description |
|------|------|-------------|
| **Unit Tests** | `tests/unit/` | Minimal coverage |
| **Integration Tests** | `tests/integration/` | Partial |
| **API Docs** | - | No OpenAPI spec |

---

## 10. Port Map (No Conflicts)

| Port | Service | URL |
|------|---------|-----|
| 3000 | Grafana | http://localhost:3000 |
| 3001 | SuperAGI GUI | http://localhost:3001 |
| 5433 | TimescaleDB | postgresql://localhost:5433/awet |
| 6379 | Redis | redis://localhost:6379 |
| 8080 | Kafka UI | http://localhost:8080 |
| 8081 | Schema Registry | http://localhost:8081 |
| 8100 | SuperAGI API | http://localhost:8100 |
| 9090 | Prometheus | http://localhost:9090 |
| 9092 | Kafka | localhost:9092 |
| 9093 | Alertmanager | http://localhost:9093 |
| 11434 | Ollama | http://localhost:11434 |

**Agent Ports (when running locally):**
- 8001: DataIngestionAgent
- 8002: FeatureEngineeringAgent
- 8003: TimeSeriesPredictionAgent
- 8004: RiskAgent
- 8005: ExecutionAgent
- 8006: WatchtowerAgent

---

## 11. Verification Commands

```bash
# Full system check
./scripts/awet_doctor.sh      # TODO: create this

# Quick health check
python scripts/awet_health_check.py

# Database check
psql postgresql://awet:awet@localhost:5433/awet -c "SELECT COUNT(*) FROM market_raw_minute"

# Kafka check
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# LLM check
curl -s http://localhost:11434/api/tags | jq '.models[].name'

# Agent health (when running)
curl -s http://localhost:8001/health  # DataIngestion
curl -s http://localhost:8002/health  # FeatureEngineering
curl -s http://localhost:8003/health  # Prediction
curl -s http://localhost:8004/health  # Risk
curl -s http://localhost:8005/health  # Execution
curl -s http://localhost:8006/health  # Watchtower
```

---

## 12. How to Start Fresh

```bash
# 1. Start infrastructure
make up

# 2. Load market data
make load-market-data

# 3. Build features
make build-features

# 4. Train model
make train-tft-gpu

# 5. Promote model to production
python -m execution.promote_model <run_id>

# 6. Enable paper trading
make paper-on

# 7. Run demo
make demo
```
