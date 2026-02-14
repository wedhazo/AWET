# AWET - Automated Trading Platform
## Technical & Business Overview for Management

**Document Version:** 1.0  
**Last Updated:** January 16, 2026  
**Author:** Engineering Team  
**Classification:** Internal

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Business Objectives](#2-business-objectives)
3. [System Architecture](#3-system-architecture)
4. [Technology Stack](#4-technology-stack)
5. [Microservices & Components](#5-microservices--components)
6. [Data Flow & Pipeline](#6-data-flow--pipeline)
7. [Machine Learning Pipeline](#7-machine-learning-pipeline)
8. [Database Schema](#8-database-schema)
9. [Safety & Risk Controls](#9-safety--risk-controls)
10. [Monitoring & Observability](#10-monitoring--observability)
11. [Deployment Infrastructure](#11-deployment-infrastructure)
12. [Operations & Maintenance](#12-operations--maintenance)
13. [Current Status](#13-current-status)
14. [Roadmap & Next Steps](#14-roadmap--next-steps)
15. [Appendix: Quick Reference](#15-appendix-quick-reference)

---

## 1. Executive Summary

### What is AWET?

**AWET (Automated Web-based Electronic Trading)** is an institutional-grade, audit-ready **paper trading platform** that demonstrates production-quality algorithmic trading infrastructure without risking real capital.

### Key Value Propositions

| Value | Description |
|-------|-------------|
| **Zero Financial Risk** | Paper trading only - no real money is ever at risk |
| **Production-Ready Architecture** | Enterprise patterns ready for live deployment |
| **Full Audit Trail** | Every decision is traceable end-to-end |
| **AI-Powered** | Machine learning predictions + LLM-based orchestration |
| **Cloud-Native** | Containerized, scalable, Kubernetes-ready |

### Current Metrics (as of January 16, 2026)

| Metric | Value |
|--------|-------|
| **GPU Performance** | NVIDIA RTX 5090 (24GB VRAM) - 291 tokens/sec |
| **LLM Model** | Llama 3.2 3B (local, GPU-accelerated) |
| **Database** | TimescaleDB with 12+ hypertables |
| **Streaming** | Apache Kafka with 9 topics |
| **Symbols Tracked** | AAPL, MSFT, GOOG, AMZN, NVDA, META, TSLA |
| **Test Coverage** | 40+ unit tests passing |

---

## 2. Business Objectives

### 2.1 Primary Goals

1. **Demonstrate Algorithmic Trading Capability**
   - Prove the team can build institutional-quality trading infrastructure
   - Validate ML-based signal generation
   - Establish operational best practices

2. **Risk-Free Development Environment**
   - Test trading strategies without financial exposure
   - Iterate on models and logic safely
   - Train team on trading system operations

3. **Production Readiness**
   - Build infrastructure that can transition to live trading
   - Establish audit and compliance frameworks
   - Implement enterprise monitoring and alerting

### 2.2 Key Business Processes

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         AWET TRADING PIPELINE                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   DATA INGESTION          SIGNAL GENERATION         EXECUTION               │
│   ─────────────          ─────────────────         ─────────                │
│                                                                             │
│   ┌─────────────┐        ┌─────────────┐          ┌─────────────┐          │
│   │  Polygon.io │───────▶│   Feature   │─────────▶│    Risk     │          │
│   │   yfinance  │        │ Engineering │          │   Gates     │          │
│   └─────────────┘        └─────────────┘          └──────┬──────┘          │
│                                 │                        │                  │
│                                 ▼                        ▼                  │
│                          ┌─────────────┐          ┌─────────────┐          │
│                          │  TFT Model  │          │   Alpaca    │          │
│                          │ Predictions │          │Paper Trading│          │
│                          └─────────────┘          └─────────────┘          │
│                                                                             │
│   RECONCILIATION          REPORTING              ORCHESTRATION              │
│   ──────────────          ─────────              ────────────               │
│                                                                             │
│   ┌─────────────┐        ┌─────────────┐          ┌─────────────┐          │
│   │Order Fills  │───────▶│  Daily PnL  │          │  SuperAGI   │          │
│   │Positions    │        │   Reports   │          │   Agents    │          │
│   └─────────────┘        └─────────────┘          └─────────────┘          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.3 Trading Logic Parameters

| Parameter | Value | Purpose |
|-----------|-------|---------|
| Max Trade Notional | $1,000 USD | Maximum per-trade exposure |
| Max Qty Per Trade | 100 shares | Position size limit |
| Max Orders/Minute | 5 | Rate limiting |
| Max Orders/Symbol/Day | 3 | Prevent overtrading |
| Cooldown Per Symbol | 15 minutes | Minimum time between trades |
| Max Total Exposure | $50,000 USD | Portfolio cap |
| Max Per-Symbol Exposure | $10,000 USD | Concentration limit |
| Take Profit | 1.0% | Exit at profit target |
| Stop Loss | 0.5% | Exit at max loss |
| Max Holding Time | 60 minutes | Time-based exit |

---

## 3. System Architecture

### 3.1 Three-Layer Architecture

AWET implements a **separation of concerns** between intent, decision-making, and execution:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         LAYER 1: DIRECTIVES                                 │
│                     (What to do - SOPs in Markdown)                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   directives/trading_pipeline.md     - End-to-end trading flow              │
│   directives/live_daily_ingestion.md - Daily data refresh                   │
│   directives/awet_e2e_demo.md        - Demo execution                       │
│   directives/superagi_orchestration.md - Agent configuration                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       LAYER 2: ORCHESTRATION                                │
│                  (Decision Making - AI Agents)                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   SuperAGI Agents:                                                          │
│   • AWET Orchestrator    - Interactive pipeline control                     │
│   • Night Trainer        - Nightly model training (02:00)                   │
│   • Morning Deployer     - Pre-market health check (08:30)                  │
│   • Trade Watchdog       - Market-hours monitoring (every 15 min)           │
│                                                                             │
│   Alternative Orchestrators: GitHub Copilot, Claude, GPT                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        LAYER 3: EXECUTION                                   │
│               (Deterministic Python Services)                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Long-Running Agents (src/agents/):                                        │
│   • DataIngestionAgent      • RiskAgent                                     │
│   • FeatureEngineeringAgent • ExecutionAgent                                │
│   • TimeSeriesPredictionAgent • ExitAgent                                   │
│   • WatchtowerAgent                                                         │
│                                                                             │
│   One-Shot Scripts (execution/):                                            │
│   • demo.py                 • backfill_polygon.py                           │
│   • run_live_ingestion.py   • yfinance_backfill.py                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Why This Architecture?

| Problem | Solution |
|---------|----------|
| **LLMs are probabilistic** | Trading logic must be deterministic - separation ensures reliability |
| **Audit requirements** | Clear boundary between "what was decided" and "what was executed" |
| **Scalability** | Each layer can scale independently |
| **Testability** | Execution layer is fully unit-testable |
| **Flexibility** | Swap orchestrators without changing execution code |

---

## 4. Technology Stack

### 4.1 Core Technologies

| Category | Technology | Version | Purpose |
|----------|------------|---------|---------|
| **Language** | Python | 3.11+ | Primary development language |
| **Web Framework** | FastAPI | 0.111+ | REST APIs for agents |
| **Async Server** | Uvicorn | 0.30+ | ASGI server |
| **Streaming** | Apache Kafka | 7.6.1 (KRaft) | Event streaming backbone |
| **Schema Registry** | Confluent | 7.6.1 | Avro schema management |
| **Serialization** | Apache Avro | via fastavro | Binary event encoding |
| **Database** | TimescaleDB | 2.13.1 | Time-series + PostgreSQL |
| **Cache** | Redis | 7.2 | Session/state caching |

### 4.2 Machine Learning Stack

| Category | Technology | Version | Purpose |
|----------|------------|---------|---------|
| **Deep Learning** | PyTorch | 2.2+ | Neural network training |
| **Training** | PyTorch Lightning | 2.0+ | Training loop abstraction |
| **Model Export** | ONNX | 1.17+ | Production inference format |
| **Inference** | ONNX Runtime | 1.17+ | Fast model serving |
| **LLM** | Ollama | 0.14.1 | Local LLM hosting |
| **LLM Model** | Llama 3.2 3B | - | GPU-accelerated inference |

### 4.3 Infrastructure Stack

| Category | Technology | Version | Purpose |
|----------|------------|---------|---------|
| **Containerization** | Docker | 24+ | Application packaging |
| **Orchestration** | Docker Compose | v2 | Local multi-container |
| **Kubernetes** | K8s | 1.28+ | Production orchestration |
| **IaC** | Terraform | 1.5+ | Cloud infrastructure |
| **Monitoring** | Prometheus | 2.54+ | Metrics collection |
| **Visualization** | Grafana | 11.1 | Dashboards |
| **Alerting** | Alertmanager | 0.27 | Alert routing |

### 4.4 External Integrations

| Service | Purpose | API Type |
|---------|---------|----------|
| **Alpaca** | Paper trading broker | REST + WebSocket |
| **Polygon.io** | Historical market data | REST |
| **yfinance** | Real-time market data | Python library |

---

## 5. Microservices & Components

### 5.1 Docker Services Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DOCKER COMPOSE SERVICES                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   CORE INFRASTRUCTURE                     SUPERAGI STACK                    │
│   ────────────────────                    ──────────────                    │
│   ┌─────────────┐ :9092                   ┌─────────────┐ :3001             │
│   │    kafka    │ Message Broker          │superagi-proxy│ Nginx Frontend   │
│   └─────────────┘                         └──────┬──────┘                   │
│   ┌─────────────┐ :8081                          │                          │
│   │schema-registry│ Avro Schemas           ┌─────┴─────┐                    │
│   └─────────────┘                          │           │                    │
│   ┌─────────────┐ :8080                ┌───┴───┐ ┌─────┴─────┐             │
│   │  kafka-ui   │ Topic Browser        │  GUI  │ │  Backend  │:8100        │
│   └─────────────┘                      └───────┘ └─────┬─────┘             │
│   ┌─────────────┐ :5433                              │                      │
│   │ timescaledb │ Time-series DB       ┌─────────────┴───────────┐         │
│   └─────────────┘                      │                         │         │
│   ┌─────────────┐ :6379                │       superagi-celery   │         │
│   │    redis    │ Cache                │       (Task Workers)    │         │
│   └─────────────┘                      └─────────────────────────┘         │
│                                                                             │
│   MONITORING STACK                        AWET SERVICES                     │
│   ────────────────                        ─────────────                     │
│   ┌─────────────┐ :9090                   ┌─────────────┐                   │
│   │ prometheus  │ Metrics                 │awet-reconciler│ Order Sync      │
│   └─────────────┘                         └─────────────┘                   │
│   ┌─────────────┐ :3000                   ┌─────────────┐                   │
│   │   grafana   │ Dashboards              │  awet-eod   │ EOD Jobs          │
│   └─────────────┘                         └─────────────┘                   │
│   ┌─────────────┐ :9093                                                     │
│   │alertmanager │ Alerts                                                    │
│   └─────────────┘                                                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Agent Services (Python Processes)

| Agent | Port | Input Topic | Output Topic | Responsibility |
|-------|------|-------------|--------------|----------------|
| **DataIngestionAgent** | 8001 | (external data) | `market.raw` | Pull OHLCV from data providers |
| **FeatureEngineeringAgent** | 8002 | `market.raw` | `market.engineered` | Compute technical indicators |
| **TimeSeriesPredictionAgent** | 8003 | `market.engineered` | `predictions.tft` | TFT model inference |
| **RiskAgent** | 8004 | `predictions.tft` | `risk.approved` / `risk.rejected` | Position limits, CVaR |
| **ExecutionAgent** | 8005 | `risk.approved` | `execution.completed` / `execution.blocked` | Paper trade execution |
| **ExitAgent** | - | (positions table) | `execution.completed` | Stop-loss, take-profit |
| **WatchtowerAgent** | 8006 | (all topics) | (metrics) | Health monitoring |

### 5.3 Service Dependencies

```
                         ┌─────────────┐
                         │    kafka    │
                         └──────┬──────┘
                                │
              ┌─────────────────┼─────────────────┐
              │                 │                 │
              ▼                 ▼                 ▼
       ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
       │schema-registry│ │ timescaledb │  │    redis    │
       └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
              │                │                 │
              └────────────────┼─────────────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
              ▼                ▼                ▼
       ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
       │ All Agents  │  │  SuperAGI   │  │awet-reconciler│
       └─────────────┘  └─────────────┘  └─────────────┘
```

---

## 6. Data Flow & Pipeline

### 6.1 Event-Driven Architecture

All communication between services uses **Apache Kafka** with **Avro schemas**:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           KAFKA TOPICS                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   market.raw ───────▶ market.engineered ───────▶ predictions.tft            │
│       │                      │                         │                    │
│       │                      │                         ▼                    │
│       │                      │              ┌──────────────────────┐        │
│       │                      │              │                      │        │
│       │                      │              ▼                      ▼        │
│       │                      │       risk.approved          risk.rejected   │
│       │                      │              │                      │        │
│       │                      │              ▼                      │        │
│       │                      │    ┌────────────────────┐           │        │
│       │                      │    │                    │           │        │
│       │                      │    ▼                    ▼           │        │
│       │                      │ execution.completed execution.blocked│        │
│       │                      │                                     │        │
│       └──────────────────────┴─────────────────────────────────────┘        │
│                                       │                                     │
│                                       ▼                                     │
│                                    audit                                    │
│                          (all events logged)                                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 Event Contract (All Events)

Every event in the system **MUST** include these fields:

```json
{
  "event_id": "uuid",           // Unique event identifier
  "correlation_id": "uuid",     // Trace ID across pipeline
  "idempotency_key": "string",  // Deduplication key
  "symbol": "string",           // Trading symbol (e.g., "AAPL")
  "ts": "timestamp",            // Event timestamp (UTC)
  "schema_version": 1,          // Avro schema version
  "source": "string"            // Producing service name
}
```

### 6.3 Data Flow Example

**Scenario:** AAPL price update triggers a BUY signal

```
1. DATA INGESTION
   ├─ DataIngestionAgent receives AAPL tick from yfinance
   ├─ Creates MarketEvent with OHLCV data
   ├─ Publishes to market.raw topic
   └─ Writes to market_raw_minute table

2. FEATURE ENGINEERING
   ├─ FeatureEngineeringAgent consumes from market.raw
   ├─ Computes: RSI(14), SMA(5,20), EMA(5,20), volatility
   ├─ Publishes EngineeredEvent to market.engineered
   └─ Writes to features_tft table

3. PREDICTION
   ├─ TimeSeriesPredictionAgent consumes from market.engineered
   ├─ Loads "green" TFT model from registry
   ├─ Runs ONNX inference → direction="long", confidence=0.72
   ├─ Publishes PredictionEvent to predictions.tft
   └─ Writes to predictions_tft table

4. RISK ASSESSMENT
   ├─ RiskAgent consumes from predictions.tft
   ├─ Checks: position limits, exposure, CVaR
   ├─ Approves trade → publishes to risk.approved
   └─ Writes to risk_decisions table

5. EXECUTION
   ├─ ExecutionAgent consumes from risk.approved
   ├─ Checks: approval file exists, not dry-run
   ├─ Calculates qty = min($1000 / $175, 100) = 5 shares
   ├─ Submits order to Alpaca Paper API
   ├─ Publishes ExecutionEvent to execution.completed
   └─ Writes to trades table

6. AUDIT
   ├─ Each agent writes to audit_events table
   ├─ All events share same correlation_id
   └─ Full trace available: data → signal → risk → execution
```

---

## 7. Machine Learning Pipeline

### 7.1 Model Architecture: Temporal Fusion Transformer (TFT)

The TFT model is designed for **multi-horizon time-series forecasting**:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    TEMPORAL FUSION TRANSFORMER                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   INPUT FEATURES                                                            │
│   ──────────────                                                            │
│   Static:    symbol (categorical)                                           │
│   Known:     minute_of_day, hour_of_day, day_of_week                        │
│   Observed:  price, volume, returns_1/5/15, volatility, RSI, SMA, EMA       │
│                                                                             │
│   ARCHITECTURE                                                              │
│   ────────────                                                              │
│   ┌─────────────────┐                                                       │
│   │Variable Selection│ → Attention over input features                      │
│   └────────┬────────┘                                                       │
│            ▼                                                                │
│   ┌─────────────────┐                                                       │
│   │ LSTM Encoder    │ → Captures temporal patterns (lookback=120)           │
│   └────────┬────────┘                                                       │
│            ▼                                                                │
│   ┌─────────────────┐                                                       │
│   │ Interpretable   │ → Multi-head attention (4 heads)                      │
│   │ Attention       │                                                       │
│   └────────┬────────┘                                                       │
│            ▼                                                                │
│   ┌─────────────────┐                                                       │
│   │ Quantile Output │ → Predicts q10, q50 (median), q90                     │
│   └─────────────────┘                                                       │
│                                                                             │
│   OUTPUT                                                                    │
│   ──────                                                                    │
│   Horizons:    30, 45, 60 minutes ahead                                     │
│   Direction:   UP if q50 > 0, else DOWN                                     │
│   Confidence:  Based on quantile spread                                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 7.2 Training Pipeline

```bash
# Full training pipeline
make train

# Steps executed:
# 1. Load features from features_tft table
# 2. Create train/val split
# 3. Train TFT with PyTorch Lightning
# 4. Export to ONNX format
# 5. Register in models_registry table as "candidate"
```

### 7.3 Model Registry Lifecycle

```
┌───────────┐        ┌───────────┐        ┌────────────┐
│ candidate │───────▶│   green   │───────▶│ deprecated │
└───────────┘        └───────────┘        └────────────┘
    │                     │                     │
    │  Newly trained      │  Production         │  Replaced by
    │  Not validated      │  Active inference   │  newer model
    │                     │                     │
```

**Commands:**
```bash
make train                    # Train new model → candidate
make promote MODEL_ID=xxx     # Promote to green
make list-models              # Show all models with status
```

### 7.4 Inference Pipeline

```python
# TimeSeriesPredictionAgent loads model on startup:
1. Query models_registry for status='green'
2. Load ONNX model into ONNX Runtime
3. On each EngineeredEvent:
   a. Prepare input tensor (lookback window)
   b. Run inference → quantile predictions
   c. Determine direction (UP/DOWN)
   d. Publish PredictionEvent
```

---

## 8. Database Schema

### 8.1 TimescaleDB Tables Overview

TimescaleDB extends PostgreSQL with **hypertables** for efficient time-series storage.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         TIMESCALEDB SCHEMA                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   MARKET DATA                           TRADING                             │
│   ───────────                           ───────                             │
│   ┌─────────────────────┐               ┌─────────────────────┐             │
│   │ market_raw_minute   │ ◄── Hypertable│      trades         │◄── Hypertable│
│   │ market_raw_day      │               │      positions      │             │
│   └─────────────────────┘               └─────────────────────┘             │
│                                                                             │
│   FEATURES & PREDICTIONS                RISK & AUDIT                        │
│   ──────────────────────                ────────────                        │
│   ┌─────────────────────┐               ┌─────────────────────┐             │
│   │ features_tft        │◄── Hypertable │ risk_decisions      │◄── Hypertable│
│   │ predictions_tft     │◄── Hypertable │ audit_events        │◄── Hypertable│
│   └─────────────────────┘               └─────────────────────┘             │
│                                                                             │
│   ML & OPERATIONS                       OBSERVABILITY                       │
│   ───────────────                       ─────────────                       │
│   ┌─────────────────────┐               ┌─────────────────────┐             │
│   │ models_registry     │               │ llm_traces          │◄── Hypertable│
│   │ backfill_checkpoints│               │ llm_daily_summary   │             │
│   └─────────────────────┘               │ daily_pnl_summary   │             │
│                                         └─────────────────────┘             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 8.2 Key Tables Detail

#### trades (Paper Trade Execution)

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGSERIAL | Primary key |
| `ts` | TIMESTAMPTZ | Order timestamp |
| `symbol` | TEXT | Trading symbol |
| `side` | TEXT | 'buy' or 'sell' |
| `qty` | INT | Ordered quantity |
| `filled_qty` | INT | Actually filled |
| `avg_fill_price` | NUMERIC | Execution price |
| `status` | TEXT | filled/blocked/rejected/pending |
| `alpaca_order_id` | TEXT | Broker order ID |
| `alpaca_status` | TEXT | Broker status |
| `filled_at` | TIMESTAMPTZ | Fill timestamp |
| `correlation_id` | TEXT | Trace ID |
| `idempotency_key` | TEXT | Deduplication |
| `paper_trade` | BOOLEAN | Always TRUE |
| `dry_run` | BOOLEAN | If blocked by dry_run flag |

#### positions (Current Portfolio)

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | TEXT | PRIMARY KEY |
| `qty` | NUMERIC | Current shares held |
| `avg_entry_price` | NUMERIC | Average cost basis |
| `market_value` | NUMERIC | Current market value |
| `unrealized_pl` | NUMERIC | Unrealized PnL |
| `cost_basis` | NUMERIC | Total cost |
| `updated_at` | TIMESTAMPTZ | Last sync time |

#### daily_pnl_summary (Performance Metrics)

| Column | Type | Description |
|--------|------|-------------|
| `report_date` | DATE | PRIMARY KEY |
| `realized_pnl_usd` | NUMERIC | Closed trade PnL |
| `unrealized_pnl_usd` | NUMERIC | Open position PnL |
| `total_pnl_usd` | NUMERIC | Combined PnL |
| `num_trades` | INT | Trade count |
| `num_wins` | INT | Profitable trades |
| `num_losses` | INT | Losing trades |
| `win_rate` | NUMERIC | Win % |
| `best_symbol` | TEXT | Top performer |
| `worst_symbol` | TEXT | Worst performer |

#### audit_events (Full Trace Log)

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | UUID | Unique event ID |
| `correlation_id` | UUID | Pipeline trace ID |
| `idempotency_key` | TEXT | Deduplication |
| `symbol` | TEXT | Trading symbol |
| `ts` | TIMESTAMPTZ | Event time |
| `source` | TEXT | Producing service |
| `event_type` | TEXT | Event category |
| `payload` | JSONB | Full event data |

---

## 9. Safety & Risk Controls

### 9.1 Three-Layer Safety Architecture

**AWET is PAPER TRADING ONLY** with multiple safety gates:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SAFETY GATES                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   LAYER 1: HARDCODED (Cannot be changed)                                    │
│   ──────────────────────────────────────                                    │
│   • paper_trade = True (hardcoded in execution_agent.py)                    │
│   • AlpacaClient blocks any non-paper endpoint at startup                   │
│   • No TradingClient or real order submission code exists                   │
│                                                                             │
│   LAYER 2: CONFIGURATION (config/app.yaml)                                  │
│   ────────────────────────────────────────                                  │
│   • execution_dry_run: true (default)                                       │
│   • When true: ALL trades blocked, even paper trades                        │
│   • Set to false only when ready for paper trading                          │
│                                                                             │
│   LAYER 3: FILE GATE (Runtime toggle)                                       │
│   ────────────────────────────────────                                      │
│   • File: .tmp/APPROVE_EXECUTION                                            │
│   • Even if dry_run=false, file must exist                                  │
│   • Quick enable/disable without config changes                             │
│                                                                             │
│   COMMANDS                                                                  │
│   ────────                                                                  │
│   make approve    → Creates approval file                                   │
│   make revoke     → Removes approval file                                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 9.2 Risk Management Controls

| Control | Implementation | Value |
|---------|----------------|-------|
| **Max Trade Size** | Position sizing in ExecutionAgent | $1,000 / trade |
| **Max Position** | Per-symbol exposure cap | $10,000 |
| **Max Portfolio** | Total exposure cap | $50,000 |
| **Rate Limiting** | Orders per minute | 5 max |
| **Cooldown** | Time between same-symbol trades | 15 minutes |
| **CVaR Check** | Risk gate validation | Configurable threshold |
| **Stop Loss** | Automatic exit | 0.5% loss |
| **Take Profit** | Automatic exit | 1.0% gain |
| **Time Exit** | Maximum holding period | 60 minutes |

### 9.3 Alpaca Paper Trading Validation

The AlpacaClient enforces paper-only operation:

```python
# src/integrations/alpaca_client.py
PAPER_BASE_URL = "https://paper-api.alpaca.markets"

class AlpacaClient:
    def __init__(self):
        base_url = os.getenv("ALPACA_BASE_URL", PAPER_BASE_URL)
        if "paper-api" not in base_url:
            raise AlpacaLiveEndpointError(
                "BLOCKED: Live trading endpoint detected. "
                "This system only supports paper trading."
            )
```

---

## 10. Monitoring & Observability

### 10.1 Monitoring Stack

| Tool | URL | Purpose |
|------|-----|---------|
| **Prometheus** | http://localhost:9090 | Metrics collection & storage |
| **Grafana** | http://localhost:3000 | Visualization & dashboards |
| **Alertmanager** | http://localhost:9093 | Alert routing & notifications |
| **Kafka UI** | http://localhost:8080 | Topic inspection & debugging |
| **SuperAGI UI** | http://localhost:3001 | Agent orchestration |

### 10.2 Key Metrics

#### Application Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `events_processed_total` | Counter | agent, topic, status | Events processed |
| `event_latency_seconds` | Histogram | agent, topic | Processing latency |
| `kafka_consumer_lag` | Gauge | group, topic | Consumer lag |
| `model_inference_latency` | Histogram | model | TFT inference time |
| `trade_count` | Counter | symbol, side, status | Trades executed |
| `llm_request_latency` | Histogram | model, agent | LLM response time |
| `llm_token_count` | Counter | model | Token usage |

#### Infrastructure Metrics

| Metric | Source | Description |
|--------|--------|-------------|
| CPU/Memory usage | Docker | Container resource usage |
| Kafka lag | Kafka Admin | Consumer group lag |
| PostgreSQL connections | TimescaleDB | Active connections |
| GPU utilization | nvidia-smi | Model inference load |

### 10.3 Health Endpoints

Every agent exposes standardized endpoints:

```
GET /health  → {"status": "ok", "agent": "ExecutionAgent"}
GET /metrics → Prometheus-format metrics
```

### 10.4 Logging

**Structured JSON logging** with correlation IDs:

```json
{
  "event": "trade_executed",
  "correlation_id": "abc123-def456",
  "symbol": "AAPL",
  "side": "buy",
  "qty": 5,
  "price": 175.50,
  "timestamp": "2026-01-16T10:30:00Z",
  "agent": "ExecutionAgent"
}
```

**Log locations:**

| Log | Location |
|-----|----------|
| Agent logs | Console (structlog JSON) |
| Docker logs | `docker logs <container>` |
| Scheduled jobs | `logs/*.log` |
| Reconciler | `docker logs awet-reconciler` |

---

## 11. Deployment Infrastructure

### 11.1 Local Development (Docker Compose)

```bash
# Start all services
./scripts/awet_start.sh

# Stop all services
./scripts/awet_stop.sh

# Start specific services
docker compose up -d kafka timescaledb
```

### 11.2 Production Deployment (Kubernetes)

The `deploy/k8s/` directory contains Kubernetes manifests:

```
deploy/k8s/
├── base/                    # Base resources
│   ├── namespace.yaml
│   ├── kafka/
│   ├── timescaledb/
│   └── agents/
└── overlays/                # Environment-specific
    ├── dev/
    ├── staging/
    └── production/
```

### 11.3 Cloud Infrastructure (Terraform)

The `deploy/terraform/aws/` directory contains AWS infrastructure:

```
deploy/terraform/aws/
├── main.tf              # VPC, subnets, security groups
├── eks.tf               # Kubernetes cluster
├── rds.tf               # PostgreSQL (TimescaleDB)
├── msk.tf               # Managed Kafka
├── elasticache.tf       # Redis
└── variables.tf         # Configuration
```

### 11.4 Hardware Requirements

| Environment | CPU | RAM | Storage | GPU |
|-------------|-----|-----|---------|-----|
| Development | 4 cores | 16 GB | 100 GB SSD | Optional |
| Staging | 8 cores | 32 GB | 500 GB SSD | Optional |
| Production | 16 cores | 64 GB | 1 TB SSD | Recommended |

**Current Development Machine:**
- CPU: Intel Core i9 (14th gen)
- RAM: 64 GB
- GPU: NVIDIA RTX 5090 (24 GB VRAM)
- Storage: 2 TB NVMe SSD

---

## 12. Operations & Maintenance

### 12.1 Daily Operations

| Time | Task | Command | Automated |
|------|------|---------|-----------|
| 02:00 | Night training | (cron triggers agent) | ✅ Yes |
| 08:30 | Morning health check | (cron triggers agent) | ✅ Yes |
| 09:30-16:00 | Order reconciliation | `docker compose up awet-reconciler` | ✅ Yes |
| Every 15 min | Trade watchdog | (cron triggers agent) | ✅ Yes |
| 16:30 | End-of-day job | `make eod` or auto | ✅ Yes |
| EOD | PnL review | `make pnl-today` | Manual |

### 12.2 Common Commands

```bash
# System Control
./scripts/awet_start.sh           # Start everything
./scripts/awet_stop.sh            # Stop everything
python scripts/awet_health_check.py  # Health check

# Trading Control
make approve                      # Enable paper trading
make revoke                       # Disable paper trading

# Reconciliation
make reconciler-up                # Start Docker reconciler
make reconciler-down              # Stop reconciler
docker logs -f awet-reconciler    # View reconciler logs
make eod                          # Run EOD job locally
docker compose run --rm awet-eod  # Run EOD in Docker

# PnL Reports
make pnl-today                    # Today's PnL
make pnl-week                     # Last 7 days
make pnl-save                     # Save to database

# ML Operations
make train                        # Train new model
make promote MODEL_ID=xxx         # Promote to production
make list-models                  # Show model registry

# LLM Operations
make llm-gpu                      # Verify GPU setup
make llm-test                     # Test LLM connectivity
make llm-last                     # View recent LLM traces
```

### 12.3 Troubleshooting

| Issue | Solution |
|-------|----------|
| **Services won't start** | Check `docker compose logs`, ensure ports not in use |
| **Kafka connection refused** | Wait 30s after startup, check `docker logs kafka` |
| **Orders stuck in "accepted"** | Run `make reconcile` to sync with Alpaca |
| **PnL shows $0** | Ensure orders are filled, run `make eod` |
| **LLM not responding** | Check `ollama list`, ensure model loaded |
| **GPU not used** | Run `make llm-gpu` to diagnose |
| **Agent health check fails** | Check agent logs, restart with `docker compose restart` |

### 12.4 Maintenance Schedule

| Frequency | Task |
|-----------|------|
| Daily | Review PnL reports, check reconciler logs |
| Weekly | Review LLM usage, check disk space |
| Monthly | Update dependencies, review model performance |
| Quarterly | Security audit, infrastructure review |

---

## 13. Current Status

### 13.1 Completed Features ✅

| Category | Feature | Status |
|----------|---------|--------|
| **Architecture** | 3-layer separation | ✅ Complete |
| **Streaming** | Kafka + Schema Registry + Avro | ✅ Complete |
| **Database** | TimescaleDB with 12 hypertables | ✅ Complete |
| **ML** | TFT training + ONNX inference | ✅ Complete |
| **ML** | Model registry (candidate/green/deprecated) | ✅ Complete |
| **Trading** | Alpaca paper trading integration | ✅ Complete |
| **Trading** | 3-layer safety gates | ✅ Complete |
| **Trading** | Position sizing and limits | ✅ Complete |
| **Trading** | Execution throttles | ✅ Complete |
| **Trading** | Exit logic (stop-loss, take-profit) | ✅ Complete |
| **Reconciliation** | Order fill synchronization | ✅ Complete |
| **Reconciliation** | Position truth from Alpaca | ✅ Complete |
| **Reconciliation** | Docker-based reconciler service | ✅ Complete |
| **Reporting** | Daily PnL computation | ✅ Complete |
| **Observability** | LLM tracing + database logging | ✅ Complete |
| **Observability** | Trade chain traceability | ✅ Complete |
| **Orchestration** | SuperAGI integration | ✅ Complete |
| **LLM** | GPU-accelerated Ollama (291 tok/s) | ✅ Complete |
| **Testing** | 40+ unit tests passing | ✅ Complete |

### 13.2 Metrics Summary

| Metric | Value |
|--------|-------|
| **Lines of Python** | ~15,000+ |
| **Database Tables** | 12 |
| **Kafka Topics** | 9 |
| **Docker Services** | 14 |
| **Unit Tests** | 40+ |
| **Documentation Pages** | 5+ |

---

## 14. Roadmap & Next Steps

### 14.1 Short-Term (Next 30 Days)

| Priority | Task | Effort |
|----------|------|--------|
| 🔴 High | Create real Grafana dashboards | 2 days |
| 🔴 High | Add Kafka lag alerting | 1 day |
| 🟡 Medium | Implement backtesting framework | 5 days |
| 🟡 Medium | Add slippage modeling | 2 days |
| 🟢 Low | Multi-strategy tagging | 2 days |

### 14.2 Medium-Term (Next 90 Days)

| Priority | Task | Effort |
|----------|------|--------|
| 🔴 High | Production Kubernetes deployment | 10 days |
| 🔴 High | CI/CD pipeline | 5 days |
| 🟡 Medium | Advanced risk models (VaR, stress testing) | 5 days |
| 🟡 Medium | Bracket orders (OCO) at broker level | 3 days |
| 🟢 Low | Options trading support | 10 days |

### 14.3 Long-Term (6+ Months)

| Priority | Task | Description |
|----------|------|-------------|
| 🔴 High | Live trading readiness | Final audit, security review |
| 🟡 Medium | Multi-asset expansion | Crypto, futures |
| 🟡 Medium | Real-time news integration | Sentiment analysis |
| 🟢 Low | Mobile monitoring app | React Native dashboard |

---

## 15. Appendix: Quick Reference

### 15.1 URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | - |
| Kafka UI | http://localhost:8080 | - |
| SuperAGI | http://localhost:3001 | - |
| Schema Registry | http://localhost:8081 | - |

### 15.2 Database Connection

```bash
# Connect to TimescaleDB
PGPASSWORD=awet psql -h localhost -p 5433 -U awet -d awet

# Quick queries
SELECT * FROM trades ORDER BY ts DESC LIMIT 10;
SELECT * FROM positions;
SELECT * FROM daily_pnl_summary ORDER BY report_date DESC;
```

### 15.3 Environment Variables

```bash
# Required in .env
ALPACA_API_KEY=your_key
ALPACA_API_SECRET=your_secret
POLYGON_API_KEY=your_key        # Optional for historical data

# Optional
LLM_BASE_URL=http://localhost:11434/v1
LLM_MODEL=llama3.2:3b
```

### 15.4 Directory Structure

```
/home/kironix/Awet/
├── config/              # Configuration files
├── db/                  # Database schema
├── directives/          # SOP documents
├── execution/           # One-shot scripts
├── grafana/             # Grafana provisioning
├── logs/                # Log files
├── models/              # Trained ML models
├── prometheus/          # Prometheus config
├── scripts/             # Operational scripts
├── src/                 # Source code
│   ├── agents/          # Long-running services
│   ├── audit/           # Audit trail
│   ├── core/            # Shared utilities
│   ├── features/        # Feature engineering
│   ├── integrations/    # External APIs
│   ├── ml/              # Machine learning
│   ├── models/          # Pydantic models
│   ├── monitoring/      # Metrics
│   ├── schemas/         # Avro schemas
│   └── streaming/       # Kafka utilities
├── superagi/            # SuperAGI integration
├── tests/               # Test suite
├── docker-compose.yml   # Service definitions
├── Makefile             # Common commands
└── pyproject.toml       # Python dependencies
```

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-16 | Engineering | Initial comprehensive document |

---

**For questions or updates, contact the Engineering Team.**
