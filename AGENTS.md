# Agent Instructions

> This file is mirrored across `CLAUDE.md`, `AGENTS.md`, and `GEMINI.md` so the same instructions load in any AI environment.

You operate within a 3-layer architecture that separates concerns to maximize reliability for an **institutional-grade, audit-ready, paper-trading platform**. LLMs are probabilistic, whereas trading logic must be deterministic, replayable, and safe. This system fixes that mismatch.

## The 3-Layer Architecture

**Layer 1: Directive (What to do)**
- SOPs written in Markdown, live in `directives/`
- Each directive MUST define:
  - Goal + success criteria
  - Inputs (topics, symbols, time ranges, configs)
  - Tools/scripts/services to use
  - Outputs (Kafka topics, DB tables, artifacts)
  - Edge cases + failure modes + rollback/replay steps
- Natural language instructions, like you'd give a mid-level engineer

**Layer 2: Orchestration (Decision making)**
- This is the orchestrator — can be:
  - **SuperAGI** (autonomous, runs in Docker via `make superagi-up`)
  - **GitHub Copilot / Claude / GPT** (interactive, via chat)
  - Any LLM agent framework
- Your job: intelligent routing
- Read directives, call execution tools in the right order, handle errors, request clarification when required, and update directives with learnings.
- You are the glue between intent and execution. Example:
  - Read `directives/trading_pipeline.md`
  - Trigger deterministic actions via Tool Gateway (preferred) or scripts in `execution/`
  - Coordinate agents: ingestion → features → prediction → risk → execution → audit
- Orchestration rules:
  - Do NOT run streaming loops inside the orchestrator.
  - Do NOT place trades directly.
  - Only trigger deterministic scripts/services that implement the real logic.
  - Propagate a single `correlation_id` across the full chain.

### SuperAGI Setup (Optional Autonomous Mode)
```bash
# Start SuperAGI for autonomous orchestration
make superagi-up

# GUI available at http://localhost:3001
# Configure API keys in .env:
#   OPENAI_API_KEY=sk-...
#   ANTHROPIC_API_KEY=sk-ant-...

# Custom AWET tools available:
#   - awet_read_directive
#   - awet_run_backfill
#   - awet_train_model
#   - awet_promote_model
#   - awet_approve_execution
#   - awet_revoke_execution
#   - awet_run_demo
#   - awet_check_pipeline_health
#   - awet_query_audit_trail
#   - awet_check_kafka_lag
#   - awet_generate_plan      # LLM-powered: creates execution plan from directive
#   - awet_explain_run        # LLM-powered: explains pipeline run from audit trail
```

### SuperAGI as Layer 2 Orchestrator

SuperAGI acts as the autonomous orchestration layer, calling AWET tools to coordinate the pipeline. The architecture ensures that **SuperAGI never touches Kafka, TimescaleDB, or trading logic directly**—it only calls HTTP endpoints exposed by the Tool Gateway.

**Available AWET Tools:**

| Tool | Description | Endpoint |
|------|-------------|----------|
| `awet_read_directive` | Read SOP from directives/ | GET |
| `awet_run_backfill` | Ingest historical data | POST |
| `awet_train_model` | Train TFT model | POST |
| `awet_promote_model` | Promote model to green | POST |
| `awet_approve_execution` | Enable paper trading | POST |
| `awet_revoke_execution` | Disable paper trading | POST |
| `awet_run_demo` | Run pipeline demo | POST |
| `awet_check_pipeline_health` | Check agent health | GET |
| `awet_check_kafka_lag` | Check consumer lag | GET |
| `awet_query_audit_trail` | Query audit events | POST |
| `awet_run_live_ingestion` | Daily data from yfinance | POST |
| `awet_generate_plan` | LLM-powered plan generation | POST |
| `awet_explain_run` | LLM-powered run explanation | POST |

**Quick Start:**

```bash
# 1. Start infrastructure
make up

# 2. Start SuperAGI
make superagi-up

# 3. Start Tool Gateway (in separate terminal)
make tool-gateway

# 4. Register AWET tools
make superagi-register-tools

# 5. Open http://localhost:3001 and create "AWET Orchestrator" agent
#    See superagi/agents/pipeline_orchestrator.yaml for template
```

### Programmatic Agent Registration (Recommended)

Create or update the "AWET Orchestrator" agent without using the GUI:

```bash
SUPERAGI_BASE_URL=http://localhost:3001/api \
SUPERAGI_API_KEY=YOUR_TOKEN_HERE \
make superagi-register-awet-agent
```

Then open http://localhost:3001 → Agents → **AWET Orchestrator** → Run.

### SuperAGI Agent Team (Optional)

Register a team of specialized agents for scheduling:

- **AWET Orchestrator** — interactive, manual orchestration
- **AWET Night Trainer** — nightly backfill + training
- **AWET Morning Deployer** — morning health check + smoke test
- **AWET Trade Watchdog** — periodic health monitoring

```bash
export SUPERAGI_API_KEY=...
export SUPERAGI_BASE_URL=http://localhost:3001/api
make superagi-register-agents
```

Open each agent in the SuperAGI UI to configure schedules (e.g., night, morning, every 15 minutes).

**Creating the AWET Orchestrator Agent in UI:**

1. Open http://localhost:3001
2. Go to **Agents** → **+ Create Agent**
3. Set **Name**: `AWET Orchestrator`
4. Set **Model**: Your LLM (gpt-4 or local Llama)
5. Select **Tools**: All `awet_*` tools from the toolkit
6. Set **Goal**: "Run the full AWET trading pipeline end-to-end"
7. Click **Create Agent** → **Run**
8. View progress in **Runs** tab and APM dashboard

### Local LLM Integration
The orchestration layer can use a local Llama 3.1 70B model (or similar) for planning and explanation:

```bash
# Test local LLM connectivity
make llm-test

# Generate an execution plan from a directive
make llm-plan
# or: python -m execution.generate_plan trading_pipeline --context '{"symbol": "AAPL"}'

# Explain the latest pipeline run
make llm-explain
# or: python -m execution.explain_run --id <correlation_id>
```

Configuration in `config/llm.yaml` with env overrides:
- `LLM_BASE_URL` (default: `http://localhost:11434/v1`)
- `LLM_MODEL` (default: `llama-3.1-70b-instruct-q4_k_m`)

**Layer 3: Execution (Doing the work)**
- Deterministic Python scripts in `execution/` and long-running services in `src/agents/`
- Environment variables and API tokens are stored in `.env` (never committed)
- Handle:
  - Kafka produce/consume (Avro + Schema Registry)
  - TimescaleDB writes (audit trail)
  - Feature engineering and model inference
  - Risk gates and execution simulation (paper only)
- Execution MUST be reliable, testable, restart-safe, and idempotent.

**Why this works:** if you do everything yourself, errors compound. In trading pipelines, compounding errors can become compounding losses. Push complexity into deterministic code so orchestration stays thin, auditable, and safe.

---

## Trading-Specific Non-Negotiables

### 1) Event contracts are law
All inter-service communication uses Kafka topics with Avro schemas. Every event MUST include:
- `event_id` (UUID)
- `correlation_id` (UUID)
- `idempotency_key` (string, stable across retries)
- `symbol` (string)
- `ts` (UTC timestamp)
- `schema_version` (int)
- `source` (service name)

Schemas live in `src/schemas/` and are registered in Schema Registry. Backward compatibility must be preserved.

### 2) Idempotency everywhere
- All consumers/scripts must be safe to retry
- Use `idempotency_key` to dedupe side effects (DB writes, emitted events)
- Prefer UPSERTs and unique constraints in TimescaleDB audit tables
- Kafka consumers must be restart-safe (commit after successful processing)

### 3) Safety gates (paper trading only)
- Default mode is PAPER trading only
- ExecutionAgent must refuse to execute unless an approval token/file exists
- No trade executes unless:
  - Prediction signal exists and validates
  - Risk approves (with logged reasons)
  - Approval gate passes
- No automatic “real broker” integration in this repo

### 4) Traceability and audit
- Every decision must be traceable end-to-end:
  `market.raw → market.engineered → predictions.tft → risk.approved/rejected → execution.completed → audit`
- Structured logs (structlog JSON) MUST include `correlation_id` in every log line
- Audit trail must be written to TimescaleDB for each stage (at minimum: ingestion, prediction, risk decision, execution result)
- The system should support replay-by-time-window using Kafka offsets/time filters + audit tables

### 5) Observability
- Every agent exposes:
  - `/health` endpoint
  - `/metrics` Prometheus endpoint
- Metrics must include at least:
  - success_count, failure_count
  - latency histogram
  - per-topic produced/consumed counters
  - consumer lag monitoring (Watchtower)

---

## Operating Principles

**1. Check for tools first**
Before writing a script, check `execution/` per the directive. Only create new scripts if none exist.

**2. Self-anneal when things break**
- Read the error message and stack trace
- Fix the script/service and test again
- Update the directive with what you learned (API limits, timing, edge cases, recovery steps)
- Example: rate limit → implement token bucket/backoff in deterministic code → add tests → update directive

If a fix would cause paid external usage (tokens/credits) or sensitive side effects, request user approval first.

**3. Update directives as you learn**
Directives are living documents. When you discover constraints, better approaches, common errors, or expected timing—update the directive.
Do NOT create or overwrite directives unless asked. Improve existing ones incrementally.

---

## Self-annealing loop

Errors are learning opportunities. When something breaks:
1. Fix it in deterministic code
2. Update the tool/script/service
3. Test it locally
4. Update the directive with the new flow and edge cases
5. System becomes stronger and more reliable

---

## File Organization

**Deliverables vs Intermediates:**
- **Deliverables**: Kafka topics + TimescaleDB audit tables + dashboards + reports
- **Intermediates**: Temporary artifacts needed during processing

**Directory structure:**
- `.tmp/` — intermediate files (never commit; always regenerable)
- `execution/` — deterministic tools/scripts (short-lived)
- `src/agents/` — long-running deterministic services
- `directives/` — SOPs in Markdown (instruction set)
- `config/` — YAML configs (env overrides allowed)
- `.env` — env vars + API keys (never commit)

**Key principle:** Local files are only for processing. The system’s truth lives in Kafka + TimescaleDB + metrics/logs. Everything in `.tmp/` can be deleted and regenerated.

---

## Webhooks (Optional)
If using webhooks, each webhook maps to exactly one directive and only calls whitelisted deterministic tools.
(If your project is not using Modal webhooks, omit this section.)

---

## Summary

You sit between human intent (directives) and deterministic execution (Python services and scripts). Read instructions, make decisions, call tools, handle errors, continuously improve the system.

Be pragmatic. Be reliable. Self-anneal.

Also: avoid hard-coding any specific model name inside project rules—keep the system model-agnostic so it stays future-proof.
