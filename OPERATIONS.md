# AWET Operations Runbook

> **Paper Trading Only** – This system is safety-gated. No real money trades are executed.

This document explains how to operate the AWET platform day-to-day.

---

## Portal URLs

## Local Portals

| Portal | URL | Credentials | Purpose |
|--------|-----|-------------|---------|
| **Grafana** | http://localhost:3000 | admin/admin | Dashboards & visualization |
| **Prometheus** | http://localhost:9090 | - | Metrics & queries |
| **Alertmanager** | http://localhost:9093 | - | Alert management |
| **Kafka UI** | http://localhost:8080 | - | Topic browser, consumer groups |
| **Schema Registry** | http://localhost:8081 | - | Avro schema management |
| **SuperAGI** | http://localhost:3001 | - | AI orchestration GUI |
| **Ollama** | http://localhost:11434 | - | LLM API (local) |
| **TimescaleDB** | localhost:5433 | awet/awet | PostgreSQL + time-series |

---

Access all monitoring and management portals:

| Portal | URL | Credentials | Purpose |
|--------|-----|-------------|---------|
| **Grafana** | http://localhost:3000 | admin/admin | Dashboards & visualization |
| **Prometheus** | http://localhost:9090 | - | Metrics & queries |
| **Alertmanager** | http://localhost:9093 | - | Alert management |
| **Kafka UI** | http://localhost:8080 | - | Topic browser, consumer groups |
| **Schema Registry** | http://localhost:8081 | - | Avro schema management |
| **SuperAGI** | http://localhost:3001 | - | AI orchestration GUI |
| **Ollama** | http://localhost:11434 | - | LLM API (local) |
| **TimescaleDB** | localhost:5433 | awet/awet | PostgreSQL + time-series |

**Quick command:** `make portals` to print all URLs.

## Monitoring Access

| Name | Local URL | Default Login | Used For | Verify |
|------|-----------|---------------|----------|--------|
| Grafana | http://localhost:3000 | admin/admin | Dashboards & visualization | `curl -s http://localhost:3000/api/health` |
| Prometheus | http://localhost:9090 | - | Metrics scrape & queries | `curl -s http://localhost:9090/-/healthy` |
| Alertmanager | http://localhost:9093 | - | Alert routing & silences | `curl -s http://localhost:9093/-/healthy` |
| Kafka UI | http://localhost:8080 | - | Kafka topics/consumers | Open in browser |
| Agent metrics | http://localhost:8001/metrics .. http://localhost:8006/metrics | - | Agent Prometheus metrics (if agents running) | Open in browser |

**Grafana login / password reset**
- Default: admin/admin
- If changed, check logs for the generated password:
   ```bash
   docker logs grafana 2>&1 | grep -i "password\|admin"
   ```
- If you set `GF_SECURITY_ADMIN_PASSWORD`, use that value.

**Prometheus target verification**
- http://localhost:9090/targets → ensure Prometheus, Alertmanager, and agent jobs are **UP**.

**Optional system metrics (not in this compose)**
- cAdvisor and node-exporter are not defined in [docker-compose.yml](docker-compose.yml).
- If you run a separate monitoring stack, expose cAdvisor on 8080 and node-exporter on 9100.

**Grafana dashboards empty?**
1. Confirm Prometheus datasource exists and is default in Grafana → Connections → Data sources.
2. Use Prometheus internal URL in Docker: `http://prometheus:9090`.
3. Check Prometheus targets are UP: http://localhost:9090/targets or `make monitoring-status`.

### Monitoring Access Guide

**Login to Grafana**
- URL: http://localhost:3000
- Default credentials: admin/admin
- If the password is different, check container logs:
   ```bash
   docker logs grafana 2>&1 | grep -i "password\|admin"
   ```
   If you set `GF_SECURITY_ADMIN_PASSWORD` or provisioned secrets, use that value instead.

**Confirm Prometheus is scraping node-exporter + cAdvisor**
- Open http://localhost:9090 → Status → Targets
- Ensure `node-exporter` and `cadvisor` show **UP**
- Or run: `make monitoring-status`

**Troubleshooting: Grafana loads but dashboards are empty**
1. Check the Prometheus datasource exists and is default in Grafana → Connections → Data sources.
2. Verify Prometheus is reachable from Grafana (use the datasource “Save & test”).
3. If running in Docker, use the internal URL for Prometheus (e.g., `http://prometheus:9090`) instead of localhost.
4. Confirm Prometheus targets are UP: http://localhost:9090/targets or `make monitoring-status`.

### Agent Health Endpoints (when running locally)

| Agent | Port | Health URL |
|-------|------|------------|
| DataIngestion | 8001 | http://localhost:8001/health |
| FeatureEngineering | 8002 | http://localhost:8002/health |
| Prediction | 8003 | http://localhost:8003/health |
| Risk | 8004 | http://localhost:8004/health |
| Execution | 8005 | http://localhost:8005/health |
| Watchtower | 8006 | http://localhost:8006/health |

---

## Quick Reference

| Task | Command |
|------|---------|
| Start everything | `./scripts/awet_start.sh` |
| Stop everything | `./scripts/awet_stop.sh` |
| System diagnostics | `make doctor` |
| Health check | `.venv/bin/python scripts/awet_health_check.py` |
| Portal URLs | `make portals` |
| Watch logs | `tail -f logs/*.log` |
| Trigger agent manually | `.venv/bin/python scripts/schedule_agents.py <agent>` |
| SuperAGI UI | http://localhost:3001 |
| Grafana | http://localhost:3000 (admin/admin) |

---

## 1. Starting the System

### After Reboot (Manual)

```bash
cd /home/kironix/Awet
./scripts/awet_start.sh
```

This script:
1. Starts the Docker stack (Kafka, TimescaleDB, Redis, Prometheus, Grafana)
2. Starts SuperAGI (backend, celery, GUI, proxy)
3. Waits for all services to be healthy
4. Prints "SYSTEM READY" when done

### Auto-Start (via Cron)

If you installed the `@reboot` cron job, the system starts automatically after a reboot:

```
@reboot cd /home/kironix/Awet && ./scripts/awet_start.sh >> logs/startup.log 2>&1
```

Check if it's installed:
```bash
crontab -l | grep @reboot
```

---

## 2. Stopping the System

### Graceful Shutdown

```bash
cd /home/kironix/Awet
./scripts/awet_stop.sh
```

### Shutdown + Remove Volumes (Destructive)

```bash
./scripts/awet_stop.sh --clean
```

⚠️ This removes all data. Only use when you want to start fresh.

### Managing Multiple Compose Projects

AWET may run alongside other Docker Compose projects (monitoring-stack, superagi, ollama, etc.). Use the following to identify which containers belong to which project:

```bash
# List all containers grouped by Compose project
make stack-list

# Or run the script directly
python scripts/list_compose_projects.py
```

Example output:
```
================================================================================
🐳 Docker Containers by Compose Project
================================================================================

⭐ Project: awet
   Services: 7
   Stop with: docker compose down  (from repo root)
--------------------------------------------------------------------------------
   SERVICE              CONTAINER                 STATUS                    PORTS
--------------------------------------------------------------------------------
   kafka                awet-kafka-1              🟢 Up 2 hours             9092->9092/tcp
   schema-registry      awet-schema-registry-1    🟢 Up 2 hours             8081->8081/tcp
   timescaledb          timescaledb               🟢 Up 2 hours             5433->5432/tcp
   ...

📦 Project: monitoring-stack
   Services: 3
   Stop with: docker compose -p monitoring-stack down
--------------------------------------------------------------------------------
   ...
```

#### Stop Only AWET Containers

```bash
# From the AWET repo root
docker compose down

# Or explicitly by project name
docker compose -p awet down
```

#### Stop Other Projects Without Affecting AWET

```bash
# Stop monitoring-stack only
docker compose -p monitoring-stack down

# Stop superagi only
docker compose -p superagi down

# Stop a demo project
docker compose -p demo down
```

#### Identifying Container Ownership

Docker labels tell you which project owns a container:
- `com.docker.compose.project` - The compose project name
- `com.docker.compose.service` - The service name within that project

```bash
# Inspect a specific container's project
docker inspect <container> --format '{{index .Config.Labels "com.docker.compose.project"}}'
```

---

## 3. Health Check

### Run the Health Check

```bash
cd /home/kironix/Awet
source .venv/bin/activate
python scripts/awet_health_check.py
```

### Expected Output (Healthy)

```
==================================================
🏥 AWET System Health Check
==================================================

✅ TimescaleDB: Connected successfully
✅ Kafka: 9 topics available
✅ SuperAGI API: Responding on http://localhost:8100
✅ SuperAGI Agents: All 4 agents found
✅ Ollama: Ollama is running

--------------------------------------------------
✅ All 5 checks passed - System is HEALTHY
```

### Interpreting Results

| Check | What It Means |
|-------|---------------|
| **TimescaleDB** | Database is reachable and accepting connections |
| **Kafka** | Message broker is running and has topics created |
| **SuperAGI API** | Orchestration API is responding |
| **SuperAGI Agents** | All 4 agents (Orchestrator, Night Trainer, Morning Deployer, Trade Watchdog) exist |
| **Ollama** | LLM service is running for agent reasoning |

### JSON Output (for scripts)

```bash
python scripts/awet_health_check.py --json
```

Exit code: `0` = healthy, `1` = unhealthy

---

## 4. Scheduled Agents (Cron)

These agents run automatically via cron:

| Agent | Schedule | Purpose |
|-------|----------|---------|
| **Night Trainer** | 02:00 Mon–Fri | Backfill data, train TFT models |
| **Morning Deployer** | 08:30 Mon–Fri | Health check, promote models to production |
| **Trade Watchdog** | Every 15 min (09:00–16:00) Mon–Fri | Monitor pipeline during market hours |

### View Installed Cron Jobs

```bash
crontab -l
```

### Trigger an Agent Manually

```bash
cd /home/kironix/Awet
source .venv/bin/activate
export SUPERAGI_API_KEY="your-api-key"

# Trigger any agent
python scripts/schedule_agents.py orchestrator
python scripts/schedule_agents.py night_trainer
python scripts/schedule_agents.py morning_deployer
python scripts/schedule_agents.py trade_watchdog

# Dry run (see what would happen)
python scripts/schedule_agents.py night_trainer --dry-run
```

---

## 5. Logs

### Log Locations

| Log | Location |
|-----|----------|
| Night Trainer | `logs/night_trainer.log` |
| Morning Deployer | `logs/morning_deployer.log` |
| Trade Watchdog | `logs/trade_watchdog.log` |
| Startup (if using @reboot) | `logs/startup.log` |

### Watch Logs in Real-Time

```bash
tail -f /home/kironix/Awet/logs/*.log
```

### Watch a Specific Agent

```bash
tail -f /home/kironix/Awet/logs/trade_watchdog.log
```

### Docker Container Logs

```bash
# All containers
docker compose logs -f

# Specific service
docker compose logs -f superagi-backend
docker compose logs -f kafka
```

---

## 6. SuperAGI Web UI

### Access

Open: **http://localhost:3001**

### Key Tabs

| Tab | What It Shows |
|-----|---------------|
| **Agents** | List of all agents, click to see config/runs |
| **APM** | All agent runs, tool calls, timing |
| **Runs** | History of all executions |
| **Toolkits** | Registered AWET tools |

### Verify Agent Runs

1. Go to **APM** tab
2. You should see runs for:
   - AWET Orchestrator
   - AWET Night Trainer
   - AWET Morning Deployer
   - AWET Trade Watchdog
3. Click a run to see tool calls and results

---

## 7. Troubleshooting

### System Won't Start

```bash
# Check Docker
docker compose ps

# Check for port conflicts
sudo lsof -i :3001  # SuperAGI
sudo lsof -i :5433  # TimescaleDB
sudo lsof -i :9092  # Kafka
```

### Health Check Fails

| Failure | Fix |
|---------|-----|
| TimescaleDB unhealthy | `docker compose restart timescaledb` |
| Kafka unhealthy | `docker compose restart kafka` |
| SuperAGI API unhealthy | `docker compose restart superagi-backend superagi-celery` |
| Agents missing | Re-register: `make superagi-register-agents` |
| Ollama unhealthy | Check: `curl http://localhost:11434/` |

### Agent Run Stuck

1. Check SuperAGI celery logs: `docker compose logs superagi-celery`
2. Check Ollama is responding: `curl http://localhost:11434/`
3. Restart SuperAGI: `make superagi-down && make superagi-up`

---

## 8. Paper Trading Safety

⚠️ **Paper Trading Only** — This system is designed for **simulation only**.

### 8.1 Safety Gates in the Code

| Safety Layer | Location | Description |
|--------------|----------|-------------|
| **AlpacaClient Validation** | [src/integrations/alpaca_client.py](src/integrations/alpaca_client.py) | Blocks any non-paper endpoint at initialization |
| **Alpaca Endpoint** | `.env` → `ALPACA_BASE_URL` | Must be `https://paper-api.alpaca.markets` |
| **Dry-Run Mode** | `config/app.yaml` → `execution_dry_run` | When `true` (default), ALL trades blocked |
| **Approval File** | `.tmp/APPROVE_EXECUTION` | Must exist for trades to execute |
| **ExecutionAgent Logic** | [src/agents/execution_agent.py](src/agents/execution_agent.py) | All trades have `paper_trade=True` hardcoded |

### 8.2 Quick Start: Enable/Disable Paper Trading

**One-command controls:**

```bash
# Enable paper trading (sets dry_run=false, creates approval file, restarts agents)
make paper-on

# Disable paper trading (sets dry_run=true, removes approval file)
make paper-off

# Check current status
make paper-status

# Verify Alpaca credentials are configured
make verify-alpaca
```

**Manual controls:**

```bash
# APPROVE execution only (keeps current dry_run setting)
make approve

# REVOKE execution only
make revoke
```

### 8.3 Approval Gate Implementation

The ExecutionAgent implements a two-gate safety system:

```python
# Gate 1: Dry-run mode (config/app.yaml)
dry_run = self.settings.app.execution_dry_run  # default: True

# Gate 2: Approval file
approval_file_exists = os.path.exists(".tmp/APPROVE_EXECUTION")

# Both gates must pass for trades to execute
should_trade = (not dry_run) and approval_file_exists
```

| `execution_dry_run` | Approval File | Result |
|--------------------|---------------|--------|
| `true` (default)   | exists        | ❌ BLOCKED |
| `true`             | missing       | ❌ BLOCKED |
| `false`            | missing       | ❌ BLOCKED |
| `false`            | exists        | ✅ TRADES EXECUTE |

SuperAGI agents can also control approval via tools:
- `awet_approve_execution` — creates the approval file
- `awet_revoke_execution` — removes the approval file

### 8.4 Alpaca Paper Trading Integration

The ExecutionAgent now submits **real paper trades** to Alpaca's Paper Trading API when approved.

#### Safety Guarantees

| Layer | Protection |
|-------|------------|
| **AlpacaClient** | Refuses to start if `ALPACA_BASE_URL` is not `paper-api.alpaca.markets` |
| **Environment** | `.env` hardcoded to paper endpoint |
| **Dry-Run Mode** | `execution_dry_run: true` (default) blocks all API calls |
| **Approval File** | Must run `make approve` to enable trading |

#### Order Flow

```
RiskEvent → dry_run check → approval file check → AlpacaClient.submit_market_order() → ExecutionEvent
```

If `dry_run=True` OR approval file missing:
- No Alpaca API call is made
- Status = "blocked"
- Published to `execution.blocked` topic

If approved (`dry_run=False` AND approval file exists):
- MARKET order submitted to Alpaca Paper API
- Order ID and status recorded in ExecutionEvent
- Published to `execution.completed` topic

#### ExecutionEvent Fields

| Field | Description |
|-------|-------------|
| `alpaca_order_id` | Alpaca order UUID (if submitted) |
| `alpaca_status` | Order status: accepted, filled, rejected, simulated |
| `error_message` | Error details if order failed |
| `side` | "buy" or "sell" |
| `dry_run` | True if blocked by dry-run mode |

#### Alpaca Client Location

- **Module:** [src/integrations/alpaca_client.py](src/integrations/alpaca_client.py)
- **Unit Tests:** [tests/unit/test_alpaca_client.py](tests/unit/test_alpaca_client.py)

#### Enable Paper Trading

```bash
# Step 1: Disable dry-run mode in config/app.yaml
# Set: execution_dry_run: false

# Step 2: Create approval file
make approve

# Step 3: Run the demo
make demo

# Step 4: Check trades
make trades
```

#### Disable Paper Trading

```bash
# Remove approval file
make revoke

# Or set dry_run: true in config (safest)
```

### 8.5 Execution Throttles

The ExecutionAgent enforces **rate limiting and cooldowns** to prevent spammy or duplicate orders. All limits are configurable in `config/app.yaml`.

#### Configuration

```yaml
# config/app.yaml → execution section

# Rate limiting: max orders per time window
max_orders_per_minute: 5          # Max orders across all symbols per minute
max_orders_per_symbol_per_day: 3  # Max orders for same symbol per day
max_open_orders_total: 20         # Max pending/open orders at any time

# Cooldown: min seconds between trades on same symbol
cooldown_seconds_per_symbol: 900  # 15 minutes default
```

#### How Throttles Work

When a RiskEvent is received, the ExecutionAgent checks **before calling Alpaca**:

1. **Duplicate check**: Is this `idempotency_key` already in the trades table?
   - If yes → Block with reason `duplicate_idempotency_key`

2. **Rate limit**: How many orders placed in the last minute?
   - If ≥ `max_orders_per_minute` → Block with reason `rate_limit`

3. **Per-symbol daily limit**: How many orders for this symbol today?
   - If ≥ `max_orders_per_symbol_per_day` → Block with reason `max_symbol_orders`

4. **Max open orders**: How many orders are pending (not yet filled/canceled)?
   - If ≥ `max_open_orders_total` → Block with reason `max_open_orders`

5. **Cooldown**: When was the last trade on this symbol?
   - If < `cooldown_seconds_per_symbol` ago → Block with reason `cooldown`

#### Blocked Trade Behavior

When a trade is throttled:
- **No Alpaca API call** is made (saves rate limits)
- Trade is persisted to `trades` table with `status='blocked'`
- `error_message` contains the throttle reason
- Event is published to `execution.blocked` topic

#### Tuning Throttles

| Scenario | Adjustment |
|----------|------------|
| More frequent trading | Increase `max_orders_per_minute` |
| Same symbol more often | Increase `max_orders_per_symbol_per_day` |
| Shorter wait between symbol trades | Decrease `cooldown_seconds_per_symbol` |
| Allow more pending orders | Increase `max_open_orders_total` |
| Conservative paper trading | Keep defaults or lower limits |

#### Viewing Throttled Trades

```bash
# View recent blocked trades
make trades STATUS=blocked

# Or query directly
psql -h localhost -p 5433 -U awet -d awet \
  -c "SELECT ts, symbol, error_message FROM trades WHERE status='blocked' ORDER BY ts DESC LIMIT 10;"
```

### 8.6 Portfolio Truth & Exposure Caps

The system maintains a local `positions` table that mirrors Alpaca positions. This enables:
- **Fast SELL validation** without API calls
- **Exposure cap enforcement** to prevent over-concentration
- **Portfolio visibility** via `make portfolio`

#### Reconciling Positions

Sync positions from Alpaca to the local database:

```bash
# One-shot reconciliation
make positions

# Continuous sync every 60 seconds
make positions-watch

# View current portfolio from DB
make portfolio
```

#### How Position Reconciliation Works

1. Fetches all open positions from Alpaca Paper API
2. Upserts each position into the `positions` table
3. Zeros out any symbols no longer held (sold to 0)
4. Prints a portfolio summary with P/L

#### Exposure Caps Configuration

```yaml
# config/app.yaml → execution section

# Max total portfolio exposure (sum of all position market values)
max_total_exposure_usd: 50000     # $50k total portfolio cap

# Max exposure per individual symbol
max_exposure_per_symbol_usd: 10000  # $10k per symbol cap
```

#### Exposure Cap Behavior

When a BUY order is received:
1. Calculate `intended_notional = current_price × qty`
2. Check if `current_total_exposure + intended_notional > max_total_exposure_usd`
   - If yes → Block with reason `exposure_cap`
3. Check if `current_symbol_exposure + intended_notional > max_exposure_per_symbol_usd`
   - If yes → Block with reason `symbol_exposure_cap`

SELL orders always pass exposure checks (they reduce exposure).

#### SELL Validation (DB-First)

When an order direction is SELL:
1. Check `positions` table for the symbol's qty
2. If `qty > 0` → Allow sell (up to available qty)
3. If `qty = 0` → Block sell (no position)
4. If symbol not in DB → Fallback to Alpaca API once

This eliminates API calls for known positions and catches stale data.

#### Bracket Orders (Take-Profit + Stop-Loss)

When `use_bracket_orders: true`, BUY signals submit a bracket order:
- Entry: market buy
- Take-profit: limit at $entry \times (1 + take\_profit\_pct)$
- Stop-loss: stop at $entry \times (1 - stop\_loss\_pct)$

Configuration (`config/app.yaml`):

```yaml
use_bracket_orders: true
take_profit_pct: 0.03
stop_loss_pct: 0.015
allow_add_to_position: false
```

If no reliable price is available, the system falls back to a standard market order and logs the reason.

#### Viewing Portfolio & Exposure

```bash
# Quick portfolio summary
make portfolio

# Direct SQL query
psql -h localhost -p 5433 -U awet -d awet -c "
SELECT symbol, qty, market_value, unrealized_pl, updated_at
FROM positions
WHERE qty > 0
ORDER BY market_value DESC;
"

# Total exposure
psql -h localhost -p 5433 -U awet -d awet -c "
SELECT 
  COUNT(*) as positions,
  SUM(market_value) as total_exposure,
  SUM(unrealized_pl) as total_pl
FROM positions WHERE qty > 0;
"
```

#### Daily Automated Operations

Run the reconciliation scheduler (orders every 60s, positions every 120s) during market hours:

```bash
# Continuous ops loop (market-hours aware)
python scripts/reconcile_scheduler.py --watch

# One-shot status snapshot
make ops-snapshot

# Tail reconcile/execution logs
make ops-watch
```

Scheduler behavior:
- Uses Alpaca clock when available; otherwise falls back to local time.
- Logs `skipped_market_closed` when market is closed.
- Emits a daily portfolio summary at 16:10 local time.

Prometheus metrics:
- Exposed on port 9108 by default (set `OPS_METRICS_PORT` to override).
- Metrics: `reconcile_errors_total`, `pending_orders`, `open_positions`, `portfolio_value_usd`.

#### Systemd (auto-start on reboot)

Install and enable the ops service:

```bash
make ops-install
sudo systemctl enable --now awet-ops
```

Check logs/status:

```bash
make ops-logs
make ops-status
```

#### Upgrading an Existing DB (Bracket Orders)

If your DB already exists, run the migration and verify schema:

```bash
make db-migrate
python scripts/check_schema.py
```

### 8.7 Exit Logic v1 (Automatic Position Closing)

AWET includes automatic position exit logic that closes positions based on configurable thresholds.

#### Configuration (`config/app.yaml`)

```yaml
# EXIT LOGIC (automatic position closing)
enable_exit_logic: true      # Master switch for exit logic
take_profit_pct: 1.0         # Exit when profit >= +1.0%
stop_loss_pct: 0.5           # Exit when loss >= -0.5%
max_holding_minutes: 60      # Exit after 60 minutes regardless of PnL
exit_check_interval_seconds: 60  # Check positions every 60 seconds
```

#### Exit Reasons

| Reason | Trigger | Description |
|--------|---------|-------------|
| `take_profit` | `pnl_pct >= take_profit_pct` | Secure gains when target reached |
| `stop_loss` | `pnl_pct <= -stop_loss_pct` | Cut losses when threshold exceeded |
| `time_exit` | `holding_minutes >= max_holding_minutes` | Exit stale positions (day trading) |

Priority: `take_profit` > `stop_loss` > `time_exit`

#### Commands

```bash
# One-shot check: evaluate all positions once
make exit-run

# Continuous monitoring: check at interval
make exit-watch

# Enable/disable exit logic
make exit-on
make exit-off
```

#### Safety Gates

Exit logic respects the same safety gates as ExecutionAgent:
- **dry_run gate**: If `execution_dry_run: true`, exits are logged but not executed
- **approval file gate**: SELL orders only submitted if approval file exists

#### How It Works

1. **ExitAgent** reads positions from `positions` table (DB-first, Alpaca fallback)
2. For each position:
   - Fetch current price via Alpaca
   - Calculate PnL% = `(current_price - avg_entry_price) / avg_entry_price * 100`
   - Calculate holding time from entry timestamp
   - Check exit conditions (take_profit, stop_loss, time_exit)
3. If exit condition met and safety gates pass:
   - Submit SELL order via Alpaca
   - Persist trade record to `trades` table with exit reason
   - Update `positions` table

#### Viewing Exit Trades

```bash
# View all exit trades
psql -h localhost -p 5433 -U awet -d awet -c "
SELECT 
  ts, symbol, side, qty, price,
  CASE 
    WHEN error_message LIKE '%take_profit%' THEN 'take_profit'
    WHEN error_message LIKE '%stop_loss%' THEN 'stop_loss'
    WHEN error_message LIKE '%time_exit%' THEN 'time_exit'
  END as exit_reason,
  status, paper_trade
FROM trades
WHERE side = 'sell' AND error_message LIKE '%_exit%'
ORDER BY ts DESC
LIMIT 20;
"
```

#### Tuning Guidelines

| Parameter | Conservative | Moderate | Aggressive |
|-----------|--------------|----------|------------|
| `take_profit_pct` | 2.0 | 1.0 | 0.5 |
| `stop_loss_pct` | 1.0 | 0.5 | 0.25 |
| `max_holding_minutes` | 240 | 60 | 30 |

- **Conservative**: Wider thresholds, less churn, more time for positions to develop
- **Moderate**: Default settings, balanced approach
- **Aggressive**: Tight stops, quick takes, suitable for volatile markets

---

## 9. Monitoring LLM Traces

### Overview

AWET includes always-on LLM tracing. Every agent that calls the LLM (via `LLMClient`) automatically logs structured trace records with the `LLM_TRACE` prefix. This enables:

- **Real-time visibility**: Watch what agents ask the LLM during runtime
- **Historical analysis**: Query past requests/responses from TimescaleDB
- **Debugging**: Trace errors, latency issues, and unexpected responses
- **Cost tracking**: Monitor token usage and identify high-cost patterns

### Quick Commands

| Task | Command |
|------|---------|
| Last 20 traces | `make llm-last` |
| Real-time log stream | `make llm-watch` |
| Poll DB for new traces | `make llm-follow` |
| Daily summary stats | `make llm-summary` |
| Filter by agent | `make llm-agent AGENT=RiskAgent` |

### Viewing Traces in Real-Time

Watch the structured log output as agents call the LLM:

```bash
make llm-watch
# or: tail -f logs/*.log | grep LLM_TRACE
```

Example output:
```
LLM_TRACE agent_name=RiskAgent model=llama3.2:1b latency_ms=1234.56 status=ok \
  request_preview='{"messages": [{"role": "user", "content": "Analyze risk for AAPL..."}]}' \
  response_preview='{"content": "Risk assessment: LOW. Volatility within normal..."}'
```

### Querying from Database

Traces are persisted to `llm_traces` table in TimescaleDB:

```bash
# Last 20 traces
python scripts/show_llm_traces.py -n 20

# Filter by agent
python scripts/show_llm_traces.py --agent RiskAgent

# Filter by model
python scripts/show_llm_traces.py --model llama3.2:1b

# Filter by time range
python scripts/show_llm_traces.py --since "2026-01-16T08:00:00Z"

# Only errors
python scripts/show_llm_traces.py --status error

# JSON output for scripting
python scripts/show_llm_traces.py --json | jq '.[] | {ts, agent_name, status}'
```

### Daily Summary

View aggregated statistics:

```bash
make llm-summary
# or: python scripts/show_llm_traces.py --summary
```

Example output:
```
╔══════════════════════════════════════════════════════╗
║               LLM Daily Summary                       ║
╚══════════════════════════════════════════════════════╝
 Date:          2026-01-16
 Total Calls:   47
 Errors:        2 (4.3%)
 Avg Latency:   823 ms
 P95 Latency:   2341 ms
 Top Agents:    RiskAgent (18), WatchtowerAgent (12), ExecutionAgent (9)
 Top Models:    llama3.2:1b (45), gpt-4 (2)
```

### What's Captured

Each trace includes:

| Field | Description |
|-------|-------------|
| `ts` | Timestamp (UTC) |
| `correlation_id` | Request correlation ID for tracing |
| `agent_name` | Which agent made the call |
| `model` | LLM model used |
| `base_url` | LLM endpoint |
| `latency_ms` | Round-trip time |
| `status` | `ok` or `error` |
| `error_message` | Error details (if failed) |
| `request` | Full request JSON (JSONB) |
| `response` | Full response JSON (JSONB) |
| `prompt_tokens` | Input token count |
| `completion_tokens` | Output token count |

### Security & Redaction

Sensitive values are automatically redacted from log output:
- API keys (`sk-...`, `OPENAI_API_KEY=...`)
- Bearer tokens
- Email addresses
- Long numeric sequences (12+ digits)

⚠️ **Note**: The full request/response is stored in TimescaleDB unredacted for debugging purposes. Ensure database access is properly secured.

### Configuration

Edit `config/llm.yaml`:

```yaml
# Tracing configuration
trace_enabled: true       # Enable/disable tracing
trace_preview_chars: 800  # Chars shown in log preview
trace_log_level: INFO     # DEBUG, INFO, WARNING
trace_store_db: true      # Persist to TimescaleDB
```

Or via environment variables:
```bash
export LLM_TRACE_ENABLED=false  # Disable tracing
export LLM_TRACE_STORE_DB=false # Log only, no DB persistence
```

### Troubleshooting

**No traces appearing in logs:**
- Check `trace_enabled: true` in `config/llm.yaml`
- Ensure agents are using `LLMClient` (not direct HTTP calls)

**Traces in logs but not in DB:**
- Check `trace_store_db: true` in config
- Verify TimescaleDB is running: `docker compose ps`
- Check DB connectivity: `make verify-db`

**High latency in traces:**
- Check Ollama status: `curl http://localhost:11434/api/tags`
- Consider upgrading to a faster model or adding GPU

---

## 10. Tracing Trades Back to LLM Prompts

### Overview

Every trade in AWET carries a `correlation_id` that flows through the entire pipeline:

```
DataIngestion → FeatureEngineering → Prediction → Risk → Execution → Trade
      ↓                                              ↓
   audit_events                                  llm_traces
```

This enables you to answer: **"This Alpaca order came from this exact LLM prompt."**

### Quick Commands

| Task | Command |
|------|---------|
| Trace by correlation ID | `make trade-chain CORR=<uuid>` |
| Trace by Alpaca order ID | `make trade-chain ORDER=<alpaca_order_id>` |
| Trace by trade ID | `make trade-chain TRADE=<id>` |
| JSON output | `make trade-chain-json CORR=<uuid>` |

### Example: Trace a Trade

```bash
# You have an Alpaca order ID from the trades table or Alpaca dashboard
make trade-chain ORDER=abc123def456

# Or if you have the correlation_id
make trade-chain CORR=550e8400-e29b-41d4-a716-446655440000
```

Output:
```
======================================================================
 TRADE CHAIN TIMELINE
======================================================================

Correlation ID: 550e8400-e29b-41d4-a716-446655440000

  2026-01-16 14:30:12.123  📝 [AUDIT] market.raw AAPL
  2026-01-16 14:30:12.456  📝 [AUDIT] market.engineered AAPL
  2026-01-16 14:30:12.789  📝 [AUDIT] predictions.tft AAPL
  2026-01-16 14:30:13.012  🟢 [LLM  ] RiskAgent → llama3.2:1b (1234ms) | Analyze risk...
  2026-01-16 14:30:14.345  ✅ [RISK ] AAPL APPROVED (score=0.15) | Within limits
  2026-01-16 14:30:14.678  ✅ [TRADE] BUY 100 AAPL @ 175.50

======================================================================
 TRADE DETAILS
======================================================================

  Symbol:          AAPL
  Side:            BUY
  Quantity:        100
  Status:          filled ✅
  Fill Price:      175.50
  Alpaca Order ID: abc123def456
  Paper Trade:     True

======================================================================
 LLM TRACES
======================================================================

  [1] Agent: RiskAgent
      Model:   llama3.2:1b
      Latency: 1234ms
      Status:  ok 🟢
      
      PROMPT:
      Analyze risk for AAPL with volatility 2.5%, position size...
      
      RESPONSE:
      Risk assessment: LOW. Volatility within normal parameters...
```

### Using the Script Directly

```bash
# Full details with all flags
python scripts/show_trade_chain.py --correlation-id abc-123 --detail

# JSON for scripting
python scripts/show_trade_chain.py --alpaca-order-id xyz789 --json | jq '.llm_traces[0].request'

# Just the summary
python scripts/show_trade_chain.py -c abc-123 | grep "SUMMARY" -A 10
```

### SQL Queries (Advanced)

```sql
-- Find all LLM traces for a specific trade
SELECT lt.ts, lt.agent_name, lt.model, lt.latency_ms, 
       lt.request->'messages'->-1->>'content' as prompt_preview
FROM llm_traces lt
JOIN trades t ON lt.correlation_id = t.correlation_id
WHERE t.alpaca_order_id = 'your-order-id';

-- Find all events for a correlation ID
SELECT ts, 'trade' as source, symbol, status as detail FROM trades WHERE correlation_id = 'abc'
UNION ALL
SELECT ts, 'llm' as source, agent_name, status FROM llm_traces WHERE correlation_id = 'abc'
UNION ALL  
SELECT ts, 'audit' as source, event_type, NULL FROM audit_events WHERE correlation_id::text = 'abc'
ORDER BY ts;
```

### How Correlation ID Flows

1. **DataIngestion** generates a new `correlation_id` for each market tick
2. The ID is embedded in every event (Avro schema field)
3. Each agent calls `set_correlation_id(event.correlation_id)` before processing
4. `LLMClient.chat()` picks up the ID from context and includes it in traces
5. `ExecutionAgent` persists the ID to the `trades` table
6. You can now trace any trade back to its origin

### Debugging a Bad Trade

If a trade looks wrong:

1. Get the correlation_id from the trades table or Alpaca dashboard
2. Run `make trade-chain CORR=<id>`
3. Check the **LLM TRACES** section for the exact prompt/response
4. Verify the risk decision reasoning
5. Check audit events for data quality issues

---

## 11. Performance Reporting (PnL)

AWET includes daily PnL reporting to track trading performance.

### Quick Commands

```bash
# Today's PnL report
make pnl-today

# Last 7 days summary table
make pnl-week

# Watch mode (refresh every 60s)
make pnl-watch

# Save summary to database
make pnl-save

# Run end-of-day job (reconcile + compute + save)
make pnl-eod
```

### How to Read the Report

The daily report shows:

| Metric | Description |
|--------|-------------|
| **Realized PnL** | Profit/loss from completed round-trip trades (buy→sell) |
| **Unrealized PnL** | Current paper profit/loss on open positions |
| **Total PnL** | Realized + Unrealized |
| **Win Rate** | Percentage of profitable round-trips |
| **Avg Win/Loss** | Average dollar amount per winning/losing trade |
| **Max Drawdown** | Largest peak-to-trough decline in equity |
| **Best/Worst Symbol** | Best and worst performing symbols for the day |

### Round-Trip Matching

PnL is computed using FIFO (First-In-First-Out) matching:
- First BUY matches first SELL for the same symbol
- Partial fills are supported (5 of 10 shares sold = partial round-trip)
- Unmatched buys (open positions) contribute to unrealized PnL

### Database Table

The `daily_pnl_summary` table stores historical reports:

```sql
SELECT 
  report_date, 
  realized_pnl_usd, 
  unrealized_pnl_usd, 
  total_pnl_usd,
  win_rate,
  num_trades,
  best_symbol,
  worst_symbol
FROM daily_pnl_summary
ORDER BY report_date DESC
LIMIT 7;
```

### End-of-Day Job

Schedule `scripts/eod_pnl_job.py` to run at market close:

```bash
# Cron example (23:59 ET weekdays)
59 23 * * 1-5 /path/to/venv/bin/python /path/to/scripts/eod_pnl_job.py
```

The job:
1. Reconciles orders with Alpaca
2. Reconciles positions
3. Computes daily PnL summary
4. Saves to `daily_pnl_summary` table
5. Logs `PNL_DAILY_SUMMARY` event

### Sample Output

```
============================================================
  📊 Daily PnL Report: 2026-01-16
============================================================

  💰 P&L Summary
  ────────────────────────────────────────
  Realized PnL:    +$127.50
  Unrealized PnL:  +$45.00
  Total PnL:       +$172.50

  📈 Trade Stats
  ────────────────────────────────────────
  Total Trades:    12
  Buys:            6
  Sells:           6
  Volume:          $15,234.00

  🎯 Performance
  ────────────────────────────────────────
  Win Rate:        66.7%
  Wins/Losses:     4/2
  Avg Win:         +$45.00
  Avg Loss:        -$22.50
  Max Drawdown:    $35.00

  🏆 By Symbol
  ────────────────────────────────────────
  Best:            AAPL (+$85.00)
  Worst:           MSFT (-$22.50)

============================================================
```

---

## 12. Daily Operator Checklist

### Morning (Optional)

- [ ] Health check: `python scripts/awet_health_check.py`
- [ ] Check APM for overnight runs: http://localhost:3001 → APM
- [ ] Check LLM summary: `make llm-summary`
- [ ] Tail logs: `tail -50 logs/night_trainer.log`

### During Market Hours

- [ ] Trade Watchdog runs every 15 min automatically
- [ ] Check logs if needed: `tail -f logs/trade_watchdog.log`
- [ ] Monitor LLM calls: `make llm-watch` (optional)

### End of Day

- [ ] Review LLM summary: `make llm-summary`
- [ ] Reconcile pending orders: `make reconcile`
- [ ] Nothing else required – system continues running
- [ ] Or stop if not needed: `./scripts/awet_stop.sh`

---

## 12. Order Lifecycle Reconciliation

### Overview

Orders submitted to Alpaca start with status `accepted` and later become `filled`, `canceled`, etc. The reconciliation scheduler polls Alpaca and updates the `trades` table with:
- `alpaca_status`: Current Alpaca order status
- `filled_qty`: Number of shares filled
- `avg_fill_price`: Average execution price
- `filled_at`: Timestamp when order was filled
- `updated_at`: Last reconciliation time

### Quick Commands

| Task | Command |
|------|---------|
| One-time reconciliation | `make reconcile` |
| Continuous watching (market hours) | `make reconcile-watch` |
| End-of-day job (reconcile + PnL) | `make eod` |
| Today's PnL report | `make pnl-today` |
| Weekly PnL report | `make pnl-week` |

### Automatic Daily Monitoring

The system is designed for fully automated daily operation:

**During Market Hours (9:30 AM - 4:00 PM ET, Mon-Fri):**
```bash
# Start the reconciliation scheduler
make reconcile-watch
```

This runs continuously and:
- Polls Alpaca every 60 seconds for order updates
- Updates trades table with fill info
- Stops tracking terminal orders (filled/canceled/rejected)
- Logs: pending found, updated, became terminal, errors
- Retries with backoff on API failures (5s, 15s, 30s)

**After Market Close:**
```bash
# Run end-of-day job
make eod
```

This performs:
1. Final order reconciliation
2. Position reconciliation with Alpaca
3. Daily PnL computation
4. Saves to `daily_pnl_summary` table

### Running Manually

```bash
# One-time reconciliation
python scripts/reconcile_scheduler.py --once

# Watch mode with custom interval
python scripts/reconcile_scheduler.py --interval 30

# Force end-of-day job
python scripts/reconcile_scheduler.py --eod
```

### Example Logs

```
reconcile_loop_started     interval=60 market_hours=9:30:00-16:00:00 ET Mon-Fri
RECONCILE_CYCLE            pending_found=4 updated=2 became_terminal=1 unchanged=1 errors=0 market_hours=True next_run_in=60
order_reconciled           order_id=abc123 symbol=AAPL old_status=accepted new_status=filled filled_qty=100 avg_price=175.50 is_terminal=True
```

### Terminal Statuses

Orders with these statuses are no longer polled:

| Status | Description |
|--------|-------------|
| `filled` | Order fully executed |
| `canceled` | Order canceled by user or broker |
| `rejected` | Order rejected by broker |
| `expired` | Order expired (e.g., day order) |
| `replaced` | Order replaced with new order |

### Cron Setup (Production)

For production deployment, use cron:

```bash
# Edit crontab
crontab -e

# Reconciliation every minute during market hours (9:30 AM - 4:00 PM ET)
*/1 9-15 * * 1-5 cd /home/kironix/Awet && .venv/bin/python scripts/reconcile_scheduler.py --once >> logs/reconcile.log 2>&1

# Extra runs at market open/close
30 9 * * 1-5 cd /home/kironix/Awet && .venv/bin/python scripts/reconcile_scheduler.py --once >> logs/reconcile.log 2>&1
0 16 * * 1-5 cd /home/kironix/Awet && .venv/bin/python scripts/reconcile_scheduler.py --once >> logs/reconcile.log 2>&1

# End-of-day job at 4:30 PM ET
30 16 * * 1-5 cd /home/kironix/Awet && .venv/bin/python scripts/reconcile_scheduler.py --eod >> logs/eod.log 2>&1
```

### Why PnL Was $0

If PnL reports show $0, it's usually because:

1. **Orders still `accepted`**: No fill info yet → run `make reconcile`
2. **Missing `avg_fill_price`**: Alpaca hasn't reported fills → wait and retry
3. **No round-trips**: Only buys, no sells → PnL is unrealized
4. **Outside market hours**: Orders won't fill when market is closed

**Fix**: Run `make reconcile` then `make pnl-today` to see updated numbers.

### Docker Deployment (Recommended)

The reconciler runs as a Docker service for fully automated operation:

**Start the Reconciler:**
```bash
# Start reconciler alongside other services
docker compose up -d awet-reconciler

# Or start everything
docker compose up -d
```

**View Logs:**
```bash
# Follow reconciler logs
docker logs -f awet-reconciler

# Example output:
# reconcile_loop_started  interval=60 market_hours=True
# RECONCILE_CYCLE         pending_found=4 updated=2 became_terminal=1 unchanged=1 errors=0
```

**Stop the Reconciler:**
```bash
docker compose stop awet-reconciler
```

**Run End-of-Day Job Manually:**
```bash
# Run EOD job (reconcile + positions + PnL)
docker compose run --rm awet-eod

# Output:
# EOD_JOB_STARTING
# Orders reconciled: 5
# Positions synced: 3
# PnL computed for 2026-01-16
# EOD_JOB_COMPLETE
```

**Scheduled EOD (Built-in):**
The reconciler automatically triggers EOD at 4:30 PM ET if running with `--watch`. No separate cron needed.

### Quick Commands Summary

| Task | Docker Command | Make Command |
|------|---------------|--------------|
| Start reconciler | `docker compose up -d awet-reconciler` | `make reconcile-watch` |
| Stop reconciler | `docker compose stop awet-reconciler` | Ctrl+C |
| View logs | `docker logs -f awet-reconciler` | (console output) |
| Run EOD job | `docker compose run --rm awet-eod` | `make eod` |
| One-time reconcile | `docker compose run --rm awet-eod --entrypoint "python scripts/reconcile_scheduler.py --once"` | `make reconcile` |
| Check pending orders | See SQL query below | – |

### Troubleshooting

| Issue | Solution |
|-------|----------|
| "Max retries exceeded" | Check Alpaca API status, verify credentials |
| Orders stuck in `accepted` | May be outside market hours, wait for fills |
| PnL still $0 after reconcile | Check trades table for `avg_fill_price` values |
| Reconciler won't start | Verify `DATABASE_URL` and Alpaca env vars |
| Container exits immediately | Check `docker logs awet-reconciler` for errors |
| "Connection refused" to DB | Ensure `timescaledb` service is running |

```bash
# Check pending orders in DB
PGPASSWORD=awet psql -h localhost -p 5433 -U awet -d awet -c "
SELECT alpaca_order_id, symbol, alpaca_status, filled_qty, avg_fill_price, updated_at
FROM trades
WHERE alpaca_status IN ('new', 'accepted', 'pending_new', 'partially_filled')
ORDER BY ts DESC LIMIT 10;
"
```

---

## 13. Key Files

| File | Purpose |
|------|---------|
| `scripts/awet_start.sh` | One-command startup |
| `scripts/awet_stop.sh` | Clean shutdown |
| `scripts/awet_health_check.py` | System health check |
| `scripts/schedule_agents.py` | Trigger agents via CLI |
| `scripts/show_llm_traces.py` | Query LLM traces from DB |
| `scripts/show_trade_chain.py` | Trace trades to LLM prompts |
| `scripts/reconcile_scheduler.py` | Automated order reconciliation |
| `scripts/reconcile_orders.py` | Sync order status with Alpaca |
| `.env` | API keys and config |
| `logs/` | Agent execution logs |

---

*Last updated: January 16, 2026*
