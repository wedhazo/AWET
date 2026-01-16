# GAP AUDIT REPORT: AWET Trading Pipeline

**Date**: January 15, 2026  
**Auditor**: GitHub Copilot  
**Repo**: /home/kironix/Awet

---

## Executive Summary

The codebase has **substantial infrastructure in place** but has **critical gaps** preventing true end-to-end operation. Below is each requirement with status, gaps, and minimal patch plans.

---

## 1. Kafka KRaft + Schema Registry + Avro Schemas

**Status: ✅ COMPLETE**

| Component | Status | Location |
|-----------|--------|----------|
| Kafka KRaft | ✅ | docker-compose.yml:1-27 - `KAFKA_PROCESS_ROLES: broker,controller`, `CLUSTER_ID` set |
| Schema Registry | ✅ | docker-compose.yml:29-41 |
| Avro Schemas | ✅ | src/schemas/ - 5 schemas defined |

**No gaps.**

---

## 2. correlation_id + idempotency End-to-End

**Status: ⚠️ PARTIAL - 2 GAPS**

| Component | Status | Location |
|-----------|--------|----------|
| BaseEvent fields | ✅ | src/models/base.py:10-22 - All required fields present |
| correlation_id propagation | ✅ | All agents call `set_correlation_id()` |
| Idempotency check | ✅ | src/audit/trail_logger.py:38-46 `is_duplicate()` |

### GAP 2.1: No test enforcing ALL events have required fields

- **File**: tests/unit/test_event_validation.py:1-14
- **Issue**: Test only checks one event type fails without fields, not that all agents emit valid events
- **Patch Plan**: Add integration test that validates emitted events from each agent have: `event_id`, `correlation_id`, `idempotency_key`, `symbol`, `ts`, `schema_version`, `source`

### GAP 2.2: Backfill scripts don't track checkpoint/resume in DB

- **File**: execution/backfill_polygon.py:1-200
- **Issue**: No checkpoint table for resumable backfill; restart = start from scratch
- **Patch Plan**: Add `backfill_checkpoints` table + checkpoint after each file processed

---

## 3. Prometheus Metrics per Agent + /health Endpoints

**Status: ⚠️ PARTIAL - 2 GAPS**

| Component | Status | Location |
|-----------|--------|----------|
| BaseAgent /health | ✅ | src/agents/base_agent.py:27 |
| BaseAgent /metrics | ✅ | src/agents/base_agent.py:28 |
| Prometheus scrape config | ✅ | prometheus/prometheus.yml:1-14 |
| EVENTS_PROCESSED counter | ✅ | src/monitoring/metrics.py:8-13 |
| EVENT_LATENCY histogram | ✅ | src/monitoring/metrics.py:22-27 |

### GAP 3.1: WatchtowerAgent doesn't track Kafka consumer lag

- **File**: src/agents/watchtower_agent.py:1-30
- **Issue**: Only emits heartbeat, no actual Kafka lag metrics via `kafka-consumer-groups --describe`
- **Patch Plan**: Add `CONSUMER_LAG` Gauge metric, poll Kafka admin API or shell out to `kafka-consumer-groups`

### GAP 3.2: Grafana dashboard is empty placeholder

- **File**: grafana/provisioning/dashboards/empty.json:1-8
- **Issue**: No actual panels for throughput, latency, lag
- **Patch Plan**: Create real dashboard JSON with panels for `events_processed_total`, `event_latency_seconds`, `kafka_consumer_lag`

---

## 4. TimescaleDB Audit Trail

**Status: ✅ COMPLETE**

| Component | Status | Location |
|-----------|--------|----------|
| audit_events table | ✅ | db/init.sql:146-175 - Hypertable with all fields |
| AuditTrailLogger | ✅ | src/audit/trail_logger.py:1-70 |
| All agents write audit | ✅ | Each agent calls `await self.audit.write_event()` |

**No gaps.**

---

## 5. Retry + Circuit Breaker

**Status: ✅ COMPLETE**

| Component | Status | Location |
|-----------|--------|----------|
| retry_async() | ✅ | src/core/retry.py:1-28 - exponential backoff + jitter |
| CircuitBreaker | ✅ | src/core/circuit_breaker.py:1-42 |
| Usage in providers | ✅ | src/market_data/providers.py:44,77-97 |
| Tests | ✅ | tests/unit/test_retry.py, tests/unit/test_circuit_breaker.py |

**No gaps.**

---

## 6. Approval Gate for ExecutionAgent

**Status: ✅ COMPLETE**

| Component | Status | Location |
|-----------|--------|----------|
| Approval file check | ✅ | src/agents/execution_agent.py:61-62 |
| Config for approval path | ✅ | config/app.yaml:12 |
| Blocked status in event | ✅ | src/agents/execution_agent.py:63-64 |
| execution.blocked topic | ✅ | src/streaming/topics.py - EXECUTION_BLOCKED constant |
| make approve/revoke | ✅ | Makefile - approve and revoke targets |

### ~~GAP 6.1~~ ✅ FIXED: Blocked events now go to `execution.blocked` topic

- **Fixed in**: src/streaming/topics.py, src/agents/execution_agent.py
- **Solution**: Added `EXECUTION_BLOCKED` topic constant, execution_agent publishes to separate topic based on approval status

### ~~GAP 6.2~~ ✅ FIXED: `make approve` / `make revoke` targets added

- **Fixed in**: Makefile
- **Solution**: Added targets:
```makefile
approve:
	@mkdir -p .tmp && touch .tmp/APPROVE_EXECUTION
	@echo "✅ Execution APPROVED"
revoke:
	@rm -f .tmp/APPROVE_EXECUTION
	@echo "🚫 Execution REVOKED"
```

---

## 7. TFT Real Training + ONNX Export + Model Registry + PredictionAgent Loads Green

**Status: ⚠️ MOSTLY COMPLETE - 1 GAP**

| Component | Status | Location |
|-----------|--------|----------|
| TFT model | ✅ | src/ml/tft/model.py |
| Training from DB | ✅ | src/ml/train.py:45-150 |
| ONNX export | ✅ | src/ml/tft/model.py `export_to_onnx()` |
| Model registry | ✅ | src/ml/registry.py:1-338 |
| CLI commands | ✅ | src/ml/train.py - train/export/promote/list |
| PredictionAgent loads green | ✅ | src/prediction/engine.py:325-340 via `get_green_model_path()` |
| Auto-detect lookback | ✅ | src/ml/onnx_engine.py:118-121 |

### GAP 7.1: PredictionAgent doesn't hot-reload model without restart

- **File**: src/prediction/engine.py:242-258
- **Issue**: Model loaded once at warmup, no periodic check for new green model
- **Patch Plan**: Add background task in `ONNXPredictionEngine` that calls `reload_if_needed()` every N seconds

---

## 8. Risk Engine Real Gates (Not Placeholder)

**Status: ⚠️ MOSTLY COMPLETE - 1 GAP**

| Component | Status | Location |
|-----------|--------|----------|
| Position sizing | ✅ | src/risk/engine.py:143-145 - max_position_pct |
| Max exposure per ticker | ✅ | src/risk/engine.py:143 |
| Daily loss limit | ✅ | src/risk/engine.py:119-121 |
| CVaR placeholder | ✅ | src/risk/engine.py:159-180 `_calculate_cvar()` |
| Reject path to risk.rejected | ✅ | src/agents/risk_agent.py:98-100 |
| Audit every decision | ✅ | src/agents/risk_agent.py:101 |

### GAP 8.1: Risk limits not loaded from config/limits.yaml

- **File**: src/risk/engine.py:100-106
- **Issue**: Only loads from env vars, ignores config/limits.yaml
- **Patch Plan**: Add YAML loader in `_load_config()` to read from `config/limits.yaml`

---

## 9. Backfill Scripts (Polygon + Reddit)

**Status: ⚠️ PARTIAL - 3 GAPS**

| Component | Status | Location |
|-----------|--------|----------|
| Polygon backfill | ✅ | execution/backfill_polygon.py |
| Reddit backfill | ⚠️ | execution/backfill_reddit.py - exists but no schema |
| Avro validation | ✅ | Uses AvroSerializer |

### GAP 9.1: No `reddit.raw` Avro schema

- **File**: src/schemas/
- **Issue**: Reddit backfill references schema that doesn't exist
- **Patch Plan**: Create `src/schemas/reddit_raw.avsc`

### GAP 9.2: No checkpoint table for resumable backfill

- **File**: db/init.sql
- **Issue**: Missing `backfill_checkpoints` table
- **Patch Plan**: Add table with `(source, filename, last_offset, completed_at)`

### GAP 9.3: Reddit backfill uses wrong paths

- **File**: Makefile:55-63
- **Issue**: Uses `/home/kironix/train/reddit/submissions` but user has `submissions/*` subdirs
- **Patch Plan**: Update loader to handle subdirectory structure

---

## 10. Database Schema

**Status: ✅ COMPLETE**

| Table | Status | Location |
|-------|--------|----------|
| audit_events | ✅ | db/init.sql:146-175 |
| features_tft | ✅ | db/init.sql:49-94 |
| predictions_tft | ✅ | db/init.sql:96-122 |
| paper_trades | ✅ | db/init.sql:124-145 |
| models_registry | ✅ | db/init.sql:181-200 |
| risk_decisions | ✅ | db/init.sql - hypertable |
| backfill_checkpoints | ✅ | db/init.sql |

### ~~GAP 10.1~~ ✅ FIXED: `risk_decisions` table added

- **Fixed in**: db/init.sql
- **Solution**: Added `risk_decisions` hypertable with: `ticker, ts, approved, reason, limits_snapshot JSONB`

### ~~GAP 10.2~~ ✅ FIXED: `backfill_checkpoints` table added

- **Fixed in**: db/init.sql
- **Solution**: Added table with: `source, filename, last_offset, records_processed, updated_at`

---

## 11. Demo / End-to-End Runnable

**Status: ⚠️ PARTIAL - 1 GAP**

### ~~GAP 11.1~~ ✅ FIXED: `make demo` now verifies full message flow

- **Fixed in**: execution/demo.py
- **Solution**: Complete rewrite - verifies events flow through: market.raw → market.engineered → predictions.tft → risk.approved → execution.completed/blocked

### ~~GAP 11.2~~ ✅ FIXED: Demo generates synthetic data

- **Fixed in**: execution/demo.py
- **Solution**: Demo now generates 15 synthetic MarketRawEvent messages (5 per ticker: AAPL, MSFT, NVDA) with deterministic idempotency keys

### GAP 11.3: README doesn't have complete runbook

- **File**: README.md
- **Issue**: Missing exact commands for fresh machine setup
- **Patch Plan**: Rewrite with step-by-step: setup → up → backfill → train → promote → approve → demo

---

## Summary: Priority Order for Fixes

| Priority | Requirement | Gaps | Effort | Status |
|----------|-------------|------|--------|--------|
| ~~🔴 P0~~ | ~~DB Schema~~ | ~~2 tables~~ | ~~Small~~ | ✅ DONE |
| ~~🔴 P0~~ | ~~Backfill checkpoints~~ | ~~1 table + code~~ | ~~Medium~~ | ✅ DONE |
| ~~🟡 P1~~ | ~~Make demo runnable~~ | ~~3 fixes~~ | ~~Medium~~ | ✅ DONE |
| ~~🟡 P1~~ | ~~Approval gate `make approve/revoke`~~ | ~~Makefile~~ | ~~Tiny~~ | ✅ DONE |
| ~~🟡 P1~~ | ~~Risk loads from YAML~~ | ~~1 function~~ | ~~Small~~ | ✅ DONE |
| ~~🟢 P2~~ | ~~Watchtower lag metrics~~ | ~~New feature~~ | ~~Medium~~ | ✅ DONE |
| ~~🟢 P2~~ | ~~Grafana dashboard~~ | ~~JSON~~ | ~~Medium~~ | ✅ DONE |
| ~~🟢 P2~~ | ~~Hot-reload models~~ | ~~Background task~~ | ~~Small~~ | ✅ DONE |
| ~~🟢 P2~~ | ~~Event validation tests~~ | ~~Tests~~ | ~~Small~~ | ✅ DONE |
| ~~🟢 P3~~ | ~~Reddit schema + paths~~ | ~~Schema + loader~~ | ~~Medium~~ | ✅ DONE |

---

## Recommended Execution Order

```
#4 (DB schema) → #2 (demo runnable) → #3 (backfill) → #5 (train) → #6 (risk) → #7 (execution) → #8 (observability) → #9 (tests) → #10 (README)
```

This ensures:
1. Database tables exist before anything writes to them
2. Demo works end-to-end before adding complexity
3. Training has data to work with
4. Risk/execution gates are enforced
5. Observability proves it works
6. Tests catch regressions
7. README documents final state

---

## Total Gap Count

| Category | Complete | Gaps | Fixed |
|----------|----------|------|-------|
| Kafka + Schema Registry | ✅ | 0 | - |
| correlation_id + idempotency | ✅ | ~~2~~ 0 | **2** |
| Prometheus + /health | ✅ | ~~2~~ 0 | **2** |
| TimescaleDB audit | ✅ | 0 | - |
| Retry + Circuit Breaker | ✅ | 0 | - |
| Approval Gate | ✅ | ~~2~~ 0 | **2** |
| TFT + ONNX + Registry | ✅ | ~~1~~ 0 | **1** |
| Risk Engine | ✅ | ~~1~~ 0 | **1** |
| Backfill Scripts | ✅ | ~~3~~ 0 | **3** |
| Database Schema | ✅ | ~~2~~ 0 | **2** |
| Demo E2E | ✅ | ~~3~~ 0 | **3** |
| **TOTAL** | **11/11** | **0** | **16 FIXED** |

---

## ✅ ALL GAPS FIXED

**Session 3 Fixes (10 gaps):**
- ✅ GAP 2.1: Comprehensive event validation tests
- ✅ GAP 2.2: Backfill checkpoint code (`src/backfill/checkpoint.py`)
- ✅ GAP 3.1: Watchtower Kafka consumer lag metrics
- ✅ GAP 3.2: Real Grafana dashboard (`awet-pipeline.json`)
- ✅ GAP 7.1: Hot-reload models in PredictionAgent
- ✅ GAP 8.1: Risk engine loads from `config/limits.yaml`
- ✅ GAP 9.1: Reddit Avro schema (`src/schemas/reddit_raw.avsc`)
- ✅ GAP 9.2: Reddit backfill uses checkpoints
- ✅ GAP 9.3: Reddit backfill handles subdirectories (`**/*.zst`)
- ✅ GAP 11.3: Complete README runbook

**Previous Sessions (6 gaps):**
- ✅ GAP 10.1: Added `risk_decisions` hypertable
- ✅ GAP 10.2: Added `backfill_checkpoints` table
- ✅ GAP 6.1: Added `execution.blocked` topic
- ✅ GAP 6.2: Added `make approve/revoke` targets
- ✅ GAP 11.1: Demo verifies full message flow
- ✅ GAP 11.2: Demo generates synthetic data

**Verification commands:**
```bash
# Fresh machine setup
make setup && make up

# Run demo in BLOCKED mode
make revoke && make demo   # → execution.blocked

# Run demo in APPROVED mode
make approve && make demo  # → execution.completed

# Test with resume capability
python execution/backfill_polygon.py --data-dir /path/to/data --resume
python execution/backfill_reddit.py --submissions-dir /path/to/reddit --resume

# View Grafana dashboard
open http://localhost:3000  # awet-pipeline dashboard
```
