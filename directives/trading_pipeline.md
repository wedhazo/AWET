# Trading Pipeline Directive

## Goal
Run the end-to-end paper-trading pipeline from ingestion to execution with full audit trail.

## Success Criteria
- Events flow through: market.raw → market.engineered → predictions.tft → risk.approved|risk.rejected → execution.completed
- Every event includes contract fields and valid Avro schema
- Audit table contains at least one row per stage
- Idempotency enforced by idempotency_key

## Inputs
- Symbols: configured in config/app.yaml
- Time range: real-time demo loop
- Risk limits: config/limits.yaml
- Kafka topics: src/streaming/topics.py

## Tools/Services
- Execution scripts: execution/demo.py
- Agents: src/agents/*
- Kafka + Schema Registry via docker-compose
- TimescaleDB audit via db/init.sql

## Outputs
- Kafka topics: market.raw, market.engineered, predictions.tft, risk.approved, risk.rejected, execution.completed
- TimescaleDB: audit_events
- Metrics: /metrics per agent

## Steps
1. Start infrastructure via docker compose.
2. Start all agents as processes.
3. Ingest sample market ticks.
4. Engineer features and emit engineered events.
5. Predict (TFT stub) and emit predictions.
6. Run risk gates and emit approval/rejection.
7. Execute paper trade if approval gate file exists.
8. Write audit trail per stage with idempotency checks.

## Edge Cases
- Duplicate idempotency_key → skip processing
- Schema registry unavailable → retry with backoff
- Execution approval missing → emit blocked execution status

## Rollback/Replay
- Re-run consumers from earliest offsets after clearing audit table or using new idempotency keys
