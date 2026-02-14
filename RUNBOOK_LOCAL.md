# Local Runbook (AWET)

## Quick Reference

| Action | Command |
|--------|---------|
| Start infra | `docker compose up -d` |
| Stop all | `docker compose down` |
| **EMERGENCY STOP** | `docker compose down && docker compose rm -f` |
| Check health | `curl -s localhost:800{1..6}/health` |
| View logs | `docker compose logs -f --tail=100 <service>` |
| Kafka lag | `docker exec awet-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --all-groups` |

---

## 1) Start infrastructure
```bash
docker compose up -d
```

## 2) Verify DB schema
```bash
python scripts/verify_db.py
```

## 3) Ingest synthetic data
```bash
python scripts/ingest_market_data.py --symbols AAPL --start 2024-01-01 --end 2024-01-03 --source synthetic
```

## 4) Run E2E test
```bash
python scripts/e2e_test.py
```

## 5) Run pytest E2E
```bash
pytest -m e2e -v
```

## 6) Run pipeline scheduling list/run
```bash
python scripts/schedule_pipeline.py list
python scripts/schedule_pipeline.py run nightly
python scripts/schedule_pipeline.py run morning
python scripts/schedule_pipeline.py run market
```

---

## 7) Emergency Stop Procedure

**WHEN**: Kill switch triggered, unexpected losses, agent crash loop, data corruption suspected.

### Step 1: Kill all agents immediately
```bash
# Stop all containers (agents + infra)
docker compose down

# Verify nothing is running
docker ps --filter "name=awet"
```

### Step 2: Disable paper trading approval gate
```bash
# Remove or rename the approval file (execution agent will refuse trades)
mv .approval_token .approval_token.disabled 2>/dev/null || true
```

### Step 3: Check state of play
```bash
# Check last audit events
PGPASSWORD="${POSTGRES_PASSWORD:-awet}" psql -h localhost -p 5433 -U awet -d awet -c \
  "SELECT ts, source, event_type FROM audit_events ORDER BY ts DESC LIMIT 20;"

# Check for stuck Kafka consumer offsets
docker compose up -d kafka
docker exec awet-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --all-groups
```

### Step 4: Restart selectively
```bash
# Bring up infra only (no agents)
docker compose up -d kafka schema-registry timescaledb redis

# Verify infra health
docker compose ps

# Bring up agents one-by-one after confirming infra is healthy
docker compose up -d data-ingestion
docker compose up -d feature-engineering
docker compose up -d prediction
docker compose up -d trader-decision
docker compose up -d risk
docker compose up -d execution
```

---

## 8) Data Recovery Procedure

**WHEN**: TimescaleDB data loss, Kafka topic corruption, schema desync.

### 8a) TimescaleDB Recovery

```bash
# Check what data exists
PGPASSWORD="${POSTGRES_PASSWORD:-awet}" psql -h localhost -p 5433 -U awet -d awet -c \
  "SELECT 'market_raw_minute' AS tbl, COUNT(*), MIN(ts), MAX(ts) FROM market_raw_minute
   UNION ALL
   SELECT 'features_tft', COUNT(*), MIN(ts), MAX(ts) FROM features_tft
   UNION ALL
   SELECT 'predictions_tft', COUNT(*), MIN(ts), MAX(ts) FROM predictions_tft
   UNION ALL
   SELECT 'paper_trades', COUNT(*), MIN(ts), MAX(ts) FROM paper_trades;"

# Re-ingest missing date ranges
python scripts/ingest_market_data.py \
  --symbols AAPL MSFT NVDA \
  --start 2024-01-01 --end 2024-06-01 \
  --source yfinance

# Re-run feature engineering batch for missing dates
python -c "
import asyncio
from datetime import datetime, timezone
from src.agents.feature_engineering import run_feature_engineering_batch
asyncio.run(run_feature_engineering_batch(['AAPL'], datetime(2024,1,1,tzinfo=timezone.utc), datetime(2024,6,1,tzinfo=timezone.utc)))
"
```

### 8b) Kafka Recovery

```bash
# List topics and check offsets
docker exec awet-kafka kafka-topics --bootstrap-server localhost:9092 --list
docker exec awet-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --all-groups

# Reset consumer offset to earliest (re-process all data)
docker exec awet-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group <group-id> \
  --topic <topic> \
  --reset-offsets --to-earliest --execute

# If topic is corrupted, delete and recreate
docker exec awet-kafka kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic <topic>
# Topics auto-recreate when producers write to them
```

### 8c) Schema Registry Recovery

```bash
# Check registered schemas
curl -s http://localhost:8081/subjects | python -m json.tool

# Re-register schemas if missing
for schema in src/schemas/*.avsc; do
  subject=$(basename "$schema" .avsc)-value
  curl -X POST "http://localhost:8081/subjects/$subject/versions" \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "{\"schema\": $(python -c "import json; print(json.dumps(open('$schema').read()))")}"
done
```

---

## 9) Monitoring Checklist

```bash
# Prometheus targets up?
curl -s http://localhost:9090/api/v1/targets | python -m json.tool | grep '"health"'

# Grafana running?
curl -s http://localhost:3000/api/health

# Check all agent health endpoints
for port in 8001 8002 8003 8004 8005 8006; do
  echo "Port $port: $(curl -sf http://localhost:$port/health 2>/dev/null || echo 'DOWN')"
done
```

---

## 10) Rollback Procedure

If a deployment causes issues:

```bash
# 1. Stop all agents
docker compose down

# 2. Checkout previous working commit
git log --oneline -5
git checkout <previous-commit>

# 3. Rebuild images
docker compose build

# 4. Start infra first, then agents
docker compose up -d kafka schema-registry timescaledb redis
sleep 10
docker compose up -d
```
