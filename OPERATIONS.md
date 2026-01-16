# AWET Operations Runbook

> **Paper Trading Only** – This system is safety-gated. No real money trades are executed.

This document explains how to operate the AWET platform day-to-day.

---

## Quick Reference

| Task | Command |
|------|---------|
| Start everything | `./scripts/awet_start.sh` |
| Stop everything | `./scripts/awet_stop.sh` |
| Health check | `.venv/bin/python scripts/awet_health_check.py` |
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

## 8. Safety Reminders

⚠️ **Paper Trading Only**

- The `execution_agent` checks for an approval file before any trade
- Default mode is BLOCKED (no trades execute)
- To enable paper trading: `make approve`
- To disable: `make revoke`

Check current status:
```bash
ls -la /home/kironix/Awet/.tmp/APPROVE_EXECUTION 2>/dev/null && echo "APPROVED" || echo "BLOCKED"
```

---

## 9. Daily Operator Checklist

### Morning (Optional)

- [ ] Health check: `python scripts/awet_health_check.py`
- [ ] Check APM for overnight runs: http://localhost:3001 → APM
- [ ] Tail logs: `tail -50 logs/night_trainer.log`

### During Market Hours

- [ ] Trade Watchdog runs every 15 min automatically
- [ ] Check logs if needed: `tail -f logs/trade_watchdog.log`

### End of Day

- [ ] Nothing required – system continues running
- [ ] Or stop if not needed: `./scripts/awet_stop.sh`

---

## 10. Key Files

| File | Purpose |
|------|---------|
| `scripts/awet_start.sh` | One-command startup |
| `scripts/awet_stop.sh` | Clean shutdown |
| `scripts/awet_health_check.py` | System health check |
| `scripts/schedule_agents.py` | Trigger agents via CLI |
| `.env` | API keys and config |
| `logs/` | Agent execution logs |

---

*Last updated: January 16, 2026*
