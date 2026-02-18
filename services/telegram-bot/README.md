# telegram-bot

Telegram gateway to the **AWET KB-Query** service for reddit-sourced market intelligence.

## Overview

Users chat with the bot via Telegram. The bot translates commands into HTTP requests against the internal `kb-query` service and formats the replies for Telegram Markdown.

```
Telegram user
    │  (long-poll getUpdates)
    ▼
telegram-bot  ──►  kb-query:8000  ──►  Redis / TimescaleDB vector index
    │
    ├── /healthz, /readyz, /metrics  (aiohttp, port 9200)
    └── state.json  (last_update_id, survives container restart)
```

---

## Commands

| Command | Description |
|---|---|
| `/search <query>` | Full-text search of the Reddit KB |
| `/search <query> ticker:TSLA limit:10 from:2026-01-01 to:2026-02-01` | Filtered search |
| `/source <chunk_id>` | Fetch full body of a single KB document |
| `/help` | Show command reference |
| `/start` | Alias for `/help` |

### Filter keys

| Key | Example | Notes |
|---|---|---|
| `ticker:` | `ticker:TSLA` | Upper-cased automatically |
| `limit:` | `limit:10` | Must be a positive integer; ignored if not parseable |
| `from:` | `from:2026-01-01` | ISO 8601 date string, passed through verbatim |
| `to:` | `to:2026-02-01` | ISO 8601 date string, passed through verbatim |

---

## Configuration

All configuration is via environment variables.

| Variable | Default | Description |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | **required** | BotFather API token |
| `KB_QUERY_BASE_URL` | `http://kb-query:8000` | Base URL of the kb-query service |
| `HTTP_TIMEOUT_SEC` | `5` | Per-request HTTP timeout (seconds) |
| `HTTP_MAX_RETRIES` | `3` | Max retry attempts for transient errors |
| `DEFAULT_LIMIT` | `5` | Default result count for `/search` |
| `MAX_LIMIT` | `50` | Hard cap on `/search` result count |
| `LOG_LEVEL` | `INFO` | Structlog level (INFO, DEBUG, WARNING) |
| `METRICS_PORT` | `9200` | Prometheus / health HTTP port |
| `STATE_FILE` | `/var/lib/telegram-bot/state.json` | Persisted `last_update_id` location |
| `RATE_LIMIT_MESSAGES_PER_SEC` | `1.0` | Per-chat token-bucket rate (msgs/sec) |

---

## Failure Modes and Mitigations

| Failure | Detection | Mitigation |
|---|---|---|
| `kb-query` unreachable | `/readyz` → 503; `KBQueryError` in logs | Retries with exponential backoff (up to `HTTP_MAX_RETRIES`); user sees ⚠️ error message with correlation_id |
| `kb-query` slow / timed out | `KBQueryTimeout` exception | Caught at dispatch layer; user receives `format_timeout()` message with retry guidance |
| Telegram `sendMessage` fails | `TelegramSendError` exception | 3 attempts with 1 s / 2 s / 4 s backoff; on final failure, exception logged with correlation_id — no user crash |
| Container restart loses position | — | `last_update_id` persisted atomically to `STATE_FILE` after every update; next restart resumes from offset + 1 |
| Chat floods the bot | Rate counter in logs | Token-bucket (1 msg/s default): excess messages receive `format_rate_limited()` immediately, no upstream call made |
| Telegram API returns duplicate updates | idempotency via `last_update_id` | Already-processed update IDs are below the offset and skipped automatically |
| Schema Registry / Kafka down | Not used here — bot is read-only | N/A; bot only queries kb-query and never writes to Kafka |
| State file corrupted | JSON decode error on load | Log error + default to `last_update_id = 0` (may re-process some updates, harmless) |

---

## Running Locally

```bash
cd services/telegram-bot

# install deps
pip install -r requirements.txt

# configure
export TELEGRAM_BOT_TOKEN=<your-token>
export KB_QUERY_BASE_URL=http://localhost:8000

# run
python -m telegram_bot.main
```

### Observability endpoints

| Endpoint | Description |
|---|---|
| `GET :9200/healthz` | Always 200 (process alive) |
| `GET :9200/readyz` | 200 if kb-query is reachable, 503 otherwise |
| `GET :9200/metrics` | Prometheus text format |

---

## Docker

```bash
# Build
docker build -t awet/telegram-bot:latest .

# Run (set TELEGRAM_BOT_TOKEN in .env)
docker run --rm \
  --env-file ../../.env \
  -v telegram-bot-state:/var/lib/telegram-bot \
  --network awet \
  awet/telegram-bot:latest
```

Via docker-compose (from the workspace root):

```bash
docker-compose up telegram-bot
```

---

## Tests

```bash
cd services/telegram-bot

# unit tests (pure, no I/O)
pytest tests/unit/ -v

# integration tests (mocked HTTP via respx)
pytest tests/integration/ -v

# full suite
pytest -v
```

---

## Architecture Decisions

| Decision | Rationale |
|---|---|
| Raw `httpx` polling instead of python-telegram-bot | Fewer dependencies; full control over retry / backoff / correlation headers |
| Separate `parser.py` pure module | Enables 100 % unit test coverage without I/O setup |
| Separate `formatter.py` pure module | Same as parser; deterministic output verified directly |
| In-memory token bucket (not Redis) | Bot is single-instance; Redis adds operational complexity for marginal gain |
| `aiohttp` for observability server | Lightweight; avoids mixing polling and serving in the same event-loop-blocking call |
| Atomic state writes (tmp rename) | Prevents partial state file on crash — POSIX rename is atomic on the same filesystem |
