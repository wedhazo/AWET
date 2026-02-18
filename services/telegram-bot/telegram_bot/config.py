"""Configuration loaded from environment variables.

All secrets are read once at import time. Nothing is re-read during
request handling, ensuring deterministic config throughout the lifecycle.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Config:
    """Immutable configuration object."""

    # Telegram
    telegram_bot_token: str
    telegram_owner_id: int = 0

    # Upstream
    kb_query_base_url: str = "http://kb-query:8000"

    # HTTP
    http_timeout_sec: float = 5.0
    http_max_retries: int = 3

    # Bot behaviour
    default_limit: int = 5
    max_limit: int = 50

    # Observability
    log_level: str = "INFO"
    metrics_port: int = 9200

    # State persistence (last_update_id survives restarts)
    state_file: str = "/var/lib/telegram-bot/state.json"

    # Rate limiting
    rate_limit_messages_per_sec: float = 1.0

    # LLM (RAG /ask command)
    llm_provider: str = "anthropic"   # anthropic | kimi | openai
    llm_api_key: str = ""             # required when /ask is used
    llm_model: str = ""               # empty → auto-selected per provider
    llm_base_url: str = ""            # custom base URL (for local Ollama, etc.)
    llm_timeout_sec: float = 30.0
    llm_max_retries: int = 2
    llm_rag_limit: int = 5            # KB docs fetched for each /ask query


def load_config() -> Config:
    """Load config from environment. Raises ValueError on missing required vars."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise ValueError(
            "TELEGRAM_BOT_TOKEN environment variable is required but not set."
        )

    return Config(
        telegram_bot_token=token,
        telegram_owner_id=int(os.environ.get("TELEGRAM_OWNER_ID", "0")),
        kb_query_base_url=os.environ.get(
            "KB_QUERY_BASE_URL", "http://kb-query:8000"
        ).rstrip("/"),
        http_timeout_sec=float(os.environ.get("HTTP_TIMEOUT_SEC", "5")),
        http_max_retries=int(os.environ.get("HTTP_MAX_RETRIES", "3")),
        default_limit=int(os.environ.get("DEFAULT_LIMIT", "5")),
        max_limit=int(os.environ.get("MAX_LIMIT", "50")),
        log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        metrics_port=int(os.environ.get("METRICS_PORT", "9200")),
        state_file=os.environ.get(
            "STATE_FILE", "/var/lib/telegram-bot/state.json"
        ),
        rate_limit_messages_per_sec=float(
            os.environ.get("RATE_LIMIT_MESSAGES_PER_SEC", "1.0")
        ),
        llm_provider=os.environ.get("LLM_PROVIDER", "anthropic").lower(),
        llm_api_key=os.environ.get("LLM_API_KEY", ""),
        llm_model=os.environ.get("LLM_MODEL", ""),
        llm_base_url=os.environ.get("LLM_BASE_URL", ""),
        llm_timeout_sec=float(os.environ.get("LLM_TIMEOUT_SEC", "30")),
        llm_max_retries=int(os.environ.get("LLM_MAX_RETRIES", "2")),
        llm_rag_limit=int(os.environ.get("LLM_RAG_LIMIT", "5")),
    )
