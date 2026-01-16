from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncpg
import structlog
from prometheus_client import Counter, Histogram

from src.core.config import load_settings
from src.core.logging import get_correlation_id
from src.llm.client import LLMClient
from src.monitoring.metrics import REGISTRY

LLM_REQUESTS_TOTAL = Counter(
    "llm_requests_total",
    "Total LLM requests",
    ["operation"],
    registry=REGISTRY,
)

LLM_ERRORS_TOTAL = Counter(
    "llm_errors_total",
    "Total LLM errors",
    ["operation"],
    registry=REGISTRY,
)

LLM_LATENCY_SECONDS = Histogram(
    "llm_latency_seconds",
    "LLM latency in seconds",
    ["operation"],
    registry=REGISTRY,
)

SYSTEM_PROMPT = (
    "You are the AWET orchestration assistant. Follow the 3-layer architecture: "
    "Directives define what to do, orchestration decides, execution is deterministic. "
    "Paper trading only. Do not place trades directly."
)


async def generate_plan(directive_name: str, context: dict[str, Any]) -> str:
    settings = load_settings()
    client = LLMClient(settings.llm)
    root = Path(__file__).resolve().parents[2]
    directive_path = root / "directives" / f"{directive_name}.md"
    directive_text = directive_path.read_text(encoding="utf-8")
    correlation_id = str(context.get("correlation_id")) if "correlation_id" in context else None
    correlation_id = correlation_id or get_correlation_id()

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                "Directive:\n"
                f"{directive_text}\n\n"
                "Context (JSON):\n"
                f"{json.dumps(context, default=str, indent=2)}"
            ),
        },
    ]

    logger = structlog.get_logger(__name__)
    LLM_REQUESTS_TOTAL.labels("generate_plan").inc()
    start = time.perf_counter()
    try:
        result = await client.chat(messages)
        return result
    except Exception:
        LLM_ERRORS_TOTAL.labels("generate_plan").inc()
        logger.exception("llm_generate_plan_failed", correlation_id=correlation_id)
        raise
    finally:
        elapsed = time.perf_counter() - start
        LLM_LATENCY_SECONDS.labels("generate_plan").observe(elapsed)
        logger.info("llm_generate_plan_completed", correlation_id=correlation_id)


async def explain_run(correlation_id: str) -> str:
    settings = load_settings()
    client = LLMClient(settings.llm)
    events = await _fetch_audit_trail(correlation_id)

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                "Summarize the following pipeline run in plain English."
                "\nCorrelation ID: "
                f"{correlation_id}\n\n"
                "Audit events (JSON):\n"
                f"{json.dumps(events, default=str, indent=2)}"
            ),
        },
    ]

    logger = structlog.get_logger(__name__)
    LLM_REQUESTS_TOTAL.labels("explain_run").inc()
    start = time.perf_counter()
    try:
        result = await client.chat(messages)
        return result
    except Exception:
        LLM_ERRORS_TOTAL.labels("explain_run").inc()
        logger.exception("llm_explain_run_failed", correlation_id=correlation_id)
        raise
    finally:
        elapsed = time.perf_counter() - start
        LLM_LATENCY_SECONDS.labels("explain_run").observe(elapsed)
        logger.info("llm_explain_run_completed", correlation_id=correlation_id)


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


async def _fetch_audit_trail(correlation_id: str) -> list[dict[str, Any]]:
    dsn = (
        f"postgresql://{_env('POSTGRES_USER', 'awet')}:{_env('POSTGRES_PASSWORD', 'awet')}"
        f"@{_env('POSTGRES_HOST', 'localhost')}:{_env('POSTGRES_PORT', '5433')}"
        f"/{_env('POSTGRES_DB', 'awet')}"
    )
    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=3)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT event_type, ts, payload
                FROM audit_events
                WHERE correlation_id=$1
                ORDER BY ts ASC
                LIMIT 500
                """,
                correlation_id,
            )
            events: list[dict[str, Any]] = []
            for row in rows:
                ts_value = row["ts"]
                if isinstance(ts_value, datetime):
                    ts_value = ts_value.isoformat()
                events.append(
                    {
                        "event_type": row["event_type"],
                        "ts": ts_value,
                        "payload": row["payload"],
                    }
                )
            return events
    finally:
        await pool.close()
