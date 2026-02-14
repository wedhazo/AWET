"""
LLM Tracing - Always-on observability for LLM calls.

This module provides:
1. Structured logging of all LLM requests/responses (LLM_TRACE prefix)
2. Optional database persistence to llm_traces table
3. Redaction of sensitive data (API keys, emails, long numbers)
4. Daily summary generation

Usage:
    from src.llm.tracing import LLMTracer, get_tracer
    
    tracer = get_tracer(settings)
    await tracer.trace(
        agent_name="RiskAgent",
        correlation_id="abc-123",
        model="llama3.2:1b",
        base_url="http://localhost:11434/v1",
        request_messages=[{"role": "user", "content": "Hello"}],
        response_content="Hi there!",
        latency_ms=150.5,
        status="ok",
    )
"""
from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import asyncpg
import structlog

logger = structlog.get_logger(__name__)

# Patterns for redaction
REDACTION_PATTERNS = [
    # API keys (common patterns)
    (re.compile(r'(sk-[a-zA-Z0-9]{20,})', re.IGNORECASE), '[REDACTED_API_KEY]'),
    (re.compile(r'(pk[a-zA-Z0-9_]{20,})', re.IGNORECASE), '[REDACTED_API_KEY]'),
    (re.compile(r'(api[_-]?key["\s:=]+)["\']?([a-zA-Z0-9_-]{20,})["\']?', re.IGNORECASE), r'\1[REDACTED]'),
    # Emails
    (re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'), '[REDACTED_EMAIL]'),
    # Long numbers (12+ digits) - credit cards, account numbers
    (re.compile(r'\b\d{12,}\b'), '[REDACTED_NUMBER]'),
    # Bearer tokens
    (re.compile(r'(Bearer\s+)[a-zA-Z0-9._-]{20,}', re.IGNORECASE), r'\1[REDACTED_TOKEN]'),
    # Common secret key patterns
    (re.compile(r'(secret[_-]?key["\s:=]+)["\']?([a-zA-Z0-9_-]{20,})["\']?', re.IGNORECASE), r'\1[REDACTED]'),
]

# Environment variables that should be redacted from content
SENSITIVE_ENV_VARS = [
    'OPENAI_API_KEY',
    'ALPACA_API_KEY',
    'ALPACA_SECRET_KEY',
    'POLYGON_API_KEY',
    'ANTHROPIC_API_KEY',
    'DATABASE_URL',
    'POSTGRES_PASSWORD',
]


def redact_sensitive(text: str) -> str:
    """Redact sensitive information from text."""
    if not text:
        return text
    
    result = text
    
    # Redact env var values if they appear in the text
    for env_var in SENSITIVE_ENV_VARS:
        value = os.getenv(env_var)
        if value and len(value) > 8:  # Only redact non-trivial values
            result = result.replace(value, f'[REDACTED_{env_var}]')
    
    # Apply pattern-based redaction
    for pattern, replacement in REDACTION_PATTERNS:
        result = pattern.sub(replacement, result)
    
    return result


def truncate_preview(text: str, max_chars: int) -> str:
    """Truncate text for preview logging."""
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + f"... [{len(text) - max_chars} more chars]"


@dataclass
class LLMTraceRecord:
    """A single LLM trace record."""
    ts: datetime
    correlation_id: str
    agent_name: str
    model: str
    base_url: str
    latency_ms: float
    status: str  # "ok" or "error"
    error_message: str | None
    request: dict[str, Any]
    response: dict[str, Any]
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    
    def to_log_dict(self, preview_chars: int = 800) -> dict[str, Any]:
        """Convert to a dict for structured logging."""
        request_str = json.dumps(self.request, default=str)
        response_str = json.dumps(self.response, default=str)
        
        return {
            "ts": self.ts.isoformat(),
            "correlation_id": self.correlation_id,
            "agent_name": self.agent_name,
            "model": self.model,
            "base_url": redact_sensitive(self.base_url),
            "latency_ms": round(self.latency_ms, 2),
            "status": self.status,
            "error_message": self.error_message,
            "request_preview": truncate_preview(redact_sensitive(request_str), preview_chars),
            "response_preview": truncate_preview(redact_sensitive(response_str), preview_chars),
            "request_chars": len(request_str),
            "response_chars": len(response_str),
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
        }


@dataclass
class LLMTracerConfig:
    """Configuration for LLM tracing."""
    enabled: bool = True
    preview_chars: int = 800
    log_level: str = "INFO"
    store_db: bool = True
    db_dsn: str = ""


class LLMTracer:
    """
    Centralized LLM tracing for observability.
    
    Features:
    - Structured logging with LLM_TRACE prefix
    - Optional Postgres/TimescaleDB persistence
    - Sensitive data redaction
    - Configurable preview length
    """
    
    def __init__(self, config: LLMTracerConfig) -> None:
        self._config = config
        self._pool: asyncpg.Pool | None = None
        self._logger = structlog.get_logger(__name__)
    
    async def connect(self) -> None:
        """Connect to database for trace persistence."""
        if not self._config.store_db or not self._config.db_dsn:
            return
        
        try:
            self._pool = await asyncpg.create_pool(
                self._config.db_dsn,
                min_size=1,
                max_size=5,
            )
            self._logger.info("llm_tracer_db_connected")
        except Exception as e:
            self._logger.warning("llm_tracer_db_connect_failed", error=str(e))
            self._pool = None
    
    async def close(self) -> None:
        """Close database connection."""
        if self._pool:
            await self._pool.close()
            self._pool = None
    
    async def trace(
        self,
        agent_name: str,
        correlation_id: str,
        model: str,
        base_url: str,
        request_messages: list[dict[str, str]],
        response_content: str | None,
        latency_ms: float,
        status: str = "ok",
        error_message: str | None = None,
        prompt_tokens: int | None = None,
        completion_tokens: int | None = None,
    ) -> None:
        """
        Record an LLM trace.
        
        Args:
            agent_name: Name of the calling agent
            correlation_id: Request correlation ID
            model: LLM model name
            base_url: API base URL
            request_messages: Chat messages sent to LLM
            response_content: LLM response text (or None if error)
            latency_ms: Request latency in milliseconds
            status: "ok" or "error"
            error_message: Error description if status="error"
            prompt_tokens: Number of prompt tokens (if available)
            completion_tokens: Number of completion tokens (if available)
        """
        if not self._config.enabled:
            return
        
        # Auto-connect to DB if needed and configured
        if self._config.store_db and self._pool is None:
            await self.connect()
        
        record = LLMTraceRecord(
            ts=datetime.now(tz=timezone.utc),
            correlation_id=correlation_id,
            agent_name=agent_name,
            model=model,
            base_url=base_url,
            latency_ms=latency_ms,
            status=status,
            error_message=error_message,
            request={"messages": request_messages},
            response={"content": response_content} if response_content else {"error": error_message},
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )
        
        # Log to stdout with LLM_TRACE prefix
        log_data = record.to_log_dict(self._config.preview_chars)
        self._logger.info("LLM_TRACE", **log_data)
        
        # Persist to DB if enabled
        if self._config.store_db and self._pool:
            await self._persist_trace(record)
    
    async def _persist_trace(self, record: LLMTraceRecord) -> None:
        """Persist trace record to database."""
        if not self._pool:
            return
        
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO llm_traces (
                        ts, correlation_id, agent_name, model, base_url,
                        latency_ms, status, error_message,
                        request, response, prompt_tokens, completion_tokens
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
                    )
                    """,
                    record.ts,
                    record.correlation_id,
                    record.agent_name,
                    record.model,
                    redact_sensitive(record.base_url),
                    int(record.latency_ms),
                    record.status,
                    record.error_message,
                    json.dumps(record.request, default=str),
                    json.dumps(record.response, default=str),
                    record.prompt_tokens,
                    record.completion_tokens,
                )
        except Exception as e:
            self._logger.warning("llm_trace_persist_failed", error=str(e))
    
    async def get_daily_summary(self, date: datetime | None = None) -> dict[str, Any]:
        """
        Generate daily summary of LLM usage.
        
        Returns:
            Dictionary with call counts, errors, latencies, top agents/models
        """
        if not self._pool:
            return {"error": "DB not connected"}
        
        if date is None:
            date = datetime.now(tz=timezone.utc)
        
        start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = date.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        async with self._pool.acquire() as conn:
            # Basic stats
            stats = await conn.fetchrow(
                """
                SELECT 
                    COUNT(*) as total_calls,
                    COUNT(*) FILTER (WHERE status = 'error') as error_count,
                    AVG(latency_ms) as avg_latency_ms,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) as p95_latency_ms
                FROM llm_traces
                WHERE ts >= $1 AND ts <= $2
                """,
                start_of_day, end_of_day
            )
            
            # Top agents
            top_agents = await conn.fetch(
                """
                SELECT agent_name, COUNT(*) as call_count
                FROM llm_traces
                WHERE ts >= $1 AND ts <= $2
                GROUP BY agent_name
                ORDER BY call_count DESC
                LIMIT 10
                """,
                start_of_day, end_of_day
            )
            
            # Top models
            top_models = await conn.fetch(
                """
                SELECT model, COUNT(*) as call_count
                FROM llm_traces
                WHERE ts >= $1 AND ts <= $2
                GROUP BY model
                ORDER BY call_count DESC
                LIMIT 10
                """,
                start_of_day, end_of_day
            )
        
        return {
            "date": date.strftime("%Y-%m-%d"),
            "total_calls": stats["total_calls"] if stats else 0,
            "error_count": stats["error_count"] if stats else 0,
            "avg_latency_ms": round(stats["avg_latency_ms"] or 0, 2),
            "p95_latency_ms": round(stats["p95_latency_ms"] or 0, 2),
            "top_agents": [{"agent": r["agent_name"], "calls": r["call_count"]} for r in top_agents],
            "top_models": [{"model": r["model"], "calls": r["call_count"]} for r in top_models],
        }


# Global tracer instance
_tracer: LLMTracer | None = None


def get_tracer(settings: Any = None) -> LLMTracer:
    """Get or create the global LLM tracer."""
    global _tracer
    
    if _tracer is not None:
        return _tracer
    
    # Build config from settings
    if settings is not None:
        llm_config = getattr(settings, 'llm', None)
        app_config = getattr(settings, 'app', None)
        
        config = LLMTracerConfig(
            enabled=getattr(llm_config, 'trace_enabled', True) if llm_config else True,
            preview_chars=getattr(llm_config, 'trace_preview_chars', 800) if llm_config else 800,
            log_level=getattr(llm_config, 'trace_log_level', 'INFO') if llm_config else 'INFO',
            store_db=getattr(llm_config, 'trace_store_db', True) if llm_config else True,
            db_dsn=_build_db_dsn(),
        )
    else:
        config = LLMTracerConfig(db_dsn=_build_db_dsn())
    
    _tracer = LLMTracer(config)
    return _tracer


def _build_db_dsn() -> str:
    """Build database DSN from environment."""
    return os.getenv(
        "DATABASE_URL",
        f"postgresql://{os.getenv('POSTGRES_USER', 'awet')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'awet')}"
        f"@{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5433')}/"
        f"{os.getenv('POSTGRES_DB', 'awet')}"
    )


async def log_daily_summary() -> None:
    """Log daily summary (called by scheduler at 23:59)."""
    tracer = get_tracer()
    if not tracer._pool:
        await tracer.connect()
    
    summary = await tracer.get_daily_summary()
    
    logger.info(
        "LLM_DAILY_SUMMARY",
        **summary,
    )
    
    # Optionally persist to summary table
    if tracer._pool:
        try:
            async with tracer._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO llm_daily_summary (
                        summary_date, total_calls, error_count,
                        avg_latency_ms, p95_latency_ms,
                        top_agents, top_models
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (summary_date) DO UPDATE SET
                        total_calls = EXCLUDED.total_calls,
                        error_count = EXCLUDED.error_count,
                        avg_latency_ms = EXCLUDED.avg_latency_ms,
                        p95_latency_ms = EXCLUDED.p95_latency_ms,
                        top_agents = EXCLUDED.top_agents,
                        top_models = EXCLUDED.top_models,
                        updated_at = NOW()
                    """,
                    datetime.now(tz=timezone.utc).date(),
                    summary["total_calls"],
                    summary["error_count"],
                    summary["avg_latency_ms"],
                    summary["p95_latency_ms"],
                    json.dumps(summary["top_agents"]),
                    json.dumps(summary["top_models"]),
                )
        except Exception as e:
            logger.warning("llm_daily_summary_persist_failed", error=str(e))
