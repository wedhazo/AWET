"""Async HTTP client for the kb-query service.

Design decisions:
- Uses httpx.AsyncClient with a shared connection pool (one per process).
- Every request carries X-Correlation-Id for end-to-end tracing.
- Retries transient failures (5xx, connection errors, timeouts) up to
  ``max_retries`` times with exponential back-off.
- Timeouts and non-200 responses are reported as typed exceptions so
  callers can route them to the correct user-facing message.
- User-Agent is pinned to "awet-telegram-bot/1.0" per spec.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import httpx

from telegram_bot.log import get_logger
from telegram_bot.metrics import (
    KB_QUERY_LATENCY,
    KB_QUERY_REQUESTS_TOTAL,
)

logger = get_logger(__name__)

_USER_AGENT = "awet-telegram-bot/1.0"

# Status codes that are safe to retry
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class KBQueryError(Exception):
    """Non-retryable KB-query error (4xx, unexpected)."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class KBQueryTimeout(KBQueryError):
    """Request to kb-query timed out."""


@dataclass
class KBClient:
    """Async wrapper around the kb-query HTTP service."""

    base_url: str
    timeout_sec: float = 5.0
    max_retries: int = 3
    _client: httpx.AsyncClient | None = None

    # ── Lifecycle ──────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Create the shared AsyncClient. Call once at startup."""
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.timeout_sec,
            headers={"User-Agent": _USER_AGENT},
            follow_redirects=True,
        )

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    # ── Public methods ─────────────────────────────────────────────────────

    async def search(
        self,
        q: str,
        *,
        ticker: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int = 5,
        correlation_id: str,
    ) -> list[dict[str, Any]]:
        """Search the knowledge base. Returns list of result dicts."""
        params: dict[str, Any] = {"q": q, "limit": limit}
        if ticker:
            params["ticker"] = ticker
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date

        data = await self._get("/search", params=params, correlation_id=correlation_id)
        # kb-query may return {"results": [...]} or a bare list
        if isinstance(data, dict):
            return data.get("results", [])
        if isinstance(data, list):
            return data
        return []

    async def get_source(
        self,
        source_id: str,
        *,
        correlation_id: str,
    ) -> dict[str, Any]:
        """Fetch a single source by ID."""
        data = await self._get(
            f"/source/{source_id}", params={}, correlation_id=correlation_id
        )
        if isinstance(data, dict):
            return data
        return {}

    async def healthz(self) -> bool:
        """Return True if kb-query /healthz responds 200."""
        if not self._client:
            return False
        try:
            r = await self._client.get("/healthz", timeout=3.0)
            return r.status_code == 200
        except Exception:
            return False

    # ── Private helpers ────────────────────────────────────────────────────

    async def _get(
        self,
        path: str,
        params: dict[str, Any],
        correlation_id: str,
    ) -> Any:
        """Execute a GET with retries + metrics."""
        assert self._client is not None, "KBClient.start() was not called"

        headers = {"X-Correlation-Id": correlation_id}
        attempt = 0
        delay = 0.5  # seconds, doubled each retry

        while True:
            attempt += 1
            with KB_QUERY_LATENCY.time():
                try:
                    response = await self._client.get(
                        path,
                        params=params,
                        headers=headers,
                    )
                except (httpx.TimeoutException, asyncio.TimeoutError) as exc:
                    logger.warning(
                        "kb_query_timeout",
                        correlation_id=correlation_id,
                        path=path,
                        attempt=attempt,
                    )
                    if attempt >= self.max_retries:
                        KB_QUERY_REQUESTS_TOTAL.labels(status="timeout").inc()
                        raise KBQueryTimeout(f"kb-query timed out after {attempt} attempts") from exc
                    await asyncio.sleep(delay)
                    delay *= 2
                    continue
                except httpx.RequestError as exc:
                    logger.warning(
                        "kb_query_request_error",
                        correlation_id=correlation_id,
                        path=path,
                        attempt=attempt,
                        error=str(exc),
                    )
                    if attempt >= self.max_retries:
                        KB_QUERY_REQUESTS_TOTAL.labels(status="error").inc()
                        raise KBQueryError(f"kb-query connection error: {exc}") from exc
                    await asyncio.sleep(delay)
                    delay *= 2
                    continue

            status_code = response.status_code
            if status_code == 200:
                KB_QUERY_REQUESTS_TOTAL.labels(status="ok").inc()
                logger.debug(
                    "kb_query_ok",
                    correlation_id=correlation_id,
                    path=path,
                    attempt=attempt,
                )
                return response.json()

            if status_code in _RETRYABLE_STATUS and attempt < self.max_retries:
                logger.warning(
                    "kb_query_retryable_error",
                    correlation_id=correlation_id,
                    path=path,
                    status=status_code,
                    attempt=attempt,
                )
                await asyncio.sleep(delay)
                delay *= 2
                continue

            # Non-retryable failure
            KB_QUERY_REQUESTS_TOTAL.labels(status="error").inc()
            logger.error(
                "kb_query_error",
                correlation_id=correlation_id,
                path=path,
                status=status_code,
            )
            raise KBQueryError(
                f"kb-query returned HTTP {status_code}",
                status_code=status_code,
            )
