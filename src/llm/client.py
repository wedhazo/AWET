from __future__ import annotations

import asyncio
import os
import time
from typing import Any

import httpx
import structlog

from src.core.config import LLMConfig
from src.core.logging import get_correlation_id
from src.llm.types import ChatCompletionRequest, ChatCompletionResponse, ChatMessage

# Threshold for logging slow operations (seconds)
SLOW_OP_SECONDS = float(os.getenv("SLOW_OP_SECONDS", "2.0"))


def _build_timeout(config: LLMConfig) -> httpx.Timeout:
    """Build httpx.Timeout from config with env override.

    LLM_TIMEOUT_SECONDS env var overrides config.timeout_seconds.
    Read timeout is the main bottleneck for LLM inference.
    """
    timeout_seconds = int(os.getenv("LLM_TIMEOUT_SECONDS", str(config.timeout_seconds)))
    return httpx.Timeout(
        connect=30.0,           # Connection setup
        read=float(timeout_seconds),  # Response read (main bottleneck)
        write=30.0,             # Request write
        pool=10.0,              # Connection pool wait
    )


class LLMClient:
    """Async client for OpenAI-compatible LLM endpoints.

    Timeout behavior:
        - Uses LLM_TIMEOUT_SECONDS env var if set, else config.timeout_seconds.
        - Read timeout is the main bottleneck for LLM inference.
        - Logs slow operations (>SLOW_OP_SECONDS) for observability.
    """

    def __init__(self, config: LLMConfig) -> None:
        self._config = config
        self._logger = structlog.get_logger(__name__)
        self._semaphore = asyncio.Semaphore(config.concurrency_limit)
        self._base_url = config.base_url.rstrip("/")
        self._timeout = _build_timeout(config)

    async def chat(
        self, messages: list[dict[str, str]], *, tools: list[dict[str, Any]] | None = None
    ) -> str:
        request = ChatCompletionRequest(
            model=self._config.model,
            messages=[ChatMessage(**message) for message in messages],
            temperature=self._config.temperature,
            max_tokens=self._config.max_tokens,
            top_p=self._config.top_p,
            tools=tools or None,
            tool_choice="auto" if tools else None,
        )
        payload = request.model_dump(exclude_none=True)
        url = f"{self._base_url}/chat/completions"
        correlation_id = get_correlation_id()

        start = time.perf_counter()
        async with self._semaphore:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
        latency_ms = (time.perf_counter() - start) * 1000.0
        latency_s = latency_ms / 1000.0

        # Log slow operations for observability
        if latency_s > SLOW_OP_SECONDS:
            self._logger.warning(
                "llm_slow_response",
                correlation_id=correlation_id,
                latency_s=round(latency_s, 2),
                threshold_s=SLOW_OP_SECONDS,
            )

        parsed = ChatCompletionResponse.model_validate(response.json())
        content = parsed.choices[0].message.content if parsed.choices else ""
        usage = parsed.usage

        self._logger.info(
            "llm_chat_completed",
            correlation_id=correlation_id,
            prompt_tokens=usage.prompt_tokens if usage else None,
            completion_tokens=usage.completion_tokens if usage else None,
            latency_ms=latency_ms,
            model=self._config.model,
        )
        return content
