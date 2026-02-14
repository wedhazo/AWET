from __future__ import annotations

import asyncio
import os
import time
from typing import Any

import httpx
import structlog

from src.core.config import LLMConfig
from src.core.logging import get_correlation_id
from src.llm.tracing import LLMTracer, LLMTracerConfig, get_tracer
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
    
    Tracing:
        - All calls are traced via LLMTracer with LLM_TRACE log prefix.
        - Optional DB persistence to llm_traces table.
        - Sensitive data is automatically redacted.
    """

    def __init__(
        self, 
        config: LLMConfig, 
        agent_name: str = "unknown",
        tracer: LLMTracer | None = None,
    ) -> None:
        self._config = config
        self._agent_name = agent_name
        self._logger = structlog.get_logger(__name__)
        self._semaphore = asyncio.Semaphore(config.concurrency_limit)
        self._base_url = config.base_url.rstrip("/")
        self._timeout = _build_timeout(config)
        
        # Initialize tracer
        if tracer is not None:
            self._tracer = tracer
        else:
            self._tracer = get_tracer()

    async def chat(
        self, 
        messages: list[dict[str, str]], 
        *, 
        tools: list[dict[str, Any]] | None = None,
        agent_name: str | None = None,
    ) -> str:
        """
        Send chat completion request to LLM.
        
        Args:
            messages: List of chat messages
            tools: Optional list of tool definitions
            agent_name: Override agent name for tracing (defaults to init value)
        
        Returns:
            LLM response content string
        """
        effective_agent = agent_name or self._agent_name
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
        
        # Guard: warn if correlation_id is missing (indicates unbound context)
        if not correlation_id:
            # Generate a fallback to maintain traceability (better than "unknown")
            import uuid
            fallback_id = f"unbound-{uuid.uuid4().hex[:12]}"
            self._logger.warning(
                "LLM_TRACE_MISSING_CORRELATION_ID",
                agent_name=effective_agent,
                fallback_id=fallback_id,
                hint="Caller should bind correlation_id before LLM call",
            )
            correlation_id = fallback_id

        start = time.perf_counter()
        status = "ok"
        error_message = None
        content = ""
        prompt_tokens = None
        completion_tokens = None
        
        try:
            async with self._semaphore:
                async with httpx.AsyncClient(timeout=self._timeout) as client:
                    response = await client.post(url, json=payload)
                    response.raise_for_status()
            
            parsed = ChatCompletionResponse.model_validate(response.json())
            content = parsed.choices[0].message.content if parsed.choices else ""
            usage = parsed.usage
            
            if usage:
                prompt_tokens = usage.prompt_tokens
                completion_tokens = usage.completion_tokens
                
        except Exception as e:
            status = "error"
            error_message = str(e)
            self._logger.exception("llm_chat_error", error=str(e))
            raise
        finally:
            latency_ms = (time.perf_counter() - start) * 1000.0
            latency_s = latency_ms / 1000.0
            
            # Log slow operations for observability
            if latency_s > SLOW_OP_SECONDS and status == "ok":
                self._logger.warning(
                    "llm_slow_response",
                    correlation_id=correlation_id,
                    latency_s=round(latency_s, 2),
                    threshold_s=SLOW_OP_SECONDS,
                )
            
            # Trace the call
            # For long-running services, use fire-and-forget. For scripts, await.
            trace_coro = self._tracer.trace(
                agent_name=effective_agent,
                correlation_id=correlation_id or "unknown",
                model=self._config.model,
                base_url=self._base_url,
                request_messages=messages,
                response_content=content if status == "ok" else None,
                latency_ms=latency_ms,
                status=status,
                error_message=error_message,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
            )
            # Await directly to ensure trace persists before function returns
            # This adds negligible latency (just a DB insert) and guarantees persistence
            await trace_coro
            
            # Standard log line
            self._logger.info(
                "llm_chat_completed",
                correlation_id=correlation_id,
                agent_name=effective_agent,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                latency_ms=round(latency_ms, 2),
                model=self._config.model,
                status=status,
            )
        
        return content
