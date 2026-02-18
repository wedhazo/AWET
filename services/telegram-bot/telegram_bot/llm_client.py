"""LLM client supporting Claude Sonnet (Anthropic) and Kimi (Moonshot AI).

Both providers are accessed over HTTP using httpx.
Kimi uses an OpenAI-compatible API; Anthropic uses the Messages API.

Provider selection is controlled by LLM_PROVIDER env var:
  - "anthropic"  → api.anthropic.com  (default; uses claude-sonnet-4-5)
  - "kimi"       → api.moonshot.cn/v1 (uses moonshot-v1-8k)
  - "openai"     → api.openai.com/v1  (uses gpt-4o by default; openai-compat)

RAG prompt structure
--------------------
The bot calls `ask(question, context_docs)` where context_docs is a list
of KB result dicts (title, body, subreddit, url, created_utc).  The LLM
is given those docs as grounding material and asked to answer the question.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from telegram_bot.log import get_logger

logger = get_logger(__name__)

_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
_ANTHROPIC_VERSION = "2023-06-01"

_KIMI_URL = "https://api.moonshot.cn/v1/chat/completions"
_OPENAI_URL = "https://api.openai.com/v1/chat/completions"

_MAX_CONTEXT_CHARS = 6_000   # trim total doc context to stay within token budget
_TIMEOUT = 30.0
_SYSTEM_PROMPT = (
    "You are AWET, an institutional-grade financial intelligence assistant. "
    "Your answers are grounded exclusively in the Reddit KB documents provided. "
    "Be concise, factual, and cite sources by their chunk_id when possible. "
    "Never speculate beyond the provided context. "
    "If the context is insufficient, say so clearly."
)


class LLMError(Exception):
    """Non-retryable LLM API error."""


class LLMTimeout(Exception):
    """LLM request timed out."""


@dataclass
class LLMClient:
    """Unified async LLM client for Anthropic and Kimi/OpenAI-compat providers."""

    provider: str          # "anthropic" | "kimi" | "openai"
    api_key: str
    model: str
    timeout_sec: float = _TIMEOUT
    max_retries: int = 2
    base_url: str | None = None  # Optional: override API base URL (for local Ollama)
    _client: httpx.AsyncClient = field(init=False, repr=False)

    async def start(self) -> None:
        self._client = httpx.AsyncClient(timeout=self.timeout_sec)

    async def close(self) -> None:
        await self._client.aclose()

    async def ask(
        self,
        question: str,
        context_docs: list[dict],
        *,
        correlation_id: str,
    ) -> str:
        """Answer `question` grounded in `context_docs`.

        Returns the LLM's reply as a plain string.
        Raises LLMTimeout or LLMError on failure.
        """
        context_text = _build_context(context_docs)
        user_message = (
            f"Context documents:\n{context_text}\n\n"
            f"Question: {question}"
        )

        t0 = time.monotonic()
        if self.provider == "anthropic":
            reply = await self._call_anthropic(user_message, correlation_id)
        else:
            # kimi and openai both use OpenAI-compatible chat completions
            reply = await self._call_openai_compat(user_message, correlation_id)

        latency = time.monotonic() - t0
        logger.info(
            "llm_ok",
            correlation_id=correlation_id,
            provider=self.provider,
            model=self.model,
            latency=round(latency, 3),
            reply_len=len(reply),
        )
        return reply

    # ── Anthropic ─────────────────────────────────────────────────────────

    async def _call_anthropic(self, user_message: str, cid: str) -> str:
        payload = {
            "model": self.model,
            "max_tokens": 1024,
            "system": _SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": user_message}],
        }
        headers = {
            "x-api-key": self.api_key,
            "anthropic-version": _ANTHROPIC_VERSION,
            "content-type": "application/json",
            "x-correlation-id": cid,
        }
        return await self._post(_ANTHROPIC_URL, payload, headers, cid, provider="anthropic")

    async def _extract_anthropic(self, data: dict) -> str:
        content = data.get("content", [])
        if content and isinstance(content, list):
            return content[0].get("text", "").strip()
        raise LLMError(f"Unexpected Anthropic response shape: {data}")

    # ── OpenAI-compat (Kimi / OpenAI) ─────────────────────────────────────

    async def _call_openai_compat(self, user_message: str, cid: str) -> str:
        # Use custom base_url if provided (for local Ollama), otherwise use defaults
        if self.base_url:
            url = f"{self.base_url.rstrip('/')}/chat/completions"
        else:
            url = _KIMI_URL if self.provider == "kimi" else _OPENAI_URL
        payload = {
            "model": self.model,
            "max_tokens": 1024,
            "messages": [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "content-type": "application/json",
            "x-correlation-id": cid,
        }
        return await self._post(url, payload, headers, cid, provider=self.provider)

    def _extract_openai_compat(self, data: dict) -> str:
        choices = data.get("choices", [])
        if choices:
            return choices[0].get("message", {}).get("content", "").strip()
        raise LLMError(f"Unexpected OpenAI-compat response shape: {data}")

    # ── Shared HTTP ────────────────────────────────────────────────────────

    async def _post(
        self,
        url: str,
        payload: dict,
        headers: dict,
        cid: str,
        provider: str,
    ) -> str:
        delay = 1.0
        last_exc: Exception = LLMError("no attempts made")

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = await self._client.post(url, json=payload, headers=headers)

                if resp.status_code == 200:
                    data = resp.json()
                    if provider == "anthropic":
                        return await self._extract_anthropic(data)
                    return self._extract_openai_compat(data)

                if resp.status_code in (429, 500, 502, 503, 504):
                    logger.warning(
                        "llm_retryable_error",
                        correlation_id=cid,
                        provider=provider,
                        status=resp.status_code,
                        attempt=attempt,
                    )
                    last_exc = LLMError(f"HTTP {resp.status_code}")
                else:
                    body = resp.text[:300]
                    raise LLMError(f"HTTP {resp.status_code}: {body}")

            except httpx.TimeoutException as exc:
                logger.warning(
                    "llm_timeout",
                    correlation_id=cid,
                    provider=provider,
                    attempt=attempt,
                )
                last_exc = LLMTimeout(str(exc))
                # Don't retry timeouts aggressively
                break

            if attempt < self.max_retries:
                await asyncio.sleep(delay)
                delay *= 2

        if isinstance(last_exc, LLMTimeout):
            raise last_exc
        raise last_exc


# ── Helpers ───────────────────────────────────────────────────────────────────


def _build_context(docs: list[dict]) -> str:
    """Serialize KB docs into a compact context string for the LLM.

    Trims total length to _MAX_CONTEXT_CHARS to avoid token budget blowout.
    """
    if not docs:
        return "(no documents retrieved)"

    parts: list[str] = []
    total = 0

    for i, doc in enumerate(docs, 1):
        title = doc.get("title", "untitled")
        chunk_id = doc.get("chunk_id", "?")
        subreddit = doc.get("subreddit", "?")
        body = (doc.get("body") or doc.get("text") or "").strip()
        url = doc.get("url", "")

        snippet = (
            f"[{i}] chunk_id={chunk_id} | r/{subreddit} | {title}\n"
            f"    URL: {url}\n"
            f"    {body[:400]}"
        )
        if total + len(snippet) > _MAX_CONTEXT_CHARS:
            parts.append(f"[{i}] ... (truncated, {len(docs) - i + 1} more docs)")
            break
        parts.append(snippet)
        total += len(snippet)

    return "\n\n".join(parts)


def default_model(provider: str) -> str:
    """Return the recommended default model name for a provider."""
    return {
        "anthropic": "claude-sonnet-4-5",
        "kimi": "moonshot-v1-8k",
        "openai": "gpt-4o",
    }.get(provider, "claude-sonnet-4-5")
