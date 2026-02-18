"""Telegram polling loop and deterministic gateway to kb-query.

Hard rules (enforced here):
- Only supported commands: /help, /search, /source
- Any non-command text (not starting with '/') replies exactly: "Use /search or /help"
- No LLM, no RAG, no assistant behavior
- Deterministic correlation_id per update:
    sha256(f"{chat_id}:{message_id}:{date}") as hex
- Every kb-query call must forward X-Correlation-Id
- Deterministic formatting and ordering
"""
from __future__ import annotations

import asyncio
import hashlib
import time
from typing import Any

import httpx

from telegram_bot.config import Config
from telegram_bot.formatter import format_help, format_search_results, format_source
from telegram_bot.kb_client import KBClient, KBQueryError, KBQueryTimeout
from telegram_bot.log import get_logger
from telegram_bot.metrics import BOT_REPLIES_TOTAL, TELEGRAM_UPDATES_TOTAL
from telegram_bot.parser import HelpCommand, SearchCommand, SourceCommand, UnknownCommand, parse
from telegram_bot.state import BotState

logger = get_logger(__name__)

_POLL_TIMEOUT_SEC = 30
_RETRY_DELAY_SEC = 5.0


class TelegramBot:
    def __init__(
        self,
        config: Config,
        kb_client: KBClient,
        state: BotState,
    ) -> None:
        self._cfg = config
        self._kb = kb_client
        self._state = state
        self._base = f"https://api.telegram.org/bot{config.telegram_bot_token}"
        self._tg: httpx.AsyncClient | None = None

    async def start(self) -> None:
        self._tg = httpx.AsyncClient(
            timeout=_POLL_TIMEOUT_SEC + 5,
            headers={"User-Agent": "awet-telegram-gateway/1.0"},
        )

    async def close(self) -> None:
        if self._tg:
            await self._tg.aclose()

    async def run(self) -> None:
        logger.info(
            "polling_started",
            correlation_id="SYSTEM",
            status="ok",
        )
        while True:
            try:
                updates = await self._get_updates()
            except (httpx.TimeoutException, asyncio.TimeoutError):
                continue
            except Exception as exc:
                logger.error(
                    "get_updates_error",
                    correlation_id="SYSTEM",
                    status="error",
                    error=str(exc),
                )
                await asyncio.sleep(_RETRY_DELAY_SEC)
                continue

            for update in updates:
                await self._process_update(update)

    async def _get_updates(self) -> list[dict]:
        assert self._tg is not None
        offset = self._state.last_update_id + 1
        resp = await self._tg.get(
            f"{self._base}/getUpdates",
            params={"offset": offset, "timeout": _POLL_TIMEOUT_SEC, "limit": 100},
        )
        data = resp.json()
        if not data.get("ok"):
            raise RuntimeError(f"getUpdates not ok: {data.get('description')}")
        return data.get("result", [])

    async def _process_update(self, update: dict) -> None:
        update_id: int = update["update_id"]

        message = update.get("message") or update.get("edited_message")
        if not message:
            TELEGRAM_UPDATES_TOTAL.labels(type="other").inc()
            self._state.mark_processed(update_id)
            return

        chat_id: int = message["chat"]["id"]
        message_id: int = message.get("message_id", 0)
        date: int = message.get("date", 0)
        text: str = (message.get("text") or "").strip()

        correlation_id = build_correlation_id(chat_id=chat_id, message_id=message_id, date=date)

        is_command = text.startswith("/")
        TELEGRAM_UPDATES_TOTAL.labels(type="command" if is_command else "message").inc()

        command = text.split()[0].lower() if is_command and text else ""

        logger.info(
            "update_received",
            correlation_id=correlation_id,
            chat_id=chat_id,
            message_id=message_id,
            command=command,
            status="ok",
        )

        start = time.monotonic()
        try:
            reply = await self._handle_text(
                chat_id=chat_id,
                message_id=message_id,
                date=date,
                text=text,
                correlation_id=correlation_id,
            )
            await self._send(chat_id, reply, correlation_id)
            latency_ms = int((time.monotonic() - start) * 1000)
            logger.info(
                "reply_sent",
                correlation_id=correlation_id,
                chat_id=chat_id,
                message_id=message_id,
                command=command,
                status="ok",
                latency_ms=latency_ms,
            )
        except Exception as exc:
            latency_ms = int((time.monotonic() - start) * 1000)
            logger.error(
                "update_failed",
                correlation_id=correlation_id,
                chat_id=chat_id,
                message_id=message_id,
                command=command,
                status="error",
                latency_ms=latency_ms,
                error=str(exc),
            )

        self._state.mark_processed(update_id)

    async def _handle_text(
        self,
        *,
        chat_id: int,
        message_id: int,
        date: int,
        text: str,
        correlation_id: str,
    ) -> str:
        # 2) Any non-command text: exact response, no kb-query call.
        if not text.startswith("/"):
            return "Use /search or /help"

        cmd = parse(text)

        if isinstance(cmd, HelpCommand) or isinstance(cmd, UnknownCommand):
            return format_help()

        if isinstance(cmd, SearchCommand):
            return await self._handle_search(cmd, correlation_id)

        if isinstance(cmd, SourceCommand):
            return await self._handle_source(cmd, correlation_id)

        return format_help()

    async def _handle_search(self, cmd: SearchCommand, correlation_id: str) -> str:
        limit = cmd.limit or self._cfg.default_limit
        limit = max(1, min(int(limit), 10))

        started = time.monotonic()
        try:
            results = await self._kb.search(
                cmd.query,
                ticker=cmd.ticker,
                from_date=cmd.from_date,
                to_date=cmd.to_date,
                limit=limit,
                correlation_id=correlation_id,
            )
            latency_ms = int((time.monotonic() - started) * 1000)
            logger.info(
                "kb_search_ok",
                correlation_id=correlation_id,
                command="/search",
                status="ok",
                latency_ms=latency_ms,
            )
        except KBQueryTimeout as exc:
            latency_ms = int((time.monotonic() - started) * 1000)
            logger.warning(
                "kb_search_timeout",
                correlation_id=correlation_id,
                command="/search",
                status="timeout",
                latency_ms=latency_ms,
                error=str(exc),
            )
            return "No results."
        except KBQueryError as exc:
            latency_ms = int((time.monotonic() - started) * 1000)
            logger.error(
                "kb_search_error",
                correlation_id=correlation_id,
                command="/search",
                status="error",
                latency_ms=latency_ms,
                error=str(exc),
            )
            return "No results."

        # Deterministic ordering regardless of upstream ordering.
        results_sorted = sorted(
            results,
            key=lambda r: (
                str(r.get("source_id") or r.get("id") or ""),
                str(r.get("chunk_id") or ""),
            ),
        )
        return format_search_results(results_sorted)

    async def _handle_source(self, cmd: SourceCommand, correlation_id: str) -> str:
        if not cmd.source_id:
            return "Use /search or /help"

        started = time.monotonic()
        try:
            source = await self._kb.get_source(cmd.source_id, correlation_id=correlation_id)
            latency_ms = int((time.monotonic() - started) * 1000)
            logger.info(
                "kb_source_ok",
                correlation_id=correlation_id,
                command="/source",
                status="ok",
                latency_ms=latency_ms,
            )
        except KBQueryTimeout as exc:
            latency_ms = int((time.monotonic() - started) * 1000)
            logger.warning(
                "kb_source_timeout",
                correlation_id=correlation_id,
                command="/source",
                status="timeout",
                latency_ms=latency_ms,
                error=str(exc),
            )
            return "No results."
        except KBQueryError as exc:
            latency_ms = int((time.monotonic() - started) * 1000)
            logger.error(
                "kb_source_error",
                correlation_id=correlation_id,
                command="/source",
                status="error",
                latency_ms=latency_ms,
                error=str(exc),
            )
            return "No results."

        return format_source(source)

    async def _send(self, chat_id: int, text: str, correlation_id: str) -> bool:
        assert self._tg is not None
        delay = 1.0
        for attempt in range(1, self._cfg.http_max_retries + 1):
            try:
                resp = await self._tg.post(
                    f"{self._base}/sendMessage",
                    json={
                        "chat_id": chat_id,
                        "text": text,
                    },
                    timeout=10.0,
                )
                if resp.status_code == 200:
                    BOT_REPLIES_TOTAL.labels(status="ok").inc()
                    return True

                logger.warning(
                    "send_message_error",
                    correlation_id=correlation_id,
                    chat_id=chat_id,
                    status="error",
                    attempt=attempt,
                    http_status=resp.status_code,
                )
            except Exception as exc:
                logger.warning(
                    "send_message_exception",
                    correlation_id=correlation_id,
                    chat_id=chat_id,
                    status="error",
                    attempt=attempt,
                    error=str(exc),
                )
            if attempt < self._cfg.http_max_retries:
                await asyncio.sleep(delay)
                delay *= 2

        BOT_REPLIES_TOTAL.labels(status="error").inc()
        return False


def build_correlation_id(*, chat_id: int, message_id: int, date: int) -> str:
    raw = f"{chat_id}:{message_id}:{date}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()
