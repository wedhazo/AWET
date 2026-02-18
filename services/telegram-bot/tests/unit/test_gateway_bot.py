"""Unit tests for Telegram gateway behavior.

We test the deterministic routing logic without hitting Telegram network.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from telegram_bot.bot import TelegramBot, build_correlation_id
from telegram_bot.config import Config
from telegram_bot.state import BotState


class FakeKB:
    def __init__(self):
        self.search_calls = []
        self.source_calls = []

    async def search(self, q: str, *, ticker=None, from_date=None, to_date=None, limit=5, correlation_id: str):
        self.search_calls.append(
            {
                "q": q,
                "ticker": ticker,
                "from_date": from_date,
                "to_date": to_date,
                "limit": limit,
                "correlation_id": correlation_id,
            }
        )
        return []

    async def get_source(self, source_id: str, *, correlation_id: str):
        self.source_calls.append({"source_id": source_id, "correlation_id": correlation_id})
        return {
            "title": "Title",
            "subreddit": "sub",
            "created_at_utc": "2026-02-18T00:00:00Z",
            "url": "https://example.com",
            "body": "x" * 1300,
        }


@pytest.mark.asyncio
async def test_plain_text_does_not_call_kb_query():
    kb = FakeKB()
    cfg = Config(telegram_bot_token="t", telegram_owner_id=0)
    state = BotState("/tmp/awet-test-state.json")
    bot = TelegramBot(config=cfg, kb_client=kb, state=state)

    reply = await bot._handle_text(
        chat_id=1,
        message_id=2,
        date=3,
        text="hello",
        correlation_id="cid",
    )
    assert reply == "Use /search or /help"
    assert kb.search_calls == []
    assert kb.source_calls == []


@pytest.mark.asyncio
async def test_search_calls_kb_query_with_correct_params():
    kb = FakeKB()
    cfg = Config(telegram_bot_token="t", telegram_owner_id=0, default_limit=5)
    state = BotState("/tmp/awet-test-state.json")
    bot = TelegramBot(config=cfg, kb_client=kb, state=state)

    reply = await bot._handle_text(
        chat_id=1,
        message_id=2,
        date=3,
        text="/search tsla earnings ticker:TSLA limit:3",
        correlation_id="cid-123",
    )
    assert reply == "No results."
    assert len(kb.search_calls) == 1
    call = kb.search_calls[0]
    assert call["q"] == "tsla earnings"
    assert call["ticker"] == "TSLA"
    assert call["limit"] == 3
    assert call["correlation_id"] == "cid-123"


@pytest.mark.asyncio
async def test_source_calls_endpoint_and_truncates_body():
    kb = FakeKB()
    cfg = Config(telegram_bot_token="t", telegram_owner_id=0)
    state = BotState("/tmp/awet-test-state.json")
    bot = TelegramBot(config=cfg, kb_client=kb, state=state)

    reply = await bot._handle_text(
        chat_id=1,
        message_id=2,
        date=3,
        text="/source abc123",
        correlation_id="cid-x",
    )

    assert len(kb.source_calls) == 1
    assert kb.source_calls[0]["source_id"] == "abc123"
    assert "body:" in reply
    assert reply.endswith("...")


def test_correlation_id_deterministic():
    c1 = build_correlation_id(chat_id=1, message_id=2, date=3)
    c2 = build_correlation_id(chat_id=1, message_id=2, date=3)
    assert c1 == c2
    assert len(c1) == 64
