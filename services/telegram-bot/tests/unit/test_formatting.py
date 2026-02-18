"""Unit tests for deterministic gateway formatter.

These tests validate:
- /help is EXACT (byte-for-byte)
- /search formatting is deterministic
- /source truncation rules
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from telegram_bot.formatter import HELP_TEXT, format_help, format_search_results, format_source


def test_help_exact_match():
    assert format_help() == HELP_TEXT


def test_search_no_results():
    assert format_search_results([]) == "No results."


def test_search_deterministic_and_contains_fields():
    results = [
        {
            "subreddit": "wallstreetbets",
            "title": "TSLA Q4 Earnings",
            "created_utc": 1704067200,
            "url": "https://reddit.com/r/wallstreetbets/comments/abc",
            "source_id": "abc",
            "chunk_id": "chunk_1",
        }
    ]
    out1 = format_search_results(results)
    out2 = format_search_results(results)
    assert out1 == out2
    assert "Results (1):" in out1
    assert "- [wallstreetbets] TSLA Q4 Earnings (2024-01-01) — source_id=abc" in out1
    assert "url: https://reddit.com" in out1
    assert "chunk_id: chunk_1" in out1


def test_source_truncation():
    source = {
        "title": "T",
        "subreddit": "investing",
        "created_at_utc": "2026-02-18T00:00:00Z",
        "url": "https://example.com",
        "body": "x" * 1300,
    }
    out = format_source(source)
    assert "title: T" in out
    assert "subreddit: investing" in out
    assert "created_at_utc:" in out
    assert "url: https://example.com" in out
    assert "body:\n" in out
    assert out.endswith("...")
