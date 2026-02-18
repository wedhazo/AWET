"""Unit tests for the command parser.

Pure function tests — no I/O, no fixtures required.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Make the service importable without installing it
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from telegram_bot.parser import (
    HelpCommand,
    SearchCommand,
    SourceCommand,
    UnknownCommand,
    parse,
)


# ── /help ────────────────────────────────────────────────────────────────────


class TestHelpCommand:
    def test_help(self):
        assert isinstance(parse("/help"), HelpCommand)

    def test_help_uppercase(self):
        assert isinstance(parse("/HELP"), HelpCommand)

    def test_help_with_bot_suffix(self):
        assert isinstance(parse("/help@MyBot"), HelpCommand)


# ── /source ──────────────────────────────────────────────────────────────────


class TestSourceCommand:
    def test_source_with_id(self):
        cmd = parse("/source abc123")
        assert isinstance(cmd, SourceCommand)
        assert cmd.source_id == "abc123"

    def test_source_empty(self):
        cmd = parse("/source")
        assert isinstance(cmd, SourceCommand)
        assert cmd.source_id == ""

    def test_source_only_first_token(self):
        cmd = parse("/source abc extra")
        assert isinstance(cmd, SourceCommand)
        assert cmd.source_id == "abc"

    def test_source_uppercase(self):
        cmd = parse("/SOURCE abc123")
        assert isinstance(cmd, SourceCommand)


# ── /search — basic ──────────────────────────────────────────────────────────


class TestSearchCommandBasic:
    def test_simple_query(self):
        cmd = parse("/search tsla earnings")
        assert isinstance(cmd, SearchCommand)
        assert cmd.query == "tsla earnings"

    def test_single_word(self):
        cmd = parse("/search nvda")
        assert isinstance(cmd, SearchCommand)
        assert cmd.query == "nvda"

    def test_empty_query(self):
        cmd = parse("/search")
        assert isinstance(cmd, SearchCommand)
        assert cmd.query == ""

    def test_query_no_filters(self):
        cmd = parse("/search aapl split announcement")
        assert isinstance(cmd, SearchCommand)
        assert cmd.query == "aapl split announcement"
        assert cmd.ticker is None
        assert cmd.limit is None
        assert cmd.from_date is None
        assert cmd.to_date is None

    def test_search_uppercase_command(self):
        cmd = parse("/SEARCH tsla")
        assert isinstance(cmd, SearchCommand)
        assert cmd.query == "tsla"


# ── /search — filters ────────────────────────────────────────────────────────


class TestSearchFilters:
    def test_ticker_filter(self):
        cmd = parse("/search earnings ticker:TSLA")
        assert isinstance(cmd, SearchCommand)
        assert cmd.ticker == "TSLA"
        assert cmd.query == "earnings"

    def test_ticker_lowercase_normalised(self):
        cmd = parse("/search earnings ticker:tsla")
        assert isinstance(cmd, SearchCommand)
        assert cmd.ticker == "TSLA"

    def test_limit_filter(self):
        cmd = parse("/search earnings limit:10")
        assert isinstance(cmd, SearchCommand)
        assert cmd.limit == 10
        assert cmd.query == "earnings"

    def test_limit_one(self):
        cmd = parse("/search query limit:1")
        assert isinstance(cmd, SearchCommand)
        assert cmd.limit == 1

    def test_limit_invalid_becomes_query_token(self):
        cmd = parse("/search query limit:abc")
        assert isinstance(cmd, SearchCommand)
        assert cmd.limit is None
        assert "limit:abc" in cmd.query

    def test_from_date_filter(self):
        cmd = parse("/search query from:2026-01-01")
        assert isinstance(cmd, SearchCommand)
        assert cmd.from_date == "2026-01-01"

    def test_to_date_filter(self):
        cmd = parse("/search query to:2026-02-01")
        assert isinstance(cmd, SearchCommand)
        assert cmd.to_date == "2026-02-01"

    def test_all_filters(self):
        cmd = parse(
            "/search tsla earnings ticker:TSLA limit:10 from:2026-01-01 to:2026-02-01"
        )
        assert isinstance(cmd, SearchCommand)
        assert cmd.query == "tsla earnings"
        assert cmd.ticker == "TSLA"
        assert cmd.limit == 10
        assert cmd.from_date == "2026-01-01"
        assert cmd.to_date == "2026-02-01"

    def test_filters_before_query(self):
        cmd = parse("/search ticker:NVDA limit:5 guidance")
        assert isinstance(cmd, SearchCommand)
        assert cmd.ticker == "NVDA"
        assert cmd.limit == 5
        assert cmd.query == "guidance"

    def test_filters_interspersed(self):
        cmd = parse("/search nvda ticker:NVDA guidance limit:3")
        assert isinstance(cmd, SearchCommand)
        assert cmd.query == "nvda guidance"
        assert cmd.ticker == "NVDA"
        assert cmd.limit == 3


# ── Determinism ───────────────────────────────────────────────────────────────


class TestDeterminism:
    def test_same_input_same_output(self):
        text = "/search tsla earnings ticker:TSLA limit:5 from:2026-01-01"
        assert parse(text) == parse(text)

    def test_repeated_calls_identical(self):
        text = "/search nvda guidance"
        results = [parse(text) for _ in range(100)]
        assert all(r == results[0] for r in results)


# ── Unknown ───────────────────────────────────────────────────────────────────


class TestUnknownCommand:
    def test_empty_string(self):
        assert isinstance(parse(""), UnknownCommand)

    def test_no_slash(self):
        assert isinstance(parse("hello world"), UnknownCommand)

    def test_unknown_command(self):
        assert isinstance(parse("/foo bar"), UnknownCommand)

    def test_whitespace_only(self):
        assert isinstance(parse("   "), UnknownCommand)
