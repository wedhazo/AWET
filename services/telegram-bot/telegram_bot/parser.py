"""Deterministic command parser for Telegram bot messages.

Parsing rules:
- Command is the first word beginning with '/'.
- Optional filters follow the main query text and are of the form
  ``key:value`` (case-insensitive keys).
- Supported filters: ticker, limit, from, to.
- All parsing is pure / side-effect free, making it trivially testable.

Design note: we deliberately avoid regexes on the full text and instead
use a token-by-token approach so that edge cases (colons inside query
text, empty queries) are handled predictably.
"""
from __future__ import annotations

import shlex
from dataclasses import dataclass, field
from typing import Literal


CommandName = Literal["/search", "/source", "/help", "unknown"]

# Keys recognised as filter tokens (exact set — no abbreviations)
_FILTER_KEYS = frozenset({"ticker", "limit", "from", "to"})


@dataclass(frozen=True)
class SearchCommand:
    command: Literal["/search"] = "/search"
    query: str = ""
    ticker: str | None = None
    limit: int | None = None
    from_date: str | None = None      # YYYY-MM-DD
    to_date: str | None = None        # YYYY-MM-DD


@dataclass(frozen=True)
class SourceCommand:
    command: Literal["/source"] = "/source"
    source_id: str = ""


@dataclass(frozen=True)
class HelpCommand:
    command: Literal["/help"] = "/help"


@dataclass(frozen=True)
class UnknownCommand:
    command: str = "unknown"
    raw: str = ""


ParsedCommand = SearchCommand | SourceCommand | HelpCommand | UnknownCommand


def parse(text: str) -> ParsedCommand:
    """Parse a Telegram message text into a structured command object.

    Always returns a value (never raises). Unknown / malformed inputs
    return ``UnknownCommand``.

    The function is pure and deterministic: identical input → identical
    output for any Python ≥ 3.10 with no side effects.
    """
    text = (text or "").strip()
    if not text:
        return UnknownCommand(raw=text)

    # Normalise bot username suffix: /search@MyBot → /search
    tokens = text.split()
    cmd_token = tokens[0].lower()
    if "@" in cmd_token:
        cmd_token = cmd_token.split("@")[0]

    rest_tokens = tokens[1:]

    if cmd_token == "/help":
        return HelpCommand()

    if cmd_token == "/source":
        source_id = rest_tokens[0].strip() if rest_tokens else ""
        return SourceCommand(source_id=source_id)

    if cmd_token == "/search":
        return _parse_search(rest_tokens)

    return UnknownCommand(command=cmd_token, raw=text)


def _parse_search(tokens: list[str]) -> SearchCommand:
    """Extract query text and optional filters from token list.

    Filter tokens look like ``key:value`` where key is in _FILTER_KEYS.
    Everything else is part of the free-text query.
    """
    query_parts: list[str] = []
    ticker: str | None = None
    limit: int | None = None
    from_date: str | None = None
    to_date: str | None = None

    for token in tokens:
        lower = token.lower()
        # Check if this token looks like a filter (key:value)
        if ":" in token:
            key, _, value = token.partition(":")
            if key.lower() in _FILTER_KEYS and value:
                k = key.lower()
                if k == "ticker":
                    ticker = value.upper()
                elif k == "limit":
                    try:
                        limit = max(1, int(value))
                    except ValueError:
                        query_parts.append(token)
                elif k == "from":
                    from_date = value
                elif k == "to":
                    to_date = value
                continue
        query_parts.append(token)

    query = " ".join(query_parts).strip()
    return SearchCommand(
        query=query,
        ticker=ticker,
        limit=limit,
        from_date=from_date,
        to_date=to_date,
    )
