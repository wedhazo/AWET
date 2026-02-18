"""Deterministic reply formatters for the AWET Telegram KB gateway.

Hard rules:
- Pure functions only (no I/O).
- Plain text replies (no Markdown/HTML parse modes).
- Deterministic ordering: same input list => same output string.
"""
from __future__ import annotations

from datetime import datetime, timezone


HELP_TEXT = (
    "AWET KB Search Bot\n\n"
    "Commands:\n"
    "/search <query>\n"
    "  Search the Reddit knowledge base.\n\n"
    "Optional filters (append to query):\n"
    "  ticker:TSLA\n"
    "  limit:10\n"
    "  from:2026-01-01\n"
    "  to:2026-02-01\n\n"
    "Examples:\n"
    "  /search tsla earnings\n"
    "  /search nvda guidance ticker:NVDA limit:10\n"
    "  /search aapl split from:2025-01-01 to:2026-01-01 ticker:AAPL\n\n"
    "/source <source_id>\n"
    "  Fetch full detail for a specific KB source.\n\n"
    "/help\n"
    "  Show this message."
)


def format_help() -> str:
    return HELP_TEXT


def format_search_results(results: list[dict]) -> str:
    if not results:
        return "No results."

    lines: list[str] = [f"Results ({len(results)}):"]
    for r in results:
        subreddit = str(r.get("subreddit") or "—")
        title = _one_line(str(r.get("title") or "—"))
        created = _fmt_date(r.get("created_at_utc") or r.get("created_utc"))
        source_id = str(r.get("source_id") or r.get("id") or r.get("sourceId") or "")
        url = str(r.get("url") or "")
        chunk_id = str(r.get("chunk_id") or r.get("chunkId") or r.get("chunk") or "")

        lines.append(f"- [{subreddit}] {title} ({created}) — source_id={source_id}")
        lines.append(f"  url: {url}")
        lines.append(f"  chunk_id: {chunk_id}")

    return "\n".join(lines)


def format_source(source: dict) -> str:
    title = _one_line(str(source.get("title") or "—"))
    subreddit = str(source.get("subreddit") or "—")
    created_at_utc = str(
        source.get("created_at_utc")
        or source.get("createdAtUtc")
        or _fmt_datetime(source.get("created_utc"))
        or "—"
    )
    url = str(source.get("url") or "")
    body = str(source.get("body") or "")

    body_out = _truncate(body, 1200)

    return (
        f"title: {title}\n"
        f"subreddit: {subreddit}\n"
        f"created_at_utc: {created_at_utc}\n"
        f"url: {url}\n\n"
        f"body:\n{body_out}"
    )


def _one_line(text: str) -> str:
    return " ".join((text or "").split())


def _truncate(text: str, max_len: int) -> str:
    if text is None:
        return ""
    if len(text) <= max_len:
        return text
    return text[:max_len] + "..."


def _fmt_date(value: object) -> str:
    """Return YYYY-MM-DD from either epoch seconds or an ISO-like string."""
    if value is None:
        return "—"

    # epoch seconds
    try:
        ts = float(value)  # type: ignore[arg-type]
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        pass

    s = str(value)
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return s


def _fmt_datetime(value: object) -> str | None:
    if value is None:
        return None
    try:
        ts = float(value)  # type: ignore[arg-type]
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None
