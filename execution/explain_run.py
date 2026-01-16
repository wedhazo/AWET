#!/usr/bin/env python
"""Explain a pipeline run using the local LLM.

Usage:
    python -m execution.explain_run --id <correlation_id>
    python -m execution.explain_run --latest
"""
from __future__ import annotations

import argparse
import asyncio
import os

import asyncpg


async def get_latest_correlation_id() -> str | None:
    """Fetch the most recent correlation_id from audit_events."""
    dsn = (
        f"postgresql://{os.environ.get('POSTGRES_USER', 'awet')}:"
        f"{os.environ.get('POSTGRES_PASSWORD', 'awet')}"
        f"@{os.environ.get('POSTGRES_HOST', 'localhost')}:"
        f"{os.environ.get('POSTGRES_PORT', '5433')}"
        f"/{os.environ.get('POSTGRES_DB', 'awet')}"
    )
    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT DISTINCT correlation_id FROM audit_events ORDER BY correlation_id DESC LIMIT 1"
            )
            return row["correlation_id"] if row else None
    finally:
        await pool.close()


async def _run(correlation_id: str) -> None:
    from src.orchestration.llm_gateway import explain_run

    print(f"Explaining run: {correlation_id}")
    print("-" * 60)
    explanation = await explain_run(correlation_id)
    print(explanation)


def main() -> None:
    parser = argparse.ArgumentParser(description="Explain a pipeline run using the local LLM")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--id", dest="correlation_id", help="Correlation ID to explain")
    group.add_argument("--latest", action="store_true", help="Explain the most recent run")
    args = parser.parse_args()

    if args.latest:
        cid = asyncio.run(get_latest_correlation_id())
        if not cid:
            print("No runs found in audit_events table.")
            return
        correlation_id = cid
    else:
        correlation_id = args.correlation_id

    asyncio.run(_run(correlation_id))


if __name__ == "__main__":
    main()
