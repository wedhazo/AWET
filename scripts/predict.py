#!/usr/bin/env python3
"""
Generate predictions from the latest trained model.

Reads from: features_tft or market_raw_day (TimescaleDB)
Writes to:  predictions_tft (TimescaleDB)

Usage:
    python scripts/predict.py --symbols AAPL --start 2024-01-01 --end 2024-01-31
    python scripts/predict.py --symbols AAPL,MSFT --use-synthetic  # Generate synthetic predictions
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import asyncpg
import structlog

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.core.logging import configure_logging

logger = structlog.get_logger("predict")

REGISTRY_PATH = REPO_ROOT / "models" / "registry.json"


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _parse_date(value: str, end: bool = False) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if end:
        dt = dt.replace(hour=23, minute=59, second=59)
    return dt.astimezone(timezone.utc)


def _load_registry() -> dict:
    """Load model registry."""
    if not REGISTRY_PATH.exists():
        return {"models": [], "green": None}
    return json.loads(REGISTRY_PATH.read_text())


def _get_latest_model(registry: dict) -> dict | None:
    """Get the latest model (green if available, else newest)."""
    if registry.get("green"):
        for m in registry.get("models", []):
            if m.get("run_id") == registry["green"]:
                return m
    models = registry.get("models", [])
    if models:
        return models[-1]
    return None


async def generate_predictions(
    conn: asyncpg.Connection,
    symbols: list[str],
    start: datetime,
    end: datetime,
    model_version: str,
    use_synthetic: bool = False,
) -> int:
    """Generate predictions for given symbols and date range."""
    inserted = 0
    correlation_id = uuid4()

    for symbol in symbols:
        # Prefer engineered features if available
        rows = await conn.fetch(
            """
            SELECT ts, price
            FROM features_tft
            WHERE ticker = $1 AND ts >= $2 AND ts <= $3
            ORDER BY ts
            """,
            symbol,
            start,
            end,
        )

        if not rows:
            rows = await conn.fetch(
                """
                SELECT ts, close as price
                FROM market_raw_day
                WHERE ticker = $1 AND ts >= $2 AND ts <= $3
                ORDER BY ts
                """,
                symbol,
                start,
                end,
            )

        if not rows:
            logger.warning("no_market_data", symbol=symbol)
            continue

        for row in rows:
            ts = row["ts"]
            close = float(row["price"])

            if use_synthetic:
                # Generate synthetic predictions with some signal
                base_return = random.gauss(0.001, 0.02)  # Slight positive bias
                q10 = base_return - 0.015
                q50 = base_return
                q90 = base_return + 0.015
                confidence = random.uniform(0.5, 0.9)
                direction = "long" if q50 > 0.003 else ("short" if q50 < -0.003 else "neutral")
            else:
                # Placeholder for real model inference
                # In production, this would load the ONNX model and run inference
                q50 = 0.0
                q10 = -0.01
                q90 = 0.01
                confidence = 0.5
                direction = "neutral"

            event_id = uuid4()
            idempotency_key = f"{symbol}:{ts.isoformat()}:{model_version}"

            await conn.execute(
                """
                INSERT INTO predictions_tft (
                    event_id, correlation_id, idempotency_key, ticker, ts,
                    horizon_minutes, direction, confidence, q10, q50, q90, model_version
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (ts, idempotency_key) DO NOTHING
                """,
                event_id,
                correlation_id,
                idempotency_key,
                symbol,
                ts,
                1440,  # Daily horizon
                direction,
                confidence,
                q10,
                q50,
                q90,
                model_version,
            )
            inserted += 1

    return inserted


async def main() -> int:
    parser = argparse.ArgumentParser(description="Generate predictions from trained model")
    parser.add_argument("--symbols", required=True, help="Comma-separated symbols")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--use-synthetic",
        action="store_true",
        help="Generate synthetic predictions (for testing)",
    )
    parser.add_argument(
        "--model-version",
        default=None,
        help="Model version to use (default: latest from registry)",
    )
    args = parser.parse_args()

    configure_logging()

    symbols = [s.strip().upper() for s in args.symbols.split(",")]
    start = _parse_date(args.start)
    end = _parse_date(args.end, end=True)

    # Get model version
    if args.model_version:
        model_version = args.model_version
    else:
        registry = _load_registry()
        model = _get_latest_model(registry)
        if model:
            model_version = model.get("run_id", "unknown")
        else:
            model_version = "synthetic" if args.use_synthetic else "unknown"

    dsn = (
        f"postgresql://{_env('POSTGRES_USER', 'awet')}:{_env('POSTGRES_PASSWORD', 'awet')}"
        f"@{_env('POSTGRES_HOST', 'localhost')}:{_env('POSTGRES_PORT', '5433')}"
        f"/{_env('POSTGRES_DB', 'awet')}"
    )

    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=3)

    try:
        async with pool.acquire() as conn:
            inserted = await generate_predictions(
                conn,
                symbols,
                start,
                end,
                model_version,
                use_synthetic=args.use_synthetic,
            )

            logger.info(
                "predictions_generated",
                symbols=symbols,
                start=str(start),
                end=str(end),
                model_version=model_version,
                rows=inserted,
            )
            print(f"Inserted {inserted} prediction rows (model: {model_version})")

    finally:
        await pool.close()

    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        print("\n👋 Cancelled")
        sys.exit(1)
