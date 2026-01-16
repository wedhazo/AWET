#!/usr/bin/env python3
"""
YFinance Daily Data Backfill Script

Downloads historical daily OHLCV data from Yahoo Finance (FREE).
No API key required. Ideal for paper trading and development.

Usage:
    python -m execution.yfinance_backfill --symbols AAPL,MSFT,GOOGL --days 365
    python -m execution.yfinance_backfill --symbols AAPL --start 2024-01-01 --end 2025-01-01
    
Environment:
    MARKET_DATA_PROVIDER=yfinance  # Optional, script uses yfinance by default
"""

import argparse
import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.core.config import load_settings
from src.core.logging import configure_logging, set_correlation_id
from src.market_data.providers import YFinanceProvider, OHLCVBar
from src.models.events_market import MarketRawEvent
from src.streaming.kafka_producer import AvroProducer
from src.streaming.topics import MARKET_RAW

import structlog

logger = structlog.get_logger(__name__)


async def backfill_symbol(
    provider: YFinanceProvider,
    producer: AvroProducer | None,
    symbol: str,
    start: datetime,
    end: datetime,
    dry_run: bool = False,
) -> int:
    """Backfill data for a single symbol."""
    logger.info("backfill_start", symbol=symbol, start=start.isoformat(), end=end.isoformat())
    
    try:
        bars = await provider.get_bars(symbol, "1D", start, end)
    except Exception as e:
        logger.error("backfill_fetch_error", symbol=symbol, error=str(e))
        return 0
    
    if not bars:
        logger.warning("backfill_no_data", symbol=symbol)
        return 0
    
    count = 0
    for bar in bars:
        if dry_run:
            print(f"{bar.symbol} | {bar.timestamp.isoformat()} | O:{bar.open:.2f} H:{bar.high:.2f} L:{bar.low:.2f} C:{bar.close:.2f} V:{bar.volume:.0f}")
        elif producer:
            event = MarketRawEvent(
                event_id=str(uuid4()),
                correlation_id=str(uuid4()),
                idempotency_key=f"{bar.symbol}:{bar.timestamp.isoformat()}",
                symbol=bar.symbol,
                ts=bar.timestamp.isoformat(),
                schema_version=1,
                source="yfinance_backfill",
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
                vwap=bar.vwap,
                trades=bar.trades,
            )
            await producer.produce(MARKET_RAW, event.model_dump(), key=bar.symbol)
        count += 1
    
    logger.info("backfill_complete", symbol=symbol, bars=count)
    return count


async def main():
    parser = argparse.ArgumentParser(description="YFinance Daily Data Backfill")
    parser.add_argument(
        "--symbols",
        type=str,
        required=True,
        help="Comma-separated list of symbols (e.g., AAPL,MSFT,GOOGL)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to backfill (default: 30)",
    )
    parser.add_argument(
        "--start",
        type=str,
        help="Start date (YYYY-MM-DD). Overrides --days",
    )
    parser.add_argument(
        "--end",
        type=str,
        help="End date (YYYY-MM-DD). Default: today",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print data without publishing to Kafka",
    )
    parser.add_argument(
        "--no-kafka",
        action="store_true",
        help="Skip Kafka publishing (just fetch and validate)",
    )
    
    args = parser.parse_args()
    
    # Configure logging
    configure_logging()
    correlation_id = str(uuid4())
    set_correlation_id(correlation_id)
    
    # Parse dates
    end = datetime.now(tz=timezone.utc)
    if args.end:
        end = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    
    if args.start:
        start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        start = end - timedelta(days=args.days)
    
    # Parse symbols
    symbols = [s.strip().upper() for s in args.symbols.split(",")]
    
    print(f"=" * 60)
    print(f"YFinance Daily Data Backfill")
    print(f"=" * 60)
    print(f"Symbols: {', '.join(symbols)}")
    print(f"Date Range: {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}")
    print(f"Correlation ID: {correlation_id}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE' if not args.no_kafka else 'FETCH ONLY'}")
    print(f"=" * 60)
    
    # Create provider
    provider = YFinanceProvider()
    
    # Create Kafka producer if needed
    producer = None
    if not args.dry_run and not args.no_kafka:
        try:
            settings = load_settings()
            producer = AvroProducer(settings.kafka)
            logger.info("kafka_producer_created")
        except Exception as e:
            logger.warning("kafka_producer_failed", error=str(e))
            print(f"⚠️  Kafka unavailable: {e}")
            print("   Continuing in fetch-only mode...")
    
    # Backfill each symbol
    total_bars = 0
    for symbol in symbols:
        bars = await backfill_symbol(
            provider=provider,
            producer=producer,
            symbol=symbol,
            start=start,
            end=end,
            dry_run=args.dry_run,
        )
        total_bars += bars
    
    # Cleanup
    await provider.close()
    
    print(f"\n{'=' * 60}")
    print(f"✅ Backfill complete: {total_bars} bars across {len(symbols)} symbols")
    print(f"{'=' * 60}")
    
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
