#!/usr/bin/env python3
"""Verify TimescaleDB schema and print table stats."""

from __future__ import annotations

import asyncio
import os
import sys


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


REQUIRED_TABLES = [
    "market_raw_minute",
    "market_raw_day",
    "features_tft",
    "predictions_tft",
    "paper_trades",
    "audit_events",
]


async def verify_db() -> bool:
    import asyncpg

    dsn = (
        f"postgresql://{_env('POSTGRES_USER', 'awet')}:{_env('POSTGRES_PASSWORD', 'awet')}"
        f"@{_env('POSTGRES_HOST', 'localhost')}:{_env('POSTGRES_PORT', '5433')}"
        f"/{_env('POSTGRES_DB', 'awet')}"
    )

    print(f"Connecting to: {dsn.replace(_env('POSTGRES_PASSWORD', 'awet'), '***')}")

    try:
        conn = await asyncpg.connect(dsn)
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

    print("✅ Connected to TimescaleDB\n")

    # Check tables exist
    all_ok = True
    print("=" * 60)
    print(f"{'TABLE':<25} {'EXISTS':<10} {'ROW COUNT':<15}")
    print("=" * 60)

    for table in REQUIRED_TABLES:
        exists = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = $1
            )
            """,
            table,
        )

        if exists:
            count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
            print(f"{table:<25} {'✅ YES':<10} {count:<15}")
        else:
            print(f"{table:<25} {'❌ NO':<10} {'-':<15}")
            all_ok = False

    print("=" * 60)

    # Check hypertables
    print("\n📊 Hypertable Info:")
    hypertables = await conn.fetch(
        """
        SELECT hypertable_name, num_chunks
        FROM timescaledb_information.hypertables
        WHERE hypertable_schema = 'public'
        """
    )
    for ht in hypertables:
        print(f"  - {ht['hypertable_name']}: {ht['num_chunks']} chunks")

    # Check indexes
    print("\n🔍 Indexes:")
    indexes = await conn.fetch(
        """
        SELECT tablename, indexname 
        FROM pg_indexes 
        WHERE schemaname = 'public' 
        ORDER BY tablename, indexname
        """
    )
    current_table = None
    for idx in indexes:
        if idx["tablename"] != current_table:
            current_table = idx["tablename"]
            print(f"\n  {current_table}:")
        print(f"    - {idx['indexname']}")

    await conn.close()

    print("\n" + "=" * 60)
    if all_ok:
        print("✅ All required tables exist!")
    else:
        print("❌ Some tables are missing. Run: docker exec -i timescaledb psql -U awet < db/init.sql")
    print("=" * 60)

    return all_ok


def main() -> int:
    try:
        import asyncpg  # noqa: F401
    except ImportError:
        print("❌ asyncpg not installed. Run: pip install asyncpg")
        return 1

    success = asyncio.run(verify_db())
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
