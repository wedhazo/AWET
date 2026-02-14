from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import asyncpg

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.serializing_producer import SerializingProducer

from src.core.config import load_settings

APPROVAL_FILE = Path(".tmp/APPROVE_EXECUTION")
RAW_SCHEMA_PATH = "src/schemas/market_raw.avsc"


def is_execution_approved() -> tuple[bool, str]:
    """
    Check if execution is approved based on BOTH dry_run flag AND approval file.
    
    Returns:
        tuple: (is_approved, reason_message)
    """
    try:
        settings = load_settings()
        dry_run = settings.app.execution_dry_run
    except Exception:
        dry_run = True  # Default to safe mode if config fails
    
    approval_file_exists = APPROVAL_FILE.exists()
    
    if dry_run:
        return False, "execution_dry_run=True in config (safety override)"
    elif not approval_file_exists:
        return False, "approval file missing (run 'make approve')"
    else:
        return True, "dry_run=False AND approval file exists"

# Synthetic market data for demo (deterministic, idempotent)
SYNTHETIC_TICKERS = [
    {"symbol": "AAPL", "base_price": 185.50, "volume": 1000000},
    {"symbol": "MSFT", "base_price": 420.75, "volume": 800000},
    {"symbol": "NVDA", "base_price": 875.25, "volume": 1500000},
]

# Expected topics in the pipeline flow
PIPELINE_TOPICS = [
    "market.raw",
    "market.engineered",
    "predictions.tft",
    "risk.approved",
]

# Execution output depends on approval status
EXECUTION_TOPIC_APPROVED = "execution.completed"
EXECUTION_TOPIC_BLOCKED = "execution.blocked"


def get_db_dsn() -> str:
    return (
        f"postgresql://{os.getenv('POSTGRES_USER', 'awet')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'awet')}"
        f"@{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5433')}/"
        f"{os.getenv('POSTGRES_DB', 'awet')}"
    )


def _parse_ts(ts_value: str | datetime) -> datetime:
    if isinstance(ts_value, datetime):
        return ts_value
    return datetime.fromisoformat(ts_value.replace("Z", "+00:00"))


async def clear_demo_data() -> None:
    """Clear previous demo data to ensure clean state."""
    dsn = get_db_dsn()
    try:
        conn = await asyncpg.connect(dsn)
        # Clear demo-generated data by idempotency key prefix
        await conn.execute(
            "DELETE FROM audit_events WHERE idempotency_key LIKE 'demo_%'"
        )
        await conn.close()
        print("🧹 Cleared previous demo data")
    except Exception as e:
        print(f"⚠️ Could not clear demo data: {e}")


async def generate_synthetic_events(correlation_id: str) -> int:
    """Generate synthetic MarketRawEvent messages to Kafka.
    
    This is idempotent: uses deterministic idempotency_keys based on
    symbol + timestamp, so re-running produces the same keys.
    """
    print("📤 Generating synthetic market events...")
    
    # Load schema
    with open(RAW_SCHEMA_PATH, "r", encoding="utf-8") as f:
        raw_schema = f.read()
    
    # Get Kafka config from environment or defaults
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    
    schema_registry = SchemaRegistryClient({"url": schema_registry_url})
    value_serializer = AvroSerializer(schema_registry, raw_schema)
    
    producer = SerializingProducer({
        "bootstrap.servers": bootstrap_servers,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": value_serializer,
    })
    
    base_ts = datetime(2026, 1, 15, 9, 30, 0, tzinfo=timezone.utc)
    events_sent = 0
    dsn = get_db_dsn()
    conn = await asyncpg.connect(dsn)
    
    # Generate 5 events per ticker (enough to trigger pipeline)
    for ticker_info in SYNTHETIC_TICKERS:
        symbol = ticker_info["symbol"]
        base_price = ticker_info["base_price"]
        base_volume = ticker_info["volume"]
        
        for i in range(5):
            # Deterministic timestamp and idempotency key
            ts = base_ts.replace(second=i * 10)
            idempotency_key = f"demo_{symbol}_{ts.isoformat()}"
            
            # Slight price variation
            price_mult = 1.0 + (i - 2) * 0.001  # -0.2% to +0.2%
            price = base_price * price_mult
            
            event = {
                "event_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, idempotency_key)),
                "correlation_id": correlation_id,
                "idempotency_key": idempotency_key,
                "symbol": symbol,
                "ts": ts.isoformat(),
                "schema_version": 1,
                "source": "synthetic_demo",
                "open": price * 0.999,
                "high": price * 1.002,
                "low": price * 0.998,
                "close": price,
                "price": price,
                "volume": float(base_volume + i * 10000),
                "vwap": price * 1.0001,
                "trades": 1000 + i * 100,
            }
            
            producer.produce(
                topic="market.raw",
                key=symbol,
                value=event,
            )
            await conn.execute(
                """
                INSERT INTO audit_events (
                    event_id, correlation_id, idempotency_key, symbol, ts,
                    schema_version, source, event_type, payload
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (ts, idempotency_key) DO NOTHING
                """,
                event["event_id"],
                event["correlation_id"],
                event["idempotency_key"],
                event["symbol"],
                _parse_ts(event["ts"]),
                event["schema_version"],
                event["source"],
                "market.raw",
                json.dumps(event, default=str),
            )
            events_sent += 1
    
    await conn.close()

    # Flush to ensure delivery
    remaining = producer.flush(30.0)
    if remaining > 0:
        print(f"⚠️ {remaining} messages not delivered")
    
    print(f"✅ Sent {events_sent} synthetic events to market.raw")
    return events_sent


async def wait_for_pipeline_completion(
    expected_event_types: list[str],
    since_time: datetime,
    min_events_per_type: int = 1,
    timeout_seconds: int = 45,
) -> dict[str, int]:
    """Wait for audit_events to contain expected event types.
    
    Only counts events created AFTER since_time, ensuring we see fresh data.
    Returns dict of event_type -> count.
    """
    dsn = get_db_dsn()
    start_time = datetime.now(tz=timezone.utc)
    
    while True:
        elapsed = (datetime.now(tz=timezone.utc) - start_time).total_seconds()
        if elapsed > timeout_seconds:
            break
        
        try:
            conn = await asyncpg.connect(dsn)
            rows = await conn.fetch(
                """
                SELECT event_type, COUNT(*) as cnt 
                FROM audit_events 
                WHERE created_at > $1
                GROUP BY event_type
                """,
                since_time
            )
            await conn.close()
            
            counts = {row["event_type"]: row["cnt"] for row in rows}
            
            # Check if we have all expected types
            all_present = all(
                counts.get(et, 0) >= min_events_per_type 
                for et in expected_event_types
            )
            
            if all_present:
                return counts
            
            # Print progress
            found = [et for et in expected_event_types if counts.get(et, 0) >= min_events_per_type]
            missing = [et for et in expected_event_types if counts.get(et, 0) < min_events_per_type]
            print(f"  ⏳ Found: {found}, Missing: {missing}")
                
        except Exception as e:
            print(f"⏳ Waiting for DB... ({e})")
        
        await asyncio.sleep(3)
    
    # Return whatever we have
    try:
        conn = await asyncpg.connect(dsn)
        rows = await conn.fetch(
            """
            SELECT event_type, COUNT(*) as cnt 
            FROM audit_events 
            WHERE created_at > $1
            GROUP BY event_type
            """,
            since_time
        )
        await conn.close()
        return {row["event_type"]: row["cnt"] for row in rows}
    except Exception:
        return {}


async def verify_pipeline_flow(approved: bool, since_time: datetime) -> bool:
    """Verify the complete pipeline flow based on approval status."""
    print("\n" + "=" * 60)
    print("🔍 VERIFYING PIPELINE FLOW")
    print("=" * 60)
    
    # Determine expected final topic
    if approved:
        expected_types = PIPELINE_TOPICS + [EXECUTION_TOPIC_APPROVED]
        print(f"📋 Approval status: APPROVED (expecting {EXECUTION_TOPIC_APPROVED})")
    else:
        expected_types = PIPELINE_TOPICS + [EXECUTION_TOPIC_BLOCKED]
        print(f"📋 Approval status: BLOCKED (expecting {EXECUTION_TOPIC_BLOCKED})")
    
    print(f"📋 Expected event types: {expected_types}")
    print("-" * 60)
    
    # Wait for events (only count events created after since_time)
    counts = await wait_for_pipeline_completion(expected_types, since_time, min_events_per_type=1)
    
    # Report results
    print("\n📊 AUDIT EVENT COUNTS:")
    all_passed = True
    for et in expected_types:
        count = counts.get(et, 0)
        status = "✅" if count >= 1 else "❌"
        print(f"  {status} {et}: {count}")
        if count < 1:
            all_passed = False
    
    # Show any unexpected event types
    unexpected = set(counts.keys()) - set(expected_types)
    if unexpected:
        print(f"\n📌 Other event types found: {unexpected}")
    
    print("-" * 60)
    if all_passed:
        print("✅ PIPELINE VERIFICATION PASSED")
    else:
        print("❌ PIPELINE VERIFICATION FAILED")
    print("=" * 60 + "\n")
    
    return all_passed


async def main() -> None:
    print("\n" + "=" * 60)
    print("🚀 AWET TRADING PIPELINE DEMO")
    print("=" * 60)
    
    # Check approval status (both dry_run AND approval file)
    approved, reason = is_execution_approved()
    if approved:
        print(f"🟢 Execution APPROVED - trades will be FILLED")
        print(f"   Reason: {reason}")
    else:
        print(f"🔴 Execution BLOCKED - trades will NOT be filled")
        print(f"   Reason: {reason}")
    
    # Generate correlation ID for this demo run
    correlation_id = str(uuid.uuid4())
    print(f"🔗 Correlation ID: {correlation_id}")
    
    # Clear previous demo data
    await clear_demo_data()
    
    # Start all agents
    print("\n🔧 Starting pipeline agents...")
    processes = []
    modules = [
        "src.agents.data_ingestion",
        "src.agents.feature_engineering",
        "src.agents.time_series_prediction",
        "src.agents.risk_agent",
        "src.agents.execution_agent",
        "src.agents.watchtower_agent",
    ]
    
    for module in modules:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            module,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        processes.append(process)
        print(f"  ▶ Started {module.split('.')[-1]}")
    
    # Wait for agents to initialize
    print("⏳ Waiting for agents to initialize...")
    await asyncio.sleep(5)
    
    # Record the time BEFORE generating events so we only count new ones
    since_time = datetime.now(tz=timezone.utc)
    
    # Generate synthetic events
    events_sent = await generate_synthetic_events(correlation_id)
    if events_sent == 0:
        raise RuntimeError("Failed to generate synthetic events")
    
    # Verify pipeline flow (only count events created after since_time)
    success = await verify_pipeline_flow(approved, since_time)
    
    # Shutdown agents
    print("🛑 Shutting down agents...")
    for process in processes:
        try:
            process.send_signal(signal.SIGTERM)
        except ProcessLookupError:
            pass
    
    await asyncio.sleep(2)
    
    # Force kill any remaining
    for process in processes:
        try:
            process.kill()
        except ProcessLookupError:
            pass
    
    if not success:
        print("\n❌ Demo failed - not all pipeline stages completed")
        sys.exit(1)
    else:
        print("\n✅ Demo completed successfully!")
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
