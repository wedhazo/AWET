"""Query Audit Trail Tool - Queries TimescaleDB audit_events"""

from typing import Type
from pydantic import BaseModel, Field
from superagi.tools.base_tool import BaseTool
import asyncio
import asyncpg
import os


class QueryAuditTrailInput(BaseModel):
    event_type: str = Field(
        default="",
        description="Filter by event type (e.g., 'market.raw', 'predictions.tft', 'risk.approved')"
    )
    symbol: str = Field(
        default="",
        description="Filter by symbol (e.g., 'AAPL')"
    )
    limit: int = Field(
        default=20,
        description="Maximum number of events to return"
    )
    hours_back: int = Field(
        default=24,
        description="How many hours back to query"
    )


class QueryAuditTrailTool(BaseTool):
    """
    Tool to query the audit trail in TimescaleDB.
    
    Returns recent events from the audit_events table, which tracks
    all events flowing through the trading pipeline.
    """
    
    name: str = "awet_query_audit_trail"
    description: str = (
        "Query the audit trail to see events that have flowed through the pipeline. "
        "Can filter by event_type, symbol, and time range."
    )
    args_schema: Type[BaseModel] = QueryAuditTrailInput
    
    def _execute(
        self,
        event_type: str = "",
        symbol: str = "",
        limit: int = 20,
        hours_back: int = 24
    ) -> str:
        import time
        start_ts = time.perf_counter()
        async def query_db():
            dsn = (
                f"postgresql://{os.getenv('AWET_TIMESCALE_USER', 'awet')}:"
                f"{os.getenv('AWET_TIMESCALE_PASSWORD', 'awet')}@"
                f"{os.getenv('AWET_TIMESCALE_HOST', 'timescaledb')}:"
                f"{os.getenv('AWET_TIMESCALE_PORT', '5432')}/"
                f"{os.getenv('AWET_TIMESCALE_DB', 'awet')}"
            )
            
            conn = await asyncpg.connect(dsn)
            
            # Build query
            query = """
                SELECT event_id, event_type, symbol, source, ts, created_at
                FROM audit_events
                WHERE created_at > NOW() - INTERVAL '%s hours'
            """ % hours_back
            
            params = []
            if event_type:
                query += " AND event_type = $1"
                params.append(event_type)
            if symbol:
                param_num = len(params) + 1
                query += f" AND symbol = ${param_num}"
                params.append(symbol)
            
            query += f" ORDER BY created_at DESC LIMIT {limit}"
            
            rows = await conn.fetch(query, *params)
            await conn.close()
            
            return rows
        
        try:
            rows = asyncio.run(query_db())
            
            if not rows:
                return "No events found matching the criteria"
            
            output = f"# Audit Trail ({len(rows)} events)\n\n"
            output += "| Time | Event Type | Symbol | Source |\n"
            output += "|------|------------|--------|--------|\n"
            
            for row in rows:
                ts = row['created_at'].strftime('%Y-%m-%d %H:%M:%S')
                output += f"| {ts} | {row['event_type']} | {row['symbol']} | {row['source']} |\n"
            
            return output
            
        except Exception as e:
            return f"Error querying audit trail: {e}"
        finally:
            duration = time.perf_counter() - start_ts
            print(f"[awet_tool_timing] tool=awet_query_audit_trail duration_s={duration:.3f}")
