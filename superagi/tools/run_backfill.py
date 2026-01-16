"""Run Backfill Tool - Triggers deterministic backfill scripts"""

from typing import Type
import time
from pydantic import BaseModel, Field
from superagi.tools.base_tool import BaseTool
import subprocess
import os


class RunBackfillInput(BaseModel):
    source: str = Field(
        ...,
        description="Data source to backfill: 'polygon' or 'reddit'"
    )
    tickers: str = Field(
        default="AAPL,MSFT,NVDA",
        description="Comma-separated list of tickers to backfill"
    )
    start_date: str = Field(
        default="",
        description="Start date in YYYY-MM-DD format (optional)"
    )
    end_date: str = Field(
        default="",
        description="End date in YYYY-MM-DD format (optional)"
    )


class RunBackfillTool(BaseTool):
    """
    Tool to trigger backfill scripts that ingest historical data.
    
    This tool calls deterministic Python scripts in /execution/ that:
    - Fetch data from external APIs (Polygon, Reddit)
    - Validate against Avro schemas
    - Publish to Kafka topics
    - Track checkpoints for resumable backfills
    """
    
    name: str = "awet_run_backfill"
    description: str = (
        "Run a backfill to ingest historical market data or sentiment data. "
        "Supports 'polygon' for market data and 'reddit' for sentiment."
    )
    args_schema: Type[BaseModel] = RunBackfillInput
    
    def _execute(
        self, 
        source: str,
        tickers: str = "AAPL,MSFT,NVDA",
        start_date: str = "",
        end_date: str = ""
    ) -> str:
        start_ts = time.perf_counter()
        # Validate source
        valid_sources = ["polygon", "reddit"]
        if source.lower() not in valid_sources:
            return f"Error: Invalid source '{source}'. Must be one of: {valid_sources}"
        
        script_map = {
            "polygon": "/app/awet_tools/execution/backfill_polygon.py",
            "reddit": "/app/awet_tools/execution/backfill_reddit.py",
        }
        
        script_path = script_map[source.lower()]
        
        if not os.path.exists(script_path):
            return f"Error: Backfill script not found at {script_path}"
        
        # Build command
        cmd = ["python", script_path, "--tickers", tickers]
        
        if start_date:
            cmd.extend(["--start", start_date])
        if end_date:
            cmd.extend(["--end", end_date])
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout
                env={**os.environ}
            )
            
            output = f"Exit code: {result.returncode}\n"
            if result.stdout:
                output += f"\nSTDOUT:\n{result.stdout[-2000:]}"
            if result.stderr:
                output += f"\nSTDERR:\n{result.stderr[-1000:]}"
            
            return output
            
        except subprocess.TimeoutExpired:
            return "Error: Backfill timed out after 1 hour"
        except Exception as e:
            return f"Error running backfill: {e}"
        finally:
            duration = time.perf_counter() - start_ts
            print(f"[awet_tool_timing] tool=awet_run_backfill duration_s={duration:.3f}")
