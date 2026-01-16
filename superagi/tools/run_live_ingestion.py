"""
Run Live Ingestion Tool - SuperAGI tool for on-demand daily data ingestion

This tool fetches the latest OHLCV bars from Yahoo Finance (FREE) and 
publishes them to Kafka, optionally triggering the full trading pipeline.

Use cases:
- Daily trading workflow: run after market close to get fresh data
- On-demand data refresh before running predictions
- Testing pipeline end-to-end with real market data
"""

from typing import Type, Optional
import time
from pydantic import BaseModel, Field
from superagi.tools.base_tool import BaseTool
import subprocess
import os
import json


class RunLiveIngestionInput(BaseModel):
    symbols: str = Field(
        default="",
        description="Comma-separated list of stock symbols (e.g., 'AAPL,MSFT,NVDA'). "
                    "Leave empty to use default symbols from config."
    )
    trigger_pipeline: bool = Field(
        default=False,
        description="If True, triggers the full pipeline after ingestion: "
                    "feature engineering → prediction → risk → paper execution"
    )
    dry_run: bool = Field(
        default=False,
        description="If True, only prints what would be done without publishing to Kafka"
    )


class RunLiveIngestionTool(BaseTool):
    """
    Tool to run live daily data ingestion from Yahoo Finance.
    
    This tool:
    1. Fetches the latest daily OHLCV bars for specified symbols
    2. Publishes them to Kafka (market.raw topic)
    3. Optionally triggers the full trading pipeline
    
    The data source is Yahoo Finance which is FREE and requires no API key.
    Data is typically delayed 15-20 minutes but sufficient for daily trading.
    
    Example usage:
    - Fetch latest data for AAPL and MSFT: {"symbols": "AAPL,MSFT"}
    - Run full pipeline: {"symbols": "AAPL", "trigger_pipeline": true}
    - Test without side effects: {"dry_run": true}
    """
    
    name: str = "awet_run_live_ingestion"
    description: str = (
        "Fetch the latest daily OHLCV data from Yahoo Finance (FREE) and publish to Kafka. "
        "Optionally triggers the full trading pipeline (features → prediction → risk → execution). "
        "Use this for daily trading workflows or on-demand data refresh."
    )
    args_schema: Type[BaseModel] = RunLiveIngestionInput
    
    def _execute(
        self, 
        symbols: str = "",
        trigger_pipeline: bool = False,
        dry_run: bool = False,
    ) -> str:
        """Execute the live ingestion."""
        start_ts = time.perf_counter()
        
        script_path = "/app/awet_tools/execution/run_live_ingestion.py"
        
        # Fallback for local development
        if not os.path.exists(script_path):
            script_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                "execution",
                "run_live_ingestion.py"
            )
        
        if not os.path.exists(script_path):
            return json.dumps({
                "success": False,
                "error": f"Live ingestion script not found. Tried: {script_path}"
            })
        
        # Build command
        cmd = ["python", script_path, "--json"]
        
        if symbols:
            cmd.extend(["--symbols", symbols])
        
        if trigger_pipeline:
            cmd.append("--trigger-pipeline")
        
        if dry_run:
            cmd.append("--dry-run")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
                env={**os.environ, "PYTHONPATH": "/app/awet_tools"},
            )
            
            # Parse JSON output
            if result.returncode == 0:
                try:
                    output = json.loads(result.stdout)
                    return json.dumps({
                        "success": True,
                        "result": output,
                        "message": f"✅ Ingested {output.get('bars_ingested', 0)} bars for {len(output.get('symbols_processed', []))} symbols"
                    }, indent=2)
                except json.JSONDecodeError:
                    return json.dumps({
                        "success": True,
                        "message": "Ingestion completed",
                        "stdout": result.stdout[-1000:] if result.stdout else ""
                    })
            else:
                return json.dumps({
                    "success": False,
                    "error": result.stderr[-500:] if result.stderr else "Unknown error",
                    "stdout": result.stdout[-500:] if result.stdout else ""
                })
                
        except subprocess.TimeoutExpired:
            return json.dumps({
                "success": False,
                "error": "Live ingestion timed out after 5 minutes"
            })
        except Exception as e:
            return json.dumps({
                "success": False,
                "error": str(e)
            })
        finally:
            duration = time.perf_counter() - start_ts
            print(f"[awet_tool_timing] tool=awet_run_live_ingestion duration_s={duration:.3f}")


# For direct testing
if __name__ == "__main__":
    tool = RunLiveIngestionTool()
    print("Testing dry run...")
    result = tool._execute(symbols="AAPL,MSFT", dry_run=True)
    print(result)
