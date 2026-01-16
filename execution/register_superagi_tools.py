#!/usr/bin/env python3
"""
Register AWET Tools with SuperAGI

This script registers all AWET deterministic tools with SuperAGI's backend API,
enabling the "AWET Orchestrator" agent to call them from the SuperAGI UI.

The tools are registered as external HTTP tools that call the Tool Gateway
endpoints exposed by src/orchestration/tool_gateway.py.

Usage:
    # Register tools (requires SuperAGI to be running)
    python -m execution.register_superagi_tools
    
    # Or via Make
    make superagi-register-tools

Environment Variables:
    SUPERAGI_API_URL: SuperAGI backend URL (default: http://localhost:3001/api)
    TOOL_GATEWAY_URL: Tool Gateway URL (default: http://host.docker.internal:8200)
"""
from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Optional

import requests
import structlog

from src.core.logging import configure_logging

configure_logging("INFO")
logger = structlog.get_logger(__name__)


@dataclass
class ToolDefinition:
    """Definition of an AWET tool for SuperAGI registration."""
    name: str
    display_name: str
    description: str
    endpoint: str
    method: str
    parameters: list[dict[str, Any]]
    folder_name: str
    class_name: str
    file_name: str
    

# ============================================
# AWET Tool Definitions
# ============================================

AWET_TOOLS: list[ToolDefinition] = [
    ToolDefinition(
        name="awet_read_directive",
        display_name="AWET: Read Directive",
        description=(
            "Read a directive (SOP) document from the directives folder. "
            "Directives define goals, inputs, tools, outputs, and edge cases."
        ),
        endpoint="/tools/read_directive",
        method="POST",
        parameters=[
            {
                "name": "directive_name",
                "type": "string",
                "required": True,
                "description": "Directive file name (e.g., trading_pipeline.md)",
            }
        ],
        folder_name="awet",
        class_name="ReadDirectiveTool",
        file_name="read_directive.py",
    ),
    ToolDefinition(
        name="awet_run_backfill",
        display_name="AWET: Run Backfill",
        description=(
            "Run a data backfill to ingest historical market or sentiment data. "
            "Supports 'polygon' for market data, 'reddit' for sentiment, and 'yfinance' for free daily data."
        ),
        endpoint="/tools/run_backfill",
        method="POST",
        parameters=[
            {"name": "source", "type": "string", "required": True, "description": "Data source: 'polygon', 'reddit', or 'yfinance'"},
            {"name": "symbols", "type": "string", "required": False, "default": "AAPL,MSFT,NVDA", "description": "Comma-separated ticker symbols"},
            {"name": "start_date", "type": "string", "required": False, "description": "Start date YYYY-MM-DD (polygon/reddit)"},
            {"name": "end_date", "type": "string", "required": False, "description": "End date YYYY-MM-DD (polygon/reddit)"},
            {"name": "days", "type": "integer", "required": False, "default": 30, "description": "Days of history (yfinance)"},
            {"name": "dry_run", "type": "boolean", "required": False, "default": False, "description": "Preview without publishing"},
        ],
        folder_name="awet",
        class_name="RunBackfillTool",
        file_name="run_backfill.py",
    ),
    ToolDefinition(
        name="awet_train_model",
        display_name="AWET: Train TFT Model",
        description=(
            "Train a new TFT (Temporal Fusion Transformer) model on historical data. "
            "The model will be registered with 'yellow' status pending promotion."
        ),
        endpoint="/tools/train_model",
        method="POST",
        parameters=[
            {"name": "symbols", "type": "string", "required": False, "default": "AAPL,MSFT,NVDA", "description": "Comma-separated ticker symbols"},
            {"name": "lookback_days", "type": "integer", "required": False, "default": 30, "description": "Historical lookback window in days"},
            {"name": "epochs", "type": "integer", "required": False, "default": 100, "description": "Number of training epochs"},
        ],
        folder_name="awet",
        class_name="TrainModelTool",
        file_name="train_model.py",
    ),
    ToolDefinition(
        name="awet_promote_model",
        display_name="AWET: Promote Model to Green",
        description=(
            "Promote a trained model to 'green' (production) status. "
            "The PredictionAgent will automatically load the promoted model."
        ),
        endpoint="/tools/promote_model",
        method="POST",
        parameters=[
            {"name": "model_id", "type": "string", "required": True, "description": "Model ID to promote (e.g., tft_20260115_123456_abc123)"},
        ],
        folder_name="awet",
        class_name="PromoteModelTool",
        file_name="promote_model.py",
    ),
    ToolDefinition(
        name="awet_run_demo",
        display_name="AWET: Run Pipeline Demo",
        description=(
            "Run the end-to-end trading pipeline demo. Generates synthetic data "
            "and verifies the full message flow: market.raw → market.engineered → predictions.tft → risk → execution."
        ),
        endpoint="/tools/run_demo",
        method="POST",
        parameters=[],
        folder_name="awet",
        class_name="RunDemoTool",
        file_name="run_demo.py",
    ),
    ToolDefinition(
        name="awet_check_pipeline_health",
        display_name="AWET: Check Pipeline Health",
        description=(
            "Check the health status of all trading pipeline agents. "
            "Reports which agents are healthy and which are down."
        ),
        endpoint="/tools/check_pipeline_health",
        method="GET",
        parameters=[],
        folder_name="awet",
        class_name="CheckPipelineHealthTool",
        file_name="check_pipeline_health.py",
    ),
    ToolDefinition(
        name="awet_check_kafka_lag",
        display_name="AWET: Check Kafka Lag",
        description=(
            "Check Kafka consumer lag for all pipeline consumer groups. "
            "High lag indicates agents are falling behind in processing."
        ),
        endpoint="/tools/check_kafka_lag",
        method="GET",
        parameters=[],
        folder_name="awet",
        class_name="CheckKafkaLagTool",
        file_name="check_kafka_lag.py",
    ),
    ToolDefinition(
        name="awet_query_audit_trail",
        display_name="AWET: Query Audit Trail",
        description=(
            "Query the audit trail to see events that have flowed through the pipeline. "
            "Can filter by event_type, symbol, and time range."
        ),
        endpoint="/tools/query_audit_trail",
        method="POST",
        parameters=[
            {"name": "event_type", "type": "string", "required": False, "description": "Filter by event type (e.g., 'market.raw')"},
            {"name": "symbol", "type": "string", "required": False, "description": "Filter by symbol (e.g., 'AAPL')"},
            {"name": "correlation_id", "type": "string", "required": False, "description": "Filter by correlation ID"},
            {"name": "limit", "type": "integer", "required": False, "default": 50, "description": "Max events to return"},
            {"name": "hours_back", "type": "integer", "required": False, "default": 24, "description": "Hours to look back"},
        ],
        folder_name="awet",
        class_name="QueryAuditTrailTool",
        file_name="query_audit_trail.py",
    ),
    ToolDefinition(
        name="awet_approve_execution",
        display_name="AWET: Approve Execution",
        description=(
            "Approve trade execution by creating the approval gate file. "
            "This allows the ExecutionAgent to process paper trades."
        ),
        endpoint="/tools/approve_execution",
        method="POST",
        parameters=[],
        folder_name="awet",
        class_name="ApproveExecutionTool",
        file_name="approve_execution.py",
    ),
    ToolDefinition(
        name="awet_revoke_execution",
        display_name="AWET: Revoke Execution",
        description=(
            "Revoke trade execution by removing the approval gate file. "
            "All trades will be blocked and sent to the execution.blocked topic."
        ),
        endpoint="/tools/revoke_execution",
        method="POST",
        parameters=[],
        folder_name="awet",
        class_name="RevokeExecutionTool",
        file_name="revoke_execution.py",
    ),
    ToolDefinition(
        name="awet_run_live_ingestion",
        display_name="AWET: Run Live Ingestion",
        description=(
            "Run live daily ingestion from Yahoo Finance (FREE - no API key). "
            "Fetches latest OHLCV bars and publishes to Kafka."
        ),
        endpoint="/tools/run_live_ingestion",
        method="POST",
        parameters=[
            {"name": "symbols", "type": "string", "required": False, "description": "Comma-separated symbols (uses config default if empty)"},
            {"name": "trigger_pipeline", "type": "boolean", "required": False, "default": False, "description": "Trigger full pipeline after ingestion"},
            {"name": "dry_run", "type": "boolean", "required": False, "default": False, "description": "Preview without publishing"},
        ],
        folder_name="awet",
        class_name="RunLiveIngestionTool",
        file_name="run_live_ingestion.py",
    ),
]


# ============================================
# SuperAGI API Client
# ============================================


class SuperAGIClient:
    """Client for interacting with SuperAGI's API."""
    
    def __init__(
        self,
        api_url: str = "http://localhost:3001/api",
        tool_gateway_url: str = "http://host.docker.internal:8200",
        api_key: Optional[str] = None,
    ):
        self.api_url = api_url.rstrip("/")
        self.tool_gateway_url = tool_gateway_url.rstrip("/")
        self.session = requests.Session()
        self.timeout = 30.0
        if api_key:
            self.session.headers.update({"X-API-Key": api_key})
        self.session.headers.update({"Content-Type": "application/json"})
    
    def health_check(self) -> bool:
        """Check if SuperAGI is running."""
        try:
            response = self.session.get(f"{self.api_url}/openapi.json", timeout=self.timeout)
            return response.status_code == 200
        except Exception:
            return False
    
    def list_tools(self) -> list[dict[str, Any]]:
        """List all tools registered in SuperAGI."""
        try:
            response = self.session.get(f"{self.api_url}/tools/list", timeout=self.timeout)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.warning("list_tools_error", error=str(e))
        return []
    
    def register_tool(self, tool: ToolDefinition) -> Optional[dict[str, Any]]:
        """Register or update a single tool and return the tool record."""
        try:
            payload = {
                "name": tool.name,
                "description": tool.description,
                "folder_name": tool.folder_name,
                "class_name": tool.class_name,
                "file_name": tool.file_name,
            }
            
            existing = next((t for t in self.list_tools() if t.get("name") == tool.name), None)
            if existing and existing.get("id"):
                if self._update_tool(int(existing["id"]), payload):
                    return existing
                return None

            response = self.session.post(
                f"{self.api_url}/tools/add",
                data=json.dumps(payload),
                timeout=self.timeout,
            )

            if response.status_code in (200, 201):
                logger.info("tool_registered", tool=tool.name)
                return response.json()

            logger.error("register_tool_failed", tool=tool.name, status=response.status_code, body=response.text[:500])
            return None
                
        except Exception as e:
            logger.exception("register_tool_error", tool=tool.name, error=str(e))
            return None
    
    def _update_tool(self, tool_id: int, payload: dict[str, Any]) -> bool:
        """Update an existing tool."""
        try:
            update_response = self.session.put(
                f"{self.api_url}/tools/update/{tool_id}",
                data=json.dumps(payload),
                timeout=self.timeout,
            )

            if update_response.status_code in (200, 204):
                logger.info("tool_updated", tool_id=tool_id)
                return True
        except Exception as e:
            logger.warning("update_tool_error", tool_id=tool_id, error=str(e))
        return False
    
    def close(self):
        """Close the HTTP client."""
        self.session.close()


# ============================================
# Registration Functions
# ============================================


def check_tool_gateway(gateway_url: str) -> bool:
    """Check if the Tool Gateway is running."""
    try:
        response = requests.get(f"{gateway_url}/health", timeout=5.0)
        return response.status_code == 200
    except Exception:
        return False


def register_all_tools(
    superagi_url: str = "http://localhost:3001/api",
    tool_gateway_url: str = "http://host.docker.internal:8200",
    api_key: Optional[str] = None,
) -> tuple[int, int, dict[str, dict[str, Any]]]:
    """
    Register all AWET tools with SuperAGI.
    
    Returns (success_count, failure_count).
    """
    client = SuperAGIClient(superagi_url, tool_gateway_url, api_key=api_key)
    
    # Check SuperAGI health
    if not client.health_check():
        logger.error("superagi_not_running", url=superagi_url)
        print(f"❌ SuperAGI is not running at {superagi_url}")
        print("   Start it with: make superagi-up")
        client.close()
        return 0, len(AWET_TOOLS)
    
    logger.info("superagi_connected", url=superagi_url)
    print(f"✅ Connected to SuperAGI at {superagi_url}")
    
    # Register each tool
    success_count = 0
    failure_count = 0
    tool_records: dict[str, dict[str, Any]] = {}
    
    print(f"\n📦 Registering {len(AWET_TOOLS)} tools...\n")
    
    for tool in AWET_TOOLS:
        record = client.register_tool(tool)
        if record:
            print(f"  ✅ {tool.display_name}")
            success_count += 1
            tool_records[tool.name] = record
        else:
            print(f"  ❌ {tool.display_name}")
            failure_count += 1
    
    client.close()
    
    print(f"\n{'='*50}")
    print(f"✅ Registered: {success_count}")
    print(f"❌ Failed: {failure_count}")
    
    return success_count, failure_count, tool_records


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Register AWET tools with SuperAGI",
    )
    parser.add_argument(
        "--superagi-url",
        default=os.getenv("SUPERAGI_API_URL", "http://localhost:3001/api"),
        help="SuperAGI API URL",
    )
    parser.add_argument(
        "--tool-gateway-url",
        default=os.getenv("TOOL_GATEWAY_URL", "http://host.docker.internal:8200"),
        help="Tool Gateway URL (as seen from SuperAGI container)",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("SUPERAGI_API_KEY"),
        help="SuperAGI API key (X-API-Key)",
    )
    parser.add_argument(
        "--check-gateway",
        action="store_true",
        help="Check Tool Gateway health before registration",
    )
    parser.add_argument(
        "--list-tools",
        action="store_true",
        help="List all tools without registering",
    )
    
    args = parser.parse_args()
    
    if args.list_tools:
        print("AWET Tools:\n")
        for tool in AWET_TOOLS:
            print(f"  • {tool.display_name}")
            print(f"    Name: {tool.name}")
            print(f"    Endpoint: {tool.method} {tool.endpoint}")
            print(f"    Description: {tool.description[:80]}...")
            print()
        return
    
    print("=" * 60)
    print("  AWET SuperAGI Tool Registration")
    print("=" * 60)
    print()
    print(f"SuperAGI URL:     {args.superagi_url}")
    print(f"Tool Gateway URL: {args.tool_gateway_url}")
    print()
    
    if args.check_gateway:
        # For local testing, use localhost instead of host.docker.internal
        local_gateway = args.tool_gateway_url.replace("host.docker.internal", "localhost")
        if check_tool_gateway(local_gateway):
            print(f"✅ Tool Gateway is running at {local_gateway}")
        else:
            print(f"⚠️  Tool Gateway not reachable at {local_gateway}")
            print("   Start it with: make tool-gateway")
    
    success, failed, _ = register_all_tools(args.superagi_url, args.tool_gateway_url, api_key=args.api_key)
    
    if failed == 0:
        print("\n🎉 All tools registered successfully!")
        print("\nNext steps:")
        print("  1. Open SuperAGI UI: http://localhost:3001")
        print("  2. Go to Agents → New Agent")
        print("  3. Name: 'AWET Orchestrator'")
        print("  4. Attach the AWET toolkit")
        print("  5. See README.md for example goals")
    else:
        print(f"\n⚠️  {failed} tool(s) failed to register")
        sys.exit(1)


if __name__ == "__main__":
    main()
