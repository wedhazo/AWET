"""Explain Run Tool - Uses local LLM to explain a pipeline run from audit trail"""

import asyncio
import os
import sys
from typing import Type

from pydantic import BaseModel, Field

# Add AWET src to path for imports (mounted at /app/awet_tools)
AWET_PATH = os.environ.get("AWET_TOOLS_PATH", "/app/awet_tools")
if AWET_PATH not in sys.path:
    sys.path.insert(0, AWET_PATH)

try:
    from superagi.tools.base_tool import BaseTool
except ImportError:
    class BaseTool:
        name: str = ""
        description: str = ""
        args_schema: type = None
        def _execute(self, **kwargs) -> str:
            raise NotImplementedError


class ExplainRunInput(BaseModel):
    correlation_id: str = Field(
        ...,
        description="The correlation_id of the pipeline run to explain (UUID)"
    )


class ExplainRunTool(BaseTool):
    """
    Tool that uses the local Llama LLM to explain what happened in a pipeline run.
    
    This calls src/orchestration/llm_gateway.explain_run() which:
    - Fetches audit events from TimescaleDB for the given correlation_id
    - Sends them to the local LLM
    - Returns a human-readable explanation
    """
    
    name: str = "awet_explain_run"
    description: str = (
        "Explain a pipeline run in plain English using the local LLM. "
        "Provide a correlation_id and this tool will fetch the audit trail "
        "and ask the LLM to summarize what happened step by step."
    )
    args_schema: Type[BaseModel] = ExplainRunInput
    
    def _execute(self, correlation_id: str) -> str:
        try:
            from src.orchestration.llm_gateway import explain_run
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(explain_run(correlation_id))
                return f"# Pipeline Run Explanation\n**Correlation ID:** {correlation_id}\n\n{result}"
            finally:
                loop.close()
                
        except Exception as e:
            return f"Error explaining run: {type(e).__name__}: {e}"
