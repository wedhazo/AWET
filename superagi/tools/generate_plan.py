"""Generate Plan Tool - Uses local LLM to create execution plans from directives"""

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
    # Fallback for local testing
    class BaseTool:
        name: str = ""
        description: str = ""
        args_schema: type = None
        def _execute(self, **kwargs) -> str:
            raise NotImplementedError


class GeneratePlanInput(BaseModel):
    directive_name: str = Field(
        ...,
        description="Name of the directive to plan (e.g., 'trading_pipeline', 'backfill')"
    )
    context: str = Field(
        default="{}",
        description="JSON string with context (symbols, time range, etc.)"
    )


class GeneratePlanTool(BaseTool):
    """
    Tool that uses the local Llama LLM to generate an execution plan
    based on a directive and context.
    
    This calls src/orchestration/llm_gateway.generate_plan() which:
    - Loads the directive from directives/{name}.md
    - Sends it to the local LLM with context
    - Returns an execution plan as a string
    """
    
    name: str = "awet_generate_plan"
    description: str = (
        "Generate an execution plan using the local LLM. "
        "Reads a directive and context, then asks the LLM to produce a step-by-step plan. "
        "Use this before running pipeline operations to get intelligent guidance."
    )
    args_schema: Type[BaseModel] = GeneratePlanInput
    
    def _execute(self, directive_name: str, context: str = "{}") -> str:
        import json
        
        try:
            context_dict = json.loads(context)
        except json.JSONDecodeError:
            return f"Error: Invalid JSON context: {context}"
        
        try:
            from src.orchestration.llm_gateway import generate_plan
            
            # Run the async function
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(generate_plan(directive_name, context_dict))
                return f"# Execution Plan for: {directive_name}\n\n{result}"
            finally:
                loop.close()
                
        except FileNotFoundError as e:
            return f"Error: Directive not found - {e}"
        except Exception as e:
            return f"Error generating plan: {type(e).__name__}: {e}"
