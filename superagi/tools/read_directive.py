"""Read Directive Tool - Reads SOP documents from /directives/"""

from typing import Type
import time
from pydantic import BaseModel, Field
from superagi.tools.base_tool import BaseTool
import os


class ReadDirectiveInput(BaseModel):
    directive_name: str = Field(
        ...,
        description="Name of the directive file to read (e.g., 'trading_pipeline.md', 'backfill.md')"
    )


class ReadDirectiveTool(BaseTool):
    """
    Tool to read directive documents that define SOPs for the trading pipeline.
    
    Directives contain:
    - Goal and success criteria
    - Inputs (topics, symbols, time ranges, configs)
    - Tools/scripts to use
    - Outputs (Kafka topics, DB tables, artifacts)
    - Edge cases and failure modes
    """
    
    name: str = "awet_read_directive"
    description: str = (
        "Read a directive (SOP) document from the directives folder. "
        "Directives contain instructions for executing trading pipeline tasks."
    )
    args_schema: Type[BaseModel] = ReadDirectiveInput
    
    def _execute(self, directive_name: str) -> str:
        start_ts = time.perf_counter()
        directives_path = "/app/awet_tools/directives"
        
        # Ensure .md extension
        if not directive_name.endswith('.md'):
            directive_name = f"{directive_name}.md"
        
        file_path = os.path.join(directives_path, directive_name)
        
        # Security: prevent path traversal
        if ".." in directive_name or directive_name.startswith("/"):
            return "Error: Invalid directive name (path traversal not allowed)"
        
        if not os.path.exists(file_path):
            # List available directives
            available = []
            if os.path.exists(directives_path):
                available = [f for f in os.listdir(directives_path) if f.endswith('.md')]
            return f"Error: Directive '{directive_name}' not found. Available: {available}"
        
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            return f"# Directive: {directive_name}\n\n{content}"
        except Exception as e:
            return f"Error reading directive: {e}"
        finally:
            duration = time.perf_counter() - start_ts
            print(f"[awet_tool_timing] tool=awet_read_directive duration_s={duration:.3f}")
