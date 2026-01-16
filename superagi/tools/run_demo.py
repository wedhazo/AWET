"""Run Demo Tool - Runs the end-to-end pipeline demo"""

from typing import Type
import time
from pydantic import BaseModel
from superagi.tools.base_tool import BaseTool
import subprocess
import os


class RunDemoTool(BaseTool):
    """
    Tool to run the end-to-end trading pipeline demo.
    
    This tool:
    - Starts all pipeline agents
    - Generates synthetic market events
    - Verifies events flow through all pipeline stages
    - Reports success/failure
    """
    
    name: str = "awet_run_demo"
    description: str = (
        "Run the end-to-end trading pipeline demo. "
        "Generates synthetic data and verifies the full message flow: "
        "market.raw → market.engineered → predictions.tft → risk.approved → execution.completed/blocked"
    )
    args_schema: Type[BaseModel] = BaseModel
    
    def _execute(self) -> str:
        start_ts = time.perf_counter()
        cmd = ["python", "/app/awet_tools/execution/demo.py"]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=180,  # 3 minute timeout
                cwd="/app/awet_tools",
                env={**os.environ}
            )
            
            output = f"Demo completed with exit code: {result.returncode}\n"
            if result.stdout:
                output += f"\n{result.stdout}"
            if result.stderr:
                output += f"\nErrors:\n{result.stderr[-500:]}"
            
            if result.returncode == 0:
                output += "\n\n✅ DEMO PASSED - Pipeline is working correctly"
            else:
                output += "\n\n❌ DEMO FAILED - Check the output above for details"
            
            return output
            
        except subprocess.TimeoutExpired:
            return "Error: Demo timed out after 3 minutes"
        except Exception as e:
            return f"Error running demo: {e}"
        finally:
            duration = time.perf_counter() - start_ts
            print(f"[awet_tool_timing] tool=awet_run_demo duration_s={duration:.3f}")
