"""Check Pipeline Health Tool - Checks /health endpoints of all agents"""

from typing import Type
import time
from pydantic import BaseModel
from superagi.tools.base_tool import BaseTool
import httpx
import asyncio


AGENT_HEALTH_ENDPOINTS = [
    ("data_ingestion", "http://localhost:8010/health"),
    ("feature_engineering", "http://localhost:8011/health"),
    ("time_series_prediction", "http://localhost:8012/health"),
    ("risk_agent", "http://localhost:8013/health"),
    ("execution_agent", "http://localhost:8014/health"),
    ("watchtower", "http://localhost:8015/health"),
]


class CheckPipelineHealthTool(BaseTool):
    """
    Tool to check the health status of all pipeline agents.
    
    Queries the /health endpoint of each agent and reports their status.
    """
    
    name: str = "awet_check_pipeline_health"
    description: str = (
        "Check the health status of all trading pipeline agents. "
        "Reports which agents are healthy and which are down."
    )
    args_schema: Type[BaseModel] = BaseModel
    
    def _execute(self) -> str:
        start_ts = time.perf_counter()
        async def check_health():
            results = []
            async with httpx.AsyncClient(timeout=5.0) as client:
                for name, url in AGENT_HEALTH_ENDPOINTS:
                    try:
                        response = await client.get(url)
                        if response.status_code == 200:
                            results.append(f"✅ {name}: healthy")
                        else:
                            results.append(f"⚠️ {name}: unhealthy (status {response.status_code})")
                    except Exception as e:
                        results.append(f"❌ {name}: unreachable ({type(e).__name__})")
            return results
        
        try:
            results = asyncio.run(check_health())
            
            output = "# Pipeline Health Status\n\n"
            output += "\n".join(results)
            
            healthy_count = sum(1 for r in results if r.startswith("✅"))
            total_count = len(results)
            
            output += f"\n\nSummary: {healthy_count}/{total_count} agents healthy"
            
            return output
            
        except Exception as e:
            return f"Error checking health: {e}"
        finally:
            duration = time.perf_counter() - start_ts
            print(f"[awet_tool_timing] tool=awet_check_pipeline_health duration_s={duration:.3f}")
