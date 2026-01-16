"""Approve/Revoke Execution Tools - Controls the execution gate"""

from typing import Type
import time
from pydantic import BaseModel
from superagi.tools.base_tool import BaseTool
import os
from pathlib import Path


class ApproveExecutionTool(BaseTool):
    """
    Tool to approve trade execution (paper trading only).
    
    Creates the approval file that allows ExecutionAgent to process trades.
    Without this file, all trades are blocked and sent to execution.blocked topic.
    """
    
    name: str = "awet_approve_execution"
    description: str = (
        "Approve trade execution by creating the approval gate file. "
        "This allows the ExecutionAgent to process paper trades."
    )
    args_schema: Type[BaseModel] = BaseModel
    
    def _execute(self) -> str:
        start_ts = time.perf_counter()
        approval_path = Path("/app/awet_tools/.tmp/APPROVE_EXECUTION")
        
        try:
            approval_path.parent.mkdir(parents=True, exist_ok=True)
            approval_path.touch()
            return "✅ Execution APPROVED - trades will now be processed"
        except Exception as e:
            return f"Error approving execution: {e}"
        finally:
            duration = time.perf_counter() - start_ts
            print(f"[awet_tool_timing] tool=awet_approve_execution duration_s={duration:.3f}")


class RevokeExecutionTool(BaseTool):
    """
    Tool to revoke trade execution approval.
    
    Removes the approval file, causing all trades to be blocked.
    """
    
    name: str = "awet_revoke_execution"
    description: str = (
        "Revoke trade execution by removing the approval gate file. "
        "All trades will be blocked and sent to execution.blocked topic."
    )
    args_schema: Type[BaseModel] = BaseModel
    
    def _execute(self) -> str:
        start_ts = time.perf_counter()
        approval_path = Path("/app/awet_tools/.tmp/APPROVE_EXECUTION")
        
        try:
            if approval_path.exists():
                approval_path.unlink()
            return "🚫 Execution REVOKED - trades will now be blocked"
        except Exception as e:
            return f"Error revoking execution: {e}"
        finally:
            duration = time.perf_counter() - start_ts
            print(f"[awet_tool_timing] tool=awet_revoke_execution duration_s={duration:.3f}")
