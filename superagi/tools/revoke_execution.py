"""Revoke Execution Tool - Controls the execution gate"""

from typing import Type
from pydantic import BaseModel
from superagi.tools.base_tool import BaseTool
from pathlib import Path


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
        approval_path = Path("/app/awet_tools/.tmp/APPROVE_EXECUTION")
        
        try:
            if approval_path.exists():
                approval_path.unlink()
            return "🚫 Execution REVOKED - trades will now be blocked"
        except Exception as e:
            return f"Error revoking execution: {e}"


__all__ = ["RevokeExecutionTool"]
