"""
AWET Trading Pipeline Tools for SuperAGI

These tools allow SuperAGI agents to orchestrate the AWET trading pipeline
by calling deterministic execution scripts. The tools enforce the 3-layer
architecture where the LLM (SuperAGI) only triggers actions, never implements
trading logic directly.

Architecture:
- Layer 1: Directives (read-only instructions in /directives/)
- Layer 2: Orchestration (SuperAGI agents using these tools)
- Layer 3: Execution (deterministic scripts in /execution/)

LLM-Powered Tools:
- GeneratePlanTool: Uses local Llama to create execution plans from directives
- ExplainRunTool: Uses local Llama to explain pipeline runs from audit trail
"""

from .read_directive import ReadDirectiveTool
from .run_backfill import RunBackfillTool
from .train_model import TrainModelTool
from .promote_model import PromoteModelTool
from .approve_execution import ApproveExecutionTool
from .revoke_execution import RevokeExecutionTool
from .run_demo import RunDemoTool
from .check_pipeline_health import CheckPipelineHealthTool
from .query_audit_trail import QueryAuditTrailTool
from .check_kafka_lag import CheckKafkaLagTool
from .generate_plan import GeneratePlanTool
from .explain_run import ExplainRunTool

__all__ = [
    "ReadDirectiveTool",
    "RunBackfillTool", 
    "TrainModelTool",
    "PromoteModelTool",
    "ApproveExecutionTool",
    "RevokeExecutionTool",
    "RunDemoTool",
    "CheckPipelineHealthTool",
    "QueryAuditTrailTool",
    "CheckKafkaLagTool",
    "GeneratePlanTool",
    "ExplainRunTool",
]
