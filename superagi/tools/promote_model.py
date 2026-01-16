"""Promote Model Tool - Promotes a model to green status"""

from typing import Type
import time
from pydantic import BaseModel, Field
from superagi.tools.base_tool import BaseTool
import subprocess
import os


class PromoteModelInput(BaseModel):
    model_id: str = Field(
        ...,
        description="The model ID to promote (e.g., 'tft_20260115_081459_7f6cae3b')"
    )


class PromoteModelTool(BaseTool):
    """
    Tool to promote a trained model to 'green' (production) status.
    
    This tool:
    - Validates the model exists in the registry
    - Demotes any existing green model to 'blue' (archived)
    - Promotes the specified model to 'green'
    - The PredictionAgent will automatically load the new green model
    """
    
    name: str = "awet_promote_model"
    description: str = (
        "Promote a trained model to 'green' (production) status. "
        "The model will be used for live predictions after promotion."
    )
    args_schema: Type[BaseModel] = PromoteModelInput
    
    def _execute(self, model_id: str) -> str:
        start_ts = time.perf_counter()
        cmd = [
            "python", "-m", "src.ml.train",
            "promote",
            "--model-id", model_id,
        ]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60,
                cwd="/app/awet_tools",
                env={**os.environ}
            )
            
            output = f"Promotion completed with exit code: {result.returncode}\n"
            if result.stdout:
                output += f"\nSTDOUT:\n{result.stdout}"
            if result.stderr:
                output += f"\nSTDERR:\n{result.stderr}"
            
            return output
            
        except Exception as e:
            return f"Error promoting model: {e}"
        finally:
            duration = time.perf_counter() - start_ts
            print(f"[awet_tool_timing] tool=awet_promote_model duration_s={duration:.3f}")
