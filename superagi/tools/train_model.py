"""Train Model Tool - Triggers TFT model training"""

from typing import Type
import time
from pydantic import BaseModel, Field
from superagi.tools.base_tool import BaseTool
import subprocess
import os


class TrainModelInput(BaseModel):
    tickers: str = Field(
        default="AAPL,MSFT,NVDA",
        description="Comma-separated list of tickers to train on"
    )
    lookback_days: int = Field(
        default=30,
        description="Number of days of historical data to use for training"
    )
    epochs: int = Field(
        default=100,
        description="Number of training epochs"
    )


class TrainModelTool(BaseTool):
    """
    Tool to trigger TFT model training.
    
    This tool calls the deterministic training script that:
    - Loads engineered features from TimescaleDB
    - Trains a Temporal Fusion Transformer model
    - Exports to ONNX format
    - Registers in the model registry with 'yellow' status
    """
    
    name: str = "awet_train_model"
    description: str = (
        "Train a new TFT (Temporal Fusion Transformer) model on historical data. "
        "The trained model will be registered with 'yellow' status, pending promotion."
    )
    args_schema: Type[BaseModel] = TrainModelInput
    
    def _execute(
        self,
        tickers: str = "AAPL,MSFT,NVDA",
        lookback_days: int = 30,
        epochs: int = 100
    ) -> str:
        start_ts = time.perf_counter()
        cmd = [
            "python", "-m", "src.ml.train",
            "train",
            "--tickers", tickers,
            "--lookback-days", str(lookback_days),
            "--epochs", str(epochs),
        ]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=7200,  # 2 hour timeout for training
                cwd="/app/awet_tools",
                env={**os.environ}
            )
            
            output = f"Training completed with exit code: {result.returncode}\n"
            if result.stdout:
                output += f"\nSTDOUT:\n{result.stdout[-3000:]}"
            if result.stderr:
                output += f"\nSTDERR:\n{result.stderr[-1000:]}"
            
            return output
            
        except subprocess.TimeoutExpired:
            return "Error: Training timed out after 2 hours"
        except Exception as e:
            return f"Error during training: {e}"
        finally:
            duration = time.perf_counter() - start_ts
            print(f"[awet_tool_timing] tool=awet_train_model duration_s={duration:.3f}")
