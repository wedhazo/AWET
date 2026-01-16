"""Check Kafka Lag Tool - Monitors consumer lag.

Observability:
    - Logs execution duration.
    - Logs slow operations (>SLOW_OP_SECONDS threshold).
    - Returns duration in output footer.
"""

from typing import Type
import logging
import os
import subprocess
import time

from pydantic import BaseModel
from superagi.tools.base_tool import BaseTool

# Threshold for logging slow operations (seconds)
SLOW_OP_SECONDS = float(os.getenv("SLOW_OP_SECONDS", "2.0"))

logger = logging.getLogger(__name__)


class CheckKafkaLagTool(BaseTool):
    """
    Tool to check Kafka consumer lag for pipeline agents.

    Uses kafka-consumer-groups to get lag metrics.

    Observability:
        - Measures total execution duration.
        - Logs slow operations (>SLOW_OP_SECONDS).
        - Includes duration in returned output.
    """

    name: str = "awet_check_kafka_lag"
    description: str = (
        "Check Kafka consumer lag for all pipeline consumer groups. "
        "High lag indicates agents are falling behind in processing."
    )
    args_schema: Type[BaseModel] = BaseModel

    def _execute(self) -> str:
        start_ts = time.perf_counter()
        consumer_groups = [
            "data-ingestion-group",
            "feature-engineering-group",
            "prediction-group",
            "risk-group",
            "execution-group",
            "watchtower-group",
        ]

        kafka_bootstrap = os.getenv("AWET_KAFKA_BOOTSTRAP", "kafka:29092")

        output = "# Kafka Consumer Lag Report\n\n"

        try:
            for group in consumer_groups:
                cmd = [
                    "kafka-consumer-groups",
                    "--bootstrap-server", kafka_bootstrap,
                    "--describe",
                    "--group", group,
                ]

                try:
                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        timeout=30
                    )

                    if result.returncode == 0:
                        output += f"## {group}\n```\n{result.stdout}\n```\n\n"
                    else:
                        output += f"## {group}\n⚠️ Group not found or no active members\n\n"

                except Exception as e:
                    output += f"## {group}\n❌ Error: {e}\n\n"

            return output
        finally:
            duration = time.perf_counter() - start_ts
            # Always log timing
            print(f"[awet_tool_timing] tool=awet_check_kafka_lag duration_s={duration:.3f}")
            # Log warning if slow
            if duration > SLOW_OP_SECONDS:
                logger.warning(
                    "[awet_slow_op] tool=awet_check_kafka_lag duration_s=%.3f threshold_s=%.1f",
                    duration,
                    SLOW_OP_SECONDS,
                )
