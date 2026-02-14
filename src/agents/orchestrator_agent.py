from __future__ import annotations

import asyncio
from typing import Any

from src.agents.base_agent import BaseAgent
from src.core.config import load_settings
from src.monitoring.metrics import EVENTS_PROCESSED


class OrchestratorAgent(BaseAgent):
    """Orchestrates high-level workflow by triggering downstream agents."""

    def __init__(self) -> None:
        settings = load_settings()
        super().__init__("orchestrator", settings.app.http.orchestrator_port)
        self.add_protected_route("/run", self.run_pipeline, methods=["POST"])

    async def start(self) -> None:
        asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self) -> None:
        while True:
            EVENTS_PROCESSED.labels(agent=self.name, event_type="heartbeat").inc()
            await asyncio.sleep(10)

    async def run_pipeline(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        EVENTS_PROCESSED.labels(agent=self.name, event_type="run_request").inc()
        self.logger.info("pipeline_run_requested", payload=payload or {})
        return {
            "status": "accepted",
            "agent": self.name,
            "message": "Pipeline run request recorded",
        }


def main() -> None:
    OrchestratorAgent().run()


if __name__ == "__main__":
    main()
