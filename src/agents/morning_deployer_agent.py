from __future__ import annotations

import asyncio
from typing import Any

from src.agents.base_agent import BaseAgent
from src.core.config import load_settings
from src.monitoring.metrics import EVENTS_PROCESSED


class MorningDeployerAgent(BaseAgent):
    """Runs morning health checks and prepares the system for the trading day."""

    def __init__(self) -> None:
        settings = load_settings()
        super().__init__("morning_deployer", settings.app.http.morning_deployer_port)
        self.add_protected_route("/prepare", self.prepare_day, methods=["POST"])

    async def start(self) -> None:
        asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self) -> None:
        while True:
            EVENTS_PROCESSED.labels(agent=self.name, event_type="heartbeat").inc()
            await asyncio.sleep(15)

    async def prepare_day(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        EVENTS_PROCESSED.labels(agent=self.name, event_type="prepare_request").inc()
        self.logger.info("morning_prepare_requested", payload=payload or {})
        return {
            "status": "accepted",
            "agent": self.name,
            "message": "Morning preparation request recorded",
        }


def main() -> None:
    MorningDeployerAgent().run()


if __name__ == "__main__":
    main()
