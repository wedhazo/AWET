from __future__ import annotations

import asyncio
from typing import Any

from src.agents.base_agent import BaseAgent
from src.core.config import load_settings
from src.monitoring.metrics import EVENTS_PROCESSED


class NightTrainerAgent(BaseAgent):
    """Runs model training jobs during off-hours."""

    def __init__(self) -> None:
        settings = load_settings()
        super().__init__("night_trainer", settings.app.http.night_trainer_port)
        self.add_protected_route("/train", self.run_training, methods=["POST"])

    async def start(self) -> None:
        asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self) -> None:
        while True:
            EVENTS_PROCESSED.labels(agent=self.name, event_type="heartbeat").inc()
            await asyncio.sleep(15)

    async def run_training(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        EVENTS_PROCESSED.labels(agent=self.name, event_type="train_request").inc()
        self.logger.info("training_requested", payload=payload or {})
        return {
            "status": "accepted",
            "agent": self.name,
            "message": "Training request recorded",
        }


def main() -> None:
    NightTrainerAgent().run()


if __name__ == "__main__":
    main()
