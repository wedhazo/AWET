from __future__ import annotations

import asyncio

from src.agents.base_agent import BaseAgent
from src.core.config import load_settings
from src.monitoring.metrics import EVENTS_PROCESSED


class TradeWatchdogAgent(BaseAgent):
    """Monitors trading operations during market hours."""

    def __init__(self) -> None:
        settings = load_settings()
        super().__init__("trade_watchdog", settings.app.http.trade_watchdog_port)

    async def start(self) -> None:
        asyncio.create_task(self._watch_loop())

    async def _watch_loop(self) -> None:
        while True:
            EVENTS_PROCESSED.labels(agent=self.name, event_type="heartbeat").inc()
            await asyncio.sleep(10)


def main() -> None:
    TradeWatchdogAgent().run()


if __name__ == "__main__":
    main()
