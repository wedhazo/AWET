from __future__ import annotations

import asyncio
from typing import Any

import structlog
import uvicorn
from fastapi import FastAPI

from src.core.config import Settings, load_settings
from src.core.logging import configure_logging, correlation_middleware
from src.monitoring.metrics import metrics_response


class BaseAgent:
    def __init__(self, name: str, port: int) -> None:
        self.name = name
        self.settings: Settings = load_settings()
        configure_logging(self.settings.logging.level)
        self.logger = structlog.get_logger(name)
        self.port = port
        self.app = FastAPI(title=name)
        self.app.middleware("http")(correlation_middleware(self.settings.app.correlation_header))
        self.app.add_api_route("/health", self.health, methods=["GET"])
        self.app.add_api_route("/metrics", metrics_response, methods=["GET"])
        self.app.add_event_handler("startup", self._startup)

    async def _startup(self) -> None:
        await self.start()

    async def start(self) -> None:
        raise NotImplementedError

    async def health(self) -> dict[str, Any]:
        return {"status": "ok", "agent": self.name}

    def run(self) -> None:
        uvicorn.run(self.app, host=self.settings.app.http.host, port=self.port, log_level="info")
