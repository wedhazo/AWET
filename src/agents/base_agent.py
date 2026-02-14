from __future__ import annotations

import asyncio
import os
import signal
from typing import Any

import structlog
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.security import APIKeyHeader

from src.core.config import Settings, load_settings
from src.core.logging import configure_logging, correlation_middleware
from src.monitoring.metrics import metrics_response

# API Key authentication
API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)
AGENT_API_KEY = os.getenv("AGENT_API_KEY", "")


async def verify_api_key(
    request: Request,
    api_key: str | None = Depends(API_KEY_HEADER),
) -> str | None:
    """Verify API key if AGENT_API_KEY is set.
    
    If AGENT_API_KEY env var is not set, authentication is disabled.
    If set, all requests (except /health and /metrics) require valid key.
    """
    # Skip auth if not configured
    if not AGENT_API_KEY:
        return None
    
    # Allow health/metrics without auth
    if request.url.path in ("/health", "/metrics"):
        return None
    
    # Require valid key for other endpoints
    if not api_key or api_key != AGENT_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    
    return api_key


class BaseAgent:
    def __init__(self, name: str, port: int) -> None:
        self.name = name
        self.settings: Settings = load_settings()
        configure_logging(self.settings.logging.level)
        self.logger = structlog.get_logger(name)
        self.port = port
        self._shutdown_event = asyncio.Event()
        self._background_tasks: list[asyncio.Task] = []
        self.app = FastAPI(title=name)
        self.app.middleware("http")(correlation_middleware(self.settings.app.correlation_header))
        
        # Add API key dependency to all routes
        self.app.add_api_route("/health", self.health, methods=["GET"])
        self.app.add_api_route("/metrics", metrics_response, methods=["GET"])
        self.app.add_event_handler("startup", self._startup)
        self.app.add_event_handler("shutdown", self._on_shutdown)
        
        # Log auth status
        if AGENT_API_KEY:
            self.logger.info("api_auth_enabled", agent=name)
        else:
            self.logger.warning("api_auth_disabled", agent=name, hint="Set AGENT_API_KEY env var")

    def add_protected_route(self, path: str, endpoint, methods: list[str] = None):
        """Add a route that requires API key authentication."""
        self.app.add_api_route(
            path,
            endpoint,
            methods=methods or ["POST"],
            dependencies=[Depends(verify_api_key)],
        )

    async def _startup(self) -> None:
        # Install signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self._signal_handler(s)))
        self.logger.info("agent_starting", agent=self.name, port=self.port)
        await self.start()

    async def _signal_handler(self, sig: signal.Signals) -> None:
        self.logger.warning("shutdown_signal_received", signal=sig.name, agent=self.name)
        self._shutdown_event.set()

    async def _on_shutdown(self) -> None:
        """FastAPI shutdown hook — clean up background tasks and resources."""
        self.logger.info("agent_shutting_down", agent=self.name)
        self._shutdown_event.set()
        # Cancel background tasks with grace period
        for task in self._background_tasks:
            if not task.done():
                task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        await self.shutdown()
        self.logger.info("agent_shutdown_complete", agent=self.name)

    def track_task(self, task: asyncio.Task) -> asyncio.Task:
        """Register a background task for cleanup on shutdown."""
        self._background_tasks.append(task)
        task.add_done_callback(self._task_done_callback)
        return task

    def _task_done_callback(self, task: asyncio.Task) -> None:
        """Log if a background task exits unexpectedly."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            self.logger.error("background_task_failed", agent=self.name, error=str(exc), exc_info=exc)

    @property
    def is_shutting_down(self) -> bool:
        return self._shutdown_event.is_set()

    async def start(self) -> None:
        raise NotImplementedError

    async def shutdown(self) -> None:
        """Override in subclasses to release resources (close consumers, producers, DB pools)."""
        pass

    async def health(self) -> dict[str, Any]:
        """Deep health check — probes subsystems registered by subclasses."""
        subsystems = await self.health_subsystems()
        all_ok = all(v.get("ok", False) for v in subsystems.values()) if subsystems else True
        overall = "shutting_down" if self.is_shutting_down else ("ok" if all_ok else "degraded")
        return {
            "status": overall,
            "agent": self.name,
            "auth_enabled": bool(AGENT_API_KEY),
            "subsystems": subsystems,
        }

    async def health_subsystems(self) -> dict[str, dict[str, Any]]:
        """Override in subclasses to register deep health probes.

        Return a dict like::

            {"kafka": {"ok": True}, "db": {"ok": True, "latency_ms": 2}}
        """
        return {}

    def run(self) -> None:
        uvicorn.run(self.app, host=self.settings.app.http.host, port=self.port, log_level="info")
