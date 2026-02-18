"""Lightweight HTTP server for /healthz, /readyz, /metrics.

Uses aiohttp so the service doesn't pull in FastAPI/uvicorn.
"""
from __future__ import annotations

import structlog
from aiohttp import web
from reddit_ingestor.metrics import metrics_body, metrics_content_type

logger = structlog.get_logger("reddit_ingestor.http")


class HealthServer:
    """Async HTTP health/metrics server."""

    def __init__(self, port: int) -> None:
        self._port = port
        self._app = web.Application()
        self._runner: web.AppRunner | None = None
        self._ready = False

        self._app.router.add_get("/healthz", self._healthz)
        self._app.router.add_get("/readyz", self._readyz)
        self._app.router.add_get("/metrics", self._metrics)

    # -- handlers -----------------------------------------------------------

    async def _healthz(self, _: web.Request) -> web.Response:
        return web.Response(text="ok")

    async def _readyz(self, _: web.Request) -> web.Response:
        if self._ready:
            return web.Response(text="ready")
        return web.Response(status=503, text="not ready")

    async def _metrics(self, _: web.Request) -> web.Response:
        body = metrics_body()
        ct = metrics_content_type()
        # aiohttp rejects charset in content_type kwarg — strip it
        ct_base = ct.split(";")[0].strip()
        resp = web.Response(body=body, content_type=ct_base)
        # Re-add full header (with charset) so Prometheus scraper is happy
        resp.headers["Content-Type"] = ct
        return resp

    # -- lifecycle -----------------------------------------------------------

    def set_ready(self, ready: bool = True) -> None:
        self._ready = ready

    async def start(self) -> None:
        self._runner = web.AppRunner(self._app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", self._port)
        await site.start()
        logger.info("http_server_started", port=self._port)

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            logger.info("http_server_stopped")
