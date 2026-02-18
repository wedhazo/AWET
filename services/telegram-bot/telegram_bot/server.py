"""Lightweight aiohttp-based HTTP server for /healthz, /readyz, /metrics.

Runs in the same event loop as the polling loop on a configurable port.
"""
from __future__ import annotations

import asyncio

from aiohttp import web

from telegram_bot.kb_client import KBClient
from telegram_bot.log import get_logger
from telegram_bot.metrics import render_metrics

logger = get_logger(__name__)


def build_app(kb_client: KBClient) -> web.Application:
    app = web.Application()
    app["kb_client"] = kb_client
    app.router.add_get("/healthz", _healthz)
    app.router.add_get("/readyz", _readyz)
    app.router.add_get("/metrics", _metrics)
    return app


async def _healthz(request: web.Request) -> web.Response:
    """Always 200 while the process is alive."""
    return web.Response(text="ok", status=200)


async def _readyz(request: web.Request) -> web.Response:
    """200 only if kb-query is reachable."""
    kb: KBClient = request.app["kb_client"]
    if await kb.healthz():
        return web.Response(text="ready", status=200)
    logger.warning("readyz_kb_query_unreachable", correlation_id="SYSTEM")
    return web.Response(text="kb-query unreachable", status=503)


async def _metrics(request: web.Request) -> web.Response:
    body, content_type = render_metrics()
    # prometheus_client exposes content type including charset; aiohttp expects charset separately.
    ct = content_type
    charset = None
    if ";" in ct and "charset=" in ct:
        parts = [p.strip() for p in ct.split(";")]
        ct = parts[0]
        for p in parts[1:]:
            if p.lower().startswith("charset="):
                charset = p.split("=", 1)[1].strip() or None
    return web.Response(body=body, content_type=ct, charset=charset)


async def start_server(kb_client: KBClient, port: int) -> web.AppRunner:
    """Start the observability HTTP server. Returns runner (caller handles stop)."""
    app = build_app(kb_client)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("obs_server_started", correlation_id="SYSTEM", port=port)
    return runner
