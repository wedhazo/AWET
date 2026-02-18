"""Entry point: wire up all components and run the bot.

Starts two concurrent tasks:
1. The observability HTTP server (health/ready/metrics).
2. The Telegram long-polling loop.

Graceful shutdown: SIGINT or ctrl-C stops both tasks cleanly.
"""
from __future__ import annotations

import asyncio
import signal
import sys

from telegram_bot.bot import TelegramBot
from telegram_bot.config import load_config
from telegram_bot.kb_client import KBClient
from telegram_bot.log import configure_logging, get_logger
from telegram_bot.server import start_server
from telegram_bot.state import BotState

logger = get_logger(__name__)


async def _run() -> None:
    cfg = load_config()
    configure_logging(cfg.log_level)

    logger.info("bot_starting", correlation_id="SYSTEM", version="1.0.0")
    logger.info(
        "config_loaded",
        correlation_id="SYSTEM",
        metrics_port=cfg.metrics_port,
        rate_limit_messages_per_sec=cfg.rate_limit_messages_per_sec,
        kb_query_base_url=cfg.kb_query_base_url,
    )

    # Shared components
    kb_client = KBClient(
        base_url=cfg.kb_query_base_url,
        timeout_sec=cfg.http_timeout_sec,
        max_retries=cfg.http_max_retries,
    )
    await kb_client.start()

    state = BotState(cfg.state_file)

    bot = TelegramBot(
        config=cfg,
        kb_client=kb_client,
        state=state,
    )
    await bot.start()

    # Start observability server
    obs_runner = await start_server(kb_client, cfg.metrics_port)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("shutdown_signal_received", correlation_id="SYSTEM")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    # Run bot polling + wait for stop
    polling_task = asyncio.create_task(bot.run(), name="polling")
    try:
        await stop_event.wait()
    finally:
        polling_task.cancel()
        try:
            await polling_task
        except asyncio.CancelledError:
            pass
        await obs_runner.cleanup()
        await bot.close()
        await kb_client.close()
        logger.info("bot_stopped", correlation_id="SYSTEM")


def main() -> None:
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
