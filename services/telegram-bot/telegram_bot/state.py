"""Persistent state: last processed Telegram update_id.

The state is stored in a JSON file on a Docker volume so that the bot
resumes from the correct offset after a container restart, and no
updates are reprocessed or lost.

Concurrency model: single asyncio event loop, so no locking is needed.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from telegram_bot.log import get_logger

logger = get_logger(__name__)

_DEFAULT_STATE: dict = {"last_update_id": 0}


class BotState:
    """Read/write the bot's persistent state file."""

    def __init__(self, path: str) -> None:
        self._path = Path(path)
        self._state: dict = dict(_DEFAULT_STATE)
        self._load()

    # ── Public ─────────────────────────────────────────────────────────────

    @property
    def last_update_id(self) -> int:
        return self._state["last_update_id"]

    def mark_processed(self, update_id: int) -> None:
        """Advance the cursor. Persists immediately."""
        if update_id > self._state["last_update_id"]:
            self._state["last_update_id"] = update_id
            self._save()

    # ── Private ────────────────────────────────────────────────────────────

    def _load(self) -> None:
        if not self._path.exists():
            logger.info(
                "state_file_not_found",
                correlation_id="SYSTEM",
                path=str(self._path),
                note="starting from update_id=0",
            )
            return
        try:
            data = json.loads(self._path.read_text())
            self._state.update(data)
            logger.info(
                "state_loaded",
                correlation_id="SYSTEM",
                last_update_id=self._state["last_update_id"],
            )
        except Exception as exc:
            logger.warning(
                "state_load_error",
                correlation_id="SYSTEM",
                error=str(exc),
                note="starting from update_id=0",
            )

    def _save(self) -> None:
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self._state, indent=2))
            tmp.replace(self._path)
        except Exception as exc:
            logger.error("state_save_error", correlation_id="SYSTEM", error=str(exc))
