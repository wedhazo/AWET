"""In-process token-bucket rate limiter, one bucket per chat_id.

Thread-safe for asyncio single-event-loop use.  Not shared across
processes (suitable for single-instance bot deployment).

Design: simple token bucket — each chat gets 1 token per
``interval_sec`` seconds (refilled lazily on each check).
"""
from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class _Bucket:
    tokens: float = 1.0
    last_refill: float = field(default_factory=time.monotonic)


class RateLimiter:
    """Per-chat-id token bucket rate limiter."""

    def __init__(self, messages_per_sec: float = 1.0) -> None:
        self._interval = 1.0 / max(messages_per_sec, 0.001)
        self._buckets: dict[int, _Bucket] = defaultdict(lambda: _Bucket(tokens=1.0, last_refill=time.monotonic()))

    def allow(self, chat_id: int) -> bool:
        """Return True if the message is allowed; False if rate-limited.

        Note: the first message for a new ``chat_id`` should always be allowed.
        """
        now = time.monotonic()

        # First message for a chat_id: allow immediately and consume the token.
        # This avoids confusing "you're sending messages too fast" on the first interaction.
        if chat_id not in self._buckets:
            self._buckets[chat_id] = _Bucket(tokens=0.0, last_refill=now)
            return True

        bucket = self._buckets[chat_id]
        elapsed = now - bucket.last_refill
        # Refill tokens (max 1.0)
        bucket.tokens = min(1.0, bucket.tokens + elapsed / self._interval)
        bucket.last_refill = now
        if bucket.tokens >= 1.0:
            bucket.tokens -= 1.0
            return True
        return False
