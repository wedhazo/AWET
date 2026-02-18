"""Redis-backed deduplication for reddit-ingestor.

Key format: ``seen:{content_hash}``
TTL: configurable (default 7 days).

If Redis is unavailable the event is **still published** (availability
over strict dedup) — a warning is logged and a metric incremented.
"""
from __future__ import annotations

import structlog
from redis.asyncio import Redis
from redis.exceptions import RedisError

logger = structlog.get_logger("reddit_ingestor.dedup")


class Deduplicator:
    """Async Redis dedup checker."""

    def __init__(self, redis: Redis, ttl_sec: int = 7 * 86400) -> None:
        self._redis = redis
        self._ttl = ttl_sec

    async def is_seen(self, content_hash: str, *, correlation_id: str) -> bool | None:
        """Check if *content_hash* has been seen before.

        Returns
        -------
        True   — duplicate, skip publish
        False  — new, proceed to publish
        None   — Redis unavailable, proceed but log warning
        """
        key = f"seen:{content_hash}"
        try:
            exists = await self._redis.exists(key)
            if exists:
                logger.info(
                    "dedup_hit",
                    content_hash=content_hash,
                    correlation_id=correlation_id,
                )
                return True
            # Mark as seen
            await self._redis.set(key, "1", ex=self._ttl)
            return False
        except RedisError as exc:
            logger.warning(
                "redis_unavailable",
                error=str(exc),
                content_hash=content_hash,
                correlation_id=correlation_id,
            )
            return None

    async def close(self) -> None:
        await self._redis.aclose()
