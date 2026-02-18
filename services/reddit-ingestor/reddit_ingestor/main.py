"""reddit-ingestor — main entry-point and poll-loop orchestrator.

Lifecycle:
    1. Load settings from env vars.
    2. Boot HTTP server (/healthz, /readyz, /metrics).
    3. Connect to Redis, create Deduplicator.
    4. Create EventPublisher (Kafka).
    5. Create RedditProvider.
    6. Enter poll loop: fetch → normalise → dedup → publish → sleep.
    7. On SIGTERM / SIGINT: flush Kafka, close Redis, stop HTTP, exit 0.
"""
from __future__ import annotations

import asyncio
import os
import signal
import socket
import time
from datetime import UTC, datetime
from uuid import uuid4

import structlog

# Local package imports
from reddit_ingestor.config import Settings, load_settings
from reddit_ingestor.dedup import Deduplicator
from reddit_ingestor.metrics import (
    DEDUPED_TOTAL,
    INGESTION_LATENCY,
    PROVIDER_ERRORS_TOTAL,
    REDIS_UNAVAILABLE_TOTAL,
)
from reddit_ingestor.normalizer import author_hash, content_hash, normalize_text
from reddit_ingestor.provider import RawPost, create_provider
from reddit_ingestor.publisher import EventPublisher
from reddit_ingestor.server import HealthServer

logger = structlog.get_logger("reddit_ingestor.main")


def _now_utc() -> str:
    return datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_envelope(
    post: RawPost,
    *,
    correlation_id: str,
    hostname: str,
) -> dict:
    """Build the full ``social.reddit.raw.v1`` Avro-compatible dict."""
    title_norm = normalize_text(post.title)
    body_norm = normalize_text(post.body)
    created_str = post.created_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    c_hash = content_hash(
        title=post.title,
        body=post.body,
        subreddit=post.subreddit,
        post_id=post.post_id,
        created_at_utc=created_str,
    )

    return {
        # -- envelope --
        "event_id": str(uuid4()),
        "correlation_id": correlation_id,
        "event_version": 1,
        "produced_at_utc": _now_utc(),
        "schema_version": "social.reddit.raw.v1",
        "source": "reddit",
        # -- trace --
        "trace": {
            "service_name": "reddit-ingestor",
            "host": hostname,
            "attempt": 1,
            "parent_event_id": None,
        },
        # -- payload --
        "payload": {
            "source_id": f"reddit:{post.subreddit}:{post.post_id}",
            "subreddit": post.subreddit,
            "post_id": post.post_id,
            "comment_id": post.comment_id,
            "author_hash": author_hash(post.author),
            "created_at_utc": created_str,
            "title": title_norm,
            "body": body_norm,
            "url": post.url,
            "score": post.score,
            "num_comments": post.num_comments,
            "content_hash": c_hash,
            "fetched_at_utc": _now_utc(),
        },
    }


class RedditIngestor:
    """Main service orchestrator."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or load_settings()
        self._hostname = socket.gethostname()
        self._shutdown = asyncio.Event()

    # ------------------------------------------------------------------
    async def run(self) -> None:
        """Boot everything and enter the poll loop."""
        cfg = self._settings

        # -- HTTP server --
        http = HealthServer(cfg.http_port)
        await http.start()

        # -- Redis --
        import redis.asyncio as aioredis

        redis_client = aioredis.from_url(
            cfg.redis_url, decode_responses=True, socket_connect_timeout=5
        )
        dedup = Deduplicator(redis_client, cfg.dedup_ttl_sec)

        # -- Kafka publisher --
        publisher = EventPublisher(
            bootstrap_servers=cfg.kafka_brokers,
            schema_registry_url=cfg.schema_registry_url,
            raw_topic=cfg.raw_topic,
            dlq_topic=cfg.dlq_topic,
            max_retries=cfg.dlq_max_retries,
        )

        # -- Provider --
        provider = create_provider(
            cfg.provider,
            client_id=cfg.reddit_client_id,
            client_secret=cfg.reddit_client_secret,
            username=cfg.reddit_username,
            password=cfg.reddit_password,
            user_agent=cfg.reddit_user_agent,
        )

        # Mark ready once all deps are initialised
        http.set_ready(True)
        logger.info(
            "ingestor_started",
            subreddits=cfg.subreddits,
            provider=cfg.provider,
            poll_interval=cfg.poll_interval_sec,
        )

        # -- Install signal handlers --
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._handle_signal, sig)

        # -- Poll loop --
        try:
            while not self._shutdown.is_set():
                await self._poll_cycle(provider, dedup, publisher, cfg)
                try:
                    await asyncio.wait_for(
                        self._shutdown.wait(), timeout=cfg.poll_interval_sec
                    )
                except TimeoutError:
                    pass
        finally:
            logger.info("shutting_down")
            http.set_ready(False)
            publisher.close()
            await dedup.close()
            await provider.close()
            await http.stop()
            logger.info("shutdown_complete")

    # ------------------------------------------------------------------
    async def _poll_cycle(
        self,
        provider,
        dedup: Deduplicator,
        publisher: EventPublisher,
        cfg: Settings,
    ) -> None:
        correlation_id = str(uuid4())
        t0 = time.monotonic()
        log = logger.bind(correlation_id=correlation_id)

        # 1. Fetch
        try:
            posts = await provider.fetch_posts(cfg.subreddits)
        except Exception as exc:
            PROVIDER_ERRORS_TOTAL.inc()
            log.error("provider_fetch_failed", error=str(exc))
            return

        log.info("fetched_posts", count=len(posts))

        published = 0
        deduped = 0
        redis_miss = 0

        for post in posts:
            created_str = post.created_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            c_hash = content_hash(
                title=post.title,
                body=post.body,
                subreddit=post.subreddit,
                post_id=post.post_id,
                created_at_utc=created_str,
            )

            # 2. Dedup
            seen = await dedup.is_seen(c_hash, correlation_id=correlation_id)
            if seen is True:
                deduped += 1
                DEDUPED_TOTAL.inc()
                continue
            if seen is None:
                redis_miss += 1
                REDIS_UNAVAILABLE_TOTAL.inc()

            # 3. Build envelope + publish
            envelope = _build_envelope(
                post,
                correlation_id=correlation_id,
                hostname=self._hostname,
            )
            key = f"reddit:{post.subreddit}:{post.post_id}"
            publisher.publish_raw(envelope, key=key)
            published += 1

        publisher.poll(0)

        elapsed = time.monotonic() - t0
        INGESTION_LATENCY.observe(elapsed)
        log.info(
            "poll_cycle_done",
            published=published,
            deduped=deduped,
            redis_miss=redis_miss,
            elapsed_sec=round(elapsed, 3),
        )

    # ------------------------------------------------------------------
    def _handle_signal(self, sig: signal.Signals) -> None:
        logger.info("signal_received", signal=sig.name)
        self._shutdown.set()


# -- CLI entry-point -----------------------------------------------------
def main() -> None:
    """Console entry-point for ``python -m reddit_ingestor``."""
    # Configure structlog early
    import structlog as _structlog

    _structlog.configure(
        processors=[
            _structlog.contextvars.merge_contextvars,
            _structlog.processors.add_log_level,
            _structlog.processors.TimeStamper(fmt="iso"),
            _structlog.processors.StackInfoRenderer(),
            _structlog.processors.format_exc_info,
            _structlog.processors.JSONRenderer(),
        ],
        wrapper_class=_structlog.make_filtering_bound_logger(
            int(os.environ.get("LOG_LEVEL", "20"))
        ),
        context_class=dict,
        logger_factory=_structlog.PrintLoggerFactory(),
    )

    ingestor = RedditIngestor()
    asyncio.run(ingestor.run())


if __name__ == "__main__":
    main()
