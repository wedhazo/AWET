"""Environment-driven configuration for reddit-ingestor."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class Settings:
    """Immutable settings populated from environment variables."""

    # Kafka
    kafka_brokers: str = os.environ.get("KAFKA_BROKERS", "localhost:9092")
    schema_registry_url: str = os.environ.get(
        "SCHEMA_REGISTRY_URL", "http://localhost:8081"
    )

    # Redis
    redis_url: str = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

    # Polling
    poll_interval_sec: int = int(os.environ.get("POLL_INTERVAL_SEC", "60"))

    # DLQ
    dlq_max_retries: int = int(os.environ.get("DLQ_MAX_RETRIES", "3"))

    # Dedup TTL (seconds)
    dedup_ttl_sec: int = int(os.environ.get("DEDUP_TTL_SEC", str(7 * 86400)))

    # HTTP server
    http_port: int = int(os.environ.get("HTTP_PORT", "8090"))

    # Subreddits to watch (comma-separated)
    subreddits: list[str] = field(
        default_factory=lambda: os.environ.get(
            "SUBREDDITS", "wallstreetbets,stocks,investing"
        ).split(",")
    )

    # Provider
    provider: str = os.environ.get("REDDIT_PROVIDER", "stub")

    # Reddit API credentials (only needed when provider="reddit_api")
    reddit_client_id: str = os.environ.get("REDDIT_CLIENT_ID", "")
    reddit_client_secret: str = os.environ.get("REDDIT_CLIENT_SECRET", "")
    reddit_username: str = os.environ.get("REDDIT_USERNAME", "")
    reddit_password: str = os.environ.get("REDDIT_PASSWORD", "")
    reddit_user_agent: str = os.environ.get(
        "REDDIT_USER_AGENT", "awet-ingestor:v1.0 (by /u/awet_bot)"
    )

    # Topics
    raw_topic: str = "social.reddit.raw"
    dlq_topic: str = "dlq.social.reddit.raw"


def load_settings() -> Settings:
    """Return a fresh ``Settings`` from the current environment."""
    return Settings()
