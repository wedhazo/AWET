"""Reddit data provider interface + stub implementation.

The Provider protocol defines how the ingestor fetches raw Reddit content.
Concrete implementations (real API, stub for tests) must implement
``fetch_posts(subreddits) -> list[RawPost]``.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class RawPost:
    """Minimal representation of a Reddit post/comment as returned by a provider."""

    subreddit: str
    post_id: str
    comment_id: str | None
    author: str
    title: str
    body: str
    url: str
    score: int
    num_comments: int
    created_utc: datetime  # must be UTC-aware
    is_submission: bool


@runtime_checkable
class RedditProvider(Protocol):
    """Abstract provider interface."""

    async def fetch_posts(self, subreddits: list[str]) -> list[RawPost]:
        """Return a batch of raw posts for the given subreddits."""
        ...

    async def close(self) -> None:
        """Release resources."""
        ...


class StubProvider:
    """Deterministic fixture provider for testing.

    Returns fixed post data so that unit and integration tests
    always see the same content_hash and author_hash values.
    """

    _FIXTURES: list[RawPost] = [
        RawPost(
            subreddit="wallstreetbets",
            post_id="t3_abc123",
            comment_id=None,
            author="testuser1",
            title="AAPL to the moon 🚀",
            body="I think Apple will hit $300 by end of year. Bought 100 calls.",
            url="https://www.reddit.com/r/wallstreetbets/comments/abc123/",
            score=1542,
            num_comments=387,
            created_utc=datetime(2026, 2, 17, 10, 30, 0, tzinfo=UTC),
            is_submission=True,
        ),
        RawPost(
            subreddit="wallstreetbets",
            post_id="t3_def456",
            comment_id=None,
            author="testuser2",
            title="TSLA earnings play",
            body="Selling puts before earnings. IV is insane right now.",
            url="https://www.reddit.com/r/wallstreetbets/comments/def456/",
            score=823,
            num_comments=145,
            created_utc=datetime(2026, 2, 17, 11, 0, 0, tzinfo=UTC),
            is_submission=True,
        ),
        RawPost(
            subreddit="stocks",
            post_id="t3_ghi789",
            comment_id=None,
            author="testuser3",
            title="Fed meeting discussion thread",
            body="What are your expectations for the rate decision?",
            url="https://www.reddit.com/r/stocks/comments/ghi789/",
            score=2104,
            num_comments=891,
            created_utc=datetime(2026, 2, 17, 12, 0, 0, tzinfo=UTC),
            is_submission=True,
        ),
    ]

    async def fetch_posts(self, subreddits: list[str]) -> list[RawPost]:
        """Return fixture posts filtered by requested subreddits."""
        return [p for p in self._FIXTURES if p.subreddit in subreddits]

    async def close(self) -> None:
        """No-op for stub."""


def create_provider(
    name: str,
    *,
    client_id: str = "",
    client_secret: str = "",
    username: str = "",
    password: str = "",
    user_agent: str = "",
) -> RedditProvider:
    """Factory: create the appropriate provider by name."""
    if name == "stub":
        return StubProvider()
    if name == "reddit_api":
        from reddit_ingestor.reddit_api_provider import RedditApiProvider

        return RedditApiProvider(
            client_id=client_id,
            client_secret=client_secret,
            username=username,
            password=password,
            user_agent=user_agent,
        )
    msg = f"Unknown provider: {name!r} (available: stub, reddit_api)"
    raise ValueError(msg)
