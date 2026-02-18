"""Synchronous Reddit API client using OAuth2 *script* (password grant) flow.

This module provides a ``RedditClient`` that:
  • Authenticates via ``POST /api/v1/access_token`` with HTTP Basic Auth.
  • Caches the bearer token in memory until expiry.
  • Fetches ``/new`` and ``/hot`` listings from any subreddit.
  • Returns deterministic, normalised JSON dicts (RFC 3339 timestamps,
    zero-width chars stripped, whitespace collapsed).

All functions are deterministic, pure where possible, and raise explicit
exceptions on failure.  No live-trading logic is included.
"""
from __future__ import annotations

import logging
import os
import time
from datetime import UTC, datetime
from typing import Any

import requests
from reddit_ingestor.normalizer import normalize_text

# ---------------------------------------------------------------------------
# Structured JSON logging
# ---------------------------------------------------------------------------

_LOG_FMT = logging.Formatter(
    fmt='{"ts":"%(asctime)s","level":"%(levelname)s",'
    '"logger":"%(name)s","msg":"%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
_LOG_FMT.converter = time.gmtime  # always UTC

_handler = logging.StreamHandler()
_handler.setFormatter(_LOG_FMT)

logger = logging.getLogger("reddit_client")
logger.addHandler(_handler)
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------


class RedditAuthError(Exception):
    """Raised when OAuth2 token retrieval fails."""


class RedditApiError(Exception):
    """Raised on non-2xx responses from the Reddit API."""


class RedditRateLimitError(RedditApiError):
    """Raised when Reddit returns HTTP 429."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
_API_BASE = "https://oauth.reddit.com"
_TOKEN_MARGIN_SEC = 60  # refresh 60 s before real expiry


def ts_to_rfc3339(unix_ts: float) -> str:
    """Convert a UNIX timestamp to an RFC 3339 UTC string.

    >>> ts_to_rfc3339(1739788200.0)
    '2025-02-17T10:30:00Z'
    """
    return datetime.fromtimestamp(unix_ts, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalize_post(item: dict[str, Any], subreddit: str) -> dict[str, Any]:
    """Return a deterministic, normalised representation of a Reddit listing child."""
    full_name: str = item.get("name", "")
    post_id = full_name or item.get("id", "")
    created_utc = ts_to_rfc3339(float(item.get("created_utc", 0)))

    return {
        "source_id": f"reddit:{subreddit}:{post_id}",
        "subreddit": subreddit,
        "post_id": post_id,
        "title": normalize_text(item.get("title", "")),
        "body": normalize_text(item.get("selftext", "")),
        "created_at_utc": created_utc,
        "score": int(item.get("score", 0)),
        "num_comments": int(item.get("num_comments", 0)),
        "url": item.get("url", ""),
    }


# ---------------------------------------------------------------------------
# RedditClient
# ---------------------------------------------------------------------------


class RedditClient:
    """Synchronous Reddit API client with OAuth2 password-grant caching.

    Parameters
    ----------
    client_id, client_secret, username, password, user_agent:
        Reddit *script-type* app credentials.  Defaults fall back to
        the matching ``REDDIT_*`` environment variables.
    timeout:
        Per-request HTTP timeout in seconds.
    """

    def __init__(
        self,
        *,
        client_id: str | None = None,
        client_secret: str | None = None,
        username: str | None = None,
        password: str | None = None,
        user_agent: str | None = None,
        timeout: float = 15.0,
    ) -> None:
        self._client_id = client_id or os.environ.get("REDDIT_CLIENT_ID", "")
        self._client_secret = client_secret or os.environ.get("REDDIT_CLIENT_SECRET", "")
        self._username = username or os.environ.get("REDDIT_USERNAME", "")
        self._password = password or os.environ.get("REDDIT_PASSWORD", "")
        self._user_agent = user_agent or os.environ.get(
            "REDDIT_USER_AGENT", "awet-ingestor:v1.0 (by /u/awet_bot)"
        )
        self._timeout = timeout

        # Token cache
        self._access_token: str | None = None
        self._token_expires_at: float = 0.0

        # Persistent ``requests.Session`` for connection pooling
        self._session = requests.Session()
        self._session.headers["User-Agent"] = self._user_agent

    # ------------------------------------------------------------------
    # Token management
    # ------------------------------------------------------------------

    def get_access_token(self) -> str:
        """Return a valid bearer token, refreshing from Reddit if expired.

        Raises
        ------
        RedditAuthError
            On HTTP error or unexpected response body during token exchange.
        """
        if self._access_token and time.monotonic() < self._token_expires_at:
            return self._access_token

        logger.info("Requesting new OAuth2 token from Reddit")

        try:
            resp = self._session.post(
                _TOKEN_URL,
                auth=(self._client_id, self._client_secret),
                data={
                    "grant_type": "password",
                    "username": self._username,
                    "password": self._password,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=self._timeout,
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            msg = f"OAuth2 token request failed: {exc}"
            logger.error(msg)
            raise RedditAuthError(msg) from exc

        body = resp.json()
        if "access_token" not in body:
            msg = f"Unexpected token response: {body}"
            logger.error(msg)
            raise RedditAuthError(msg)

        self._access_token = body["access_token"]
        expires_in = int(body.get("expires_in", 3600))
        self._token_expires_at = time.monotonic() + expires_in - _TOKEN_MARGIN_SEC

        logger.info("OAuth2 token acquired (expires_in=%ds)", expires_in)
        return self._access_token

    # ------------------------------------------------------------------
    # Listing endpoints
    # ------------------------------------------------------------------

    def _fetch_listing(
        self, subreddit: str, sort: str, limit: int
    ) -> list[dict[str, Any]]:
        """Fetch a Reddit listing and return normalised post dicts.

        Raises
        ------
        RedditRateLimitError
            On HTTP 429.
        RedditApiError
            On other non-2xx responses.
        """
        token = self.get_access_token()
        url = f"{_API_BASE}/r/{subreddit}/{sort}.json"

        try:
            resp = self._session.get(
                url,
                params={"limit": min(max(limit, 1), 100), "raw_json": 1},
                headers={"Authorization": f"Bearer {token}"},
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            msg = f"Request to {url} failed: {exc}"
            logger.error(msg)
            raise RedditApiError(msg) from exc

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After", "?")
            msg = f"Rate limited on {url} (Retry-After: {retry_after})"
            logger.warning(msg)
            raise RedditRateLimitError(msg)

        if not resp.ok:
            msg = f"HTTP {resp.status_code} from {url}: {resp.text[:200]}"
            logger.error(msg)
            raise RedditApiError(msg)

        data = resp.json().get("data", {})
        posts: list[dict[str, Any]] = []
        for child in data.get("children", []):
            item = child.get("data", {})
            posts.append(_normalize_post(item, subreddit))

        logger.info(
            "Fetched %d posts from r/%s/%s", len(posts), subreddit, sort
        )
        return posts

    def fetch_subreddit_new(
        self, subreddit: str, limit: int = 25
    ) -> list[dict[str, Any]]:
        """Fetch the newest posts from *subreddit*."""
        return self._fetch_listing(subreddit, "new", limit)

    def fetch_subreddit_hot(
        self, subreddit: str, limit: int = 25
    ) -> list[dict[str, Any]]:
        """Fetch the hottest posts from *subreddit*."""
        return self._fetch_listing(subreddit, "hot", limit)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session.close()
        logger.info("RedditClient session closed")

    def __enter__(self) -> RedditClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
