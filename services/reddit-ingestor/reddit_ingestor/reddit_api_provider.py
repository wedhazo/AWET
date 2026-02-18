"""Reddit API provider using httpx OAuth2 (script-app flow).

Uses Reddit's *password* grant (script-type app) so that no interactive
browser auth is needed.  Falls back gracefully with clear error messages
for missing credentials.

Why httpx instead of PRAW:
  • Async-native – no executor/thread pool needed.
  • Deterministic retry control (no hidden PRAW retry logic).
  • Lighter dependency footprint.

Token lifecycle:
  Tokens are requested lazily on first ``fetch_posts`` and refreshed
  automatically when they expire (default 1 h).
"""
from __future__ import annotations

import time
from datetime import UTC, datetime

import httpx
import structlog
from reddit_ingestor.provider import RawPost

logger = structlog.get_logger("reddit_ingestor.reddit_api")

_TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
_API_BASE = "https://oauth.reddit.com"
_TOKEN_MARGIN_SEC = 60  # refresh 60 s before expiry


class RedditApiProvider:
    """Async Reddit provider that talks to the real Reddit API.

    Parameters
    ----------
    client_id, client_secret, username, password:
        Reddit *script-type* app credentials.
    user_agent:
        Custom User-Agent string.  Reddit rate-limits on UA.
    post_limit:
        Max posts to fetch per subreddit per poll (25-100).
    timeout_sec:
        Per-request HTTP timeout.
    """

    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        username: str,
        password: str,
        user_agent: str = "awet-ingestor:v1.0 (by /u/awet_bot)",
        post_limit: int = 50,
        timeout_sec: float = 15.0,
    ) -> None:
        if not client_id or not client_secret:
            msg = (
                "RedditApiProvider requires REDDIT_CLIENT_ID and "
                "REDDIT_CLIENT_SECRET env vars"
            )
            raise ValueError(msg)

        self._client_id = client_id
        self._client_secret = client_secret
        self._username = username
        self._password = password
        self._user_agent = user_agent
        self._post_limit = min(max(post_limit, 1), 100)
        self._timeout = timeout_sec

        self._http: httpx.AsyncClient | None = None
        self._access_token: str | None = None
        self._token_expires_at: float = 0.0

    # ------------------------------------------------------------------
    # HTTP client lifecycle
    # ------------------------------------------------------------------

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                headers={"User-Agent": self._user_agent},
                timeout=self._timeout,
            )
        return self._http

    async def _ensure_token(self) -> str:
        """Return a valid OAuth bearer token, refreshing if needed."""
        if (
            self._access_token
            and time.monotonic() < self._token_expires_at
        ):
            return self._access_token

        client = self._ensure_client()
        resp = await client.post(
            _TOKEN_URL,
            auth=(self._client_id, self._client_secret),
            data={
                "grant_type": "password",
                "username": self._username,
                "password": self._password,
            },
            headers={
                "User-Agent": self._user_agent,
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        resp.raise_for_status()
        body = resp.json()
        self._access_token = body["access_token"]
        expires_in = int(body.get("expires_in", 3600))
        self._token_expires_at = time.monotonic() + expires_in - _TOKEN_MARGIN_SEC
        logger.info("reddit_token_acquired", expires_in=expires_in)
        return self._access_token  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_posts(self, subreddits: list[str]) -> list[RawPost]:
        """Fetch new/hot posts from each subreddit."""
        token = await self._ensure_token()
        client = self._ensure_client()
        client.headers["Authorization"] = f"Bearer {token}"

        posts: list[RawPost] = []
        for sub in subreddits:
            url = f"{_API_BASE}/r/{sub}/new.json"
            try:
                resp = await client.get(
                    url,
                    params={"limit": self._post_limit, "raw_json": 1},
                )
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                logger.error(
                    "reddit_api_error",
                    subreddit=sub,
                    error=str(exc),
                )
                continue

            data = resp.json().get("data", {})
            for child in data.get("children", []):
                item = child.get("data", {})
                posts.append(self._to_raw_post(item, sub))

            # Respect Reddit's 60-req/min rate limit
            # (caller controls poll interval; we just yield)

        logger.info("reddit_api_fetched", total=len(posts))
        return posts

    async def close(self) -> None:
        if self._http and not self._http.is_closed:
            await self._http.aclose()
            logger.info("reddit_http_client_closed")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _to_raw_post(item: dict, subreddit: str) -> RawPost:
        """Map a Reddit API listing child to our ``RawPost`` dataclass."""
        created_ts = float(item.get("created_utc", 0))
        created_dt = datetime.fromtimestamp(created_ts, tz=UTC)

        # Reddit uses 't3_' prefix for submissions, 't1_' for comments
        full_name: str = item.get("name", "")
        is_submission = full_name.startswith("t3_")
        post_id = full_name or item.get("id", "")

        return RawPost(
            subreddit=subreddit,
            post_id=post_id,
            comment_id=None if is_submission else full_name,
            author=item.get("author", "[deleted]"),
            title=item.get("title", ""),
            body=item.get("selftext", ""),
            url=item.get("url", ""),
            score=int(item.get("score", 0)),
            num_comments=int(item.get("num_comments", 0)),
            created_utc=created_dt,
            is_submission=is_submission,
        )
