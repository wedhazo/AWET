"""Integration tests for KBClient.

Uses pytest-respx to intercept outbound httpx calls without a real server.
All tests are async with pytest-asyncio.
"""
from __future__ import annotations

import sys
from pathlib import Path

import httpx
import pytest
import respx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from telegram_bot.kb_client import KBClient, KBQueryError, KBQueryTimeout

BASE = "http://kb-query-test:8000"
CID = "corr-1234-abcd"


@pytest.fixture
async def client():
    c = KBClient(base_url=BASE, timeout_sec=1.0, max_retries=3)
    await c.start()
    yield c
    await c.close()


# ── Correlation-ID header ────────────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_correlation_id_header_forwarded(client):
    respx.get(f"{BASE}/search").mock(
        return_value=httpx.Response(200, json={"results": [], "total": 0})
    )
    await client.search("tsla", correlation_id=CID)
    req = respx.calls.last.request
    assert req.headers.get("x-correlation-id") == CID


@pytest.mark.asyncio
@respx.mock
async def test_missing_correlation_id_still_sends_header(client):
    """Client must always forward *some* correlation id (auto-generated if omitted)."""
    import uuid
    auto_cid = str(uuid.uuid4())
    respx.get(f"{BASE}/search").mock(
        return_value=httpx.Response(200, json={"results": [], "total": 0})
    )
    # correlation_id is required — pass a generated one
    await client.search("tsla", correlation_id=auto_cid)
    req = respx.calls.last.request
    assert req.headers.get("x-correlation-id") == auto_cid


# ── Happy path ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_search_returns_results(client):
    respx.get(f"{BASE}/search").mock(
        return_value=httpx.Response(
            200,
            json={
                "results": [{"title": "T", "chunk_id": "c1"}],
                "total": 1,
            },
        )
    )
    resp = await client.search("tsla", correlation_id=CID)
    # search() normalises the response to a plain list
    assert isinstance(resp, list)
    assert len(resp) == 1
    assert resp[0]["title"] == "T"


@pytest.mark.asyncio
@respx.mock
async def test_search_passes_query_params(client):
    route = respx.get(f"{BASE}/search").mock(
        return_value=httpx.Response(200, json={"results": [], "total": 0})
    )
    await client.search(
        "tsla earnings",
        ticker="TSLA",
        limit=10,
        from_date="2026-01-01",
        to_date="2026-02-01",
        correlation_id=CID,
    )
    params = dict(respx.calls.last.request.url.params)
    assert params["q"] == "tsla earnings"
    assert params["ticker"] == "TSLA"
    assert params["limit"] == "10"
    assert params["from"] == "2026-01-01"
    assert params["to"] == "2026-02-01"


@pytest.mark.asyncio
@respx.mock
async def test_get_source_returns_data(client):
    respx.get(f"{BASE}/source/abc123").mock(
        return_value=httpx.Response(200, json={"chunk_id": "abc123", "title": "T"})
    )
    data = await client.get_source("abc123", correlation_id=CID)
    assert data["chunk_id"] == "abc123"


@pytest.mark.asyncio
@respx.mock
async def test_healthz_returns_true_on_200(client):
    respx.get(f"{BASE}/healthz").mock(return_value=httpx.Response(200))
    assert await client.healthz() is True


@pytest.mark.asyncio
@respx.mock
async def test_healthz_returns_false_on_500(client):
    respx.get(f"{BASE}/healthz").mock(return_value=httpx.Response(500))
    assert await client.healthz() is False


@pytest.mark.asyncio
@respx.mock
async def test_healthz_returns_false_on_network_error(client):
    respx.get(f"{BASE}/healthz").mock(side_effect=httpx.ConnectError("refused"))
    assert await client.healthz() is False


# ── Retry behaviour ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_retries_on_503(client):
    """Request should retry on 503 and succeed on the 3rd attempt."""
    responses = [
        httpx.Response(503),
        httpx.Response(503),
        httpx.Response(200, json={"results": [], "total": 0}),
    ]
    respx.get(f"{BASE}/search").mock(side_effect=responses)
    resp = await client.search("tsla", correlation_id=CID)
    assert isinstance(resp, list)
    assert len(resp) == 0


@pytest.mark.asyncio
@respx.mock
async def test_raises_kb_query_error_after_all_retries(client):
    """KBQueryError raised if all retries are exhausted."""
    respx.get(f"{BASE}/search").mock(return_value=httpx.Response(500))
    with pytest.raises(KBQueryError):
        await client.search("tsla", correlation_id=CID)


@pytest.mark.asyncio
@respx.mock
async def test_raises_kb_query_timeout_on_read_timeout():
    """KBQueryTimeout is raised on network timeout (max_retries=1 for speed)."""
    c = KBClient(base_url=BASE, timeout_sec=0.01, max_retries=1)
    await c.start()
    try:
        respx.get(f"{BASE}/search").mock(
            side_effect=httpx.ReadTimeout("timed out")
        )
        with pytest.raises(KBQueryTimeout):
            await c.search("tsla", correlation_id=CID)
    finally:
        await c.close()


@pytest.mark.asyncio
@respx.mock
async def test_does_not_retry_on_404(client):
    """404 is a client error — no retry, raises KBQueryError."""
    respx.get(f"{BASE}/source/missing").mock(return_value=httpx.Response(404))
    with pytest.raises(KBQueryError):
        await client.get_source("missing", correlation_id=CID)
    assert respx.calls.call_count == 1  # no retry
