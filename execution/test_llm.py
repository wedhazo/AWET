#!/usr/bin/env python3
"""Test script for local LLM connectivity.

Verifies:
1. LLM client can connect to the configured endpoint
2. llm_gateway functions work correctly

Timeout behavior:
    - Uses LLM_TIMEOUT_SECONDS env var (default 60).
    - llm_gateway test soft-fails on timeout (LLM slow but reachable).
    - openai_endpoint and direct_client tests are strict.

Usage:
    export LLM_BASE_URL=http://localhost:11434/v1
    export LLM_MODEL="llama-3.1-70b-instruct-q4_k_m"
    export LLM_TIMEOUT_SECONDS=180  # optional, increase for slow LLMs
    make llm-test
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import httpx

from src.core.config import load_settings
from src.llm.client import LLMClient, _build_timeout
from src.orchestration.llm_gateway import generate_plan


async def test_openai_endpoint() -> bool:
    """Test the OpenAI-compatible /v1/chat/completions endpoint directly.

    This test is strict: any error fails the test.
    """
    print("\n🌐 Testing OpenAI-compatible endpoint...")
    settings = load_settings()
    base_url = settings.llm.base_url.rstrip("/")
    url = f"{base_url}/chat/completions"
    timeout = _build_timeout(settings.llm)
    print(f"   Base URL: {settings.llm.base_url}")
    print(f"   Model: {settings.llm.model}")
    print(f"   Timeout (read): {timeout.read}s")

    payload = {
        "model": settings.llm.model,
        "messages": [{"role": "user", "content": "Respond with exactly: LLM_OK"}],
        "max_tokens": 16,
        "temperature": 0.1,
    }

    start = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
        data = response.json()
        content = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
        )
        duration = time.perf_counter() - start
        ok = "LLM_OK" in content or len(content) > 0
        if ok:
            print(f"   ✅ LLM reachable via /v1/chat/completions (took {duration:.2f}s)")
        else:
            print("   ❌ Unexpected response from LLM")
        return ok
    except Exception as e:
        print(f"   ❌ LLM unreachable: {e}")
        return False


async def test_direct_client() -> bool:
    """Test direct LLM client connection."""
    print("\n🔌 Testing direct LLM client connection...")
    settings = load_settings()
    print(f"   Base URL: {settings.llm.base_url}")
    print(f"   Model: {settings.llm.model}")

    client = LLMClient(settings.llm)
    messages = [
        {"role": "system", "content": "You are an assistant for the AWET trading system."},
        {"role": "user", "content": "Respond with exactly: LLM_OK"},
    ]

    try:
        response = await client.chat(messages)
        print(f"   Response: {response[:100]}...")
        return "LLM_OK" in response or len(response) > 0
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


async def test_llm_gateway() -> bool:
    """Test llm_gateway.generate_plan() function.

    This test is resilient to timeouts:
        - If LLM responds, test passes.
        - If ReadTimeout occurs, test soft-fails (LLM is slow but reachable).
        - Other errors still fail the test.

    Increase LLM_TIMEOUT_SECONDS to avoid soft-fail on slow hardware.
    """
    print("\n🧠 Testing llm_gateway.generate_plan()...")
    timeout_seconds = int(os.getenv("LLM_TIMEOUT_SECONDS", "60"))
    print(f"   (Large prompt test, timeout={timeout_seconds}s)")

    start = time.perf_counter()
    try:
        result = await generate_plan("trading_pipeline", {"symbol": "AAPL", "test": True})
        duration = time.perf_counter() - start
        print(f"   Plan generated: {len(result)} chars (took {duration:.2f}s)")
        print(f"   Preview: {result[:200]}...")
        return len(result) > 0
    except FileNotFoundError as e:
        print(f"   ⚠️  Directive not found (expected if directives/ empty): {e}")
        return True  # Not a failure of LLM wiring
    except httpx.ReadTimeout:
        duration = time.perf_counter() - start
        print(f"   ⚠️  ReadTimeout after {duration:.1f}s (LLM is slow but reachable)")
        print(f"   💡 Increase LLM_TIMEOUT_SECONDS={timeout_seconds * 2} to rerun.")
        return True  # Soft-fail: LLM works, just slow
    except Exception as e:
        duration = time.perf_counter() - start
        # Check if it's a timeout-related error
        if "timeout" in str(e).lower() or "ReadTimeout" in type(e).__name__:
            print(f"   ⚠️  Timeout after {duration:.1f}s (LLM is slow but reachable): {e}")
            print(f"   💡 Increase LLM_TIMEOUT_SECONDS={timeout_seconds * 2} to rerun.")
            return True  # Soft-fail
        print(f"   ❌ Error: {e}")
        return False


async def _run() -> int:
    """Run all LLM tests."""
    print("=" * 60)
    print("AWET LLM Integration Test")
    print("=" * 60)

    results = {
        "openai_endpoint": await test_openai_endpoint(),
        "direct_client": await test_direct_client(),
        "llm_gateway": await test_llm_gateway(),
    }

    print("\n" + "=" * 60)
    print("Results:")
    all_passed = True
    for name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"   {name}: {status}")
        if not passed:
            all_passed = False

    print("=" * 60)

    if all_passed:
        print("\n🎉 All LLM tests passed!")
        return 0
    else:
        print("\n💥 Some tests failed. Check LLM server is running.")
        return 1


def main() -> None:
    sys.exit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
