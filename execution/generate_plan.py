#!/usr/bin/env python
"""Generate an execution plan using the local LLM.

Usage:
    python -m execution.generate_plan trading_pipeline
    python -m execution.generate_plan trading_pipeline --context '{"symbol": "AAPL"}'
"""
from __future__ import annotations

import argparse
import asyncio
import json


async def _run(directive_name: str, context: dict) -> None:
    from src.orchestration.llm_gateway import generate_plan

    print(f"Generating plan for directive: {directive_name}")
    print(f"Context: {json.dumps(context, indent=2)}")
    print("-" * 60)
    plan = await generate_plan(directive_name, context)
    print(plan)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate an execution plan using the local LLM")
    parser.add_argument("directive", help="Directive name (e.g., trading_pipeline)")
    parser.add_argument(
        "--context", "-c", default="{}", help="JSON context (e.g., '{\"symbol\": \"AAPL\"}')"
    )
    args = parser.parse_args()

    try:
        context = json.loads(args.context)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON context: {e}")
        return

    asyncio.run(_run(args.directive, context))


if __name__ == "__main__":
    main()
