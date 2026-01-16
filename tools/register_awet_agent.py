#!/usr/bin/env python3
"""
Register or update the AWET Orchestrator agent in SuperAGI.

This script:
- Ensures required AWET tools are registered in SuperAGI
- Creates or updates the "AWET Orchestrator" agent via SuperAGI API
- Prints a summary with the agent ID and GUI URL

Environment Variables:
    SUPERAGI_BASE_URL: SuperAGI API base URL (default: http://localhost:3001/api)
    SUPERAGI_API_KEY: SuperAGI API key (required)
    TOOL_GATEWAY_URL: Tool Gateway URL (default: http://host.docker.internal:8200)
    SUPERAGI_PROJECT_ID: Optional project ID override
    SUPERAGI_MODEL: Optional model name override
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Any, Optional

from execution.register_superagi_tools import SuperAGIClient, register_all_tools

AGENT_NAME = "AWET Orchestrator"


@dataclass(frozen=True)
class AgentSpec:
    name: str
    description: str
    goals: list[str]
    instructions: list[str]
    constraints: list[str]
    tool_names: list[str]
    max_iterations: int = 25


AWET_ORCHESTRATOR = AgentSpec(
    name=AGENT_NAME,
    description="Orchestrates the AWET trading pipeline end-to-end.",
    goals=[
        "run backfill",
        "train TFT model",
        "promote latest model to green",
        "run the paper-trading demo",
    ],
    instructions=[
        "Only call the AWET tools (no arbitrary shell commands).",
        "Respect paper-trading only; never send real orders.",
    ],
    constraints=[
        "Paper trading only",
        "Use deterministic tools via the Tool Gateway",
    ],
    tool_names=[
        "awet_read_directive",
        "awet_run_backfill",
        "awet_train_model",
        "awet_promote_model",
        "awet_run_demo",
        "awet_check_pipeline_health",
    ],
)

AWET_NIGHT_TRAINER = AgentSpec(
    name="AWET Night Trainer",
    description="Runs nightly backfill and model training for AWET.",
    goals=[
        "run backfill for the last trading day",
        "train the TFT model on updated features",
        "promote the new model to green if training succeeds",
    ],
    instructions=[
        "Use only AWET tools (no arbitrary shell commands).",
        "If any tool fails, stop and report the error.",
    ],
    constraints=[
        "Paper trading only",
        "Use deterministic tools via the Tool Gateway",
    ],
    tool_names=[
        "awet_run_backfill",
        "awet_train_model",
        "awet_promote_model",
        "awet_check_pipeline_health",
    ],
)

AWET_MORNING_DEPLOYER = AgentSpec(
    name="AWET Morning Deployer",
    description="Runs morning health check and smoke tests before market open.",
    goals=[
        "check pipeline health at the start of the day",
        "verify a green model exists",
        "run a smoke test demo if health is OK",
    ],
    instructions=[
        "Use awet_check_pipeline_health first.",
        "Only if health is OK, run the demo as a smoke test.",
        "Do not change any risk limits or DB configuration.",
    ],
    constraints=[
        "Paper trading only",
        "Use deterministic tools via the Tool Gateway",
    ],
    tool_names=[
        "awet_check_pipeline_health",
        "awet_run_demo",
        "awet_read_directive",
    ],
)

AWET_TRADE_WATCHDOG = AgentSpec(
    name="AWET Trade Watchdog",
    description="Monitors pipeline health during market hours.",
    goals=[
        "periodically check pipeline health",
        "surface failures or lag in a structured summary",
    ],
    instructions=[
        "Only observe and report; never change state.",
        "If a health check fails, respond with a clear summary of what broke.",
    ],
    constraints=[
        "Paper trading only",
        "Use deterministic tools via the Tool Gateway",
    ],
    tool_names=[
        "awet_check_pipeline_health",
        "awet_read_directive",
    ],
)

AWET_AGENT_TEAM = [
    AWET_ORCHESTRATOR,
    AWET_NIGHT_TRAINER,
    AWET_MORNING_DEPLOYER,
    AWET_TRADE_WATCHDOG,
]


@dataclass(frozen=True)
class Settings:
    api_url: str
    api_key: str
    tool_gateway_url: str
    project_id: int
    model: Optional[str]


def _get_settings() -> Settings:
    api_url = os.getenv("SUPERAGI_BASE_URL") or os.getenv("SUPERAGI_API_URL") or "http://localhost:3001/api"
    api_key = os.getenv("SUPERAGI_API_KEY")
    if not api_key:
        raise SystemExit("SUPERAGI_API_KEY is required")
    tool_gateway_url = os.getenv("TOOL_GATEWAY_URL", "http://host.docker.internal:8200")
    project_id_raw = os.getenv("SUPERAGI_PROJECT_ID", "1")
    project_id = int(project_id_raw)
    model = os.getenv("SUPERAGI_MODEL")
    return Settings(
        api_url=api_url.rstrip("/"),
        api_key=api_key,
        tool_gateway_url=tool_gateway_url.rstrip("/"),
        project_id=project_id,
        model=model or "default",
    )


def _gui_url(api_url: str, agent_id: int) -> str:
    base = api_url.rstrip("/")
    if base.endswith("/api"):
        base = base[:-4]
    return f"{base}/agents/{agent_id}"


def _parse_agents(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("data", "agents"):
            if key in data and isinstance(data[key], list):
                return data[key]
    return []


def _request_first_success(
    client: SuperAGIClient,
    method: str,
    paths: list[str],
    payload: Optional[dict[str, Any]] = None,
) -> Optional[dict[str, Any]]:
    for path in paths:
        url = f"{client.api_url}{path}"
        response = client.session.request(
            method=method,
            url=url,
            json=payload,
            timeout=client.timeout,
        )
        if response.status_code in (200, 201):
            return response.json()
        if response.status_code == 404:
            continue
    return None


def _list_agents(client: SuperAGIClient, project_id: int) -> list[dict[str, Any]]:
    response = client.session.get(
        f"{client.api_url}/agents/get/project/{project_id}",
        timeout=client.timeout,
    )
    if response.status_code == 200:
        return _parse_agents(response.json())

    fallback = client.session.get(
        f"{client.api_url}/analytics/agents/all",
        timeout=client.timeout,
    )
    if fallback.status_code == 200:
        return _parse_agents(fallback.json())

    return []


def _build_agent_payload(agent: AgentSpec, tool_ids: list[int], settings: Settings) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "name": agent.name,
        "description": agent.description,
        "goal": agent.goals,
        "instruction": agent.instructions,
        "constraints": agent.constraints,
        "tools": tool_ids,
        "agent_workflow": "Goal Based Workflow",
        "toolkits": [],
        "exit": "manual",
        "iteration_interval": 0,
        "permission_type": "God Mode",
        "LTM_DB": "default",
        "max_iterations": agent.max_iterations,
    }
    payload["project_id"] = settings.project_id
    if settings.model:
        payload["model"] = settings.model
    return payload


def _resolve_tool_ids(tool_records: dict[str, dict[str, Any]]) -> dict[str, int]:
    return {name: int(record.get("id")) for name, record in tool_records.items() if record.get("id") is not None}


def _resolve_tool_objects(tool_records: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {name: record for name, record in tool_records.items() if record.get("id") is not None}


def register_agents(agent_specs: list[AgentSpec]) -> list[dict[str, Any]]:
    settings = _get_settings()

    # Ensure tools are registered first
    success, failed, tool_records = register_all_tools(
        superagi_url=settings.api_url,
        tool_gateway_url=settings.tool_gateway_url,
        api_key=settings.api_key,
    )
    if failed:
        raise SystemExit(f"Tool registration failed ({failed} failures)")

    client = SuperAGIClient(
        api_url=settings.api_url,
        tool_gateway_url=settings.tool_gateway_url,
        api_key=settings.api_key,
    )
    try:
        tool_ids_by_name = _resolve_tool_ids(tool_records)
        tool_objects_by_name = _resolve_tool_objects(tool_records)
        agents = _list_agents(client, settings.project_id)
        results: list[dict[str, Any]] = []

        for agent in agent_specs:
            missing = [name for name in agent.tool_names if name not in tool_ids_by_name]
            if missing:
                raise SystemExit(f"Missing tools in SuperAGI: {missing}")

            tool_ids = [tool_ids_by_name[name] for name in agent.tool_names]
            tool_objects = [tool_objects_by_name[name] for name in agent.tool_names]
            payload = _build_agent_payload(agent, tool_ids, settings)

            existing = next((a for a in agents if a.get("name") == agent.name), None)

            if existing and existing.get("id"):
                agent_id = int(existing["id"])
                update_payload = {
                    "name": agent.name,
                    "description": agent.description,
                    "goal": agent.goals,
                    "instruction": agent.instructions,
                    "constraints": agent.constraints,
                    "tools": tool_objects,
                    "agent_workflow": "Goal Based Workflow",
                    "permission_type": "God Mode",
                    "iteration_interval": 0,
                    "model": settings.model,
                    "max_iterations": agent.max_iterations,
                }
                result = _request_first_success(client, "PUT", [f"/v1/agent/{agent_id}"], update_payload)
                if result is None:
                    delete_response = client.session.put(
                        f"{client.api_url}/agents/delete/{agent_id}",
                        timeout=client.timeout,
                    )
                    if delete_response.status_code not in (200, 204):
                        raise SystemExit("Failed to update existing agent via SuperAGI API")
                    create_paths = ["/agents/create", "/agents"]
                    result = _request_first_success(client, "POST", create_paths, payload)
                    if result is None:
                        raise SystemExit("Failed to create agent via SuperAGI API")
                    agent_id = int(result.get("id") or result.get("agent_id"))
                    status = "recreated"
                else:
                    status = "updated"
            else:
                create_paths = ["/agents/create", "/agents"]
                result = _request_first_success(client, "POST", create_paths, payload)
                if result is None:
                    raise SystemExit("Failed to create agent via SuperAGI API")
                agent_id = int(result.get("id") or result.get("agent_id"))
                status = "created"

            print("=" * 60)
            print(f"✅ {agent.name} agent {status} successfully")
            print("=" * 60)
            print(f"Agent ID: {agent_id}")
            print(f"Name: {agent.name}")
            print(f"Tools assigned: {', '.join(agent.tool_names)}")
            print(f"Open in GUI: {_gui_url(settings.api_url, agent_id)}")

            results.append(
                {
                    "agent_id": agent_id,
                    "name": agent.name,
                    "tools": agent.tool_names,
                    "status": status,
                }
            )

        return results
    finally:
        client.close()


def register_awet_agent() -> dict[str, Any]:
    result = register_agents([AWET_ORCHESTRATOR])
    return result[0]


def main() -> None:
    try:
        register_awet_agent()
    except SystemExit:
        raise
    except Exception as exc:
        raise SystemExit(f"Registration failed: {exc}") from exc


if __name__ == "__main__":
    main()
