#!/usr/bin/env python3
"""Register the AWET SuperAGI agent team."""
from __future__ import annotations

from tools.register_awet_agent import AWET_AGENT_TEAM, register_agents


def main() -> None:
    register_agents(AWET_AGENT_TEAM)


if __name__ == "__main__":
    main()
