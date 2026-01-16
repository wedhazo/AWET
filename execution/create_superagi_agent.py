#!/usr/bin/env python3
"""
Create AWET Orchestrator Agent in SuperAGI.

This script:
1. Creates the AWET toolkit if it doesn't exist
2. Registers all AWET tools
3. Creates the AWET Orchestrator agent
"""

import sys
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import json

# Database connection - works inside Docker
DB_URL = "postgresql://superagi:superagi@superagi-postgres:5432/superagi"

# AWET Tools definition
AWET_TOOLS = [
    {
        "name": "awet_read_directive",
        "description": "Read a directive (SOP) document from the directives folder. Directives contain instructions for executing trading pipeline tasks including goals, inputs, tools, outputs, and edge cases.",
        "folder_name": "awet"
    },
    {
        "name": "awet_run_backfill",
        "description": "Run historical data backfill from yfinance or alpha_vantage. Ingests OHLCV data into Kafka market.raw topic. Parameters: symbols (comma-separated), source, start_date, end_date.",
        "folder_name": "awet"
    },
    {
        "name": "awet_train_model",
        "description": "Train a Temporal Fusion Transformer (TFT) model on historical market data. Parameters: symbols, epochs (default 100). Returns run_id for model tracking.",
        "folder_name": "awet"
    },
    {
        "name": "awet_promote_model",
        "description": "Promote a trained model to green (production) deployment. Parameter: run_id from training. This makes the model active for predictions.",
        "folder_name": "awet"
    },
    {
        "name": "awet_approve_execution",
        "description": "Approve paper trading execution. Creates an approval token that allows ExecutionAgent to process trades. Parameter: reason for approval.",
        "folder_name": "awet"
    },
    {
        "name": "awet_revoke_execution",
        "description": "Revoke paper trading approval. Removes the approval token to stop trade execution. Parameter: reason for revocation.",
        "folder_name": "awet"
    },
    {
        "name": "awet_run_demo",
        "description": "Run a demo of the full trading pipeline. Simulates market events through ingestion, feature engineering, prediction, risk, and execution stages.",
        "folder_name": "awet"
    },
    {
        "name": "awet_check_pipeline_health",
        "description": "Check health status of all pipeline agents (IngestionAgent, FeatureAgent, PredictionAgent, RiskAgent, ExecutionAgent). Returns health status for each.",
        "folder_name": "awet"
    },
    {
        "name": "awet_check_kafka_lag",
        "description": "Check Kafka consumer lag for all pipeline consumer groups. Returns lag metrics and alerts if any exceed thresholds.",
        "folder_name": "awet"
    },
    {
        "name": "awet_query_audit_trail",
        "description": "Query the audit trail in TimescaleDB. Filter by time range, correlation_id, event_type, or symbol. Returns audit events for traceability.",
        "folder_name": "awet"
    },
    {
        "name": "awet_generate_plan",
        "description": "Use local Llama LLM to generate an execution plan from a directive. Reads the directive and produces step-by-step instructions.",
        "folder_name": "awet"
    },
    {
        "name": "awet_explain_run",
        "description": "Use local Llama LLM to explain a pipeline run. Takes correlation_id and produces human-readable explanation from audit trail.",
        "folder_name": "awet"
    },
    {
        "name": "awet_run_live_ingestion",
        "description": "Run live daily data ingestion from yfinance. Fetches today's market data for specified symbols and publishes to Kafka.",
        "folder_name": "awet"
    },
]


def create_awet_agent():
    """Create AWET toolkit, tools, and agent in SuperAGI database."""
    engine = create_engine(DB_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        now = datetime.utcnow()
        
        # 1. Create AWET Toolkit
        print("Creating AWET Toolkit...")
        result = session.execute(
            text("SELECT id FROM toolkits WHERE name = 'AWET Pipeline Tools'")
        )
        existing_toolkit = result.fetchone()
        
        if existing_toolkit:
            toolkit_id = existing_toolkit[0]
            print(f"  Toolkit already exists with id={toolkit_id}")
        else:
            session.execute(
                text("""
                    INSERT INTO toolkits (name, description, show_toolkit, organisation_id, created_at, updated_at)
                    VALUES ('AWET Pipeline Tools', 'Tools for orchestrating the AWET trading pipeline', true, 1, :now, :now)
                    RETURNING id
                """),
                {"now": now}
            )
            result = session.execute(text("SELECT id FROM toolkits WHERE name = 'AWET Pipeline Tools'"))
            toolkit_id = result.fetchone()[0]
            print(f"  Created toolkit with id={toolkit_id}")
        
        # 2. Register AWET Tools
        print("\nRegistering AWET Tools...")
        tool_ids = []
        for tool in AWET_TOOLS:
            result = session.execute(
                text("SELECT id FROM tools WHERE name = :name"),
                {"name": tool["name"]}
            )
            existing_tool = result.fetchone()
            
            if existing_tool:
                tool_ids.append(existing_tool[0])
                print(f"  Tool '{tool['name']}' already exists (id={existing_tool[0]})")
            else:
                session.execute(
                    text("""
                        INSERT INTO tools (name, description, folder_name, created_at, updated_at)
                        VALUES (:name, :description, :folder_name, :now, :now)
                    """),
                    {
                        "name": tool["name"],
                        "description": tool["description"],
                        "folder_name": tool["folder_name"],
                        "now": now
                    }
                )
                result = session.execute(
                    text("SELECT id FROM tools WHERE name = :name"),
                    {"name": tool["name"]}
                )
                tool_id = result.fetchone()[0]
                tool_ids.append(tool_id)
                print(f"  Registered tool '{tool['name']}' (id={tool_id})")
        
        # 3. Create AWET Orchestrator Agent
        print("\nCreating AWET Orchestrator Agent...")
        result = session.execute(
            text("SELECT id FROM agents WHERE name = 'AWET Orchestrator'")
        )
        existing_agent = result.fetchone()
        
        if existing_agent:
            agent_id = existing_agent[0]
            print(f"  Agent already exists with id={agent_id}")
        else:
            session.execute(
                text("""
                    INSERT INTO agents (name, project_id, description, created_at, updated_at)
                    VALUES ('AWET Orchestrator', 1, 'Autonomous agent for running the AWET trading pipeline', :now, :now)
                """),
                {"now": now}
            )
            result = session.execute(text("SELECT id FROM agents WHERE name = 'AWET Orchestrator'"))
            agent_id = result.fetchone()[0]
            print(f"  Created agent with id={agent_id}")
        
        # 4. Add Agent Configuration
        print("\nConfiguring agent...")
        
        # Define configurations
        configs = [
            ("goal", json.dumps([
                "Run the full AWET trading pipeline end-to-end",
                "Monitor pipeline health and report issues",
                "Ensure all steps complete successfully with proper error handling"
            ])),
            ("instruction", json.dumps([
                "Read directives to understand the task requirements",
                "Execute tools in sequence: backfill → train → promote → approve → demo",
                "Check pipeline health after operations",
                "Query audit trail to verify completion",
                "Report results clearly"
            ])),
            ("constraints", json.dumps([
                "Never execute real trades - paper trading only",
                "Always check pipeline health before critical operations",
                "Propagate correlation_id through all operations",
                "Stop on errors and report before continuing"
            ])),
            ("tools", json.dumps(tool_ids)),
            ("model", "gpt-4"),
            ("max_iterations", "25"),
            ("permission_type", "God Mode"),
            ("agent_workflow", "Goal Based Workflow"),
        ]
        
        for key, value in configs:
            # Check if config exists
            result = session.execute(
                text("SELECT id FROM agent_configurations WHERE agent_id = :agent_id AND key = :key"),
                {"agent_id": agent_id, "key": key}
            )
            existing_config = result.fetchone()
            
            if existing_config:
                session.execute(
                    text("UPDATE agent_configurations SET value = :value, updated_at = :now WHERE id = :id"),
                    {"value": value, "now": now, "id": existing_config[0]}
                )
                print(f"  Updated config: {key}")
            else:
                session.execute(
                    text("""
                        INSERT INTO agent_configurations (agent_id, key, value, created_at, updated_at)
                        VALUES (:agent_id, :key, :value, :now, :now)
                    """),
                    {"agent_id": agent_id, "key": key, "value": value, "now": now}
                )
                print(f"  Added config: {key}")
        
        session.commit()
        
        print("\n" + "="*60)
        print("✅ AWET Orchestrator Agent Created Successfully!")
        print("="*60)
        print(f"\n  Agent ID: {agent_id}")
        print(f"  Toolkit ID: {toolkit_id}")
        print(f"  Tools: {len(tool_ids)} registered")
        print(f"\n  View in SuperAGI UI: http://localhost:3001")
        print("  Go to Agents → AWET Orchestrator → Run")
        print("="*60)
        
        return agent_id
        
    except Exception as e:
        session.rollback()
        print(f"\n❌ Error: {e}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    create_awet_agent()
