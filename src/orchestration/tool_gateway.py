"""
AWET Tool Gateway - HTTP endpoints for SuperAGI integration

This module exposes AWET's deterministic execution scripts as REST endpoints
that SuperAGI can call. Each endpoint wraps an existing script in execution/
or src/agents/ and returns structured JSON responses.

Architecture:
- Layer 1: Directives (read-only instructions)
- Layer 2: Orchestration (SuperAGI calls these endpoints)
- Layer 3: Execution (this gateway calls deterministic scripts)

All endpoints:
- Accept small JSON payloads with validated inputs
- Return small JSON payloads with status/summary
- Use structured logging with correlation_id
- Are idempotent where possible
"""
from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

import asyncpg
import structlog
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram

from src.core.config import load_settings
from src.core.logging import configure_logging, get_correlation_id, set_correlation_id
from src.monitoring.metrics import REGISTRY, metrics_response

# ============================================
# Prometheus Metrics
# ============================================

TOOL_REQUESTS_TOTAL = Counter(
    "tool_gateway_requests_total",
    "Total tool gateway requests",
    ["tool"],
    registry=REGISTRY,
)

TOOL_ERRORS_TOTAL = Counter(
    "tool_gateway_errors_total",
    "Total tool gateway errors",
    ["tool"],
    registry=REGISTRY,
)

TOOL_LATENCY_SECONDS = Histogram(
    "tool_gateway_latency_seconds",
    "Tool gateway latency in seconds",
    ["tool"],
    registry=REGISTRY,
)

# ============================================
# Request/Response Models
# ============================================


class ToolResponse(BaseModel):
    """Standard response from all tool endpoints."""
    success: bool
    correlation_id: str
    tool: str
    message: str
    data: Optional[dict[str, Any]] = None
    duration_seconds: float = 0.0


class BackfillRequest(BaseModel):
    """Request for backfill operations."""
    source: str = Field(..., description="Data source: 'polygon', 'reddit', or 'yfinance'")
    symbols: str = Field(default="AAPL,MSFT,NVDA", description="Comma-separated symbols")
    start_date: Optional[str] = Field(None, description="Start date YYYY-MM-DD")
    end_date: Optional[str] = Field(None, description="End date YYYY-MM-DD")
    days: int = Field(default=30, description="Number of days (for yfinance)")
    dry_run: bool = Field(default=False, description="Preview without publishing")


class TrainModelRequest(BaseModel):
    """Request for model training."""
    symbols: str = Field(default="AAPL,MSFT,NVDA", description="Comma-separated symbols")
    lookback_days: int = Field(default=30, description="Historical lookback window")
    epochs: int = Field(default=100, description="Training epochs")


class PromoteModelRequest(BaseModel):
    """Request for model promotion."""
    model_id: str = Field(..., description="Model ID to promote (e.g., tft_20260115_123456_abc123)")


class AuditQueryRequest(BaseModel):
    """Request for audit trail queries."""
    event_type: Optional[str] = Field(None, description="Filter by event type")
    symbol: Optional[str] = Field(None, description="Filter by symbol")
    correlation_id: Optional[str] = Field(None, description="Filter by correlation ID")
    limit: int = Field(default=50, description="Max events to return")
    hours_back: int = Field(default=24, description="Hours to look back")


class LiveIngestionRequest(BaseModel):
    """Request for live ingestion."""
    symbols: str = Field(default="", description="Comma-separated symbols (uses config default if empty)")
    trigger_pipeline: bool = Field(default=False, description="Trigger full pipeline after ingestion")
    dry_run: bool = Field(default=False, description="Preview without publishing")


class ReadDirectiveRequest(BaseModel):
    """Request for reading a directive."""
    directive_name: str = Field(
        ..., description="Directive file name (e.g., trading_pipeline.md)"
    )


# ============================================
# Application Setup
# ============================================

settings = load_settings()
configure_logging(settings.logging.level)
logger = structlog.get_logger(__name__)

app = FastAPI(
    title="AWET Tool Gateway",
    description="HTTP endpoints for SuperAGI to call AWET deterministic scripts",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _project_root() -> Path:
    """Get project root directory."""
    return Path(__file__).resolve().parents[2]


def _run_script(
    cmd: list[str],
    tool_name: str,
    timeout: int = 300,
    cwd: Optional[Path] = None,
) -> tuple[bool, str, str]:
    """
    Run a deterministic script and capture output.
    
    Returns (success, stdout, stderr).
    """
    start = datetime.now(timezone.utc)
    cwd = cwd or _project_root()
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(cwd),
            env={**os.environ, "PYTHONPATH": str(cwd)},
        )
        
        success = result.returncode == 0
        return success, result.stdout[-5000:], result.stderr[-2000:]
        
    except subprocess.TimeoutExpired:
        return False, "", f"Timeout after {timeout}s"
    except Exception as e:
        return False, "", str(e)


async def _get_db_pool() -> asyncpg.Pool:
    """Get database connection pool."""
    dsn = (
        f"postgresql://{os.getenv('POSTGRES_USER', 'awet')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'awet')}@"
        f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5433')}/"
        f"{os.getenv('POSTGRES_DB', 'awet')}"
    )
    return await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5)


# ============================================
# Health & Metrics Endpoints
# ============================================


@app.get("/health")
async def health() -> dict[str, Any]:
    """Health check endpoint."""
    return {"status": "ok", "service": "tool-gateway", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/metrics")
def metrics():
    """Prometheus metrics endpoint."""
    return metrics_response()


# ============================================
# Tool Endpoints
# ============================================


@app.post("/tools/run_backfill", response_model=ToolResponse)
async def run_backfill(request: BackfillRequest) -> ToolResponse:
    """
    Run a data backfill operation.
    
    Supports:
    - polygon: Historical market data from Polygon API
    - reddit: Historical sentiment data from Reddit archives
    - yfinance: Free daily data from Yahoo Finance
    """
    correlation_id = str(uuid4())
    set_correlation_id(correlation_id)
    start = datetime.now(timezone.utc)
    tool_name = "run_backfill"
    
    TOOL_REQUESTS_TOTAL.labels(tool_name).inc()
    logger.info("tool_request", tool=tool_name, source=request.source, symbols=request.symbols)
    
    try:
        valid_sources = ["polygon", "reddit", "yfinance"]
        if request.source.lower() not in valid_sources:
            raise HTTPException(400, f"Invalid source '{request.source}'. Must be one of: {valid_sources}")
        
        root = _project_root()
        
        if request.source.lower() == "polygon":
            cmd = [
                sys.executable, "-m", "execution.backfill_polygon",
                "--symbols", request.symbols,
            ]
            if request.start_date:
                cmd.extend(["--start", request.start_date])
            if request.end_date:
                cmd.extend(["--end", request.end_date])
            if request.dry_run:
                cmd.append("--dry-run")
            timeout = 3600
            
        elif request.source.lower() == "reddit":
            cmd = [
                sys.executable, "-m", "execution.backfill_reddit",
                "--symbols", request.symbols,
            ]
            if request.dry_run:
                cmd.append("--dry-run")
            timeout = 3600
            
        else:  # yfinance
            cmd = [
                sys.executable, "-m", "execution.yfinance_backfill",
                "--symbols", request.symbols,
                "--days", str(request.days),
            ]
            if request.dry_run:
                cmd.append("--dry-run")
            timeout = 300
        
        success, stdout, stderr = _run_script(cmd, tool_name, timeout=timeout)
        
        duration = (datetime.now(timezone.utc) - start).total_seconds()
        TOOL_LATENCY_SECONDS.labels(tool_name).observe(duration)
        
        if not success:
            TOOL_ERRORS_TOTAL.labels(tool_name).inc()
            
        return ToolResponse(
            success=success,
            correlation_id=correlation_id,
            tool=tool_name,
            message=f"Backfill {'completed' if success else 'failed'} for {request.source}",
            data={
                "source": request.source,
                "symbols": request.symbols,
                "dry_run": request.dry_run,
                "stdout_tail": stdout[-1000:] if stdout else "",
                "stderr_tail": stderr[-500:] if stderr else "",
            },
            duration_seconds=duration,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        TOOL_ERRORS_TOTAL.labels(tool_name).inc()
        logger.exception("tool_error", tool=tool_name, error=str(e))
        raise HTTPException(500, str(e))


@app.post("/tools/read_directive", response_model=ToolResponse)
async def read_directive(request: ReadDirectiveRequest) -> ToolResponse:
    """Read a directive document from the directives folder."""
    correlation_id = str(uuid4())
    set_correlation_id(correlation_id)
    start = datetime.now(timezone.utc)
    tool_name = "read_directive"

    TOOL_REQUESTS_TOTAL.labels(tool_name).inc()
    logger.info("tool_request", tool=tool_name, directive=request.directive_name)

    try:
        directive_name = request.directive_name.strip()
        if not directive_name:
            raise HTTPException(400, "Directive name is required")
        if ".." in directive_name or directive_name.startswith("/"):
            raise HTTPException(400, "Invalid directive name")
        if not directive_name.endswith(".md"):
            directive_name = f"{directive_name}.md"

        directives_path = _project_root() / "directives"
        file_path = directives_path / directive_name

        if not file_path.exists():
            available = []
            if directives_path.exists():
                available = sorted([p.name for p in directives_path.glob("*.md")])
            duration = (datetime.now(timezone.utc) - start).total_seconds()
            TOOL_LATENCY_SECONDS.labels(tool_name).observe(duration)
            TOOL_ERRORS_TOTAL.labels(tool_name).inc()
            return ToolResponse(
                success=False,
                correlation_id=correlation_id,
                tool=tool_name,
                message=f"Directive '{directive_name}' not found",
                data={"available": available},
                duration_seconds=duration,
            )

        content = file_path.read_text(encoding="utf-8")

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        TOOL_LATENCY_SECONDS.labels(tool_name).observe(duration)

        return ToolResponse(
            success=True,
            correlation_id=correlation_id,
            tool=tool_name,
            message=f"Read directive '{directive_name}'",
            data={"directive_name": directive_name, "content": content},
            duration_seconds=duration,
        )

    except HTTPException:
        raise
    except Exception as e:
        TOOL_ERRORS_TOTAL.labels(tool_name).inc()
        logger.exception("tool_error", tool=tool_name, error=str(e))
        raise HTTPException(500, str(e))


@app.post("/tools/train_model", response_model=ToolResponse)
async def train_model(request: TrainModelRequest) -> ToolResponse:
    """
    Train a new TFT model.
    
    The model will be registered with 'yellow' status, pending promotion.
    """
    correlation_id = str(uuid4())
    set_correlation_id(correlation_id)
    start = datetime.now(timezone.utc)
    tool_name = "train_model"
    
    TOOL_REQUESTS_TOTAL.labels(tool_name).inc()
    logger.info("tool_request", tool=tool_name, symbols=request.symbols, epochs=request.epochs)
    
    try:
        cmd = [
            sys.executable, "-m", "src.ml.train",
            "train",
            "--tickers", request.symbols,
            "--lookback-days", str(request.lookback_days),
            "--epochs", str(request.epochs),
        ]
        
        success, stdout, stderr = _run_script(cmd, tool_name, timeout=7200)  # 2 hour timeout
        
        duration = (datetime.now(timezone.utc) - start).total_seconds()
        TOOL_LATENCY_SECONDS.labels(tool_name).observe(duration)
        
        if not success:
            TOOL_ERRORS_TOTAL.labels(tool_name).inc()
        
        # Extract model ID from output if possible
        model_id = None
        for line in stdout.split("\n"):
            if "model_id" in line.lower() or "registered" in line.lower():
                model_id = line.strip()
                break
        
        return ToolResponse(
            success=success,
            correlation_id=correlation_id,
            tool=tool_name,
            message=f"Training {'completed' if success else 'failed'}",
            data={
                "symbols": request.symbols,
                "epochs": request.epochs,
                "model_id": model_id,
                "stdout_tail": stdout[-1500:] if stdout else "",
                "stderr_tail": stderr[-500:] if stderr else "",
            },
            duration_seconds=duration,
        )
        
    except Exception as e:
        TOOL_ERRORS_TOTAL.labels(tool_name).inc()
        logger.exception("tool_error", tool=tool_name, error=str(e))
        raise HTTPException(500, str(e))


@app.post("/tools/promote_model", response_model=ToolResponse)
async def promote_model(request: PromoteModelRequest) -> ToolResponse:
    """
    Promote a model to 'green' (production) status.
    
    The model will be used for live predictions after promotion.
    """
    correlation_id = str(uuid4())
    set_correlation_id(correlation_id)
    start = datetime.now(timezone.utc)
    tool_name = "promote_model"
    
    TOOL_REQUESTS_TOTAL.labels(tool_name).inc()
    logger.info("tool_request", tool=tool_name, model_id=request.model_id)
    
    try:
        cmd = [
            sys.executable, "-m", "src.ml.train",
            "promote",
            "--model-id", request.model_id,
        ]
        
        success, stdout, stderr = _run_script(cmd, tool_name, timeout=60)
        
        duration = (datetime.now(timezone.utc) - start).total_seconds()
        TOOL_LATENCY_SECONDS.labels(tool_name).observe(duration)
        
        if not success:
            TOOL_ERRORS_TOTAL.labels(tool_name).inc()
        
        return ToolResponse(
            success=success,
            correlation_id=correlation_id,
            tool=tool_name,
            message=f"Model {request.model_id} {'promoted to green' if success else 'promotion failed'}",
            data={
                "model_id": request.model_id,
                "stdout": stdout,
                "stderr": stderr if stderr else None,
            },
            duration_seconds=duration,
        )
        
    except Exception as e:
        TOOL_ERRORS_TOTAL.labels(tool_name).inc()
        logger.exception("tool_error", tool=tool_name, error=str(e))
        raise HTTPException(500, str(e))


@app.post("/tools/run_demo", response_model=ToolResponse)
async def run_demo() -> ToolResponse:
    """
    Run the end-to-end pipeline demo.
    
    Generates synthetic data and verifies the full message flow through all agents.
    """
    correlation_id = str(uuid4())
    set_correlation_id(correlation_id)
    start = datetime.now(timezone.utc)
    tool_name = "run_demo"
    
    TOOL_REQUESTS_TOTAL.labels(tool_name).inc()
    logger.info("tool_request", tool=tool_name)
    
    try:
        cmd = [sys.executable, "-m", "execution.demo"]
        
        success, stdout, stderr = _run_script(cmd, tool_name, timeout=300)
        
        duration = (datetime.now(timezone.utc) - start).total_seconds()
        TOOL_LATENCY_SECONDS.labels(tool_name).observe(duration)
        
        if not success:
            TOOL_ERRORS_TOTAL.labels(tool_name).inc()
        
        # Parse demo output for summary
        passed = "passed" in stdout.lower() or "✅" in stdout
        
        return ToolResponse(
            success=success and passed,
            correlation_id=correlation_id,
            tool=tool_name,
            message="Demo PASSED - Pipeline is working" if (success and passed) else "Demo FAILED",
            data={
                "passed": passed,
                "stdout_tail": stdout[-2000:] if stdout else "",
                "stderr_tail": stderr[-500:] if stderr else "",
            },
            duration_seconds=duration,
        )
        
    except Exception as e:
        TOOL_ERRORS_TOTAL.labels(tool_name).inc()
        logger.exception("tool_error", tool=tool_name, error=str(e))
        raise HTTPException(500, str(e))


@app.get("/tools/check_pipeline_health", response_model=ToolResponse)
async def check_pipeline_health() -> ToolResponse:
    """
    Check health of all pipeline agents.
    
    Queries /health endpoint of each agent and reports status.
    """
    import httpx
    
    correlation_id = str(uuid4())
    set_correlation_id(correlation_id)
    start = datetime.now(timezone.utc)
    tool_name = "check_pipeline_health"
    
    TOOL_REQUESTS_TOTAL.labels(tool_name).inc()
    logger.info("tool_request", tool=tool_name)
    
    agents = [
        ("data_ingestion", "http://localhost:8001/health"),
        ("feature_engineering", "http://localhost:8002/health"),
        ("prediction", "http://localhost:8003/health"),
        ("risk", "http://localhost:8004/health"),
        ("execution", "http://localhost:8005/health"),
        ("watchtower", "http://localhost:8006/health"),
    ]
    
    results = {}
    healthy_count = 0
    
    async with httpx.AsyncClient(timeout=5.0) as client:
        for name, url in agents:
            try:
                response = await client.get(url)
                if response.status_code == 200:
                    results[name] = {"status": "healthy", "code": 200}
                    healthy_count += 1
                else:
                    results[name] = {"status": "unhealthy", "code": response.status_code}
            except Exception as e:
                results[name] = {"status": "unreachable", "error": type(e).__name__}
    
    duration = (datetime.now(timezone.utc) - start).total_seconds()
    TOOL_LATENCY_SECONDS.labels(tool_name).observe(duration)
    
    all_healthy = healthy_count == len(agents)
    
    return ToolResponse(
        success=all_healthy,
        correlation_id=correlation_id,
        tool=tool_name,
        message=f"{healthy_count}/{len(agents)} agents healthy",
        data={
            "agents": results,
            "healthy_count": healthy_count,
            "total_count": len(agents),
        },
        duration_seconds=duration,
    )


@app.get("/tools/check_kafka_lag", response_model=ToolResponse)
async def check_kafka_lag() -> ToolResponse:
    """
    Check Kafka consumer lag for all pipeline consumer groups.
    """
    correlation_id = str(uuid4())
    set_correlation_id(correlation_id)
    start = datetime.now(timezone.utc)
    tool_name = "check_kafka_lag"
    
    TOOL_REQUESTS_TOTAL.labels(tool_name).inc()
    logger.info("tool_request", tool=tool_name)
    
    consumer_groups = [
        "data-ingestion",
        "feature-engineering",
        "prediction",
        "risk",
        "execution",
        "watchtower",
    ]
    
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    results = {}
    total_lag = 0
    
    for group in consumer_groups:
        cmd = [
            "docker", "exec", "kafka",
            "kafka-consumer-groups",
            "--bootstrap-server", "localhost:9092",
            "--describe",
            "--group", group,
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                # Parse lag from output
                lines = result.stdout.strip().split("\n")
                group_lag = 0
                for line in lines[1:]:  # Skip header
                    parts = line.split()
                    if len(parts) >= 6 and parts[5].isdigit():
                        group_lag += int(parts[5])
                results[group] = {"status": "ok", "lag": group_lag}
                total_lag += group_lag
            else:
                results[group] = {"status": "not_found", "lag": 0}
        except Exception as e:
            results[group] = {"status": "error", "error": str(e)}
    
    duration = (datetime.now(timezone.utc) - start).total_seconds()
    TOOL_LATENCY_SECONDS.labels(tool_name).observe(duration)
    
    return ToolResponse(
        success=True,
        correlation_id=correlation_id,
        tool=tool_name,
        message=f"Total lag: {total_lag} messages",
        data={
            "groups": results,
            "total_lag": total_lag,
        },
        duration_seconds=duration,
    )


@app.post("/tools/query_audit_trail", response_model=ToolResponse)
async def query_audit_trail(request: AuditQueryRequest) -> ToolResponse:
    """
    Query the audit trail in TimescaleDB.
    
    Returns recent events that have flowed through the pipeline.
    """
    correlation_id = str(uuid4())
    set_correlation_id(correlation_id)
    start = datetime.now(timezone.utc)
    tool_name = "query_audit_trail"
    
    TOOL_REQUESTS_TOTAL.labels(tool_name).inc()
    logger.info("tool_request", tool=tool_name, event_type=request.event_type, symbol=request.symbol)
    
    try:
        pool = await _get_db_pool()
        
        query = """
            SELECT event_type, symbol, ts, correlation_id, 
                   payload->>'source' as source
            FROM audit_events
            WHERE ts > NOW() - INTERVAL '%s hours'
        """
        params = [request.hours_back]
        
        conditions = []
        if request.event_type:
            conditions.append(f"event_type = ${len(params) + 1}")
            params.append(request.event_type)
        if request.symbol:
            conditions.append(f"symbol = ${len(params) + 1}")
            params.append(request.symbol)
        if request.correlation_id:
            conditions.append(f"correlation_id = ${len(params) + 1}")
            params.append(request.correlation_id)
        
        if conditions:
            query += " AND " + " AND ".join(conditions)
        
        query += f" ORDER BY ts DESC LIMIT ${len(params) + 1}"
        params.append(request.limit)
        
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
        
        events = []
        for row in rows:
            events.append({
                "event_type": row["event_type"],
                "symbol": row["symbol"],
                "ts": row["ts"].isoformat() if row["ts"] else None,
                "correlation_id": row["correlation_id"],
                "source": row["source"],
            })
        
        await pool.close()
        
        duration = (datetime.now(timezone.utc) - start).total_seconds()
        TOOL_LATENCY_SECONDS.labels(tool_name).observe(duration)
        
        return ToolResponse(
            success=True,
            correlation_id=correlation_id,
            tool=tool_name,
            message=f"Found {len(events)} events",
            data={
                "events": events,
                "count": len(events),
                "filters": {
                    "event_type": request.event_type,
                    "symbol": request.symbol,
                    "hours_back": request.hours_back,
                },
            },
            duration_seconds=duration,
        )
        
    except Exception as e:
        TOOL_ERRORS_TOTAL.labels(tool_name).inc()
        logger.exception("tool_error", tool=tool_name, error=str(e))
        raise HTTPException(500, str(e))


@app.post("/tools/approve_execution", response_model=ToolResponse)
async def approve_execution() -> ToolResponse:
    """
    Approve trade execution by creating the approval gate file.
    
    This allows the ExecutionAgent to process paper trades.
    """
    correlation_id = str(uuid4())
    set_correlation_id(correlation_id)
    start = datetime.now(timezone.utc)
    tool_name = "approve_execution"
    
    TOOL_REQUESTS_TOTAL.labels(tool_name).inc()
    logger.info("tool_request", tool=tool_name)
    
    try:
        approval_path = _project_root() / ".tmp" / "APPROVE_EXECUTION"
        approval_path.parent.mkdir(parents=True, exist_ok=True)
        approval_path.touch()
        
        duration = (datetime.now(timezone.utc) - start).total_seconds()
        TOOL_LATENCY_SECONDS.labels(tool_name).observe(duration)
        
        logger.info("execution_approved", correlation_id=correlation_id)
        
        return ToolResponse(
            success=True,
            correlation_id=correlation_id,
            tool=tool_name,
            message="✅ Execution APPROVED - trades will now be processed",
            data={"approval_file": str(approval_path), "exists": True},
            duration_seconds=duration,
        )
        
    except Exception as e:
        TOOL_ERRORS_TOTAL.labels(tool_name).inc()
        logger.exception("tool_error", tool=tool_name, error=str(e))
        raise HTTPException(500, str(e))


@app.post("/tools/revoke_execution", response_model=ToolResponse)
async def revoke_execution() -> ToolResponse:
    """
    Revoke trade execution by removing the approval gate file.
    
    All trades will be blocked and sent to execution.blocked topic.
    """
    correlation_id = str(uuid4())
    set_correlation_id(correlation_id)
    start = datetime.now(timezone.utc)
    tool_name = "revoke_execution"
    
    TOOL_REQUESTS_TOTAL.labels(tool_name).inc()
    logger.info("tool_request", tool=tool_name)
    
    try:
        approval_path = _project_root() / ".tmp" / "APPROVE_EXECUTION"
        
        existed = approval_path.exists()
        if existed:
            approval_path.unlink()
        
        duration = (datetime.now(timezone.utc) - start).total_seconds()
        TOOL_LATENCY_SECONDS.labels(tool_name).observe(duration)
        
        logger.info("execution_revoked", correlation_id=correlation_id)
        
        return ToolResponse(
            success=True,
            correlation_id=correlation_id,
            tool=tool_name,
            message="🚫 Execution REVOKED - trades will now be blocked",
            data={"approval_file": str(approval_path), "existed": existed},
            duration_seconds=duration,
        )
        
    except Exception as e:
        TOOL_ERRORS_TOTAL.labels(tool_name).inc()
        logger.exception("tool_error", tool=tool_name, error=str(e))
        raise HTTPException(500, str(e))


@app.post("/tools/run_live_ingestion", response_model=ToolResponse)
async def run_live_ingestion(request: LiveIngestionRequest) -> ToolResponse:
    """
    Run live daily ingestion from Yahoo Finance (FREE).
    
    Fetches latest OHLCV bars and publishes to Kafka.
    """
    correlation_id = str(uuid4())
    set_correlation_id(correlation_id)
    start = datetime.now(timezone.utc)
    tool_name = "run_live_ingestion"
    
    TOOL_REQUESTS_TOTAL.labels(tool_name).inc()
    logger.info("tool_request", tool=tool_name, symbols=request.symbols)
    
    try:
        cmd = [sys.executable, "-m", "execution.run_live_ingestion"]
        
        if request.symbols:
            cmd.extend(["--symbols", request.symbols])
        if request.trigger_pipeline:
            cmd.append("--trigger-pipeline")
        if request.dry_run:
            cmd.append("--dry-run")
        
        success, stdout, stderr = _run_script(cmd, tool_name, timeout=120)
        
        duration = (datetime.now(timezone.utc) - start).total_seconds()
        TOOL_LATENCY_SECONDS.labels(tool_name).observe(duration)
        
        if not success:
            TOOL_ERRORS_TOTAL.labels(tool_name).inc()
        
        return ToolResponse(
            success=success,
            correlation_id=correlation_id,
            tool=tool_name,
            message=f"Live ingestion {'completed' if success else 'failed'}",
            data={
                "symbols": request.symbols or "default",
                "dry_run": request.dry_run,
                "trigger_pipeline": request.trigger_pipeline,
                "stdout_tail": stdout[-1500:] if stdout else "",
                "stderr_tail": stderr[-500:] if stderr else "",
            },
            duration_seconds=duration,
        )
        
    except Exception as e:
        TOOL_ERRORS_TOTAL.labels(tool_name).inc()
        logger.exception("tool_error", tool=tool_name, error=str(e))
        raise HTTPException(500, str(e))


@app.get("/tools/list", response_model=dict)
async def list_tools() -> dict:
    """List all available tools with their descriptions."""
    return {
        "tools": [
            {
                "name": "run_backfill",
                "method": "POST",
                "path": "/tools/run_backfill",
                "description": "Run data backfill (polygon, reddit, or yfinance)",
            },
            {
                "name": "train_model",
                "method": "POST",
                "path": "/tools/train_model",
                "description": "Train a new TFT model",
            },
            {
                "name": "promote_model",
                "method": "POST",
                "path": "/tools/promote_model",
                "description": "Promote a model to green (production)",
            },
            {
                "name": "run_demo",
                "method": "POST",
                "path": "/tools/run_demo",
                "description": "Run end-to-end pipeline demo",
            },
            {
                "name": "check_pipeline_health",
                "method": "GET",
                "path": "/tools/check_pipeline_health",
                "description": "Check health of all pipeline agents",
            },
            {
                "name": "check_kafka_lag",
                "method": "GET",
                "path": "/tools/check_kafka_lag",
                "description": "Check Kafka consumer lag",
            },
            {
                "name": "query_audit_trail",
                "method": "POST",
                "path": "/tools/query_audit_trail",
                "description": "Query audit trail events",
            },
            {
                "name": "approve_execution",
                "method": "POST",
                "path": "/tools/approve_execution",
                "description": "Approve trade execution",
            },
            {
                "name": "revoke_execution",
                "method": "POST",
                "path": "/tools/revoke_execution",
                "description": "Revoke trade execution",
            },
            {
                "name": "run_live_ingestion",
                "method": "POST",
                "path": "/tools/run_live_ingestion",
                "description": "Run live daily ingestion from Yahoo Finance",
            },
        ]
    }


# ============================================
# Main Entry Point
# ============================================


def main():
    """Run the tool gateway server."""
    import uvicorn
    
    port = int(os.getenv("TOOL_GATEWAY_PORT", "8200"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")


if __name__ == "__main__":
    main()
