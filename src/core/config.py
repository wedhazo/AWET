from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field


class AppHttpConfig(BaseModel):
    host: str
    data_ingestion_port: int
    feature_engineering_port: int
    prediction_port: int
    risk_port: int
    execution_port: int
    watchtower_port: int


class AppConfig(BaseModel):
    environment: str
    correlation_header: str
    http: AppHttpConfig
    execution_approval_file: str
    symbols: list[str]


class KafkaGroupIds(BaseModel):
    feature_engineering: str
    prediction: str
    risk: str
    execution: str
    watchtower: str


class KafkaConfig(BaseModel):
    bootstrap_servers: list[str]
    schema_registry_url: str
    auto_offset_reset: str
    group_ids: KafkaGroupIds


class LimitsConfig(BaseModel):
    """Risk limits configuration - matches config/limits.yaml schema."""
    # Position limits (can use either pct or absolute)
    max_position_pct: float = 0.02
    max_position: int = 1000  # Legacy/absolute fallback
    max_notional: int = 100000  # Max notional value per position
    max_daily_loss_pct: float = 0.05
    max_loss_per_trade: int = 1000  # Legacy/absolute fallback
    max_volatility: float = 0.10
    min_confidence: float = 0.3
    max_correlation_spike: float = 0.95
    kill_switch_loss_pct: float = 0.10


class PolygonConfig(BaseModel):
    base_url: str = "https://api.polygon.io"


class AlpacaConfig(BaseModel):
    base_url: str = "https://data.alpaca.markets"


class YFinanceConfig(BaseModel):
    """YFinance config (FREE - no API key required)."""
    rate_limit_per_minute: int = 30


class MarketDataConfig(BaseModel):
    provider: str = "yfinance"  # polygon, alpaca, or yfinance (free)
    timeframe: str = "1D"
    backfill_minutes: int = 60
    backfill_days: int = 30
    polling_interval_seconds: int = 60
    rate_limit_per_minute: int = 5
    request_timeout_seconds: float = 30.0
    polygon: PolygonConfig = Field(default_factory=PolygonConfig)
    alpaca: AlpacaConfig = Field(default_factory=AlpacaConfig)
    yfinance: YFinanceConfig = Field(default_factory=YFinanceConfig)


class LoggingConfig(BaseModel):
    level: str = Field(default="INFO")
    json_format: bool = Field(default=True, alias="json")


class DataConfig(BaseModel):
    """Configuration for historical data paths."""
    data_root: str = "/home/kironix/train"
    polygon_minute_dir: str = "poligon/Minute Aggregates"
    polygon_day_dir: str = "poligon/Day Aggregates"
    reddit_submissions_dir: str = "reddit/submissions"
    reddit_comments_dir: str = "reddit/comments"
    start_date: str | None = None
    end_date: str | None = None


class TrainingConfig(BaseModel):
    """Configuration for TFT model training."""
    lookback_window: int = 120
    horizons: list[int] = Field(default_factory=lambda: [30, 45, 60])
    batch_size: int = 64
    epochs: int = 50
    learning_rate: float = 0.001
    hidden_size: int = 64
    num_attention_heads: int = 4
    dropout: float = 0.1
    model_output_dir: str = "models"
    onnx_model_path: str = "models/tft_model.onnx"
    mlflow_tracking_uri: str = "http://localhost:5000"
    mlflow_experiment_name: str = "awet_tft"


class LLMConfig(BaseModel):
    provider: str = "local"
    base_url: str = "http://localhost:11434/v1"
    model: str = "llama-3.1-70b-instruct-q4_k_m"
    timeout_seconds: int = 60
    max_tokens: int = 4096
    temperature: float = 0.2
    top_p: float = 0.9
    concurrency_limit: int = 4


class Settings(BaseModel):
    app: AppConfig
    kafka: KafkaConfig
    limits: LimitsConfig
    logging: LoggingConfig
    market_data: MarketDataConfig = Field(default_factory=MarketDataConfig)
    data: DataConfig = Field(default_factory=DataConfig)
    training: TrainingConfig = Field(default_factory=TrainingConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _deep_set(data: dict[str, Any], keys: list[str], value: Any) -> None:
    target = data
    for key in keys[:-1]:
        if key not in target or not isinstance(target[key], dict):
            target[key] = {}
        target = target[key]
    target[keys[-1]] = value


def _apply_env_overrides(data: dict[str, Any]) -> dict[str, Any]:
    for key, value in os.environ.items():
        if "__" not in key:
            continue
        keys = [k.lower() for k in key.split("__")]
        _deep_set(data, keys, value)

    if "KAFKA_BOOTSTRAP_SERVERS" in os.environ:
        servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(",")
        _deep_set(data, ["kafka", "bootstrap_servers"], servers)
    if "SCHEMA_REGISTRY_URL" in os.environ:
        _deep_set(data, ["kafka", "schema_registry_url"], os.environ["SCHEMA_REGISTRY_URL"])
    if "LOG_LEVEL" in os.environ:
        _deep_set(data, ["logging", "level"], os.environ["LOG_LEVEL"])
    if "LLM_BASE_URL" in os.environ:
        _deep_set(data, ["llm", "base_url"], os.environ["LLM_BASE_URL"])
    if "LLM_MODEL" in os.environ:
        _deep_set(data, ["llm", "model"], os.environ["LLM_MODEL"])
    if "LLM_TIMEOUT_SECONDS" in os.environ:
        _deep_set(data, ["llm", "timeout_seconds"], os.environ["LLM_TIMEOUT_SECONDS"])
    if "LLM_MAX_TOKENS" in os.environ:
        _deep_set(data, ["llm", "max_tokens"], os.environ["LLM_MAX_TOKENS"])

    return data


def load_settings(config_dir: str = "config") -> Settings:
    load_dotenv()
    base = {}
    base.update(_load_yaml(Path(config_dir) / "app.yaml"))
    base.update(_load_yaml(Path(config_dir) / "kafka.yaml"))
    base.update(_load_yaml(Path(config_dir) / "limits.yaml"))
    base.update(_load_yaml(Path(config_dir) / "logging.yaml"))
    base.update(_load_yaml(Path(config_dir) / "market_data.yaml"))
    base.update(_load_yaml(Path(config_dir) / "llm.yaml"))
    merged = _apply_env_overrides(base)
    return Settings.model_validate(merged)
