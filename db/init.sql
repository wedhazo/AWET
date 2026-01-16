CREATE EXTENSION IF NOT EXISTS timescaledb;

-- =============================================================================
-- MARKET_RAW_MINUTE (Polygon minute OHLCV bars)
-- Columns from CSV: ticker,volume,open,close,high,low,window_start,transactions
-- =============================================================================
CREATE TABLE IF NOT EXISTS market_raw_minute (
  ticker TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  open NUMERIC(18,6) NOT NULL,
  high NUMERIC(18,6) NOT NULL,
  low NUMERIC(18,6) NOT NULL,
  close NUMERIC(18,6) NOT NULL,
  volume NUMERIC(18,2) NOT NULL,
  transactions INT NOT NULL,
  vwap NUMERIC(18,6),
  idempotency_key TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (ts, ticker)
);

SELECT create_hypertable('market_raw_minute', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS market_raw_minute_ticker_idx
  ON market_raw_minute (ticker, ts DESC);

-- =============================================================================
-- MARKET_RAW_DAY (Polygon daily OHLCV bars)
-- =============================================================================
CREATE TABLE IF NOT EXISTS market_raw_day (
  ticker TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  open NUMERIC(18,6) NOT NULL,
  high NUMERIC(18,6) NOT NULL,
  low NUMERIC(18,6) NOT NULL,
  close NUMERIC(18,6) NOT NULL,
  volume NUMERIC(18,2) NOT NULL,
  transactions INT NOT NULL,
  vwap NUMERIC(18,6),
  idempotency_key TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (ts, ticker)
);

SELECT create_hypertable('market_raw_day', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS market_raw_day_ticker_idx
  ON market_raw_day (ticker, ts DESC);

-- =============================================================================
-- FEATURES_TFT (engineered features for TFT model training)
-- =============================================================================
CREATE TABLE IF NOT EXISTS features_tft (
  ticker TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  time_idx INT NOT NULL,
  split TEXT NOT NULL DEFAULT 'train',
  -- Price features
  price NUMERIC(18,6) NOT NULL,
  open NUMERIC(18,6),
  high NUMERIC(18,6),
  low NUMERIC(18,6),
  close NUMERIC(18,6),
  volume NUMERIC(18,2),
  -- Returns
  returns_1 NUMERIC(12,8),
  returns_5 NUMERIC(12,8),
  returns_15 NUMERIC(12,8),
  target_return NUMERIC(12,8),
  -- Volatility
  volatility_5 NUMERIC(12,8),
  volatility_15 NUMERIC(12,8),
  -- Moving averages
  sma_5 NUMERIC(18,6),
  sma_20 NUMERIC(18,6),
  ema_5 NUMERIC(18,6),
  ema_20 NUMERIC(18,6),
  -- Indicators
  rsi_14 NUMERIC(8,4),
  volume_zscore NUMERIC(10,6),
  -- Temporal
  minute_of_day INT,
  hour_of_day INT,
  day_of_week INT,
  -- Metadata
  idempotency_key TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (ts, ticker)
);

SELECT create_hypertable('features_tft', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS features_tft_ticker_idx
  ON features_tft (ticker, ts DESC);

CREATE INDEX IF NOT EXISTS features_tft_split_idx
  ON features_tft (split);

-- =============================================================================
-- PREDICTIONS_TFT (TFT model predictions)
-- =============================================================================
CREATE TABLE IF NOT EXISTS predictions_tft (
  event_id UUID NOT NULL,
  correlation_id UUID NOT NULL,
  idempotency_key TEXT NOT NULL,
  ticker TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  horizon_minutes INT NOT NULL,
  direction TEXT NOT NULL,
  confidence NUMERIC(6,4) NOT NULL,
  q10 NUMERIC(10,6),
  q50 NUMERIC(10,6),
  q90 NUMERIC(10,6),
  model_version TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (ts, idempotency_key)
);

SELECT create_hypertable('predictions_tft', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS predictions_tft_ticker_ts_idx
  ON predictions_tft (ticker, ts DESC);

CREATE INDEX IF NOT EXISTS predictions_tft_correlation_idx
  ON predictions_tft (correlation_id);

-- =============================================================================
-- PAPER_TRADES (paper execution results)
-- =============================================================================
CREATE TABLE IF NOT EXISTS paper_trades (
  event_id UUID NOT NULL,
  correlation_id UUID NOT NULL,
  idempotency_key TEXT NOT NULL,
  ticker TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  side TEXT NOT NULL,
  qty NUMERIC(18,8) NOT NULL,
  limit_price NUMERIC(18,8),
  fill_price NUMERIC(18,8),
  slippage_bps NUMERIC(10,4),
  status TEXT NOT NULL,
  reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (ts, idempotency_key)
);

SELECT create_hypertable('paper_trades', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS paper_trades_ticker_ts_idx
  ON paper_trades (ticker, ts DESC);

CREATE INDEX IF NOT EXISTS paper_trades_correlation_idx
  ON paper_trades (correlation_id);

CREATE INDEX IF NOT EXISTS paper_trades_status_idx
  ON paper_trades (status);

-- =============================================================================
-- AUDIT_EVENTS (pipeline trace log)
-- =============================================================================
CREATE TABLE IF NOT EXISTS audit_events (
  event_id UUID NOT NULL,
  correlation_id UUID NOT NULL,
  idempotency_key TEXT NOT NULL,
  symbol TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  schema_version INT NOT NULL,
  source TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (ts, idempotency_key)
);

SELECT create_hypertable('audit_events', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS audit_events_correlation_idx
  ON audit_events (correlation_id);

CREATE INDEX IF NOT EXISTS audit_events_event_type_idx
  ON audit_events (event_type);

CREATE INDEX IF NOT EXISTS audit_events_symbol_idx
  ON audit_events (symbol);

-- =============================================================================
-- RISK_DECISIONS (risk gate decisions - GAP 10.1)
-- =============================================================================
CREATE TABLE IF NOT EXISTS risk_decisions (
  event_id UUID NOT NULL,
  correlation_id UUID NOT NULL,
  idempotency_key TEXT NOT NULL,
  ticker TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  approved BOOLEAN NOT NULL,
  reason TEXT,
  risk_score NUMERIC(10,6),
  cvar_95 NUMERIC(10,6),
  max_position NUMERIC(18,8),
  limits_snapshot JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (ts, idempotency_key)
);

SELECT create_hypertable('risk_decisions', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS risk_decisions_ticker_ts_idx
  ON risk_decisions (ticker, ts DESC);

CREATE INDEX IF NOT EXISTS risk_decisions_correlation_idx
  ON risk_decisions (correlation_id);

CREATE INDEX IF NOT EXISTS risk_decisions_approved_idx
  ON risk_decisions (approved);

-- =============================================================================
-- BACKFILL_CHECKPOINTS (resumable backfill tracking - GAP 10.2)
-- =============================================================================
CREATE TABLE IF NOT EXISTS backfill_checkpoints (
  source TEXT NOT NULL,
  filename TEXT NOT NULL,
  last_offset BIGINT NOT NULL DEFAULT 0,
  records_processed BIGINT NOT NULL DEFAULT 0,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (source, filename)
);

CREATE INDEX IF NOT EXISTS backfill_checkpoints_source_idx
  ON backfill_checkpoints (source);

-- =============================================================================
-- MODELS_REGISTRY (TFT model versions and deployment status)
-- =============================================================================
CREATE TABLE IF NOT EXISTS models_registry (
  model_id TEXT NOT NULL PRIMARY KEY,
  run_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('candidate', 'green', 'deprecated')),
  onnx_path TEXT NOT NULL,
  checkpoint_path TEXT NOT NULL,
  train_loss NUMERIC(12,8),
  val_loss NUMERIC(12,8),
  epochs INT,
  samples INT,
  train_duration_seconds NUMERIC(10,2),
  config JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  promoted_at TIMESTAMPTZ,
  deprecated_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS models_registry_status_idx
  ON models_registry (status);

CREATE INDEX IF NOT EXISTS models_registry_created_idx
  ON models_registry (created_at DESC);
