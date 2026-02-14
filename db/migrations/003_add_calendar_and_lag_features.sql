-- Migration: Add calendar features, Reddit lag features, and market context to features_tft
-- Run with: psql -h localhost -p 5433 -U awet -d awet -f db/migrations/003_add_calendar_and_lag_features.sql

BEGIN;

-- =============================================================================
-- A) Calendar Features
-- =============================================================================
DO $$
BEGIN
    -- is_weekend (0/1)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'is_weekend'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN is_weekend INTEGER DEFAULT 0;
    END IF;

    -- day_of_month (1-31)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'day_of_month'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN day_of_month INTEGER DEFAULT 1;
    END IF;

    -- week_of_year (1-53)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'week_of_year'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN week_of_year INTEGER DEFAULT 1;
    END IF;

    -- month_of_year (1-12)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'month_of_year'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN month_of_year INTEGER DEFAULT 1;
    END IF;

    -- is_month_end (0/1)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'is_month_end'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN is_month_end INTEGER DEFAULT 0;
    END IF;
END $$;

-- =============================================================================
-- B) Reddit Lag Features (lag1, lag3, lag5)
-- =============================================================================
DO $$
BEGIN
    -- reddit_mentions lags
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_mentions_lag1'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN reddit_mentions_lag1 INTEGER DEFAULT 0;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_mentions_lag3'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN reddit_mentions_lag3 INTEGER DEFAULT 0;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_mentions_lag5'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN reddit_mentions_lag5 INTEGER DEFAULT 0;
    END IF;

    -- reddit_sentiment_mean lags
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_sentiment_mean_lag1'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN reddit_sentiment_mean_lag1 NUMERIC(8,6) DEFAULT 0.0;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_sentiment_mean_lag3'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN reddit_sentiment_mean_lag3 NUMERIC(8,6) DEFAULT 0.0;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_sentiment_mean_lag5'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN reddit_sentiment_mean_lag5 NUMERIC(8,6) DEFAULT 0.0;
    END IF;

    -- reddit_sentiment_weighted lags
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_sentiment_weighted_lag1'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN reddit_sentiment_weighted_lag1 NUMERIC(8,6) DEFAULT 0.0;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_sentiment_weighted_lag3'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN reddit_sentiment_weighted_lag3 NUMERIC(8,6) DEFAULT 0.0;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_sentiment_weighted_lag5'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN reddit_sentiment_weighted_lag5 NUMERIC(8,6) DEFAULT 0.0;
    END IF;

    -- reddit_positive_ratio lags
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_positive_ratio_lag1'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN reddit_positive_ratio_lag1 NUMERIC(8,6) DEFAULT 0.0;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_positive_ratio_lag3'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN reddit_positive_ratio_lag3 NUMERIC(8,6) DEFAULT 0.0;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_positive_ratio_lag5'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN reddit_positive_ratio_lag5 NUMERIC(8,6) DEFAULT 0.0;
    END IF;

    -- reddit_negative_ratio lags
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_negative_ratio_lag1'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN reddit_negative_ratio_lag1 NUMERIC(8,6) DEFAULT 0.0;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_negative_ratio_lag3'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN reddit_negative_ratio_lag3 NUMERIC(8,6) DEFAULT 0.0;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_negative_ratio_lag5'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN reddit_negative_ratio_lag5 NUMERIC(8,6) DEFAULT 0.0;
    END IF;
END $$;

-- =============================================================================
-- C) Market Context (SPY/QQQ benchmark) - Optional
-- =============================================================================
DO $$
BEGIN
    -- market_return_1 (benchmark 1-period return)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'market_return_1'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN market_return_1 NUMERIC(12,8) DEFAULT 0.0;
    END IF;

    -- market_volatility_5 (benchmark 5-period volatility)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'market_volatility_5'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN market_volatility_5 NUMERIC(12,8) DEFAULT 0.0;
    END IF;
END $$;

COMMIT;

-- =============================================================================
-- Verification
-- =============================================================================
SELECT 'New calendar columns:' AS info;
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'features_tft' AND column_name IN ('is_weekend', 'day_of_month', 'week_of_year', 'month_of_year', 'is_month_end')
ORDER BY column_name;

SELECT 'New Reddit lag columns:' AS info;
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'features_tft' AND column_name LIKE 'reddit_%_lag%'
ORDER BY column_name;

SELECT 'New market context columns:' AS info;
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'features_tft' AND column_name IN ('market_return_1', 'market_volatility_5')
ORDER BY column_name;
