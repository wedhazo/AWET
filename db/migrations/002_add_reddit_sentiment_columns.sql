-- Migration: Add Reddit sentiment columns to features_tft and reddit_daily_mentions
-- Run with: psql -h localhost -p 5433 -U awet -d awet -f db/migrations/002_add_reddit_sentiment_columns.sql

BEGIN;

-- 1) Add reddit columns to features_tft (if missing)
DO $$
BEGIN
    -- Rename old reddit_mentions to reddit_mentions_count if exists
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_mentions'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_mentions_count'
    ) THEN
        ALTER TABLE features_tft RENAME COLUMN reddit_mentions TO reddit_mentions_count;
    END IF;

    -- Add reddit_mentions_count if missing
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_mentions_count'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN reddit_mentions_count INT DEFAULT 0;
    END IF;

    -- Add reddit_sentiment_mean
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_sentiment_mean'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN reddit_sentiment_mean NUMERIC(8,6) DEFAULT 0.0;
    END IF;

    -- Add reddit_sentiment_weighted
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_sentiment_weighted'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN reddit_sentiment_weighted NUMERIC(8,6) DEFAULT 0.0;
    END IF;

    -- Add reddit_positive_ratio
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_positive_ratio'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN reddit_positive_ratio NUMERIC(8,6) DEFAULT 0.0;
    END IF;

    -- Add reddit_negative_ratio
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'features_tft' AND column_name = 'reddit_negative_ratio'
    ) THEN
        ALTER TABLE features_tft ADD COLUMN reddit_negative_ratio NUMERIC(8,6) DEFAULT 0.0;
    END IF;
END $$;

-- 2) Add columns to reddit_daily_mentions (if missing)
DO $$
BEGIN
    -- Rename old mentions to mentions_count if exists
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'reddit_daily_mentions' AND column_name = 'mentions'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'reddit_daily_mentions' AND column_name = 'mentions_count'
    ) THEN
        ALTER TABLE reddit_daily_mentions RENAME COLUMN mentions TO mentions_count;
    END IF;

    -- Add mentions_count if missing
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'reddit_daily_mentions' AND column_name = 'mentions_count'
    ) THEN
        ALTER TABLE reddit_daily_mentions ADD COLUMN mentions_count INT DEFAULT 0;
    END IF;

    -- Add sentiment_mean
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'reddit_daily_mentions' AND column_name = 'sentiment_mean'
    ) THEN
        ALTER TABLE reddit_daily_mentions ADD COLUMN sentiment_mean NUMERIC(8,6) DEFAULT 0.0;
    END IF;

    -- Add sentiment_weighted
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'reddit_daily_mentions' AND column_name = 'sentiment_weighted'
    ) THEN
        ALTER TABLE reddit_daily_mentions ADD COLUMN sentiment_weighted NUMERIC(8,6) DEFAULT 0.0;
    END IF;

    -- Add positive_ratio
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'reddit_daily_mentions' AND column_name = 'positive_ratio'
    ) THEN
        ALTER TABLE reddit_daily_mentions ADD COLUMN positive_ratio NUMERIC(8,6) DEFAULT 0.0;
    END IF;

    -- Add negative_ratio
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'reddit_daily_mentions' AND column_name = 'negative_ratio'
    ) THEN
        ALTER TABLE reddit_daily_mentions ADD COLUMN negative_ratio NUMERIC(8,6) DEFAULT 0.0;
    END IF;
END $$;

-- 3) Create index on reddit_daily_mentions if missing
CREATE INDEX IF NOT EXISTS reddit_daily_mentions_ticker_idx
  ON reddit_daily_mentions (ticker, day DESC);

COMMIT;

-- Verification
SELECT 'features_tft columns:' AS info;
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'features_tft' AND column_name LIKE 'reddit_%'
ORDER BY column_name;

SELECT 'reddit_daily_mentions columns:' AS info;
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'reddit_daily_mentions'
ORDER BY column_name;
