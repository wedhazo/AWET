-- Migration: De-duplicate predictions_tft rows from repeated demo runs
-- Run with: psql -h localhost -p 5433 -U awet -d awet -f db/migrations/004_dedup_predictions_tft.sql
--
-- Root cause: BaseEvent.ts defaults to datetime.now(), so each pipeline run
-- generates a different ts. The PK (ts, idempotency_key) therefore never
-- fires ON CONFLICT across runs, leaving N duplicate rows per
-- idempotency_key (one per run).
--
-- TimescaleDB hypertables require the partitioning column (`ts`) in every
-- unique constraint, so we cannot add UNIQUE(idempotency_key) alone.
-- The chosen fix is two-fold:
--   1. Data layer: keep only the row matching the most recent
--      correlation_id for each idempotency_key (this migration).
--   2. Query layer: replay_pipeline.py now filters by correlation_id
--      when fetching predictions, so it always picks the correct run.
--   3. Optionally add UNIQUE(ts, idempotency_key, correlation_id) to
--      catch future duplicate inserts from the same run.

BEGIN;

-- Step 1: Remove duplicate rows, keeping only the latest per idempotency_key.
-- "Latest" = max(created_at), ties broken by max(ts).
DELETE FROM predictions_tft p
WHERE p.ctid NOT IN (
    SELECT DISTINCT ON (idempotency_key) ctid
    FROM predictions_tft
    ORDER BY idempotency_key, created_at DESC, ts DESC
);

-- Step 2: Report remaining row count.
DO $$
DECLARE
    cnt bigint;
BEGIN
    SELECT count(*) INTO cnt FROM predictions_tft;
    RAISE NOTICE 'predictions_tft: % rows after dedup', cnt;
END $$;

COMMIT;
