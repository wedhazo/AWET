-- Migration: Add unique constraint on (ts, correlation_id, idempotency_key)
-- Run with: psql -h localhost -p 5433 -U awet -d awet -f db/migrations/005_unique_prediction_per_run.sql
--
-- Rationale:
--   A single pipeline run (identified by correlation_id) must never insert
--   more than one prediction row for the same idempotency_key.  Without a
--   DB-level constraint this invariant can only be enforced at the
--   application layer, which is fragile.
--
--   TimescaleDB hypertables require the partitioning column (`ts`) in
--   every unique index.  We therefore include `ts` in the constraint.
--   Because each run already produces a single ts per idempotency_key,
--   including ts does not weaken the guarantee.
--
-- Prerequisite: run 004_dedup_predictions_tft.sql first to remove
--   any pre-existing duplicates.

BEGIN;

CREATE UNIQUE INDEX IF NOT EXISTS uq_predictions_tft_run
    ON predictions_tft (ts, correlation_id, idempotency_key);

-- Verify: should be 0
DO $$
DECLARE
    dup_cnt bigint;
BEGIN
    SELECT count(*) INTO dup_cnt
    FROM (
        SELECT ts, correlation_id, idempotency_key, count(*)
        FROM predictions_tft
        GROUP BY ts, correlation_id, idempotency_key
        HAVING count(*) > 1
    ) sub;
    IF dup_cnt > 0 THEN
        RAISE EXCEPTION 'Found % duplicate groups — run 004 dedup first', dup_cnt;
    END IF;
    RAISE NOTICE 'uq_predictions_tft_run created; 0 duplicates detected';
END $$;

COMMIT;
