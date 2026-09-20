-- Phase 7 migration
-- Add token_decimals column to tokens table
-- Run this once before deploying Phase 7

ALTER TABLE tokens ADD COLUMN IF NOT EXISTS token_decimals INTEGER;

-- Backfill: standard pump.fun tokens use 6 decimals
-- Tokens already in DB will have NULL until re-enriched or manually set.
-- The WS price calculator falls back to 6 if NULL, so this is safe.

COMMENT ON COLUMN tokens.token_decimals IS
  'SPL token decimal places. Fetched from Helius DAS at enrichment time. '
  'Used by Phase 7 WebSocket price calculator. NULL = not yet fetched (fallback: 6).';
