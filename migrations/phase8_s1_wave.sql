-- Phase 8 migration
-- S1 Wave Entry Layer
-- Run once before deploying Phase 8. Safe to run on a live DB — all changes
-- are additive (new enum type, new columns, new exit reason value).

-- ── 1. entry_source enum ─────────────────────────────────────────────────────
-- Identifies how a trade was originated.
--   scorer       — opened by the 3-window rolling scorer (existing behaviour)
--   s1_wave      — opened by the S1 wave entry layer on snapshot 1 signal
--   scorer_addon — position added by scorer STRONG_BUY on an existing s1_wave trade

CREATE TYPE entry_source AS ENUM ('scorer', 's1_wave', 'scorer_addon');

-- ── 2. New columns on trades ──────────────────────────────────────────────────

-- Which entry layer opened this trade
ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS entry_source entry_source NOT NULL DEFAULT 'scorer';

-- Trailing stop is only active for s1_wave trades.
-- False for all scorer trades — they use the fixed stop loss / take profit rules.
ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS trailing_stop_active BOOLEAN NOT NULL DEFAULT FALSE;

-- Current stop floor price. Updated by the staircase on every 10% profit increment.
-- NULL for scorer trades (they don't use a trailing stop).
-- For s1_wave trades: starts at entry_price * (1 - STOP_LOSS_PCT) = entry * 0.94
ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS trailing_stop_floor NUMERIC(30, 12);

-- Highest price seen since entry. Updated on every MarketEvent tick.
-- NULL for scorer trades.
-- For s1_wave trades: initialised to entry_price at open, updated upward only.
ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS high_watermark_price NUMERIC(30, 12);

-- ── 3. New exit reason for trailing stop hits ─────────────────────────────────
-- PostgreSQL requires ALTER TYPE to add enum values.
-- The existing exit_reason enum is: HARD_FLOOR, STOP_LOSS, TAKE_PROFIT, TIME_EXIT

ALTER TYPE exit_reason ADD VALUE IF NOT EXISTS 'TRAILING_STOP';

-- ── 4. Index for fast open s1_wave trade lookups ──────────────────────────────
CREATE INDEX IF NOT EXISTS ix_trades_entry_source
    ON trades (entry_source)
    WHERE status = 'OPEN';

-- ── 5. Backfill ───────────────────────────────────────────────────────────────
-- All existing trades are scorer-originated. The DEFAULT above handles new rows.
-- Existing rows already have entry_source = 'scorer' from the DEFAULT.
-- trailing_stop_active = FALSE is already correct for all existing trades.
-- trailing_stop_floor and high_watermark_price remain NULL for all existing trades.

COMMENT ON COLUMN trades.entry_source IS
    'Entry layer that opened this position: scorer (3-window), s1_wave (snapshot-1 BP signal), scorer_addon (scorer STRONG_BUY add to existing s1_wave position).';

COMMENT ON COLUMN trades.trailing_stop_active IS
    'True only for s1_wave trades. Scorer trades use fixed stop loss / take profit rules.';

COMMENT ON COLUMN trades.trailing_stop_floor IS
    'Current stop price for trailing stop staircase. NULL for scorer trades. '
    'Starts at entry_price * 0.94 (-6%). Moves up at each 10% profit increment. Never moves down.';

COMMENT ON COLUMN trades.high_watermark_price IS
    'Highest price seen since entry. Updated on every tick for s1_wave trades. '
    'Drives the trailing stop staircase — next floor = (floor(hwm / (entry*0.1)) - 1) * entry*0.1.';
