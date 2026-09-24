-- Phase 17 migration
-- Layered exit (breakeven + trailing stop, engine/trailing_stop.py) and
-- compounding position sizing for confluence_entry_v1, replacing the
-- fixed -6%/+30% stop/take-profit pair and the fixed $10-per-trade size.
--
-- Built 2026-09-23, same session as the removal of the old scorer/
-- S1Wave/CapitalEngine paper-trading pipeline. Applies to both the real
-- money table (confluence_live_trades) and its paper benchmark
-- (confluence_shadow_positions), so the two stay comparable.

ALTER TABLE confluence_live_trades
    ADD COLUMN IF NOT EXISTS high_watermark_price NUMERIC(30,12),
    ADD COLUMN IF NOT EXISTS trailing_stop_floor   NUMERIC(30,12);

ALTER TABLE confluence_shadow_positions
    ADD COLUMN IF NOT EXISTS high_watermark_price NUMERIC(30,12),
    ADD COLUMN IF NOT EXISTS trailing_stop_floor   NUMERIC(30,12);

COMMENT ON COLUMN confluence_live_trades.high_watermark_price IS
    'Highest price observed since entry (engine/trailing_stop.py staircase state). NULL until first post-entry tick.';
COMMENT ON COLUMN confluence_live_trades.trailing_stop_floor IS
    'Current stop floor, ratchets up in 10% steps as high_watermark_price rises, never moves down. NULL until first post-entry tick.';
COMMENT ON COLUMN confluence_shadow_positions.high_watermark_price IS
    'Highest price observed since entry (engine/trailing_stop.py staircase state). NULL until first post-entry tick.';
COMMENT ON COLUMN confluence_shadow_positions.trailing_stop_floor IS
    'Current stop floor, ratchets up in 10% steps as high_watermark_price rises, never moves down. NULL until first post-entry tick.';
