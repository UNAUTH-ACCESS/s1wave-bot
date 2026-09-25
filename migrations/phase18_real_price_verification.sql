-- Phase 18 migration
-- Real, verified price for the dashboard (2026-09-25) — the user kept
-- being confused by a DexScreener-sourced snapshot price sitting still
-- ("frozen"). That's real, thin-liquidity market behavior (see the
-- frozen-price analysis in workers/confluence_live_worker.py's history:
-- 60+ min frozen positions had a 0% rug rate and 98.6% win rate — freezing
-- is not a bug and not inherently bad), but a snapshot that looks stuck
-- reads as broken even when it isn't. Rather than fetch prices faster
-- (which caused a real rate-limit incident earlier this session — see
-- _LIQUIDITY_CHECK_INTERVAL_S's history), this surfaces the REAL,
-- executable price already being fetched every ~60s by the liquidity
-- guard, so the dashboard shows a verified number instead of a raw
-- market snapshot.

ALTER TABLE confluence_live_trades
    ADD COLUMN IF NOT EXISTS real_price             NUMERIC(30,12),
    ADD COLUMN IF NOT EXISTS real_pnl_pct            NUMERIC(10,4),
    ADD COLUMN IF NOT EXISTS real_price_checked_at   TIMESTAMPTZ;

COMMENT ON COLUMN confluence_live_trades.real_price IS
    'Real, executable price derived from an actual Jupiter sell quote for the exact held size — not the DexScreener snapshot price. Refreshed on the same ~60s cadence as the liquidity guard. NULL until the first real check completes for a new position.';
COMMENT ON COLUMN confluence_live_trades.real_pnl_pct IS
    'Real P&L percent implied by real_price, computed the same way the liquidity guard does. NULL until the first real check.';
COMMENT ON COLUMN confluence_live_trades.real_price_checked_at IS
    'When real_price was last verified via an actual Jupiter quote.';
