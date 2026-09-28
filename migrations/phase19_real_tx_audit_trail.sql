-- Phase 19 migration
-- Real, on-chain-verified cash flow per trade (2026-09-28) — a full audit
-- (triggered by a 3-day dry-spell investigation, then a deep wallet
-- reconciliation) found entry_sol_lamports/exit_sol_lamports (both
-- INTENDED/quoted amounts) understated real spend by 6.36x in aggregate
-- across 71 real trades. Root cause, confirmed via a direct on-chain
-- instruction trace: a mandatory Pump.fun protocol-fee token account
-- (owned by Pump.fun's fee collector, never this wallet, never
-- reclaimable) that some sells must create — on one confirmed trade this
-- fee alone exceeded the entire quoted gain, turning a trade pnl_usd
-- called profitable into a real net loss. See models/orm.py's
-- ConfluenceLiveTrade.entry_real_sol_lamports docstring for the full story.

ALTER TABLE confluence_live_trades
    ADD COLUMN IF NOT EXISTS entry_real_sol_lamports  BIGINT,
    ADD COLUMN IF NOT EXISTS exit_real_sol_lamports    BIGINT,
    ADD COLUMN IF NOT EXISTS reclaim_tx_signature      VARCHAR(128),
    ADD COLUMN IF NOT EXISTS reclaim_sol_lamports       BIGINT,
    ADD COLUMN IF NOT EXISTS real_pnl_usd               NUMERIC(12,6);

COMMENT ON COLUMN confluence_live_trades.entry_real_sol_lamports IS
    'Real wallet SOL balance delta on the entry transaction (from actual pre/post balances, never a quote estimate). Always negative.';
COMMENT ON COLUMN confluence_live_trades.exit_real_sol_lamports IS
    'Real wallet SOL balance delta on the exit (sell) transaction itself, excluding the separate rent-reclaim transaction — from actual pre/post balances, never a quote estimate.';
COMMENT ON COLUMN confluence_live_trades.reclaim_tx_signature IS
    'Signature of the separate CloseAccount transaction that reclaimed this trade''s own token-account rent, if any.';
COMMENT ON COLUMN confluence_live_trades.reclaim_sol_lamports IS
    'Real lamports reclaimed by the CloseAccount transaction above, if any.';
COMMENT ON COLUMN confluence_live_trades.real_pnl_usd IS
    'True net P&L in USD = (entry_real_sol_lamports + exit_real_sol_lamports + reclaim_sol_lamports) converted at the SOL price at close. The trustworthy number — pnl_usd is kept for historical continuity but understates real losses.';
